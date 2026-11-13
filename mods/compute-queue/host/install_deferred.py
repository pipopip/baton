#!/usr/bin/env python3
"""Install the user-scoped deferred controller and task-only timer on a Linux host."""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile

SOURCE_BIN = Path(__file__).resolve().parent.parent / 'bin'
sys.path.insert(0, str(SOURCE_BIN))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from queue_policy import FLEET_SLICE, fleet_slice_cpu_quota, fleet_slice_limit, policy_fields

BASE_KEYS = ('version', 'machine_id', 'pool_slots', 'load_cap', 'ram_floor_gb')


def policy_relation(old, new):
    try:
        if (not isinstance(old, dict) or any(old.get(k) != new[k] for k in BASE_KEYS)
                or set(old) - set(BASE_KEYS) - set(policy_fields(new))):
            return 'base differs'
        return 'same' if policy_fields(old, new['pool_slots'], validate_zone=True) == policy_fields(new, new['pool_slots'], validate_zone=True) else 'update'
    except (ValueError, KeyError):
        return 'base differs'


@contextlib.contextmanager
def update_locks(queue, pool):
    """Serialize registration and hold the heavy gate and every legacy slot."""
    import fcntl
    uid = os.getuid()
    st = queue.lstat()
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != uid:
        raise RuntimeError('unsafe queue directory')
    held = []
    try:
        for name in ('.deferred.lock', '.heavy.gate', *(f'.heavy.{i}' for i in range(pool))):
            fd = os.open(queue / name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            held.append(fd)
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode) or st.st_uid != uid:
                raise RuntimeError('unsafe queue lock')
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError('queue or heavy work is active') from exc
        yield
    finally:
        for fd in reversed(held):
            os.close(fd)


def _owned(path, directory=False):
    st = path.lstat()
    if st.st_uid != os.getuid() or not (stat.S_ISDIR(st.st_mode) if directory else stat.S_ISREG(st.st_mode)):
        raise RuntimeError('unsafe queue evidence: ' + str(path))


def _read_json(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'r') as stream:
        st = os.fstat(stream.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid():
            raise RuntimeError('unsafe queue evidence: ' + str(path))
        return json.load(stream)


def _never_registers(queue, name):
    """Whether the job directory `name`, which has no state, was cancelled before its registration: the helper keeps a
    record of that beside the directories (.deferred-cancel-<id>, written by its `cancel`) and answers every later
    `register` of that ID with it, so the directory can never become a registered job, whatever its descriptor says.
    Such a directory stays until the controller's cleanup takes it. On 13 Nov 2026 one, two days old, kept ember's
    pool from growing."""
    record = queue / ('.deferred-cancel-' + name)
    if not (record.exists() or record.is_symlink()):
        return False
    value = _read_json(record)   # raises for a link or another owner's file: unsafe evidence is never a reason to go on
    return isinstance(value, dict) and value.get('id') == name and value.get('phase') == 'cancelled'


def check_drained(queue):
    """Reject staged, active, malformed and ambiguous records; keep history intact."""
    with os.scandir(queue) as entries:
        jobs = [queue / entry.name for entry in entries if entry.name.startswith('cq-')]
    for job in jobs:
        _owned(job, True)
        descriptor, state, exit_file = job / 'deferred.json', job / 'deferred-state.json', job / 'exit'
        has_desc = descriptor.exists() or descriptor.is_symlink()
        has_state = state.exists() or state.is_symlink()
        if not has_state and _never_registers(queue, job.name):
            continue
        if has_desc or has_state:
            if not has_desc or not has_state:
                raise RuntimeError('unresolved staged deferred job: ' + job.name)
            value, record = _read_json(descriptor), _read_json(state)
            digest = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            if (not isinstance(value, dict) or value.get('version') != 1 or value.get('id') != job.name
                    or not isinstance(record, dict) or record.get('version') != 1 or record.get('id') != job.name
                    or record.get('descriptor_hash') != digest
                    or not isinstance(record.get('policy_hash'), str)
                    or not re.fullmatch(r'[0-9a-f]{64}', record['policy_hash'])
                    or record.get('phase') not in ('done', 'failed', 'cancelled')):
                raise RuntimeError('unresolved or invalid deferred job: ' + job.name)
        elif exit_file.exists() or exit_file.is_symlink():
            _owned(exit_file)
        else:
            raise RuntimeError('unknown or unresolved queue job: ' + job.name)


def pool_grows(old, new):
    """Whether the installed policy `old` differs from `new` in one thing only: a larger pool. Machine, load cap, RAM
    floor and the scheduling fields (each read against its own pool) are the same and nothing unknown is installed."""
    try:
        return (isinstance(old, dict) and all(old.get(k) == new[k] for k in BASE_KEYS if k != 'pool_slots')
                and type(old.get('pool_slots')) is int and 0 < old['pool_slots'] < new['pool_slots']
                and not set(old) - set(BASE_KEYS) - set(policy_fields(new))
                and policy_fields(old, old['pool_slots'], validate_zone=True) == policy_fields(new, new['pool_slots'], validate_zone=True))
    except (ValueError, KeyError, TypeError):
        return False


def check_nothing_unstarted(queue):
    """For a pool that only grows. A running job keeps its unit, its slot locks and its state, and none of them is
    read against the pool's size again (the descriptor and the policy fingerprint are consulted at registration and
    at the start only), so running jobs may stay. A registered job that has not started carries the old pool in its
    descriptor and the old policy in its state and would be refused at its start: while one exists, or a record
    that cannot be read, the pool is not changed. Nor while a directory has neither file and no sign that an
    ordinary job's wrapper ran in it (`pid`, written as the wrapper starts, or `exit`): that is a job whose source
    is still being pushed, and its descriptor, written when the push ends, would name the old pool and be refused,
    leaving a directory the host never registers and a job its controller cannot settle. A directory left like that
    by a push that died has to be removed by hand before the pool can grow. A directory without state whose job was
    cancelled before registration is passed over (_never_registers)."""
    with os.scandir(queue) as entries:
        jobs = [queue / entry.name for entry in entries if entry.name.startswith('cq-')]
    for job in jobs:
        _owned(job, True)
        descriptor, state = job / 'deferred.json', job / 'deferred-state.json'
        has_desc = descriptor.exists() or descriptor.is_symlink()
        has_state = state.exists() or state.is_symlink()
        if not has_state and _never_registers(queue, job.name):
            continue
        if not has_desc and not has_state:
            ran = [job / name for name in ('pid', 'exit') if (job / name).exists() or (job / name).is_symlink()]
            if not ran:
                raise RuntimeError('a job is being staged, or was left half staged: ' + job.name)
            for path in ran:
                _owned(path)
            continue   # an ordinary immediate job: it never reads the deferred policy
        if not has_desc or not has_state:
            raise RuntimeError('unresolved staged deferred job: ' + job.name)
        record = _read_json(state)
        if not isinstance(record, dict) or record.get('phase') not in ('running', 'done', 'failed', 'cancelled'):
            raise RuntimeError('a registered job has not started yet: ' + job.name)


@contextlib.contextmanager
def registration_lock(queue):
    """The helper's own lock: no registration, tick or probe runs while it is held. Waits up to a minute for it."""
    import fcntl
    import time
    fd = os.open(queue / '.deferred.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid():
            raise RuntimeError('unsafe queue lock')
        deadline = time.monotonic() + 60
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if time.monotonic() >= deadline:
                    raise RuntimeError('the helper stayed busy for a minute') from exc
                time.sleep(.5)
        yield
    finally:
        os.close(fd)


def check_no_helper_process():
    """Do not release an already-loaded old helper after changing policy."""
    proc = Path('/proc')
    if not proc.is_dir():
        raise RuntimeError('cannot inspect helper processes')
    for item in proc.iterdir():
        if not item.name.isdigit() or int(item.name) == os.getpid():
            continue
        try:
            if item.stat().st_uid != os.getuid():
                continue
            args = (item / 'cmdline').read_bytes().split(b'\0')
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise RuntimeError('cannot inspect helper processes') from exc
        if any(arg.replace(b'\\', b'/').rsplit(b'/', 1)[-1] == b'deferred.py' for arg in args):
            raise RuntimeError('a deferred helper process is active or waiting')


def atomic_policy(config, policy):
    fd, temp = tempfile.mkstemp(prefix='.deferred-policy-', dir=config.parent)
    try:
        with os.fdopen(fd, 'w') as out:
            json.dump(policy, out, sort_keys=True)
            out.write('\n')
            out.flush()
            os.fsync(out.fileno())
        os.chmod(temp, 0o600)
        os.replace(temp, config)
        directory_fd = os.open(config.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def install_file(source, target):
    fd, temp = tempfile.mkstemp(prefix='.deferred-', dir=target.parent)
    try:
        with os.fdopen(fd, 'wb') as out, source.open('rb') as src:
            shutil.copyfileobj(src, out)
            out.flush()
            os.fsync(out.fileno())
        os.chmod(temp, 0o700)
        os.replace(temp, target)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def meminfo_total():
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemTotal:'):
            return int(line.split()[1]) * 1024
    raise RuntimeError('MemTotal unavailable')


def fleet_slice_unit(total_bytes, ram_floor_gb, pool_slots=None, foreground_slots=2):
    """Text of the user unit for the slice every fleet job runs in: its memory limit is the host's memory less the RAM
    floor, and it has no swap. Jobs that together need more than that squeeze one another's file cache first and can
    only be killed inside the slice, so the floor stays with the rest of the machine; the caps the controller admits
    never add up to more than the memory available less the floor, so the limit does not cut a job below its own cap.
    A host whose pool would be under 2 GiB gets no limit (and then no expected-use admission: the helper checks the
    limit)."""
    limit = fleet_slice_limit(total_bytes, ram_floor_gb)
    text = '[Unit]\nDescription=Baton fleet jobs memory pool\n\n[Slice]\n'
    if limit is not None:
        text += 'MemoryMax=' + str(limit) + '\nMemorySwapMax=0\n'
    if pool_slots is not None:
        # all fleet jobs together: the CPU of the fleet's slots (the helper charges expected CPU only with this in place)
        text += 'CPUQuota=' + str(fleet_slice_cpu_quota(pool_slots, foreground_slots)) + '%\n'
    return text


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--pool-slots', type=int, required=True)
    p.add_argument('--load-cap', type=float, required=True)
    p.add_argument('--ram-floor-gb', type=float, required=True)
    p.add_argument('--foreground-slots', type=int, default=2)
    p.add_argument('--timezone', default='Europe/London')
    p.add_argument('--window-start', default='00:00')
    p.add_argument('--window-end', default='08:00')
    p.add_argument('--update-policy', action='store_true', help='update scheduling fields only on a fully drained host')
    p.add_argument('--grow-pool', action='store_true',
                   help='raise --pool-slots above the installed pool; running jobs may stay, none may be registered and unstarted')
    args = p.parse_args()
    if not 1 <= args.pool_slots <= min(32, os.cpu_count() or 0) or not 0 < args.load_cap < 100000 or not 0 < args.ram_floor_gb < 100000:
        p.error('invalid host policy')
    try:
        fields = policy_fields(vars(args), args.pool_slots, validate_zone=True)
    except ValueError as exc:
        p.error(str(exc))
    if subprocess.check_output(['loginctl', 'show-user', str(os.getuid()), '--property=Linger', '--value'], text=True).strip() != 'yes':
        raise SystemExit('enable user lingering before installation')
    home = Path.home()
    target = home / '.local/share/baton/compute-deferred'
    units = home / '.config/systemd/user'
    target.mkdir(parents=True, exist_ok=True, mode=0o700)
    units.mkdir(parents=True, exist_ok=True, mode=0o700)
    policy = {'version': 1, 'machine_id': Path('/etc/machine-id').read_text().strip(),
              'pool_slots': args.pool_slots, 'load_cap': args.load_cap, 'ram_floor_gb': args.ram_floor_gb, **fields}
    config = target / 'config.json'
    source_policy = Path(__file__).with_name('queue_policy.py')
    if not source_policy.exists():
        source_policy = SOURCE_BIN / 'queue_policy.py'
    source_helper = Path(__file__).with_name('deferred.py')
    if config.exists():
        old = json.loads(config.read_text())
        relation = policy_relation(old, policy)
        if relation == 'base differs' and args.grow_pool and pool_grows(old, policy):
            # The one base change that is supported, and only this way. The controller must stop sending this host
            # work first (host.conf.json "paused": true) and have its own "slots" raised to match afterwards: a job
            # staged across the change would be refused at registration.
            try:
                with registration_lock(home / 'q'):
                    current = _read_json(config)   # nothing is inferred from the pre-lock read
                    if not pool_grows(current, policy):
                        raise RuntimeError('installed policy changed meanwhile')
                    for source, installed in ((source_helper, target / 'deferred.py'), (source_policy, target / 'queue_policy.py')):
                        _owned(installed)
                        if source.read_bytes() != installed.read_bytes():
                            raise RuntimeError('install this helper version with unchanged policy before growing the pool')
                    check_nothing_unstarted(home / 'q')
                    atomic_policy(config, policy)
            except (OSError, RuntimeError, ValueError) as exc:
                raise SystemExit('pool not grown: ' + str(exc)) from exc
            relation = 'same'
        if relation == 'base differs':
            raise SystemExit('installed base policy differs; load, RAM and machine changes are not supported, and the pool '
                             'only grows, with --grow-pool')
        if relation == 'update':
            if not args.update_policy:
                raise SystemExit('installed scheduling policy differs; pass --update-policy after draining the host')
            try:
                with update_locks(home / 'q', args.pool_slots):
                    # Nothing is inferred from the pre-lock read.
                    current = _read_json(config)
                    relation = policy_relation(current, policy)
                    if relation == 'base differs':
                        raise RuntimeError('installed base policy differs')
                    if relation == 'update':
                        # A previously loaded old helper could ignore new fields.
                        for source, installed in ((source_helper, target / 'deferred.py'), (source_policy, target / 'queue_policy.py')):
                            _owned(installed)
                            if source.read_bytes() != installed.read_bytes():
                                raise RuntimeError('install this helper version with unchanged policy before updating policy')
                        check_no_helper_process()
                        check_drained(home / 'q')
                        atomic_policy(config, policy)
            except (OSError, RuntimeError, ValueError) as exc:
                raise SystemExit('policy update refused: ' + str(exc)) from exc
    # Install the dependency first; never rewrite an existing equivalent policy:
    # registered jobs fingerprint the exact original JSON object.
    install_file(source_policy, target / 'queue_policy.py')
    install_file(source_helper, target / 'deferred.py')
    if not config.exists():
        config.write_text(json.dumps(policy, sort_keys=True) + '\n')
        os.chmod(config, 0o600)
    # Escape paths for systemd's ExecStart parser; never interpolate through a shell.
    def quote(s):
        return '"' + str(s).replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%') + '"'
    (units / 'baton-deferred.service').write_text('[Unit]\nDescription=Baton deferred task scheduler\n\n[Service]\nType=oneshot\nExecStart=' + quote(sys.executable) + ' ' + quote(target / 'deferred.py') + ' tick\n')
    (units / 'baton-deferred.timer').write_text('[Unit]\nDescription=Check Baton deferred tasks\n\n[Timer]\nOnBootSec=30s\nOnUnitActiveSec=60s\nAccuracySec=10s\nUnit=baton-deferred.service\n\n[Install]\nWantedBy=timers.target\n')
    (units / FLEET_SLICE).write_text(fleet_slice_unit(meminfo_total(), args.ram_floor_gb, args.pool_slots,
                                                      policy_fields(policy, args.pool_slots)['foreground_slots']))
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
    subprocess.run(['systemctl', '--user', 'enable', '--now', 'baton-deferred.timer'], check=True)
    subprocess.run([sys.executable, str(target / 'deferred.py'), 'check'], check=True)


if __name__ == '__main__':
    main()
