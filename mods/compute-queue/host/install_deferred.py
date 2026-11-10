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
from queue_policy import policy_fields

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


def check_drained(queue):
    """Reject staged, active, malformed and ambiguous records; keep history intact."""
    with os.scandir(queue) as entries:
        jobs = [queue / entry.name for entry in entries if entry.name.startswith('cq-')]
    for job in jobs:
        _owned(job, True)
        descriptor, state, exit_file = job / 'deferred.json', job / 'deferred-state.json', job / 'exit'
        has_desc = descriptor.exists() or descriptor.is_symlink()
        has_state = state.exists() or state.is_symlink()
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
        if relation == 'base differs':
            raise SystemExit('installed base policy differs; pool, load, RAM and machine changes are not supported')
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
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
    subprocess.run(['systemctl', '--user', 'enable', '--now', 'baton-deferred.timer'], check=True)
    subprocess.run([sys.executable, str(target / 'deferred.py'), 'check'], check=True)


if __name__ == '__main__':
    main()
