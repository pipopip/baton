#!/usr/bin/env python3
"""Durable, conservative user-systemd controller for staged compute jobs."""
import argparse
import contextlib
import ctypes
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from zoneinfo import ZoneInfo

# The installed helper lives next to this module; the source tree keeps it in bin.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'bin'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from queue_policy import (CPU_OVERCOMMIT, CPU_QUOTA_WAIT, FLEET_SLICE, OOM_REASON_PREFIX, POOL_MARGIN_BYTES, POOL_OVERCOMMIT, POOL_SLACK_BYTES,
                          RESOURCE_POLICY_VERSION, policy_fields)

VERSION = 1
GIB = 1024 ** 3
Q = Path.home() / 'q'
CGROUP = Path('/sys/fs/cgroup')
SCRIPT = Path(__file__).resolve()
POLICY = SCRIPT.parent / 'config.json'
ID = re.compile(r'cq-[A-Za-z0-9_-]+(?:\.p[1-8])?\Z')
SHA = re.compile(r'[0-9a-f]{40,64}\Z')
TERMINAL = ('done', 'failed', 'cancelled')


COMMAND_TIMEOUT_S = 30
TICK_SOON_S = (1, 6)   # tick_soon: how long after a runner's last act the scheduler looks again, and once more in case
                       # the unit had not yet gone at the first look
TICK_RETRY_S = (15,)   # ... after a runner that did not start its command (the claim goes back to the queue): slower,
                       # so a condition that flaps cannot turn into a claim and a refusal every second


def command(*args, check=True):
    return subprocess.run(args, capture_output=True, text=True, check=check, timeout=COMMAND_TIMEOUT_S)


def safe(path, directory=False):
    st = path.lstat()
    kind = stat.S_ISDIR(st.st_mode) if directory else stat.S_ISREG(st.st_mode)
    if not kind or st.st_uid != os.getuid():
        raise RuntimeError('unsafe or unowned path: ' + str(path))
    return st


def valid_id(jid):
    if not ID.fullmatch(jid or ''):
        raise RuntimeError('invalid job ID')
    safe(Q, True)


def jobdir(jid):
    valid_id(jid)
    path = Q / jid
    safe(path, True)
    return path


def atomic(path, data):
    fd, tmp = tempfile.mkstemp(prefix='.deferred-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(data, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def read(path):
    try:
        safe(path)
    except FileNotFoundError:
        return None
    return json.loads(path.read_text())


def open_lock(path):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    st = os.fstat(fd)
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid():
        os.close(fd)
        raise RuntimeError('unsafe lock: ' + str(path))
    return fd


@contextlib.contextmanager
def locked(path):
    fd = open_lock(path)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def try_gate(shared=False):
    """Never wait for the heavy gate while holding the deferred lock."""
    fd = open_lock(Q / '.heavy.gate')
    try:
        fcntl.flock(fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        os.close(fd)
        return None
    except BaseException:
        os.close(fd)
        raise


def validate_descriptor(value, jid, policy):
    if not isinstance(value, dict) or value.get('version') != VERSION or value.get('id') != jid:
        raise RuntimeError('invalid descriptor identity or version')
    if type(value.get('fleet', False)) is not bool or value.get('priority', 'lane') not in ('verify', 'fixcheck', 'lane', 'background'):
        raise RuntimeError('invalid fleet or priority')
    if (value.get('mode') not in ('spare', 'overnight', 'immediate')
            or (value.get('mode') == 'immediate' and not value.get('fleet'))):
        raise RuntimeError('invalid scheduling policy')
    for key, low, high in (('slots', 1, 32), ('pool_slots', 1, 32), ('est_minutes', 1, 480), ('timeout_minutes', 1, 10080)):
        if type(value.get(key)) is not int or not low <= value[key] <= high:
            raise RuntimeError('invalid ' + key)
    for key in ('memory_max_gb', 'ram_floor_gb', 'load_cap'):
        v = value.get(key)
        if type(v) not in (int, float) or not math.isfinite(v) or v <= 0 or v > 100000:
            raise RuntimeError('invalid ' + key)
    expected = value.get('memory_expected_gb')
    if expected is not None and (type(expected) not in (int, float) or not math.isfinite(expected)
                                 or expected <= 0 or expected > value['memory_max_gb']):
        raise RuntimeError('invalid memory_expected_gb')
    cores = value.get('cpu_expected')
    if cores is not None and (type(cores) not in (int, float) or not math.isfinite(cores) or cores <= 0 or cores > value['slots']):
        raise RuntimeError('invalid cpu_expected')
    submitted = value.get('submitted')
    if submitted is not None and (type(submitted) not in (int, float) or not math.isfinite(submitted) or submitted <= 0):
        raise RuntimeError('invalid submitted')
    if any(value[k] != policy[k] for k in ('pool_slots', 'load_cap', 'ram_floor_gb')):
        raise RuntimeError('descriptor does not match installed host policy')
    try:
        installed = policy_fields(policy, value['pool_slots'], validate_zone=True)
        described = policy_fields(value, value['pool_slots'], validate_zone=True)
    except ValueError as exc:
        raise RuntimeError('invalid scheduling policy') from exc
    if described != installed:
        raise RuntimeError('descriptor does not match installed host policy')
    if value['slots'] > value['pool_slots'] - installed['foreground_slots']:
        raise RuntimeError('job must leave configured pool slots for foreground')
    if not isinstance(value.get('source_sha'), str) or not SHA.fullmatch(value['source_sha']):
        raise RuntimeError('invalid source SHA')


def desc(jid):
    d = jobdir(jid)
    value = read(d / 'deferred.json')
    validate_descriptor(value, jid, host_policy())
    for name in ('run.sh', 'cmd.sh', 'job.env'):
        safe(d / name)
    wrapper = (d / 'run.sh').read_text()
    if '. "$D/job.env"\nSELFHEAVY=1\nexport SLOTS SELFHEAVY TIMEOUT_S SUBDIR' not in wrapper:
        raise RuntimeError('deferred wrapper must force SELFHEAVY=1 after job.env')
    safe(d / 'src', True)
    actual = command('git', '-C', str(d / 'src'), 'rev-parse', 'HEAD').stdout.strip()
    if actual != value['source_sha']:
        raise RuntimeError('source SHA mismatch')
    if command('git', '-C', str(d / 'src'), 'status', '--porcelain', '--untracked-files=all').stdout.strip():
        raise RuntimeError('staged source is not clean')
    return value


def machine_id():
    value = Path('/etc/machine-id').read_text().strip()
    if not re.fullmatch(r'[0-9a-f]{32}', value):
        raise RuntimeError('invalid machine ID')
    return value


def host_policy():
    value = read(POLICY)
    if not isinstance(value, dict) or value.get('version') != VERSION or value.get('machine_id') != machine_id():
        raise RuntimeError('missing or mismatched installed host policy')
    pool = value.get('pool_slots')
    if type(pool) is not int or not 1 <= pool <= 32 or pool > (os.cpu_count() or 0):
        raise RuntimeError('invalid installed pool size')
    for key in ('load_cap', 'ram_floor_gb'):
        if type(value.get(key)) not in (int, float) or not math.isfinite(value[key]) or value[key] <= 0:
            raise RuntimeError('invalid installed ' + key)
    try:
        policy_fields(value, pool, validate_zone=True)
    except ValueError as exc:
        raise RuntimeError('invalid installed scheduling policy') from exc
    return value


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def boot():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def self_group():
    path = next((line.split(':', 2)[2] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::')), '')
    if not path.startswith('/') or '..' in path.split('/') or path == '/':
        raise RuntimeError('runner cgroup is unavailable')
    return path


def save(jid, state):
    atomic(jobdir(jid) / 'deferred-state.json', state)


def register(jid):
    valid_id(jid)
    tombstone = read(Q / ('.deferred-cancel-' + jid))
    if tombstone:
        return tombstone
    d = jobdir(jid)
    old = read(d / 'deferred-state.json')
    if old:
        value = read(d / 'deferred.json')
        digest = fingerprint(value)
        if old.get('descriptor_hash') != digest:
            raise RuntimeError('job ID already registered with another descriptor')
        return old
    check()
    value = desc(jid)
    digest = fingerprint(value)
    for name in ('pid', 'pgid', 'launched', 'started', 'finished', 'exit'):
        path = d / name
        if path.exists() or path.is_symlink():
            raise RuntimeError('staged job has pre-existing execution evidence: ' + name)
    state = {'version': VERSION, 'id': jid, 'phase': 'queued', 'reason': 'waiting for resources',
             'descriptor_hash': digest, 'policy_hash': fingerprint(host_policy()), 'registered': int(time.time()),
             'fleet': bool(value.get('fleet')), 'priority': value.get('priority', 'lane')}
    if value.get('submitted') is not None:
        # when its controller says the job was submitted (waiting_since), never later than now: the two clocks differ.
        # Under a key of its own: `submitted` in a state is the tick's mark that the unit was handed to systemd.
        state['submitted_at'] = min(int(value['submitted']), state['registered'])
    save(jid, state)
    return state


def waiting_since(state):
    """When a queued job began to wait, for the order of the queue (tick): the time its controller says it was
    submitted (`submitted_at`, which register takes from the descriptor of an immediate fleet job) when that is a time
    not after its registration, else its registration. A job its controller hands over after it has waited there
    (fleet_queue._reserve) would otherwise be new here and could be passed for another hour by work that came in
    after it."""
    registered = state.get('registered', 0)
    if type(registered) not in (int, float) or not math.isfinite(registered):
        raise RuntimeError('invalid registration time')
    submitted = state.get('submitted_at')
    if type(submitted) in (int, float) and math.isfinite(submitted) and 0 < submitted <= registered:
        return submitted
    return registered


RECALLED = 'recalled before start'   # the reason on a job `recall` took out of the queue; the controller's fleet_queue.RECALLED


def recall(jid):
    """Take a fleet job that has not started out of this host's queue, for its controller to place it on another host
    that has room (fleet_queue._recall): its state becomes cancelled with the reason RECALLED and it never starts
    here. Like every operation this runs under the helper's lock, so no tick starts the job meanwhile. A job that
    has started or ended, an ordinary deferred job and one cancelled before registration are left as they are: what
    is known of the job is returned either way, and the controller acts only on RECALLED."""
    valid_id(jid)
    tombstone = read(Q / ('.deferred-cancel-' + jid))
    if tombstone:
        return tombstone
    try:
        state = read(jobdir(jid) / 'deferred-state.json')
    except FileNotFoundError:
        state = None
    if not state:
        return {'version': VERSION, 'id': jid, 'phase': 'absent', 'reason': 'not registered'}
    if state['phase'] == 'queued' and state.get('fleet'):
        state.update(phase='cancelled', reason=RECALLED)
        state.pop('held_bound', None)
        state.pop('held_since', None)
        save(jid, state)
    return state


def _wall_boundary(day, clock, zone, latest):
    """Resolve folds outward and advance gaps to the first valid local minute."""
    hour, minute = map(int, clock.split(':'))
    wall = dt.datetime.combine(day, dt.time(hour, minute))
    for _ in range(2 * 24 * 60 + 1):
        choices = set()
        for fold in (0, 1):
            utc = wall.replace(tzinfo=zone, fold=fold).astimezone(dt.timezone.utc)
            if utc.astimezone(zone).replace(tzinfo=None) == wall:
                choices.add(utc)
        if choices:
            return max(choices) if latest else min(choices)
        wall += dt.timedelta(minutes=1)
    raise RuntimeError('timezone has no valid boundary within two days')


def window(value, now=None):
    if value['mode'] == 'immediate' and value.get('fleet'):
        return True
    if value['mode'] == 'spare' and value['est_minutes'] <= 15 and value['timeout_minutes'] <= 20:
        return True
    policy = policy_fields(value, validate_zone=True)
    zone = ZoneInfo(policy['timezone'])
    now = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    today = now.astimezone(zone).date()
    start, end = policy['window_start'], policy['window_end']
    for day in (today - dt.timedelta(days=1), today):
        end_day = day + dt.timedelta(days=1) if end < start else day
        begin = _wall_boundary(day, start, zone, False)
        finish = _wall_boundary(end_day, end, zone, True)
        if begin <= now < finish and (finish - now).total_seconds() >= value['est_minutes'] * 60:
            return True
    return False


def available():
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            return int(line.split()[1]) * 1024
    raise RuntimeError('MemAvailable unavailable')


DISK_FLOOR_BYTES = 10 * GIB   # free space on the job directories' file system below which no fleet job starts (the
#                               controller places nothing on the host below it either: fleet_probe reports the figure)


def queue_free():
    """Free bytes on the file system that holds the job directories; None when it cannot be read."""
    try:
        return shutil.disk_usage(Q).free
    except OSError:
        return None


def disk_low():
    free = queue_free()
    return free is not None and free < DISK_FLOOR_BYTES


def eligibility(value, now=None):
    if not window(value, now):
        return 'outside window or estimate exceeds remaining time'
    if os.getloadavg()[0] > value['load_cap']:
        return 'load above cap'
    if available() < math.ceil((value['ram_floor_gb'] + value['memory_max_gb']) * GIB):
        return 'insufficient available memory for floor and full reservation'
    return None


def unit(jid):
    return 'cq-deferred-' + jid + '.service'


def show(jid):
    result = command('systemctl', '--user', 'show', unit(jid), '--property=LoadState,ActiveState,SubState,Result,ControlGroup,Job', check=False)
    props = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
    if not props.get('LoadState'):
        raise RuntimeError('cannot query systemd user manager: ' + result.stderr.strip())
    return props


def empty(props, recorded_group=None):
    inactive = props.get('ActiveState') in ('inactive', 'failed')
    exited = props.get('ActiveState') == 'active' and props.get('SubState') == 'exited'
    job = props.get('Job')
    if job is None or job.strip() not in ('', '0') or not (inactive or exited):
        return False
    group = props.get('ControlGroup') or recorded_group
    if group:
        if not group.startswith('/') or '..' in group.split('/'):
            raise RuntimeError('invalid cgroup path')
        try:
            events = dict(line.split() for line in (CGROUP / group.lstrip('/') / 'cgroup.events').read_text().splitlines())
            return events.get('populated') == '0'
        except FileNotFoundError:
            pass
    return True


def pool_dir(group):
    """The cgroup directory of the fleet slice that holds the unit with this cgroup path, or None when the unit is
    not directly inside a slice of that name (a job started before the slice existed, or an invalid path)."""
    if not isinstance(group, str) or not group.startswith('/') or '..' in group.split('/'):
        return None
    parent = group.rsplit('/', 1)[0]
    return CGROUP / parent.lstrip('/') if parent.rsplit('/', 1)[-1] == FLEET_SLICE else None


def pool_event(group, name):
    """A counter of the fleet slice's own memory.events.local for the unit with this cgroup path (`oom`: the slice's
    limit was reached and an allocation was about to fail; a unit's own limit counts on the unit's cgroup, not
    here), or None when the slice or its counters cannot be read."""
    try:
        for line in (pool_dir(group) / 'memory.events.local').read_text().splitlines():
            key, _, value = line.partition(' ')
            if key == name:
                return int(value)
    except (OSError, ValueError, TypeError):
        pass
    return None


def pool_inode(group):
    """The inode number of the fleet slice's cgroup directory for the unit with this cgroup path, or None. A slice
    that was removed when it emptied and made again starts its counters at 0 under a new inode, so two readings of
    a counter are comparable only when this is the same."""
    try:
        return os.stat(pool_dir(group)).st_ino
    except (OSError, TypeError):
        return None


def failure_reason(state, props):
    """Why a unit that did not finish with success failed. A unit the kernel killed for memory (systemd Result
    oom-kill) says so, and what the evidence shows: its own cgroup is gone by then, so what it held cannot be
    read, but the fleet slice's `oom` count (pool_event) was recorded when its command started (pool_oom_start,
    with the slice directory's inode, pool_ino) and is read again when status() runs, up to a minute after the unit
    ended. It grew: the limit was reached between the start and that check, so jobs running together may have
    needed more than the pool, though which of them the kernel chose to kill is not known. It did not: the job's own
    cap was reached (or the host itself ran out of memory). Without both readings from the same slice cgroup (same
    inode), neither can be told."""
    if props.get('Result') != 'oom-kill':
        return 'unit exited without success'
    cap = state.get('memory_bytes')
    cap = f'{cap / GIB:g} GB' if type(cap) is int and cap > 0 else 'its cap'
    group, start = state.get('control_group'), state.get('pool_oom_start')
    now = pool_event(group, 'oom')
    if type(state.get('pool_ino')) is not int or pool_inode(group) != state['pool_ino']:
        now = None   # the readings are of different slice cgroups, or the slice cannot be found
    if type(start) is int and type(now) is int and now > start:
        try:
            limit = int((pool_dir(group) / 'memory.max').read_text().strip())
            limit = f' ({limit / GIB:.1f} GB)'
        except (OSError, ValueError, TypeError):
            limit = ''
        return (f"{OOM_REASON_PREFIX}: the fleet pool limit{limit} was reached between the job's start and the check after its "
                f'end, so jobs together may have needed more than the pool; its cap was {cap}')
    if type(start) is int and type(now) is int and now == start:
        return f'{OOM_REASON_PREFIX} at its own cap of {cap} (the fleet pool limit was not reached while it ran), or host memory ran out'
    return (f'{OOM_REASON_PREFIX}: no fleet pool reading for this job, so it is not known whether its own cap of {cap} '
            f'or the pool limit was reached')


def status(jid):
    valid_id(jid)
    tombstone = read(Q / ('.deferred-cancel-' + jid))
    if tombstone:
        return tombstone
    try:
        d = jobdir(jid)
    except FileNotFoundError:
        return {'version': VERSION, 'id': jid, 'phase': 'absent', 'reason': 'not registered'}
    state = read(d / 'deferred-state.json')
    if not state:
        return {'version': VERSION, 'id': jid, 'phase': 'absent', 'reason': 'not registered'}
    if state['phase'] in TERMINAL or state['phase'] == 'queued':
        return state
    props = show(jid)
    if props.get('ControlGroup'):
        state['control_group'] = props['ControlGroup']
    if state.get('boot') != boot():
        state.update(phase='cancelled' if state.get('cancel_requested') else 'failed', reason='host rebooted; job will not replay')
    elif props.get('LoadState') == 'not-found' and not state.get('control_group'):
        state['reason'] = 'unit identity is uncertain; stop confirmation requires a known cgroup or reboot'
    elif state.get('cancel_requested') and state.get('stop_confirmed') and empty(props, state.get('control_group')):
        state.update(phase='cancelled', reason='unit and cgroup stopped')
    elif state.get('submitted') and empty(props, state.get('control_group')):
        if state.get('retry_safe') and not state.get('cancel_requested'):
            # The runner durably recorded that it did not launch a command. Wait
            # for its cgroup to be empty and remove the old transient unit before
            # making the job eligible for another claim.
            if props.get('LoadState') != 'not-found':
                stopped = command('systemctl', '--user', 'stop', unit(jid), check=False)
                if stopped.returncode:
                    state['reason'] = 'waiting to retire unstarted unit'
                    save(jid, state)
                    return state
                command('systemctl', '--user', 'reset-failed', unit(jid), check=False)
            state.update(phase='queued', reason='resources changed before command start; waiting')
            for key in ('nonce', 'boot', 'submitted', 'retry_safe', 'control_group', 'memory_bytes', 'memory_expected_bytes', 'quota_slots',
                        'pool_oom_start', 'pool_ino', 'slice', 'runtime_max_s', 'estimate_s', 'unit_started_by', 'passed', 'held_bound', 'held_since'):
                state.pop(key, None)
            save(jid, state)
            return state
        try:
            code = int((d / 'exit').read_text().strip())
        except (OSError, ValueError):
            code = None
        if state.get('cancel_requested'):
            # An explicit stop barrier is required even if the unit appears inactive.
            state['reason'] = 'cancellation requires stop confirmation'
        else:
            ok = code == 0 and props.get('Result') == 'success'
            state.update(phase='done' if ok else 'failed', reason='completed' if ok else failure_reason(state, props), exit=code)
    save(jid, state)
    if state['phase'] in TERMINAL and props.get('ActiveState') == 'active' and props.get('SubState') == 'exited':
        # RemainAfterExit keeps the transient service loaded after its cgroup is
        # empty. Persist the result first, then retire that no-longer-running unit.
        try:
            stopped = command('systemctl', '--user', 'stop', unit(jid), check=False)
            state['unit_retired'] = stopped.returncode == 0
        except (OSError, subprocess.SubprocessError):
            state['unit_retired'] = False
        save(jid, state)
    return state


def cancel(jid):
    valid_id(jid)
    tombstone_path = Q / ('.deferred-cancel-' + jid)
    tombstone = read(tombstone_path)
    if tombstone:
        return tombstone
    try:
        state = read(jobdir(jid) / 'deferred-state.json')
    except FileNotFoundError:
        state = None
    if not state:
        tombstone = {'version': VERSION, 'id': jid, 'phase': 'cancelled', 'reason': 'cancelled before registration'}
        atomic(tombstone_path, tombstone)
        return tombstone
    if state['phase'] in TERMINAL:
        return state
    if state['phase'] == 'queued':
        state.update(phase='cancelled', reason='cancelled before start')
        save(jid, state)
        return state
    state['cancel_requested'] = True
    state['reason'] = 'stopping unit'
    save(jid, state)
    result = command('systemctl', '--user', 'stop', unit(jid), check=False)
    if result.returncode:
        raise RuntimeError('systemd stop failed; cancellation remains unconfirmed: ' + result.stderr.strip())
    state['stop_confirmed'] = True
    save(jid, state)
    return status(jid)


def process_matches(pid, d):
    proc = Path('/proc') / str(pid)
    if proc.stat().st_uid != os.getuid():
        return False
    os.kill(pid, 0)
    # Prevent PID reuse from mistaking an unrelated process for this wrapper.
    args = (proc / 'cmdline').read_bytes().split(b'\0')
    return os.fsencode(str(d / 'run.sh')) in args


def foreground_waiter():
    """Find a live ordinary wrapper that has not acquired its heavy slots yet."""
    for d in Q.glob('cq-*'):
        try:
            safe(d, True)
            if (d / 'deferred-state.json').exists() or (d / 'started').exists() or (d / 'exit').exists():
                continue
            safe(d / 'pid')
            raw = (d / 'pid').read_text().strip()
            if not re.fullmatch(r'[1-9][0-9]{0,9}', raw) or int(raw) < 2:
                continue
            pid = int(raw)
            if not process_matches(pid, d):
                continue
            try:
                safe(d / 'job.env')
                env = (d / 'job.env').read_text()
                values = re.findall(r'(?m)^SLOTS=([0-9]+)\r?$', env)
                if len(values) != 1:
                    return True
                if int(values[0]) > 0:
                    return True
            except (OSError, ValueError, RuntimeError):
                return True  # live wrapper with unsafe/ambiguous demand: yield
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, ValueError, RuntimeError):
            return True  # an unsafe/ambiguous live-job scan is not idle evidence
    return False


def legacy_busy():
    """A live ordinary heavy wrapper, waiting or already started, blocks fleet."""
    for d in Q.glob('cq-*'):
        try:
            safe(d, True)
            if (d / 'deferred-state.json').exists() or (d / 'exit').exists():
                continue
            if not (d / 'pid').exists():
                continue
            safe(d / 'pid')
            raw = (d / 'pid').read_text().strip()
            if not re.fullmatch(r'[1-9][0-9]{0,9}', raw) or int(raw) < 2:
                return True
            if not process_matches(int(raw), d):
                continue
            safe(d / 'job.env')
            values = re.findall(r'(?m)^SLOTS=([0-9]+)\r?$', (d / 'job.env').read_text())
            if len(values) != 1 or int(values[0]) > 0:
                return True
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, ValueError, RuntimeError):
            return True
    return False


def ordinary_waiter(d, state, policy=None, memory_total=None):
    """Validate immutable scheduling facts without inspecting the staged Git tree."""
    policy = policy or host_policy()
    value = read(d / 'deferred.json')
    validate_descriptor(value, d.name, policy)
    if state.get('id') != d.name or state.get('fleet') or value.get('fleet'):
        raise RuntimeError('ordinary waiter identity changed')
    if fingerprint(value) != state.get('descriptor_hash') or fingerprint(policy) != state.get('policy_hash'):
        raise RuntimeError('ordinary waiter descriptor or policy changed')
    if state.get('priority', 'lane') != value.get('priority', 'lane'):
        raise RuntimeError('ordinary waiter priority changed')
    if not window(value):
        return False, 'outside window or estimate exceeds remaining time'
    memory_total = total_memory() if memory_total is None else memory_total
    if value['slots'] > value['pool_slots'] or math.ceil(value['memory_max_gb'] * GIB) + math.ceil(value['ram_floor_gb'] * GIB) > memory_total:
        return False, 'declared demand exceeds total host capacity'
    return True, None


def charged_slots(value):
    """The pool slots a job (its descriptor) is charged: the CPU it is expected to use, rounded up to whole slots
    (cpu_expected, which the controller sends only to a host set to admit on it, from what earlier runs of the same
    command used), otherwise the slots it declared. The declared slots stay the job's limit either way (CPUQuota):
    the charge is what it takes from the pool, the slot locks included, so a job that declares four slots and uses
    one no longer keeps three idle (10-13 Nov 2026: with 12 or more of ember's 14 slots taken, the jobs used 0.49
    cores per slot). The quotas of all fleet jobs together are bounded separately (fleet_reason)."""
    cores = value.get('cpu_expected')
    if type(cores) in (int, float) and math.isfinite(cores) and 0 < cores <= value['slots']:
        return max(1, min(value['slots'], math.ceil(cores)))
    return value['slots']


def cpu_usage(group):
    """Microseconds of CPU the cgroup at this path has used so far (cpu.stat usage_usec), or None."""
    try:
        for line in (group / 'cpu.stat').read_text().splitlines():
            name, _, value = line.partition(' ')
            if name == 'usage_usec':
                return int(value) if int(value) >= 0 else None
    except (OSError, ValueError):
        pass
    return None


def held_memory(state):
    """Bytes of a running fleet job's cap that it already holds as memory MemAvailable no longer counts: the pages
    on the anonymous lists of its own unit's cgroup (memory.stat active_anon + inactive_anon: anonymous and shared
    memory, which cannot be reclaimed while the unit has no swap), at most its cap. Admission subtracts this from
    the cap, because MemAvailable has already lost it; charging the whole cap as well counts it twice. Not the
    `anon` figure: that includes pages freed lazily (MADV_FREE), which sit on the file lists and MemAvailable still
    counts. Page cache is left out for the same reason, and kernel and locked memory too, so the figure errs low.
    0 when the job is not running or the cgroup cannot be read: it is then charged its whole cap."""
    try:
        group = state.get('control_group')
        if (state.get('phase') != 'running' or not isinstance(group, str) or not group.startswith('/')
                or '..' in group.split('/') or group.rsplit('/', 1)[-1] != unit(state['id'])):
            return 0
        held = anon_lists(CGROUP / group.lstrip('/'))
        return max(0, min(held, state['memory_bytes'])) if held is not None else 0
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return 0


def anon_lists(group):
    """Bytes on the anonymous lists of the cgroup at this path (memory.stat active_anon + inactive_anon), or None
    when either figure is missing or the file cannot be read."""
    try:
        found = {}
        for line in (group / 'memory.stat').read_text().splitlines():
            name, _, value = line.partition(' ')
            if name in ('active_anon', 'inactive_anon'):
                found[name] = int(value)
        return sum(found.values()) if len(found) == 2 and min(found.values()) >= 0 else None
    except (OSError, ValueError):
        return None


HELD_SAMPLE_S = 5
CPU_SAMPLE_MIN_S = 4   # sample_held: a stretch shorter than this says little about the cores a job uses
HELD_SAMPLE_START = (.2, .5, 1, 2)   # the first waits: a short job is read a few times too


def sample_held(group, stop, result):
    """Runs beside a job's command, started once the command exists: after each wait (HELD_SAMPLE_START, then every
    HELD_SAMPLE_S) it reads what the job's cgroup holds on its anonymous lists and keeps the largest figure in
    result['peak'] and the number of readings in result['samples']. memory.peak cannot say this: it counts file
    cache, which grows to any cap and is given back under pressure. A spike between two readings is missed, so the
    figure is a lower bound on what the job needed; a reader should want several readings before trusting it."""
    waits = iter(HELD_SAMPLE_START)
    while not stop.wait(next(waits, HELD_SAMPLE_S)):
        value = anon_lists(group)
        if value is not None:
            result['peak'] = max(result.get('peak', 0), value)   # before the count: a count always has its peak
            result['samples'] = result.get('samples', 0) + 1
        # CPU beside it: the cores used over the busiest stretch between two readings at least CPU_SAMPLE_MIN_S apart
        used, at = cpu_usage(group), time.monotonic()
        if used is not None:
            base = result.get('cpu_base')
            if base is None:
                result['cpu_base'] = (at, used)
            elif at - base[0] >= CPU_SAMPLE_MIN_S:
                if used >= base[1]:
                    result['cpu_peak_cores'] = max(result.get('cpu_peak_cores', 0.0), (used - base[1]) / 1e6 / (at - base[0]))
                result['cpu_base'] = (at, used)


def start_sampler():
    """Start sample_held on this runner's own cgroup. (readings, stop, thread); the thread is None when it could not
    start. Called with the command already running, so nothing here may raise: the runner must go on to wait for it."""
    sampled, stop, thread = {}, threading.Event(), None
    try:
        thread = threading.Thread(target=sample_held, args=(CGROUP / self_group().lstrip('/'), stop, sampled), daemon=True)
        thread.start()
    except Exception:
        thread = None
    return sampled, stop, thread


def finish_sampler(sampled, stop, thread):
    """Stop the sampler and give (peak bytes, readings), or None when it read nothing. Never raises."""
    try:
        stop.set()
        if thread is not None:
            thread.join(10)
        peak, samples = sampled.get('peak'), sampled.get('samples')
        return (peak, samples) if type(peak) is int and type(samples) is int and samples > 0 else None
    except Exception:
        return None


def available_after(active):
    """available(), read between two looks at what each job in `active` holds: reservations() made the first, this
    makes the second, and each job keeps the smaller figure. A job that grew in between is charged for the growth,
    and one that shrank (or ended) is not credited with memory available() already shows as free."""
    free = available()
    for row in active:
        if row.get('held_memory_bytes'):
            try:
                state = read(Q / row['id'] / 'deferred-state.json')
            except (OSError, ValueError, RuntimeError):
                state = None
            row['held_memory_bytes'] = min(row['held_memory_bytes'], held_memory(state) if isinstance(state, dict) else 0)
    return free


def reservations():
    """Read reservations without mutating state; malformed active state fails closed. Callers that compare what
    the jobs hold with available memory read that through available_after(active)."""
    active, ordinary, waiting = [], False, False
    policy, memory_total = None, None
    for d in sorted(Q.glob('cq-*')):
        safe(d, True)
        s = read(d / 'deferred-state.json')
        if not s:
            continue
        phase = s.get('phase')
        if phase == 'queued' and not s.get('fleet'):
            policy = policy or host_policy()
            if memory_total is None:
                memory_total = total_memory()
            eligible, _ = ordinary_waiter(d, s, policy, memory_total)
            waiting = waiting or eligible
        if phase not in ('starting', 'running'):
            continue
        if not s.get('fleet'):
            ordinary = True
            continue
        if (type(s.get('slots')) is not int or s['slots'] < 1
                or type(s.get('memory_bytes')) is not int or s['memory_bytes'] < 1
                or s.get('id') != d.name):
            raise RuntimeError('invalid active fleet reservation')
        row = {'id': d.name, 'phase': phase, 'slots': s['slots'],
               'reserved_memory_bytes': s['memory_bytes'], 'held_memory_bytes': held_memory(s)}
        quota = s.get('quota_slots')   # the declared slots (CPUQuota) of a job charged fewer (charged_slots)
        row['quota_slots'] = quota if type(quota) is int and quota >= s['slots'] else s['slots']
        group = s.get('control_group')
        if isinstance(group, str) and group:
            row['control_group'] = group
            row['pooled'] = pool_dir(group) is not None   # the unit is directly inside the fleet slice
        else:
            row['pooled'] = phase == 'starting' and s.get('slice') == FLEET_SLICE   # tick's intent, before the runner shows it
        expected = s.get('memory_expected_bytes')
        if type(expected) is int and 0 < expected <= s['memory_bytes']:
            row['expected_memory_bytes'] = expected   # a job without a valid expectation is charged its cap
        active.append(row)
    return active, ordinary, waiting


def pool_limit(floor_bytes):
    """MemoryMax of the fleet slice as the user manager has it, in bytes, or None when it is not set (systemd shows
    infinity), is above the host's memory less the RAM floor (`floor_bytes`) by more than POOL_SLACK_BYTES (the limit
    was written at install, and MemTotal moves a little between kernels), or cannot be read. This is the
    configuration; expected_mode checks that it is in effect."""
    try:
        result = command('systemctl', '--user', 'show', FLEET_SLICE, '--property=MemoryMax', '--value', check=False)
        raw = result.stdout.strip()
        if result.returncode or not raw.isdigit():
            return None
        limit = int(raw)
        return limit if 0 < limit <= total_memory() - floor_bytes + POOL_SLACK_BYTES else None
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        return None


def pool_cpu_bounded(fleet_slots, active=()):
    """Whether the fleet slice holds all fleet jobs together to the CPU of the fleet's slots, as the user manager has it
    (CPUQuotaPerSecUSec, a time span such as `14s`; `infinity` when unset). Only then may jobs be charged fewer
    slots than their own quotas: the slice is what keeps the foreground slots free when they burst together. The
    installer writes the quota (queue_policy.fleet_slice_cpu_quota); a host installed before it reports False. That
    is the configuration: once a fleet job runs inside the slice (`active`, the rows of reservations), cpu.max in the
    slice's cgroup directory must say the same, or the kernel is not enforcing it (the cpu controller is not enabled
    for the slice, or a reload has not reached it) and the answer is False."""
    try:
        result = command('systemctl', '--user', 'show', FLEET_SLICE, '--property=CPUQuotaPerSecUSec', '--value', check=False)
        if result.returncode:
            return False
        seconds = 0.0
        for part in result.stdout.split():
            number = part.rstrip('abcdefghijklmnopqrstuvwxyz')
            unit_s = {'us': 1e-6, 'ms': 1e-3, 's': 1.0, 'min': 60.0, 'h': 3600.0}[part[len(number):]]
            seconds += float(number) * unit_s
        if not 0 < seconds <= fleet_slots + .001:
            return False
        for row in active:
            directory = pool_dir(row.get('control_group'))
            if directory is not None:
                quota, period = (directory / 'cpu.max').read_text().split()
                return quota != 'max' and 0 < int(quota) / int(period) <= fleet_slots + .001
        return True
    except (OSError, ValueError, KeyError, RuntimeError, ZeroDivisionError, subprocess.SubprocessError):
        return False


def pool_memory_max(group):
    """memory.max of the fleet slice's cgroup directory for the unit with this cgroup path, in bytes: what the kernel
    enforces. None when it is `max` (no limit), another non-number, or cannot be read (the memory controller is
    not enabled for the slice)."""
    try:
        raw = (pool_dir(group) / 'memory.max').read_text().strip()
        return int(raw) if raw.isdigit() else None
    except (OSError, ValueError, TypeError):
        return None


def expected_mode(active, free, policy):
    """(True, None) when fleet jobs may be admitted on the memory they are expected to use (charge), else (False, the
    reason as a short text). Both fleet_reason and fleet_probe ask this, so admission and the advertised flag agree.
    Charging less than the caps is safe only while the slice's limit really bounds every running fleet job and the
    rest of the machine leaves room for the pool to fill, so all of these must hold (otherwise every job is charged
    its cap, as before the slice existed): the slice's limit is set and not above the host's memory less the RAM floor
    (pool_limit); every active fleet row is inside the slice (pooled); once a job runs in it, the kernel's own
    memory.max for the slice is the same number (pool_memory_max: the property alone shows configuration, which is
    nothing where the memory controller is not delegated); and what is neither available (`free`) nor held by
    the fleet rows leaves at least POOL_MARGIN_BYTES of the floor (the pool bounds fleet jobs only, so the rest
    of the machine must not hold what the floor needs once they have grown to the limit)."""
    floor = math.ceil(policy['ram_floor_gb'] * GIB)
    limit = pool_limit(floor)
    if limit is None:
        return False, 'fleet slice limit not set, or above memory less the floor'
    if not all(row.get('pooled') for row in active):
        return False, 'a fleet job runs outside the fleet slice'
    running = next((row for row in active if row.get('phase') == 'running'), None)
    if running is not None and pool_memory_max(running.get('control_group')) != limit:
        return False, 'fleet slice limit not in effect'
    outside = total_memory() - free - sum(row.get('held_memory_bytes', 0) for row in active)
    if outside > floor - POOL_MARGIN_BYTES:
        return False, 'memory outside the fleet pool exceeds the floor'
    return True, None


def charge(row, expected):
    """Bytes of memory a running job (a row of reservations()) is still to be kept: with `expected` (the slice's
    limit is in effect) its expectation less what it holds while it holds no more than that, its cap less what it holds
    once it holds more (it has shown the expectation wrong); a job without an expectation, or `expected` false, is
    charged its cap less what it holds. What it holds is already missing from available memory. Never below 0."""
    cap, held = row['reserved_memory_bytes'], row.get('held_memory_bytes', 0)
    want = row.get('expected_memory_bytes') if expected else None
    return max(0, (want if want is not None and held <= want else cap) - held)


def total_memory():
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemTotal:'):
            return int(line.split()[1]) * 1024
    raise RuntimeError('MemTotal unavailable')


def existing_lock_free(path, shared=False):
    """Probe an existing lock without creating any files."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return True
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid():
            raise RuntimeError('unsafe lock: ' + str(path))
        try:
            fcntl.flock(fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False
    finally:
        os.close(fd)


def run_facts(d):
    facts = {}
    for name in ('launched', 'started', 'finished', 'exit'):
        path = d / name
        try:
            st = safe(path)
            if st.st_size > 64:
                raise RuntimeError('oversized job fact')
            facts[name] = path.read_text().strip()
        except FileNotFoundError:
            facts[name] = None
    try:
        safe(d / 'src', True)
        facts['src_exists'] = True
    except FileNotFoundError:
        facts['src_exists'] = False
    return facts


def fleet_probe():
    """Read-only, bounded snapshot. Admission is always repeated under the lock."""
    check()  # Fail closed if the manager, timer or cgroup protocol is unavailable.
    policy = host_policy()
    active, ordinary, waiting = reservations()
    if len(active) > policy['pool_slots']:
        raise RuntimeError('too many fleet reservations')
    load = os.getloadavg()[0]
    avail, total = available_after(active), total_memory()
    busy = legacy_busy() or ordinary or not existing_lock_free(Q / '.heavy.gate', shared=True)
    if not any(s['phase'] == 'running' for s in active):
        busy = busy or any(not existing_lock_free(Q / ('.heavy.' + str(i))) for i in range(policy['pool_slots']))
    for item in active:
        item.update(run_facts(Q / item['id']))
    expected, blocker = expected_mode(active, avail, policy)
    limit = pool_limit(math.ceil(policy['ram_floor_gb'] * GIB))
    charged = sum(charge(s, expected) for s in active)
    reasons = []
    if busy:
        reasons.append('legacy work active or waiting')
    if waiting:
        reasons.append('ordinary deferred work waiting')
    if load > policy['load_cap']:
        reasons.append('load above cap')
    if disk_low():
        reasons.append('queue disk below floor')
    terminal, queued = [], []
    for d in sorted(Q.glob('cq-*')):
        safe(d, True)
        state = read(d / 'deferred-state.json')
        if state and state.get('fleet') and state.get('phase') == 'queued':
            # registered and not started: its controller reads this list instead of asking about each such job, and
            # recalls only a job that is on it
            queued.append({'id': d.name, 'reason': str(state.get('reason', ''))[:120]})
            continue
        if not state or not state.get('fleet') or state.get('phase') not in TERMINAL:
            continue
        item = {'id': d.name, 'phase': state['phase'], 'exit_code': state.get('exit'),
                'reason': str(state.get('reason', ''))[:200], **run_facts(d)}
        peak = state.get('memory_peak_bytes')
        if type(peak) is int and peak >= 0:
            item.update(memory_peak_bytes=peak, memory_kind='cgroup_memory')
        held_peak, samples = state.get('memory_held_peak_bytes'), state.get('memory_held_samples')
        if type(held_peak) is int and held_peak >= 0 and type(samples) is int and samples > 0:
            item.update(memory_held_peak_bytes=held_peak, memory_held_samples=samples)
        used, wall, busiest = state.get('cpu_usage_usec'), state.get('cpu_wall_s'), state.get('cpu_peak_cores')
        if type(used) is int and used >= 0 and type(wall) in (int, float) and wall > 0:
            item.update(cpu_usage_usec=used, cpu_wall_s=wall)
            if type(busiest) in (int, float) and busiest >= 0:
                item['cpu_peak_cores'] = busiest
        terminal.append(item)
    capabilities = ['linux']
    for name, candidates in (('python', ('python3', 'python')), ('bash', ('bash',)), ('go', ('go',)),
                             ('node', ('node',)), ('ffmpeg', ('ffmpeg',)), ('blender', ('blender',)),
                             ('adb', ('adb',))):
        if any(shutil.which(candidate) for candidate in candidates):
            capabilities.append(name)
    effective_policy = dict(policy, **policy_fields(policy, policy['pool_slots'], validate_zone=True))
    return {'version': VERSION, 'supported': True, 'safety_protocol': 'shared-heavy-gate-v1',
            'resource_policy_version': RESOURCE_POLICY_VERSION, 'policy': effective_policy,
            'capabilities': capabilities,
            'machine_id': machine_id(), 'load': load, 'available_bytes': avail,
            'total_bytes': total, 'logical_cpus': os.cpu_count(),
            'active_fleet_slots': sum(s['slots'] for s in active),
            'quota_fleet_slots': sum(s.get('quota_slots', s['slots']) for s in active),
            'expected_cpu_admission': pool_cpu_bounded(policy['pool_slots'] - effective_policy['foreground_slots'], active),
            'reserved_memory_bytes': sum(s['reserved_memory_bytes'] for s in active),
            'held_memory_bytes': sum(s.get('held_memory_bytes', 0) for s in active),
            'expected_memory_admission': expected, 'expected_memory_blocker': blocker, 'pool_limit_bytes': limit,
            'charged_memory_bytes': charged,
            'legacy_busy': busy, 'ordinary_waiting': waiting, 'reasons': reasons, 'queue_free_bytes': queue_free(),
            'active_fleet': active, 'terminal_fleet': terminal[-32:], 'queued_fleet': queued[:256]}


def fleet_reason(value, jid=None):
    active, ordinary, waiting = reservations()
    if ordinary or waiting or legacy_busy():
        return 'legacy or ordinary deferred work active or waiting'
    if not window(value):
        return 'outside window or estimate exceeds remaining time'
    if os.getloadavg()[0] > value['load_cap']:
        return 'load above cap'
    if disk_low():  # a job started on a full disk dies at its first write (ember, 12 Nov: 0 bytes free for seven hours)
        return 'queue disk below floor'
    other = [s for s in active if s['id'] != jid]
    limit = value['pool_slots'] - policy_fields(value, value['pool_slots'])['foreground_slots']
    if sum(s['slots'] for s in other) + charged_slots(value) > limit:
        return 'fleet CPU pool exhausted'
    # A job charged fewer slots than it declared may still use its declared ones (CPUQuota): the quotas of all fleet
    # jobs together stay within CPU_OVERCOMMIT times the pool. With every charge a declared count this never binds.
    if sum(s.get('quota_slots', s['slots']) for s in other) + value['slots'] > CPU_OVERCOMMIT * limit:
        return CPU_QUOTA_WAIT
    # Each other job is charged the part of its cap it does not hold yet (held_memory): what it holds is already
    # missing from available memory. available_after reads that between two looks at what they hold. In expected mode
    # (expected_mode: the fleet slice's limit bounds every running fleet job, the jobs included, and the rest of the
    # machine leaves room) a job is charged its expected use instead while it holds no more than that (charge), the
    # job being admitted is charged its expectation, and the caps of all of them together may not exceed
    # POOL_OVERCOMMIT times what the pool can hold. Otherwise every charge is a cap.
    free = available_after(active)
    expected, _ = expected_mode(active, free, value)
    floor = math.ceil(value['ram_floor_gb'] * GIB)
    cap = math.ceil(value['memory_max_gb'] * GIB)
    new = min(cap, math.ceil(value['memory_expected_gb'] * GIB)) if expected and 'memory_expected_gb' in value else cap
    if expected and sum(s['reserved_memory_bytes'] for s in other) + cap > POOL_OVERCOMMIT * max(0, total_memory() - floor):
        return 'fleet memory caps exceed the overcommit bound'
    reserve = sum(charge(s, expected) for s in other) + new
    if free < floor + reserve:
        return 'insufficient available memory for fleet reservations and floor'
    return None


# Backfill (tick). A queued fleet job that is blocked only by its own size is the held job: later fleet jobs may start
# past it when that cannot delay it beyond the moment the running jobs' hard ends guarantee it room (the hard rule),
# and when their estimates say they are gone before it could start (the estimate rule; the controller's _shadow works
# by estimates as well). The hard bound is the promise and is kept on the held job's state; the estimate bound is a
# heuristic, looked at afresh on every tick.
BACKFILL_REASONS = frozenset({
    'fleet CPU pool exhausted',
    'insufficient available memory for fleet reservations and floor',
    'fleet memory caps exceed the overcommit bound',
    'insufficient free heavy slots and foreground headroom'})
HARD_END_GRACE_S = 60   # after RuntimeMaxSec: TimeoutStopSec (30) before SIGKILL, and the time to reap
CLOCK_SLACK_S = 5
# The latest a unit's start is recorded after the moment a job was judged: systemd-run may take as long as command()
# lets it, and unit_started_by (what run_times dates a unit from) is saved only when it has returned.
START_SLACK_S = COMMAND_TIMEOUT_S


def runtime_max_s(value):
    """RuntimeMaxSec of the unit started for this descriptor: the hard end of a fleet job, whatever its estimate."""
    return value['timeout_minutes'] * 60 + 30


def _epoch(x):
    return type(x) in (int, float) and math.isfinite(x) and x > 0


def run_times(active, now):
    """(hard, expected): {job id: epoch} for every running fleet job, or None when that is not known for every row.
    hard is the epoch by which the job has surely released its slots and memory: its start plus runtime_max_s plus the
    grace. expected is when it is expected to: its start plus estimate_s, but at once (now) when it is past that,
    since it may end at any moment, and never after its hard end. The start is the earlier of unit_started_by (saved
    after systemd-run returned) and command_started (saved by the runner inside the unit). No time is told for a
    row outside the fleet slice, one without a saved runtime_max_s or estimate_s or without a start, one that
    started in the future (a clock stepped back), or one already past its hard end yet still listed: nothing is
    inferred for such a row."""
    hard, expected = {}, {}
    for row in active:
        state = read(Q / row['id'] / 'deferred-state.json')
        if not row.get('pooled') or not isinstance(state, dict):
            return None
        limit, estimate = state.get('runtime_max_s'), state.get('estimate_s')
        marks = [x for x in (state.get('unit_started_by'), state.get('command_started')) if _epoch(x)]
        if (type(limit) is not int or limit < 1 or type(estimate) is not int or estimate < 1
                or not marks or min(marks) > now + CLOCK_SLACK_S):
            return None
        start = min(marks)
        end = start + limit + HARD_END_GRACE_S
        if end < now:
            return None
        hard[row['id']] = end
        expected[row['id']] = min(end, max(now, start + estimate))
    return hard, expected


def _fits_at(t, active, ends, hold, extra=None):
    """Whether the held job (`hold`) fits at epoch t beside the rows whose end (in `ends`) is after t (and `extra`, a
    job that runs past t), every one of them at its cap or at what it holds if that is more: the accounting of
    fleet_reason with caps, where what is missing from the memory available now is what the jobs hold."""
    left = [row for row in active if ends[row['id']] > t]
    slot_sum = sum(row['slots'] for row in left) + hold['slots'] + (extra['slots'] if extra else 0)
    memory = (sum(max(row['reserved_memory_bytes'], row.get('held_memory_bytes', 0)) for row in left)
              + hold['memory'] + (extra['memory'] if extra else 0))
    return slot_sum <= hold['limit'] and memory <= hold['capacity']


def _room_time(active, ends, hold):
    """The earliest end in `ends` at which the held job fits (None when it does not even then)."""
    for t in sorted(ends.values()):
        if _fits_at(t, active, ends, hold):
            return t
    return None


def backfill_hold(value, now):
    """The record that bounds what may pass a blocked fleet job (`value`, its descriptor), or None when no bound
    can be told, in which case nothing passes. bound is the earliest epoch at which, if every running job ran to
    its hard end (run_times), the job would have room: slots against pool_slots less the foreground slots, memory
    against caps (never the expected charges: a job can grow to its cap) with the room that free memory and what
    the jobs hold add up to, less the floor. Caps are the safe side here: a later bound is only a weaker promise,
    but an earlier one would be a promise the jobs may break. Also None when the job already fits now by that
    accounting: something the accounting does not see (slot locks held outside the fleet rows) keeps it out, and a
    bound that is the moment of the look promises nothing."""
    active, _, _ = reservations()
    times = run_times(active, now)
    if times is None:
        return None
    hard, _ = times
    free = available_after(active)
    hold = {'slots': charged_slots(value), 'memory': math.ceil(value['memory_max_gb'] * GIB),
            'limit': value['pool_slots'] - policy_fields(value, value['pool_slots'])['foreground_slots'],
            'capacity': free + sum(row.get('held_memory_bytes', 0) for row in active) - math.ceil(value['ram_floor_gb'] * GIB)}
    if _fits_at(now, active, hard, hold):
        return None
    bound = _room_time(active, hard, hold)
    return None if bound is None else dict(hold, bound=bound)


def hold_for(state, value, now):
    """backfill_hold for the job whose saved `state` it is, with the bound kept across ticks: the first bound is put
    in the state (held_bound, with held_since), which the caller saves. A later look that gives a bound later than
    the saved one by more than CLOCK_SLACK_S means the promise has slipped for a reason the rule does not control
    (memory outside the fleet, the clock, anything), and one made when the saved bound has passed and the job is
    still held means the same: nothing passes then (None), the strict order, and the saved value stays. Otherwise the
    saved bound becomes the earlier of the two. So once the first bound is past, the job is never passed again, and
    a wall-clock step forward fails closed; one back does through run_times (a unit started in the future) and a
    held_since in the future."""
    try:
        fresh = backfill_hold(value, now)
    except (OSError, ValueError, RuntimeError, KeyError, TypeError):
        return None
    if fresh is None:
        return None
    stored, since = state.get('held_bound'), state.get('held_since')
    if _epoch(stored) and _epoch(since):
        if stored <= now or since > now + CLOCK_SLACK_S or fresh['bound'] > stored + CLOCK_SLACK_S:
            return None
        state['held_bound'] = min(stored, fresh['bound'])
    else:
        state['held_bound'], state['held_since'] = fresh['bound'], now
    return fresh


def may_pass(hold, active, value, now):
    """Whether a fleet job that passed every admission check may start although an earlier one is blocked (`hold`,
    from hold_for, kept for the whole tick). Both rules must allow it. The hard rule: the job has left the held job's
    bound untouched, because its hard end (now + START_SLACK_S + RuntimeMaxSec + grace) is not after the bound, or
    the held job would still fit at the bound beside it running to its hard end. Either way the bound a fresh look at
    the rows gives afterwards is not later than hold['bound']: the rows' hard ends do not move, and a passing job is
    gone by the bound or leaves the room the bound needs. That fresh look is made here as well, and a bound that has
    moved later or is past lets nothing pass. The estimate rule is the same with the rows' expected ends (a job past
    its estimate may end at any moment) and the job's estimate: it keeps small jobs with long timeouts that end early
    from outlasting the hold, and is a heuristic that needs no memory between ticks. Anything odd in a row or a
    descriptor means it may not pass."""
    try:
        times = run_times(active, now)
        if times is None or hold['bound'] <= now:
            return False
        hard, expected = times
        current = _room_time(active, hard, hold)
        if current is None or current > hold['bound']:
            return False
        extra = {'slots': charged_slots(value), 'memory': math.ceil(value['memory_max_gb'] * GIB)}
        if not (now + START_SLACK_S + runtime_max_s(value) + HARD_END_GRACE_S <= hold['bound']
                or _fits_at(hold['bound'], active, hard, hold, extra)):
            return False
        soon = _room_time(active, expected, hold)
        return soon is not None and (now + START_SLACK_S + value['est_minutes'] * 60 <= soon
                                     or _fits_at(soon, active, expected, hold, extra))
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return False


def slots(pool, needed, foreground=2):
    """Acquire demand only if the configured foreground slots remain free."""
    held = []
    try:
        for i in range(pool):
            fd = open_lock(Q / ('.heavy.' + str(i)))
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(fd)
                continue
            held.append(fd)
        if len(held) < needed + foreground:
            return None
        result, held = held[:needed], held[needed:]
        return result
    finally:
        for fd in held:
            os.close(fd)


def pool_empty(pool):
    """Called with the shared gate held before the first fleet runner."""
    held = []
    try:
        for i in range(pool):
            fd = open_lock(Q / ('.heavy.' + str(i)))
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                os.close(fd)
                return False
            held.append(fd)
        return True
    finally:
        for fd in held:
            os.close(fd)


def staged_state(jid):
    state = read(jobdir(jid) / 'deferred-state.json')
    value = desc(jid)
    if not state or fingerprint(value) != state.get('descriptor_hash'):
        raise RuntimeError('descriptor changed after registration')
    if bool(value.get('fleet')) != bool(state.get('fleet')):
        raise RuntimeError('fleet reservation identity changed')
    if value.get('priority', 'lane') != state.get('priority', 'lane'):
        raise RuntimeError('priority changed after registration')
    if fingerprint(host_policy()) != state.get('policy_hash'):
        raise RuntimeError('installed host policy changed after registration')
    return state, value


def tick():
    # Ordinary deferred work remains serial. Fleet work shares a bounded pool;
    # ordinary waiters stop admission so its gate can drain.
    # Ordinary deferred work goes first to ensure the fleet pool drains for it.
    # A fleet job blocked only by its own size (BACKFILL_REASONS) is the held job: the first one in queue order defines
    # the bound (hold_for, kept on its state across ticks), and a later fleet job may start past it when may_pass says
    # it leaves that bound alone. That bound never moves later because of a job that passed. Any other reason, an
    # error, or a bound that cannot be told or has slipped stops admission for everything, as it always did. A newer,
    # higher-priority job that is not yet an hour old is ordered ahead of an older one (queue_order) and takes the
    # held place, with a bound of its own; the older job is then a later job like any other.
    started = []
    blocker = None

    def result(*, reason=None, error=None, job=None):
        out = {'version': VERSION}
        if started:
            out.update(id=started[0], phase='starting', started=list(started))
            if job:
                out['blocked_id' if reason else 'error_id'] = job
        elif job:
            out['id'] = job
        if reason:
            out['reason'] = reason
        if error:
            out['error'] = error
        return out

    def queue_order(d):
        s = read(d / 'deferred-state.json') or {}
        rank = {'verify': 0, 'fixcheck': 1, 'lane': 2, 'background': 3}
        since = waiting_since(s)   # its submission when its controller gave it, else its registration
        aged = time.time() - since >= 3600
        return (bool(s.get('fleet')), not aged,
                since if aged else rank.get(s.get('priority', 'lane'), 4), since, d.name)

    for d in sorted(Q.glob('cq-*')):
        try:
            safe(d, True)
            s = read(d / 'deferred-state.json')
            if s and s.get('phase') in ('starting', 'running'):
                status(d.name)
                s = read(d / 'deferred-state.json')
                if s['phase'] in ('starting', 'running') and not s.get('fleet'):
                    return {'version': VERSION, 'reason': 'another deferred reservation is active'}
        except (OSError, ValueError, RuntimeError):
            return {'version': VERSION, 'reason': 'cannot reconcile an existing reservation'}
    for d in sorted(Q.glob('cq-*'), key=queue_order):
        try:
            safe(d, True)
            s = read(d / 'deferred-state.json')
            if not s or s.get('phase') != 'queued':
                continue
            if not s.get('fleet'):
                eligible, why = ordinary_waiter(d, s)
                if not eligible:
                    s['reason'] = why
                    save(d.name, s)
                    continue
            s, v = staged_state(d.name)
            is_fleet = bool(v.get('fleet'))
            active, ordinary, waiting = reservations()
            if not is_fleet and active:
                s['reason'] = 'fleet reservation active; waiting for pool to drain'
                save(d.name, s)
                continue
            gate = try_gate(shared=is_fleet)
            reason = 'heavy gate busy' if gate is None else None
            try:
                if not reason:
                    reason = fleet_reason(v) if is_fleet else eligibility(v)
                if not reason and not is_fleet and foreground_waiter():
                    reason = 'foreground heavy job is waiting'
                if not reason and is_fleet and not any(x['phase'] == 'running' for x in active) and not pool_empty(v['pool_slots']):
                    reason = 'legacy heavy slots held'
                held = slots(v['pool_slots'], charged_slots(v), policy_fields(v, v['pool_slots'])['foreground_slots']) if not reason else None
                if not reason and held is None:
                    reason = 'insufficient free heavy slots and foreground headroom'
                if held:
                    for fd in held:
                        os.close(fd)
            finally:
                if gate is not None:
                    os.close(gate)
            if reason:
                s['reason'] = reason
                hold = hold_for(s, v, time.time()) if is_fleet and blocker is None and reason in BACKFILL_REASONS else None
                save(d.name, s)
                if is_fleet and reason != 'outside window or estimate exceeds remaining time':
                    if hold is not None:
                        blocker = {'id': d.name, 'reason': reason, 'hold': hold}
                        continue
                    if blocker is not None and reason in BACKFILL_REASONS:
                        continue   # also too big for now: skipped, and it does not make a bound of its own
                    if reason == CPU_QUOTA_WAIT and time.time() - waiting_since(s) < 3600:
                        # its declared slots do not fit beside the others' yet: a smaller job behind it may start, but
                        # only until it has waited the hour after which queue_order puts it first. Then nothing passes
                        # it, so jobs that keep the quotas high cannot keep it out for good.
                        continue
                    return result(reason=reason, job=d.name)
                continue
            if blocker is not None and not may_pass(blocker['hold'], active, v, time.time()):
                s['reason'] = 'waiting behind ' + blocker['id'] + ': it could start no later than a running job could end'
                save(d.name, s)
                continue
            s.update(phase='starting', reason='unit submission pending', nonce=uuid.uuid4().hex,
                     boot=boot(), submitted=False, memory_bytes=math.ceil(v['memory_max_gb'] * GIB),
                     fleet=is_fleet, slots=charged_slots(v), quota_slots=v['slots'], runtime_max_s=runtime_max_s(v))
            if is_fleet:
                s['estimate_s'] = v['est_minutes'] * 60   # run_times' expected end of this unit
            s.pop('unit_started_by', None)   # the hard end of this unit (run_times) is known once systemd-run has returned
            s.pop('held_bound', None)   # it is no longer held
            s.pop('held_since', None)
            if blocker is not None:
                s['passed'] = blocker['id']
            else:
                s.pop('passed', None)
            if 'memory_expected_gb' in v:
                s['memory_expected_bytes'] = min(s['memory_bytes'], math.ceil(v['memory_expected_gb'] * GIB))
            if is_fleet:
                s['slice'] = FLEET_SLICE   # the intent: the unit below is started in the pool slice (reservations)
            save(d.name, s)  # persistent claim precedes the potentially ambiguous submission
            cmd = ['systemd-run', '--user', '--quiet', '--unit=' + unit(d.name),
                   '--property=Type=exec', '--property=ExitType=cgroup', '--property=RemainAfterExit=yes',
                   '--property=KillMode=control-group', '--property=SendSIGKILL=yes', '--property=TimeoutStopSec=30',
                   '--property=RuntimeMaxSec=' + str(s['runtime_max_s']),
                   '--property=CPUQuota=' + str(v['slots'] * 100) + '%',
                   '--property=MemoryMax=' + str(s['memory_bytes']), '--property=MemorySwapMax=0',
                   '--property=OOMPolicy=kill', '--property=StandardOutput=append:' + str(d / 'run.log'),
                   '--property=StandardError=inherit', sys.executable, str(SCRIPT), 'run', d.name, s['nonce']]
            if is_fleet:
                cmd.insert(3, '--slice=' + FLEET_SLICE)   # the pool's limit bounds fleet units together; ordinary ones stay out
            command(*cmd)
            s['submitted'] = True
            s['reason'] = 'unit submitted'
            s['unit_started_by'] = time.time()
            save(d.name, s)
            started.append(d.name)
            if not is_fleet:
                return result()
            if len(started) >= min(8, v['pool_slots']):
                return result(reason=blocker['reason'], job=blocker['id']) if blocker else result()
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
            # Never reset an ambiguous claim: status/cancel can settle it safely.
            return result(error=str(exc), job=d.name)
    if blocker is not None:
        return result(reason=blocker['reason'], job=blocker['id'])
    return result() if started else result(reason='no eligible jobs')


def runner(jid, nonce):
    held = []
    gate = None
    with locked(Q / '.deferred.lock'):
        s, v = staged_state(jid)
        if s['phase'] != 'starting' or s.get('nonce') != nonce or s.get('cancel_requested'):
            raise RuntimeError('stale or cancelled claim')
        s['control_group'] = self_group()
        if pool_dir(s['control_group']) is None:
            s.pop('slice', None)   # the unit did not land in the fleet slice: it is not bounded by the pool
        save(jid, s)
        is_fleet = bool(v.get('fleet'))
        gate = try_gate(shared=is_fleet)
        reason = 'heavy gate busy' if gate is None else None
        claimed = False
        try:
            active, ordinary, waiting = reservations()
            if not reason:
                reason = fleet_reason(v, jid) if is_fleet else eligibility(v)
            if not reason and not is_fleet and (active or foreground_waiter()):
                reason = 'fleet active or foreground heavy job is waiting'
            if not reason and is_fleet and not any(x['phase'] == 'running' and x['id'] != jid for x in active) and not pool_empty(v['pool_slots']):
                reason = 'legacy heavy slots held'
            if not reason:
                held = slots(v['pool_slots'], charged_slots(v), policy_fields(v, v['pool_slots'])['foreground_slots'])
                if held is None:
                    reason = 'insufficient free heavy slots and foreground headroom'
            if reason:
                s.update(retry_safe=True, reason=reason + '; command not started')
                save(jid, s)
                return
            # Durable start marker is written BEFORE creating the command process.
            s.update(phase='running', reason='command start claimed', command_started=int(time.time()))
            start, ino = pool_event(s['control_group'], 'oom'), pool_inode(s['control_group'])
            if start is not None and ino is not None:   # failure_reason compares them with the readings it takes later
                s['pool_oom_start'], s['pool_ino'] = start, ino
            save(jid, s)
            claimed = True
        finally:
            if gate is not None and (not claimed or not is_fleet):
                os.close(gate)
                gate = None
            if not claimed and held:
                for fd in held:
                    os.close(fd)
    try:
        # Hold locks until all descendants exit, including reparented daemon children.
        if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), 'PR_SET_CHILD_SUBREAPER failed')
        begun = time.monotonic()
        proc = subprocess.Popen(['/bin/bash', str(jobdir(jid) / 'run.sh')], pass_fds=tuple(held + ([gate] if gate is not None else [])),
                                stdin=subprocess.DEVNULL, start_new_session=True)
        sampler = start_sampler()   # what the job really holds, read while it runs
        proc.wait()
        while True:
            try:
                os.waitpid(-1, 0)
            except ChildProcessError:
                break
        held_peak = finish_sampler(*sampler)
        # Read the cgroup while this runner still keeps it alive.
        try:
            relative = self_group()
            group = CGROUP / relative.lstrip('/')
            used, wall = cpu_usage(group), round(time.monotonic() - begun, 1)   # the whole unit's CPU, daemons included
            busiest = sampler[0].get('cpu_peak_cores')
            try:
                metrics = {name: int((group / name).read_text().strip()) for name in ('memory.current', 'memory.peak')}
            except (OSError, ValueError):
                metrics = None   # a kernel without memory.peak still has cpu.stat
            with locked(Q / '.deferred.lock'):
                state = read(jobdir(jid) / 'deferred-state.json')
                if state and state.get('nonce') == nonce:
                    if metrics:
                        state['memory_current_bytes'] = metrics['memory.current']
                        state['memory_peak_bytes'] = metrics['memory.peak']
                        if held_peak:
                            state['memory_held_peak_bytes'], state['memory_held_samples'] = held_peak
                    if used is not None and wall > 0:
                        state['cpu_usage_usec'], state['cpu_wall_s'] = used, wall
                        if type(busiest) is float and math.isfinite(busiest):
                            state['cpu_peak_cores'] = round(busiest, 2)
                    save(jid, state)
        except (OSError, ValueError, RuntimeError, StopIteration):
            pass  # unavailable cgroup counters (or a state that cannot be written) do not change the job result
    finally:
        for fd in held:
            os.close(fd)
        if gate is not None:
            os.close(gate)
    return True   # the command was started (main: how soon the scheduler should look again)


def tick_soon(delays=TICK_SOON_S):
    """Have the scheduler look again a moment after this runner has gone. What a runner leaves behind is settled
    only by status(), which tick calls: a finished job's terminal phase, and with it the pool slots and memory that
    reservations() still counts for it, or an unstarted claim's return to the queue. Left to the minute timer that
    took up to 70 s (10-13 Nov 2026, 1,192 jobs: a median of 53 s from a job's end to the controller's record of
    it), during which the host refused work it had room for. The tick runs as a transient unit of the user manager,
    so outside this unit's cgroup, which must be empty before status() believes the job is over; when it comes too
    early or cannot be scheduled, the timer's own tick settles the job as before. Never raises."""
    for delay in delays:
        try:
            subprocess.run(['systemd-run', '--user', '--quiet', '--collect', '--on-active=' + str(delay),
                            '--timer-property=AccuracySec=100ms', sys.executable, str(SCRIPT), 'tick'],
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           timeout=COMMAND_TIMEOUT_S, check=False)
        except (OSError, subprocess.SubprocessError):
            pass


def check():
    safe(Q, True)
    policy = host_policy()
    linger = command('loginctl', 'show-user', str(os.getuid()), '--property=Linger', '--value').stdout.strip()
    if linger != 'yes':
        raise RuntimeError('user lingering required')
    if not (CGROUP / 'cgroup.controllers').is_file():
        raise RuntimeError('cgroup v2 required')
    version = command('systemctl', '--user', 'show', '--property=Version', '--value').stdout.strip()
    if not version:
        raise RuntimeError('cannot query systemd user manager version')
    command('systemctl', '--user', '--quiet', 'is-enabled', 'baton-deferred.timer')
    command('systemctl', '--user', '--quiet', 'is-active', 'baton-deferred.timer')
    return {'version': VERSION, 'ok': True, 'resource_policy_version': RESOURCE_POLICY_VERSION,
            'machine_id': machine_id(), 'linger': linger,
            'user_manager': True, 'user_manager_version': version, 'timer_enabled': True, 'timer_active': True,
            'capabilities': ['systemd-user', 'cgroup-v2', 'serial-reservations', 'shared-heavy-gate-v1'],
            'policy': policy, 'cpu_count': os.cpu_count(), 'available_bytes': available()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('register', 'status', 'cancel', 'recall', 'check', 'tick', 'run', 'fleet_probe'))
    parser.add_argument('job', nargs='?')
    parser.add_argument('nonce', nargs='?')
    args = parser.parse_args()
    os.umask(0o077)
    safe(Q, True)
    if args.operation == 'run':
        ran = False
        try:
            ran = runner(args.job, args.nonce) is True
        finally:
            # this unit's end is news for the scheduler whichever way the runner left
            tick_soon(TICK_SOON_S if ran else TICK_RETRY_S)
        return
    with locked(Q / '.deferred.lock'):
        out = {'register': lambda: register(args.job), 'status': lambda: status(args.job),
               'cancel': lambda: cancel(args.job), 'recall': lambda: recall(args.job), 'check': check, 'tick': tick,
               'fleet_probe': fleet_probe}[args.operation]()
    print(json.dumps(out, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(json.dumps({'version': VERSION, 'error': str(error)}))
        raise SystemExit(1)
