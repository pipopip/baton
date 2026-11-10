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
import time
import uuid
from zoneinfo import ZoneInfo

# The installed helper lives next to this module; the source tree keeps it in bin.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'bin'))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from queue_policy import RESOURCE_POLICY_VERSION, policy_fields

VERSION = 1
GIB = 1024 ** 3
Q = Path.home() / 'q'
CGROUP = Path('/sys/fs/cgroup')
SCRIPT = Path(__file__).resolve()
POLICY = SCRIPT.parent / 'config.json'
ID = re.compile(r'cq-[A-Za-z0-9_-]+(?:\.p[1-8])?\Z')
SHA = re.compile(r'[0-9a-f]{40,64}\Z')
TERMINAL = ('done', 'failed', 'cancelled')


def command(*args, check=True):
    return subprocess.run(args, capture_output=True, text=True, check=check, timeout=30)


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
            for key in ('nonce', 'boot', 'submitted', 'retry_safe', 'control_group', 'memory_bytes'):
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
            state.update(phase='done' if code == 0 and props.get('Result') == 'success' else 'failed',
                         reason='completed' if code == 0 and props.get('Result') == 'success' else 'unit exited without success', exit=code)
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


def reservations():
    """Read reservations without mutating state; malformed active state fails closed."""
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
        active.append({'id': d.name, 'phase': phase, 'slots': s['slots'],
                       'reserved_memory_bytes': s['memory_bytes']})
    return active, ordinary, waiting


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
    avail, total = available(), total_memory()
    busy = legacy_busy() or ordinary or not existing_lock_free(Q / '.heavy.gate', shared=True)
    if not any(s['phase'] == 'running' for s in active):
        busy = busy or any(not existing_lock_free(Q / ('.heavy.' + str(i))) for i in range(policy['pool_slots']))
    for item in active:
        item.update(run_facts(Q / item['id']))
    reasons = []
    if busy:
        reasons.append('legacy work active or waiting')
    if waiting:
        reasons.append('ordinary deferred work waiting')
    if load > policy['load_cap']:
        reasons.append('load above cap')
    terminal = []
    for d in sorted(Q.glob('cq-*')):
        safe(d, True)
        state = read(d / 'deferred-state.json')
        if not state or not state.get('fleet') or state.get('phase') not in TERMINAL:
            continue
        item = {'id': d.name, 'phase': state['phase'], 'exit_code': state.get('exit'),
                'reason': str(state.get('reason', ''))[:200], **run_facts(d)}
        peak = state.get('memory_peak_bytes')
        if type(peak) is int and peak >= 0:
            item.update(memory_peak_bytes=peak, memory_kind='cgroup_memory')
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
            'reserved_memory_bytes': sum(s['reserved_memory_bytes'] for s in active),
            'legacy_busy': busy, 'ordinary_waiting': waiting, 'reasons': reasons,
            'active_fleet': active, 'terminal_fleet': terminal[-32:]}


def fleet_reason(value, jid=None):
    active, ordinary, waiting = reservations()
    if ordinary or waiting or legacy_busy():
        return 'legacy or ordinary deferred work active or waiting'
    if not window(value):
        return 'outside window or estimate exceeds remaining time'
    if os.getloadavg()[0] > value['load_cap']:
        return 'load above cap'
    other = [s for s in active if s['id'] != jid]
    if sum(s['slots'] for s in other) + value['slots'] > value['pool_slots'] - policy_fields(value, value['pool_slots'])['foreground_slots']:
        return 'fleet CPU pool exhausted'
    reserve = sum(s['reserved_memory_bytes'] for s in other) + math.ceil(value['memory_max_gb'] * GIB)
    if available() < math.ceil(value['ram_floor_gb'] * GIB) + reserve:
        return 'insufficient available memory for fleet reservations and floor'
    return None


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
    started = []

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
        registered = s.get('registered', 0)
        if type(registered) not in (int, float) or not math.isfinite(registered):
            raise RuntimeError('invalid registration time')
        aged = time.time() - registered >= 3600
        return (bool(s.get('fleet')), not aged,
                registered if aged else rank.get(s.get('priority', 'lane'), 4), registered, d.name)

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
                held = slots(v['pool_slots'], v['slots'], policy_fields(v, v['pool_slots'])['foreground_slots']) if not reason else None
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
                save(d.name, s)
                if is_fleet and reason != 'outside window or estimate exceeds remaining time':
                    return result(reason=reason, job=d.name)
                continue
            s.update(phase='starting', reason='unit submission pending', nonce=uuid.uuid4().hex,
                     boot=boot(), submitted=False, memory_bytes=math.ceil(v['memory_max_gb'] * GIB),
                     fleet=is_fleet, slots=v['slots'])
            save(d.name, s)  # persistent claim precedes the potentially ambiguous submission
            cmd = ['systemd-run', '--user', '--quiet', '--unit=' + unit(d.name),
                   '--property=Type=exec', '--property=ExitType=cgroup', '--property=RemainAfterExit=yes',
                   '--property=KillMode=control-group', '--property=SendSIGKILL=yes', '--property=TimeoutStopSec=30',
                   '--property=RuntimeMaxSec=' + str(v['timeout_minutes'] * 60 + 30),
                   '--property=CPUQuota=' + str(v['slots'] * 100) + '%',
                   '--property=MemoryMax=' + str(s['memory_bytes']), '--property=MemorySwapMax=0',
                   '--property=OOMPolicy=kill', '--property=StandardOutput=append:' + str(d / 'run.log'),
                   '--property=StandardError=inherit', sys.executable, str(SCRIPT), 'run', d.name, s['nonce']]
            command(*cmd)
            s['submitted'] = True
            s['reason'] = 'unit submitted'
            save(d.name, s)
            started.append(d.name)
            if not is_fleet or len(started) >= min(8, v['pool_slots']):
                return result()
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
            # Never reset an ambiguous claim: status/cancel can settle it safely.
            return result(error=str(exc), job=d.name)
    return result() if started else result(reason='no eligible jobs')


def runner(jid, nonce):
    held = []
    gate = None
    with locked(Q / '.deferred.lock'):
        s, v = staged_state(jid)
        if s['phase'] != 'starting' or s.get('nonce') != nonce or s.get('cancel_requested'):
            raise RuntimeError('stale or cancelled claim')
        s['control_group'] = self_group()
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
                held = slots(v['pool_slots'], v['slots'], policy_fields(v, v['pool_slots'])['foreground_slots'])
                if held is None:
                    reason = 'insufficient free heavy slots and foreground headroom'
            if reason:
                s.update(retry_safe=True, reason=reason + '; command not started')
                save(jid, s)
                return
            # Durable start marker is written BEFORE creating the command process.
            s.update(phase='running', reason='command start claimed', command_started=int(time.time()))
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
        proc = subprocess.Popen(['/bin/bash', str(jobdir(jid) / 'run.sh')], pass_fds=tuple(held + ([gate] if gate is not None else [])),
                                stdin=subprocess.DEVNULL, start_new_session=True)
        proc.wait()
        while True:
            try:
                os.waitpid(-1, 0)
            except ChildProcessError:
                break
        # Read the cgroup while this runner still keeps it alive.
        try:
            relative = self_group()
            group = CGROUP / relative.lstrip('/')
            metrics = {name: int((group / name).read_text().strip()) for name in ('memory.current', 'memory.peak')}
            with locked(Q / '.deferred.lock'):
                state = read(jobdir(jid) / 'deferred-state.json')
                if state and state.get('nonce') == nonce:
                    state['memory_current_bytes'] = metrics['memory.current']
                    state['memory_peak_bytes'] = metrics['memory.peak']
                    save(jid, state)
        except (OSError, ValueError, StopIteration):
            pass  # unavailable cgroup counters do not change the job result
    finally:
        for fd in held:
            os.close(fd)
        if gate is not None:
            os.close(gate)


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
    parser.add_argument('operation', choices=('register', 'status', 'cancel', 'check', 'tick', 'run', 'fleet_probe'))
    parser.add_argument('job', nargs='?')
    parser.add_argument('nonce', nargs='?')
    args = parser.parse_args()
    os.umask(0o077)
    safe(Q, True)
    if args.operation == 'run':
        runner(args.job, args.nonce)
        return
    with locked(Q / '.deferred.lock'):
        out = {'register': lambda: register(args.job), 'status': lambda: status(args.job),
               'cancel': lambda: cancel(args.job), 'check': check, 'tick': tick, 'fleet_probe': fleet_probe}[args.operation]()
    print(json.dumps(out, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(json.dumps({'version': VERSION, 'error': str(error)}))
        raise SystemExit(1)
