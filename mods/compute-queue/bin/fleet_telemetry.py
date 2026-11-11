"""Bounded, cache-only fleet telemetry. No scheduler or transport imports."""
from __future__ import annotations

import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import time
from contextlib import contextmanager

MAX_EVENT_BYTES = 2 * 1024 * 1024
MAX_JOB_FILES = 4096
MAX_JSON_BYTES = 256 * 1024
STALE_SECONDS = 300
QUEUE_DELAY_SECONDS = 900
RECENT_SECONDS = 86400
HOSTS = (('laptop', 'L'), ('hearth', 'H'), ('ember', 'E'))
STATUSES = ('queued', 'launching', 'running', 'done', 'failed', 'cancelled', 'timeout')
JOB_SETTLE_S = 10.0    # _read_jobs: a job file that changed this recently is read again next time
JOB_REFRESH_S = 300.0  # _read_jobs: and every file is read again this often, whatever its size and times say
_JOB_CACHE = {}        # _read_jobs: jobs directory -> {file name: (signature, job, monotonic time read)}; this process only
_TOKEN = re.compile(r'^[A-Za-z0-9_.:-]{1,100}$')
_OOM = re.compile(r'\boom\b|out[ -]of[ -]memory|memory limit', re.I)


def _number(value):
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _time(value):
    if _number(value):
        return float(value)
    if isinstance(value, str):
        try:
            result = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
            if result.tzinfo is None:
                result = result.replace(tzinfo=dt.timezone.utc)
            return result.timestamp()
        except (ValueError, OverflowError, OSError):
            pass
    return None


def _read(path, size=None):
    """The JSON object in a file of at most MAX_JSON_BYTES, else None. `size`: the file's size when the caller already
    has it from a directory listing, which saves the stat."""
    try:
        if (path.stat().st_size if size is None else size) > MAX_JSON_BYTES:
            return None
        with path.open(encoding='utf-8') as f:
            value = json.load(f)
        return value if isinstance(value, dict) else None
    except (OSError, ValueError, UnicodeError):
        return None


def _read_jobs(directory):
    """(number of job files in the directory, the readable jobs among the first MAX_JOB_FILES in name order).

    One directory listing gives each file's size and times; a file is opened only when those differ from what this
    process saw when it last read the file, when the file had changed within JOB_SETTLE_S of that read, or once
    JOB_REFRESH_S has passed. One status snapshot used to open every fleet job file three times, in every session's
    tick. The jobs returned are shared between calls: callers only read them."""
    directory = Path(directory)
    try:
        with os.scandir(directory) as found:
            entries = sorted(((e.name, e) for e in found if e.name.endswith('.json')), key=lambda pair: pair[0])
    except OSError:
        _JOB_CACHE.pop(str(directory), None)
        return 0, []
    cache, kept, jobs = _JOB_CACHE.get(str(directory), {}), {}, []
    for name, entry in entries[:MAX_JOB_FILES]:
        try:
            info = entry.stat()
            stamp = (info.st_mtime_ns, info.st_ctime_ns, info.st_size)
        except OSError:
            stamp = None  # never hide a job because its metadata could not be read
        hit, clock = cache.get(name), time.monotonic()
        if hit is not None and stamp is not None and hit[0] == stamp and 0 <= clock - hit[2] < JOB_REFRESH_S:
            kept[name] = hit
            job = hit[1]
        else:
            job = _read(directory / name, None if stamp is None else stamp[2])
            if job is not None and stamp is not None and time.time() - stamp[0] / 1e9 >= JOB_SETTLE_S:
                kept[name] = (stamp, job, clock)
        if job:
            jobs.append(job)
    _JOB_CACHE[str(directory)] = kept
    return len(entries), jobs


# The status line. Device words: UPPERCASE for a host that is down, stale or paused (somebody can act); lowercase for
# a limit that is working as designed (a RAM floor, the legacy queue holding the host). A device that is merely full
# shows its numbers alone. Lifetime or 24 h totals of failures and missing artifacts are not in the line: they never
# clear. They stay in `alerts` for the fleet view.
DISPLAY_ORDER = ('ember', 'hearth', 'laptop')
LINE_WIDTH = 100
_ACTIVE = ('queued', 'launching', 'running')


def _worktree_key(job):
    top = job.get('top') if isinstance(job.get('top'), str) and job.get('top') else job.get('root')
    if not isinstance(top, str) or not top:
        return None
    return top.replace(chr(92), '/').rstrip('/').lower()[:300]


def _reason_word(reason):
    r = (reason or '').lower()
    if 'paus' in r:
        return 'PAUSED'
    if 'probe unavailable' in r or 'offline' in r:
        return 'DOWN'
    if 'stale' in r:
        return 'STALE'
    if 'legacy' in r or 'draining' in r:
        return 'legacy busy'
    if 'ram' in r or 'memory' in r or 'commit' in r:
        return 'RAM floor'
    if 'load' in r:
        return 'load cap'
    if 'unknown size' in r:
        return 'size unknown'
    if 'cpu slots' in r:
        return ''
    return 'blocked'


def _label(host, d):
    """(text, attention) for one device; text None when the cache never reported it."""
    if d.get('present') is False:
        return None, None
    name = ' '.join(str(d.get('name') or host).split())[:12].strip() or host
    if d.get('stale'):
        return f'{name} STALE', f'{name} stale'
    if d.get('up') is False:
        return f'{name} DOWN', f'{name} down'
    used, total = d.get('slots_used'), d.get('slots_total')
    text = f"{name} {'?' if used is None else used}/{'?' if total is None else total}"
    if d.get('eligible') is False:
        full = used is not None and total is not None and used >= total
        word = _reason_word(d.get('reason')) if d.get('reason') else ('' if full else 'unavailable')
        if word in ('PAUSED', 'DOWN', 'STALE'):
            return f'{text} {word}', f'{name} {word.lower()}'
        if word:
            text += ' ' + word
    return text, None


def _compose(devices, order, counts, tops, sessions, truncated, started=0):
    """The line and its structured parts, from device facts and live job records. ``started`` counts the jobs whose
    record still says 'launching' while their host reports them running (a fleet job keeps that status for its
    whole run): they are running work, not waiting work."""
    started = max(0, min(started, counts['launching']))
    running = counts['running'] + started
    waiting = counts['queued'] + counts['launching'] - started  # a registered overnight job waits as 'launching'
    labels, attention = [], []
    for host in order:
        text, attn = _label(host, devices[host])
        if text is not None:
            labels.append(text)
        if attn:
            attention.append(attn)
    fresh = [d for d in devices.values() if d.get('present') is not False and not d.get('stale')]

    def can_take(d):
        used, total = d.get('slots_used'), d.get('slots_total')
        full = used is not None and total is not None and used >= total
        return d.get('up') is not False and (d.get('eligible') is not False or full)
    blocked = waiting > 0 and bool(fresh) and not any(can_take(d) for d in fresh)
    if blocked:
        attention.append('dispatch blocked')
    activity = {'running': running, 'waiting': waiting, 'worktrees': len(tops), 'sessions': len(sessions)}

    def build(run, queued, tree):
        wt = '' if not tops else f" · {len(tops)} {tree if len(tops) != 1 else tree.rstrip('s')}"
        work = f'{running} {run} · {waiting} {queued}{wt}' if running or waiting else 'idle'
        parts = list(labels) or ['no capacity data']
        parts.append(work)
        if blocked:
            parts.append('DISPATCH BLOCKED')
        if truncated:
            parts.append('truncated')
        return ' · '.join(parts)
    line = build('running', 'queued', 'worktrees')
    if len(line) > LINE_WIDTH:
        line = build('run', 'queued', 'wt')
    return line, activity, attention, blocked


def summarize(state, now=None, _collect=None):
    """Read a bounded snapshot. Output contains no commands, reasons or job IDs."""
    state = Path(state)
    now = _time(now) if now is not None else time.time()
    if now is None:
        raise ValueError('invalid now')
    capacity = _read(state / 'capacity.json') or {}
    raw_devices = capacity.get('devices')
    raw_devices = raw_devices if isinstance(raw_devices, dict) else {}
    devices, alerts = {}, {}

    def alert(name):
        alerts[name] = alerts.get(name, 0) + 1

    keys = {host for host, _ in HOSTS} | {host for host in raw_devices if isinstance(host, str) and re.fullmatch(r'[a-z][a-z0-9_]{0,31}', host)}
    labels = dict(HOSTS)
    ordered = [host for host, _ in HOSTS] + sorted(keys - set(labels))[:64]
    for host in ordered:
        raw = raw_devices.get(host)
        raw = raw if isinstance(raw, dict) else {}
        at = _time(raw.get('at'))
        if at is None:
            at = _time(capacity.get('at'))
        stale = at is None or now - at > STALE_SECONDS or at > now + STALE_SECONDS
        up = raw.get('up') if isinstance(raw.get('up'), bool) else None
        used = raw.get('slots_used')
        total = raw.get('slots_total')
        used = int(used) if _number(used) and 0 <= used <= 100000 else None
        total = int(total) if _number(total) and 0 <= total <= 100000 else None
        if stale:
            alert('stale')
        if up is False:
            alert('down')
        if used is not None and total is not None and used > total:
            alert('overrun')
        name = raw.get('name')
        name = ''.join(c if c.isprintable() else ' ' for c in name)[:80] if isinstance(name, str) else host
        devices[host] = {'name': name, 'up': up, 'eligible': raw.get('eligible') if isinstance(raw.get('eligible'), bool) else None,
                         'stale': stale, 'slots_used': used, 'slots_total': total,
                         'present': isinstance(raw_devices.get(host), dict)}
        # Older caches do not establish a configured pool or a reserve. Do not
        # infer a reserve from a host label or make an old sample look fresh.
        for key in ('slots_configured', 'slots_reserved'):
            value = raw.get(key)
            devices[host][key] = int(value) if _number(value) and 0 <= value <= 100000 else None
        for key in ('memory_available_gb', 'memory_reserved_gb', 'memory_held_gb', 'memory_floor_gb', 'commit_free_gb', 'load', 'load_cap'):
            value = raw.get(key)
            devices[host][key] = value if _number(value) and value >= 0 else None
        reason = raw.get('reason')
        devices[host]['reason'] = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', reason)[:160] if isinstance(reason, str) else ''
        if devices[host]['eligible'] is False and up is True and not stale:
            alert('blocked')

    counts = {status: 0 for status in STATUSES}
    started = 0
    tops, sessions = set(), set()
    found, jobs = _read_jobs(state / 'jobs')
    truncated = found > MAX_JOB_FILES
    for job in jobs:
        status = job.get('status')
        if not isinstance(status, str) or status not in counts:
            continue
        counts[status] += 1
        if status == 'launching' and job.get('deferred_phase') == 'running':
            started += 1
        if status in _ACTIVE:
            key = _worktree_key(job)
            if key:
                tops.add(key)
            if isinstance(job.get('session'), str) and job['session']:
                sessions.add(job['session'])
        submitted = _time(job.get('t_submitted'))
        finished = _time(job.get('t_finished'))
        recent = finished is None or 0 <= now - finished <= RECENT_SECONDS
        if status == 'queued' and submitted is not None and now - submitted > QUEUE_DELAY_SECONDS:
            alert('queue_delay')
        if job.get('launch_uncertain') is True and status in ('launching', 'running'):
            alert('uncertain')
        if job.get('cancel_requested') and status in ('queued', 'launching', 'running'):
            alert('cancel_pending')
        if job.get('artifact_missing') and recent:
            alert('artifact_missing')
        if status in ('failed', 'timeout') and recent:
            alert('failure')
        reason = job.get('reason')
        if recent and isinstance(reason, str) and _OOM.search(reason[:1024]):
            alert('oom')
        metrics = job.get('metrics') if isinstance(job.get('metrics'), dict) else {}
        peak, cap = metrics.get('peak_memory_bytes'), job.get('memory_max_gb')
        if _number(peak) and _number(cap) and cap > 0 and peak > cap * 1024 ** 3:
            alert('overrun')
    if _collect is not None:
        _collect['tops'] |= tops
        _collect['sessions'] |= sessions
    order = [h for h in DISPLAY_ORDER if h in devices] + sorted(set(devices) - set(DISPLAY_ORDER))
    line, activity, attention, blocked = _compose(devices, order, counts, tops, sessions, truncated, started)
    return {'line': line, 'devices': devices, 'counts': counts, 'alerts': alerts, 'truncated': truncated,
            'activity': activity, 'attention': attention, 'dispatch_blocked': blocked, 'alerts_window_s': RECENT_SECONDS,
            'started': started}


def summarize_generations(queue_state, now=None):
    """Combine both fleet generations without adding shared device capacities."""
    queue_state = Path(queue_state)
    timestamp = _time(now) if now is not None else time.time()
    if timestamp is None:
        raise ValueError('invalid now')
    v2 = queue_state / 'queue-v2'
    marker = v2 / '.generation'
    try:
        valid_v2 = (not v2.is_symlink() and not marker.is_symlink() and marker.is_file()
                    and marker.stat().st_size == len(b'baton-queue-generation-2\n')
                    and marker.read_bytes() == b'baton-queue-generation-2\n')
    except OSError:
        valid_v2 = False
    roots = [queue_state / 'fleet'] + ([v2 / 'fleet'] if valid_v2 else [])
    collect = {'tops': set(), 'sessions': set()}
    summaries = [summarize(root, timestamp, collect) for root in roots]
    capacities = [_read(root / 'capacity.json') or {} for root in roots]
    raw = [c.get('devices') if isinstance(c.get('devices'), dict) else {} for c in capacities]
    counts = {name: sum(s['counts'][name] for s in summaries) for name in STATUSES}
    alerts = {}
    for summary in summaries:
        for name, value in summary['alerts'].items():
            if name not in ('stale', 'down', 'blocked', 'overrun'):
                alerts[name] = alerts.get(name, 0) + value
    # Count persisted active bindings from both stores, independently of cache
    # age. Neither capacity cache is additive: host probes share one broker.
    active = {}
    unknown = set()
    for root in roots:
        for job in _read_jobs(root / 'jobs')[1]:
            host = job.get('host_id')
            if job.get('status') not in ('launching', 'running') or not isinstance(host, str):
                continue
            if not re.fullmatch(r'[a-z][a-z0-9_]{0,31}', host):
                continue
            # A host-registered deferred job waiting for its window holds no slot (admission leaves it out too,
            # fleet_queue._pending). Counting it showed a host as over its pool, with an overrun alert, for the
            # whole evening before the night's jobs started.
            if host != 'laptop' and job.get('deferred_registered') is True and job.get('deferred_phase') == 'queued':
                continue
            slots = job.get('slots')
            if type(slots) is not int or not 0 < slots <= 32:
                unknown.add(host)
            else:
                active[host] = active.get(host, 0) + slots
    hosts = set().union(*(set(summary['devices']) for summary in summaries), set(active), unknown)
    devices = {}
    for host in sorted(hosts):
        def sample_time(index):
            entry = raw[index].get(host)
            if not isinstance(entry, dict):
                return float('-inf')
            t = _time(entry.get('at'))
            return t if t is not None else (_time(capacities[index].get('at')) or float('-inf'))
        chosen = max(range(len(roots)), key=sample_time)
        d = dict(summaries[chosen]['devices'].get(host) or {'name': host, 'up': None, 'eligible': None,
                 'stale': True, 'slots_used': None, 'slots_total': None, 'present': False})
        if host in unknown or host in active:
            d['present'] = True
        if host in unknown:
            d['slots_used'] = None
            d['eligible'] = False
            d['reason'] = 'active reservation has unknown size'
        elif active.get(host):
            d['slots_used'] = max(d['slots_used'] or 0, active[host])
            # A stale sample cannot establish spare capacity for active work.
            if d['stale']:
                d['eligible'] = False
                d['reason'] = 'active reservation; capacity sample stale'
        devices[host] = d
        for name, yes in (('stale', d.get('stale')), ('down', d.get('up') is False),
                          ('blocked', d.get('eligible') is False and d.get('up') is True and not d.get('stale')),
                          ('overrun', d.get('slots_used') is not None and d.get('slots_total') is not None
                           and d['slots_used'] > d['slots_total'])):
            if yes:
                alerts[name] = alerts.get(name, 0) + 1
    truncated = any(s['truncated'] for s in summaries)
    order = [h for h in DISPLAY_ORDER if h in devices] + sorted(set(devices) - set(DISPLAY_ORDER))
    line, activity, attention, blocked = _compose(devices, order, counts, collect['tops'], collect['sessions'], truncated,
                                                  sum(s.get('started', 0) for s in summaries))
    return {'line': line, 'devices': devices, 'counts': counts, 'alerts': alerts, 'truncated': truncated,
            'activity': activity, 'attention': attention, 'dispatch_blocked': blocked, 'alerts_window_s': RECENT_SECONDS}


@contextmanager
def _lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    locked = False
    try:
        deadline = time.monotonic() + 5
        while not locked:
            try:
                if os.name == 'nt':
                    import msvcrt
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError('fleet event lock timed out')
                time.sleep(0.025)
        yield
    finally:
        if locked:
            if os.name == 'nt':
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def append_event(state, event):
    """Append an allowlisted compact event; keep one bounded rotation under a process lock."""
    if not isinstance(event, dict):
        raise TypeError('event must be a dict')
    safe = {}
    for key in ('type', 'id', 'status', 'host_id', 'memory_kind'):
        val = event.get(key)
        if isinstance(val, str) and _TOKEN.fullmatch(val):
            safe[key] = val
    for key in ('at', 'slots', 'memory_gb', 'memory_max_gb', 'peak_memory_bytes', 'exit'):
        val = event.get(key)
        if _number(val):
            safe[key] = val
    safe.setdefault('at', time.time())
    line = (json.dumps(safe, separators=(',', ':'), allow_nan=False) + '\n').encode('utf-8')
    if len(line) > MAX_EVENT_BYTES:
        raise ValueError('event exceeds event log limit')
    state = Path(state)
    with _lock(state / 'events.lock'):
        path = state / 'events.jsonl'
        if path.exists() and path.stat().st_size + len(line) > MAX_EVENT_BYTES:
            os.replace(path, state / 'events.jsonl.1')
        with path.open('ab') as f:
            f.write(line)


def estimate_memory(history, profile_key, command_key, platform, host_id, slots, default_gb):
    """Suggest a conservative RAM reservation; never change a job's hard cap.

    Only fully completed, successful exact matches with one shared memory kind
    count. Mixed measurement kinds fall back rather than being combined.
    """
    default = float(default_gb) if _number(default_gb) else 1.0
    default = min(1024.0, max(0.25, default))
    result = {'memory_gb': default, 'sample_count': 0, 'memory_kind': None, 'method': 'default'}
    if any(x is None or x == '' for x in (profile_key, command_key, platform, host_id, slots)):
        return result
    values, kinds = [], set()
    for row in history:
        if (not isinstance(row, dict) or row.get('status') != 'done'
                or not _number(row.get('exit')) or row.get('exit') != 0):
            continue
        if any(row.get(k) != v for k, v in (('profile_key', profile_key), ('command_key', command_key),
                                           ('platform', platform), ('host_id', host_id), ('slots', slots))):
            continue
        start, finish = _time(row.get('t_started')), _time(row.get('t_finished'))
        if start is None or finish is None or finish < start:
            continue
        reason = row.get('reason')
        if isinstance(reason, str) and _OOM.search(reason[:1024]):
            continue
        metrics = row.get('metrics') if isinstance(row.get('metrics'), dict) else {}
        kind, peak = metrics.get('memory_kind'), metrics.get('peak_memory_bytes')
        if not isinstance(kind, str) or not _TOKEN.fullmatch(kind) or not _number(peak) or peak <= 0:
            continue
        values.append(float(peak) / 1024 ** 3)
        kinds.add(kind)
    if not values or len(kinds) != 1:
        return result
    # Max plus 25% avoids underestimating small or skewed samples; clamp a
    # finite advisory value without lowering the caller's default reservation.
    estimate = max(default, min(1024.0, max(values) * 1.25))
    return {'memory_gb': math.ceil(estimate * 4) / 4, 'sample_count': len(values),
            'memory_kind': next(iter(kinds)), 'method': 'max_plus_25_percent'}
