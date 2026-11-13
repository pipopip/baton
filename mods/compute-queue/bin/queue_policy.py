"""Shared, backwards-compatible compute host policy validation."""
import math
import re

RESOURCE_POLICY_VERSION = 2
DEFAULTS = {'foreground_slots': 2, 'timezone': 'Europe/London',
            'window_start': '00:00', 'window_end': '08:00'}
_ZONE = re.compile(r'[A-Za-z0-9_+.-]+(?:/[A-Za-z0-9_+.-]+)*\Z')
_TIME = re.compile(r'(?:[01][0-9]|2[0-3]):[0-5][0-9]\Z')


def _time(value):
    if type(value) is int and 0 <= value <= 23:
        return f'{value:02d}:00'
    if isinstance(value, str) and _TIME.fullmatch(value):
        return value
    raise ValueError('window times must be HH:MM or an integer hour')


def policy_fields(raw, pool_slots=None, validate_zone=False):
    if not isinstance(raw, dict):
        raise ValueError('policy must be an object')
    reserve = raw.get('foreground_slots', DEFAULTS['foreground_slots'])
    if type(reserve) is not int or reserve < 0 or reserve > 31:
        raise ValueError('invalid foreground_slots')
    if pool_slots is not None and (type(pool_slots) is not int or not 1 <= pool_slots <= 32 or reserve >= pool_slots):
        raise ValueError('foreground_slots must be less than pool_slots')
    zone = raw.get('timezone', DEFAULTS['timezone'])
    if (not isinstance(zone, str) or len(zone) > 255 or not _ZONE.fullmatch(zone)
            or any(part in ('.', '..') for part in zone.split('/'))):
        raise ValueError('invalid timezone')
    if validate_zone:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
        try:
            ZoneInfo(zone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError('unknown timezone') from exc
    start = _time(raw.get('window_start', DEFAULTS['window_start']))
    end = _time(raw.get('window_end', DEFAULTS['window_end']))
    if start == end:
        raise ValueError('window start and end must differ')
    return {'foreground_slots': reserve, 'timezone': zone, 'window_start': start, 'window_end': end}


def policy_is_default(raw):
    return policy_fields(raw) == DEFAULTS


# Memory of fleet jobs on a Linux host (host/deferred.py, host/install_deferred.py, fleet_queue.py).
# All fleet units run in one systemd slice whose memory limit is the host's total less its RAM floor. A job may be
# admitted on the memory it is expected to use only while that limit is in effect on every running fleet job and the
# rest of the machine leaves room for it (host/deferred.py expected_mode), and the caps of the jobs running together
# may then add up to POOL_OVERCOMMIT times what the pool can hold.
FLEET_SLICE = 'cq-fleet.slice'
POOL_OVERCOMMIT = 2
CPU_OVERCOMMIT = 2                   # the declared slots (CPU quotas) of the fleet jobs on a host, as a multiple of its fleet
                                     # slots, when jobs are charged the CPU they are expected to use (deferred.py charged_slots)
CPU_QUOTA_WAIT = 'fleet CPU quotas exceed the overcommit bound'   # the reason both sides give when that bound is reached


def fleet_slice_cpu_quota(pool_slots, foreground_slots):
    """CPUQuota of the fleet slice, in percent: all fleet jobs together get the CPU of the fleet's slots and no more,
    whatever their own quotas add up to. With every job charged what it declares this never binds; where jobs are
    charged the CPU they are expected to use it is what keeps the foreground slots free when they burst together."""
    return max(1, pool_slots - foreground_slots) * 100
POOL_MARGIN_BYTES = 1024 ** 3        # of the RAM floor that memory held outside the fleet jobs may not use (expected_mode)
POOL_MIN_BYTES = 2 * 1024 ** 3       # a pool smaller than this gets no limit, and so no expected-use admission
POOL_SLACK_BYTES = 256 * 1024 ** 2   # an installed limit may exceed the host's memory less the floor by this: MemTotal
                                     # moves by some MB between kernels, and the margin above covers it
# A failed job's reason when the kernel killed its unit for memory starts with this text; it matches the
# controller's telemetry test for out-of-memory reasons (fleet_telemetry._OOM).
OOM_REASON_PREFIX = 'out of memory (oom-kill)'


def fleet_slice_limit(total_bytes, ram_floor_gb):
    """MemoryMax of the fleet slice for a host with this much memory and this RAM floor, or None when the pool
    would be under POOL_MIN_BYTES (no limit is written then)."""
    limit = int(total_bytes - math.ceil(ram_floor_gb * 1024 ** 3))
    return limit if limit >= POOL_MIN_BYTES else None
