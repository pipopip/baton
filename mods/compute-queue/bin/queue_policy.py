"""Shared, backwards-compatible compute host policy validation."""
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
