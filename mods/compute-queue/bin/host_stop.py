"""Request termination of positively identified members of one host job.

The session number narrows the search; it is never itself a signal target.
Each candidate is bound to a pidfd before its session and inherited job markers
are inspected. An unavailable pidfd or unverifiable member is never signalled.
"""
from __future__ import annotations

import errno
import json
import os
import re
import signal
import sys
import time

MAX_ENV = 1024 * 1024
JOB_ID = re.compile(r'cq-[A-Za-z0-9_-]+(?:\.p[1-8])?\Z')


def _session_from_stat(data):
    end = data.rfind(b')')
    if end < 0:
        raise ValueError('invalid proc stat')
    fields = data[end + 1:].split()
    if len(fields) < 4:
        raise ValueError('invalid proc stat')
    return int(fields[3])


def _matching_environment(data, jid, source):
    found = {}
    for item in data.split(b'\0'):
        key, separator, value = item.partition(b'=')
        if separator and key in (b'CQ_JOB', b'CQ_SRC'):
            if key in found:
                return False
            found[key] = value
    return found == {b'CQ_JOB': os.fsencode(jid), b'CQ_SRC': os.fsencode(source)}


def _scan(sid, jid, source, sig, *, proc_root='/proc', pidfd_open=None, pidfd_send=None, close=None):
    """Signal only pidfd-bound, identity-matched members; return counts."""
    pidfd_open = pidfd_open or os.pidfd_open
    pidfd_send = pidfd_send or signal.pidfd_send_signal
    close = close or os.close
    matched = sent = unverified = 0
    for name in os.listdir(proc_root):
        if not name.isascii() or not name.isdigit() or int(name) <= 1:
            continue
        pid = int(name)
        try:
            fd = pidfd_open(pid, 0)
        except OSError as exc:
            if exc.errno not in (errno.ENOENT, errno.ESRCH):
                unverified += 1
            continue
        try:
            try:
                with open(os.path.join(proc_root, name, 'stat'), 'rb') as f:
                    stat_data = f.read(8192)
                if _session_from_stat(stat_data) != sid:
                    continue
                with open(os.path.join(proc_root, name, 'environ'), 'rb') as f:
                    env = f.read(MAX_ENV + 1)
                if len(env) > MAX_ENV or not _matching_environment(env, jid, source):
                    unverified += 1
                    continue
                matched += 1
                pidfd_send(fd, sig, None, 0)
                sent += 1
            except (OSError, ValueError) as exc:
                if not isinstance(exc, OSError) or exc.errno not in (errno.ENOENT, errno.ESRCH):
                    unverified += 1
        finally:
            close(fd)
    return {'matched': matched, 'sent': sent, 'unverified': unverified}


def stop(jid, grace=3):
    if not isinstance(jid, str) or not JOB_ID.fullmatch(jid):
        return {'ok': False, 'reason': 'invalid job identity'}
    if type(grace) is not int or not 0 <= grace <= 10:
        return {'ok': False, 'reason': 'invalid stop grace'}
    if not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        return {'ok': False, 'reason': 'pidfd signalling unavailable'}
    directory = os.path.join(os.path.expanduser('~'), 'q', jid)
    source = os.path.join(directory, 'src')
    try:
        def number(filename):
            fd = os.open(os.path.join(directory, filename), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as f:
                value = f.read(65)
            if not re.fullmatch(rb'[0-9]{1,20}\s*', value):
                raise ValueError('invalid session identity')
            n = int(value)
            if n <= 1:
                raise ValueError('invalid session identity')
            return n
        sid = number('pid')
        try:
            pgid = number('pgid')
        except FileNotFoundError:
            pgid = sid
        if pgid != sid:
            raise ValueError('session identity disagrees with process group')
        # Signal zero to our own pidfd checks kernel support before any stop.
        fd = os.pidfd_open(os.getpid(), 0)
        try:
            signal.pidfd_send_signal(fd, 0, None, 0)
        finally:
            os.close(fd)
        term = _scan(sid, jid, source, signal.SIGTERM)
        if term['sent'] and grace:
            time.sleep(grace)
        kill = _scan(sid, jid, source, signal.SIGKILL) if term['sent'] else {'matched': 0, 'sent': 0, 'unverified': 0}
        return {'ok': not (term['unverified'] or kill['unverified']),
                'term_sent': term['sent'], 'kill_sent': kill['sent'],
                'unverified': term['unverified'] + kill['unverified']}
    except (OSError, ValueError, AttributeError):
        return {'ok': False, 'reason': 'pidfd stop or session identity unavailable'}


def main():
    try:
        result = stop(sys.argv[1], int(sys.argv[2])) if len(sys.argv) == 3 else {'ok': False, 'reason': 'invalid arguments'}
    except ValueError:
        result = {'ok': False, 'reason': 'invalid arguments'}
    print('CQ_PIDFD_STOP ' + json.dumps(result, separators=(',', ':')))
    return 0 if result.get('ok') else 2


if __name__ == '__main__':
    raise SystemExit(main())
