"""Workstream-local deferred compute pickup; imports stay hook-safe and offline."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import time
import unicodedata

from . import config, paths, harvest

JOB_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}\Z')
TERMINAL = {'done', 'failed', 'timeout', 'cancelled'}


def identity(root):
    try:
        st = os.stat(root)
    except OSError:
        return None
    birth = getattr(st, 'st_birthtime_ns', None)
    if birth is None and os.name == 'nt':
        birth = st.st_ctime_ns
    return {'device': st.st_dev, 'inode': st.st_ino, 'birth_ns': birth}


def read(path):
    try:
        if path.is_symlink() or path.stat().st_size > 1024 * 1024:
            return {}
        value = json.loads(path.read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _norm(value):
    value = str(value)
    if value.startswith('\\\\?\\UNC\\'):
        value = '\\\\' + value[8:]
    elif value.startswith('\\\\?\\'):
        value = value[4:]
    # Windows TEMP commonly uses an 8.3 alias while the CLI resolves its root
    # to the long path. Junction/symlink aliases can name the same directory too;
    # identity still gates adoption, independently of this spelling comparison.
    return config.norm(os.path.realpath(value))


def state_for(host, state):
    if not host or host == 'ember':
        return state
    registry = read(state / 'hosts.json')
    host = registry.get('aliases', {}).get(host, host)
    entry = registry.get('hosts', {}).get(host, {})
    relative = entry.get('state')
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError('host state unavailable')
    target = (state / relative).resolve()
    if target == state.resolve() or not target.is_relative_to(state.resolve()):
        raise ValueError('host state outside queue')
    return target


def fingerprint(job):
    fields = {k: job.get(k) for k in ('status', 'sha', 'exit', 'finished', 't_finished',
                                     'out_paths', 'artifact_missing', 'reason',
                                     'artifact_digests', 'tail_digest', 'tail')}
    # Transport digests bind the captured bytes. Cheap local metadata also
    # invalidates a review after an ordinary edit/removal, without reading
    # every render on every prompt. Remote outputs are immutable snapshots,
    # not a watched directory after completion.
    fields['local_artifacts'] = []
    for name in job.get('out_paths') or []:
        try:
            st = Path(name).lstat()
            signature = [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_mode]
        except (OSError, ValueError, TypeError):
            signature = None
        fields['local_artifacts'].append([name, signature])
    return hashlib.sha256(json.dumps(fields, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


def local_dir(root, directory):
    return directory.resolve().is_relative_to(root.resolve()) and not directory.is_symlink()


def job_record_paths(state, jid, host_id, archived=False):
    """Files that can hold the queue record for a job id and host: one for a fleet job, else private then legacy.
    With `archived`, the same files in the stores' archives (<store>-archive), where the queue moves a finished job
    nothing has written for two days."""
    v2 = jid.startswith('cq-v2--')
    if host_id == 'fleet' and jid.startswith(('cq-fleet-', 'cq-v2--fleet-')):
        target = state / 'queue-v2' if v2 else state
        paths = [target / 'fleet/jobs' / (jid + '.json')]
    else:
        target = state_for(host_id, state)
        if v2:
            target = target / 'queue-v2'
        paths = [target / 'deferred-jobs' / (jid + '.json'), target / 'jobs' / (jid + '.json')]
    return [p.parent.with_name(p.parent.name + '-archive') / p.name for p in paths] if archived else paths


def job_record(state, jid, host_id):
    """Authoritative queue record for a job id and host; {} when unknown or ambiguous. A record in a store wins over
    one in that store's archive."""
    try:
        for archived in (False, True):
            files = [p for p in job_record_paths(state, jid, host_id, archived) if p.exists()]
            if len(files) > 1:
                return {}  # Never pick one of two conflicting records with the same ID.
            if files:
                return read(files[0])
        return {}
    except (OSError, ValueError, TypeError, AttributeError):
        return {}


def seen_by_session(job):
    """A finished job whose result the session has already been given: it was not deferred (no `requested_defer`,
    `defer` empty or `immediate`: it finished while the session was working) and the queue has delivered its
    completion notice (`notified` is true, set by the notice, a wait/status answer or a review). Such a job needs
    no `bt compute --reviewed`; a deferred job (`spare`, `overnight`) finished while nobody watched and does."""
    if job.get('status') not in TERMINAL or job.get('notified') is not True:
        return False
    return not job.get('requested_defer') and job.get('defer') in (None, '', 'immediate')


def collect(root, state=None, limit=None):
    root = Path(root)
    state = Path(state) if state else config.STATE_DIR / 'compute'
    current = identity(root)
    receipt_dir = root / '.baton/compute'
    if current is None or not local_dir(root, receipt_dir):
        return []
    receipts = sorted(receipt_dir.glob('*.json'))
    if limit is not None and len(receipts) > limit:
        raise OverflowError('More than 128 deferred receipts; use bt compute for the full inbox')
    rows = []
    for receipt in receipts:
        jid = receipt.stem
        if not JOB_ID.fullmatch(jid):
            continue
        pointer = read(receipt)
        row = {'id': jid, 'host_id': pointer.get('host_id') or 'ember', 'state': 'held'}
        if pointer.get('id') != jid or pointer.get('worktree_identity') != current:
            row['reason'] = 'Receipt belongs to another or unknown directory incarnation'
            rows.append(row)
            continue
        job = job_record(state, jid, row['host_id'])
        if (job.get('id') != jid or job.get('worktree_identity') != current
                or _norm(job.get('top') or job.get('root') or '') != _norm(root)):
            row['reason'] = 'Authoritative queue job does not match this worktree'
            rows.append(row)
            continue
        stamp = fingerprint(job)
        review_dir = receipt_dir / 'reviewed'
        ack = read(review_dir / (jid + '.json')) if local_dir(root, review_dir) else {}
        terminal = job.get('status') in TERMINAL
        acked = terminal and ack.get('fingerprint') == stamp
        reviewed = acked or seen_by_session(job)
        row.update(state=('follow-up' if acked and ack.get('resolution') == 'follow-up' else
                          'reviewed' if reviewed else 'needs-review' if terminal else 'pending'),
                   status=job.get('status'), phase=job.get('deferred_phase'),
                   registered=job.get('deferred_registered') is True, defer=job.get('defer'),
                   source=job.get('sha'), out_paths=job.get('out_paths') if isinstance(job.get('out_paths'), list) else [],
                   artifact_missing=job.get('artifact_missing') if isinstance(job.get('artifact_missing'), list) else [],
                   reason=job.get('reason') or '', worktree_identity=current,
                   fingerprint=stamp, finished=job.get('t_finished') if isinstance(job.get('t_finished'), (int, float)) else 0)
        if row['host_id'] == 'fleet':
            row['placed_host'] = job.get('host_id')
        rows.append(row)
    rank = {'needs-review': 0, 'follow-up': 1, 'pending': 2, 'held': 3, 'reviewed': 4}
    return sorted(rows, key=lambda r: (rank[r['state']], -r.get('finished', 0), r['id']))


def summary(root, state=None):
    try:
        rows = [r for r in collect(root, state, limit=128) if r['state'] != 'reviewed']
    except OverflowError as exc:
        return '## Deferred compute\n' + str(exc)
    if not rows:
        return ''
    lines = ['## Deferred compute',
             'Deferred results waiting for pickup and jobs still running. Look at them when this work is picked up '
             'or a result is needed (`bt compute --refresh`), not on every turn; a finished job whose notice reached '
             'this session is not listed.']
    for row in rows[:5]:
        durable = 'host-registered' if row.get('registered') else 'local/cached'
        lines.append(f"- {row['id']}: {row['state']}, {row.get('status', 'unknown')}, {durable}; {len(row.get('out_paths', []))} files")
    if len(rows) > 5:
        lines.append(f'+{len(rows)-5} more: `bt compute`')
    return '\n'.join(lines)


def prompt_summary(root, session):
    """Inject changed state once per session; this reminder does not mark review."""
    owner = read(Path(root) / '.baton/owner.json')
    if owner.get('session_id') and owner['session_id'] != session:
        return ''
    text = summary(root)
    if not text or not session:
        return text
    token = hashlib.sha256(str(session).encode()).hexdigest()[:24]
    flag = Path(root) / '.baton/compute' / ('.shown-' + token)
    digest = hashlib.sha256(text.encode()).hexdigest()
    try:
        if flag.read_text(encoding='utf-8') == digest:
            return ''
    except OSError:
        pass
    harvest.atomic_write(flag, digest)
    return text


def _owner(root, session=None):
    owner = read(root / '.baton/owner.json')
    vendor = owner.get('vendor')
    keys = ('CODEX_THREAD_ID', 'CODEX_SESSION_ID') if vendor == 'codex' else ('CLAUDE_CODE_SESSION_ID',)
    caller = session or next((os.environ.get(k) for k in keys if os.environ.get(k)), None)
    if not caller or owner.get('session_id') != caller:
        raise ValueError('Current worktree owner required; pass your own --session if the shell has no session identity')
    return caller


def _mark_notified(job_id):
    """A reviewed result has been seen: the queue's completion notice for it must not arrive afterwards (in this
    session or, after a handover, in the next). Best effort; the review itself is already recorded."""
    try:
        import sys
        sys.path.insert(0, str(config.INSTALL_HOME / 'mods/compute-queue/bin'))
        import quiet_helper
        quiet_helper.install()
        import cq
        cq.execute(cq.Ctx(state=str(config.STATE_DIR / 'compute')), 'notified', {'ids': [job_id]})
    except Exception:
        pass


def run(args):
    root = paths.find_worktree_root(args.worktree or os.getcwd())
    if root is None:
        raise ValueError('Use this command in the owned worktree or specify --worktree')
    if args.refresh or args.reviewed:
        session = _owner(root, args.session)
    if args.refresh:
        # Explicit CLI operation only; hooks never import the engine or use SSH.
        import sys
        sys.path.insert(0, str(config.INSTALL_HOME / 'mods/compute-queue/bin'))
        import quiet_helper
        quiet_helper.install()
        import cq
        ctx = cq.Ctx(state=str(config.STATE_DIR / 'compute'))
        for row in collect(root):
            if row['state'] != 'held' and (row['state'] != 'reviewed' or row.get('artifact_missing')):
                result = cq.execute(ctx, 'status', {'job': row['id'], 'refresh': True})
                if (result.get('refresh') or {}).get('ok') is False:
                    print(json.dumps({'id': row['id'], 'refresh': result['refresh']}))
    if args.reviewed:
        if not JOB_ID.fullmatch(args.reviewed):
            raise ValueError('Invalid job ID')
        row = next((r for r in collect(root) if r['id'] == args.reviewed), None)
        if row is None or row['state'] == 'held' or row.get('status') not in TERMINAL:
            raise ValueError('Only a matching finished job can be marked reviewed')
        review_dir = root / '.baton/compute/reviewed'
        if (not local_dir(root, review_dir) or identity(root) != row['worktree_identity']
                or _owner(root, args.session) != session):
            raise ValueError('Worktree ownership or receipt location changed')
        record = dict(fingerprint=row['fingerprint'], resolution=args.resolution,
                      session=session, at=time.time(), note=args.note or '')
        harvest.atomic_write(review_dir / (row['id'] + '.json'), json.dumps(record))
        _mark_notified(row['id'])
    rows = collect(root)
    if args.json:
        print(json.dumps({'jobs': rows}, indent=2))
    else:
        for row in rows:
            reason = ''.join(' ' if unicodedata.category(c).startswith('C') else c for c in str(row.get('reason', '')))[:200]
            print(f"{row['id']} {row['host_id']} {row['state']} {row.get('status', '')} {row.get('phase') or ''} {reason}")
            for path in row.get('out_paths', []):
                print('  ' + json.dumps(path, ensure_ascii=True))
        if not rows:
            print('No deferred compute receipts in this worktree.')
    return 0


def register(sub):
    parser = sub.add_parser('compute', help='Inspect this workstream’s deferred jobs and morning results')
    parser.add_argument('--worktree')
    parser.add_argument('--session', help='Your own session identity for refresh/review')
    parser.add_argument('--refresh', action='store_true', help='Reconcile bound remote jobs; never launches or replays work')
    parser.add_argument('--reviewed', metavar='JOB', help='Mark an inspected finished result; does not mark tests passed')
    parser.add_argument('--resolution', choices=('checked', 'follow-up', 'superseded'), default='checked')
    parser.add_argument('--note', help='Brief result or next action')
    parser.add_argument('--json', action='store_true')
    parser.set_defaults(func=run)
