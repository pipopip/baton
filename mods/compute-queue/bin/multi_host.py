"""Isolated remote queue shards, layered over the legacy primary engine.

The primary store remains readable by old engines. A shard never shares its
job store, lease, samples or locks with the primary or another shard.
"""
import copy
import os
from pathlib import Path
import re
import queue_paths

_ID = re.compile(r"[a-z][a-z0-9_]{0,31}\Z")
_JOB = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}\Z")
_SHARD_JOB = re.compile(r"cq-([a-z][a-z0-9_]{0,31})-\d{8}T\d{6}-")


def _registry(engine, ctx, aliases=None):
    root = os.path.realpath(queue_paths.config_state(ctx.state))
    path = os.path.join(root, 'hosts.json')
    if not os.path.exists(path):
        return {}
    raw = engine.read_json(path)
    if not isinstance(raw, dict) or raw.get('version') != 1 or not isinstance(raw.get('hosts'), dict):
        raise engine.CqError('invalid hosts.json registry')
    seen = {os.path.normcase(root)}
    result = {}
    for host_id, entry in raw['hosts'].items():
        if not isinstance(host_id, str) or not _ID.fullmatch(host_id) or host_id in ('ember', 'laptop', 'fleet'):
            raise engine.CqError('invalid or reserved host ID in hosts.json')
        if not isinstance(entry, dict) or not isinstance(entry.get('enabled'), bool):
            raise engine.CqError('invalid host entry in hosts.json')
        relative = entry.get('state')
        if not isinstance(relative, str) or not relative or os.path.isabs(relative):
            raise engine.CqError('host state must be relative to the queue state')
        parts = relative.replace('\\', '/').split('/')
        if any(p in ('', '.', '..') for p in parts) or ':' in relative:
            raise engine.CqError('invalid host state path')
        state = os.path.realpath(os.path.join(root, *parts))
        try:
            inside = os.path.commonpath([root, state]) == root
        except ValueError:
            inside = False
        key = os.path.normcase(state)
        if not inside or key in seen:
            raise engine.CqError('host state escapes or duplicates a queue state')
        seen.add(key)
        result[host_id] = (state, entry['enabled'])
    # Nested shard roots could expose one another's data or locks.
    states = list(seen)
    for i, state in enumerate(states):
        for other in states[i + 1:]:
            if state == os.path.normcase(root) or other == os.path.normcase(root):
                continue
            if os.path.commonpath([state, other]) in (state, other):
                raise engine.CqError('host state directories must be disjoint')
    names = raw.get('aliases', {})
    if not isinstance(names, dict):
        raise engine.CqError('host aliases must be an object')
    for alias, target in names.items():
        if (not isinstance(alias, str) or not _ID.fullmatch(alias)
                or alias in ('ember', 'laptop', 'fleet') or alias in result
                or not isinstance(target, str) or target not in (*result, 'ember')):
            raise engine.CqError('invalid host alias: use a distinct name and a canonical host target')
    if 'hestia' not in names and 'hestia' not in result and 'hearth' in result:
        names = dict(names, hestia='hearth')
    if len(result) > 64:
        raise engine.CqError('too many configured hosts')
    if aliases is not None:
        aliases.update(names)
    return result


def _context(engine, ctx, host_id, registry, create=True):
    state, _ = registry[host_id]
    if queue_paths.is_generation2(ctx.state):
        state = os.path.join(state, 'queue-v2')
        if not create and not queue_paths.is_generation2(state):
            if os.path.islink(state) or (os.path.exists(state) and
                    (not os.path.isdir(state) or any(Path(state).iterdir()))):
                raise engine.CqError('generation-two host state exists without a valid marker')
            return None
        if create:
            queue_paths.ensure_generation2(state)
    child = engine.Ctx(state=state, now=ctx.now, ssh=ctx.ssh,
                       run_remoterun=ctx.run_remoterun, spawn=ctx.spawn,
                       laptop=ctx.laptop, sync=ctx.sync)
    child.host_id = host_id
    if hasattr(ctx, 'artifact_stream'):
        child.artifact_stream = ctx.artifact_stream
    return child


def _location(engine, ctx, job_id, registry):
    if not isinstance(job_id, str) or not _JOB.fullmatch(job_id):
        raise engine.CqError('invalid job ID')
    if job_id.startswith(('cq-fleet-', 'cq-v2--fleet-')):
        return ctx
    location_id = 'cq-' + job_id[len('cq-v2--'):] if job_id.startswith('cq-v2--') else job_id
    match = _SHARD_JOB.match(location_id)
    if match:
        host_id = match.group(1)
        if host_id not in registry:
            raise engine.CqError('unknown host for job')
        return _context(engine, ctx, host_id, registry, create=False)
    return ctx


def load_job(engine, ctx, job_id):
    registry = _registry(engine, ctx)
    target = _location(engine, ctx, job_id, registry)
    if job_id.startswith(('cq-fleet-', 'cq-v2--fleet-')):
        return engine.load_job(ctx, job_id)
    found = []
    candidates = [ctx] + [_context(engine, ctx, h, registry, create=False) for h in registry]
    for candidate in (c for c in candidates if c is not None):
        job = engine.load_job(candidate, job_id)
        if job is not None:
            found.append((candidate, job))
    if len(found) > 1:
        raise engine.CqError('duplicate job ID across queue shards')
    if found and (target is None or os.path.normcase(found[0][0].state) != os.path.normcase(target.state)):
        raise engine.CqError('job ID found in wrong queue shard')
    return found[0][1] if found else None


def all_jobs(engine, ctx, reader=None):
    """Validate every job ID; an optional poll reader may omit settled bodies."""
    registry = _registry(engine, ctx)
    out, seen = [], set()
    candidates = [ctx] + [_context(engine, ctx, h, registry, create=False) for h in registry]
    for candidate in (c for c in candidates if c is not None):
        pairs = reader(candidate) if reader is not None else ((j['id'], j) for j in engine.all_jobs(candidate))
        for jid, job in pairs:
            if jid in seen:
                raise engine.CqError('duplicate job ID across queue shards')
            target = _location(engine, ctx, jid, registry)
            if target is None or os.path.normcase(target.state) != os.path.normcase(candidate.state):
                raise engine.CqError('job ID found in wrong queue shard')
            seen.add(jid)
            if job is not None:
                out.append(job)
    return out


def _job_context(engine, ctx, req, registry):
    ids = list(req.get('ids') or [])
    if req.get('job'):
        ids.append(req['job'])
    if not ids:
        return ctx
    locations = [_location(engine, ctx, jid, registry) for jid in ids]
    if any(c is None for c in locations):
        raise engine.CqError('no such job in generation-two host store')
    if len({os.path.normcase(c.state) for c in locations}) != 1:
        raise engine.CqError('job batch spans queue shards')
    for jid in ids:
        load_job(engine, ctx, jid)  # detect duplicate and misplaced IDs before mutation
    return locations[0]


def execute(engine, ctx, command, req, leaf_execute):
    aliases = {}
    registry = _registry(engine, ctx, aliases)
    req = dict(req)
    if isinstance(req.get('host_id'), str):
        req['host_id'] = aliases.get(req['host_id'], req['host_id'])
    if command == 'notified' and req.get('job') and not req.get('ids'):
        req['ids'] = [req['job']]
    if command == 'submit':
        selected = req.pop('host_id', None)
        host_id = selected or 'ember'
        if req.get('defer') and selected:
            req['_deferred_host_selected'] = True
        if host_id == 'ember':
            return leaf_execute(ctx, command, req)
        if host_id not in registry or not registry[host_id][1]:
            raise engine.CqError('unknown or disabled host')
        if req.get('where', 'host') != 'host':
            raise engine.CqError('a selected host requires where=host')
        return leaf_execute(_context(engine, ctx, host_id, registry), command, req)

    if command in ('notified', 'notice_authorize') and req.get('ids'):
        # A completion prompt may cover independent hosts. Authorize/ack each
        # shard under its own lock; acknowledgements are idempotent on retry.
        groups = {}
        for jid in req['ids']:
            target = _location(engine, ctx, jid, registry)
            if target is None:
                raise engine.CqError('no such job in generation-two host store')
            load_job(engine, ctx, jid)
            groups.setdefault(target.state, (target, []))[1].append(jid)
        if command == 'notice_authorize' and len(req['ids']) > 5:
            return {'ok': False, 'error': 'invalid notice ids'}
        jobs, acknowledged, deferred, errors = [], [], [], []
        for target, ids in groups.values():
            bounded = copy.copy(target)
            bounded.qlock_timeout = .1
            try:
                result = leaf_execute(bounded, command, dict(req, ids=ids))
            except (engine.CqError, OSError) as exc:
                result = {'ok': False, 'error': str(exc)}
            if result.get('ok'):
                jobs.extend(result.get('jobs', []))
                if command == 'notified':
                    acknowledged.extend(ids)
            else:
                deferred.extend(ids)
                errors.append({'ids': ids, 'error': result.get('error', 'notice operation failed')})
        if req.get('notice_protocol') != 2 and errors:
            return {'ok': False, 'error': errors[0]['error']}
        return {'ok': True, 'jobs': jobs, 'acknowledged_ids': acknowledged,
                'deferred_ids': deferred, 'errors': errors}
    if command == 'notified' and req.get('host_id'):
        host_id = req.pop('host_id')
        if host_id not in registry:
            raise engine.CqError('unknown alert host')
        return leaf_execute(_context(engine, ctx, host_id, registry), command, req)
    if command in ('get', 'cancel', 'wait', 'notified', 'note', 'notice_authorize') or (command == 'status' and req.get('job')):
        return leaf_execute(_job_context(engine, ctx, req, registry), command, req)

    if command == 'list':
        now = ctx.now()
        jobs = all_jobs(engine, ctx)
        if req.get('status'):
            jobs = [j for j in jobs if j.get('status') == req['status']]
        if req.get('root'):
            top = engine._top_of(req['root'])
            jobs = [j for j in jobs if engine.same_path(j.get('top', ''), top)]
        jobs.sort(key=engine.order_key)
        jobs = jobs[-int(req.get('limit') or 100):]
        caps = {}
        rows = []
        for job in jobs:
            target = _location(engine, ctx, job['id'], registry)
            if target.state not in caps:
                caps[target.state] = engine.load_capacity(target)
            rows.append(engine.status_row(job, now, caps[target.state]))
        return {'ok': True, 'jobs': rows}

    primary = leaf_execute(ctx, command, req)
    if command == 'status':
        capacity = dict(primary.get('capacity') or {})
        summary = dict(primary.get('summary') or {})
        capacity['hosts'], summary['hosts'] = {}, {}
        queue, recent = list(primary.get('queue') or []), list(primary.get('recent') or [])
        for host_id in registry:
            try:
                child = _context(engine, ctx, host_id, registry, create=False)
                if child is None:
                    continue
                result = leaf_execute(child, 'status', req)
            except (engine.CqError, OSError) as exc:
                capacity['hosts'][host_id] = {'name': host_id, 'up': False, 'usable': False, 'error': str(exc)}
                summary['hosts'][host_id] = str(exc)
                continue
            queue.extend(result.get('queue') or [])
            recent.extend(result.get('recent') or [])
            capacity['hosts'][host_id] = (result.get('capacity') or {}).get('host')
            summary['hosts'][host_id] = (result.get('summary') or {}).get('host')
        primary.update(queue=queue, recent=recent, capacity=capacity, summary=summary)
    elif command in ('tick', 'probe', 'reap', 'watch', 'ensure_deferred') or (command == 'leader' and req.get('release')):
        results = {}
        for host_id, (_, enabled) in registry.items():
            try:
                child = _context(engine, ctx, host_id, registry)
                result = leaf_execute(child, command, req)
                if command == 'tick' and enabled and result.get('leader') and result.get('can_start'):
                    with engine.FileLock(child.p('leader.lock'), .1):
                        lease = engine.read_json(child.p('leader.json')) or {}
                        age = child.now() - lease.get('heartbeat', 0)
                        if lease.get('session') == req.get('session') and 0 <= age < engine.LEADER_STALE:
                            result['apply'] = leaf_execute(child, 'apply', {'planner': 'code'})
                results[host_id] = result
            except (engine.CqError, OSError) as exc:
                results[host_id] = {'ok': False, 'error': str(exc)}
        primary['per_host'] = results
        if command in ('tick', 'probe', 'watch'):
            capacity = dict(primary.get('capacity') or {})
            capacity['hosts'] = {
                host_id: dict((r.get('capacity') or {}).get('host') or {},
                              sample_at=(r.get('capacity') or {}).get('at'))
                for host_id, r in results.items()}
            primary['capacity'] = capacity
        if command in ('tick', 'watch') and req.get('notice_protocol') == 2:
            watches = [primary.get('watch') or {}] + [r.get('watch') or {} for r in results.values()]
            merged = dict(watches[0])
            for name in ('mine_running', 'mine_queued', 'mine_unverified', 'queued'):
                merged[name] = sum(w.get(name, 0) for w in watches)
            merged['to_notify'] = [j for w in watches for j in w.get('to_notify', [])]
            primary['watch'] = merged
    return primary
