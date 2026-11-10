"""Route legacy and generation-two queue records without migrating live work."""
import os
from pathlib import Path
import re

import multi_host
import queue_paths

_V2 = re.compile(r'cq-v2--(?:(?:[a-z][a-z0-9_]{0,31})-)?\d{8}T\d{6}-[0-9a-f]{16}\Z')


def job_generation(engine, jid):
    if not isinstance(jid, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}', jid):
        raise engine.CqError('invalid job ID')
    if jid.startswith('cq-v2--'):
        if not _V2.fullmatch(jid):
            raise engine.CqError('invalid generation-two job ID')
        return 2
    return 1


def _configured_overlap(engine, ctx, state):
    if getattr(ctx, 'host_id', None):
        return False
    root = os.path.normcase(os.path.realpath(state))
    for shard, _enabled in multi_host._registry(engine, ctx).values():
        candidate = os.path.normcase(os.path.realpath(shard))
        try:
            if os.path.commonpath([root, candidate]) == root:
                return True
        except ValueError:
            pass
    return False


def context(engine, ctx, generation):
    base = queue_paths.config_state(ctx.state)
    state = os.path.join(base, 'queue-v2') if generation == 2 else base
    if generation == 2:
        # An uncreated configured shard still owns its legacy subtree.
        if _configured_overlap(engine, ctx, state):
            raise engine.CqError('generation-two state overlaps a configured legacy host shard')
        queue_paths.ensure_generation2(state)
    if os.path.normcase(os.path.normpath(ctx.state)) == os.path.normcase(os.path.normpath(state)):
        return ctx
    result = engine.Ctx(state=state, now=ctx.now, ssh=ctx.ssh,
                        run_remoterun=ctx.run_remoterun, spawn=ctx.spawn,
                        laptop=ctx.laptop, sync=ctx.sync)
    # Preserve injected lock bounds and config-refresh behavior in tests and bridge.
    for attr in ('qlock_timeout', 'refresh_config', 'artifact_stream'):
        if hasattr(ctx, attr):
            setattr(result, attr, getattr(ctx, attr))
    result.host_id = ctx.host_id
    return result


def _v2_availability(engine, ctx):
    state = Path(queue_paths.config_state(ctx.state)) / 'queue-v2'
    try:
        if _configured_overlap(engine, ctx, state):
            return False, 'generation-two state overlaps a configured legacy host shard'
        if queue_paths.is_generation2(state):
            return True, None
        if state.is_symlink() or (state.exists() and (not state.is_dir() or any(state.iterdir()))):
            return False, 'generation-two state exists without a valid marker'
        return False, None
    except (OSError, ValueError) as exc:
        return False, str(exc)


def all_jobs(engine, ctx, reader=None):
    out, seen = [], set()
    if reader is None:
        def reader(candidate):
            for job in engine.all_jobs(candidate):
                yield job['id'], job
            if not candidate.host_id:
                for job in engine.all_fleet_jobs(candidate):
                    yield job['id'], job
    for generation in (1, 2):
        if generation == 2 and not _v2_availability(engine, ctx)[0]:
            continue
        c = context(engine, ctx, generation)
        for job in multi_host.all_jobs(engine, c, reader=reader):
            jid = job['id']
            if job_generation(engine, jid) != generation:
                raise engine.CqError('job ID found in wrong queue generation')
            if jid in seen:
                raise engine.CqError('duplicate job ID across queue generations')
            seen.add(jid)
            out.append(job)
    return out


def load_job(engine, ctx, jid):
    expected = job_generation(engine, jid)
    if expected == 1:
        # A legacy ID cannot legally live in the v2 store. Do not make existing
        # jobs depend on creating or adopting a new generation directory.
        return multi_host.load_job(engine, context(engine, ctx, 1), jid)
    found = []
    for generation in (1, 2):
        if generation == 2 and not _v2_availability(engine, ctx)[0]:
            continue
        job = multi_host.load_job(engine, context(engine, ctx, generation), jid)
        if job is not None:
            if generation != expected or not isinstance(job, dict) or job.get('id') != jid:
                raise engine.CqError('job ID found in wrong queue generation')
            found.append(job)
    if len(found) > 1:
        raise engine.CqError('duplicate job ID across queue generations')
    return found[0] if found else None


def _notices(engine, ctx, command, req, leaf):
    ids = req.get('ids') or ([req['job']] if req.get('job') else [])
    if (not isinstance(ids, list) or any(not isinstance(jid, str) for jid in ids)
            or (command == 'notice_authorize' and len(ids) > 5)):
        return {'ok': False, 'error': 'invalid notice ids'}
    groups = {1: [], 2: []}
    for jid in dict.fromkeys(ids):
        generation = job_generation(engine, jid)
        load_job(engine, ctx, jid)  # detect misplaced or duplicate records before mutation
        groups[generation].append(jid)
    results = []
    for generation, group in groups.items():
        if group:
            args = dict(req, ids=group)
            args.pop('job', None)
            try:
                if generation == 2 and not _v2_availability(engine, ctx)[0]:
                    raise engine.CqError('generation-two queue is unavailable')
                result = leaf(context(engine, ctx, generation), command, args)
            except (engine.CqError, OSError) as exc:
                result = {'ok': False, 'error': str(exc)}
            results.append((group, result))
    jobs, acknowledged, deferred, errors = [], [], [], []
    for group, result in results:
        if result.get('ok'):
            jobs.extend(result.get('jobs') or [])
            acknowledged.extend(result.get('acknowledged_ids', group if command == 'notified' else []))
            deferred.extend(result.get('deferred_ids') or [])
            errors.extend(result.get('errors') or [])
        else:
            deferred.extend(group)
            errors.append({'ids': group, 'error': result.get('error', 'notice operation failed')})
    if errors and req.get('notice_protocol') != 2:
        return {'ok': False, 'error': errors[0]['error']}
    return {'ok': True, 'jobs': jobs, 'acknowledged_ids': acknowledged,
            'deferred_ids': deferred, 'errors': errors}


def _merge_watch(primary, secondary):
    watch = dict(primary.get('watch') or {})
    other = secondary.get('watch') or {}
    for key in ('mine_running', 'mine_queued', 'mine_unverified', 'queued'):
        watch[key] = watch.get(key, 0) + other.get(key, 0)
    watch['to_notify'] = list(watch.get('to_notify') or []) + list(other.get('to_notify') or [])
    primary['watch'] = watch


def _apply_v2(engine, ctx, req, result, leaf):
    if not result.get('leader') or not result.get('can_start'):
        return
    try:
        with engine.FileLock(ctx.p('leader.lock'), .1):
            lease = engine.read_json(ctx.p('leader.json')) or {}
            age = ctx.now() - lease.get('heartbeat', 0)
            if lease.get('session') == req.get('session') and 0 <= age < engine.LEADER_STALE:
                result['apply'] = leaf(ctx, 'apply', {'planner': 'code'})
    except (engine.CqError, OSError) as exc:
        result['apply'] = {'ok': False, 'error': str(exc)}


def execute(engine, ctx, command, request, leaf):
    # Internal detached controllers are already pinned to their generation.
    if queue_paths.is_generation2(ctx.state):
        return leaf(ctx, command, request)
    req = dict(request)
    if command == 'submit':
        v2 = req.get('where') == 'fleet' or req.get('gets') is not None or any(
            req.get(key) is not None for key in ('artifact_max_mb', 'artifact_total_mb'))
        if v2 and req.get('where') != 'fleet':
            if (req.get('where', 'host') != 'host' or req.get('self_heavy') or req.get('split') is not None
                    or req.get('temp_storage', 'disk') != 'disk' or req.get('platform', 'linux') != 'linux'
                    or req.get('candidates') is not None or req.get('host_id') in ('laptop', 'fleet')):
                raise engine.CqError('bounded artifacts require a contained disk fleet job; no split, RAM scratch or self_heavy; use platform=windows for local work')
            # A second ordinary scheduler would not share the old client's RAM
            # reservations. Pin to the existing authoritative host fleet broker.
            selected = req.pop('host_id', None) or 'ember'
            req.update(where='fleet', candidates=[selected], platform='linux')
        return leaf(context(engine, ctx, 2 if v2 else 1), command, req)
    if command in ('notified', 'notice_authorize') and (req.get('ids') or req.get('job')):
        return _notices(engine, ctx, command, req, leaf)
    if req.get('job') and command in ('get', 'status', 'cancel', 'wait', 'note'):
        generation = job_generation(engine, req['job'])
        load_job(engine, ctx, req['job'])
        if generation == 2 and not _v2_availability(engine, ctx)[0]:
            raise engine.CqError('generation-two queue is unavailable')
        return leaf(context(engine, ctx, generation), command, req)
    if command not in ('list', 'status', 'watch', 'tick', 'ensure_deferred', 'probe', 'reap'):
        return leaf(ctx, command, req)
    # Cached fleet view is scoped to the current fleet, now generation two.
    if command == 'status' and req.get('view'):
        if req['view'] != 'fleet' or req.get('job') or req.get('refresh'):
            raise engine.CqError('view=fleet is a cached overview; omit job and refresh')
        import fleet_telemetry
        result = {'ok': True, 'fleet': fleet_telemetry.summarize_generations(queue_paths.config_state(ctx.state), ctx.now())}
        _, problem = _v2_availability(engine, ctx)
        if problem:
            result['generation2_unavailable'] = problem
        return result
    args = dict(req)
    if command == 'list':
        args['limit'] = 1000
    primary = leaf(context(engine, ctx, 1), command, args)
    available, problem = _v2_availability(engine, ctx)
    if not available:
        if problem:
            primary['generation2_unavailable'] = problem
        return primary
    secondary = leaf(context(engine, ctx, 2), command, args)
    if not primary.get('ok') or not secondary.get('ok'):
        return {'ok': False, 'error': 'queue generation operation failed', 'legacy': primary, 'v2': secondary}
    if command == 'tick':
        _apply_v2(engine, context(engine, ctx, 2), req, secondary, leaf)
    if command == 'list':
        rows = list(primary.get('jobs') or []) + list(secondary.get('jobs') or [])
        seen = set()
        for row in rows:
            jid = row.get('id')
            if jid in seen:
                raise engine.CqError('duplicate job ID across queue generations')
            seen.add(jid)
        rows.sort(key=lambda j: (engine.PRIO_RANK.get(j.get('priority'), 2), -float(j.get('age_min') or 0), j.get('id') or ''))
        primary['jobs'] = rows[-max(1, min(1000, int(req.get('limit') or 100))):]
    elif command == 'status':
        for key in ('queue', 'recent'):
            primary[key] = list(primary.get(key) or []) + list(secondary.get(key) or [])
    elif command in ('watch', 'tick'):
        _merge_watch(primary, secondary)
    primary['generations'] = {'v2': {k: secondary[k] for k in ('capacity', 'summary', 'fleet', 'per_host', 'apply') if k in secondary}}
    return primary
