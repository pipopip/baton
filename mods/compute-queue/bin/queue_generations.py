"""Route legacy and generation-two queue records without migrating live work."""
import json
import os
from pathlib import Path
import re

import multi_host
import queue_paths
import queue_policy

_V2 =re.compile(r'cq-v2--(?:(?:[a-z][a-z0-9_]{0,31})-)?\d{8}T\d{6}-[0-9a-f]{16}\Z')


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
    for attr in ('qlock_timeout', 'refresh_config', 'artifact_stream', 'claude_home'):
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
    mine = dict(watch.get('mine_attention') or {})
    for key, value in (other.get('mine_attention') or {}).items():
        mine[key] = mine.get(key, 0) + value
    if mine:
        watch['mine_attention'] = mine
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


HOST_JOB_CAP_FLOOR_GB = 8      # a where: host job that becomes a fleet job without a stated memory cap gets the larger
HOST_JOB_CAP_PER_SLOT_GB = 4   # of this floor and this much per slot (50 at most): above what fleet submitters state today
DEFAULT_JOB_BASE_TOOLS = ('bash', 'python')   # on every host: a job that declares only these says nothing about what it needs


def host_job_for_fleet(engine, ctx, req):
    """Whether an ordinary heavy `where: host` submission becomes a fleet job pinned to its host. An ordinary heavy
    job has no memory cap, so the host cannot run it beside fleet work: while one runs or waits, the host admits no
    fleet job at all. As a fleet job it is contained (CPU held to its slots, memory capped) and shares the host.
    The host is the default one (ember) when the submission names none, names `ember` or names an alias of it.
    A submission that names another configured host (by its id or an alias) is converted only when that host's own
    settings opt in: "host_jobs_to_fleet": true in the host.conf.json of its shard state directory. Its GPU and
    Blender jobs are qualified on its ordinary queue and keep it unless its owner says otherwise. An unknown or
    disabled host, `laptop` and `fleet` are never converted: they keep the ordinary path and its own refusal.
    Also kept ordinary: a light job (slots 0, which does not stop fleet work), and what the fleet backend does not
    do (split, self_heavy, RAM scratch, deferred through the ordinary queue, estimates over 480 minutes).
    A job larger than the host's fleet pool (its slots less the foreground reserve) stays ordinary too: the fleet
    backend would accept it and never place it. So does every job while generation two cannot be used.
    The other host must also be one the fleet backend can use: an `address`, a `host_id` equal to its registry id and
    not `paused` in that same file; otherwise the job would wait for good as offline, so it keeps the ordinary path.
    An empty host_id is the same as none. "host_jobs_to_fleet": false in the queue's own host.conf.json turns the default host's conversion off. The
    host's file is read here, once and only for a request that would otherwise convert, because the engine's loaded
    settings are those of whichever host or shard served the last request; its pool, memory budget and foreground
    reserve come from that file. When it cannot be read, nothing is converted.

    Returns None, or (host, cap): the host the fleet job is pinned to (canonical id) and the default memory cap in
    GB for a job that states none: the larger of HOST_JOB_CAP_FLOOR_GB and HOST_JOB_CAP_PER_SLOT_GB a slot, at most
    the host's memory budget (and 50). "Pinned" is the rule for a submission that names a host. A default-host job
    that declares its tools may also run elsewhere: see default_job_hosts, which `execute` asks after this."""
    slots, est, timeout = req.get('slots', 1), req.get('est_minutes'), req.get('timeout_minutes')
    named = req.get('host_id')
    if named == '':
        named = None  # multi_host.execute sends an empty host_id to the default host
    if not ((req.get('where') or 'host') == 'host' and type(slots) is int and slots >= 1
            and not req.get('defer') and not req.get('self_heavy') and req.get('split') is None
            and req.get('temp_storage', 'disk') == 'disk' and req.get('platform', 'linux') == 'linux'
            and req.get('candidates') is None
            and (named is None or (isinstance(named, str) and named not in ('laptop', 'fleet')))
            and isinstance(req.get('command'), str) and '{part}' not in req['command']
            and (est is None or (type(est) is int and 1 <= est <= 480))
            and (timeout is None or (type(timeout) is int and 1 <= timeout <= 480))):
        return None
    try:
        host, state = 'ember', queue_paths.config_state(ctx.state)
        if named not in (None, 'ember'):
            aliases = {}
            registry = multi_host._registry(engine, ctx, aliases)
            host = aliases.get(named, named)
            if host != 'ember':
                if host not in registry or not registry[host][1]:
                    return None  # unknown or disabled: the ordinary path gives its refusal
                state = registry[host][0]
        try:
            with open(os.path.join(state, 'host.conf.json'), encoding='utf-8') as handle:
                conf = json.load(handle)
        except FileNotFoundError:
            conf = {}  # no settings: the engine's defaults
        if not isinstance(conf, dict):
            return None
        if host == 'ember':
            if conf.get('host_jobs_to_fleet') is False:
                return None
        elif conf.get('host_jobs_to_fleet') is not True:
            return None
        elif not (conf.get('address') and conf.get('host_id') == host and not conf.get('paused')):
            return None  # the fleet backend would not take this host (fleet_queue._hosts): the ordinary path refuses loudly
        pool, budget = conf.get('slots', 8), conf.get('memory_budget_gb', 42)
        if type(pool) is not int or isinstance(budget, bool) or not isinstance(budget, (int, float)) or not budget >= 1:
            return None
        if slots > pool - queue_policy.policy_fields(conf, pool)['foreground_slots']:
            return None
        usable, why = _v2_availability(engine, ctx)
        if not usable and why:
            return None
        return host, min(50.0, float(budget), max(HOST_JOB_CAP_FLOOR_GB, HOST_JOB_CAP_PER_SLOT_GB * slots))
    except Exception:
        return None  # whatever is wrong with the settings, the request keeps the path it always had


def default_job_hosts(engine, ctx, req, selected, cap):
    """The other hosts a converted default-host job may also run on, as host ids in sorted order; [] for none. The
    queue cannot know what a job needs from a host, so only a job that says so spreads, and only to a host that says
    it is qualified: the submission named no host at all (naming `ember` or an alias of it is a pin, an empty host_id
    is none), it went to the default host (ember), and its `requires` is a list of tool names (the fleet backend's
    own rules: at most 32, each of letters, digits and _.+- up to 64) naming at least one tool beyond
    DEFAULT_JOB_BASE_TOOLS. A registry host joins when it is enabled, the fleet backend can use it (its settings,
    read as the fleet backend reads them with the engine's host_conf, hold an `address`, a `host_id` equal to its
    registry id and no `paused`), its `default_jobs` list holds every name in `requires`, the job's slots fit its
    fleet pool (its slots less the foreground reserve) and its `memory_budget_gb`, when it states one, is not below
    `cap` (the default memory cap, which is ember's and does not change). A host that cannot be read or judged is
    left out, as is every host when the registry cannot be read: the job then stays on `selected` alone, as it did
    before this setting existed. `requires` stays in the request when a host joins: every kept name is on that
    host's own list, so the fleet backend can place the job there, and a name outside every list means no spreading,
    the ember-only path on which `requires` is dropped, so the job cannot be stranded by it."""
    requires, slots = req.get('requires'), req.get('slots', 1)
    if (req.get('host_id') or None) is not None or selected != 'ember' or type(slots) is not int or slots < 1:
        return []
    if not (isinstance(requires, list) and 1 <= len(requires) <= 32 and all(
            isinstance(x, str) and re.fullmatch(r'[a-zA-Z0-9_.+-]{1,64}', x) for x in requires)):
        return []
    if all(name in DEFAULT_JOB_BASE_TOOLS for name in requires):
        return []
    try:
        registry = multi_host._registry(engine, ctx, {})
    except Exception:
        return []
    found = []
    for host in sorted(registry):
        try:
            state, enabled = registry[host]
            if not enabled:
                continue
            conf = engine.host_conf(state)
            if not (conf.get('address') and conf.get('host_id') == host and not conf.get('paused')
                    and set(requires) <= set(conf.get('default_jobs', ()))):
                continue
            pool = conf.get('slots', 8)
            if type(pool) is not int or slots > pool - queue_policy.policy_fields(conf, pool)['foreground_slots']:
                continue
            if conf.get('memory_budget_gb', cap) < cap:
                continue  # the fleet backend's own admission keeps a job off a host without a budget that cannot fit
            found.append(host)
        except Exception:
            continue  # unreadable or senseless settings: this host stays out
    return found


LAPTOP_FLEET_FORM = ("where: \"fleet\", candidates: [\"laptop\"], platform: \"macos\" or \"portable\", slots of at least 1, "
                     "no split, self_heavy, RAM scratch or defer")


def laptop_job_for_fleet(engine, req):
    """A Mac's `where: laptop` submission as the fleet job it can only be: the ordinary queue's laptop path is Windows',
    and the Mac's laptop runs contained fleet jobs (mac_pool). The job is pinned to the laptop, so the laptop's
    time limit (laptop_max_minutes) does not apply to it. What the fleet backend does not do is refused with the form
    that does it."""
    slots, platform = req.get('slots', 1), req.get('platform') or 'macos'
    if (type(slots) is not int or slots < 1 or req.get('self_heavy') or req.get('split') is not None
            or req.get('temp_storage', 'disk') != 'disk' or req.get('defer') or platform not in ('macos', 'portable')
            or req.get('host_id') not in (None, '', 'laptop') or req.get('candidates') not in (None, ['laptop'])):
        raise engine.CqError("where 'laptop' on this Mac runs as a fleet job on the laptop; this submission cannot: use "
                             + LAPTOP_FLEET_FORM)
    req.pop('host_id', None)
    req.update(where='fleet', candidates=['laptop'], platform=platform, slots=slots)
    if not req.get('label'):
        words = [w for w in str(req.get('command') or '').split() if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*=', w)]
        req['label'] = ' '.join(words[:6])[:60] or None
    return req


def execute(engine, ctx, command, request, leaf):
    # One refusal for every route (single host, shard, generation, fleet): who submits or cancels is always named.
    if command in ('submit', 'cancel'):
        engine.require_request_identity(request, 'submission' if command == 'submit' else 'cancellation')
    # Internal detached controllers are already pinned to their generation.
    if queue_paths.is_generation2(ctx.state):
        return leaf(ctx, command, request)
    req = dict(request)
    if command == 'submit':
        req.pop('_default_memory_max_gb', None)  # set here only, for a converted where: host job
        laptop_fleet = req.get('where') == 'laptop' and engine.laptop_executes() and engine.laptop_platform() == 'macos'
        if laptop_fleet:
            laptop_job_for_fleet(engine, req)
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
        plan = None if v2 or getattr(ctx, 'host_id', None) else host_job_for_fleet(engine, ctx, req)
        fleet = None
        if plan is not None:
            try:
                fleet = context(engine, ctx, 2)
            except (engine.CqError, ValueError, OSError):
                fleet = None  # generation two cannot be opened: the request keeps the path it always had
        if fleet is not None:
            selected, default_cap = plan
            extras = default_job_hosts(engine, ctx, req, selected, default_cap)  # asks the submission's host_id: before it is dropped
            req.pop('host_id', None)  # the fleet backend takes candidates, not a host_id
            slots, text = req.get('slots', 1), req['command']
            candidates = [selected, *extras]
            if not extras:
                # The ordinary backend ignored `requires` and named a job after its command: both stay so. (With extras
                # `requires` stays: each name is on the extra host's own list, and the fleet backend places by it.)
                req.pop('requires', None)
            if not req.get('label'):
                words = [w for w in text.split() if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*=', w)]
                req['label'] = (' '.join(words[:6]) or ' '.join(text.split()[:6]))[:60] or None
            req.update(where='fleet', candidates=candidates, platform='linux', slots=slots, _default_memory_max_gb=default_cap)
            try:
                result = leaf(fleet, command, req)
            except engine.CqError as exc:
                raise engine.CqError(f'{exc} (a heavy where: host job now runs as a contained fleet job '
                                     f'on {selected}{"" if len(candidates) == 1 else ", or another host that takes default jobs"}, '
                                     f'so {"the host" if len(candidates) == 1 else "a host"} can share its capacity)') from exc
            if isinstance(result, dict) and result.get('ok') and isinstance(result.get('job'), str):
                try:
                    job = engine.load_job(fleet, result['job'])
                    cap = job['memory_max_gb']
                    stored = job.get('candidates')  # what the fleet backend kept: a sandbox job loses a host without one
                    where_text = ' or '.join(stored if isinstance(stored, list) and stored and all(
                        isinstance(h, str) and h for h in stored) else candidates)
                    result['note'] = (f"{result.get('note') or 'accepted'}; this where: host job runs as a contained fleet job on "
                                      f"{where_text}: CPU held to {slots} slot{'s' if slots != 1 else ''}, memory capped at {cap:g} GB "
                                      f"(state memory_max_gb to change it)")
                except Exception:
                    pass  # the note is a courtesy; the job is accepted
            return result
        result = leaf(context(engine, ctx, 2 if v2 else 1), command, req)
        if laptop_fleet and isinstance(result, dict) and result.get('ok'):
            result['note'] = (result.get('note') or 'accepted') + '; this where: laptop job runs as a contained fleet job on the Mac'
        return result
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
    if command in ('watch', 'tick', 'status') and ('fleet' in primary or 'fleet' in secondary):
        # The primary result carries the older generation's snapshot alone: its capacity cache is no longer written
        # once generation two runs, so a status line read from it showed a stale, unchanging fleet. One view of both.
        # The tick also schedules the queue and delivers notices: a snapshot that cannot be built must not fail it.
        try:
            import fleet_telemetry
            primary['fleet'] = fleet_telemetry.summarize_generations(queue_paths.config_state(ctx.state), ctx.now())
        except Exception as exc:
            primary['fleet_warning'] = 'combined fleet snapshot unavailable: ' + type(exc).__name__
    primary['generations'] = {'v2': {k: secondary[k] for k in ('capacity', 'summary', 'fleet', 'per_host', 'apply') if k in secondary}}
    return primary
