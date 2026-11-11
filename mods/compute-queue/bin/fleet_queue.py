"""Fleet coordinator. All accepted jobs live in the primary fleet store.

The controller uses a content-pinned runtime and a single state lock; it never
retries a staging operation after the endpoint may have observed it.
"""
from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
import posixpath
import re
import secrets
import shlex
import sys
import time
from pathlib import Path

import fleet_telemetry as telemetry
import queue_paths
import queue_policy

E = None
ACTIVE = ('queued', 'launching', 'running')
RESTAGE_AFTER_S = 900  # an unconfirmed staging attempt this old that left no directory on its host is made again
HISTORY_S = 30 * 86400  # the memory estimate also reads the fleet jobs archived within this time
SIZE_WEIGHT = 0.5          # _order: minutes a job stands back in its class per slot-minute of estimated work
SIZE_CAP_SLOT_MIN = 240    # _order: slot-minutes of work counted at most (so two hours back at most)
AGE_S = 3600               # _order: a queued job that has waited this long goes ahead of newer work of every class
BACKFILL_OVERRUN = 0.25    # _expected_end: a running job past its estimate is given this share of it again
BACKFILL_MIN_LEFT_S = 600  # _expected_end: ... and at least this long
BACKFILL_UNKNOWN_S = 600   # _reserve: with no shadow time, a job estimated to end within this may pass a held host
HOLD_PATIENCE_S = 900      # _reserve: ... and only this long after the held job began to hold; then nothing passes
HELD_MIN_SAMPLES = 6       # _sizing_note: readings a run's held peak needs to count (the host's first six span about 14 s)
TERMINAL = ('done', 'failed', 'cancelled', 'timeout')
RUNTIME_FILES = ('artifact_transfer.py', 'host_stop.py', 'queue_generations.py', 'queue_paths.py', 'cq.py', 'multi_host.py', 'remote_stage.py', 'quiet_helper.py',
                 'fleet_queue.py', 'fleet_telemetry.py', 'windows_pool.py', 'windows_broker.py', 'queue_policy.py')
GIB = 1024 ** 3


def bind(engine):
    global E
    E = engine


def _engine():
    if E is None:
        raise RuntimeError('fleet_queue.bind(engine) required')
    return E


def _error(message):
    raise _engine().CqError(message)


def _num(value, lo=0, hi=1e12):
    try:
        return type(value) in (int, float) and math.isfinite(value) and lo <= value <= hi
    except (TypeError, OverflowError):
        return False


def _legacy_state(ctx):
    return getattr(ctx, 'legacy_state', ctx.state)


def _legacy_context(ctx):
    if _legacy_state(ctx) == ctx.state:
        return ctx
    e = _engine()
    return e.Ctx(state=_legacy_state(ctx), now=ctx.now, ssh=ctx.ssh,
                 run_remoterun=ctx.run_remoterun, spawn=ctx.spawn,
                 laptop=ctx.laptop, sync=ctx.sync)


def all_jobs(ctx):
    e = _engine()
    return e._jobs_in(ctx, os.path.join('fleet', 'jobs'))


def _get(ctx, jid):
    prefix = 'cq-v2--fleet-' if getattr(ctx, 'generation', 1) == 2 else 'cq-fleet-'
    if not isinstance(jid, str) or not re.fullmatch(re.escape(prefix) + r'\d{8}T\d{6}-[0-9a-f]{16}', jid):
        _error('invalid fleet job ID')
    job = _engine().load_job(ctx, jid)
    if not isinstance(job, dict) or job.get('id') != jid or not job.get('fleet'):
        _error('no such fleet job')
    return job


def _safe_relative(value, name):
    if not isinstance(value, str) or not value or '\0' in value:
        _error(name + ' must be a relative path')
    path = posixpath.normpath(value.replace('\\', '/'))
    if path == '..' or path.startswith('../') or posixpath.isabs(path) or re.match(r'^[A-Za-z]:', path):
        _error(name + ' must stay inside the worktree')
    return path


def _hosts(ctx):
    """Read explicit configured endpoints only; no CQ_HOST or LANE_HOST fallbacks."""
    e = _engine()
    result = {}
    legacy_ctx = _legacy_context(ctx)
    primary = e.host_conf(legacy_ctx.state)
    if primary.get('address') and primary.get('host_id', 'ember') == 'ember' and not primary.get('paused'):
        result['ember'] = (legacy_ctx.state, primary)
    registry = e.multi_host._registry(e, legacy_ctx)
    for host_id, (path, enabled) in sorted(registry.items()):
        if not enabled:
            continue
        conf = e.host_conf(path)
        if conf.get('address') and conf.get('host_id') == host_id and not conf.get('paused'):
            result[host_id] = (path, conf)
    return result


def _digest(*parts):
    return hashlib.sha256(json.dumps(parts, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def _descriptor_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                                     allow_nan=False).encode('utf-8')).hexdigest()


def _boot_valid(value):
    return isinstance(value, str) and 1 <= len(value) <= 128 and value.isprintable()


def _event(ctx, kind, job):
    try:
        telemetry.append_event(ctx.p('fleet'), dict(type=kind, id=job['id'], status=job.get('status'),
                               host_id=job.get('host_id'), slots=job.get('slots'), at=ctx.now()))
    except (OSError, ValueError, TimeoutError):
        pass  # advisory logging must not reject an already accepted job


def _submit(ctx, req):
    e = _engine()
    command, root = req.get('command'), req.get('root')
    if not isinstance(command, str) or not command.strip() or '\0' in command:
        _error('command is required')
    e.require_request_identity(req, 'submission')
    if not isinstance(root, str) or not os.path.isdir(root):
        _error('root must be a worktree directory')
    if req.get('host_id') is not None:
        _error('where=fleet cannot use host_id; use candidates')
    if req.get('self_heavy') or req.get('split') is not None or req.get('temp_storage', 'disk') != 'disk' or '{part}' in command:
        _error('fleet requires disk, no split and no self_heavy')
    slots = req.get('slots', 1)
    if type(slots) is not int or not 1 <= slots <= 32:
        _error('slots must be an integer in 1..32')
    platform = req.get('platform', 'linux')
    if platform not in ('linux', 'windows', 'portable'):
        _error('platform must be linux, windows or portable')
    aliases = {}
    registry = e.multi_host._registry(e, _legacy_context(ctx), aliases)
    allowed = {'ember', 'laptop'} | {h for h, (_, enabled) in registry.items() if enabled}
    candidates = req.get('candidates', sorted(allowed))
    if not isinstance(candidates, list) or not candidates or any(not isinstance(x, str) for x in candidates):
        _error('candidates must be a nonempty list of configured host IDs or aliases')
    candidates = list(dict.fromkeys(aliases.get(x, x) for x in candidates))
    if any(x not in allowed for x in candidates):
        _error('unknown or disabled fleet candidate')
    candidates = [h for h in candidates if (h == 'laptop' and platform in ('windows', 'portable')) or (h != 'laptop' and platform in ('linux', 'portable'))]
    defer = req.get('defer')
    if defer not in (None, 'spare', 'overnight'):
        _error('defer must be spare or overnight')
    if defer:
        candidates = [h for h in candidates if h != 'laptop']
    try:
        sandbox, network = e.sandbox_request(req)
    except e.CqError as exc:
        _error(str(exc))
    if sandbox is True and platform != 'linux':
        _error(e.SANDBOX_LINUX_ONLY)
    if not candidates:
        _error(e.SANDBOX_LINUX_ONLY if sandbox is True else 'no candidates match platform and deferral')
    # keep the hosts whose setting takes the request (a host with an invalid setting takes none); a host not configured
    # here is checked at placement
    configured, refusals, kept = _hosts(ctx), [], []
    for host in candidates:
        try:
            e.sandbox_plan(dict(sandbox=sandbox, network=network), configured[host][1] if host in configured else {})
            kept.append(host)
        except e.SandboxError as exc:
            refusals.append(str(exc))
    if not kept:
        _error(refusals[0])
    candidates = kept
    requires = req.get('requires') or []
    if not isinstance(requires, list) or len(requires) > 32 or any(not isinstance(x, str) or not re.fullmatch(r'[a-zA-Z0-9_.+-]{1,64}', x) for x in requires):
        _error('requires must be a list of tool names')
    requires = list(dict.fromkeys(requires))
    if platform == 'portable' and re.match(r'^[ \t]*python(?:3)?(?=[ \t\r\n]|$)', command) and 'python' not in requires:
        requires.append('python')
    subdir = _safe_relative(req.get('subdir') or '.', 'subdir')
    gets = e.normalize_gets(req.get('gets'))
    try:
        artifact_policy = e.artifact_transfer.normalize_policy(req)
    except ValueError as exc:
        _error(str(exc))
    est = req.get('est_minutes', 10)
    if type(est) is not int or not 1 <= est <= 480:
        _error('est_minutes must be an integer in 1..480')
    timeout = req.get('timeout_minutes', min(480, max(30, 3 * est)))
    if type(timeout) is not int or not 1 <= timeout <= 480:
        _error('timeout_minutes must be an integer in 1..480')
    priority = req.get('priority') or 'lane'
    if priority not in e.PRIORITIES:
        _error('invalid priority')
    if defer:
        priority = 'background'
    resource_key = req.get('resource_key')
    if resource_key is not None and (not isinstance(resource_key, str) or len(resource_key) > 256):
        _error('resource_key must be a string of at most 256 characters')
    for name in ('memory_gb', 'memory_max_gb'):
        if req.get(name) is not None and not _num(req[name], .25, 50):
            _error(name + ' must be finite and between 0.25 and 50 GiB')
    if (req.get('memory_gb') is not None and req.get('memory_max_gb') is not None
            and req['memory_max_gb'] < req['memory_gb']):
        _error('memory_max_gb must be at least memory_gb')
    top = e.git(root, 'rev-parse', '--show-toplevel')
    identity = e.worktree_identity(top)
    owner = e.read_json(os.path.join(top, '.baton', 'owner.json'))
    session = req.get('session') or ''
    if identity is None or not isinstance(owner, dict) or not session or owner.get('session_id') != session:
        _error('fleet submission requires the current worktree owner session')
    now = ctx.now()
    jid = e.new_id(now, 'fleet')
    if getattr(ctx, 'generation', 1) == 2:
        jid = 'cq-v2--' + jid[3:]
    sha, snap = e.snapshot(ctx, top, jid, req.get('source') or 'worktree')
    receipt = None
    try:
        base = e.git(top, 'rev-parse', sha + '^') if snap else sha
        tree = e.git(top, 'rev-parse', sha + '^{tree}')
        common = e.git(top, 'rev-parse', '--git-common-dir')
        common = os.path.realpath(os.path.join(top, common))
        profile_key = _digest(os.path.normcase(common), resource_key or '')
        command_key = _digest(command, subdir, tree)
        memory = req.get('memory_gb')
        if memory is None:
            # finished jobs leave the scanned store after two days (cq.archive_jobs): the estimate keeps their peaks
            history = all_jobs(ctx) + e._jobs_in(ctx, os.path.join('fleet', 'jobs') + e.ARCHIVE_SUFFIX, HISTORY_S)
            recommendations = [telemetry.estimate_memory(history, profile_key, command_key, platform, host, slots, 2)['memory_gb'] for host in candidates]
            memory = min(50.0, max(recommendations or [2]))
        cap = req.get('memory_max_gb')
        if cap is None:
            cap = min(50.0, math.ceil(memory * 1.25 * 4) / 4)
            # A where: host job turned into a fleet job never had a cap: it gets a roomier default than a fleet
            # job whose submitter left the cap out (queue_generations.host_job_for_fleet).
            roomy = req.get('_default_memory_max_gb')
            if _num(roomy, .25, 50):
                cap = max(cap, float(roomy))
        if cap < memory:
            _error('memory_max_gb must be at least memory_gb')
        job = dict(id=jid, fleet=True, status='queued', command=command, subdir=subdir, source=req.get('source') or 'worktree',
                   sha=sha, base=base, snapshot=snap, where='fleet', slots=slots, self_heavy=False,
                   est_minutes=est, timeout_minutes=timeout, priority=priority, label=(str(req.get('label') or 'fleet job')[:60]),
                   gets=gets, temp_storage='disk', sandbox=sandbox, network=network, defer=defer or 'immediate', requested_defer=defer,
                   deferred_registered=False, deferred_phase='staging_pending', memory_gb=float(memory), memory_max_gb=float(cap),
                   profile_key=profile_key, command_key=command_key, platform=platform, candidates=candidates, requires=requires,
                   artifact_policy=artifact_policy,
                   session=session, root=e.norm_path(root), top=e.norm_path(top), worktree_identity=identity, host_id=None,
                   workstream=os.path.basename(e.norm_path(top)), submitted=e.iso(now), t_submitted=now, notified=False, notify=True, attempts=0)
        with e.owner_lock(top), ctx.qlock():
            if e.load_job(ctx, jid) or identity != e.worktree_identity(top):
                _error('job collision or worktree changed before acceptance')
            current_owner = e.read_json(os.path.join(top, '.baton', 'owner.json'))
            if not isinstance(current_owner, dict) or current_owner.get('session_id') != session:
                _error('worktree owner changed before acceptance')
            legacy_ctx = _legacy_context(ctx)
            if legacy_ctx is ctx:
                queue_paths.initialize_default_lock(ctx.state)
            else:
                with legacy_ctx.qlock():
                    queue_paths.initialize_default_lock(legacy_ctx.state)
            receipt = e.create_deferred_receipt(top, identity, dict(version=1, id=jid, host_id='fleet', worktree_identity=identity,
                          root=e.norm_path(top), session=session, source_sha=sha, defer=defer or 'immediate', created=e.iso(now)))
            try:
                e.save_job(ctx, job)
            except Exception:
                if e.load_job(ctx, jid) != job:
                    raise
    except Exception:
        if receipt:
            e.remove_deferred_receipt(*receipt)
        if snap:
            e.drop_ref(top, jid)
        raise
    _event(ctx, 'submitted', job)
    warning = None
    try:
        ensure_controller(ctx)
    except Exception as exc:
        warning = 'accepted; controller could not start: ' + str(exc)[:200]
    sizing = _sizing_note(ctx, job, memory=req.get('memory_gb') is not None or req.get('memory_max_gb') is not None,
                          estimate=req.get('est_minutes') is not None)
    return {'ok': True, 'job': jid, 'note': 'fleet job accepted' + ('; ' + sizing if sizing else ''),
            **({'receipt_warning': warning} if warning else {})}


def _sizing_note(ctx, job, memory=True, estimate=True):
    """One sentence for the submitter when this command's own record shows it declares far more than it uses: at
    least three successful runs of the same command on the same platform in the same repository (store and 30 days
    of archive), and a memory cap of twice their largest peak plus 1 GB or more, or an estimate of twice their
    longest run plus 5 minutes or more. A peak counts file cache, which grows to any cap; when the cap is not twice
    the peak, but three runs carry what they held that could not be given back (peak_held_bytes from at least
    HELD_MIN_SAMPLES readings on the host) and the cap is four times that plus 2 GB or more, the note gives both
    figures of those runs. `memory` and `estimate` say which of the two the submitter stated: a default
    is not theirs to correct. A new job's cap is reserved in full and the estimate decides whether a job may pass a held
    host, so both cost the submitter waiting time. '' when there is nothing to say; never raises."""
    try:
        e = _engine()
        history = all_jobs(ctx) + e._jobs_in(ctx, os.path.join('fleet', 'jobs') + e.ARCHIVE_SUFFIX, HISTORY_S)
        peaks, minutes, helds = [], [], []
        for row in history:
            if (row.get('id') == job['id'] or row.get('profile_key') != job['profile_key'] or row.get('command') != job['command']
                    or row.get('subdir') != job['subdir'] or row.get('platform') != job['platform']
                    or row.get('status') != 'done' or row.get('exit') != 0):
                continue
            metrics = row.get('metrics') if isinstance(row.get('metrics'), dict) else {}
            peak, start, finish = metrics.get('peak_memory_bytes'), row.get('t_started'), row.get('t_finished')
            if _num(peak, 1) and _num(start, 1) and _num(finish, 1) and finish >= start:
                peaks.append(peak / GIB)
                minutes.append((finish - start) / 60)
                held = metrics.get('peak_held_bytes')
                if (type(held) is int and 0 <= held <= peak and type(metrics.get('held_samples')) is int
                        and metrics['held_samples'] >= HELD_MIN_SAMPLES):
                    helds.append((held / GIB, peak / GIB))
        if len(peaks) < 3:
            return ''
        peak, longest, parts = max(peaks), max(minutes), []
        tenth = lambda gb: f'{max(0.1, math.ceil(gb * 10) / 10):g}'
        if memory and job['memory_max_gb'] >= 2 * peak + 1:
            parts.append(f"it reserves {job['memory_max_gb']:g} GB and {len(peaks)} earlier successful runs of this command "
                         f"peaked at {tenth(peak)} GB at most")
        elif memory and len(helds) >= 3 and job['memory_max_gb'] >= 4 * max(h for h, _ in helds) + 2:
            # The peak fills the cap with file cache, which says nothing about need: say what the runs could not give
            # back. Both figures are of the runs that carry enough readings, not of every run counted above.
            parts.append(f"it reserves {job['memory_max_gb']:g} GB; {len(helds)} earlier successful runs of this command peaked "
                         f"at {tenth(max(p for _, p in helds))} GB with file cache, and held at most "
                         f"{tenth(max(h for h, _ in helds))} GB that could not be given back (read every 5 s, so a short "
                         f"spike can be missed)")
        if estimate and job['est_minutes'] >= 2 * longest + 5:
            parts.append(f"it is estimated at {job['est_minutes']:g} min and {len(peaks)} earlier successful runs took "
                         f"at most {max(1, math.ceil(longest))} min")
        if not parts:
            return ''
        return 'sizing: ' + '; '.join(parts) + '. A job that declares less starts sooner.'
    except Exception:
        return ''


def _laptop_config(ctx, probe):
    if not queue_paths.default_lock_confirmed(_legacy_state(ctx)):
        return None
    raw = _engine().read_json(os.path.join(_legacy_state(ctx), 'fleet', 'config.json')) or {}
    if not isinstance(raw, dict):
        return None
    cpus = probe.get('logical_cpus')
    if type(cpus) is not int or cpus < 1:
        return None
    slots = raw.get('laptop_slots', min(4, cpus // 4))
    ram_floor = raw.get('ram_floor_gb', 6)
    commit_floor = raw.get('commit_floor_gb', 6)
    enabled = raw.get('enabled', True)
    if type(slots) is not int or not 0 <= slots <= cpus or not _num(ram_floor, 0, 1024) or not _num(commit_floor, 0, 1024) or type(enabled) is not bool:
        return None
    return dict(slots=slots, ram_floor_gb=ram_floor, commit_floor_gb=commit_floor, enabled=enabled)


def _remote_probe(ctx, host, state, conf):
    if conf.get('_policy_error'):
        return None
    e = _engine()
    binding = dict(conf)
    # An explicit configured address, optionally with a pinned machine identity.
    address = e.host_address({}, conf)
    probe_job = {'host': address, 'host_config': binding}
    rc, out, _ = e.job_ssh(ctx, probe_job, 'python3 "$HOME/.local/share/baton/compute-deferred/deferred.py" fleet_probe\n', timeout=20)
    if rc:
        return None
    try:
        data = json.loads(out.strip().splitlines()[-1])
        policy = data['policy']
        mid = data['machine_id']
        if (not isinstance(data, dict) or data.get('version') != 1 or data.get('supported') is not True or
                data.get('safety_protocol') != 'shared-heavy-gate-v1' or
                not isinstance(mid, str) or not re.fullmatch(r'[0-9a-f]{32}', mid) or
                (conf.get('machine_id') and conf['machine_id'] != mid) or not isinstance(policy, dict) or
                type(policy.get('pool_slots')) is not int or not 1 <= policy['pool_slots'] <= 32 or
                not _num(policy.get('ram_floor_gb'), .01, 1024) or not _num(policy.get('load_cap'), .01, 1024) or
                any(not _num(data.get(k), 0) for k in ('load', 'available_bytes', 'total_bytes', 'active_fleet_slots', 'reserved_memory_bytes')) or
                type(data.get('logical_cpus')) is not int or data['logical_cpus'] < 1 or
                not isinstance(data.get('active_fleet'), list) or not isinstance(data.get('terminal_fleet'), list) or
                type(data.get('legacy_busy')) is not bool or type(data.get('ordinary_waiting')) is not bool):
            return None
        if conf.get('slots') and conf['slots'] != policy['pool_slots']:
            return None
        if conf.get('load_cap') and conf['load_cap'] != policy['load_cap']:
            return None
        if conf.get('ram_floor_gb') is not None and conf['ram_floor_gb'] != policy['ram_floor_gb']:
            return None
        version = data.get('resource_policy_version')
        if version is not None and version != queue_policy.RESOURCE_POLICY_VERSION:
            return None
        if version == queue_policy.RESOURCE_POLICY_VERSION and not all(
                key in policy for key in ('foreground_slots', 'timezone', 'window_start', 'window_end')):
            return None
        effective = queue_policy.policy_fields(policy, pool_slots=policy['pool_slots'], validate_zone=False)
        if version is None:
            if not queue_policy.policy_is_default(effective):
                return None
            # Old helpers cannot attest to any custom scheduling policy.
            configured = queue_policy.policy_fields(conf, pool_slots=policy['pool_slots'], validate_zone=False)
            if not queue_policy.policy_is_default(configured):
                return None
        for key, value in effective.items():
            if key in conf:
                configured = queue_policy.policy_fields(conf, pool_slots=policy['pool_slots'], validate_zone=False)
                if configured[key] != value:
                    return None
        policy.update(effective)
        if data['active_fleet_slots'] > policy['pool_slots'] - effective['foreground_slots']:
            return None
        binding.update(effective)
        binding.update(address=address, machine_id=mid, slots=policy['pool_slots'],
                       ram_floor_gb=policy['ram_floor_gb'], load_cap=policy['load_cap'], portable_runner=True)
        return dict(host=host, state=state, binding=binding, data=data, at=ctx.now())
    except (ValueError, KeyError, TypeError, IndexError, AttributeError):
        return None


def _probes(ctx):
    now = ctx.now()
    hosts = _hosts(ctx)
    # Pre-stage recovery stays pinned to the saved endpoint, even after removal.
    # These extra probes cannot make a removed host eligible for new work.
    for job in all_jobs(ctx):
        host = job.get('host_id')
        if (host and host != 'laptop' and host not in hosts and job.get('status') == 'launching'
                and not job.get('stage_attempted') and job.get('dispatch_state') and job.get('host_config')):
            hosts[host] = (job['dispatch_state'], job['host_config'])
    signature = _digest(hosts)
    cached = getattr(ctx, '_fleet_remote_probes', None)
    sampled = getattr(ctx, '_fleet_remote_sampled', None)
    if (cached is None or sampled is None or not 0 <= now - sampled < 30 or
            getattr(ctx, '_fleet_remote_signature', None) != signature):
        cached = {}
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix='fleet-probe') as pool:
            pending = {host: pool.submit(_remote_probe, ctx, host, state, conf)
                       for host, (state, conf) in hosts.items()}
            for host, future in pending.items():
                try:
                    cached[host] = future.result()
                except Exception:
                    cached[host] = None
        ctx._fleet_remote_probes = cached
        ctx._fleet_remote_sampled = ctx.now()
        ctx._fleet_remote_signature = signature
    probes = dict(cached)
    try:
        import windows_pool
        raw = windows_pool.probe()
        config = _laptop_config(ctx, raw) if isinstance(raw, dict) else None
        ready = all(hasattr(windows_pool, name) for name in ('process_identity', 'observe', 'read_claim', 'claim_path', 'boot_identity'))
        if (config and ready and raw.get('supported') is True and _boot_valid(raw.get('boot_identity')) and
                all(_num(raw.get(k), 0, 1e6) for k in ('available_gb', 'commit_free_gb', 'total_gb'))):
            probes['laptop'] = dict(host='laptop', data=raw, config=config, at=ctx.now())
    except Exception:
        probes['laptop'] = None
    return probes


def _pending(jobs, host, known):
    return [j for j in jobs if j.get('host_id') == host and j.get('status') in ('launching', 'running')
            and j.get('id') not in known
            # Once the host confirms its waiting queue, that host's atomic
            # broker owns admission. Overnight waiting is not occupied RAM.
            # Unknown staging outcomes still reserve their full declared cap.
            and not (host != 'laptop' and j.get('deferred_registered') is True
                     and j.get('deferred_phase') == 'queued')]


def _host_charge(data):
    """Bytes a remote host's active fleet jobs still have to be kept: their caps less what they already hold
    (held_memory_bytes: the memory on the anonymous lists of each running job's cgroup, which available_bytes has
    already lost; charging the whole cap as well counts it twice). A helper that does not report it, or reports
    more than the caps, is charged the caps in full."""
    reserved, held = data['reserved_memory_bytes'], data.get('held_memory_bytes')
    return reserved - held if type(held) is int and 0 <= held <= reserved else reserved


def _fit(ctx, job, probe, jobs, now, explain=False):
    def no(reason):
        return (None, reason) if explain else None

    def yes(result):
        return (result, None) if explain else result

    if not probe:
        return no('offline or unconfigured')
    if not _num(probe.get('at')) or not 0 <= now - probe['at'] <= 30:
        return no('stale capacity')
    host, data = probe['host'], probe['data']
    caps = data.get('capabilities')
    if job.get('requires') and (not isinstance(caps, list) or not set(job['requires']).issubset(set(x for x in caps if isinstance(x, str)))):
        return no('required tools unavailable or unverified')
    if host == 'laptop':
        cfg = probe['config']
        live = _pending(jobs, host, set())
        if (not cfg['enabled'] or job.get('requested_defer') or
                (not live and not getattr(getattr(ctx, '_fleet_local_pool', None), 'gate_held', False)
                 and ctx.laptop.heavy_lock_held())):
            return no('disabled, deferred, or legacy work active')
        slots_used = sum(j['slots'] for j in live)
        reserved = sum(j['memory_max_gb'] for j in live)
        if slots_used + job['slots'] > cfg['slots']:
            return no('CPU slots unavailable')
        if (data['available_gb'] - reserved - job['memory_max_gb'] < cfg['ram_floor_gb'] or
                data['commit_free_gb'] - reserved - job['memory_max_gb'] < cfg['commit_floor_gb']):
            return no('physical or commit RAM floor')
        return yes(dict(slots_total=cfg['slots'], slots_used=slots_used, reserved=reserved,
                        total_gb=data['total_gb'], available_gb=data['available_gb'], floor=cfg['ram_floor_gb']))
    policy = data['policy']
    total = policy['pool_slots'] - policy.get('foreground_slots', 2)
    active = data['active_fleet']
    if any(not isinstance(x, dict) or not isinstance(x.get('id'), str) for x in active):
        return no('invalid host reservation data')
    known = {x['id'] for x in active}
    pending = _pending(jobs, host, known)
    slots_used = data['active_fleet_slots'] + sum(j['slots'] for j in pending)
    reserved = _host_charge(data) / GIB + sum(j['memory_max_gb'] for j in pending)
    if job['slots'] > total:
        return no('CPU slots exceed host pool')
    if job['memory_max_gb'] + policy['ram_floor_gb'] > data['total_bytes'] / GIB:
        return no('RAM cap and floor exceed host memory')
    # Deferred work may bind despite a temporary load/window constraint; host
    # admission independently rechecks safety before executing.
    if not job.get('requested_defer'):
        if data['legacy_busy'] or data['ordinary_waiting']:
            return no('legacy work active or waiting')
        if slots_used + job['slots'] > total:
            return no('CPU slots unavailable')
        if data['load'] + job['slots'] * probe['binding'].get('slot_vcpu', 1.5) > policy['load_cap']:
            return no('host load cap')
        if data['available_bytes'] / GIB - reserved - job['memory_max_gb'] < policy['ram_floor_gb']:
            return no('RAM floor')
    return yes(dict(slots_total=total, slots_used=slots_used, reserved=reserved, total_gb=data['total_bytes'] / GIB,
                    available_gb=data['available_bytes'] / GIB, floor=policy['ram_floor_gb']))


def _score(job, host, fit, jobs):
    """Approximate completion work from CPU and memory, with a laptop UI penalty."""
    total = max(1, fit['slots_total'])
    mem = max(.25, fit['total_gb'] - fit['floor'])
    work = 0.0
    for other in jobs:
        if other.get('host_id') == host and other.get('status') in ('launching', 'running'):
            duration = max(1, other.get('est_minutes') or 10)
            work += duration * max(other['slots'] / total, other['memory_max_gb'] / mem)
    duration = max(1, job.get('est_minutes') or 10)
    work += duration * max(job['slots'] / total, job['memory_max_gb'] / mem)
    return work + (duration * .75 if host == 'laptop' else 0)


def _fits_idle(job, probe):
    """Could this compatible job fit the device once current work drains?"""
    if not probe:
        return False
    data = probe['data']
    caps = data.get('capabilities')
    if job.get('requires') and (not isinstance(caps, list) or not set(job['requires']).issubset(set(x for x in caps if isinstance(x, str)))):
        return False
    if probe['host'] == 'laptop':
        cfg = probe['config']
        return (cfg['enabled'] and not job.get('requested_defer') and job['slots'] <= cfg['slots'] and
                job['memory_max_gb'] + max(cfg['ram_floor_gb'], cfg['commit_floor_gb']) <= data['total_gb'])
    policy = data['policy']
    return (job['slots'] <= policy['pool_slots'] - policy.get('foreground_slots', 2) and
            job['memory_max_gb'] + policy['ram_floor_gb'] <= data['total_bytes'] / GIB and
            job['slots'] * probe['binding'].get('slot_vcpu', 1.5) <= policy['load_cap'])


def _size_delay(job):
    """Seconds a job stands back in its priority class for its size: SIZE_WEIGHT minutes per slot-minute of estimated
    work, counted up to SIZE_CAP_SLOT_MIN. A 3-minute one-slot check gives way by 90 s, a 90-minute five-slot verify by
    the full two hours."""
    est, slots = job.get('est_minutes'), job.get('slots')
    work = (est if _num(est, 0, 1e6) else 10) * (slots if type(slots) is int and slots > 0 else 1)
    return min(work, SIZE_CAP_SLOT_MIN) * 60 * SIZE_WEIGHT


def _order(job, now=None):
    """Placement order. A job that has waited AGE_S goes first, oldest first, whatever its class or size (given
    `now`; a deferred job waits by request and does not age): without that, steady verify and fixcheck work keeps
    lane work out for as long as it lasts. So an aged lane job can be the one that holds a host (_reserve) against
    fresh verify work, and after an hour without a controller (laptop asleep) the whole backlog is in submission
    order. The host's tick has a like rule for its registered jobs, from registration time and for deferred ones too.
    Among the rest: priority class first; within a class the smaller job first, by submission time plus
    _size_delay. The delay is fixed at submission, so a large job is overtaken only by smaller ones submitted within
    its delay, and no longer than until it has aged. Backfill (_reserve) is bounded separately."""
    submitted = job.get('t_submitted', 0)
    if now is not None and not job.get('requested_defer') and _num(submitted, 1) and now - submitted >= AGE_S:
        return (0, 0, submitted, job['id'])
    return (1, _engine().PRIO_RANK.get(job.get('priority'), 2), submitted + _size_delay(job), job['id'])


def _expected_end(job, now):
    """When a launching or running job is expected to leave its host: its start plus its estimate. One already past
    its estimate is given BACKFILL_OVERRUN of it again (BACKFILL_MIN_LEFT_S at least), and none is expected beyond
    its timeout."""
    start = next((job[k] for k in ('t_started', 't_launch') if _num(job.get(k), 1)), now)
    est = (job['est_minutes'] if _num(job.get('est_minutes'), .01, 1e6) else 10) * 60
    end = start + est
    if end <= now:
        end = now + max(BACKFILL_MIN_LEFT_S, BACKFILL_OVERRUN * est)
    if _num(job.get('timeout_minutes'), .01, 1e6):
        end = min(end, max(now + 60, start + job['timeout_minutes'] * 60))
    return end


def _end_is_firm(job, now):
    """Whether _expected_end(job, now) stays where it is as time passes: the start plus the estimate of a job still
    within it, or the job's timeout. The allowance given to a job past its estimate is counted from `now`, so it
    moves on with every look and promises nothing."""
    start = next((job[k] for k in ('t_started', 't_launch') if _num(job.get(k), 1)), None)
    if start is None:
        return False
    est = (job['est_minutes'] if _num(job.get('est_minutes'), .01, 1e6) else 10) * 60
    end = _expected_end(job, now)
    return ((start + est > now and end == start + est)
            or (_num(job.get('timeout_minutes'), .01, 1e6) and end == start + job['timeout_minutes'] * 60))


def _shadow(job, probe, jobs, now):
    """When a queued job that does not fit this remote host now could start there, if the fleet work on it ends as
    estimated (the "shadow time" of backfill scheduling). None when that cannot be told: the laptop, a host whose
    reservations include work this store does not know, a job that slots and memory would already admit (something
    else keeps it out), one that would not fit even with every known job gone, or one waiting for a job that is
    already past its estimate (_end_is_firm: a time that moves on with every look would let short jobs pass for as
    long as something overruns). Only slots and reserved memory are modelled: load, legacy work and the probe's age
    are _fit's business."""
    try:
        if not probe or probe['host'] == 'laptop':
            return None
        data, host = probe['data'], probe['host']
        policy = data['policy']
        active = data['active_fleet']
        known = {x['id'] for x in active}
        here = [j for j in jobs if j.get('host_id') == host and j.get('status') in ('launching', 'running')]
        if known - {j['id'] for j in here}:
            return None
        pending = _pending(jobs, host, known)
        slots = (policy['pool_slots'] - policy.get('foreground_slots', 2)
                 - data['active_fleet_slots'] - sum(j['slots'] for j in pending))
        memory = (data['available_bytes'] / GIB - _host_charge(data) / GIB
                  - sum(j['memory_max_gb'] for j in pending) - policy['ram_floor_gb'])
        if slots >= job['slots'] and memory >= job['memory_max_gb']:
            return None  # capacity is not what keeps it out
        counted = known | {j['id'] for j in pending}
        for other in sorted((j for j in here if j['id'] in counted), key=lambda j: _expected_end(j, now)):
            slots += other['slots']
            memory += other['memory_max_gb']  # its cap: the part still charged, and the part it held comes back as available
            if slots >= job['slots'] and memory >= job['memory_max_gb']:
                return _expected_end(other, now) if _end_is_firm(other, now) else None
        return None
    except (KeyError, TypeError, ValueError):
        return None


def _execution_command(job, host):
    original = job['command']
    if job.get('platform') != 'portable':
        return original
    replacement = shlex.quote(sys.executable.replace('\\', '/')) if host == 'laptop' else 'python3'
    return re.sub(r'^([ \t]*)python(?:3)?(?=[ \t\r\n]|$)',
                  lambda match: match.group(1) + replacement, original, count=1)


def _laptop_admission(ctx, fleet_jobs):
    """Persist a bounded fleet cohort and a post-drain legacy quiet gap.

    Caller holds the primary queue lock. Legacy jobs visible in that store stop
    new admission immediately; a cohort expiry also gives external waiters a
    chance even when their intent is invisible to the queue.
    """
    e = _engine()
    now = ctx.now()
    active = any(j.get('host_id') == 'laptop' and
                 (j.get('status') == 'running' or (j.get('status') == 'launching' and j.get('stage_attempted')))
                 for j in fleet_jobs)
    legacy = any(j.get('status') in ('queued', 'launching') and j.get('slots', 0) > 0 and
                 (j.get('placed_where') == 'laptop' or
                  (not j.get('placed_where') and j.get('where') in ('laptop', 'any')))
                 for j in (e.all_jobs(_legacy_context(ctx)) +
                           (e.all_jobs(ctx) if _legacy_state(ctx) != ctx.state else [])))
    path = ctx.p('fleet', 'laptop-cohort.json')
    state = e.read_json(path)
    if not isinstance(state, dict) or state.get('mode') not in ('open', 'draining', 'quiet'):
        # If prior live jobs exist but cohort history is unavailable, drain
        # first rather than reset their accumulated admission window.
        state = {'version': 1, 'mode': 'draining' if active else 'open', 'since': now}
    before = dict(state)
    since = state.get('since')
    if not _num(since) or now < since:
        state = {'version': 1, 'mode': 'draining', 'since': now}
    if legacy and state['mode'] != 'draining':
        state.update(mode='draining', since=now)
    elif state['mode'] == 'open' and now - state['since'] >= 30:
        state.update(mode='draining', since=now)
    if state['mode'] == 'draining' and not active and not legacy:
        state.update(mode='quiet', since=now)
    elif state['mode'] == 'quiet' and not legacy and now - state['since'] >= 2:
        state.update(mode='open', since=now)
    if state != before or not os.path.exists(path):
        e.write_json(path, state)
    admitted = state['mode'] == 'open' and not legacy
    pool = getattr(ctx, '_fleet_local_pool', None)
    if not admitted and pool is not None:
        pool.request_yield()
    return admitted


def _reserve(ctx, probes, limit=3):
    """Choose and durably pin at most one new stage per device this pass."""
    e = _engine()
    chosen = []
    with ctx.qlock():
        jobs = all_jobs(ctx)
        laptop_open = _laptop_admission(ctx, jobs)
        eligible_hosts = set(_hosts(ctx)) | {'laptop'}
        # Backfill. The first job in order that a host cannot take now, but could take idle, holds that host: `holds`
        # maps the host to (that job's shadow time, when it began to hold). A later job may still start there when
        # its own estimate ends before the shadow time (_shadow), so by the estimates it is gone before the held job
        # could start. A shadow time is a fixed moment (_end_is_firm), and a job passes only if it ends by then, so
        # passing jobs cannot push it back. With no shadow time nothing shows that a passing job leaves in time, and
        # a stream of short jobs could keep the held job out for good: then only a job estimated within
        # BACKFILL_UNKNOWN_S may pass, and only for HOLD_PATIENCE_S after the hold began.
        holds = {}
        start = ctx.now()
        # A prior reservation can still be staging in another process after a
        # controller handoff. Do not start a second launcher for that device.
        inflight = {j.get('host_id') for j in jobs if j.get('status') == 'launching'
                    and (j.get('launch_uncertain') or not j.get('stage_attempted'))}
        for job in sorted((j for j in jobs if j.get('status') == 'queued'), key=lambda j: _order(j, start)):
            if len(chosen) >= limit:
                break
            options = []
            reasons = {}
            temporarily_blocked = set()
            for host in job.get('candidates', []):
                if host not in eligible_hosts:
                    reasons[host] = 'offline or unconfigured'
                    continue
                if host != 'laptop' and probes.get(host):
                    try:  # the host's setting as probed now, which may differ from the one the candidates were filtered by
                        e.sandbox_plan(job, probes[host]['binding'])
                    except e.SandboxError as exc:
                        reasons[host] = str(exc)
                        continue
                if host == 'laptop' and not laptop_open:
                    reasons[host] = 'draining for legacy laptop work'
                    continue
                if host in holds:
                    shadow, held_since = holds[host]
                    est = (job['est_minutes'] if _num(job.get('est_minutes'), .01, 1e6) else 10) * 60
                    if shadow is not None:
                        passes = start + est <= shadow
                    else:
                        passes = est <= BACKFILL_UNKNOWN_S and 0 <= start - held_since <= HOLD_PATIENCE_S
                    if job.get('requested_defer') or not passes:
                        reasons[host] = 'reserved for an older eligible job'
                        continue
                if host in inflight or host in {x['host_id'] for x in chosen}:
                    reasons[host] = 'another launcher is in flight'
                    if host in inflight and _fits_idle(job, probes.get(host)):
                        temporarily_blocked.add(host)
                    continue
                fit, reason = _fit(ctx, job, probes.get(host), jobs, ctx.now(), explain=True)
                if fit:
                    options.append((_score(job, host, fit, jobs), host, fit))
                else:
                    reasons[host] = reason
                    if reason in ('CPU slots unavailable', 'physical or commit RAM floor', 'RAM floor',
                                  'legacy work active or waiting', 'disabled, deferred, or legacy work active',
                                  'host load cap', 'stale capacity') and _fits_idle(job, probes.get(host)):
                        temporarily_blocked.add(host)
            if not options:
                changed = False
                for host in temporarily_blocked:
                    if host not in holds:
                        if not _num(job.get('hold_since'), 1):
                            job['hold_since'] = start  # kept until the job is placed: its patience does not restart
                            changed = True
                        holds[host] = (_shadow(job, probes.get(host), jobs, start), job['hold_since'])
                if changed or job.get('placement_reasons') != reasons:
                    job['placement_reasons'] = reasons
                    job['reason'] = 'waiting for eligible fleet capacity'
                    e.save_job(ctx, job)
                continue
            _, host, fit = min(options)
            probe = probes[host]
            now = ctx.now()
            update = dict(status='launching', host_id=host, placed_where='laptop' if host == 'laptop' else 'host',
                          t_launch=now, t_touch=now, launch_uncertain=True, attempts=1,
                          reason='staging outcome pending confirmation', deferred_phase='staging', placement_reasons={},
                          execution_command=_execution_command(job, host))
            if host != 'laptop':
                update.update(host=probe['binding']['address'], host_config=dict(probe['binding']),
                              deferred_load_cap=probe['data']['policy']['load_cap'], dispatch_state=probe['state'])
            else:
                update.update(laptop_config=dict(probe['config']), laptop_pending_gb=fit['reserved'] + job['memory_max_gb'])
            job.update(update)
            job.pop('hold_since', None)
            e.save_job(ctx, job)
            _event(ctx, 'reserved', job)
            chosen.append(job)
    return chosen


def _stage_remote(ctx, job):
    e = _engine()
    jid = job['id']
    shard = e.Ctx(state=job['dispatch_state'], now=ctx.now, ssh=ctx.ssh, run_remoterun=ctx.run_remoterun,
                  spawn=ctx.spawn, laptop=ctx.laptop, sync=ctx.sync)
    try:
        with e.FileLock(e.dispatch_lock_path(shard, job['top']), 540):
            with ctx.qlock():
                current = e.load_job(ctx, jid)
                if (not current or current.get('status') != 'launching' or current.get('cancel_requested')
                        or current.get('deferred_registered')):  # registered while this worker waited for the lock
                    return
                if e.worktree_identity(current['top']) != current['worktree_identity']:
                    raise RuntimeError('submitting worktree identity changed')
                # the binding was copied from a probe when the host was reserved: a setting made stricter since then applies
                binding = dict(current['host_config'])
                binding['sandbox'] = e.stricter_sandbox(binding.get('sandbox', 'off'), e.host_conf(current['dispatch_state']).get('sandbox', 'off'))
                script = e.make_launcher(dict(current, host_config=binding,
                                              command=current.get('execution_command', current['command'])))
                current['stage_attempted'] = True
                current['stage_at'] = ctx.now()  # _restage_if_absent measures the attempt's age from here
                current.pop('restage_due', None)
                e.save_job(ctx, current)
            encoded = base64.b64encode(script.encode()).decode()
            argv = ['--rev', current['sha'], '--subdir', current['subdir'], '--', 'bash', '-c',
                    f'echo {encoded} | base64 -d | bash']
            if ctx.run_remoterun is e.real_run_remoterun:
                rc, out, err = e.real_run_remoterun(argv, current['top'], host=current['host'], binding=current['host_config'])
            else:
                rc, out, err = ctx.run_remoterun(argv, current['top'])
        if f'CQ_NO_JOBBOX {jid}' in out:  # the launcher stopped before it made anything on the host
            final = e.finish(ctx, jid, 'failed', None, reason=e.JOBBOX_MISSING, tail=[])
            if final:
                _event(ctx, 'finished', final)
            return
        registered = f'CQ_REGISTERED {jid}' in out
        with ctx.qlock():
            current = e.load_job(ctx, jid)
            if current and current.get('status') == 'launching':
                if registered:
                    current.update(deferred_registered=True, deferred_phase='queued', launch_uncertain=False,
                                   host_dir=f'~/q/{jid}', reason='registered on pinned host')
                else:
                    current.update(launch_uncertain=True, deferred_phase='registration_unconfirmed',
                                   reason=f'staging outcome unconfirmed (exit {rc}); reconcile original host')
                e.save_job(ctx, current)
                _event(ctx, 'registered' if registered else 'uncertain', current)
    except e.SandboxError as exc:  # the host's setting no longer takes this job's request: nothing was staged
        final = e.finish(ctx, jid, 'failed', None, reason=str(exc), tail=[])
        if final:
            _event(ctx, 'finished', final)
    except Exception as exc:
        _stage_error(ctx, jid, exc)


def _stage_error(ctx, jid, exc):
    e = _engine()
    with ctx.qlock():
        current = e.load_job(ctx, jid)
        if not current or current.get('status') != 'launching':
            return
        if current.get('stage_attempted') and not current.get('restage_due'):
            current.update(launch_uncertain=True, reason='staging unconfirmed: ' + type(exc).__name__)
        else:
            attempts = int(current.get('preparation_failures', 0)) + 1
            current.update(preparation_failures=attempts, preparation_blocked=attempts >= 5,
                           stage_resume_after=ctx.now() + min(300, 30 * 2 ** min(attempts - 1, 4)),
                           reason='pinned preparation failed: ' + type(exc).__name__)
        e.save_job(ctx, current)


def _external_pending_gb(jobs, current_id, pool):
    """Charge pending local jobs only if the pool does not already hold them."""
    held = pool.reservations()
    if not isinstance(held, dict):
        raise RuntimeError('local pool reservations unavailable')
    total = 0.0
    for job in _pending(jobs, 'laptop', set()):
        if job.get('id') == current_id:
            continue
        cap = job.get('memory_max_gb')
        if not _num(cap, .25, 50):
            raise RuntimeError('invalid local pending reservation')
        amount = held.get(job['id'])
        if not _num(amount, cap, 50):
            total += cap
    return total


def _stage_laptop(ctx, job):
    e = _engine()
    jid = job['id']
    try:
        import windows_pool
        # Without process creation identity, a controller crash between spawn
        # and receipt cannot safely distinguish a live launcher from PID reuse.
        if not all(hasattr(windows_pool, name) for name in ('process_identity', 'observe', 'read_claim', 'claim_path', 'boot_identity')):
            raise RuntimeError('Windows worker identity support unavailable')
        boot = windows_pool.boot_identity()
        if not _boot_valid(boot):
            raise RuntimeError('Windows boot identity unavailable')
        if e.worktree_identity(job['top']) != job['worktree_identity']:
            raise RuntimeError('submitting worktree identity changed')
        descriptor_path = job.get('prepared_descriptor') if job.get('prepared_boot') == boot else None
        if descriptor_path:
            descriptor = e.read_json(descriptor_path)
            if (not isinstance(descriptor, dict) or _descriptor_digest(descriptor) != job.get('prepared_hash') or
                    descriptor.get('id') != jid or descriptor.get('boot_identity') != boot):
                raise RuntimeError('prepared descriptor changed')
            src = job['prepared_src']
        else:
            run = ctx.p('fleet', 'run', jid, 'prep-' + secrets.token_hex(8))
            src = os.path.join(run, 'src')
            os.makedirs(run, exist_ok=True)
            skipped = e.checkout_tree(job['top'], job['sha'], src)
            if skipped:
                raise RuntimeError('source checkout incomplete')
            sub = os.path.realpath(os.path.join(src, *job['subdir'].split('/')))
            if os.path.commonpath([os.path.realpath(src), sub]) != os.path.realpath(src) or not os.path.isdir(sub):
                raise RuntimeError('invalid or missing source subdirectory')
            command_path = os.path.join(run, 'cmd.sh')
            e.atomic_write(command_path, job.get('execution_command', job['command']).replace('\r\n', '\n').encode())
            descriptor_path = os.path.join(run, 'descriptor.json')
            descriptor = dict(version=1, id=jid, nonce=secrets.token_hex(16), boot_identity=boot,
                              argv=[e.bash_exe(), command_path], cwd=sub,
                              env=dict(CQ_JOB=jid, CQ_SHA=job['sha'], CQ_BASE=job['base'], CQ_SRC=src,
                                       GIT_CEILING_DIRECTORIES=run.replace('\\', '/')),
                              log_path=os.path.join(run, 'run.log'), timeout_seconds=job['timeout_minutes'] * 60,
                              cpu_slots=job['slots'], memory_max_gb=job['memory_max_gb'],
                              legacy_lock_path=queue_paths.runtime_paths(_legacy_state(ctx))[2],
                              result_path=os.path.join(run, 'result.json'), cancel_path=os.path.join(run, 'cancel'),
                              ram_floor_gb=max(job['laptop_config']['ram_floor_gb'], job['laptop_config']['commit_floor_gb']),
                              pending_reserved_gb=job['laptop_pending_gb'])
            e.write_json(descriptor_path, descriptor)
        probe_data = windows_pool.probe()
        with ctx.qlock():
            current = e.load_job(ctx, jid)
            if (not current or current.get('status') != 'launching' or current.get('cancel_requested')
                    or current.get('stage_attempted')):
                return
            if e.worktree_identity(current['top']) != current['worktree_identity']:
                raise RuntimeError('submitting worktree identity changed')
            current.update(prepared_descriptor=descriptor_path, prepared_src=src, prepared_boot=boot,
                           prepared_hash=_descriptor_digest(descriptor))
            jobs = all_jobs(ctx)
            config = _laptop_config(ctx, probe_data) if isinstance(probe_data, dict) else None
            fresh = (config and probe_data.get('supported') is True and probe_data.get('boot_identity') == boot and
                     all(_num(probe_data.get(k), 0, 1e6) for k in ('available_gb', 'commit_free_gb', 'total_gb')))
            probe = dict(host='laptop', data=probe_data, config=config, at=ctx.now()) if fresh else None
            fit = _fit(ctx, current, probe, [j for j in jobs if j['id'] != jid], ctx.now()) if probe else None
            if (not _laptop_admission(ctx, jobs) or not fit or
                    descriptor.get('legacy_lock_path') != queue_paths.runtime_paths(_legacy_state(ctx))[2]):
                current.update(stage_resume_after=ctx.now()+5, reason='pinned laptop preparation waits for safe admission')
                e.save_job(ctx, current)
                return
            # The published preparation is immutable. If this process dies
            # after writing the final descriptor but before saving the job,
            # recovery can still verify and reuse the original preparation.
            nonce = secrets.token_hex(16)
            launch_dir = os.path.join(os.path.dirname(descriptor_path), 'launch-' + nonce)
            launch_path = os.path.join(launch_dir, 'descriptor.json')
            pending_gb = fit['reserved'] + current['memory_max_gb']
            if getattr(ctx, 'generation', 1) == 2:
                pool = getattr(ctx, '_fleet_local_pool', None)
                if pool is None:
                    current.update(stage_resume_after=ctx.now()+5, reason='local pool unavailable')
                    e.save_job(ctx, current)
                    return
                pending_gb = _external_pending_gb(jobs, jid, pool)
            launch_descriptor = dict(descriptor, nonce=nonce, log_path=os.path.join(launch_dir, 'run.log'),
                                     result_path=os.path.join(launch_dir, 'result.json'),
                                     cancel_path=os.path.join(launch_dir, 'cancel'),
                                     ram_floor_gb=max(config['ram_floor_gb'], config['commit_floor_gb']),
                                     pending_reserved_gb=pending_gb)
            e.write_json(launch_path, launch_descriptor)
            current.update(stage_attempted=True, descriptor_path=launch_path, local_src=src,
                           log_path=launch_descriptor['log_path'], worker_nonce=nonce,
                           descriptor_hash=_descriptor_digest(launch_descriptor), boot_identity=boot,
                           worker_claim_protocol='v2', t_stage_attempt=ctx.now(), last_laptop_spawn=ctx.now(),
                           laptop_spawn_attempts=1)
            e.save_job(ctx, current)
            if getattr(ctx, 'generation', 1) == 2:
                import windows_broker
                pool = getattr(ctx, '_fleet_local_pool', None)
                if pool is None:
                    current.update(stage_attempted=False, stage_resume_after=ctx.now()+5,
                                   reason='local pool unavailable')
                    e.save_job(ctx, current)
                    return
                try:
                    receipt = pool.submit(launch_path)
                except windows_broker.BusyError:
                    # BusyError guarantees the descriptor was not claimed.
                    current.update(stage_attempted=False, stage_resume_after=ctx.now()+5,
                                   reason='local pool admission pending')
                    e.save_job(ctx, current)
                    return
                current.update(launcher_pid=receipt['pid'], launcher_start_identity=receipt['process_start_identity'],
                               reason='Windows worker confirmation pending')
            else:
                process = e.spawn_detached([sys.executable, os.path.join(os.path.dirname(__file__), 'windows_pool.py'), launch_path])
                identity = windows_pool.process_identity(process.pid)
                current.update(launcher_pid=process.pid, launcher_start_identity=identity,
                               reason='Windows worker confirmation pending')
            e.save_job(ctx, current)
    except Exception as exc:
        _stage_error(ctx, jid, exc)


def _run_facts(facts):
    def number(value):
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None
    return dict(started=number(facts.get('started')), launched=number(facts.get('launched')),
                finished=number(facts.get('finished')), exit=number(facts.get('exit') if facts.get('exit') is not None else facts.get('exit_code')),
                alive=facts.get('phase') == 'running', cpu=None)


def _remote_reconcile(ctx, job, fact=None, explicit=False, restage=False):
    e = _engine()
    state = None
    if explicit or job.get('cancel_requested'):
        state = e.deferred_control(ctx, job, 'cancel' if job.get('cancel_requested') else 'status')
        # Terminal exit evidence is separate from the deferred service status.
        ent = _run_facts(fact) if fact else None
        if state.get('phase') in ('done', 'failed') and ent is None:
            snap, _ = e.host_snapshot(ctx, host=job['host'], binding=job['host_config'])
            ent = ((snap or {}).get('jobs') or {}).get(job['id'])
        _remote_metrics(ctx, job, state)
        e.reap_deferred(ctx, job, ent, ctx.now(), [], allow_registration=False, state=state)
        if restage:  # only the controller's pass: it alone knows that no stage worker of its own holds this job
            _restage_if_absent(ctx, job, state)
    elif fact:
        phase = fact.get('phase')
        state = dict(version=1, id=job['id'], phase=phase, registered=True, reason=fact.get('reason') or '')
        _remote_metrics(ctx, job, fact)
        e.reap_deferred(ctx, job, _run_facts(fact), ctx.now(), [], allow_registration=False, state=state)
    final = e.load_job(ctx, job['id'])
    if final and final.get('status') in TERMINAL:
        _event(ctx, 'finished', final)
    if state and state.get('error'):
        return {'ok': False, 'cached': True, 'error': state['error']}
    return {'ok': state is not None, 'cached': state is None, 'phase': state.get('phase') if state else None}


def _restage_wanted(job, now):
    """An unconfirmed, unregistered staging attempt old enough that its push has ended. The attempt's age runs from
    stage_at, which _stage_remote sets once it holds the dispatch lock; the push that follows is given 540 s, and a
    live controller's worker holds the job until it returns. A push orphaned by a controller that died is not bounded:
    it can overlap a restage, and the host's refusal to make an existing job directory is what keeps the job from
    running twice. A job reserved by an older engine has no stage_at: its reservation time stands in, plus the 540 s
    the attempt may have waited for that lock, plus a minute."""
    since = job.get('stage_at')
    if not _num(since):
        since = job.get('t_touch') + 600 if _num(job.get('t_touch')) else now
    return bool(job.get('status') == 'launching' and job.get('launch_uncertain') and job.get('stage_attempted')
                and not job.get('restage_due') and not job.get('cancel_requested')
                and not job.get('deferred_registered') and now - since >= RESTAGE_AFTER_S)


def _restage_if_absent(ctx, job, state):
    """Stage a job again on the host it is pinned to when its one staging attempt was never confirmed and left
    nothing there. Without this the job stays `launching` for good and, as a launcher in flight, keeps every other
    job off that host. Only from the controller's own pass (which skips a job one of its stage workers still holds),
    only when the host answers that no directory exists for the job (a partial or complete one is left to the
    ordinary reconcile), and only when _restage_wanted. The job keeps its host and stays `stage_attempted`, so a
    cancel still asks the host; `restage_due` lets _resume_pending take it, and each further attempt waits longer.
    True when the job was handed back."""
    e = _engine()
    now = ctx.now()
    if not isinstance(state, dict) or state.get('phase') != 'absent' or not _restage_wanted(job, now):
        return False
    jid = shlex.quote(job['id'])
    rc, out, _ = e.job_ssh(ctx, job, f'd="$HOME/q/"{jid}; if [ -e "$d" ] || [ -L "$d" ]; then echo present; else echo absent; fi\n')
    if rc != 0 or out.strip() != 'absent':
        return False
    with ctx.qlock():
        current = e.load_job(ctx, job['id'])
        # the attempt this answer was about must still be the last one: another may have started meanwhile
        if (not current or not _restage_wanted(current, now) or current.get('host_id') != job.get('host_id')
                or current.get('stage_at') != job.get('stage_at')):
            return False
        attempt = int(current.get('restages', 0)) + 1
        current.update(restage_due=True, restages=attempt, deferred_phase='staging',
                       stage_resume_after=now + min(1800, 60 * 2 ** min(attempt - 1, 5)),
                       reason='host has no staged copy; staging again on the pinned host')
        e.save_job(ctx, current)
        _event(ctx, 'restage', current)
    return True


def _remote_metrics(ctx, job, state):
    peak = state.get('memory_peak_bytes')
    if not _num(peak, 1):
        return
    metrics = dict(peak_memory_bytes=peak, memory_kind='linux-cgroup')
    # What the job held that could not be given back, as the host sampled it while the job ran (the cgroup's
    # anonymous lists). The peak above counts file cache too, which grows to any cap.
    held, samples = state.get('memory_held_peak_bytes'), state.get('memory_held_samples')
    if type(held) is int and 0 <= held <= peak and type(samples) is int and samples > 0:
        metrics.update(peak_held_bytes=held, held_samples=samples)
    with ctx.qlock():
        current = _engine().load_job(ctx, job['id'])
        if current and current.get('status') in ACTIVE:
            current['metrics'] = metrics
            _engine().save_job(ctx, current)


def _regular_inside(src, relative, max_size=8 * 1024 * 1024):
    import stat
    root = os.path.realpath(src)
    path = root
    try:
        for component in relative.split('/'):
            path = os.path.join(path, component)
            st = os.lstat(path)
            if stat.S_ISLNK(st.st_mode) or getattr(st, 'st_file_attributes', 0) & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0x400):
                return None
        if not stat.S_ISREG(st.st_mode) or (max_size is not None and st.st_size > max_size):
            return None
        if os.path.commonpath([root, os.path.realpath(path)]) != root:
            return None
        return path
    except (OSError, ValueError):
        return None


def _local_tail(run):
    """Read at most 64 KiB of the regular log in this run directory."""
    import stat
    path = _regular_inside(run, 'run.log', max_size=None)
    if path is None:
        return []
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as f:
            info = os.fstat(f.fileno())
            if not stat.S_ISREG(info.st_mode):
                return []
            f.seek(max(0, info.st_size - 65536))
            lines = f.read(65536).decode('utf-8', 'replace').splitlines()
        return _engine().cut_lines(lines)
    except OSError:
        return []


def _claim_or_restart(ctx, job, worker):
    """Adopt the winning worker, or retry the identical claim-guarded descriptor."""
    e = _engine()
    desc = job.get('descriptor_path')
    if (not desc or not job.get('worker_nonce') or
            not all(hasattr(worker, name) for name in ('read_claim', 'claim_path', 'process_identity'))):
        return None
    if _regular_inside(os.path.dirname(desc), os.path.basename(desc)) is None:
        return None
    descriptor = e.read_json(desc)
    if (not isinstance(descriptor, dict) or descriptor.get('id') != job['id'] or
            descriptor.get('nonce') != job['worker_nonce'] or not job.get('descriptor_hash')):
        return None
    try:
        if _descriptor_digest(descriptor) != job['descriptor_hash']:
            return None
    except (TypeError, ValueError):
        return None
    claim_file = worker.claim_path(desc)
    if os.path.realpath(claim_file) != os.path.realpath(os.path.join(os.path.dirname(desc), 'worker-claim.json')):
        return None
    reboot_job = None
    with ctx.qlock():
        current = e.load_job(ctx, job['id'])
        if (not current or current.get('status') not in ('launching', 'running') or
                current.get('descriptor_path') != desc or current.get('worker_nonce') != job['worker_nonce'] or
                current.get('descriptor_hash') != job['descriptor_hash']):
            return None
        claim = worker.read_claim(desc)
        if isinstance(claim, dict):
            pid, identity = claim.get('worker_pid'), claim.get('worker_start_filetime')
            if (claim.get('id') != current['id'] or claim.get('nonce') != current['worker_nonce'] or
                    claim.get('descriptor_hash') != current['descriptor_hash'] or
                    type(pid) is not int or pid <= 0 or type(identity) is not int or identity <= 0 or
                    (current.get('worker_pid') and (current['worker_pid'], current.get('worker_start_identity')) != (pid, identity))):
                return None
            current.update(worker_pid=pid, worker_start_identity=identity, launch_uncertain=False,
                           reason='Windows worker claim confirmed')
            e.save_job(ctx, current)
            return current
        boot = current.get('boot_identity')
        if (current.get('worker_claim_protocol') != 'v2' or not _boot_valid(boot) or
                descriptor.get('boot_identity') != boot or not hasattr(worker, 'boot_identity')):
            return None
        try:
            current_boot = worker.boot_identity()
        except (OSError, ValueError):
            return None
        if not _boot_valid(current_boot):
            return None
        if current_boot != boot:
            current['reason'] = 'Windows rebooted before worker outcome was confirmed'
            e.save_job(ctx, current)
            reboot_job = current
        else:
            try:
                os.lstat(claim_file)
                return None  # existing malformed, partial or unreadable claim is uncertain
            except FileNotFoundError:
                pass
            except OSError:
                return None
            attempts = current.get('laptop_spawn_attempts', 1)
            attempts = attempts if type(attempts) is int and attempts > 0 else 1
            delay = min(300, 30 * 2 ** min(attempts - 1, 4))
            last = current.get('last_laptop_spawn') or current.get('t_stage_attempt') or ctx.now()
            if not _num(last) or ctx.now() - last < delay:
                return None
            current.update(last_laptop_spawn=ctx.now(), laptop_spawn_attempts=attempts + 1,
                           reason='waiting for Windows worker claim')
            e.save_job(ctx, current)
            try:
                if getattr(ctx, 'generation', 1) == 2:
                    import windows_broker
                    pool = getattr(ctx, '_fleet_local_pool', None)
                    if pool is None:
                        return None
                    receipt = pool.submit(desc)
                    current.update(launcher_pid=receipt['pid'], launcher_start_identity=receipt['process_start_identity'])
                else:
                    process = e.spawn_detached([sys.executable, os.path.join(os.path.dirname(__file__), 'windows_pool.py'), desc])
                    current.update(launcher_pid=process.pid, launcher_start_identity=worker.process_identity(process.pid))
                e.save_job(ctx, current)
            except (OSError, RuntimeError):
                pass  # claim-guarded descriptor can be retried after backoff
    if reboot_job:
        missing = [dict(path=rel, reason='Windows rebooted before artifact capture') for rel in reboot_job.get('gets') or []]
        final = e.finish(ctx, reboot_job['id'], 'cancelled' if reboot_job.get('cancel_requested') else 'failed', None,
                         reason=reboot_job['reason'], tail=_local_tail(os.path.dirname(desc)), artifact_missing=missing)
        if final:
            _event(ctx, 'finished', final)
    return None


def _local_reconcile(ctx, job):
    e = _engine()
    import windows_pool
    if job.get('worker_claim_protocol') in ('v1', 'v2'):
        job = _claim_or_restart(ctx, job, windows_pool)
        if not job:
            return
    desc_path = job.get('descriptor_path')
    if not desc_path or not job.get('worker_pid') or not job.get('worker_start_identity') or not hasattr(windows_pool, 'observe'):
        return
    pool = getattr(ctx, '_fleet_local_pool', None) if getattr(ctx, 'generation', 1) == 2 else None
    observer = pool if pool is not None else windows_pool
    obs = observer.observe(desc_path, job['worker_pid'], job['worker_start_identity'])
    if not isinstance(obs, dict):
        return
    result = e.read_json(os.path.join(os.path.dirname(desc_path), 'result.json'))
    if isinstance(result, dict):
        if (result.get('worker_pid') != job['worker_pid'] or
                result.get('worker_start_filetime') != job['worker_start_identity']):
            return
    elif obs.get('empty_confirmed') is True:
        result = dict(phase='failed', active_processes=0, exit=None,
                      reason='worker exited without a result after containment became empty')
    else:
        return
    phase = result.get('phase')
    if phase in ('waiting', 'running') and obs.get('empty_confirmed') is True:
        result = dict(result, phase='failed', active_processes=0, exit=None,
                      reason='worker exited before writing its terminal result')
        phase = 'failed'
    if phase in ('waiting', 'running'):
        with ctx.qlock():
            current = e.load_job(ctx, job['id'])
            if current and current.get('status') in ('launching', 'running'):
                if phase == 'running':
                    current.update(status='running', t_started=result.get('started') or ctx.now(), launch_uncertain=False)
                current['reason'] = 'Windows worker ' + phase
                e.save_job(ctx, current)
        return
    if phase not in TERMINAL or result.get('active_processes') != 0 or obs.get('empty_confirmed') is not True:
        return
    # Only the owning live pool can attest that its exact worker thread returned.
    # Persist that proof before publication so a separate refresh can retry it.
    proof = obs.get('pool_completion_proof') if pool is not None else None
    if isinstance(proof, dict):
        with ctx.qlock():
            current = e.load_job(ctx, job['id'])
            if (current and current.get('status') in ACTIVE and
                    current.get('descriptor_path') == desc_path and
                    current.get('worker_nonce') == job.get('worker_nonce') == proof.get('nonce') and
                    current.get('descriptor_hash') == job.get('descriptor_hash') == proof.get('descriptor_hash') and
                    proof.get('id') == current['id'] and proof.get('version') == 1 and
                    (proof.get('worker_pid'), proof.get('worker_start_identity')) ==
                    (current.get('worker_pid'), current.get('worker_start_identity')) and
                    proof.get('claim_fingerprint') is not None and
                    windows_pool.claim_fingerprint(desc_path) == proof['claim_fingerprint']):
                current['pool_completion_proof'] = proof
                e.save_job(ctx, current)
    try:
        paths, digests, missing = e.capture_local_job(ctx, job, job['local_src'])
    except (OSError, ValueError):
        return  # recover the durable publication journal on the next reconciliation
    peak = result.get('peak_private_bytes')
    with ctx.qlock():
        current = e.load_job(ctx, job['id'])
        if current and current.get('status') in ACTIVE and _num(peak, 1):
            current['metrics'] = dict(peak_memory_bytes=peak, memory_kind='windows-job-private')
            e.save_job(ctx, current)
    tail = _local_tail(os.path.dirname(desc_path))
    final = e.finish(ctx, job['id'], phase, result.get('exit'), reason=result.get('reason') or '', tail=tail, out_paths=paths,
             started=result.get('started'), finished=result.get('finished'), artifact_missing=missing, artifact_digests=digests)
    if final:
        _event(ctx, 'finished', final)


def _recover_local(ctx, job):
    """Retry only missing artifacts, after confirming the original worker is empty."""
    e = _engine()
    import windows_pool
    desc = job.get('descriptor_path')
    if not desc or not job.get('worker_pid') or not job.get('worker_start_identity'):
        return {'ok': False, 'cached': True, 'error': 'worker identity unavailable'}
    with e.FileLock(ctx.p('locks', 'fleet-result-' + job['id'] + '.lock'), 60):
        current = _get(ctx, job['id'])
        if (not current or current.get('descriptor_path') != desc or
                (current.get('worker_pid'), current.get('worker_start_identity')) !=
                (job.get('worker_pid'), job.get('worker_start_identity'))):
            return {'ok': False, 'cached': True, 'error': 'worker identity changed'}
        obs = windows_pool.observe(desc, current['worker_pid'], current['worker_start_identity'])
        if not isinstance(obs, dict):
            return {'ok': False, 'cached': True, 'error': 'worker shutdown not confirmed'}
        if obs.get('empty_confirmed') is not True:
            proof = current.get('pool_completion_proof')
            descriptor = e.read_json(desc)
            claim = windows_pool.read_claim(desc) if hasattr(windows_pool, 'read_claim') else None
            fingerprint = windows_pool.claim_fingerprint(desc) if hasattr(windows_pool, 'claim_fingerprint') else None
            try:
                valid = (current.get('status') in TERMINAL and isinstance(proof, dict) and
                         proof.get('version') == 1 and proof.get('id') == current['id'] and
                         proof.get('nonce') == current.get('worker_nonce') and
                         proof.get('descriptor_hash') == current.get('descriptor_hash') and
                         isinstance(descriptor, dict) and _descriptor_digest(descriptor) == proof['descriptor_hash'] and
                         isinstance(claim, dict) and claim.get('id') == current['id'] and
                         claim.get('nonce') == proof['nonce'] and claim.get('descriptor_hash') == proof['descriptor_hash'] and
                         (claim.get('worker_pid'), claim.get('worker_start_filetime')) ==
                         (current['worker_pid'], current['worker_start_identity']) ==
                         (proof.get('worker_pid'), proof.get('worker_start_identity')) and
                         fingerprint is not None and fingerprint == proof.get('claim_fingerprint') and
                         (obs.get('job_exists') is False or
                          (obs.get('job_exists') is True and obs.get('active_processes') == 0)))
            except (KeyError, TypeError, ValueError):
                valid = False
            if not valid:
                return {'ok': False, 'cached': True, 'error': 'worker shutdown not confirmed'}
        try:
            paths, digests, missing = e.capture_local_job(ctx, current, current['local_src'])
        except (OSError, ValueError) as exc:
            return {'ok': False, 'cached': True, 'error': str(exc)}
        with ctx.qlock():
            current = _get(ctx, job['id'])
            current.update(artifact_missing=missing,
                           out_paths=list(dict.fromkeys((current.get('out_paths') or []) + paths)),
                           artifact_digests=digests)
            e.save_job(ctx, current)
        if not missing:
            e.drop_ref(current.get('top'), current['id'])
        return {'ok': True, 'cached': False, 'remaining': len(missing)}


def _capacity(ctx, probes, jobs, cache_only=False):
    devices = {}
    now = ctx.now()
    if cache_only:
        previous = _engine().read_json(ctx.p('fleet', 'capacity.json')) or {}
        previous = previous if isinstance(previous, dict) else {}
        devices_old = previous.get('devices')
        devices_old = devices_old if isinstance(devices_old, dict) else {}
        previous_laptop = devices_old.get('laptop')
        previous_laptop = previous_laptop if isinstance(previous_laptop, dict) else {}
        laptop_open = False
    else:
        with ctx.qlock():
            laptop_open = _laptop_admission(ctx, jobs)
    minimum = dict(slots=1, memory_max_gb=.25, requires=[], requested_defer=None)
    registry = _engine().multi_host._registry(_engine(), _legacy_context(ctx))
    for host in sorted(set(probes) | set(registry) | {'ember', 'laptop'}):
        probe = probes.get(host)
        if not probe:
            state = registry[host][0] if host in registry else _legacy_state(ctx) if host == 'ember' else None
            conf = _engine().host_conf(state) if state else {}
            name = conf.get('name') or host
            name = ''.join(c if c.isprintable() else ' ' for c in str(name))[:80]
            devices[host] = dict(name=name, up=False, eligible=False, at=now, slots_total=None, slots_used=None,
                                 memory_available_gb=None, memory_reserved_gb=None, memory_floor_gb=None,
                                 load=None, load_cap=None, reason='probe unavailable', capabilities=[])
            continue
        data = probe['data']
        name = (probe.get('binding') or {}).get('name') or host
        name = ''.join(c if c.isprintable() else ' ' for c in str(name))[:80]
        if host == 'laptop':
            cfg = probe['config']
            pending = _pending(jobs, host, set())
            used = sum(j['slots'] for j in pending)
            reserved = sum(j['memory_max_gb'] for j in pending)
            if cache_only:
                # No new lock, load, RAM, or cohort probe during finalization.
                # Keep prior eligibility conservative; the sample timestamp
                # makes its age visible to readers.
                fit = previous_laptop.get('eligible') is True and 0 <= now - probe['at'] <= 30
                reason = previous_laptop.get('reason') or ('' if fit else 'cached admission not rechecked')
            else:
                fit, reason = _fit(ctx, minimum, probe, jobs, now, explain=True)
                if not laptop_open:
                    fit, reason = None, 'draining for legacy laptop work'
            devices[host] = dict(name=name, up=True, eligible=bool(fit), at=probe['at'], slots_total=cfg['slots'],
                                 slots_configured=cfg['slots'], slots_reserved=0,
                                 slots_used=used, memory_available_gb=data['available_gb'], memory_reserved_gb=reserved,
                                 commit_free_gb=data['commit_free_gb'],
                                 memory_floor_gb=cfg['ram_floor_gb'], load=None, load_cap=None,
                                 reason=reason or '', capabilities=data.get('capabilities', []))
        else:
            known = {x.get('id') for x in data['active_fleet'] if isinstance(x, dict)}
            pending = _pending(jobs, host, known)
            fit, reason = _fit(ctx, minimum, probe, jobs, now, explain=True)
            devices[host] = dict(name=name, up=True, eligible=bool(fit),
                                 at=probe['at'], slots_total=data['policy']['pool_slots']-data['policy'].get('foreground_slots', 2),
                                 slots_configured=data['policy']['pool_slots'], slots_reserved=data['policy'].get('foreground_slots', 2),
                                 slots_used=data['active_fleet_slots']+sum(j['slots'] for j in pending),
                                 memory_available_gb=data['available_bytes']/GIB,
                                 memory_reserved_gb=data['reserved_memory_bytes']/GIB+sum(j['memory_max_gb'] for j in pending),
                                 # The part of those caps already in use; admission keeps reserved less held free.
                                 memory_held_gb=(data['reserved_memory_bytes']-_host_charge(data))/GIB,
                                 memory_floor_gb=data['policy']['ram_floor_gb'], load=data['load'],
                                 load_cap=data['policy']['load_cap'], reason=reason or '', capabilities=data.get('capabilities', []))
    _engine().write_json(ctx.p('fleet', 'capacity.json'), dict(version=1, at=now, devices=devices))


def _finalize_capacity(ctx, jobs):
    """Refresh accounting from held probes without polling devices.

    Caller holds the queue lock. Remove a cached host reservation only when
    that exact job is terminal locally and bound to the probed machine ID.
    Unknown or malformed facts stay charged conservatively.
    """
    probes = getattr(ctx, '_fleet_pass_probes', None)
    if not isinstance(probes, dict):
        return
    terminal = {j.get('id'): j for j in jobs if j.get('status') in TERMINAL and isinstance(j.get('id'), str)}
    scanned = {j.get('id') for j in jobs}

    def finished(jid):
        """The job when it is finished: from the scan, or by ID when it has left the scanned store (archived)."""
        if jid in terminal or jid in scanned or not isinstance(jid, str):
            return terminal.get(jid)
        if not re.fullmatch(r'cq-(?:v2--)?fleet-\d{8}T\d{6}-[0-9a-f]{16}', jid):
            return None
        job = _engine().load_job(ctx, jid)
        if isinstance(job, dict) and job.get('id') == jid and job.get('status') in TERMINAL:
            terminal[jid] = job
        scanned.add(jid)
        return terminal.get(jid)
    final_probes = dict(probes)
    for host in sorted(h for h in probes if h != 'laptop'):
        probe = probes.get(host)
        if not isinstance(probe, dict):
            continue
        binding = probe.get('binding') or {}
        machine_id = binding.get('machine_id')
        data = probe.get('data') or {}
        active = data.get('active_fleet')
        if not machine_id or not isinstance(active, list):
            continue
        counts = {}
        for fact in active:
            if isinstance(fact, dict) and isinstance(fact.get('id'), str):
                counts[fact['id']] = counts.get(fact['id'], 0) + 1
        remove = set()
        slots, memory = 0, 0
        for fact in active:
            if not isinstance(fact, dict):
                continue
            jid = fact.get('id')
            job = finished(jid)
            amount, bytes_ = fact.get('slots'), fact.get('reserved_memory_bytes')
            if (job and counts.get(jid) == 1 and job.get('host_id') == host and
                    (job.get('host_config') or {}).get('machine_id') == machine_id and
                    type(amount) is int and amount > 0 and type(bytes_) is int and bytes_ > 0):
                remove.add(jid)
                slots += amount
                memory += bytes_
        if (not remove or not _num(data.get('active_fleet_slots'), slots) or
                not _num(data.get('reserved_memory_bytes'), memory)):
            continue
        adjusted = dict(data, active_fleet=[fact for fact in active if not isinstance(fact, dict) or fact.get('id') not in remove],
                        active_fleet_slots=data['active_fleet_slots'] - slots,
                        reserved_memory_bytes=data['reserved_memory_bytes'] - memory)
        if 'held_memory_bytes' in data:
            # What the remaining jobs hold, by their own facts and never more than the host's total: a finished
            # job's share leaves with it, and the sample's available memory does not yet show it back.
            kept = sum(fact['held_memory_bytes'] for fact in adjusted['active_fleet']
                       if isinstance(fact, dict) and type(fact.get('held_memory_bytes')) is int
                       and type(fact.get('reserved_memory_bytes')) is int
                       and 0 <= fact['held_memory_bytes'] <= fact['reserved_memory_bytes'])
            # A total _host_charge would not credit stays uncredited, whatever the remaining facts add up to.
            credited = data['reserved_memory_bytes'] - _host_charge(data)
            adjusted['held_memory_bytes'] = min(kept, credited)
        final_probes[host] = dict(probe, data=adjusted)
    _capacity(ctx, final_probes, jobs, cache_only=True)


def _pass(ctx, futures, reconcile_pool=None, reconcile_futures=None):
    """One probe per configured host; only stage workers may block on a push."""
    jobs = [j for j in all_jobs(ctx) if j.get('status') in ACTIVE]
    if not jobs:
        return False
    probes = _probes(ctx)
    ctx._fleet_pass_probes = probes
    reconcile = {}
    reconcile_futures = reconcile_futures if reconcile_futures is not None else {}
    for job in jobs:
        if job.get('status') not in ('launching', 'running'):
            continue
        if job['id'] in futures or job['id'] in reconcile_futures:
            continue
        try:
            if job.get('host_id') == 'laptop':
                _local_reconcile(ctx, job)
            elif job.get('host_id') and job.get('host_id') != 'laptop':
                probe = probes.get(job['host_id'])
                fact = None
                if probe and probe['binding']['machine_id'] == job['host_config']['machine_id']:
                    for entry in probe['data']['active_fleet'] + probe['data']['terminal_fleet']:
                        if isinstance(entry, dict) and entry.get('id') == job['id']:
                            fact = entry
                            break
                if fact and not job.get('cancel_requested') and fact.get('phase') in ('starting', 'running'):
                    _remote_reconcile(ctx, job, fact)
                elif fact and not job.get('cancel_requested'):
                    if ctx.now() - job.get('last_reconcile', 0) >= 30:
                        reconcile.setdefault(job['host_id'], []).append((job, fact, False))
                elif job.get('stage_attempted'):
                    fresh = (probe and _num(probe.get('at')) and 0 <= ctx.now() - probe['at'] <= 30 and
                             probe['binding']['machine_id'] == job['host_config']['machine_id'])
                    if (fresh or not probe or job.get('launch_uncertain') or job.get('cancel_requested')) and ctx.now() - job.get('last_reconcile', 0) >= 30:
                        reconcile.setdefault(job['host_id'], []).append((job, None, True))
        except Exception:
            # Unreachable or malformed evidence is never a completion receipt.
            pass
    remaining = max(0, 4 - len(reconcile_futures))
    for host in sorted(reconcile):
        due = sorted(reconcile[host], key=lambda item: (item[0].get('last_reconcile', 0), item[0].get('t_launch', 0), item[0]['id']))
        slots = min(remaining, max(0, 2 - sum(1 for assigned_host, _ in reconcile_futures.values() if assigned_host == host)))
        for job, fact, explicit in due[:slots]:
            remaining -= 1
            try:
                with ctx.qlock():
                    current = _engine().load_job(ctx, job['id'])
                    if not current or current.get('status') not in ('launching', 'running'):
                        continue
                    current['last_reconcile'] = ctx.now()
                    _engine().save_job(ctx, current)
                if reconcile_pool is None:
                    _remote_reconcile(ctx, current, fact, explicit=explicit, restage=True)
                else:
                    reconcile_futures[current['id']] = (host, reconcile_pool.submit(_remote_reconcile, ctx, current, fact, explicit, True))
            except Exception:
                pass
    jobs = all_jobs(ctx)
    _capacity(ctx, probes, jobs)
    return True


def _runtime(ctx):
    e = _engine()
    source = os.path.dirname(os.path.abspath(__file__))
    digest = hashlib.sha256()
    files = {}
    for name in RUNTIME_FILES:
        with open(os.path.join(source, name), 'rb') as f:
            data = f.read()
        files[name] = data
        digest.update(name.encode('ascii') + b'\0' + len(data).to_bytes(8, 'big') + data)
    version = digest.hexdigest()
    directory = ctx.p('fleet', 'runtime', version)
    for name, data in files.items():
        path = os.path.join(directory, name)
        try:
            with open(path, 'rb') as f:
                if f.read() == data:
                    continue
        except OSError:
            pass
        e.atomic_write(path, data)
    return os.path.join(directory, 'fleet_queue.py')


def _pid_identity(pid):
    try:
        if os.name == 'nt':
            import windows_pool
            return windows_pool.process_identity(pid)
        # Linux process creation ticks, not a PID-only liveness test.
        with open(f'/proc/{pid}/stat', encoding='ascii') as f:
            return int(f.read().rsplit(')', 1)[1].split()[19])
    except (OSError, ValueError, IndexError, ImportError):
        return None


def _starter_gone(receipt):
    pid, started = receipt.get('pid'), receipt.get('process_start_identity')
    if type(pid) is not int or pid <= 0:
        return False  # a launcher died before recording its child: uncertain
    if os.name == 'nt':
        try:
            import windows_pool
            windows_pool._setup()
            if started is None:
                import ctypes
                ctypes.set_last_error(0)
                handle = windows_pool.K.OpenProcess(0x1000 | 0x100000, False, pid)
                if not handle:
                    return ctypes.get_last_error() == 87  # nonexistent PID; access denied stays unknown
                try:
                    return windows_pool.K.WaitForSingleObject(handle, 0) == 0
                finally:
                    windows_pool.K.CloseHandle(handle)
            return windows_pool._worker_alive(pid, started) is False
        except (OSError, AttributeError, ImportError):
            return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except (OSError, PermissionError):
        return False
    actual = _pid_identity(pid)
    return started is not None and actual is not None and started != actual


def ensure_controller(ctx):
    """Start one contender, serialized with submissions and empty exit."""
    e = _engine()
    if os.environ.get('CQ_FLEET_AUTOSTART') == '0' or ctx.sync:
        return
    with ctx.qlock():
        if not any(j.get('status') in ACTIVE for j in all_jobs(ctx)):
            return
        lock = e.FileLock(ctx.p('fleet', 'controller.lock'), 0)
        try:
            lock.__enter__()
        except e.LockTimeout:
            return  # the current controller owns the work
        try:
            receipt = e.read_json(ctx.p('fleet', 'controller.json')) or {}
            if receipt.get('status') in ('starting', 'start_uncertain'):
                started = receipt.get('started')
                age = ctx.now() - started if _num(started) else None
                # A starter has one short lease. The controller lock still
                # serializes a late starter and its replacement after expiry.
                if (age is None or age < 30) and not _starter_gone(receipt):
                    return
            runtime = _runtime(ctx)
            receipt = dict(version=1, status='starting', pid=None, process_start_identity=None,
                           runtime=runtime, runtime_digest=os.path.basename(os.path.dirname(runtime)),
                           started=ctx.now(), heartbeat=ctx.now())
            # A durable short claim closes the Popen/receipt crash window;
            # expired contenders still compete for the exclusive lease.
            e.write_json(ctx.p('fleet', 'controller.json'), receipt)
        finally:
            lock.__exit__(None, None, None)
        try:
            with open(ctx.p('fleet', 'controller.log'), 'ab') as log:
                argv = [sys.executable, runtime, '--state', ctx.state]
                if getattr(ctx, 'generation', 1) == 2:
                    argv += ['--legacy-state', _legacy_state(ctx)]
                process = e.spawn_detached(argv, env=dict(os.environ, CQ_STATE=_legacy_state(ctx)), stdout=log)
            receipt.update(pid=process.pid, process_start_identity=_pid_identity(process.pid))
            e.write_json(ctx.p('fleet', 'controller.json'), receipt)
        except Exception:
            receipt['status'] = 'start_uncertain'
            e.write_json(ctx.p('fleet', 'controller.json'), receipt)
            raise


def _resume_pending(ctx, busy, probes=None):
    """Recover pre-launch reservations only, without selecting a new device."""
    e = _engine()
    chosen = []
    with ctx.qlock():
        jobs = all_jobs(ctx)
        laptop_open = _laptop_admission(ctx, jobs)
        for job in sorted(jobs, key=_order):
            if len(busy) >= 3:
                break
            host = job.get('host_id')
            if (job.get('status') != 'launching' or (job.get('stage_attempted') and not job.get('restage_due')) or
                    job.get('deferred_registered') or job.get('cancel_requested') or
                    job.get('preparation_blocked') or not host or host in busy or
                    ctx.now() < job.get('stage_resume_after', 0)):
                continue
            probe = (probes or {}).get(host)
            if not probe or not _num(probe.get('at')) or not 0 <= ctx.now() - probe['at'] <= 30:
                continue
            if host == 'laptop':
                others = [j for j in jobs if j['id'] != job['id']]
                if not laptop_open or not _fit(ctx, job, probe, others, ctx.now()):
                    continue
            elif probe['binding'].get('machine_id') != (job.get('host_config') or {}).get('machine_id'):
                continue
            # Persist the retry lease before queuing the worker. A controller
            # dying before submit merely delays the next safe preparation.
            job['stage_resume_after'] = ctx.now() + 30
            e.save_job(ctx, job)
            chosen.append(job)
            busy.add(host)
    return chosen


def controller(ctx):
    e = _engine()
    lease = e.FileLock(ctx.p('fleet', 'controller.lock'), 0)
    try:
        lease.__enter__()
    except e.LockTimeout:
        return
    try:
        if getattr(ctx, 'generation', 1) == 2:
            try:
                import windows_pool
                import windows_broker
                sample = windows_pool.probe()
                cfg = _laptop_config(ctx, sample) if isinstance(sample, dict) else None
                if cfg and cfg['enabled'] and cfg['slots'] > 0:
                    ctx._fleet_local_pool = windows_broker.LocalPool(
                        queue_paths.runtime_paths(_legacy_state(ctx))[2], max_cpu_slots=cfg['slots'])
            except (ImportError, OSError, ValueError):
                pass  # host-only execution remains available
        with ctx.qlock():
            receipt = dict(version=1, status='running', pid=os.getpid(), process_start_identity=_pid_identity(os.getpid()),
                           runtime=os.path.abspath(__file__), runtime_digest=os.path.basename(os.path.dirname(os.path.abspath(__file__))),
                           started=ctx.now(), heartbeat=ctx.now())
            e.write_json(ctx.p('fleet', 'controller.json'), receipt)
        try:
            futures = {}
            with ThreadPoolExecutor(max_workers=3, thread_name_prefix='fleet-stage') as pool, \
                    ThreadPoolExecutor(max_workers=4, thread_name_prefix='fleet-reconcile') as reconcile_pool:
                reconcile_futures = {}
                while True:
                    futures = {jid: (host, f) for jid, (host, f) in futures.items() if not f.done()}
                    reconcile_futures = {jid: (host, f) for jid, (host, f) in reconcile_futures.items() if not f.done()}
                    try:
                        active = _pass(ctx, futures, reconcile_pool, reconcile_futures)
                        if not active and not futures and not reconcile_futures:
                            # Submit writes under this same lock. Recheck and
                            # release the lease before allowing another submit.
                            with ctx.qlock():
                                final_jobs = all_jobs(ctx)
                                if not any(j.get('status') in ACTIVE for j in final_jobs):
                                    _finalize_capacity(ctx, final_jobs)
                                    receipt.update(status='stopped', heartbeat=ctx.now(), stopped=ctx.now())
                                    e.write_json(ctx.p('fleet', 'controller.json'), receipt)
                                    lease.__exit__(None, None, None)
                                    return
                            continue
                        receipt['heartbeat'] = ctx.now()
                        e.write_json(ctx.p('fleet', 'controller.json'), receipt)
                        busy = {host for host, _ in futures.values()}
                        for job in _resume_pending(ctx, busy, getattr(ctx, '_fleet_pass_probes', {})):
                            host = job['host_id']
                            fn = _stage_laptop if host == 'laptop' else _stage_remote
                            futures[job['id']] = (host, pool.submit(fn, ctx, job))
                        for job in _reserve(ctx, _probes_for_reserve(ctx, busy), max(0, 3 - len(futures))):
                            host = job['host_id']
                            fn = _stage_laptop if host == 'laptop' else _stage_remote
                            futures[job['id']] = (host, pool.submit(fn, ctx, job))
                    except Exception:
                        try:
                            telemetry.append_event(ctx.p('fleet'), {'type': 'controller_error', 'at': ctx.now()})
                        except (OSError, ValueError, TimeoutError):
                            pass
                    time.sleep(5)
        finally:
            pass
    finally:
        pool = getattr(ctx, '_fleet_local_pool', None)
        if pool is not None:
            pool.close()
        lease.__exit__(None, None, None)


def _probes_for_reserve(ctx, busy):
    # Capacity was just written from this pass; keep the exact in-memory
    # observations to avoid a second remote probe in a controller cycle.
    probes = getattr(ctx, '_fleet_pass_probes', None)
    if probes is None:
        return {}
    return {key: value for key, value in probes.items() if key not in busy}


def _cancel(ctx, req):
    e = _engine()
    jid = req.get('job')
    e.require_request_identity(req, 'cancellation')
    root, session = req['root'], req['session']
    if not os.path.isdir(root):
        _error('fleet cancellation requires the current worktree root and owner session')
    top = e._top_of(root)
    with e.owner_lock(top), ctx.qlock():
        job = _get(ctx, jid)
        if not e.same_path(os.path.realpath(top), os.path.realpath(job.get('top') or '')):
            _error('fleet cancellation worktree does not match the job')
        identity = e.worktree_identity(top)
        if identity is None or identity != job.get('worktree_identity'):
            _error('fleet cancellation worktree identity changed')
        owner = e.read_json(os.path.join(top, '.baton', 'owner.json'))
        if not isinstance(owner, dict) or owner.get('session_id') != session:
            _error('fleet cancellation requires the current worktree owner')
        if job['status'] in TERMINAL:
            return {'ok': True, 'job': jid, 'status': job['status'], 'note': 'already finished'}
        if job['status'] == 'queued' or (job['status'] == 'launching' and not job.get('stage_attempted')):
            job.update(status='cancelled', reason='cancelled before staging', t_finished=ctx.now(),
                       finished=e.iso(ctx.now()), exit=None, tail=[], tail_digest=e.stored_tail_digest([]), artifact_digests=[])
            e.save_job(ctx, job)
            _event(ctx, 'cancelled', job)
            e.drop_ref(job.get('top'), jid)
            return {'ok': True, 'job': jid, 'status': 'cancelled'}
        job.update(cancel_requested=True, reason='cancellation pending confirmation')
        e.save_job(ctx, job)
        _event(ctx, 'cancel_requested', job)
    if job.get('host_id') == 'laptop':
        path = job.get('descriptor_path')
        if path:
            e.atomic_write(os.path.join(os.path.dirname(path), 'cancel'), b'cancel\n')
    elif job.get('host_id') and job.get('host_id') != 'laptop':
        _remote_reconcile(ctx, job, explicit=True)
    return {'ok': True, 'job': jid, 'status': _get(ctx, jid)['status'], 'note': 'cancellation pending confirmation'}


def execute(ctx, command, req):
    e = _engine()
    if command == 'submit':
        return _submit(ctx, req)
    if command == 'cancel':
        return _cancel(ctx, req)
    if command in ('status', 'get'):
        job = _get(ctx, req.get('job'))
        refresh = None
        if command == 'status' and req.get('refresh'):
            if job['status'] in TERMINAL and (job.get('artifact_missing') or job.get('artifact_pending')):
                if job.get('host_id') == 'laptop':
                    refresh = _recover_local(ctx, job)
                elif job.get('host_id') and job.get('host_id') != 'laptop':
                    refresh = e.refresh_deferred(ctx, job)
                    latest = _get(ctx, job['id'])
                    if refresh.get('ok') and not latest.get('artifact_missing'):
                        e.drop_ref(latest.get('top'), latest['id'])
            elif job['status'] in ('launching', 'running'):
                if job.get('host_id') == 'laptop':
                    _local_reconcile(ctx, job)
                    latest = _get(ctx, job['id'])
                    refresh = {'ok': latest.get('status') in TERMINAL or not latest.get('launch_uncertain'),
                               'cached': bool(latest.get('launch_uncertain'))}
                elif job.get('host_id') and job.get('host_id') != 'laptop':
                    refresh = _remote_reconcile(ctx, job, explicit=True)
            job = _get(ctx, job['id'])
        delivered = {}
        if job['status'] in TERMINAL:   # the caller has the result: no notice repeats it
            marked = e.mark_delivered(ctx, job['id'], req.get('seen_by'), 'status')
            delivered = {'delivered': bool(marked or job.get('notified'))} if req.get('compact') else {}
        view = e.job_brief(job, ctx.now()) if req.get('compact') else job
        return {'ok': True, **delivered, 'job': view, **({'refresh': refresh} if refresh is not None else {})}
    if command == 'wait':
        deadline = time.monotonic() + max(1, min(540, int(req.get('timeout_s') or 540)))
        while True:
            job = _get(ctx, req.get('job'))
            if job['status'] in TERMINAL:
                marked = e.mark_delivered(ctx, job['id'], req.get('seen_by'), 'wait')
                return {'ok': True, 'ended': True, 'delivered': bool(marked or job.get('notified')),
                        'job': e.result_view(job) | {'status': job['status']}}
            if time.monotonic() >= deadline:
                return {'ok': True, 'ended': False, 'job': e.job_summary(job, ctx.now())}
            time.sleep(min(10, max(0, deadline-time.monotonic())))
    _error('unsupported fleet command')


def _status_row(job, now):
    row = _engine().status_row(job, now, None)
    reasons = job.get('placement_reasons')
    if job.get('status') == 'queued' and isinstance(reasons, dict):
        reasons = {h: str(reasons[h])[:100] for h in sorted(reasons) if isinstance(h, str)}
        row['placement_reasons'] = reasons
        if reasons:
            row['wait_reason'] = '; '.join(f'{h}: {reason}' for h, reason in reasons.items())
    return row


def augment(ctx, command, req, result):
    """Add fleet views to primary aggregate commands without polling a host."""
    e = _engine()
    result = dict(result)
    if not result.get('ok'):
        return result
    jobs = all_jobs(ctx)
    if command in ('tick', 'ensure_deferred') and any(j.get('status') in ACTIVE for j in jobs):
        try:
            ensure_controller(ctx)
        except Exception as exc:
            result['fleet_start_warning'] = 'fleet controller wake failed: ' + type(exc).__name__
    if not jobs and not os.path.isfile(ctx.p('fleet', 'capacity.json')):
        return result
    now = ctx.now()
    result['fleet'] = telemetry.summarize(ctx.p('fleet'), now)
    if command == 'list':
        selected = jobs
        if req.get('status'):
            selected = [j for j in selected if j.get('status') == req['status']]
        if req.get('root'):
            top = e._top_of(req['root'])
            selected = [j for j in selected if e.same_path(j.get('top', ''), top)]
        rows = list(result.get('jobs') or [])
        rows.extend(_status_row(j, now) for j in sorted(selected, key=_order))
        result['jobs'] = rows[-max(1, min(1000, int(req.get('limit') or 100))):]
    elif command == 'status' and not req.get('job'):
        result['queue'] = list(result.get('queue') or []) + [_status_row(j, now) for j in sorted(jobs, key=_order) if j.get('status') in ACTIVE]
        recent = sorted((j for j in jobs if j.get('status') in TERMINAL), key=lambda j: j.get('t_finished') or 0)[-5:]
        result['recent'] = list(result.get('recent') or []) + [_status_row(j, now) for j in recent]
    elif command in ('watch', 'tick'):
        watch = dict(result.get('watch') or {})
        root, session = req.get('root') or '', req.get('watch_session') or req.get('session') or ''
        top = e._top_of(root) if root and os.path.isdir(root) else root
        identity = e.worktree_identity(top) if top else None
        mine = [j for j in jobs if identity is not None and e.notice_matches_worktree(j, top, identity, session)]
        watch['mine_running'] = watch.get('mine_running', 0) + sum(j.get('status') in ('launching', 'running') for j in mine)
        watch['mine_queued'] = watch.get('mine_queued', 0) + sum(j.get('status') == 'queued' for j in mine)
        watch['queued'] = watch.get('queued', 0) + sum(j.get('status') == 'queued' for j in jobs)
        owner = e.read_json(os.path.join(top, '.baton', 'owner.json')) if top else None
        if isinstance(owner, dict) and owner.get('session_id') == session:
            pending = [j for j in mine if j.get('status') in TERMINAL and j.get('notify', True) and not j.get('notified')]
            watch['to_notify'] = list(watch.get('to_notify') or []) + [e.notice_data(j) for j in pending]
            extra = e.attention_counts(pending)
            watch['mine_attention'] = {k: (watch.get('mine_attention') or {}).get(k, 0) + extra[k] for k in extra}
        result['watch'] = watch
    return result


if __name__ == '__main__':
    import argparse
    import cq
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', required=True)
    parser.add_argument('--legacy-state')
    args = parser.parse_args()
    bind(cq)
    context = cq.Ctx(state=args.state)
    if args.legacy_state:
        context.legacy_state = args.legacy_state
        context.generation = 2
    controller(context)
