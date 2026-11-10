"""Isolated fleet coordinator tests: no host connections or real workers."""
import json
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cq
import fleet_queue as fq


class FleetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'work'
        (self.root / '.baton').mkdir(parents=True)
        (self.root / '.baton' / 'owner.json').write_text(json.dumps({'session_id': 's1', 'vendor': 'codex'}))
        self.state = Path(self.tmp.name) / 'state'
        self.state.mkdir()
        cq.write_json(str(self.state / 'hosts.json'), {'version': 1, 'hosts': {
            'hearth': {'state': 'hosts/hearth', 'enabled': True}}})
        cq.write_json(str(self.state / 'host.conf.json'), {'address': 'ember', 'host_id': 'ember'})
        cq.write_json(str(self.state / 'hosts/hearth/host.conf.json'), {'address': 'hearth', 'host_id': 'hearth'})
        self.clock = [1700000000.0]
        self.ctx = cq.Ctx(state=str(self.state), now=lambda: self.clock[0], sync=True)
        self.ctx.host_id = None
        fq.bind(cq)
        old_file = cq.job_file
        old_load = cq.load_job
        self.jobfile = patch.object(cq, 'job_file', side_effect=lambda ctx, jid, deferred=False:
                             ctx.p('fleet', 'jobs', jid + '.json') if jid.startswith(('cq-fleet-', 'cq-v2--fleet-')) else old_file(ctx, jid, deferred))
        self.jobfile.start(); self.addCleanup(self.jobfile.stop)
        self.load = patch.object(cq, 'load_job', side_effect=lambda ctx, jid:
                          cq.read_json(ctx.p('fleet', 'jobs', jid + '.json')) if jid.startswith(('cq-fleet-', 'cq-v2--fleet-')) else old_load(ctx, jid))
        self.load.start(); self.addCleanup(self.load.stop)
        def fake_git(top, *args, **kw):
            if args == ('rev-parse', '--show-toplevel'):
                return str(self.root)
            if args == ('rev-parse', '--git-common-dir'):
                return '.git'
            if args and args[0] == 'rev-parse':
                return 'a' * 40
            raise AssertionError(args)
        self.git = patch.object(cq, 'git', side_effect=fake_git)
        self.git.start(); self.addCleanup(self.git.stop)
        self.snapshot = patch.object(cq, 'snapshot', return_value=('a' * 40, False))
        self.snapshot_mock = self.snapshot.start(); self.addCleanup(self.snapshot.stop)

    def submit(self, **changes):
        req = dict(where='fleet', root=str(self.root), command='echo test', session='s1', slots=1)
        req.update(changes)
        return fq.execute(self.ctx, 'submit', req)

    def test_baton_owner_lock_serializes_fleet_submission_and_cancellation(self):
        spec = importlib.util.spec_from_file_location('baton_locking_test', Path(__file__).resolve().parents[3] / 'baton' / 'locking.py')
        baton_locking = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(baton_locking)
        owner = self.root / '.baton' / 'owner.json'
        for operation in ('submit', 'cancel'):
            with self.subTest(operation=operation):
                cq.write_json(str(owner), {'vendor': 'codex', 'session_id': 's1'})
                jid = self.submit()['job'] if operation == 'cancel' else None
                started, acquired = threading.Event(), threading.Event()
                errors = []
                def handoff():
                    try:
                        started.set()
                        with baton_locking.exclusive(owner.parent / 'owner.lock'):
                            acquired.set()
                            cq.write_json(str(owner), {'vendor': 'claude', 'session_id': 's2'})
                    except Exception as exc:
                        errors.append(exc)
                thread = None
                original = cq.save_job
                def save(ctx, job):
                    nonlocal thread
                    if thread is None:
                        thread = threading.Thread(target=handoff)
                        thread.start()
                        self.assertTrue(started.wait(1))
                        self.assertFalse(acquired.wait(.15), 'owner changed during queue mutation')
                    return original(ctx, job)
                with patch.object(cq, 'save_job', side_effect=save):
                    if operation == 'submit':
                        self.submit()
                    else:
                        fq.execute(self.ctx, 'cancel', {'job': jid, 'root': str(self.root), 'session': 's1'})
                thread.join(3)
                self.assertFalse(thread.is_alive())
                self.assertTrue(acquired.is_set())
                self.assertEqual(errors, [])

    def job(self, jid):
        return cq.load_job(self.ctx, jid)

    def remote(self, host='ember', slots=8, avail=40, load=0, caps=None):
        policy = dict(pool_slots=slots, load_cap=20, ram_floor_gb=4)
        data = dict(policy=policy, load=load, available_bytes=avail*fq.GIB, total_bytes=64*fq.GIB,
                    active_fleet_slots=0, reserved_memory_bytes=0, legacy_busy=False, ordinary_waiting=False,
                    active_fleet=[], terminal_fleet=[], capabilities=caps or ['linux', 'python'])
        return dict(host=host, state=str(self.state), binding=dict(address=host, machine_id='a'*32,
                    slots=slots, slot_vcpu=1.5, ram_floor_gb=4, load_cap=20), data=data, at=self.clock[0])

    def laptop(self, available=20, commit=20, slots=4):
        return dict(host='laptop', at=self.clock[0], data=dict(available_gb=available, commit_free_gb=commit,
                    total_gb=32, logical_cpus=16, capabilities=['python']),
                    config=dict(slots=slots, ram_floor_gb=6, commit_floor_gb=6, enabled=True))

    def test_generation_uses_private_jobs_and_legacy_configuration(self):
        v2 = cq.Ctx(state=str(self.state / 'queue-v2'), now=lambda: self.clock[0], sync=True)
        v2.generation = 2
        v2.legacy_state = str(self.state)
        req = dict(where='fleet', root=str(self.root), command='echo test', session='s1', candidates=['hestia'])
        jid = fq.execute(v2, 'submit', req)['job']
        self.assertTrue(jid.startswith('cq-v2--fleet-'))
        self.assertEqual(fq._get(v2, jid)['candidates'], ['hearth'])
        self.assertEqual(fq.all_jobs(self.ctx), [])
        self.assertEqual(len(fq.all_jobs(v2)), 1)
        self.assertIn('hearth', fq._hosts(v2))
        self.assertEqual(fq._legacy_state(v2), str(self.state))
        self.assertFalse(Path(self.ctx.p('fleet', 'controller.lock')).exists())
        with self.assertRaises(cq.CqError):
            fq._get(self.ctx, jid)

    def test_v2_controller_does_not_compete_for_old_controller_lock(self):
        v2 = cq.Ctx(state=str(self.state / 'queue-v2'), now=lambda: self.clock[0], sync=True)
        v2.generation, v2.legacy_state = 2, str(self.state)
        req = dict(where='fleet', root=str(self.root), command='true', session='s1')
        fq.execute(v2, 'submit', req)
        v2.sync = False
        with cq.FileLock(self.ctx.p('fleet', 'controller.lock'), 0), \
                patch.object(fq, '_runtime', return_value='runtime.py'), \
                patch.object(cq, 'spawn_detached', return_value=types.SimpleNamespace(pid=123)) as spawn, \
                patch.object(fq, '_pid_identity', return_value=456):
            fq.ensure_controller(v2)
        args, kw = spawn.call_args
        self.assertIn('--legacy-state', args[0])
        self.assertEqual(kw['env']['CQ_STATE'], str(self.state))
        self.assertTrue(Path(v2.p('fleet', 'controller.json')).exists())
        self.assertFalse(Path(self.ctx.p('fleet', 'controller.json')).exists())

    def test_remote_probe_reservations_cover_both_generations(self):
        probe = self.remote('hearth', slots=8)
        probe['data'].update(active_fleet_slots=5, reserved_memory_bytes=3*fq.GIB,
                             active_fleet=[dict(id='cq-fleet-old'), dict(id='cq-v2--fleet-new')])
        job = dict(slots=2, memory_max_gb=1, requires=[], requested_defer=None)
        fit, reason = fq._fit(self.ctx, job, probe, [], self.clock[0], explain=True)
        self.assertIsNone(fit)
        self.assertEqual(reason, 'CPU slots unavailable')

    def test_broker_memory_union_excludes_held_and_current_jobs(self):
        jobs = [dict(id='held', host_id='laptop', status='running', memory_max_gb=3),
                dict(id='waiting', host_id='laptop', status='launching', memory_max_gb=5),
                dict(id='current', host_id='laptop', status='launching', memory_max_gb=2)]
        pool = types.SimpleNamespace(reservations=lambda: {'held': 3})
        self.assertEqual(fq._external_pending_gb(jobs, 'current', pool), 5)
        pool.reservations = lambda: {'held': 2}  # an inconsistent cap cannot erase a reservation
        self.assertEqual(fq._external_pending_gb(jobs, 'current', pool), 8)

    def test_receipt_first_and_owner_required(self):
        jid = self.submit()['job']
        job = self.job(jid)
        receipt = json.loads((self.root / '.baton' / 'compute' / (jid + '.json')).read_text())
        self.assertEqual(receipt['host_id'], 'fleet')
        self.assertEqual(receipt['source_sha'], job['sha'])
        self.assertEqual(job['memory_gb'], 2)
        self.assertEqual(job['memory_max_gb'], 2.5)
        self.assertEqual(self.snapshot_mock.call_count, 1)
        with self.assertRaises(cq.CqError):
            self.submit(session='wrong')
        self.assertEqual(self.snapshot_mock.call_count, 1)

    def test_invalid_shapes_and_deferred_platform(self):
        cases = [dict(slots=0), dict(slots=True), dict(memory_gb=float('nan')), dict(memory_gb=3, memory_max_gb=2),
                 dict(host_id='ember'), dict(split={}), dict(self_heavy=True), dict(temp_storage='ram'),
                 dict(candidates=['laptop'], platform='windows', defer='spare'), dict(subdir='../escape'),
                 dict(gets=['../../outside']), dict(requires='python')]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(cq.CqError):
                self.submit(**case)
        self.assertEqual(self.snapshot_mock.call_count, 0)

    def test_select_by_capacity_and_pin_before_stage(self):
        jid = self.submit(platform='portable', candidates=['laptop','hestia','ember'], memory_gb=3, memory_max_gb=5)['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            selected = fq._reserve(self.ctx, {'laptop': self.laptop(), 'ember': self.remote(), 'hearth': self.remote('hearth', 16)})
        self.assertEqual(selected[0]['host_id'], 'hearth')
        job = self.job(jid)
        self.assertTrue(job['launch_uncertain'])
        self.assertEqual(job['host_config']['machine_id'], 'a'*32)
        self.assertEqual(job['status'], 'launching')
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.remote(), 'hearth': self.remote('hearth',16)}), [])

    def test_slot_ram_and_stale_capacity_independent(self):
        jid = self.submit(slots=2, memory_gb=2, memory_max_gb=10)['job']
        job = self.job(jid)
        self.assertIsNotNone(fq._fit(self.ctx, job, self.remote(slots=8, avail=20), [], self.clock[0]))
        self.assertIsNone(fq._fit(self.ctx, job, self.remote(slots=8, avail=13), [], self.clock[0]))
        self.assertIsNone(fq._fit(self.ctx, job, self.remote(slots=3, avail=40), [], self.clock[0]))
        stale = self.remote(); stale['at'] -= 31
        self.assertIsNone(fq._fit(self.ctx, job, stale, [], self.clock[0]))
        blocked = self.remote(); blocked['data']['legacy_busy'] = True
        self.assertIsNone(fq._fit(self.ctx, job, blocked, [], self.clock[0]))

    def test_requirements_fail_closed_and_defer_stage_despite_load(self):
        jid = self.submit(requires=['blender'], defer='overnight', candidates=['ember'])['job']
        job = self.job(jid)
        no_tool = self.remote(load=100)
        self.assertIsNone(fq._fit(self.ctx, job, no_tool, [], self.clock[0]))
        with_tool = self.remote(load=100, caps=['blender'])
        with_tool['data']['legacy_busy'] = True
        self.assertIsNotNone(fq._fit(self.ctx, job, with_tool, [], self.clock[0]))

    def test_laptop_checks_both_memory_floors_and_full_cap(self):
        jid = self.submit(platform='portable', candidates=['laptop'], memory_gb=1, memory_max_gb=4)['job']
        job = self.job(jid)
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            self.assertIsNone(fq._fit(self.ctx, job, self.laptop(available=20, commit=9), [], self.clock[0]))
            self.assertIsNotNone(fq._fit(self.ctx, job, self.laptop(available=20, commit=20), [], self.clock[0]))
            other = dict(id='other', host_id='laptop', status='running', slots=1, memory_max_gb=7)
            self.assertIsNone(fq._fit(self.ctx, job, self.laptop(available=16, commit=20), [other], self.clock[0]))

    def test_uncertain_stage_remains_pinned_and_never_retries(self):
        jid = self.submit(candidates=['ember'])['job']
        job = fq._reserve(self.ctx, {'ember': self.remote()})[0]
        self.ctx.run_remoterun = lambda argv, cwd: (255, '', 'connection lost')
        with patch.object(cq, 'make_launcher', return_value='true'):
            fq._stage_remote(self.ctx, job)
        after = self.job(jid)
        self.assertTrue(after['launch_uncertain'])
        self.assertTrue(after['stage_attempted'])
        self.assertEqual(after['host_id'], 'ember')
        self.assertEqual(after['status'], 'launching')
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.remote()}), [])

    def test_cancel_race_does_not_claim_remote_exit(self):
        jid = self.submit(candidates=['ember'])['job']
        job = fq._reserve(self.ctx, {'ember': self.remote()})[0]
        job['stage_attempted'] = True
        cq.save_job(self.ctx, job)
        with patch.object(fq, '_remote_reconcile') as reconcile:
            result = fq.execute(self.ctx, 'cancel', {'job': jid, 'root': str(self.root), 'session': 's1'})
        self.assertEqual(result['status'], 'launching')
        self.assertTrue(self.job(jid)['cancel_requested'])
        reconcile.assert_called_once()

    def test_cancel_rejects_wrong_owner_worktree_and_directory_incarnation(self):
        jid = self.submit(platform='windows', candidates=['laptop'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
        run = self.state / 'run-for-cancel'; run.mkdir()
        job.update(stage_attempted=True, descriptor_path=str(run / 'descriptor.json'))
        cq.save_job(self.ctx, job)
        original = self.job(jid)
        other = self.state / 'different-worktree'; other.mkdir()
        requests = [dict(job=jid), dict(job=jid, root=str(self.root), session='wrong')]
        for request in requests:
            with self.subTest(request=request), self.assertRaises(cq.CqError):
                fq.execute(self.ctx, 'cancel', request)
        with patch.object(cq, '_top_of', return_value=str(other)), self.assertRaises(cq.CqError):
            fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(other), session='s1'))
        with patch.object(cq, 'worktree_identity', return_value={'device': -1, 'inode': -1}), self.assertRaises(cq.CqError):
            fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(self.root), session='s1'))
        self.assertEqual(self.job(jid), original)
        self.assertFalse((run / 'cancel').exists())

    def test_current_handoff_owner_can_cancel_predecessor_job(self):
        jid = self.submit()['job']
        (self.root / '.baton' / 'owner.json').write_text(json.dumps({'session_id': 's2', 'vendor': 'codex'}))
        with self.assertRaises(cq.CqError):
            fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(self.root), session='s1'))
        result = fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(self.root), session='s2'))
        self.assertEqual(result['status'], 'cancelled')
        self.assertEqual(self.job(jid)['session'], 's1')

    def test_concurrent_reservation_does_not_duplicate_device(self):
        self.submit(candidates=['ember'])
        self.submit(candidates=['ember'])
        outcomes = []
        def reserve():
            outcomes.extend(fq._reserve(self.ctx, {'ember': self.remote()}))
        threads = [threading.Thread(target=reserve) for _ in range(2)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(len([j for j in fq.all_jobs(self.ctx) if j['status'] == 'launching']), 1)

    def test_artifacts_reject_symlink_and_outside(self):
        src = self.state / 'src'; src.mkdir()
        (src / 'good').write_text('ok')
        self.assertEqual(fq._regular_inside(str(src), 'good'), str((src / 'good').resolve()))
        try:
            (src / 'link').symlink_to(src / 'good')
        except OSError:
            return
        self.assertIsNone(fq._regular_inside(str(src), 'link'))
        self.assertIsNone(fq._regular_inside(str(src), '../outside'))

    def test_status_augmentation_has_no_probe(self):
        self.submit()
        with patch.object(fq, '_probes', side_effect=AssertionError('unexpected network')):
            result = fq.augment(self.ctx, 'status', {}, {'ok': True, 'queue': [], 'recent': []})
        self.assertEqual(len(result['queue']), 1)
        self.assertEqual(result['fleet']['counts']['queued'], 1)

    def test_remote_probe_requires_protocol_and_identity(self):
        payload = dict(version=1, supported=True, safety_protocol='shared-heavy-gate-v1', machine_id='a'*32,
                       policy=dict(pool_slots=8, load_cap=20, ram_floor_gb=4), load=1, available_bytes=40*fq.GIB,
                       total_bytes=64*fq.GIB, logical_cpus=16, active_fleet_slots=0, reserved_memory_bytes=0,
                       legacy_busy=False, ordinary_waiting=False, active_fleet=[], terminal_fleet=[])
        conf = dict(address='host', host_id='ember', machine_id='a'*32, slots=8)
        with patch.object(cq, 'job_ssh', return_value=(0, json.dumps(payload), '')):
            self.assertIsNotNone(fq._remote_probe(self.ctx, 'ember', str(self.state), conf))
        payload['safety_protocol'] = 'legacy'
        with patch.object(cq, 'job_ssh', return_value=(0, json.dumps(payload), '')):
            self.assertIsNone(fq._remote_probe(self.ctx, 'ember', str(self.state), conf))
        payload['safety_protocol'] = 'shared-heavy-gate-v1'
        payload['machine_id'] = 'b'*32
        with patch.object(cq, 'job_ssh', return_value=(0, json.dumps(payload), '')):
            self.assertIsNone(fq._remote_probe(self.ctx, 'ember', str(self.state), conf))

    def test_configured_hosts_aliases_tie_and_removed_recovery(self):
        registry = {'version': 1, 'hosts': {
            'hearth': {'state': 'hosts/hearth', 'enabled': True},
            'zephyr': {'state': 'hosts/zephyr', 'enabled': True}},
            'aliases': {'main': 'ember', 'third': 'zephyr'}}
        cq.write_json(self.ctx.p('hosts.json'), registry)
        cq.write_json(self.ctx.p('hosts', 'zephyr', 'host.conf.json'), {'host_id': 'zephyr', 'address': 'zephyr'})
        jid = self.submit(candidates=['third', 'main', 'hestia'])['job']
        self.assertEqual(self.job(jid)['candidates'], ['zephyr', 'ember', 'hearth'])
        probes = {h: self.remote(h) for h in ('ember', 'hearth', 'zephyr')}
        job = fq._reserve(self.ctx, probes)[0]
        self.assertEqual(job['host_id'], 'ember')  # lexical tie, independent of input order
        job.update(host_id='zephyr', host='zephyr', host_config=probes['zephyr']['binding'],
                   dispatch_state=self.ctx.p('hosts', 'zephyr'), stage_attempted=True, launch_uncertain=False)
        cq.save_job(self.ctx, job)
        del registry['hosts']['zephyr']
        del registry['aliases']['third']
        cq.write_json(self.ctx.p('hosts.json'), registry)
        with patch.object(fq, '_probes', return_value={}), patch.object(fq, '_remote_reconcile') as reconcile:
            fq._pass(self.ctx, {})
            self.assertEqual(reconcile.call_args.args[1]['host_config']['address'], 'zephyr')
        with patch.object(fq, '_remote_reconcile') as reconcile:
            fq._cancel(self.ctx, {'job': jid, 'root': str(self.root), 'session': 's1'})
            self.assertEqual(reconcile.call_args.args[1]['host'], 'zephyr')

    def test_removed_host_can_resume_pinned_but_cannot_receive_new_work(self):
        jid = self.submit(candidates=['hearth'])['job']
        pending = fq._reserve(self.ctx, {'hearth': self.remote('hearth')})[0]
        pending.update(stage_attempted=False, host_config=self.remote('hearth')['binding'],
                       dispatch_state=self.ctx.p('hosts', 'hearth'))
        cq.save_job(self.ctx, pending)
        queued = self.submit(candidates=['hearth'])['job']
        cq.write_json(self.ctx.p('hosts.json'), {'version': 1, 'hosts': {}})
        with patch.object(fq, '_remote_probe', return_value=self.remote('hearth')) as probe:
            probes = fq._probes(self.ctx)
        self.assertEqual(probe.call_args.args[3]['address'], 'hearth')
        self.assertEqual(fq._reserve(self.ctx, probes), [])
        self.assertEqual(self.job(queued)['status'], 'queued')
        self.assertEqual(fq._resume_pending(self.ctx, set(), probes)[0]['id'], jid)

    def test_verified_policy_controls_reserve_and_legacy_capability(self):
        payload = dict(version=1, supported=True, safety_protocol='shared-heavy-gate-v1', machine_id='a'*32,
                       policy=dict(pool_slots=8, load_cap=20, ram_floor_gb=4), load=1, available_bytes=40*fq.GIB,
                       total_bytes=64*fq.GIB, logical_cpus=16, active_fleet_slots=0, reserved_memory_bytes=0,
                       legacy_busy=False, ordinary_waiting=False, active_fleet=[], terminal_fleet=[])
        conf = dict(address='host', host_id='ember')
        def probe():
            with patch.object(cq, 'job_ssh', return_value=(0, json.dumps(payload), '')):
                return fq._remote_probe(self.ctx, 'ember', str(self.state), conf)
        self.assertIsNotNone(probe())
        conf['foreground_slots'] = 3
        self.assertIsNone(probe())
        payload['resource_policy_version'] = 2
        payload['policy'].update(foreground_slots=3, timezone='Europe/London', window_start='00:00', window_end='08:00')
        p = probe()
        self.assertEqual(p['binding']['foreground_slots'], 3)
        job = dict(slots=6, memory_max_gb=1, requires=[], requested_defer=None)
        self.assertIsNone(fq._fit(self.ctx, job, p, [], self.clock[0]))
        self.assertFalse(fq._fits_idle(job, p))
        conf['foreground_slots'] = 4
        self.assertIsNone(probe())

    def test_uncertain_reconciles_at_most_once_per_30_seconds(self):
        self.submit(candidates=['ember'])
        job = fq._reserve(self.ctx, {'ember': self.remote()})[0]
        job['stage_attempted'] = True
        cq.save_job(self.ctx, job)
        probe = self.remote()
        probe['data']['active_fleet'] = []
        probe['data']['terminal_fleet'] = []
        with patch.object(fq, '_probes', return_value={'ember': probe}) as probes, \
                patch.object(fq, '_remote_reconcile') as reconcile:
            fq._pass(self.ctx, {})
            fq._pass(self.ctx, {})
            self.assertEqual(probes.call_count, 2)
            self.assertEqual(reconcile.call_count, 1)
            self.assertTrue(reconcile.call_args.kwargs['explicit'])
            self.assertEqual(self.job(job['id'])['host_id'], 'ember')

    def test_windows_result_needs_same_worker_and_empty_proof(self):
        jid = self.submit(platform='windows', candidates=['laptop'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
        run = self.state / 'worker'; run.mkdir()
        src = run / 'src'; src.mkdir()
        desc = run / 'descriptor.json'; desc.write_text('{}')
        job.update(worker_pid=111, worker_start_identity=222, descriptor_path=str(desc), local_src=str(src))
        cq.save_job(self.ctx, job)
        result = dict(phase='done', active_processes=0, exit=0, worker_pid=111, worker_start_filetime=222)
        (run / 'result.json').write_text(json.dumps(result))
        worker = types.SimpleNamespace(observe=lambda *args: {'empty_confirmed': False})
        with patch.dict(sys.modules, {'windows_pool': worker}), patch.object(cq, 'finish') as finish:
            fq._local_reconcile(self.ctx, job)
            finish.assert_not_called()
            result['worker_start_filetime'] = 223
            (run / 'result.json').write_text(json.dumps(result))
            worker.observe = lambda *args: {'empty_confirmed': True}
            fq._local_reconcile(self.ctx, job)
            finish.assert_not_called()
            result['worker_start_filetime'] = 222
            (run / 'result.json').write_text(json.dumps(result))
            fq._local_reconcile(self.ctx, job)
            finish.assert_called_once()

    def test_local_missing_artifact_can_be_recovered_after_proof(self):
        jid = self.submit(platform='windows', candidates=['laptop'], gets=['result.txt'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
        run = self.state / 'recover'; run.mkdir()
        src = run / 'src'; src.mkdir()
        (src / 'result.txt').write_text('recovered')
        desc = run / 'descriptor.json'; desc.write_text('{}')
        job.update(status='done', worker_pid=111, worker_start_identity=222, descriptor_path=str(desc),
                   local_src=str(src), artifact_missing=[{'path': 'result.txt'}], artifact_digests=[], out_paths=[])
        cq.save_job(self.ctx, job)
        worker = types.SimpleNamespace(observe=lambda *args: {'empty_confirmed': True})
        with patch.dict(sys.modules, {'windows_pool': worker}), patch.object(cq, 'drop_ref') as drop:
            result = fq.execute(self.ctx, 'status', {'job': jid, 'refresh': True})
        self.assertEqual(result['job']['artifact_missing'], [])
        self.assertEqual(result['refresh']['remaining'], 0)
        self.assertEqual(Path(result['job']['out_paths'][0]).read_text(), 'recovered')
        drop.assert_called_once()

    @unittest.skipUnless(os.name == 'nt', 'Windows Job Objects')
    def test_pooled_completion_proof_recovers_while_other_job_runs(self):
        import windows_broker
        import windows_pool
        from test_windows_pool import descriptor
        self.ctx.generation = 2
        jid = self.submit(platform='windows', candidates=['laptop'], gets=['result.txt'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
        run = self.state / 'pooled'; run.mkdir()
        src = run / 'src'; src.mkdir()
        lock = str(self.state / 'heavy.lock')
        d = descriptor(str(run), [sys.executable, '-c', 'pass'])
        d.update(id=jid, legacy_lock_path=lock, ram_floor_gb=0, pending_reserved_gb=0)
        desc = run / 'descriptor.json'; desc.write_text(json.dumps(d))
        other = self.state / 'other'; other.mkdir()
        d2 = descriptor(str(other), [sys.executable, '-c', 'import time;time.sleep(15)'])
        d2.update(id='other', legacy_lock_path=lock, ram_floor_gb=0, pending_reserved_gb=0, timeout_seconds=20)
        desc2 = other / 'descriptor.json'; desc2.write_text(json.dumps(d2))
        pool = windows_broker.LocalPool(lock, cohort_seconds=30)
        self.addCleanup(pool.close)
        self.addCleanup(lambda: Path(d2['cancel_path']).touch())
        receipt = pool.submit(str(desc))
        pool.submit(str(desc2))
        self.ctx.generation = 2
        self.ctx._fleet_local_pool = pool
        job.update(status='running', worker_claim_protocol='v2', worker_nonce=d['nonce'],
                   descriptor_hash=fq._descriptor_digest(d), descriptor_path=str(desc), local_src=str(src),
                   worker_pid=receipt['pid'], worker_start_identity=receipt['process_start_identity'])
        cq.save_job(self.ctx, job)
        deadline = time.monotonic() + 7
        while time.monotonic() < deadline and not pool.observe(str(desc))['empty_confirmed']:
            time.sleep(.03)
        self.assertTrue(pool.observe(str(desc))['empty_confirmed'])
        self.assertIn('other', pool.reservations())
        with patch.object(cq, 'drop_ref'):
            fq._local_reconcile(self.ctx, job)
            saved = self.job(jid)
            self.assertEqual(saved['status'], 'done')
            self.assertTrue(saved['artifact_missing'])
            self.assertIn('pool_completion_proof', saved)
            self.assertFalse(windows_pool.observe(str(desc))['empty_confirmed'])
            (src / 'result.txt').write_text('recovered')
            helper = ("import sys,json,cq,fleet_queue as f;"
                      "c=cq.Ctx(state=sys.argv[1],sync=True);c.generation=2;f.bind(cq);"
                      "cq.job_file=lambda c,j,deferred=False:c.p('fleet','jobs',j+'.json');"
                      "cq.load_job=lambda c,j:cq.read_json(c.p('fleet','jobs',j+'.json'));"
                      "cq.drop_ref=lambda *a:None;"
                      "print(json.dumps(f._recover_local(c,cq.load_job(c,sys.argv[2]))))")
            refresh = subprocess.run([sys.executable, '-c', helper, str(self.state), jid],
                                     cwd=Path(__file__).resolve().parent,
                                     capture_output=True, text=True, timeout=10, creationflags=0x08000000)
            self.assertEqual(refresh.returncode, 0, refresh.stderr)
            self.assertTrue(json.loads(refresh.stdout)['ok'])
            saved = self.job(jid)
            original_proof = dict(saved['pool_completion_proof'])
            saved['pool_completion_proof'] = dict(original_proof, nonce='tampered')
            cq.save_job(self.ctx, saved)
            self.assertFalse(fq._recover_local(self.ctx, saved)['ok'])
            saved['pool_completion_proof'] = original_proof
            cq.save_job(self.ctx, saved)
            claim = Path(windows_pool.claim_path(str(desc)))
            original = claim.read_bytes()
            replacement = claim.with_name('replacement')
            replacement.write_bytes(original)
            os.replace(replacement, claim)
            self.assertFalse(fq._recover_local(self.ctx, saved)['ok'])
            self.assertIn('other', pool.reservations())

    def test_remote_probe_cache_keeps_original_timestamp(self):
        remote = self.remote()
        windows = types.SimpleNamespace(probe=lambda: {'supported': False})
        with patch.object(fq, '_hosts', return_value={'ember': (str(self.state), {'address': 'ember'})}), \
                patch.object(fq, '_remote_probe', return_value=remote) as probe, \
                patch.dict(sys.modules, {'windows_pool': windows}):
            first = fq._probes(self.ctx)['ember']
            self.clock[0] += 29
            second = fq._probes(self.ctx)['ember']
            self.assertEqual(first['at'], second['at'])
            self.assertEqual(probe.call_count, 1)
            self.clock[0] += 1
            fq._probes(self.ctx)
            self.assertEqual(probe.call_count, 2)

    def test_wait_reasons_are_cached_and_exposed(self):
        jid = self.submit(candidates=['ember', 'hearth'], memory_gb=2, memory_max_gb=10)['job']
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.remote(avail=12), 'hearth': None}), [])
        self.assertEqual(self.job(jid)['placement_reasons'], {'ember': 'RAM floor', 'hearth': 'offline or unconfigured'})
        response = fq.augment(self.ctx, 'status', {}, {'ok': True, 'queue': [], 'recent': []})
        self.assertIn('ember: RAM floor', response['queue'][0]['wait_reason'])

    def test_a_result_answered_to_its_owner_is_marked_delivered_and_brief(self):
        seen = {'seen_by': {'root': str(self.root), 'session': 's1', 'vendor': 'codex'}}
        waited, read, other = (self.submit()['job'] for _ in range(3))
        self.assertNotIn('host_config', fq.execute(self.ctx, 'status', {'job': waited, 'compact': True, **seen})['job'])
        self.assertFalse(self.job(waited).get('notified'))   # still queued: looking at it delivers nothing
        for jid in (waited, read, other):
            job = self.job(jid)
            job.update(status='failed', exit=1, tail=['boom'], artifact_digests=[{'path': 'r.txt', 'sha256': 'a' * 64}])
            cq.save_job(self.ctx, job)
        answer = fq.execute(self.ctx, 'wait', {'job': waited, 'timeout_s': 1, **seen})['job']
        self.assertEqual((answer['status'], answer['exit'], answer['tail'], answer['artifacts_verified']), ('failed', 1, ['boom'], 1))
        self.assertNotIn('artifact_digests', answer)
        self.assertEqual((self.job(waited)['notified'], self.job(waited)['notified_via']), (True, 'wait'))
        fq.execute(self.ctx, 'status', {'job': read, 'compact': True, **seen})
        self.assertEqual(self.job(read)['notified_via'], 'status')
        # not the owner's session, or no caller named: the notice stays due; and the full record is still served
        fq.execute(self.ctx, 'wait', {'job': other, 'timeout_s': 1, 'seen_by': {'root': str(self.root), 'session': 's2', 'vendor': 'codex'}})
        self.assertIn('worktree_identity', fq.execute(self.ctx, 'status', {'job': other})['job'])
        self.assertFalse(self.job(other).get('notified'))

    def test_local_failure_includes_bounded_log_tail(self):
        jid = self.submit(platform='windows', candidates=['laptop'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
        run = self.state / 'tail'; run.mkdir()
        src = run / 'src'; src.mkdir()
        desc = run / 'descriptor.json'; desc.write_text('{}')
        (run / 'run.log').write_text('\n'.join('line ' + str(i) for i in range(100)) + '\nFAILURE DETAIL\n')
        (run / 'result.json').write_text(json.dumps(dict(phase='failed', active_processes=0, exit=1,
                                                        worker_pid=111, worker_start_filetime=222)))
        job.update(worker_pid=111, worker_start_identity=222, descriptor_path=str(desc), local_src=str(src))
        cq.save_job(self.ctx, job)
        worker = types.SimpleNamespace(observe=lambda *args: {'empty_confirmed': True})
        with patch.dict(sys.modules, {'windows_pool': worker}), patch.object(cq, 'drop_ref'):
            fq._local_reconcile(self.ctx, job)
        finished = self.job(jid)
        self.assertEqual(finished['status'], 'failed')
        self.assertEqual(finished['tail'][-1], 'FAILURE DETAIL')
        self.assertLessEqual(len(finished['tail']), cq.TAIL_LINES)
        self.assertEqual(finished['tail_digest'], cq.stored_tail_digest(finished['tail']))

    def test_portable_python_only_rewrites_first_literal(self):
        jid = self.submit(platform='portable', command='python -m pytest && python another.py')['job']
        job = self.job(jid)
        self.assertIn('python', job['requires'])
        self.assertEqual(fq._execution_command(job, 'ember'), 'python3 -m pytest && python another.py')
        self.assertTrue(fq._execution_command(job, 'laptop').endswith(' -m pytest && python another.py'))
        self.assertEqual(fq._execution_command(dict(job, platform='linux'), 'ember'), job['command'])
        self.assertEqual(fq._execution_command(dict(job, command='env python test.py'), 'ember'), 'env python test.py')

    def test_controller_rechecks_queue_before_empty_exit(self):
        calls = []
        jid = self.submit()['job']
        job = self.job(jid)
        job['status'] = 'done'
        cq.save_job(self.ctx, job)
        def fleet_pass(ctx, futures, *extra):
            calls.append(1)
            row = self.job(jid)
            row['status'] = 'queued' if len(calls) == 1 else 'done'
            cq.save_job(ctx, row)
            return False
        with patch.object(fq, '_pass', side_effect=fleet_pass), patch.object(fq.time, 'sleep'):
            fq.controller(self.ctx)
        self.assertEqual(len(calls), 2)
        receipt = cq.read_json(self.ctx.p('fleet', 'controller.json'))
        self.assertEqual(receipt['status'], 'stopped')
        self.assertIn('heartbeat', receipt)

    def test_existing_controller_prevents_startup_work(self):
        self.submit()
        self.ctx.sync = False
        with patch.dict(os.environ, {'CQ_FLEET_AUTOSTART': '1'}), \
                cq.FileLock(self.ctx.p('fleet', 'controller.lock'), 0), \
                patch.object(fq, '_runtime') as runtime:
            fq.ensure_controller(self.ctx)
        runtime.assert_not_called()

    def test_expired_unidentified_starter_allows_replacement(self):
        self.submit()
        self.ctx.sync = False
        cq.write_json(self.ctx.p('fleet', 'controller.json'),
                      {'status': 'starting', 'pid': None, 'started': self.clock[0] - 31})
        fake_process = types.SimpleNamespace(pid=os.getpid())
        with patch.dict(os.environ, {'CQ_FLEET_AUTOSTART': '1'}), \
                patch.object(fq, '_runtime', return_value=self.ctx.p('fleet', 'runtime', 'x', 'fleet_queue.py')), \
                patch.object(cq, 'spawn_detached', return_value=fake_process) as spawn:
            fq.ensure_controller(self.ctx)
        spawn.assert_called_once()
        receipt = cq.read_json(self.ctx.p('fleet', 'controller.json'))
        self.assertEqual(receipt['pid'], os.getpid())
        self.assertEqual(receipt['status'], 'starting')

    def test_large_head_reserves_device_through_small_arrivals(self):
        head = self.submit(candidates=['ember'], slots=5)['job']
        probe = self.remote()
        probe['data']['active_fleet_slots'] = 3
        probe['data']['active_fleet'] = [{'id': 'external'}]
        for _ in range(3):
            self.clock[0] += 1
            self.submit(candidates=['ember'], slots=1)
            probe['at'] = self.clock[0]
            self.assertEqual(fq._reserve(self.ctx, {'ember': probe}), [])
        younger = [j for j in fq.all_jobs(self.ctx) if j['id'] != head]
        self.assertTrue(all(j['placement_reasons']['ember'] == 'reserved for an older eligible job' for j in younger))
        probe['data']['active_fleet_slots'] = 0
        probe['data']['active_fleet'] = []
        selected = fq._reserve(self.ctx, {'ember': probe})
        self.assertEqual([j['id'] for j in selected], [head])

    def test_head_reservation_leaves_other_devices_usable(self):
        self.submit(candidates=['ember'], slots=5)
        self.clock[0] += 1
        small = self.submit(candidates=['ember', 'hearth'], slots=1)['job']
        busy = self.remote()
        busy['data']['active_fleet_slots'] = 3
        busy['data']['active_fleet'] = [{'id': 'external'}]
        selected = fq._reserve(self.ctx, {'ember': busy, 'hearth': self.remote('hearth')})
        self.assertEqual([(j['id'], j['host_id']) for j in selected], [(small, 'hearth')])

    def test_tick_and_startup_wake_persisted_work_without_new_submit(self):
        self.submit()
        with patch.object(fq, 'ensure_controller') as ensure:
            for command in ('tick', 'ensure_deferred'):
                fq.augment(self.ctx, command, {}, {'ok': True})
            self.assertEqual(ensure.call_count, 2)
            for command in ('status', 'list', 'watch'):
                fq.augment(self.ctx, command, {}, {'ok': True})
            self.assertEqual(ensure.call_count, 2)

    def test_unattempted_reservation_resumes_only_original_endpoint_with_backoff(self):
        jid = self.submit(candidates=['ember', 'hearth'])['job']
        job = fq._reserve(self.ctx, {'ember': self.remote(), 'hearth': self.remote('hearth', 16)})[0]
        chosen_host = job['host_id']
        probe = self.remote(chosen_host, 16 if chosen_host == 'hearth' else 8)
        resumed = fq._resume_pending(self.ctx, set(), {chosen_host: probe})
        self.assertEqual([(j['id'], j['host_id']) for j in resumed], [(jid, chosen_host)])
        self.assertEqual(fq._resume_pending(self.ctx, set(), {chosen_host: probe}), [])
        for _ in range(5):
            fq._stage_error(self.ctx, jid, OSError('preparation failed'))
        self.clock[0] += 500
        self.assertEqual(fq._resume_pending(self.ctx, set(), {chosen_host: probe}), [])
        self.assertTrue(self.job(jid)['preparation_blocked'])
        self.assertEqual(self.job(jid)['host_id'], chosen_host)

    def test_same_descriptor_claim_controls_windows_relaunch_and_adoption(self):
        jid = self.submit(platform='windows', candidates=['laptop'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
        run = self.state / 'claim'; run.mkdir()
        desc = run / 'descriptor.json'
        descriptor = {'id': jid, 'nonce': 'a' * 32, 'boot_identity': 'boot-a'}
        desc.write_text(json.dumps(descriptor))
        claim_file = run / 'worker-claim.json'
        job.update(stage_attempted=True, worker_claim_protocol='v2', descriptor_path=str(desc), worker_nonce='a' * 32,
                   descriptor_hash=fq._descriptor_digest(descriptor), boot_identity='boot-a',
                   t_stage_attempt=self.clock[0] - 31, last_laptop_spawn=self.clock[0] - 31, laptop_spawn_attempts=1)
        cq.save_job(self.ctx, job)
        worker = types.SimpleNamespace(claim_path=lambda _: str(claim_file), read_claim=lambda _: None,
                                       process_identity=lambda _: 222, boot_identity=lambda: 'boot-a')
        with patch.object(cq, 'spawn_detached', return_value=types.SimpleNamespace(pid=111)) as spawn:
            self.assertIsNone(fq._claim_or_restart(self.ctx, job, worker))
            self.assertEqual(spawn.call_args.args[0][-1], str(desc))
            self.assertNotIn('worker_pid', self.job(jid))
            claim_file.write_text('invalid or partially written')
            self.clock[0] += 61
            self.assertIsNone(fq._claim_or_restart(self.ctx, self.job(jid), worker))
            self.assertEqual(spawn.call_count, 1)
            worker.read_claim = lambda _: {'id': jid, 'nonce': 'a' * 32, 'worker_pid': 333, 'worker_start_filetime': 444,
                                           'descriptor_hash': fq._descriptor_digest(descriptor)}
            adopted = fq._claim_or_restart(self.ctx, self.job(jid), worker)
            self.assertEqual((adopted['worker_pid'], adopted['worker_start_identity']), (333, 444))
            self.assertEqual(adopted['launcher_pid'], 111)
            self.assertEqual(spawn.call_count, 1)

    def test_windows_missing_claim_after_reboot_never_replays(self):
        jid = self.submit(platform='windows', candidates=['laptop'], gets=['result.txt'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
        run = self.state / 'reboot'; run.mkdir()
        desc = run / 'descriptor.json'
        descriptor = {'id': jid, 'nonce': 'b' * 32, 'boot_identity': 'boot-old'}
        desc.write_text(json.dumps(descriptor))
        job.update(stage_attempted=True, worker_claim_protocol='v2', descriptor_path=str(desc), worker_nonce='b' * 32,
                   descriptor_hash=fq._descriptor_digest(descriptor), boot_identity='boot-old',
                   t_stage_attempt=self.clock[0]-300, last_laptop_spawn=self.clock[0]-300)
        cq.save_job(self.ctx, job)
        worker = types.SimpleNamespace(claim_path=lambda _: str(run / 'worker-claim.json'), read_claim=lambda _: None,
                                       process_identity=lambda _: 222, boot_identity=lambda: 'boot-new')
        with patch.object(cq, 'spawn_detached') as spawn, patch.object(cq, 'drop_ref') as drop:
            self.assertIsNone(fq._claim_or_restart(self.ctx, job, worker))
        spawn.assert_not_called()
        drop.assert_not_called()
        result = self.job(jid)
        self.assertEqual(result['status'], 'failed')
        self.assertIn('rebooted', result['reason'])
        self.assertEqual(result['artifact_missing'][0]['path'], 'result.txt')

    def test_windows_descriptor_change_and_missing_boot_hold(self):
        jid = self.submit(platform='windows', candidates=['laptop'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
        run = self.state / 'tamper'; run.mkdir()
        desc = run / 'descriptor.json'
        descriptor = {'id': jid, 'nonce': 'c' * 32, 'boot_identity': 'boot-a'}
        desc.write_text(json.dumps(descriptor))
        job.update(stage_attempted=True, worker_claim_protocol='v2', descriptor_path=str(desc), worker_nonce='c' * 32,
                   descriptor_hash=fq._descriptor_digest(descriptor), boot_identity='boot-a',
                   t_stage_attempt=self.clock[0]-300, last_laptop_spawn=self.clock[0]-300)
        cq.save_job(self.ctx, job)
        worker = types.SimpleNamespace(claim_path=lambda _: str(run / 'worker-claim.json'), read_claim=lambda _: None,
                                       process_identity=lambda _: 222, boot_identity=lambda: None)
        with patch.object(cq, 'spawn_detached') as spawn:
            self.assertIsNone(fq._claim_or_restart(self.ctx, job, worker))
            descriptor['extra'] = 'changed'
            desc.write_text(json.dumps(descriptor))
            worker.boot_identity = lambda: 'boot-a'
            self.assertIsNone(fq._claim_or_restart(self.ctx, job, worker))
        spawn.assert_not_called()
        self.assertEqual(self.job(jid)['status'], 'launching')

    def test_valid_claim_with_terminal_result_survives_boot_change(self):
        jid = self.submit(platform='windows', candidates=['laptop'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
        run = self.state / 'reboot-complete'; run.mkdir()
        src = run / 'src'; src.mkdir()
        desc = run / 'descriptor.json'
        descriptor = {'id': jid, 'nonce': 'd' * 32, 'boot_identity': 'boot-old'}
        desc.write_text(json.dumps(descriptor))
        (run / 'result.json').write_text(json.dumps(dict(phase='done', active_processes=0, exit=0,
                                                        worker_pid=333, worker_start_filetime=444)))
        job.update(stage_attempted=True, worker_claim_protocol='v2', descriptor_path=str(desc), local_src=str(src),
                   worker_nonce='d' * 32, descriptor_hash=fq._descriptor_digest(descriptor), boot_identity='boot-old')
        cq.save_job(self.ctx, job)
        worker = types.SimpleNamespace(claim_path=lambda _: str(run / 'worker-claim.json'),
                 read_claim=lambda _: {'id': jid, 'nonce': 'd' * 32, 'worker_pid': 333, 'worker_start_filetime': 444,
                                        'descriptor_hash': fq._descriptor_digest(descriptor)},
                 process_identity=lambda _: 222, boot_identity=lambda: 'boot-new',
                 observe=lambda *args: {'empty_confirmed': True})
        with patch.dict(sys.modules, {'windows_pool': worker}), patch.object(cq, 'drop_ref'):
            fq._local_reconcile(self.ctx, job)
        self.assertEqual(self.job(jid)['status'], 'done')

    def test_missing_laptop_boot_identity_excludes_device(self):
        worker = types.SimpleNamespace(probe=lambda: {'supported': True, 'logical_cpus': 16, 'available_gb': 20,
                         'commit_free_gb': 20, 'total_gb': 32, 'boot_identity': None},
                         process_identity=lambda _: 1, observe=lambda *args: {}, read_claim=lambda _: None,
                         claim_path=lambda _: '', boot_identity=lambda: None)
        with patch.object(fq, '_hosts', return_value={}), patch.dict(sys.modules, {'windows_pool': worker}):
            self.assertIsNone(fq._probes(self.ctx).get('laptop'))

    def test_capacity_eligibility_reflects_floors_load_and_legacy(self):
        laptop = self.laptop(available=6.2, commit=20)
        remote = self.remote(load=19)
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            fq._capacity(self.ctx, {'laptop': laptop, 'ember': remote}, [])
            devices = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']
            self.assertFalse(devices['laptop']['eligible'])
            self.assertIn('RAM floor', devices['laptop']['reason'])
            self.assertFalse(devices['ember']['eligible'])
            self.assertEqual(devices['ember']['reason'], 'host load cap')
            self.assertEqual(devices['laptop']['slots_reserved'], 0)
            self.assertEqual(devices['laptop']['slots_configured'], devices['laptop']['slots_total'])
            self.assertEqual(devices['ember']['slots_reserved'], 2)
            self.assertEqual(devices['ember']['slots_configured'], devices['ember']['slots_total'] + 2)
            laptop['data']['available_gb'] = 20
            legacy = {'id': 'cq-legacy', 'status': 'queued', 'where': 'laptop', 'slots': 1}
            cq.save_job(self.ctx, legacy)
            fq._capacity(self.ctx, {'laptop': laptop}, [])
            devices = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']
            self.assertFalse(devices['laptop']['eligible'])
            self.assertIn('legacy', devices['laptop']['reason'])

    def test_idle_finalization_removes_only_confirmed_terminal_reservations(self):
        first = self.submit(candidates=['ember'])['job']
        second = self.submit(candidates=['hearth'])['job']
        for jid, host in ((first, 'ember'), (second, 'hearth')):
            job = self.job(jid)
            job.update(status='done', host_id=host, host_config={'machine_id': 'a'*32}, t_finished=self.clock[0])
            cq.save_job(self.ctx, job)
        ember, hearth = self.remote('ember'), self.remote('hearth', 8)
        half_gib = fq.GIB // 2
        ember['data'].update(active_fleet_slots=1, reserved_memory_bytes=half_gib,
                             active_fleet=[dict(id=first, slots=1, reserved_memory_bytes=half_gib)])
        hearth['data'].update(active_fleet_slots=2, reserved_memory_bytes=fq.GIB,
                              active_fleet=[dict(id=second, slots=1, reserved_memory_bytes=half_gib),
                                            dict(id='external-job', slots=1, reserved_memory_bytes=half_gib)])
        previous_ember = json.dumps(ember, sort_keys=True)
        sampled = self.clock[0]
        self.ctx._fleet_pass_probes = {'ember': ember, 'hearth': hearth, 'laptop': self.laptop()}
        self.clock[0] += 60
        with patch.object(fq, '_probes', side_effect=AssertionError('unexpected probe')), \
                patch.object(cq, 'job_ssh', side_effect=AssertionError('unexpected SSH')), \
                patch.object(fq, '_laptop_admission', side_effect=AssertionError('unexpected local refresh')), \
                patch.object(self.ctx.laptop, 'heavy_lock_held', side_effect=AssertionError('unexpected lock probe')):
            fq.controller(self.ctx)
        devices = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']
        self.assertEqual((devices['ember']['slots_used'], devices['ember']['memory_reserved_gb']), (0, 0))
        self.assertEqual((devices['hearth']['slots_used'], devices['hearth']['memory_reserved_gb']), (1, .5))
        self.assertEqual(devices['ember']['at'], sampled)
        self.assertEqual(devices['hearth']['at'], sampled)
        self.assertFalse(devices['ember']['eligible'])  # original hardware sample is stale
        self.assertEqual(json.dumps(ember, sort_keys=True), previous_ember)
        self.assertEqual(cq.read_json(self.ctx.p('fleet', 'controller.json'))['status'], 'stopped')

    def test_idle_finalization_preserves_mismatched_machine_reservation(self):
        jid = self.submit(candidates=['ember'])['job']
        job = self.job(jid)
        job.update(status='done', host_id='ember', host_config={'machine_id': 'b'*32}, t_finished=self.clock[0])
        cq.save_job(self.ctx, job)
        probe = self.remote('ember')
        probe['data'].update(active_fleet_slots=1, reserved_memory_bytes=fq.GIB // 2,
                             active_fleet=[dict(id=jid, slots=1, reserved_memory_bytes=fq.GIB // 2)])
        self.ctx._fleet_pass_probes = {'ember': probe}
        with self.ctx.qlock():
            fq._finalize_capacity(self.ctx, fq.all_jobs(self.ctx))
        device = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']['ember']
        self.assertEqual((device['slots_used'], device['memory_reserved_gb']), (1, .5))

    def test_failed_spawn_leaves_pinned_descriptor_for_same_boot_retry(self):
        jid = self.submit(platform='windows', candidates=['laptop'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
        def checkout(top, sha, dest):
            os.makedirs(dest, exist_ok=True)
            return []
        worker = types.SimpleNamespace(process_identity=lambda _: 123, observe=lambda *args: {},
                 read_claim=lambda _: None, claim_path=lambda p: str(Path(p).parent / 'worker-claim.json'),
                 boot_identity=lambda: 'boot-a', probe=lambda: dict(supported=True, logical_cpus=16,
                 available_gb=20, commit_free_gb=20, total_gb=32, boot_identity='boot-a', capabilities=[]))
        with patch.dict(sys.modules, {'windows_pool': worker}), patch.object(cq, 'checkout_tree', side_effect=checkout), \
                patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False), \
                patch.object(cq, 'spawn_detached', side_effect=OSError('lost receipt')):
            fq._stage_laptop(self.ctx, job)
        after = self.job(jid)
        self.assertTrue(after['stage_attempted'])
        self.assertEqual(after['worker_claim_protocol'], 'v2')
        self.assertEqual(after['boot_identity'], 'boot-a')
        self.assertEqual(after['descriptor_hash'], fq._descriptor_digest(cq.read_json(after['descriptor_path'])))
        self.clock[0] += 31
        with patch.object(cq, 'spawn_detached', return_value=types.SimpleNamespace(pid=789)) as spawn:
            fq._claim_or_restart(self.ctx, after, worker)
        self.assertEqual(spawn.call_args.args[0][-1], after['descriptor_path'])

    def test_legacy_arriving_during_checkout_defers_and_reuses_preparation(self):
        jid = self.submit(platform='windows', candidates=['laptop'])['job']
        worker = types.SimpleNamespace(process_identity=lambda _: 123, observe=lambda *args: {},
                 read_claim=lambda _: None, claim_path=lambda p: str(Path(p).parent / 'worker-claim.json'),
                 boot_identity=lambda: 'boot-a', probe=lambda: dict(supported=True, logical_cpus=16,
                 available_gb=20, commit_free_gb=20, total_gb=32, boot_identity='boot-a', capabilities=[]))
        legacy = {'id': 'cq-legacy', 'status': 'queued', 'where': 'laptop', 'slots': 1}
        def checkout(top, sha, dest):
            os.makedirs(dest, exist_ok=True)
            cq.save_job(self.ctx, legacy)
            return []
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False), \
                patch.dict(sys.modules, {'windows_pool': worker}), patch.object(cq, 'checkout_tree', side_effect=checkout) as clone, \
                patch.object(cq, 'spawn_detached', return_value=types.SimpleNamespace(pid=789)) as spawn:
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
            fq._stage_laptop(self.ctx, job)
            waiting = self.job(jid)
            self.assertFalse(waiting.get('stage_attempted', False))
            self.assertTrue(Path(waiting['prepared_descriptor']).is_file())
            spawn.assert_not_called()
            legacy['status'] = 'done'; cq.save_job(self.ctx, legacy)
            fq._stage_laptop(self.ctx, waiting)
            spawn.assert_not_called()
            self.clock[0] += 2
            fq._stage_laptop(self.ctx, self.job(jid))
            spawn.assert_called_once()
            clone.assert_called_once()

    def test_prepared_descriptor_survives_crash_before_attempt_save(self):
        jid = self.submit(platform='windows', candidates=['laptop'])['job']
        worker = types.SimpleNamespace(process_identity=lambda _: 123, observe=lambda *args: {},
                 read_claim=lambda _: None, claim_path=lambda p: str(Path(p).parent / 'worker-claim.json'),
                 boot_identity=lambda: 'boot-a', probe=lambda: dict(supported=True, logical_cpus=16,
                 available_gb=20, commit_free_gb=20, total_gb=32, boot_identity='boot-a', capabilities=[]))
        legacy = {'id': 'cq-legacy', 'status': 'queued', 'where': 'laptop', 'slots': 1}
        def checkout(top, sha, dest):
            os.makedirs(dest, exist_ok=True)
            cq.save_job(self.ctx, legacy)
            return []
        real_save = cq.save_job
        tripped = []
        def crash_before_attempt_save(ctx, value):
            if value.get('stage_attempted') and not tripped:
                final = cq.read_json(value['descriptor_path'])
                self.assertEqual(final['ram_floor_gb'], 7)
                tripped.append(value['descriptor_path'])
                raise OSError('simulated crash after final descriptor write')
            return real_save(ctx, value)
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False), \
                patch.dict(sys.modules, {'windows_pool': worker}), patch.object(cq, 'checkout_tree', side_effect=checkout) as clone, \
                patch.object(cq, 'spawn_detached', return_value=types.SimpleNamespace(pid=789)) as spawn:
            job = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
            fq._stage_laptop(self.ctx, job)
            waiting = self.job(jid)
            prep_path = Path(waiting['prepared_descriptor'])
            prep_before = prep_path.read_bytes()
            legacy['status'] = 'done'; cq.save_job(self.ctx, legacy)
            fq._stage_laptop(self.ctx, waiting)  # enter quiet gap
            self.clock[0] += 2
            cq.write_json(self.ctx.p('fleet', 'config.json'), {'ram_floor_gb': 7})
            with patch.object(cq, 'save_job', side_effect=crash_before_attempt_save):
                fq._stage_laptop(self.ctx, self.job(jid))
            self.assertEqual(len(tripped), 1)
            self.assertEqual(prep_path.read_bytes(), prep_before)
            self.assertFalse(self.job(jid).get('stage_attempted', False))
            spawn.assert_not_called()
            self.clock[0] += 31
            fq._stage_laptop(self.ctx, self.job(jid))  # next cohort enters quiet gap
            self.clock[0] += 2
            stale_waiting = self.job(jid)
            fq._stage_laptop(self.ctx, stale_waiting)
            final = self.job(jid)
            self.assertTrue(final['stage_attempted'])
            self.assertNotEqual(final['descriptor_path'], tripped[0])
            self.assertNotEqual(final['descriptor_path'], str(prep_path))
            self.assertEqual(final['descriptor_hash'], fq._descriptor_digest(cq.read_json(final['descriptor_path'])))
            fq._stage_laptop(self.ctx, stale_waiting)
            spawn.assert_called_once()
            clone.assert_called_once()

    def test_resumed_remote_waits_for_pinned_fresh_host(self):
        self.submit(candidates=['ember'])
        job = fq._reserve(self.ctx, {'ember': self.remote()})[0]
        self.assertEqual(fq._resume_pending(self.ctx, set(), {}), [])
        wrong = self.remote(); wrong['binding']['machine_id'] = 'b' * 32
        self.assertEqual(fq._resume_pending(self.ctx, set(), {'ember': wrong}), [])
        stale = self.remote(); stale['at'] -= 31
        self.assertEqual(fq._resume_pending(self.ctx, set(), {'ember': stale}), [])
        self.assertEqual(fq._resume_pending(self.ctx, set(), {'ember': self.remote()})[0]['id'], job['id'])

    def test_remote_reconciliation_does_not_block_controller_pass(self):
        for _ in range(3):
            jid = self.submit(candidates=['ember'])['job']
            job = self.job(jid)
            job.update(status='launching', host_id='ember', host='ember', host_config={'machine_id': 'a'*32},
                       stage_attempted=True, deferred_registered=True, launch_uncertain=False)
            cq.save_job(self.ctx, job)
        gate = threading.Event()
        pending = {}
        with patch.object(fq, '_probes', return_value={'ember': self.remote()}), \
                patch.object(fq, '_remote_reconcile', side_effect=lambda *args, **kwargs: gate.wait(3)):
            with ThreadPoolExecutor(max_workers=4) as pool:
                try:
                    start = time.monotonic()
                    fq._pass(self.ctx, {}, pool, pending)
                    self.assertLess(time.monotonic() - start, 1.5)
                    self.assertEqual(len(pending), 2)
                    fq._pass(self.ctx, {}, pool, pending)
                    self.assertEqual(len(pending), 2)
                finally:
                    gate.set()

    def test_acknowledged_old_job_absent_from_probe_is_collected(self):
        jid = self.submit(candidates=['ember'])['job']
        job = fq._reserve(self.ctx, {'ember': self.remote()})[0]
        job.update(stage_attempted=True, launch_uncertain=False, deferred_registered=True)
        cq.save_job(self.ctx, job)
        ent = dict(started=self.clock[0]-10, launched=self.clock[0]-12, finished=self.clock[0], exit=0, alive=False, cpu=1)
        state = dict(version=1, id=jid, phase='done', registered=True)
        with patch.object(fq, '_probes', return_value={'ember': self.remote()}), \
                patch.object(cq, 'deferred_control', return_value=state), \
                patch.object(cq, 'host_snapshot', return_value=({'jobs': {jid: ent}}, None)), \
                patch.object(cq, 'fetch_results', return_value=([], [])) as fetch, patch.object(cq, 'drop_ref'):
            fq._pass(self.ctx, {})
        self.assertEqual(self.job(jid)['status'], 'done')
        fetch.assert_called_once()

    def test_missing_remote_facts_reconcile_two_per_host_per_pass_fairly(self):
        ids = []
        for _ in range(5):
            jid = self.submit(candidates=['ember'])['job']
            job = self.job(jid)
            job.update(status='launching', host_id='ember', host='ember', host_config={'machine_id': 'a'*32},
                       stage_attempted=True, deferred_registered=True, launch_uncertain=False)
            cq.save_job(self.ctx, job)
            ids.append(jid)
        seen = []
        with patch.object(fq, '_probes', return_value={'ember': self.remote()}), \
                patch.object(fq, '_remote_reconcile', side_effect=lambda ctx, job, *args, **kw: seen.append(job['id'])):
            for expected in (2, 4, 5):
                fq._pass(self.ctx, {})
                self.assertEqual(len(seen), expected)
        self.assertEqual(set(seen), set(ids))

    def test_legacy_laptop_job_drains_new_fleet_admission_and_quiet_gap(self):
        first = self.submit(platform='windows', candidates=['laptop'])['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            running = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
            running.update(status='running', launch_uncertain=False)
            cq.save_job(self.ctx, running)
            second = self.submit(platform='windows', candidates=['laptop'])['job']
            legacy = {'id': 'cq-legacy', 'status': 'queued', 'where': 'laptop', 'slots': 1}
            cq.save_job(self.ctx, legacy)
            self.assertEqual(fq._reserve(self.ctx, {'laptop': self.laptop()}), [])
            legacy['status'] = 'done'; cq.save_job(self.ctx, legacy)
            running['status'] = 'done'; cq.save_job(self.ctx, running)
            self.assertEqual(fq._reserve(self.ctx, {'laptop': self.laptop()}), [])
            self.clock[0] += 1
            self.assertEqual(fq._reserve(self.ctx, {'laptop': self.laptop()}), [])
            self.clock[0] += 1
            self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'laptop': self.laptop()})], [second])
        self.assertEqual(self.job(first)['status'], 'done')

    def test_laptop_cohort_expiry_survives_controller_restart(self):
        self.submit(platform='windows', candidates=['laptop'])
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            running = fq._reserve(self.ctx, {'laptop': self.laptop()})[0]
            running.update(status='running', launch_uncertain=False)
            cq.save_job(self.ctx, running)
            second = self.submit(platform='windows', candidates=['laptop'])['job']
            self.clock[0] += 31
            self.assertEqual(fq._reserve(self.ctx, {'laptop': self.laptop()}), [])
            restarted = cq.Ctx(state=str(self.state), now=lambda: self.clock[0], sync=True, laptop=self.ctx.laptop)
            self.assertEqual(fq._reserve(restarted, {'laptop': self.laptop()}), [])
            running['status'] = 'done'; cq.save_job(self.ctx, running)
            self.assertEqual(fq._reserve(restarted, {'laptop': self.laptop()}), [])
            self.clock[0] += 2
            self.assertEqual([j['id'] for j in fq._reserve(restarted, {'laptop': self.laptop()})], [second])


class FleetIntegratedSubmissionTests(unittest.TestCase):
    def test_many_processes_start_only_one_controller(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / 'state'
            jobs = state / 'fleet' / 'jobs'
            jobs.mkdir(parents=True)
            (jobs / 'job.json').write_text(json.dumps({'id': 'cq-fleet-20261110T080000-0123456789abcdef',
                                                       'fleet': True, 'status': 'queued'}))
            marker = Path(directory) / 'started.txt'
            script = '''import os, subprocess, sys
sys.path.insert(0, sys.argv[1])
import cq, fleet_queue as f
f.bind(cq)
f._runtime = lambda c: os.path.join(c.state, "fleet", "runtime", "x", "fleet_queue.py")
def spawn(argv, stdout=None, env=None):
    assert os.path.normcase(os.path.abspath(env['CQ_STATE'])) == os.path.normcase(os.path.abspath(sys.argv[2]))
    with open(sys.argv[3], "a", encoding="utf-8") as out:
        out.write("start\\n")
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(2)"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=env, creationflags=(0x08000000 if os.name == "nt" else 0))
cq.spawn_detached = spawn
ctx = cq.Ctx(state=sys.argv[2])
for _ in range(5): f.ensure_controller(ctx)
'''
            env = dict(os.environ, CQ_FLEET_AUTOSTART='1')
            procs = [subprocess.Popen([sys.executable, '-c', script, str(Path(fq.__file__).parent), str(state), str(marker)],
                                      env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                      creationflags=(0x08000000 if os.name == 'nt' else 0)) for _ in range(8)]
            outputs = [process.communicate(timeout=30) for process in procs]
            for process, (out, err) in zip(procs, outputs):
                self.assertEqual(process.returncode, 0, (out + err).decode('utf-8', 'replace'))
            self.assertEqual(marker.read_text().splitlines(), ['start'])
            receipt = json.loads((state / 'fleet' / 'controller.json').read_text())
            self.assertEqual(receipt['status'], 'starting')
            self.assertIsInstance(receipt['pid'], int)

    def test_real_git_snapshot_and_primary_routing(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'CQ_FLEET_AUTOSTART': '0'}):
            root = Path(directory) / 'repo'
            root.mkdir()
            subprocess.run(['git', 'init', '-q', str(root)], check=True, capture_output=True)
            (root / 'tracked.txt').write_text('baseline')
            subprocess.run(['git', '-C', str(root), 'add', 'tracked.txt'], check=True, capture_output=True)
            subprocess.run(['git', '-C', str(root), '-c', 'user.name=test', '-c', 'user.email=test@example.invalid',
                            'commit', '-qm', 'baseline'], check=True, capture_output=True)
            (root / '.baton').mkdir()
            (root / '.baton' / 'owner.json').write_text(json.dumps({'session_id': 'owner', 'vendor': 'codex'}))
            ctx = cq.Ctx(state=str(Path(directory) / 'state'), sync=True)
            response = cq.execute(ctx, 'submit', dict(root=str(root), session='owner', command='echo ok',
                                                      where='fleet', source='head', slots=1))
            jid = response['job']
            self.assertTrue(jid.startswith('cq-v2--fleet-'))
            self.assertTrue(Path(ctx.p('queue-v2', 'fleet', 'jobs', jid + '.json')).is_file())
            self.assertEqual(cq.execute(ctx, 'get', {'job': jid})['job']['status'], 'queued')
            self.assertEqual(cq.all_jobs(ctx), [])
            self.assertEqual(cq.execute(ctx, 'cancel', {'job': jid, 'root': str(root), 'session': 'owner'})['status'], 'cancelled')


if __name__ == '__main__':
    unittest.main()
