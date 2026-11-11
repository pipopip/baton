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

    def test_fleet_submit_and_cancel_name_their_worktree_and_session(self):
        jobs = self.state / 'fleet' / 'jobs'
        before = sorted(p.name for p in jobs.glob('*.json')) if jobs.exists() else []
        for drop in ('session', 'root'):
            req = dict(where='fleet', root=str(self.root), command='echo test', session='s1', slots=1)
            del req[drop]
            for call in (lambda: fq.execute(self.ctx, 'submit', req), lambda: cq.execute(self.ctx, 'submit', req)):
                with self.assertRaisesRegex(cq.CqError, 'submission requires ' + drop):
                    call()
        self.assertEqual(sorted(p.name for p in jobs.glob('*.json')) if jobs.exists() else [], before)
        jid = self.submit()['job']
        original = self.job(jid)
        for req in (dict(job=jid), dict(job=jid, root=str(self.root)), dict(job=jid, session='s1')):
            for call in (lambda: fq.execute(self.ctx, 'cancel', req), lambda: cq.execute(self.ctx, 'cancel', req)):
                with self.assertRaisesRegex(cq.CqError, 'cancellation requires'):
                    call()
        other = self.state / 'other-worktree'; other.mkdir()
        with patch.object(cq, '_top_of', return_value=str(other)), self.assertRaisesRegex(cq.CqError, 'does not match'):
            fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(other), session='s1'))
        with self.assertRaisesRegex(cq.CqError, 'owner'):
            fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(self.root), session='not-owner'))
        self.assertEqual(self.job(jid), original)
        self.assertEqual(fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(self.root), session='s1'))['status'], 'cancelled')

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

    def test_watch_reports_this_worktrees_undelivered_failures_for_the_status_line(self):
        jid = self.submit()['job']
        req = {'root': str(self.root), 'watch_session': 's1'}
        with patch.object(fq, '_probes', side_effect=AssertionError('unexpected network')), patch.object(fq, 'ensure_controller'):
            queued = fq.augment(self.ctx, 'tick', req, {'ok': True})['watch']
            job = cq.read_json(self.ctx.p('fleet', 'jobs', jid + '.json'))
            job.update(status='failed', exit=1)
            cq.write_json(self.ctx.p('fleet', 'jobs', jid + '.json'), job)
            failed = fq.augment(self.ctx, 'tick', req, {'ok': True})['watch']
            job.update(notified=True)
            cq.write_json(self.ctx.p('fleet', 'jobs', jid + '.json'), job)
            delivered = fq.augment(self.ctx, 'tick', req, {'ok': True})['watch']
            stranger = fq.augment(self.ctx, 'tick', {**req, 'watch_session': 'someone-else'}, {'ok': True})['watch']
        self.assertEqual(queued['mine_attention'], {'failed': 0, 'missing': 0})
        self.assertEqual(failed['mine_attention'], {'failed': 1, 'missing': 0})
        self.assertEqual(delivered['mine_attention'], {'failed': 0, 'missing': 0})
        self.assertNotIn('mine_attention', stranger)

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

    def running_on_ember(self, slots=3, est=20, started_ago=600, memory=2, **fields):
        """A fleet job this store knows to be running on ember, and the probe that reports it."""
        jid = self.submit(candidates=['ember'], slots=slots, est_minutes=est, memory_gb=memory, memory_max_gb=memory)['job']
        job = self.job(jid)
        job.update(status='running', host_id='ember', host='ember', host_config={'machine_id': 'a' * 32},
                   stage_attempted=True, deferred_registered=True, deferred_phase='running',
                   t_launch=self.clock[0] - started_ago, t_started=self.clock[0] - started_ago, **fields)
        cq.save_job(self.ctx, job)
        probe = self.remote()
        probe['data'].update(active_fleet_slots=slots, reserved_memory_bytes=int(memory * fq.GIB),
                             active_fleet=[{'id': jid, 'slots': slots, 'reserved_memory_bytes': int(memory * fq.GIB)}])
        return jid, probe

    def queued(self, jid):
        job = self.job(jid)
        return job['status'], (job.get('placement_reasons') or {}).get('ember')

    def test_a_smaller_job_stands_ahead_of_a_larger_one_in_its_class_for_a_bounded_time(self):
        self.assertEqual(fq._size_delay({'est_minutes': 3, 'slots': 1}), 90)
        self.assertEqual(fq._size_delay({'est_minutes': 90, 'slots': 5}), 7200)   # capped at 240 slot-minutes
        self.assertEqual(fq._size_delay({}), 300)                                 # 10 minutes on one slot when unstated
        big = self.submit(candidates=['ember'], slots=5, est_minutes=90, priority='lane')['job']
        self.clock[0] += 600
        small = self.submit(candidates=['ember'], slots=1, est_minutes=3, priority='lane')['job']
        self.clock[0] += 7200
        late = self.submit(candidates=['ember'], slots=1, est_minutes=3, priority='lane')['job']
        urgent_big = self.submit(candidates=['ember'], slots=5, est_minutes=90, priority='verify')['job']
        order = [j['id'] for j in sorted(fq.all_jobs(self.ctx), key=fq._order)]
        # the class comes first; in a class the small job passes the large one, but one submitted after the large
        # job's two hours does not
        self.assertEqual(order, [urgent_big, small, big, late])
        # placement also lets a job that has waited an hour go first, oldest first: here both of the first two
        probe = self.remote()
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': probe})], [big])

    def test_a_job_that_has_waited_an_hour_goes_ahead_of_newer_work_of_any_class(self):
        lane = self.submit(candidates=['ember'], slots=5, est_minutes=90, priority='lane')['job']
        self.clock[0] += 10
        background = self.submit(candidates=['ember'], slots=1, est_minutes=3, priority='background')['job']
        self.clock[0] += 1
        deferred = self.submit(candidates=['ember'], slots=1, est_minutes=3, defer='spare', memory_gb=1, memory_max_gb=1)['job']
        self.clock[0] += fq.AGE_S - 12
        verify = self.submit(candidates=['ember'], slots=5, est_minutes=5, priority='verify')['job']
        now = self.clock[0]
        order = lambda: [j['id'] for j in sorted(fq.all_jobs(self.ctx), key=lambda j: fq._order(j, self.clock[0]))]
        # one second short of the hour nothing has aged: by class
        self.assertEqual(order(), [verify, lane, background, deferred])
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': self.remote()})], [verify])
        placed = self.job(verify)
        placed.update(status='queued', host_id=None)
        for key in ('t_launch', 'stage_at'):
            placed.pop(key, None)
        cq.save_job(self.ctx, placed)
        self.clock[0] += 1
        # the lane job has waited the hour: it goes first; ten seconds later the background job follows it, in the
        # order they were submitted, whatever their size; a job deferred by request does not age
        self.assertEqual(order(), [lane, verify, background, deferred])
        self.clock[0] += 10
        self.assertEqual(order(), [lane, background, verify, deferred])
        self.assertEqual([j['id'] for j in sorted(fq.all_jobs(self.ctx), key=fq._order)], [verify, lane, background, deferred])
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': self.remote()})], [lane])
        self.assertEqual(self.job(verify)['status'], 'queued')
        # an hour after its own submission the deferred job still has not aged; the verify job has, in its turn
        placed = self.job(lane)
        placed.update(status='done')
        cq.save_job(self.ctx, placed)
        self.clock[0] += fq.AGE_S
        queued = [j for j in fq.all_jobs(self.ctx) if j['status'] == 'queued']
        self.assertEqual([j['id'] for j in sorted(queued, key=lambda j: fq._order(j, self.clock[0]))], [background, verify, deferred])
        self.assertEqual(fq._order(self.job(deferred), self.clock[0]), fq._order(self.job(deferred)))
        self.assertEqual(fq._order({'id': 'x', 'priority': 'lane'}, now), fq._order({'id': 'x', 'priority': 'lane'}))  # no submission time: never aged

    def test_a_short_job_passes_a_held_host_and_a_long_one_does_not(self):
        running, probe = self.running_on_ember(slots=3, est=20, started_ago=600)   # expected to end in 10 minutes
        head = self.submit(candidates=['ember'], slots=5, est_minutes=30, priority='verify')['job']   # 5 > 6 - 3
        self.clock[0] += 1
        short = self.submit(candidates=['ember'], slots=1, est_minutes=5, priority='lane')['job']
        long = self.submit(candidates=['ember'], slots=1, est_minutes=15, priority='lane')['job']
        probe['at'] = self.clock[0]
        self.assertEqual(fq._shadow(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]), self.clock[0] - 1 + 600)
        selected = fq._reserve(self.ctx, {'ember': probe})
        self.assertEqual([j['id'] for j in selected], [short])   # gone, by its estimate, before the head could start
        self.assertEqual(self.queued(head), ('queued', 'CPU slots unavailable'))
        self.assertEqual(self.queued(long), ('queued', 'reserved for an older eligible job'))
        # the head goes first as soon as it fits, whatever else waits
        done = self.job(running)
        done.update(status='done')
        cq.save_job(self.ctx, done)
        placed = self.job(short)
        placed.update(status='done')
        cq.save_job(self.ctx, placed)
        idle = self.remote()
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': idle})], [head])

    def test_with_no_shadow_time_only_a_job_within_ten_minutes_passes(self):
        head = self.submit(candidates=['ember'], slots=5, priority='verify')['job']
        self.clock[0] += 1
        ten = self.submit(candidates=['ember'], slots=1, est_minutes=10, priority='lane')['job']
        eleven = self.submit(candidates=['ember'], slots=1, est_minutes=11, priority='lane')['job']
        probe = self.remote()
        probe['data']['active_fleet_slots'] = 3
        probe['data']['active_fleet'] = [{'id': 'external'}]   # work this store does not know: its end cannot be told
        self.assertIsNone(fq._shadow(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]))
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': probe})], [ten])
        self.assertEqual(self.queued(eleven), ('queued', 'reserved for an older eligible job'))
        self.assertEqual(self.queued(head)[0], 'queued')

    def test_a_stream_of_short_jobs_cannot_keep_the_held_job_out_for_good(self):
        head = self.submit(candidates=['ember'], slots=5, priority='verify')['job']
        probe = self.remote()
        probe['data']['active_fleet_slots'] = 3
        probe['data']['active_fleet'] = [{'id': 'external'}]   # no shadow time can be worked out
        placed = []
        for minute in range(40):
            self.clock[0] += 60
            arrival = self.submit(candidates=['ember'], slots=1, est_minutes=3, priority='lane')['job']
            probe['at'] = self.clock[0]
            for job in fq._reserve(self.ctx, {'ember': probe}):
                placed.append((minute, job['id']))
                done = self.job(job['id'])
                done.update(status='done')   # it runs and ends before the next pass: the slot is free again
                cq.save_job(self.ctx, done)
        # short jobs pass for the patience time after the hold began, then nothing does
        self.assertTrue(placed)
        self.assertLessEqual(max(minute for minute, _ in placed) * 60, fq.HOLD_PATIENCE_S)
        self.assertEqual(self.queued(arrival), ('queued', 'reserved for an older eligible job'))
        held = self.job(head)
        self.assertEqual((held['status'], held['hold_since']), ('queued', 1700000000.0 + 60))
        # the head takes the host as soon as it fits, and its patience mark goes with it
        idle = self.remote()
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': idle})], [head])
        self.assertNotIn('hold_since', self.job(head))

    def test_short_jobs_cannot_keep_passing_while_the_job_that_must_end_overruns(self):
        # the head waits for a job already past its estimate: its allowance is counted from now, so it is no shadow time
        running, probe = self.running_on_ember(slots=3, est=3, started_ago=240, timeout_minutes=600)
        head = self.submit(candidates=['ember'], slots=5, est_minutes=30, priority='verify')['job']
        self.assertIsNone(fq._shadow(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]))
        placed = []
        for minute in range(40):
            self.clock[0] += 60
            arrival = self.submit(candidates=['ember'], slots=1, est_minutes=3, priority='lane')['job']
            probe['at'] = self.clock[0]
            for job in fq._reserve(self.ctx, {'ember': probe}):
                placed.append((minute, job['id']))
                done = self.job(job['id'])
                done.update(status='done')
                cq.save_job(self.ctx, done)
        self.assertTrue(placed)
        self.assertLessEqual(max(minute for minute, _ in placed) * 60, fq.HOLD_PATIENCE_S)
        self.assertEqual(self.queued(arrival), ('queued', 'reserved for an older eligible job'))
        self.assertEqual(self.job(head)['status'], 'queued')
        # its timeout is a fixed moment again: a job that ends before it may pass, one that does not may not
        late = self.job(running)
        late['timeout_minutes'] = (self.clock[0] - late['t_started']) / 60 + 5
        cq.save_job(self.ctx, late)
        self.assertEqual(fq._shadow(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]), self.clock[0] + 300)
        for job in fq.all_jobs(self.ctx):
            if job['status'] == 'queued' and job['id'] != head:
                job.update(status='cancelled')
                cq.save_job(self.ctx, job)
        six = self.submit(candidates=['ember'], slots=1, est_minutes=6, priority='lane')['job']
        self.clock[0] += 1
        three = self.submit(candidates=['ember'], slots=1, est_minutes=3, priority='lane')['job']
        probe['at'] = self.clock[0]
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': probe})], [three])   # 3 minutes, within the 5
        done = self.job(three)
        done.update(status='done')
        cq.save_job(self.ctx, done)
        self.assertEqual(fq._reserve(self.ctx, {'ember': probe}), [])
        self.assertEqual(self.queued(six), ('queued', 'reserved for an older eligible job'))

    def test_the_cap_on_new_reservations_holds_when_a_host_is_held(self):
        self.submit(candidates=['ember'], slots=5, priority='verify')
        self.clock[0] += 1
        for host in ('ember', 'hearth'):
            self.submit(candidates=[host], slots=1, est_minutes=2, priority='lane')
        busy = self.remote()
        busy['data']['active_fleet_slots'] = 3
        busy['data']['active_fleet'] = [{'id': 'external'}]
        probes = {'ember': busy, 'hearth': self.remote('hearth')}
        self.assertEqual(len(fq._reserve(self.ctx, probes, limit=1)), 1)
        self.assertEqual(len(fq._reserve(self.ctx, probes, limit=0)), 0)

    def test_the_shadow_time_follows_estimates_overruns_and_timeouts(self):
        now = self.clock[0]
        job = lambda **f: dict(dict(t_started=now - 600, est_minutes=20), **f)
        self.assertEqual(fq._expected_end(job(), now), now + 600)
        self.assertEqual(fq._expected_end(job(t_started=now - 3600), now), now + 600)                     # past it: 10 min more
        self.assertEqual(fq._expected_end(job(t_started=now - 7200, est_minutes=90), now), now + 1350)   # or a quarter of it
        self.assertEqual(fq._expected_end(job(t_started=now - 3600, timeout_minutes=61), now), now + 60)  # never past its timeout
        self.assertEqual(fq._expected_end(job(t_started=None, t_launch=now - 60), now), now + 1140)
        self.assertEqual(fq._expected_end({}, now), now + 600)
        # a fixed moment: within the estimate, or the timeout; not the allowance past the estimate, nor an unknown start
        self.assertTrue(fq._end_is_firm(job(), now))
        self.assertFalse(fq._end_is_firm(job(t_started=now - 3600), now))
        self.assertTrue(fq._end_is_firm(job(t_started=now - 3600, timeout_minutes=65), now))
        self.assertFalse(fq._end_is_firm(job(t_started=now - 3600, timeout_minutes=55), now))   # past its timeout too
        self.assertFalse(fq._end_is_firm(job(t_started=now - 3600, timeout_minutes=120), now))  # the allowance ends first
        self.assertFalse(fq._end_is_firm({}, now))
        # memory can be what the head waits for: 40 GB available, 30 reserved, floor 4, head needs 12
        running, probe = self.running_on_ember(slots=1, est=20, started_ago=300, memory=30)
        head = self.submit(candidates=['ember'], slots=1, memory_gb=12, memory_max_gb=12)['job']
        probe['at'] = self.clock[0]
        self.assertEqual(fq._shadow(self.job(head), probe, fq.all_jobs(self.ctx), now), now - 300 + 1200)
        # a job that already fits is kept out by something else: no shadow time
        fits = self.submit(candidates=['ember'], slots=1, memory_gb=1, memory_max_gb=1)['job']
        self.assertIsNone(fq._shadow(self.job(fits), probe, fq.all_jobs(self.ctx), now))
        self.assertIsNone(fq._shadow(self.job(head), None, [], now))
        self.assertIsNone(fq._shadow(self.job(head), self.laptop(), [], now))
        self.assertIsNone(fq._shadow(self.job(head), {'host': 'ember', 'data': {}}, [], now))

    def test_a_running_job_is_charged_only_the_part_of_its_cap_it_does_not_hold(self):
        gib = fq.GIB
        self.assertEqual(fq._host_charge(dict(reserved_memory_bytes=30 * gib, held_memory_bytes=20 * gib)), 10 * gib)
        self.assertEqual(fq._host_charge(dict(reserved_memory_bytes=30 * gib, held_memory_bytes=30 * gib)), 0)
        # a helper that does not say, or says something impossible, is charged the caps in full
        for held in ({}, {'held_memory_bytes': None}, {'held_memory_bytes': 31 * gib}, {'held_memory_bytes': -1},
                     {'held_memory_bytes': True}, {'held_memory_bytes': 1.5 * gib}, {'held_memory_bytes': '1'}):
            self.assertEqual(fq._host_charge(dict(reserved_memory_bytes=30 * gib, **held)), 30 * gib, held)
        # a 64 GB host whose one job holds 20 of its 30 GB cap: 40 GB available, floor 4. The 10 GB it may still
        # take are kept for it, so 26 GB are free; charging all 30 on top of the 20 already gone left 6.
        running, probe = self.running_on_ember(slots=1, est=20, started_ago=300, memory=30)
        head = self.job(self.submit(candidates=['ember'], slots=1, memory_gb=12, memory_max_gb=12)['job'])
        jobs = fq.all_jobs(self.ctx)
        now = self.clock[0]
        self.assertEqual(fq._fit(self.ctx, head, probe, jobs, now, explain=True), (None, 'RAM floor'))
        self.assertEqual(fq._shadow(head, probe, jobs, now), now - 300 + 1200)
        probe['data']['held_memory_bytes'] = 20 * gib
        fit, reason = fq._fit(self.ctx, head, probe, jobs, now, explain=True)
        self.assertEqual((fit['reserved'], reason), (10, None))
        self.assertIsNone(fq._shadow(head, probe, jobs, now))   # it fits: nothing to wait for
        exact = dict(head, memory_max_gb=26)
        self.assertIsNotNone(fq._fit(self.ctx, exact, probe, jobs, now))
        over = dict(head, memory_max_gb=26.5)
        self.assertEqual(fq._fit(self.ctx, over, probe, jobs, now, explain=True), (None, 'RAM floor'))
        # when the running job ends its whole cap comes back: the part still charged and the part it held
        self.assertEqual(fq._shadow(over, probe, jobs, now), now - 300 + 1200)
        # a launch the host has not confirmed yet holds nothing: its whole cap is charged
        pending = self.job(self.submit(candidates=['ember'], slots=1, memory_gb=20, memory_max_gb=20)['job'])
        pending.update(status='launching', host_id='ember', host='ember', stage_attempted=True)
        cq.save_job(self.ctx, pending)
        self.assertEqual(fq._fit(self.ctx, head, probe, fq.all_jobs(self.ctx), now, explain=True), (None, 'RAM floor'))
        self.assertIsNotNone(fq._fit(self.ctx, dict(head, memory_max_gb=6), probe, fq.all_jobs(self.ctx), now))
        fq._capacity(self.ctx, {'ember': probe}, fq.all_jobs(self.ctx))
        device = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']['ember']
        self.assertEqual((device['memory_reserved_gb'], device['memory_held_gb']), (50, 20))

    def test_a_converted_host_job_gets_a_roomier_default_cap_only_when_none_is_stated(self):
        cap = lambda **fields: self.job(self.submit(candidates=['ember'], **fields)['job'])['memory_max_gb']
        self.assertEqual(cap(), 2.5)                                           # a fleet job that leaves the cap out
        self.assertEqual(cap(_default_memory_max_gb=12), 12)                    # a where: host job turned into a fleet job
        self.assertEqual(cap(_default_memory_max_gb=12, memory_max_gb=3), 3)    # a stated cap stands
        self.assertEqual(cap(_default_memory_max_gb=12, memory_gb=20), 25)      # a stated estimate above it gets its margin
        for bad in ('12', True, 99, 0, float('nan'), None):
            self.assertEqual(cap(_default_memory_max_gb=bad), 2.5, bad)
        # the default is not the submitter's figure: the sizing note does not comment on it
        self.finished_runs(3, .5, 1, command='echo host')
        self.assertEqual(self.submit(candidates=['ember'], command='echo host', _default_memory_max_gb=12)['note'], 'fleet job accepted')

    def test_a_deferred_job_never_passes_a_held_host(self):
        head = self.submit(candidates=['ember'], slots=5, priority='verify')['job']
        self.clock[0] += 1
        later = self.submit(candidates=['ember'], slots=1, est_minutes=2, priority='lane')['job']
        job = self.job(later)
        job['requested_defer'] = 'spare'
        cq.save_job(self.ctx, job)
        probe = self.remote()
        probe['data']['active_fleet_slots'] = 3
        probe['data']['active_fleet'] = [{'id': 'external'}]
        self.assertEqual(fq._reserve(self.ctx, {'ember': probe}), [])
        self.assertEqual(self.queued(later), ('queued', 'reserved for an older eligible job'))

    def finished_runs(self, n, peak_gb, minutes, held_gb=None, held_samples=12, **fields):
        for _ in range(n):
            self.clock[0] += 1
            jid = self.submit(candidates=['ember'], memory_gb=1, memory_max_gb=1, est_minutes=1, **fields)['job']
            job = self.job(jid)
            job.update(status='done', exit=0, t_started=self.clock[0], t_finished=self.clock[0] + minutes * 60,
                       metrics={'peak_memory_bytes': int(peak_gb * fq.GIB), 'memory_kind': 'linux-cgroup'})
            if held_gb is not None:
                job['metrics'].update(peak_held_bytes=int(held_gb * fq.GIB), held_samples=held_samples)
            cq.save_job(self.ctx, job)

    def test_a_cap_filled_by_file_cache_is_measured_by_what_the_runs_could_not_give_back(self):
        gib = fq.GIB
        jid = self.submit(candidates=['ember'])['job']
        job = self.job(jid)
        job.update(status='running', host_id='ember')
        cq.save_job(self.ctx, job)
        fq._remote_metrics(self.ctx, job, {'memory_peak_bytes': 4 * gib, 'memory_held_peak_bytes': gib, 'memory_held_samples': 7})
        self.assertEqual(self.job(jid)['metrics'], {'peak_memory_bytes': 4 * gib, 'memory_kind': 'linux-cgroup',
                                                    'peak_held_bytes': gib, 'held_samples': 7})
        for held, samples in ((5 * gib, 7), (True, 7), (1.5, 7), (-1, 7), (gib, 0), (gib, None), (None, 7)):
            fq._remote_metrics(self.ctx, job, {'memory_peak_bytes': 4 * gib, 'memory_held_peak_bytes': held, 'memory_held_samples': samples})
            self.assertEqual(self.job(jid)['metrics'], {'peak_memory_bytes': 4 * gib, 'memory_kind': 'linux-cgroup'}, (held, samples))
        done = self.job(jid)
        done.update(status='done')
        cq.save_job(self.ctx, done)
        # three runs that filled an 12 GB cap with file cache and held 0.8 GB: the peak says nothing, the held figure does
        self.finished_runs(2, 11.5, 3, held_gb=.8, command='echo cache')
        self.finished_runs(1, 11.9, 3, command='echo cache')                    # an older run without the figure
        self.finished_runs(1, 11.9, .05, held_gb=.02, held_samples=fq.HELD_MIN_SAMPLES - 1, command='echo cache')  # too few readings
        stated = dict(candidates=['ember'], command='echo cache', memory_gb=12, memory_max_gb=12, est_minutes=5)
        self.assertEqual(self.submit(**stated)['note'], 'fleet job accepted')   # two runs with the figure are not a record
        self.finished_runs(1, 11.5, 3, held_gb=.75, held_samples=fq.HELD_MIN_SAMPLES, command='echo cache')
        # both figures are of the three runs that carry enough readings: not the 11.9 GB or the 0.02 GB of the others
        self.assertEqual(self.submit(**stated)['note'],
                         'fleet job accepted; sizing: it reserves 12 GB; 3 earlier successful runs of this command peaked at '
                         '11.5 GB with file cache, and held at most 0.8 GB that could not be given back (read every 5 s, so a '
                         'short spike can be missed). A job that declares less starts sooner.')
        self.assertEqual(self.submit(**dict(stated, memory_gb=5, memory_max_gb=5))['note'], 'fleet job accepted')  # under 4 x 0.8 + 2
        self.assertEqual(self.submit(candidates=['ember'], command='echo cache', est_minutes=5)['note'], 'fleet job accepted')
        # when the peak itself is far below the cap, the plainer sentence stands
        self.assertIn('5 earlier successful runs of this command peaked at 11.9 GB at most',
                      self.submit(**dict(stated, memory_gb=30, memory_max_gb=30))['note'])

    def test_a_submission_is_told_when_its_own_record_shows_it_declares_far_too_much(self):
        self.assertEqual(self.submit(candidates=['ember'], memory_gb=12, memory_max_gb=12, est_minutes=30)['note'], 'fleet job accepted')
        self.finished_runs(2, 1.5, 3)
        self.assertEqual(self.submit(candidates=['ember'], memory_gb=12, memory_max_gb=12, est_minutes=30)['note'],
                         'fleet job accepted')  # two runs are not a record
        self.finished_runs(1, 1.2, 2.2)
        note = self.submit(candidates=['ember'], memory_gb=12, memory_max_gb=12, est_minutes=30)['note']
        self.assertEqual(note, 'fleet job accepted; sizing: it reserves 12 GB and 3 earlier successful runs of this command '
                               'peaked at 1.5 GB at most; it is estimated at 30 min and 3 earlier successful runs took '
                               'at most 3 min. A job that declares less starts sooner.')
        # what the submitter did not state is not theirs to correct: the defaults are 10 minutes and a derived cap
        self.assertEqual(self.submit(candidates=['ember'])['note'], 'fleet job accepted')
        self.assertNotIn('estimated', self.submit(candidates=['ember'], memory_gb=12, memory_max_gb=12)['note'])
        self.assertNotIn('reserves', self.submit(candidates=['ember'], est_minutes=30)['note'])
        self.assertEqual(self.submit(candidates=['ember'], platform='portable', memory_gb=12, memory_max_gb=12,
                                     est_minutes=30)['note'], 'fleet job accepted')  # another platform's runs do not count
        # close enough on either count says nothing about that count; another command has its own record
        self.assertEqual(self.submit(candidates=['ember'], memory_gb=3, memory_max_gb=3.9, est_minutes=10)['note'], 'fleet job accepted')
        only_memory = self.submit(candidates=['ember'], memory_gb=4, memory_max_gb=4, est_minutes=10)['note']
        self.assertIn('it reserves 4 GB', only_memory)
        self.assertNotIn('estimated', only_memory)
        self.assertEqual(self.submit(candidates=['ember'], command='echo other', memory_gb=12, memory_max_gb=12)['note'],
                         'fleet job accepted')
        last = self.job(self.submit(candidates=['ember'], memory_gb=12, memory_max_gb=12, est_minutes=30)['job'])
        self.assertIn('sizing: ', fq._sizing_note(self.ctx, last))
        # a record small enough that the defaults themselves are far above it (1 minute, 0.5 GB): still only the
        # figure the submitter stated is mentioned
        self.finished_runs(3, .5, 1, command='echo small')
        self.assertEqual(self.submit(candidates=['ember'], command='echo small')['note'], 'fleet job accepted')
        stated_memory = self.submit(candidates=['ember'], command='echo small', memory_gb=12, memory_max_gb=12)['note']
        self.assertIn('it reserves 12 GB', stated_memory)
        self.assertNotIn('estimated', stated_memory)
        stated_estimate = self.submit(candidates=['ember'], command='echo small', est_minutes=30)['note']
        self.assertIn('it is estimated at 30 min', stated_estimate)
        self.assertNotIn('reserves', stated_estimate)
        small = self.job(self.submit(candidates=['ember'], command='echo small')['job'])
        both = fq._sizing_note(self.ctx, small)   # the same job, had the submitter stated both figures
        self.assertIn('it reserves', both)
        self.assertIn('it is estimated at 10 min', both)
        self.assertEqual(fq._sizing_note(self.ctx, small, memory=False, estimate=False), '')
        with patch.object(fq, 'all_jobs', side_effect=RuntimeError('store')):
            self.assertEqual(fq._sizing_note(self.ctx, last), '')

    def test_a_held_host_leaves_other_devices_usable(self):
        self.submit(candidates=['ember'], slots=5, priority='verify')
        self.clock[0] += 1
        long = self.submit(candidates=['ember', 'hearth'], slots=1, est_minutes=60, priority='lane')['job']
        busy = self.remote()
        busy['data']['active_fleet_slots'] = 3
        busy['data']['active_fleet'] = [{'id': 'external'}]
        selected = fq._reserve(self.ctx, {'ember': busy, 'hearth': self.remote('hearth')})
        self.assertEqual([(j['id'], j['host_id']) for j in selected], [(long, 'hearth')])

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
        quarter = fq.GIB // 4
        ember['data'].update(active_fleet_slots=1, reserved_memory_bytes=half_gib, held_memory_bytes=half_gib,
                             active_fleet=[dict(id=first, slots=1, reserved_memory_bytes=half_gib, held_memory_bytes=half_gib)])
        hearth['data'].update(active_fleet_slots=2, reserved_memory_bytes=fq.GIB, held_memory_bytes=3 * quarter,
                              active_fleet=[dict(id=second, slots=1, reserved_memory_bytes=half_gib, held_memory_bytes=half_gib),
                                            dict(id='external-job', slots=1, reserved_memory_bytes=half_gib,
                                                 held_memory_bytes=quarter)])
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
        # what a finished job held leaves the count with it; the job still there keeps its own quarter
        self.assertEqual((devices['ember']['memory_held_gb'], devices['hearth']['memory_held_gb']), (0, .25))
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

    def unconfirmed(self, **fields):
        """A job reserved on ember whose one staging attempt, begun now, was never confirmed."""
        jid = self.submit(candidates=['ember'])['job']
        job = fq._reserve(self.ctx, {'ember': self.remote()})[0]
        job.update(stage_attempted=True, stage_at=self.clock[0], launch_uncertain=True,
                   deferred_phase='registration_unconfirmed')
        job.update(fields)
        for key in [k for k, v in fields.items() if v is None]:
            del job[key]
        cq.save_job(self.ctx, job)
        return jid

    def absent(self, jid):
        return dict(version=1, id=jid, phase='absent', registered=False)

    def reconcile_absent(self, jid, host_answer=(0, 'absent\n', '')):
        """One controller pass in which the host's service knows nothing of the job; returns the scripts sent."""
        scripts = []

        def ssh(ctx, job, script, **kw):
            scripts.append(script)
            return host_answer
        with patch.object(fq, '_probes', return_value={'ember': self.remote()}), \
                patch.object(cq, 'deferred_control', return_value=self.absent(jid)), patch.object(cq, 'job_ssh', side_effect=ssh):
            fq._pass(self.ctx, {})
        return scripts

    def test_an_unconfirmed_staging_that_left_nothing_on_the_host_is_made_again(self):
        jid = self.unconfirmed()
        self.assertEqual(self.reconcile_absent(jid), [])  # the attempt is recent: its push may still be on its way
        self.assertNotIn('restage_due', self.job(jid))
        self.clock[0] += fq.RESTAGE_AFTER_S + 1
        scripts = self.reconcile_absent(jid)
        self.assertEqual(len(scripts), 1)
        self.assertIn(jid, scripts[0])
        job = self.job(jid)
        self.assertEqual((job['status'], job['host_id'], job['restage_due'], job['launch_uncertain'], job['restages']),
                         ('launching', 'ember', True, True, 1))
        self.assertTrue(job['stage_attempted'])  # so a cancel still asks the host
        self.assertEqual(job['stage_resume_after'], self.clock[0] + 60)
        self.assertEqual(job['reason'], 'host has no staged copy; staging again on the pinned host')
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.remote()}), [])  # the host stays held meanwhile
        self.assertEqual(fq._resume_pending(self.ctx, set(), {'ember': self.remote()}), [])  # not before the wait is over
        self.assertEqual(self.reconcile_absent(jid), [])  # and the host is not asked again while the restage is due
        self.clock[0] += 61
        self.assertEqual([j['id'] for j in fq._resume_pending(self.ctx, set(), {'ember': self.remote()})], [jid])
        # the new attempt is stamped, and is itself left alone until it is old
        self.ctx.run_remoterun = lambda argv, cwd: (255, '', 'connection lost')
        with patch.object(cq, 'make_launcher', return_value='true'):
            fq._stage_remote(self.ctx, self.job(jid))
        job = self.job(jid)
        self.assertEqual((job['stage_at'], job.get('restage_due'), job['stage_attempted']), (self.clock[0], None, True))
        self.assertEqual(self.reconcile_absent(jid), [])
        self.clock[0] += fq.RESTAGE_AFTER_S + 1
        self.assertEqual(len(self.reconcile_absent(jid)), 1)
        self.assertEqual(self.job(jid)['restages'], 2)

    def test_a_cancel_after_a_restage_was_decided_still_asks_the_host(self):
        jid = self.unconfirmed()
        self.clock[0] += fq.RESTAGE_AFTER_S + 1
        self.reconcile_absent(jid)
        with patch.object(fq, '_remote_reconcile') as reconcile:
            answer = fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(self.root), session='s1'))
        reconcile.assert_called_once()
        self.assertEqual(answer['note'], 'cancellation pending confirmation')
        job = self.job(jid)
        self.assertEqual((job['status'], job['cancel_requested']), ('launching', True))
        self.assertEqual(fq._resume_pending(self.ctx, set(), {'ember': self.remote()}), [])

    def test_only_the_controller_pass_restages(self):
        jid = self.unconfirmed()
        self.clock[0] += fq.RESTAGE_AFTER_S + 1
        with patch.object(cq, 'deferred_control', return_value=self.absent(jid)), \
                patch.object(cq, 'job_ssh', return_value=(0, 'absent\n', '')) as ssh:
            fq.execute(self.ctx, 'status', dict(job=jid, refresh=True))  # another process: a worker may hold the job
            fq._remote_reconcile(self.ctx, self.job(jid), explicit=True)
            ssh.assert_not_called()
            self.assertNotIn('restage_due', self.job(jid))
            fq._remote_reconcile(self.ctx, self.job(jid), explicit=True, restage=True)
        self.assertTrue(self.job(jid)['restage_due'])

    def test_a_job_one_of_this_controllers_workers_holds_is_not_reconciled(self):
        jid = self.unconfirmed()
        self.clock[0] += fq.RESTAGE_AFTER_S + 1
        with patch.object(fq, '_probes', return_value={'ember': self.remote()}), \
                patch.object(fq, '_remote_reconcile') as reconcile:
            fq._pass(self.ctx, {jid: ('ember', None)})
        reconcile.assert_not_called()

    def test_each_further_staging_attempt_waits_longer(self):
        jid = self.unconfirmed(restages=3)
        self.clock[0] += fq.RESTAGE_AFTER_S + 1
        self.reconcile_absent(jid)
        job = self.job(jid)
        self.assertEqual((job['restages'], job['stage_resume_after']), (4, self.clock[0] + 480))
        job.update(restage_due=False, stage_at=self.clock[0], restages=40)
        cq.save_job(self.ctx, job)
        self.clock[0] += fq.RESTAGE_AFTER_S + 1
        self.reconcile_absent(jid)
        self.assertEqual(self.job(jid)['stage_resume_after'], self.clock[0] + 1800)

    def test_an_attempt_without_a_stamp_is_aged_from_its_reservation_with_the_lock_wait_added(self):
        jid = self.unconfirmed(stage_at=None)
        job = self.job(jid)
        self.assertNotIn('stage_at', job)
        self.assertFalse(fq._restage_wanted(job, job['t_touch'] + fq.RESTAGE_AFTER_S + 599))
        self.assertTrue(fq._restage_wanted(job, job['t_touch'] + fq.RESTAGE_AFTER_S + 600))
        self.assertFalse(fq._restage_wanted(dict(job, t_touch=None), self.clock[0] + 10 ** 6))

    def test_an_unconfirmed_staging_is_left_alone_unless_the_host_holds_nothing(self):
        for label, fields, answer in (
                ('a directory exists', {}, (0, 'present\n', '')),
                ('the host cannot be asked', {}, (255, '', 'timeout')),
                ('an unexpected answer', {}, (0, '', '')),
                ('being cancelled', {'cancel_requested': True}, (0, 'absent\n', '')),
                ('registered', {'deferred_registered': True}, (0, 'absent\n', '')),
                ('confirmed', {'launch_uncertain': False}, (0, 'absent\n', '')),
                ('a restage already due', {'restage_due': True}, (0, 'absent\n', ''))):
            with self.subTest(label):
                jid = self.unconfirmed(**fields)
                self.clock[0] += fq.RESTAGE_AFTER_S + 1
                job = self.job(jid)
                with patch.object(cq, 'job_ssh', return_value=answer):
                    self.assertFalse(fq._restage_if_absent(self.ctx, job, self.absent(jid)))
                self.assertNotIn('restages', self.job(jid))
                job.update(status='cancelled')  # free the host for the next case
                cq.save_job(self.ctx, job)
        jid = self.unconfirmed()
        self.clock[0] += fq.RESTAGE_AFTER_S + 1
        with patch.object(cq, 'job_ssh', return_value=(0, 'absent\n', '')) as ssh:
            for state in (None, {'error': 'unreachable'}, dict(phase='queued'), dict(phase='running')):
                self.assertFalse(fq._restage_if_absent(self.ctx, self.job(jid), state))
            ssh.assert_not_called()

    def test_a_restage_is_dropped_when_another_attempt_began_while_the_host_was_asked(self):
        jid = self.unconfirmed()
        self.clock[0] += fq.RESTAGE_AFTER_S + 1
        seen = self.job(jid)

        def ssh(ctx, job, script, **kw):
            current = self.job(jid)
            current['stage_at'] = self.clock[0]  # a newer attempt started after the pass read the job
            cq.save_job(self.ctx, current)
            return 0, 'absent\n', ''
        with patch.object(cq, 'job_ssh', side_effect=ssh):
            self.assertFalse(fq._restage_if_absent(self.ctx, seen, self.absent(jid)))
        self.assertNotIn('restage_due', self.job(jid))

    def test_a_registered_job_is_never_staged_again(self):
        jid = self.unconfirmed(restage_due=True, deferred_registered=True, stage_resume_after=0)
        self.assertEqual(fq._resume_pending(self.ctx, set(), {'ember': self.remote()}), [])
        job = self.job(jid)
        job.update(deferred_registered=False)
        cq.save_job(self.ctx, job)
        self.assertEqual([j['id'] for j in fq._resume_pending(self.ctx, set(), {'ember': self.remote()})], [jid])

    def test_a_failed_preparation_of_a_restage_backs_off_like_a_first_one(self):
        jid = self.unconfirmed(restage_due=True)
        fq._stage_error(self.ctx, jid, RuntimeError('launcher'))
        job = self.job(jid)
        self.assertEqual((job['preparation_failures'], job['reason']), (1, 'pinned preparation failed: RuntimeError'))
        self.assertEqual(job['stage_resume_after'], self.clock[0] + 30)

    def test_the_memory_estimate_reads_archived_fleet_jobs_of_the_last_month(self):
        import time as real_time
        archive = Path(self.ctx.p('fleet', 'jobs-archive'))
        archive.mkdir(parents=True)
        for name, age in (('recent', 86400), ('old', fq.HISTORY_S + 86400)):
            path = archive / f'cq-fleet-20260101T000000-{name:0>16}.json'
            path.write_text(json.dumps({'id': path.stem, 'status': 'done', 'fleet': True}))
            stamp = real_time.time() - age
            os.utime(path, (stamp, stamp))
        live = self.submit(candidates=['ember'], memory_gb=1)['job']
        with patch.object(fq.telemetry, 'estimate_memory', return_value={'memory_gb': 3}) as estimate:
            self.submit(candidates=['ember'])
        history = [j['id'] for j in estimate.call_args[0][0]]
        self.assertIn(live, history)
        self.assertEqual([i for i in history if i.endswith(('recent', 'old'))], ['cq-fleet-20260101T000000-0000000000recent'])

    def test_idle_finalization_drops_the_reservation_of_a_finished_job_that_was_archived(self):
        jid = self.submit(candidates=['ember'])['job']
        job = self.job(jid)
        job.update(status='done', host_id='ember', host_config={'machine_id': 'a'*32}, t_finished=self.clock[0])
        cq.save_job(self.ctx, job)
        live = Path(cq.job_file(self.ctx, jid))
        archived = Path(cq.archived_file(str(live)))
        archived.parent.mkdir(parents=True)
        live.replace(archived)
        probe = self.remote('ember')
        probe['data'].update(active_fleet_slots=1, reserved_memory_bytes=fq.GIB // 2,
                             active_fleet=[dict(id=jid, slots=1, reserved_memory_bytes=fq.GIB // 2),
                                           dict(id='../../x', slots=1, reserved_memory_bytes=1)])
        self.ctx._fleet_pass_probes = {'ember': probe}
        self.load.stop()  # this class reads fleet jobs straight from the store: use the engine's own lookup here
        self.addCleanup(self.load.start)
        with self.ctx.qlock():
            self.assertEqual(fq.all_jobs(self.ctx), [])
            fq._finalize_capacity(self.ctx, fq.all_jobs(self.ctx))
        device = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']['ember']
        self.assertEqual((device['slots_used'], device['memory_reserved_gb']), (0, 0))

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
