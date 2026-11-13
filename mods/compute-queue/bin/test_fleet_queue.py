"""Isolated fleet coordinator tests: no host connections or real workers."""
import copy
import json
import hashlib
import importlib.util
import os
from pathlib import Path
import re
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
# these tests place work on the Windows laptop device on any OS (cq.laptop_executes is off elsewhere than Windows by default)
os.environ.setdefault('CQ_LAPTOP_EXECUTOR', '1')
import cq
import fleet_queue as fq
import fleet_telemetry as telemetry

HOST_HELPER = Path(__file__).resolve().parent.parent / 'host' / 'deferred.py'


def load_host_helper():
    """The host helper (host/deferred.py) as its own tests load it, fresh for each caller."""
    try:
        import fcntl  # noqa: F401
    except ImportError:   # development on Windows: no real locks are taken here
        sys.modules['fcntl'] = types.SimpleNamespace(LOCK_EX=2, LOCK_SH=1, LOCK_NB=4, flock=lambda *a: None)
    spec = importlib.util.spec_from_file_location('host_deferred_for_agreement', HOST_HELPER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
            if args[:3] == ('rev-parse', '--verify', '--quiet') and ':' in args[3]:   # the blob of a file in the snapshot
                path = self.root / args[3].split(':', 1)[1]
                return (0, hashlib.sha1(path.read_bytes()).hexdigest()) if path.is_file() else (1, '')
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
        # queue_on_host False: these hosts keep waiting jobs with the controller, which is what the placement, hold and
        # backfill tests are about. The hand-over tests take the key away (the default is that a host takes them).
        return dict(host=host, state=str(self.state), binding=dict(address=host, machine_id='a'*32, queue_on_host=False,
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

    def stage_with(self, rc, out, err):
        jid = self.submit(candidates=['ember'])['job']
        job = fq._reserve(self.ctx, {'ember': self.remote()})[0]
        self.ctx.run_remoterun = lambda argv, cwd: (rc, out.replace('JID', jid), err)
        with patch.object(cq, 'make_launcher', return_value='true'):
            fq._stage_remote(self.ctx, job)
        return self.job(jid)

    def test_stderr_of_a_staging_helper_that_succeeded_stays_on_the_record(self):
        note = '[remote_stage] incremental staging failed; sending the full history\x1b[0m\n'
        after = self.stage_with(0, 'CQ_REGISTERED JID\n', note)
        self.assertTrue(after['deferred_registered'])
        self.assertEqual(after['staging_stderr'], '[remote_stage] incremental staging failed; sending the full history?[0m')
        long = self.stage_with(0, 'CQ_REGISTERED JID\n', 'x' * 5000 + 'END\n')
        self.assertEqual(len(long['staging_stderr']), cq.STAGING_STDERR_CHARS)
        self.assertTrue(long['staging_stderr'].endswith('xxEND'))
        self.assertIn('staging_stderr', cq.job_brief(after, self.clock[0]))

    def test_a_staging_helper_that_said_nothing_adds_no_key(self):
        for err in ('', ' \n'):
            after = self.stage_with(0, 'CQ_REGISTERED JID\n', err)
            self.assertTrue(after['deferred_registered'])
            self.assertNotIn('staging_stderr', after)

    def test_a_staging_that_did_not_confirm_keeps_its_old_reason_and_no_note(self):
        after = self.stage_with(255, '', 'connection lost')
        self.assertTrue(after['launch_uncertain'])
        self.assertEqual(after['reason'], 'staging outcome unconfirmed (exit 255); reconcile original host')
        self.assertNotIn('staging_stderr', after)

    def test_an_exit_of_zero_without_the_registration_marker_keeps_its_reason_and_no_note(self):
        unconfirmed = self.stage_with(0, 'no marker\n', 'some stderr\n')   # exit 0 but not registered: the unchanged branch
        self.assertEqual(unconfirmed['reason'], 'staging outcome unconfirmed (exit 0); reconcile original host')
        self.assertNotIn('staging_stderr', unconfirmed)

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
            self.clock[0] += fq.PROBE_REUSE_S - 1
            second = fq._probes(self.ctx)['ember']
            self.assertEqual(first['at'], second['at'])
            self.assertEqual(probe.call_count, 1)
            self.clock[0] += 1
            fq._probes(self.ctx)
            self.assertEqual(probe.call_count, 2)
        # a finished job's result and its freed slot reach the controller with the next probe: read well inside the
        # 30 s for which _fit still believes one
        self.assertLessEqual(fq.PROBE_REUSE_S, 10)

    def test_a_job_record_says_how_long_it_waited_for_each_reason(self):
        jid = self.submit(candidates=['ember'], memory_gb=2, memory_max_gb=10)['job']
        short = lambda: {'ember': self.remote(avail=12)}                    # a fresh probe each look: a stale one is its own reason
        self.clock[0] += 4
        self.assertEqual(fq._reserve(self.ctx, short()), [])                # first look, 4 s after submission
        self.assertEqual(self.job(jid)['waited'], {fq.WAIT_FIRST_LOOK: 4})
        self.clock[0] += 50
        fq._reserve(self.ctx, short())                                      # same reason: nothing is rewritten
        self.assertEqual(self.job(jid)['waited'], {fq.WAIT_FIRST_LOOK: 4})
        self.clock[0] += 1000                                               # the laptop slept: no look for over WAIT_GAP_S
        fq._reserve(self.ctx, short())
        self.assertEqual(self.job(jid)['waited'], {fq.WAIT_FIRST_LOOK: 4, 'RAM floor': 50, fq.WAIT_NO_CONTROLLER: 1000})
        self.clock[0] += 30
        placed = fq._reserve(self.ctx, {'ember': self.remote()})
        self.assertEqual([j['id'] for j in placed], [jid])
        job = self.job(jid)
        self.assertEqual(job['waited'], {fq.WAIT_FIRST_LOOK: 4, 'RAM floor': 80, fq.WAIT_NO_CONTROLLER: 1000})
        self.assertNotIn('waited_at', job)
        self.assertEqual(sum(job['waited'].values()), job['t_launch'] - job['t_submitted'])

    def test_wait_reasons_from_several_hosts_make_one_label_and_the_record_stays_small(self):
        self.assertEqual(fq._wait_label({'ember': 'RAM floor', 'hearth': 'RAM floor'}), 'RAM floor')
        self.assertEqual(fq._wait_label({'ember': 'RAM floor', 'hearth': 'CPU slots unavailable'}), 'CPU slots unavailable | RAM floor')
        self.assertEqual(fq._wait_label({}), fq.WAIT_FIRST_LOOK)
        job = {'t_submitted': 100.0}
        for n in range(fq.WAIT_KEYS_MAX + 5):
            job['placement_reasons'] = {'ember': f'reason {n}'}
            fq._wait_account(job, 110.0 + 10 * n)
        self.assertEqual(len(job['waited']), fq.WAIT_KEYS_MAX + 1)
        self.assertEqual(job['waited']['other reasons'], 50)                # the five reasons past the twelve kept
        self.assertEqual(sum(job['waited'].values()), 10 * (fq.WAIT_KEYS_MAX + 5))

    def test_a_staging_error_keeps_its_message(self):
        self.assertEqual(fq._error_text(RuntimeError('source checkout' + chr(10) + '  incomplete')), 'RuntimeError: source checkout incomplete')
        self.assertEqual(fq._error_text(TimeoutError()), 'TimeoutError')
        self.assertLessEqual(len(fq._error_text(RuntimeError('x' * 900))), 220)

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
        with patch.object(fq, '_pass', side_effect=fleet_pass), patch.object(fq.time, 'sleep'),                 patch.object(fq, '_cleanup_rounds', return_value=False):
            fq.controller(self.ctx)
        self.assertEqual(len(calls), 2)
        receipt = cq.read_json(self.ctx.p('fleet', 'controller.json'))
        self.assertEqual(receipt['status'], 'stopped')
        self.assertIn('heartbeat', receipt)

    def test_controller_stays_for_a_cleanup_round_and_never_for_a_failing_one(self):
        jid = self.submit()['job']
        job = self.job(jid)
        job['status'] = 'done'
        cq.save_job(self.ctx, job)
        for tidy, passes in (([True, True, False], 3), (OSError('boom'), 1)):
            calls = []
            with patch.object(fq, '_pass', side_effect=lambda *a: (calls.append(1), False)[1]), patch.object(fq.time, 'sleep'),                     patch.object(fq, '_cleanup_rounds', side_effect=tidy) as rounds:
                fq.controller(self.ctx)
            self.assertEqual(len(calls), passes)
            self.assertIs(rounds.call_args.args[3], False)   # told that nothing is active
            self.assertEqual(cq.read_json(self.ctx.p('fleet', 'controller.json'))['status'], 'stopped')

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

    def quarter_minute_job(self):
        """A 15-second job: a submission takes whole minutes, so its record is given the estimate."""
        jid = self.submit(candidates=['ember'], slots=1, est_minutes=1, priority='lane')['job']
        job = self.job(jid)
        job['est_minutes'] = .25
        cq.save_job(self.ctx, job)
        return jid

    def test_short_jobs_pass_while_the_job_that_must_end_has_overrun_but_has_a_timeout(self):
        # the head waits for a job already past its estimate: its allowance is counted from now, so there is no shadow
        # time. But the job cannot outlast its timeout, and a job that ends before that may pass, patience or not.
        running, probe = self.running_on_ember(slots=3, est=3, started_ago=240, timeout_minutes=600)
        head = self.submit(candidates=['ember'], slots=5, est_minutes=30, priority='verify')['job']
        started = self.clock[0] - 240
        self.assertIsNone(fq._shadow(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]))
        self.assertEqual(fq._shadow_limit(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]),
                         (None, (started + 600 * 60, started)))
        placed = []
        for minute in range(40):
            self.clock[0] += 60
            self.submit(candidates=['ember'], slots=1, est_minutes=3, priority='lane')
            probe['at'] = self.clock[0]
            for job in fq._reserve(self.ctx, {'ember': probe}):
                placed.append((minute, job['id']))
                done = self.job(job['id'])
                done.update(status='done')
                cq.save_job(self.ctx, done)
        # one every minute for 40 minutes, far past the 15 of patience
        self.assertGreater(40 * 60, fq.HOLD_PATIENCE_S)
        self.assertEqual([minute for minute, _ in placed], list(range(40)))
        self.assertEqual(self.job(head)['hold_since'], 1700000000.0 + 60)
        self.assertEqual(self.job(head)['status'], 'queued')
        # a job estimated at more than ten minutes never passes
        long = self.submit(candidates=['ember'], slots=1, est_minutes=11, priority='lane')['job']
        probe['at'] = self.clock[0]
        self.assertEqual(fq._reserve(self.ctx, {'ember': probe}), [])
        self.assertEqual(self.queued(long), ('queued', 'reserved for an older eligible job'))
        # within the last ten minutes of its timeout its end is a fixed moment again (the shadow time): a job that
        # ends before it may pass, one that does not may not
        late = self.job(running)
        late['timeout_minutes'] = (self.clock[0] - late['t_started']) / 60 + 5
        cq.save_job(self.ctx, late)
        self.assertEqual(fq._shadow(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]), self.clock[0] + 300)
        self.assertEqual(fq._shadow_limit(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]), (self.clock[0] + 300, None))
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
        # in its last minute the end moves on with every look again, but the timeout is still the bound: only a job
        # that ends by it passes; once it is past it (still running) none does
        late = self.job(running)
        late['timeout_minutes'] = (self.clock[0] - late['t_started']) / 60 + .5
        cq.save_job(self.ctx, late)
        for job in fq.all_jobs(self.ctx):
            if job['status'] == 'queued' and job['id'] != head:
                job.update(status='cancelled')
                cq.save_job(self.ctx, job)
        self.assertEqual(fq._shadow_limit(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]),
                         (None, (self.clock[0] + 30, started)))
        one = self.submit(candidates=['ember'], slots=1, est_minutes=1, priority='lane')['job']
        quarter = self.quarter_minute_job()
        probe['at'] = self.clock[0]
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': probe})], [quarter])   # 15 s, within the 30
        self.assertEqual(self.queued(one), ('queued', 'reserved for an older eligible job'))
        done = self.job(quarter)
        done.update(status='done')
        cq.save_job(self.ctx, done)
        self.clock[0] += 31
        again = self.quarter_minute_job()
        probe['at'] = self.clock[0]
        self.assertEqual(fq._reserve(self.ctx, {'ember': probe}), [])
        self.assertEqual(self.queued(again), ('queued', 'reserved for an older eligible job'))

    def test_an_overrunner_with_no_timeout_grants_nothing_once_patience_has_run_out(self):
        # (every submitted job has a timeout; a record without a usable one is what this stands for)
        running, probe = self.running_on_ember(slots=3, est=3, started_ago=240, timeout_minutes=None)
        head = self.submit(candidates=['ember'], slots=5, est_minutes=30, priority='verify')['job']
        self.assertEqual(fq._shadow_limit(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]), (None, None))
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

    def age_hold(self, jid, seconds):
        """The held job began to hold `seconds` ago and was seen holding just now."""
        job = self.job(jid)
        job.update(hold_since=self.clock[0] - seconds, hold_last=self.clock[0] - 10)
        cq.save_job(self.ctx, job)

    def test_an_overrunner_launched_after_the_hold_began_grants_nothing(self):
        # the head needs all 6 slots; r0 holds two and ends in 10 minutes, so a 5-minute job passes by the shadow time
        r0, probe = self.running_on_ember(slots=2, est=20, started_ago=600)
        head = self.submit(candidates=['ember'], slots=6, est_minutes=30, priority='verify')['job']
        self.clock[0] += 1
        s = self.submit(candidates=['ember'], slots=1, est_minutes=5, priority='lane')['job']
        probe['at'] = self.clock[0]
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': probe})], [s])
        began = self.job(head)['hold_since']
        # it launched in the pass the hold began in (the same clock reading): not before the hold, so it grants nothing
        self.assertEqual(self.job(s)['t_launch'], began)
        # it runs, with a timeout, and r0 is done: s is the job the head waits for
        done = self.job(r0)
        done.update(status='done')
        cq.save_job(self.ctx, done)
        running = self.job(s)
        running.update(status='running', host='ember', host_config={'machine_id': 'a' * 32}, stage_attempted=True,
                       deferred_registered=True, deferred_phase='running', t_started=self.clock[0], timeout_minutes=600)
        cq.save_job(self.ctx, running)
        reserved = int(running['memory_max_gb'] * fq.GIB)
        probe = self.remote()
        probe['data'].update(active_fleet_slots=1, reserved_memory_bytes=reserved,
                             active_fleet=[{'id': s, 'slots': 1, 'reserved_memory_bytes': reserved}])
        for _ in range(30):   # it passes its estimate; 30 minutes in all, with the head seen holding every minute
            self.clock[0] += 60
            probe['at'] = self.clock[0]
            self.assertEqual(fq._reserve(self.ctx, {'ember': probe}), [])
        self.assertEqual(self.job(head)['hold_since'], began)
        self.assertEqual(fq._shadow_limit(self.job(head), probe, fq.all_jobs(self.ctx), self.clock[0]),
                         (None, (began + 600 * 60, began)))
        short = self.submit(candidates=['ember'], slots=1, est_minutes=3, priority='lane')['job']
        self.assertEqual(fq._reserve(self.ctx, {'ember': probe}), [])
        self.assertEqual(self.queued(short), ('queued', 'reserved for an older eligible job'))
        # the same moment with s launched before the hold began (here: the hold is made to begin a second later)
        held = self.job(head)
        held.update(hold_since=began + 1, hold_last=self.clock[0])
        cq.save_job(self.ctx, held)
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': probe})], [short])

    def test_the_timeout_allowance_needs_a_timeout_a_known_host_and_a_job_that_is_not_deferred(self):
        running, probe = self.running_on_ember(slots=3, est=3, started_ago=3000, timeout_minutes=600)
        head = self.submit(candidates=['ember'], slots=5, est_minutes=30, priority='verify')['job']
        self.clock[0] += 1
        deferred = self.submit(candidates=['ember'], slots=1, est_minutes=2, priority='lane')['job']
        job = self.job(deferred)
        job['requested_defer'] = 'spare'
        cq.save_job(self.ctx, job)
        plain = self.submit(candidates=['ember'], slots=1, est_minutes=2, priority='lane')['job']
        self.age_hold(head, 2000)   # patience is long gone; the running job launched before the hold began
        probe['at'] = self.clock[0]
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': probe})], [plain])
        self.assertEqual(self.queued(deferred), ('queued', 'reserved for an older eligible job'))
        # the laptop and a host with work this store does not know give no bound at all
        jobs, now = fq.all_jobs(self.ctx), self.clock[0]
        self.assertEqual(fq._shadow_limit(self.job(head), self.laptop(), jobs, now), (None, None))
        probe['data']['active_fleet'].append({'id': 'external'})
        self.assertEqual(fq._shadow_limit(self.job(head), probe, jobs, now), (None, None))
        self.assertEqual(fq._shadow_limit(self.job(head), None, jobs, now), (None, None))
        self.assertEqual(fq._shadow_limit(self.job(head), {'host': 'ember', 'data': {}}, jobs, now), (None, None))

    def busy_probe(self):
        probe = self.remote()
        probe['data']['active_fleet_slots'] = 3
        probe['data']['active_fleet'] = [{'id': 'external'}]   # no shadow time can be worked out
        return probe

    def test_patience_restarts_after_a_gap_longer_than_it_and_not_after_a_shorter_one(self):
        head = self.submit(candidates=['ember'], slots=5, priority='verify')['job']
        probe = self.busy_probe()
        began = self.clock[0]
        fq._reserve(self.ctx, {'ember': probe})
        self.assertEqual((self.job(head)['hold_since'], self.job(head)['hold_last']), (began, began))
        # ten minutes with the host not probed: the job holds nothing, but the gap is shorter than the patience
        self.clock[0] += 600
        fq._reserve(self.ctx, {})
        self.assertEqual(self.job(head)['hold_last'], began)
        self.clock[0] += 5
        probe['at'] = self.clock[0]
        fq._reserve(self.ctx, {'ember': probe})
        self.assertEqual((self.job(head)['hold_since'], self.job(head)['hold_last']), (began, self.clock[0]))
        # a gap of 25 minutes: the next hold begins a new patience, in which a short job may pass at once
        seen = self.clock[0]
        self.clock[0] += 1500
        fq._reserve(self.ctx, {})
        self.assertEqual(self.job(head)['hold_since'], began)
        short = self.submit(candidates=['ember'], slots=1, est_minutes=3, priority='lane')['job']
        self.clock[0] += 1
        probe['at'] = self.clock[0]
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': probe})], [short])
        self.assertEqual((self.job(head)['hold_since'], self.job(head)['hold_last']), (self.clock[0], self.clock[0]))
        self.assertGreater(self.clock[0] - seen, fq.HOLD_PATIENCE_S)
        # a record from before hold_last has no sighting time: it may have held all along, so its patience stands
        old = self.job(head)
        old.pop('hold_last')
        old['hold_since'] = self.clock[0] - 1000
        cq.save_job(self.ctx, old)
        self.clock[0] += 5
        probe['at'] = self.clock[0]
        fq._reserve(self.ctx, {'ember': probe})
        self.assertEqual(self.job(head)['hold_since'], self.clock[0] - 1005)
        self.assertEqual(self.job(head)['hold_last'], self.clock[0])
        old = self.job(head)
        old.pop('hold_last')
        old['hold_since'] = self.clock[0] - 2000   # however long ago: a long hold must not get a second patience at an upgrade
        cq.save_job(self.ctx, old)
        self.clock[0] += 5
        probe['at'] = self.clock[0]
        fq._reserve(self.ctx, {'ember': probe})
        self.assertEqual(self.job(head)['hold_since'], self.clock[0] - 2005)
        self.assertEqual(self.job(head)['hold_last'], self.clock[0])

    def test_a_held_job_is_saved_once_per_interval_not_on_every_pass(self):
        head = self.submit(candidates=['ember'], slots=5, priority='verify')['job']
        probe = self.busy_probe()
        saves = []
        original = cq.save_job
        def save(ctx, job):
            if job['id'] == head:
                saves.append(self.clock[0])
            return original(ctx, job)
        with patch.object(cq, 'save_job', side_effect=save):
            for _ in range(40):   # a pass a minute for 40 minutes, the head held in every one
                self.clock[0] += 60
                probe['at'] = self.clock[0]
                fq._reserve(self.ctx, {'ember': probe})
        # the first pass, then the first pass more than HOLD_SEEN_WRITE_S (300 s) after the last write: every sixth
        self.assertEqual(len(saves), 1 + 39 // 6)
        self.assertTrue(all(b - a > fq.HOLD_SEEN_WRITE_S for a, b in zip(saves, saves[1:])))
        self.assertEqual(self.job(head)['hold_since'], 1700000000.0 + 60)
        self.assertEqual(self.job(head)['hold_last'], saves[-1])

    def test_hold_since_and_hold_last_go_when_the_job_is_placed_or_fails_for_good(self):
        holding = lambda jid: {k for k in self.job(jid) if k.startswith('hold_')}
        head = self.submit(candidates=['ember'], slots=5, priority='verify')['job']
        fq._reserve(self.ctx, {'ember': self.busy_probe()})
        self.assertEqual(holding(head), {'hold_since', 'hold_last'})
        # the host goes idle: the head is placed
        self.clock[0] += 1
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': self.remote()})], [head])
        self.assertEqual(holding(head), set())
        done = self.job(head)
        done.update(status='done')
        cq.save_job(self.ctx, done)
        # another job holds the host next; then a probe shows that no host could ever take it
        wide = self.submit(candidates=['ember'], slots=5, priority='verify')['job']
        self.clock[0] += 1
        fq._reserve(self.ctx, {'ember': self.busy_probe()})
        self.assertEqual(holding(wide), {'hold_since', 'hold_last'})
        self.clock[0] += 1
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.remote(slots=3)}), [])
        self.assertEqual(self.job(wide)['status'], 'failed')
        self.assertEqual(holding(wide), set())

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

    def expecting(self, held_gb=3, expected_gb=5, cap_gb=30, avail=37, switch=True, charged=None, host_flag=True):
        """A 64 GB host (floor 4) whose helper admits on expected use and that runs one job of this store: cap 30 GB,
        expected to use 5, holding 3 (so charged 5 - 3 = 2 GB on expectations, 27 GB on caps). Returns (job id, probe)."""
        gib = fq.GIB
        jid, probe = self.running_on_ember(slots=1, est=20, started_ago=300, memory=cap_gb)
        row = dict(id=jid, slots=1, reserved_memory_bytes=cap_gb * gib, held_memory_bytes=int(held_gb * gib))
        if expected_gb is not None:
            row['expected_memory_bytes'] = int(expected_gb * gib)
        if charged is None:
            charged = (expected_gb if expected_gb is not None and held_gb <= expected_gb else cap_gb) - held_gb
        probe['data'].update(available_bytes=avail * gib, held_memory_bytes=int(held_gb * gib), active_fleet=[row],
                             charged_memory_bytes=int(charged * gib))
        if host_flag:
            probe['data']['expected_memory_admission'] = True
        probe['binding']['expected_memory_admission'] = switch
        return jid, probe

    def test_a_new_jobs_expected_use_is_twice_the_largest_held_peak_plus_a_gigabyte_from_three_runs(self):
        self.assertEqual(self.job(self.submit(candidates=['ember'], command='echo exp', memory_gb=2, memory_max_gb=12)['job'])
                         ['memory_expected_gb'], 12)   # no record: the cap
        self.finished_runs(2, 11.5, 3, held_gb=.8, command='echo exp')
        self.assertEqual(self.job(self.submit(candidates=['ember'], command='echo exp', memory_gb=2, memory_max_gb=12)['job'])
                         ['memory_expected_gb'], 12)   # two runs with a held peak are not a record
        self.finished_runs(1, 11.9, 3, command='echo exp')   # a run without one does not count
        self.finished_runs(1, 11.9, 3, held_gb=.02, held_samples=fq.HELD_MIN_SAMPLES - 1, command='echo exp')   # too few readings
        self.assertEqual(self.job(self.submit(candidates=['ember'], command='echo exp', memory_gb=2, memory_max_gb=12)['job'])
                         ['memory_expected_gb'], 12)
        self.finished_runs(1, 11.5, 3, held_gb=.75, command='echo exp')
        # three runs: 2 x the largest held peak (0.8) + 1 GB
        self.assertEqual(self.job(self.submit(candidates=['ember'], command='echo exp', memory_gb=2, memory_max_gb=12)['job'])
                         ['memory_expected_gb'], 2.6)
        # never above the cap, and the cap bounds it from below only by itself
        self.assertEqual(self.job(self.submit(candidates=['ember'], command='echo exp', memory_gb=2, memory_max_gb=2)['job'])
                         ['memory_expected_gb'], 2)
        # a run that held more than the whole cap: the cap
        self.finished_runs(3, 11.9, 3, held_gb=8, command='echo big')
        self.assertEqual(self.job(self.submit(candidates=['ember'], command='echo big', memory_gb=2, memory_max_gb=12)['job'])
                         ['memory_expected_gb'], 12)
        # a run that held almost nothing: at least a gigabyte
        self.finished_runs(3, 11.9, 3, held_gb=.01, command='echo tiny')
        self.assertEqual(self.job(self.submit(candidates=['ember'], command='echo tiny', memory_gb=2, memory_max_gb=12)['job'])
                         ['memory_expected_gb'], 1.02)
        # only runs of the same command, subdir, platform and repository: another command or platform has its own
        # (a command that names a script file also takes that file's record: see the script test below)
        self.assertEqual(self.job(self.submit(candidates=['ember'], command='echo exp other', memory_gb=2, memory_max_gb=12)['job'])
                         ['memory_expected_gb'], 12)
        self.assertEqual(self.job(self.submit(candidates=['ember'], command='echo exp', platform='portable', memory_gb=2,
                                              memory_max_gb=12)['job'])['memory_expected_gb'], 12)
        # the sizing note and the expectation read the same runs
        last = self.job(self.submit(candidates=['ember'], command='echo exp', memory_gb=2, memory_max_gb=12)['job'])
        self.assertEqual(len(fq._earlier_runs(fq.all_jobs(self.ctx), last)), 5)
        self.assertEqual(len([h for _, _, h in fq._earlier_runs(fq.all_jobs(self.ctx), last) if h is not None]), 3)

    def test_a_host_that_admits_on_expected_use_takes_a_job_its_caps_would_refuse(self):
        gib = fq.GIB
        jid, probe = self.expecting()
        # 37 GB available, floor 4: on caps the running job keeps 27 of them, so a 34 GB job cannot start; on
        # expectations it keeps 2, and a job expected to use 4 can
        head = self.job(self.submit(candidates=['ember'], slots=1, memory_gb=12, memory_max_gb=34)['job'])
        head = dict(head, memory_expected_gb=4)
        jobs = fq.all_jobs(self.ctx)
        now = self.clock[0]
        fit, reason = fq._fit(self.ctx, head, probe, jobs, now, explain=True)
        self.assertEqual((fit['reserved'], reason), (2, None))
        # exactly what is left: 37 - 2 - 4 (floor) = 31 GB
        self.assertIsNotNone(fq._fit(self.ctx, dict(head, memory_expected_gb=31), probe, jobs, now))
        self.assertEqual(fq._fit(self.ctx, dict(head, memory_expected_gb=31.01), probe, jobs, now, explain=True), (None, 'RAM floor'))
        # a job with no expectation is charged its cap
        self.assertEqual(fq._fit(self.ctx, {k: v for k, v in head.items() if k != 'memory_expected_gb'}, probe, jobs, now,
                                 explain=True), (None, 'RAM floor'))
        # the same host without the feature (an old helper: no flag, no charge) or with the switch off charges caps
        old = copy.deepcopy(probe)
        for key in ('charged_memory_bytes', 'expected_memory_admission'):
            old['data'].pop(key, None)
        old['binding'].pop('expected_memory_admission')
        old['data']['active_fleet'][0].pop('expected_memory_bytes')
        self.assertEqual(fq._fit(self.ctx, head, old, jobs, now, explain=True), (None, 'RAM floor'))
        off = copy.deepcopy(old)   # the switch off: nothing is sent to the host with an expectation, so it charges caps
        off['data']['expected_memory_admission'] = True
        off['binding']['expected_memory_admission'] = False
        self.assertEqual(fq._fit(self.ctx, head, off, jobs, now, explain=True), (None, 'RAM floor'))
        # the controller's own switch off, though the host reports a charge made on an expectation: the new job is
        # still charged its cap
        off = copy.deepcopy(probe)
        off['binding']['expected_memory_admission'] = False
        self.assertEqual(fq._fit(self.ctx, head, off, jobs, now, explain=True), (None, 'RAM floor'))
        # a job the host has not confirmed yet: charged what its descriptor carried, so the cap when it was placed
        # on a host not yet in expected mode, its expectation when it was
        for sent, charge in ((True, 4), (False, 20)):
            pending = self.job(self.submit(candidates=['ember'], slots=1, memory_gb=12, memory_max_gb=20)['job'])
            pending.update(status='launching', host_id='ember', host='ember', stage_attempted=True, memory_expected_gb=4,
                           host_config={'expected_memory_admission': sent})
            cq.save_job(self.ctx, pending)
            fit = fq._fit(self.ctx, dict(head, memory_expected_gb=4), probe, fq.all_jobs(self.ctx), now)
            self.assertEqual(fit['reserved'], 2 + charge, sent)
            pending.update(status='failed')
            cq.save_job(self.ctx, pending)

    def test_a_charge_the_host_reports_is_believed_only_when_it_is_plausible(self):
        gib = fq.GIB
        data = dict(reserved_memory_bytes=30 * gib, held_memory_bytes=3 * gib)
        self.assertEqual(fq._host_charge(dict(data, charged_memory_bytes=2 * gib)), 2 * gib)
        self.assertEqual(fq._host_charge(dict(data, charged_memory_bytes=0)), 0)
        self.assertEqual(fq._host_charge(dict(data, charged_memory_bytes=27 * gib)), 27 * gib)
        for bad in (28 * gib, -1, True, 1.5 * gib, '2', None):
            self.assertEqual(fq._host_charge(dict(data, charged_memory_bytes=bad)), 27 * gib, bad)   # the cap charge
        self.assertEqual(fq._host_charge(data), 27 * gib)
        self.assertEqual(fq._host_charge(dict(reserved_memory_bytes=30 * gib, charged_memory_bytes=2 * gib)), 2 * gib)

    def test_caps_may_add_up_to_twice_what_the_host_can_commit_and_no_more(self):
        gib = fq.GIB
        # 64 GB host, floor 4: 60 GB can be committed, so the caps together may be 120 GB
        jid, probe = self.expecting(held_gb=0, avail=60)
        probe['data'].update(active_fleet_slots=2, reserved_memory_bytes=100 * gib, charged_memory_bytes=2 * gib,
                             held_memory_bytes=0,
                             active_fleet=[dict(id=jid, slots=1, reserved_memory_bytes=50 * gib, expected_memory_bytes=gib),
                                           dict(id='b', slots=1, reserved_memory_bytes=50 * gib, expected_memory_bytes=gib)])
        job = dict(slots=1, memory_max_gb=20, memory_expected_gb=1, requires=[], requested_defer=None)
        now = self.clock[0]
        self.assertIsNotNone(fq._fit(self.ctx, job, probe, [], now))
        self.assertEqual(fq._fit(self.ctx, dict(job, memory_max_gb=20.5), probe, [], now, explain=True), (None, fq.OVERCOMMIT_WAIT))
        # a job placed but not yet there counts with its cap
        pending = self.job(self.submit(candidates=['ember'], slots=1, memory_gb=1, memory_max_gb=10)['job'])
        pending.update(status='launching', host_id='ember', host='ember', stage_attempted=True, memory_expected_gb=1,
                       host_config={'expected_memory_admission': True})
        cq.save_job(self.ctx, pending)
        self.assertEqual(fq._fit(self.ctx, dict(job, memory_max_gb=10.5), probe, fq.all_jobs(self.ctx), now, explain=True),
                         (None, fq.OVERCOMMIT_WAIT))
        self.assertIsNotNone(fq._fit(self.ctx, dict(job, memory_max_gb=10), probe, fq.all_jobs(self.ctx), now))
        # no bound without expected-use admission: caps are charged in full instead, as before
        for host_flag, switch in ((False, True), (True, False)):
            off = copy.deepcopy(probe)
            off['data']['expected_memory_admission'] = host_flag
            off['binding']['expected_memory_admission'] = switch
            self.assertIsNotNone(fq._fit(self.ctx, dict(job, memory_max_gb=20.5), off, [], now), (host_flag, switch))
        # waiting for the bound is temporary: the job keeps waiting and holds the host, it is not failed
        waiting = self.submit(candidates=['ember'], slots=1, memory_gb=1, memory_max_gb=21)['job']
        self.assertTrue(fq._fits_idle(self.job(waiting), probe))
        pending.update(status='failed')
        cq.save_job(self.ctx, pending)
        probe['at'] = self.clock[0]
        self.assertEqual(fq._reserve(self.ctx, {'ember': probe}), [])
        self.assertEqual(self.queued(waiting), ('queued', fq.OVERCOMMIT_WAIT))

    def test_the_shadow_time_uses_the_same_memory_model_as_placement(self):
        gib = fq.GIB
        jid, probe = self.expecting()
        head = self.job(self.submit(candidates=['ember'], slots=1, memory_gb=12, memory_max_gb=50)['job'])
        head = dict(head, memory_expected_gb=35)
        jobs = fq.all_jobs(self.ctx)
        now = self.clock[0]
        end = now - 300 + 1200
        # 31 GB are free now and the job needs 35; when the running job ends 5 (its expectation: 2 charged, 3 held) come back
        self.assertEqual(fq._fit(self.ctx, head, probe, jobs, now, explain=True), (None, 'RAM floor'))
        self.assertEqual(fq._shadow(head, probe, jobs, now), end)
        self.assertIsNone(fq._shadow(dict(head, memory_expected_gb=36.01), probe, jobs, now))   # not even then
        self.assertEqual(fq._shadow(dict(head, memory_expected_gb=36), probe, jobs, now), end)
        self.assertIsNone(fq._shadow(dict(head, memory_expected_gb=31), probe, jobs, now))   # it fits now: nothing to wait for
        self.assertIsNotNone(fq._fit(self.ctx, dict(head, memory_expected_gb=31), probe, jobs, now))
        # the running job holds 6 of an expected 5: it is charged its cap (24 more) and all 30 come back
        over = copy.deepcopy(probe)
        over['data'].update(available_bytes=34 * gib, held_memory_bytes=6 * gib, charged_memory_bytes=24 * gib)
        over['data']['active_fleet'][0]['held_memory_bytes'] = 6 * gib
        self.assertIsNotNone(fq._fit(self.ctx, dict(head, memory_expected_gb=6), over, jobs, now))
        self.assertEqual(fq._fit(self.ctx, dict(head, memory_expected_gb=6.01), over, jobs, now, explain=True), (None, 'RAM floor'))
        self.assertEqual(fq._shadow(dict(head, memory_expected_gb=34), over, jobs, now), end)   # 34 - 24 - 4 + 30 = 36
        self.assertIsNone(fq._shadow(dict(head, memory_expected_gb=36.5), over, jobs, now))
        # the overcommit bound ends with a job too: two running jobs of 50 GB caps, a third of 21 waits for one to end
        probe['data'].update(active_fleet_slots=2, reserved_memory_bytes=100 * gib, charged_memory_bytes=2 * gib,
                             held_memory_bytes=0, available_bytes=60 * gib,
                             active_fleet=[dict(id=jid, slots=1, reserved_memory_bytes=50 * gib, expected_memory_bytes=gib),
                                           dict(id='second', slots=1, reserved_memory_bytes=50 * gib, expected_memory_bytes=gib)])
        second = self.job(self.submit(candidates=['ember'], slots=1, est_minutes=40, memory_gb=1, memory_max_gb=50)['job'])
        second.update(id='second', status='running', host_id='ember', host='ember', host_config={'machine_id': 'a' * 32},
                      t_launch=now - 300, t_started=now - 300)
        jobs = [dict(j, memory_max_gb=50) if j['id'] == jid else j for j in jobs] + [second]
        small = dict(head, memory_max_gb=21, memory_expected_gb=1)
        self.assertEqual(fq._fit(self.ctx, small, probe, jobs, now, explain=True), (None, fq.OVERCOMMIT_WAIT))
        self.assertEqual(fq._shadow(small, probe, jobs, now), end)
        # the same host on caps (switch off): the bound plays no part, and nothing here is kept out by it
        probe['binding']['expected_memory_admission'] = False
        self.assertNotEqual(fq._fit(self.ctx, small, probe, jobs, now, explain=True)[1], fq.OVERCOMMIT_WAIT)

    def test_a_host_is_in_expected_mode_when_its_helper_says_so_and_its_configuration_does_not_switch_it_off(self):
        payload = dict(version=1, supported=True, safety_protocol='shared-heavy-gate-v1', machine_id='a'*32,
                       policy=dict(pool_slots=8, load_cap=20, ram_floor_gb=4), load=1, available_bytes=40*fq.GIB,
                       total_bytes=64*fq.GIB, logical_cpus=16, active_fleet_slots=0, reserved_memory_bytes=0,
                       legacy_busy=False, ordinary_waiting=False, active_fleet=[], terminal_fleet=[])
        conf = dict(address='host', host_id='ember')
        def probe(**data):
            with patch.object(cq, 'job_ssh', return_value=(0, json.dumps(dict(payload, **data)), '')):
                return fq._remote_probe(self.ctx, 'ember', str(self.state), dict(conf))
        self.assertIs(probe()['binding']['expected_memory_admission'], False)   # an old helper says nothing
        self.assertFalse(fq._expects(probe()))
        for odd in (None, 'yes', 1, False):
            self.assertFalse(fq._expects(probe(expected_memory_admission=odd)), odd)
        self.assertTrue(fq._expects(probe(expected_memory_admission=True)))
        conf['expected_memory_admission'] = False
        self.assertFalse(fq._expects(probe(expected_memory_admission=True)))
        conf['expected_memory_admission'] = True
        self.assertTrue(fq._expects(probe(expected_memory_admission=True)))
        # the key is read from host.conf.json (a mistyped value is left out, as for the other keys) and the laptop never expects
        (self.state / 'host.conf.json').write_text(json.dumps({'address': 'ember', 'host_id': 'ember', 'expected_memory_admission': False}))
        self.assertIs(cq.host_conf(str(self.state))['expected_memory_admission'], False)
        (self.state / 'host.conf.json').write_text(json.dumps({'address': 'ember', 'host_id': 'ember', 'expected_memory_admission': 'no'}))
        self.assertNotIn('expected_memory_admission', cq.host_conf(str(self.state)))
        self.assertFalse(fq._expects(dict(host='laptop', data=dict(expected_memory_admission=True),
                                          binding=dict(expected_memory_admission=True))))

    def descriptor(self, **changes):
        import base64
        job = self.job(self.submit(candidates=['ember'], memory_gb=1, memory_max_gb=12)['job'])
        job.update(memory_expected_gb=3, deferred_load_cap=12, host_config={'slots': 8, 'expected_memory_admission': True})
        job.update(changes)
        script = cq.make_launcher(job)
        encoded = re.search(r'printf %s ([A-Za-z0-9+/=]+) \| base64 -d > "\$D/deferred.json"', script).group(1)
        return json.loads(base64.b64decode(encoded))

    def test_the_descriptor_carries_the_expectation_only_for_a_host_in_expected_mode_and_below_the_cap(self):
        self.assertEqual(self.descriptor()['memory_expected_gb'], 3)
        self.assertEqual(self.descriptor()['memory_max_gb'], 12)
        self.assertNotIn('memory_expected_gb', self.descriptor(host_config={'slots': 8, 'expected_memory_admission': False}))
        self.assertNotIn('memory_expected_gb', self.descriptor(host_config={'slots': 8}))     # a binding from an older controller
        self.assertNotIn('memory_expected_gb', self.descriptor(memory_expected_gb=12))        # not below the cap
        self.assertNotIn('memory_expected_gb', self.descriptor(memory_expected_gb=13))
        job = self.job(self.submit(candidates=['ember'], memory_gb=1, memory_max_gb=12)['job'])
        job.update(deferred_load_cap=12, host_config={'slots': 8, 'expected_memory_admission': True})
        job.pop('memory_expected_gb', None)
        import base64
        script = cq.make_launcher(job)   # a record from before the field
        encoded = re.search(r'printf %s ([A-Za-z0-9+/=]+) \| base64 -d > "\$D/deferred.json"', script).group(1)
        self.assertNotIn('memory_expected_gb', json.loads(base64.b64decode(encoded)))
        for bad in (0, -1, True, 'x', float('nan')):
            self.assertNotIn('memory_expected_gb', self.descriptor(memory_expected_gb=bad), bad)

    def test_a_host_reason_for_a_unit_killed_for_memory_reaches_the_job_record(self):
        reason = ('out of memory (oom-kill): the fleet pool limit (20.0 GB) was reached while it ran, so jobs running together '
                  'may have needed more than the pool; its own use is not known, its cap was 4 GB')
        for exit_code in (None, 137):
            with self.subTest(exit=exit_code):
                jid = self.submit(candidates=['ember'])['job']
                job = fq._reserve(self.ctx, {'ember': self.remote()})[0]
                job.update(stage_attempted=True, launch_uncertain=False, deferred_registered=True, status='running')
                cq.save_job(self.ctx, job)
                ent = dict(started=self.clock[0] - 10, launched=self.clock[0] - 12, finished=self.clock[0], exit=exit_code, alive=False, cpu=1)
                state = dict(version=1, id=jid, phase='failed', registered=True, reason=reason)
                with patch.object(fq, '_probes', return_value={'ember': self.remote()}), \
                        patch.object(cq, 'deferred_control', return_value=state), \
                        patch.object(cq, 'host_snapshot', return_value=({'jobs': {jid: ent}}, None)), \
                        patch.object(cq, 'fetch_results', return_value=([], [])), patch.object(cq, 'drop_ref'):
                    fq._pass(self.ctx, {})
                final = self.job(jid)
                self.assertEqual((final['status'], final['exit']), ('failed', exit_code))
                self.assertEqual(final['reason'], reason)
                self.assertRegex(final['reason'], telemetry._OOM)
                # a failure that is not a memory kill keeps the reason it has always had
        jid = self.submit(candidates=['ember'])['job']
        job = fq._reserve(self.ctx, {'ember': self.remote()})[0]
        job.update(stage_attempted=True, launch_uncertain=False, deferred_registered=True, status='running')
        cq.save_job(self.ctx, job)
        ent = dict(started=self.clock[0] - 10, launched=self.clock[0] - 12, finished=self.clock[0], exit=2, alive=False, cpu=1)
        state = dict(version=1, id=jid, phase='failed', registered=True, reason='unit exited without success')
        with patch.object(fq, '_probes', return_value={'ember': self.remote()}), \
                patch.object(cq, 'deferred_control', return_value=state), \
                patch.object(cq, 'host_snapshot', return_value=({'jobs': {jid: ent}}, None)), \
                patch.object(cq, 'fetch_results', return_value=([], [])), patch.object(cq, 'drop_ref'):
            fq._pass(self.ctx, {})
        self.assertEqual((self.job(jid)['status'], self.job(jid)['reason']), ('failed', 'unit exited without success'))

    def test_idle_finalization_takes_a_finished_jobs_charge_from_the_hosts_own_figure(self):
        gib = fq.GIB
        first = self.submit(candidates=['ember'])['job']
        job = self.job(first)
        job.update(status='done', host_id='ember', host_config={'machine_id': 'a' * 32}, t_finished=self.clock[0])
        cq.save_job(self.ctx, job)
        ember = self.remote('ember')
        ember['data'].update(active_fleet_slots=2, reserved_memory_bytes=40 * gib, held_memory_bytes=4 * gib,
                             expected_memory_admission=True, charged_memory_bytes=29 * gib,
                             active_fleet=[dict(id=first, slots=1, reserved_memory_bytes=10 * gib, held_memory_bytes=gib,
                                                expected_memory_bytes=3 * gib),
                                           dict(id='external-job', slots=1, reserved_memory_bytes=30 * gib, held_memory_bytes=3 * gib)])
        # the finished job was charged 3 - 1 = 2 GB; the other, with no expectation, 30 - 3 = 27 GB
        self.ctx._fleet_pass_probes = {'ember': ember}
        final = {}
        with patch.object(fq, '_capacity', side_effect=lambda ctx, probes, jobs, cache_only=False: final.update(probes)):
            fq._finalize_capacity(self.ctx, fq.all_jobs(self.ctx))
        data = final['ember']['data']
        self.assertEqual((data['reserved_memory_bytes'], data['held_memory_bytes'], data['charged_memory_bytes']),
                         (30 * gib, 3 * gib, 27 * gib))
        self.assertEqual(fq._host_charge(data), 27 * gib)

    def test_the_capacity_row_carries_the_hosts_reason_for_charging_caps(self):
        blocker = 'a fleet job runs outside the fleet slice'
        probe = self.remote()
        probe['data'].update(expected_memory_admission=False, expected_memory_blocker=blocker)
        fq._capacity(self.ctx, {'ember': probe}, fq.all_jobs(self.ctx))
        devices = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']
        self.assertEqual(devices['ember']['expected_memory_blocker'], blocker)
        self.assertNotIn('expected_memory_blocker', devices['hearth'])   # no probe, no host figure
        # nothing to say (the host is in expected mode, or its helper is older), or not text: no key
        for value in (None, '', 5, ['x']):
            probe['data']['expected_memory_blocker'] = value
            fq._capacity(self.ctx, {'ember': probe}, fq.all_jobs(self.ctx))
            self.assertNotIn('expected_memory_blocker', cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']['ember'])
        probe['data'].pop('expected_memory_blocker')
        fq._capacity(self.ctx, {'ember': probe}, fq.all_jobs(self.ctx))
        self.assertNotIn('expected_memory_blocker', cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']['ember'])
        # a probe field the controller does not know changes nothing else about the host's row
        probe['data'].update(expected_memory_blocker=blocker)
        plain = self.remote()
        fq._capacity(self.ctx, {'ember': plain}, fq.all_jobs(self.ctx))
        without = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']['ember']
        fq._capacity(self.ctx, {'ember': probe}, fq.all_jobs(self.ctx))
        with_text = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']['ember']
        self.assertEqual({k: v for k, v in with_text.items() if k != 'expected_memory_blocker'}, without)

    def agreement(self, active, free_gib, cap, expected_gb, limit=60, memory_max='limit', disk_gib=100):
        """A 64 GiB host (RAM floor 4 GiB) in this state, asked for a job of this cap and expectation. Returns
        (what the real fleet_probe says, what the real fleet_reason says, what the controller's _fit says on that
        probe); the host's checks are the helper's own, on what the state patches below show."""
        gib = fq.GIB
        helper = load_host_helper()
        value = {'fleet': True, 'mode': 'immediate', 'load_cap': 20, 'ram_floor_gb': 4, 'memory_max_gb': cap,
                 'memory_expected_gb': expected_gb, 'pool_slots': 8, 'slots': 1}
        policy = {'pool_slots': 8, 'load_cap': 20, 'ram_floor_gb': 4}
        shown = 'infinity\n' if limit is None else str(limit * gib) + '\n'
        enforced = limit if memory_max == 'limit' else memory_max
        with tempfile.TemporaryDirectory() as temp, \
             patch.object(helper, 'Q', Path(temp)), patch.object(helper, 'check', return_value={'ok': True}), \
             patch.object(helper, 'host_policy', return_value=policy), \
             patch.object(helper, 'command', return_value=types.SimpleNamespace(stdout=shown, returncode=0, stderr='')), \
             patch.object(helper, 'total_memory', return_value=64 * gib), \
             patch.object(helper, 'pool_memory_max', return_value=None if enforced is None else enforced * gib), \
             patch.object(helper, 'reservations', side_effect=lambda: (copy.deepcopy(active), False, False)), \
             patch.object(helper, 'legacy_busy', return_value=False), \
             patch.object(helper, 'existing_lock_free', return_value=True), \
             patch.object(helper, 'run_facts', return_value={}), \
             patch.object(helper, 'machine_id', return_value='a' * 32), \
             patch.object(helper, 'available_after', return_value=int(free_gib * gib)), \
             patch.object(helper, 'queue_free', return_value=None if disk_gib is None else int(disk_gib * gib)), \
             patch.object(helper.os, 'getloadavg', return_value=(1, 0, 0), create=True):
            data = json.loads(json.dumps(helper.fleet_probe()))
            reason = helper.fleet_reason(value)
        probe = self.remote()
        probe['data'] = data
        probe['binding'].update(expected_memory_admission=data.get('expected_memory_admission') is True)
        head = self.job(self.submit(candidates=['ember'], slots=1, memory_gb=1, memory_max_gb=2)['job'])
        job = dict(head, memory_max_gb=cap, memory_expected_gb=expected_gb)
        fit, why = fq._fit(self.ctx, job, probe, fq.all_jobs(self.ctx), self.clock[0], explain=True)
        return data, reason, (fit, why)

    def agrees(self, *args, admitted, flag, **host):
        data, reason, (fit, why) = self.agreement(*args, **host)
        self.assertIs(data['expected_memory_admission'], flag, data['expected_memory_blocker'])
        self.assertEqual(reason is None, admitted, reason)
        self.assertEqual(fit is not None, reason is None, (reason, why))   # the controller admits if and only if the helper does
        return data, reason, why

    def test_the_controller_admits_exactly_when_the_host_helper_does(self):
        gib = fq.GIB
        def row(jid, cap, expected, held, pooled=True):
            return {'id': jid, 'phase': 'running', 'slots': 1, 'reserved_memory_bytes': cap * gib, 'held_memory_bytes': held * gib,
                    'expected_memory_bytes': expected * gib, 'pooled': pooled,
                    'control_group': '/user.slice/cq-fleet.slice/cq-deferred-' + jid + '.service'}
        one = [row('cq-a', 30, 5, 3)]   # cap 30 GiB, expected 5, holds 3: charged 2 on expectations, 27 on caps
        # the caps fit (4 + 27 + 10 of 58): the same on either charge
        data, _, _ = self.agrees(one, 58, 10, 3, admitted=True, flag=True)
        self.assertEqual(data['charged_memory_bytes'], 2 * gib)
        self.agrees(one, 58, 10, 3, admitted=True, flag=False, limit=None)
        # the expectations fit (4 + 2 + 4) and the caps do not (4 + 27 + 40): only a host in expected mode takes it
        self.agrees(one, 58, 40, 4, admitted=True, flag=True)
        data, reason, why = self.agrees(one, 58, 40, 4, admitted=False, flag=False, limit=None)
        self.assertIn('insufficient', reason)
        self.assertEqual(why, 'RAM floor')
        # one more byte of what the new job is expected to use than is left on expectations (58 - 4 - 2 = 52 GiB)
        self.agrees(one, 58, 52, 52, admitted=True, flag=True)
        self.agrees(one, 58, 53, 52.01, admitted=False, flag=True)
        # expected mode blocked by memory outside the fleet jobs (64 - 50 - 3 = 11 GiB, the floor allows 3): caps
        data, _, _ = self.agrees(one, 50, 40, 4, admitted=False, flag=False)
        self.assertEqual(data['expected_memory_blocker'], 'memory outside the fleet pool exceeds the floor')
        self.assertEqual(data['charged_memory_bytes'], 27 * gib)
        self.agrees(one, 50, 10, 3, admitted=True, flag=False)           # 4 + 27 + 10 of 50: it fits on caps
        # blocked by a job outside the pool slice (one that was running before the upgrade): caps
        outside = [row('cq-old', 30, 5, 3, pooled=False)]
        data, _, _ = self.agrees(outside, 58, 40, 4, admitted=False, flag=False)
        self.assertEqual(data['expected_memory_blocker'], 'a fleet job runs outside the fleet slice')
        self.agrees(outside, 58, 10, 3, admitted=True, flag=False)
        # blocked by a limit the kernel does not enforce
        data, _, _ = self.agrees(one, 58, 40, 4, admitted=False, flag=False, memory_max=None)
        self.assertEqual(data['expected_memory_blocker'], 'fleet slice limit not in effect')
        # the overcommit bound: the caps together may be twice the pool (2 x 60 GiB), counted when in expected mode
        two = [row('cq-a', 55, 1, 0), row('cq-b', 55, 1, 0)]
        self.agrees(two, 62, 10, 1, admitted=True, flag=True)             # 120
        data, reason, why = self.agrees(two, 62, 11, 1, admitted=False, flag=True)   # 121
        self.assertIn('overcommit', reason)
        self.assertEqual(why, fq.OVERCOMMIT_WAIT)
        # without the pool the bound does not apply, and the caps (4 + 110 + 10) do not fit 62 GiB anyway
        self.agrees(two, 62, 10, 1, admitted=False, flag=False, limit=None)

    def test_a_host_whose_job_disk_is_nearly_full_takes_no_fleet_job(self):
        # ember, 12 Nov 2026: 0 bytes free on the job directories' disk for seven hours; the controller went on placing
        # jobs there (it showed "CPU slots unavailable") and the jobs died at their first write
        data, reason, why = self.agrees([], 62, 10, 3, admitted=False, flag=True, disk_gib=9.9)
        self.assertEqual((reason, why), ('queue disk below floor', fq.DISK_WAIT))
        self.assertIn('queue disk below floor', data['reasons'])
        self.assertEqual(data['queue_free_bytes'], int(9.9 * fq.GIB))
        self.assertNotIn(fq.DISK_WAIT, fq.NEVER_FITS)   # the job waits for room; it is not failed
        data, _, _ = self.agrees([], 62, 10, 3, admitted=True, flag=True, disk_gib=10)
        self.assertNotIn('queue disk below floor', data['reasons'])
        # a file system that cannot be read holds nothing back, and neither does a helper that reports no figure
        data, _, _ = self.agrees([], 62, 10, 3, admitted=True, flag=True, disk_gib=None)
        self.assertIsNone(data['queue_free_bytes'])
        probe = self.remote()
        self.assertNotIn('queue_free_bytes', probe['data'])
        head = self.job(self.submit(candidates=['ember'], slots=1, memory_gb=1, memory_max_gb=2)['job'])
        self.assertIsNotNone(fq._fit(self.ctx, head, probe, fq.all_jobs(self.ctx), self.clock[0]))
        # deferred work does not bind to the host either: staging is what writes the source copy
        probe['data']['queue_free_bytes'] = 0
        night = dict(head, requested_defer='overnight')
        self.assertEqual(fq._fit(self.ctx, night, probe, fq.all_jobs(self.ctx), self.clock[0], explain=True), (None, fq.DISK_WAIT))
        self.assertEqual(fq.DISK_FLOOR_BYTES, load_host_helper().DISK_FLOOR_BYTES)
        self.assertEqual(fq.RECALLED, load_host_helper().RECALLED)   # the reason a recall is known by, on both sides

    def test_cleanup_rounds_on_the_remote_hosts(self):
        # no fleet job's source copy was ever removed from its host: the engine's cleanup ran for the ordinary queue only
        done = self.job(self.submit(candidates=['ember'], slots=1, memory_gb=1, memory_max_gb=2)['job'])
        binding = dict(self.remote()['binding'])
        with self.ctx.qlock():
            done.update(status='done', host_id='ember', host=binding['address'], host_config=binding)
            cq.save_job(self.ctx, done)
        gone = 'cq-20261107T000000-00000000000000aa'   # the ordinary queue's: this store has no record of it
        ent = dict(exit=0, started=1, launched=1, finished=2, cpu=1, alive=False, src=True)
        sent = []
        with patch.object(cq, 'host_snapshot', return_value=({'jobs': {done['id']: dict(ent), gone: dict(ent)}}, '')) as snapshot, \
                patch.object(cq, 'job_ssh', side_effect=lambda ctx, job, script, timeout=60: (sent.append((job, script)), (0, '', ''))[1]):
            self.assertEqual(fq._cleanup_host(self.ctx, binding), 0)   # one command, taken: nothing is left
            self.assertEqual(snapshot.call_args.kwargs, dict(host=binding['address'], binding=binding))
        (job, script), = sent
        self.assertEqual((job['host'], job['host_config']['machine_id']), (binding['address'], binding['machine_id']))
        self.assertIn(f'rm -rf "$HOME/q/{done["id"]}/src" "$HOME/q/{done["id"]}/tmp"', script)
        self.assertNotIn(gone, script)
        # a job pinned to another machine at the same address is not this host's to clean
        other = dict(binding, machine_id='f' * 32)
        with patch.object(cq, 'host_snapshot', return_value=({'jobs': {done['id']: dict(ent)}}, '')), \
                patch.object(cq, 'job_ssh') as ssh:
            self.assertEqual(fq._cleanup_host(self.ctx, other), 0)
            ssh.assert_not_called()
        with patch.object(cq, 'host_snapshot', return_value=(None, 'ssh exit 255')):
            self.assertEqual(fq._cleanup_host(self.ctx, binding), 0)
        # a command the host does not take is work left, and so is what the round's four commands did not reach
        with patch.object(cq, 'host_snapshot', return_value=({'jobs': {done['id']: dict(ent)}}, '')), \
                patch.object(cq, 'job_ssh', return_value=(255, '', 'ssh timed out')):
            self.assertEqual(fq._cleanup_host(self.ctx, binding), 1)
        many = {f'cq-v2--fleet-20261111T{i:06d}-{i:016x}': dict(ent) for i in range(400)}
        with patch.object(cq, 'host_snapshot', return_value=({'jobs': many}, '')), \
                patch.object(cq, 'load_job', side_effect=lambda ctx, name: dict(done, id=name)), \
                patch.object(cq, 'job_ssh', return_value=(0, '', '')) as ssh:
            self.assertGreater(fq._cleanup_host(self.ctx, binding), 0)
            self.assertEqual(ssh.call_count, cq.CLEANUP_CALLS)
        # the disk check does not hide that a job can never run on the host
        low = self.remote()
        low['data']['queue_free_bytes'] = 0
        wide = dict(done, status='queued', slots=20, memory_max_gb=2)
        self.assertEqual(fq._fit(self.ctx, wide, low, [], self.clock[0], explain=True)[1], fq.SLOTS_NEVER)
        heavy = dict(done, status='queued', slots=1)
        low['binding']['slot_vcpu'] = 30
        self.assertEqual(fq._fit(self.ctx, heavy, low, [], self.clock[0], explain=True)[1], fq.LOAD_NEVER)

        # the pacing: at once, again soon while rounds send commands, slowly otherwise, once more when the jobs are gone
        class Done:
            def __init__(self, value):
                self.value = value

            def done(self):
                return True

            def result(self):
                if isinstance(self.value, Exception):
                    raise self.value
                return self.value

        class Pool:
            def __init__(self):
                self.results, self.calls = [], []

            def submit(self, fn, ctx, binding):
                self.calls.append(binding['machine_id'])
                return Done(self.results.pop(0) if self.results else 0)
        pool, rounds = Pool(), {}
        probes = {'ember': self.remote()}
        with patch.object(fq, '_hosts', return_value={'ember': (None, {})}), patch.object(fq, '_probes', side_effect=lambda ctx: probes):
            pool.results = [3, 2, 0]
            self.assertTrue(fq._cleanup_rounds(self.ctx, pool, rounds, True))      # the first round starts at once
            self.assertEqual(len(pool.calls), 1)
            self.assertTrue(fq._cleanup_rounds(self.ctx, pool, rounds, True))      # it sent commands: a backlog
            self.assertEqual(len(pool.calls), 1)                                    # ... the next round is not due yet
            self.clock[0] += fq.CLEANUP_AGAIN_S
            self.assertTrue(fq._cleanup_rounds(self.ctx, pool, rounds, True))
            self.assertEqual(len(pool.calls), 2)
            self.assertTrue(fq._cleanup_rounds(self.ctx, pool, rounds, True))      # its result is read on the next pass
            self.clock[0] += fq.CLEANUP_AGAIN_S
            self.assertTrue(fq._cleanup_rounds(self.ctx, pool, rounds, True))
            self.assertEqual(len(pool.calls), 3)
            self.assertFalse(fq._cleanup_rounds(self.ctx, pool, rounds, True))     # that one sent nothing: the slow pace
            self.clock[0] += fq.CLEANUP_EVERY_S - 1
            self.assertFalse(fq._cleanup_rounds(self.ctx, pool, rounds, True))
            self.assertEqual(len(pool.calls), 3)
            # the last active job has gone: one more round now, for its copy, and the controller waits for it
            self.assertTrue(fq._cleanup_rounds(self.ctx, pool, rounds, False))
            self.assertEqual(len(pool.calls), 4)
            self.assertFalse(fq._cleanup_rounds(self.ctx, pool, rounds, False))    # nothing sent, nothing active: it may stop
            self.assertEqual(len(pool.calls), 4)
            # a host that never takes the commands keeps the controller for CLEANUP_STREAK rounds, not for good
            pool.results = [1] * (fq.CLEANUP_STREAK + 5)
            self.clock[0] += fq.CLEANUP_EVERY_S
            before = len(pool.calls)
            for _ in range(4 * fq.CLEANUP_STREAK):
                if not fq._cleanup_rounds(self.ctx, pool, rounds, False):
                    break
                self.clock[0] += fq.CLEANUP_AGAIN_S
            self.assertEqual(len(pool.calls) - before, fq.CLEANUP_STREAK)
            pool.results = []
            # a round that raised counts as one that sent nothing; a host without a fresh identity is not touched
            pool.results = [OSError('boom')]
            self.clock[0] += fq.CLEANUP_EVERY_S
            calls = len(pool.calls)
            fq._cleanup_rounds(self.ctx, pool, rounds, True)
            self.assertFalse(fq._cleanup_rounds(self.ctx, pool, rounds, True))
            probes['ember'] = None
            self.clock[0] += fq.CLEANUP_EVERY_S
            self.assertFalse(fq._cleanup_rounds(self.ctx, pool, rounds, True))
            self.assertEqual(len(pool.calls), calls + 1)

    def test_the_sizing_note_takes_the_history_it_is_given(self):
        self.finished_runs(3, .5, 1, command='echo small')
        small = self.job(self.submit(candidates=['ember'], command='echo small', memory_gb=12, memory_max_gb=12)['job'])
        own = fq._sizing_note(self.ctx, small)
        self.assertIn('sizing: ', own)
        history = fq.all_jobs(self.ctx)
        with patch.object(fq, 'all_jobs', side_effect=RuntimeError('scanned again')):
            self.assertEqual(fq._sizing_note(self.ctx, small, history=history), own)
        self.assertEqual(fq._sizing_note(self.ctx, small, history=[]), '')   # no record, no note
        # one submission reads the archive once
        scans = []
        real = fq._engine()._jobs_in
        def counted(ctx, subdir, *rest):
            scans.append(subdir)
            return real(ctx, subdir, *rest)
        with patch.object(fq._engine(), '_jobs_in', side_effect=counted):
            note = self.submit(candidates=['ember'], command='echo small', memory_gb=12, memory_max_gb=12)['note']
        self.assertIn('sizing: ', note)
        self.assertEqual(sum(1 for s in scans if str(s).endswith(fq._engine().ARCHIVE_SUFFIX)), 1)

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

    def finished_runs(self, n, peak_gb, minutes, held_gb=None, held_samples=12, cores=None, **fields):
        for _ in range(n):
            self.clock[0] += 1
            jid = self.submit(candidates=['ember'], memory_gb=1, memory_max_gb=1, est_minutes=1, **fields)['job']
            job = self.job(jid)
            job.update(status='done', exit=0, t_started=self.clock[0], t_finished=self.clock[0] + minutes * 60,
                       metrics={'peak_memory_bytes': int(peak_gb * fq.GIB), 'memory_kind': 'linux-cgroup'})
            if held_gb is not None:
                job['metrics'].update(peak_held_bytes=int(held_gb * fq.GIB), held_samples=held_samples)
            if cores is not None:   # the unit's CPU time over its wall time
                job['metrics'].update(cpu_usage_usec=int(cores * minutes * 60 * 1e6), cpu_wall_s=minutes * 60)
            cq.save_job(self.ctx, job)

    def script(self, relative, text):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + chr(10), encoding='utf-8')

    def test_cpu_readings_reach_the_job_record_with_or_without_a_memory_peak(self):
        gib = fq.GIB
        jid = self.submit(candidates=['ember'])['job']
        job = self.job(jid)
        job.update(status='running', host_id='ember')
        cq.save_job(self.ctx, job)
        fq._remote_metrics(self.ctx, job, {'memory_peak_bytes': 4 * gib, 'cpu_usage_usec': 90_000_000, 'cpu_wall_s': 60.5,
                                           'cpu_peak_cores': 3.5})
        self.assertEqual(self.job(jid)['metrics'], {'peak_memory_bytes': 4 * gib, 'memory_kind': 'linux-cgroup',
                                                    'cpu_usage_usec': 90_000_000, 'cpu_wall_s': 60.5, 'cpu_peak_cores': 3.5})
        fq._remote_metrics(self.ctx, job, {'cpu_usage_usec': 5, 'cpu_wall_s': 2})
        self.assertEqual(self.job(jid)['metrics'], {'cpu_usage_usec': 5, 'cpu_wall_s': 2})
        for used, wall in ((-1, 60), (1.5, 60), (True, 60), (None, 60), (5, 0), (5, None), (5, float('inf'))):
            fq._remote_metrics(self.ctx, job, {'memory_peak_bytes': gib, 'cpu_usage_usec': used, 'cpu_wall_s': wall})
            self.assertEqual(self.job(jid)['metrics'], {'peak_memory_bytes': gib, 'memory_kind': 'linux-cgroup'}, (used, wall))

    def test_a_new_jobs_expected_cpu_is_a_quarter_over_the_largest_average_of_three_runs(self):
        def expected(command='echo cpu', slots=4):
            return self.job(self.submit(candidates=['ember'], command=command, slots=slots)['job']).get('cpu_expected')
        self.assertIsNone(expected())                                    # no record: charged what it declares
        self.finished_runs(2, 1, 3, cores=.9, command='echo cpu', slots=4)
        self.assertIsNone(expected())                                    # two runs are not a record
        self.finished_runs(1, 1, 3, command='echo cpu', slots=4)         # a run without CPU readings does not count
        self.finished_runs(1, 1, 0.05, cores=4, command='echo cpu', slots=4)   # nor one of three seconds
        self.finished_runs(3, 1, 3, cores=.4, command='echo cpu', slots=2)     # nor runs held to another quota
        self.assertIsNone(expected())
        self.finished_runs(1, 1, 3, cores=1.1, command='echo cpu', slots=4)
        self.assertEqual(expected(), 1.63)                               # 1.25 x 1.1 + 0.25
        self.assertEqual(fq._job_slots({'slots': 4, 'cpu_expected': 1.63}, True), 2)
        self.assertEqual(expected(slots=2), .75)                         # the two-slot runs are that declaration's record
        self.assertIsNone(expected(slots=8))                             # what it did within 4 slots says nothing about 8
        self.assertIsNone(expected('echo cpu other'))                    # `echo cpu` names no script: no borrowing
        self.finished_runs(3, 1, 3, cores=3.9, command='echo busy', slots=4)
        self.assertEqual(expected('echo busy'), 4.0)                     # a command that uses its slots keeps them
        # a command with no record of its own takes that of the same script file with other arguments
        self.script('tools/run.sh', 'echo run')
        self.finished_runs(3, 1, 3, cores=1.5, command='bash tools/run.sh a', slots=4)
        self.assertEqual(expected('bash tools/run.sh b'), 2.13)
        self.assertIsNone(expected('bash tools/other.sh b'))             # no such file in the snapshot
        note = fq._sizing_note(self.ctx, self.job(self.submit(candidates=['ember'], command='echo cpu', slots=4)['job']),
                               memory=False, estimate=False)
        self.assertIn('it reserves 4 slots and 3 earlier successful runs of this command used 1.1 cores on average at most', note)
        self.assertNotIn('slots', fq._sizing_note(self.ctx, self.job(self.submit(candidates=['ember'], command='echo busy', slots=4)['job']),
                                                  memory=False, estimate=False))

    def test_only_a_named_script_file_lends_its_record_to_other_arguments(self):
        for command, name in (('bash .baton/remote/pymut.sh pkg/a', ('bash', '.baton/remote/pymut.sh')),
                              ('python3 tools/x.py --fast', ('python3', 'tools/x.py')),
                              ('./tools/check.sh all', ('./tools/check.sh',)),
                              ('python3 -m pytest tests', None), ('python3 -m mypy .', None), ('bash -c "make all"', None),
                              ('go test -race ./...', None), ('go test -run X ./pkg', None), ('timeout 600 make', None),
                              ('FOO=1 go build', None), ('cd sub && make', None), ('check.sh', None), ('', None)):
            self.assertEqual(fq._script_of(command), name, command)
        # three light runs of one tool invocation say nothing about another: the cap, and the declared slots
        self.finished_runs(3, 11.5, 3, held_gb=.3, cores=1.0, command='python3 -m pytest tests', slots=4)
        heavy = self.job(self.submit(candidates=['ember'], command='python3 -m mypy .', slots=4, memory_gb=2, memory_max_gb=12)['job'])
        self.assertEqual(heavy['memory_expected_gb'], 12)
        self.assertNotIn('cpu_expected', heavy)
        self.script('tools/mem.sh', 'echo mem')
        self.finished_runs(3, 11.5, 3, held_gb=.8, command='bash tools/mem.sh a')
        borrow = lambda: self.job(self.submit(candidates=['ember'], command='bash tools/mem.sh b', memory_gb=2, memory_max_gb=12)['job'])
        self.assertEqual(borrow()['memory_expected_gb'], 2.6)
        self.assertTrue(borrow()['script_key'])
        # the file's content is part of its name: an edited script, or another worktree's file at the same path, starts anew
        self.script('tools/mem.sh', 'echo something heavier')
        self.assertEqual(borrow()['memory_expected_gb'], 12)
        self.assertIsNone(self.job(self.submit(candidates=['ember'], command='python3 -m pytest tests')['job'])['script_key'])
        self.assertIsNone(fq._script_key(str(self.root), 'HEAD', '.', 'bash ../outside.sh'))

    def taking(self, host='ember', used=6, **kw):
        """A probe of a host that takes waiting jobs (no queue_on_host key) and has `used` of its 6 fleet slots taken."""
        probe = self.remote(host=host, **kw)
        del probe['binding']['queue_on_host']
        probe['data'].update(active_fleet_slots=used, reserved_memory_bytes=fq.GIB,
                             active_fleet=[{'id': 'cq-there-' + host, 'slots': used, 'reserved_memory_bytes': fq.GIB}])
        return probe

    def test_a_job_that_has_waited_for_room_is_handed_to_its_host_to_wait_there(self):
        jid = self.submit(candidates=['ember'], slots=2)['job']
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.taking()}), [])                 # full, and the job is new
        self.assertEqual(self.job(jid)['placement_reasons'], {'ember': 'CPU slots unavailable'})
        self.clock[0] += fq.HANDOVER_AFTER_S - 1
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.taking()}), [])
        self.clock[0] += 1
        placed = fq._reserve(self.ctx, {'ember': self.taking()})
        self.assertEqual([j['id'] for j in placed], [jid])
        job = self.job(jid)
        self.assertEqual((job['status'], job['host_id'], job['handed_over']), ('launching', 'ember', 'CPU slots unavailable'))
        self.assertEqual(job['waited'], {'CPU slots unavailable': fq.HANDOVER_AFTER_S})
        # a job that fits is placed as before and is not marked
        fits = self.submit(candidates=['hearth'], slots=1)['job']
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'hearth': self.taking('hearth', used=2)})], [fits])
        self.assertNotIn('handed_over', self.job(fits))

    def test_hand_over_is_only_for_want_of_room_and_within_bounds(self):
        def waits(probes, **job):
            jid = self.submit(**dict(dict(candidates=['ember'], slots=2), **job))['job']
            self.clock[0] += fq.HANDOVER_AFTER_S + 5
            made = {name: (make() if callable(make) else make) for name, make in probes.items()}
            placed = [j['id'] for j in fq._reserve(self.ctx, made)]
            if jid not in placed:
                fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(self.root), session='s1'))
            return jid in placed
        self.assertTrue(waits({'ember': self.taking}))
        off = lambda: dict(self.taking(), binding=dict(self.taking()['binding'], queue_on_host=False))
        self.assertFalse(waits({'ember': off}))                                               # the host's configuration says no
        legacy = lambda: dict(self.taking(used=0), data=dict(self.taking(used=0)['data'], legacy_busy=True))
        self.assertFalse(waits({'ember': legacy}))                                            # not a matter of room
        self.assertFalse(waits({'ember': self.taking}, slots=7))                              # never fits: nothing to wait for
        self.assertFalse(waits({'ember': self.taking}, defer='overnight'))                    # waits by request already
        self.assertFalse(waits({'ember': lambda: None}))                                      # no fresh word from the host
        # of two hosts without room, the one with less work ahead
        busy = lambda: self.taking('ember')
        quiet = lambda: self.taking('hearth')
        long_one = self.submit(candidates=['ember'], slots=1, est_minutes=90)['job']
        job = self.job(long_one)
        job.update(status='running', host_id='ember', t_started=self.clock[0])
        cq.save_job(self.ctx, job)
        both = self.submit(candidates=['ember', 'hearth'], slots=2)['job']
        self.clock[0] += fq.HANDOVER_AFTER_S + 5
        self.assertEqual([(j['id'], j['host_id']) for j in fq._reserve(self.ctx, {'ember': busy(), 'hearth': quiet()})], [(both, 'hearth')])
        # a host takes HANDOVER_MAX_WAITING unstarted jobs and no more
        for _ in range(fq.HANDOVER_MAX_WAITING - 1):
            extra = self.job(self.submit(candidates=['hearth'], slots=1)['job'])
            extra.update(status='launching', host_id='hearth', deferred_registered=True, deferred_phase='queued')
            cq.save_job(self.ctx, extra)
        self.assertFalse(waits({'hearth': quiet}, candidates=['hearth']))

    def waiting_there(self, host='ember', **fields):
        """A job of this store registered on `host` and not yet started (handed over to wait there)."""
        job = self.job(self.submit(candidates=[host], **fields)['job'])
        job.update(status='launching', host_id=host, stage_attempted=True, deferred_registered=True, deferred_phase='queued',
                   t_launch=self.clock[0], handed_over='CPU slots unavailable', host=host,
                   host_config=self.remote(host)['binding'], dispatch_state=str(self.state))
        cq.save_job(self.ctx, job)
        return job['id']

    def test_a_job_waiting_on_its_host_is_charged_there_so_its_room_is_not_given_away(self):
        # the host has 6 fleet slots, all free by its probe; a 5-slot job is registered there and has not started
        held = self.waiting_there(slots=5, memory_gb=2, memory_max_gb=20)
        later = self.submit(candidates=['ember'], slots=2)['job']
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.remote()}), [])
        self.assertEqual(self.job(later)['placement_reasons'], {'ember': 'CPU slots unavailable'})
        fits = self.submit(candidates=['ember'], slots=1)['job']                              # beside it there is room for one slot
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': self.remote()})], [fits])
        fq.execute(self.ctx, 'cancel', dict(job=fits, root=str(self.root), session='s1'))
        # memory likewise: 40 GB available less its 20 GB cap and the 4 GB floor leaves 16
        fq.execute(self.ctx, 'cancel', dict(job=later, root=str(self.root), session='s1'))
        big = self.submit(candidates=['ember'], slots=1, memory_gb=2, memory_max_gb=17)['job']
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.remote()}), [])
        self.assertEqual(self.job(big)['placement_reasons'], {'ember': 'RAM floor'})
        # when it ends cannot be told before it starts: no shadow time for a job held behind it
        self.assertEqual(fq._shadow_limit(self.job(big), self.remote(), fq.all_jobs(self.ctx), self.clock[0]), (None, None))
        # once the host lists it as active the probe carries its charge, and the record adds nothing
        self.assertEqual(fq._pending(fq.all_jobs(self.ctx), 'ember', {held}), [])
        # deferred work registered for the night waits by request and is still not charged
        job = self.job(held)
        job['requested_defer'] = 'overnight'
        cq.save_job(self.ctx, job)
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': self.remote()})], [big])

    def test_a_job_is_handed_only_to_a_host_where_it_could_start_once_the_running_jobs_end(self):
        def probe(avail, held=0):
            made = self.taking(used=1, avail=avail)
            made['data'].update(reserved_memory_bytes=30*fq.GIB, held_memory_bytes=held*fq.GIB,
                                active_fleet=[{'id': 'cq-there-ember', 'slots': 1, 'reserved_memory_bytes': 30*fq.GIB,
                                               'held_memory_bytes': held*fq.GIB}])
            return made
        # cap 20 GB and the 4 GB floor fit the host's 64 GB, so the job is not refused; 18 GB are available
        jid = self.submit(candidates=['ember'], slots=1, memory_gb=2, memory_max_gb=20)['job']
        self.clock[0] += fq.HANDOVER_AFTER_S + 5
        self.assertTrue(fq._fits_idle(self.job(jid), probe(18)))
        self.assertFalse(fq._fits_drained(self.job(jid), probe(18)))                         # the running job holds nothing: none comes back
        self.assertEqual(fq._reserve(self.ctx, {'ember': probe(18)}), [])                     # it stays with the controller
        self.assertEqual(self.job(jid)['placement_reasons'], {'ember': 'RAM floor'})
        self.assertFalse(fq._fits_drained(self.job(jid), probe(18, held=40)))                # more held than reserved: not believed
        self.assertFalse(fq._fits_drained(self.job(jid), self.laptop()))
        self.assertFalse(fq._fits_drained(self.job(jid), None))
        self.assertTrue(fq._fits_drained(self.job(jid), probe(18, held=6)))                  # 6 GB come back when it ends: 24 are enough
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': probe(18, held=6)})], [jid])
        self.assertEqual(self.job(jid)['handed_over'], 'RAM floor')

    def test_of_two_hosts_a_job_is_handed_to_the_one_where_it_is_expected_to_start_sooner(self):
        def running(host, est, since):
            job = self.job(self.submit(candidates=[host], slots=6, est_minutes=est, timeout_minutes=480)['job'])
            job.update(status='running', host_id=host, stage_attempted=True, deferred_registered=True, deferred_phase='running',
                       t_launch=self.clock[0] - since, t_started=self.clock[0] - since)
            cq.save_job(self.ctx, job)
            return job['id']

        def probe(host, jid):
            made = self.remote(host=host)
            del made['binding']['queue_on_host']
            made['data'].update(active_fleet_slots=6, reserved_memory_bytes=fq.GIB,
                                active_fleet=[{'id': jid, 'slots': 6, 'reserved_memory_bytes': fq.GIB}])
            return made
        self.clock[0] += 2 * 3600
        long_one = running('ember', 90, 85 * 60)       # more work on ember by the estimates, but it ends in five minutes
        short_one = running('hearth', 30, 60)          # hearth's job has 29 minutes left
        jid = self.submit(candidates=['ember', 'hearth'], slots=2)['job']
        self.clock[0] += fq.HANDOVER_AFTER_S + 5
        placed = fq._reserve(self.ctx, {'ember': probe('ember', long_one), 'hearth': probe('hearth', short_one)})
        self.assertEqual([(j['id'], j['host_id']) for j in placed], [(jid, 'ember')])
        self.assertEqual(self.job(jid)['handed_over'], 'CPU slots unavailable')

    def test_work_deferred_to_the_night_does_not_use_up_a_hosts_hand_over_places(self):
        for _ in range(fq.HANDOVER_MAX_WAITING):
            night = self.job(self.submit(candidates=['ember'], slots=1, defer='overnight')['job'])
            night.update(status='launching', host_id='ember', stage_attempted=True, deferred_registered=True, deferred_phase='queued')
            cq.save_job(self.ctx, night)
        jid = self.submit(candidates=['ember'], slots=2)['job']
        self.clock[0] += fq.HANDOVER_AFTER_S + 5
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': self.taking()})], [jid])

    def test_the_wait_on_the_host_is_recorded_when_the_job_starts(self):
        jid = self.submit(candidates=['ember'], slots=2)['job']
        self.clock[0] += fq.HANDOVER_AFTER_S
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': self.taking()})], [jid])
        self.clock[0] += 20                                                                   # the push
        self.ctx.run_remoterun = lambda argv, cwd: (0, f'CQ_REGISTERED {jid}\n', '')
        with patch.object(cq, 'make_launcher', return_value='true'):
            fq._stage_remote(self.ctx, self.job(jid))
        job = self.job(jid)
        self.assertEqual((job['deferred_phase'], job['t_registered']), ('queued', self.clock[0]))
        registered = self.clock[0]
        self.clock[0] += 600                                                                  # it waits there ten minutes
        row = cq.status_row(self.job(jid), self.clock[0])
        self.assertEqual(row['wait_min'], round((fq.HANDOVER_AFTER_S + 20 + 600) / 60, 1))   # still waiting: to now, not to the hand-over
        fact = dict(id=jid, phase='running', started=str(int(registered) + 540), launched=str(int(registered) + 539), finished=None, exit=None)
        fq._remote_reconcile(self.ctx, self.job(jid), fact)
        job = self.job(jid)
        self.assertEqual((job['status'], job['host_waited']), ('running', 540))
        self.assertEqual(job['waited'], {fq.WAIT_FIRST_LOOK: fq.HANDOVER_AFTER_S, fq.WAIT_ON_HOST: 540})
        self.assertEqual(cq.status_row(job, self.clock[0])['wait_min'], round((fq.HANDOVER_AFTER_S + 20 + 540) / 60, 1))
        fq._remote_reconcile(self.ctx, self.job(jid), fact)                                   # recorded once
        self.assertEqual(self.job(jid)['waited'][fq.WAIT_ON_HOST], 540)
        # a job that was not handed over keeps its wait to the launch, and its seconds on the host are recorded too
        plain = self.submit(candidates=['hearth'], slots=1)['job']
        self.clock[0] += 30
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'hearth': self.remote(host='hearth')})], [plain])
        self.ctx.run_remoterun = lambda argv, cwd: (0, f'CQ_REGISTERED {plain}\n', '')
        with patch.object(cq, 'make_launcher', return_value='true'):
            fq._stage_remote(self.ctx, self.job(plain))
        fq._remote_reconcile(self.ctx, self.job(plain), dict(id=plain, phase='running', started=str(int(self.clock[0]) + 3)))
        job = self.job(plain)
        self.assertEqual((job['host_waited'], job['waited'][fq.WAIT_ON_HOST]), (3, 3))
        self.clock[0] += 300
        self.assertEqual(cq.status_row(job, self.clock[0])['wait_min'], 0.5)

    def test_a_reservation_made_under_a_host_policy_that_then_changed_goes_back_to_the_queue(self):
        jid = self.submit(candidates=['ember'], slots=2)['job']
        self.clock[0] += 5
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': self.remote()})], [jid])   # pool 8 in its binding
        self.assertEqual(self.job(jid)['host_config']['slots'], 8)
        self.assertFalse(fq._policy_stale(self.job(jid), self.remote()))
        self.clock[0] += 40
        grown = self.remote(slots=12)                                                         # the host's pool has grown
        self.assertTrue(fq._policy_stale(self.job(jid), grown))
        self.assertEqual(fq._resume_pending(self.ctx, {'ember'}, {'ember': grown}), [])       # a worker holds the host: left alone
        self.assertEqual(self.job(jid)['status'], 'launching')
        self.assertEqual(fq._resume_pending(self.ctx, set(), {'ember': grown}), [])           # not staged with the old descriptor
        job = self.job(jid)
        self.assertEqual((job['status'], job['host_id'], job['placed_where'], job['deferred_phase']), ('queued', None, None, 'staging_pending'))
        for key in ('host_config', 'dispatch_state', 't_launch', 'launch_uncertain', 'execution_command'):
            self.assertNotIn(key, job)
        self.assertEqual(job['waited'], {fq.WAIT_FIRST_LOOK: 5, fq.WAIT_REPLACED: 40})
        self.clock[0] += 10
        placed = fq._reserve(self.ctx, {'ember': self.remote(slots=12)})                      # placed afresh, under the new policy
        self.assertEqual([(j['id'], j['host_config']['slots']) for j in placed], [(jid, 12)])
        self.assertEqual(sum(self.job(jid)['waited'].values()), self.job(jid)['t_launch'] - self.job(jid)['t_submitted'])
        fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(self.root), session='s1'))
        # a restage that is due is judged the same way; a changed load cap counts as well
        other = self.unconfirmed(restage_due=True, stage_resume_after=0)
        capped = self.remote()
        capped['data']['policy']['load_cap'] = capped['binding']['load_cap'] = 24
        self.assertEqual(fq._resume_pending(self.ctx, set(), {'ember': capped}), [])
        self.assertEqual(self.job(other)['status'], 'queued')

    def test_a_waiting_job_is_charged_by_admission_but_not_shown_as_occupying(self):
        held = self.waiting_there(slots=5, memory_gb=2, memory_max_gb=20)
        probe = self.remote()
        probe['data'].update(active_fleet_slots=6, reserved_memory_bytes=8*fq.GIB,
                             active_fleet=[{'id': 'cq-there', 'slots': 6, 'reserved_memory_bytes': 8*fq.GIB}])
        fq._capacity(self.ctx, {'ember': probe}, fq.all_jobs(self.ctx))
        device = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']['ember']
        # the host's six fleet slots are taken by what runs; the job that waits for them is not added on top
        self.assertEqual((device['slots_used'], device['slots_total'], device['memory_reserved_gb']), (6, 6, 8))
        # a job being staged, whose outcome is not known, is still shown
        staging = self.job(held)
        staging.update(deferred_registered=False, deferred_phase='staging')
        cq.save_job(self.ctx, staging)
        fq._capacity(self.ctx, {'ember': probe}, fq.all_jobs(self.ctx))
        self.assertEqual(cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']['ember']['slots_used'], 11)

    def test_a_waiting_job_the_host_lists_as_ended_is_no_longer_charged(self):
        held = self.waiting_there(slots=5)
        later = self.submit(candidates=['ember'], slots=2)['job']
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.remote()}), [])                 # charged while it waits
        ended = self.remote()
        ended['data']['terminal_fleet'] = [{'id': held, 'phase': 'done', 'exit_code': 0}]     # it ran and ended between two looks
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': ended})], [later])

    def test_a_registered_job_its_host_no_longer_has_is_ended(self):
        def controller_pass(jid, directory):
            with patch.object(fq, '_probes', return_value={'ember': self.remote()}), \
                    patch.object(cq, 'deferred_control', return_value=self.absent(jid)), \
                    patch.object(cq, 'job_ssh', return_value=(0, directory + '\n', '')):
                fq._pass(self.ctx, {})
        held = self.waiting_there(slots=5)
        controller_pass(held, 'present')                                                      # something is there: not this rule's business
        self.assertEqual(self.job(held)['status'], 'launching')
        self.clock[0] += 31
        controller_pass(held, 'absent')
        job = self.job(held)
        self.assertEqual((job['status'], job['reason']), ('failed', 'registered on its host, which no longer has it; submit it again'))
        self.assertEqual(fq._pending(fq.all_jobs(self.ctx), 'ember', set()), [])              # and its room is free again
        night = self.waiting_there(slots=1, defer='overnight')                                # deferred work is left to its own pickup
        controller_pass(night, 'absent')
        self.assertEqual(self.job(night)['status'], 'launching')

    def test_a_restage_that_is_due_gets_its_probe_while_its_host_is_paused(self):
        jid = self.submit(candidates=['hearth'])['job']
        pending = fq._reserve(self.ctx, {'hearth': self.remote('hearth')})[0]
        pending.update(stage_attempted=True, stage_at=self.clock[0], launch_uncertain=True, host_config=self.remote('hearth')['binding'],
                       dispatch_state=self.ctx.p('hosts', 'hearth'))
        cq.save_job(self.ctx, pending)
        cq.write_json(self.ctx.p('hosts.json'), {'version': 1, 'hosts': {}})                  # the host is taken out (or paused)
        with patch.object(fq, '_remote_probe', return_value=self.remote('hearth')) as probe:
            self.assertNotIn('hearth', fq._probes(self.ctx))                                  # an attempt under way: nothing to resume
            job = self.job(jid)
            job.update(restage_due=True, stage_resume_after=0)
            cq.save_job(self.ctx, job)
            self.ctx._fleet_remote_sampled = None
            probes = fq._probes(self.ctx)
        self.assertIn('hearth', probes)
        self.assertEqual([j['id'] for j in fq._resume_pending(self.ctx, set(), probes)], [jid])

    def test_the_wait_on_the_host_of_a_registration_learnt_later_is_recorded_too(self):
        jid = self.unconfirmed()                                                              # the acknowledgement was lost
        staged = self.clock[0]
        self.clock[0] += 50
        fact = dict(id=jid, phase='running', started=str(int(staged) + 45))
        fq._remote_reconcile(self.ctx, self.job(jid), fact)                                   # the host has it, and it runs
        job = self.job(jid)
        self.assertNotIn('t_registered', job)
        self.assertEqual((job['status'], job['host_waited'], job['waited'][fq.WAIT_ON_HOST]), ('running', 45, 45))

    def test_a_job_that_fits_now_goes_before_a_hand_over_on_the_same_host(self):
        # ember has one fleet slot left. A two-slot job has waited its two minutes and would be handed over; a
        # one-slot job that fits now and is short enough to pass it takes the host's one launcher first.
        waiting = self.submit(candidates=['ember'], slots=2, est_minutes=1)['job']
        self.clock[0] += fq.HANDOVER_AFTER_S + 5
        fitting = self.submit(candidates=['ember'], slots=1, est_minutes=1)['job']
        placed = fq._reserve(self.ctx, {'ember': self.taking(used=5)})
        self.assertEqual([j['id'] for j in placed], [fitting])
        job = self.job(waiting)
        self.assertEqual((job['status'], job['placement_reasons']), ('queued', {'ember': 'CPU slots unavailable'}))
        self.assertNotIn('handed_over', job)
        # with the launcher free again it is handed over
        fq.execute(self.ctx, 'cancel', dict(job=fitting, root=str(self.root), session='s1'))
        self.clock[0] += 5
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': self.taking(used=5)})], [waiting])
        self.assertEqual(self.job(waiting)['handed_over'], 'CPU slots unavailable')

    def listing(self, probe, *jids):
        """`probe` with the host's list of registered jobs that have not started (a helper that has `recall`)."""
        probe['data']['queued_fleet'] = [{'id': jid, 'reason': 'fleet CPU pool exhausted'} for jid in jids]
        return probe

    def test_a_handed_over_job_is_recalled_when_another_candidate_has_room(self):
        held = self.waiting_there(slots=2)
        job = self.job(held)
        job.update(candidates=['ember', 'hearth'], t_registered=self.clock[0])
        cq.save_job(self.ctx, job)

        def look(hearth=None, listed=True, wanted=True):
            recalls = []
            ember = self.listing(self.taking(), held) if listed else self.taking()
            placed = fq._reserve(self.ctx, {'ember': ember, 'hearth': hearth or self.remote(host='hearth')}, 3,
                                 recalls if wanted else None)
            self.assertEqual(placed, [])
            return [j['id'] for j in recalls]
        self.assertEqual(look(), [])                                                          # it has only just got there
        self.clock[0] += fq.RECALL_AFTER_S
        self.assertEqual(look(listed=False), [])                                              # a helper without `recall`
        self.assertEqual(look(hearth=self.taking('hearth')), [])                              # no room there either
        self.assertEqual(look(wanted=False), [])
        self.assertEqual(look(), [held])
        self.assertEqual(self.job(held)['recall_tried'], self.clock[0])
        self.assertEqual(look(), [])                                                          # not asked again at once
        # the host has started it meanwhile: it stays where it is
        with patch.object(cq, 'deferred_control', return_value=dict(version=1, id=held, phase='starting')) as control:
            self.assertFalse(fq._recall(self.ctx, self.job(held)))
        self.assertEqual(control.call_args.args[2], 'recall')
        self.assertEqual((self.job(held)['status'], self.job(held)['host_id']), ('launching', 'ember'))
        # the host gave it back: queued again, without that host, and placed on the other
        self.clock[0] += 30
        given = dict(version=1, id=held, phase='cancelled', reason=fq.RECALLED)
        with patch.object(cq, 'deferred_control', return_value=given):
            self.assertTrue(fq._recall(self.ctx, self.job(held)))
        job = self.job(held)
        self.assertEqual((job['status'], job['host_id'], job['candidates']), ('queued', None, ['hearth']))
        self.assertEqual([(r['host_id'], r['machine_id']) for r in job['recalled_from']], [('ember', 'a' * 32)])
        self.assertEqual(job['waited'][fq.WAIT_ON_HOST], fq.RECALL_AFTER_S + 30)
        for key in ('handed_over', 't_registered', 'host_config', 'host_dir'):
            self.assertNotIn(key, job)
        asked = self.job(held)                                                                # what a slower caller still holds
        other = self.remote(host='hearth')
        other['binding']['machine_id'] = 'b' * 32
        placed = fq._reserve(self.ctx, {'ember': self.taking(), 'hearth': other}, 3, [])
        self.assertEqual([(j['id'], j['host_id']) for j in placed], [(held, 'hearth')])
        # the same answer arriving again, from a status call that asked the first host before the job moved, is about
        # a placement that is over: the job stays where it now is
        stale = dict(asked, status='launching', host_id='ember', t_launch=self.clock[0] - fq.RECALL_AFTER_S - 30)
        self.assertFalse(fq._requeue_recalled(self.ctx, stale))
        with patch.object(cq, 'deferred_control', return_value=given):
            fq._remote_reconcile(self.ctx, stale, explicit=True)
        job = self.job(held)
        self.assertEqual((job['status'], job['host_id'], job['candidates'], len(job['recalled_from'])), ('launching', 'hearth', ['hearth'], 1))
        # and so is any other answer about the first host: a status that still said `queued` there is not written
        # on the record the job has become
        still = dict(version=1, id=held, phase='queued', registered=1, reason='fleet CPU pool exhausted')
        with patch.object(cq, 'deferred_control', return_value=still):
            fq._remote_reconcile(self.ctx, stale, explicit=True)
        job = self.job(held)
        self.assertEqual((job['deferred_registered'], job['deferred_phase'], job['reason']), (False, 'staging', 'staging outcome pending confirmation'))
        # its directory on the host it was recalled from is removed by that host's cleanup, though it has not ended
        binding = dict(self.remote()['binding'])
        ent = dict(exit=None, started=None, launched=None, finished=None, cpu=None, alive=False, src=True)
        sent = []
        there = {held: dict(ent)}
        with patch.object(cq, 'host_snapshot', side_effect=lambda *a, **kw: ({'jobs': dict(there)}, '')), \
                patch.object(cq, 'job_ssh', side_effect=lambda ctx, job, script, timeout=60: (sent.append(script), (0, '', ''))[1]):
            there[held]['alive'] = True                                                       # never under something alive
            fq._cleanup_host(self.ctx, binding)
            self.assertEqual(sent, [])
            there[held]['alive'] = False
            fq._cleanup_host(self.ctx, binding)
            self.assertEqual(sent, [f'rm -rf "$HOME/q/{held}"\n'])
            del sent[:]
            fq._cleanup_host(self.ctx, dict(binding, machine_id='b' * 32))                    # not on the host it is on now
            self.assertEqual(sent, [])
            self.assertEqual(self.job(held)['candidates'], ['hearth'])                        # the directory was still listed
            # once the first host no longer lists the directory, that host is a candidate again, and its cleanup
            # leaves a directory of that ID alone from then on (the job may be staged there afresh)
            del there[held]
            fq._cleanup_host(self.ctx, binding)
            job = self.job(held)
            self.assertEqual((job['candidates'], job['recalled_from'][0]['removed']), (['hearth', 'ember'], True))
            there[held] = dict(ent)
            fq._cleanup_host(self.ctx, binding)
            self.assertEqual(sent, [])
        # a job is recalled once in its life
        again = self.job(held)
        again.update(status='launching', deferred_registered=True, deferred_phase='queued', handed_over='CPU slots unavailable',
                     stage_attempted=True, t_registered=self.clock[0] - 10 * fq.RECALL_AFTER_S, candidates=['hearth', 'ember'])
        again.pop('recall_tried', None)
        cq.save_job(self.ctx, again)
        recalls = []
        fq._reserve(self.ctx, {'hearth': self.listing(self.taking('hearth'), held), 'ember': self.remote()}, 3, recalls)
        self.assertEqual(recalls, [])

    def test_one_recall_at_a_time_and_a_recalled_job_goes_first(self):
        def waiting(registered_ago):
            jid = self.waiting_there(slots=2)
            job = self.job(jid)
            job.update(candidates=['ember', 'hearth'], t_registered=self.clock[0] - registered_ago,
                       t_submitted=self.clock[0] - registered_ago - 150)                      # the older one is first in order
            cq.save_job(self.ctx, job)
            return jid

        def look():
            recalls = []
            fq._reserve(self.ctx, {'ember': self.listing(self.taking(), first, second), 'hearth': self.remote(host='hearth')}, 3, recalls)
            return [j['id'] for j in recalls]
        first, second = waiting(500), waiting(400)
        self.assertEqual(look(), [first])
        self.clock[0] += fq.RECALL_PENDING_S - 1
        self.assertEqual(look(), [])                                                          # the room is promised to the first
        self.clock[0] += 1
        self.assertEqual(look(), [second])
        # given back, the first is ahead of every queued job, an aged one included
        aged = self.job(self.submit(candidates=['hearth'])['job'])
        aged['t_submitted'] = self.clock[0] - 2 * fq.AGE_S
        with patch.object(cq, 'deferred_control', return_value=dict(version=1, id=first, phase='cancelled', reason=fq.RECALLED)):
            self.assertTrue(fq._recall(self.ctx, self.job(first)))
        self.assertLess(fq._order(self.job(first), self.clock[0]), fq._order(aged, self.clock[0]))
        later = self.clock[0] + fq.RECALL_PENDING_S                                           # ... while its recall is fresh, no longer
        self.assertGreater(fq._order(self.job(first), later), fq._order(aged, later))
        # a job whose host is being asked about it now is not chosen: its recall could not be sent
        third = waiting(300)
        self.clock[0] += fq.RECALL_PENDING_S
        recalls = []
        probes = {'ember': self.listing(self.taking(), third), 'hearth': self.remote(host='hearth')}
        fq._reserve(self.ctx, probes, 3, recalls, {third})
        self.assertEqual(recalls, [])
        self.assertNotIn('recall_tried', self.job(third))

    def test_hand_overs_keep_to_the_limit_of_a_pass(self):
        fits = self.submit(candidates=['hearth'], slots=1)['job']
        waits = self.submit(candidates=['ember'], slots=2)['job']
        self.clock[0] += fq.HANDOVER_AFTER_S + 5
        probes = lambda: {'ember': self.taking(), 'hearth': self.remote(host='hearth')}
        self.assertEqual(len(fq._reserve(self.ctx, probes(), 1)), 1)                          # one placement, whichever came first
        self.assertEqual(sorted(j['status'] for j in (self.job(fits), self.job(waits))), ['launching', 'queued'])
        self.assertEqual(len(fq._reserve(self.ctx, probes(), 0)), 0)
        self.assertEqual(len(fq._reserve(self.ctx, probes(), 3)), 1)
        self.assertEqual({self.job(fits)['status'], self.job(waits)['status']}, {'launching'})

    def test_a_recall_whose_answer_was_lost_is_found_by_the_next_look_and_is_not_an_end(self):
        held = self.waiting_there(slots=2)
        job = self.job(held)
        job.update(candidates=['ember', 'hearth'], t_launch=self.clock[0] - 200, t_registered=self.clock[0] - 190)
        cq.save_job(self.ctx, job)
        fact = dict(id=held, phase='cancelled', reason=fq.RECALLED, exit_code=None)
        self.assertEqual(fq._remote_reconcile(self.ctx, self.job(held), fact)['phase'], 'recalled')    # the probe's row
        job = self.job(held)
        self.assertEqual((job['status'], job['candidates'], job['waited'][fq.WAIT_ON_HOST]), ('queued', ['hearth'], 200))
        # the same from a status answer; and a cancel the owner asked for stays a cancel
        other = self.waiting_there(slots=1)
        job = self.job(other)
        job.update(candidates=['ember', 'hearth'], t_registered=self.clock[0])
        cq.save_job(self.ctx, job)
        state = dict(version=1, id=other, phase='cancelled', reason=fq.RECALLED, registered=1)
        with patch.object(cq, 'deferred_control', return_value=state):
            fq._remote_reconcile(self.ctx, self.job(other), explicit=True)
        self.assertEqual(self.job(other)['status'], 'queued')
        third = self.waiting_there(slots=1)
        job = self.job(third)
        job.update(cancel_requested=True)
        cq.save_job(self.ctx, job)
        with patch.object(cq, 'deferred_control', return_value=dict(state, id=third)), \
                patch.object(cq, 'fetch_results', return_value=([], [])), patch.object(cq, 'drop_ref'):
            fq._remote_reconcile(self.ctx, self.job(third), explicit=True)
        self.assertEqual(self.job(third)['status'], 'cancelled')

    def stuck_pass(self, jid, register, cancel=None):
        """One controller pass for an unconfirmed staging whose directory exists; returns the operations asked of the
        helper. `register` is what the ssh call that asks the helper to register gives: (exit, stdout, stderr)."""
        asked = []

        def control(ctx, job, operation):
            asked.append(operation)
            return {'status': self.absent(jid), 'cancel': cancel}[operation]

        def ssh(ctx, job, script, **kw):
            if ' register ' in script:
                asked.append('register')
                return register
            return (0, 'present\n', '')
        with patch.object(fq, '_probes', return_value={'ember': self.remote()}), \
                patch.object(cq, 'deferred_control', side_effect=control), \
                patch.object(cq, 'job_ssh', side_effect=ssh):
            fq._pass(self.ctx, {})
        return asked

    def test_an_unconfirmed_staging_that_left_a_complete_copy_is_registered(self):
        jid = self.unconfirmed()
        self.clock[0] += fq.STUCK_AFTER_S - 1
        self.assertEqual(self.stuck_pass(jid, None), ['status'])                              # its push may still be on its way
        self.clock[0] += 31
        self.assertEqual(self.stuck_pass(jid, (255, '', 'connection lost')), ['status', 'register'])   # no word: nothing concluded
        self.assertEqual((self.job(jid)['status'], self.job(jid).get('deferred_registered')), ('launching', False))
        self.clock[0] += 31
        registered = json.dumps(dict(version=1, id=jid, phase='queued', registered=1, reason='waiting for resources'))
        self.assertEqual(self.stuck_pass(jid, (0, registered + '\n', '')), ['status', 'register'])
        job = self.job(jid)
        self.assertEqual((job['status'], job['deferred_registered'], job['deferred_phase'], job['launch_uncertain'], job['t_registered']),
                         ('launching', True, 'queued', False, self.clock[0]))
        self.assertEqual(fq._pending(fq.all_jobs(self.ctx), 'ember', set()), [job])           # charged while it waits, like any

    def test_an_unconfirmed_staging_the_host_will_not_register_is_ended_and_frees_the_host(self):
        jid = self.unconfirmed()
        later = self.submit(candidates=['ember'])['job']
        self.clock[0] += fq.STUCK_AFTER_S + 1
        self.assertEqual(fq._reserve(self.ctx, {'ember': self.remote()}), [])                 # the host is held: a launcher in flight
        refused = (1, json.dumps({'error': 'descriptor does not match installed host policy', 'version': 1}) + '\n', '')
        # an exit other than the helper's own refusal (the connection went, or timed out) ends nothing, whatever it printed
        self.assertEqual(self.stuck_pass(jid, (255, refused[1], 'timed out')), ['status', 'register'])
        self.assertEqual(self.job(jid)['status'], 'launching')
        self.clock[0] += 31
        # nor does a host whose helper fails its own check (the user manager down, a reinstall under way)
        self.assertEqual(self.stuck_pass(jid, (75, 'CQ_HELPER_UNWELL\n', '')), ['status', 'register'])
        self.assertEqual(self.job(jid)['status'], 'launching')
        self.clock[0] += 31
        kept = dict(version=1, id=jid, phase='queued')                                        # the cancel did not take: nothing is ended
        self.assertEqual(self.stuck_pass(jid, refused, kept), ['status', 'register', 'cancel'])
        self.assertEqual(self.job(jid)['status'], 'launching')
        self.clock[0] += 31
        started = dict(version=1, id=jid, phase='cancelled', reason='cancelled before start')   # it had registered after all:
        self.assertEqual(self.stuck_pass(jid, refused, started), ['status', 'register', 'cancel'])   # not this rule's end
        self.assertEqual(self.job(jid)['status'], 'launching')
        self.clock[0] += 31
        cancelled = dict(version=1, id=jid, phase='cancelled', reason='cancelled before registration')
        self.assertEqual(self.stuck_pass(jid, refused, cancelled), ['status', 'register', 'cancel'])
        job = self.job(jid)
        self.assertEqual(job['status'], 'failed')
        self.assertIn('the host will not register', job['reason'])
        self.assertIn('descriptor does not match installed host policy', job['reason'])
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': self.remote()})], [later])

    def test_a_job_the_probe_lists_as_waiting_is_not_asked_about(self):
        held = self.waiting_there(slots=2)
        with patch.object(fq, '_probes', return_value={'ember': self.listing(self.remote(), held)}), \
                patch.object(cq, 'deferred_control') as control:
            fq._pass(self.ctx, {})
            control.assert_not_called()
        self.assertEqual(self.job(held)['reason'], 'fleet CPU pool exhausted')                # the host's word for the wait
        waiting = dict(version=1, id=held, phase='queued', registered=1, reason='fleet CPU pool exhausted')
        with patch.object(fq, '_probes', return_value={'ember': self.remote()}), \
                patch.object(cq, 'deferred_control', return_value=waiting) as control:
            fq._pass(self.ctx, {})                                                            # a helper without the list: as before
            self.assertEqual(control.call_args.args[2], 'status')

    def test_a_placement_starts_unregistered_whatever_the_record_carried(self):
        jid = self.submit(candidates=['ember'])['job']
        job = self.job(jid)
        job.update(deferred_registered=True, deferred_phase='queued', stage_attempted=True, t_registered=5, host_dir='~/q/x')
        cq.save_job(self.ctx, job)                                                            # an answer about an earlier placement, written late
        placed = fq._reserve(self.ctx, {'ember': self.remote()})
        self.assertEqual([j['id'] for j in placed], [jid])
        job = self.job(jid)
        self.assertEqual((job['deferred_registered'], job['deferred_phase']), (False, 'staging'))
        for key in ('stage_attempted', 't_registered', 'host_dir'):
            self.assertNotIn(key, job)
        self.ctx.run_remoterun = lambda argv, cwd: (0, f'CQ_REGISTERED {jid}\n', '')
        with patch.object(cq, 'make_launcher', return_value='true'):
            fq._stage_remote(self.ctx, self.job(jid))                                         # so its staging goes ahead
        self.assertEqual((self.job(jid)['deferred_registered'], self.job(jid)['stage_attempted']), (True, True))

    def test_the_descriptor_of_an_immediate_job_carries_when_it_was_submitted(self):
        self.assertEqual(self.descriptor()['submitted'], int(self.clock[0]))
        self.assertNotIn('submitted', self.descriptor(defer='overnight'))                     # deferred work waits by request

    def test_a_job_that_ends_while_queued_keeps_the_time_it_waited(self):
        jid = self.submit(candidates=['ember'], memory_gb=2, memory_max_gb=10)['job']
        self.clock[0] += 5
        fq._reserve(self.ctx, {'ember': self.remote(avail=12)})
        self.clock[0] += 40
        fq.execute(self.ctx, 'cancel', dict(job=jid, root=str(self.root), session='s1'))
        job = self.job(jid)
        self.assertEqual(job['status'], 'cancelled')
        self.assertEqual(job['waited'], {fq.WAIT_FIRST_LOOK: 5, 'RAM floor': 40})
        self.assertNotIn('waited_at', job)

    def test_slots_are_charged_on_expected_cpu_only_where_the_host_is_set_to(self):
        self.assertEqual(fq._job_slots({'slots': 4, 'cpu_expected': 1.2}, True), 2)
        self.assertEqual(fq._job_slots({'slots': 4, 'cpu_expected': 1.2}, False), 4)
        self.assertEqual(fq._job_slots({'slots': 4, 'cpu_expected': .2}, True), 1)
        for bad in (None, 0, -1, 4.5, '2', True, float('nan')):
            self.assertEqual(fq._job_slots({'slots': 4, 'cpu_expected': bad}, True), 4, bad)
        job = dict(self.job(self.submit(candidates=['ember'], slots=4)['job']), cpu_expected=1.2)

        def probe(helper=True, switch=True, active=4, quota=None):
            p = self.remote()                                            # 8 pool slots, 2 kept for the foreground: 6 for the fleet
            p['data'].update(active_fleet_slots=active, active_fleet=[{'id': 'cq-other', 'slots': active, 'reserved_memory_bytes': fq.GIB}],
                             reserved_memory_bytes=fq.GIB)
            if helper:
                p['data']['expected_cpu_admission'] = True
            if quota is not None:
                p['data']['quota_fleet_slots'] = quota
            p['binding']['expected_cpu_admission'] = switch
            return p

        def fit(p, j=job):
            return fq._fit(self.ctx, j, p, [], self.clock[0], explain=True)
        result, reason = fit(probe())                                    # 4 charged there + 2 for this one fill the 6
        self.assertIsNone(reason)
        self.assertEqual(result['slots_used'], 4)
        self.assertEqual(fit(probe(active=5))[1], 'CPU slots unavailable')
        self.assertEqual(fit(probe(switch=False))[1], 'CPU slots unavailable')     # the host is not set to: 4 + 4 declared
        self.assertEqual(fit(probe(helper=False))[1], 'CPU slots unavailable')     # nor can an older helper
        self.assertEqual(fit(probe(), dict(job, cpu_expected=None))[1], 'CPU slots unavailable')
        # the bound holds wherever jobs charged below their quotas still run, also after the setting was switched off
        off = dict(job, slots=2)
        self.assertEqual(fit(probe(switch=False, active=2, quota=11), off)[1], fq.CPU_OVERCOMMIT_WAIT)   # 11 + 2 > 12
        self.assertIsNone(fit(probe(switch=False, active=2, quota=10), off)[1])
        # the declared slots of everything on the host stay within CPU_OVERCOMMIT times its fleet slots
        self.assertIsNone(fit(probe(quota=8))[1])                        # 8 + 4 = 12 = 2 x 6
        self.assertEqual(fit(probe(quota=9))[1], fq.CPU_OVERCOMMIT_WAIT)
        self.assertIsNone(fit(probe(quota=3))[1])                        # a figure below the charged slots is not believed
        # a job placed there but not yet in the probe is charged as it was sent
        sent = dict(job, id='cq-sent', status='launching', host_id='ember', host_config={'expected_cpu_admission': True})
        unsent = dict(sent, id='cq-unsent', host_config={})
        self.assertIsNone(fq._fit(self.ctx, job, probe(active=2), [sent], self.clock[0], explain=True)[1])        # 2 + 2 + 2
        self.assertEqual(fq._fit(self.ctx, job, probe(active=2), [unsent], self.clock[0], explain=True)[1], 'CPU slots unavailable')
        self.assertFalse(fq._expects_cpu(dict(host='laptop', data=dict(expected_cpu_admission=True), binding=dict(expected_cpu_admission=True))))

    def test_the_descriptor_carries_the_cpu_expectation_only_for_a_host_set_to_charge_it(self):
        on = {'slots': 8, 'expected_cpu_admission': True}
        self.assertEqual(self.descriptor(slots=4, cpu_expected=1.2, host_config=on)['cpu_expected'], 1.2)
        self.assertNotIn('cpu_expected', self.descriptor(slots=4, cpu_expected=1.2, host_config={'slots': 8, 'expected_cpu_admission': False}))
        self.assertNotIn('cpu_expected', self.descriptor(slots=4, cpu_expected=1.2))             # the default binding of the helper
        self.assertNotIn('cpu_expected', self.descriptor(slots=4, cpu_expected=4.0, host_config=on))   # not below the declared slots
        for bad in (0, -1, 'x', None, True):
            self.assertNotIn('cpu_expected', self.descriptor(slots=4, cpu_expected=bad, host_config=on), bad)
        (self.state / 'host.conf.json').write_text(json.dumps({'address': 'ember', 'host_id': 'ember', 'expected_cpu_admission': True}))
        self.assertIs(cq.host_conf(str(self.state)).get('expected_cpu_admission'), True)
        (self.state / 'host.conf.json').write_text(json.dumps({'address': 'ember', 'host_id': 'ember', 'expected_cpu_admission': 'yes'}))
        self.assertNotIn('expected_cpu_admission', cq.host_conf(str(self.state)))

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

    # A job no candidate can ever run: refused at submission from the cached capacity rows, failed at placement from fresh probes.

    def write_capacity(self, **probes):
        """Write capacity.json from these probes (a host given as None is a down row), as the controller does."""
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            fq._capacity(self.ctx, probes, [])

    def edit_capacity(self, host, **fields):
        path = self.ctx.p('fleet', 'capacity.json')
        data = cq.read_json(path)
        data['devices'][host].update(fields)
        cq.write_json(path, data)

    def no_job_left_behind(self):
        self.assertEqual(fq.all_jobs(self.ctx), [])
        self.assertEqual(list((self.root / '.baton' / 'compute').glob('*.json')) if (self.root / '.baton' / 'compute').exists() else [], [])

    def test_capacity_rows_carry_the_total_memory_of_an_up_device(self):
        self.write_capacity(ember=self.remote(), laptop=self.laptop(), hearth=None)
        devices = cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']
        self.assertEqual((devices['ember']['memory_total_gb'], devices['laptop']['memory_total_gb']), (64, 32))
        self.assertIsNone(devices['hearth']['memory_total_gb'])

    def test_a_submission_no_candidate_can_ever_take_is_refused_with_each_reason(self):
        small_ember, small_hearth = self.remote(slots=8), self.remote('hearth', slots=6)   # pools of 6 and 4 slots
        for probe in (small_ember, small_hearth):
            probe['data']['total_bytes'] = 32 * fq.GIB
        self.write_capacity(ember=small_ember, hearth=small_hearth)
        with self.assertRaises(cq.CqError) as raised:
            self.submit(slots=8)
        self.assertEqual(str(raised.exception), 'no candidate can ever run this job: ember: 8 slots exceed its pool of 6; hearth: 8 slots exceed its pool of 4')
        with self.assertRaises(cq.CqError) as raised:
            self.submit(memory_gb=2, memory_max_gb=30)   # 30 GB and the 4 GB floor on 32 GB hosts
        self.assertEqual(str(raised.exception), 'no candidate can ever run this job: ember: cap 30 GB plus the 4 GB floor exceed its 32 GB of memory; '
                         'hearth: cap 30 GB plus the 4 GB floor exceed its 32 GB of memory')
        with self.assertRaises(cq.CqError) as raised:
            self.submit(requires=['blender', 'python'])
        self.assertEqual(str(raised.exception), 'no candidate can ever run this job: ember: lacks the tools blender; hearth: lacks the tools blender')
        self.no_job_left_behind()

    def test_a_memory_refusal_names_the_remedy_only_when_the_cap_was_not_stated(self):
        tiny_ember, tiny_hearth = self.remote(), self.remote('hearth')
        for probe in (tiny_ember, tiny_hearth):
            probe['data']['total_bytes'] = 5 * fq.GIB   # the derived cap of 2.5 GB plus the 4 GB floor exceeds it
        self.write_capacity(ember=tiny_ember, hearth=tiny_hearth)
        hint = ('; the cap was not stated (it came from this command\'s earlier runs or the default for a where: host job): '
                'state memory_max_gb to set it')
        stated = 'no candidate can ever run this job: ember: cap 2.5 GB plus the 4 GB floor exceed its 5 GB of memory; hearth: cap 2.5 GB plus the 4 GB floor exceed its 5 GB of memory'
        with self.assertRaises(cq.CqError) as raised:
            self.submit(memory_gb=2, memory_max_gb=2.5)
        self.assertEqual(str(raised.exception), stated)
        with self.assertRaises(cq.CqError) as raised:
            self.submit(memory_gb=2)
        self.assertEqual(str(raised.exception), stated + hint)
        self.no_job_left_behind()

    def test_a_row_with_no_capability_list_does_not_refuse_a_job_that_requires_tools(self):
        self.write_capacity(ember=self.remote(), hearth=self.remote('hearth'))
        self.edit_capacity('ember', capabilities=[])
        self.edit_capacity('hearth', capabilities=[])
        self.assertTrue(self.submit(requires=['blender'])['ok'])
        self.edit_capacity('ember', capabilities=['linux'])
        self.edit_capacity('hearth', capabilities=['linux'])
        with self.assertRaisesRegex(cq.CqError, 'lacks the tools blender'):
            self.submit(requires=['blender'])

    def test_a_submission_one_candidate_can_take_or_cannot_be_told_about_is_accepted(self):
        big = self.remote('hearth', slots=16)
        self.write_capacity(ember=self.remote(slots=8), hearth=big)
        self.assertTrue(self.submit(slots=8)['ok'])   # hearth has 14 slots
        for name, change in (('no row', lambda: cq.write_json(self.ctx.p('fleet', 'capacity.json'), dict(version=1, at=self.clock[0], devices={
                                  'ember': cq.read_json(self.ctx.p('fleet', 'capacity.json'))['devices']['ember']}))),
                             ('down row', lambda: self.edit_capacity('hearth', up=False)),
                             ('stale row', lambda: self.edit_capacity('hearth', at=self.clock[0] - fq.CAPACITY_ROW_MAX_AGE_S - 1)),
                             ('missing field', lambda: self.edit_capacity('hearth', slots_total=None)),
                             ('not a number', lambda: self.edit_capacity('hearth', slots_total='many'))):
            with self.subTest(name):
                self.write_capacity(ember=self.remote(slots=8), hearth=self.remote('hearth', slots=6))
                change()
                self.assertTrue(self.submit(slots=8)['ok'])
        self.write_capacity(ember=self.remote(slots=8), hearth=self.remote('hearth', slots=6))
        self.edit_capacity('hearth', at=self.clock[0] - fq.CAPACITY_ROW_MAX_AGE_S)   # the bound itself still counts
        with self.assertRaises(cq.CqError):
            self.submit(slots=8)

    def test_a_submission_with_no_capacity_file_is_accepted(self):
        self.assertTrue(self.submit(slots=8, candidates=['ember'])['ok'])

    def test_placement_fails_a_job_every_candidate_never_fits_and_leaves_the_hosts_free(self):
        never = self.submit(slots=8, candidates=['ember', 'hearth'], priority='verify')['job']   # no capacity rows yet: accepted
        small = self.submit(slots=1, candidates=['ember', 'hearth'], priority='lane')['job']
        probes = {'ember': self.remote(slots=8), 'hearth': self.remote('hearth', slots=6)}
        selected = fq._reserve(self.ctx, probes, limit=1)   # the failed job does not use the one placement of the pass
        self.assertEqual([j['id'] for j in selected], [small])
        failed = self.job(never)
        self.assertEqual((failed['status'], failed['exit'], failed['tail'], failed['artifact_digests']), ('failed', None, [], []))
        self.assertEqual(failed['tail_digest'], cq.stored_tail_digest([]))
        self.assertEqual((failed['t_finished'], failed['finished']), (self.clock[0], cq.iso(self.clock[0])))
        self.assertEqual(failed['placement_reasons'], {'ember': fq.SLOTS_NEVER, 'hearth': fq.SLOTS_NEVER})
        self.assertEqual(failed['reason'], 'no candidate can ever run this job: ember: CPU slots exceed host pool; hearth: CPU slots exceed host pool')
        self.assertEqual((failed['notify'], failed['notified'], failed['host_id']), (True, False, None))
        self.assertNotIn('hold_since', failed)
        self.assertTrue((self.root / '.baton' / 'compute' / (never + '.json')).exists())
        self.assertEqual(self.job(small)['status'], 'launching')
        events = [json.loads(line) for line in Path(self.ctx.p('fleet', 'events.jsonl')).read_text().splitlines()]
        self.assertIn((never, 'failed'), [(x['id'], x['status']) for x in events if x['type'] == 'finished'])

    def test_placement_memory_never_fit_fails_the_job(self):
        jid = self.submit(memory_gb=2, memory_max_gb=30, candidates=['ember'])['job']
        probe = self.remote()
        probe['data']['total_bytes'] = 32 * fq.GIB
        self.assertEqual(fq._reserve(self.ctx, {'ember': probe}), [])
        self.assertEqual((self.job(jid)['status'], self.job(jid)['placement_reasons']), ('failed', {'ember': fq.MEMORY_NEVER}))

    def test_placement_keeps_a_job_waiting_when_any_candidate_may_still_take_it(self):
        for name, hearth in (('offline', None), ('stale', 'stale'), ('tools unverified', 'tools')):
            with self.subTest(name):
                jid = self.submit(slots=8, candidates=['ember', 'hearth'], requires=['blender'] if name == 'tools unverified' else [])['job']
                other = self.remote('hearth', slots=6)
                if hearth == 'stale':
                    other['at'] -= 31
                probes = {'ember': self.remote(slots=8, caps=['linux', 'python'] if name == 'tools unverified' else None)}
                if hearth:
                    probes['hearth'] = other
                self.assertEqual(fq._reserve(self.ctx, probes), [])
                self.assertEqual(self.job(jid)['status'], 'queued')
                self.assertEqual(self.job(jid)['reason'], 'waiting for eligible fleet capacity')
                cancelled = self.job(jid)
                cancelled.update(status='cancelled')
                cq.save_job(self.ctx, cancelled)

    def test_a_laptop_never_fits_by_slots_and_physical_memory_only(self):
        jid = self.submit(platform='windows', candidates=['laptop'], slots=5, memory_gb=1, memory_max_gb=2)['job']
        small = self.submit(platform='windows', candidates=['laptop'], memory_gb=1, memory_max_gb=28)['job']   # 28 + 6 GB floor on 32 GB
        borderline = self.submit(platform='windows', candidates=['laptop'], memory_gb=1, memory_max_gb=26)['job']   # 26 + 6 = 32: fits
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            probe = self.laptop(slots=4, commit=10)
            self.assertEqual(fq._fit(self.ctx, self.job(jid), probe, [], self.clock[0], explain=True), (None, fq.SLOTS_NEVER))
            self.assertEqual(fq._fit(self.ctx, self.job(small), probe, [], self.clock[0], explain=True), (None, fq.MEMORY_NEVER))
            # commit free below the commit floor is a temporary shortfall, not a never-fits
            self.assertEqual(fq._fit(self.ctx, self.job(borderline), probe, [], self.clock[0], explain=True), (None, 'physical or commit RAM floor'))
            self.assertEqual(fq._reserve(self.ctx, {'laptop': self.laptop(slots=4, commit=10)}), [])
        self.assertEqual([self.job(j)['status'] for j in (jid, small, borderline)], ['failed', 'failed', 'queued'])

    def test_a_laptop_never_fit_is_said_while_legacy_work_holds_the_laptop(self):
        too_big = self.submit(platform='windows', candidates=['laptop'], slots=5, memory_gb=1, memory_max_gb=2)['job']
        ordinary = self.submit(platform='windows', candidates=['laptop'], memory_gb=1, memory_max_gb=2)['job']
        probe = self.laptop(slots=4)
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=True):
            self.assertEqual(fq._fit(self.ctx, self.job(too_big), probe, [], self.clock[0], explain=True), (None, fq.SLOTS_NEVER))
            self.assertEqual(fq._fit(self.ctx, self.job(ordinary), probe, [], self.clock[0], explain=True),
                             (None, 'disabled, deferred, or legacy work active'))

    def test_a_slot_load_that_fails_even_at_load_zero_never_fits_unless_deferred(self):
        big = self.remote(slots=20)   # 18 slots; 14 slots at 1.5 vCPU = 21 > the load cap of 20
        now_job = self.submit(slots=14, candidates=['ember'], priority='verify')['job']
        later = self.submit(slots=14, candidates=['ember'], defer='overnight')['job']
        self.assertEqual(fq._fit(self.ctx, self.job(now_job), big, [], self.clock[0], explain=True), (None, fq.LOAD_NEVER))
        fit, reason = fq._fit(self.ctx, self.job(later), big, [], self.clock[0], explain=True)
        self.assertIsNotNone(fit)
        # 13 slots is 19.5: it passes at load 0 and only waits at load 1
        thirteen = self.submit(slots=13, candidates=['ember'])['job']
        busy = self.remote(slots=20, load=1)
        self.assertEqual(fq._fit(self.ctx, self.job(thirteen), busy, [], self.clock[0], explain=True), (None, 'host load cap'))
        self.assertEqual([j['id'] for j in fq._reserve(self.ctx, {'ember': big})], [thirteen])   # one placement per host a pass
        self.assertEqual(self.job(now_job)['status'], 'failed')
        self.assertEqual(self.job(later)['status'], 'queued')

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
        self.assertEqual((job['preparation_failures'], job['reason']), (1, 'pinned preparation failed: RuntimeError: launcher'))   # the message, not only the type
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
