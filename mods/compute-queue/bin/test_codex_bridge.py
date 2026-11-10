"""Offline bridge checks: isolated state, fake engine/CLI, no host or live notices.

From the mod directory: python -m unittest discover -s bin -p test_codex_bridge.py
"""
import io
import atexit
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import shutil
import threading
import types
import unittest
import weakref
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
# Discovery imports this module before test_cq; neither may seed cq's globals from live host configuration.
_STATE_DIR = tempfile.mkdtemp(prefix='cq-bridge-tests-')
atexit.register(shutil.rmtree, _STATE_DIR, True)
os.environ['CQ_STATE'] = _STATE_DIR
for _key in ('CQ_HOST', 'LANE_HOST', 'CQ_LOAD_CAP', 'CQ_HOST_RAM_FLOOR_GB'):
    os.environ.pop(_key, None)
import codex_bridge as bridge
import cq

SESSION = '01a14e7f-5edf-7e83-b3e7-7a6dd17ad2e7'
OTHER = '01a14e7f-5edf-7e83-b3e7-7a6dd17ad2e8'


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name).resolve()
        self.root = self.base / 'repo'
        self.root.mkdir()
        self.home = self.base / 'codex'
        self.env = {'CODEX_HOME': str(self.home), 'CODEX_THREAD_ID': SESSION, 'BATON_CODEX_EXE': 'C:/tools/codex.exe'}
        self.owners = bridge.Owners(self.env, git=lambda root, *args: str(self.root), alive=lambda session: True)
        self.ctx = cq.Ctx(state=str(self.base / 'state'), now=lambda: 1000)
        self.owner()
        self.calls = []
        self.deliveries = []

        def engine(name, req):
            self.calls.append((name, req))
            if name == 'tick':
                result = cq.cmd_leader(self.ctx, req)
                return dict(result, can_start=True)
            if name == 'notified':
                return cq.cmd_notified(self.ctx, req)
            if name == 'leader':
                return cq.cmd_leader(self.ctx, req)
            return {'ok': True}

        def runner(argv, **kw):
            self.deliveries.append(argv)
            return subprocess.CompletedProcess(argv, 0, f'Queued message {SESSION} for thread {argv[3]}.\n')

        self.b = bridge.Bridge(self.ctx, self.owners, check_policy=lambda command: None,
                               engine=engine, runner=runner)

    def owner(self, session=SESSION, vendor='codex', home=None, **extra):
        home = home or self.home
        cq.write_json(str(self.root / '.baton' / 'owner.json'),
                      dict(vendor=vendor, account='main', session_id=session,
                           transcript=str(home / 'sessions' / ('rollout-' + session + '.jsonl')), **extra))

    def job(self, job_id='j1', **extra):
        value = dict(id=job_id, root=str(self.root), top=str(self.root), session=OTHER,
                     status='done', notified=False, notify=True,
                     worktree_identity=cq.worktree_identity(str(self.root)))
        value.update(extra)
        cq.save_job(self.ctx, value)
        return value

    def test_run_binds_root_session_and_original_fields(self):
        args = dict(root=str(self.root), command='lake build', slots=2, source='head',
                    est_minutes=5, gets=['result.txt'], host_id='hearth', where='host', split=None)
        del args['split']
        self.b.call('run', args)
        name, req = self.calls[-1]
        self.assertEqual(name, 'submit')
        self.assertEqual(req, dict(args, session=SESSION))

    def test_allowlist_refusal_before_submit_or_snapshot(self):
        self.b.check_policy = mock.Mock(side_effect=bridge.Refusal('not allowed'))
        with mock.patch.object(cq, 'snapshot', side_effect=AssertionError('snapshot')):
            with self.assertRaisesRegex(bridge.Refusal, 'not allowed'):
                self.b.call('run', {'root': str(self.root), 'command': 'curl bad'})
        self.assertEqual(self.calls, [])

    def test_root_unknown_other_owner_home_and_session_refuse(self):
        for mode in ('unknown', 'claude', 'home', 'thread', 'missing_thread'):
            with self.subTest(mode=mode):
                self.owner()
                self.owners.session = SESSION
                if mode == 'unknown':
                    (self.root / '.baton' / 'owner.json').unlink()
                if mode == 'claude':
                    self.owner(vendor='claude')
                if mode == 'home':
                    self.owner(home=self.base / 'other-account')
                if mode == 'thread':
                    self.owner(session=OTHER)
                if mode == 'missing_thread':
                    self.owners.session = None
                with self.assertRaises(bridge.Refusal):
                    self.b.call('run', {'root': str(self.root), 'command': 'lake build'})
        self.assertFalse(self.calls)

    def test_explicit_session_for_appserver_and_env_cannot_be_overridden(self):
        self.owners.session = None
        self.b.call('run', {'root': str(self.root), 'command': 'lake build', 'session_id': SESSION})
        self.assertEqual(self.calls[-1][1]['session'], SESSION)
        self.assertNotIn('session_id', self.calls[-1][1])
        self.owners.session = OTHER
        with self.assertRaises(bridge.Refusal):
            self.b.call('run', {'root': str(self.root), 'command': 'lake build', 'session_id': SESSION})

    @unittest.skipUnless(os.name == 'nt', 'Windows extended-path spelling')
    def test_extended_transcript_path_matches_same_account(self):
        path = self.root / '.baton' / 'owner.json'
        owner = cq.read_json(str(path))
        owner['transcript'] = '\\\\?\\' + owner['transcript']
        cq.write_json(str(path), owner)
        self.b.call('run', {'root': str(self.root), 'command': 'lake build'})
        self.assertEqual(self.calls[-1][1]['session'], SESSION)

    @unittest.skipUnless(os.name == 'nt', 'Windows extended-path spelling')
    def test_extended_transcript_path_cannot_cross_account(self):
        self.owner(home=self.base / 'other-account')
        path = self.root / '.baton' / 'owner.json'
        owner = cq.read_json(str(path))
        owner['transcript'] = '\\\\?\\' + owner['transcript']
        cq.write_json(str(path), owner)
        with self.assertRaisesRegex(bridge.Refusal, 'another Codex home/account'):
            self.b.call('run', {'root': str(self.root), 'command': 'lake build'})
        self.assertFalse(self.calls)

    def test_root_must_be_toplevel_existing_absolute_git(self):
        subdir = self.root / 'sub'
        subdir.mkdir()
        with self.assertRaisesRegex(bridge.Refusal, 'subdirectory'):
            self.b.call('run', {'root': str(subdir), 'command': 'lake build'})
        with self.assertRaises(bridge.Refusal):
            self.b.call('run', {'root': 'relative', 'command': 'lake build'})
        self.owners.git = mock.Mock(side_effect=cq.CqError('not git'))
        with self.assertRaisesRegex(bridge.Refusal, 'not a git'):
            self.b.call('run', {'root': str(self.root), 'command': 'lake build'})

    def test_schema_validation_and_job_path_refusal(self):
        invalid = [dict(root=str(self.root), command='lake build', slots=True),
                   dict(root=str(self.root), command='lake build', model='spoof'),
                   dict(root=str(self.root), command='lake build', session='spoof'),
                   dict(root=str(self.root), command='lake build', split={'max_parts': 1}),
                   dict(root=str(self.root), command='lake build', gets='not-list')]
        for args in invalid:
            with self.assertRaises(bridge.Refusal):
                self.b.call('run', args)
        for job_id in ('../leader', 'a/b', '..', 'a\\b'):
            with self.assertRaises(bridge.Refusal):
                self.b.call('status', {'job': job_id})
        self.assertEqual(self.calls, [])

    def test_cancel_only_owned_worktree_and_shared_engine(self):
        self.job(status='queued')
        self.b.call('cancel', {'root': str(self.root), 'job': 'j1'})
        self.assertEqual(self.calls[-1], ('cancel', {'job': 'j1', 'root': str(self.root), 'session': SESSION}))
        self.job(root=str(self.base / 'somewhere-else'))
        with self.assertRaises(bridge.Refusal):
            self.b.call('cancel', {'root': str(self.root), 'job': 'j1'})

    def test_shard_jobs_are_read_for_cancel_and_delivery(self):
        shard = dict(id='cq-hearth-123', root=str(self.root), top=str(self.root), session=OTHER,
                     status='done', notified=False, notify=True,
                     worktree_identity=cq.worktree_identity(str(self.root)))
        registry = types.SimpleNamespace(all_jobs=mock.Mock(return_value=[shard]),
                                         load_job=mock.Mock(return_value=shard))
        with mock.patch('queue_generations.all_jobs', registry.all_jobs), mock.patch('queue_generations.load_job', registry.load_job):
            self.b.call('cancel', {'root': str(self.root), 'job': shard['id']})
            self.b.cycle()
        self.assertEqual(registry.all_jobs.call_count, 1)
        self.assertEqual(registry.load_job.call_count, 2)
        self.assertEqual(self.calls[0], ('cancel', {'job': shard['id'], 'root': str(self.root), 'session': SESSION}))
        self.assertEqual(self.calls[-1][0], 'notified')
        self.assertEqual(len(self.deliveries), 1)

    def test_wait_cap_and_no_dispatch_exposed(self):
        self.b.call('wait', {'job': 'j1'})
        self.assertEqual(self.calls[-1], ('wait', {'job': 'j1', 'timeout_s': 60}))
        with self.assertRaises(bridge.Refusal):
            self.b.call('wait', {'job': 'j1', 'timeout_s': 61})
        with self.assertRaises(bridge.Refusal):
            self.b.call('dispatch', {'job': 'j1'})
        self.assertEqual({t['name'] for t in bridge.TOOLS}, {'run', 'status', 'wait', 'cancel'})

    def test_idle_cycle_never_probes_or_claims_leader(self):
        self.b.cycle()
        self.assertEqual(self.calls, [])

    def test_idle_cycle_scans_once_and_delivers_snapshot(self):
        self.job()
        with mock.patch.object(self.b, 'all_jobs', wraps=self.b.all_jobs) as scan:
            self.b.cycle()
        self.assertEqual(scan.call_count, 1)
        self.assertEqual(len(self.deliveries), 1)
        self.assertTrue(cq.load_job(self.ctx, 'j1')['notified'])

    def test_active_cycle_rescans_after_tick_completion(self):
        self.job(status='running')
        def engine(name, req):
            if name == 'tick':
                self.job(status='done')
                return {'leader': False}
            return cq.cmd_notified(self.ctx, req)
        self.b.engine = engine
        with mock.patch.object(self.b, 'all_jobs', wraps=self.b.all_jobs) as scan:
            self.b.cycle()
        self.assertEqual(scan.call_count, 2)
        self.assertEqual(len(self.deliveries), 1)
        self.assertTrue(cq.load_job(self.ctx, 'j1')['notified'])

    def test_direct_deliver_scans_current_jobs(self):
        self.b.deliver()
        self.job()
        with mock.patch.object(self.b, 'all_jobs', wraps=self.b.all_jobs) as scan:
            self.b.deliver()
        self.assertEqual(scan.call_count, 1)
        self.assertEqual(len(self.deliveries), 1)

    def test_completion_added_after_idle_scan_delivers_next_cycle(self):
        real_scan = self.b.all_jobs
        def scan_then_add():
            snapshot = real_scan()
            self.job()
            return snapshot
        with mock.patch.object(self.b, 'all_jobs', side_effect=scan_then_add):
            self.b.cycle()
        self.assertFalse(self.deliveries)
        self.b.cycle()
        self.assertEqual(len(self.deliveries), 1)
        self.assertTrue(cq.load_job(self.ctx, 'j1')['notified'])

    def test_idle_snapshot_reloads_changed_candidates_under_lock(self):
        self.job('stale')
        self.job('cancelled')
        real_scan = self.b.all_jobs
        def scan_then_change():
            snapshot = real_scan()
            self.job('stale', notify=False)
            self.job('cancelled', status='cancelled')
            return snapshot
        with mock.patch.object(self.b, 'all_jobs', side_effect=scan_then_change):
            self.b.cycle()
        self.assertEqual(len(self.deliveries), 1)
        self.assertIn('cancelled', self.deliveries[0][-1])
        self.assertFalse(cq.load_job(self.ctx, 'stale')['notified'])
        self.assertTrue(cq.load_job(self.ctx, 'cancelled')['notified'])

    def test_fresh_claude_leader_keeps_control(self):
        self.job(status='queued')
        cq.cmd_leader(self.ctx, {'session': 'claude-lead', 'root': 'claude-root'})
        self.b.cycle()
        self.assertEqual([name for name, req in self.calls], ['tick'])
        self.assertEqual(cq.read_json(self.ctx.p('leader.json'))['session'], 'claude-lead')

    def test_codex_leader_uses_fallback_only_and_releases(self):
        self.job(status='queued')
        self.b.cycle()
        self.assertEqual([name for name, req in self.calls], ['tick', 'apply'])
        self.assertEqual(self.calls[-1][1], {'planner': 'code'})
        self.b.close()
        self.assertEqual(cq.read_json(self.ctx.p('leader.json'))['heartbeat'], 0)

    def test_lost_or_expired_lease_no_apply(self):
        self.job(status='queued')
        for lease in ({'session': 'someone-else', 'heartbeat': 1000},
                      {'session': self.b.leader_session, 'heartbeat': 900}):
            with self.subTest(lease=lease):
                self.calls.clear()
                cq.write_json(self.ctx.p('leader.json'), lease)
                self.b.engine = lambda name, req: self.calls.append((name, req)) or {'leader': True, 'can_start': True}
                self.b.cycle()
                self.assertEqual([name for name, req in self.calls], ['tick'])

    def test_delivery_retry_adoption_current_owner_no_duplicate(self):
        self.job(session='old-claude-session')
        self.b.runner = mock.Mock(return_value=subprocess.CompletedProcess([], 1))
        self.b.deliver()
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])
        self.owner(session=OTHER)
        self.b.runner.return_value = subprocess.CompletedProcess([], 0, f'Queued message {SESSION} for thread {OTHER}.\n')
        self.b.deliver()
        self.assertTrue(cq.load_job(self.ctx, 'j1')['notified'])
        argv = self.b.runner.call_args.args[0]
        self.assertEqual(argv[:4], ['C:/tools/codex.exe', 'queue', '--thread', OTHER])
        self.b.deliver()
        self.assertEqual(self.b.runner.call_count, 2)

    def test_accepted_notice_mark_retries_without_resending_across_bridge_instances(self):
        self.job()
        real_engine = self.b.engine
        self.b.engine = lambda name, req: {'ok': False, 'error': 'busy'} if name == 'notified' else real_engine(name, req)
        self.b.deliver()
        self.assertEqual(len(self.deliveries), 1)
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])
        other = bridge.Bridge(self.ctx, self.owners, engine=real_engine, runner=self.b.runner)
        other.deliver()
        self.assertTrue(cq.load_job(self.ctx, 'j1')['notified'])
        self.assertEqual(len(self.deliveries), 1)

    def test_crash_during_enqueue_holds_uncertain_attempt_across_restart(self):
        self.job()
        self.b.runner = mock.Mock(side_effect=SystemExit('simulated crash'))
        with self.assertRaises(SystemExit):
            self.b.deliver()
        peer = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine, runner=mock.Mock())
        peer.deliver()
        peer.runner.assert_not_called()
        self.assertEqual(peer.call('status', {'job': 'j1'})['notice_delivery']['state'], 'uncertain')
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])
        self.owner(session=OTHER)
        peer.runner = lambda argv, **kw: subprocess.CompletedProcess(argv, 0, f'Queued message {SESSION} for thread {OTHER}.\n')
        peer.deliver()
        self.assertTrue(cq.load_job(self.ctx, 'j1')['notified'])

    def test_old_owner_memory_does_not_hide_new_owner_uncertain_attempt(self):
        self.job()
        self.b.engine = lambda name, req: {'ok': False}
        self.b.deliver()
        self.owner(session=OTHER)
        peer = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine, runner=mock.Mock(side_effect=SystemExit()))
        with self.assertRaises(SystemExit):
            peer.deliver()
        self.b.deliver()
        self.assertEqual(len(self.deliveries), 1)
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])

    def test_uncertain_attempt_survives_a_to_b_to_a_after_restarts(self):
        self.job()
        self.b.runner = mock.Mock(side_effect=SystemExit())
        with self.assertRaises(SystemExit):
            self.b.deliver()
        self.owner(session=OTHER)
        second = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine, runner=mock.Mock(side_effect=SystemExit()))
        with self.assertRaises(SystemExit):
            second.deliver()
        self.assertEqual(second.runner.call_count, 1)
        self.owner(session=SESSION)
        restarted = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine, runner=mock.Mock())
        restarted.deliver()
        restarted.runner.assert_not_called()
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])
        self.assertEqual(len(bridge.read_notice_receipts(self.ctx.p('notify', 'j1.accepted.json'))), 2)

    def test_legacy_receipt_is_preserved_for_prior_owner_after_handoff(self):
        self.job()
        path = self.ctx.p('notify', 'j1.accepted.json')
        old = dict(root=bridge.canonical(self.root), session=SESSION, home=bridge.canonical(self.home),
                   identity=cq.worktree_identity(str(self.root)), state='uncertain')
        cq.write_json(path, old)
        self.owner(session=OTHER)
        peer = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine, runner=mock.Mock(side_effect=SystemExit()))
        with self.assertRaises(SystemExit):
            peer.deliver()
        self.assertEqual(bridge.read_notice_receipts(path)[bridge.notice_receipt_key(old)], old)
        self.owner(session=SESSION)
        again = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine, runner=mock.Mock())
        again.deliver()
        again.runner.assert_not_called()

    def test_legacy_accepted_receipt_retries_mark_without_requeue(self):
        self.job()
        path = self.ctx.p('notify', 'j1.accepted.json')
        cq.write_json(path, dict(root=bridge.canonical(self.root), session=SESSION,
                                home=bridge.canonical(self.home), identity=cq.worktree_identity(str(self.root))))
        self.b.deliver()
        self.assertFalse(self.deliveries)
        self.assertTrue(cq.load_job(self.ctx, 'j1')['notified'])

    def test_later_owner_mark_does_not_delete_prior_uncertain_receipt(self):
        self.job()
        path = self.ctx.p('notify', 'j1.accepted.json')
        old = dict(root=bridge.canonical(self.root), session=SESSION, home=bridge.canonical(self.home),
                   identity=cq.worktree_identity(str(self.root)), state='uncertain')
        cq.write_json(path, old)
        self.owner(session=OTHER)
        peer = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine,
                             runner=lambda argv, **kw: subprocess.CompletedProcess(argv, 0, f'Queued message {SESSION} for thread {OTHER}.\n'))
        peer.deliver()
        self.assertTrue(cq.load_job(self.ctx, 'j1')['notified'])
        self.assertEqual(bridge.read_notice_receipts(path), {bridge.notice_receipt_key(old): old})

    def test_failed_attempt_write_never_calls_enqueue(self):
        self.job()
        with mock.patch.object(cq, 'atomic_write', side_effect=OSError('disk full')):
            self.b.deliver()
            self.b.deliver()
        self.assertFalse(self.deliveries)
        self.assertEqual(self.b.call('status', {'job': 'j1'})['notice_delivery']['state'], 'uncertain')

    def test_unreadable_receipt_is_held_instead_of_replayed(self):
        self.job()
        path = Path(self.ctx.p('notify', 'j1.accepted.json'))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{broken', encoding='utf-8')
        self.b.deliver()
        self.assertFalse(self.deliveries)
        self.assertEqual(self.b.call('status', {'job': 'j1'})['notice_delivery']['state'], 'uncertain')

    def test_failed_acceptance_write_remains_uncertain_after_restart(self):
        self.job()
        original = cq.atomic_write
        def fail_accepted(path, data, durable=False):
            if (str(path).endswith('.accepted.json') and
                    any(v.get('state') == 'accepted' for v in json.loads(data).get('receipts', {}).values())):
                raise OSError('disk full')
            return original(path, data, durable=durable)
        self.b.engine = lambda name, req: {'ok': False}
        with mock.patch.object(cq, 'atomic_write', side_effect=fail_accepted):
            self.b.deliver()
        self.assertEqual(len(self.deliveries), 1)
        peer = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine, runner=mock.Mock())
        peer.deliver()
        peer.runner.assert_not_called()
        self.assertEqual(peer.call('status', {'job': 'j1'})['notice_delivery']['state'], 'uncertain')

    def test_busy_notice_does_not_block_later_completions(self):
        self.job('busy')
        self.job('ready')
        original = cq.FileLock
        class Busy:
            def __enter__(self):
                raise cq.LockTimeout('other delivery in progress')
            def __exit__(self, *args):
                pass
        def lock(path, timeout=30):
            return Busy() if str(path).endswith('busy.lock') else original(path, timeout)
        with mock.patch.object(cq, 'FileLock', side_effect=lock):
            self.b.deliver()
        self.assertFalse(cq.load_job(self.ctx, 'busy')['notified'])
        self.assertTrue(cq.load_job(self.ctx, 'ready')['notified'])

    def test_successful_exit_without_exact_queue_acceptance_remains_retryable(self):
        self.job()
        for stdout in ('done', f'Queued message {SESSION} for thread {OTHER}.\n'):
            self.b.runner = mock.Mock(return_value=subprocess.CompletedProcess([], 0, stdout))
            self.b.deliver()
            self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])

    def test_completed_or_crashed_thread_is_not_resumed_by_a_completion(self):
        self.job()
        self.owners.alive = lambda session: False
        self.b.runner = mock.Mock()
        self.b.deliver()
        self.b.runner.assert_not_called()
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])

    def test_owner_change_during_delivery_does_not_mark_completion_for_new_owner(self):
        self.job()
        def runner(argv, **kw):
            self.owner(session=OTHER)
            return subprocess.CompletedProcess(argv, 0, f'Queued message {SESSION} for thread {SESSION}.\n')
        self.b.runner = runner
        self.b.deliver()
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])
        self.b.runner = lambda argv, **kw: subprocess.CompletedProcess(argv, 0, f'Queued message {SESSION} for thread {OTHER}.\n')
        self.b.deliver()
        self.assertTrue(cq.load_job(self.ctx, 'j1')['notified'])

    def test_reused_worktree_path_does_not_receive_a_prior_completion(self):
        identity = cq.worktree_identity(str(self.root))
        identity['inode'] += 1
        self.job(worktree_identity=identity)
        self.b.deliver()
        self.assertFalse(self.deliveries)
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])

    def test_legacy_completion_is_held_for_a_different_session(self):
        self.job(session=OTHER, worktree_identity=None)
        self.b.deliver()
        self.assertFalse(self.deliveries)
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])

    def test_completion_uses_recipient_home_without_parent_session_or_auth_override(self):
        self.job()
        self.owners.env.update(CLAUDECODE='1', OPENAI_API_KEY='fake', CLAUDE_CONFIG_DIR='sender')
        self.b.runner = mock.Mock(return_value=subprocess.CompletedProcess([], 0, f'Queued message {OTHER} for thread {SESSION}.\n'))
        self.b.deliver()
        env = self.b.runner.call_args.kwargs['env']
        self.assertEqual(env['CODEX_HOME'], str(self.home))
        for key in ('CODEX_THREAD_ID', 'CLAUDECODE', 'OPENAI_API_KEY', 'CLAUDE_CONFIG_DIR'):
            self.assertNotIn(key, env)

    def test_delivery_leaves_claude_owner_other_account_and_children_alone(self):
        self.job()
        for owner in ('claude', 'other-home'):
            self.owner(vendor='claude' if owner == 'claude' else 'codex',
                       home=self.base / 'different' if owner == 'other-home' else None)
            self.b.deliver()
        self.owner()
        self.job(parent='parent')
        self.b.deliver()
        self.assertFalse(self.deliveries)

    def test_ineligible_notice_owners_do_not_spawn_git(self):
        self.job()
        self.owners.git = mock.Mock(return_value=str(self.root))
        for mode in ('missing', 'claude', 'other-account', 'inactive'):
            with self.subTest(mode=mode):
                self.owner(vendor='claude' if mode == 'claude' else 'codex',
                           home=self.base / 'different' if mode == 'other-account' else None)
                self.owners.alive = lambda session: mode != 'inactive'
                if mode == 'missing':
                    (self.root / '.baton' / 'owner.json').unlink()
                self.b.deliver()
                self.owners.git.assert_not_called()
        self.assertFalse(self.deliveries)

    def test_eligible_notice_still_validates_git_root(self):
        self.job()
        self.owners.git = mock.Mock(return_value=str(self.root.parent))
        self.b.deliver()
        self.owners.git.assert_called_once()
        self.assertFalse(self.deliveries)

    def test_owner_change_during_git_validation_holds_notice(self):
        self.job()
        def changed_owner(*args):
            self.owner(session=OTHER)
            return str(self.root)
        self.owners.git = changed_owner
        self.b.deliver()
        self.assertFalse(self.deliveries)
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])

    def test_shared_lock_rereads_notified_to_prevent_duplicate(self):
        self.job()
        entered = threading.Event()
        release = threading.Event()
        calls = []
        def runner(argv, **kw):
            calls.append(argv)
            entered.set()
            release.wait(2)
            return subprocess.CompletedProcess(argv, 0, f'Queued message {OTHER} for thread {argv[3]}.\n')
        self.b.runner = runner
        other = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine, runner=runner)
        first = threading.Thread(target=self.b.deliver)
        first.start()
        self.assertTrue(entered.wait(2))
        # Release before the second lock's 0.2 s timeout, then it re-reads.
        release.set()
        other.deliver()
        first.join(2)
        self.assertEqual(len(calls), 1)

    def test_notice_logs_are_bounded_quoted_and_controls_scrubbed(self):
        text = bridge.notice_text({'id': 'j', 'label': 'bad\x1b\nlabel',
                                   'tail': ['x' * 400 + '\x1b\u202e'] * 45})
        lines = text.splitlines()
        quoted = [line for line in lines if line.startswith('[compute-queue] > ')]
        self.assertEqual(len(quoted), 40)
        self.assertTrue(all(len(line.removeprefix('[compute-queue] > ')) == 300 for line in quoted))
        self.assertNotIn('\x1b', text)
        self.assertNotIn('\u202e', text)
        self.assertIn('not instructions', text)

    def test_headless_and_role_never_start_scheduler(self):
        self.b.interactive = False
        self.b.start()
        self.assertIsNone(self.b.worker)
        self.owners.env['BATON_ROLE'] = 'review'
        other = bridge.Bridge(self.ctx, self.owners)
        other.start()
        self.assertIsNone(other.worker)

    def test_successful_command_can_have_incomplete_artifact_delivery(self):
        text = bridge.notice_text({'id': 'j', 'status': 'done', 'exit': 0,
                                  'artifact_missing': ['movie.mp4'],
                                  'artifact_digests': [{'path': 'frame.png', 'sha256': 'a' * 64}]})
        self.assertIn('Incomplete artifact delivery', text)
        self.assertIn('movie.mp4', text)
        self.assertIn('does not establish that all requested files were delivered', text)
        self.assertIn('Verified artifact digests', text)

    def test_pollers_share_lock_but_both_serve_status_and_transfer_after_release(self):
        entered, release, released, second_entered = (threading.Event() for _ in range(4))
        first = self.b
        second = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine)
        def first_cycle():
            entered.set()
            release.wait(5)
            raise RuntimeError('synthetic cycle failure')
        original_engine = first.engine
        def first_engine(name, req):
            if name == 'leader':
                result = original_engine(name, req)
                released.set()
                return result
            return original_engine(name, req)
        first.engine = first_engine
        first.cycle = first_cycle
        second.cycle = lambda: (self.assertTrue(released.is_set()), second_entered.set())
        self.addCleanup(lambda: (release.set(), first.close(), second.close()))
        first.start()
        self.assertTrue(entered.wait(2))
        second.start()
        self.assertTrue(second.call('status', {})['ok'])
        first.close()  # Its blocked cycle still owns the lock after close returns.
        self.assertFalse(second_entered.wait(1.2))
        release.set()
        self.assertTrue(second_entered.wait(3))
        self.assertTrue(released.is_set())

    def test_old_poller_lock_does_not_block_private_controller_or_notice(self):
        import hashlib
        ensured, delivered = threading.Event(), threading.Event()
        self.job('private-job', defer='spare')
        original_engine, original_runner = self.b.engine, self.b.runner
        def engine(name, req):
            if name == 'ensure_deferred':
                ensured.set()
            result = original_engine(name, req)
            if name == 'notified':
                delivered.set()
            return result
        def runner(*args, **kwargs):
            return original_runner(*args, **kwargs)
        self.b.engine, self.b.runner = engine, runner
        lock_path = self.ctx.p('codex-poller-' + hashlib.sha256(
            bridge.canonical(self.home).encode('utf-8')).hexdigest() + '.lock')
        self.addCleanup(self.b.close)
        with cq.FileLock(lock_path, 0):
            self.b.start()
            self.assertTrue(ensured.wait(2))
            self.assertTrue(delivered.wait(2))
        self.assertTrue(cq.load_job(self.ctx, 'private-job')['notified'])

    def test_process_death_releases_poller_lock(self):
        import hashlib
        lock_path = self.ctx.p('codex-poller-' + hashlib.sha256(
            bridge.canonical(self.home).encode('utf-8')).hexdigest() + '.lock')
        script = ("import sys, time; sys.path.insert(0, sys.argv[1]); import cq; "
                  "lock = cq.FileLock(sys.argv[2], 0); lock.__enter__(); "
                  "print('locked', flush=True); time.sleep(30)")
        child = subprocess.Popen([sys.executable, '-u', '-c', script, str(Path(cq.__file__).parent), lock_path],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                 creationflags=cq.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        def cleanup():
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=5)
        self.addCleanup(cleanup)
        # Bounded readiness even if child setup fails.
        ready = threading.Event()
        lines = []
        def read_ready():
            lines.append(child.stdout.readline())
            ready.set()
        threading.Thread(target=read_ready, daemon=True).start()
        self.assertTrue(ready.wait(5))
        self.assertEqual(lines, ['locked\n'])
        entered = threading.Event()
        self.b.cycle = entered.set
        self.addCleanup(self.b.close)
        self.b.start()
        self.assertFalse(entered.wait(1.2))
        child.kill()
        child.wait(timeout=5)
        self.assertTrue(entered.wait(3))

    def test_failed_delivery_is_not_replayed_by_a_capable_peer(self):
        self.job()
        self.b.runner = mock.Mock(side_effect=OSError('uncertain enqueue failure'))
        self.b.deliver()
        peer = bridge.Bridge(self.ctx, self.owners, engine=self.b.engine, runner=mock.Mock())
        peer.deliver()
        peer.runner.assert_not_called()
        self.assertFalse(cq.load_job(self.ctx, 'j1')['notified'])

    def test_transient_poll_lock_error_retries_without_log_flood(self):
        entered = threading.Event()
        original = cq.FileLock
        attempts = []
        class Unavailable:
            def __enter__(self):
                raise OSError('temporary poll lock access failure')
        def lock(path, timeout=30):
            if Path(path).name.startswith('codex-poller-'):
                attempts.append(path)
                if len(attempts) <= 2:
                    return Unavailable()
            return original(path, timeout)
        self.b.cycle = entered.set
        errors = io.StringIO()
        with mock.patch.object(cq, 'FileLock', side_effect=lock), mock.patch.object(sys, 'stderr', errors):
            try:
                self.b.start()
                self.assertTrue(entered.wait(4))
                self.assertTrue(self.b.worker.is_alive())
            finally:
                self.b.close()
        self.assertEqual(len(attempts), 3)
        self.assertEqual(errors.getvalue().count('temporary poll lock access failure'), 1)

    def test_distinct_home_and_state_poll_independently(self):
        others = [bridge.Bridge(self.ctx, bridge.Owners({'CODEX_HOME': str(self.base / 'other')}), engine=self.b.engine),
                  bridge.Bridge(cq.Ctx(state=str(self.base / 'other-state')), self.owners, engine=self.b.engine)]
        all_bridges = [self.b, *others]
        events = [threading.Event() for _ in all_bridges]
        for item, event in zip(all_bridges, events):
            item.cycle = event.set
            self.addCleanup(item.close)
            item.start()
        self.assertTrue(all(event.wait(2) for event in events))

    def test_headless_does_not_attempt_poll_lock(self):
        self.b.interactive = False
        with mock.patch.object(cq, 'FileLock', side_effect=AssertionError('lock attempted')):
            self.b.start()
        self.assertIsNone(self.b.worker)

    def test_active_snapshot_released_before_tick(self):
        class Snapshot(list):
            pass
        snapshot = Snapshot([{'status': 'running'}])
        ref = weakref.ref(snapshot)
        snapshots = [snapshot, []]
        self.b.all_jobs = lambda: snapshots.pop(0)
        del snapshot
        def tick(name, req):
            self.assertIsNone(ref())
            return {'leader': False}
        self.b.engine = tick
        self.b.cycle()

    def test_real_policy_reuses_ts_allow_and_classify(self):
        executable = os.environ.get('CQ_NODE', 'node')
        version = subprocess.run([executable, '--version'], capture_output=True, text=True, timeout=10,
                                 creationflags=cq.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        match = re.match(r'v(\d+)\.(\d+)', version.stdout)
        if not match or tuple(map(int, match.groups())) < (24, 14):
            self.skipTest('controller policy requires Node >=24.14; remote job hosts do not run this policy')
        self.assertTrue(bridge.policy('lake build')['allowed']['ok'])
        self.assertFalse(bridge.policy('go test ./...')['classification']['deny'])
        classified = subprocess.run([executable, str(Path(bridge.__file__).with_name('policy.mjs'))],
                                    input=json.dumps({'command': 'python .baton/remote/remoterun.py x'}),
                                    capture_output=True, text=True, timeout=15,
                                    creationflags=cq.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        self.assertTrue(json.loads(classified.stdout)['classification']['deny'])
        for command in ('curl https://example.com', 'python -c "print(1)"', 'go test ./...; rm x'):
            with self.assertRaisesRegex(bridge.Refusal, 'allowlist'):
                bridge.policy(command)

    def test_policy_failure_fails_closed(self):
        with self.assertRaises(bridge.Refusal):
            bridge.policy('lake build', runner=mock.Mock(side_effect=FileNotFoundError()))
        with self.assertRaises(bridge.Refusal):
            bridge.policy('lake build', runner=lambda *a, **k: subprocess.CompletedProcess([], 0, stdout='not-json'))

    def test_protocol_initialize_list_errors_and_tool_result(self):
        out = io.StringIO()
        p = bridge.Protocol(self.b, out)
        def request(method, params=None, ident=1):
            p.handle({'jsonrpc': '2.0', 'id': ident, 'method': method, 'params': params or {}})
            return json.loads(out.getvalue().splitlines()[-1])
        self.assertEqual(request('tools/list')['error']['code'], -32000)
        init = request('initialize', {'protocolVersion': '2025-03-26'})['result']
        self.assertEqual(init['protocolVersion'], '2025-03-26')
        self.assertEqual(init['instructions'], bridge.INSTRUCTIONS)
        self.assertLessEqual(len(init['instructions']), 512)
        self.assertIn('deferred_registered', init['instructions'])
        self.assertIn('tools', init['capabilities'])
        self.assertTrue(init['capabilities']['tools']['listChanged'])
        tools = request('tools/list')['result']['tools']
        self.assertIn('root', tools[0]['inputSchema']['required'])
        self.assertEqual(request('tools/call', {'name': 'dispatch'})['error']['code'], -32602)
        self.assertEqual(request('invalid')['error']['code'], -32601)
        self.assertTrue(request('tools/call', {'name': 'run', 'arguments': {}})['result']['isError'])
        self.assertFalse(request('tools/call', {'name': 'status'})['result']['isError'])
        p.handle([])
        self.assertEqual(json.loads(out.getvalue().splitlines()[-1])['error']['code'], -32600)
        p.handle({'jsonrpc': '2.0', 'id': 4, 'method': 'ping', 'params': []})
        self.assertEqual(json.loads(out.getvalue().splitlines()[-1])['error']['code'], -32602)
        before = out.getvalue()
        p.handle({'jsonrpc': '2.0', 'method': 'notifications/initialized'})
        self.b.close()
        after = out.getvalue()
        self.assertTrue(after.startswith(before))
        self.assertEqual(json.loads(after[len(before):]),
                         {'jsonrpc': '2.0', 'method': 'notifications/tools/list_changed'})
        # Repeated ready notifications must not flood clients with refresh requests.
        p.handle({'jsonrpc': '2.0', 'method': 'notifications/initialized'})
        self.assertEqual(out.getvalue(), after)

    def test_stdio_server_starts_normal_no_tests_or_stdout_noise(self):
        requests = ['bad-json', json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize',
                                          'params': {'protocolVersion': '2024-11-05'}}),
                    json.dumps({'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'})]
        result = subprocess.run([sys.executable, str(Path(bridge.__file__)), '--headless', '--state', self.ctx.state],
                                input='\n'.join(requests) + '\n', capture_output=True, text=True, timeout=15,
                                creationflags=cq.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        self.assertEqual(result.returncode, 0, result.stderr)
        replies = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(len(replies), 3)
        self.assertEqual(replies[0]['error']['code'], -32700)
        self.assertEqual(replies[1]['result']['serverInfo']['name'], 'compute-queue-codex')
        self.assertEqual(len(replies[2]['result']['tools']), 4)

    def test_stdio_wait_limit_returns_busy_and_control_still_runs(self):
        release = threading.Event()
        waits = []
        def engine(name, args):
            if name == 'wait':
                waits.append(args['job'])
                release.wait(3)
            elif name == 'status':
                release.set()
            return {'ok': True}
        self.b.engine = engine
        requests = [{'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {}}]
        for ident in (2, 3, 4):
            requests.append({'jsonrpc': '2.0', 'id': ident, 'method': 'tools/call',
                             'params': {'name': 'wait', 'arguments': {'job': str(ident)}}})
        requests.append({'jsonrpc': '2.0', 'id': 5, 'method': 'tools/call', 'params': {'name': 'status'}})
        source = io.StringIO('\n'.join(map(json.dumps, requests)) + '\n')
        output = io.StringIO()
        with mock.patch.object(bridge, 'Bridge', return_value=self.b), \
                mock.patch.object(sys, 'stdin', source), mock.patch.object(sys, 'stdout', output):
            self.assertEqual(bridge.main(['--headless', '--state', self.ctx.state]), 0)
        replies = {r['id']: r for r in map(json.loads, output.getvalue().splitlines())}
        self.assertEqual(set(replies), {1, 2, 3, 4, 5})
        self.assertTrue(replies[4]['result']['isError'])
        self.assertTrue(json.loads(replies[4]['result']['content'][0]['text'])['retryable'])
        self.assertCountEqual(waits, ['2', '3'])
        self.assertFalse(replies[5]['result']['isError'])


if __name__ == '__main__':
    unittest.main()
