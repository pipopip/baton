"""Deferred queue admission, staging, reconciliation and recovery contracts."""
import base64
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
import types
from unittest import mock

from test_cq import Base, cap
import cq
import artifact_transfer


class DeferredTests(Base):
    def deferred(self, **kw):
        return self.submit(where='host', host_id='ember', defer='overnight', memory_max_gb=2, **kw)

    def test_admission_receipt_and_launcher(self):
        for args in ({'defer': 'overnight', 'memory_max_gb': 2},
                     {'where': 'host', 'host_id': 'ember', 'defer': 'spare'},
                     {'where': 'host', 'host_id': 'ember', 'defer': 'spare', 'memory_max_gb': 2, 'slots': 3},
                     {'where': 'host', 'host_id': 'ember', 'defer': 'overnight', 'memory_max_gb': 2, 'est_minutes': 481},
                     {'where': 'host', 'host_id': 'ember', 'defer': 'spare', 'memory_max_gb': 2, 'self_heavy': True}):
            with self.subTest(args=args), self.assertRaises(cq.CqError):
                self.submit(**args)
        jid = self.deferred(priority='verify')
        job = self.job(jid)
        self.assertEqual(job['priority'], 'background')
        receipt = cq.read_json(os.path.join(self.repo, '.baton', 'compute', jid + '.json'))
        self.assertEqual(receipt['source_sha'], job['sha'])
        self.assertEqual(receipt['worktree_identity'], job['worktree_identity'])
        script = cq.make_launcher(dict(job, host_config={'slots': 4}, deferred_load_cap=12))
        self.assertIn('. "$D/job.env"\nSELFHEAVY=1', script)
        self.assertIn('deferred.py" register "$ID"', script)
        self.assertNotIn('setsid nohup bash', script)
        self.assertIn('export TMPDIR=%q\\n\' "$D/tmp"', script)
        encoded = re.search(r'printf %s ([A-Za-z0-9+/=]+) \| base64 -d > "\$D/deferred.json"', script).group(1)
        descriptor = json.loads(base64.b64decode(encoded))
        self.assertEqual((descriptor['timezone'], descriptor['window_start'], descriptor['window_end']), ('Europe/London', 0, 8))

    def test_offline_and_full_host_staging_no_slot_charge(self):
        jid = self.deferred()
        self.ctx.sync = False
        self.host.up = False
        cq.deferred_pass(self.ctx)
        self.assertEqual(self.job(jid)['status'], 'queued')
        self.assertEqual(self.job(jid)['attempts'], 0)
        self.host.up = True
        self.host.held = 4
        self.advance(61)
        cq.deferred_pass(self.ctx)
        self.assertEqual(self.job(jid)['status'], 'launching')
        self.assertEqual(self.spawned, [[jid]])
        snapshot, _ = cq.host_snapshot(self.ctx)
        host = cq.probe(self.ctx, snapshot)['capacity']['host']
        self.assertEqual(host['pending'], 0)
        self.assertEqual(cq.reserved_memory(self.ctx), 0)
        self.assertEqual(cq.running_jobs(self.ctx), [])

    def test_legacy_scheduler_and_cancel_cannot_find_private_job(self):
        jid = self.deferred()
        self.assertTrue(os.path.isfile(self.ctx.p('deferred-jobs', jid + '.json')))
        self.assertFalse(os.path.exists(self.ctx.p('jobs', jid + '.json')))
        # Legacy clients enumerate and directly load only jobs/*.json.
        def legacy_jobs():
            path = self.ctx.p('jobs')
            return [cq.read_json(os.path.join(path, name)) for name in os.listdir(path)] if os.path.isdir(path) else []
        self.assertEqual([j for j in legacy_jobs() if j and j.get('status') == 'queued'], [])
        self.assertIsNone(cq.read_json(self.ctx.p('jobs', jid + '.json')))
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        self.advance(10000)
        self.assertEqual([j for j in legacy_jobs() if j and j.get('status') in ('launching', 'running')], [])
        script = cq.make_launcher(self.job(jid))
        self.assertIn('printf deferred > "$D/temp_storage"', script)  # legacy orphan pruning skips marked dirs
        self.assertEqual(self.job(jid)['status'], 'launching')

    def test_deferred_controller_ignores_legacy_leader_lease(self):
        jid = self.deferred()
        self.ctx.sync = False
        self.tick(session='old-leader')
        with mock.patch.object(cq, 'leader_claim', side_effect=AssertionError('must not claim legacy lease')):
            cq.deferred_pass(self.ctx)
        self.assertEqual(self.job(jid)['status'], 'launching')
        self.assertEqual(self.spawned, [[jid]])

    def test_controller_uses_pinned_runtime_and_records_process_identity(self):
        self.deferred()
        self.ctx.sync, self.ctx.spawn = False, None
        with mock.patch.dict(os.environ, {'CQ_DEFERRED_AUTOSTART': '0'}), mock.patch.object(cq, 'spawn_detached') as spawn:
            cq.ensure_deferred_controller(self.ctx, force=True)
            spawn.assert_not_called()
        with mock.patch.dict(os.environ, {'CQ_DEFERRED_AUTOSTART': '1'}), mock.patch.object(cq, 'spawn_detached') as spawn:
            cq.ensure_deferred_controller(self.ctx, force=True)
        argv = spawn.call_args.args[0]
        runtime = argv[1]
        self.assertIn(os.path.join('deferred-runtime', ''), runtime)
        self.assertTrue(os.path.isfile(runtime))
        for name in cq.DEFERRED_RUNTIME_FILES:
            self.assertTrue(os.path.isfile(os.path.join(os.path.dirname(runtime), name)))
        result = subprocess.run([sys.executable, runtime, 'status', '--state', self.ctx.state], input='{}',
                                text=True, capture_output=True, timeout=20)
        self.assertTrue(json.loads(result.stdout)['ok'], result.stderr)
        with mock.patch.object(cq, 'refresh_host_config'), mock.patch.object(cq, 'deferred_pass', return_value=False), \
             mock.patch.object(cq, '_controller_start_identity', return_value=1234):
            cq.deferred_loop(self.ctx)
        receipt = cq.read_json(self.ctx.p('deferred-controller.json'))
        self.assertEqual((receipt['pid'], receipt['process_start_filetime'], receipt['status']), (os.getpid(), 1234, 'stopped'))

    def test_uncertain_registration_reconciles_existing_copy(self):
        jid = self.deferred()
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        job = self.job(jid)
        job.update(launch_uncertain=True, deferred_phase='registration_unconfirmed')
        cq.save_job(self.ctx, job)
        ops = []
        def control(ctx, job, op):
            ops.append(op)
            return {'version': 1, 'id': jid, 'phase': 'absent' if op == 'status' else 'queued'}
        with mock.patch.object(cq, 'deferred_control', side_effect=control), mock.patch.object(cq, 'job_ssh', return_value=(0, 'complete\n', '')):
            cq.reap_deferred(self.ctx, job, None, self.clock[0], [])
        self.assertEqual(ops, ['status', 'register'])
        self.assertTrue(self.job(jid)['deferred_registered'])
        self.assertEqual(self.job(jid)['deferred_phase'], 'queued')
        self.advance(10000)
        with mock.patch.object(cq, 'deferred_control', return_value={'version': 1, 'id': jid, 'phase': 'queued'}):
            cq.reap_deferred(self.ctx, self.job(jid), None, self.clock[0], [])
        self.assertEqual(self.job(jid)['status'], 'launching')

    def test_cancel_requires_host_confirmation(self):
        jid = self.deferred()
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        with mock.patch.object(cq, 'deferred_control', return_value={'error': 'offline'}):
            self.assertEqual(self.cancel(jid)['status'], 'launching')
            cq.reap_deferred(self.ctx, self.job(jid), None, self.clock[0], [])
        self.assertTrue(self.job(jid)['cancel_requested'])
        self.assertEqual(self.job(jid)['status'], 'launching')
        with mock.patch.object(cq, 'deferred_control', return_value={'version': 1, 'id': jid, 'phase': 'cancelled'}):
            cq.reap_deferred(self.ctx, self.job(jid), None, self.clock[0], [])
        self.assertEqual(self.job(jid)['status'], 'cancelled')

    def test_receipt_failure_rejects_without_saving_or_starting(self):
        jid = 'cq-20270101T000000-1234567890123456'
        with mock.patch.object(cq, 'new_id', return_value=jid), \
             mock.patch.object(cq, 'create_deferred_receipt', side_effect=OSError('denied')), \
             mock.patch.object(cq, 'ensure_deferred_controller') as start, \
             self.assertRaisesRegex(cq.CqError, 'submission rejected'):
            self.deferred()
        self.assertIsNone(self.job(jid))
        start.assert_not_called()

    def test_job_save_failure_removes_receipt_without_starting(self):
        jid = 'cq-20270101T000000-1234567890123456'
        with mock.patch.object(cq, 'new_id', return_value=jid), \
             mock.patch.object(cq, 'save_job', side_effect=OSError('disk full')), \
             mock.patch.object(cq, 'ensure_deferred_controller') as start, \
             self.assertRaisesRegex(cq.CqError, 'submission rejected'):
            self.deferred()
        self.assertIsNone(self.job(jid))
        self.assertFalse(os.path.exists(os.path.join(self.repo, '.baton', 'compute', jid + '.json')))
        start.assert_not_called()

    def test_save_error_after_atomic_commit_preserves_job_and_receipt(self):
        jid = 'cq-20270101T000000-1234567890123456'
        save = cq.save_job
        def save_then_fail(ctx, job):
            save(ctx, job)
            raise OSError('late write error')
        with mock.patch.object(cq, 'new_id', return_value=jid), mock.patch.object(cq, 'save_job', side_effect=save_then_fail):
            self.assertEqual(self.deferred(), jid)
        self.assertEqual(self.job(jid)['status'], 'queued')
        self.assertEqual(cq.read_json(os.path.join(self.repo, '.baton', 'compute', jid + '.json'))['id'], jid)

    def test_receipt_never_replaces_an_existing_path(self):
        jid = 'cq-20270101T000000-1234567890123456'
        directory = os.path.join(self.repo, '.baton', 'compute')
        os.makedirs(directory)
        path = os.path.join(directory, jid + '.json')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('existing')
        with mock.patch.object(cq, 'new_id', return_value=jid), self.assertRaisesRegex(cq.CqError, 'submission rejected'):
            self.deferred()
        with open(path, encoding='utf-8') as f:
            self.assertEqual(f.read(), 'existing')
        self.assertIsNone(self.job(jid))

    def test_receipt_rejects_reparse_directory(self):
        actual = cq.os.lstat
        def fake_lstat(path, *args, **kwargs):
            if os.path.basename(str(path)) == 'compute':
                return types.SimpleNamespace(st_mode=stat.S_IFDIR, st_file_attributes=0x400)
            return actual(path, *args, **kwargs)
        with mock.patch.object(cq.os, 'lstat', side_effect=fake_lstat), \
             mock.patch.object(cq, 'ensure_deferred_controller') as start, \
             self.assertRaisesRegex(cq.CqError, 'reparse'):
            self.deferred()
        self.assertEqual(cq.all_deferred_jobs(self.ctx), [])
        start.assert_not_called()

    def test_missing_helper_is_explicit(self):
        jid = self.deferred()
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        with mock.patch.object(cq, 'job_ssh', return_value=(42, 'CQ_DEFERRED_HELPER_MISSING\n', '')):
            state = cq.deferred_control(self.ctx, self.job(jid), 'status')
        self.assertIn('not installed', state['error'])

    def test_normal_admission_does_not_reserve_staged_deferred_job(self):
        jid = self.deferred(slots=2)
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        normal = self.submit(slots=2)
        with mock.patch.object(cq, 'RAM_JOBS', True), mock.patch.object(cq, 'RAM_BUDGET_GB', 5):
            self.assertTrue(cq.dispatch(self.ctx, {'job': normal, 'where': 'host'})['ok'])

    def test_failed_fetch_retains_source_and_refresh_is_cached_when_offline(self):
        jid = self.deferred(gets=['result.bin'])
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        job = self.job(jid)
        ent = dict(exit=0, started=int(self.clock[0])-10, launched=int(self.clock[0])-10,
                   finished=int(self.clock[0]), cpu=1, alive=False, src=True)
        with mock.patch.object(cq, 'fetch_results', return_value=([], [])):
            cq.reap_host(self.ctx, job, ent, self.clock[0], [])
        self.assertEqual(self.job(jid)['artifact_missing'][0]['path'], 'result.bin')
        with mock.patch.object(cq, 'job_ssh') as ssh:
            cq.cleanup_host(self.ctx, {'jobs': {jid: ent}}, self.clock[0], host=job['host'], binding=job['host_config'])
            ssh.assert_not_called()
        with mock.patch.object(cq, 'deferred_control', return_value={'error': 'offline'}):
            result = cq.cmd_status(self.ctx, {'job': jid, 'refresh': True})
        self.assertFalse(result['refresh']['ok'])
        self.assertTrue(result['refresh']['cached'])
        self.assertEqual(result['job']['status'], 'done')

    def test_artifact_hashes_and_refresh_preserve_completed_snapshot(self):
        jid = self.deferred(gets=['first.bin', 'later.bin'])
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        job = self.job(jid)
        ent = dict(exit=0, started=int(self.clock[0])-10, launched=int(self.clock[0])-10,
                   finished=int(self.clock[0]), cpu=1, alive=False, src=True)
        first = b'original bytes'
        later = b'late bytes'
        source = self.ctx.p('mock-artifacts')
        os.makedirs(source)
        with open(os.path.join(source, 'first.bin'), 'wb') as f:
            f.write(first)
        class Process:
            def __init__(self, data):
                self.stdout, self.stderr, self.code = io.BytesIO(data), io.BytesIO(), None
            def wait(self):
                self.code = 0
                return 0
            def poll(self):
                return self.code
            def kill(self):
                self.code = -1
        def stream(current, script, timeout):
            captured = {d['path'] for d in current.get('artifact_digests', [])}
            wanted = [(i, rel) for i, rel in enumerate(current['gets']) if rel not in captured]
            wire = io.BytesIO(b'{"tail":["original log"]}\n')
            wire.seek(0, 2)
            artifact_transfer.send(source, wanted, artifact_transfer.LEGACY_FILE,
                                   artifact_transfer.DEFAULT_TOTAL, wire)
            return Process(wire.getvalue())
        self.ctx.artifact_stream = stream
        cq.reap_host(self.ctx, job, ent, self.clock[0], [])
        done = self.job(jid)
        self.assertEqual(done['artifact_digests'], [{'path': 'first.bin', 'sha256': hashlib.sha256(first).hexdigest(), 'size_bytes': len(first)}])
        self.assertEqual(done['tail_digest'], cq.stored_tail_digest(done['tail']))
        self.assertEqual(done['artifact_missing'][0]['path'], 'later.bin')
        original_tail = done['tail_digest']
        with open(os.path.join(source, 'later.bin'), 'wb') as f:
            f.write(later)
        def ssh(ctx, job, script, timeout=60):
            if 'CQ_REFRESH_END' in script:
                return 0, 'CQ_REFRESH_END\n', ''
            raise AssertionError('unexpected non-streaming fetch')
        with mock.patch.object(cq, 'deferred_control', return_value={'version': 1, 'id': jid, 'phase': 'done'}), \
             mock.patch.object(cq, 'job_ssh', side_effect=ssh):
            result = cq.cmd_status(self.ctx, {'job': jid, 'refresh': True})
        self.assertTrue(result['refresh']['ok'])
        done = self.job(jid)
        self.assertEqual(done['artifact_missing'], [])
        self.assertEqual(done['tail_digest'], original_tail)
        self.assertEqual(done['artifact_digests'][1], {'path': 'later.bin', 'sha256': hashlib.sha256(later).hexdigest(), 'size_bytes': len(later)})
        with open(self.ctx.p('out', jid, 'first.bin'), 'rb') as f:
            self.assertEqual(f.read(), first)

    def test_explicit_refresh_never_registers_an_absent_job(self):
        jid = self.deferred()
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        job = self.job(jid)
        job['launch_uncertain'] = True
        cq.save_job(self.ctx, job)
        with mock.patch.object(cq, 'deferred_control', return_value={'version': 1, 'id': jid, 'phase': 'absent'}) as control, \
             mock.patch.object(cq, 'job_ssh', return_value=(0, 'CQ_REFRESH_END\n', '')):
            result = cq.cmd_status(self.ctx, {'job': jid, 'refresh': True})
        self.assertTrue(result['refresh']['ok'])
        self.assertEqual([call.args[2] for call in control.call_args_list], ['status'])
        self.assertFalse(result['job']['deferred_registered'])
