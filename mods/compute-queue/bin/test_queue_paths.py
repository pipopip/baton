import json
import os
import tempfile
import unittest
from unittest import mock

import cq
import fleet_queue
import queue_paths


class PortabilityTests(unittest.TestCase):
    def test_state_overrides_and_local_runtime_paths(self):
        with tempfile.TemporaryDirectory() as state, mock.patch.dict(os.environ, {}, clear=True):
            root = queue_paths.Path(queue_paths.__file__).resolve().parents[3]
            if root.parent.name == 'worktrees' and root.parent.parent.name == '.claude':
                root = root.parents[2]
            self.assertEqual(queue_paths.default_state(), str(root / '.state' / 'compute'))
            os.environ['BATON_STATE'] = state
            self.assertEqual(queue_paths.default_state(), os.path.join(state, 'compute'))
            os.environ['CQ_STATE'] = state
            self.assertEqual(queue_paths.default_state(), state)
            with open(os.path.join(state, 'runtime.local.json'), 'w') as f:
                json.dump({'heavyrun': '/existing/heavyrun.py', 'heavy_lock': '/existing/heavy.lock', 'remoterun': '/existing/remoterun.py'}, f)
            self.assertEqual(queue_paths.runtime_paths(state), ('/existing/remoterun.py', '/existing/heavyrun.py', '/existing/heavy.lock'))
            os.environ['CQ_HEAVY_LOCK'] = '/override/heavy.lock'
            self.assertEqual(queue_paths.runtime_paths(state)[2], '/override/heavy.lock')

    def test_legacy_directory_and_override_precedence(self):
        with tempfile.TemporaryDirectory() as state, mock.patch.dict(os.environ, {'CQ_BATON_DIR': state}, clear=True):
            legacy = queue_paths.Path(state)
            runner, helper, lock = queue_paths.runtime_paths(state)
            self.assertEqual(runner, str(legacy / 'remote' / 'remoterun.py'))
            self.assertEqual(helper, str(legacy / 'heavyrun.py'))
            self.assertEqual(lock, str(legacy / 'heavy.lock'))
            with open(os.path.join(state, 'runtime.local.json'), 'w') as f:
                json.dump({'heavyrun': '/local/helper.py', 'remoterun': '/local/runner.py'}, f)
            os.environ['CQ_REMOTERUN'] = '/env/runner.py'
            self.assertEqual(queue_paths.runtime_paths(state), ('/env/runner.py', '/local/helper.py', str(queue_paths.Path('/local/helper.py').parent / 'heavy.lock')))
            self.assertTrue(queue_paths.default_lock_confirmed(state))

    def test_existing_queue_requires_explicit_lock_and_fresh_queue_marks_before_jobs(self):
        with tempfile.TemporaryDirectory() as state, mock.patch.dict(os.environ, {}, clear=True):
            ctx = cq.Ctx(state=state)
            os.makedirs(ctx.p('jobs'))
            with open(ctx.p('jobs', 'old.json'), 'w') as f:
                f.write('{}')
            queue_paths.initialize_default_lock(state)
            self.assertIsNone(fleet_queue._laptop_config(ctx, {'logical_cpus': 16}))
            os.environ['CQ_HEAVY_LOCK'] = os.path.join(state, 'old-heavy.lock')
            with mock.patch.object(fleet_queue, 'E', cq):
                self.assertIsNotNone(fleet_queue._laptop_config(ctx, {'logical_cpus': 16}))
        with tempfile.TemporaryDirectory() as state, mock.patch.dict(os.environ, {}, clear=True):
            queue_paths.initialize_default_lock(state)
            self.assertTrue(queue_paths.default_lock_confirmed(state))
            os.makedirs(os.path.join(state, 'fleet', 'jobs'))
            with open(os.path.join(state, 'fleet', 'jobs', 'new.json'), 'w') as f:
                f.write('{}')
            self.assertTrue(queue_paths.default_lock_confirmed(state))
            os.environ['CQ_HEAVY_LOCK'] = os.path.join(state, 'legacy.lock')
            queue_paths.initialize_default_lock(state)
            del os.environ['CQ_HEAVY_LOCK']
            self.assertFalse(queue_paths.default_lock_confirmed(state))

    @unittest.skipUnless(os.name == 'nt', 'Windows legacy lock')
    def test_configured_legacy_lock_blocks_fleet_shared_lock(self):
        import msvcrt
        import windows_pool
        with tempfile.TemporaryDirectory() as state, mock.patch.dict(os.environ, {'CQ_BATON_DIR': state}, clear=True):
            path = queue_paths.runtime_paths(state)[2]
            windows_pool._setup()
            with open(path, 'w+b', buffering=0) as f:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
                lock = windows_pool.SharedLock(path)
                try:
                    self.assertFalse(lock.try_acquire())
                    f.seek(0)
                    msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
                    self.assertTrue(lock.try_acquire())
                finally:
                    lock.close()

    def test_no_host_never_spawns(self):
        with mock.patch.object(cq, 'HOST', ''), mock.patch.object(cq, '_run') as run:
            self.assertEqual(cq.real_ssh('true')[0], 255)
            self.assertEqual(cq.real_run_remoterun([], '.')[0], 255)
            run.assert_not_called()

    def test_default_bundled_runner_and_lock(self):
        with tempfile.TemporaryDirectory() as state, mock.patch.dict(os.environ, {}, clear=True):
            runner, heavy, lock = queue_paths.runtime_paths(state)
            self.assertTrue(runner.endswith('remote_stage.py'))
            self.assertEqual(heavy, '')
            self.assertEqual(lock, os.path.join(state, 'heavy.lock'))

if __name__ == '__main__':
    unittest.main()
