import io
import threading
import unittest
from unittest import mock

import rpc_workers


class WorkersTests(unittest.TestCase):
    def test_waits_cannot_occupy_every_worker(self):
        release = threading.Event()
        pool = rpc_workers.Workers()
        self.addCleanup(pool.close)
        self.addCleanup(release.set)
        self.assertTrue(pool.submit(release.wait, 3, long_wait=True))
        self.assertTrue(pool.submit(release.wait, 3, long_wait=True))
        self.assertFalse(pool.submit(self.fail, 'third wait executed', long_wait=True))
        control = threading.Event()
        self.assertTrue(pool.submit(control.set))
        self.assertTrue(control.wait(2), 'status/cancel worker was occupied by waits')

    def test_bounded_admission_no_replay_and_idle_threads_leave(self):
        release = threading.Event()
        pool = rpc_workers.Workers(workers=2, outstanding=4)
        self.addCleanup(pool.close)
        self.addCleanup(release.set)
        calls = []
        def call(i):
            release.wait(3)
            calls.append(i)
        for i in range(4):
            self.assertTrue(pool.submit(call, i))
        self.assertFalse(pool.submit(call, 99))
        self.assertEqual(pool.pending, 4)
        release.set()
        with pool.condition:
            self.assertTrue(pool.condition.wait_for(lambda: not pool.threads, 3))
        self.assertCountEqual(calls, range(4))
        self.assertEqual(pool.pending, 0)
        self.assertEqual(pool.waiting, 0)
        self.assertEqual(len(pool.queue), 0)
        again = threading.Event()
        self.assertTrue(pool.submit(again.set))
        self.assertTrue(again.wait(2))

    def test_close_drains_and_rejects_new_calls(self):
        pool = rpc_workers.Workers()
        release = threading.Event()
        done = threading.Event()
        self.assertTrue(pool.submit(release.wait, 3))
        closer = threading.Thread(target=lambda: (pool.close(), done.set()))
        closer.start()
        try:
            with pool.condition:
                pool.condition.wait_for(lambda: pool.closed, 2)
            self.assertFalse(pool.submit(self.fail, 'call accepted after close'))
            self.assertFalse(done.is_set())
        finally:
            release.set()
            closer.join(3)
        self.assertTrue(done.is_set())

    def test_failed_start_has_no_admitted_or_replayed_call(self):
        with rpc_workers.Workers() as pool, mock.patch.object(threading.Thread, 'start', side_effect=RuntimeError):
            self.assertFalse(pool.submit(self.fail, 'unstarted call executed'))
            self.assertEqual(pool.pending, 0)
            self.assertEqual(pool.waiting, 0)
            self.assertEqual(len(pool.queue), 0)
            self.assertFalse(pool.threads)

    def test_handler_failure_does_not_strand_other_calls(self):
        release = threading.Event()
        done = threading.Event()
        def fail():
            release.wait(3)
            raise RuntimeError('simulated writer failure')
        with mock.patch('sys.stderr', io.StringIO()):
            with rpc_workers.Workers(workers=1) as pool:
                self.assertTrue(pool.submit(fail))
                self.assertTrue(pool.submit(done.set))
                release.set()
        self.assertTrue(done.is_set())


if __name__ == '__main__':
    unittest.main()
