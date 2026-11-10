import unittest
from unittest import mock

import atexit
import os
import shutil
import tempfile

_TEST_STATE = tempfile.mkdtemp(prefix="baton-lifecycle-state-")
os.environ["BATON_STATE"] = _TEST_STATE
atexit.register(shutil.rmtree, _TEST_STATE, True)

from baton import procs


class WindowsEndProcessTests(unittest.TestCase):
    def setUp(self):
        self.kernel = mock.Mock()
        self.kernel.OpenProcess.return_value = 42
        self.kernel.TerminateProcess.return_value = 1
        self.kernel.WaitForSingleObject.return_value = 0
        self.windows = mock.patch.object(procs.os, "name", "nt")
        self.kernel_patch = mock.patch.object(procs, "_windows_kernel", return_value=self.kernel)
        self.start_patch = mock.patch.object(procs, "_windows_start_time", return_value=123)
        self.alive_patch = mock.patch("baton.hook._pid_alive", side_effect=AssertionError("PID lookup used"))
        for patcher in (self.windows, self.kernel_patch, self.start_patch, self.alive_patch):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_signaled_same_handle_proves_exit_even_if_pid_is_reused(self):
        self.assertTrue(procs.end_process(99, wait=1.25, expected_start=123))
        self.kernel.OpenProcess.assert_called_once_with(0x101001, False, 99)
        procs._windows_start_time.assert_called_once_with(self.kernel, 42)
        self.kernel.TerminateProcess.assert_called_once_with(42, 1)
        self.kernel.WaitForSingleObject.assert_called_once_with(42, 1250)
        self.kernel.CloseHandle.assert_called_once_with(42)

    def test_identity_mismatch_or_missing_never_terminates(self):
        for expected in (124, None):
            with self.subTest(expected=expected):
                self.assertFalse(procs.end_process(99, expected_start=expected))
        self.kernel.TerminateProcess.assert_not_called()
        self.kernel.WaitForSingleObject.assert_not_called()
        self.assertEqual(self.kernel.CloseHandle.call_count, 2)

    def test_open_denied_fails_closed(self):
        self.kernel.OpenProcess.return_value = 0
        self.assertFalse(procs.end_process(99, expected_start=123))
        self.kernel.TerminateProcess.assert_not_called()
        self.kernel.CloseHandle.assert_not_called()

    def test_identity_query_or_termination_denied_fails_closed(self):
        procs._windows_start_time.return_value = None
        self.assertFalse(procs.end_process(99, expected_start=123))
        procs._windows_start_time.return_value = 123
        self.kernel.TerminateProcess.return_value = 0
        self.assertFalse(procs.end_process(99, expected_start=123))
        self.kernel.WaitForSingleObject.assert_not_called()
        self.assertEqual(self.kernel.CloseHandle.call_count, 2)

    def test_timeout_and_wait_failure_fail_closed(self):
        for result in (0x102, 0xffffffff):
            with self.subTest(result=result):
                self.kernel.WaitForSingleObject.return_value = result
                self.assertFalse(procs.end_process(99, wait=0, expected_start=123))
        self.assertEqual(self.kernel.CloseHandle.call_count, 2)


if __name__ == "__main__":
    unittest.main()
