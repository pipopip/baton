import unittest
from unittest import mock

import atexit
import ctypes
import errno
import os
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import time

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
        self.windows = mock.patch.object(procs, "platform_name", return_value="windows")
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


REAL_PS_PROBE = procs._ps_probe


def fake_stat(pid=4242, comm="python", state="S", start=98765):
    """A /proc/<pid>/stat line: fields 4-21 are zero, field 22 is ``start``."""
    return f"{pid} ({comm}) {state} " + " ".join(["0"] * 18) + f" {start} " + " ".join(["0"] * 30) + "\n"


def fake_bsdinfo(pid, status=1, sec=1_700_000_000, usec=123456) -> bytes:
    """A proc_bsdinfo buffer as libproc fills it."""
    return struct.pack(procs._BSDINFO_FORMAT, 0, status, 0, pid, 1, 501, 20, 501, 20, 501, 20, 0,
                       b"python", b"python", 3, pid, 0, 0, 0, 0, sec, usec)


class PlatformNameTests(unittest.TestCase):
    def test_names_this_host(self):
        self.assertIn(procs.platform_name(), ("windows", "darwin", "linux", "other"))
        self.assertEqual(procs.platform_name() == "windows", os.name == "nt")

    @unittest.skipIf(os.name == "nt", "os.name is not patched: the sys.platform mapping needs a POSIX host")
    def test_maps_sys_platform(self):
        for value, want in (("darwin", "darwin"), ("linux", "linux"), ("linux2", "linux"), ("freebsd14", "other")):
            with self.subTest(platform=value), mock.patch.object(procs.sys, "platform", value):
                self.assertEqual(procs.platform_name(), want)

    def test_group_kwargs_follow_the_platform(self):
        with mock.patch.object(procs, "platform_name", return_value="windows"):
            self.assertEqual(procs.popen_group_kwargs(), {})
        for name in ("linux", "darwin"):
            with mock.patch.object(procs, "platform_name", return_value=name):
                self.assertEqual(procs.popen_group_kwargs(), {"start_new_session": True})


class LinuxIdentityTests(unittest.TestCase):
    def setUp(self):
        self.stat = mock.Mock(return_value=fake_stat())
        self.mounted = mock.Mock(return_value=True)
        for patcher in (mock.patch.object(procs, "platform_name", return_value="linux"),
                        mock.patch.object(procs, "_read_proc_stat", self.stat),
                        mock.patch.object(procs, "_proc_mounted", self.mounted)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_start_time_is_field_22(self):
        self.assertEqual(procs.process_start_time(4242), 98765)
        self.stat.assert_called_once_with(4242)

    def test_command_name_with_spaces_and_parentheses_does_not_shift_the_fields(self):
        for comm in ("a b) c", "(a b) c)", "x) S 1 2 3 (y", "plain"):
            with self.subTest(comm=comm):
                self.stat.return_value = fake_stat(comm=comm, start=555)
                self.assertEqual(procs.process_start_time(4242), 555)
                self.assertEqual(procs._parse_linux_stat(fake_stat(comm=comm, state="Z")), ("Z", 98765))

    def test_unreadable_or_malformed_stat_has_no_start_time(self):
        for text in ("", "4242 python S 1", "4242 (p) S 1 2", fake_stat(start="x"), fake_stat(start=0), fake_stat(start=-3)):
            with self.subTest(text=text[:30]):
                self.stat.return_value = text
                self.assertIsNone(procs.process_start_time(4242))
        self.stat.side_effect = PermissionError(errno.EACCES, "denied")
        self.assertIsNone(procs.process_start_time(4242))
        self.stat.side_effect = FileNotFoundError()
        self.assertIsNone(procs.process_start_time(4242))

    def test_pids_that_are_not_pids_are_refused_without_reading(self):
        for pid in (0, -1, True, "12", None, 1.5):
            with self.subTest(pid=pid):
                self.assertIsNone(procs.process_start_time(pid))
        self.stat.assert_not_called()

    def test_state_alive_gone_other_start_and_unknown(self):
        self.assertIs(procs.process_identity_state(4242, 98765), True)
        self.assertIs(procs.process_identity_state(4242, 98766), False)       # the pid was reused
        self.stat.side_effect = FileNotFoundError()
        self.assertIs(procs.process_identity_state(4242, 98765), False)       # gone
        self.stat.side_effect = ProcessLookupError()
        self.assertIs(procs.process_identity_state(4242, 98765), False)       # exited while being read
        self.mounted.return_value = False
        self.assertIsNone(procs.process_identity_state(4242, 98765))          # no /proc: cannot say gone
        self.stat.side_effect = PermissionError(errno.EACCES, "denied")
        self.assertIsNone(procs.process_identity_state(4242, 98765))
        self.stat.side_effect = None
        self.stat.return_value = "garbage"
        self.assertIsNone(procs.process_identity_state(4242, 98765))
        for bad in (0, -1, None, "98765", True):
            self.assertIsNone(procs.process_identity_state(4242, bad))

    def test_zombie_counts_as_gone(self):
        for state in ("Z", "X"):
            self.stat.return_value = fake_stat(state=state)
            self.assertIs(procs.process_identity_state(4242, 98765), False)
        for state in ("R", "S", "D", "T"):
            self.stat.return_value = fake_stat(state=state)
            self.assertIs(procs.process_identity_state(4242, 98765), True)
        self.stat.return_value = fake_stat(state="Z")
        self.assertEqual(procs.process_start_time(4242), 98765)    # a zombie still has its start time

    def test_started_epoch_is_boot_time_plus_ticks(self):
        with mock.patch.object(procs, "_boot_time", return_value=1000.0), \
                mock.patch.object(procs.os, "sysconf", return_value=100, create=True):
            self.stat.return_value = fake_stat(start=250)
            self.assertAlmostEqual(procs.process_started_epoch(4242), 1002.5)
        with mock.patch.object(procs, "_boot_time", return_value=None), \
                mock.patch.object(procs.os, "sysconf", return_value=100, create=True):
            self.assertIsNone(procs.process_started_epoch(4242))
        with mock.patch.object(procs, "_boot_time", return_value=1000.0), \
                mock.patch.object(procs.os, "sysconf", return_value=0, create=True):
            self.assertIsNone(procs.process_started_epoch(4242))


class DarwinIdentityTests(unittest.TestCase):
    def setUp(self):
        self.table = {}
        self.calls = []

        def call(pid):
            self.calls.append(pid)
            return self.table.get(pid, (0, b"\0" * 136, errno.ESRCH))
        self.ps = mock.Mock(return_value=("ok", 1_700_000_000_000_000, False))
        for patcher in (mock.patch.object(procs, "platform_name", return_value="darwin"),
                        mock.patch.object(procs, "_DARWIN_SOURCE", None),
                        mock.patch.object(procs, "_libproc_call", side_effect=call),
                        mock.patch.object(procs, "_ps_probe", self.ps)):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.answer(os.getpid())

    def answer(self, pid, **kw):
        self.table[pid] = (136, fake_bsdinfo(pid, **kw), 0)

    def test_struct_format_matches_the_c_layout(self):
        class BsdInfo(ctypes.Structure):
            _fields_ = ([(n, ctypes.c_uint32) for n in ("flags", "status", "xstatus", "pid", "ppid", "uid", "gid", "ruid",
                                                        "rgid", "svuid", "svgid", "rfu_1")]
                        + [("comm", ctypes.c_char * 16), ("name", ctypes.c_char * 32)]
                        + [(n, ctypes.c_uint32) for n in ("nfiles", "pgid", "pjobc", "e_tdev", "e_tpgid")]
                        + [("nice", ctypes.c_int32), ("start_tvsec", ctypes.c_uint64), ("start_tvusec", ctypes.c_uint64)])
        self.assertEqual(ctypes.sizeof(BsdInfo), 136)
        self.assertEqual(struct.calcsize(procs._BSDINFO_FORMAT), procs._BSDINFO_SIZE)
        info = BsdInfo.from_buffer_copy(fake_bsdinfo(77, status=5, sec=11, usec=22))
        self.assertEqual((info.pid, info.status, info.start_tvsec, info.start_tvusec), (77, 5, 11, 22))
        self.assertEqual(procs._parse_bsdinfo(136, fake_bsdinfo(77, status=5, sec=11, usec=22), 77), (5, 11_000_022))

    def test_start_time_is_seconds_and_microseconds(self):
        self.answer(500, sec=1_700_000_000, usec=123456)
        self.assertEqual(procs.process_start_time(500), 1_700_000_000_123_456)
        self.assertEqual(procs.process_started_epoch(500), 1_700_000_000.123456)
        self.ps.assert_not_called()

    def test_a_short_or_wrong_size_answer_is_not_accepted(self):
        raw = fake_bsdinfo(500)
        for rc, buf in ((0, raw), (100, raw), (137, raw), (-1, raw), (136, raw[:100])):
            with self.subTest(rc=rc, size=len(buf)):
                self.assertIsNone(procs._parse_bsdinfo(rc, buf, 500))

    def test_an_answer_for_another_pid_is_not_accepted(self):
        self.assertIsNone(procs._parse_bsdinfo(136, fake_bsdinfo(501), 500))
        self.assertIsNone(procs._parse_bsdinfo(136, fake_bsdinfo(500, sec=0), 500))
        self.assertIsNone(procs._parse_bsdinfo(136, fake_bsdinfo(500, usec=1_000_000), 500))

    def test_after_the_probe_a_bad_answer_is_unknown_and_ps_is_not_consulted(self):
        self.table[500] = (136, fake_bsdinfo(501), 0)             # wrong pid inside the struct
        self.assertIsNone(procs.process_start_time(500))
        self.table[501] = (100, fake_bsdinfo(501), 0)             # wrong size
        self.assertIsNone(procs.process_start_time(501))
        self.assertIsNone(procs.process_identity_state(500, 1_700_000_000_123_456))
        self.ps.assert_not_called()

    def test_a_failing_probe_chooses_ps_once_for_every_later_call(self):
        self.table[os.getpid()] = (136, fake_bsdinfo(os.getpid() + 1), 0)   # libproc answers for another pid
        self.assertEqual(procs.process_start_time(500), 1_700_000_000_000_000)
        self.assertEqual(procs.process_start_time(501), 1_700_000_000_000_000)
        self.assertIs(procs.process_identity_state(500, 1_700_000_000_000_000), True)
        self.assertEqual(self.calls, [os.getpid()])               # the probe, once; never asked per call
        self.assertEqual(procs._DARWIN_SOURCE, "ps")
        self.assertEqual(self.ps.call_count, 3)

    def test_a_probe_that_cannot_load_libproc_chooses_ps(self):
        with mock.patch.object(procs, "_libproc_call", side_effect=OSError("no libproc")) as call:
            self.assertEqual(procs.process_start_time(500), 1_700_000_000_000_000)
            self.assertEqual(procs.process_start_time(501), 1_700_000_000_000_000)
        call.assert_called_once()

    def test_a_working_probe_chooses_libproc_once(self):
        self.answer(500)
        procs.process_start_time(500)
        procs.process_start_time(500)
        self.assertEqual(self.calls, [os.getpid(), 500, 500])
        self.assertEqual(procs._DARWIN_SOURCE, "libproc")

    def test_zombie_status_counts_as_gone(self):
        start = 1_700_000_000_123_456
        self.answer(500, status=5)
        self.assertIs(procs.process_identity_state(500, start), False)
        self.assertEqual(procs.process_start_time(500), start)
        for status in (1, 2, 3, 4, 6):
            self.answer(500, status=status)
            self.assertIs(procs.process_identity_state(500, start), True)

    def test_gone_other_start_and_unknown(self):
        start = 1_700_000_000_123_456
        self.answer(500)
        self.assertIs(procs.process_identity_state(500, start + 1), False)
        self.assertIs(procs.process_identity_state(999, start), False)       # ESRCH
        self.table[998] = (0, b"\0" * 136, errno.EPERM)
        self.assertIsNone(procs.process_identity_state(998, start))

    def test_ps_output_is_whole_seconds_in_the_c_locale(self):
        want = int(time.mktime((2026, 11, 12, 13, 1, 36, 0, 0, -1))) * 1_000_000
        self.assertEqual(procs._parse_ps_lstart("Thu Nov 12 13:01:36 2026"), want)
        self.assertEqual(procs._parse_ps_lstart("Thu Nov 12 13:01:36 2026\n"), want)
        self.assertEqual(procs._parse_ps_lstart("Thu Nov  5 09:00:00 2026"),
                         int(time.mktime((2026, 11, 5, 9, 0, 0, 0, 0, -1))) * 1_000_000)
        for text in ("", "garbage", "Thu Nov 12 25:01:36 2026", "Do Nov 12 13:01:36 2026"):
            self.assertIsNone(procs._parse_ps_lstart(text))

    def test_ps_fallback_runs_ps_with_lc_all_c(self):
        done = subprocess.CompletedProcess([], 0, stdout="Thu Nov 12 13:01:36 2026\n", stderr="")
        with mock.patch("subprocess.run", return_value=done) as run:
            kind, start, zombie = REAL_PS_PROBE(321)
        self.assertEqual((kind, zombie), ("ok", False))
        self.assertEqual(start, int(time.mktime((2026, 11, 12, 13, 1, 36, 0, 0, -1))) * 1_000_000)
        self.assertEqual(run.call_args.args[0], ["ps", "-o", "lstart=", "-p", "321"])
        self.assertEqual(run.call_args.kwargs["env"]["LC_ALL"], "C")
        gone = subprocess.CompletedProcess([], 1, stdout="", stderr="")
        with mock.patch("subprocess.run", return_value=gone):
            self.assertEqual(REAL_PS_PROBE(321)[0], "gone")
        failed = subprocess.CompletedProcess([], 2, stdout="", stderr="ps: illegal option")
        with mock.patch("subprocess.run", return_value=failed):
            self.assertEqual(REAL_PS_PROBE(321)[0], "unknown")
        with mock.patch("subprocess.run", side_effect=FileNotFoundError("ps")):
            self.assertEqual(REAL_PS_PROBE(321)[0], "unknown")


class PosixEndProcessDecisionTests(unittest.TestCase):
    """end_process off Windows, with the kernel faked (these run on every OS)."""

    def setUp(self):
        self.actual = 100
        self.kill = mock.Mock()
        self.states = []
        for patcher in (mock.patch.object(procs, "platform_name", return_value="linux"),
                        mock.patch.object(procs, "process_start_time", side_effect=lambda pid: self.actual),
                        mock.patch.object(procs, "process_identity_state", side_effect=lambda pid, start: self.states.pop(0)),
                        mock.patch.object(procs.os, "kill", self.kill),
                        mock.patch.object(procs.os, "pidfd_open", side_effect=OSError(errno.ENOSYS, "no pidfd"), create=True),
                        mock.patch.object(signal, "SIGKILL", 9, create=True),
                        mock.patch.object(signal, "pidfd_send_signal", mock.Mock(), create=True),
                        mock.patch.object(time, "sleep")):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_refuses_without_or_with_a_different_start_and_never_signals(self):
        self.states = [False] * 10
        for expected in (None, 99, 101, 0, -1, "100", True):
            with self.subTest(expected=expected):
                self.assertFalse(procs.end_process(7, wait=0, expected_start=expected))
        for pid in (0, -5, None, True):
            with self.subTest(pid=pid):
                self.assertFalse(procs.end_process(pid, wait=0, expected_start=100))
        self.kill.assert_not_called()

    def test_refuses_when_the_start_time_cannot_be_read(self):
        self.actual = None
        self.states = [False]
        self.assertFalse(procs.end_process(7, wait=0, expected_start=100))
        self.kill.assert_not_called()

    def test_matching_start_is_killed_and_gone_is_the_answer(self):
        self.states = [True, False]
        self.assertTrue(procs.end_process(7, wait=5, expected_start=100))
        self.kill.assert_called_once_with(7, 9)

    def test_a_process_that_does_not_go_is_not_reported_gone(self):
        self.states = [True, None, True]
        self.assertFalse(procs.end_process(7, wait=0, expected_start=100))
        self.kill.assert_called_once_with(7, 9)

    def test_signal_errors(self):
        self.kill.side_effect = ProcessLookupError()     # exited between check and signal
        self.states = [False]
        self.assertTrue(procs.end_process(7, wait=0, expected_start=100))
        self.kill.side_effect = PermissionError()
        self.assertFalse(procs.end_process(7, wait=0, expected_start=100))

    def test_pidfd_signals_through_the_descriptor_and_closes_it(self):
        fd = os.open(os.devnull, os.O_RDONLY)
        sender = mock.Mock()
        self.states = [False]
        with mock.patch.object(procs.os, "pidfd_open", return_value=fd, create=True) as opened, \
                mock.patch.object(signal, "pidfd_send_signal", sender, create=True):
            self.assertTrue(procs.end_process(7, wait=0, expected_start=100))
        opened.assert_called_once_with(7)
        sender.assert_called_once_with(fd, 9)
        self.kill.assert_not_called()
        with self.assertRaises(OSError):
            os.fstat(fd)                                 # closed

    def test_pidfd_with_a_different_start_signals_nothing_and_is_closed(self):
        fd = os.open(os.devnull, os.O_RDONLY)
        sender = mock.Mock()
        self.actual = 101
        with mock.patch.object(procs.os, "pidfd_open", return_value=fd, create=True), \
                mock.patch.object(signal, "pidfd_send_signal", sender, create=True):
            self.assertFalse(procs.end_process(7, wait=0, expected_start=100))
        sender.assert_not_called()
        self.kill.assert_not_called()
        with self.assertRaises(OSError):
            os.fstat(fd)

    def test_pidfd_for_a_pid_that_is_gone_refuses(self):
        with mock.patch.object(procs.os, "pidfd_open", side_effect=ProcessLookupError(), create=True):
            self.assertFalse(procs.end_process(7, wait=0, expected_start=100))
        self.kill.assert_not_called()


class KillProcessGroupDecisionTests(unittest.TestCase):
    def test_group_is_signalled_only_for_an_unreaped_group_leader(self):
        killpg, getpgid = mock.Mock(), mock.Mock()
        with mock.patch.object(procs.os, "killpg", killpg, create=True), \
                mock.patch.object(procs.os, "getpgid", getpgid, create=True), \
                mock.patch.object(procs.os, "getpgrp", return_value=1, create=True), \
                mock.patch.object(signal, "SIGKILL", 9, create=True):
            proc = mock.Mock(pid=500, returncode=None)
            getpgid.return_value = 500
            procs.kill_process_group(proc)
            killpg.assert_called_once_with(500, 9)
            proc.kill.assert_not_called()
            killpg.reset_mock()
            getpgid.return_value = 1                      # shares another group: never killpg
            procs.kill_process_group(proc)
            killpg.assert_not_called()
            proc.kill.assert_called_once()
            proc.reset_mock()
            getpgid.return_value = 500
            proc.returncode = 0                           # reaped: the pid may belong to someone else now
            procs.kill_process_group(proc)
            killpg.assert_not_called()
            proc.kill.assert_called_once()
            proc.reset_mock()
            proc.returncode = None
            getpgid.side_effect = ProcessLookupError()
            procs.kill_process_group(proc)
            killpg.assert_not_called()
            proc.kill.assert_called_once()

    def test_a_leader_that_is_this_process_group_is_not_group_killed(self):
        killpg = mock.Mock()
        with mock.patch.object(procs.os, "killpg", killpg, create=True), \
                mock.patch.object(procs.os, "getpgid", return_value=500, create=True), \
                mock.patch.object(procs.os, "getpgrp", return_value=500, create=True):
            proc = mock.Mock(pid=500, returncode=None)
            procs.kill_process_group(proc)
        killpg.assert_not_called()
        proc.kill.assert_called_once()


SLEEPER = "import time; time.sleep(60)"
FAMILY = ("import subprocess, sys, time\n"
          "g = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
          "print(g.pid, flush=True)\n"
          "time.sleep(60)\n")


def spawn(code=SLEEPER, **kw):
    return subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **kw)


def reap(proc):
    """End and collect a test child so no zombie is left behind."""
    if proc.poll() is None:
        try:
            if procs.platform_name() != "windows" and os.getpgid(proc.pid) == proc.pid:
                os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.kill()
        except OSError:
            pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
    if proc.stdout is not None:
        proc.stdout.close()


@unittest.skipIf(os.name == "nt", "real POSIX processes")
class PosixRealProcessTests(unittest.TestCase):
    def child(self, code=SLEEPER, **kw):
        proc = spawn(code, stdout=kw.pop("stdout", subprocess.DEVNULL), **kw)
        self.addCleanup(reap, proc)
        return proc

    def test_start_time_is_a_stable_positive_integer(self):
        proc = self.child()
        first = procs.process_start_time(proc.pid)
        self.assertIsInstance(first, int)
        self.assertGreater(first, 0)
        self.assertEqual(procs.process_start_time(proc.pid), first)
        self.assertNotEqual(procs.process_start_time(os.getpid()), first)
        self.assertEqual(procs.process_start_time(os.getpid()), procs.process_start_time(os.getpid()))

    def test_started_epoch_is_about_now(self):
        proc = self.child()
        born = procs.process_started_epoch(proc.pid)
        self.assertIsNotNone(born)
        self.assertLess(abs(time.time() - born), 5)

    def test_no_start_time_for_a_pid_that_is_gone(self):
        proc = self.child()
        proc.kill()
        proc.wait(timeout=10)
        self.assertIsNone(procs.process_start_time(proc.pid))
        self.assertIsNone(procs.process_started_epoch(proc.pid))

    def test_end_process_refuses_a_wrong_start_and_ends_the_right_one(self):
        proc = self.child()
        start = procs.process_start_time(proc.pid)
        self.assertFalse(procs.end_process(proc.pid, wait=0.2))
        self.assertFalse(procs.end_process(proc.pid, wait=0.2, expected_start=start + 1))
        self.assertFalse(procs.end_process(proc.pid, wait=0.2, expected_start=max(1, start - 1)))
        time.sleep(0.1)
        self.assertIsNone(proc.poll())
        self.assertIs(procs.process_identity_state(proc.pid, start), True)
        self.assertTrue(procs.end_process(proc.pid, wait=10, expected_start=start))
        self.assertEqual(proc.wait(timeout=10), -signal.SIGKILL)
        self.assertIs(procs.process_identity_state(proc.pid, start), False)

    def test_end_process_of_a_pid_that_is_gone_is_not_a_success(self):
        proc = self.child()
        start = procs.process_start_time(proc.pid)
        proc.kill()
        proc.wait(timeout=10)
        self.assertFalse(procs.end_process(proc.pid, wait=0.2, expected_start=start))

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux reads the process state from /proc")
    def test_an_unreaped_child_is_a_zombie_and_counts_as_gone(self):
        proc = self.child()
        start = procs.process_start_time(proc.pid)
        os.kill(proc.pid, signal.SIGKILL)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and procs.process_identity_state(proc.pid, start) is not False:
            time.sleep(0.02)
        self.assertIs(procs.process_identity_state(proc.pid, start), False)
        self.assertTrue(os.path.exists(f"/proc/{proc.pid}"))      # the pid still exists: only its state says gone
        self.assertEqual(procs.process_start_time(proc.pid), start)

    def test_a_group_leader_is_killed_with_its_children(self):
        proc = self.child(FAMILY, stdout=subprocess.PIPE, text=True, **procs.popen_group_kwargs())
        grandchild = int(proc.stdout.readline())
        gstart = procs.process_start_time(grandchild)
        self.assertIsNotNone(gstart)
        self.addCleanup(lambda: procs.end_process(grandchild, wait=5, expected_start=gstart))
        self.assertIs(procs.process_identity_state(grandchild, gstart), True)
        self.assertEqual(os.getpgid(grandchild), proc.pid)
        procs.kill_process_group(proc)
        self.assertEqual(proc.wait(timeout=10), -signal.SIGKILL)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and procs.process_identity_state(grandchild, gstart) is not False:
            time.sleep(0.05)
        self.assertIs(procs.process_identity_state(grandchild, gstart), False)

    def test_a_child_in_the_callers_group_is_killed_alone(self):
        proc = self.child()
        self.assertEqual(os.getpgid(proc.pid), os.getpgrp())
        procs.kill_process_group(proc)
        self.assertEqual(proc.wait(timeout=10), -signal.SIGKILL)
        self.assertEqual(os.getpgid(0), os.getpgrp())             # this process is still here to check

if __name__ == "__main__":
    unittest.main()
