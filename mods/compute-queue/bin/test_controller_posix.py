"""Two small macOS behaviours of the queue, each behind a function that takes its platform dependency as an argument:
the process start time read with `ps -o lstart=` (fleet_queue._ps_start_identity, used where /proc does not exist) and
the full flush of a durable write (cq.full_fsync: F_FULLFSYNC where the platform has it, else fsync).

Run: python -m unittest test_controller_posix   (from the mod's bin folder)
"""
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cq
import fleet_queue as fq


class FakePs:
    """Stands in for subprocess.run: records the call and answers with what a `ps` would."""

    def __init__(self, stdout="", returncode=0, raises=None):
        self.stdout, self.returncode, self.raises, self.calls = stdout, returncode, raises, []

    def __call__(self, argv, **kw):
        self.calls.append((argv, kw))
        if self.raises is not None:
            raise self.raises
        return SimpleNamespace(stdout=self.stdout, stderr="", returncode=self.returncode)


def epoch(*local):
    return int(time.mktime((*local, 0, 0, -1)))


class PsStartIdentityTests(unittest.TestCase):
    def test_lstart_becomes_the_epoch_of_that_local_time(self):
        ps = FakePs("Thu Nov 12 16:49:13 2026\n")
        self.assertEqual(fq._ps_start_identity(123, run=ps), epoch(2026, 11, 12, 16, 49, 13))

    def test_ps_runs_in_the_c_locale_with_the_pid(self):
        ps = FakePs("Thu Nov 12 16:49:13 2026\n")
        fq._ps_start_identity(123, run=ps)
        argv, kw = ps.calls[0]
        self.assertEqual(argv, ["ps", "-o", "lstart=", "-p", "123"])
        self.assertEqual((kw["env"]["LC_ALL"], kw["env"]["LANG"]), ("C", "C"))
        self.assertTrue(kw["timeout"] > 0)

    def test_a_single_digit_day_is_padded_with_two_spaces(self):
        self.assertEqual(fq._ps_start_identity(1, run=FakePs("Sun Jan  5 09:01:02 2026\n")), epoch(2026, 1, 5, 9, 1, 2))

    def test_the_month_is_read_by_name_not_by_the_parent_locale(self):
        for number, name in enumerate(("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), 1):
            self.assertEqual(fq._ps_start_identity(1, run=FakePs(f"Mon {name} 10 00:00:01 2026\n")), epoch(2026, number, 10, 0, 0, 1), name)

    def test_two_start_times_one_second_apart_differ(self):
        a = fq._ps_start_identity(1, run=FakePs("Thu Nov 12 16:49:13 2026\n"))
        b = fq._ps_start_identity(1, run=FakePs("Thu Nov 12 16:49:14 2026\n"))
        self.assertEqual(b - a, 1)

    def test_every_failure_is_none(self):
        for name, ps in (("no such process", FakePs("", returncode=1)),
                         ("empty output", FakePs("")),
                         ("garbage", FakePs("not a date at all\n")),
                         ("bad month", FakePs("Thu Foo 12 16:49:13 2026\n")),
                         ("impossible day", FakePs("Thu Nov 99 16:49:13 2026\n")),
                         ("no ps", FakePs(raises=FileNotFoundError("ps"))),
                         ("ps hangs", FakePs(raises=subprocess.TimeoutExpired("ps", 5))),
                         ("ps fails to start", FakePs(raises=PermissionError("ps")))):
            self.assertIsNone(fq._ps_start_identity(123, run=ps), name)

    def test_a_pid_that_is_not_a_positive_int_is_never_passed_to_ps(self):
        ps = FakePs("Thu Nov 12 16:49:13 2026\n")
        for pid in (None, 0, -1, "1; rm -rf /", 1.5, True):
            self.assertIsNone(fq._ps_start_identity(pid, run=ps), pid)
        self.assertEqual(ps.calls, [])


class PidIdentityTests(unittest.TestCase):
    def test_macos_reads_ps(self):
        with mock.patch.object(fq, "_is_macos", return_value=True), mock.patch.object(os, "name", "posix"), \
                mock.patch.object(fq, "_ps_start_identity", return_value=1700000000) as ps:
            self.assertEqual(fq._pid_identity(77), 1700000000)
        ps.assert_called_once_with(77)

    def test_macos_without_ps_is_unknown_as_before(self):
        with mock.patch.object(fq, "_is_macos", return_value=True), mock.patch.object(os, "name", "posix"), \
                mock.patch.object(fq, "_ps_start_identity", return_value=None):
            self.assertIsNone(fq._pid_identity(77))

    def test_elsewhere_ps_is_not_asked(self):
        with mock.patch.object(fq, "_is_macos", return_value=False), mock.patch.object(os, "name", "posix"), \
                mock.patch.object(fq, "_ps_start_identity") as ps:
            fq._pid_identity(os.getpid())
        ps.assert_not_called()

    def test_a_recycled_pid_is_a_gone_starter_on_macos(self):
        receipt = dict(pid=4242, process_start_identity=1700000000)
        with mock.patch.object(fq, "_is_macos", return_value=True), mock.patch.object(os, "name", "posix"), \
                mock.patch.object(os, "kill"):
            for now, gone in ((1700000000, False), (1700009999, True), (None, False)):
                with mock.patch.object(fq, "_pid_identity", return_value=now):
                    self.assertEqual(fq._starter_gone(receipt), gone, now)


class FullFsyncTests(unittest.TestCase):
    class Fcntl:
        F_FULLFSYNC = 51

        def __init__(self, fail=None):
            self.calls, self.fail = [], fail

        def fcntl(self, fd, command, *args):
            self.calls.append((fd, command))
            if self.fail:
                raise self.fail

    def test_f_fullfsync_is_used_when_the_platform_has_it(self):
        fake = self.Fcntl()
        with mock.patch.object(os, "fsync") as fsync:
            cq.full_fsync(9, fcntl_module=fake)
        self.assertEqual(fake.calls, [(9, 51)])
        fsync.assert_not_called()

    def test_a_failing_f_fullfsync_falls_back_to_fsync(self):
        for error in (OSError(25, "not a file system that can"), PermissionError("x")):
            fake = self.Fcntl(fail=error)
            with mock.patch.object(os, "fsync") as fsync:
                cq.full_fsync(9, fcntl_module=fake)
            self.assertEqual(fake.calls, [(9, 51)])
            fsync.assert_called_once_with(9)

    def test_without_the_constant_it_is_fsync(self):
        for module in (SimpleNamespace(fcntl=mock.Mock()), SimpleNamespace()):
            with mock.patch.object(os, "fsync") as fsync:
                cq.full_fsync(9, fcntl_module=module)
            fsync.assert_called_once_with(9)

    def test_an_fsync_error_after_the_fallback_still_raises(self):
        with mock.patch.object(os, "fsync", side_effect=OSError(5, "io")), self.assertRaises(OSError):
            cq.full_fsync(9, fcntl_module=self.Fcntl(fail=OSError(25, "x")))

    def test_the_module_default_is_the_real_fcntl_where_there_is_one(self):
        with tempfile.TemporaryFile() as f, mock.patch.object(os, "fsync") as fsync:
            cq.full_fsync(f.fileno())
        if cq.fcntl is None or not hasattr(cq.fcntl, "F_FULLFSYNC"):
            fsync.assert_called_once()  # Windows and Linux: plain fsync


class AtomicWriteTests(unittest.TestCase):
    def test_a_durable_write_flushes_through_full_fsync(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(cq, "full_fsync") as flush:
            path = os.path.join(directory, "x.json")
            cq.atomic_write(path, b"{}", durable=True)
            self.assertEqual(Path(path).read_bytes(), b"{}")
        # the file; and, where a directory can be opened and flushed, the directory entry of the replace
        self.assertEqual(flush.call_count, 1 if os.name == "nt" else 2)

    def test_a_plain_write_flushes_nothing(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(cq, "full_fsync") as flush, \
                mock.patch.object(os, "fsync") as fsync:
            cq.atomic_write(os.path.join(directory, "x.json"), b"{}")
        flush.assert_not_called()
        fsync.assert_not_called()


if __name__ == "__main__":
    unittest.main()
