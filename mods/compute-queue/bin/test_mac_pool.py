"""mac_pool: pure parsers and tree logic on any platform, and real processes on macOS.

Run: python -m unittest test_mac_pool   (from the mod's bin folder)
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
if os.name == "nt":       # mac_pool runs on macOS only; Linux CI still runs its platform-neutral tests
    raise unittest.SkipTest("mac_pool is the macOS laptop pool")

import mac_pool as pool

DARWIN = sys.platform == "darwin"

VM_STAT = """Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                                    94732.
Pages active:                                1330839.
Pages inactive:                              1464557.
Pages speculative:                             1116.
Pages throttled:                                   0.
Pages wired down:                             177437.
Pages purgeable:                               43262.
"Translation faults":                      280873054.
File-backed pages:                           1288906.
Anonymous pages:                             1509553.
"""


class ParserTests(unittest.TestCase):
    def test_available_memory_counts_only_the_file_backed_share_of_inactive_pages(self):
        share = 1288906 / (1288906 + 1509553)
        expected = int((94732 + 1116 + 1464557 * share) * 16384)
        self.assertEqual(pool.parse_vm_stat(VM_STAT), expected)
        self.assertLess(pool.parse_vm_stat(VM_STAT), (94732 + 1116 + 1464557) * 16384)   # not all of inactive
        self.assertNotEqual(pool.parse_vm_stat(VM_STAT), pool.parse_vm_stat(VM_STAT.replace("43262", "9999999")) + 1)  # purgeable does not count
        self.assertEqual(pool.parse_vm_stat(VM_STAT), pool.parse_vm_stat(VM_STAT.replace("43262", "9999999")))

    def test_the_page_size_comes_from_the_header(self):
        self.assertEqual(pool.parse_vm_stat(VM_STAT.replace("16384", "4096")) * 4, pool.parse_vm_stat(VM_STAT), )

    def test_text_that_is_not_vm_stat_gives_none(self):
        self.assertIsNone(pool.parse_vm_stat(""))
        self.assertIsNone(pool.parse_vm_stat("Pages free: 5."))
        self.assertIsNone(pool.parse_vm_stat(VM_STAT.replace("Anonymous pages", "Other pages")))

    def test_memorystatus_level_is_a_percentage_of_the_memory(self):
        self.assertEqual(pool.parse_memorystatus("93\n", 48 * 2 ** 30), int(48 * 2 ** 30 * 0.93))
        for bad in ("", "x", "0", "101", "-5"):
            self.assertIsNone(pool.parse_memorystatus(bad, 48 * 2 ** 30), bad)
        self.assertIsNone(pool.parse_memorystatus("50", 0))

    def test_the_level_is_the_primary_figure_and_vm_stat_the_fallback(self):
        calls = {"hw.memsize": "51539607552", "kern.memorystatus_level": "50"}
        with mock.patch.object(pool, "_sysctl", side_effect=lambda name: calls[name]), \
                mock.patch.object(pool, "_out", return_value=VM_STAT):
            self.assertAlmostEqual(pool.available_gb(), 24.0)
            calls["kern.memorystatus_level"] = "0"       # not a sane percentage: vm_stat
            self.assertAlmostEqual(pool.available_gb(), pool.parse_vm_stat(VM_STAT) / 2 ** 30)

            def broken(name):
                if name == "kern.memorystatus_level":
                    raise OSError("no such sysctl")
                return calls[name]
            with mock.patch.object(pool, "_sysctl", side_effect=broken):
                self.assertAlmostEqual(pool.available_gb(), pool.parse_vm_stat(VM_STAT) / 2 ** 30)

    def test_pmset_says_battery_unless_the_first_line_says_ac(self):
        battery = "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1)\t96%; discharging; 9:45 remaining"
        ac = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=1)\t100%; charged; 0:00 remaining present: true"
        ups = "Now drawing from 'UPS Power'\n -APC (id=2)\t90%"
        self.assertIs(pool.parse_pmset(battery), True)
        self.assertIs(pool.parse_pmset(ac), False)
        self.assertIs(pool.parse_pmset(ups), True)
        self.assertIsNone(pool.parse_pmset(""))
        self.assertIsNone(pool.parse_pmset("pmset: command not found"))

    def test_swap_units(self):
        self.assertEqual(pool.parse_swap("total = 2048.00M  used = 100.00M  free = 1948.00M  (encrypted)"), 100 * 1024 ** 2)
        self.assertEqual(pool.parse_swap("total = 4.00G  used = 1.50G  free = 2.50G"), 1.5 * 1024 ** 3)
        self.assertIsNone(pool.parse_swap("garbage"))


def P(pid, ppid, pgid, start, stat="S"):
    return pool.Proc(pid, ppid, pgid, stat, start, 0)


ROWS = [P(1, 0, 1, 1), P(100, 1, 100, 10),                 # the runner
        P(200, 100, 200, 20), P(201, 200, 200, 21),         # the leader and a child in its group
        P(300, 1, 300, 30), P(301, 300, 300, 31),           # a setsid child orphaned to launchd, and its child
        P(400, 200, 200, 40, "Z"),                          # a zombie
        P(500, 1, 500, 50)]                                 # unrelated


class TreeTests(unittest.TestCase):
    def test_members_are_the_verified_recorded_pids_the_leaders_group_and_the_descendants(self):
        # 300 left the group and was orphaned: it is a member only because it was recorded; 301 is its child
        self.assertEqual(pool.members(ROWS, leader=(200, 20), known=[(300, 30)]), {200, 201, 300, 301})
        self.assertEqual(pool.members(ROWS, leader=(200, 20)), {200, 201})
        self.assertEqual(pool.members(ROWS, known={300: 30}), {300, 301})

    def test_a_recorded_pid_with_another_start_time_is_another_process(self):
        self.assertEqual(pool.members(ROWS, leader=(200, 99), known=[(300, 99), (500, 50)]), {500})
        # the leader's pid reused after its group ended: the group of the new process is not the job's
        reused = [P(200, 1, 200, 777), P(201, 200, 200, 778)]
        self.assertEqual(pool.members(reused, leader=(200, 20)), set())

    def test_the_environment_and_command_lines_decide_nothing(self):
        # a process whose command line merely names the tag (an agent's grep) is an unrelated process
        self.assertEqual(pool.members([P(600, 1, 600, 60)], leader=(200, 20)), set())
        self.assertFalse(hasattr(pool, "_tag_regex"))

    def test_this_process_and_zombies_are_never_members(self):
        me = P(os.getpid(), 1, os.getpid(), 5)
        self.assertEqual(pool.members([me], leader=(me.pid, 5)), set())
        self.assertNotIn(400, pool.members(ROWS, leader=(200, 20)))

    def test_ps_rows_have_no_start_time_and_are_not_checked_against_one(self):
        rows = pool.parse_ps("  200   100   200 S+   3000\n  201   200   200 S     4000\n")
        self.assertEqual(pool.members(rows, leader=(200, 12345)), {200, 201})

    def test_tree_of_walks_parent_links(self):
        self.assertEqual(pool.tree_of(ROWS, 300), {300, 301})
        self.assertEqual(pool.tree_of(ROWS, 1234), set())

    def test_exit_codes_of_signals(self):
        self.assertEqual(pool._exit_code(-9), 137)
        self.assertEqual(pool._exit_code(-15), 143)
        self.assertEqual(pool._exit_code(3), 3)

    def test_there_is_no_priority_clamp(self):
        self.assertFalse(hasattr(pool, "_wrap"))
        source = Path(pool.__file__).read_text()
        self.assertNotIn("taskpolicy -c", source.replace("`taskpolicy", ""))   # the docstring may name it; no argv does
        self.assertNotIn('"/usr/bin/nice"', source)

    def test_the_pressure_level_is_the_kernels_one_two_or_four(self):
        self.assertEqual([pool.parse_pressure(x) for x in ("1\n", "2", " 4 ")], [1, 2, 4])
        for bad in ("", "0", "3", "5", "x", "-1"):
            self.assertIsNone(pool.parse_pressure(bad), bad)
        self.assertEqual(pool.PRESSURE_NAMES, {1: "normal", 2: "warn", 4: "critical"})

    def test_the_pressure_comes_from_the_sysctl_when_the_call_fails(self):
        with mock.patch.object(pool.ctypes, "CDLL", side_effect=OSError("no libc")), \
                mock.patch.object(pool, "_sysctl", return_value="2"):
            self.assertEqual(pool.memory_pressure(), 2)
        with mock.patch.object(pool.ctypes, "CDLL", side_effect=OSError("no libc")), \
                mock.patch.object(pool, "_sysctl", side_effect=OSError("no sysctl")):
            self.assertIsNone(pool.memory_pressure())

    def test_a_process_that_cannot_be_killed_ends_the_kill_after_the_patience_not_never(self):
        stubborn = {4242}
        sent = []
        with mock.patch.object(pool, "_signal", side_effect=lambda pids, sig: sent.append((set(pids), sig)) or set()):
            started = time.monotonic()
            touched, stuck = pool._kill_members(lambda: set(stubborn), grace=0.2, patience=0.4)
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual((touched, stuck), ({4242}, {4242}))
        self.assertTrue(any(sig == signal.SIGKILL for _, sig in sent))

    def test_a_pid_that_refuses_the_signal_is_given_up_on_at_once(self):
        with mock.patch.object(pool, "_signal", side_effect=lambda pids, sig: set(pids)):
            started = time.monotonic()
            touched, stuck = pool._kill_members(lambda: {4242}, grace=0.2, patience=30)
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual(stuck, {4242})

    def test_signal_reports_the_pids_that_say_eperm(self):
        def kill(pid, sig):
            if pid == 2:
                raise PermissionError
            if pid == 3:
                raise ProcessLookupError
        with mock.patch.object(pool.os, "kill", side_effect=kill):
            self.assertEqual(pool._signal([1, 2, 3], signal.SIGTERM), {2})

    def test_a_group_is_signalled_only_while_a_verified_member_is_in_it(self):
        with mock.patch.object(pool.os, "killpg") as killpg, mock.patch.object(pool.os, "getpgid", return_value=999999):
            pool._killpg(4242, {1, 2}, signal.SIGTERM)
            killpg.assert_not_called()
        with mock.patch.object(pool.os, "killpg") as killpg, mock.patch.object(pool.os, "getpgid", return_value=4242):
            pool._killpg(4242, {1}, signal.SIGTERM)
            killpg.assert_called_once_with(4242, signal.SIGTERM)


def descriptor(tmp, argv, **kw):
    d = dict(version=1, id="unit", nonce="0123456789abcdef0123456789abcdef", argv=argv,
             cwd=str(Path(tmp).resolve()), env={}, log_path=str(Path(tmp, "log.txt").resolve()),
             timeout_seconds=20, cpu_slots=1, memory_max_gb=2,
             legacy_lock_path=str(Path(tmp, "heavy.lock").resolve()),
             result_path=str(Path(tmp, "result.json").resolve()), cancel_path=str(Path(tmp, "cancel").resolve()))
    d.update(kw)
    return d


def gone(pid):
    info = pool._bsd(pid)
    return info is None or info[0] == pool._SZOMB


def wait_for(predicate, seconds=10):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    return predicate()


GRANDCHILD = ("import os,subprocess,sys,time\n"
              "p=sys.argv[1]\n"
              "code=\"import os,time;open(%r,'w').write(str(os.getpid()));time.sleep(120)\" % (p,)\n"
              "subprocess.Popen([sys.executable,'-c',code],start_new_session=True)\n"
              "time.sleep(120)\n")


@unittest.skipUnless(DARWIN, "macOS processes")
class NativeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.path = self.dir / "descriptor.json"
        self.pids = []
        self.addCleanup(self.sweep)
        patcher = mock.patch.object(pool, "MEM_POLL_S", 0.2)
        patcher.start()
        self.addCleanup(patcher.stop)
        grace = mock.patch.object(pool, "KILL_GRACE_S", 2.0)
        grace.start()
        self.addCleanup(grace.stop)

    def sweep(self):
        for pid in self.pids:
            with_tree = pool.tree_of(pool.snapshot(0), pid) if not gone(pid) else set()
            for p in with_tree:
                try:
                    os.kill(p, signal.SIGKILL)
                except OSError:
                    pass

    def prepare(self, argv, **kw):
        self.d = descriptor(self.dir, argv, **kw)
        self.path.write_text(json.dumps(self.d), encoding="utf-8")
        return self.d

    def result(self):
        try:
            return json.loads(Path(self.d["result_path"]).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def start(self):
        """Run the descriptor in a thread of this process; returns the thread and the dict that gets the final state."""
        box = {}
        thread = threading.Thread(target=lambda: box.update(state=pool.run(str(self.path))), daemon=True)
        thread.start()
        self.addCleanup(thread.join, 30)
        return thread, box

    def test_a_job_that_succeeds_and_one_that_fails(self):
        self.prepare([sys.executable, "-c", "print('hello')"])
        state = pool.run(str(self.path))
        self.assertEqual((state["phase"], state["exit"], state["active_processes"]), ("done", 0, 0))
        self.assertIn("hello", Path(self.d["log_path"]).read_text())
        self.assertEqual(state["worker_pid"], os.getpid())
        self.assertEqual(state["worker_start_filetime"], pool.process_identity(os.getpid()))
        claim = pool.read_claim(str(self.path))
        self.assertEqual(claim["worker_pid"], os.getpid())
        with tempfile.TemporaryDirectory() as other:
            self.dir = Path(other)
            self.path = self.dir / "descriptor.json"
            self.prepare([sys.executable, "-c", "import sys;sys.exit(3)"])
            state = pool.run(str(self.path))
            self.assertEqual((state["phase"], state["exit"]), ("failed", 3))

    def test_the_job_runs_in_its_own_session_at_low_priority(self):
        self.prepare([sys.executable, "-c", "import os,time;print(os.getsid(0)==os.getpid(), os.getpgrp()==os.getpid());time.sleep(0.3)"])
        state = pool.run(str(self.path))
        self.assertIn("True True", Path(self.d["log_path"]).read_text())
        self.assertEqual(state["job_pgid"], state["pid"])

    def test_cancel_kills_the_tree_including_a_setsid_grandchild(self):
        marker = self.dir / "grandchild.pid"
        self.prepare([sys.executable, "-c", GRANDCHILD, str(marker)])
        thread, box = self.start()
        pid = wait_for(lambda: int(marker.read_text()) if marker.exists() and marker.read_text() else 0)
        self.assertTrue(pid)
        self.pids.append(pid)
        leader = self.result()["pid"]
        info = pool._bsd(pid)
        self.assertNotEqual(os.getpgid(pid), leader, "the grandchild left the job's process group")
        Path(self.d["cancel_path"]).touch()
        thread.join(30)
        self.assertFalse(thread.is_alive())
        self.assertEqual(box["state"]["phase"], "cancelled")
        self.assertTrue(wait_for(lambda: gone(pid)), "the setsid grandchild is dead")
        self.assertTrue(wait_for(lambda: gone(leader)))
        self.assertIsNotNone(info)

    def test_timeout_kills_the_tree_and_exits_124(self):
        marker = self.dir / "grandchild.pid"
        self.prepare([sys.executable, "-c", GRANDCHILD, str(marker)], timeout_seconds=1.5)
        started = time.monotonic()
        state = pool.run(str(self.path))
        self.assertLess(time.monotonic() - started, 15)
        self.assertEqual((state["phase"], state["exit"]), ("timeout", 124))
        self.assertIn("timeout", state["reason"])
        pid = int(marker.read_text())
        self.assertTrue(wait_for(lambda: gone(pid)))

    ALLOCATE = "b=bytearray(400*1024*1024)\nfor i in range(0,len(b),4096): b[i]=1\nimport time;time.sleep(%s)\n"

    def test_a_tree_over_its_declared_cap_is_killed_with_exit_137_under_memory_pressure(self):
        for level in (2, 4):
            with self.subTest(level=level), tempfile.TemporaryDirectory() as other:
                self.dir = Path(other)
                self.path = self.dir / "descriptor.json"
                self.prepare([sys.executable, "-c", self.ALLOCATE % 60], memory_max_gb=0.15)
                with mock.patch.object(pool, "memory_pressure", return_value=level):
                    state = pool.run(str(self.path))
                self.assertEqual((state["phase"], state["exit"]), ("failed", 137))
                self.assertIn("memory cap exceeded under memory pressure (%s)" % pool.PRESSURE_NAMES[level], state["reason"])
                self.assertIn("0.15", state["reason"])
                self.assertGreater(state["peak_private_bytes"], 0.15 * 2 ** 30)

    def test_at_normal_pressure_a_job_may_exceed_its_cap_and_the_log_says_so_once(self):
        self.prepare([sys.executable, "-c", self.ALLOCATE % 1.5], memory_max_gb=0.15)
        with mock.patch.object(pool, "memory_pressure", return_value=1):
            state = pool.run(str(self.path))
        self.assertEqual((state["phase"], state["exit"]), ("done", 0))
        log = Path(self.d["log_path"]).read_text()
        self.assertEqual(log.count("enforced only under memory pressure"), 1)
        self.assertIn("declared cap of 0.15 GB", log)
        self.assertGreater(state["peak_private_bytes"], 0.15 * 2 ** 30)

    def test_a_cap_the_submitter_did_not_state_is_never_enforced(self):
        self.prepare([sys.executable, "-c", self.ALLOCATE % 1.5], memory_max_gb=0.15, memory_cap_declared=False)
        with mock.patch.object(pool, "memory_pressure", return_value=4):     # even critical: the kernel's job
            state = pool.run(str(self.path))
        self.assertEqual((state["phase"], state["exit"]), ("done", 0))
        self.assertNotIn("enforced only", Path(self.d["log_path"]).read_text())

    def test_pressure_that_turns_up_later_enforces_the_cap(self):
        levels = iter([1, 1, 4, 4, 4, 4, 4, 4])
        self.prepare([sys.executable, "-c", self.ALLOCATE % 60], memory_max_gb=0.15)
        with mock.patch.object(pool, "memory_pressure", side_effect=lambda: next(levels, 4)):
            state = pool.run(str(self.path))
        self.assertEqual((state["phase"], state["exit"]), ("failed", 137))

    def test_the_job_is_over_when_its_tree_is_empty_not_when_its_main_process_exits(self):
        # as for a Job Object: a process left running keeps the job running, and its exit ends it
        marker = self.dir / "straggler.pid"
        code = ("import subprocess,sys;p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(1.5)'],start_new_session=True);"
                "open(%r,'w').write(str(p.pid))" % str(marker))
        self.prepare([sys.executable, "-c", code])
        started = time.monotonic()
        state = pool.run(str(self.path))
        self.assertEqual((state["phase"], state["exit"]), ("done", 0))
        self.assertGreater(time.monotonic() - started, 1.2, "it waited for the background process")
        self.assertTrue(gone(int(marker.read_text())))

    def test_a_process_left_running_past_the_timeout_ends_the_job_with_124_and_is_killed(self):
        marker = self.dir / "straggler.pid"
        code = ("import subprocess,sys;p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(120)'],start_new_session=True);"
                "open(%r,'w').write(str(p.pid))" % str(marker))
        self.prepare([sys.executable, "-c", code], timeout_seconds=1.5)
        state = pool.run(str(self.path))
        self.assertEqual((state["phase"], state["exit"]), ("timeout", 124))
        self.assertTrue(wait_for(lambda: gone(int(marker.read_text()))))

    def test_a_setsid_apple_binary_forked_and_orphaned_at_once_is_still_in_the_tree(self):
        # the reviewer's case: /bin/sleep is a platform binary (ps shows no environment) and its parent exits at once
        for attempt in range(5):
            with self.subTest(attempt=attempt):
                with tempfile.TemporaryDirectory() as other:
                    self.dir = Path(other)
                    self.path = self.dir / "descriptor.json"
                    marker = self.dir / "sleep.pid"
                    code = ("import subprocess;p=subprocess.Popen(['/bin/sleep','60'],start_new_session=True);"
                            "open(%r,'w').write(str(p.pid))" % str(marker))
                    self.prepare([sys.executable, "-c", code], timeout_seconds=6)
                    thread, box = self.start()
                    pid = wait_for(lambda: int(marker.read_text()) if marker.exists() and marker.read_text() else 0)
                    self.assertTrue(pid)
                    self.pids.append(pid)
                    # the main process is gone; the job is not over, observe sees the sleep and does not say empty
                    time.sleep(0.5)
                    seen = pool.observe(str(self.path))
                    self.assertEqual(seen["job_exists"], True, seen)
                    self.assertGreaterEqual(seen["active_processes"], 1)
                    self.assertFalse(seen["empty_confirmed"])
                    self.assertFalse(gone(pid))
                    self.assertEqual(self.result()["phase"], "running")
                    Path(self.d["cancel_path"]).touch()
                    thread.join(30)
                    self.assertEqual(box["state"]["phase"], "cancelled")
                    self.assertTrue(wait_for(lambda: gone(pid)), "the sleep is dead after the job")

    def test_an_unkillable_member_ends_the_job_failed_and_unconfirmed_not_never(self):
        self.prepare([sys.executable, "-c", "import time;time.sleep(60)"])
        with mock.patch.object(pool, "KILL_GRACE_S", 0.3), mock.patch.object(pool, "KILL_PATIENCE_S", 0.5), \
                mock.patch.object(pool, "_signal", side_effect=lambda pids, sig: set(pids)), \
                mock.patch.object(pool.os, "killpg"):     # every signal says EPERM
            thread, box = self.start()
            leader = wait_for(lambda: self.result().get("pid"))
            self.assertTrue(leader)
            self.pids.append(leader)
            Path(self.d["cancel_path"]).touch()
            thread.join(20)
        self.assertFalse(thread.is_alive())
        state = box["state"]
        self.assertEqual(state["phase"], "failed")
        self.assertIn("shutdown unconfirmed: pids", state["reason"])
        self.assertIn(str(leader), state["reason"])
        self.assertGreaterEqual(state["active_processes"], 1)

    def test_no_launch_without_the_guard(self):
        marker = self.dir / "ran"
        self.prepare([sys.executable, "-c", "open(%r,'w').close()" % str(marker)])
        with mock.patch.object(pool.sys, "executable", "/nonexistent/python"):
            state = pool.run(str(self.path))
        self.assertEqual(state["phase"], "failed")
        self.assertIn("launch refused", state["reason"])
        self.assertFalse(marker.exists())
        self.assertIsNone(self.result().get("pid"))

    def test_the_guard_leaves_at_once_when_the_job_never_starts(self):
        self.prepare([sys.executable, "-c", "pass"])
        g = pool._start_guard(str(self.path), os.getpid(), pool.process_identity(os.getpid()))
        g.stdin.close()
        self.assertEqual(g.wait(10), 0)
        g.stdout.close()

    def test_the_tree_is_recorded_beside_the_result_with_start_times(self):
        marker = self.dir / "grandchild.pid"
        self.prepare([sys.executable, "-c", GRANDCHILD, str(marker)])
        thread, box = self.start()
        pid = wait_for(lambda: int(marker.read_text()) if marker.exists() and marker.read_text() else 0)
        self.assertTrue(pid)
        self.pids.append(pid)
        leader = wait_for(lambda: pool.read_tree(self.d)[0])
        self.assertEqual(leader, (self.result()["pid"], self.result()["process_start_filetime"]))
        self.assertTrue(wait_for(lambda: (pid, pool.process_identity(pid)) in pool.read_tree(self.d)[1]), "the setsid grandchild is recorded")
        Path(self.d["cancel_path"]).touch()
        thread.join(30)
        self.assertEqual(pool.read_tree(self.d)[1], [])

    def test_a_group_of_a_reused_job_pid_is_not_counted(self):
        # the job is over; its pid is now another process leading its own group: observe must be able to confirm empty
        self.prepare([sys.executable, "-c", "pass"])
        runner = subprocess.run([sys.executable, str(Path(pool.__file__)), str(self.path)], timeout=60,
                                stdin=subprocess.DEVNULL, capture_output=True)
        self.assertEqual(runner.returncode, 0)
        result = self.result()
        impostor = subprocess.Popen(["sleep", "30"], start_new_session=True)
        self.pids.append(impostor.pid)
        # make the recorded leader the impostor's pid with the old start time
        state = dict(result, pid=impostor.pid, job_pgid=impostor.pid)
        Path(self.d["result_path"]).write_text(json.dumps(state))
        seen = pool.observe(str(self.path))
        self.assertEqual((seen["job_exists"], seen["empty_confirmed"]), (False, True))
        impostor.kill()
        impostor.wait()

    def test_caffeinate_holds_the_mac_awake_for_the_job(self):
        self.prepare([sys.executable, "-c", "import time;time.sleep(60)"])
        thread, box = self.start()
        leader = wait_for(lambda: self.result().get("pid"))
        self.assertTrue(leader)
        self.pids.append(leader)
        needle = "-i -w %d" % leader
        def caffeinated():
            lines = subprocess.run(["ps", "-axo", "command="], capture_output=True, text=True).stdout.splitlines()
            return [l for l in lines if l.split(" ")[0].endswith("caffeinate") and needle in l]
        self.assertTrue(wait_for(caffeinated), "caffeinate -i -w <job pid> is running")
        Path(self.d["cancel_path"]).touch()
        thread.join(30)
        self.assertTrue(wait_for(lambda: not caffeinated()))

    def test_a_dead_runner_leaves_nothing_and_observe_proves_it(self):
        marker = self.dir / "grandchild.pid"
        self.prepare([sys.executable, "-c", GRANDCHILD, str(marker)])
        runner = subprocess.Popen([sys.executable, str(Path(pool.__file__)), str(self.path)], stdin=subprocess.DEVNULL,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        self.pids.append(runner.pid)
        pid = wait_for(lambda: int(marker.read_text()) if marker.exists() and marker.read_text() else 0)
        self.assertTrue(pid)
        self.pids.append(pid)
        leader = self.result()["pid"]
        seen = pool.observe(str(self.path))
        self.assertEqual((seen["worker_alive"], seen["job_exists"], seen["empty_confirmed"]), (True, True, False))
        self.assertGreaterEqual(seen["active_processes"], 2)
        runner.kill()
        runner.wait(10)
        # the guard, a separate process, kills the tree of a job whose runner died
        self.assertTrue(wait_for(lambda: gone(pid) and gone(leader), 15), "the guard killed the orphaned tree")
        after = pool.observe(str(self.path))
        self.assertEqual((after["worker_alive"], after["job_exists"], after["active_processes"], after["empty_confirmed"]),
                         (False, False, 0, True))

    def test_a_finished_job_is_confirmed_empty_by_a_later_observer(self):
        self.prepare([sys.executable, "-c", "pass"])
        runner = subprocess.run([sys.executable, str(Path(pool.__file__)), str(self.path)], timeout=60,
                                stdin=subprocess.DEVNULL, capture_output=True)
        self.assertEqual(runner.returncode, 0)
        seen = pool.observe(str(self.path))
        self.assertEqual((seen["worker_alive"], seen["empty_confirmed"]), (False, True))
        self.assertEqual(self.result()["phase"], "done")
        # an identity that is not the claimed worker's is unknown, never empty
        self.assertFalse(pool.observe(str(self.path), os.getpid(), 1)["empty_confirmed"])

    def test_the_heavy_lock_is_shared_between_jobs_and_excludes_an_exclusive_holder(self):
        lock = str(self.dir / "heavy.lock")
        first, second = pool.SharedLock(lock), pool.SharedLock(lock)
        self.assertFalse(pool.heavy_lock_held(lock))
        self.assertTrue(first.try_acquire() and second.try_acquire())
        self.assertTrue(pool.heavy_lock_held(lock))
        exclusive = pool.ExclusiveLock(lock)
        self.assertFalse(exclusive.try_acquire())
        first.close()
        second.close()
        self.assertTrue(exclusive.try_acquire())
        self.assertFalse(pool.SharedLock(lock).try_acquire())
        exclusive.close()
        self.assertFalse(pool.heavy_lock_held(lock))

    def test_a_busy_heavy_lock_holds_the_job_back_and_cancel_ends_the_wait(self):
        self.prepare([sys.executable, "-c", "pass"])
        holder = pool.ExclusiveLock(self.d["legacy_lock_path"])
        self.assertTrue(holder.try_acquire())
        self.addCleanup(holder.close)
        thread, box = self.start()
        self.assertTrue(wait_for(lambda: self.result().get("reason") == "legacy heavy lock busy"))
        Path(self.d["cancel_path"]).touch()
        thread.join(10)
        self.assertEqual(box["state"]["phase"], "cancelled")
        self.assertIsNone(self.result().get("pid"))

    def test_the_mac_reserves_no_memory_so_a_floor_in_the_descriptor_refuses_nothing(self):
        marker = self.dir / "ran"
        self.prepare([sys.executable, "-c", "open(%r,'w').close()" % str(marker)], ram_floor_gb=10 ** 6, pending_reserved_gb=10 ** 6)
        state = pool.run(str(self.path))
        self.assertEqual((state["phase"], state["exit"]), ("done", 0))
        self.assertTrue(marker.exists())

    def test_the_job_runs_at_normal_priority(self):
        self.prepare([sys.executable, "-c", "import os;print('nice', os.nice(0))"])
        pool.run(str(self.path))
        self.assertIn("nice 0", Path(self.d["log_path"]).read_text())

    def test_probe_and_identity_on_this_machine(self):
        p = pool.probe()
        self.assertTrue(p["supported"])
        self.assertIn("macos", p["capabilities"])
        self.assertGreater(p["total_gb"], 0)
        self.assertEqual(p["available_gb"], p["commit_free_gb"])
        self.assertLessEqual(p["available_gb"], p["total_gb"])
        self.assertIn(p["on_battery"], (True, False, None))
        self.assertIn(p["pressure"], (1, 2, 4, None))
        self.assertEqual(p["boot_identity"], pool.boot_identity())
        self.assertGreater(pool.process_identity(os.getpid()), 0)
        self.assertIs(pool._worker_alive(os.getpid(), pool.process_identity(os.getpid())), True)
        self.assertIs(pool._worker_alive(os.getpid(), 1), False)  # the same PID with another start time is another process

    def test_a_zombie_worker_is_not_alive(self):
        child = subprocess.Popen(["sh", "-c", "exit 0"])
        start = pool.process_identity(child.pid) or 1
        deadline = time.monotonic() + 10
        while pool._info(child.pid) is not None and time.monotonic() < deadline and pool._worker_alive(child.pid, start) is not False:
            time.sleep(0.05)
        # not reaped yet: a zombie, which libproc may not even read
        self.assertIs(pool._worker_alive(child.pid, start), False)
        child.wait()

    def test_kill_tree_takes_a_process_and_its_descendants(self):
        child = subprocess.Popen(["sh", "-c", "sleep 120 & sleep 120"], start_new_session=True)
        self.pids.append(child.pid)
        self.assertTrue(wait_for(lambda: len(pool.tree_of(pool.snapshot(0), child.pid)) >= 3))
        tree = pool.tree_of(pool.snapshot(0), child.pid)
        pool.kill_tree(child.pid, grace=2)
        child.wait(10)
        self.assertTrue(all(wait_for(lambda p=p: gone(p)) for p in tree))


@unittest.skipUnless(DARWIN, "macOS processes")
class BrokerTests(unittest.TestCase):
    """mac_broker.LocalPool over mac_pool, as the controller drives it for a generation-two queue."""

    def test_a_pooled_job_runs_and_is_proved_empty_by_its_own_pool(self):
        import mac_broker
        with tempfile.TemporaryDirectory() as tmp:
            d = descriptor(tmp, [sys.executable, "-c", "print('pooled')"], boot_identity=pool.boot_identity(),
                           ram_floor_gb=1, pending_reserved_gb=0)
            path = Path(tmp, "descriptor.json")
            path.write_text(json.dumps(d), encoding="utf-8")
            broker = mac_broker.LocalPool(d["legacy_lock_path"], max_cpu_slots=2, cohort_seconds=5, quiet_seconds=0.1)
            receipt = broker.submit(str(path))
            self.assertEqual(receipt["pid"], os.getpid())
            self.assertEqual(receipt["process_start_identity"], pool.process_identity(os.getpid()))
            self.assertTrue(broker.gate_held)
            result = Path(d["result_path"])
            self.assertTrue(wait_for(lambda: result.exists() and json.loads(result.read_text()).get("phase") == "done", 20))
            self.assertTrue(wait_for(lambda: broker.observe(str(path), receipt["pid"], receipt["process_start_identity"])
                                     .get("pool_completion_proof")))
            seen = broker.observe(str(path), receipt["pid"], receipt["process_start_identity"])
            self.assertTrue(seen["empty_confirmed"])
            self.assertEqual(seen["pool_completion_proof"]["worker_start_identity"], receipt["process_start_identity"])
            self.assertIn("pooled", Path(d["log_path"]).read_text())
            broker.close()
            self.assertFalse(broker.gate_held)


if __name__ == "__main__":
    unittest.main()
