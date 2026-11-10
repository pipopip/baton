import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

PATH = Path(__file__).with_name("windows_pool.py")
spec = importlib.util.spec_from_file_location("windows_pool", PATH)
pool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pool)


class ValidationTests(unittest.TestCase):
    def test_probe_is_complete_or_explicitly_unsupported(self):
        p = pool.probe()
        self.assertEqual(set(p), {"supported", "logical_cpus", "available_gb", "commit_free_gb", "total_gb", "capabilities", "boot_identity"})
        if os.name == "nt":
            self.assertTrue(p["supported"])
            self.assertGreater(p["logical_cpus"], 0)
            self.assertGreater(p["total_gb"], 0)
            self.assertIn("windows", p["capabilities"])
            self.assertEqual(p["boot_identity"], pool.boot_identity())

    @unittest.skipUnless(os.name == "nt", "Windows probe")
    def test_capabilities_are_path_allowlist_only(self):
        with mock.patch.object(pool.shutil, "which", side_effect=lambda name: "C:/bin/" + name if name in ("python3", "go") else None):
            self.assertEqual(pool.probe()["capabilities"], ["windows", "python", "go"])

    def test_limits_and_environment_are_validated(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = descriptor(tmp, [sys.executable, "-c", "pass"])
            pool._descriptor(d)
            for key, value in (("memory_max_gb", float("nan")), ("cpu_slots", 0), ("timeout_seconds", True),
                               ("nonce", "short"), ("env", {"X=Y": "z"})):
                bad = dict(d, **{key: value})
                with self.subTest(key=key), self.assertRaises(ValueError):
                    pool._descriptor(bad)
            with self.assertRaises(ValueError):
                pool._descriptor(dict(d, ram_floor_gb=1))
            with self.assertRaises(ValueError):
                pool._descriptor(dict(d, boot_identity="not-a-guid"))

    @unittest.skipUnless(os.name == "nt", "Windows boot identity")
    def test_boot_mismatch_or_unknown_never_claims_or_launches(self):
        with tempfile.TemporaryDirectory() as tmp:
            marker = str(Path(tmp, "must-not-appear"))
            d = descriptor(tmp, [sys.executable, "-c", "open(%r,'w').close()" % marker])
            d["boot_identity"] = "11111111-1111-1111-1111-111111111111"
            path = Path(tmp, "descriptor.json")
            path.write_text(json.dumps(d), encoding="utf-8")
            for observed in (None, "22222222-2222-2222-2222-222222222222"):
                with self.subTest(observed=observed), mock.patch.object(pool, "boot_identity", return_value=observed):
                    with self.assertRaises(OSError):
                        pool.run(str(path))
                self.assertFalse(Path(pool.claim_path(str(path))).exists())
                self.assertFalse(Path(d["result_path"]).exists())
                self.assertFalse(Path(marker).exists())

    @unittest.skipUnless(os.name == "nt", "Windows Job Object API")
    def test_failure_setting_limit_closes_job(self):
        fake = mock.Mock()
        fake.CreateJobObjectW.return_value = 123
        fake.SetInformationJobObject.return_value = 0
        with mock.patch.object(pool, "K", fake), mock.patch.object(pool.ctypes, "get_last_error", return_value=5):
            with self.assertRaises(OSError):
                pool.Job("test", 1, 2, 1)
        fake.CloseHandle.assert_called_once_with(123)

    def test_claim_binding_and_incomplete_claim_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = descriptor(tmp, [sys.executable, "-c", "pass"])
            path = Path(tmp, "descriptor.json")
            path.write_text(json.dumps(d), encoding="utf-8")
            pool._claim(str(path), d, 123, 456)
            claim = pool.read_claim(str(path))
            self.assertEqual((claim["worker_pid"], claim["worker_start_filetime"]), (123, 456))
            with self.assertRaises(FileExistsError):
                pool._claim(str(path), d, 124, 457)
            d["timeout_seconds"] += 1
            path.write_text(json.dumps(d), encoding="utf-8")
            self.assertIsNone(pool.read_claim(str(path)))
            Path(pool.claim_path(str(path))).write_text('{"version":', encoding="utf-8")
            self.assertIsNone(pool.read_claim(str(path)))
            with self.assertRaises(FileExistsError):
                pool._claim(str(path), d, 124, 457)

    def test_claim_and_descriptor_cannot_be_result_or_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "descriptor.json")
            for field, forbidden in (("result_path", path), ("log_path", Path(tmp, "worker-claim.json"))):
                d = descriptor(tmp, [sys.executable, "-c", "pass"])
                d[field] = str(forbidden)
                path.write_text(json.dumps(d), encoding="utf-8")
                with self.assertRaises(ValueError):
                    pool._load_descriptor(str(path))


def descriptor(tmp, argv):
    return dict(version=1, id="unit", nonce="0123456789abcdef0123456789abcdef", argv=argv,
                cwd=str(Path(tmp).resolve()), env={}, log_path=str(Path(tmp, "log.txt").resolve()),
                timeout_seconds=5, cpu_slots=1, memory_max_gb=0.5,
                legacy_lock_path=str(Path(tmp, "heavy.lock").resolve()),
                result_path=str(Path(tmp, "result.json").resolve()), cancel_path=str(Path(tmp, "cancel").resolve()))


@unittest.skipUnless(os.name == "nt", "Windows Job Objects")
class NativeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.d = descriptor(self.tmp.name, [sys.executable, "-c", "pass"])
        self.desc_path = Path(self.tmp.name, "descriptor.json")
        self.proc = None
        self.addCleanup(self.stop_worker)

    def stop_worker(self):
        if self.proc and self.proc.poll() is None:
            Path(self.d["cancel_path"]).touch()
            try:
                self.proc.wait(timeout=6)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=3)

    def start(self):
        self.desc_path.write_text(json.dumps(self.d), encoding="utf-8")
        self.proc = subprocess.Popen([sys.executable, str(PATH), str(self.desc_path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=0x08000000)

    def result(self):
        try:
            return json.loads(Path(self.d["result_path"]).read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def wait_phase(self, phases, timeout=8):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            r = self.result()
            if r.get("phase") in phases:
                return r
            if self.proc.poll() is not None:
                self.fail(f"worker exited unexpectedly: {self.proc.communicate()} {r}")
            time.sleep(.05)
        self.fail(f"worker timed out; result {self.result()}")

    def completed(self):
        out, err = self.proc.communicate(timeout=8)
        self.assertTrue(Path(self.d["result_path"]).exists(), (out, err))
        return self.result()

    def test_child_success_and_early_parent_waits_for_grandchild(self):
        marker = str(Path(self.tmp.name, "grandchild-finished"))
        child = "import time,pathlib;time.sleep(.6);pathlib.Path(%r).touch()" % marker
        parent = "import subprocess,sys;subprocess.Popen([sys.executable,'-c',%r])" % child
        self.d["argv"] = [sys.executable, "-c", parent]
        self.start()
        r = self.completed()
        self.assertEqual(r["phase"], "done", r)
        self.assertTrue(Path(marker).exists())
        self.assertEqual(r["active_processes"], 0)
        self.assertGreater(r["peak_private_bytes"], 0)

    def test_cancel_kills_child_and_grandchild_before_done(self):
        marker = str(Path(self.tmp.name, "must-not-appear"))
        ready = str(Path(self.tmp.name, "grandchild-started"))
        child = "import time,pathlib;pathlib.Path(%r).touch();time.sleep(1.5);pathlib.Path(%r).touch()" % (ready, marker)
        parent = "import subprocess,sys,time;subprocess.Popen([sys.executable,'-c',%r]);time.sleep(3)" % child
        self.d["argv"] = [sys.executable, "-c", parent]
        self.start()
        self.wait_phase({"running"})
        deadline = time.monotonic() + 4
        while not Path(ready).exists() and time.monotonic() < deadline:
            time.sleep(.03)
        self.assertTrue(Path(ready).exists(), self.result())
        Path(self.d["cancel_path"]).touch()
        r = self.completed()
        self.assertEqual(r["phase"], "cancelled", r)
        self.assertEqual(r["active_processes"], 0)
        time.sleep(1.6)
        self.assertFalse(Path(marker).exists())

    def test_timeout_terminates_job(self):
        self.d.update(argv=[sys.executable, "-c", "import time;time.sleep(3)"], timeout_seconds=.4)
        self.start()
        r = self.completed()
        self.assertEqual(r["phase"], "timeout", r)
        self.assertEqual(r["active_processes"], 0)

    def test_legacy_exclusive_lock_blocks_until_released(self):
        import msvcrt
        with open(self.d["legacy_lock_path"], "w+b", buffering=0) as f:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            self.start()
            end = time.monotonic() + 4
            while time.monotonic() < end and self.result().get("reason") != "legacy heavy lock busy":
                time.sleep(.05)
            waiting = self.result()
            self.assertEqual(waiting.get("reason"), "legacy heavy lock busy", waiting)
            self.assertIsNone(waiting["pid"])
            observed = pool.observe(str(self.desc_path))
            self.assertEqual(observed["worker_alive"], True)
            self.assertEqual(observed["job_exists"], False)
            self.assertFalse(observed["empty_confirmed"])
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        r = self.completed()
        self.assertEqual(r["phase"], "done", r)

    def test_shared_locks_coexist_and_block_exclusive_legacy_lock(self):
        import msvcrt
        pool._setup()
        first = pool.SharedLock(self.d["legacy_lock_path"])
        second = pool.SharedLock(self.d["legacy_lock_path"])
        try:
            self.assertTrue(first.try_acquire())
            self.assertTrue(second.try_acquire())
            with open(self.d["legacy_lock_path"], "r+b", buffering=0) as f:
                with self.assertRaises(OSError):
                    msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        finally:
            second.close()
            first.close()

    def test_impossible_headroom_fails_before_start(self):
        marker = str(Path(self.tmp.name, "must-not-appear"))
        self.d.update(argv=[sys.executable, "-c", "open(%r,'w').close()" % marker], ram_floor_gb=1e9, pending_reserved_gb=1)
        self.start()
        r = self.completed()
        self.assertEqual(r["phase"], "failed", r)
        self.assertFalse(Path(marker).exists())

    def test_tiny_memory_limit_stops_payload(self):
        marker = str(Path(self.tmp.name, "must-not-appear"))
        self.d.update(argv=[sys.executable, "-c", "open(%r,'w').close()" % marker], memory_max_gb=.000001)
        self.start()
        r = self.completed()
        self.assertEqual(r["phase"], "failed", r)
        self.assertEqual(r["active_processes"], 0)
        self.assertFalse(Path(marker).exists())

    def test_observe_worker_crash_drains_job(self):
        ready = str(Path(self.tmp.name, "child-ready"))
        marker = str(Path(self.tmp.name, "must-not-appear"))
        script = "import pathlib,time;pathlib.Path(%r).touch();time.sleep(2);pathlib.Path(%r).touch()" % (ready, marker)
        self.d["argv"] = [sys.executable, "-c", script]
        self.start()
        running = self.wait_phase({"running"})
        end = time.monotonic() + 3
        while not Path(ready).exists() and time.monotonic() < end:
            time.sleep(.03)
        self.assertTrue(Path(ready).exists())
        self.assertEqual(running["worker_pid"], self.proc.pid)
        self.assertEqual(running["worker_start_filetime"], pool.process_identity(self.proc.pid))
        observed = pool.observe(str(self.desc_path))
        self.assertTrue(observed["worker_alive"], observed)
        self.assertTrue(observed["job_exists"], observed)
        self.assertGreaterEqual(observed["active_processes"], 1)
        self.assertFalse(observed["empty_confirmed"])
        self.proc.kill()  # only this test's Popen-owned worker; Job Object kills its child
        self.proc.communicate(timeout=5)
        end = time.monotonic() + 5
        while time.monotonic() < end:
            observed = pool.observe(str(self.desc_path))
            if observed["empty_confirmed"]:
                break
            time.sleep(.05)
        self.assertTrue(observed["empty_confirmed"], observed)
        self.assertFalse(observed["worker_alive"])
        time.sleep(2.1)
        self.assertFalse(Path(marker).exists())

    def test_observe_identity_missing_and_pid_reuse(self):
        self.desc_path.write_text(json.dumps(self.d), encoding="utf-8")
        unknown = pool.observe(str(self.desc_path))
        self.assertIsNone(unknown["worker_alive"])
        self.assertFalse(unknown["empty_confirmed"])
        creation = pool.process_identity(os.getpid())
        self.assertIsNotNone(creation)
        before_result = pool.observe(str(self.desc_path), os.getpid(), creation)
        self.assertTrue(before_result["worker_alive"], before_result)
        self.assertFalse(before_result["empty_confirmed"])
        observed = pool.observe(str(self.desc_path), os.getpid(), creation + 1)
        self.assertFalse(observed["worker_alive"], observed)
        self.assertFalse(observed["empty_confirmed"], observed)  # another launcher may still claim

    def test_observe_finished_worker_and_conflicting_identity(self):
        self.start()
        r = self.completed()
        self.assertEqual(r["phase"], "done", r)
        observed = pool.observe(str(self.desc_path))
        self.assertFalse(observed["worker_alive"], observed)
        self.assertTrue(observed["empty_confirmed"], observed)
        conflicting = pool.observe(str(self.desc_path), r["worker_pid"], r["worker_start_filetime"] + 1)
        self.assertIsNone(conflicting["worker_alive"])
        self.assertFalse(conflicting["empty_confirmed"])

    def test_racing_workers_execute_only_once_and_duplicate_preserves_result(self):
        marker = str(Path(self.tmp.name, "executions"))
        self.d["argv"] = [sys.executable, "-c", "import time;open(%r,'a').write('x');time.sleep(.3)" % marker]
        self.desc_path.write_text(json.dumps(self.d), encoding="utf-8")
        one = subprocess.Popen([sys.executable, str(PATH), str(self.desc_path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=0x08000000)
        two = subprocess.Popen([sys.executable, str(PATH), str(self.desc_path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=0x08000000)
        try:
            one.communicate(timeout=8)
            two.communicate(timeout=8)
        finally:
            for proc in (one, two):
                if proc.poll() is None:
                    proc.kill()
                    proc.communicate(timeout=3)
        self.assertEqual(sorted((one.returncode, two.returncode)), [0, 1])
        self.assertEqual(Path(marker).read_text(), "x")
        claim = pool.read_claim(str(self.desc_path))
        self.assertIn(claim["worker_pid"], (one.pid, two.pid))
        result = Path(self.d["result_path"]).read_bytes()
        again = subprocess.run([sys.executable, str(PATH), str(self.desc_path)], capture_output=True, timeout=5, creationflags=0x08000000)
        self.assertNotEqual(again.returncode, 0)
        self.assertEqual(Path(self.d["result_path"]).read_bytes(), result)
        self.assertEqual(Path(marker).read_text(), "x")

    def test_worker_crash_before_command_cannot_execute_on_retry(self):
        import msvcrt
        marker = str(Path(self.tmp.name, "must-not-appear"))
        self.d["argv"] = [sys.executable, "-c", "open(%r,'w').close()" % marker]
        with open(self.d["legacy_lock_path"], "w+b", buffering=0) as f:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            self.start()
            end = time.monotonic() + 4
            while time.monotonic() < end and self.result().get("reason") != "legacy heavy lock busy":
                time.sleep(.05)
            self.assertEqual(self.result().get("reason"), "legacy heavy lock busy")
            claim = pool.read_claim(str(self.desc_path))
            self.assertEqual(claim["worker_pid"], self.proc.pid)
            self.proc.kill()  # only this test's owned worker, before command launch
            self.proc.communicate(timeout=5)
            Path(self.d["result_path"]).unlink()  # simulate a crash before the first result
            observed = pool.observe(str(self.desc_path))
            self.assertFalse(observed["worker_alive"], observed)
            self.assertTrue(observed["empty_confirmed"], observed)
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        retry = subprocess.run([sys.executable, str(PATH), str(self.desc_path)], capture_output=True, timeout=5, creationflags=0x08000000)
        self.assertNotEqual(retry.returncode, 0)
        self.assertFalse(Path(self.d["result_path"]).exists())
        self.assertFalse(Path(marker).exists())


if __name__ == "__main__":
    unittest.main()
