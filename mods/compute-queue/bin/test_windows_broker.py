import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import time
import unittest
from unittest import mock
import windows_broker as broker
import windows_pool as pool
from test_windows_pool import descriptor

@unittest.skipUnless(os.name == 'nt', 'Windows Job Objects')
class BrokerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.lock = str(self.base / 'heavy.lock')
        self.p = broker.LocalPool(self.lock, cohort_seconds=2, quiet_seconds=.15)
        self.addCleanup(self.p.close)

    def make(self, name, script='import time;time.sleep(.8)', **kw):
        directory = self.base / name
        directory.mkdir()
        d = descriptor(str(directory), [sys.executable, '-c', script])
        d.update(id=name, legacy_lock_path=self.lock, ram_floor_gb=0, pending_reserved_gb=0)
        d.update(kw)
        path = directory / 'descriptor.json'
        path.write_text(json.dumps(d), encoding='utf-8')
        return path, d

    def wait(self, fn):
        end = time.monotonic() + 7
        while time.monotonic() < end:
            if fn(): return
            time.sleep(.03)
        self.fail('condition timed out')

    def result(self, d):
        try: return json.loads(Path(d['result_path']).read_text(encoding='utf-8'))
        except (OSError, ValueError): return {}

    def test_legacy_shared_and_exclusive_busy_without_claim(self):
        import msvcrt
        pool._setup()
        shared = pool.SharedLock(self.lock)
        self.assertTrue(shared.try_acquire())
        path, _ = self.make('busy')
        try:
            with self.assertRaises(broker.BusyError): self.p.submit(str(path))
            self.assertFalse(Path(pool.claim_path(str(path))).exists())
        finally: shared.close()
        with open(self.lock, 'r+b', buffering=0) as f:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            with self.assertRaises(broker.BusyError): self.p.submit(str(path))
            self.assertFalse(Path(pool.claim_path(str(path))).exists())
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)

    def test_parallel_slots_drain_descendants_and_fairness(self):
        marker = str(self.base / 'descendant')
        child = "import pathlib,time;time.sleep(1);pathlib.Path(%r).touch()" % marker
        parent = "import subprocess,sys;subprocess.Popen([sys.executable,'-c',%r])" % child
        a, da = self.make('a', parent, cpu_slots=2)
        b, db = self.make('b', cpu_slots=2)
        c, dc = self.make('c')
        receipt = self.p.submit(str(a))
        self.assertEqual(receipt['pid'], os.getpid())
        self.assertEqual(pool.read_claim(str(a))['worker_start_filetime'], receipt['process_start_identity'])
        self.p.submit(str(b))
        self.assertEqual(self.p.reservations(), {'a': da['memory_max_gb'], 'b': db['memory_max_gb']})
        with self.assertRaises(broker.BusyError): self.p.submit(str(c))
        self.wait(lambda: self.result(da).get('phase') == 'running' and self.result(db).get('phase') == 'running')
        self.p.request_yield()
        self.assertTrue(self.p.gate_held)
        with self.assertRaises(broker.BusyError): self.p.submit(str(c))
        shared = pool.SharedLock(self.lock)
        try:
            self.assertFalse(shared.try_acquire())
            self.wait(lambda: not self.p.gate_held)
            self.assertTrue(Path(marker).exists())
            self.assertEqual(self.p.reservations(), {})
            self.assertTrue(shared.try_acquire())
        finally: shared.close()
        self.wait(lambda: not self.p.draining)
        self.p.submit(str(c))
        self.wait(lambda: self.result(dc).get('phase') == 'done')
        self.wait(lambda: self.p.observe(str(c))['empty_confirmed'])
        self.assertTrue(self.p.observe(str(c))['worker_alive'])
        self.assertFalse(self.p.observe(str(c), receipt['pid'], receipt['process_start_identity'] + 1)['empty_confirmed'])

    def test_timeout_cancel_and_reservations(self):
        a, da = self.make('timeout', 'import time;time.sleep(30)', timeout_seconds=.3)
        b, db = self.make('cancel', 'import time;time.sleep(30)')
        self.p.submit(str(a)); self.p.submit(str(b))
        self.wait(lambda: self.result(db).get('phase') == 'running')
        Path(db['cancel_path']).touch()
        try:
            self.wait(lambda: self.result(da).get('phase') == 'timeout' and self.result(db).get('phase') == 'cancelled')
        except AssertionError:
            self.fail('timeout/cancel states: ' + repr((self.result(da), self.result(db))))
        c, dc = self.make('memory', 'raise SystemExit(99)')
        probe = dict(supported=True, logical_cpus=os.cpu_count(), available_gb=.1, commit_free_gb=.1)
        with mock.patch.object(pool, 'probe', return_value=probe):
            self.p.submit(str(c))
            self.wait(lambda: self.result(dc).get('phase') == 'failed')
        self.assertIn('headroom', self.result(dc)['reason'])

    def test_atomic_status_retries_a_reader_without_delete_sharing(self):
        import threading
        path = self.base / 'state.json'
        path.write_text('{"phase":"old"}')
        pool._setup()
        handle = pool.K.CreateFileW(str(path), 0x80000000, 3, None, 3, 0x80, None)
        self.assertNotEqual(handle, pool.ctypes.c_void_p(-1).value)
        errors = []
        def write():
            try:
                pool._atomic(str(path), {'phase': 'new'})
            except Exception as exc:
                errors.append(exc)
        thread = threading.Thread(target=write)
        thread.start()
        try:
            time.sleep(.1)
            self.assertTrue(thread.is_alive())
            self.assertEqual(json.loads(path.read_text())['phase'], 'old')
        finally:
            pool.K.CloseHandle(handle)
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(json.loads(path.read_text())['phase'], 'new')


    def test_disjoint_external_pending_and_pool_reservations_both_count(self):
        a, da = self.make('first', 'import time;time.sleep(2)', pending_reserved_gb=.4)
        b, db = self.make('second', 'raise SystemExit(99)', pending_reserved_gb=.4)
        probe = dict(supported=True, logical_cpus=os.cpu_count(), available_gb=1.2, commit_free_gb=1.2)
        with mock.patch.object(pool, 'probe', return_value=probe):
            self.p.submit(str(a))
            self.wait(lambda: self.result(da).get('phase') == 'running')
            self.p.submit(str(b))
            self.wait(lambda: self.result(db).get('phase') == 'failed')
        self.assertIn('headroom', self.result(db)['reason'])
        Path(da['cancel_path']).touch()

    def test_partial_claim_never_replayed(self):
        path, _ = self.make('partial')
        Path(pool.claim_path(str(path))).write_text('{', encoding='utf-8')
        self.assertIsNone(pool.read_claim(str(path)))
        self.assertFalse(pool.observe(str(path))['empty_confirmed'])
        with self.assertRaises(FileExistsError):
            self.p.submit(str(path))

    def test_owned_controller_crash_kills_descendants_and_releases_gate(self):
        ready = str(self.base / 'grandchild-ready')
        marker = str(self.base / 'must-not-appear')
        child = "import pathlib,time;pathlib.Path(%r).touch();time.sleep(3);pathlib.Path(%r).touch()" % (ready, marker)
        script = "import subprocess,sys,time;subprocess.Popen([sys.executable,'-c',%r]);time.sleep(10)" % child
        path, d = self.make('crash', script, timeout_seconds=15)
        helper = ("import sys,time,windows_broker;"
                  "p=windows_broker.LocalPool(sys.argv[1]);p.submit(sys.argv[2]);time.sleep(15)")
        proc = subprocess.Popen([sys.executable, '-c', helper, self.lock, str(path)],
                                cwd=str(Path(broker.__file__).parent), creationflags=0x08000000,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        def cleanup():
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
        self.addCleanup(cleanup)
        self.wait(lambda: Path(ready).exists() or proc.poll() is not None)
        self.assertTrue(Path(ready).exists(), 'owned broker exited before descendant ready')
        claim = pool.read_claim(str(path))
        self.assertEqual(claim['worker_pid'], proc.pid)
        observed = pool.observe(str(path), proc.pid, claim['worker_start_filetime'])
        self.assertTrue(observed['worker_alive'])
        self.assertFalse(observed['empty_confirmed'])
        proc.kill()  # this Popen-owned controller only
        proc.wait(timeout=5)
        self.wait(lambda: pool.observe(str(path), proc.pid, claim['worker_start_filetime'])['empty_confirmed'])
        gate = broker._ExclusiveLock(self.lock)
        try:
            self.wait(gate.try_acquire)
        finally:
            gate.close()
        with self.assertRaises(FileExistsError):
            self.p.submit(str(path))
        time.sleep(3.1)
        self.assertFalse(Path(marker).exists())

if __name__ == '__main__': unittest.main()
