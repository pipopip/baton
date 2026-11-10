"""Controller-owned local Windows job cohort, fenced against legacy workers."""
from __future__ import annotations

import contextlib
import math
import os
import threading
import time

import windows_pool as pool


class BusyError(RuntimeError):
    """Temporary admission refusal; no launch claim was created."""


class _ExclusiveLock(pool.SharedLock):
    def try_acquire(self):
        ok = pool.K.LockFileEx(pool.msvcrt.get_osfhandle(self.fd), 3, 0, 1, 0, pool.ctypes.byref(self.ov))
        if not ok and pool.ctypes.get_last_error() not in (33, 158):
            pool._check(False, "LockFileEx")
        self.held = bool(ok)
        return self.held


class _BorrowedGate:
    def __init__(self, owner):
        self.owner = owner

    def try_acquire(self):
        return True  # owner keeps the exclusive lock until this worker returns

    def close(self):
        pass

    @contextlib.contextmanager
    def launch_guard(self):
        # Hold through probe and CreateProcess. All admitted memory is reserved,
        # even if some is already reflected in physical/commit usage.
        with self.owner._lock:
            yield sum(item[1] for item in self.owner._jobs.values())


class LocalPool:
    def __init__(self, legacy_lock_path, max_cpu_slots=4, cohort_seconds=60, quiet_seconds=1):
        for name, value in (("max_cpu_slots", max_cpu_slots), ("cohort_seconds", cohort_seconds),
                            ("quiet_seconds", quiet_seconds)):
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
                raise ValueError(name + " must be finite and positive")
        self.path = os.path.normcase(os.path.realpath(legacy_lock_path))
        self.max_cpu_slots = max_cpu_slots
        self.cohort_seconds = cohort_seconds
        self.quiet_seconds = quiet_seconds
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._jobs = {}
        self._completed = {}
        self._gate = None
        self._draining = False
        self._closed = False
        self._quiet_until = 0
        self._generation = 0

    @property
    def used_cpu_slots(self):
        with self._lock:
            return sum(item[0] for item in self._jobs.values())

    def reservations(self):
        """Snapshot full caps by ID until each admitted job has fully drained."""
        with self._lock:
            return {item[2]: item[1] for item in self._jobs.values()}

    @property
    def draining(self):
        with self._lock:
            return self._draining or time.monotonic() < self._quiet_until

    @property
    def gate_held(self):
        with self._lock:
            return self._gate is not None

    def _release_if_drained(self):
        if self._draining and not self._jobs and self._gate is not None:
            self._gate.close()
            self._gate = None
            self._draining = False
            self._quiet_until = time.monotonic() + self.quiet_seconds
            self._condition.notify_all()

    def request_yield(self):
        with self._lock:
            if self._gate is not None:
                self._draining = True
                self._release_if_drained()

    def _expire(self, generation):
        with self._lock:
            if generation == self._generation and self._gate is not None:
                self._draining = True
                self._release_if_drained()

    def submit(self, descriptor_path):
        d = pool._load_descriptor(descriptor_path)
        if os.path.normcase(os.path.realpath(d["legacy_lock_path"])) != self.path:
            raise ValueError("descriptor has a different legacy lock")
        # A declared reserve is mandatory for the pooled admission contract.
        if "ram_floor_gb" not in d:
            raise ValueError("pooled jobs require RAM floor and pending reservation")
        if d["cpu_slots"] > self.max_cpu_slots:
            raise ValueError("job CPU slots exceed pool limit")
        if os.path.lexists(pool.claim_path(descriptor_path)):
            raise FileExistsError("descriptor already claimed")
        outcome = {}
        ready = threading.Event()
        key = object()
        def claimed(pid, creation):
            outcome["claim_fingerprint"] = pool.claim_fingerprint(descriptor_path)
            outcome["receipt"] = {"pid": pid, "process_start_identity": creation}
            ready.set()
        def worker():
            try:
                pool.run(descriptor_path, gate=_BorrowedGate(self), on_claim=claimed,
                         expected_hash=pool._descriptor_hash(d))
            except BaseException as exc:
                outcome["error"] = exc
                ready.set()
            finally:
                with self._lock:
                    if "receipt" in outcome:
                        self._completed[os.path.normcase(os.path.abspath(descriptor_path))] = (
                            pool._descriptor_hash(d), outcome["receipt"], outcome["claim_fingerprint"])
                    self._jobs.pop(key, None)
                    self._release_if_drained()
                    self._condition.notify_all()
        with self._lock:
            if self._closed or self._draining or time.monotonic() < self._quiet_until:
                raise BusyError("pool draining or in quiet period")
            if d["id"] in self.reservations():
                raise BusyError("job ID already admitted")
            if self.used_cpu_slots + d["cpu_slots"] > self.max_cpu_slots:
                raise BusyError("pool CPU capacity busy")
            if self._gate is None:
                pool._setup()
                gate = _ExclusiveLock(self.path)
                try:
                    if not gate.try_acquire():
                        raise BusyError("legacy lock busy")
                except BaseException:
                    gate.close()
                    raise
                self._gate = gate
                self._generation += 1
                timer = threading.Timer(self.cohort_seconds, self._expire, args=(self._generation,))
                timer.daemon = True
                timer.start()
            self._jobs[key] = (d["cpu_slots"], d["memory_max_gb"], d["id"])
            try:
                threading.Thread(target=worker, daemon=True, name="windows-pool-job").start()
            except BaseException:
                self._jobs.pop(key)
                self._draining = True
                self._release_if_drained()
                raise
        ready.wait()
        if "receipt" in outcome:
            return outcome["receipt"]
        raise outcome["error"]

    def observe(self, descriptor_path, pid=None, process_start_identity=None):
        """Observe a claim, including finished threads in this live controller.

        After controller death, the underlying worker-identity check supplies
        the proof. While it lives, only a returned thread registered by this
        pool can establish that no future launch remains possible.
        """
        with self._lock:
            out = pool.observe(descriptor_path, pid, process_start_identity)
            completed = self._completed.get(os.path.normcase(os.path.abspath(descriptor_path)))
            if not completed:
                return out
            try:
                d = pool._load_descriptor(descriptor_path)
                claim = pool.read_claim(descriptor_path)
                digest, receipt, fingerprint = completed
                identity = (receipt["pid"], receipt["process_start_identity"])
                supplied = (pid, process_start_identity)
                if (fingerprint is not None and pool.claim_fingerprint(descriptor_path) == fingerprint and
                        pool._descriptor_hash(d) == digest and claim is not None and
                        (claim["worker_pid"], claim["worker_start_filetime"]) == identity and
                        supplied in ((None, None), identity) and
                        (out["job_exists"] is False or
                         (out["job_exists"] is True and out["active_processes"] == 0))):
                    out["empty_confirmed"] = True
                    out["pool_completion_proof"] = dict(version=1, id=d["id"], nonce=d["nonce"],
                        descriptor_hash=digest, worker_pid=identity[0], worker_start_identity=identity[1],
                        claim_fingerprint=fingerprint)
            except (OSError, ValueError, TypeError):
                pass
            return out

    def close(self):
        with self._condition:
            self._closed = True
            self._draining = True
            self._release_if_drained()
            while self._jobs:
                self._condition.wait()
            self._release_if_drained()
