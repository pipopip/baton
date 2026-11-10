"""Contained Windows executor for fleet jobs. No admission decisions live here.

The CLI takes one path to a version-1 descriptor. A successful launch always
assigns the suspended process to a non-breakaway Job Object before resuming it.
"""
from __future__ import annotations

import ctypes
import contextlib
import threading
from ctypes import wintypes as W
import hashlib
import json
import math
import os
import re
import shutil
import stat
import subprocess
import sys
import time
import uuid

if os.name == "nt":
    import msvcrt
    K = ctypes.WinDLL("kernel32", use_last_error=True)
else:
    K = None

SIZE_T = ctypes.c_size_t
ULONG_PTR = ctypes.c_size_t


class OVERLAPPED(ctypes.Structure):
    _fields_ = [("Internal", ULONG_PTR), ("InternalHigh", ULONG_PTR), ("Offset", W.DWORD),
                ("OffsetHigh", W.DWORD), ("hEvent", W.HANDLE)]


class IO_COUNTERS(ctypes.Structure):
    _fields_ = [(name, ctypes.c_ulonglong) for name in
                ("ReadOperationCount", "WriteOperationCount", "OtherOperationCount", "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]


class BASIC_LIMIT(ctypes.Structure):
    _fields_ = [("PerProcessUserTimeLimit", ctypes.c_longlong), ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", W.DWORD), ("MinimumWorkingSetSize", SIZE_T), ("MaximumWorkingSetSize", SIZE_T),
                ("ActiveProcessLimit", W.DWORD), ("Affinity", ULONG_PTR), ("PriorityClass", W.DWORD), ("SchedulingClass", W.DWORD)]


class EXTENDED_LIMIT(ctypes.Structure):
    _fields_ = [("BasicLimitInformation", BASIC_LIMIT), ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", SIZE_T), ("JobMemoryLimit", SIZE_T),
                ("PeakProcessMemoryUsed", SIZE_T), ("PeakJobMemoryUsed", SIZE_T)]


class CPU_LIMIT(ctypes.Structure):
    _fields_ = [("ControlFlags", W.DWORD), ("CpuRate", W.DWORD)]


class ACCOUNTING(ctypes.Structure):
    _fields_ = [("TotalUserTime", ctypes.c_longlong), ("TotalKernelTime", ctypes.c_longlong),
                ("ThisPeriodTotalUserTime", ctypes.c_longlong), ("ThisPeriodTotalKernelTime", ctypes.c_longlong),
                ("TotalPageFaultCount", W.DWORD), ("TotalProcesses", W.DWORD),
                ("ActiveProcesses", W.DWORD), ("TotalTerminatedProcesses", W.DWORD)]


class MEMORY_STATUS(ctypes.Structure):
    _fields_ = [("dwLength", W.DWORD), ("dwMemoryLoad", W.DWORD),
                ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                ("sullAvailExtendedVirtual", ctypes.c_ulonglong)]


class SECURITY_ATTRIBUTES(ctypes.Structure):
    _fields_ = [("nLength", W.DWORD), ("lpSecurityDescriptor", W.LPVOID), ("bInheritHandle", W.BOOL)]


class STARTUPINFO(ctypes.Structure):
    _fields_ = [("cb", W.DWORD), ("lpReserved", W.LPWSTR), ("lpDesktop", W.LPWSTR), ("lpTitle", W.LPWSTR),
                ("dwX", W.DWORD), ("dwY", W.DWORD), ("dwXSize", W.DWORD), ("dwYSize", W.DWORD),
                ("dwXCountChars", W.DWORD), ("dwYCountChars", W.DWORD), ("dwFillAttribute", W.DWORD),
                ("dwFlags", W.DWORD), ("wShowWindow", W.WORD), ("cbReserved2", W.WORD),
                ("lpReserved2", ctypes.POINTER(ctypes.c_byte)), ("hStdInput", W.HANDLE),
                ("hStdOutput", W.HANDLE), ("hStdError", W.HANDLE)]


class PROCESS_INFORMATION(ctypes.Structure):
    _fields_ = [("hProcess", W.HANDLE), ("hThread", W.HANDLE), ("dwProcessId", W.DWORD), ("dwThreadId", W.DWORD)]


class FILETIME(ctypes.Structure):
    _fields_ = [("dwLowDateTime", W.DWORD), ("dwHighDateTime", W.DWORD)]


def _api(name, args, result):
    f = getattr(K, name)
    f.argtypes, f.restype = args, result
    return f


def _setup():
    if K is None:
        raise OSError("Windows Job Objects are unavailable")
    _api("GlobalMemoryStatusEx", [ctypes.POINTER(MEMORY_STATUS)], W.BOOL)
    _api("LockFileEx", [W.HANDLE, W.DWORD, W.DWORD, W.DWORD, W.DWORD, ctypes.POINTER(OVERLAPPED)], W.BOOL)
    _api("UnlockFileEx", [W.HANDLE, W.DWORD, W.DWORD, W.DWORD, ctypes.POINTER(OVERLAPPED)], W.BOOL)
    _api("CreateJobObjectW", [W.LPVOID, W.LPCWSTR], W.HANDLE)
    _api("OpenJobObjectW", [W.DWORD, W.BOOL, W.LPCWSTR], W.HANDLE)
    _api("SetInformationJobObject", [W.HANDLE, ctypes.c_int, W.LPVOID, W.DWORD], W.BOOL)
    _api("QueryInformationJobObject", [W.HANDLE, ctypes.c_int, W.LPVOID, W.DWORD, W.LPVOID], W.BOOL)
    _api("AssignProcessToJobObject", [W.HANDLE, W.HANDLE], W.BOOL)
    _api("TerminateJobObject", [W.HANDLE, W.UINT], W.BOOL)
    _api("CreateFileW", [W.LPCWSTR, W.DWORD, W.DWORD, ctypes.POINTER(SECURITY_ATTRIBUTES), W.DWORD, W.DWORD, W.HANDLE], W.HANDLE)
    _api("CreateProcessW", [W.LPCWSTR, W.LPWSTR, W.LPVOID, W.LPVOID, W.BOOL, W.DWORD, W.LPVOID, W.LPCWSTR, ctypes.POINTER(STARTUPINFO), ctypes.POINTER(PROCESS_INFORMATION)], W.BOOL)
    _api("ResumeThread", [W.HANDLE], W.DWORD)
    _api("TerminateProcess", [W.HANDLE, W.UINT], W.BOOL)
    _api("GetExitCodeProcess", [W.HANDLE, ctypes.POINTER(W.DWORD)], W.BOOL)
    _api("GetProcessTimes", [W.HANDLE, ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME)], W.BOOL)
    _api("OpenProcess", [W.DWORD, W.BOOL, W.DWORD], W.HANDLE)
    _api("WaitForSingleObject", [W.HANDLE, W.DWORD], W.DWORD)
    _api("CloseHandle", [W.HANDLE], W.BOOL)


def _check(ok, name):
    if not ok:
        raise ctypes.WinError(ctypes.get_last_error(), name)
    return ok


def _creation_filetime(handle):
    created, exited, kernel, user = FILETIME(), FILETIME(), FILETIME(), FILETIME()
    _check(K.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited),
                             ctypes.byref(kernel), ctypes.byref(user)), "GetProcessTimes")
    return (created.dwHighDateTime << 32) | created.dwLowDateTime


def process_identity(pid):
    """Return a process creation FILETIME, or None when it cannot be read."""
    if isinstance(pid, bool) or not isinstance(pid, int) or not 0 < pid <= 0xFFFFFFFF:
        return None
    try:
        _setup()
        handle = K.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return None
        try:
            return _creation_filetime(handle)
        finally:
            K.CloseHandle(handle)
    except (OSError, AttributeError):
        return None


def _worker_alive(pid, creation):
    """Check the same process identity and handle state, not a bare PID."""
    if (isinstance(pid, bool) or not isinstance(pid, int) or not 0 < pid <= 0xFFFFFFFF or
            isinstance(creation, bool) or not isinstance(creation, int) or creation <= 0):
        return None
    ctypes.set_last_error(0)
    handle = K.OpenProcess(0x1000 | 0x100000, False, pid)  # query + synchronize
    if not handle:
        # Invalid PID means that process no longer exists; access denied is unknown.
        return False if ctypes.get_last_error() == 87 else None
    try:
        if _creation_filetime(handle) != creation:
            return False  # PID reused: the recorded worker is gone
        status = K.WaitForSingleObject(handle, 0)
        return False if status == 0 else True if status == 258 else None
    except OSError:
        return None
    finally:
        K.CloseHandle(handle)


def _job_name(d):
    return "Local\\cq-fleet-" + d["id"] + "-" + d["nonce"]


def claim_path(descriptor_path):
    return os.path.join(os.path.dirname(os.path.abspath(descriptor_path)), "worker-claim.json")


def _regular_file(path):
    info = os.lstat(path)
    return stat.S_ISREG(info.st_mode) and not (getattr(info, "st_file_attributes", 0) & 0x400)


def _load_descriptor(path):
    path = os.path.abspath(path)
    if not _regular_file(path):
        raise ValueError("descriptor must be a regular file")
    with open(path, encoding="utf-8") as f:
        d = _descriptor(json.load(f))
    protected = {os.path.normcase(os.path.realpath(path)), os.path.normcase(os.path.realpath(claim_path(path)))}
    for key in ("log_path", "legacy_lock_path", "result_path", "cancel_path"):
        if os.path.normcase(os.path.realpath(d[key])) in protected:
            raise ValueError(key + " aliases the descriptor or claim")
    return d


def _descriptor_hash(d):
    canonical = json.dumps(d, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def read_claim(descriptor_path):
    """Return a nonce- and descriptor-bound claim, or None when unavailable.

    None includes malformed and partially written claims as well as absence;
    callers must not equate it with permission to remove a claim or rerun work.
    """
    try:
        d = _load_descriptor(descriptor_path)
        path = claim_path(descriptor_path)
        if not _regular_file(path):
            return None
        with open(path, "rb") as f:
            raw = f.read(65537)
        if len(raw) > 65536:
            return None
        claim = json.loads(raw.decode("utf-8"))
        if not isinstance(claim, dict):
            return None
        if (claim.get("version") != 1 or claim.get("id") != d["id"] or
                claim.get("nonce") != d["nonce"] or claim.get("descriptor_hash") != _descriptor_hash(d)):
            return None
        pid, creation = claim.get("worker_pid"), claim.get("worker_start_filetime")
        if (isinstance(pid, bool) or not isinstance(pid, int) or not 0 < pid <= 0xFFFFFFFF or
                isinstance(creation, bool) or not isinstance(creation, int) or creation <= 0):
            return None
        return claim
    except (OSError, ValueError, TypeError, UnicodeError, AttributeError):
        return None


def claim_fingerprint(descriptor_path):
    """Identify the exact regular claim file and its bytes, or fail closed."""
    try:
        path = claim_path(descriptor_path)
        if not _regular_file(path):
            return None
        with open(path, "rb") as f:
            before = os.fstat(f.fileno())
            raw = f.read(65537)
            after = os.fstat(f.fileno())
        if (len(raw) > 65536 or not stat.S_ISREG(before.st_mode) or
                not before.st_ino or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) !=
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
            return None
        return dict(device=before.st_dev, inode=before.st_ino, size=before.st_size,
                    mtime_ns=before.st_mtime_ns, sha256=hashlib.sha256(raw).hexdigest())
    except (OSError, ValueError):
        return None


def _claim(descriptor_path, d, pid, creation):
    """Create exactly one durable launch claim; never remove a failed claim."""
    claim = dict(version=1, id=d["id"], nonce=d["nonce"], descriptor_hash=_descriptor_hash(d),
                 worker_pid=pid, worker_start_filetime=creation)
    fd = os.open(claim_path(descriptor_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(claim, f, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        f.flush()
        os.fsync(f.fileno())


def observe(descriptor_path, worker_pid=None, worker_start_filetime=None):
    """Read worker identity and named-job state without terminating any process.

    Optional identity comes from the launcher immediately after Popen and covers
    a crash before the worker writes its first result. Conflicting identities
    are treated as unknown. An absent job alone cannot prove a worker has
    finished: it may still be waiting to create it.
    """
    out = dict(worker_alive=None, job_exists=None, active_processes=None, empty_confirmed=False)
    try:
        _setup()
        d = _load_descriptor(descriptor_path)
        claim = read_claim(descriptor_path)
        if claim is None and os.path.lexists(claim_path(descriptor_path)):
            return out  # incomplete, invalid, or unreadable claim: never infer empty
        result = None
        try:
            with open(d["result_path"], encoding="utf-8") as f:
                result = json.load(f)
        except (OSError, ValueError):
            pass
        if not isinstance(result, dict):
            result = {}
        recorded = (result.get("worker_pid"), result.get("worker_start_filetime"))
        supplied = (worker_pid, worker_start_filetime)
        if result.get("job_name") not in (None, _job_name(d)):
            return out
        claimed = ((claim["worker_pid"], claim["worker_start_filetime"]) if claim else (None, None))
        identities = [pair for pair in (supplied, recorded, claimed) if pair != (None, None)]
        if identities and any(pair != identities[0] for pair in identities):
            return out
        identity = identities[0] if identities else (None, None)
        out["worker_alive"] = _worker_alive(*identity)
        ctypes.set_last_error(0)
        handle = K.OpenJobObjectW(0x4, False, _job_name(d))  # JOB_OBJECT_QUERY
        if not handle:
            if ctypes.get_last_error() == 2:  # ERROR_FILE_NOT_FOUND
                out["job_exists"] = False
        else:
            out["job_exists"] = True
            try:
                a, ext = ACCOUNTING(), EXTENDED_LIMIT()
                if K.QueryInformationJobObject(handle, 1, ctypes.byref(a), ctypes.sizeof(a), None):
                    out["active_processes"] = a.ActiveProcesses
                if K.QueryInformationJobObject(handle, 9, ctypes.byref(ext), ctypes.sizeof(ext), None):
                    out["peak_private_bytes"] = ext.PeakJobMemoryUsed
            finally:
                K.CloseHandle(handle)
        # Until a winner has claimed this descriptor, a delayed launcher may
        # still create the job even if one observed launcher has died.
        out["empty_confirmed"] = (claim is not None and out["worker_alive"] is False and
                                  (out["job_exists"] is False or
                                   (out["job_exists"] is True and out["active_processes"] == 0)))
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return out


def boot_identity():
    """Return the native Windows boot GUID, or None if it cannot be queried."""
    if os.name != "nt":
        return None
    try:
        ntdll = ctypes.WinDLL("ntdll")
        query = ntdll.NtQuerySystemInformation
        query.argtypes = [W.ULONG, W.LPVOID, W.ULONG, ctypes.POINTER(W.ULONG)]
        query.restype = ctypes.c_long
        buf = ctypes.create_string_buffer(64)
        returned = W.ULONG()
        status = query(90, buf, len(buf), ctypes.byref(returned))  # SystemBootEnvironmentInformation
        if status < 0 or returned.value < 16:
            return None
        value = uuid.UUID(bytes_le=buf.raw[:16])
        return str(value) if value.int else None
    except (OSError, ValueError, AttributeError):
        return None


def probe():
    """Return physical and commit headroom in GiB; unknown means unsupported."""
    result = dict(supported=False, logical_cpus=None, available_gb=None, commit_free_gb=None, total_gb=None,
                  capabilities=[], boot_identity=boot_identity())
    try:
        _setup()
        m = MEMORY_STATUS()
        m.dwLength = ctypes.sizeof(m)
        _check(K.GlobalMemoryStatusEx(ctypes.byref(m)), "GlobalMemoryStatusEx")
        n = os.cpu_count()
        if not n or not m.ullTotalPhys:
            return result
        result.update(supported=True, logical_cpus=n, available_gb=m.ullAvailPhys / 2**30,
                      commit_free_gb=m.ullAvailPageFile / 2**30, total_gb=m.ullTotalPhys / 2**30)
        result["capabilities"] = ["windows"]
        if shutil.which("python") or shutil.which("python3"):
            result["capabilities"].append("python")
        result["capabilities"].extend(name for name in ("bash", "go", "node", "ffmpeg", "blender", "adb") if shutil.which(name))
    except (OSError, AttributeError):
        pass
    return result


def _atomic(path, data):
    path = os.path.abspath(path)
    tmp = path + "." + uuid.uuid4().hex + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, allow_nan=False)
            f.flush()
            os.fsync(f.fileno())
        deadline = time.monotonic() + 1.0
        while True:
            try:
                os.replace(tmp, path)
                break
            except PermissionError as exc:
                # Short-lived readers on Windows often omit FILE_SHARE_DELETE.
                # Publishing a status sample must not kill its running job merely
                # because the controller is reading the previous sample.
                if os.name != 'nt' or getattr(exc, 'winerror', None) not in (5, 32, 33) or time.monotonic() >= deadline:
                    raise
                time.sleep(.01)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _descriptor(d):
    if d.get("version") != 1:
        raise ValueError("unsupported descriptor version")
    for key in ("id", "nonce"):
        if not isinstance(d.get(key), str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", d[key]):
            raise ValueError("invalid " + key)
    if len(d["nonce"]) < 16:
        raise ValueError("nonce must have at least 16 characters")
    if not isinstance(d.get("argv"), list) or not d["argv"] or not all(isinstance(x, str) and "\0" not in x for x in d["argv"]):
        raise ValueError("argv must be a nonempty string array")
    if not isinstance(d.get("env"), dict) or not all(isinstance(k, str) and k and "=" not in k and "\0" not in k and isinstance(v, str) and "\0" not in v for k, v in d["env"].items()):
        raise ValueError("invalid env")
    for key in ("cwd", "log_path", "legacy_lock_path", "result_path", "cancel_path"):
        if not isinstance(d.get(key), str) or not os.path.isabs(d[key]) or "\0" in d[key]:
            raise ValueError(key + " must be an absolute path")
    for key in ("timeout_seconds", "cpu_slots", "memory_max_gb"):
        if isinstance(d.get(key), bool) or not isinstance(d.get(key), (int, float)) or not math.isfinite(d[key]) or d[key] <= 0:
            raise ValueError(key + " must be finite and positive")
    for key in ("ram_floor_gb", "pending_reserved_gb"):
        if key in d and (isinstance(d[key], bool) or not isinstance(d[key], (int, float)) or not math.isfinite(d[key]) or d[key] < 0):
            raise ValueError(key + " must be finite and nonnegative")
    if ("ram_floor_gb" in d) != ("pending_reserved_gb" in d):
        raise ValueError("ram_floor_gb and pending_reserved_gb must be supplied together")
    if "boot_identity" in d:
        try:
            if not isinstance(d["boot_identity"], str) or not uuid.UUID(d["boot_identity"]).int:
                raise ValueError("invalid boot GUID")
        except (ValueError, AttributeError) as exc:
            raise ValueError("boot_identity must be a nonzero UUID") from exc
    return d


class SharedLock:
    """Shared LockFileEx byte 0 interoperates with the legacy msvcrt lock."""
    def __init__(self, path):
        self.fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        self.ov = OVERLAPPED()
        self.held = False

    def try_acquire(self):
        ok = K.LockFileEx(msvcrt.get_osfhandle(self.fd), 1, 0, 1, 0, ctypes.byref(self.ov))
        if not ok and ctypes.get_last_error() not in (33, 158):
            _check(False, "LockFileEx")
        self.held = bool(ok)
        return self.held

    def close(self):
        if self.held:
            K.UnlockFileEx(msvcrt.get_osfhandle(self.fd), 0, 1, 0, ctypes.byref(self.ov))
        os.close(self.fd)


class Job:
    def __init__(self, name, cpu_slots, cpus, memory_gb):
        ctypes.set_last_error(0)
        self.handle = K.CreateJobObjectW(None, name)
        _check(self.handle, "CreateJobObjectW")
        try:
            if ctypes.get_last_error() == 183:
                raise OSError("job name already exists")
            limits = EXTENDED_LIMIT()
            limits.BasicLimitInformation.LimitFlags = 0x2000 | 0x200 | 0x20  # kill on close, job commit cap, priority
            limits.BasicLimitInformation.PriorityClass = 0x4000  # BELOW_NORMAL_PRIORITY_CLASS
            limits.JobMemoryLimit = int(memory_gb * 2**30)
            _check(K.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)), "job memory/priority limits")
            rate = CPU_LIMIT(0x1 | 0x4, max(1, min(10000, math.floor(cpu_slots / cpus * 10000))))
            _check(K.SetInformationJobObject(self.handle, 15, ctypes.byref(rate), ctypes.sizeof(rate)), "job CPU hard cap")
        except BaseException:
            self.close()
            raise

    def stats(self):
        a, ext = ACCOUNTING(), EXTENDED_LIMIT()
        _check(K.QueryInformationJobObject(self.handle, 1, ctypes.byref(a), ctypes.sizeof(a), None), "job accounting")
        _check(K.QueryInformationJobObject(self.handle, 9, ctypes.byref(ext), ctypes.sizeof(ext), None), "job memory accounting")
        return a.ActiveProcesses, ext.PeakJobMemoryUsed

    def terminate(self):
        _check(K.TerminateJobObject(self.handle, 1), "TerminateJobObject")

    def close(self):
        if self.handle:
            K.CloseHandle(self.handle)
            self.handle = None


_launch_lock = threading.Lock()


def _launch(d, job):
    # All inheritable standard handles must be closed before another launch.
    with _launch_lock:
        return _launch_locked(d, job)


def _launch_locked(d, job):
    sa = SECURITY_ATTRIBUTES(ctypes.sizeof(SECURITY_ATTRIBUTES), None, True)
    # OPEN_ALWAYS, append-only; worker owns both inheritable file handles.
    log = K.CreateFileW(d["log_path"], 0x4, 0x7, ctypes.byref(sa), 4, 0x80, None)
    _check(log != ctypes.c_void_p(-1).value, "open log")
    null = K.CreateFileW("NUL", 0x80000000, 0x7, ctypes.byref(sa), 3, 0x80, None)
    if null == ctypes.c_void_p(-1).value:
        K.CloseHandle(log)
        _check(False, "open NUL")
    pi = PROCESS_INFORMATION()
    assigned = False
    try:
        si = STARTUPINFO()
        si.cb, si.dwFlags = ctypes.sizeof(si), 0x100
        si.hStdInput, si.hStdOutput, si.hStdError = null, log, log
        env = dict(os.environ)
        env.update(d["env"])
        block = ctypes.create_unicode_buffer("\0".join(k + "=" + v for k, v in sorted(env.items(), key=lambda x: x[0].upper())) + "\0\0")
        cmd = ctypes.create_unicode_buffer(subprocess.list2cmdline(d["argv"]))
        _check(K.CreateProcessW(None, cmd, None, None, True, 0x4 | 0x8000000 | 0x400 | 0x4000,
                                block, d["cwd"], ctypes.byref(si), ctypes.byref(pi)), "CreateProcessW")
        _check(K.AssignProcessToJobObject(job.handle, pi.hProcess), "AssignProcessToJobObject")
        assigned = True
        start = _creation_filetime(pi.hProcess)
        # Honor cancellation before the command is allowed to execute.
        if os.path.exists(d["cancel_path"]):
            raise InterruptedError("cancelled before resume")
        _check(K.ResumeThread(pi.hThread) != 0xFFFFFFFF, "ResumeThread")
        return pi, start
    except BaseException:
        if pi.hProcess:
            if assigned:
                job.terminate()
            else:
                K.TerminateProcess(pi.hProcess, 1)
            K.CloseHandle(pi.hProcess)
        if pi.hThread:
            K.CloseHandle(pi.hThread)
        raise
    finally:
        K.CloseHandle(log)
        K.CloseHandle(null)


def run(descriptor_path, gate=None, on_claim=None, expected_hash=None):
    d = _load_descriptor(descriptor_path)
    if expected_hash is not None and _descriptor_hash(d) != expected_hash:
        raise ValueError("descriptor changed before claiming; launch refused")
    _setup()
    worker_pid = os.getpid()
    worker_start = process_identity(worker_pid)
    if worker_start is None:
        raise OSError("cannot establish worker process identity")
    if "boot_identity" in d:
        current_boot = boot_identity()
        if current_boot is None or uuid.UUID(current_boot) != uuid.UUID(d["boot_identity"]):
            raise OSError("boot identity unavailable or changed; launch refused")
    _claim(descriptor_path, d, worker_pid, worker_start)
    if on_claim is not None:
        on_claim(worker_pid, worker_start)
    if _descriptor_hash(_load_descriptor(descriptor_path)) != _descriptor_hash(d):
        raise ValueError("descriptor changed while claiming; launch refused")
    state = dict(phase="waiting", pid=None, process_start_filetime=None, job_name=None, started=None,
                 worker_pid=worker_pid, worker_start_filetime=worker_start,
                 finished=None, exit=None, active_processes=0, peak_private_bytes=0, reason="")
    def save():
        _atomic(d["result_path"], state)
    save()
    lock = gate if gate is not None else SharedLock(d["legacy_lock_path"])
    job = pi = None
    try:
        while not lock.try_acquire():
            if os.path.exists(d["cancel_path"]):
                state.update(phase="cancelled", finished=time.time(), reason="cancelled waiting for legacy lock")
                save()
                return state
            state["reason"] = "legacy heavy lock busy"
            save()
            time.sleep(1)
        if os.path.exists(d["cancel_path"]):
            state.update(phase="cancelled", finished=time.time(), reason="cancelled before launch")
            save()
            return state
        launch_guard = gate.launch_guard() if gate is not None else contextlib.nullcontext(0)
        with launch_guard as reserved_gb:
            p = probe()
            if not p["supported"]:
                raise OSError("Windows resource probe unavailable")
            if d["cpu_slots"] > p["logical_cpus"]:
                raise ValueError("cpu_slots exceeds logical CPUs")
            if "ram_floor_gb" in d and (p["available_gb"] - d["pending_reserved_gb"] - reserved_gb < d["ram_floor_gb"] or
                                        p["commit_free_gb"] - d["pending_reserved_gb"] - reserved_gb < d["ram_floor_gb"]):
                raise OSError("insufficient current physical or commit headroom")
            name = _job_name(d)
            state["job_name"] = name
            job = Job(name, d["cpu_slots"], p["logical_cpus"], d["memory_max_gb"])
            pi, creation = _launch(d, job)
        state.update(phase="running", pid=pi.dwProcessId, process_start_filetime=creation, started=time.time(), reason="")
        start_mono = time.monotonic()
        save()
        outcome = None
        while True:
            active, peak = job.stats()
            state.update(active_processes=active, peak_private_bytes=peak)
            if not active:
                code = W.DWORD()
                _check(K.GetExitCodeProcess(pi.hProcess, ctypes.byref(code)), "GetExitCodeProcess")
                state.update(phase=outcome or ("done" if code.value == 0 else "failed"), exit=code.value,
                             finished=time.time(), reason=state["reason"])
                save()
                return state
            if outcome is None and (os.path.exists(d["cancel_path"]) or time.monotonic() - start_mono >= d["timeout_seconds"]):
                outcome = "cancelled" if os.path.exists(d["cancel_path"]) else "timeout"
                state["reason"] = "termination pending confirmation"
                job.terminate()
            save()
            time.sleep(0.25)
    except BaseException as exc:
        if job:
            # Never report a terminal state until the job really has no processes.
            while True:
                try:
                    active, peak = job.stats()
                    state.update(active_processes=active, peak_private_bytes=peak)
                    if not active:
                        break
                    job.terminate()
                    state.update(phase="running", reason="shutdown pending: " + str(exc))
                    save()
                except Exception as shutdown_error:
                    state.update(phase="running", reason="shutdown unconfirmed: " + str(shutdown_error))
                    try:
                        save()
                    except OSError:
                        pass  # retain the job handle and retry; never claim shutdown
                time.sleep(0.25)
        state.update(phase="cancelled" if isinstance(exc, InterruptedError) else "failed", finished=time.time(), reason=str(exc))
        save()
        return state
    finally:
        if pi:
            K.CloseHandle(pi.hThread)
            K.CloseHandle(pi.hProcess)
        if job:
            job.close()
        lock.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: windows_pool.py DESCRIPTOR.json")
    result = run(sys.argv[1])
    raise SystemExit(0 if result["phase"] == "done" else 1)
