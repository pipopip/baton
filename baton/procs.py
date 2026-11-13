"""Detached process creation shared by the follower runner, the login keepalive and the background closeout."""
from __future__ import annotations

import os
import subprocess
import sys

from . import config

DETACHED_PROCESS = 0x00000008
CREATE_NEW_PROCESS_GROUP = 0x00000200
CREATE_BREAKAWAY_FROM_JOB = 0x01000000


def popen_detached(argv, **kwargs) -> "subprocess.Popen":
    """Start ``argv`` detached from the caller's console and process group.

    On Windows the child also leaves the caller's job object (CREATE_BREAKAWAY_FROM_JOB): a runner started
    from an agent's shell tool would otherwise be killed together with that tool's job. A job without the
    breakaway permission makes CreateProcess refuse the flag (access denied); the spawn is then retried
    without it, which is no worse than before.
    """
    if os.name != "nt":
        return subprocess.Popen(argv, start_new_session=True, **kwargs)
    flags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | config.CREATE_NO_WINDOW
    try:
        return subprocess.Popen(argv, creationflags=flags | CREATE_BREAKAWAY_FROM_JOB, **kwargs)
    except OSError:
        return subprocess.Popen(argv, creationflags=flags, **kwargs)


def _windows_kernel():
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    kernel.GetProcessTimes.restype = wintypes.BOOL
    kernel.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel.TerminateProcess.restype = wintypes.BOOL
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    return kernel


def _windows_start_time(kernel, handle) -> int | None:
    import ctypes
    from ctypes import wintypes
    created, exited, kernel_time, user_time = (wintypes.FILETIME() for _ in range(4))
    if not kernel.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited),
                                  ctypes.byref(kernel_time), ctypes.byref(user_time)):
        return None
    return (created.dwHighDateTime << 32) | created.dwLowDateTime


def platform_name() -> str:
    """'windows', 'darwin', 'linux' or 'other'. Every platform decision in this module and its callers goes
    through here (not ``os.name``) so a test can name another platform without patching ``os.name``, which
    breaks pathlib."""
    if os.name == "nt":
        return "windows"
    if sys.platform == "darwin":
        return "darwin"
    if sys.platform.startswith("linux"):
        return "linux"
    return "other"


def _valid_pid(pid) -> bool:
    return isinstance(pid, int) and not isinstance(pid, bool) and pid > 0


# --------------------------------------------------------------------------- POSIX process identity
#
# Off Windows there is no handle that pins a process, so identity is the pair (pid, start time) read from the
# kernel. The start-time value is a positive integer whose unit depends on the platform (Linux: clock ticks
# since boot; macOS: microseconds since the epoch). A value is only meaningful when compared with another value
# obtained by the same function on the same machine.

_PROC_BSDINFO = 3                      # PROC_PIDTBSDINFO
_BSDINFO_FORMAT = "=12I16s32s5IiQQ"    # struct proc_bsdinfo, 136 bytes
_BSDINFO_SIZE = 136
_SZOMB = 5
_DARWIN_SOURCE: str | None = None      # 'libproc' or 'ps', decided once per process


def _read_proc_stat(pid: int) -> str:
    """Text of /proc/<pid>/stat (raises OSError)."""
    with open(f"/proc/{pid}/stat", "rb") as fh:
        return fh.read().decode("utf-8", "replace")


def _proc_mounted() -> bool:
    return os.path.isdir("/proc/self")


def _parse_linux_stat(text: str) -> tuple[str, int] | None:
    """(state, starttime) from a /proc/<pid>/stat line. The command name (field 2) sits in parentheses and may
    itself contain spaces and parentheses, so the fields are counted from after the LAST ')'. State is field 3,
    starttime (clock ticks since boot) field 22."""
    end = text.rfind(")")
    if end < 0:
        return None
    rest = text[end + 1:].split()
    if len(rest) < 20 or len(rest[0]) != 1:
        return None
    try:
        start = int(rest[19])
    except ValueError:
        return None
    return (rest[0], start) if start > 0 else None


def _linux_probe(pid: int) -> tuple[str, int | None, bool]:
    try:
        text = _read_proc_stat(pid)
    except (FileNotFoundError, ProcessLookupError):
        # a missing entry means the process is gone only when /proc itself is there to be asked
        return ("gone", None, False) if _proc_mounted() else ("unknown", None, False)
    except OSError:
        return "unknown", None, False
    parsed = _parse_linux_stat(text)
    if parsed is None:
        return "unknown", None, False
    return "ok", parsed[1], parsed[0] in ("Z", "X")


def _libproc_call(pid: int) -> tuple[int, bytes, int]:
    """(return value, buffer, errno) of ``proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, buf, 136)``."""
    import ctypes
    try:
        lib = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    except OSError:   # libSystem re-exports libproc
        lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
    lib.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int]
    lib.proc_pidinfo.restype = ctypes.c_int
    buf = ctypes.create_string_buffer(_BSDINFO_SIZE)
    ctypes.set_errno(0)
    rc = lib.proc_pidinfo(pid, _PROC_BSDINFO, 0, buf, _BSDINFO_SIZE)
    return rc, buf.raw, ctypes.get_errno()


def _parse_bsdinfo(rc: int, raw: bytes, pid: int) -> tuple[int, int] | None:
    """(pbi_status, start in microseconds since the epoch) when the call filled the whole struct and the struct
    describes ``pid``; None otherwise."""
    import struct
    if rc != _BSDINFO_SIZE or len(raw) != _BSDINFO_SIZE:
        return None
    fields = struct.unpack(_BSDINFO_FORMAT, raw)
    status, got_pid, seconds, micros = fields[1], fields[3], fields[20], fields[21]
    if got_pid != pid or seconds <= 0 or micros >= 1_000_000:
        return None
    return status, seconds * 1_000_000 + micros


def _libproc_probe(pid: int) -> tuple[str, int | None, bool]:
    """libproc's answer for one pid: ('ok', start, zombie); ('gone', ...) when the kernel says there is no such
    process; else ('unknown', ...). It never falls back to ps: the source was chosen once by _darwin_source."""
    try:
        rc, raw, err = _libproc_call(pid)
    except (OSError, AttributeError, ValueError):
        return "unknown", None, False
    if rc == 0:
        import errno
        return ("gone", None, False) if err == errno.ESRCH else ("unknown", None, False)
    parsed = _parse_bsdinfo(rc, raw, pid)
    if parsed is None:
        return "unknown", None, False
    return "ok", parsed[1], parsed[0] == _SZOMB


def _parse_ps_lstart(text: str) -> int | None:
    """Microseconds since the epoch (whole seconds) from ``ps -o lstart=`` output such as
    'Thu Nov 12 13:01:36 2026' (a single-digit day is padded with two spaces)."""
    import time
    try:
        parsed = time.strptime(" ".join(text.split()), "%a %b %d %H:%M:%S %Y")
        seconds = time.mktime(tuple(parsed)[:8] + (-1,))
    except (ValueError, OverflowError, OSError):
        return None
    return int(seconds) * 1_000_000 if seconds > 0 else None


def _ps_probe(pid: int) -> tuple[str, int | None, bool]:
    """``ps -o lstart=``: a whole-second start time, so two processes that reuse one pid within a second share
    a value. A zombie is not recognised here and counts as alive."""
    import subprocess as sp
    try:
        cp = sp.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, timeout=10,
                    stdin=sp.DEVNULL, env=dict(os.environ, LC_ALL="C"))
    except (OSError, sp.SubprocessError):
        return "unknown", None, False
    out = cp.stdout.strip()
    if not out:
        return ("gone", None, False) if cp.returncode == 1 and not cp.stderr.strip() else ("unknown", None, False)
    start = _parse_ps_lstart(out) if cp.returncode == 0 else None
    return ("ok", start, False) if start else ("unknown", None, False)


def _darwin_source() -> str:
    """'libproc' when proc_pidinfo answers correctly for this very process, else 'ps'; decided on the first call
    and kept for the life of the process, so every start time this process hands out has one unit."""
    global _DARWIN_SOURCE
    if _DARWIN_SOURCE is None:
        _DARWIN_SOURCE = "libproc" if _libproc_probe(os.getpid())[0] == "ok" else "ps"
    return _DARWIN_SOURCE


def _probe(pid: int) -> tuple[str, int | None, bool]:
    """('ok' | 'gone' | 'unknown', start time, zombie) for one pid on Linux and macOS."""
    plat = platform_name()
    if plat == "linux":
        return _linux_probe(pid)
    if plat == "darwin":
        return _libproc_probe(pid) if _darwin_source() == "libproc" else _ps_probe(pid)
    return "unknown", None, False


def process_start_time(pid: int) -> int | None:
    """A positive integer that stays the same for the life of the process and differs for a later process that
    reuses the pid; None when it cannot be determined.

    Windows: the creation FILETIME. Linux: field 22 of /proc/<pid>/stat (clock ticks since boot). macOS:
    microseconds since the epoch from libproc's proc_bsdinfo or, on a machine where that call does not answer
    correctly for this process, whole seconds from ``ps -o lstart=`` (decided once per process). The unit differs
    by platform and source, so a value is only compared with another value obtained by this function on the same
    machine, never with a session record's ``procStart`` or any other source."""
    if platform_name() == "windows":
        kernel = _windows_kernel()
        handle = kernel.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return None
        try:
            return _windows_start_time(kernel, handle)
        finally:
            kernel.CloseHandle(handle)
    if not _valid_pid(pid):
        return None
    kind, start, _ = _probe(pid)
    return start if kind == "ok" else None


def _boot_time() -> float | None:
    """``btime`` of /proc/stat: boot time in seconds since the epoch."""
    try:
        with open("/proc/stat", "rb") as fh:
            for line in fh.read().decode("utf-8", "replace").splitlines():
                if line.startswith("btime "):
                    return float(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return None


def process_started_epoch(pid: int) -> float | None:
    """Wall-clock start of the process in seconds since the epoch (for comparing with a session record's
    ``startedAt``), or None. Windows: from the FILETIME. macOS: from the same source as process_start_time.
    Linux: /proc/stat ``btime`` plus starttime over the clock tick rate, which can be a second or so off when
    the system clock was stepped after boot."""
    start = process_start_time(pid)
    if start is None:
        return None
    plat = platform_name()
    if plat == "windows":
        return (start - 116444736000000000) / 1e7
    if plat == "darwin":
        return start / 1e6
    if plat == "linux":
        boot = _boot_time()
        try:
            ticks = os.sysconf("SC_CLK_TCK")
        except (ValueError, OSError, AttributeError):
            return None
        return boot + start / ticks if boot is not None and ticks > 0 else None
    return None


def process_identity_state(pid: int, start: int) -> bool | None:
    """POSIX counterpart of the Windows handle check: True when this exact identity (pid and start time) is
    alive; False when it is gone, the pid now belongs to a process with another start time, or the process is a
    zombie (Linux state Z, macOS pbi_status SZOMB; the ps fallback cannot tell a zombie and says alive); None when
    it cannot be told (unreadable start time, no /proc, libproc or ps failing)."""
    if not _valid_pid(pid) or not isinstance(start, int) or isinstance(start, bool) or start <= 0:
        return None
    kind, actual, zombie = _probe(pid)
    if kind == "gone":
        return False
    if kind != "ok" or actual is None:
        return None
    return actual == start and not zombie


def end_process(pid: int, wait: float = 5.0, *, expected_start: int | None = None) -> bool:
    """End one process by pid (the context cycler ends the session it replaced, after checking the pid against
    the session's own record twice). Refuses (False) when ``expected_start`` is None or is not the process's
    ``process_start_time``. Returns whether that exact process is gone within ``wait`` seconds.

    Windows: TerminateProcess; the identity check, termination, and exit proof use the same process handle, so
    PID reuse cannot affect the result. POSIX: SIGKILL, and "gone" means the pid no longer exists, its start time
    changed, or it is a zombie. POSIX has no handle: between the start-time check and the signal a pid reused
    inside that window (microseconds) would be hit. On Linux with pidfd support the window is closed: a pidfd is
    opened first, the start time is re-checked, and the signal goes through the fd, which names the process that
    held the pid when it was opened."""
    import time
    if platform_name() == "windows":
        import math
        try:
            # SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE
            kernel32 = _windows_kernel()
            handle = kernel32.OpenProcess(0x101001, False, pid)
            if not handle:
                return False
            try:
                if expected_start is None or _windows_start_time(kernel32, handle) != expected_start:
                    return False
                if not kernel32.TerminateProcess(handle, 1):
                    return False
                # Avoid INFINITE (0xffffffff); WAIT_OBJECT_0 alone proves this handle is signaled.
                timeout_ms = min(0xfffffffe, max(0, math.ceil(wait * 1000)))
                return kernel32.WaitForSingleObject(handle, timeout_ms) == 0
            finally:
                kernel32.CloseHandle(handle)
        except (OSError, ValueError, OverflowError):
            return False
    if (not _valid_pid(pid) or not isinstance(expected_start, int) or isinstance(expected_start, bool)
            or expected_start <= 0):
        return False
    if not _signal_exact(pid, expected_start):
        return False
    deadline = time.monotonic() + wait
    while True:
        if process_identity_state(pid, expected_start) is False:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.05)


def _signal_exact(pid: int, expected_start: int) -> bool:
    """Check the start time, then send SIGKILL (through a pidfd where Linux has one). False: refused or failed."""
    import signal
    fd = None
    try:
        if platform_name() == "linux" and hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"):
            try:
                fd = os.pidfd_open(pid)
            except ProcessLookupError:
                return False
            except OSError:
                fd = None   # kernel without pidfd, or not permitted: signal by pid below
        if process_start_time(pid) != expected_start:
            return False
        try:
            if fd is not None:
                signal.pidfd_send_signal(fd, signal.SIGKILL)
            else:
                os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            return True   # exited since the check: the wait that follows proves it
        except OSError:
            return False
        return True
    finally:
        if fd is not None:
            os.close(fd)


def popen_group_kwargs() -> dict:
    """Popen arguments that make a child the leader of its own session and process group on POSIX, so
    ``kill_process_group`` can end it with everything it spawned. Empty on Windows, where ``taskkill /T`` walks
    the tree. A child in its own session no longer gets the terminal's Ctrl+C: a caller that waits on it must end
    it itself when interrupted."""
    return {} if platform_name() == "windows" else {"start_new_session": True}


def kill_process_group(proc: "subprocess.Popen") -> None:
    """SIGKILL the process group ``proc`` leads (it was started with ``popen_group_kwargs``); POSIX only. The
    group is signalled only while ``proc`` is unreaped and really leads a group other than this process's own,
    so a reaped pid that was reused, or a child that shares this process's group, is never hit by killpg;
    ``proc.kill()`` is used then."""
    import signal
    if proc.returncode is None:
        try:
            if os.getpgid(proc.pid) == proc.pid and proc.pid != os.getpgrp():
                os.killpg(proc.pid, signal.SIGKILL)
                return
        except OSError:
            pass
    try:
        proc.kill()
    except OSError:
        pass
