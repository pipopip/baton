"""Detached process creation shared by the follower runner, the login keepalive and the background closeout."""
from __future__ import annotations

import os
import subprocess

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


def process_start_time(pid: int) -> int | None:
    """Windows creation FILETIME, or None when identity cannot be verified."""
    if os.name != "nt":
        return None
    kernel = _windows_kernel()
    handle = kernel.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        return None
    try:
        return _windows_start_time(kernel, handle)
    finally:
        kernel.CloseHandle(handle)


def end_process(pid: int, wait: float = 5.0, *, expected_start: int | None = None) -> bool:
    """End one process by pid (the context cycler ends the session it replaced, after checking the pid against
    the session's own record twice). Windows: TerminateProcess; elsewhere SIGKILL. Returns whether the process
    has exited within ``wait`` seconds. On Windows the identity check, termination, and exit proof use the
    same process handle, so PID reuse cannot affect the result."""
    import time
    from .hook import _pid_alive
    if os.name == "nt":
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
    else:
        try:
            os.kill(pid, 9)
        except OSError:
            pass
    deadline = time.monotonic() + wait
    while time.monotonic() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(0.2)
    return not _pid_alive(pid)
