"""Single-use, fail-closed handoff between a cycler and its replacement process."""
from __future__ import annotations

import json
import math
import os
import re
import secrets
import time
from pathlib import Path

from . import harvest, locking, paths, procs

_TOKEN = re.compile(r"[0-9a-f]{64}\Z")
_POLL_SECONDS = 0.1


def _process_state(pid: int, start: int) -> bool | None:
    """Exact identity alive, gone, or unknown. Unknown never grants a handoff. Off Windows the identity is the pid
    with its ``procs.process_start_time`` and a zombie counts as gone (see ``procs.process_identity_state``)."""
    if procs.platform_name() != "windows":
        return procs.process_identity_state(pid, start)
    if not isinstance(pid, int) or pid <= 0 or not isinstance(start, int) or start <= 0:
        return None
    import ctypes
    from ctypes import wintypes
    kernel = procs._windows_kernel()
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    handle = kernel.OpenProcess(0x101000, False, pid)  # query + synchronize
    if not handle:
        # ERROR_INVALID_PARAMETER means the PID no longer exists. Access failures are unknown.
        return False if ctypes.get_last_error() == 87 else None
    try:
        actual = procs._windows_start_time(kernel, handle)
        if actual is None:
            return None
        if actual != start:
            return False
        result = kernel.WaitForSingleObject(handle, 0)
        return True if result == 0x102 else False if result == 0 else None
    finally:
        kernel.CloseHandle(handle)


def _identity() -> tuple[int, int]:
    pid = os.getpid()
    start = procs.process_start_time(pid)
    if not isinstance(start, int) or start <= 0:
        raise OSError("cannot establish process identity")
    return pid, start


def _files(root: Path, token: str) -> tuple[Path, Path]:
    if not isinstance(token, str) or not _TOKEN.fullmatch(token):
        raise ValueError("invalid handoff token")
    directory = paths.baton_dir(Path(root)) / "handoffs"
    return directory / (token + ".json"), directory / (token + ".lock")


def _read(path: Path, token: str) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("token") != token:
        raise ValueError("invalid handoff state")
    return data


def _write(path: Path, data: dict) -> None:
    harvest.atomic_write(path, json.dumps(data, sort_keys=True))


def _valid(data: dict) -> bool:
    expiry = data.get("expires")
    return isinstance(expiry, (float, int)) and math.isfinite(expiry) and time.time() < expiry


def _alive(data: dict, prefix: str) -> bool:
    return _process_state(data.get(prefix + "_pid"), data.get(prefix + "_start")) is True


def prepare(root: Path, old_session: str, old_pid: int, expected_start: int, timeout_seconds: float) -> str:
    """Create an attempt owned by the current cycler; return its secret nonce."""
    if (not isinstance(old_session, str) or not old_session or not isinstance(old_pid, int) or old_pid <= 0
            or not isinstance(expected_start, int) or expected_start <= 0
            or not isinstance(timeout_seconds, (int, float)) or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0):
        raise ValueError("invalid handoff parameters")
    cycler_pid, cycler_start = _identity()
    token = secrets.token_hex(32)
    path, lock = _files(root, token)
    with locking.exclusive(lock):
        if path.exists():
            raise FileExistsError("handoff token collision")
        _write(path, {"token": token, "old_session": old_session, "old_pid": old_pid,
                      "old_start": expected_start, "cycler_pid": cycler_pid, "cycler_start": cycler_start,
                      "expires": time.time() + timeout_seconds, "phase": "waiting"})
    return token


def ready(root: Path, token: str) -> bool:
    """True only for a live registered waiter on an unexpired attempt."""
    try:
        path, lock = _files(root, token)
        with locking.exclusive(lock):
            data = _read(path, token)
            return (_valid(data) and data.get("phase") == "ready" and _alive(data, "cycler")
                    and _alive(data, "waiter"))
    except (OSError, ValueError, TypeError, TimeoutError):
        return False


def authorize(root: Path, token: str) -> bool:
    """Authorize only the creator cycler and its live, registered waiter."""
    try:
        identity = _identity()
        path, lock = _files(root, token)
        with locking.exclusive(lock):
            data = _read(path, token)
            if (not _valid(data) or data.get("phase") != "ready"
                    or identity != (data.get("cycler_pid"), data.get("cycler_start"))
                    or not _alive(data, "cycler") or not _alive(data, "waiter")):
                return False
            data["phase"] = "authorized"
            _write(path, data)
            return True
    except (OSError, ValueError, TypeError, TimeoutError):
        return False


def cancel(root: Path, token: str) -> bool:
    """Invalidate an unconsumed attempt, including an already authorized grant."""
    try:
        path, lock = _files(root, token)
        with locking.exclusive(lock):
            data = _read(path, token)
            if data.get("phase") in ("consumed", "canceled"):
                return False
            data["phase"] = "canceled"
            _write(path, data)
            return True
    except (OSError, ValueError, TypeError, TimeoutError):
        return False


def wait_ready(root: Path, token: str) -> bool:
    """Register this process, wait for authorization and consume it exactly once."""
    try:
        identity = _identity()
        path, lock = _files(root, token)
        with locking.exclusive(lock):
            data = _read(path, token)
            if not _valid(data) or data.get("phase") != "waiting" or not _alive(data, "cycler"):
                return False
            data.update(waiter_pid=identity[0], waiter_start=identity[1], phase="ready")
            _write(path, data)
        while True:
            with locking.exclusive(lock):
                data = _read(path, token)
                if not _valid(data) or identity != (data.get("waiter_pid"), data.get("waiter_start")):
                    return False
                if data.get("phase") == "authorized":
                    # Query independently: inability to prove the exact predecessor gone is failure.
                    if _process_state(data.get("old_pid"), data.get("old_start")) is not False:
                        return False
                    data["phase"] = "consumed"
                    _write(path, data)
                    return True
                if data.get("phase") != "ready" or not _alive(data, "cycler"):
                    return False
            time.sleep(_POLL_SECONDS)
    except (OSError, ValueError, TypeError, TimeoutError):
        return False
