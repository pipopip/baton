"""Small process-scoped file lock, released by the OS even if a hook crashes."""
from __future__ import annotations

import errno
import os
import stat
import time
from contextlib import contextmanager
from enum import Enum, auto
from pathlib import Path


class WriterLockState(Enum):
    HELD = auto()
    UNLOCKED = auto()
    MISSING = auto()
    UNKNOWN = auto()


def codex_writer_lock(path: Path) -> WriterLockState:
    """Inspect a Codex writer lock without waiting, creating it or reading on POSIX."""
    try:
        if os.name == "nt":
            with path.open("rb") as source:
                try:
                    source.read(1)  # Windows enforces the writer's byte-range lock on reads.
                except OSError as exc:
                    if getattr(exc, "winerror", None) == 33 or exc.errno == errno.EACCES:
                        return WriterLockState.HELD
                    return WriterLockState.UNKNOWN
        else:
            import fcntl
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    return WriterLockState.UNKNOWN
                try:
                    fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except OSError as exc:
                    if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                        return WriterLockState.HELD
                    return WriterLockState.UNKNOWN
            finally:
                os.close(fd)  # Releases the shared probe immediately.
    except FileNotFoundError:
        return WriterLockState.MISSING
    except OSError:
        return WriterLockState.UNKNOWN
    return WriterLockState.UNLOCKED


@contextmanager
def exclusive(path: Path, timeout: float = 2.0):
    """Hold a persistent lock file; never unlink it (that would split contenders)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    locked = False
    try:
        deadline = time.monotonic() + timeout
        while True:
            try:
                if os.name == "nt":
                    import msvcrt
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
                break
            except OSError as exc:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out locking {path}") from exc
                time.sleep(0.025)
        yield
    finally:
        if locked:
            if os.name == "nt":
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
