"""Free space on the drives a session needs, and a warning when one runs low.

On 2026-11-11 the laptop's system drive went to 0.2 GB free while ten sessions worked: the Go build cache had grown
to 74 GB there. Nothing said so until tests that check free space began to fail. A full system drive breaks every
session at once (transcripts, temp files, git), so the prompt of each session that owns a worktree now carries a
warning while a drive is low, and the night report lists it. The warning asks an agent to tell the user and to
delete nothing: every session gets it at the same moment, and the caches and temp folders belong to all of them.
Stdlib only; hook-safe: nothing here raises, and nothing here writes a file.
"""
from __future__ import annotations

import math
import os
import shutil
from pathlib import Path

LOW_GB = 15.0      # below this a drive is reported; the cache and temp growth seen was tens of GB in a day
LOW_SHARE = 0.10   # ... or below this share of a smaller drive (a tmpfs of a few GB is not low at 14 GB free)


def session_paths(*extra) -> list:
    """The places a session writes: the temp directory, the home directory, and `extra` (a work tree, the queue's
    state). The temp directory comes from the environment: tempfile.gettempdir() writes a probe file to find it."""
    paths = []
    temp = next((os.environ[k] for k in ('TMPDIR', 'TEMP', 'TMP') if os.environ.get(k)), None if os.name == 'nt' else '/tmp')
    if temp:
        paths.append(temp)
    try:
        paths.append(Path.home())
    except (RuntimeError, OSError, KeyError):
        pass
    return paths + [p for p in extra if p]


def _drive(path) -> str | None:
    """The root of the drive a path is on ('C:\\' on Windows, the nearest existing mount point elsewhere)."""
    try:
        resolved = Path(path).resolve()
    except (OSError, RuntimeError, ValueError):
        return None
    if os.name == 'nt':
        anchor = resolved.anchor
        for prefix in ('\\\\?\\UNC\\', '\\\\?\\'):   # the long-path spellings of the same drive or share
            if anchor.startswith(prefix):
                anchor = ('\\\\' if 'UNC' in prefix else '') + anchor[len(prefix):]
                break
        return anchor or None
    while not os.path.ismount(resolved) and resolved != resolved.parent:
        resolved = resolved.parent
    return str(resolved)


def low_drives(paths=None, low_gb: float = LOW_GB) -> list[tuple[str, float]]:
    """(drive, free GB) for each distinct drive under `paths` (default session_paths()) that is low: less than
    low_gb free, or on a drive smaller than low_gb / LOW_SHARE, less than LOW_SHARE of it. Lowest first; the figure
    is rounded down, so a drive reported under a threshold never reads as being at it."""
    seen, out = set(), []
    for path in (session_paths() if paths is None else paths):
        drive = _drive(path)
        if not drive or os.path.normcase(drive) in seen:
            continue
        seen.add(os.path.normcase(drive))
        try:
            usage = shutil.disk_usage(drive)
        except (OSError, ValueError):
            continue
        free = usage.free / 1024 ** 3
        if free < min(low_gb, LOW_SHARE * usage.total / 1024 ** 3):
            out.append((drive, math.floor(free * 10) / 10))
    return sorted(out, key=lambda item: item[1])


def warnings(paths=None, low_gb: float = LOW_GB) -> list[str]:
    """One sentence per low drive for the night report, which the owner reads: it names the usual causes."""
    try:
        return [f'{drive} has {free:g} GB free. A full drive breaks every session; the usual growth here is the Go '
                f'build cache and old folders under the temp directory.' for drive, free in low_drives(paths, low_gb)]
    except Exception:
        return []


def prompt_warning(paths=None, low_gb: float = LOW_GB) -> str:
    """Text for a session's prompt context while a drive is low; '' otherwise. Agents read this one: it asks them
    to report and to delete nothing."""
    try:
        low = low_drives(paths, low_gb)
    except Exception:
        return ''
    if not low:
        return ''
    return '\n'.join(['## Disk space'] + [f'{drive} has {free:g} GB free. A full drive breaks every session.' for drive, free in low] + [
        'Tell the user now, before other work, and do not start work that writes large files to that drive. Do not '
        'delete anything to make room, caches and temp folders included: every session sees this at once and other '
        'sessions are using them. The user decides what to clear.'])
