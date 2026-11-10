"""Thin git wrappers. Never raise on git failure unless ``check=True``."""
from __future__ import annotations

import subprocess
from pathlib import Path

from .config import CREATE_NO_WINDOW


def git(root: str | Path, *args: str, check: bool = False, timeout: float = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=check,
        stdin=subprocess.DEVNULL,
        creationflags=CREATE_NO_WINDOW,
    )


def out(root: str | Path, *args: str, timeout: float = 30) -> str:
    try:
        cp = git(root, *args, timeout=timeout)
    except (OSError, subprocess.SubprocessError):
        return ""
    return cp.stdout.strip() if cp.returncode == 0 else ""


def head(root: Path) -> tuple[str, str]:
    """(full sha, subject) of HEAD, or ('', '')."""
    line = out(root, "log", "-1", "--format=%H%x1f%s")
    if "\x1f" not in line:
        return "", ""
    sha, subject = line.split("\x1f", 1)
    return sha, subject


def branch(root: Path) -> str:
    """Current branch name, or '' when detached."""
    return out(root, "symbolic-ref", "--quiet", "--short", "HEAD")


def dirty_files(root: Path) -> list[str]:
    """Paths with uncommitted changes (porcelain v1), excluding baton's own directory."""
    try:
        cp = git(root, "status", "--porcelain", "--untracked-files=normal")
    except (OSError, subprocess.SubprocessError):
        return []
    text = cp.stdout if cp.returncode == 0 else ""  # not stripped: the status column may start with a space
    files = []
    for line in text.splitlines():
        path = line[3:].strip().strip('"')
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        if path and not path.startswith(".baton/"):
            files.append(path)
    return files


def default_branch(root: Path) -> str:
    for name in ("master", "main"):
        if out(root, "rev-parse", "--verify", "--quiet", f"refs/heads/{name}"):
            return name
    return "master"


def merge_base(root: Path, a: str, b: str) -> str:
    return out(root, "merge-base", a, b)


def changed_lines(root: Path, base: str, tip: str) -> int:
    """Added + deleted lines between two commits (binary files count as 0)."""
    text = out(root, "diff", "--numstat", f"{base}..{tip}")
    total = 0
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            total += int(parts[0]) + int(parts[1])
    return total
