"""Worktree discovery without spawning git (hooks call this on every session start)."""
from __future__ import annotations

import os
from pathlib import Path

BATON_DIRNAME = ".baton"


def find_worktree_root(start: str | Path) -> Path | None:
    """Nearest ancestor of ``start`` that contains a ``.git`` file or directory."""
    try:
        cur = Path(start).resolve()
    except OSError:
        return None
    for candidate in (cur, *cur.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def git_dir(root: Path) -> Path | None:
    """The worktree's private git dir (``.git`` itself, or the target of a ``gitdir:`` file)."""
    dotgit = root / ".git"
    if dotgit.is_dir():
        return dotgit
    try:
        text = dotgit.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not text.startswith("gitdir:"):
        return None
    target = Path(text.split(":", 1)[1].strip())
    return target if target.is_absolute() else (root / target).resolve()


def git_common_dir(root: Path) -> Path | None:
    """The repository's shared git dir (hooks/, info/exclude live here)."""
    gd = git_dir(root)
    if gd is None:
        return None
    commondir = gd / "commondir"
    if commondir.exists():
        rel = commondir.read_text(encoding="utf-8").strip()
        return (gd / rel).resolve()
    return gd


def main_checkout(root: Path) -> Path:
    """Main checkout of the repository that owns ``root``."""
    common = git_common_dir(root)
    if common is not None and common.name == ".git":
        return common.parent
    return root


def repo_name(root: Path) -> str:
    return main_checkout(root).name


def ws_name(root: Path) -> str:
    """Workstream name: the worktree directory's name (``<ws>`` for ``<repo>/.claude/worktrees/<ws>``, else the repo name)."""
    return root.name


def key_for(root: Path) -> str:
    """``<repo>/<ws>`` label of a worktree, ``<repo>/main`` for the main checkout."""
    is_main = os.path.normcase(os.path.abspath(main_checkout(root))) == os.path.normcase(os.path.abspath(root))
    return f"{repo_name(root)}/{'main' if is_main else ws_name(root)}"


def baton_dir(root: Path, create: bool = False) -> Path:
    path = root / BATON_DIRNAME
    if create:
        (path / "inbox").mkdir(parents=True, exist_ok=True)
    return path


def ensure_excluded(root: Path) -> bool:
    """Add ``.baton/`` to the repo's info/exclude once. Returns True if it changed the file."""
    common = git_common_dir(root)
    if common is None:
        return False
    exclude = common / "info" / "exclude"
    line = f"/{BATON_DIRNAME}/"
    try:
        existing = exclude.read_text(encoding="utf-8", errors="replace") if exclude.exists() else ""
    except OSError:
        return False
    if any(l.strip() in (line, f"{BATON_DIRNAME}/", f"/{BATON_DIRNAME}") for l in existing.splitlines()):
        return False
    exclude.parent.mkdir(parents=True, exist_ok=True)
    sep = "" if (not existing or existing.endswith("\n")) else "\n"
    with open(exclude, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(f"{sep}{line}\n")
    return True
