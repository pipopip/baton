"""Locate the last Claude / Codex session for a worktree and size its context.

``last_session`` prefers the registry (written by the SessionStart/SessionEnd
hooks) and falls back to scanning the vendors' transcript directories, so the
launcher also works for sessions that started before the hooks were installed.
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path

from . import config, harvest, registry

FIRST_LINE_LIMIT = 1024 * 1024   # Codex session_meta can be large
CODEX_LOOKBACK_DAYS = 30
_UUID_TAIL = re.compile(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$")


@dataclass
class SessionInfo:
    vendor: str
    session_id: str
    transcript: Path
    # None when it cannot be known (Claude transcripts are shared between accounts).
    account: str | None
    last_active: float            # epoch seconds, transcript mtime
    ctx_tokens: int | None
    model: str | None = None      # model id read from the transcript tail (or the registry record)


# --------------------------------------------------------------------- claude

def claude_slug(worktree: str | os.PathLike) -> str:
    """Claude's project directory name: every non-alphanumeric character becomes '-'."""
    return re.sub(r"[^A-Za-z0-9]", "-", os.path.abspath(str(worktree)))


def claude_project_dir(worktree: str | os.PathLike, config_dir: str | os.PathLike) -> Path:
    """``<config_dir>/projects/<slug>``, matched case-insensitively against what exists."""
    projects = Path(config_dir) / "projects"
    slug = claude_slug(worktree)
    exact = projects / slug
    if exact.is_dir():
        return exact
    want = slug.lower()
    try:
        for child in projects.iterdir():
            if child.name.lower() == want and child.is_dir():
                return child
    except OSError:
        pass
    return exact


def _newest_jsonl(directory: Path) -> Path | None:
    best: tuple[float, Path] | None = None
    try:
        for child in directory.iterdir():
            if child.suffix != ".jsonl":
                continue
            try:
                mtime = child.stat().st_mtime
            except OSError:
                continue
            if best is None or mtime > best[0]:
                best = (mtime, child)
    except OSError:
        return None
    return best[1] if best else None


def scan_claude(worktree: str | os.PathLike, config_dir: str | os.PathLike) -> SessionInfo | None:
    """Newest ``*.jsonl`` directly inside the worktree's Claude project directory."""
    newest = _newest_jsonl(claude_project_dir(worktree, config_dir))
    if newest is None:
        return None
    tokens, model = harvest.ctx_info("claude", newest)
    return SessionInfo("claude", newest.stem, newest, None, newest.stat().st_mtime, tokens, model)


# ---------------------------------------------------------------------- codex

def _norm_cwd(value: str) -> str:
    if value.startswith("\\\\?\\"):
        value = value[4:]
    return config.norm(value)


def _read_first_line(path: Path) -> str:
    with open(path, "rb") as fh:
        return fh.readline(FIRST_LINE_LIMIT).decode("utf-8", errors="replace")


def _numeric_dirs(parent: Path, width: int) -> list[Path]:
    try:
        return [c for c in parent.iterdir() if c.is_dir() and c.name.isdigit() and len(c.name) == width]
    except OSError:
        return []


def codex_rollouts(home: Path, now: float, days: int = CODEX_LOOKBACK_DAYS) -> list[tuple[float, Path]]:
    """Rollout files under ``<home>/sessions/YYYY/MM/DD`` modified within ``days``, newest mtime first.

    Pruned by file mtime only: Codex appends to the original rollout when a session is resumed, so a
    recent session can sit in an old date directory.
    """
    cutoff = now - days * 86400
    found: list[tuple[float, Path]] = []
    for ydir in _numeric_dirs(home / "sessions", 4):
        for mdir in _numeric_dirs(ydir, 2):
            for ddir in _numeric_dirs(mdir, 2):
                try:
                    with os.scandir(ddir) as entries:
                        for entry in entries:
                            if not (entry.name.startswith("rollout-") and entry.name.endswith(".jsonl")):
                                continue
                            try:
                                mtime = entry.stat().st_mtime
                            except OSError:
                                continue
                            if mtime >= cutoff:
                                found.append((mtime, Path(entry.path)))
                except OSError:
                    continue
    found.sort(key=lambda item: item[0], reverse=True)
    return found


def _rollout_meta(path: Path) -> dict | None:
    try:
        obj = json.loads(_read_first_line(path))
    except (OSError, ValueError):
        return None
    if not isinstance(obj, dict):
        return None
    payload = obj.get("payload")
    if obj.get("type") != "session_meta" or not isinstance(payload, dict):
        return None
    return payload


def _rollout_id(path: Path, meta: dict) -> str:
    sid = meta.get("id")
    if isinstance(sid, str) and sid:
        return sid
    match = _UUID_TAIL.search(path.stem)
    return match.group(1) if match else path.stem


def scan_codex(worktree: str | os.PathLike, homes: dict[str, Path], now: float | None = None) -> SessionInfo | None:
    """Newest rollout (any account) whose recorded cwd is ``worktree``; only first lines are read."""
    now = time.time() if now is None else now
    want = config.norm(worktree)
    best: SessionInfo | None = None
    for account, home in homes.items():
        for mtime, path in codex_rollouts(Path(home), now):
            if best is not None and mtime <= best.last_active:
                break  # everything left in this account is older than the current best
            meta = _rollout_meta(path)
            cwd = meta.get("cwd") if meta else None
            if not isinstance(cwd, str) or _norm_cwd(cwd) != want:
                continue
            source = meta.get("source")
            if isinstance(source, dict) or source == "exec":
                continue  # a spawned sub-agent or a headless `codex exec` run (e.g. a baton follower), not the lead's session
            tokens, model = harvest.ctx_info("codex", path)
            best = SessionInfo("codex", _rollout_id(path, meta), path, account, mtime, tokens, model)
            break
    return best


# ---------------------------------------------------------------- entry point

def codex_homes() -> dict[str, Path]:
    return {name: config.account_dir("codex", name) for name in config.account_names("codex")}


def _from_registry(worktree: str | os.PathLike, vendor: str) -> SessionInfo | None:
    rec = registry.session(worktree, vendor)
    transcript = rec.get("transcript") if isinstance(rec, dict) else None
    if not transcript:
        return None
    path = Path(transcript)
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    tokens, model = harvest.ctx_info(vendor, path)  # the live tail beats the value the hooks recorded at the last prompt or SessionEnd
    if tokens is None and vendor != "codex" and isinstance(rec.get("ctx_tokens"), int):
        tokens = rec["ctx_tokens"]
    if model is None and isinstance(rec.get("model"), str):
        model = rec["model"]
    return SessionInfo(vendor, str(rec.get("session_id") or path.stem), path, rec.get("account") or None, mtime, tokens, model)


def last_session(worktree: str | os.PathLike, vendor: str) -> SessionInfo | None:
    """Most recent session of ``vendor`` in ``worktree``: registry first, then a transcript scan."""
    found = _from_registry(worktree, vendor)
    if found is not None:
        return found
    if vendor == "claude":
        return scan_claude(worktree, config.account_dir("claude", "main"))
    if vendor == "codex":
        return scan_codex(worktree, codex_homes())
    return None
