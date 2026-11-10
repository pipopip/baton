"""``bt startup``: reopen the owner's live workstreams as Windows Terminal tabs at logon.

The list lives in STATE_DIR/startup.json and is maintained by the hooks (see ``registry.startup_add``):
SessionStart adds or refreshes the worktree's entry, a deliberate exit (SessionEnd) removes it, a tab
killed by a reboot fires no SessionEnd and stays listed. ``snapshot`` reseeds the list from the sessions
that are live right now. ``run`` opens one Windows Terminal window with one tab per workstream, each
already running ``bt <worktree>``. ``install`` puts a ``.cmd`` in the user's Startup folder that calls
``run`` after a delay at logon.

What ``run`` opens, in order, capped at ``--max``:

1. pinned workstreams from STATE_DIR/startup.toml (``pinned = ["app/feature_design"]``);
2. the listed workstreams whose directory still exists and that were refreshed within the last 7 days,
   newest first.
"""
from __future__ import annotations

import base64
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

from . import config, harvest, launch, registry, worktrees
from .hook import _pid_alive
from .paths import find_worktree_root, key_for, main_checkout
from .sessions import _rollout_meta

PINNED_NAME = "startup.toml"
STARTUP_CMD = "baton-startup.cmd"
MAX_AGE = registry.STARTUP_MAX_AGE   # listed entries not refreshed for this long are skipped (and pruned by the hooks)
DEFAULT_MAX = 8
DEFAULT_DELAY = 30


def snapshot_path() -> Path:
    return registry.startup_path()


def pinned_path() -> Path:
    return config.STATE_DIR / PINNED_NAME


def startup_dir() -> Path:
    """The user's Startup folder (no admin needed); BATON_STARTUP_DIR overrides it."""
    override = os.environ.get("BATON_STARTUP_DIR")
    if override:
        return Path(override)
    appdata = os.environ.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
    return Path(appdata) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"


# -------------------------------------------------------------------- worktree helpers

def is_baton_checkout(root: Path) -> bool:
    return config.norm(main_checkout(root)) == config.norm(config.INSTALL_HOME)


def is_follower_root(root: Path) -> bool:
    """A follower's sibling or scratch worktree: baton-managed, never a startup tab."""
    try:
        return config.norm(main_checkout(root)) != config.norm(root) and worktrees.is_follower_tree(root)
    except Exception:  # noqa: BLE001 - classification is best effort
        return False


def _usable_root(cwd: str | os.PathLike | None) -> Path | None:
    """Worktree root of ``cwd`` when it is a git worktree outside the baton repo."""
    if not cwd:
        return None
    root = find_worktree_root(cwd)
    if root is None or is_baton_checkout(root):
        return None
    return root


# -------------------------------------------------------------------- live sessions

def claude_live() -> list[dict]:
    """Live Claude sessions: ``<config dir>/sessions/<pid>.json`` whose pid is running."""
    found = []
    for account in config.account_names("claude"):
        sessions = config.account_dir("claude", account) / "sessions"
        try:
            files = sorted(sessions.glob("*.json"))
        except OSError:
            continue
        for f in files:
            try:
                rec = json.loads(f.read_text(encoding="utf-8"))
                pid = int(rec.get("pid") or f.stem)
            except (OSError, ValueError, AttributeError, TypeError):
                continue
            if rec.get("kind") not in (None, "interactive") or not _pid_alive(pid):
                continue
            root = _usable_root(rec.get("cwd"))
            if root is not None:
                found.append({"root": root, "vendor": "claude", "account": account, "session_id": rec.get("sessionId")})
    return found


def _lock_held(lock: Path) -> bool:
    """A running Codex holds a byte-range lock on its thread lock file: reading it fails."""
    try:
        with open(lock, "rb") as fh:
            fh.read(1)
        return False
    except OSError:
        return True


def codex_live() -> list[dict]:
    """Live Codex threads: held ``thread-writer-locks/*.lock`` mapped to their rollout's cwd.

    Subagent threads (``source`` is a dict) are skipped.
    """
    found = []
    deadline = time.monotonic() + worktrees.LIVE_LOOKUP_S
    for account in config.account_names("codex"):
        home = config.account_dir("codex", account)
        try:
            locks = sorted((home / "thread-writer-locks").glob("*.lock"))
        except OSError:
            continue
        for lock in locks:
            if not _lock_held(lock):
                continue
            rollout = worktrees.find_rollout(lock.stem, [home], deadline)
            meta = _rollout_meta(rollout) if rollout else None
            if meta is None or isinstance(meta.get("source"), dict):
                continue
            root = _usable_root(meta.get("cwd"))
            if root is not None:
                found.append({"root": root, "vendor": "codex", "account": account, "session_id": lock.stem})
    return found


def take_snapshot(now: float | None = None) -> list[dict]:
    """Live workstreams right now as startup-list entries; Claude wins when a worktree has both vendors."""
    now = time.time() if now is None else now
    seen: dict[str, dict] = {}
    for item in claude_live() + codex_live():
        if not is_follower_root(Path(item["root"])):
            seen.setdefault(config.norm(item["root"]), item)
    return [{"path": str(i["root"]), "key": key_for(i["root"]), "vendor": i["vendor"], "account": i["account"],
             "session_id": i.get("session_id"), "updated_at": now} for i in seen.values()]


# -------------------------------------------------------------------- pinned

def load_pinned() -> list[str]:
    try:
        pinned = config.load_toml(pinned_path()).get("pinned", [])
    except Exception:  # noqa: BLE001 - a hand-edited file with a syntax error must not stop logon
        return []
    return [p for p in pinned if isinstance(p, str) and p.strip()] if isinstance(pinned, list) else []


_PINNED_RE = re.compile(r"^pinned\s*=\s*\[.*?\]", re.S | re.M)


def save_pinned(pinned: list[str]) -> None:
    line = "pinned = [" + ", ".join(json.dumps(p) for p in pinned) + "]"
    path = pinned_path()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        text = ""
    new = _PINNED_RE.sub(lambda _m: line, text, count=1) if _PINNED_RE.search(text) else (text.rstrip("\n") + "\n" if text.strip() else "") + line + "\n"
    harvest.atomic_write(path, new)


# -------------------------------------------------------------------- what run opens

def listed_entries() -> list[dict]:
    """Startup-list entries, newest refresh first. An entry without a timestamp counts as stale."""
    def stamp(e: dict) -> float:
        value = e.get("updated_at")
        return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0
    return sorted(registry.startup_entries(), key=stamp, reverse=True)


def plan_entries(now: float | None = None, limit: int = DEFAULT_MAX) -> list[dict]:
    """Ordered tab list: [{'path', 'key', 'vendor', 'account', 'source'}] (vendor/account None when unknown)."""
    now = time.time() if now is None else now
    entries: list[dict] = []
    seen: set[str] = set()

    def add(path, key, vendor, account, source):
        if not path or not os.path.isdir(path):
            return
        root = find_worktree_root(path)
        if root is None or config.norm(root) != config.norm(path) or is_baton_checkout(root) or is_follower_root(root):
            return
        norm = config.norm(path)
        if norm in seen:
            return
        seen.add(norm)
        entries.append({"path": str(path), "key": key or key_for(Path(path)), "vendor": vendor, "account": account, "source": source})

    for query in load_pinned():
        try:
            root = launch.resolve_worktree(query)
        except LookupError:
            continue
        add(str(root), query, None, None, "pinned")

    for e in listed_entries():
        updated = e.get("updated_at")
        if isinstance(updated, (int, float)) and not isinstance(updated, bool) and now - updated <= MAX_AGE:
            add(e.get("path"), e.get("key"), e.get("vendor"), e.get("account"), "listed")
    return entries[:limit]


# -------------------------------------------------------------------- wt command line

def tab_command(entry: dict) -> str:
    """PowerShell text the tab runs: dot-source bt.ps1, then ``bt '<path>' [-Vendor v] [-Account a]``."""
    def lit(text: str) -> str:
        return "'" + str(text).replace("'", "''") + "'"
    cmd = f". {lit(config.INSTALL_HOME / 'bt.ps1')}; bt {lit(entry['path'])}"
    if entry.get("vendor"):
        cmd += f" -Vendor {lit(entry['vendor'])}"
    if entry.get("account"):
        cmd += f" -Account {lit(entry['account'])}"
    return cmd


def encode_command(text: str) -> str:
    return base64.b64encode(text.encode("utf-16-le")).decode("ascii")


def wt_argv(entries: list[dict], wt: str = "wt.exe") -> list[str]:
    """One wt invocation: a new window whose first tab is ``new-tab`` and the rest ``; new-tab`` segments."""
    argv = [wt, "-w", "new"]
    for i, entry in enumerate(entries):
        if i:
            argv.append(";")
        argv += ["new-tab", "-d", entry["path"], "--title", entry["key"], "--suppressApplicationTitle",
                 "pwsh", "-EncodedCommand", encode_command(launch.managed_tab_command(tab_command(entry)))]
    return argv


# -------------------------------------------------------------------- startup folder

def cmd_text(delay: int = DEFAULT_DELAY) -> str:
    python = config.PYTHON if os.path.isabs(config.PYTHON) else (shutil.which(config.PYTHON) or config.PYTHON)
    bt_py = config.INSTALL_HOME / "bt.py"
    q = lambda p: "'" + str(p).replace("'", "''") + "'"  # noqa: E731 - PowerShell literal
    inner = f"Start-Sleep -Seconds {int(delay)}; & {q(python)} {q(bt_py)} startup run"
    inner = inner.replace("%", "%%")  # cmd expands %var% even inside quotes
    return "\r\n".join(["@echo off", f'start "" /min pwsh -NoProfile -WindowStyle Hidden -Command "{inner}"', ""])


def install(delay: int = DEFAULT_DELAY, directory: Path | None = None) -> Path:
    target = (directory or startup_dir()) / STARTUP_CMD
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="ascii", errors="replace", newline="") as fh:
        fh.write(cmd_text(delay))
    return target


def uninstall(directory: Path | None = None) -> bool:
    target = (directory or startup_dir()) / STARTUP_CMD
    try:
        target.unlink()
        return True
    except FileNotFoundError:
        return False


# -------------------------------------------------------------------- commands

def _fmt_entry(e: dict) -> str:
    who = f"{e['vendor']}/{e['account']}" if e.get("vendor") else "auto"
    return f"{e['key']:40} {who:16} {e['source']:9} {e['path']}"


def _cmd_snapshot(args) -> int:
    live = take_snapshot()
    registry.startup_save(live)
    print(f"[startup] list reseeded with {len(live)} live workstream(s) -> {snapshot_path()}")
    for ws in live:
        print("  " + _fmt_entry({**ws, "source": "live"}))
    return 0


def _cmd_run(args) -> int:
    entries = plan_entries(limit=args.max)
    if not entries:
        print("[startup] nothing to open")
        return 0
    argv = wt_argv(entries, shutil.which("wt.exe") or "wt.exe")
    print(f"[startup] {len(entries)} tab(s):")
    for e in entries:
        print("  " + _fmt_entry(e))
    if args.dry_run:
        print("wt argv: " + subprocess.list2cmdline(argv))
        for e in entries:
            print(f"  tab command ({e['key']}): {tab_command(e)}")
        return 0
    subprocess.Popen(argv, env=config.clean_env(), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                     stderr=subprocess.DEVNULL, close_fds=True)
    print("[startup] opened a Windows Terminal window")
    return 0


def _cmd_show(args) -> int:
    now = time.time()
    listed = listed_entries()
    print(f"list ({snapshot_path()}): {len(listed)} workstream(s), maintained by the session hooks"
          + ("" if listed else "; `bt startup snapshot` reseeds it from live sessions"))
    for e in listed:
        updated = e.get("updated_at")
        age = f"{(now - updated) / 3600:.1f} h ago" if isinstance(updated, (int, float)) and not isinstance(updated, bool) else "no timestamp"
        print("  " + _fmt_entry({**e, "key": e.get("key") or e["path"], "source": "listed"}) + f"  ({age})")
    pinned = load_pinned()
    print(f"pinned ({pinned_path()}): " + (", ".join(pinned) if pinned else "none"))
    entries = plan_entries(limit=args.max)
    print(f"run would open {len(entries)} tab(s) (max {args.max}):")
    for e in entries:
        print("  " + _fmt_entry(e))
    print(f"startup file: {startup_dir() / STARTUP_CMD} ({'installed' if (startup_dir() / STARTUP_CMD).exists() else 'not installed'})")
    return 0


def _cmd_install(args) -> int:
    print(f"[startup] wrote {install(args.delay)} (runs `bt startup run` {args.delay}s after logon)")
    return 0


def _cmd_uninstall(args) -> int:
    print(f"[startup] {'removed' if uninstall() else 'not installed:'} {startup_dir() / STARTUP_CMD}")
    return 0


def _cmd_pin(args) -> int:
    try:
        root = launch.resolve_worktree(args.query)
    except LookupError as exc:
        print(f"bt startup pin: {exc}")
        return 1
    key = launch._label(root)
    pinned = load_pinned()
    if key.lower() not in (p.lower() for p in pinned):
        pinned.append(key)
        save_pinned(pinned)
    print(f"[startup] pinned: {', '.join(pinned)}")
    return 0


def _cmd_unpin(args) -> int:
    pinned = load_pinned()
    q = args.query.strip().replace("\\", "/").lower()
    drop = [p for p in pinned if p.lower() == q] or [p for p in pinned if q in p.lower()]
    if not drop:
        try:
            label = launch._label(launch.resolve_worktree(args.query)).lower()
            drop = [p for p in pinned if p.lower() == label]
        except LookupError:
            pass
    if len(drop) != 1:
        print(f"bt startup unpin: {args.query!r} matches {len(drop)} pinned entries ({', '.join(pinned) or 'none pinned'})")
        return 1
    save_pinned([p for p in pinned if p != drop[0]])
    print(f"[startup] unpinned {drop[0]}")
    return 0


def register(subparsers) -> None:
    p = subparsers.add_parser("startup", help="reopen live workstreams as Windows Terminal tabs at logon")
    sub = p.add_subparsers(dest="startup_cmd", required=True)

    s = sub.add_parser("snapshot", help="reseed the list from the worktrees with a live Claude/Codex session now")
    s.set_defaults(func=_cmd_snapshot)

    s = sub.add_parser("run", help="open one Windows Terminal window with a tab per workstream")
    s.add_argument("--dry-run", action="store_true", help="print the tabs and the wt command line without launching")
    s.add_argument("--max", type=int, default=DEFAULT_MAX)
    s.set_defaults(func=_cmd_run)

    s = sub.add_parser("show", help="the startup list, pinned list and what `run` would open")
    s.add_argument("--max", type=int, default=DEFAULT_MAX)
    s.set_defaults(func=_cmd_show)

    s = sub.add_parser("install", help="add baton-startup.cmd to the Startup folder")
    s.add_argument("--delay", type=int, default=DEFAULT_DELAY, help="seconds to wait after logon (default 30)")
    s.set_defaults(func=_cmd_install)

    s = sub.add_parser("uninstall", help="remove baton-startup.cmd from the Startup folder")
    s.set_defaults(func=_cmd_uninstall)

    s = sub.add_parser("pin", help="always open this worktree first")
    s.add_argument("query")
    s.set_defaults(func=_cmd_pin)

    s = sub.add_parser("unpin", help="remove a pinned worktree")
    s.add_argument("query")
    s.set_defaults(func=_cmd_unpin)
