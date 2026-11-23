"""iTerm2's Python API side of `bt iterm arrange` and `bt iterm prefs --live` (macOS).

Runs in the virtualenv `bt install` makes (``baton.iterm.api_python``), which holds the ``iterm2`` package; started by
``baton.iterm.run_api`` with ``ITERM2_COOKIE`` set, so iTerm2 accepts the connection without a prompt. What to change is
decided by ``baton.iterm.plan_arrangement`` (pure, tested without iTerm2); this module only reads the sessions and
applies the plan. It never closes a tab, a window or a session: tabs move with ``Window.async_set_tabs`` (a window left
without tabs is closed by iTerm2) and a group's first window comes from ``Tab.async_move_to_window``.

    python -m baton.iterm_api arrange [--dry-run] [--quiet] [--settle SECONDS]
    python -m baton.iterm_api prefs
"""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import subprocess
import sys
import time

from . import config, iterm, paths

iterm2 = None      # imported by main() once this process holds the lock: a start that loses the race pays nothing more

LOCK = "iterm-arrange.lock"
DIRTY = "iterm-arrange.dirty"


async def _var(obj, name):
    try:
        return await obj.async_get_variable(name)
    except Exception:  # noqa: BLE001 - a variable iTerm2 does not know reads as missing
        return None


def _cwd(pid) -> str | None:
    """The working directory of process ``pid`` (the session's foreground job): an agent's, unlike iTerm2's ``path``,
    which is the shell's when `bt` was run from elsewhere."""
    try:
        done = subprocess.run(["lsof", "-a", "-p", str(int(pid)), "-d", "cwd", "-Fn"], capture_output=True, text=True,
                              timeout=5)
    except (ValueError, TypeError, OSError, subprocess.SubprocessError):
        return None
    for line in done.stdout.splitlines():
        if line.startswith("n/"):
            return line[1:]
    return None


SESSION_VARS = ("jobName", "path", "jobPid", "profileName", "user.baton_ws", "user.baton_group")


async def _row(window, window_title, tab, tab_title, session) -> dict:
    job, path, pid, profile, ws, group = await asyncio.gather(*(_var(session, name) for name in SESSION_VARS))
    if (job or "").lower() in iterm.AGENTS and not (ws and path and paths.find_worktree_root(path)):
        path = await asyncio.to_thread(_cwd, pid) or path  # the agent's own folder: the shell's may be elsewhere
    return {"session": session.session_id, "tab": tab.tab_id, "window": window.window_id, "profile": profile,
            "job": job, "path": path, "ws": ws, "group": group, "tab_title": tab_title, "window_title": window_title}


async def snapshot(app) -> list[dict]:
    """Every session of every window, with what plan_arrangement reads; the variables are read concurrently."""
    pending = []
    for window in app.terminal_windows:
        window_title = await _var(window, "titleOverrideFormat")
        for tab in window.tabs:
            tab_title = await _var(tab, "titleOverrideFormat")
            pending.extend(_row(window, window_title, tab, tab_title, session) for session in tab.sessions)
    return list(await asyncio.gather(*pending))


async def _profiles(connection) -> dict:
    out = {}
    for partial in await iterm2.PartialProfile.async_query(connection):
        if partial.name in (iterm.profile_name() or iterm.PROFILE_DEFAULT, iterm.SHELL_PROFILE):
            out[partial.name] = await partial.async_get_full_profile()
    return out


async def arrange(connection, dry_run: bool, quiet: bool) -> int:
    app = await iterm2.async_get_app(connection)
    plan = iterm.plan_arrangement(await snapshot(app))
    if dry_run or not quiet:
        print(json.dumps(plan, indent=2, ensure_ascii=False))
    if dry_run:
        return 0
    profiles = await _profiles(connection) if any(i.get("profile") for i in plan["sessions"]) else {}
    for item in plan["sessions"]:
        session = app.get_session_by_id(item["session"])
        if session is None:
            continue
        if item.get("profile") in profiles:
            await session.async_set_profile(profiles[item["profile"]])
        if item.get("inject"):
            await session.async_inject(item["inject"].encode("utf-8"))
        if item.get("title"):
            tab = app.get_tab_by_id(item["tab"])
            if tab is not None:
                await tab.async_set_title(item["title"])
    for entry in plan["windows"]:
        tabs = [t for t in (app.get_tab_by_id(tid) for tid in entry["tabs"]) if t is not None]
        if len(tabs) < 2:
            continue
        window = app.get_window_by_id(entry["window"]) if entry["window"] else None
        if window is None:
            home = app.get_window_for_tab(tabs[0].tab_id)
            if home is not None and all(t.tab_id in entry["tabs"] for t in home.tabs):
                window = home                                   # already alone with its group: keep that window
            else:
                await tabs[0].async_move_to_window()
                app = await iterm2.async_get_app(connection)    # the move made a window: read the app again
                window = app.get_window_for_tab(tabs[0].tab_id)
                tabs = [t for t in (app.get_tab_by_id(tid) for tid in entry["tabs"]) if t is not None]
            if window is None:
                continue
            await window.async_set_title(entry["group"])
        current = [t.tab_id for t in window.tabs][: len(tabs)]
        if current != [t.tab_id for t in tabs]:
            await window.async_set_tabs(tabs)
    return 0


async def prefs(connection) -> int:
    for key, kind, value, _why in iterm.PREFS:
        typed = {"int": int, "float": float, "bool": lambda v: v == "true", "string": str}[kind](value)
        await iterm2.async_set_preference(connection, key, typed)
        print(f"set {key} = {value}")
    if iterm.look_name() != "inherit":
        for partial in await iterm2.PartialProfile.async_query(connection):
            if partial.guid == iterm.SHELL_GUID:
                await (await partial.async_get_full_profile()).async_make_default()
                print(f"default profile = {iterm.SHELL_PROFILE}")
    return 0


def _locked(path):
    handle = open(path, "a")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return None
    return handle


def _pass(args) -> None:
    iterm2.run_until_complete(lambda c: arrange(c, args.dry_run, args.quiet), retry=False)


def main(argv=None) -> int:
    global iterm2
    ap = argparse.ArgumentParser(prog="baton.iterm_api")
    ap.add_argument("action", choices=["arrange", "prefs"])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--settle", type=float, default=0.0, help="seconds to wait first (a starting session sets its variables)")
    args = ap.parse_args(argv)
    if args.action == "prefs":
        import iterm2 as module
        iterm2 = module
        return iterm2.run_until_complete(prefs, retry=False) or 0
    if args.dry_run:
        import iterm2 as module
        iterm2 = module
        _pass(args)
        return 0
    config.STATE_DIR.mkdir(parents=True, exist_ok=True)
    dirty, lock_path = config.STATE_DIR / DIRTY, config.STATE_DIR / LOCK
    dirty.touch()                                       # this start asks for a pass, whoever makes it
    time.sleep(max(0.0, args.settle))
    while True:
        lock = _locked(lock_path)
        if lock is None:                                # an arranger is running: its dirty check sees this start
            return 0
        try:
            if iterm2 is None:
                import iterm2 as module
                iterm2 = module
            for _ in range(3):
                started = time.time()
                _pass(args)
                if dirty.stat().st_mtime < started:
                    break
                time.sleep(1.0)
        finally:
            lock.close()
        try:                                            # a start that lost the race after the last check: go again
            if dirty.stat().st_mtime < started:
                return 0
        except FileNotFoundError:
            return 0


if __name__ == "__main__":
    sys.exit(main())
