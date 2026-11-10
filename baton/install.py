"""``bt install``: wire baton into Claude Code, Codex and PowerShell.

Idempotent. Every file it changes is backed up once per run as
``<name>.bak-baton-<timestamp>``; baton's own entries are recognised by the
``baton`` paths in their commands and replaced in place, never duplicated.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import time
from pathlib import Path

from . import config, wtguard


INSTALL_HOME = config.INSTALL_HOME
HOOK_SCRIPT = (INSTALL_HOME / "hook.py").as_posix()
CYCLE_SCRIPT = (INSTALL_HOME / "cycle_hook.py").as_posix()   # Stop + PreToolUse(Agent|Workflow): context cycling
INBOX_CMD = INSTALL_HOME / "inbox_check.cmd"
WT_GUARD = INSTALL_HOME / "wt_guard.cmd"              # PreToolUse gate for Claude Code (exit code 2 blocks)
WT_GUARD_CODEX = INSTALL_HOME / "wt_guard_codex.cmd"  # same gate for Codex (blocks through a JSON decision)
DENIED_TOOLS = ("EnterWorktree",)                      # Claude permissions.deny entries baton adds
PROFILE_MARK = "# baton (bt command)"
CLAUDE_SETTINGS = Path.home() / ".claude" / "settings.json"
CODEX_HOOKS = Path.home() / ".codex" / "hooks.json"
PS_PROFILE = Path.home() / "Documents" / "PowerShell" / "Microsoft.PowerShell_profile.ps1"
CMD_EXE = r"C:\Windows\System32\cmd.exe"
# Baton hook entries, wherever the checkout lived when they were installed.
_OURS = re.compile(r"baton[^\s\"']*/(hook\.py|cycle_hook\.py|inbox_check\.cmd|wt_guard(?:_codex)?\.cmd)\b")


def _python() -> str:
    return Path(config.PYTHON).as_posix()


def _is_ours(handler: dict) -> bool:
    text = " ".join([str(handler.get("command", ""))] + [str(a) for a in handler.get("args", [])])
    return bool(_OURS.search(text.replace("\\", "/").lower()))


def _claude_handlers() -> dict[str, dict | list[dict]]:
    """Event -> matcher group (or list of groups) for Claude Code (exec form: command + args, no shell)."""
    py = {"type": "command", "command": _python(), "args": ["-S", HOOK_SCRIPT, "claude"], "timeout": 10}
    cycle = {"type": "command", "command": _python(), "args": ["-S", CYCLE_SCRIPT, "stop"], "timeout": 60,
             "statusMessage": "baton cycle check"}
    return {
        "SessionStart": {"matcher": "startup|resume|clear|compact|fork", "hooks": [dict(py, statusMessage="baton capsule")]},
        "PostCompact": {"hooks": [dict(py)]},
        "SessionEnd": {"hooks": [dict(py)]},
        "UserPromptSubmit": {"hooks": [{"type": "command", "command": CMD_EXE,
                                        "args": ["/d", "/c", str(INBOX_CMD), "claude"], "timeout": 10}]},
        "PreToolUse": [
            {"matcher": "Bash", "hooks": [{"type": "command", "command": CMD_EXE, "args": ["/d", "/c", str(WT_GUARD)], "timeout": 10}]},
            {"matcher": "Agent|Workflow", "hooks": [dict(cycle, args=["-S", CYCLE_SCRIPT, "pretool"], timeout=15,
                                                         statusMessage="baton cycle gate")]},
        ],
        "Stop": {"hooks": [cycle]},
    }


def _codex_handlers() -> dict[str, dict]:
    """Event -> matcher group for Codex hooks.json (string commands)."""
    py = f"{_python()} -S {HOOK_SCRIPT} codex"
    base = {"type": "command", "command": py, "timeout": 10}
    return {
        "SessionStart": {"matcher": "^(startup|resume|clear|compact)$", "hooks": [dict(base, additionalContextLimit=2400)]},
        "PostCompact": {"hooks": [dict(base)]},
        "SessionEnd": {"hooks": [dict(base, timeout=3)]},  # Codex clamps SessionEnd to 3 s
        "UserPromptSubmit": {"hooks": [{"type": "command", "command": f"{CMD_EXE} /d /c {INBOX_CMD} codex",
                                        "timeout": 10, "additionalContextLimit": 1000}]},
        "PreToolUse": {"matcher": "Bash", "hooks": [{"type": "command", "command": f"{CMD_EXE} /d /c {WT_GUARD_CODEX}",
                                                     "timeout": 10}]},
    }


def merge_hooks(hooks: dict, wanted: dict[str, dict]) -> dict:
    """Return ``hooks`` with baton's handlers replaced by ``wanted`` (other handlers untouched)."""
    out = {}
    for event, groups in hooks.items():
        kept = []
        for group in groups:
            handlers = [h for h in group.get("hooks", []) if not _is_ours(h)]
            if handlers:
                kept.append(dict(group, hooks=handlers))
        out[event] = kept
    for event, group in wanted.items():
        out.setdefault(event, []).extend(group if isinstance(group, list) else [group])
    return {k: v for k, v in out.items() if v}


def _backup(path: Path, stamp: str) -> None:
    backup = path.with_name(f"{path.name}.bak-baton-{stamp}")
    if path.exists() and not backup.exists():
        shutil.copy2(path, backup)


def _write_json(path: Path, data: dict) -> None:
    tmp = path.with_name(path.name + ".baton-tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    os.replace(tmp, path)


def merge_deny(permissions: dict, tools) -> dict:
    """``permissions`` with every tool in ``tools`` present in ``deny`` (order kept, nothing duplicated)."""
    deny = list(permissions.get("deny") or [])
    deny += [t for t in tools if t not in deny]
    return dict(permissions, deny=deny)


def install_json_hooks(path: Path, wanted: dict[str, dict], stamp: str, dry_run: bool, deny=()) -> str:
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    new_hooks = merge_hooks(data.get("hooks", {}), wanted)
    new_perms = merge_deny(data.get("permissions") or {}, deny) if deny else data.get("permissions")
    if new_hooks == data.get("hooks") and new_perms == data.get("permissions"):
        return f"unchanged {path}"
    what = ", ".join(sorted(wanted)) + (f"; deny {', '.join(deny)}" if deny else "")
    if dry_run:
        return f"would update hooks in {path}: {what}"
    _backup(path, stamp)
    data["hooks"] = new_hooks
    if deny:
        data["permissions"] = new_perms
    _write_json(path, data)
    return f"updated hooks in {path}: {what}"


def inbox_cmd_text() -> str:
    """UserPromptSubmit entry: runs the hook on every prompt.

    The hook records the session's context size and may add a hand-over nudge, so it cannot wait for an
    unread-inbox flag any more; Python starts with ``-S`` and imports only the hook modules (about 60 ms).
    """
    python = str(Path(config.PYTHON))
    lines = [
        "@echo off",
        "rem Generated by `bt install`. Runs the UserPromptSubmit hook (context size, follower results).",
        f'"{python}" -S "%~dp0hook.py" %1',
        "exit /b 0",
    ]
    return "\r\n".join(lines) + "\r\n"


def _write_cmd(target: Path, text: str, dry_run: bool) -> str:
    if target.exists() and target.read_bytes() == text.encode("utf-8"):
        return f"unchanged {target}"
    if dry_run:
        return f"would write {target}"
    with open(target, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)
    return f"wrote {target}"


def write_inbox_cmd(dry_run: bool) -> str:
    return _write_cmd(INBOX_CMD, inbox_cmd_text(), dry_run)


def wt_guard_message() -> str:
    return wtguard.message(str(INSTALL_HOME))


def wt_guard_text(vendor: str = "claude") -> str:
    """PreToolUse gate: runs ``wt_guard.py``, which parses the tool payload and blocks only a real
    ``git worktree add`` command (not text that merely mentions it).

    Claude Code blocks a tool call on exit code 2 with the message on stderr (checked with claude 2.1.296).
    Codex 0.160.1 treats exit code 2 as a failed hook and still runs the command; it blocks on a
    ``{"decision": "block", "reason": ...}`` object on stdout with exit code 0. The script makes that
    difference itself, from its ``claude``/``codex`` argument, and allows the command on any error.
    """
    lines = ["@echo off", "rem Generated by `bt install`. Blocks agents' raw `git worktree add`; the lead uses `bt wt new`.",
             f'"{Path(config.PYTHON)}" -S "%~dp0wt_guard.py" {"codex" if vendor == "codex" else "claude"}',
             "exit /b %errorlevel%"]
    return "\r\n".join(lines) + "\r\n"


def write_wt_guards(dry_run: bool) -> list[str]:
    return [_write_cmd(WT_GUARD, wt_guard_text("claude"), dry_run),
            _write_cmd(WT_GUARD_CODEX, wt_guard_text("codex"), dry_run)]


def install_profile(stamp: str, dry_run: bool) -> str:
    script = (INSTALL_HOME / "bt.ps1").as_posix().replace("'", "''")
    line = f". '{script}'  {PROFILE_MARK}"
    text = PS_PROFILE.read_text(encoding="utf-8") if PS_PROFILE.exists() else ""
    lines = [l for l in text.splitlines() if PROFILE_MARK not in l]
    new = "\n".join(lines + [line]) + "\n"
    if new == text:
        return f"unchanged {PS_PROFILE}"
    if dry_run:
        return f"would add bt to {PS_PROFILE}"
    _backup(PS_PROFILE, stamp)
    PS_PROFILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PS_PROFILE, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(new)
    return f"added bt to {PS_PROFILE}"


def run(args) -> int:
    targets = {t for t in ("claude", "codex", "profile", "routing", "lane") if getattr(args, t, False)}
    targets = targets or {"claude", "codex", "profile", "routing"}
    if INSTALL_HOME != config.BATON_HOME and not args.dry_run:
        print(f"error: run `bt install` from the main checkout {INSTALL_HOME} (after merging), not from a worktree")
        return 1
    if any(" " in p for p in (HOOK_SCRIPT, _python(), str(INBOX_CMD), str(WT_GUARD), str(WT_GUARD_CODEX))):
        print("error: baton and Python paths must not contain spaces (hook commands are unquoted for Codex)")
        return 1
    stamp = time.strftime("%Y%m%d-%H%M%S")
    actions = [write_inbox_cmd(args.dry_run), *write_wt_guards(args.dry_run)] if targets != {"lane"} else []
    if "lane" in targets:
        from . import lane
        try:
            actions.extend(lane.install(stamp, args.dry_run, INSTALL_HOME))
        except (OSError, ValueError) as exc:
            print(f"error: {exc}")
            return 1
        if "routing" not in targets:
            from . import routing
            actions.extend(("would " if args.dry_run else "") + action
                           for action in routing.apply(dry_run=args.dry_run))
    if "claude" in targets:
        actions.append(install_json_hooks(CLAUDE_SETTINGS, _claude_handlers(), stamp, args.dry_run, deny=DENIED_TOOLS))
    if "codex" in targets:
        actions.append(install_json_hooks(CODEX_HOOKS, _codex_handlers(), stamp, args.dry_run))
        if not args.dry_run:
            actions.append("Codex: open `codex` and approve the new hooks with /hooks (hook trust is per hooks.json).")
    if "routing" in targets:
        from . import routing
        actions.extend(("would " if args.dry_run else "") + a for a in routing.apply(dry_run=args.dry_run))
    if "profile" in targets:
        actions.append(install_profile(stamp, args.dry_run))
    others = [f"{v}:{a}" for v in config.VENDORS for a in config.account_names(v) if a != "main"]
    if others:
        actions.append("Other accounts pick up these hooks when `bt` next launches them (it syncs settings), "
                       f"or run `bt account link <vendor> <name>` now: {', '.join(others)}.")
    for action in actions:
        print(action)
    return 0


def register(sub) -> None:
    p = sub.add_parser("install", help="wire hooks, role agents and the bt shell command")
    for name in ("claude", "codex", "profile", "routing", "lane"):
        p.add_argument(f"--{name}", action="store_true", help=f"only the {name} part")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=run)
