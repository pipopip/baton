"""``bt install``: wire baton into Claude Code, Codex and PowerShell.

Idempotent. Every file it changes is backed up once per run as
``<name>.bak-baton-<timestamp>``; baton's own entries are recognised by the
``baton`` paths in their commands and replaced in place, never duplicated.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import stat
import sys
import tempfile
import time
from pathlib import Path

from . import config, hook, wtguard


INSTALL_HOME = config.INSTALL_HOME
HOOK_SCRIPT = (INSTALL_HOME / "hook.py").as_posix()
CYCLE_SCRIPT = (INSTALL_HOME / "cycle_hook.py").as_posix()   # Stop + PreToolUse(Agent|Workflow): context cycling
INBOX_CMD = INSTALL_HOME / "inbox_check.cmd"          # Windows: UserPromptSubmit through cmd.exe (always exits 0)
WT_GUARD = INSTALL_HOME / "wt_guard.cmd"              # Windows: PreToolUse gate for Claude Code (exit code 2 blocks)
WT_GUARD_CODEX = INSTALL_HOME / "wt_guard_codex.cmd"  # Windows: same gate for Codex (blocks through a JSON decision)
INBOX_PY = INSTALL_HOME / "inbox_check.py"            # elsewhere the same two entries run Python directly, no shell
WT_GUARD_PY = INSTALL_HOME / "wt_guard.py"            # (the gate takes claude|codex as its argument on both platforms)
BT_SH = INSTALL_HOME / "bt.sh"
WORKFLOW_HOOK_SH = INSTALL_HOME / "workflow_hook.sh"   # session-workflow's PostToolUse hook off Windows (hook.cmd's twin)
PLUGIN_DIR = INSTALL_HOME / "mods" / "compute-queue"   # the compute-queue plugin: Claude loads it through CLAUDE_CODE_PLUGIN_DIRS
DENIED_TOOLS = ("EnterWorktree",)                      # Claude permissions.deny entries baton adds
PROFILE_MARK = "# baton (bt command)"
CLAUDE_SETTINGS = Path.home() / ".claude" / "settings.json"
CODEX_HOOKS = Path.home() / ".codex" / "hooks.json"
PS_PROFILE = Path.home() / "Documents" / "PowerShell" / "Microsoft.PowerShell_profile.ps1"
CMD_EXE = r"C:\Windows\System32\cmd.exe"
# Baton hook entries, wherever the checkout lived when they were installed.
# Both platforms' forms (.cmd wrappers on Windows, the .py scripts run directly elsewhere), so a settings file that
# carries the other platform's entries is cleaned on install too.
_OURS = re.compile(r"baton[^\s\"']*/(hook\.py|cycle_hook\.py|inbox_check\.(?:cmd|py)|wt_guard(?:_codex)?\.cmd|wt_guard\.py)\b")


_SCRIPT_NAMES = re.compile(_OURS.pattern[_OURS.pattern.index("/"):])


def _python() -> str:
    return Path(config.PYTHON).as_posix()


def _is_ours(handler: dict) -> bool:
    text = " ".join([str(handler.get("command", ""))] + [str(a) for a in handler.get("args", [])])
    text = text.replace("\\", "/").lower()
    if _OURS.search(text):
        return True
    # A checkout whose directory is not called baton (for example ~/src/bt): its scripts under INSTALL_HOME.
    home = INSTALL_HOME.as_posix().lower().rstrip("/") + "/"
    return home in text and bool(_SCRIPT_NAMES.search(text))


def _claude_handlers() -> dict[str, dict | list[dict]]:
    """Event -> matcher group (or list of groups) for Claude Code (exec form: command + args, no shell).

    The UserPromptSubmit and PreToolUse entries go through cmd.exe and the generated .cmd wrappers on Windows and
    run Python directly elsewhere."""
    py = {"type": "command", "command": _python(), "args": ["-S", HOOK_SCRIPT, "claude"], "timeout": 10}
    cycle = {"type": "command", "command": _python(), "args": ["-S", CYCLE_SCRIPT, "stop"], "timeout": 60,
             "statusMessage": "baton cycle check"}
    return {
        "SessionStart": {"matcher": "startup|resume|clear|compact|fork", "hooks": [dict(py, statusMessage="baton capsule")]},
        "PostCompact": {"hooks": [dict(py)]},
        "SessionEnd": {"hooks": [dict(py)]},
        "UserPromptSubmit": {"hooks": [{"type": "command", "command": CMD_EXE,
                                        "args": ["/d", "/c", str(INBOX_CMD), "claude"], "timeout": 10}
                                       if config.WINDOWS else
                                       {"type": "command", "command": _python(),
                                        "args": ["-S", INBOX_PY.as_posix(), "claude"], "timeout": 10}]},
        "PreToolUse": [
            {"matcher": "Bash", "hooks": [{"type": "command", "command": CMD_EXE, "args": ["/d", "/c", str(WT_GUARD)], "timeout": 10}
                                          if config.WINDOWS else
                                          {"type": "command", "command": _python(),
                                           "args": ["-S", WT_GUARD_PY.as_posix(), "claude"], "timeout": 10}]},
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
        "UserPromptSubmit": {"hooks": [{"type": "command",
                                        "command": (f"{CMD_EXE} /d /c {INBOX_CMD} codex" if config.WINDOWS
                                                    else f"{_python()} -S {INBOX_PY.as_posix()} codex"),
                                        "timeout": 10, "additionalContextLimit": 1000}]},
        "PreToolUse": {"matcher": "Bash", "hooks": [{"type": "command",
                                                     "command": (f"{CMD_EXE} /d /c {WT_GUARD_CODEX}" if config.WINDOWS
                                                                 else f"{_python()} -S {WT_GUARD_PY.as_posix()} codex"),
                                                     "timeout": 10}]},
    }


# --------------------------------------------------------------------------- session-workflow
# A sibling tool: a context cycle refuses unless the session is registered there, and peer notices arrive through its
# hooks. On Windows its entries were merged by hand (hooks.proposed.json) and stay as they are; off Windows they are
# written here, with this machine's paths, when the tool's checkout is present.

_SW_MARKS = ("session-workflow/workflow.py", "session-workflow/hook.cmd")
# Baton's POSIX wrapper for it, named the way _OURS names Baton's scripts (a directory called like a Baton checkout, a
# checkout that moved from another machine included) but ending the file name exactly: another tool's
# ``my_workflow_hook.sh`` or ``workflow_hook.sh.bak`` is not session-workflow's.
_SW_WRAPPER = re.compile(r"baton[^\s\"']*/workflow_hook\.sh(?=$|[\s\"'])")


def session_workflow_dir() -> Path:
    return Path(hook.SESSION_WORKFLOW)


def session_workflow_enabled() -> bool:
    """Whether ``bt install`` manages session-workflow's hook entries: off Windows, with the tool's checkout present."""
    return not config.WINDOWS and (session_workflow_dir() / "workflow.py").exists()


def is_session_workflow(handler: dict) -> bool:
    """A handler that runs session-workflow, in the Windows or the POSIX form, whatever its directory is called."""
    text = " ".join([str(handler.get("command", ""))] + [str(a) for a in handler.get("args", [])])
    text = text.replace("\\", "/").lower()
    if any(mark in text for mark in _SW_MARKS) or _SW_WRAPPER.search(text):
        return True
    wrapper = re.escape(WORKFLOW_HOOK_SH.as_posix().lower())     # this checkout's, wherever it is called
    if re.search(rf"(?<![^\s\"']){wrapper}(?=$|[\s\"'])", text):
        return True
    return session_workflow_dir().as_posix().lower().rstrip("/") + "/" in text


def session_workflow_paths() -> list[str]:
    """Every path the entries name except the Python (that one is checked with baton's own)."""
    sw = session_workflow_dir()
    return [sw.as_posix(), (sw / "workflow.py").as_posix(), (sw / ".state" / "workflow.sqlite3").as_posix(),
            WORKFLOW_HOOK_SH.as_posix()]


def session_workflow_handlers(vendor: str) -> dict[str, dict]:
    """Event -> matcher group for session-workflow in the form of ``vendor``: exec form (command + args) for Claude
    Code, plain strings for Codex. The events, matchers and limits of the hand-merged Windows entries."""
    sw = session_workflow_dir()
    direct = ["-S", (sw / "workflow.py").as_posix(), "--state", (sw / ".state" / "workflow.sqlite3").as_posix(), "hook"]
    shell = [WORKFLOW_HOOK_SH.as_posix(), sw.as_posix(), _python()]
    if vendor == "claude":
        run = {"type": "command", "command": _python(), "args": direct, "timeout": 10}
        tool = {"type": "command", "command": "/bin/sh", "args": shell, "timeout": 10}
        return {
            "SessionStart": {"matcher": "startup|resume|compact", "hooks": [dict(run)]},
            "UserPromptSubmit": {"hooks": [dict(run)]},
            "SessionEnd": {"hooks": [dict(run)]},
            "PostToolUse": {"hooks": [tool]},
        }
    run = {"type": "command", "command": " ".join([_python(), *direct]), "timeout": 10}
    tool = {"type": "command", "command": " ".join(["/bin/sh", *shell]), "timeout": 10}
    limited = {"additionalContextLimit": 1600}
    return {
        "SessionStart": {"matcher": "^(startup|resume|compact)$", "hooks": [dict(run, **limited)]},
        "UserPromptSubmit": {"hooks": [dict(run, **limited)]},
        "PreCompact": {"hooks": [dict(run, **limited)]},
        "SessionEnd": {"hooks": [dict(run)]},
        "PostToolUse": {"matcher": ".*", "hooks": [dict(tool, **limited)]},
    }


def _combine(wanted: dict, extra: dict) -> dict:
    out = {event: (list(g) if isinstance(g, list) else [g]) for event, g in wanted.items()}
    for event, group in extra.items():
        out.setdefault(event, []).extend(group if isinstance(group, list) else [group])
    return out


def merge_hooks(hooks: dict, wanted: dict[str, dict], replace_session_workflow: bool = False) -> dict:
    """Return ``hooks`` with baton's handlers replaced by ``wanted`` (other handlers untouched). With
    ``replace_session_workflow`` session-workflow's handlers, in any form, are dropped as well: ``wanted`` then
    carries their replacements."""
    out = {}
    for event, groups in hooks.items():
        kept = []
        for group in groups:
            handlers = [h for h in group.get("hooks", [])
                        if not _is_ours(h) and not (replace_session_workflow and is_session_workflow(h))]
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
    path.parent.mkdir(parents=True, exist_ok=True)   # ~/.codex does not exist before Codex first runs
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


def plugin_env() -> dict[str, str]:
    """The ``env`` entries the compute-queue plugin needs off Windows. On Windows they are set by hand: the hook's
    default ``python`` is right there, and the plugin directory is added to CLAUDE_CODE_PLUGIN_DIRS once."""
    return {"CQ_PYTHON": _python(), "CLAUDE_CODE_PLUGIN_DIRS": PLUGIN_DIR.as_posix()}


def _of_another_machine(value: str, exists=os.path.exists) -> bool:
    """A path that cannot be this machine's: it has a directory part (a drive letter included) and is not there."""
    value = value.strip()
    return bool(re.match(r"^[A-Za-z]:", value) or "/" in value or "\\" in value) and not exists(value)


def _norm_dir(text: str) -> str:
    return text.replace("\\", "/").rstrip("/")


def plugin_dir_entries(value: str) -> list[str]:
    """The entries of a CLAUDE_CODE_PLUGIN_DIRS value: split at ``;`` and at the platform's ``os.pathsep``, except
    that a Windows drive letter (``C:/dir``, ``C:\dir``) is not split at its colon."""
    out: list[str] = []
    for chunk in value.split(";"):
        parts = chunk.split(os.pathsep) if os.pathsep != ";" else [chunk]
        i = 0
        while i < len(parts):
            part = parts[i]
            if len(part) == 1 and part.isalpha() and i + 1 < len(parts) and parts[i + 1][:1] in ("/", "\\"):
                part, i = part + os.pathsep + parts[i + 1], i + 1
            out.append(part)
            i += 1
    return [e.strip() for e in out if e.strip()]


def names_plugin_dir(value: str, wanted: str) -> bool:
    """Whether the list ``value`` has ``wanted`` as one of its entries (compared as whole, normalised paths: a
    directory whose name merely starts with it, ``compute-queue-old``, is another entry)."""
    want = _norm_dir(wanted).lower()
    return any(_norm_dir(e).lower() == want for e in plugin_dir_entries(value))


def merge_env(env: dict, wanted: dict[str, str], exists=os.path.exists, which=shutil.which) -> tuple[dict, list[str]]:
    """``env`` with the plugin entries set (absent), kept (equal, or a different value that is right as it stands) or
    replaced (the old value is a path of another machine, so it cannot work here). Returns the new env and one line
    for each entry left as it is that still needs a hand edit. Other keys are never touched."""
    out, notes = dict(env), []
    python, dirs = wanted["CQ_PYTHON"], wanted["CLAUDE_CODE_PLUGIN_DIRS"]
    old = env.get("CQ_PYTHON")
    if not isinstance(old, str) or not old.strip():
        out["CQ_PYTHON"] = python
    elif old != python:
        if "/" in old or "\\" in old:
            if not exists(old):  # a missing interpreter is a path of another machine (or a stale one): it cannot be kept
                out["CQ_PYTHON"] = python
        elif not which(old):
            notes.append(f"env CQ_PYTHON is {old!r}, which is not on PATH: set it to {python} by hand")
    old = env.get("CLAUDE_CODE_PLUGIN_DIRS")
    if not isinstance(old, str) or not old.strip():
        out["CLAUDE_CODE_PLUGIN_DIRS"] = dirs
    elif old != dirs and not names_plugin_dir(old, dirs):
        entries = plugin_dir_entries(old)
        if all(_of_another_machine(e, exists) for e in entries):
            out["CLAUDE_CODE_PLUGIN_DIRS"] = dirs
        else:
            notes.append(f"env CLAUDE_CODE_PLUGIN_DIRS is {old!r} and does not name {dirs}: add it by hand "
                         f"({old}{os.pathsep}{dirs})")
    return out, notes


def install_json_hooks(path: Path, wanted: dict[str, dict], stamp: str, dry_run: bool, deny=(),
                       session_workflow: dict | None = None, plugin_env_wanted: dict[str, str] | None = None,
                       notes: list[str] | None = None) -> str:
    """``session_workflow`` (event -> group, from ``session_workflow_handlers``) adds those entries and replaces
    any session-workflow entry the file already has. ``plugin_env_wanted`` (from ``plugin_env``) sets the plugin's
    ``env`` entries by ``merge_env``; the lines it leaves for a hand edit are appended to ``notes``."""
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    if session_workflow is not None:
        wanted = _combine(wanted, session_workflow)
    new_hooks = merge_hooks(data.get("hooks", {}), wanted, replace_session_workflow=session_workflow is not None)
    new_perms = merge_deny(data.get("permissions") or {}, deny) if deny else data.get("permissions")
    old_env = data.get("env") if isinstance(data.get("env"), dict) else {}
    new_env = old_env
    if plugin_env_wanted is not None:
        new_env, found = merge_env(old_env, plugin_env_wanted)
        if notes is not None:
            notes.extend(found)
    env_changed = new_env != old_env
    if new_hooks == data.get("hooks") and new_perms == data.get("permissions") and not env_changed:
        return f"unchanged {path}"
    what = ", ".join(sorted(wanted)) + (f"; deny {', '.join(deny)}" if deny else "")
    if env_changed:
        what += "; env " + ", ".join(sorted(k for k in new_env if new_env.get(k) != old_env.get(k)))
    if dry_run:
        return f"would update hooks in {path}: {what}"
    _backup(path, stamp)
    data["hooks"] = new_hooks
    if deny:
        data["permissions"] = new_perms
    if env_changed:
        data["env"] = new_env
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


def posix_rc_file() -> Path:
    """The rc file of the login shell: ~/.bashrc when $SHELL is bash, else ~/.zshrc (zsh is the macOS default; bash
    is the default elsewhere only when $SHELL names nothing)."""
    shell = os.path.basename(os.environ.get("SHELL", ""))
    if shell == "bash" or (not shell and sys.platform != "darwin"):
        return Path.home() / ".bashrc"
    return Path.home() / ".zshrc"


def posix_source_text() -> str:
    """The command that defines ``bt`` in a POSIX shell: bt.sh, the checkout it lives in and the Python this install
    runs (bt.sh uses both). The profile line is this text plus the mark; a new terminal tab runs it too, so the tab
    never depends on the user's rc file."""
    return f". {shlex.quote(BT_SH.as_posix())} {shlex.quote(INSTALL_HOME.as_posix())} {shlex.quote(_python())}"


def posix_profile_line() -> str:
    """The one marked line of the rc file."""
    return f"{posix_source_text()}  {PROFILE_MARK}"


def profile_target() -> Path:
    return PS_PROFILE if config.WINDOWS else posix_rc_file()


def read_rc(target: Path) -> str:
    """The rc file as text that encodes back to exactly the same bytes (``surrogateescape``: bytes that are not UTF-8
    are carried through; newlines are not translated). A file with NUL bytes is not a shell rc file (a UTF-16 profile,
    say) and is refused rather than extended with UTF-8 text."""
    if not target.exists():
        return ""
    data = target.read_bytes()
    if b"\0" in data:
        raise ValueError(f"{target} holds NUL bytes, so it is not a UTF-8 text file: add the line by hand")
    return data.decode("utf-8", errors="surrogateescape")


def write_rc(target: Path, text: str) -> None:
    """Replace the rc file atomically: a temp file in its directory, then ``os.replace``, so an interrupted write
    leaves the old file. A symlinked file (a dotfile manager's) is written through to its target, the link kept; the
    mode of an existing file is kept (0644 for a new one)."""
    if target.is_symlink():
        target = Path(os.path.realpath(target))
    target.parent.mkdir(parents=True, exist_ok=True)
    mode = stat.S_IMODE(target.stat().st_mode) if target.exists() else 0o644
    fd, tmp_name = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.baton-")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(text.encode("utf-8", errors="surrogateescape"))
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, target)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def install_profile(stamp: str, dry_run: bool, text: str | None = None) -> str:
    """Add (or replace, never duplicate) the marked line that gives the shell its ``bt`` command: bt.ps1 in the
    PowerShell profile on Windows; bt.sh in ~/.zshrc or ~/.bashrc elsewhere. The PowerShell profile is never
    touched off Windows."""
    if config.WINDOWS:
        script = (INSTALL_HOME / "bt.ps1").as_posix().replace("'", "''")
        line = f". '{script}'  {PROFILE_MARK}"
    else:
        line = posix_profile_line()
    target = profile_target()
    if text is None:
        text = read_rc(target)
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()                                    # the file's final newline, put back below
    lines = [l for l in lines if PROFILE_MARK not in l]
    eol = "\r" if "\r\n" in text else ""               # a CRLF file keeps its line ends, the new line included
    new = "\n".join(lines + [line + eol]) + "\n"
    if new == text:
        return f"unchanged {target}"
    if dry_run:
        return f"would add bt to {target}"
    _backup(target, stamp)
    write_rc(target, new)
    return f"added bt to {target}"


def run(args) -> int:
    targets = {t for t in ("claude", "codex", "profile", "routing", "lane") if getattr(args, t, False)}
    targets = targets or {"claude", "codex", "profile", "routing"}
    if INSTALL_HOME != config.BATON_HOME and not args.dry_run:
        print(f"error: run `bt install` from the main checkout {INSTALL_HOME} (after merging), not from a worktree")
        return 1
    hook_paths = ((HOOK_SCRIPT, _python(), str(INBOX_CMD), str(WT_GUARD), str(WT_GUARD_CODEX)) if config.WINDOWS
                  else (HOOK_SCRIPT, _python(), INBOX_PY.as_posix(), WT_GUARD_PY.as_posix()))
    sw = session_workflow_enabled()
    if sw and targets & {"claude", "codex"}:
        hook_paths += tuple(session_workflow_paths())
    if any(" " in p for p in hook_paths):
        print("error: baton and Python paths must not contain spaces (hook commands are unquoted for Codex)")
        return 1
    rc_text, failed = None, False
    if "profile" in targets:        # read first: a file that cannot be read stops the install before anything is written
        try:
            rc_text = read_rc(profile_target())
        except (OSError, ValueError) as exc:
            print(f"error: cannot read {profile_target()}: {exc}; nothing was changed")
            return 1
    stamp = time.strftime("%Y%m%d-%H%M%S")
    # The .cmd wrappers exist only for the Windows hook entries; off Windows nothing is written for them.
    actions = ([write_inbox_cmd(args.dry_run), *write_wt_guards(args.dry_run)]
               if config.WINDOWS and targets != {"lane"} else [])
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
    if not sw and not config.WINDOWS and targets & {"claude", "codex"}:
        actions.append(f"session-workflow not found at {session_workflow_dir()}: its hooks were not written "
                       "(context cycles refuse and peer notices do not arrive until it is there and `bt install` runs again)")
    if "claude" in targets:
        notes: list[str] = []
        actions.append(install_json_hooks(CLAUDE_SETTINGS, _claude_handlers(), stamp, args.dry_run, deny=DENIED_TOOLS,
                                          session_workflow=session_workflow_handlers("claude") if sw else None,
                                          plugin_env_wanted=None if config.WINDOWS else plugin_env(), notes=notes))
        actions.extend(notes)
    if "codex" in targets:
        actions.append(install_json_hooks(CODEX_HOOKS, _codex_handlers(), stamp, args.dry_run,
                                          session_workflow=session_workflow_handlers("codex") if sw else None))
        if not args.dry_run:
            actions.append("Codex: open `codex` and approve the new hooks with /hooks (hook trust is per hooks.json).")
    if "routing" in targets:
        from . import routing
        actions.extend(("would " if args.dry_run else "") + a for a in routing.apply(dry_run=args.dry_run))
    if "profile" in targets:
        try:
            actions.append(install_profile(stamp, args.dry_run, rc_text))
        except OSError as exc:
            actions.append(f"error: could not update {profile_target()}: {exc}")
            failed = True
    others = [f"{v}:{a}" for v in config.VENDORS for a in config.account_names(v) if a != "main"]
    if others:
        actions.append("Other accounts pick up these hooks when `bt` next launches them (it syncs settings), "
                       f"or run `bt account link <vendor> <name>` now: {', '.join(others)}.")
    for action in actions:
        print(action)
    return 1 if failed else 0


def register(sub) -> None:
    p = sub.add_parser("install", help="wire hooks, role agents and the bt shell command")
    for name in ("claude", "codex", "profile", "routing", "lane"):
        p.add_argument(f"--{name}", action="store_true", help=f"only the {name} part")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=run)
