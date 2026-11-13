"""``bt doctor``: catch drift from Claude Code, Codex and Python updates before a hook silently stops working.

Checks are read-only (credential files: existence only; config.toml is parsed but only key names are
used). ``--live`` adds a subprocess self-test of the hooks, a format check against real transcripts and the
follower check; ``--fix`` repairs what can be repaired mechanically. ``daily_warnings`` is the cheap,
rate-limited subset for launch-time use.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path

from . import accounts, capsule, config, follow, gitutil, harvest, install, paths, registry

OK, WARN, FAIL, SKIP = "OK", "WARN", "FAIL", "SKIP"   # SKIP: the check has no meaning on this platform (never a failure)
HOOK_EVENTS = ("SessionStart", "PostCompact", "SessionEnd", "UserPromptSubmit", "PreToolUse")
MIN_PYTHON = (3, 11)
LOG_WINDOW = 7 * 86400
LOG_MAX_BYTES = 1 << 20
LOG_KEEP_LINES = 2000
SELFTEST_LIMIT_S = 2.0
VERSION_TIMEOUT_S = 10
CLAUDE_SCAN_FILES = 40
CODEX_SCAN_FILES = 60
CODEX_SCAN_DAYS = 14
DETAIL_WIDTH = 200
_COMPACT_SUMMARY = re.compile(rb'"isCompactSummary"\s*:\s*true')
_COMPACTED = re.compile(rb'"type"\s*:\s*"compacted"')
_VERSION = re.compile(r"\d+(?:\.\d+)+")


@dataclass
class Check:
    name: str
    status: str
    detail: str
    remedy: str = ""


def _short(text: object, width: int = 160) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= width else text[: width - 3] + "..."


def _ago(seconds: float) -> str:
    if seconds < 3600:
        return f"{max(0, int(seconds // 60))} min"
    if seconds < 2 * 86400:
        return f"{seconds / 3600:.0f} h"
    return f"{seconds / 86400:.0f} days"


def _read_json(path: Path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


# --------------------------------------------------------------------------- hook files


def _hook_files() -> list[tuple[str, str, Path]]:
    """(vendor, account, file that holds the hooks) for every configured account."""
    out = []
    for name in config.account_names("claude"):
        out.append(("claude", name, config.account_dir("claude", name) / "settings.json"))
    for name in config.account_names("codex"):
        out.append(("codex", name, config.account_dir("codex", name) / "hooks.json"))
    return out


def baton_handlers(path: Path) -> dict[str, list[tuple[int, int, dict]]] | None:
    """``{event: [(group index, handler index, handler)]}`` for baton's handlers; None if the file is unreadable."""
    data = _read_json(path)
    if not isinstance(data, dict):
        return None
    found: dict[str, list] = {}
    hooks = data.get("hooks")
    for event, groups in (hooks.items() if isinstance(hooks, dict) else ()):
        for gi, group in enumerate(groups if isinstance(groups, list) else ()):
            handlers = group.get("hooks") if isinstance(group, dict) else None
            for hi, handler in enumerate(handlers if isinstance(handlers, list) else ()):
                if isinstance(handler, dict) and install._is_ours(handler):
                    found.setdefault(event, []).append((gi, hi, handler))
    return found


def _command_tokens(handler: dict) -> list[str]:
    raw = [str(handler.get("command", ""))] if handler.get("args") else str(handler.get("command", "")).split()
    raw += [str(a) for a in handler.get("args", [])]
    return [t.strip("\"'") for t in raw if t.strip("\"'")]


# --------------------------------------------------------------------------- a. python


def _python_version(python: str | None = None) -> tuple[int, ...] | None:
    python = python or config.PYTHON
    if os.path.normcase(python) == os.path.normcase(sys.executable):
        return tuple(sys.version_info[:3])
    try:
        cp = subprocess.run([python, "-c", "import sys;print(*sys.version_info[:3])"], capture_output=True,
                            text=True, timeout=10, creationflags=config.CREATE_NO_WINDOW)
        return tuple(int(x) for x in cp.stdout.split()) if cp.returncode == 0 else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


_SCRIPT_TOKEN = re.compile(install._SCRIPT_NAMES.pattern + "$")   # a baton hook script or .cmd wrapper, as a path


def _executable_exists(token: str) -> bool:
    """A recorded interpreter: an absolute path must exist; a bare name must resolve on PATH."""
    if os.path.isabs(token) or "/" in token or "\\" in token:
        return Path(token).exists()
    return shutil.which(token) is not None


def missing_hook_paths(handler: dict) -> list[str]:
    """The interpreter and baton script paths a hook entry names that do not exist, for either platform's form
    (cmd.exe and the .cmd wrappers on Windows; python and the .py scripts directly elsewhere)."""
    missing = []
    for index, token in enumerate(_command_tokens(handler)):
        norm = token.replace("\\", "/").lower()
        if index == 0 and norm != "cmd.exe" and not norm.endswith("/cmd.exe"):
            if (norm.endswith(".exe") or not config.WINDOWS) and not _executable_exists(token):
                missing.append(token)
        elif _SCRIPT_TOKEN.search(norm) and not Path(token).exists():
            missing.append(token)
    return missing


def check_python() -> list[Check]:
    out = []
    python_exists = _executable_exists(config.PYTHON)
    version = _python_version() if python_exists else None
    if not python_exists:
        out.append(Check("python", FAIL, f"{config.PYTHON} does not exist", "run `bt install` with the current Python"))
    elif version is None:
        out.append(Check("python", FAIL, f"{config.PYTHON} did not report a version", "reinstall Python, then `bt install`"))
    elif version < MIN_PYTHON:
        out.append(Check("python", FAIL, f"{config.PYTHON} is {'.'.join(map(str, version))}; baton needs >= 3.11",
                         "install Python 3.12, then `bt install`"))
    else:
        out.append(Check("python", OK, f"{'.'.join(map(str, version))} at {config.PYTHON}"))
    main_files = [(v, p) for v, a, p in _hook_files() if a == "main"]
    for vendor, path in main_files:
        handlers = baton_handlers(path)
        if handlers is None:
            continue  # reported by the hooks check
        missing = sorted({token for items in handlers.values() for _, _, handler in items for token in missing_hook_paths(handler)})
        name = f"hook-paths:{vendor}"
        if missing:
            out.append(Check(name, FAIL, f"installed hook commands point at missing: {', '.join(missing)}",
                             "run `bt install` from the main checkout"))
        else:
            out.append(Check(name, OK, f"python and hook script paths in {path} exist"))
    return out


# --------------------------------------------------------------------------- b. hooks installed


def check_hooks() -> list[Check]:
    out = []
    for vendor, account, path in _hook_files():
        name = f"hooks:{vendor}:{account}"
        main = account == "main"
        fix = "run `bt install`" if main else f"run `bt account link {vendor} {account}`"
        handlers = baton_handlers(path)
        if handlers is None:
            out.append(Check(name, FAIL if main else WARN, f"{path} is missing or not valid JSON", fix))
            continue
        missing = [e for e in HOOK_EVENTS if e not in handlers]
        if missing:
            out.append(Check(name, FAIL if main else WARN, f"no baton handler for {', '.join(missing)} in {path}", fix))
        else:
            out.append(Check(name, OK, f"{len(HOOK_EVENTS)} events in {path}"))
    return out


def hook_form(handler: dict) -> str | None:
    """``"windows"`` (cmd.exe / .cmd wrapper), ``"posix"`` (the .py wrapper scripts run directly) or None for an
    entry that reads the same on both (the python + hook.py / cycle_hook.py entries)."""
    tokens = [t.replace("\\", "/").lower() for t in _command_tokens(handler)]
    if any(t.endswith(".cmd") or t == "cmd.exe" or t.endswith("/cmd.exe") for t in tokens):
        return "windows"
    if any(t.endswith(("/inbox_check.py", "/wt_guard.py")) for t in tokens):
        return "posix"
    return None


def check_hook_forms() -> list[Check]:
    """The main accounts' baton entries must be in this platform's form: a Windows-form entry on a Mac runs
    ``cmd.exe`` (not found) on every prompt and tool call; a POSIX-form one on Windows bypasses the .cmd wrappers."""
    expected = "windows" if config.WINDOWS else "posix"
    out = []
    for vendor, account, path in _hook_files():
        if account != "main":
            continue
        handlers = baton_handlers(path)
        if handlers is None:
            continue  # reported by the hooks check
        wrong = sorted({event for event, items in handlers.items() for _, _, h in items if hook_form(h) not in (None, expected)})
        name = f"hook-form:{vendor}"
        if wrong:
            other = "POSIX" if expected == "windows" else "Windows"
            out.append(Check(name, FAIL, f"{', '.join(wrong)} in {path} use the {other} form (written on another platform)",
                             "run `bt install` from the main checkout"))
        else:
            out.append(Check(name, OK, f"baton entries in {path} are in the {expected} form"))
    return out


def check_wrappers() -> list[Check]:
    """The generated .cmd wrappers behind the Windows hook entries."""
    if not config.WINDOWS:
        return [Check("cmd-wrappers", SKIP, "Windows only: off Windows the hooks run python directly and no .cmd file is used")]
    missing = [str(p) for p in (install.INBOX_CMD, install.WT_GUARD, install.WT_GUARD_CODEX) if not p.is_file()]
    if missing:
        return [Check("cmd-wrappers", FAIL, f"missing: {', '.join(missing)}", "run `bt install` from the main checkout")]
    return [Check("cmd-wrappers", OK, "inbox_check.cmd, wt_guard.cmd and wt_guard_codex.cmd exist")]


def _profile_line_paths(line: str) -> list[str]:
    """The files a marked profile line names (``. <bt.sh> <baton home> <python>  # baton (bt command)``)."""
    import shlex
    try:
        words = shlex.split(line.split(install.PROFILE_MARK)[0])
    except ValueError:
        return []
    return words[1:]


def check_posix_install() -> list[Check]:
    """Off Windows: the shell profile line and bt.sh. Nothing is checked (and nothing reported) on Windows."""
    if config.WINDOWS:
        return []
    out = []
    rc = install.posix_rc_file()
    try:
        lines = [l for l in rc.read_text(encoding="utf-8", errors="replace").splitlines() if install.PROFILE_MARK in l]
    except OSError:
        lines = []
    if not lines:
        out.append(Check("profile", WARN, f"no `{install.PROFILE_MARK}` line in {rc}: the bt command is not defined in new shells",
                         "run `bt install --profile` from the main checkout"))
    else:
        gone = [p for p in _profile_line_paths(lines[-1]) if not Path(p).exists()]
        if len(lines) > 1:
            out.append(Check("profile", WARN, f"{len(lines)} baton lines in {rc}", "run `bt install --profile` (it keeps one)"))
        elif gone:
            out.append(Check("profile", WARN, f"the baton line in {rc} names missing: {', '.join(gone)}",
                             "run `bt install --profile` from the main checkout"))
        else:
            out.append(Check("profile", OK, f"{rc} sources bt.sh"))
    try:
        data = install.BT_SH.read_bytes()
    except OSError:
        out.append(Check("bt-sh", FAIL, f"{install.BT_SH} is missing", "restore it from the repository (git checkout bt.sh)"))
        return out
    if b"\r" in data:
        out.append(Check("bt-sh", FAIL, f"{install.BT_SH} has CRLF line endings: the shell cannot source it",
                         "convert to LF (git add --renormalize bt.sh, or `sed -i '' 's/\\r$//' bt.sh`)"))
    elif any(b > 127 for b in data):
        out.append(Check("bt-sh", WARN, f"{install.BT_SH} contains non-ASCII bytes", "keep bt.sh ASCII"))
    else:
        out.append(Check("bt-sh", OK, f"{install.BT_SH} has LF endings"))
    return out


def _session_workflow_entries(path: Path) -> list[tuple[str, str | None, list[str]]] | None:
    """(event, group matcher, command words) of every session-workflow handler in a hooks file; None if unreadable."""
    data = _read_json(path)
    if not isinstance(data, dict):
        return None
    found = []
    hooks = data.get("hooks")
    for event, groups in (hooks.items() if isinstance(hooks, dict) else ()):
        for group in (groups if isinstance(groups, list) else ()):
            handlers = group.get("hooks") if isinstance(group, dict) else None
            for handler in (handlers if isinstance(handlers, list) else ()):
                if isinstance(handler, dict) and install.is_session_workflow(handler):
                    found.append((event, group.get("matcher"), _command_tokens(handler)))
    return found


def check_session_workflow() -> list[Check]:
    """Off Windows: session-workflow's hook entries are in each settings file in the POSIX form and what they run exists.
    On Windows its entries are merged by hand and left alone, so nothing is checked."""
    if config.WINDOWS:
        return [Check("session-workflow", SKIP, "POSIX form only: on Windows its entries are merged by hand and left as they are")]
    sw = install.session_workflow_dir()
    if not (sw / "workflow.py").exists():
        return [Check("session-workflow", WARN, f"{sw} is absent: a context cycle refuses and peer notices do not arrive",
                      "put session-workflow next to baton (or set BATON_SESSION_WORKFLOW), then run `bt install`")]
    out = []
    missing = [str(p) for p in (sw / "workflow.py", install.WORKFLOW_HOOK_SH) if not p.is_file()]
    if not _executable_exists(config.PYTHON):
        missing.append(config.PYTHON)
    if missing:
        out.append(Check("session-workflow-files", FAIL, f"missing: {', '.join(missing)}", "run `bt install` from the main checkout"))
    else:
        out.append(Check("session-workflow-files", OK, "workflow.py, workflow_hook.sh and the Python exist"))
    for vendor, account, path in _hook_files():
        name = f"session-workflow:{vendor}:{account}"
        main = account == "main"
        fix = "run `bt install`" if main else f"run `bt account link {vendor} {account}`"
        found = _session_workflow_entries(path)
        if found is None:
            continue  # reported by the hooks check
        wanted = [(event, group.get("matcher"), _command_tokens(h))
                  for event, group in install.session_workflow_handlers(vendor).items() for h in group["hooks"]]
        missing_events = sorted({event for event, matcher, words in wanted if (event, matcher, words) not in found})
        stale = sorted({event for event, matcher, words in found if (event, matcher, words) not in wanted})
        if missing_events or stale:
            parts = ([f"no POSIX-form entry for {', '.join(missing_events)}"] if missing_events else []) +                     ([f"other entries for {', '.join(stale)}"] if stale else [])
            out.append(Check(name, FAIL if main and missing_events else WARN, f"{path}: {'; '.join(parts)}", fix))
        else:
            out.append(Check(name, OK, f"{len(wanted)} entries in {path}"))
    return out


def check_plugin_env() -> list[Check]:
    """Off Windows: the compute-queue plugin is where Claude Code will look for it, and the interpreter its hook runs
    (CQ_PYTHON; a Mac has no bare ``python``) exists and is Python 3.11 or later. On Windows both are set by hand."""
    if config.WINDOWS:
        return []
    out = []
    plugin = install.PLUGIN_DIR
    gone = [str(plugin / part) for part in ("hooks/register.tsx", "bin/cq.py") if not (plugin / part).is_file()]
    if gone:
        out.append(Check("plugin-dir", FAIL, f"missing: {', '.join(gone)}", "restore the compute-queue plugin from the repository"))
    else:
        out.append(Check("plugin-dir", OK, f"{plugin} exists"))
    for account in config.account_names("claude"):
        path = config.account_dir("claude", account) / "settings.json"
        name = f"plugin-env:{account}"
        data = _read_json(path)
        if not isinstance(data, dict):
            continue  # reported by the hooks check
        env = data.get("env") if isinstance(data.get("env"), dict) else {}
        severity = FAIL if account == "main" else WARN
        fix = "run `bt install`" if account == "main" else f"run `bt account link claude {account}`"
        problems = []
        dirs = env.get("CLAUDE_CODE_PLUGIN_DIRS")
        if not isinstance(dirs, str) or not install.names_plugin_dir(dirs, install.PLUGIN_DIR.as_posix()):
            problems.append(f"env CLAUDE_CODE_PLUGIN_DIRS does not name {plugin.as_posix()}: the queue's tools and hook are not loaded")
        python = env.get("CQ_PYTHON")
        if not isinstance(python, str) or not python.strip():
            problems.append("env CQ_PYTHON is not set: the hook falls back to a bare `python`, which this machine may not have")
        elif not _executable_exists(python):
            problems.append(f"env CQ_PYTHON {python} does not exist")
        else:
            version = _python_version(python)
            if version is None:
                problems.append(f"env CQ_PYTHON {python} did not report a version")
            elif version < MIN_PYTHON:
                problems.append(f"env CQ_PYTHON {python} is {'.'.join(map(str, version))}; the queue needs >= 3.11")
        if problems:
            out.append(Check(name, severity, f"{path}: {'; '.join(problems)}", fix))
        else:
            out.append(Check(name, OK, f"{path}: plugin directory and Python {'.'.join(map(str, _python_version(env['CQ_PYTHON']) or ()))} set"))
    return out


def check_tools() -> list[Check]:
    """Off Windows: ``fzf`` backs the picker of a bare ``bt``; without it the worktree list is printed instead."""
    if config.WINDOWS:
        return []
    if shutil.which("fzf"):
        return [Check("fzf", OK, "found")]
    return [Check("fzf", WARN, "not found: a bare `bt` prints the worktree list instead of a picker", "brew install fzf")]


# --------------------------------------------------------------------------- c. codex trust

_TRUST_KEY = re.compile(r"^(?P<file>.+):(?P<event>[a-z_]+):(?P<group>\d+):(?P<handler>\d+)$")


def _snake(event: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", event).lower()


def check_codex_trust() -> list[Check]:
    """Codex records ``[hooks.state."<hooks.json>:<event>:<group>:<handler>"] trusted_hash`` once a hook is
    approved with /hooks. The hash algorithm is Codex's own, so only the presence of the entry is checked."""
    out = []
    approve = "open codex in that home and approve with /hooks"
    for name in config.account_names("codex"):
        home = config.account_dir("codex", name)
        label = f"codex-trust:{name}"
        handlers = baton_handlers(home / "hooks.json")
        if not handlers:
            continue  # the hooks check reports a missing file or handlers
        try:
            with open(home / "config.toml", "rb") as fh:
                cfg = tomllib.load(fh)
        except FileNotFoundError:
            out.append(Check(label, WARN, f"{home / 'config.toml'} does not exist: no hook is trusted", approve))
            continue
        except (OSError, tomllib.TOMLDecodeError) as exc:
            out.append(Check(label, WARN, f"unknown: cannot read {home / 'config.toml'} ({_short(exc, 80)})", approve))
            continue
        hooks = cfg.get("hooks")
        state = hooks.get("state") if isinstance(hooks, dict) else None
        if state is None:
            out.append(Check(label, WARN, "no [hooks.state] in config.toml: no hook is trusted yet "
                                           "(or Codex changed where it records trust)", approve))
            continue
        parsed = {}
        if isinstance(state, dict):
            for key, value in state.items():
                match = _TRUST_KEY.match(key)
                if match and isinstance(value, dict):
                    parsed[(config.norm(match["file"]), match["event"], int(match["group"]), int(match["handler"]))] = value
        if not parsed:
            out.append(Check(label, WARN, "unknown: [hooks.state] has no '<hooks.json>:<event>:<n>:<n>' entries; "
                                           "cannot tell whether baton's hooks are trusted", approve))
            continue
        here = config.norm(home / "hooks.json")
        untrusted = []
        for event, items in handlers.items():
            for gi, hi, _ in items:
                entry = parsed.get((here, _snake(event), gi, hi))
                if not (isinstance(entry, dict) and entry.get("trusted_hash")):
                    untrusted.append(event)
        if untrusted:
            out.append(Check(label, WARN, f"baton hooks not trusted: {', '.join(sorted(set(untrusted)))}", approve))
        else:
            out.append(Check(label, OK, f"{sum(len(v) for v in handlers.values())} baton hooks have a trust entry "
                                        "(hash itself is not verified)"))
    return out


# --------------------------------------------------------------------------- d. hook health


def _recent_log_lines(now: float) -> list[str]:
    out = []
    for raw in harvest.read_tail_lines(config.STATE_DIR / "hook.log", LOG_MAX_BYTES):
        line = raw.decode("utf-8", errors="replace").rstrip("\r")
        stamp = harvest.parse_iso(line[:19])
        if stamp is not None and now - stamp <= LOG_WINDOW:
            out.append(line)
    return out


def check_hook_health(now: float | None = None) -> list[Check]:
    now = time.time() if now is None else now
    out = []
    lines = _recent_log_lines(now)
    log = config.STATE_DIR / "hook.log"
    if lines:
        out.append(Check("hook-log", WARN, f"{len(lines)} hook error line(s) in 7 days; last: {_short(lines[-1])}",
                         f"read {log}; `bt doctor --live` re-runs the hooks"))
    else:
        out.append(Check("hook-log", OK, "no hook errors in the last 7 days"))
    try:
        data = registry.load()
    except registry.RegistryUnavailable as exc:
        out.append(Check("registry", FAIL, f"cannot read {exc}", "move registry.json aside; the next hook creates a new one"))
        return out
    worktrees = data.get("worktrees", {})
    out.append(Check("registry", OK, f"{len(worktrees)} worktree(s)"))
    for vendor in config.VENDORS:
        newest = 0.0
        for item in worktrees.values():
            rec = item.get(vendor) if isinstance(item, dict) else None
            seen = rec.get("last_seen") if isinstance(rec, dict) else None
            if isinstance(seen, (int, float)) and not isinstance(seen, bool):
                newest = max(newest, float(seen))
        if not newest:
            out.append(Check(f"sessions:{vendor}", OK, f"no {vendor} session recorded"))
        elif now - newest >= 86400:
            out.append(Check(f"sessions:{vendor}", OK, f"no {vendor} session recorded for {int((now - newest) // 86400)} days"))
        else:
            out.append(Check(f"sessions:{vendor}", OK, f"last {vendor} session {_ago(now - newest)} ago"))
    return out


# --------------------------------------------------------------------------- e. live self-test


def _git_init(path: Path) -> bool:
    path.mkdir(parents=True, exist_ok=True)
    try:
        return subprocess.run(["git", "-C", str(path), "init", "-q"], capture_output=True, timeout=30,
                              stdin=subprocess.DEVNULL, creationflags=config.CREATE_NO_WINDOW).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def check_selftest() -> list[Check]:
    tmp = Path(tempfile.mkdtemp(prefix="baton-doctor-"))
    out = []
    try:
        env = dict(os.environ, BATON_STATE=str(tmp / "state"), BATON_SESSION_WORKFLOW=str(tmp / "no-session-workflow"))
        for key in (config.FOLLOWER_ENV, *config.ACCOUNT_ENV.values()):
            env.pop(key, None)
        script = config.BATON_HOME / "hook.py"
        for vendor in config.VENDORS:
            # A successful hook owns its worktree. Each vendor needs its own
            # synthetic root; reusing one would correctly trigger a conflict.
            repo = tmp / vendor
            if not _git_init(repo):
                return [Check("selftest", FAIL, "git init failed in a temp directory", "check that git is on PATH")]
            payload = {"hook_event_name": "SessionStart", "session_id": "doctor-selftest", "source": "startup",
                       "transcript_path": str(tmp / "t.jsonl"), "cwd": str(repo)}
            t0 = time.perf_counter()
            try:
                cp = subprocess.run([config.PYTHON, "-S", str(script), vendor], input=json.dumps(payload).encode(),
                                    capture_output=True, env=env, cwd=str(tmp), timeout=30,
                                    creationflags=config.CREATE_NO_WINDOW)
            except (OSError, subprocess.SubprocessError) as exc:
                out.append(Check(f"selftest:{vendor}", FAIL, f"could not run the hook: {_short(exc)}", "run `bt install`"))
                continue
            elapsed = time.perf_counter() - t0
            problem = ""
            if cp.returncode != 0:
                problem = f"exit {cp.returncode}"
            else:
                try:
                    context = json.loads(cp.stdout.decode("utf-8"))["hookSpecificOutput"]["additionalContext"]
                    if "[baton]" not in context:
                        problem = "additionalContext lacks the [baton] capsule header"
                except (ValueError, KeyError, TypeError, UnicodeDecodeError):
                    problem = f"stdout is not the expected JSON: {_short(cp.stdout.decode('utf-8', 'replace'), 80)!r}"
            if not problem and elapsed >= SELFTEST_LIMIT_S:
                problem = f"took {elapsed:.2f}s (limit {SELFTEST_LIMIT_S:.0f}s)"
            if problem:
                err = _short(cp.stderr.decode("utf-8", "replace"), 100) if cp.stderr else ""
                out.append(Check(f"selftest:{vendor}", FAIL, problem + (f"; stderr: {err}" if err else ""),
                                 f"run `{config.PYTHON} -S {script} {vendor}` by hand with a SessionStart payload"))
            else:
                out.append(Check(f"selftest:{vendor}", OK, f"SessionStart ok in {elapsed:.2f}s"))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return out


# --------------------------------------------------------------------------- f. format drift


def _newest(files: list[Path], limit: int) -> list[Path]:
    stamped = []
    for f in files:
        try:
            stamped.append((f.stat().st_mtime, f))
        except OSError:
            pass
    return [f for _, f in sorted(stamped, key=lambda x: x[0], reverse=True)[:limit]]


def _last_matching_line(path: Path, prefilter: bytes, pattern: re.Pattern) -> bytes | None:
    """Last line of ``path`` that matches ``pattern`` (streamed; ``prefilter`` is a cheap substring gate)."""
    found = None
    try:
        with open(path, "rb") as fh:
            for line in fh:
                if prefilter in line and pattern.search(line):
                    found = line
    except OSError:
        return None
    return found


def _sample(files: list[Path], prefilter: bytes, pattern: re.Pattern):
    for f in files:
        line = _last_matching_line(f, prefilter, pattern)
        if line is not None:
            return f, line
    return None, None


def check_formats(versions: dict[str, str | None]) -> list[Check]:
    out = []
    claude_files = _newest(list((config.account_dir("claude", "main") / "projects").glob("*/*.jsonl")), CLAUDE_SCAN_FILES)
    path, line = _sample(claude_files, b"isCompactSummary", _COMPACT_SUMMARY)
    if line is None:
        out.append(Check("format:claude", OK, f"no sample (no compaction in the newest {len(claude_files)} transcripts)"))
    else:
        with tempfile.TemporaryDirectory(prefix="baton-doctor-") as tmp:
            sample = Path(tmp) / "sample.jsonl"
            sample.write_bytes(line if line.endswith(b"\n") else line + b"\n")
            text = harvest.claude_summary_from_transcript(sample)  # the hook's own extraction
        sections = harvest.parse_claude_summary(text) if text else {}
        if len(sections) >= 3:
            out.append(Check("format:claude", OK, f"compaction summary parses ({len(sections)} of 9 sections) in {path.name}"))
        else:
            out.append(Check("format:claude", WARN,
                             f"format changed (Claude Code {versions.get('claude') or '?'}): {len(sections)} numbered "
                             f"sections found in {path.name}, expected >= 3",
                             "harvest.py needs updating for the new summary layout"))
    cutoff = time.time() - CODEX_SCAN_DAYS * 86400
    rollouts = []
    for name in config.account_names("codex"):
        for f in (config.account_dir("codex", name) / "sessions").glob("*/*/*/rollout-*.jsonl"):
            try:
                if f.stat().st_mtime >= cutoff:
                    rollouts.append(f)
            except OSError:
                pass
    rollouts = _newest(sorted(set(rollouts)), CODEX_SCAN_FILES)
    path, line = _sample(rollouts, b"compacted", _COMPACTED)
    if line is None:
        out.append(Check("format:codex", OK, f"no sample (no compaction in the newest {len(rollouts)} rollouts)"))
    else:
        messages, summary = None, None
        try:
            obj = json.loads(line)
            payload = obj.get("payload") if isinstance(obj, dict) else None
            if isinstance(payload, dict):
                summary = payload.get("message")
                retained = payload.get("retained_context")
                messages = retained.get("user_messages") if isinstance(retained, dict) else None
        except (ValueError, KeyError, TypeError):
            pass
        if isinstance(summary, str) and summary.strip():
            out.append(Check("format:codex", OK, f"compacted.message summary parses in {path.name}"))
        elif isinstance(messages, list) and all(isinstance(m, str) or (isinstance(m, dict) and "text" in m) for m in messages):
            out.append(Check("format:codex", OK, f"retained_context.user_messages parses ({len(messages)} messages) in {path.name}"))
        else:
            out.append(Check("format:codex", WARN,
                             f"format changed (Codex {versions.get('codex') or '?'}): compaction summary and retained user messages "
                             f"is missing or malformed in {path.name}", "harvest.py needs updating for the new rollout layout"))
    return out


# --------------------------------------------------------------------------- g. accounts


def check_accounts() -> list[Check]:
    out = []
    for vendor in config.VENDORS:
        primary = config.account_dir(vendor, "main")
        names = accounts.CLAUDE_JUNCTIONS if vendor == "claude" else accounts.CODEX_JUNCTIONS
        for account in config.account_names(vendor):
            home = config.account_dir(vendor, account)
            label = f"account:{vendor}:{account}"
            main = account == "main"
            if not home.is_dir():
                out.append(Check(label, FAIL if main else WARN, f"{home} does not exist",
                                 f"launch {vendor} once" if main else f"run `bt account link {vendor} {account}`"))
                continue
            problems, notes = [], []
            if not accounts.logged_in(vendor, account):
                notes.append(f"not logged in ({accounts.CREDENTIAL_FILE[vendor]} missing)")
            if not main:
                expected = [n for n in names if (primary / n).exists()]
                for n in expected:
                    link = home / n
                    if not accounts._is_junction(link):
                        problems.append(f"{n} missing" if not os.path.lexists(link)
                                        else f"{n} is not a {'junction' if config.WINDOWS else 'symlink'}")
                    elif not accounts._same_target(link, primary / n):
                        problems.append(f"{n} points elsewhere")
            if problems or notes:
                shown = problems[:5]
                detail = "; ".join(shown + (["..."] if len(problems) > 5 else []) + notes)
                hint = (f"run `bt account link {vendor} {account}`" if problems else
                        f"set {config.ACCOUNT_ENV[vendor]}={home}, run {vendor} and log in" if not main else f"run {vendor} and log in")
                out.append(Check(label, WARN, f"{home}: {detail}", hint))
            else:
                out.append(Check(label, OK, f"{home} logged in" + ("" if main else f"; {'junctions' if config.WINDOWS else 'symlinks'} point at the primary")))
    return out


# --------------------------------------------------------------------------- g2. logins


def check_logins(now: float | None = None) -> list[Check]:
    """One ``login:claude:<account>`` check per logged-in Claude account: FAIL once the refresh token has
    expired, WARN within ``accounts.LOGIN_WARN_DAYS`` days. Only the two expiry timestamps are read."""
    now = time.time() if now is None else now
    out = []
    for account in config.account_names("claude"):
        if not accounts.logged_in("claude", account):
            continue
        name = f"login:claude:{account}"
        refresh, access = accounts.login_expiry("claude", account)
        if refresh is None:
            out.append(Check(name, OK, "no refresh-token expiry recorded"))
        elif refresh <= now:
            out.append(Check(name, FAIL, f"refresh token expired {accounts.fmt_date(refresh)}", accounts.login_remedy("claude", account)))
        elif refresh - now <= accounts.LOGIN_WARN_DAYS * 86400:
            out.append(Check(name, WARN, f"refresh token expires {accounts.fmt_date(refresh)} (in {_ago(refresh - now)})",
                             f"use the account, or run `bt keepalive --account {account}`"))
        else:
            out.append(Check(name, OK, f"refresh token valid until {accounts.fmt_date(refresh)}"))
    return out


# --------------------------------------------------------------------------- h. routing


def check_routing() -> list[Check]:
    from . import routing
    out = []
    actions = routing.apply(dry_run=True)
    pending = [a for a in actions if not a.startswith("unchanged") and not a.startswith("skip (not generated by baton)")]
    if pending:
        out.append(Check("routing", WARN, f"{len(pending)} role file(s) out of date: " + _short("; ".join(pending), 150),
                         "run `bt routing apply`"))
    else:
        out.append(Check("routing", OK, f"{sum(a.startswith('unchanged') for a in actions)} role files up to date"))
    for warning in routing.check():
        out.append(Check("routing:check", WARN, _short(warning, DETAIL_WIDTH), "edit routing.toml, then `bt routing apply`"))
    return out


# --------------------------------------------------------------------------- i. CLI versions


def _state_path() -> Path:
    return config.STATE_DIR / "doctor.json"


def _read_state() -> dict:
    data = _read_json(_state_path())
    return data if isinstance(data, dict) else {}


def _save_state(**fields) -> None:
    state = _read_state()
    state.update(fields)
    harvest.atomic_write(_state_path(), json.dumps(state, indent=1, sort_keys=True))


def cli_versions() -> dict[str, str | None]:
    """``{vendor: version or None}`` from ``<cli> --version`` (both run in parallel, 10 s timeout each)."""
    result: dict[str, str | None] = {}

    def run(vendor: str) -> None:
        try:
            cp = subprocess.run([config.exe(vendor), "--version"], capture_output=True, text=True, errors="replace",
                                timeout=VERSION_TIMEOUT_S, stdin=subprocess.DEVNULL, creationflags=config.CREATE_NO_WINDOW)
            match = _VERSION.search(cp.stdout or cp.stderr or "")
            result[vendor] = match.group(0) if cp.returncode == 0 and match else None
        except (OSError, subprocess.SubprocessError):
            result[vendor] = None

    threads = [threading.Thread(target=run, args=(v,), daemon=True) for v in config.VENDORS]
    for t in threads:
        t.start()
    for t in threads:
        t.join(VERSION_TIMEOUT_S + 2)
    return {v: result.get(v) for v in config.VENDORS}


def check_versions(versions: dict[str, str | None]) -> list[Check]:
    stored = _read_state().get("versions")
    stored = stored if isinstance(stored, dict) else {}
    out, keep = [], dict(stored)
    for vendor, label in (("claude", "Claude Code"), ("codex", "Codex")):
        now = versions.get(vendor)
        if now is None:
            out.append(Check(f"version:{vendor}", WARN, f"`{vendor} --version` failed or timed out", f"check that {vendor} runs"))
            continue
        keep[vendor] = now
        before = stored.get(vendor)
        if before and before != now:
            out.append(Check(f"version:{vendor}", WARN, f"{label} updated {before} → {now}", "run `bt doctor --live`"))
        else:
            out.append(Check(f"version:{vendor}", OK, now))
    try:
        _save_state(versions=keep, time=time.time())
    except OSError:
        pass
    return out


# --------------------------------------------------------------------------- j. followers


def _hook_file(root: Path) -> Path | None:
    try:
        return follow.hooks_dir(root, force=True) / "post-commit"
    except Exception:  # noqa: BLE001 - follow.py may change independently; fall back to the default location
        common = paths.git_common_dir(root)
        return common / "hooks" / "post-commit" if common else None


def _follower_siblings(root: Path) -> list[Path]:
    """Follower sibling worktrees of the lead at ``root``: live lineage children plus any ``<ws>--*`` directory next
    to the other worktrees of its repository (siblings made before lineage was kept)."""
    from . import worktrees as wt
    main = paths.main_checkout(root)
    found: dict[str, Path] = {}
    key = config.norm(root)
    for path, rec in wt.live_records(main):
        if rec.get("parent") and config.norm(rec["parent"]) == key and path != key and Path(path).is_dir():
            found[path] = Path(path)
    try:
        for d in wt.worktrees_dir(main).glob(f"{paths.ws_name(root)}--*"):
            if d.is_dir():
                found.setdefault(config.norm(d), d)
    except OSError:
        pass
    return list(found.values())


def _follow_roots() -> list[tuple[Path, str]]:
    """(root, ws) of every worktree with a follow.toml: registered sessions, plus live lineage records and their
    parents (a follower sibling records its lead), since a member worktree driven from another session has
    followers but no registry row of its own."""
    from . import worktrees as wt
    found: dict[str, tuple[Path, str]] = {}
    try:
        items = registry.load().get("worktrees", {}).values()
    except registry.RegistryUnavailable:
        items = []
    for item in items:
        if isinstance(item, dict) and item.get("path"):
            found.setdefault(config.norm(item["path"]), (Path(item["path"]), item.get("ws") or Path(item["path"]).name))
    for key, rec in wt.load_lineage().items():
        if isinstance(rec, dict) and not rec.get("retired"):
            for path in (key, rec.get("parent")):
                if isinstance(path, str) and path:
                    found.setdefault(config.norm(path), (Path(path), Path(path).name))
    return [(root, ws) for root, ws in found.values() if (root / ".baton" / "follow.toml").is_file()]


def check_followers() -> list[Check]:
    out = []
    now = time.time()
    for root, ws in _follow_roots():
        label = f"follow:{ws}"
        hook = _hook_file(root)
        try:
            text = hook.read_text(encoding="utf-8", errors="replace") if hook and hook.is_file() else ""
        except OSError:
            text = ""
        problems = []
        if follow.HOOK_BEGIN not in text:
            problems.append("post-commit hook lacks the baton block")
        else:
            block = text[text.index(follow.HOOK_BEGIN):]
            pair = re.search(r'"([^"]+)" -S "([^"]+)"', block)
            missing = [p for p in (pair.groups() if pair else ()) if not Path(p).exists()]
            if missing:
                problems.append("post-commit block points at missing " + ", ".join(missing))
        wip = gitutil.out(root, "for-each-ref", "refs/baton/wip", "--format=%(refname)")
        wip_count = len(wip.splitlines()) if wip else 0
        siblings = len(_follower_siblings(root))
        roles, failed = [], []
        follow_cfg = {}
        try:
            with open(root / ".baton" / "follow.toml", "rb") as fh:
                follow_cfg = tomllib.load(fh)
        except (OSError, tomllib.TOMLDecodeError):
            problems.append("follow.toml is unreadable")
        for role in follow_cfg.get("roles") or []:
            st = _read_json(root / ".baton" / "follow-state" / f"{role}.json")
            if not isinstance(st, dict) or not st:
                roles.append(f"{role}=never run")
                continue
            started = st.get("last_started_at")
            when = f" {_ago(now - float(started))} ago" if isinstance(started, (int, float)) and not isinstance(started, bool) else ""
            status = st.get("status") or "?"
            if status == "running" and not follow.pid_alive(st.get("pid")):
                status = "running (runner gone)"
                problems.append(f"{role}: status is running but its runner (pid {st.get('pid')}) is gone")
            elif status == "failed":
                failed.append(role)
                problems.append(f"{role}: last run failed: {_short(st.get('error') or 'no error recorded', 160)}")
            roles.append(f"{role}={status}{when}")
        try:
            unread = sum(1 for _ in (root / ".baton" / "inbox").glob("*.md"))
        except OSError:
            unread = 0
        detail = (f"{siblings} sibling worktree(s); {wip_count} wip ref(s); {unread} unread inbox note(s); "
                  + (", ".join(roles) or "no roles"))
        if problems:
            hints = []
            if failed:
                hints.append(f"read `bt inbox --worktree {root}`, then `bt merge <role>` or `bt follow reset <role>` "
                             "there, or clear what the error names")
            if len(problems) > len(failed):
                hints.append(f"run `bt follow on` in {root} (a dead runner's claim is replaced by the next `bt follow now`)")
            out.append(Check(label, WARN, "; ".join(problems) + "; " + detail, "; ".join(hints)))
        else:
            out.append(Check(label, OK, "post-commit hook ok; " + detail))
    return out


# --------------------------------------------------------------------------- k. housekeeping


def stale_registry_paths() -> list[str]:
    """Registry keys whose worktree directory no longer exists."""
    worktrees = registry.load().get("worktrees", {})
    return [key for key, item in worktrees.items()
            if isinstance(item, dict) and item.get("path") and not Path(item["path"]).exists()]


def prune_registry() -> list[str]:
    """Remove registry entries whose path no longer exists; returns the removed keys (load/save via registry)."""
    data = registry.load()
    gone = [key for key, item in data.get("worktrees", {}).items()
            if isinstance(item, dict) and item.get("path") and not Path(item["path"]).exists()]
    for key in gone:
        del data["worktrees"][key]
    if gone:
        registry._save(data)
    return gone


def trim_hook_log(keep: int = LOG_KEEP_LINES) -> bool:
    log = config.STATE_DIR / "hook.log"
    try:
        lines = log.read_bytes().splitlines(keepends=True)
    except OSError:
        return False
    if len(lines) <= keep:
        return False
    harvest.atomic_write(log, b"".join(lines[-keep:]).decode("utf-8", errors="replace"))
    return True


def check_housekeeping() -> list[Check]:
    out = []
    try:
        gone = stale_registry_paths()
    except registry.RegistryUnavailable:
        gone = []
    if gone:
        out.append(Check("housekeeping:registry", WARN, f"{len(gone)} registry entr(ies) whose path no longer exists: "
                         + _short(", ".join(gone), 120), "run `bt doctor --fix`"))
    else:
        out.append(Check("housekeeping:registry", OK, "no entries for deleted worktrees"))
    try:
        size = (config.STATE_DIR / "hook.log").stat().st_size
    except OSError:
        size = 0
    if size > LOG_MAX_BYTES:
        out.append(Check("housekeeping:hook-log", WARN, f"hook.log is {size // 1024} KiB", "run `bt doctor --fix`"))
    else:
        out.append(Check("housekeeping:hook-log", OK, f"hook.log is {size // 1024} KiB"))
    return out


# --------------------------------------------------------------------------- l. worktree discipline

UNPUSHED_DAYS = 3


def _stack_depths(main: Path, linked: list[dict]) -> dict[str, int]:
    """Per worktree path: the longest chain of other worktree tips (not on master) that are strict ancestors of its tip."""
    from . import worktrees as wt
    heads = sorted({e["head"] for e in linked if e["head"]})
    open_heads = [h for h in heads if not wt.on_mainline(main, h)]
    wanted = set(open_heads)
    desc = {}
    for h in open_heads:
        out = gitutil.out(main, "for-each-ref", "--contains", h, "--format=%(objectname)", "refs/heads")
        desc[h] = ({line.strip() for line in out.splitlines()} & wanted) | {h}
    ancestors = {h: [a for a in open_heads if a != h and h in desc[a]] for h in open_heads}
    memo: dict[str, int] = {}

    def depth(h: str) -> int:            # strict ancestry is acyclic, so plain memoised recursion terminates
        if h not in memo:
            memo[h] = 1 + max(map(depth, ancestors[h])) if ancestors[h] else 0
        return memo[h]

    return {e["path"]: (depth(e["head"]) if e["head"] in desc else 0) for e in linked}


def _stale_scratch(main: Path, now: float | None = None) -> list[str]:
    """Names of scratch (review) checkouts older than a whole runner could take: timeout_minutes times the roles."""
    from . import worktrees as wt
    now = time.time() if now is None else now
    limit = float(follow.settings()["timeout_minutes"]) * 60 * len(follow.follower_names())
    try:
        entries = list((wt.worktrees_dir(main) / wt.SCRATCH).iterdir())
    except OSError:
        return []
    stale = []
    for entry in entries:
        try:
            if now - entry.stat().st_mtime > limit:
                stale.append(entry.name)
        except OSError:
            continue
    return sorted(stale)


def _worktree_checks(main: Path, cheap: bool) -> list[Check]:
    from . import worktrees as wt
    cfg = wt.settings()
    cap, max_depth = cfg["cap_per_repo"], cfg["max_depth"]
    label = main.name
    entries = wt.list_worktrees(main)
    followers = wt.follower_worktrees(main, entries[1:])
    linked = wt.linked_worktrees(main, entries[1:])
    out: list[Check] = []
    count = len(linked)
    if count >= cap:
        out.append(Check(f"worktrees:cap:{label}", FAIL if count > cap * 1.5 else WARN,
                         f"{count} linked worktrees (cap {cap})", "finish and remove them: `bt wt list`, `bt wt retire <name>`"))
    follower_cap = cfg["follower_cap_per_repo"]
    if len(followers) >= follower_cap:
        out.append(Check(f"worktrees:follower-cap:{label}", WARN,
                         f"{len(followers)} follower worktrees (limit {follower_cap})",
                         "free them: `bt merge <role>` for finished work, `bt follow reset <role>` otherwise"))
    by_tip: dict[str, list[str]] = {}
    for e in linked:
        if e["head"]:
            by_tip.setdefault(e["head"], []).append(Path(e["path"]).name)
    twins = [names for names in by_tip.values() if len(names) > 1]
    if twins:
        out.append(Check(f"worktrees:siblings:{label}", WARN,
                         "sibling worktrees on an identical tip: " + "; ".join(", ".join(sorted(n)) for n in twins[:4]),
                         "keep one and `bt wt retire` the others"))
    stale = _stale_scratch(main)
    if stale:
        out.append(Check(f"worktrees:scratch:{label}", WARN,
                         f"{len(stale)} stale review checkout(s) under .claude/worktrees/{wt.SCRATCH} (a runner died?): "
                         + _short(", ".join(stale), 120),
                         "the next review of that workstream removes them, or run `bt follow reset review` in its lead"))
    registered = {config.norm(e["path"]) for e in entries}
    try:
        orphans = sorted(d.name for d in wt.worktrees_dir(main).iterdir()
                         if d.is_dir() and d.name != wt.SCRATCH and config.norm(d) not in registered)
    except OSError:
        orphans = []
    if orphans:
        out.append(Check(f"worktrees:orphans:{label}", WARN,
                         f"{len(orphans)} director(ies) under .claude/worktrees that git does not know: " + _short(", ".join(orphans), 120),
                         "check them, then delete by hand (`bt wt retire` handles registered worktrees only)"))
    broken = []                      # (worktree name, lint problems); the main checkout included, it has no Exit to check
    for e in [entries[0], *linked]:
        if Path(e["path"]).is_dir() and (e is not entries[0] or capsule.now_path(Path(e["path"])).is_file()):
            found = capsule.problems_for(Path(e["path"]))
            if found:
                broken.append((Path(e["path"]).name, found))
    if broken:
        shown = "; ".join(f"{name}: " + " | ".join(_short(p_, 70) for p_ in found[:2]) + (f" (+{len(found) - 2})" if len(found) > 2 else "")
                          for name, found in broken[:4])
        more = f" (+{len(broken) - 4} more worktrees)" if len(broken) > 4 else ""
        out.append(Check(f"worktrees:now:{label}", WARN,
                         f"{len(broken)} worktree(s) whose .baton/now.md breaks the now.md contract: " + _short(shown, 420) + more,
                         "run `bt now check` in each worktree and fix what it lists (Exit:, Objective:, Next:, size, dated headings), "
                         "or `bt wt retire <name>`"))
    mismatched = []
    for e in entries:   # the main checkout included: a branch switched there leaves a stale brief too
        if not Path(e["path"]).is_dir() or e["prunable"]:
            continue
        line = capsule.brief_mismatch(Path(e["path"]), heuristic=True)
        if line:
            mismatched.append(f"{Path(e['path']).name}: " + line[len("Brief mismatch: "):].split(";")[0])
    if mismatched:
        out.append(Check(f"worktrees:brief:{label}", WARN,
                         f"{len(mismatched)} worktree(s) whose .baton/now.md describes another branch than HEAD: " + _short("; ".join(mismatched), 150),
                         "rewrite now.md for the current branch, or move that work to its own worktree"))
    if not cheap:
        depths = _stack_depths(main, linked)
        deep = sorted(Path(p_).name for p_, d in depths.items() if d > max_depth)
        if deep:
            out.append(Check(f"worktrees:depth:{label}", WARN,
                             f"{len(deep)} worktree(s) stacked more than {max_depth} deep: " + _short(", ".join(deep), 120),
                             "land or retire the lower layers; base new work on origin/master"))
        if gitutil.out(main, "remote") and Path(main).name.lower() not in wt.read_only_remotes():
            now = time.time()
            rows = gitutil.out(main, "for-each-ref", "--format=%(refname:short)\t%(committerdate:unix)", "refs/heads")
            stale = []
            for row in rows.splitlines():
                name, _, stamp = row.partition("\t")
                if stamp.isdigit() and now - int(stamp) > UNPUSHED_DAYS * 86400 and wt.has_unpushed(main, name):
                    stale.append(name)
            if stale:
                out.append(Check(f"worktrees:unpushed:{label}", WARN,
                                 f"{len(stale)} branch(es) with commits on no remote for over {UNPUSHED_DAYS} days: " + _short(", ".join(stale[:8]), 120),
                                 f"run `bt backup --repo {main}`"))
    if not out and count:
        out.append(Check(f"worktrees:{label}", OK, f"{count} linked worktree(s), cap {cap}"))
    return out


def check_worktrees(cheap: bool = False) -> list[Check]:
    from . import worktrees as wt
    out: list[Check] = []
    for main in wt.known_repos():
        out.extend(_guarded(f"worktrees:{main.name}", lambda main=main: _worktree_checks(main, cheap)))
    if not out:
        out.append(Check("worktrees", OK, "no repository with linked worktrees is known"))
    return out


# --------------------------------------------------------------------------- orchestration


def _guarded(name: str, fn) -> list[Check]:
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - one broken check must not hide the others
        return [Check(name, WARN, f"check failed: {type(exc).__name__}: {_short(exc, 120)}", "report this as a doctor bug")]


def run_checks(live: bool = False, *, cheap: bool = False) -> list[Check]:
    """All checks in order. ``cheap`` restricts to a, b, d, g, g2, h, i and the cheap part of l (worktrees); ``live`` adds the self-test (e)."""
    versions = cli_versions()
    plan = [("python", check_python), ("hooks", check_hooks), ("hook-forms", check_hook_forms), ("cmd-wrappers", check_wrappers),
            ("posix-install", check_posix_install), ("session-workflow", check_session_workflow),
            ("plugin-env", check_plugin_env)]
    if not cheap:
        plan += [("tools", check_tools), ("codex-trust", check_codex_trust)]
    plan.append(("hook-health", check_hook_health))
    if live and not cheap:
        plan.append(("selftest", check_selftest))
    if not cheap:
        plan.append(("formats", lambda: check_formats(versions)))
    plan += [("accounts", check_accounts), ("logins", check_logins), ("routing", check_routing), ("versions", lambda: check_versions(versions)),
             ("worktrees", lambda: check_worktrees(cheap))]
    if not cheap:
        plan += [("followers", check_followers), ("housekeeping", check_housekeeping)]
    out: list[Check] = []
    for name, fn in plan:
        out.extend(_guarded(name, fn))
    return out


KEEPALIVE_INTERVAL = 20 * 3600     # at most one background `bt keepalive` per this many seconds


def maybe_keepalive(now: float | None = None) -> list[str]:
    """Spawn ``bt keepalive`` detached when a Claude login is inside the warning window and not yet expired,
    at most once per ``KEEPALIVE_INTERVAL`` and unless ``[policy] keepalive = false``. Returns the notices."""
    now = time.time() if now is None else now
    if not accounts.keepalive_enabled():
        return []
    due = accounts.keepalive_due(accounts.LOGIN_WARN_DAYS, None, now)
    if not due:
        return []
    stamp = _read_state().get("keepalive_at")
    if isinstance(stamp, (int, float)) and not isinstance(stamp, bool) and 0 <= now - stamp < KEEPALIVE_INTERVAL:
        return []
    if not accounts.spawn_keepalive():
        return []
    _save_state(keepalive_at=now)
    return [f"baton: refreshing claude:{name} login (expires {accounts.fmt_date(refresh)})" for name, refresh in due]


def daily_warnings(max_age_hours: float = 24) -> list[str]:
    """One-line messages for non-OK cheap checks, at most once per ``max_age_hours``; ``[]`` otherwise. Never raises.

    A login about to expire starts a background refresh (``maybe_keepalive``) instead of being warned about.
    """
    lines: list[str] = []
    refreshing: set[str] = set()
    try:
        started = maybe_keepalive()
        lines += started
        refreshing = {f"login:claude:{m.split('claude:', 1)[1].split()[0]}" for m in started}
    except Exception:  # noqa: BLE001
        pass
    try:
        stamp = _read_state().get("time")
        if isinstance(stamp, (int, float)) and not isinstance(stamp, bool) and 0 <= time.time() - stamp < max_age_hours * 3600:
            return lines
        checks = run_checks(live=False, cheap=True)
        _save_state(time=time.time())
        return lines + [f"baton doctor: {c.name}: {c.detail}" + (f" ({c.remedy})" if c.remedy else "")
                        for c in checks if c.status not in (OK, SKIP) and not (c.status == WARN and c.name in refreshing)]
    except Exception:  # noqa: BLE001
        return lines


def apply_fixes() -> list[str]:
    """Mechanical repairs: stale registry entries, oversized hook.log, role files, non-main account links."""
    actions = []
    try:
        gone = prune_registry()
        actions.append(f"removed {len(gone)} registry entr(ies) for deleted worktrees" if gone else "registry: nothing to prune")
    except registry.RegistryUnavailable as exc:
        actions.append(f"error: registry unreadable, left alone: {exc}")
    try:
        if (config.STATE_DIR / "hook.log").stat().st_size > LOG_MAX_BYTES and trim_hook_log():
            actions.append(f"trimmed hook.log to the last {LOG_KEEP_LINES} lines")
    except OSError:
        pass
    try:
        from . import routing
        actions.extend(f"routing: {a}" for a in routing.apply())
    except Exception as exc:  # noqa: BLE001
        actions.append(f"error: routing apply failed: {exc}")
    for vendor in config.VENDORS:
        for account in config.account_names(vendor):
            if account != "main":
                actions.extend(f"{vendor}:{account}: {a}" for a in accounts.sync(vendor, account))
    return actions


def format_table(checks: list[Check]) -> str:
    width = max([len(c.name) for c in checks] + [5])
    lines = [f"{'STATUS':<6}  {'check':<{width}}  detail"]
    for c in checks:
        lines.append(f"{c.status:<6}  {c.name:<{width}}  {_short(c.detail, DETAIL_WIDTH)}")
        if c.status not in (OK, SKIP) and c.remedy:
            lines.append(f"{'':<6}  {'':<{width}}  -> {c.remedy}")
    return "\n".join(lines)


def run(args) -> int:
    fix = bool(getattr(args, "fix", False))
    if fix:
        for action in apply_fixes():
            print(f"fix: {action}")
    checks = run_checks(live=bool(getattr(args, "live", False)) or fix)
    if getattr(args, "json", False):
        print(json.dumps([asdict(c) for c in checks], indent=1, ensure_ascii=False))
    else:
        print(format_table(checks))
    return 1 if any(c.status == FAIL for c in checks) else 0


def register(sub) -> None:
    p = sub.add_parser("doctor", help="check hooks, formats, accounts and routing for drift")
    p.add_argument("--fix", action="store_true", help="repair stale registry entries, hook.log size, role files, account links (implies --live)")
    p.add_argument("--json", action="store_true", help="print the checks as JSON")
    p.add_argument("--live", action="store_true", help="also run the installed hook script as a subprocess self-test")
    p.set_defaults(func=run)
