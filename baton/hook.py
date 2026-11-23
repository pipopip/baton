"""Hook dispatcher: ``python -S hook.py <claude|codex>`` with one JSON object on stdin.

Never raises and always exits 0: any failure is logged as one line in
``STATE_DIR/hook.log``. Silent (no stdout) when ``BATON_ROLE`` is set, outside a
git worktree (unless ``bt attach`` mapped the session to one), or when there is nothing to say. Stdlib only, no ``git`` for
owner/registry work.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from . import capsule, config, diskspace, harvest, locking, paths, registry

STALE_TOKENS = 60_000          # resume warning threshold (context tokens)
LOG_MAX_BYTES = 256 * 1024
ERRORS_WINDOW = 7 * 86400      # hook errors older than this are never surfaced
ERRORS_TAIL = 64 * 1024        # only the end of hook.log is read when looking for new errors
NUDGE_STEP = 50_000            # a further nudge needs this much more context
STARTUP_TOUCH = 3600           # UserPromptSubmit refreshes a startup-list entry older than this

EVENTS = {
    "sessionstart": "SessionStart",
    "postcompact": "PostCompact",
    "sessionend": "SessionEnd",
    "userpromptsubmit": "UserPromptSubmit",
    "notification": "Notification",
    "stop": "Stop",
    "permissionrequest": "PermissionRequest",
    "posttooluse": "PostToolUse",
    "interrupt": "Interrupt",
}
# Codex only, macOS only (see install._codex_handlers): the events that move a tab's state and do nothing else. Claude's
# Stop goes to cycle_hook.py and its permission prompts arrive as Notification.
TAB_EVENTS = {"Stop": "ready", "PermissionRequest": "input", "PostToolUse": "working", "Interrupt": "idle"}
# Notification types that mean "needs you". Not idle_prompt: Claude sends it about a minute after every finished turn,
# which would turn each green tab amber.
INPUT_NOTIFICATIONS = ("permission_prompt", "elicitation_dialog")


# --------------------------------------------------------------------------- logging


def _log(vendor: str, event: str, exc: BaseException) -> None:
    """Append a one-line summary of ``exc`` to hook.log. Never raises."""
    try:
        tb = exc.__traceback__
        where = ""
        while tb is not None:  # innermost frame
            where = f"{os.path.basename(tb.tb_frame.f_code.co_filename)}:{tb.tb_lineno} in {tb.tb_frame.f_code.co_name}"
            tb = tb.tb_next
        message = " ".join(str(exc).split())[:300]
        line = f"{harvest.iso_now()} {vendor} {event or '-'} {type(exc).__name__}: {message} @ {where}\n"
        log = config.STATE_DIR / "hook.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        try:
            if log.stat().st_size > LOG_MAX_BYTES:
                os.replace(log, log.with_name("hook.log.1"))
        except OSError:
            pass
        with open(log, "a", encoding="utf-8") as fh:
            fh.write(line)
    except Exception:
        pass


def _guard(vendor: str, event: str, fn, default=None):
    """Run one step; log and swallow its failure so the remaining steps still run."""
    try:
        return fn()
    except Exception as exc:
        _log(vendor, event, exc)
        return default


def hook_error_notice(now: float | None = None) -> str:
    """One-line SessionStart notice when ``hook.log`` gained lines since ``hook-errors.seen`` was last touched.

    Stats first (the common case costs two stat calls), reads at most the last 64 KiB, and touches
    ``hook-errors.seen`` whenever it reports so each error is announced once. Without the marker,
    errors from the last 7 days count as new.
    """
    now = time.time() if now is None else now
    log = config.STATE_DIR / "hook.log"
    seen = config.STATE_DIR / "hook-errors.seen"
    cutoff = now - ERRORS_WINDOW
    try:
        log_mtime = log.stat().st_mtime
    except OSError:
        return ""
    try:
        cutoff = max(cutoff, seen.stat().st_mtime)
    except OSError:
        pass
    if log_mtime + 1 <= cutoff:
        return ""
    times = []
    for raw in harvest.read_tail_lines(log, ERRORS_TAIL):
        stamp = harvest.parse_iso(raw[:20].decode("ascii", errors="replace"))
        if stamp is not None and stamp + 1 > cutoff:   # log stamps are whole seconds
            times.append(stamp)
    if not times:
        return ""
    with open(seen, "a", encoding="utf-8"):
        pass
    os.utime(seen, (now, now))
    since = time.strftime("%Y-%m-%d %H:%M", time.localtime(min(times)))
    return f"baton: {len(times)} hook error(s) since {since} — run `bt doctor`"


# --------------------------------------------------------------------------- owner.json


def _write_json(path: Path, data: dict) -> None:
    harvest.atomic_write(path, json.dumps(data, indent=1))


def _remove(path: Path) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def _same_transcript(a: str | None, b: str | None) -> bool:
    return bool(a and b and config.norm(a) == config.norm(b))


def _owner_is_me(owner: dict, session_id: str | None, transcript: str | None,
                 vendor: str | None = None, account: str | None = None) -> bool:
    """A named owner requires its exact session ID; a shared transcript is not identity."""
    other = owner.get("session_id")
    return not owner or (not other and not owner.get("vendor")) or (
        other == session_id and (vendor is None or owner.get("vendor") == vendor)
        and (account is None or owner.get("account") == account))


def _may_record(root: Path, session_id: str | None, transcript: str | None,
                vendor: str | None = None, account: str | None = None) -> bool:
    """False when ``owner.json`` names another session: that session's registry record must stay intact
    (``registry.update`` clears the record when the session id changes, so a loser would erase it)."""
    return _owner_is_me(config.read_json(paths.baton_dir(root) / "owner.json"), session_id, transcript, vendor, account)


def _owner_last_active(root: Path, owner: dict) -> float | None:
    """Latest sign of life of the session named in ``owner.json``: its transcript's mtime, its claim time,
    or the registry's ``last_seen`` for that very session. None when nothing is known."""
    stamps = [config.mtime(owner.get("transcript")), harvest.parse_iso(owner.get("claimed_at"))]
    rec = registry.session(root, str(owner.get("vendor") or ""))
    if (isinstance(rec, dict) and rec.get("session_id") == owner.get("session_id")
            and rec.get("account") == owner.get("account")):
        last_seen = rec.get("last_seen")
        if isinstance(last_seen, (int, float)) and not isinstance(last_seen, bool):
            stamps.append(float(last_seen))
    known = [t for t in stamps if t is not None]
    return max(known) if known else None


def _pid_alive(pid: int) -> bool:
    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    import ctypes
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        return False
    try:
        code = ctypes.c_ulong()
        return bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == 259  # STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def _attached_to(session_id: str, root: str, *, vendor: str, account: str) -> bool:
    """Whether the given identity is attached to the normalized worktree path."""
    rec = registry.attached_record(session_id, vendor=vendor, account=account)
    return rec is not None and config.norm(rec["worktree"]) == root


def _session_in_root(rec: dict, root: str) -> bool:
    """Whether a Claude sessions record was launched inside the worktree whose ``config.norm`` key is ``root``."""
    cwd = rec.get("cwd")
    if not isinstance(cwd, str) or not cwd:
        return False
    found = paths.find_worktree_root(cwd)
    return found is not None and config.norm(found) == root


def _codex_rollout_home(transcript: object, sid: str) -> Path | None:
    """Recover an older claim's home from its native rollout path."""
    if not isinstance(transcript, str) or not transcript:
        return None
    path = Path(transcript)
    if not (path.is_absolute() and path.name.startswith("rollout-") and path.name.endswith(f"-{sid}.jsonl")):
        return None
    if len(path.parts) < 6 or path.parts[-5] != "sessions":
        return None
    if not all(len(part) == digits and part.isascii() and part.isdecimal()
               for part, digits in zip(path.parts[-4:-1], (4, 2, 2))):
        return None
    return path.parents[4]


def _codex_owner_homes(owner: dict, sid: str) -> list[Path]:
    recorded = owner.get("config_home")
    if isinstance(recorded, str) and Path(recorded).is_absolute():
        return [Path(recorded)]
    account = owner.get("account")
    rollout_home = _codex_rollout_home(owner.get("transcript"), sid)
    if rollout_home is not None and (not account or account == config.account_from_env(
            "codex", {config.ACCOUNT_ENV["codex"]: str(rollout_home)})):
        return [rollout_home]
    configured = config.account_names("codex")
    if account in configured:
        return [config.account_dir("codex", account)]
    if account:
        return []
    return [config.account_dir("codex", name) for name in configured]


def _codex_owner_alive(owner: dict, sid: str) -> bool | None:
    if sid in {".", ".."} or any(char in sid for char in "/\\:\0"):
        return None
    uncertain = False
    unlocked = False
    for home in _codex_owner_homes(owner, sid):
        state = locking.codex_writer_lock(home / "thread-writer-locks" / f"{sid}.lock")
        if state is locking.WriterLockState.HELD:
            return True
        uncertain |= state is locking.WriterLockState.UNKNOWN
        unlocked |= state is locking.WriterLockState.UNLOCKED
    return False if unlocked and not uncertain else None


def owner_process_alive(owner: dict, root: Path | None = None) -> bool | None:
    """Whether the session named in ``owner.json`` is still running: True, False, or None (cannot tell).

    Claude Code lists running sessions as ``<config_dir>/sessions/<pid>.json`` with a
    ``sessionId``; Codex locks ``<home>/thread-writer-locks/<thread>.lock`` while a
    thread runs. SessionEnd hooks do not run when a terminal is closed, so the process
    is the reliable signal.

    With ``root``, a Claude process counts only when its launch directory is in that worktree
    or ``bt attach`` bound its session to it (a session started in ``C:/Work`` and attached):
    one conversation can run as two processes (resumed in another worktree), and the other
    process must not keep a claim alive in a worktree it is not working in.
    """
    sid = owner.get("session_id")
    if not isinstance(sid, str) or not sid:
        return None
    vendor = owner.get("vendor")
    if vendor == "claude":
        root_key = config.norm(root) if root is not None else None
        seen_dir = False
        claimed_account = owner.get("account")
        configured = config.account_names("claude")
        accounts = [claimed_account] if claimed_account else configured
        for account in accounts:
            attached_here = root_key is not None and _attached_to(sid, root_key, vendor=vendor, account=account)
            home = owner.get("config_home")
            if isinstance(home, str) and os.path.isabs(home):
                sessions = Path(home) / "sessions"
            elif account in configured:
                sessions = config.account_dir("claude", account) / "sessions"
            else:
                continue
            if not sessions.is_dir():
                continue
            seen_dir = True
            for f in sessions.glob("*.json"):
                try:
                    rec = json.loads(f.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                if not (isinstance(rec, dict) and rec.get("sessionId") == sid):
                    continue
                if root_key is not None and not (attached_here or _session_in_root(rec, root_key)):
                    continue
                try:
                    alive = _pid_alive(int(rec.get("pid") or f.stem))
                except ValueError:
                    return None
                if alive or root_key is None:
                    return alive
        return False if seen_dir else None
    if vendor == "codex":
        return _codex_owner_alive(owner, sid)
    return None


def claim_owner(root: Path, vendor: str, account: str, session_id: str | None, transcript: str | None,
                source: str, now: float) -> str:
    """Claim ``owner.json`` for this session. Returns a conflict warning ('' when none) and leaves the file alone on conflict."""
    try:
        with locking.exclusive(paths.baton_dir(root) / "owner.lock"):
            return _claim_owner_locked(root, vendor, account, session_id, transcript, source, now)
    except (OSError, TimeoutError) as exc:
        _log(vendor, "SessionStart", exc)
        return "WARNING: cannot lock worktree ownership. Do not edit files here; tell the user."


def _claude_process_id(session_id: str | None) -> int | None:
    """Exact current terminal process evidence, not an inferred same-account predecessor."""
    pid = os.environ.get("CLAUDE_PID", "")
    if not pid.isdecimal() or int(pid) <= 0:
        return None
    home = config.expand(os.environ.get("CLAUDE_CONFIG_DIR") or "~/.claude")
    rec = config.read_json(home / "sessions" / f"{pid}.json")
    return int(pid) if session_id and rec.get("sessionId") == session_id else None


def _claim_owner_locked(root: Path, vendor: str, account: str, session_id: str | None, transcript: str | None,
                        source: str, now: float) -> str:
    path = paths.baton_dir(root) / "owner.json"
    owner = config.read_json(path)
    other_id = owner.get("session_id")
    if not _owner_is_me(owner, session_id, transcript, vendor, account):
        alive = _guard(vendor, "SessionStart", lambda: owner_process_alive(owner, root), None)
        mtime = None if alive is False else _owner_last_active(root, owner)
        if alive is True:
            mtime = max(mtime or 0.0, now)
        # /clear is a successor only when the exact terminal process now names the new session.
        pid = _claude_process_id(session_id) if vendor == "claude" else None
        predecessor = (source == "clear" and owner.get("vendor") == vendor and owner.get("account") == account
                       and pid is not None and owner.get("process_pid") == pid)
        if mtime is not None and now - mtime < config.OWNER_LIVE_SECONDS and not predecessor:
            minutes = max(0, int((now - mtime) // 60))
            return (
                f"WARNING: worktree is in use by {owner.get('vendor') or '?'}:{owner.get('account') or '?'} "
                f"session {harvest.id8(str(other_id))}, active {minutes} min ago. "
                "Do not edit files here; tell the user."
            )
    same_owner = _owner_is_me(owner, session_id, transcript, vendor, account) and bool(owner)
    token = os.environ.get("BATON_TERMINAL_TOKEN", "")
    marked = os.environ.get("BATON_LAUNCH_CWD")
    launched = launch_dir(vendor, session_id, transcript) if marked else None
    valid_terminal = (len(token) == 32 and all(c in "0123456789abcdef" for c in token)
                      and marked and config.norm(marked) in {config.norm(root), config.norm(launched or root)})
    terminal_root = marked if valid_terminal else owner.get("terminal_root") if same_owner else None
    if not valid_terminal:
        token = owner.get("terminal_token") if same_owner else None
    claim = {
        "terminal_token": token,
        "terminal_root": terminal_root,
        "vendor": vendor,
        "account": account,
        "session_id": session_id,
        "transcript": transcript,
        "claimed_at": harvest.iso_now(now),  # every claim (start, resume, compact) refreshes liveness
        "process_pid": _claude_process_id(session_id) if vendor == "claude" else None,
    }
    override = os.environ.get(config.ACCOUNT_ENV[vendor])
    if override and config.account_from_env(vendor) == account:
        claim["config_home"] = os.path.abspath(config.expand(override))
    # the iTerm2 session this agent runs in (`bt <ws>` records it); `bt <ws>` from another tab selects that tab. Taken from
    # this process's environment only: an earlier owner's value names another tab.
    iterm_session = os.environ.get("BATON_ITERM_SESSION")
    if isinstance(iterm_session, str) and iterm_session:
        claim["iterm_session"] = iterm_session
    _write_json(path, claim)
    return ""


def _release_owner_locked(root: Path, session_id: str | None, transcript: str | None,
                          vendor: str | None = None, account: str | None = None) -> None:
    path = paths.baton_dir(root) / "owner.json"
    owner = config.read_json(path)
    if owner and _owner_is_me(owner, session_id, transcript, vendor, account) and (
            (session_id and owner.get("session_id") == session_id)
            or (not owner.get("session_id") and _same_transcript(owner.get("transcript"), transcript))):
        _remove(path)


def release_owner(root: Path, session_id: str | None, transcript: str | None, *,
                  vendor: str | None = None, account: str | None = None) -> None:
    """Release this session's ownership, optionally restricting vendor and account."""
    with locking.exclusive(paths.baton_dir(root) / "owner.lock"):
        _release_owner_locked(root, session_id, transcript, vendor, account)


def _inbox_complete(root: Path, text: str) -> bool:
    """Check that every current inbox note was represented; errors preserve the marker."""
    try:
        inbox = paths.baton_dir(root) / "inbox"
        with os.scandir(inbox) as entries:
            names = [entry.name for entry in entries if entry.name.endswith(".md") and entry.is_file()]
        return all(f"- {name}:" in text for name in names) and " more: `bt inbox`)" not in text
    except OSError:
        return False


def _restore_unread(flag: Path) -> None:
    flag.parent.mkdir(parents=True, exist_ok=True)
    flag.touch(exist_ok=True)


# --------------------------------------------------------------------------- events


def _stale_resume_warning(root: Path, vendor: str, account: str, data: dict, transcript: str | None, now: float) -> str:
    if vendor == "codex":
        mtime = config.mtime(transcript)
        if mtime is None or now - mtime <= config.cache_ttl_seconds("codex"):
            return ""
        tokens = harvest.ctx_tokens("codex", transcript)
    else:
        if not data.get("prompt_cache_likely_expired"):
            return ""
        tokens = data.get("context_tokens")
    if not isinstance(tokens, int) or isinstance(tokens, bool) or tokens <= STALE_TOKENS:
        return ""
    capsule_k = max(1, round(capsule.estimate_tokens(root, vendor, account) / 1000))
    return (
        f"Resuming ~{round(tokens / 1000)}k tokens with a cold cache: the next prompt re-reads all of it. "
        f"Cheaper: /clear (Claude) or a new session — baton reloads a ~{capsule_k}k-token capsule automatically."
    )


SESSION_WORKFLOW = Path(os.environ.get("BATON_SESSION_WORKFLOW") or config.CODE_ROOT / "session-workflow")


def _registration_name(vendor: str, transcript: str, launched: str | None) -> str | None:
    if vendor == "claude":
        # /rename and -n are persisted as custom-title records; the latest one wins.
        for raw in reversed(harvest.read_tail_lines(Path(transcript), 64 * 1024)):
            try:
                rec = json.loads(raw)
            except ValueError:
                continue
            if isinstance(rec, dict) and rec.get("type") == "custom-title":
                title = rec.get("customTitle")
                if isinstance(title, str) and title.strip() and "\n" not in title and "\r" not in title:
                    return title
    marked = os.environ.get("BATON_LAUNCH_CWD")
    name = os.environ.get("BATON_SESSION_NAME")
    if launched and marked and config.norm(launched) == config.norm(marked) and name:
        return name
    return None


def register_session_workflow(root: Path, vendor: str, session_id: str | None, transcript: str | None,
                              launch_cwd: str | None = None) -> bool:
    """Register this session with the session-workflow helper (bounded, best effort).

    Keeps ``csessions`` and the Studio inventory accurate without an agent-run
    pickup step. Skipped when the helper is absent or the session lacks an id
    or transcript (the helper's hooks need both).
    """
    script = SESSION_WORKFLOW / "workflow.py"
    if not (session_id and transcript and script.exists()):
        return False
    account = config.account_from_env(vendor, os.environ)
    home = config.expand(os.environ.get(config.ACCOUNT_ENV[vendor]) or config.account_dir(vendor, account))
    launched = launch_dir(vendor, session_id, transcript) or launch_cwd
    name = _registration_name(vendor, transcript, launched)
    if vendor == "claude" and not name and launched:
        name = _native_registration_name(session_id, launched, home)
    argv = [config.PYTHON, "-S", str(script), "--state", str(SESSION_WORKFLOW / ".state" / "workflow.sqlite3"),
            "register", "--workstream", paths.ws_name(root), "--thread", session_id, "--worktree", str(root),
            "--transcript", transcript, "--branch", capsule._head_branch(root) or "HEAD", "--client", vendor,
            "--config-home", str(home)]
    if launched:
        argv += ["--launch-cwd", launched]
    if name:
        argv += ["--name", name]
    import subprocess
    try:
        result = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                env=config.clean_env(), timeout=3, creationflags=config.CREATE_NO_WINDOW)
        return result.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _native_registration_name(session_id: str, launched: str, home: Path) -> str | None:
    """Resolve a missing peer name from one exact live native endpoint, without a model."""
    import subprocess
    env = config.clean_env()
    env["CLAUDE_CONFIG_DIR"] = str(home)
    try:
        result = subprocess.run([config.exe("claude"), "agents", "--json"], env=env,
                                stdin=subprocess.DEVNULL, capture_output=True, text=True,
                                encoding="utf-8", timeout=2, creationflags=config.CREATE_NO_WINDOW)
        rows = json.loads(result.stdout)
        if result.returncode or not isinstance(rows, list):
            return None
        matches = set()
        for row in rows:
            if (not isinstance(row, dict) or row.get("sessionId") != session_id
                    or str(row.get("status", "")).lower() in {"dead", "exited", "stopped", "terminated"}
                    or not isinstance(row.get("cwd"), str) or config.norm(row["cwd"]) != config.norm(launched)
                    or not isinstance(row.get("pid"), int) or isinstance(row["pid"], bool)
                    or row["pid"] <= 0 or not _pid_alive(row["pid"])):
                continue
            name = row.get("name")
            if isinstance(name, str) and name.strip() and not any(c in name for c in "\r\n"):
                matches.add((row["pid"], name))
        return next(iter(matches))[1] if len(matches) == 1 else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def on_session_start(vendor: str, data: dict, root: Path, attachment: dict | None = None) -> dict | None:
    _guard(vendor, "SessionStart", lambda: paths.ensure_excluded(root))
    _guard(vendor, "SessionStart", lambda: paths.baton_dir(root, create=True))
    try:
        lock = locking.exclusive(paths.baton_dir(root) / "owner.lock")
        lock.__enter__()
    except (OSError, TimeoutError) as exc:
        _log(vendor, "SessionStart", exc)
        warning = "WARNING: cannot lock worktree ownership. Do not edit files here; tell the user."
        return _output("SessionStart", warning, warning)
    try:
        if not _attachment_current(vendor, data.get("session_id"), attachment):
            return None
        return _on_session_start_locked(vendor, data, root)
    finally:
        lock.__exit__(None, None, None)


def _on_session_start_locked(vendor: str, data: dict, root: Path) -> dict | None:
    now = time.time()
    session_id = data.get("session_id") or None
    transcript = data.get("transcript_path") or None
    source = data.get("source") or "startup"
    account = config.account_from_env(vendor, os.environ)

    warning = _guard(vendor, "SessionStart", lambda: _claim_owner_locked(root, vendor, account, session_id, transcript, source, now),
                     "WARNING: cannot lock worktree ownership. Do not edit files here; tell the user.")
    if warning:
        return _output("SessionStart", warning, warning)
    if not _may_record(root, session_id, transcript, vendor, account):
        warning = "WARNING: worktree ownership changed. Do not edit files here; tell the user."
        return _output("SessionStart", warning, warning)
    _tab_state("start", root, data, vendor)    # again at every start: the badge is gone after /clear and has to return on a resume
    _guard(vendor, "SessionStart", lambda: capsule.rotate_log(root))

    fields = dict(session_id=session_id, transcript=transcript, account=account, source=source, last_seen=now)
    recorded = registry.session(root, vendor)
    if recorded.get("session_id") != session_id or not recorded.get("started_at"):
        fields["started_at"] = harvest.iso_now()   # once per session: resume and compaction keep the session's age
    if source == "resume" and isinstance(data.get("context_tokens"), int):
        fields["ctx_tokens"] = data["context_tokens"]
    if not warning:   # a session that lost the owner check must not replace the owner's registry record
        _guard(vendor, "SessionStart", lambda: registry.update(root, vendor, ws=paths.ws_name(root), **fields))
        _guard(vendor, "SessionStart", lambda: registry.startup_add(root, vendor, account, session_id, now))
        if source != "compact":
            _guard(vendor, "SessionStart", lambda: register_session_workflow(
                root, vendor, session_id, transcript, data.get("cwd") or os.getcwd()))

    # Drop the unread flag before listing the inbox: a note a follower writes meanwhile raises it again.
    flag = paths.baton_dir(root) / "inbox" / ".unread"
    had_flag = flag.exists()
    _remove(flag)
    delivered = False
    try:
        parts = capsule.sections(root, vendor, source, account)
        context = capsule.assemble(parts, capsule.CAPS.get(vendor, capsule.CAPS["claude"]))
        messages = []
        if source == "resume":
            stale = _guard(vendor, "SessionStart", lambda: _stale_resume_warning(root, vendor, account, data, transcript, now), "")
            if stale:
                messages.append(stale)
        errors = _guard(vendor, "SessionStart", hook_error_notice, "")
        if errors:
            messages.append(errors)
        result = _output("SessionStart", context, "\n".join(messages))
        delivered = _inbox_complete(root, context) if had_flag else True
        return result
    finally:
        if had_flag and not delivered:
            _guard(vendor, "SessionStart", lambda: _restore_unread(flag))


def count_compaction(vendor: str, root: Path, session_id: str | None, transcript: str | None, now: float) -> None:
    with locking.exclusive(paths.baton_dir(root) / "owner.lock"):
        _count_compaction_locked(vendor, root, session_id, transcript, now)


def _count_compaction_locked(vendor: str, root: Path, session_id: str | None, transcript: str | None, now: float) -> None:
    """Add one to the ``compactions`` count of the session's registry record (``bt status`` shows it).

    ``registry.update`` clears the record when the session id changes, so the count restarts with a new
    session. Not counted when another session owns the worktree."""
    if not session_id or not _may_record(root, session_id, transcript, vendor, config.account_from_env(vendor, os.environ)):
        return
    rec = registry.session(root, vendor)
    count = rec.get("compactions") if rec.get("session_id") == session_id else 0
    count = count if isinstance(count, int) and not isinstance(count, bool) else 0
    registry.update(root, vendor, ws=paths.ws_name(root), session_id=session_id, transcript=transcript,
                    account=config.account_from_env(vendor, os.environ), last_seen=now, compactions=count + 1)


def on_post_compact(vendor: str, data: dict, root: Path, attachment: dict | None = None) -> None:
    with locking.exclusive(paths.baton_dir(root) / "owner.lock"):
        if _attachment_current(vendor, data.get("session_id"), attachment):
            _on_post_compact_locked(vendor, data, root)


def _on_post_compact_locked(vendor: str, data: dict, root: Path) -> None:
    if not _may_record(root, data.get("session_id") or None, data.get("transcript_path") or None,
                       vendor, config.account_from_env(vendor, os.environ)):
        return
    harvest.write_compact(root, vendor, data.get("session_id"), data.get("transcript_path"), data.get("compact_summary"))
    _guard(vendor, "PostCompact", lambda: _count_compaction_locked(
        vendor, root, data.get("session_id") or None, data.get("transcript_path") or None, time.time()))


def _is_lead(root: Path, data: dict) -> bool:
    """False when the hook payload's session is not the one ``owner.json`` names: a nested headless ``claude -p``
    started from the agent's shell inherits ``BATON_TTY`` but is not the tab's session."""
    mine = data.get("session_id")
    theirs = config.read_json(paths.baton_dir(root) / "owner.json").get("session_id")
    return not (mine and theirs and mine != theirs)


def _tab_state(state: str, root: Path, data: dict, vendor: str = "", detail: str = "") -> None:
    """Badge, colour, status and notification of this session's iTerm2 tab (``baton.iterm``), for the lead session of
    the worktree only; a no-op outside iTerm2, imported only when a hook needs it, and never a reason for a hook to fail
    or print."""
    try:
        if not _is_lead(root, data):
            return
        from . import iterm
        group = iterm.group_of(root)[0] if state == "start" else ""    # only a start colours the tab: no config reads otherwise
        wrote = iterm.tab_state(state, iterm.label_for(None, root), vendor=vendor, repo=group, detail=detail)
        if state == "start" and wrote:      # a real iTerm2 tab took the sequences: arrange the tabs in the background
            iterm.arrange_soon()
    except Exception:
        pass


def _blocked_marker() -> Path | None:
    """Where a Codex permission request leaves its mark for this tab (one small file per tty under the state
    directory); None when this session has no recorded tty."""
    name = os.path.basename(str(os.environ.get("BATON_TTY") or ""))
    name = "".join(ch for ch in name if ch.isascii() and ch.isalnum())
    return config.STATE_DIR / "tab-blocked" / name if name else None


def _command_of(data: dict) -> str:
    """The command of a tool call as one string ("" when it has none), to tell one call from another."""
    asked = data.get("tool_input") if isinstance(data.get("tool_input"), dict) else {}
    what = asked.get("command") or ""
    return " ".join(str(part) for part in what) if isinstance(what, (list, tuple)) else str(what)


def _blocked_by(marker: Path, data: dict, call: bool = True) -> bool:
    """True when ``marker`` records a permission request of this payload's session and, with ``call``, of this very
    tool call as far as both sides say: the same turn, tool name and command. Codex may run tools side by side, and a
    tool that needed no approval must not end another one's wait."""
    try:
        seen = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if not isinstance(seen, dict) or seen.get("session_id") != data.get("session_id"):
        return False
    if not call:
        return True
    mine = {"turn_id": data.get("turn_id"), "tool_name": data.get("tool_name"), "command": _command_of(data)}
    return all(not seen.get(key) or not mine[key] or seen[key] == mine[key] for key in mine)


def _clear_blocked(data: dict) -> None:
    """Remove this tab's mark when it is this session's (a nested agent started from the tab shares the tty)."""
    try:
        marker = _blocked_marker()
        if marker is not None and _blocked_by(marker, data, call=False):
            marker.unlink()
    except OSError:
        pass


def _permission_detail(data: dict) -> str:
    """One line naming what a Codex permission request wants: the tool and its reason, or its command."""
    tool = data.get("tool_name") if isinstance(data.get("tool_name"), str) else ""
    asked = data.get("tool_input") if isinstance(data.get("tool_input"), dict) else {}
    what = asked.get("description") or asked.get("command") or ""
    if isinstance(what, (list, tuple)):
        what = " ".join(str(part) for part in what)
    return ": ".join(part for part in (tool, str(what).strip()) if part)


def on_tab_event(event: str, data: dict) -> None:
    """A Codex ``Stop``, ``PermissionRequest``, ``PostToolUse`` or ``Interrupt``: the tab's state, nothing else, and
    nothing is ever printed (Codex then goes on as if there were no hook: a permission request still asks the user).
    Codex has no event for an answered permission request, so the request leaves a mark and the tab goes back to
    working when that tool call finishes; without a mark ``PostToolUse`` costs one failed open."""
    try:
        marker = _blocked_marker()
        if marker is None:                   # not a tab `bt <ws>` started in iTerm2
            return
        if event == "PostToolUse" and not _blocked_by(marker, data):
            return
        if _codex_child(data.get("transcript_path")):
            return
        cwd = data.get("cwd") if isinstance(data.get("cwd"), str) and data.get("cwd") else os.getcwd()
        root, _ = resolve_root("codex", cwd, data.get("session_id") or None, data.get("transcript_path") or None)
        if root is None or not _is_lead(root, data):
            return
        detail = ""
        if event == "PermissionRequest":
            detail = _permission_detail(data)
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(json.dumps({"session_id": data.get("session_id"), "turn_id": data.get("turn_id"),
                                          "tool_name": data.get("tool_name"), "command": _command_of(data)}), encoding="utf-8")
        else:
            _clear_blocked(data)
        _tab_state(TAB_EVENTS[event], root, data, "codex", detail)
    except Exception:  # noqa: BLE001 - a tab's state is never a reason for a hook to fail
        pass


def on_notification(data: dict, root: Path) -> None:
    """A Claude ``Notification`` hook (the installed matcher is ``permission_prompt|elicitation_dialog``): the tab asks
    for the user. Other notification types are ignored. The tab stays amber after the permission is granted until the
    turn ends (green) or the next prompt is sent."""
    kind = data.get("notification_type")
    if kind is None or kind in INPUT_NOTIFICATIONS:
        _tab_state("input", root, data, "claude")


def on_session_end(vendor: str, data: dict, root: Path, attachment: dict | None = None) -> None:
    if vendor == "codex":
        _clear_blocked(data)
    now = time.time()
    session_id = data.get("session_id") or None
    transcript = data.get("transcript_path") or None
    fields = dict(session_id=session_id, transcript=transcript,
                  account=config.account_from_env(vendor, os.environ), last_seen=now)
    tokens, model = _guard(vendor, "SessionEnd", lambda: harvest.ctx_info(vendor, transcript), (None, None))
    if tokens is not None:
        fields["ctx_tokens"] = tokens
    if model is not None:
        fields["model"] = model
    account = fields["account"]
    def finish() -> None:
        with locking.exclusive(paths.baton_dir(root) / "owner.lock"):
            if (_attachment_current(vendor, session_id, attachment)
                    and _may_record(root, session_id, transcript, vendor, account)):
                if data.get("reason") != "clear":  # /clear keeps the tab until SessionStart
                    _tab_state("end", root, data, vendor)
                registry.update(root, vendor, ws=paths.ws_name(root), **fields)
                _release_owner_locked(root, session_id, transcript, vendor, account)
                registry.startup_remove(root, session_id, vendor=vendor, account=account)
    _guard(vendor, "SessionEnd", finish)


def context_nudge(vendor: str, data: dict, root: Path, now: float) -> str:
    with locking.exclusive(paths.baton_dir(root) / "owner.lock"):
        return _context_nudge_locked(vendor, data, root, now)


def _context_nudge_locked(vendor: str, data: dict, root: Path, now: float) -> str:
    """Record the session's context size in the registry; return a hand-over nudge when it is large.

    The size and model come from the transcript tail (``harvest.ctx_info``). Above the session's limit
    (``config.rotate_tokens(vendor, model)``, from ``[policy] rotate_context_tokens``) a Claude session is told,
    once and then again for every further ``NUDGE_STEP`` of growth, that it continues and Baton asks for the
    handover when it cycles it; the last level nudged is kept in the registry record (``nudge_level``) and follows
    the size down after a compaction. A Codex session has no cycle hook, so it is asked once, when the limit is
    first passed, to bring now.md up to contract for the owner's ``bt <ws> -Fresh``; its level never follows the
    size down, so the request is not repeated after a compaction or for further growth.
    """
    session_id = data.get("session_id") or None
    transcript = data.get("transcript_path") or None
    tokens, model = harvest.ctx_info(vendor, transcript)
    if tokens is None or not _may_record(root, session_id, transcript, vendor, config.account_from_env(vendor, os.environ)):
        return ""   # another session owns the worktree: neither its record nor its hand-over is ours to touch
    rec = registry.session(root, vendor)
    prior = rec.get("nudge_level") if session_id is None or rec.get("session_id") == session_id else 0
    prior = prior if isinstance(prior, int) and not isinstance(prior, bool) else 0
    level = tokens // NUDGE_STEP * NUDGE_STEP
    text = ""
    limit = config.rotate_tokens(vendor, model)
    if tokens > limit and level > prior and (vendor != "codex" or prior == 0):
        if vendor == "codex":
            # Codex has no Stop hook, so nothing replaces its process: it is asked once for a handover the owner's
            # `bt <ws> -Fresh` starts from (nudge_level stays set for the rest of the session).
            text = (f"Context is ~{round(tokens / 1000)}k tokens (Baton continuity advisory), above this session's "
                    f"{round(limit / 1000)}k limit. Once, at a safe boundary, bring .baton/now.md up to its contract "
                    "(Objective, State with commit shas, In flight, Next; check with `bt now check`); the owner "
                    f"restarts you with `bt {paths.ws_name(root)} -Fresh`. Keep working until then: this is not a "
                    "request to stop or to make a checkpoint commit.")
        else:
            text = (f"Context is ~{round(tokens / 1000)}k tokens (Baton continuity advisory). "
                    "Continue the active task; nothing is needed from you now. Baton asks for the handover when it "
                    "cycles the session. A context warning is not task completion or a requirement to stop, "
                    "restart, close out, write notes, or create a checkpoint commit.")
        nudged = level
    elif vendor == "codex":
        nudged = prior
    else:
        nudged = min(prior, level)
    fields = dict(session_id=session_id, transcript=transcript, account=config.account_from_env(vendor, os.environ),
                  ctx_tokens=tokens, model=model, nudge_level=nudged, last_seen=now)
    registry.update(root, vendor, ws=paths.ws_name(root), **fields)
    return text


def on_user_prompt(vendor: str, data: dict, root: Path, attachment: dict | None = None) -> dict | None:
    with locking.exclusive(paths.baton_dir(root) / "owner.lock"):
        if not _attachment_current(vendor, data.get("session_id"), attachment):
            return None
        _tab_state("working", root, data, vendor)
        return _on_user_prompt_locked(vendor, data, root)


def _on_user_prompt_locked(vendor: str, data: dict, root: Path) -> dict | None:
    if not _may_record(root, data.get("session_id") or None, data.get("transcript_path") or None,
                       vendor, config.account_from_env(vendor, os.environ)):
        return None
    now = time.time()
    parts = []
    nudge = _guard(vendor, "UserPromptSubmit", lambda: _context_nudge_locked(vendor, data, root, now), "")
    if nudge:
        parts.append(nudge)
    session_id = data.get("session_id") or None
    # keeps sessions that started before the hook-maintained list existed (or were reseeded away) listed
    from . import compute_pickup
    pickup = _guard(vendor, "UserPromptSubmit", lambda: compute_pickup.prompt_summary(root, session_id), "")
    if pickup:
        parts.append(pickup)
    # while a drive this session needs is nearly full, every prompt of the worktree's owner says so
    low = _guard(vendor, "UserPromptSubmit", lambda: diskspace.prompt_warning(diskspace.session_paths(root)), "")
    if low:
        parts.append(low)
    if session_id and _may_record(root, session_id, data.get("transcript_path") or None, vendor, config.account_from_env(vendor, os.environ)):
        _guard(vendor, "UserPromptSubmit", lambda: registry.startup_add(
            root, vendor, config.account_from_env(vendor, os.environ), session_id, now, STARTUP_TOUCH))
    flag = paths.baton_dir(root) / "inbox" / ".unread"
    had_flag = flag.exists()
    delivered = False
    try:
        if had_flag:
            _remove(flag)  # a concurrent note raises the flag again
            notes = capsule.inbox_notes(root)
            if notes:
                parts.append(f"Follower results waiting {capsule.INBOX_HINT}:\n{capsule.inbox_lines(notes)}")
        text = "\n".join(parts)
        result = _output("UserPromptSubmit", text)
        delivered = _inbox_complete(root, text) if had_flag else True
        return result
    finally:
        if had_flag and not delivered:
            _guard(vendor, "UserPromptSubmit", lambda: _restore_unread(flag))


def _output(event: str, context: str, system: str = "") -> dict | None:
    if not context and not system:
        return None
    out: dict = {"hookSpecificOutput": {"hookEventName": event, "additionalContext": context}}
    if system:
        out["systemMessage"] = system
    return out


def _claude_launch_dir(session_id: str, environ) -> str | None:
    """``cwd`` of the Claude sessions file naming ``session_id``: ``<CLAUDE_PID>.json`` first, else any ``*.json``."""
    sessions = config.expand(environ.get("CLAUDE_CONFIG_DIR") or "~/.claude") / "sessions"
    pid = environ.get("CLAUDE_PID")
    candidates = [sessions / f"{pid}.json"] if pid else []
    candidates += sorted(sessions.glob("*.json"))
    for path in candidates:
        rec = config.read_json(path)
        if rec.get("sessionId") == session_id and isinstance(rec.get("cwd"), str) and rec["cwd"]:
            return rec["cwd"]
    return None


def _codex_launch_dir(transcript: str) -> str | None:
    """``payload.cwd`` of the rollout's first line when that line is a ``session_meta`` record."""
    with open(transcript, encoding="utf-8-sig", errors="replace") as fh:
        first = fh.readline()
    rec = json.loads(first) if first.strip() else None
    payload = rec.get("payload") if isinstance(rec, dict) and rec.get("type") == "session_meta" else None
    cwd = payload.get("cwd") if isinstance(payload, dict) else None
    return cwd if isinstance(cwd, str) and cwd else None


def launch_dir(vendor: str, session_id: str | None, transcript: str | None, environ=None) -> str | None:
    """Directory the session was launched in, from the vendor's own records; None when unknown. Never raises."""
    environ = os.environ if environ is None else environ
    try:
        if vendor == "claude":
            return _claude_launch_dir(session_id, environ) if isinstance(session_id, str) and session_id else None
        return _codex_launch_dir(transcript) if isinstance(transcript, str) and transcript else None
    except (OSError, ValueError):
        return None


def _resolve_root(vendor: str, cwd: str, session_id: str | None, transcript: str | None,
                  environ=None) -> tuple[Path | None, dict | None]:
    account = config.account_from_env(vendor, environ)
    record = registry.attached_record(session_id, vendor=vendor, account=account)
    attached = registry.attachment_target(record)
    if attached is not None:
        return attached, record
    launched = launch_dir(vendor, session_id, transcript, environ) if environ is not None else launch_dir(
        vendor, session_id, transcript)
    return paths.find_worktree_root(cwd if launched is None else launched), None


def resolve_root(vendor: str, cwd: str, session_id: str | None, transcript: str | None = None,
                 *, environ=None) -> tuple[Path | None, bool]:
    """Resolve the scoped attachment, then the session launch directory, then the hook's cwd."""
    root, attached = _resolve_root(vendor, cwd, session_id, transcript, environ)
    return root, attached is not None


def _attachment_current(vendor: str, session_id: str | None, expected: dict | None) -> bool:
    """Check the binding again under owner.lock before a hook mutates that worktree."""
    account = config.account_from_env(vendor)
    current = registry.attached_record(session_id, vendor=vendor, account=account)
    if expected is None:
        return registry.attachment_target(current) is None
    return current is not None and all(current.get(k) == expected.get(k)
                                       for k in ("worktree", "attached_at", "binding_id"))


def handle(vendor: str, data: dict) -> dict | None:
    """Process one hook payload; returns the JSON object to print, or None for silence."""
    event = EVENTS.get(str(data.get("hook_event_name") or "").replace("_", "").replace("-", "").lower())
    if event is None or vendor not in config.VENDORS:
        return None
    if os.environ.get(config.FOLLOWER_ENV) or data.get("agent_id"):
        return None
    if event in TAB_EVENTS:
        if vendor == "codex":
            on_tab_event(event, data)
        return None
    if vendor == "codex" and _codex_child(data.get("transcript_path")):
        return None
    cwd = data.get("cwd") if isinstance(data.get("cwd"), str) and data.get("cwd") else os.getcwd()
    root, attachment = _resolve_root(vendor, cwd, data.get("session_id") or None, data.get("transcript_path") or None)
    if root is None:
        return None
    if event == "Notification":
        on_notification(data, root)
        return None
    if event == "SessionStart":
        return on_session_start(vendor, data, root, attachment)
    if event == "PostCompact":
        on_post_compact(vendor, data, root, attachment)
    elif event == "SessionEnd":
        on_session_end(vendor, data, root, attachment)
    elif event == "UserPromptSubmit":
        if vendor == "codex":
            _clear_blocked(data)
        return on_user_prompt(vendor, data, root, attachment)
    return None


def _codex_child(transcript: str | None) -> bool:
    """Codex child threads identify their spawning source in the rollout header."""
    if not isinstance(transcript, str) or not transcript:
        return False
    try:
        with open(transcript, encoding="utf-8-sig", errors="replace") as fh:
            rec = json.loads(fh.readline())
        payload = rec.get("payload") if isinstance(rec, dict) and rec.get("type") == "session_meta" else None
        source = payload.get("source") if isinstance(payload, dict) else None
        return isinstance(source, dict) and "subagent" in source
    except (OSError, ValueError):
        return False


def main(vendor: str | None = None) -> int:
    """Entry point used by the repo-root ``hook.py``; always returns 0."""
    event = ""
    vendor = (vendor or "").lower()
    try:
        if os.environ.get(config.FOLLOWER_ENV):
            return 0
        raw = sys.stdin.buffer.read()
        try:
            data = json.loads(raw.decode("utf-8-sig", errors="replace"))
        except ValueError:
            data = None
        if not isinstance(data, dict):
            return 0
        event = str(data.get("hook_event_name") or "")
        output_flag = None
        event_name = EVENTS.get(event.replace("_", "").replace("-", "").lower())
        if event_name in {"SessionStart", "UserPromptSubmit"}:
            cwd = data.get("cwd") if isinstance(data.get("cwd"), str) and data.get("cwd") else os.getcwd()
            root, _ = resolve_root(vendor, cwd, data.get("session_id") or None, data.get("transcript_path") or None)
            if root is not None:
                flag = paths.baton_dir(root) / "inbox" / ".unread"
                if flag.exists():
                    output_flag = flag
        out = handle(vendor, data)
        if out is not None:
            try:
                sys.stdout.write(json.dumps(out))  # ASCII-escaped: safe on any console code page
                sys.stdout.flush()
            except Exception:
                if output_flag is not None:
                    _guard(vendor, event, lambda: _restore_unread(output_flag))
                raise
    except Exception as exc:
        _log(vendor, event, exc)
    return 0
