"""Launch planning: which vendor, account, and fresh-or-resume for a worktree.

``bt plan --json`` is the only thing ``bt.ps1`` needs from Python; the plan
carries the full argv and environment edits so the shell wrapper stays dumb.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path

from . import accounts, config, doctor, gitutil, harvest, registry, routing, sessions
from .paths import baton_dir, find_worktree_root, git_dir, main_checkout, repo_name, ws_name

RESUME_MAX_TOKENS = 40000
LIVE_SECONDS = 600
CODEX_WARN_AT = 90
SKIP_REPOS = frozenset({"brain"})


def managed_tab_command(command: str) -> str:
    """Run a command in a dedicated tab, then terminate that tab's shell.

    Only use for newly created tab shells, never for the caller's shell.
    """
    return ("$global:LASTEXITCODE = 0; $ErrorActionPreference = 'Stop'; "
            "$PSNativeCommandUseErrorActionPreference = $false; try { " + command
            + "\n; exit $global:LASTEXITCODE } catch { Write-Error -ErrorAction Continue $_; exit 1 }")


@dataclass
class LaunchPlan:
    vendor: str
    account: str
    mode: str                      # "fresh" | "resume"
    argv: list
    env_set: dict
    env_unset: list
    cwd: str
    reason: str
    session_id: str | None = None


# ------------------------------------------------------------ worktree lookup

def _list_worktrees(repo: Path) -> list[tuple[Path, str]]:
    """(path, branch) for each worktree of ``repo``; branch is '(detached)' when HEAD has none."""
    try:
        cp = gitutil.git(repo, "worktree", "list", "--porcelain", timeout=20)
    except Exception:  # noqa: BLE001 - one broken repo must not hide the others
        return []
    if cp.returncode != 0:
        return []
    records: list[tuple[Path, str]] = []
    for line in cp.stdout.splitlines():
        if line.startswith("worktree "):
            records.append((Path(line[len("worktree "):].strip()), "(detached)"))
        elif line.startswith("branch refs/heads/") and records:
            records[-1] = (records[-1][0], line[len("branch refs/heads/"):].strip())
    return records


def _worktree_records(code_root: str | os.PathLike | None = None) -> list[tuple[Path, str]]:
    """(path, branch) of every worktree of every repo directly under the code root (``brain`` skipped)."""
    root = Path(code_root) if code_root else config.CODE_ROOT
    try:
        repos = [c for c in sorted(root.iterdir()) if c.is_dir() and (c / ".git").is_dir() and c.name.lower() not in SKIP_REPOS]
    except OSError:
        return []
    seen: set[str] = set()
    found: list[tuple[Path, str]] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        for records in pool.map(_list_worktrees, repos):
            for path, branch in records:
                key = config.norm(path)
                if key not in seen and path.is_dir():
                    seen.add(key)
                    found.append((Path(os.path.normpath(path)), branch))
    return found


def all_worktrees(code_root: str | os.PathLike | None = None) -> list[Path]:
    """Every worktree of every repo directly under the code root (``brain`` skipped)."""
    return [path for path, _ in _worktree_records(code_root)]


def _label(path: Path) -> str:
    return f"{repo_name(path)}/{ws_name(path)}"


def resolve_worktree(query: str | None, code_root: str | os.PathLike | None = None, cwd: str | os.PathLike | None = None) -> Path:
    """Worktree for ``query`` (ws name, ``repo/ws`` or unique substring); None means the current one."""
    if not query:
        root = find_worktree_root(cwd if cwd is not None else os.getcwd())
        if root is None:
            raise LookupError("not inside a git worktree; pass a worktree name (see `bt status`)")
        return root
    if Path(query).is_absolute() or query.startswith("."):
        root = find_worktree_root(query)
        if root is not None:
            return root
    needle = query.strip().replace("\\", "/").lower()
    candidates = all_worktrees(code_root)
    stages = (
        [p for p in candidates if ws_name(p).lower() == needle],
        [p for p in candidates if _label(p).lower() == needle],
        [p for p in candidates if needle in _label(p).lower()],
    )
    for matches in stages:
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            listing = "\n".join(f"  {_label(p)}  {p}" for p in matches)
            raise LookupError(f"{query!r} is ambiguous; candidates:\n{listing}")
    raise LookupError(f"no worktree matches {query!r} under {code_root or config.CODE_ROOT}")


# -------------------------------------------------------------------- planning

def _lead_settings(vendor: str, role: str, account: str = "main", notes: list[str] | None = None) -> tuple[str | None, str | None]:
    """Model and effort for a fresh session: the first chain model ``account`` can use (see routing.resolve)."""
    cfg = config.routing_config()
    block = cfg.get("lead", {}) if role == "lead" else cfg.get("roles", {}).get(role, {})
    if role != "lead":
        if not block:
            raise ValueError(f"unknown role {role!r}")
        entry = routing._entry(block, vendor)
    else:
        entry = block.get(vendor) or {}
    if block.get("required_model"):
        found_model, effort = routing.required_settings(block, vendor, account)
        cache = config.account_dir(vendor, account) / routing.CACHE_NAME
        auth = config.mtime(cache.parent / "auth.json")
        stamp = config.mtime(cache)
        if routing.available_models(vendor, account) is None or (auth and (stamp is None or auth > stamp)):
            if notes is not None:
                notes.append(f"required model {found_model}: catalog availability unknown or predates login; native CLI will validate, no fallback")
        return found_model, effort
    if not routing.entry_models(entry):
        return None, entry.get("effort") or None
    found = routing.resolve(vendor, entry, account)
    if found.reason and notes is not None:
        notes.append(found.reason)
    return found.model, found.effort or None


CODEX_SANDBOXES = ("read-only", "workspace-write", "danger-full-access")


def build_argv(vendor: str, mode: str, worktree: Path, session_id: str | None, model: str | None, effort: str | None,
               prompt: str | None = None, sandbox: str | None = None) -> list[str]:
    """The CLI argv. ``prompt`` (fresh sessions only) is the first user message, given as the CLI's positional
    prompt; the context cycler uses it to tell the replacement session why it exists. ``sandbox`` (fresh Codex
    sessions only) is a role's own sandbox mode in place of the one in the Codex configuration."""
    exe = config.exe(vendor)
    wt = str(worktree)
    if vendor == "claude":
        if mode == "resume":
            return [exe, "--resume", session_id]
        argv = [exe, "-n", ws_name(worktree)]
        if model:
            argv += ["--model", model]
        if effort:
            argv += ["--effort", effort]
        if prompt:
            argv.append(prompt)
        return argv
    if mode == "resume":
        return [exe, "resume", session_id, "-C", wt]
    argv = [exe, "-C", wt]
    if model:
        argv += ["-m", model]
    if effort:
        argv += ["-c", f'model_reasoning_effort="{effort}"']
    if sandbox:
        argv += ["-s", sandbox]
    if prompt:
        argv += ["--", prompt]
    return argv


def other_account(vendor: str, current: str) -> str:
    """The account `bt switch` moves to: the least-used logged-in other account (first other one for Codex)."""
    others = [n for n in config.account_names(vendor) if n != current]
    if not others:
        raise ValueError(f"no other {vendor} account configured besides {current!r}")
    ready = [n for n in others if _usable(vendor, n)] or [n for n in others if accounts.logged_in(vendor, n)] or others
    if vendor == "claude":
        known = [(u, n) for n in ready for u in [_safe_utilisation(vendor, n)] if u is not None]
        if known:
            return min(known)[1]
    return ready[0]


def _usable(vendor: str, account: str) -> bool:
    """Logged in, and (Claude) the refresh token has not expired: safe to switch to automatically."""
    return accounts.logged_in(vendor, account) and not accounts.login_expired(vendor, account)


def _safe_utilisation(vendor: str, account: str) -> int | None:
    try:
        return accounts.utilisation(vendor, account)
    except Exception:  # noqa: BLE001 - usage is advisory
        return None


def _limit(vendor: str) -> int:
    if vendor == "claude":
        return config.policy_int("claude_auto_switch_at", 90)
    return CODEX_WARN_AT


def _pick_account(vendor: str, account: str | None, last, switch: bool, notes: list[str]) -> str:
    """Choose the account and append the explanation to ``notes``."""
    known = config.account_names(vendor)
    if account is not None:
        if account not in known:
            raise ValueError(f"unknown {vendor} account {account!r}; configured: {', '.join(known)}")
        chosen, origin = account, "requested"
    else:
        sticky = last.account if last is not None and last.account else None
        chosen = sticky if sticky in known else ("main" if "main" in known else known[0])
        origin = "sticky" if sticky in known else "default"
        if switch:
            target = other_account(vendor, chosen)
            notes.append(f"switching {vendor}:{chosen} -> {vendor}:{target}")
            return target
    percent = _safe_utilisation(vendor, chosen)
    shown = f" at {percent}%" if percent is not None else ""
    if percent is None or percent < _limit(vendor):
        notes.append(f"account {vendor}:{chosen} ({origin}){shown}")
        return chosen
    if vendor == "claude" and origin != "requested":
        lower = [(u, n) for n in known if n != chosen and _usable(vendor, n)
                 for u in [_safe_utilisation(vendor, n)] if u is not None and u < percent]
        if lower:
            u, n = min(lower)
            notes.append(f"claude:{chosen} is at {percent}% (switch threshold {_limit(vendor)}%); using claude:{n} at {u}%")
            return n
        notes.append(f"warning: claude:{chosen} is at {percent}% and no other account has more room")
        return chosen
    extra = " (baton never switches Codex accounts on its own)" if vendor == "codex" else ""
    notes.append(f"warning: {vendor}:{chosen} ({origin}) is at {percent}% of its usage limit{extra}")
    return chosen


def _fmt_idle(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds}s"
    if seconds < 5400:
        return f"{round(seconds / 60)}m"
    if seconds < 172800:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds // 86400}d"


def _decide_mode(vendor: str, mode: str, last, account: str, now: float, notes: list[str]) -> str:
    """Return 'fresh' or 'resume', appending the explanation to ``notes``."""
    if mode == "fresh":
        notes.append("fresh session (requested)")
        return "fresh"
    if mode == "resume":
        if last is None:
            notes.append(f"no previous {vendor} session found; starting fresh")
            return "fresh"
        notes.append(f"resuming {vendor} session {last.session_id[:8]} (requested)")
        return "resume"
    if last is None:
        notes.append(f"no previous {vendor} session; starting fresh")
        return "fresh"
    idle = now - last.last_active
    other_account_ = last.account is not None and last.account != account
    if vendor == "codex" and other_account_:
        notes.append(f"last codex session belongs to account {last.account} and a rollout cannot be resumed under {account}; starting fresh")
        return "fresh"
    if idle < LIVE_SECONDS:
        notes.append(f"note: session {last.session_id[:8]} was active {_fmt_idle(idle)} ago; if it is still open elsewhere, use that terminal instead")
    cold = idle > config.cache_ttl_seconds(vendor) or other_account_
    tokens = last.ctx_tokens
    rotate = config.rotate_tokens(vendor, last.model)
    if tokens is not None and tokens > rotate:
        # `bt status` tells the user to restart with `bt <ws>` from this size on; a warm cache does not
        # make resuming right, since every later turn re-reads the whole context.
        notes.append(f"last {vendor} session has ~{round(tokens / 1000)}k tokens, above rotate_context_tokens "
                     f"({round(rotate / 1000)}k); starting fresh, baton reloads the capsule. Use -Resume to reopen it anyway.")
        return "fresh"
    if not cold or tokens is None or tokens <= RESUME_MAX_TOKENS:
        size = "size unknown" if tokens is None else f"~{round(tokens / 1000)}k tokens"
        notes.append(f"resuming {vendor} session {last.session_id[:8]} (idle {_fmt_idle(idle)}, {size})")
        return "resume"
    notes.append(
        f"last {vendor} session idle {idle / 3600:.1f}h with ~{round(tokens / 1000)}k tokens; resuming would re-read it "
        "uncached; starting fresh, baton reloads the capsule. Use -Resume to override."
    )
    return "fresh"


def plan(worktree: str | os.PathLike, vendor: str | None = None, account: str | None = None, mode: str = "auto",
         role: str = "lead", *, switch: bool = False, sync: bool = True, now: float | None = None,
         prompt: str | None = None, model: str | None = None) -> LaunchPlan:
    """Decide how to start (or resume) a session in ``worktree``. Raises ValueError on bad vendor/account.

    ``prompt`` is the first user message of a fresh session; ``model`` overrides the routing lead model."""
    wt = Path(os.path.normpath(worktree))
    now = time.time() if now is None else now
    entry = registry.entry(wt)
    vendor = vendor or entry.get("last_vendor") or "claude"
    if vendor not in config.VENDORS:
        raise ValueError(f"unknown vendor {vendor!r}; expected one of {', '.join(config.VENDORS)}")
    if mode not in ("auto", "fresh", "resume"):
        raise ValueError(f"unknown mode {mode!r}")
    role_block = config.routing_config().get("roles", {}).get(role, {}) if role != "lead" else {}
    if role != "lead":
        if not role_block:
            raise ValueError(f"unknown role {role!r}")
        routing._entry(role_block, vendor)
        if mode == "resume":
            raise ValueError("a role launch requires a fresh session; use -Fresh")
        mode = "fresh"
        if model and role_block.get("required_model") and model != role_block["required_model"]:
            raise ValueError(f"role {role} requires {role_block['required_model']}; model override refused")
    if switch:
        mode = "fresh"
    last = sessions.last_session(wt, vendor)
    notes: list[str] = []
    chosen = _pick_account(vendor, account, last, switch, notes)
    if vendor == "codex" and mode == "resume" and last is not None and last.account and last.account != chosen:
        # A rollout lives in one CODEX_HOME; resuming it elsewhere would fail, so the session's account wins.
        notes.append(f"note: codex session {last.session_id[:8]} lives in account {last.account}; resuming it there instead of {chosen}")
        chosen = last.account
    if accounts.login_expired(vendor, chosen, now):
        refresh = accounts.login_expiry(vendor, chosen)[0]
        notes.append(f"warning: {vendor}:{chosen} login expired {accounts.fmt_date(refresh)}; {accounts.login_remedy(vendor, chosen)}")
    final_mode = _decide_mode(vendor, mode, last, chosen, now, notes)
    session_id = last.session_id if final_mode == "resume" and last is not None else None
    lead_model, effort = _lead_settings(vendor, role, chosen, notes) if final_mode == "fresh" else (None, None)
    if model and final_mode == "fresh":
        notes.append(f"model {model} (requested)")
        lead_model = model
    startup_prompt = prompt
    sandbox = None
    if role != "lead" and final_mode == "fresh":
        role_instructions = str(role_block.get("instructions", "")).strip()
        startup_prompt = (prompt + "\n\n" if prompt else "") + f"Baton role bt-{role}:\n{role_instructions}"
        # A role may carry its own Codex sandbox mode (routing.toml `sandbox`): the configured workspace-write mode
        # keeps git metadata read-only, so a lane that has to commit and push its own work is given another one.
        sandbox = role_block.get("sandbox") if vendor == "codex" else None
        if sandbox is not None:
            if sandbox not in CODEX_SANDBOXES:
                raise ValueError(f"role {role}: sandbox must be one of {', '.join(CODEX_SANDBOXES)}")
            notes.append(f"sandbox {sandbox} (role {role})")
    argv = build_argv(vendor, final_mode, wt, session_id, lead_model, effort,
                      startup_prompt if final_mode == "fresh" else None, sandbox)
    env_name = config.ACCOUNT_ENV[vendor]
    if chosen == "main":
        env_set, env_unset = {}, [env_name]
    else:
        env_set, env_unset = {env_name: str(config.account_dir(vendor, chosen))}, []
        if sync:
            actions = accounts.sync(vendor, chosen)
            if accounts.has_errors(actions):
                notes.append("warning: syncing the account failed: " + "; ".join(a for a in actions if a.startswith("error:")))
    env_set.update(BATON_SESSION_NAME=ws_name(wt), BATON_LAUNCH_CWD=str(wt))
    env_unset += [key for key in config.SESSION_ENV if key not in env_set]   # clear parent session identity
    notes.extend(_daily_notes())
    return LaunchPlan(vendor, chosen, final_mode, argv, env_set, env_unset, str(wt), "\n".join(notes), session_id)


def _daily_notes() -> list[str]:
    """The doctor's once-a-day warnings (and a login keep-alive notice); advisory, so never raises."""
    try:
        return [str(line) for line in doctor.daily_warnings()]
    except Exception:  # noqa: BLE001
        return []


# ------------------------------------------------------------------------ status

def owner_live(worktree: Path, now: float | None = None) -> bool:
    """True when .baton/owner.json names a session that is still running.

    The vendors' own process records decide when they can (hook.owner_process_alive).
    Otherwise any of the claim time, the claim file's mtime or the owner's transcript
    being under 15 minutes old counts as live. An unreadable claim is never live.
    """
    now = time.time() if now is None else now
    path = baton_dir(worktree) / "owner.json"
    try:
        owner = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if not isinstance(owner, dict):
        return False
    try:
        from .hook import owner_process_alive
        alive = owner_process_alive(owner, worktree) if owner else None
    except Exception:
        alive = None
    if alive is not None:
        return alive
    stamps = []
    claim = _owner_claim(worktree)
    if claim is not None:
        stamps.append(claim)
    transcript = config.mtime(owner.get("transcript"))
    if transcript is not None:
        stamps.append(transcript)
    return bool(stamps) and now - max(stamps) < config.OWNER_LIVE_SECONDS


def unread_count(worktree: Path) -> int:
    try:
        return sum(1 for p in (baton_dir(worktree) / "inbox").iterdir() if p.is_file() and p.suffix == ".md")
    except OSError:
        return 0


def followers_enabled(worktree: Path) -> list[str]:
    cfg = config.load_toml(baton_dir(worktree) / "follow.toml") if (baton_dir(worktree) / "follow.toml").is_file() else None
    if cfg is None:
        return []
    roles = cfg.get("roles")
    return [str(r) for r in roles] if isinstance(roles, list) else ["on"]


def _count(value) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


HIDE_DEAD_DAYS = 7


def status_rows(now: float | None = None) -> list[dict]:
    """One row per recorded worktree. ``ctx_tokens`` is read from the live transcript tail when the transcript
    exists (the registry value is the size at the last prompt and can predate a compaction), else taken from the
    registry; ``over_threshold`` uses the same value. ``compactions`` and ``age`` (seconds since the session's
    ``started_at``) are the other rotation signals, flagged against ``[policy] rotate_compactions`` and 24 h.

    A Claude session is also over its limit from the context cycler's threshold (cycle.toml) on, since that is where it
    is meant to be replaced: a live one past it that has not cycled is flagged. ``hidden`` marks a record without a live
    owner whose last activity is older than ``HIDE_DEAD_DAYS``; ``bt status`` leaves those out unless given ``--all``,
    and never flags a record without a live owner (nobody can act on it)."""
    now = time.time() if now is None else now
    max_compactions = config.policy_int("rotate_compactions", config.ROTATE_COMPACTIONS)
    try:
        from . import cycle
        cycle_cfg = cycle.settings()
        cycle_on = cycle.enabled(cycle_cfg)
    except Exception:  # noqa: BLE001 - a broken cycle.toml must not break `bt status`
        cycle, cycle_cfg, cycle_on = None, {}, False
    rows = []
    for key, item in registry._load_quiet()["worktrees"].items():
        path = Path(item.get("path") or key)
        if not path.is_dir():
            continue
        vendor = item.get("last_vendor")
        rec = item.get(vendor, {}) if vendor else {}
        seen = rec.get("last_seen") if isinstance(rec.get("last_seen"), (int, float)) else None
        transcript = rec.get("transcript")
        written = config.mtime(transcript)
        if written is not None:
            seen = max(seen or 0, written)
        tokens, model = harvest.ctx_info(vendor, transcript) if vendor and written is not None else (None, None)
        if tokens is None and (vendor != "codex" or written is None):
            tokens = _count(rec.get("ctx_tokens"))
        if model is None and isinstance(rec.get("model"), str):
            model = rec["model"]
        threshold = config.rotate_tokens(vendor, model)
        ws = item.get("ws") or ws_name(path)
        cycle_limit = cycle.threshold(ws, cycle_cfg) if cycle_on and vendor == "claude" else None
        started = harvest.parse_iso(rec.get("started_at"))
        compactions = _count(rec.get("compactions"))
        age = None if started is None else max(0.0, now - started)
        live = owner_live(path, now)
        idle = None if seen is None else max(0.0, now - seen)
        rows.append({
            "ws": ws,
            "path": str(path),
            "session": f"{vendor}:{rec.get('account') or 'main'}" if vendor else "-",
            "idle": idle,
            "ctx_tokens": tokens,
            "model": model,
            "threshold": threshold,
            "cycle_threshold": cycle_limit,
            "over_threshold": tokens is not None and (tokens > threshold or (cycle_limit is not None and tokens >= cycle_limit)),
            "compactions": compactions,
            "over_compactions": compactions is not None and compactions >= max_compactions,
            "age": age,
            "over_age": age is not None and age > config.ROTATE_AGE_SECONDS,
            "owner_live": live,
            "hidden": not live and idle is not None and idle > HIDE_DEAD_DAYS * 86400,
            "unread": unread_count(path),
            "followers": followers_enabled(path),
        })
    rows.sort(key=lambda r: (r["idle"] is None, r["idle"] or 0))
    return rows


# ---------------------------------------------------------------------------- list

OBJECTIVE_MAX = 80
_OBJECTIVE = re.compile(r"Objective:[*\s]*(.*)")


def _objective(worktree: Path) -> str:
    """Text after ``Objective:`` on the first line of .baton/now.md that has it ("" when absent or empty)."""
    try:
        text = (baton_dir(worktree) / "now.md").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    for line in text.splitlines():
        match = _OBJECTIVE.search(line)
        if match:
            value = match.group(1).strip()
            return value if len(value) <= OBJECTIVE_MAX else value[:OBJECTIVE_MAX - 3].rstrip() + "..."
    return ""


def _activity(worktree: Path) -> float:
    """Newest mtime of the git HEAD reflog, .baton/now.md and .baton/owner.json (0.0 when none exist). Stat only."""
    stamps = [config.mtime(baton_dir(worktree) / name) for name in ("now.md", "owner.json")]
    gd = git_dir(worktree)
    if gd is not None:
        stamps.append(config.mtime(gd / "logs" / "HEAD"))
    return max((s for s in stamps if s is not None), default=0.0)


def _owner_claim(worktree: Path) -> float | None:
    """Claim time of .baton/owner.json (its claimed_at stamp, else the file mtime); None when absent."""
    path = baton_dir(worktree) / "owner.json"
    mtime = config.mtime(path)
    if mtime is None:
        return None
    claimed = harvest.parse_iso(config.read_json(path).get("claimed_at"))
    return mtime if claimed is None else claimed


def list_rows(code_root: str | os.PathLike | None = None, now: float | None = None, *, all_checkouts: bool = False) -> list[dict]:
    """One row per worktree of every repo under the code root (follower worktrees excluded), newest activity first.

    Without ``all_checkouts`` the main checkouts are listed after the linked worktrees.
    """
    from . import worktrees
    now = time.time() if now is None else now
    lineage = worktrees.load_lineage()
    entries: list[tuple[bool, dict]] = []
    for path, branch in _worktree_records(code_root):
        main = main_checkout(path)
        is_main = config.norm(main) == config.norm(path)
        if not is_main and worktrees.is_follower_tree(path, lineage):
            continue                 # follower siblings and scratch checkouts are baton-managed, not workstreams
        claim = _owner_claim(path)
        row = {
            "key": f"{main.name}/{'main' if is_main else ws_name(path)}",
            "path": str(path),
            "branch": branch,
            "activity": _activity(path),
            "objective": _objective(path),
            "live": owner_live(path, now) if claim is not None else False,
        }
        entries.append((is_main, row))
    if all_checkouts:
        entries.sort(key=lambda e: (-e[1]["activity"], e[1]["key"]))
    else:
        entries.sort(key=lambda e: (e[0], -e[1]["activity"], e[1]["key"]))
    return [row for _, row in entries]


def _fmt_age(activity: float, now: float) -> str:
    if not activity:
        return "-"
    seconds = max(0, int(now - activity))
    if seconds < 60:
        return "<1m"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


# --------------------------------------------------------------------------- CLI

TITLE_TAIL_BYTES = 256 * 1024


def _tab_title(vendor: str, transcript: str | None) -> str:
    """Terminal tab title of a Claude session: ``customTitle`` of the last ``custom-title`` entry in the tail of
    its transcript. "" when there is none, for other vendors, or when the file cannot be read."""
    if vendor != "claude" or not transcript:
        return ""
    try:
        with open(transcript, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - TITLE_TAIL_BYTES))
            tail = fh.read()
    except OSError:
        return ""
    at = tail.rfind(b"custom-title")
    while at >= 0:
        start = tail.rfind(b"\n", 0, at) + 1
        end = tail.find(b"\n", at)
        try:
            entry = json.loads(tail[start:end if end >= 0 else len(tail)].decode("utf-8", "replace"))
            title = entry.get("customTitle") if isinstance(entry, dict) and entry.get("type") == "custom-title" else None
        except ValueError:
            title = None
        if isinstance(title, str) and title.strip():
            return title.strip()
        at = tail.rfind(b"custom-title", 0, at)
    return ""


def _owner_process(owner: dict, worktree: Path) -> dict:
    """``{"pid": int, "started": epoch seconds | None}`` of the running Claude process behind ``owner.json``
    (read from Claude's own ``sessions/<pid>.json`` records, no process query); {} when it cannot be found.
    Codex records no pid."""
    sid = owner.get("session_id")
    if owner.get("vendor") != "claude" or not isinstance(sid, str) or not sid:
        return {}
    from .hook import _attached_to, _pid_alive, _session_in_root
    root = config.norm(worktree)
    attached = _attached_to(sid, root)
    for account in config.account_names("claude"):
        try:
            files = list((config.account_dir("claude", account) / "sessions").glob("*.json"))
        except OSError:
            continue
        for f in files:
            rec = config.read_json(f)
            if rec.get("sessionId") != sid or not (attached or _session_in_root(rec, root)):
                continue
            try:
                pid = int(rec.get("pid") or f.stem)
            except ValueError:
                continue
            if not _pid_alive(pid):
                continue
            started = rec.get("startedAt")
            ok = isinstance(started, (int, float)) and not isinstance(started, bool) and started > 0
            return {"pid": pid, "started": started / 1000 if ok else None}
    return {}


def _owner_facts(owner: dict, worktree: Path, now: float) -> list[str]:
    """What can be found cheaply about where the owning session runs; each part is left out when unknown."""
    vendor = str(owner.get("vendor") or "")
    transcript = owner.get("transcript") if isinstance(owner.get("transcript"), str) else None
    facts = []
    title = ""
    try:
        title = _tab_title(vendor, transcript)
    except Exception:  # noqa: BLE001 - a lookup must never break the launch check
        pass
    if title:
        facts.append(f'tab "{title}"')
    try:
        proc = _owner_process(owner, worktree)
    except Exception:  # noqa: BLE001
        proc = {}
    if proc.get("pid"):
        started = proc.get("started")
        facts.append(f"PID {proc['pid']}" + (f" started {time.strftime('%Y-%m-%d %H:%M', time.localtime(started))}" if started else ""))
    written = config.mtime(transcript)
    if written is not None:
        facts.append(f"last write {_fmt_age(written, now)} ago")
    return facts


def live_owner_message(worktree: Path, now: float | None = None) -> str:
    """Why a launch must not start while another session holds ``worktree`` ("" when none does)."""
    if not owner_live(worktree, now):
        return ""
    now = time.time() if now is None else now
    owner = config.read_json(baton_dir(worktree) / "owner.json") or {}
    sid = str(owner.get("session_id") or "")[:8] or "unknown"
    name = Path(worktree).name
    vendor = str(owner.get("vendor") or "")
    who = ", ".join([f"{vendor.capitalize() or 'A'} session {sid}", *_owner_facts(owner, worktree, now)])
    return (f"{name} is still open in another terminal: {who}. Switch to that tab, ask it to update .baton/now.md, "
            f"/exit it, then run `bt {name} -Fresh`. If that session has crashed, add -Force.")


def _cmd_plan(args) -> int:
    try:
        wt = resolve_worktree(args.query)
        if args.json and not args.force:
            # The launcher reads --json; a second session in the same worktree would be a second writer
            # (or, resumed, a second process on the same conversation).
            blocked = live_owner_message(wt)
            if blocked:
                raise LookupError(blocked)
        mode = "fresh" if args.fresh else "resume" if args.resume else "auto"
        result = plan(wt, args.vendor, args.account, mode, role=getattr(args, "role", "lead"), switch=args.switch, sync=not args.no_sync,
                      prompt=getattr(args, "prompt", None), model=getattr(args, "model", None))
    except (LookupError, ValueError) as exc:
        print(f"bt: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(asdict(result)))  # ASCII-escaped: safe for any console code page
        return 0
    blocked = "" if args.force else live_owner_message(wt)
    if blocked:
        print(f"warning: `bt` would not launch: {blocked}")
    print(f"worktree: {result.cwd}")
    print(f"launch:   {result.vendor}:{result.account}  {result.mode}" + (f"  session {result.session_id}" if result.session_id else ""))
    for line in result.reason.splitlines():
        print(f"  {line}")
    for name, value in result.env_set.items():
        print(f"env set:   {name}={value}")
    for name in result.env_unset:
        print(f"env unset: {name}")
    print("argv:     " + " ".join(f'"{a}"' if " " in a else a for a in result.argv))
    return 0


def _fmt_ctx(tokens: int | None, over: bool = False) -> str:
    return "-" if tokens is None else f"{round(tokens / 1000)}k" + ("!" if over else "")


def _fmt_flag(text: str | None, over: bool) -> str:
    return "-" if text is None else text + ("!" if over else "")


def _cmd_status(args) -> int:
    rows = status_rows()
    if not rows:
        print("no worktrees recorded yet (sessions register themselves through the baton hooks)")
        return 0
    every = getattr(args, "all", False)
    hidden = 0 if every else sum(1 for r in rows if r["hidden"])
    shown = [r for r in rows if every or not r["hidden"]]
    # a record without a live owner is not flagged: no session is there to hand over or restart
    table = [
        [r["ws"], r["session"], "-" if r["idle"] is None else _fmt_idle(r["idle"]),
         _fmt_ctx(r["ctx_tokens"], r["over_threshold"] and r["owner_live"]),
         _fmt_flag(None if r["compactions"] is None else str(r["compactions"]), r["over_compactions"] and r["owner_live"]),
         _fmt_flag(None if r["age"] is None else _fmt_idle(r["age"]), r["over_age"] and r["owner_live"]),
         "live" if r["owner_live"] else "-", str(r["unread"]), ",".join(r["followers"]) or "off"]
        for r in shown
    ]
    print(accounts.format_table(["ws", "last session", "idle", "ctx", "cmp", "age", "owner", "inbox", "followers"], table))
    max_compactions = config.policy_int("rotate_compactions", config.ROTATE_COMPACTIONS)
    print(f"ctx = context tokens read from the transcript tail (registry value when there is none); cmp = compactions this "
          f"session; age = since the session started. ! (live owner only) = ctx above the session's limit ({config.rotate_summary()}; "
          f"rotate_context_tokens; for Claude also its cycle threshold, cycle.toml), "
          f"cmp at {max_compactions}+ (rotate_compactions) or age over {config.ROTATE_AGE_SECONDS // 3600}h: "
          "hand over and restart with `bt <ws>`")
    if hidden:
        print(f"{hidden} older records hidden: bt status --all")
    return 0


def _cmd_list(args) -> int:
    now = time.time()
    rows = list_rows(now=now, all_checkouts=args.all)
    if args.json:
        print(json.dumps(rows, indent=2))  # ASCII-escaped: safe for any console code page
        return 0
    if args.tsv:
        for row in rows:
            print("	".join([row["key"], _fmt_age(row["activity"], now), row["objective"].replace("	", " "), row["path"]]))
        return 0
    if not rows:
        print(f"no worktrees found under {config.CODE_ROOT}")
        return 0
    table = [[row["key"], _fmt_age(row["activity"], now), row["objective"]] for row in rows]
    print(accounts.format_table(["key", "age", "objective"], table))
    return 0


def register(subparsers) -> None:
    p = subparsers.add_parser("plan", help="decide vendor, account and fresh/resume for a worktree")
    p.add_argument("query", nargs="?", help="worktree: ws name, repo/ws or unique substring (default: current)")
    p.add_argument("--vendor", choices=config.VENDORS)
    p.add_argument("--account")
    p.add_argument("--role", default="lead", help="fresh lane role from routing.toml (for example lane-math)")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--fresh", action="store_true")
    group.add_argument("--resume", action="store_true")
    p.add_argument("--switch", action="store_true", help="fresh session on the other account (bt switch)")
    p.add_argument("--no-sync", action="store_true", help="do not sync the second account's config")
    p.add_argument("--force", action="store_true", help="launch even though another session looks live in the worktree")
    p.add_argument("--prompt", help="first user message of a fresh session (the context cycler's continue prompt)")
    p.add_argument("--model", help="model for a fresh session (default: the routing lead model)")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=_cmd_plan)

    p = subparsers.add_parser("status", help="one row per known worktree")
    p.add_argument("--all", action="store_true",
                   help=f"also list records without an owner whose last activity is over {HIDE_DEAD_DAYS} days old")
    p.set_defaults(func=_cmd_status)

    p = subparsers.add_parser("list", help="worktrees under the code root, most recent activity first")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--json", action="store_true")
    group.add_argument("--tsv", action="store_true", help="key, age, objective, path (for fzf)")
    p.add_argument("--all", action="store_true", help="sort main checkouts by activity with the rest (default: after them)")
    p.set_defaults(func=_cmd_list)
