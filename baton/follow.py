"""Followers: headless review / docs / optimize runs triggered by lead commits.

A git ``post-commit`` hook calls ``follow_hook.py`` (``hook_main`` below), which
decides in a few milliseconds whether any enabled follower role is due and, if
so, spawns one detached ``bt follow run`` process. The runner executes the
roles one after another in sibling worktrees, then drops a short note into the
lead worktree's ``.baton/inbox/``. ``bt merge <role>`` and ``bt inbox`` consume
the results.

The hook path (everything reachable from ``hook_main``) must stay cheap: it is
imported under ``python -S`` and has a < 300 ms budget, so it avoids heavy
imports and git calls until a role has passed the cheap checks.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from . import config, gitutil, paths, procs, registry, routing

HOOK_BEGIN = "# >>> baton follow >>>"
HOOK_END = "# <<< baton follow <<<"

DEFAULT_ROLES = ("review", "docs", "optimize")
# Used only when routing.toml has no [followers.*] entry for the name.
DEFAULT_FOLLOWERS = {
    "review": {"role": "reviewer", "vendor": "opposite", "fallback_vendor": "claude"},
    "docs": {"role": "docs", "vendor": "claude"},
    "optimize": {"role": "optimizer", "vendor": "opposite", "fallback_vendor": "claude"},
    "explore": {"role": "explorer", "vendor": "opposite", "fallback_vendor": "claude", "on_demand": True, "effort": "xhigh"},
}
COMMIT_PREFIX = {"docs": "docs", "optimize": "perf", "explore": "explore"}
LEAD_NOTE_NAME = "lead-now.md"   # read-only copy of the lead's .baton/now.md inside a sibling (explore)
DEFAULT_ALLOWED_BASH = [
    "Bash(git:*)", "Bash(python:*)", "Bash(pytest:*)", "Bash(npm test:*)",
    "Bash(npm run:*)", "Bash(node:*)", "Bash(uv run:*)",
]
DEFAULTS = {
    "min_interval_minutes": 30,
    "min_changed_lines": 40,
    "usage_ceiling": 80,
    "timeout_minutes": 40,
    "account": "main",
    "max_runs_per_day": 8,   # per worktree and role; 0 = no cap
    "codex_windows_sandbox": "unelevated",   # Codex `windows.sandbox` for followers on Windows; "" = leave the user's config
}
# Phrases in a Codex run's output that mean it could not run at all (the elevated Windows sandbox without its
# one-time admin setup fails every shell call), however confident the rest of the text sounds.
SANDBOX_FAILURE_MARKERS = ("helper_unknown_error", "Unable to complete the review")
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
# The explore follower's own instructions: routing.toml's explorer role text is written for a Claude subagent
# the lead hands a scratch worktree (`bt wt new --exit discard`), which is not this follower's situation.
EXPLORE_INSTRUCTIONS = (
    "You are the explore follower. You are already running in your own sibling worktree on its own branch; baton "
    "created it and recorded its exit (merge into the lead branch via `bt merge explore`). Do not create, remove or "
    "enter other worktrees, do not touch any tree but this one, and do not edit the `Exit:` (or any other) line of "
    ".baton/now.md. Prototype quickly and roughly; measure or test enough to compare the approaches; the lead "
    "decides what to keep."
)
NOTE_CAP = 4000
BRIEF_CAP = 4000
# A finding is a list/table item that cites path:line; prose that merely mentions a file is not.
PATH_LINE_RE = re.compile(r"[\w./\\-]+\.\w+:\d+")
ITEM_START_RE = re.compile(r"^\s*(?:[-*+]\s|\d+[.)]\s|\||(?:critical|high|medium|low)\b)", re.IGNORECASE)
# Git operations that mean the lead is mid-rewrite: commits made now are not a stable tip to follow.
BUSY_GIT_MARKERS = ("rebase-merge", "rebase-apply", "CHERRY_PICK_HEAD", "REVERT_HEAD", "MERGE_HEAD")

MAX_RUNNERS = 2        # follower runners machine-wide
SLOT_WAIT_S = 600      # how long a runner waits for a free slot before giving up with "skipped: busy"
SLOT_POLL_S = 15
LOCK_OFFSET = 1 << 20      # the byte a runner slot locks; the pid text at offset 0 stays readable (diagnostics only)
KEEP_RUNS = 30
KEEP_READ = 50

# What baton commits for a write follower that left changes uncommitted: tracked changes, plus untracked files that
# are small and not archives/bundles/patches (an agent's scratch artefacts).
COMMIT_MAX_UNTRACKED = 1 << 20
COMMIT_SKIP_GLOBS = ("*.zip", "*.bundle", "*.patch", "*.tar*", "*.gz")


class FollowError(Exception):
    """A follower step failed in an expected way; the message goes into the inbox note."""


class MergeConflict(FollowError):
    pass


class _Failure(FollowError):
    """A failed run: args = (reason for the note's first line, detail for its body)."""


# --------------------------------------------------------------------------- small helpers

def _log(message: str) -> None:
    try:
        config.STATE_DIR.mkdir(parents=True, exist_ok=True)
        with open(config.STATE_DIR / "follow.log", "a", encoding="utf-8") as fh:
            fh.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}\n")
    except OSError:
        pass


def _is_git_env(name: str) -> bool:
    upper = name.upper()
    return upper.startswith("GIT_") and upper != "GIT_EXEC_PATH"


def _clean_env(environ=None) -> dict:
    """Copy of the environment without GIT_* (hooks inherit GIT_DIR/GIT_INDEX_FILE, and the lead's
    GIT_AUTHOR_*/GIT_COMMITTER_* must not become the follower's identity). GIT_EXEC_PATH stays."""
    source = config.clean_env(os.environ if environ is None else environ)
    return {k: v for k, v in source.items() if not _is_git_env(k)}


def _scrub_git_env() -> None:
    for name in [k for k in os.environ if _is_git_env(k)]:
        os.environ.pop(name, None)


def _native_path(text: str) -> Path:
    """Accept MSYS-style ``/d/Code/x`` (what ``$(pwd)`` yields in Git's sh) on Windows."""
    path = Path(text)
    if os.name == "nt" and not path.exists():
        match = re.match(r"^/([A-Za-z])(/.*)?$", text)
        if match:
            candidate = Path(f"{match.group(1).upper()}:{match.group(2) or '/'}")
            if candidate.exists():
                return candidate
    return path


def _read_json(path: Path) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    for attempt in range(5):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            time.sleep(0.05 * (attempt + 1))
    try:
        os.remove(tmp)
    except OSError:
        pass


def _short(sha: str) -> str:
    return sha[:7]


def pid_alive(pid) -> bool:
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    from .hook import _pid_alive   # imported on demand: the post-commit hook path stays within its time budget
    return _pid_alive(pid)


# --------------------------------------------------------------------------- configuration

def settings() -> dict:
    cfg = dict(DEFAULTS)
    section = config.routing_config().get("follow", {})
    if isinstance(section, dict):
        cfg.update(section)
    return cfg


def follower_def(name: str) -> dict:
    """{'role', 'vendor', optional 'fallback_vendor'} for a follower name, from routing.toml with built-in fallbacks."""
    table = config.routing_config().get("followers", {})
    entry = table.get(name) if isinstance(table, dict) else None
    base = dict(DEFAULT_FOLLOWERS.get(name, {"role": name, "vendor": "same"}))
    if isinstance(entry, dict):
        base.update(entry)
    return base


def on_demand(name: str) -> bool:
    """True for a follower that only runs when asked (``bt follow now --roles <name>``), never from the post-commit hook."""
    return bool(follower_def(name).get("on_demand"))


def follower_names() -> list[str]:
    table = config.routing_config().get("followers", {})
    names = [k for k, v in table.items() if isinstance(v, dict)] if isinstance(table, dict) else []
    for name in DEFAULT_ROLES:
        if name not in names:
            names.append(name)
    return names


def follow_toml_path(root: Path) -> Path:
    return paths.baton_dir(root) / "follow.toml"


def load_follow_toml(root: Path) -> dict:
    try:
        return config.load_toml(follow_toml_path(root))
    except Exception as exc:  # corrupt file: behave as disabled, leave a trace
        _log(f"bad follow.toml in {root}: {exc}")
        return {}


def enabled_roles(root: Path) -> list[str]:
    roles = load_follow_toml(root).get("roles", [])
    seen: list[str] = []
    for item in roles if isinstance(roles, list) else []:
        if isinstance(item, str) and item and item not in seen:
            seen.append(item)
    return seen


def automatic_roles(root: Path) -> list[str]:
    """Enabled roles the post-commit hook and a bare ``bt follow now`` start (on-demand followers excluded)."""
    return [r for r in enabled_roles(root) if not on_demand(r)]


def owner_info(root: Path) -> tuple[str, str]:
    """Lead identity, retaining its last vendor/account after normal owner release."""
    data = _read_json(paths.baton_dir(root) / "owner.json")
    if data.get("vendor") in config.VENDORS:
        return data["vendor"], data.get("account") or "main"
    rec = registry.entry(root)
    vendor = rec.get("last_vendor") if rec.get("last_vendor") in config.VENDORS else "claude"
    return vendor, rec.get(vendor, {}).get("account") or "main"


def resolve_follower(root: Path, name: str) -> dict:
    """Everything the runner needs to know about one follower role.

    The model comes from ``routing.resolve`` for the follower's account. When that is degraded (the
    account lacks the role's preferred model) and ``fallback_vendor`` names another vendor, the role
    runs there instead; ``fallback`` then says why.
    """
    cfg = settings()
    fdef = follower_def(name)
    role_name = fdef.get("role") or name
    role_cfg = config.routing_config().get("roles", {}).get(role_name)
    if not isinstance(role_cfg, dict):
        raise FollowError(f"routing.toml has no [roles.{role_name}] for follower '{name}'")
    owner_vendor, owner_account = owner_info(root)
    pref = fdef.get("vendor", "opposite")
    if pref == "opposite":
        vendor = "codex" if owner_vendor == "claude" else "claude"
    elif pref == "same":
        vendor = owner_vendor
    elif pref in config.VENDORS:
        vendor = pref
    else:
        raise FollowError(f"followers.{name}.vendor = {pref!r} is not claude, codex, opposite or same")

    def account_for(v: str) -> str:
        account = cfg.get("account") or "main"
        if account == "lead":
            account = owner_account if v == owner_vendor or owner_account in config.account_names(v) else "main"
        return account

    account = account_for(vendor)
    found = _resolve_model(role_cfg, vendor, account)
    fallback = ""
    alt = fdef.get("fallback_vendor")
    if found is not None and found.degraded and alt in config.VENDORS and alt != vendor:
        alt_account = account_for(alt)
        alt_found = _resolve_model(role_cfg, alt, alt_account)
        if alt_found is not None:
            fallback = f"fallback from {vendor}:{account}: {found.reason}"
            vendor, account, found = alt, alt_account, alt_found
    override = str(fdef.get("effort") or "")
    if override:
        if override not in routing.EFFORTS:
            raise FollowError(f"followers.{name}.effort = {override!r} is not one of {', '.join(routing.EFFORTS)}")
        fitted, why = routing.fit_for(vendor, account, found.model if found else "", override)
        found = routing.Resolved(found.model if found else "", fitted, bool(found and found.degraded),
                                 "; ".join(x for x in ((found.reason if found else ""), why) if x))
    readonly = bool(role_cfg.get("readonly", name == "review"))
    return {
        "name": name,
        "role": role_name,
        "vendor": vendor,
        "account": account,
        "model": found.model if found else "",
        "effort": found.effort if found else "",
        "degraded": bool(found and found.degraded),
        "model_note": found.reason if found else "",
        "fallback": fallback,
        "instructions": str(role_cfg.get("instructions") or "").strip(),
        "readonly": readonly,
    }


def _resolve_model(role_cfg: dict, vendor: str, account: str):
    """routing.Resolved for the role on this vendor/account; None when the role has no entry for the vendor."""
    entry = role_cfg.get(vendor)
    if not isinstance(entry, dict):
        return None
    if not routing.entry_models(entry):
        return routing.Resolved("", str(entry.get("effort") or ""))
    return routing.resolve(vendor, entry, account)


def utilisation(vendor: str, account: str):
    """Account utilisation in percent, or None when unknown (module missing, error, no data)."""
    try:
        import importlib

        value = importlib.import_module("baton.accounts").utilisation(vendor, account)
        return None if value is None else int(value)
    except Exception:
        return None


# --------------------------------------------------------------------------- state

def state_dir(root: Path) -> Path:
    """One JSON file per role (``.baton/follow-state/<role>.json``) so concurrent runners and the
    hook never overwrite each other's roles."""
    return paths.baton_dir(root) / "follow-state"


def role_state_path(root: Path, role: str) -> Path:
    return state_dir(root) / f"{role}.json"


def _migrate_state(root: Path) -> None:
    """Split the old combined ``follow-state.json`` into per-role files (once)."""
    old = paths.baton_dir(root) / "follow-state.json"
    if not old.exists():
        return
    for role, st in _read_json(old).items():
        if isinstance(st, dict) and re.fullmatch(r"[\w.-]+", role) and not role_state_path(root, role).exists():
            _atomic_write(role_state_path(root, role), json.dumps(st, indent=1, sort_keys=True))
    try:
        old.unlink()
    except OSError:
        pass


def read_role(root: Path, role: str) -> dict:
    _migrate_state(root)
    return _read_json(role_state_path(root, role))


def read_state(root: Path) -> dict:
    """``{role: state}`` for every role that has a state file."""
    _migrate_state(root)
    try:
        files = sorted(state_dir(root).glob("*.json"))
    except OSError:
        files = []
    return {f.stem: _read_json(f) for f in files}


def update_state(root: Path, role: str, **fields) -> None:
    state = read_role(root, role)
    state.update(fields)
    _atomic_write(role_state_path(root, role), json.dumps(state, indent=1, sort_keys=True))


def clear_state(root: Path, role: str) -> None:
    try:
        role_state_path(root, role).unlink()
    except OSError:
        pass


def _is_running(st: dict, now: float, timeout_s: float) -> bool:
    """True while the runner that claimed this role is alive.

    The hook claims every due role for one runner up front and the runner works through them
    in turn, so a role can legitimately wait behind the others; a record older than every role
    timing out one after another is stale (crashed runner whose pid was reused).
    """
    if st.get("status") != "running" or not st.get("pid"):
        return False
    started = float(st.get("last_started_at") or 0)
    if now - started > timeout_s * len(follower_names()) + 300:
        return False
    return pid_alive(st["pid"])


# --------------------------------------------------------------------------- git helpers

def head_info(root: Path) -> tuple[str, str]:
    """(HEAD commit, branch name or '' when detached), read from the ref files without spawning
    git when possible; falls back to git."""
    try:
        gd, common = paths.git_dir(root), paths.git_common_dir(root)
        text = (gd / "HEAD").read_text(encoding="utf-8").strip()
        if re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", text):
            return text, ""
        if text.startswith("ref:"):
            ref = text[4:].strip()
            branch = ref[len("refs/heads/"):] if ref.startswith("refs/heads/") else ref
            for base in (gd, common):
                ref_file = base / ref
                if ref_file.is_file():
                    value = ref_file.read_text(encoding="utf-8").strip()
                    if re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", value):
                        return value, branch
            packed = common / "packed-refs"
            if packed.is_file():
                suffix = " " + ref
                for line in packed.read_text(encoding="utf-8").splitlines():
                    if line.endswith(suffix) and not line.startswith(("#", "^")):
                        return line.split(" ", 1)[0], branch
    except (OSError, AttributeError, ValueError):
        pass
    return gitutil.head(root)[0], gitutil.branch(root)


def head_sha(root: Path) -> str:
    return head_info(root)[0]


def busy_operation(root: Path) -> str:
    """Name of a rebase/cherry-pick/revert/merge in progress in this worktree, else ''."""
    gd = paths.git_dir(root)
    if gd is None:
        return ""
    for marker in BUSY_GIT_MARKERS:
        if (gd / marker).exists():
            return marker
    return ""


def _git_check(cwd: Path, *args: str, timeout: float = 120) -> str:
    cp = gitutil.git(cwd, *args, timeout=timeout)
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip().replace("\n", " ")[:300]
        raise FollowError(f"git {' '.join(args[:2])} failed: {detail}")
    return cp.stdout


def _has_ref(root: Path, ref: str) -> bool:
    return bool(gitutil.out(root, "rev-parse", "--verify", "--quiet", ref))


def review_base(root: Path, last_sha: str, last_base: str, sha: str, default: str) -> str:
    """Start of the range a follower looks at for ``sha``.

    The role's previous sha when it is still an ancestor-ish point (merge-base copes with
    rewritten history); else the fork point from the default branch; else the parent commit
    (lead working directly on the default branch); else the empty tree for a root commit.
    """
    base = ""
    if last_sha and last_sha != sha:
        base = gitutil.merge_base(root, last_sha, sha)
    elif last_sha == sha and last_base:
        base = last_base
    if not base or base == sha:
        base = gitutil.merge_base(root, sha, default)
    if not base or base == sha:
        base = gitutil.out(root, "rev-parse", "--verify", "--quiet", f"{sha}^") or EMPTY_TREE
    return base


def _worktrees_dir(root: Path) -> Path:
    from . import worktrees
    return worktrees.worktrees_dir(paths.main_checkout(root))


def _scratch_dir(root: Path) -> Path:
    from . import worktrees
    return _worktrees_dir(root) / worktrees.SCRATCH


def sibling_path(root: Path, name: str) -> Path:
    return _worktrees_dir(root) / f"{paths.ws_name(root)}--{name}"


def follower_branch(root: Path, name: str, lead: str | None = None) -> str:
    """Follower branch for a lead branch (``None`` = the lead worktree's current branch)."""
    lead = gitutil.branch(root) if lead is None else lead
    return f"{lead}--{name}" if lead else f"baton/{paths.ws_name(root)}--{name}"


# --------------------------------------------------------------------------- due logic

def daily_cap(cfg: dict) -> int:
    """Runs per worktree and role per local day; 0 means no cap."""
    try:
        return max(0, int(cfg.get("max_runs_per_day") or 0))
    except (TypeError, ValueError):
        return 0


def runs_today(st: dict, now: float) -> int:
    """Runs this role has started today (local date), from its per-role state."""
    if st.get("runs_day") != time.strftime("%Y-%m-%d", time.localtime(now)):
        return 0
    try:
        return max(0, int(st.get("runs_today") or 0))
    except (TypeError, ValueError):
        return 0


def due_roles(root: Path, roles=None, *, force: bool = False, now: float | None = None, sha: str | None = None):
    """Decide which roles should run for the current HEAD.

    Returns ``(due, why_not)``: the due role names in order and a ``{role: reason}`` map for the
    rest. ``force`` ignores the interval and size thresholds (it still refuses a running role).
    The usage gate is applied by the runner, not here: reading utilisation can mean a slow
    refresh of the usage cache, which the hook cannot afford.
    """
    cfg = settings()
    roles = list(enabled_roles(root) if roles is None else roles)
    now = time.time() if now is None else now
    interval = float(cfg["min_interval_minutes"]) * 60
    cap = daily_cap(cfg)
    min_lines = int(cfg["min_changed_lines"])
    timeout_s = float(cfg["timeout_minutes"]) * 60
    due: list[str] = []
    why: dict[str, str] = {}
    default = None
    bases: dict[tuple, str] = {}
    lines: dict[str, int] = {}
    for name in roles:
        st = read_role(root, name)
        if _is_running(st, now, timeout_s):
            why[name] = "running"
            continue
        if not force:
            if on_demand(name):
                why[name] = "on demand"
                continue
            if cap and runs_today(st, now) >= cap:
                why[name] = "daily cap"
                continue
            if now - float(st.get("last_started_at") or 0) < interval:
                why[name] = "interval"
                continue
            sha = sha or head_sha(root)
            if not sha:
                why[name] = "no HEAD"
                continue
            last = st.get("last_sha") or ""
            if last == sha:
                why[name] = "no new commits"
                continue
            default = default or gitutil.default_branch(root)
            key = (last, st.get("last_base") or "")
            if key not in bases:
                bases[key] = review_base(root, last, key[1], sha, default)
            base = bases[key]
            if base not in lines:
                lines[base] = gitutil.changed_lines(root, base, sha)
            if lines[base] < min_lines:
                why[name] = f"{lines[base]} changed lines < {min_lines}"
                continue
        due.append(name)
    return due, why


# --------------------------------------------------------------------------- hook entry and spawn

def spawn_runner(root: Path, sha: str, roles, branch: str = "", brief: str = "") -> "subprocess.Popen":
    """Start ``bt follow run`` detached from the caller's console and process group.

    ``branch`` is the lead branch the commit was made on; the runner derives follower branch
    names from it rather than from whatever the lead has checked out by the time it runs.
    """
    argv = [
        config.PYTHON, "-S", str(config.BATON_HOME / "bt.py"), "follow", "run",
        "--worktree", str(root), "--sha", sha, "--roles", ",".join(roles),
    ]
    if branch:
        argv += ["--branch", branch]
    if brief:
        argv += ["--brief", brief]
    proc = procs.popen_detached(
        argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        close_fds=True, env=_clean_env(), cwd=str(config.BATON_HOME),
    )
    now = time.time()
    for name in roles:  # claim the roles now so a quick second commit does not start a duplicate
        update_state(root, name, pid=proc.pid, last_started_at=now, status="running", brief=brief or None)
    return proc


def _hook(argv: list[str]) -> None:
    if os.environ.get(config.FOLLOWER_ENV):
        return
    _scrub_git_env()
    root = Path(os.path.abspath(_native_path(argv[0]))) if argv else Path.cwd()
    if not follow_toml_path(root).is_file():
        return
    roles = automatic_roles(root)
    if not roles:
        return
    sha, branch = head_info(root)
    if not sha or not branch or busy_operation(root):
        return  # detached HEAD or a rebase/cherry-pick/revert/merge in progress: not a tip worth following
    due, _ = due_roles(root, roles, sha=sha)
    if due:
        spawn_runner(root, sha, due, branch)


def hook_main(argv: list[str] | None = None) -> int:
    """Entry for follow_hook.py (git post-commit). Never raises, never prints, always 0."""
    try:
        _hook(list(sys.argv[1:] if argv is None else argv))
    except Exception as exc:
        _log(f"hook error: {type(exc).__name__}: {exc}")
    return 0


# --------------------------------------------------------------------------- post-commit hook installation

def _inside(path: Path, parent: Path) -> bool:
    try:
        resolved, base = path.resolve(), parent.resolve()
    except OSError:
        return False
    return resolved == base or base in resolved.parents


def hooks_dir(root: Path, force: bool = False) -> Path:
    """Where post-commit lives. A ``core.hooksPath`` outside the repository's own git dir is
    either tracked hooks in the worktree or a global hooks directory shared with other
    repositories; writing there is refused unless ``force``."""
    common = paths.git_common_dir(root)
    if common is None:
        raise FollowError(f"{root} is not a git worktree")
    custom = gitutil.out(root, "config", "--get", "core.hooksPath")
    if not custom:
        return common / "hooks"
    target = config.expand(custom)
    target = target if target.is_absolute() else root / target
    if not force and not _inside(target, common):
        where = ("inside the worktree, so the hook would be a tracked file" if _inside(target, root)
                 else "outside this repository, so the hook would run for every repository that shares it")
        raise FollowError(f"core.hooksPath = {custom} points {where}; no hook installed "
                          "(pass --force to install there anyway)")
    return target


def hook_block() -> str:
    python = str(config.PYTHON).replace("\\", "/")
    script = (config.INSTALL_HOME / "follow_hook.py").as_posix()
    return (
        f"{HOOK_BEGIN}\n"
        f'{{ [ -f .baton/follow.toml ] && "{python}" -S "{script}" "$(pwd)" >/dev/null 2>&1; }} || true\n'
        f"{HOOK_END}\n"
    )


_SHELL_SHEBANG = re.compile(r"^#!.*\b(?:ba|da|z|a)?sh\b")
_EXIT_LINE = re.compile(r"^\s*exit(?:\s+0)?\s*$")


def install_hook(root: Path, force: bool = False) -> tuple[Path, str]:
    """Install or refresh the marked block in post-commit. Returns (hook path, created|updated|unchanged)."""
    hook = hooks_dir(root, force) / "post-commit"
    block = hook_block()
    if not hook.exists():
        hook.parent.mkdir(parents=True, exist_ok=True)
        _write_hook(hook, "#!/bin/sh\n" + block + "exit 0\n")
        return hook, "created"
    with open(hook, encoding="utf-8", newline="") as fh:
        text = fh.read()
    first = text.split("\n", 1)[0]
    if first.startswith("#!") and not _SHELL_SHEBANG.match(first):
        raise FollowError(f"existing {hook} is not a shell script ({first.strip()}); add the baton block by hand")
    start = text.find(HOOK_BEGIN)
    if start >= 0:
        end = text.find(HOOK_END, start)
        if end < 0:
            raise FollowError(f"{hook} has a '{HOOK_BEGIN}' marker without its end marker; fix it by hand")
        end += len(HOOK_END)
        if text[end:end + 2] == "\r\n":
            end += 2
        elif text[end:end + 1] == "\n":
            end += 1
        new = text[:start] + block + text[end:]
    else:
        lines = text.splitlines(keepends=True)
        while lines and not lines[-1].strip():
            lines.pop()
        if not lines:
            new = "#!/bin/sh\n" + block + "exit 0\n"
        else:
            if not lines[-1].endswith("\n"):
                lines[-1] += "\n"
            if _EXIT_LINE.match(lines[-1]):
                lines.insert(len(lines) - 1, block)  # keep the hook's own final exit last
            else:
                lines.append(block)  # any other ending: run after the existing body
            new = "".join(lines)
    if new == text:
        return hook, "unchanged"
    _write_hook(hook, new)
    return hook, "updated"


def _write_hook(path: Path, text: str) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    try:
        path.chmod(path.stat().st_mode | 0o755)
    except OSError:
        pass


# --------------------------------------------------------------------------- runner

def _child_env(spec: dict) -> dict:
    env = _clean_env()
    env[config.FOLLOWER_ENV] = spec["name"]
    var = config.ACCOUNT_ENV[spec["vendor"]]
    if spec["account"] == "main":
        env.pop(var, None)
    else:
        env[var] = str(config.account_dir(spec["vendor"], spec["account"]))
    if spec["vendor"] == "codex":
        # Files the Windows sandbox creates belong to its own account and deny ours: a .pytest_cache left in a
        # sibling or checkout cannot be removed without takeown. Keep test runs from writing caches at all.
        addopts = env.get("PYTEST_ADDOPTS", "")
        if "no:cacheprovider" not in addopts:
            env["PYTEST_ADDOPTS"] = (addopts + " -p no:cacheprovider").strip()
        env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def _via_stdin(exe: str) -> bool:
    """cmd.exe parses .cmd/.bat arguments: newlines truncate them and ``& | < > %`` are live.

    The npm shims of the vendor CLIs are .cmd files, so the (multi-line, quote- and pipe-bearing)
    prompt travels on stdin for those instead of in argv.
    """
    return exe.lower().endswith((".cmd", ".bat"))


def windows_sandbox_args() -> list[str]:
    """``-c windows.sandbox="<mode>"`` for a Codex follower on Windows, from ``[follow] codex_windows_sandbox``.

    ~/.codex/config.toml may select the ``elevated`` sandbox, whose one-time admin setup has not always been run;
    every shell call then fails with ``helper_unknown_error``. ``unelevated`` needs no setup and still refuses writes
    in read-only mode. An empty value or another OS adds nothing.
    """
    if os.name != "nt":
        return []
    mode = settings().get("codex_windows_sandbox")
    return ["-c", f'windows.sandbox="{mode.strip()}"'] if isinstance(mode, str) and mode.strip() else []


def build_command(spec: dict, prompt: str, sibling: Path, out_file: Path, allowed_bash=None) -> tuple[list[str], str | None]:
    """(argv, stdin_text) for one follower run, per the Module D command lines."""
    exe = config.exe(spec["vendor"])
    stdin_text = prompt if _via_stdin(exe) else None
    model, effort = spec["model"], spec["effort"]
    if spec["vendor"] == "claude":
        argv = [exe, "-p"]
        if stdin_text is None:
            argv.append(prompt)
        if model:
            argv += ["--model", model]
        if effort:
            argv += ["--effort", effort]
        if spec["readonly"]:
            argv += [
                "--permission-mode", "dontAsk",
                "--allowedTools", "Read", "Grep", "Glob", "Bash(git log:*)", "Bash(git diff:*)", "Bash(git show:*)",
                "--disallowedTools", "Edit", "Write", "NotebookEdit",
            ]
        else:
            argv += ["--permission-mode", "acceptEdits", "--allowedTools", *(allowed_bash or DEFAULT_ALLOWED_BASH),
                     "--disallowedTools", "Bash(git push:*)", "Bash(git remote:*)"]  # followers never publish
        argv += ["--no-session-persistence", "--output-format", "text"]
    else:
        argv = [exe, "exec", "-C", str(sibling), "-s", "read-only" if spec["readonly"] else "workspace-write"]
        argv += windows_sandbox_args()
        if model:
            argv += ["-m", model]
        if effort:
            argv += ["-c", f'model_reasoning_effort="{effort}"']
        argv.append("--ephemeral")
        if not spec["readonly"]:  # followers never publish: no network from the write sandbox
            argv += ["-c", "sandbox_workspace_write.network_access=false"]
        if spec["readonly"]:
            argv.append("--skip-git-repo-check")
        argv += ["-o", str(out_file), "-" if stdin_text is not None else prompt]
    return argv, stdin_text


def _kill_tree(proc: "subprocess.Popen") -> None:
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                capture_output=True, timeout=30, creationflags=config.CREATE_NO_WINDOW,
            )
        except (OSError, subprocess.SubprocessError):
            pass
    try:
        proc.kill()
    except OSError:
        pass


def _run_cli(argv, cwd: Path, env: dict, timeout_s: float, stdin_text):
    """Run the vendor CLI. Returns (returncode | None, stdout, stderr, timed_out)."""
    proc = subprocess.Popen(
        argv,
        stdin=subprocess.PIPE if stdin_text is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=str(cwd), env=env,
        text=True, encoding="utf-8", errors="replace", creationflags=config.CREATE_NO_WINDOW,
    )
    try:
        stdout, stderr = proc.communicate(input=stdin_text, timeout=timeout_s)
        return proc.returncode, stdout or "", stderr or "", False
    except subprocess.TimeoutExpired:
        _kill_tree(proc)
        try:
            stdout, stderr = proc.communicate(timeout=15)
        except Exception:
            stdout = stderr = ""
        return None, stdout or "", stderr or "", True


def count_findings(text: str) -> int:
    """Lines that are list/table items citing ``path:line``; prose and "No findings." do not count."""
    count = 0
    for line in text.splitlines():
        if not PATH_LINE_RE.search(line):
            continue
        if ITEM_START_RE.match(line) or line.count("|") >= 2:
            count += 1
    return count


def _save_wip(path: Path, role: str, notes: list[str]) -> str:
    """Keep uncommitted work in a sibling reachable (``refs/baton/wip/<role>/<time>``) before
    anything resets, cleans or switches branches there. Returns the ref, or '' if nothing to save."""
    if not gitutil.out(path, "status", "--porcelain"):
        return ""
    gitutil.git(path, "add", "-A")  # stash create only records staged new files
    sha = gitutil.out(path, "stash", "create")
    if not sha:
        return ""
    ref = f"refs/baton/wip/{role}/{time.strftime('%Y%m%d%H%M%S')}"
    if gitutil.git(path, "update-ref", ref, sha).returncode != 0:
        return ""
    notes.append(f"Uncommitted work found in {path} was saved as {ref} ({_short(sha)}); "
                 f"recover it with `git stash apply {_short(sha)}`.")
    return ref


def scratch_path(root: Path, name: str, sha: str) -> Path:
    """Throwaway detached checkout for a read-only role: ``.claude/worktrees/_scratch/<ws>-<role>-<sha7>``."""
    return _scratch_dir(root) / f"{paths.ws_name(root)}-{name}-{_short(sha)}"


def _discard_tree(root: Path, path: Path) -> None:
    """Remove a worktree directory and its git registration; never raises."""
    try:
        from . import worktrees
        left = worktrees.remove_worktree(paths.main_checkout(root), path)
        if left:
            _log(f"could not remove {path}: {left}")
        parent = path.parent
        if parent.name == worktrees.SCRATCH and parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
    except Exception as exc:  # noqa: BLE001
        _log(f"could not remove {path}: {exc}")


def stale_scratch(root: Path, name: str) -> list[Path]:
    """Every scratch checkout of this workstream's ``name`` role, whatever sha it was made for."""
    folder = _scratch_dir(root)
    try:
        return sorted(folder.glob(f"{paths.ws_name(root)}-{name}-*")) if folder.is_dir() else []
    except OSError:
        return []


def _prepare_review_tree(root: Path, name: str, sha: str, notes: list[str]) -> Path:
    """Detached checkout of ``sha`` in a scratch directory; the caller removes it after the run.

    Runs of one role are serialised by the role claim, so any other scratch checkout of this workstream and role
    (a crashed runner's, made for another sha) is debris and goes first."""
    path = scratch_path(root, name, sha)
    for old in stale_scratch(root, name):
        _discard_tree(root, old)
    if path.exists():
        _discard_tree(root, path)
    path.parent.mkdir(parents=True, exist_ok=True)
    gitutil.git(root, "worktree", "prune")
    _git_check(root, "worktree", "add", "--detach", str(path), sha)
    return path


def _check_cap(root: Path, path: Path) -> None:
    """Refuse to create a follower sibling when the repo is at its follower-worktree limit.

    Follower siblings and scratch checkouts do not count toward ``[worktrees] cap_per_repo``; they have their
    own ``follower_cap_per_repo``."""
    from . import worktrees
    main = paths.main_checkout(root)
    count, cap = len(worktrees.follower_worktrees(main)), worktrees.settings()["follower_cap_per_repo"]
    if count >= cap:
        raise FollowError(f"follower worktree cap reached ({count}/{cap} follower worktrees in {main.name}): not creating "
                          f"{path.name}; free one with `bt follow reset <role>` (or `bt merge <role>` for finished work) "
                          "and the next run will create it")


def _record_sibling(root: Path, path: Path, name: str, branch: str, lead_branch: str, sha: str) -> None:
    """Lineage record and an Exit line for a new follower sibling (best effort)."""
    try:
        from . import worktrees
        main = paths.main_checkout(root)
        paths.ensure_excluded(path)
        spec = f"merge:{lead_branch}" if lead_branch else "discard"
        worktrees.write_now(path, path.name, f"{name} follower of {lead_branch or paths.ws_name(root)}", spec,
                            f"{lead_branch or 'HEAD'} @ {_short(sha)}", str(root))
        worktrees.record_created(main, path, branch=branch, base=lead_branch or "HEAD", base_sha=sha, parent=str(root),
                                 exit_spec=spec, purpose=f"{name} follower", created_by=f"follower:{name}")
    except Exception as exc:  # noqa: BLE001
        _log(f"could not record lineage for {path}: {exc}")


def _prepare_write_tree(root: Path, name: str, sha: str, lead_branch: str, st: dict,
                        notes: list[str]) -> tuple[Path, str]:
    """Sibling worktree on ``<lead_branch>--<name>`` with ``sha`` merged in.

    Raises MergeConflict (follower and lead disagree) or FollowError (lead history rewritten while
    the follower branch holds its own commits). ``st`` is the role's state before this run.
    """
    path = sibling_path(root, name)
    branch = follower_branch(root, name, lead_branch)
    update_state(root, name, lead_branch=lead_branch, branch=branch)
    if (path / ".git").exists():
        _save_wip(path, name, notes)
        current = gitutil.branch(path)
        _git_check(path, "reset", "--hard")
        _git_check(path, "clean", "-fd")
        if current != branch:  # the sibling was left on another branch (the lead moved, or it is detached)
            if _has_ref(root, f"refs/heads/{branch}"):
                _git_check(path, "checkout", branch)
            else:
                _git_check(path, "checkout", "-b", branch, sha)
    else:
        gitutil.git(root, "worktree", "prune")
        _check_cap(root, path)
        if _has_ref(root, f"refs/heads/{branch}"):
            _git_check(root, "worktree", "add", str(path), branch)
        else:
            _git_check(root, "worktree", "add", "--no-track", "-b", branch, str(path), sha)
        _record_sibling(root, path, name, branch, lead_branch, sha)

    merged = st.get("last_merged_lead_sha") if st.get("branch") == branch else None
    if merged and merged != sha and gitutil.git(path, "merge-base", "--is-ancestor", merged, sha).returncode != 0:
        own = gitutil.out(path, "rev-list", "--count", f"{merged}..{branch}")
        if own == "0":  # nothing of the follower's own sits on the old history: rebuild from the new tip
            _git_check(path, "checkout", "-B", branch, sha)
            notes.append(f"Lead history was rewritten; {branch} was rebuilt from {_short(sha)}.")
        else:
            held = f"{own} unmerged commits" if own else "unmerged commits"
            raise FollowError(
                f"lead history was rewritten; follower branch {branch} holds {held} on the old history: "
                f"merge with `bt merge {name}` or `bt follow reset {name}`"
            )

    cp = gitutil.git(path, "merge", "--no-edit", sha, timeout=120)
    if cp.returncode != 0:
        files = gitutil.out(path, "diff", "--name-only", "--diff-filter=U").splitlines()
        gitutil.git(path, "merge", "--abort")
        detail = f" ({', '.join(files[:8])})" if files else ""
        raise MergeConflict(
            f"follower branch {branch} conflicts with lead {_short(sha)}{detail}; "
            f"merge it with `bt merge {name}` or reset with `bt follow reset {name}`"
        )
    update_state(root, name, last_merged_lead_sha=sha)
    return path, branch


def _stage_leftovers(sibling: Path, notes: list[str]) -> None:
    """Stage tracked changes (``add -u``) and the untracked files worth keeping: not over COMMIT_MAX_UNTRACKED and
    not an archive, bundle or patch (agent scratch). The files left out are named in ``notes``."""
    import fnmatch
    gitutil.git(sibling, "add", "-u")
    cp = gitutil.git(sibling, "ls-files", "-o", "--exclude-standard", "-z", timeout=120)
    take: list[str] = []
    skipped: list[str] = []
    for rel in (r for r in (cp.stdout if cp.returncode == 0 else "").split("\0") if r):
        base = rel.rsplit("/", 1)[-1].lower()
        try:
            size = os.stat(sibling / rel).st_size
        except OSError:
            continue
        if any(fnmatch.fnmatch(base, g) for g in COMMIT_SKIP_GLOBS):
            skipped.append(f"{rel} (archive/bundle/patch)")
        elif size > COMMIT_MAX_UNTRACKED:
            skipped.append(f"{rel} ({size / (1 << 20):.1f} MB)")
        else:
            take.append(rel)
    for n in range(0, len(take), 100):
        gitutil.git(sibling, "add", "--", *take[n:n + 100], timeout=120)
    if skipped:
        notes.append(f"Baton did not commit {len(skipped)} untracked file(s) left in {sibling}: "
                     + ", ".join(skipped[:10]) + (" ..." if len(skipped) > 10 else ""))


def _commit_leftovers(sibling: Path, spec: dict, result: str, notes: list[str]) -> bool:
    """Commit what a write follower left uncommitted (a Codex workspace-write sandbox cannot write
    a linked worktree's git dir, so it cannot commit itself). Author comes from the repo config."""
    _stage_leftovers(sibling, notes)
    if gitutil.git(sibling, "diff", "--cached", "--quiet").returncode == 0:
        return False
    prefix = COMMIT_PREFIX.get(spec["name"], "chore")
    summary = " ".join((next((ln for ln in result.splitlines() if ln.strip()), "")).split()) or "follower changes"
    subject = f"{prefix}: {summary}"[:72].rstrip()
    cp = gitutil.git(sibling, "commit", "-m", subject, "-m", result.strip()[:NOTE_CAP] or "(no summary)", timeout=120)
    if cp.returncode != 0:
        notes.append("Baton could not commit the changes the follower left behind: "
                     + (cp.stderr or cp.stdout).strip().replace("\n", " ")[:200])
        return False
    notes.append("Baton committed the changes the follower left uncommitted.")
    return True


def _commit_instruction(spec: dict, prefix: str, what: str) -> str:
    """How a write follower is told to keep its work. A Codex sandbox cannot write a linked worktree's
    git dir, so Codex followers leave their changes uncommitted and baton commits them afterwards."""
    if spec.get("vendor") == "codex":
        return (f"Do not run git commit or create bundles/patches; leave your {what} in this worktree — "
                f"baton commits them on this branch after you finish with a `{prefix}: ` message built from your final summary. ")
    return f"Commit your {what} on this branch with messages starting `{prefix}: `. "


def _prompt(spec: dict, branch: str, base: str, sha: str, lead_note: str = "", brief: str = "") -> str:
    if spec["name"] == "explore" and not spec["readonly"]:
        prefix = COMMIT_PREFIX["explore"]
        note = (f"The lead's brief is {lead_note} (a read-only copy of the lead's .baton/now.md; do not edit it). "
                if lead_note else "")
        task = (
            f"{EXPLORE_INSTRUCTIONS}\n\n"
            f"Read the lead's changes in {base}..{sha} on branch {branch or '(detached)'} (`git log`, `git diff {base}..{sha}`). "
            f"{note}"
            "Identify one to three materially different approaches to the same objective. "
            "Prototype the most promising one in this worktree (rough is fine) and measure or check enough to compare it "
            "with the others. Work only in this worktree. "
            f"{_commit_instruction(spec, prefix, 'prototype')}"
            "Final message: at most 20 lines covering the approaches considered, what you prototyped, "
            "evidence for and against, and a recommendation."
        )
    elif spec["readonly"]:
        task = (
            f"Review the commit range {base}..{sha} on branch {branch or '(detached)'} "
            f"(use `git log`/`git diff {base}..{sha}`). "
            "Repository instructions: read AGENTS.md or CLAUDE.md if present."
        )
    else:
        prefix = COMMIT_PREFIX.get(spec["name"], "chore")
        task = (
            f"Scope: changes in {base}..{sha} (see `git diff {base}..{sha}`). "
            "Work only in this worktree. "
            f"{_commit_instruction(spec, prefix, 'changes')}"
            "Run the relevant tests before committing. "
            "Final message: at most 12 lines summarising what you changed and the test results, "
            "or why you changed nothing."
        )
    if brief:
        task += f"\n\nOwner brief: {brief}"
    if spec["name"] == "explore" and not spec["readonly"]:
        return task
    return f"{spec['instructions']}\n\n{task}"


def _write_note(root: Path, name: str, sha: str, header: str, body: str, log_rel: str,
                extra=(), dedupe_reason: str | None = None) -> Path:
    """Write an inbox note. ``extra`` lines go between the header and the result. With
    ``dedupe_reason``, an unread note for this role that failed for the same reason is replaced
    rather than followed by a near-identical one."""
    inbox = paths.baton_dir(root) / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    if dedupe_reason is not None:
        previous = role_notes(root, name)
        if previous:
            latest = max(previous, key=lambda p: (p.stat().st_mtime, p.name))
            if _first_line(latest).endswith(f": failed: {dedupe_reason}"):
                try:
                    latest.unlink()
                except OSError:
                    pass
    stem = f"{time.strftime('%Y%m%d-%H%M')}-{name}-{_short(sha)}"
    path = inbox / f"{stem}.md"
    n = 1
    while path.exists():
        n += 1
        path = inbox / f"{stem}-{n}.md"
    lead = "".join(f"{line}\n" for line in extra)
    text = f"{header}\n\n{lead + chr(10) if lead else ''}{body.strip()[:NOTE_CAP]}\n\nLog: {log_rel}\n"
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    (inbox / ".unread").write_text("", encoding="utf-8")
    return path


def _header(spec: dict | None, name: str, base: str, sha: str, outcome: str) -> str:
    if spec:
        who = f"{spec['vendor']}:{spec['account']} {spec['model'] or 'default'}/{spec['effort'] or 'default'}"
        if spec.get("fallback"):
            who += f"; {spec['fallback']}"
    else:
        who = "unresolved"
    return f"{name} ({who}) on {_short(base) if base else '-------'}..{_short(sha)}: {outcome}"


def _copy_lead_note(root: Path, sibling: Path, notes: list[str]) -> str:
    """Read-only copy of the lead's now.md inside ``sibling`` (a follower may be unable to read the lead's tree);
    returns the copy's path, or "" when the lead has none."""
    source = paths.baton_dir(root) / "now.md"
    target = paths.baton_dir(sibling, create=True) / LEAD_NOTE_NAME
    try:
        if target.exists():
            os.chmod(target, 0o644)
        text = source.read_text(encoding="utf-8", errors="replace")
        target.write_text(text, encoding="utf-8", newline="\n")
        os.chmod(target, 0o444)
    except OSError as exc:
        notes.append(f"Could not copy the lead's now.md for the follower: {exc}")
        return ""
    return str(target)


def run_role(root: Path, name: str, sha: str, branch: str | None = None, brief: str = "") -> str:
    """Run one follower role end to end. Never raises; returns the final status string.

    ``branch`` is the lead branch the commit belongs to (``None``: the lead worktree's current one).
    Only a successful run advances ``last_sha``; failed and skipped runs keep the old base.
    """
    started = time.time()
    if branch is None:
        branch = gitutil.branch(root)
    update_state(root, name, pid=os.getpid(), last_started_at=started, status="running", error=None,
                 brief=brief or None)
    spec = None
    base = ""
    log_rel = "(none)"
    notes: list[str] = []
    scratch = None
    try:
        cfg = settings()
        spec = resolve_follower(root, name)
        if spec["model_note"]:
            notes.append(f"Model: {spec['model_note']}")
        cap = daily_cap(cfg)
        if cap and runs_today(read_role(root, name), started) >= cap:
            status = "skipped: daily cap"
            update_state(root, name, status=status, pid=None, finished_at=time.time())
            return status
        ceiling = cfg.get("usage_ceiling")
        pct = utilisation(spec["vendor"], spec["account"])
        if ceiling is not None and pct is not None and pct >= int(ceiling):
            status = f"skipped: usage {pct}%"
            update_state(root, name, status=status, pid=None, finished_at=time.time())
            return status
        st = read_role(root, name)
        update_state(root, name, runs_day=time.strftime("%Y-%m-%d", time.localtime(started)),
                     runs_today=runs_today(st, started) + 1)
        base = review_base(root, st.get("last_sha") or "", st.get("last_base") or "", sha, gitutil.default_branch(root))
        runs = paths.baton_dir(root) / "runs"
        runs.mkdir(parents=True, exist_ok=True)
        log_name = f"{name}-{_short(sha)}.log"
        out_file = runs / f"{name}-{_short(sha)}.out"
        try:
            out_file.unlink()
        except OSError:
            pass

        _start_log(runs / log_name, spec, sha)   # a crash while the tree is being prepared leaves a trace
        log_rel = f".baton/runs/{log_name}"
        pre_head = ""
        follower = ""
        if spec["readonly"]:
            sibling = scratch = _prepare_review_tree(root, name, sha, notes)
        else:
            sibling, follower = _prepare_write_tree(root, name, sha, branch, st, notes)
            pre_head = gitutil.out(sibling, "rev-parse", "HEAD")

        lead_note = _copy_lead_note(root, sibling, notes) if name == "explore" and not spec["readonly"] else ""
        prompt = _prompt(spec, branch, base, sha, lead_note, brief)
        allowed = load_follow_toml(root).get("allowed_bash")
        argv, stdin_text = build_command(spec, prompt, sibling, out_file, allowed if isinstance(allowed, list) else None)
        timeout_min = float(cfg["timeout_minutes"])
        try:
            rc, stdout, stderr, timed_out = _run_cli(argv, sibling, _child_env(spec), timeout_min * 60, stdin_text)
        except OSError as exc:
            raise FollowError(f"could not start {argv[0]}: {exc}") from exc

        result = stdout.strip()
        if spec["vendor"] == "codex":
            try:
                text = out_file.read_text(encoding="utf-8", errors="replace").strip()
            except OSError:
                text = ""
            result = text or result
        _write_log(runs / log_name, spec, argv, sibling, rc, timed_out, stdout, stderr)
        log_rel = f".baton/runs/{log_name}"

        if timed_out:
            raise FollowError(f"timed out after {timeout_min:g} min")
        if spec["vendor"] == "codex" and any(m in text for m in SANDBOX_FAILURE_MARKERS for text in (stdout, result)):
            raise _Failure("sandbox", (stdout.strip() or result)[-600:] or stderr.strip()[-600:])
        if rc != 0:
            tail = (stderr.strip() or result)[-600:]
            raise _Failure(f"exit code {rc}", tail)
        if spec["readonly"]:
            if not result:
                raise _Failure("empty output", stderr.strip()[-600:])
            outcome = f"{count_findings(result)} findings"
        else:
            dirty = bool(gitutil.out(sibling, "status", "--porcelain"))
            if dirty:
                committed = _commit_leftovers(sibling, spec, result, notes)
                if not committed and spec["vendor"] == "codex":
                    raise FollowError(f"could not persist follower changes; uncommitted changes remain in {sibling}")
                dirty = bool(gitutil.out(sibling, "status", "--porcelain"))
            new = int(gitutil.out(sibling, "rev-list", "--count", f"{pre_head}..HEAD") or 0) if pre_head else 0
            if new:
                outcome = f"+{new} commits on {follower} — `bt merge {name}`"
            elif dirty:
                outcome = f"no commits (uncommitted changes left in {sibling})"
            else:
                outcome = "no changes"
        _write_note(root, name, sha, _header(spec, name, base, sha, outcome), result or "(no output)", log_rel,
                    extra=notes)
        update_state(root, name, last_sha=sha, last_base=base, status="done", pid=None, error=None,
                     finished_at=time.time(), vendor=spec["vendor"], account=spec["account"])
        return "done"
    except Exception as exc:
        if isinstance(exc, _Failure):
            reason, detail = exc.args
        else:
            reason, detail = str(exc) or type(exc).__name__, ""
            if not isinstance(exc, FollowError):
                reason = f"{type(exc).__name__}: {reason}"
        try:
            _write_note(root, name, sha, _header(spec, name, base, sha, f"failed: {reason}"), detail or reason,
                        log_rel, extra=notes, dedupe_reason=reason)
        except Exception as note_exc:
            _log(f"could not write failure note for {name}: {note_exc}")
        try:
            update_state(root, name, status="failed", pid=None, error=reason, finished_at=time.time())
        except Exception as state_exc:
            _log(f"could not record failure of {name}: {state_exc}")
        return "failed"
    finally:
        if scratch is not None:
            _discard_tree(root, scratch)


def _start_log(path: Path, spec: dict, sha: str) -> None:
    try:
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(f"# {spec['name']} {spec['vendor']}:{spec['account']} sha={sha} pid={os.getpid()} "
                     f"started {time.strftime('%Y-%m-%d %H:%M:%S')}\n# preparing the follower worktree\n")
    except OSError as exc:
        _log(f"could not write {path}: {exc}")


def _write_log(path: Path, spec: dict, argv, cwd: Path, rc, timed_out: bool, stdout: str, stderr: str) -> None:
    shown = " ".join(a if len(a) <= 120 else a[:117] + "..." for a in argv)
    head = f"# {spec['name']} {spec['vendor']}:{spec['account']} cwd={cwd} exit={'timeout' if timed_out else rc}\n# {shown}\n"
    try:
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(f"{head}--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}\n")
    except OSError as exc:
        _log(f"could not write {path}: {exc}")


# --------------------------------------------------------------------------- runner slots and housekeeping

def _locks_dir() -> Path:
    return config.STATE_DIR / "follow-locks"


_held_slots: dict[str, int] = {}   # lock file -> the open handle that holds the OS lock


def _os_lock(fd: int) -> bool:
    """Take an exclusive non-blocking OS lock (a byte range on Windows, flock elsewhere) on the open handle."""
    try:
        if os.name == "nt":
            import msvcrt
            os.lseek(fd, LOCK_OFFSET, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _os_unlock(fd: int) -> None:
    try:
        if os.name == "nt":
            import msvcrt
            os.lseek(fd, LOCK_OFFSET, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        pass


def _try_slot() -> Path | None:
    """One free runner slot, held by an OS lock on a handle this process keeps open: the lock vanishes when the
    process dies however it dies, and a recycled pid cannot make a dead runner look alive."""
    locks = _locks_dir()
    locks.mkdir(parents=True, exist_ok=True)
    for n in range(MAX_RUNNERS):
        lock = locks / f"{n}.lock"
        try:
            fd = os.open(lock, os.O_CREAT | os.O_RDWR)
        except OSError:
            continue
        if not _os_lock(fd):
            os.close(fd)
            continue
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, f"{os.getpid():<20}".encode("ascii"))   # for people reading the file; liveness is the lock
        except OSError:
            pass
        _held_slots[str(lock)] = fd
        return lock
    return None


def acquire_slot(wait_s: float | None = None, poll_s: float | None = None) -> Path | None:
    """Take one of MAX_RUNNERS machine-wide runner slots, polling for up to ``wait_s`` seconds."""
    wait_s = SLOT_WAIT_S if wait_s is None else wait_s
    poll_s = SLOT_POLL_S if poll_s is None else poll_s
    deadline = time.time() + wait_s
    while True:
        slot = _try_slot()
        if slot is not None:
            return slot
        remaining = deadline - time.time()
        if remaining <= 0:
            return None
        time.sleep(min(poll_s, remaining))


def release_slot(slot: Path) -> None:
    """Unlock and close the handle holding ``slot``. The lock file stays (an empty marker); a slot this process
    does not hold is left alone."""
    fd = _held_slots.pop(str(slot), None)
    if fd is None:
        return
    _os_unlock(fd)
    try:
        os.close(fd)
    except OSError:
        pass


def _prune(directory: Path, keep: int) -> None:
    try:
        files = sorted((p for p in directory.iterdir() if p.is_file()),
                       key=lambda p: (p.stat().st_mtime, p.name), reverse=True)
    except OSError:
        return
    for old in files[keep:]:
        try:
            old.unlink()
        except OSError:
            pass


def prune_history(root: Path) -> None:
    """Keep the newest KEEP_RUNS run logs and KEEP_READ dismissed notes."""
    _prune(paths.baton_dir(root) / "runs", KEEP_RUNS)
    _prune(paths.baton_dir(root) / "inbox" / "read", KEEP_READ)


def run_roles(root: Path, sha: str, roles, branch: str | None = None, brief: str = "") -> dict:
    """Run roles one after another; a failure in one never stops the next.

    At most MAX_RUNNERS runners work at once on the machine; without a free slot after
    SLOT_WAIT_S the roles are recorded as ``skipped: busy`` (last_sha untouched).
    """
    _scrub_git_env()
    roles = list(roles)
    results: dict = {}
    slot = acquire_slot()
    if slot is None:
        for name in roles:
            results[name] = "skipped: busy"
            try:
                update_state(root, name, status="skipped: busy", pid=None, finished_at=time.time())
            except Exception as exc:
                _log(f"could not record busy skip of {name}: {exc}")
        return results
    try:
        for name in roles:
            try:
                results[name] = run_role(root, name, sha, branch, brief)
            except Exception as exc:  # run_role should not raise; belt and braces
                results[name] = "failed"
                _log(f"runner error in {name}: {type(exc).__name__}: {exc}")
    finally:
        release_slot(slot)
        try:
            prune_history(root)
        except Exception as exc:
            _log(f"prune failed: {exc}")
    return results


# --------------------------------------------------------------------------- inbox

def inbox_dir(root: Path) -> Path:
    return paths.baton_dir(root) / "inbox"


def inbox_notes(root: Path) -> list[Path]:
    try:
        return sorted(p for p in inbox_dir(root).glob("*.md") if p.is_file())
    except OSError:
        return []


def _first_line(path: Path) -> str:
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            return fh.readline().strip()
    except OSError:
        return ""


def _move_to_read(root: Path, notes) -> int:
    read = inbox_dir(root) / "read"
    read.mkdir(parents=True, exist_ok=True)
    moved = 0
    for note in notes:
        try:
            os.replace(note, read / note.name)
            moved += 1
        except OSError as exc:
            print(f"could not move {note.name}: {exc}", file=sys.stderr)
    if not inbox_notes(root):
        try:
            (inbox_dir(root) / ".unread").unlink()
        except OSError:
            pass
    return moved


def role_notes(root: Path, name: str) -> list[Path]:
    pattern = re.compile(rf"^\d{{8}}-\d{{4}}-{re.escape(name)}-[0-9a-f]+(?:-\d+)?\.md$")
    return [p for p in inbox_notes(root) if pattern.match(p.name)]


def _find_note(root: Path, query: str) -> Path | None:
    notes = inbox_notes(root)
    for text in (query, query + ".md"):
        for note in notes:
            if note.name == text:
                return note
    matches = [n for n in notes if query in n.name]
    return matches[0] if len(matches) == 1 else None


# --------------------------------------------------------------------------- commands

def _root(args) -> Path:
    start = getattr(args, "worktree", None)
    root = paths.find_worktree_root(start or Path.cwd())
    if root is None:
        raise FollowError(f"not inside a git worktree: {start or Path.cwd()}")
    return root


def _guard(func):
    def wrapper(args) -> int:
        _scrub_git_env()
        try:
            return int(func(args) or 0)
        except FollowError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    wrapper.__name__ = func.__name__
    return wrapper


def _parse_roles(text: str | None) -> list[str]:
    roles: list[str] = []
    for item in (text or "").split(","):
        item = item.strip()
        if item and item not in roles:
            roles.append(item)
    return roles


def _check_known(roles) -> None:
    known = follower_names()
    bad = [r for r in roles if r not in known]
    if bad:
        raise FollowError(f"unknown follower role(s): {', '.join(bad)} (known: {', '.join(known)})")


@_guard
def cmd_on(args) -> int:
    root = _root(args)
    roles = _parse_roles(args.roles) or list(DEFAULT_ROLES)
    _check_known(roles)
    paths.ensure_excluded(root)
    paths.baton_dir(root, create=True)
    hook, action = install_hook(root, force=args.force)  # refuses before anything is enabled
    previous = load_follow_toml(root).get("allowed_bash")
    text = "# baton followers for this worktree (bt follow on / off)\n"
    text += f"roles = {json.dumps(roles)}\n"
    if isinstance(previous, list) and previous:
        text += f"allowed_bash = {json.dumps(previous)}\n"
    _atomic_write(follow_toml_path(root), text)
    print(f"followers on for {root.name}: {', '.join(roles)}")
    print(f"post-commit hook {action}: {hook}")
    return 0


def _role_artifacts(root: Path, name: str) -> bool:
    st = read_role(root, name)
    branch = st.get("branch") or follower_branch(root, name)
    return bool(st) or sibling_path(root, name).exists() or _has_ref(root, f"refs/heads/{branch}")


@_guard
def cmd_off(args) -> int:
    root = _root(args)
    try:
        follow_toml_path(root).unlink()
        print(f"followers off for {root.name} (the post-commit hook stays; it does nothing without follow.toml)")
    except FileNotFoundError:
        print(f"followers were not on for {root.name}")
    if args.clean:
        for name in dict.fromkeys([*follower_names(), *read_state(root)]):
            if not _role_artifacts(root, name):
                continue
            try:
                print(_reset_role(root, name, force=False))
            except FollowError as exc:
                print(f"kept {name}: {exc}")
    return 0


@_guard
def cmd_now(args) -> int:
    root = _root(args)
    roles = _parse_roles(args.roles) or automatic_roles(root)
    if not roles:
        raise FollowError("followers are not enabled here; run `bt follow on` (or pass --roles)")
    _check_known(roles)
    sha, branch = head_info(root)
    if not sha:
        raise FollowError("no commits yet")
    due, why = due_roles(root, roles, force=True)
    for name, reason in why.items():
        print(f"{name}: not started ({reason})")
    if not due:
        return 0
    brief = " ".join((getattr(args, "brief", None) or "").split())[:BRIEF_CAP]
    proc = spawn_runner(root, sha, due, branch, brief=brief) if brief else spawn_runner(root, sha, due, branch)
    print(f"runner pid {proc.pid} started for {', '.join(due)} at {_short(sha)}; results land in .baton/inbox/")
    return 0


@_guard
def cmd_status(args) -> int:
    root = _root(args)
    enabled = enabled_roles(root)
    state = read_state(root)
    try:
        hook = hooks_dir(root, force=True) / "post-commit"
        installed = hook.is_file() and HOOK_BEGIN in hook.read_text(encoding="utf-8", errors="replace")
    except (OSError, FollowError):
        installed = False
    print(f"worktree {root}")
    print(f"followers: {', '.join(enabled) if enabled else 'off'}   post-commit hook: {'installed' if installed else 'missing'}")
    now = time.time()
    for name in dict.fromkeys([*enabled, *state]):
        st = state.get(name, {}) if isinstance(state.get(name), dict) else {}
        status = st.get("status") or "never run"
        if status == "running" and not pid_alive(st.get("pid")):
            status = "running (runner gone)"
        started = st.get("last_started_at")
        ago = f"{(now - float(started)) / 60:.0f} min ago" if started else "-"
        try:
            spec = resolve_follower(root, name)
            who = f"{spec['vendor']}:{spec['account']} {spec['model'] or 'default'}/{spec['effort'] or 'default'}"
            if spec["fallback"]:
                who += f" ({spec['fallback']})"
            extra = ""
            if not spec["readonly"]:
                branch = st.get("branch") or follower_branch(root, name)
                if _has_ref(root, f"refs/heads/{branch}"):
                    lead = st.get("lead_branch") or "HEAD"
                    lead_ref = f"refs/heads/{lead}" if lead != "HEAD" and _has_ref(root, f"refs/heads/{lead}") else "HEAD"
                    extra = f"  branch {branch} (+{gitutil.out(root, 'rev-list', '--count', f'{lead_ref}..{branch}') or 0} unmerged)"
        except FollowError as exc:
            who, extra = f"? ({exc})", ""
        flag = "" if name in enabled else " (disabled)"
        print(f"  {name}{flag}: {status}; last sha {_short(st.get('last_sha') or '') or '-'}; started {ago}; {who}{extra}")
        if st.get("error"):
            print(f"      error: {st['error']}")
    return 0


def _listed(items: list[str], limit: int = 5) -> str:
    return ", ".join(items[:limit]) + (f", ... (+{len(items) - limit})" if len(items) > limit else "")


def _reset_role(root: Path, name: str, force: bool = False) -> str:
    """Remove a follower's sibling worktree and branch. Refuses (FollowError) while the role runs,
    while the sibling has uncommitted changes, or while its branch has commits that are not in the
    lead branch it followed, unless ``force``; uncommitted work is always saved under refs/baton/wip."""
    st = read_role(root, name)
    if _is_running(st, time.time(), float(settings()["timeout_minutes"]) * 60):
        raise FollowError(f"{name} is running (pid {st.get('pid')}); wait for it or kill it first")
    path = sibling_path(root, name)
    try:
        readonly = resolve_follower(root, name)["readonly"]
    except FollowError:
        readonly = name == "review"
    branch = "" if readonly else (st.get("branch") or follower_branch(root, name))
    has_branch = bool(branch) and _has_ref(root, f"refs/heads/{branch}")
    is_tree = (path / ".git").exists()
    if is_tree and not force:
        from . import worktrees
        left = worktrees.removal_blockers(path)
        if left:
            raise FollowError(f"{path} has uncommitted changes or ignored files ({_listed(left)}); pass --force to save the "
                              "uncommitted ones under refs/baton/wip and reset (ignored files are not saved)")
    if has_branch and not force:
        lead = st.get("lead_branch") or gitutil.branch(root)
        lead_ref = f"refs/heads/{lead}" if lead and _has_ref(root, f"refs/heads/{lead}") else "HEAD"
        unmerged = int(gitutil.out(root, "rev-list", "--count", f"{lead_ref}..{branch}") or 0)
        if unmerged:
            raise FollowError(
                f"{branch} has {unmerged} commit(s) not in the lead branch ({lead or 'HEAD'}); "
                f"run `bt merge {name}` first or pass --force to discard them"
            )
    saved: list[str] = []
    if is_tree:
        from . import worktrees
        _save_wip(path, name, saved)
        left = worktrees.remove_worktree(paths.main_checkout(root), path)
        if left:
            raise FollowError(f"could not remove {path}: {left[:300]}")
    elif path.exists():
        raise FollowError(f"{path} exists but is not a worktree; remove it by hand")
    if readonly:
        for left in stale_scratch(root, name):
            _discard_tree(root, left)
    gitutil.git(root, "worktree", "prune")
    if has_branch:
        cp = gitutil.git(root, "branch", "-D", branch)
        if cp.returncode != 0:
            raise FollowError(f"could not delete {branch}: {(cp.stderr or cp.stdout).strip()[:200]}")
    clear_state(root, name)
    return "\n".join([f"reset {name}: removed {path}" + (f" and branch {branch}" if has_branch else ""), *saved])


@_guard
def cmd_reset(args) -> int:
    root = _root(args)
    _check_known([args.role])
    print(_reset_role(root, args.role, args.force))
    return 0


@_guard
def cmd_run(args) -> int:
    root = _root(args)
    sha = args.sha or head_sha(root)
    if not sha:
        raise FollowError("no commits yet")
    roles = _parse_roles(args.roles) or automatic_roles(root)
    brief = getattr(args, "brief", None) or ""
    results = run_roles(root, sha, roles, args.branch, brief) if brief else run_roles(root, sha, roles, args.branch)
    for name, status in results.items():
        print(f"{name}: {status}")
    return 0


def _lead_lacks(root: Path, branch: str, base: str | None = None) -> int:
    """Commits of ``branch`` that HEAD lacks. After a merge that is a plain ancestry count; after a ``--pick`` (``base``
    given, '' if unknown) it is the follower's own commits whose patch HEAD still lacks."""
    if base is not None:
        return len(_pick_list(root, branch, base))
    return int(gitutil.out(root, "rev-list", "--count", f"HEAD..{branch}") or 0)


def _drop_sibling(root: Path, name: str, branch: str, picked: bool = False, base: str = "") -> str:
    """After a successful merge (or --pick): remove the role's sibling worktree when it holds nothing the lead lacks
    (no uncommitted or ignored files, no commits missing from HEAD), then delete the follower branch: it is merged
    (``branch -d``), or, after a pick, only holds rewritten copies of commits HEAD already has (``-D``). The next run
    recreates both from the lead's sha. Returns a one-line report."""
    from . import worktrees
    path = sibling_path(root, name)
    lacks = base if picked else None
    if (path / ".git").exists():
        left = worktrees.removal_blockers(path)
        if left:
            return f"kept {path}: uncommitted changes or ignored files ({_listed(left)})"
        if _lead_lacks(root, branch, lacks):
            return f"kept {path}: it has commits the lead lacks"
        if not worktrees.retire_quietly(paths.main_checkout(root), path):
            return f"could not remove {path}"
        report = f"removed sibling worktree {path}"
    else:
        report = ""
    if _has_ref(root, f"refs/heads/{branch}") and not _lead_lacks(root, branch, lacks):
        cp = gitutil.git(root, "branch", "-D" if picked else "-d", branch)
        if cp.returncode == 0:
            update_state(root, name, last_merged_lead_sha=None, branch=None)
            report = (report + " and " if report else "") + f"deleted branch {branch}"
        else:
            report = (report + "; " if report else "") + f"kept branch {branch}: {(cp.stderr or cp.stdout).strip()[:120]}"
    return report


def _lead_base(st: dict, branch: str) -> str:
    """The lead sha the follower branch last had merged in (its own commits are the ones after it), '' if unknown."""
    return (st.get("last_merged_lead_sha") or "") if st.get("branch") in (None, "", branch) else ""


def _rewritten(root: Path, base: str) -> bool:
    """``base`` (the lead commit the follower ran on) is no longer in HEAD's history: the lead was rebased or amended,
    so merging the follower branch would bring the pre-rewrite copies back."""
    return bool(base) and gitutil.git(root, "merge-base", "--is-ancestor", base, "HEAD").returncode != 0


def _pick_list(root: Path, branch: str, base: str) -> list[str]:
    """The follower's own commits, oldest first, whose patch HEAD lacks: ``--cherry-pick`` drops patches HEAD already
    has, and everything reachable from ``base`` is the lead history the follower merged in (its pre-rewrite copies)."""
    args = ["rev-list", "--reverse", "--no-merges", "--right-only", "--cherry-pick", f"HEAD...{branch}"]
    if base and gitutil.out(root, "rev-parse", "--verify", "--quiet", f"{base}^{{commit}}"):
        args.append(f"^{base}")
    return gitutil.out(root, *args, timeout=120).split()


def _pick(root: Path, name: str, branch: str, shas: list[str]) -> int:
    """Cherry-pick ``shas`` (the rewrite-safe alternative to a merge); on a conflict abort and leave HEAD as it was."""
    if not shas:
        print(f"nothing to pick: every patch of {branch} is already in HEAD")
        return 0
    cp = gitutil.git(root, "cherry-pick", *shas, timeout=300)
    if cp.returncode != 0:
        files = gitutil.out(root, "diff", "--name-only", "--diff-filter=U").splitlines()
        gitutil.git(root, "cherry-pick", "--abort")
        detail = ", ".join(files) or (cp.stderr or cp.stdout).strip().replace(chr(10), " ")[:200]
        print(f"cherry-pick of {branch} stopped at a conflict in: {detail}", file=sys.stderr)
        print("cherry-pick aborted; the worktree is as it was. Resolve in the follower worktree, or "
              f"`bt follow reset {name}`.", file=sys.stderr)
        return 1
    print(f"cherry-picked {len(shas)} commit(s) from {branch}")
    return 0


@_guard
def cmd_merge(args) -> int:
    root = _root(args)
    name = args.role
    _check_known([name])
    gd = paths.git_dir(root)
    if gd is not None and (gd / "MERGE_HEAD").exists():
        raise FollowError("a merge is already in progress here; finish or abort it first")
    if not gitutil.branch(root):
        raise FollowError("the lead worktree is on a detached HEAD; check out the lead branch before `bt merge`")
    st = read_role(root, name)
    if _is_running(st, time.time(), float(settings()["timeout_minutes"]) * 60):
        raise FollowError(f"{name} is running (pid {st.get('pid')}); merge after it finishes")
    if resolve_follower(root, name)["readonly"]:
        raise FollowError(f"{name} is read-only and has no branch; read it with `bt inbox`")
    branch = follower_branch(root, name)
    if not _has_ref(root, f"refs/heads/{branch}"):
        raise FollowError(f"no follower branch {branch}; nothing to merge")
    pick = bool(getattr(args, "pick", False))
    base = _lead_base(st, branch)
    if not pick:
        if _rewritten(root, base):
            own = _pick_list(root, branch, base)
            print(f"refusing to merge {branch}: the lead history was rewritten ({_short(base)}, the lead commit {name} last ran "
                  "on, is no longer in HEAD), so a merge would bring the pre-rewrite copies back.", file=sys.stderr)
            print(f"commits of {branch} that are the follower's own and that HEAD lacks ({len(own)}):", file=sys.stderr)
            for sha in own[:20]:
                print(f"  + {gitutil.out(root, 'log', '-1', '--format=%h %s', sha)}", file=sys.stderr)
            lacking = sum(1 for ln in gitutil.out(root, "cherry", "HEAD", branch, timeout=60).splitlines() if ln.startswith("+"))
            print(f"(`git cherry HEAD {branch}` shows {lacking} '+' commit(s) in all; the others are pre-rewrite copies of lead "
                  f"commits.) `bt merge {name} --pick` cherry-picks the follower's own; `bt follow reset {name} --force` "
                  "discards the branch.", file=sys.stderr)
            return 1
        cp = gitutil.git(root, "merge", "--no-edit", branch, timeout=120)
    else:
        if gd is not None and (gd / "CHERRY_PICK_HEAD").exists():
            raise FollowError("a cherry-pick is already in progress here; finish or abort it first")
        if _pick(root, name, branch, _pick_list(root, branch, base)):
            return 1
        cp = None
    if cp is None or cp.returncode == 0:
        if cp is not None:
            print(cp.stdout.strip() or f"merged {branch}")
        moved = _move_to_read(root, role_notes(root, name))
        if moved:
            print(f"moved {moved} {name} note(s) to inbox/read/")
        dropped = _drop_sibling(root, name, branch, picked=pick, base=base)
        if dropped:
            print(dropped)
        return 0
    if gd is not None and (gd / "MERGE_HEAD").exists():
        files = gitutil.out(root, "diff", "--name-only", "--diff-filter=U").splitlines()
        gitutil.git(root, "merge", "--abort")
        print(f"merge of {branch} conflicts in: {', '.join(files) or '(unknown files)'}", file=sys.stderr)
        print("merge aborted; the worktree is as it was. Resolve in the follower worktree, or `bt follow reset "
              f"{name}`.", file=sys.stderr)
    else:
        print((cp.stderr or cp.stdout).strip(), file=sys.stderr)
    return 1


@_guard
def cmd_inbox(args) -> int:
    root = _root(args)
    action = args.inbox_cmd or "list"
    if action == "list":
        notes = inbox_notes(root)
        if not notes:
            print("inbox empty")
        for note in notes:
            print(f"{note.name}  {_first_line(note)}")
        return 0
    if action == "show":
        note = _find_note(root, args.name)
        if note is None:
            raise FollowError(f"no single inbox note matches '{args.name}'")
        print(note.read_text(encoding="utf-8", errors="replace"), end="")
        return 0
    if args.name == "all":
        notes = inbox_notes(root)
    else:
        note = _find_note(root, args.name)
        if note is None:
            raise FollowError(f"no single inbox note matches '{args.name}'")
        notes = [note]
    print(f"moved {_move_to_read(root, notes)} note(s) to inbox/read/")
    return 0


# --------------------------------------------------------------------------- CLI

def register(subparsers) -> None:
    follow = subparsers.add_parser("follow", help="followers: review/docs/optimize runs after lead commits")
    follow.set_defaults(func=lambda a: follow.print_help() or 0)
    sub = follow.add_subparsers(dest="follow_cmd")

    def add(name, func, help_text, with_worktree=True):
        parser = sub.add_parser(name, help=help_text)
        parser.set_defaults(func=func)
        if with_worktree:
            parser.add_argument("--worktree", help="lead worktree (default: the current one)")
        return parser

    on = add("on", cmd_on, "enable followers here and install the post-commit hook")
    on.add_argument("--roles", help=f"comma list (default {','.join(DEFAULT_ROLES)})")
    on.add_argument("--force", action="store_true",
                    help="install into a core.hooksPath that is tracked in the worktree or shared outside the repo")
    off = add("off", cmd_off, "disable followers here (the hook stays and does nothing)")
    off.add_argument("--clean", action="store_true",
                     help="also remove follower worktrees and branches that are clean and fully merged")
    now = add("now", cmd_now, "run followers for HEAD now, ignoring interval and size thresholds")
    now.add_argument("--roles", help="comma list (default: the enabled roles that are not on-demand)")
    now.add_argument("--brief", metavar="TEXT", help="owner brief appended to the follower's prompt (any role)")
    add("status", cmd_status, "show follower state")
    reset = add("reset", cmd_reset, "remove a follower's sibling worktree and branch")
    reset.add_argument("role")
    reset.add_argument("--force", action="store_true",
                       help="discard follower commits not in the lead branch (uncommitted work is kept under refs/baton/wip)")
    run = add("run", cmd_run, "runner (spawned by the post-commit hook)")
    run.add_argument("--sha", help="commit to follow (default: HEAD)")
    run.add_argument("--branch", help="lead branch the commit was made on (default: the worktree's current branch)")
    run.add_argument("--roles", help="comma list (default: the enabled roles that are not on-demand)")
    run.add_argument("--brief", metavar="TEXT", help="owner brief appended to the follower's prompt")

    merge = subparsers.add_parser("merge", help="merge a follower's branch into the current lead branch")
    merge.add_argument("role")
    merge.add_argument("--worktree", help="lead worktree (default: the current one)")
    merge.add_argument("--pick", action="store_true",
                       help="cherry-pick the follower's own commits instead of merging (the way past a rewritten lead history)")
    merge.set_defaults(func=cmd_merge)

    inbox = subparsers.add_parser("inbox", help="list, show or dismiss follower results")
    inbox.add_argument("--worktree", help="lead worktree (default: the current one)")
    inbox.set_defaults(func=cmd_inbox, inbox_cmd=None, name=None)
    isub = inbox.add_subparsers(dest="inbox_cmd")
    show = isub.add_parser("show", help="print one note")
    show.add_argument("name")
    show.set_defaults(func=cmd_inbox)
    done = isub.add_parser("done", help="move a note (or 'all') to inbox/read/")
    done.add_argument("name")
    done.set_defaults(func=cmd_inbox)
