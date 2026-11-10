"""Worktree guardrails: ``bt wt new | retire | list``.

Parallel agents used to run ``git worktree add`` themselves, branch from each other's tips,
cherry-pick work back and never remove anything. Here the lead creates every worktree with
``bt wt new`` (capped per repo, stack depth limited, an Exit condition required) and ends it with
``bt wt retire`` (refuses live or unfinished trees unless archived). Lineage is recorded in
``STATE_DIR/worktrees.json``. Stdlib only; zipfile is imported on demand.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

from . import capsule, config, gitutil, harvest, paths, registry

LINEAGE_NAME = "worktrees.json"
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
SCRATCH = "_scratch"                 # throwaway checkouts (follower reviews) live in .claude/worktrees/_scratch
DEFAULT_CAP = 6
DEFAULT_FOLLOWER_CAP = 8
DEFAULT_DEPTH = 2
RECENT_SECONDS = 2 * 3600            # a tree changed this recently is not retired without --force
SPAWN_LOOKUP_S = 2.0
LIVE_LOOKUP_S = 4.0
SQUASH_SCAN_COMMITS = 200
MAX_ARCHIVE_FILE = 200 * 1024 * 1024
DEFAULT_ARCHIVE_MAX_MB = 2048
# Entries of a worktree's .baton that baton itself keeps; anything else there is agent scratch (bundles, zips,
# extracted checkouts) and is not archived unless --archive-all.
BATON_OWNED = ("now.md", "log.md", "compact.md", "owner.json", "archive", "inbox", "follow-state", "runs")
# .baton entries that are regenerable or transient: they never keep a follower sibling from being removed.
BATON_DISPOSABLE = ("now.md", "lead-now.md", "owner.json", "inbox", "runs", "follow-state")
# Directories that can be regenerated; archives skip them.
REGENERABLE = {"node_modules", ".venv", "venv", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
               ".lake", "target", "build", "dist", ".gradle", ".next", ".cache", ".tox", ".nox", ".idea"}
# Plain-text working notes that `bt wt retire` copies out before it deletes files the zip does not hold.
NOTES_SUFFIXES = frozenset(".md .txt .json .jsonl .py .lean .toml .yaml .yml .csv .tsv .sh .ps1 .patch .diff .sql .log".split())
NOTES_MAX_FILE = 5 * 1024 * 1024
NOTES_MAX_TOTAL = 500 * 1024 * 1024
NOTES_SKIP_DIRS = frozenset({".git", "cache"})
_EXIT_LINE = re.compile(r"^[ \t]*Exit:[ \t]*(\S.*?)[ \t]*$", re.MULTILINE)


class WtError(Exception):
    """A refusal or failure with a message for the user (exit code 2)."""


# --------------------------------------------------------------------------- settings


def settings() -> dict:
    cfg = config.routing_config().get("worktrees", {})
    cfg = cfg if isinstance(cfg, dict) else {}

    def number(key: str, default: int) -> int:
        value = cfg.get(key, default)
        return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else default

    return {"cap_per_repo": number("cap_per_repo", DEFAULT_CAP),
            "follower_cap_per_repo": number("follower_cap_per_repo", DEFAULT_FOLLOWER_CAP),
            "max_depth": number("max_depth", DEFAULT_DEPTH),
            "archive_max_mb": number("archive_max_mb", DEFAULT_ARCHIVE_MAX_MB),
            "read_only_remotes": read_only_remotes(cfg)}


def read_only_remotes(cfg: dict | None = None) -> set[str]:
    """Lower-cased names of repos whose origin refuses pushes (``[worktrees] read_only_remotes``, e.g. an
    archived GitHub repo): `bt backup` skips them unless named with --repo, and doctor does not ask for a push."""
    if cfg is None:
        cfg = config.routing_config().get("worktrees", {})
        cfg = cfg if isinstance(cfg, dict) else {}
    names = cfg.get("read_only_remotes", [])
    return {n.lower() for n in names if isinstance(n, str)} if isinstance(names, list) else set()


# --------------------------------------------------------------------------- lineage store


def lineage_path() -> Path:
    return config.STATE_DIR / LINEAGE_NAME


def load_lineage() -> dict:
    try:
        with open(lineage_path(), encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def save_lineage(data: dict) -> None:
    harvest.atomic_write(lineage_path(), json.dumps(data, indent=1, sort_keys=True))


def update_lineage(path: str | Path, **fields) -> None:
    data = load_lineage()
    data.setdefault(config.norm(path), {}).update(fields)
    save_lineage(data)


def live_records(main: Path, data: dict | None = None) -> list[tuple[str, dict]]:
    """(key, record) of the repo's lineage records that are not retired."""
    data = load_lineage() if data is None else data
    want = config.norm(main)
    return [(k, r) for k, r in data.items()
            if isinstance(r, dict) and not r.get("retired") and config.norm(r.get("repo", "")) == want]


# --------------------------------------------------------------------------- git helpers


def _git_text(main: Path, *args: str, timeout: float = 30) -> str:
    return gitutil.out(main, *args, timeout=timeout)


def has_ref(main: Path, ref: str) -> bool:
    return bool(_git_text(main, "rev-parse", "--verify", "--quiet", ref))


def has_unpushed(main: Path, branch: str) -> bool:
    """The local branch has commits that no remote-tracking ref contains."""
    return bool(_git_text(main, "rev-list", "-n1", f"refs/heads/{branch}", "--not", "--remotes"))


def resolve_repo(start: str | os.PathLike | None) -> Path | None:
    """Main checkout of the repo that contains ``start`` (default: the current directory)."""
    root = paths.find_worktree_root(start or os.getcwd())
    return paths.main_checkout(root) if root else None


def list_worktrees(main: Path) -> list[dict]:
    """``git worktree list --porcelain`` as dicts {path, head, branch, detached, prunable}; the main checkout is first."""
    text = gitutil.out(main, "worktree", "list", "--porcelain")
    out: list[dict] = []
    cur: dict = {}
    for line in text.splitlines() + [""]:
        if not line.strip():
            if cur.get("path"):
                out.append(cur)
            cur = {}
            continue
        key, _, value = line.partition(" ")
        if key == "worktree":
            cur = {"path": value, "head": "", "branch": "", "detached": False, "prunable": False}
        elif key == "HEAD":
            cur["head"] = value
        elif key == "branch":
            cur["branch"] = value[len("refs/heads/"):] if value.startswith("refs/heads/") else value
        elif key == "detached":
            cur["detached"] = True
        elif key == "prunable":
            cur["prunable"] = True
    return out


def is_scratch(path: str | Path) -> bool:
    return Path(path).parent.name == SCRATCH


def is_follower_sibling(path: str | Path, lineage: dict | None = None) -> bool:
    """A follower's writable sibling worktree (``<ws>--<role>``): its lineage record says ``created_by =
    follower:<role>``; without any record (a sibling made before lineage was kept) the directory name
    ``<x>--<follower name>`` decides."""
    lineage = load_lineage() if lineage is None else lineage
    rec = lineage.get(config.norm(path))
    if isinstance(rec, dict):
        return str(rec.get("created_by") or "").startswith("follower:")
    name = Path(path).name
    try:
        from . import follow
        return any(name.endswith(f"--{role}") for role in follow.follower_names())
    except Exception:  # noqa: BLE001 - classification is best effort
        return False


def is_follower_tree(path: str | Path, lineage: dict | None = None) -> bool:
    """Baton-managed, reused follower checkouts: scratch review trees and follower siblings."""
    return is_scratch(path) or is_follower_sibling(path, lineage)


def follower_worktrees(main: Path, entries: list[dict] | None = None) -> list[dict]:
    """Linked worktrees of the repo that are follower trees; they have their own limit and do not count toward the cap."""
    entries = list_worktrees(main)[1:] if entries is None else entries
    lineage = load_lineage()
    return [e for e in entries if not e["prunable"] and is_follower_tree(e["path"], lineage)]


def linked_worktrees(main: Path, entries: list[dict] | None = None) -> list[dict]:
    """Linked worktrees of the repo that count toward the cap (the main checkout and follower trees excluded)."""
    entries = list_worktrees(main)[1:] if entries is None else entries
    lineage = load_lineage()
    return [e for e in entries if not e["prunable"] and not is_follower_tree(e["path"], lineage)]


def worktrees_dir(main: Path) -> Path:
    return main / ".claude" / "worktrees"


def default_base(main: Path) -> str:
    for name in ("master", "main"):
        if has_ref(main, f"refs/remotes/origin/{name}"):
            return f"origin/{name}"
    return gitutil.default_branch(main)


def mainline_refs(main: Path) -> list[str]:
    return [r for r in ("refs/heads/master", "refs/heads/main", "refs/remotes/origin/master", "refs/remotes/origin/main")
            if has_ref(main, r)]


def is_ancestor(main: Path, a: str, b: str) -> bool:
    return gitutil.git(main, "merge-base", "--is-ancestor", a, b).returncode == 0


def on_mainline(main: Path, sha: str) -> bool:
    return any(is_ancestor(main, sha, ref) for ref in mainline_refs(main))


def branch_tip(main: Path, branch: str) -> str:
    return _git_text(main, "rev-parse", "--verify", "--quiet", f"refs/heads/{branch}^{{commit}}")


def base_depth(main: Path, sha: str, records: list[tuple[str, dict]], seen: frozenset = frozenset()) -> int:
    """0 when ``sha`` is on master/origin-master; else 1 + the depth of the lineage record whose branch tip
    contains it (the lowest such depth); 1 when no record knows it."""
    if on_mainline(main, sha):
        return 0
    best = None
    if len(seen) < 12:
        for key, rec in records:
            if key in seen:
                continue
            tip = branch_tip(main, rec.get("branch", ""))
            if tip and is_ancestor(main, sha, tip):
                depth = 1 + base_depth(main, rec.get("base_sha") or "", records, seen | {key})
                best = depth if best is None else min(best, depth)
    return 1 if best is None else best


# --------------------------------------------------------------------------- vendors: who is calling, who is live


def _code_user() -> str:
    return os.environ.get("CLAUDE_CODE_SESSION_ID") or os.environ.get("CODEX_THREAD_ID") or "user"


def _codex_homes() -> list[Path]:
    override = os.environ.get("CODEX_HOME")
    if override:
        return [config.expand(override)]
    return [config.account_dir("codex", name) for name in config.account_names("codex")]


def find_rollout(thread_id: str, homes: list[Path], deadline: float) -> Path | None:
    """Rollout file ``*<thread_id>.jsonl`` under ``<home>/sessions``, newest day first, giving up at ``deadline``."""
    from .sessions import _numeric_dirs
    suffix = f"{thread_id}.jsonl"
    for home in homes:
        for ydir in sorted(_numeric_dirs(home / "sessions", 4), reverse=True):
            for mdir in sorted(_numeric_dirs(ydir, 2), reverse=True):
                for ddir in sorted(_numeric_dirs(mdir, 2), reverse=True):
                    if time.monotonic() > deadline:
                        return None
                    try:
                        with os.scandir(ddir) as entries:
                            for entry in entries:
                                if entry.name.endswith(suffix) and entry.name.startswith("rollout-"):
                                    return Path(entry.path)
                    except OSError:
                        continue
    return None


def spawned_codex_agent() -> bool:
    """True when this process runs inside a Codex thread that another agent spawned (its rollout's
    ``session_meta.payload.source`` is a dict). False when it cannot be established within 2 s."""
    thread = os.environ.get("CODEX_THREAD_ID")
    if not thread:
        return False
    try:
        from .sessions import _rollout_meta
        path = find_rollout(thread, _codex_homes(), time.monotonic() + SPAWN_LOOKUP_S)
        meta = _rollout_meta(path) if path else None
    except Exception:  # noqa: BLE001 - detection is best effort
        return False
    return bool(meta) and isinstance(meta.get("source"), dict)


def _inside(path: str, parent: str) -> bool:
    return path == parent or path.startswith(parent.rstrip("\\/") + os.sep)


def live_cwds() -> list[str]:
    """Normalised working directories of running Claude sessions and of Codex threads holding their writer lock."""
    from .hook import _pid_alive
    from .sessions import _norm_cwd, _rollout_meta
    cwds: list[str] = []
    for account in config.account_names("claude"):
        sessions = config.account_dir("claude", account) / "sessions"
        try:
            files = list(sessions.glob("*.json"))
        except OSError:
            continue
        for f in files:
            try:
                rec = json.loads(f.read_text(encoding="utf-8"))
                pid = int(rec.get("pid") or f.stem)
            except (OSError, ValueError, AttributeError, TypeError):
                continue
            if isinstance(rec.get("cwd"), str) and _pid_alive(pid):
                cwds.append(_norm_cwd(rec["cwd"]))
    deadline = time.monotonic() + LIVE_LOOKUP_S
    for home in (config.account_dir("codex", name) for name in config.account_names("codex")):
        locks = home / "thread-writer-locks"
        try:
            files = list(locks.glob("*.lock"))
        except OSError:
            continue
        for lock in files:
            try:
                with open(lock, "rb") as fh:
                    fh.read(1)
                continue            # readable: nobody holds the byte-range lock
            except OSError:
                pass
            rollout = find_rollout(lock.stem, [home], deadline)
            meta = _rollout_meta(rollout) if rollout else None
            if meta and isinstance(meta.get("cwd"), str):
                cwds.append(_norm_cwd(meta["cwd"]))
    return cwds


def is_live(path: Path, cwds: list[str] | None = None) -> bool:
    """A session works in ``path``: owner.json names a running session, or a live Claude/Codex session's cwd is inside."""
    from .launch import owner_live
    if owner_live(Path(path)):
        return True
    want = config.norm(path)
    return any(_inside(c, want) for c in (live_cwds() if cwds is None else cwds))


def recent_change_age(path: Path, now: float | None = None) -> float | None:
    """Seconds since the newest uncommitted file or the HEAD commit changed; None when unknown."""
    now = time.time() if now is None else now
    stamps = []
    for rel in gitutil.dirty_files(Path(path))[:300]:
        try:
            stamps.append(os.stat(Path(path) / rel).st_mtime)
        except OSError:
            pass
    commit = gitutil.out(path, "log", "-1", "--format=%ct")
    if commit.isdigit():
        stamps.append(float(commit))
    return now - max(stamps) if stamps else None


# --------------------------------------------------------------------------- Exit conditions


def parse_exit(text: str) -> str:
    """Normalised exit spec ('pr', 'discard', 'merge:<branch>') or raise WtError."""
    spec = (text or "").strip()
    if spec in ("pr", "discard"):
        return spec
    if spec.startswith("merge:") and spec[6:].strip():
        return "merge:" + spec[6:].strip()
    raise WtError(f"--exit must be pr, discard or merge:<branch> (got {text!r})")


def exit_from_note(path: Path) -> str:
    try:
        text = (Path(path) / paths.BATON_DIRNAME / "now.md").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    match = _EXIT_LINE.search(text)
    return match.group(1) if match else ""


def merge_target_ref(main: Path, branch: str) -> str:
    for ref in (f"refs/heads/{branch}", f"refs/remotes/origin/{branch}", branch):
        if has_ref(main, ref):
            return ref
    return ""


def pending_patches(main: Path, target: str, tip: str) -> int:
    """Commits of ``tip`` whose patch is not in ``target`` (``git cherry`` '+' lines)."""
    return sum(1 for line in _git_text(main, "cherry", target, tip, timeout=60).splitlines() if line.startswith("+"))


def _patch_ids(main: Path, git_args: list[str]) -> list[str]:
    try:
        shown = subprocess.run(["git", "-C", str(main), *git_args], capture_output=True, timeout=60,
                               stdin=subprocess.DEVNULL, creationflags=config.CREATE_NO_WINDOW)
        if shown.returncode != 0:
            return []
        ids = subprocess.run(["git", "-C", str(main), "patch-id", "--stable"], input=shown.stdout, capture_output=True,
                             timeout=60, creationflags=config.CREATE_NO_WINDOW)
    except (OSError, subprocess.SubprocessError):
        return []
    return [line.split()[0] for line in ids.stdout.decode("utf-8", "replace").splitlines() if line.strip()]


def squash_landed(main: Path, tip: str, target: str) -> bool:
    """The cumulative change merge-base..tip equals one commit patch-wise on ``target`` (a squash merge)."""
    base = gitutil.merge_base(main, target, tip)
    if not base or base == tip:
        return False
    whole = _patch_ids(main, ["diff", base, tip])
    if not whole:
        return False
    landed = _patch_ids(main, ["log", "-p", "--no-merges", "--format=commit %H", "-n", str(SQUASH_SCAN_COMMITS), f"{base}..{target}"])
    return whole[0] in landed


def exit_state(main: Path, tip: str, spec: str) -> tuple[bool, str, int | None]:
    """(satisfied, explanation, patches still missing from the Exit target)."""
    if spec == "discard":
        return True, "exit is discard", None
    if not spec:
        return False, "no Exit recorded", None
    if spec.startswith("merge:"):
        name = spec[6:]
        target = merge_target_ref(main, name)
        if not target:
            return False, f"merge target {name} not found", None
        missing = pending_patches(main, target, tip)
        return missing == 0, (f"every patch is in {name}" if missing == 0 else f"{missing} patch(es) not yet in {name}"), missing
    if spec == "pr":
        target = default_base(main)
        missing = pending_patches(main, target, tip)
        if missing == 0:
            return True, f"contained in or patch-equivalent to {target}", 0
        if squash_landed(main, tip, target):
            return True, f"landed on {target} as a squash", 0
        return False, f"{missing} patch(es) not yet on {target}", missing
    return False, f"unknown Exit {spec!r}", None


# --------------------------------------------------------------------------- creating


def write_now(path: Path, ws: str, purpose: str, exit_spec: str, base: str, parent: str) -> None:
    lines = f"Purpose: {purpose}\nExit: {exit_spec}\nBase: {base}\nParent: {parent or '-'}\n"
    target = capsule.now_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(f"# Now — {ws}\n{lines}{capsule.BODY_TEMPLATE}")


def record_created(main: Path, path: Path, *, branch: str, base: str, base_sha: str, parent: str, exit_spec: str,
                   purpose: str = "", created_by: str | None = None, **extra) -> dict:
    rec = {"repo": str(main), "branch": branch, "base": base, "base_sha": base_sha, "parent": parent or None,
           "exit": exit_spec, "purpose": purpose, "created_by": created_by or _code_user(),
           "created_at": harvest.iso_now()}
    rec.update(extra)
    data = load_lineage()
    data[config.norm(path)] = rec     # replace: a recreated path must not inherit retired/archive from its old record
    save_lineage(data)
    return rec


def check_cap(main: Path, name: str) -> int:
    """Linked worktree count of the repo; raises WtError when creating ``name`` would exceed the cap."""
    cap = settings()["cap_per_repo"]
    count = len(linked_worktrees(main))
    if count >= cap:
        raise WtError(f"{main.name} has {count} linked worktrees (cap {cap}); retire one first "
                      f"(`bt wt list`, `bt wt retire <name>`) or pass --force REASON to exceed the cap")
    return count


def cmd_new(args) -> int:
    name = args.name
    if not NAME_RE.fullmatch(name):
        raise WtError(f"invalid worktree name {name!r}: use letters, digits, '.', '_' or '-', starting with a letter or digit")
    spec = parse_exit(args.exit)
    if spawned_codex_agent():
        raise WtError("spawned agents must not create worktrees; ask the lead")
    main = resolve_repo(args.repo)
    if main is None:
        raise WtError(f"not inside a git repository: {args.repo or os.getcwd()}")
    caller_root = paths.find_worktree_root(os.getcwd())
    branch = args.branch or f"wt/{name}"
    if gitutil.git(main, "check-ref-format", "--branch", branch).returncode != 0:
        raise WtError(f"invalid branch name {branch!r}")
    path = worktrees_dir(main) / name
    registered = {config.norm(e["path"]) for e in list_worktrees(main)}
    if path.exists() or config.norm(path) in registered:
        raise WtError(f"worktree name collision: {path} already exists")
    if has_ref(main, f"refs/heads/{branch}"):
        raise WtError(f"branch {branch} already exists; pick another name or --branch")
    base = args.base or default_base(main)
    base_sha = _git_text(main, "rev-parse", "--verify", "--quiet", f"{base}^{{commit}}")
    if not base_sha:
        raise WtError(f"base {base} does not resolve to a commit in {main}")
    if spec.startswith("merge:") and not merge_target_ref(main, spec[6:]):
        raise WtError(f"merge target {spec[6:]} does not exist in {main}")

    cfg = settings()
    overridden = []
    count = len(linked_worktrees(main))
    if count >= cfg["cap_per_repo"]:
        overridden.append(f"cap {count}/{cfg['cap_per_repo']}")
    depth = base_depth(main, base_sha, live_records(main))
    if depth > cfg["max_depth"]:
        overridden.append(f"stack depth {depth} > {cfg['max_depth']}")
    if overridden:
        reason = (args.force or "").strip()
        if not reason:
            problems = "; ".join(overridden)
            hint = ("retire one first (`bt wt list`, `bt wt retire <name>`)" if overridden[0].startswith("cap")
                    else "base the work on origin/master or an existing line of work with a shallower stack")
            raise WtError(f"refused: {problems}. {hint}, or pass --force REASON")
    else:
        reason = ""

    cp = gitutil.git(main, "worktree", "add", "--no-track", "-b", branch, str(path), base, timeout=300)
    if cp.returncode != 0:
        raise WtError(f"git worktree add failed: {(cp.stderr or cp.stdout).strip()[:300]}")
    try:
        paths.ensure_excluded(path)
    except OSError:
        pass
    same_repo = caller_root is not None and config.norm(paths.main_checkout(caller_root)) == config.norm(main)
    parent = str(caller_root) if same_repo else ""
    write_now(path, name, args.purpose or "", spec, f"{base} @ {base_sha[:7]}", parent)
    extra = {"forced": reason, "forced_over": overridden} if reason else {}
    record_created(main, path, branch=branch, base=base, base_sha=base_sha, parent=parent, exit_spec=spec,
                   purpose=args.purpose or "", **extra)
    print(f"created {path}\n  branch {branch} from {base} @ {base_sha[:7]}; exit {spec}; "
          f"{count + 1}/{cfg['cap_per_repo']} linked worktrees; stack depth {depth}")
    if reason:
        print(f"  forced past: {'; '.join(overridden)} ({reason})")
    return 0


# --------------------------------------------------------------------------- archiving and retiring


def _disposable(rel: str) -> bool:
    """A porcelain path that never keeps a follower sibling: regenerable build dirs and baton's transient files."""
    parts = rel.rstrip("/").split("/")
    if any(p in REGENERABLE or p.endswith(".egg-info") or p.startswith(".test-build") for p in parts):
        return True
    return len(parts) >= 2 and parts[0] == paths.BATON_DIRNAME and parts[1] in BATON_DISPOSABLE


def removal_blockers(path: Path) -> list[str]:
    """What removing ``path`` with ``git worktree remove --force`` would silently destroy: uncommitted changes,
    untracked files and *ignored* files (``git status`` hides the last kind), minus baton's transient files in
    ``.baton`` and regenerable build directories. Empty list = safe to remove."""
    cp = gitutil.git(path, "status", "--porcelain", "--ignored", "-z", timeout=120)
    if cp.returncode != 0:
        return ["(git status failed)"]
    found: list[str] = []
    fields = iter(cp.stdout.split("\0"))
    for field in fields:
        if len(field) < 4:
            continue
        status, rel = field[:2], field[3:]
        if status[0] in "RC":
            next(fields, None)               # the rename's source path follows as its own field
        if status == "!!" or status == "??":
            if rel.endswith("/") and rel.rstrip("/") == paths.BATON_DIRNAME:
                try:                          # a wholly ignored/untracked .baton: look inside
                    inner = [f".baton/{e.name}" for e in os.scandir(Path(path) / paths.BATON_DIRNAME)]
                except OSError:
                    inner = [rel]
                found += [r for r in inner if not _disposable(r)]
                continue
        if not _disposable(rel):
            found.append(rel)
    return found


def _extended(path: Path) -> str:
    """``path`` as an absolute extended-length path (``\\\\?\\D:\\...``), which Windows accepts beyond MAX_PATH."""
    full = os.path.abspath(str(path))
    if os.name != "nt" or full.startswith(chr(92) * 2 + "?"):
        return full
    prefix = chr(92) * 2 + "?" + chr(92)
    if full.startswith(chr(92) * 2):                       # UNC share
        return prefix + "UNC" + chr(92) + full[2:]
    return prefix + full


def remove_worktree(main: Path, path: Path) -> str:
    """Remove a linked worktree: ``git worktree remove --force``, and when that fails (long paths, read-only
    files) a ``shutil.rmtree`` on the extended-length path that clears read-only bits, then ``git worktree
    prune``. Returns '' when the directory is gone, else a description of what is left; never claims success
    while the directory exists."""
    cp = gitutil.git(main, "worktree", "remove", "--force", str(path), timeout=300)
    problem = "" if cp.returncode == 0 else (cp.stderr or cp.stdout).strip()[:300]
    if Path(path).exists() or os.path.lexists(_extended(path)):
        errors: list[str] = []

        def clear_and_retry(func, target, exc):
            try:
                os.chmod(target, stat.S_IWRITE | stat.S_IREAD)
                func(target)
            except OSError as again:
                errors.append(f"{target}: {again}")

        if sys.version_info >= (3, 12):
            shutil.rmtree(_extended(path), onexc=lambda f, t, e: clear_and_retry(f, t, e))
        else:
            shutil.rmtree(_extended(path), onerror=lambda f, t, e: clear_and_retry(f, t, e))
        gitutil.git(main, "worktree", "prune")
        if os.path.lexists(_extended(path)):
            left = f"; {len(errors)} error(s), first: {errors[0]}" if errors else ""
            return f"{path} still exists after removal{left}" + (f" (git: {problem})" if problem else "")
    else:
        gitutil.git(main, "worktree", "prune")
    return ""


def archive_root() -> Path:
    override = os.environ.get("BATON_ARCHIVE_DIR")
    if override:
        return Path(override)
    return config.CODE_ROOT.parent / f"{config.CODE_ROOT.name}-archive" / "baton-retired"


def _skip_part(rel: str) -> bool:
    return any(p in REGENERABLE or p.endswith(".egg-info") or p.startswith(".test-build") for p in rel.split("/"))


def _split0(text: str) -> list[str]:
    return [p for p in text.split("\0") if p]


def _is_scratch(rel: str) -> bool:
    """A file of ``.baton`` that baton does not own: agent scratch, not zipped unless --archive-all."""
    parts = rel.split("/")
    return len(parts) > 1 and parts[0] == paths.BATON_DIRNAME and parts[1] not in BATON_OWNED


def local_files(path: Path, include_scratch: bool = False) -> tuple[list[str], list[str], list[dict]]:
    """(files to archive, regenerable directories skipped, agent scratch skipped): untracked plus ignored files,
    relative with '/'. Entries of ``.baton`` that baton does not own (``BATON_OWNED``) are agent scratch and are
    listed with their total sizes instead of archived, unless ``include_scratch``."""
    def run(*args: str) -> str:
        cp = gitutil.git(path, *args, timeout=300)
        return cp.stdout if cp.returncode == 0 else ""

    found: dict[str, None] = {}
    skipped: set[str] = set()
    for rel in _split0(run("ls-files", "-o", "--exclude-standard", "-z")):
        if _skip_part(rel):
            skipped.add(next(p for p in rel.split("/") if p in REGENERABLE or p.endswith(".egg-info") or p.startswith(".test-build")))
        else:
            found[rel] = None
    for rel in _split0(run("ls-files", "-o", "-i", "--exclude-standard", "--directory", "-z")):
        if not rel.endswith("/"):
            if _skip_part(rel):
                skipped.add(rel)
            else:
                found[rel] = None
            continue
        if _skip_part(rel.rstrip("/")):
            skipped.add(rel.rstrip("/"))
            continue
        for dirpath, dirnames, filenames in os.walk(Path(path) / rel.rstrip("/")):
            keep = []
            for d in dirnames:
                sub = os.path.relpath(Path(dirpath) / d, path).replace(os.sep, "/")
                if _skip_part(sub):
                    skipped.add(sub)
                else:
                    keep.append(d)
            dirnames[:] = keep
            for f in filenames:
                found[os.path.relpath(Path(dirpath) / f, path).replace(os.sep, "/")] = None
    scratch: dict[str, int] = {}
    if not include_scratch:
        for rel in list(found):
            if _is_scratch(rel):
                parts = rel.split("/")
                del found[rel]
                try:
                    size = os.stat(Path(path) / rel).st_size
                except OSError:
                    size = 0
                scratch[f"{parts[0]}/{parts[1]}"] = scratch.get(f"{parts[0]}/{parts[1]}", 0) + size
    listed = [{"path": k, "size": v} for k, v in sorted(scratch.items(), key=lambda kv: (-kv[1], kv[0]))]
    return sorted(found), sorted(skipped), listed


def _unique_ref(main: Path, ref: str) -> str:
    if not has_ref(main, ref):
        return ref
    return f"{ref}-{time.strftime('%Y%m%d%H%M%S')}"


def _mb(size: int) -> str:
    return f"{size / (1 << 20):.1f} MB"


def _notes_skip_dir(part: str) -> bool:
    return part in REGENERABLE or part in NOTES_SKIP_DIRS or part.endswith(".egg-info") or part.startswith(".test-build")


def collect_notes(path: Path, only_scratch: bool = False) -> dict:
    """Untracked and ignored plain-text notes that would be lost with the tree: ``{"files": {rel: size},
    "skipped": [{"path", "size", "reason"}], "skipped_dirs": [...], "other": {"count", "bytes"}}``.

    A note is a file with a suffix in ``NOTES_SUFFIXES`` of at most ``NOTES_MAX_FILE`` bytes outside regenerable
    directories, ``.git``, any directory named ``cache`` and nested repositories. ``.baton`` and ``.claude`` are
    included. ``only_scratch`` limits it to the ``.baton`` scratch a zip leaves out."""
    files, skipped_dirs, _ = local_files(path, True)
    nested: dict[str, bool] = {}

    def in_nested_repo(rel: str) -> bool:
        parts = rel.split("/")[:-1]
        for i in range(len(parts), 0, -1):
            d = "/".join(parts[:i])
            if d not in nested:
                nested[d] = os.path.lexists(Path(path) / d / ".git")
            if nested[d]:
                return True
        return False

    notes: dict[str, int] = {}
    skipped: list[dict] = []
    other = {"count": 0, "bytes": 0}
    for rel in files:
        if only_scratch and not _is_scratch(rel):
            continue
        parts = rel.split("/")
        if any(_notes_skip_dir(p) for p in parts[:-1]):
            reason = "regenerable directory"
        elif in_nested_repo(rel):
            reason = "inside a nested repository"
        else:
            reason = ""
        try:
            size = os.stat(Path(path) / rel).st_size
        except OSError as exc:
            if not reason and Path(rel).suffix.lower() in NOTES_SUFFIXES:
                skipped.append({"path": rel, "size": None, "reason": f"unreadable: {str(exc)[:100]}"})
            continue
        if reason or Path(rel).suffix.lower() not in NOTES_SUFFIXES:
            other["count"] += 1
            other["bytes"] += size
        elif size > NOTES_MAX_FILE:
            skipped.append({"path": rel, "size": size, "reason": f"over {NOTES_MAX_FILE >> 20} MB"})
        else:
            notes[rel] = size
    return {"files": notes, "skipped": skipped, "skipped_dirs": skipped_dirs, "other": other}


def check_notes_size(name: str, notes: dict) -> None:
    """Refuse (WtError) when the notes exceed ``NOTES_MAX_TOTAL``, naming the ten largest top-level contributors."""
    total = sum(notes["files"].values())
    if total <= NOTES_MAX_TOTAL:
        return
    tops: dict[str, int] = {}
    for rel, size in notes["files"].items():
        top = rel.split("/")[0]
        tops[top] = tops.get(top, 0) + size
    biggest = sorted(tops.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    raise WtError(f"{name}: {_mb(total)} of notes ({len(notes['files'])} files) to save before removal exceeds "
                  f"{_mb(NOTES_MAX_TOTAL)}. Largest top-level entries: " + "; ".join(f"{r} {_mb(z)}" for r, z in biggest)
                  + ". Pass --archive-all to zip everything, or --discard-notes to skip the notes copy deliberately")


def _free_name(dest: Path) -> Path:
    """``dest``, or ``dest`` with a numeric suffix before the extension when that name is taken."""
    if not os.path.lexists(dest):
        return dest
    n = 2
    while True:
        cand = dest.with_name(f"{dest.stem}.{n}{dest.suffix}")
        if not os.path.lexists(cand):
            return cand
        n += 1


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def save_notes(main: Path, path: Path, name: str, notes: dict) -> dict:
    """Copy the notes of ``collect_notes`` to ``<archive root>/<repo>__<name>-notes/<relpath>`` with a
    NOTES_MANIFEST.json (path, size, sha256 per file; skipped and unreadable files with the reason). An existing
    different file is never overwritten: the copy gets a numeric suffix. Unreadable files are listed, not fatal."""
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    dest_dir = archive_root() / f"{main.name}__{safe}-notes"
    dest_dir.mkdir(parents=True, exist_ok=True)
    copied: list[dict] = []
    unreadable: list[dict] = []
    for rel, size in sorted(notes["files"].items()):
        src = Path(path) / rel
        try:
            digest = _sha256(src)
            target = dest_dir / rel
            if target.is_file() and _sha256(target) == digest:
                copied.append({"path": rel, "size": size, "sha256": digest, "copy": str(target)})
                continue
            target = _free_name(target)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, target)
            copied.append({"path": rel, "size": size, "sha256": digest, "copy": str(target)})
        except OSError as exc:
            unreadable.append({"path": rel, "size": size, "reason": f"unreadable: {str(exc)[:100]}"})
    manifest = {"repo": str(main), "name": name, "path": str(path), "saved_at": harvest.iso_now(), "files": copied,
                "skipped": notes["skipped"] + unreadable, "skipped_dirs": notes["skipped_dirs"],
                "other_files_not_copied": notes["other"]}
    manifest_path = _free_name(dest_dir / "NOTES_MANIFEST.json")
    manifest_path.write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    return {"dir": str(dest_dir), "manifest": str(manifest_path), "files": len(copied),
            "bytes": sum(e["size"] for e in copied), "unreadable": len(unreadable)}


def archive_worktree(main: Path, path: Path, name: str, rec: dict, tip: str, spec: str, *,
                     archive_all: bool = False, force: bool = False) -> dict:
    """Zip untracked and ignored local files with a JSON manifest, and keep the commits and tracked changes
    under refs/baton/retired/<name> (+ -wip). Raises on failure so the caller does not remove the tree.

    Only baton-owned ``.baton`` entries are zipped (the rest is listed in ``skipped_scratch``) unless
    ``archive_all``. Above ``[worktrees] archive_max_mb`` of files to zip it refuses unless ``archive_all`` or
    ``force``. Files over ``MAX_ARCHIVE_FILE`` are moved, not dropped, into ``<archive>.too_large/<relpath>``."""
    import zipfile
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    head = gitutil.out(path, "rev-parse", "HEAD")
    if not head:
        raise WtError(f"cannot read HEAD of {path}")
    files, skipped, scratch = local_files(path, archive_all)
    sizes: dict[str, int] = {}
    unreadable: list[dict] = []
    for rel in files:
        try:
            sizes[rel] = os.stat(Path(path) / rel).st_size
        except OSError as exc:
            unreadable.append({"path": rel, "error": str(exc)[:120]})
    zipped = {rel: size for rel, size in sizes.items() if size <= MAX_ARCHIVE_FILE}
    total, cap = sum(zipped.values()), settings()["archive_max_mb"] * (1 << 20)
    if cap and total > cap and not (archive_all or force):
        biggest = sorted(zipped.items(), key=lambda kv: -kv[1])[:5]
        raise WtError(f"{name}: {_mb(total)} of local files to archive exceeds archive_max_mb "
                      f"({settings()['archive_max_mb']} MB). Largest: " + "; ".join(f"{r} {_mb(z)}" for r, z in biggest)
                      + ". Pass --archive-all (or --force) to archive it anyway, or remove the scratch first")
    ref = _unique_ref(main, f"refs/baton/retired/{safe}")
    if gitutil.git(main, "update-ref", ref, head).returncode != 0:
        raise WtError(f"could not write {ref}")
    refs = [ref]
    wip = gitutil.out(path, "stash", "create")
    if wip:
        if gitutil.git(main, "update-ref", f"{ref}-wip", wip).returncode != 0:
            raise WtError(f"could not write {ref}-wip")
        refs.append(f"{ref}-wip")
    dest_dir = archive_root()
    dest_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dest_dir / f"{main.name}__{safe}.zip"
    if zip_path.exists():
        zip_path = dest_dir / f"{main.name}__{safe}-{time.strftime('%Y%m%d%H%M%S')}.zip"
    too_large_dir = zip_path.with_name(zip_path.stem + ".too_large")
    manifest = {"repo": str(main), "name": name, "path": str(path), "branch": rec.get("branch") or tip, "head": head,
                "exit": spec, "lineage": rec, "archived_at": harvest.iso_now(), "refs": refs,
                "tracked_changes": gitutil.dirty_files(path)[:500], "skipped_dirs": skipped,
                "skipped_scratch": scratch, "files": [], "unreadable": unreadable, "too_large": []}
    stuck: list[str] = []
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        for rel, size in sizes.items():
            src = Path(path) / rel
            if size > MAX_ARCHIVE_FILE:
                entry = {"path": rel, "size": size, "moved_to": None}
                target = too_large_dir / rel
                try:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(src), str(target))
                    entry["moved_to"] = str(target)
                except OSError as exc:
                    entry["error"] = str(exc)[:120]
                    stuck.append(rel)
                manifest["too_large"].append(entry)
                continue
            try:
                zf.write(src, arcname=f"files/{rel}")
                manifest["files"].append({"path": rel, "size": size})
            except OSError as exc:
                manifest["unreadable"].append({"path": rel, "error": str(exc)[:120]})
        zf.writestr("MANIFEST.json", json.dumps(manifest, indent=1))
    manifest_path = zip_path.with_suffix(".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    if stuck:
        raise WtError(f"could not move {len(stuck)} file(s) over {MAX_ARCHIVE_FILE >> 20} MB out of {path} "
                      f"(first: {stuck[0]}); they are still there, so the worktree was not removed")
    return {"zip": str(zip_path), "manifest": str(manifest_path), "refs": refs, "files": len(manifest["files"]),
            "scratch": scratch, "unreadable": len(manifest["unreadable"]),
            "too_large": [e for e in manifest["too_large"] if e["moved_to"]],
            "too_large_dir": str(too_large_dir) if manifest["too_large"] else ""}


def find_target(main: Path, arg: str) -> dict | None:
    """The linked worktree named by ``arg`` (a name under .claude/worktrees, a path or a branch)."""
    entries = list_worktrees(main)[1:]
    candidates = [config.norm(worktrees_dir(main) / arg), config.norm(arg)]
    for e in entries:
        if config.norm(e["path"]) in candidates:
            return e
    for e in entries:
        if Path(e["path"]).name == arg or e["branch"] == arg:
            return e
    return None


def lineage_children(main: Path, path: Path) -> list[dict]:
    """Registered worktrees whose live lineage record names ``path`` as parent."""
    key = config.norm(path)
    kids = {k for k, r in live_records(main) if r.get("parent") and config.norm(r["parent"]) == key and k != key}
    return [e for e in list_worktrees(main)[1:] if config.norm(e["path"]) in kids]


def cmd_retire(args) -> int:
    target_arg = args.target
    looks_like_path = any(c in target_arg for c in "/\\") or Path(target_arg).is_absolute()
    main = resolve_repo(args.repo or (target_arg if looks_like_path and Path(target_arg).exists() else None))
    if main is None:
        raise WtError(f"not inside a git repository: {args.repo or os.getcwd()}")
    entry = find_target(main, target_arg)
    if entry is None:
        orphan = worktrees_dir(main) / target_arg
        if orphan.exists():
            raise WtError(f"{orphan} is not a worktree registered with git (an orphan directory); check it and delete it by hand")
        raise WtError(f"no linked worktree {target_arg!r} in {main}")
    path = Path(entry["path"])
    name = path.name
    here = config.norm(os.getcwd())
    if _inside(here, config.norm(path)):
        raise WtError(f"the current directory is inside {path}; run `bt wt retire` from elsewhere")
    if is_live(path):
        raise WtError(f"{name} is live (a Claude or Codex session is working in it); end that session first")
    from . import compute_pickup
    pending_compute = [r for r in compute_pickup.collect(path) if r['state'] != 'reviewed']
    if pending_compute and not getattr(args, 'discard_compute', False):
        raise WtError(f"{name} has {len(pending_compute)} pending or unreviewed deferred compute result(s). "
                      "Use bt compute --refresh in that worktree and review the results first. "
                      "--discard-compute explicitly abandons automatic pickup; it does not cancel remote jobs")
    children = lineage_children(main, path)
    if children and not args.force:
        raise WtError(f"{name} still has {len(children)} lineage child worktree(s): "
                      + ", ".join(Path(e["path"]).name for e in children[:8])
                      + f". Retire or merge them first, or pass --force to retire {name} anyway")
    age = recent_change_age(path)
    if age is not None and age < RECENT_SECONDS and not args.force:
        raise WtError(f"{name} changed {int(age // 60)} min ago (under 2 h); wait or pass --force")

    rec = load_lineage().get(config.norm(path), {})
    spec = rec.get("exit") or exit_from_note(path)
    tip = entry["branch"] or entry["head"]
    satisfied, why, _ = exit_state(main, tip, spec)
    dirty = gitutil.dirty_files(path)
    reasons = []
    if not satisfied:
        reasons.append(f"Exit {spec or '(none)'} not satisfied: {why}")
    if dirty:
        reasons.append(f"{len(dirty)} uncommitted file(s)")
    archive_all = bool(getattr(args, "archive_all", False))
    if reasons and not (args.archive or archive_all or args.force):
        raise WtError(f"{name}: {'; '.join(reasons)}. Pass --archive to keep a zip of local files and refs/baton/retired/{name}, "
                      "or --force to remove it anyway (plain-text notes are copied out first; the branch is kept either way)")
    notes = None
    if not getattr(args, "discard_notes", False):
        # Files the zip will not hold are deleted with the tree: save their plain-text notes first. Refuses
        # (before anything is archived or deleted) when the notes are too large.
        if not (args.archive or archive_all):
            notes = collect_notes(path)
        elif not archive_all:
            notes = collect_notes(path, only_scratch=True)
        if notes is not None:
            check_notes_size(name, notes)
    archived = None
    if args.archive or archive_all:
        archived = archive_worktree(main, path, name, rec, tip, spec, archive_all=archive_all, force=bool(args.force))
        print(f"archived {archived['files']} file(s) to {archived['zip']}; refs {', '.join(archived['refs'])}")
        if archived["scratch"]:
            total = sum(e["size"] for e in archived["scratch"])
            print(f"  not archived: {len(archived['scratch'])} agent scratch entr(ies) in .baton ({_mb(total)}), listed in the "
                  "manifest as skipped_scratch: " + ", ".join(f"{e['path']} {_mb(e['size'])}" for e in archived["scratch"][:5])
                  + "; --archive-all includes them")
        if archived["too_large"]:
            print(f"  moved {len(archived['too_large'])} file(s) over {MAX_ARCHIVE_FILE >> 20} MB to {archived['too_large_dir']}")
        if archived["unreadable"]:
            print(f"  warning: {archived['unreadable']} file(s) could not be read and are not in the archive (see the manifest)")
    if notes is not None and notes["files"]:
        saved = save_notes(main, path, name, notes)
        print(f"saved {saved['files']} note file(s) ({_mb(saved['bytes'])}) to {saved['dir']} (manifest {Path(saved['manifest']).name})")
        if saved["unreadable"]:
            print(f"  warning: {saved['unreadable']} note file(s) could not be read; they are listed in the manifest")
    left = remove_worktree(main, path)
    if left:
        raise WtError(f"{name} was not fully removed: {left}")
    key = config.norm(path)
    if key in load_lineage():
        update_lineage(path, retired=True, retired_at=harvest.iso_now(), archive=archived["zip"] if archived else None)
    try:
        registry.remove(path)
    except registry.RegistryUnavailable:
        pass
    registry.startup_forget(path)
    branch = entry["branch"]
    print(f"retired {path}" + (f"; branch {branch} kept" if branch else ""))
    return 0


def retire_quietly(main: Path, path: Path) -> bool:
    """Remove a follower sibling worktree (branch kept) and mark its lineage record retired. Refuses (False)
    while uncommitted, untracked or ignored files other than regenerable ones are in it."""
    if removal_blockers(path):
        return False
    if remove_worktree(main, path):
        return False
    if config.norm(path) in load_lineage():
        update_lineage(path, retired=True, retired_at=harvest.iso_now())
    try:
        registry.remove(path)
    except registry.RegistryUnavailable:
        pass
    return True


# --------------------------------------------------------------------------- listing


def known_repos() -> list[Path]:
    """Main checkouts of repos with registered worktrees, lineage records or a .claude/worktrees directory under the code root."""
    found: dict[str, Path] = {}
    candidates: list[str] = []
    try:
        candidates += [str(i.get("path") or k) for k, i in registry.load().get("worktrees", {}).items() if isinstance(i, dict)]
    except registry.RegistryUnavailable:
        pass
    candidates += [r.get("repo", "") for r in load_lineage().values() if isinstance(r, dict)]
    try:    # repos under the code root that keep worktrees even when no session registered one
        candidates += [str(c) for c in config.CODE_ROOT.iterdir()
                       if (c / ".git").is_dir() and (c / ".claude" / "worktrees").is_dir()]
    except OSError:
        pass
    for text in candidates:
        if not text:
            continue
        root = paths.find_worktree_root(text) if Path(text).exists() else None
        if root is None:
            continue
        main = paths.main_checkout(root)
        if (main / ".git").is_dir():
            found.setdefault(config.norm(main), main)
    return sorted(found.values(), key=lambda p: str(p).lower())


def _age(created: str | None, now: float) -> str:
    stamp = harvest.parse_iso(created) if created else None
    if stamp is None:
        return "-"
    seconds = max(0.0, now - stamp)
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    if seconds < 2 * 86400:
        return f"{seconds / 3600:.0f}h"
    return f"{seconds / 86400:.0f}d"


def tree_lines(main: Path, cwds: list[str], now: float) -> list[str]:
    entries = linked_worktrees(main)
    cfg = settings()
    lineage = load_lineage()
    nodes = {config.norm(e["path"]): e for e in entries}
    children: dict[str, list[str]] = {}
    roots = []
    for key, entry in nodes.items():
        parent = lineage.get(key, {}).get("parent")
        pkey = config.norm(parent) if parent else None
        (children.setdefault(pkey, []) if pkey in nodes else roots).append(key)
    # the lines below are built recursively; the lists above hold keys in registration order
    followers = len(follower_worktrees(main))
    lines = [f"{main}  ({len(entries)} linked worktree(s), cap {cfg['cap_per_repo']}"
             + (f"; {followers} follower worktree(s), limit {cfg['follower_cap_per_repo']}" if followers else "") + ")"]

    def describe(key: str) -> str:
        entry, rec = nodes[key], lineage.get(key, {})
        spec = rec.get("exit") or exit_from_note(entry["path"])
        parts = [Path(entry["path"]).name, entry["branch"] or "(detached)", f"exit={spec or '?'}", f"age={_age(rec.get('created_at'), now)}"]
        if is_live(Path(entry["path"]), cwds):
            parts.append("LIVE")
        if spec.startswith("merge:"):
            target = merge_target_ref(main, spec[6:])
            if target:
                parts.append(f"{pending_patches(main, target, entry['branch'] or entry['head'])} patch(es) not in {spec[6:]}")
        return "  ".join(parts)

    def walk(key: str, depth: int, seen: set) -> None:
        lines.append("  " * (depth + 1) + ("- " if depth else "") + describe(key))
        for child in children.get(key, []):
            if child not in seen:
                walk(child, depth + 1, seen | {child})

    for key in roots:
        walk(key, 0, {key})
    return lines


def cmd_list(args) -> int:
    if args.repo:
        repos = [resolve_repo(args.repo)]
    else:
        here = resolve_repo(None)
        repos = [here] if here else known_repos()
    repos = [r for r in repos if r]
    if not repos:
        print("no repositories known; pass --repo PATH")
        return 0
    cwds = live_cwds()
    now = time.time()
    for main in repos:
        print("\n".join(tree_lines(main, cwds, now)))
    return 0


# --------------------------------------------------------------------------- CLI


def _guard(func):
    def wrapper(args):
        try:
            return func(args)
        except WtError as exc:
            print(f"bt wt: {exc}", file=sys.stderr)
            return 2
    return wrapper


def register(sub) -> None:
    wt = sub.add_parser("wt", help="worktree guardrails: new (capped, with an Exit), retire, list")
    wt.set_defaults(func=lambda a: wt.print_help() or 0)
    wsub = wt.add_subparsers(dest="wt_cmd")

    new = wsub.add_parser("new", help="create a worktree under <repo>/.claude/worktrees (the only sanctioned creator)")
    new.add_argument("name")
    new.add_argument("--repo", help="repository (default: the current worktree's)")
    new.add_argument("--base", help="start point (default: origin/master if present, else the default branch)")
    new.add_argument("--exit", required=True, help="how the work ends: pr, discard or merge:<branch>")
    new.add_argument("--purpose", help="one line for .baton/now.md")
    new.add_argument("--branch", help="branch name (default: wt/<name>)")
    new.add_argument("--force", metavar="REASON", help="exceed the worktree cap or stack depth; the reason is recorded")
    new.set_defaults(func=_guard(cmd_new))

    retire = wsub.add_parser("retire", help="remove a finished worktree (branch kept); refuses live or unfinished ones")
    retire.add_argument("target", help="worktree name or path")
    retire.add_argument("--repo")
    retire.add_argument("--archive", action="store_true",
                        help="zip untracked and ignored local files and write refs/baton/retired/<name> before removing")
    retire.add_argument("--archive-all", action="store_true",
                        help="like --archive, and also zip agent scratch in .baton and exceed archive_max_mb")
    retire.add_argument("--discard-notes", action="store_true",
                        help="do not copy plain-text notes (.md .txt .json .py ... up to 5 MB each, .baton and .claude included) "
                             "to <archive>/<repo>__<name>-notes/ before deleting files the zip does not hold; "
                             "without it retire refuses above 500 MB of notes")
    retire.add_argument("--discard-compute", action="store_true",
                        help="abandon pending/unreviewed compute pickup without cancelling remote jobs")
    retire.add_argument("--force", action="store_true",
                        help="also remove a recently changed tree, one whose Exit is unsatisfied, or one with lineage children "
                             "(archives nothing: plain-text notes are still copied out unless --discard-notes)")
    retire.set_defaults(func=_guard(cmd_retire))

    lst = wsub.add_parser("list", help="lineage tree of linked worktrees per repo")
    lst.add_argument("--repo")
    lst.set_defaults(func=_guard(cmd_list))
