"""Local-only files of a checkout: ignored and untracked paths, classified, never silently dropped.

Every ignored path falls in exactly one class:
  carried     a built-in carry rule (CLAUDE.md, .claude/**, .baton/*.md ...), a ``carry`` glob, or ``--include-secrets``
  rebuildable a directory or file that a restore command recreates (node_modules, .venv, *.pyc ...): summarised
  machine     ``.baton`` state bound to a session or machine (owner.json, inbox, runs, compute receipts ...)
  secret      a secret-looking name: listed by name and size, copied by hand
  left        matched by a ``leave`` glob of migrate.toml (the owner decided)
  undecided   anything else: carried by default below the size limits, otherwise named for the owner's decision
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from . import common as c
from . import secrets


def is_machine_rel(repo: str | None, parts: list[str]) -> bool:
    """``.baton`` session state, or Baton's own ``.state`` directory (queue store, registry: fresh on the new machine)."""
    if parts[0] == ".baton":
        return c.baton_rule(parts) == "machine"
    return repo == "baton" and parts[0] == ".state"


def bucket_path(kind: str, rel: str, why: str) -> str:
    """The directory a rebuildable or machine file is summarised under."""
    parts = rel.split("/")
    if kind == "rebuildable":
        if why.startswith("inside the Python virtual environment "):
            return why[len("inside the Python virtual environment "):]
        for i, p in enumerate(parts[:-1]):
            if c.is_rebuildable_dir(p):
                return "/".join(parts[:i + 1])
        return rel
    return "/".join(parts[:2]) if parts[0] == ".baton" and len(parts) > 2 else parts[0] if len(parts) > 1 else rel


def venv_above(checkout: Path | None, rel: str, cache: dict) -> str:
    """The shallowest directory above ``rel`` that is a Python virtual environment (it holds ``pyvenv.cfg``), or ''."""
    if checkout is None:
        return ""
    parts = rel.split("/")
    for i in range(1, len(parts)):
        d = "/".join(parts[:i])
        if d not in cache:
            cache[d] = c.is_venv_dir(checkout / d)
        if cache[d]:
            return d
    return ""


def classify_rel(ctx: c.Ctx, rel: str, repo: str | None, checkout: Path | None = None, cache: dict | None = None,
                 ignored: bool = True) -> tuple[str, str]:
    """(class, rule/why) of one repo-relative file path: carried, secret, left, machine, rebuildable or undecided.

    ``leave`` applies to ignored files only (``ignored`` False is an untracked file that git does not ignore: work, always
    carried). A secret-looking file that a ``leave`` rule matches stays out of the bundle and is marked as such in "left"."""
    name = rel.rsplit("/", 1)[-1]
    if ignored and c.match_any(ctx.leave_globs(repo), rel):
        return "left", "leave rule in migrate.toml" + (" (secret-looking)" if c.is_secret_name(name) else "")
    if not c.match_any(ctx.carry_globs(repo), rel):
        for p in rel.split("/")[:-1]:
            if c.is_rebuildable_dir(p):
                return "rebuildable", "inside rebuildable directory " + p
        venv = venv_above(checkout, rel, cache if cache is not None else {})
        if venv:
            return "rebuildable", "inside the Python virtual environment " + venv
    if c.is_secret_name(name):
        return ("carried", "secret-looking, --include-secrets") if ctx.include_secrets else ("secret", "secret-looking name")
    if c.match_any(ctx.carry_globs(repo), rel):
        return "carried", "carry glob"
    rule = c.carry_rule(rel)
    if rule:
        return "carried", rule
    parts = rel.split("/")
    if is_machine_rel(repo, parts):
        return "machine", "machine- and session-bound state (regenerated on the new machine)"
    for p in parts[:-1]:
        if c.is_rebuildable_dir(p):
            return "rebuildable", "inside rebuildable directory " + p
    if name.endswith(c.REBUILDABLE_SUFFIXES):
        return "rebuildable", "*" + name[name.rfind("."):]
    return "undecided", ""


def hold_reason(ctx: c.Ctx, repo: str | None, path, rel: str, size: int) -> str:
    """Why a file that would be carried is held back for copying by hand (its content, or a configuration file that cannot be
    redacted), or ''. Nothing is read for a repository in ``no_scan`` unless the file is a configuration file."""
    if ctx.include_secrets:
        return ""
    name = rel.rsplit("/", 1)[-1]
    kind = secrets.is_structured(name, rel)
    if secrets.is_opaque(name) and not kind:
        return ""                                        # compressed or media: nothing to read, nothing to open
    scan = repo not in ctx.cfg["no_scan"]
    if not scan and not kind and not name.lower().endswith(".pem"):
        return ""
    if size > secrets.MAX_CONTENT_BYTES and not kind:
        return ""
    if size > 16 * 1024 * 1024:
        return "too large to redact"
    try:
        with open(c.lp(path), "rb") as fh:
            data = fh.read()
    except OSError:
        return ""
    new, why = secrets.prepare(data, name, rel, content=scan)
    return why if new is None else ""


def _empty() -> dict:
    return {"carried": [], "rebuildable": [], "machine": [], "secrets": [], "left": [], "undecided": [], "undecided_groups": [],
            "undecided_bytes": 0, "untracked": [], "links": [], "nested": [], "worktree_dirs": 0}


def _summary(out: dict, key: str, rel: str, path, why: str) -> None:
    s = c.summarize_dir(path)
    for err in s["errors"]:
        out.setdefault("errors", []).append(err)
    out[key].append({"path": rel, "files": s["files"], "bytes": s["bytes"], "approx": s["approx"], "why": why})


def _is_worktrees_dir(parts: list[str]) -> bool:
    return len(parts) >= 2 and parts[-1] == "worktrees" and parts[-2] in c.AGENT_DIRS


def _record_file(ctx, out, repo, rel, path, group, *, untracked=False) -> None:
    try:
        size, mtime = c.stat_info(path)
    except OSError as exc:
        ctx.problem(path, f"cannot stat: {exc}")
        return
    root = Path(*path.parts[:len(path.parts) - len(rel.split("/"))]) if len(path.parts) > len(rel.split("/")) else None
    klass, why = classify_rel(ctx, rel, repo, root, out.setdefault("_vcache", {}), ignored=not untracked)
    if klass == "left" and "(secret-looking)" not in why and hold_reason(ctx, repo, path, rel, size):
        why += " (secret-looking)"                      # held by content: still named, with the mark, in "left"
    if klass in ("carried", "undecided"):
        held = hold_reason(ctx, repo, path, rel, size)
        if held:
            klass, why = "secret", held
    dirs = out.setdefault("_dirs", {})
    parts = rel.split("/")
    for i in range(1, len(parts)):
        t = dirs.setdefault("/".join(parts[:i]), [0, 0])
        t[0] += 1
        if klass == "undecided" and not untracked:
            t[1] += 1
    if klass == "carried":
        out["carried"].append({"rel": rel, "size": size, "rule": why, "untracked": untracked})
    elif klass == "secret":
        out["secrets"].append({"rel": rel, "size": size, "why": why})
    elif klass == "left":
        out["left"].append({"rel": rel, "size": size, "why": why})
    elif klass in ("machine", "rebuildable"):
        key = bucket_path(klass, rel, why)
        idx = out.setdefault("_bucket", {})
        item = idx.get((klass, key))
        if item is None:
            item = {"path": key, "files": 0, "bytes": 0, "approx": False, "why": why}
            idx[(klass, key)] = item
            out[klass].append(item)
        item["files"] += 1
        item["bytes"] += size
    elif untracked:
        out["untracked"].append({"rel": rel, "size": size})
    else:
        out["undecided"].append({"rel": rel, "size": size, "mtime": mtime})
        out["undecided_bytes"] += size
        if group is not None:
            group["undecided"] += 1
            group["bytes"] += size
            group["newest"] = max(group["newest"], mtime)
    if group is not None:
        group["total"] += 1


def _walk(ctx: c.Ctx, checkout: Path, repo: str | None, top_rel: str, out: dict) -> None:
    """Enumerate ``top_rel`` (a directory of the checkout) and classify every file below it."""
    group = None
    out.setdefault("_tops", []).append(top_rel.rstrip("/"))
    stack = [top_rel.rstrip("/")]
    seen = 0
    while stack:
        rel_dir = stack.pop()
        parts = rel_dir.split("/")
        base = checkout / rel_dir
        try:
            entries = list(os.scandir(c.lp(base)))
        except OSError as exc:
            ctx.problem(base, f"cannot read directory: {exc}")
            continue
        for entry in entries:
            rel = rel_dir + "/" + entry.name
            full = Path(entry.path.replace("\\\\?\\", "", 1) if entry.path.startswith("\\\\?\\") else entry.path)
            if c.is_link(full):
                out["links"].append({"rel": rel, "target": c.link_target(full), "kind": c.link_kind(full) or "symlink"})
                continue
            try:
                is_dir = entry.is_dir(follow_symlinks=False)
            except OSError as exc:
                ctx.problem(full, f"cannot stat: {exc}")
                continue
            if is_dir:
                sub = rel.split("/")
                if _is_worktrees_dir(sub):
                    out["worktree_dirs"] += 1
                elif c.is_rebuildable_dir(entry.name):
                    _summary(out, "rebuildable", rel, full, "rebuildable directory")
                elif c.is_venv_dir(full):
                    _summary(out, "rebuildable", rel, full, "Python virtual environment")
                elif entry.name in c.AGENT_CACHE_DIRS and any(p in c.AGENT_DIRS for p in sub[:-1]):
                    _summary(out, "rebuildable", rel, full, "agent cache directory")
                elif is_machine_rel(repo, sub):
                    _summary(out, "machine", rel, full, "machine- and session-bound .baton state")
                elif (full / ".git").exists():
                    out["nested"].append({"rel": rel})
                else:
                    stack.append(rel)
                continue
            seen += 1
            if seen > c.UNDECIDED_WALK_CAP:
                ctx.problem(base, f"more than {c.UNDECIDED_WALK_CAP} files under {top_rel}: add a carry/leave entry to migrate.toml")
                stack.clear()
                break
            _record_file(ctx, out, repo, rel, full, group)


def _group_undecided(out: dict) -> list[dict]:
    """One entry per undecided file, or per highest wholly-ignored directory in which everything is undecided."""
    dirs, tops = out.get("_dirs", {}), out.get("_tops", [])

    def eligible(d: str) -> bool:
        return any(d == t or d.startswith(t + "/") for t in tops) and dirs[d][0] == dirs[d][1]

    groups: dict[str, dict] = {}
    for f in out["undecided"]:
        parts = f["rel"].split("/")
        key, is_dir = f["rel"], False
        for i in range(1, len(parts)):
            d = "/".join(parts[:i])
            if d in dirs and eligible(d):
                key, is_dir = d + "/", True
                break
        g = groups.setdefault(key, {"path": key, "files": 0, "bytes": 0, "mtime": 0.0, "dir": is_dir})
        g["files"] += 1
        g["bytes"] += f["size"]
        g["mtime"] = max(g["mtime"], f["mtime"])
    return sorted(groups.values(), key=lambda g: g["path"])


def classify_checkout(ctx: c.Ctx, checkout: Path, repo: str | None) -> dict:
    """Classify every ignored file and every untracked, non-ignored file of ``checkout`` (read-only)."""
    out = _empty()
    out["_bucket"] = {}
    ignored = c.split_z(c.git_ok(ctx, checkout, "ls-files", "-o", "-i", "--exclude-standard", "--directory", "-z", timeout=300,
                                 what="ls-files ignored"))
    for entry in ignored:
        if entry.endswith("/"):
            rel = entry.rstrip("/")
            parts = rel.split("/")
            full = checkout / rel
            if _is_worktrees_dir(parts):
                out["worktree_dirs"] += 1
            elif c.is_link(full):
                out["links"].append({"rel": rel, "target": c.link_target(full), "kind": c.link_kind(full) or "symlink"})
            elif c.is_rebuildable_dir(parts[-1]):
                _summary(out, "rebuildable", rel, full, "rebuildable directory")
            elif c.is_venv_dir(full):
                _summary(out, "rebuildable", rel, full, "Python virtual environment")
            elif is_machine_rel(repo, parts):
                _summary(out, "machine", rel, full, "machine- and session-bound state")
            elif (full / ".git").exists():
                out["nested"].append({"rel": rel})
            else:
                _walk(ctx, checkout, repo, rel, out)
        else:
            full = checkout / entry
            if c.is_link(full):
                out["links"].append({"rel": entry, "target": c.link_target(full), "kind": c.link_kind(full) or "symlink"})
            else:
                _record_file(ctx, out, repo, entry, full, None)
    for entry in c.split_z(c.git_ok(ctx, checkout, "ls-files", "-o", "--exclude-standard", "-z", timeout=300, what="ls-files untracked")):
        if entry.endswith("/"):
            if _is_worktrees_dir(entry.rstrip("/").split("/")[:-1]):
                out["worktree_dirs"] += 1
            else:
                out["nested"].append({"rel": entry.rstrip("/")})
            continue
        full = checkout / entry
        if c.is_link(full):
            out["links"].append({"rel": entry, "target": c.link_target(full), "kind": c.link_kind(full) or "symlink"})
            continue
        _record_file(ctx, out, repo, entry, full, None, untracked=True)
    out["undecided_groups"] = _group_undecided(out)
    for k in ("_grouped", "_dirs", "_tops", "_vcache"):
        out.pop(k, None)
    return out


def plan_copy(ctx: c.Ctx, local: dict) -> tuple[list[dict], list[dict], list[dict]]:
    """(overlay files to copy, untracked files to copy, left behind) of a classified checkout, applying the size limits.

    A left-behind entry has ``decision`` True when the owner has to settle it in migrate.toml (an undecided file over a limit).
    """
    overlay, untracked, left = [], [], []
    for f in local["carried"]:
        if f["size"] > c.MAX_COPY_FILE and f["rule"] not in ("carry glob", "secret-looking, --include-secrets"):      # an explicit carry has no size limit
            left.append({"rel": f["rel"], "size": f["size"], "why": "larger than 500 MiB", "decision": False})
        else:
            overlay.append({"rel": f["rel"], "size": f["size"], "rule": f["rule"]})
    total_over = local["undecided_bytes"] >= c.UNDECIDED_TOTAL_LIMIT
    dir_groups = {g["path"] for g in local["undecided_groups"] if g["dir"]}
    group_files = {g["path"]: g["files"] for g in local["undecided_groups"] if g["dir"]}
    pending: dict[str, list] = {}
    for f in local["undecided"]:
        if total_over:
            why = f"ignored, undecided: the checkout's undecided files total {c.human(local['undecided_bytes'])} (limit {c.human(c.UNDECIDED_TOTAL_LIMIT)})"
        elif f["size"] >= c.UNDECIDED_FILE_LIMIT:
            why = f"ignored, undecided: {c.human(f['size'])} (limit {c.human(c.UNDECIDED_FILE_LIMIT)} per file)"
        else:
            overlay.append({"rel": f["rel"], "size": f["size"], "rule": "ignored, undecided (carried by default)"})
            continue
        parts = f["rel"].split("/")
        key = next((p + "/" for p in ("/".join(parts[:i]) for i in range(1, len(parts))) if p + "/" in dir_groups), f["rel"])
        pending.setdefault(key, []).append((f, why))
    for key, items in pending.items():
        if key in group_files and len(items) == group_files[key]:      # the whole directory is left: one line
            left.append({"rel": key, "size": sum(f["size"] for f, _ in items), "files": len(items), "why": items[0][1], "decision": True})
        else:
            left.extend({"rel": f["rel"], "size": f["size"], "files": 1, "why": why, "decision": True} for f, why in items)
    for f in local["left"]:
        left.append({"rel": f["rel"], "size": f["size"], "why": f["why"], "decision": False})
    for f in local["untracked"]:
        if f["size"] > c.MAX_COPY_FILE:
            left.append({"rel": f["rel"], "size": f["size"], "why": "untracked, larger than 500 MiB", "decision": False})
        else:
            untracked.append({"rel": f["rel"], "size": f["size"]})
    return overlay, untracked, left


def _is_dir(ctx: c.Ctx, entry, full) -> bool:
    try:
        return entry.is_dir(follow_symlinks=False)
    except OSError as exc:
        ctx.problem(full, f"cannot stat: {exc}")
        return False


def tree_plan(ctx: c.Ctx, base: Path, repo: str | None = None, prefix: str = "") -> dict:
    """Every file under a plain directory (a non-repo directory, an extra directory): all carried except rebuildable
    directories (summarised), secret-looking names (listed), links (recorded) and files over 500 MiB (unless a carry glob names
    them). ``leave`` and ``carry`` globs are matched against the path below the folder and against ``prefix`` + that path (the
    folder's own place below the code root or home), so ``/scratch/old/`` works as well as ``old/``. A directory that a ``leave``
    glob matches is not entered: an unreadable one needs no other settlement."""
    out = {"files": [], "rebuildable": [], "secrets": [], "links": [], "too_large": [], "left": []}
    stack = [""]
    checks: list[tuple[str, Path, int]] = []

    def matches(globs, rel: str) -> bool:
        return c.match_any(globs, rel) or bool(prefix and c.match_any(globs, prefix.strip("/") + "/" + rel))

    while stack:
        rel_dir = stack.pop()
        here = base / rel_dir if rel_dir else base
        try:
            entries = list(os.scandir(c.lp(here)))
        except OSError as exc:
            ctx.problem(here, f"cannot read directory: {exc}")
            continue
        for entry in entries:
            rel = (rel_dir + "/" if rel_dir else "") + entry.name
            full = here / entry.name
            if c.is_link(full):
                out["links"].append({"rel": rel, "target": c.link_target(full), "kind": c.link_kind(full) or "symlink"})
            elif _is_dir(ctx, entry, full):
                if matches(ctx.leave_globs(repo), rel + "/"):
                    out["left"].append({"rel": rel + "/", "why": "leave rule in migrate.toml (directory not entered)"})
                elif c.is_rebuildable_dir(entry.name) or c.is_venv_dir(full):
                    s = c.summarize_dir(full)
                    out["rebuildable"].append({"path": rel, "files": s["files"], "bytes": s["bytes"], "approx": s["approx"]})
                elif entry.name == ".git":
                    out["left"].append({"rel": rel, "why": "nested .git directory"})
                else:
                    stack.append(rel)
            else:
                try:
                    size = entry.stat(follow_symlinks=False).st_size
                except OSError as exc:
                    ctx.problem(full, f"cannot stat: {exc}")
                    continue
                if matches(ctx.leave_globs(repo), rel):
                    out["left"].append({"rel": rel, "why": "leave rule in migrate.toml"})
                elif c.is_secret_name(entry.name) and not ctx.include_secrets:
                    out["secrets"].append({"rel": rel, "size": size, "why": "secret-looking name"})
                elif size > c.MAX_COPY_FILE and not matches(ctx.carry_globs(repo), rel):
                    out["too_large"].append({"rel": rel, "size": size})
                else:
                    checks.append((rel, full, size))
    # the content check opens every file that would be carried: overlapped, because each open waits for a virus scan
    with ThreadPoolExecutor(max_workers=8) as pool:
        reasons = list(pool.map(lambda t: hold_reason(ctx, repo, t[1], t[0], t[2]), checks))
    for (rel, _full, size), held in zip(checks, reasons):
        if held:
            out["secrets"].append({"rel": rel, "size": size, "why": held})
        else:
            out["files"].append({"rel": rel, "size": size})
    out["files"].sort(key=lambda f: f["rel"])
    return out
