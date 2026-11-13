"""Survey of the repositories under the code root (read-only): refs, worktrees, local-only files, toolchains."""
from __future__ import annotations

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .. import capsule, config, gitutil, paths, worktrees
from . import common as c
from . import local as loc
from . import scan as scanmod
from . import secrets

REFLOG_TIMEOUT = 20


def get_live_cwds() -> list[str]:
    try:
        return worktrees.live_cwds()
    except Exception:  # noqa: BLE001 - process listing is best effort
        return []


# ------------------------------------------------------------------ the root

def list_root(ctx: c.Ctx) -> tuple[list[Path], list[dict], list[dict], list[dict]]:
    """(repository main checkouts, aliases, non-repository directories, root files) directly under the root.

    A directory that is a junction or symlink is an alias, never a second repository."""
    repos, aliases, nonrepo, files = [], [], [], []
    root_real = c.real(ctx.root)
    try:
        entries = sorted(os.scandir(c.lp(ctx.root)), key=lambda e: e.name.lower())
    except OSError as exc:
        ctx.problem(ctx.root, f"cannot list the root: {exc}")
        return repos, aliases, nonrepo, files
    for entry in entries:
        path = ctx.root / entry.name
        if c.is_link(path):
            target = c.link_target(path)
            treal = c.real(path)
            under = treal.startswith(root_real + os.sep) and os.path.dirname(treal) == root_real
            aliases.append({"name": entry.name, "target": target, "kind": c.link_kind(path) or "symlink",
                            "target_in_root": os.path.basename(treal) if under else "", "is_dir": os.path.isdir(path)})
            continue
        try:
            is_dir = entry.is_dir(follow_symlinks=False)
        except OSError as exc:
            ctx.problem(path, f"cannot stat: {exc}")
            continue
        if not is_dir:
            try:
                files.append({"name": entry.name, "size": entry.stat(follow_symlinks=False).st_size})
            except OSError as exc:
                ctx.problem(path, f"cannot stat: {exc}")
            continue
        if (path / ".git").is_dir():
            repos.append(path)
        elif not entry.name.startswith("."):
            leave = ctx.leave_globs(None)
            s = c.summarize_dir(path, 100000, skip=(lambda rel: c.match_any(leave, rel) or c.match_any(leave, entry.name + "/" + rel)) if leave else None)
            nonrepo.append({"name": entry.name, "files": s["files"], "bytes": s["bytes"], "approx": s["approx"],
                            "git_file": (path / ".git").exists()})
    return repos, aliases, nonrepo, files


# ------------------------------------------------------------------ helpers

def collisions(names: list[str]) -> list[list[str]]:
    """Groups of names that differ only by case."""
    seen: dict[str, list[str]] = {}
    for n in sorted(set(names)):
        seen.setdefault(n.casefold(), []).append(n)
    return [g for g in seen.values() if len(g) > 1]


def ref_collisions(refs: list[str]) -> list[list[str]]:
    """Ref names equal up to case, or where one name is a path prefix of another in a different case."""
    out = [g for g in collisions(refs)]
    folded = {}
    for r in set(refs):
        folded.setdefault(r.casefold(), set()).add(r)
    for r in sorted(set(refs)):
        parts = r.split("/")
        for i in range(2, len(parts)):
            prefix = "/".join(parts[:i])
            for q in folded.get(prefix.casefold(), ()):
                if q != prefix:
                    out.append([q, r])
    return out


def parse_note(text: str) -> tuple[dict, bool]:
    """(header and key fields of a now.md, whether Objective/Purpose and Next both have content), by Baton's own contract
    parser (capsule.fields)."""
    lines = text.splitlines()
    by: dict[str, list] = {}
    for span in capsule.fields(lines):
        by.setdefault(span[0], []).append(span)
    fields = {}
    for key in ("purpose", "exit", "base", "parent", "objective", "next"):
        for span in by.get(key, []):
            value = span[3] or next((ln.strip() for ln in lines[span[1] + 1:span[2]] if ln.strip() and not ln.strip().startswith("<!--")), "")
            if value:
                fields[key.capitalize()] = value[:300]
                break
    has = lambda keys: any(capsule._has_content(lines, s) for k in keys for s in by.get(k, []))  # noqa: E731
    return fields, has(("objective", "purpose")) and has(("next",))


def note_info(checkout: Path) -> dict:
    path = checkout / ".baton" / "now.md"
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        mtime = path.stat().st_mtime
    except OSError:
        return {"exists": False}
    fields, complete = parse_note(text)
    commit = gitutil.out(checkout, "log", "-1", "--format=%ct")
    return {"exists": True, "fields": fields, "mtime": mtime, "stale": bool(commit.isdigit() and mtime < int(commit)),
            "complete": complete}


def _tracked(ctx: c.Ctx, checkout: Path) -> tuple[list[str], list[dict]]:
    text = c.git_ok(ctx, checkout, "ls-files", "-s", "-z", timeout=120, what="ls-files")
    tracked, subs = [], []
    for item in c.split_z(text):
        meta, _, path = item.partition("\t")
        tracked.append(path)
        if meta.startswith("160000"):
            subs.append({"path": path, "sha": meta.split()[1]})
    return tracked, subs


def _count_objects(path: Path) -> int:
    text = gitutil.out(path, "count-objects", "-v")
    total = 0
    for line in text.splitlines():
        k, _, v = line.partition(":")
        if k.strip() in ("size", "size-pack") and v.strip().isdigit():
            total += int(v.strip()) * 1024
    return total


def _unique_commits(ctx, path: Path, has_remote_refs: bool) -> set[str] | None:
    if not has_remote_refs:
        return None   # everything is unique
    cp = c.git(path, "rev-list", "--branches", "--not", "--remotes", timeout=90)
    if cp.returncode != 0:
        ctx.problem(path, f"git rev-list --branches --not --remotes failed: {cp.stderr.strip()[:200]}")
        return set()
    return set(cp.stdout.split())


def _branch_rows(refs: list[dict], unique: set[str] | None, has_remote_refs: bool) -> list[dict]:
    rows = []
    for r in refs:
        if not r["ref"].startswith("refs/heads/"):
            continue
        up, track = r["upstream"], r["track"].strip("[]")
        if not up or not up.startswith("refs/remotes/"):
            status, ahead, behind = "no upstream", 0, 0
        else:
            ahead = int(m.group(1)) if (m := re.search(r"ahead (\d+)", track)) else 0
            behind = int(m.group(1)) if (m := re.search(r"behind (\d+)", track)) else 0
            if "gone" in track:
                status = "upstream gone"
            elif ahead:
                status = f"ahead {ahead}" + (f", behind {behind}" if behind else "")
            elif behind:
                status = f"behind {behind}"
            else:
                status = "in sync"
        lost = status == "no upstream" or status.startswith("ahead") or status == "upstream gone"
        rows.append({"name": r["ref"][len("refs/heads/"):], "sha": r["sha"], "upstream": up.replace("refs/remotes/", "") if up else "",
                     "status": status, "date": r["date"], "lost": lost,
                     "unique": (r["sha"] in unique) if unique is not None else True})
    return rows


def _stashes(ctx, path: Path) -> list[dict]:
    cp = c.git(path, "log", "-g", "--format=%H%x09%P%x09%gs", "refs/stash")
    out = []
    if cp.returncode != 0:
        return out
    for n, line in enumerate(cp.stdout.splitlines()):
        f = line.split("\t")
        if len(f) < 3:
            continue
        parents = f[1].split()
        out.append({"n": n, "sha": f[0], "base": parents[0] if parents else "", "has_untracked": len(parents) >= 3, "message": f[2]})
    return out


def submodule_state(path: Path, sub: dict) -> dict:
    """Whether what a submodule holds exists anywhere but here. Its contents are not carried (``git submodule update --init``
    fetches them from its remote), so a commit no remote-tracking branch contains, a branch with commits no remote has, or an
    uncommitted change in it is lost by the move."""
    sp = path / sub["path"]
    if not (sp / ".git").exists():
        return {"state": "not initialised", "lost": False, "why": ""}
    if c.git(sp, "cat-file", "-e", sub["sha"] + "^{commit}").returncode != 0:
        return {"state": "recorded commit not present", "lost": False, "why": ""}
    why = []
    if not c.git(sp, "branch", "-r", "--contains", sub["sha"]).stdout.strip():
        why.append(f"its commit {sub['sha'][:10]} is on no remote-tracking branch")
    unpushed = c.git(sp, "rev-list", "--count", "--branches", "HEAD", "--not", "--remotes").stdout.strip()
    if unpushed.isdigit() and int(unpushed):
        why.append(f"{unpushed} commit(s) on its branches are on no remote")
    dirty = len(c.porcelain(None, sp))
    if dirty:
        why.append(f"{dirty} uncommitted change(s)")
    return {"state": "initialised", "lost": bool(why), "why": "; ".join(why)}


MAX_PATCH_SCAN = 16 * 1024 * 1024


def dirty_credentials(checkout: Path) -> list[dict]:
    """What looks like a credential in the uncommitted changes of tracked files of a checkout: {file, rule}, never a value."""
    if c.git(checkout, "rev-parse", "--verify", "--quiet", "HEAD").returncode != 0:
        return []
    cp = c.patch_git(checkout, "diff", *c.DIFF_FLAGS, "HEAD")
    text = cp.stdout[:MAX_PATCH_SCAN].decode("utf-8", "replace") if cp.returncode == 0 else ""
    return [{"file": f, "rule": r} for f, r in secrets.scan_patch(text)]


def stash_credentials(path: Path, stashes: list[dict]) -> list[dict]:
    """The same for every stash entry (its tracked part and its untracked part): {n, message, file, rule}."""
    out = []
    for s in stashes:
        texts = [c.patch_git(path, "diff", *c.DIFF_FLAGS, f"{s['sha']}^1", s["sha"]).stdout]
        if s["has_untracked"]:
            texts.append(c.patch_git(path, "show", *c.DIFF_FLAGS, "--format=", f"{s['sha']}^3").stdout)
        for data in texts:
            for f, r in secrets.scan_patch(data[:MAX_PATCH_SCAN].decode("utf-8", "replace")):
                if not any(x["n"] == s["n"] and x["file"] == f and x["rule"] == r for x in out):
                    out.append({"n": s["n"], "message": s["message"], "file": f, "rule": r})
    return out


def _reflog_only(ctx, path: Path) -> dict:
    t0 = time.time()
    cp = c.git(path, "rev-list", "--count", "--reflog", "--not", "--all", timeout=REFLOG_TIMEOUT)
    if cp.returncode != 0 and time.time() - t0 >= REFLOG_TIMEOUT - 1:
        return {"commits": None, "cut_short": True}
    return {"commits": int(cp.stdout.strip()) if cp.stdout.strip().isdigit() else None, "cut_short": False}


def _worktree_kind(path: str, lineage: dict) -> str:
    if worktrees.is_scratch(path):
        return "scratch"
    if worktrees.is_follower_sibling(path, lineage):
        return "follower"
    return "workstream"


# ------------------------------------------------------------------ one repository

def survey_repo(ctx: c.Ctx, path: Path, name: str, *, scan: bool = True, cwds: list[str] | None = None, extra: dict | None = None) -> dict:
    cwds = get_live_cwds() if cwds is None else cwds
    refs = c.for_each_ref(ctx, path)
    cfg = c.local_config(path)
    remotes: dict[str, dict] = {}
    for k, v in cfg:
        m = re.match(r"remote\.(.+)\.(url|pushurl|fetch)$", k)
        if m:
            remotes.setdefault(m.group(1), {}).setdefault(m.group(2), []).append(v)
    remotes_out = {n: {"url": secrets.redact_url(r.get("url", [""])[0]), "pushurl": secrets.redact_url(r.get("pushurl", [""])[0]),
                       "fetch": r.get("fetch", [])} for n, r in remotes.items()}
    has_remote_refs = any(r["ref"].startswith("refs/remotes/") for r in refs)
    unique = _unique_commits(ctx, path, has_remote_refs)
    branches = _branch_rows(refs, unique, has_remote_refs)
    head = gitutil.out(path, "rev-parse", "HEAD")
    tracked, subs = _tracked(ctx, path)
    lfs = False
    for rel in [t for t in tracked if t == ".gitattributes" or t.endswith("/.gitattributes")][:200]:
        try:
            if "filter=lfs" in (path / rel).read_text(encoding="utf-8", errors="replace"):
                lfs = True
                break
        except OSError:
            pass
    lfs_dir = c.git_common_dir(path) / "lfs" / "objects"
    lfs_objects = c.summarize_dir(lfs_dir, 1000000) if lfs_dir.is_dir() else {"files": 0, "bytes": 0}
    for sub in subs:
        sub["state"] = submodule_state(path, sub)
    lineage = worktrees.load_lineage()
    wt_entries = worktrees.list_worktrees(path)
    main_real = c.real(path)
    seen = {main_real}
    wts = []
    for e in wt_entries[1:]:
        real = c.real(e["path"])
        if real in seen:
            continue
        seen.add(real)
        # git reports a worktree under the spelling it was created with (possibly through an alias); use its real path
        wpath = Path(os.path.realpath(e["path"])) if Path(e["path"]).exists() else Path(e["path"])
        inside = real.startswith(main_real + os.sep)
        wt = {"path": str(wpath), "rel": c.posix_rel(wpath, os.path.realpath(path)) if inside else "", "outside": not inside, "branch": e["branch"] or None,
              "head": e["head"], "detached": bool(e["detached"]), "prunable": bool(e["prunable"]) or not wpath.is_dir(),
              "kind": _worktree_kind(e["path"], lineage), "key": wpath.name}
        if not wt["prunable"]:
            entries = c.porcelain(ctx, wpath)
            wt["dirty"] = len(entries)
            wt["live"] = bool(worktrees.is_live(wpath, cwds))
            wt["credentials"] = [] if name in ctx.cfg["no_scan"] else dirty_credentials(wpath)
            wt["note"] = note_info(wpath)
            wt["exit"] = worktrees.exit_from_note(wpath)
            wt["tracked_collisions"] = collisions(c.split_z(c.git_ok(ctx, wpath, "ls-files", "-z", what="ls-files")))
            _, wsubs = _tracked(ctx, wpath)             # a worktree has its own submodule checkouts
            for sub in wsubs:
                sub["state"] = submodule_state(wpath, sub)
            wt["submodules"] = wsubs
            wt["_local"] = loc.classify_checkout(ctx, wpath, name)
            wt["local"] = local_summary(wt["_local"])
            wt["on_mainline"] = bool(worktrees.on_mainline(path, e["head"])) if e["head"] else False
        wts.append(wt)
    # worktree keys must be unique inside the bundle
    used: dict[str, int] = {}
    for wt in wts:
        n = used.get(wt["key"], 0)
        used[wt["key"]] = n + 1
        if n:
            wt["key"] = f"{wt['key']}-{n + 1}"
    dirty = c.porcelain(ctx, path)
    local_main = loc.classify_checkout(ctx, path, name)
    info = c.git_common_dir(path)
    hooks = []
    try:
        hooks = sorted(f for f in os.listdir(info / "hooks") if not f.endswith(".sample"))
    except OSError:
        pass
    ref_names = [r["ref"] for r in refs]
    repo = {
        "name": name, "path": str(path), "extra": extra or None,
        "origin": remotes_out.get("origin", {}).get("url") or None, "remotes": remotes_out,
        "branch": gitutil.branch(path) or None, "head": head, "branches": branches,
        "tags": sum(1 for r in refs if r["ref"].startswith("refs/tags/")), "refs": len(refs),
        "stashes": _stashes(ctx, path), "submodules": subs, "lfs": lfs, "lfs_objects": lfs_objects, "worktrees": wts,
        "dirty": [{"status": s, "path": p} for s, p in dirty], "local": local_summary(local_main), "_local": local_main,
        "reflog": _reflog_only(ctx, path), "hooks": hooks, "object_bytes": _count_objects(path),
        "case_collisions": {"refs": ref_collisions(ref_names), "tracked": collisions(tracked)},
        "tracked_files": len(tracked), "config_other": sorted({k for k, _ in cfg if not re.match(r"(remote|branch|user)\.", k)}),
        "skipped": name in ctx.cfg["skip_repos"],
    }
    repo["credentials"] = [] if name in ctx.cfg["no_scan"] else dirty_credentials(path)
    repo["stash_credentials"] = [] if name in ctx.cfg["no_scan"] else stash_credentials(path, repo["stashes"])
    repo["case_collisions"]["worktrees"] = {w["key"]: w["tracked_collisions"] for w in wts if w.get("tracked_collisions")}
    repo["no_scan"] = name in ctx.cfg["no_scan"]
    if scan and not repo["no_scan"]:
        repo["windows"] = scanmod.scan_tracked(path, tracked, str(ctx.root), str(ctx.home))
    repo["toolchains"] = [] if repo["no_scan"] else scanmod.detect_toolchains(path, tracked)
    return repo


def local_summary(l: dict) -> dict:
    """The JSON-sized form of a classified checkout: counts, bytes and one line per undecided group."""
    carried_b = sum(f["size"] for f in l["carried"])
    return {
        "carried": {"files": len(l["carried"]), "bytes": carried_b, "paths": [f["rel"] for f in l["carried"][:50]]},
        "rebuildable": l["rebuildable"], "machine": l["machine"], "left": l["left"],
        "secrets": [{"rel": f["rel"], "size": f["size"], "why": f.get("why", "")} for f in l["secrets"]],
        "undecided": {"files": len(l["undecided"]), "bytes": l["undecided_bytes"], "groups": l["undecided_groups"]},
        "untracked": {"files": len(l["untracked"]), "bytes": sum(f["size"] for f in l["untracked"])},
        "links": l["links"], "nested": l["nested"], "errors": l.get("errors", [])[:5],
    }


def survey_all(ctx: c.Ctx, *, scan: bool = True, only: list[str] | None = None) -> dict:
    """Everything under the root: repositories (and extra repositories), aliases, non-repo directories, root files."""
    repos, aliases, nonrepo, root_files = list_root(ctx)
    wanted = {n.lower() for n in (only or [])}
    jobs = [(p, p.name, None) for p in repos if not wanted or p.name.lower() in wanted]
    for e in ctx.cfg["extra_repos"]:
        p = Path(e["path"])
        if (p / ".git").is_dir() and (not wanted or p.name.lower() in wanted):
            jobs.append((p, p.name if p.name not in {j[1] for j in jobs} else f"extra-{p.name}", e))
    cwds = get_live_cwds()
    with ThreadPoolExecutor(max_workers=6) as pool:
        surveyed = list(pool.map(lambda j: survey_repo(ctx, j[0], j[1], scan=scan, cwds=cwds, extra=j[2]), jobs))
    return {"repos": surveyed, "aliases": aliases, "nonrepo": nonrepo, "root_files": root_files, "cwds": cwds}


# ------------------------------------------------------------------ readiness

def readiness(ctx: c.Ctx, repos: list[dict]) -> list[dict]:
    """Blockers for export (``blocking`` True) and things to redo on the Mac (False), each with what clears it."""
    out: list[dict] = []
    for repo in repos:
        if repo["skipped"]:
            continue
        for wt in repo["worktrees"]:
            where = f"{repo['name']}/{wt['rel'] or wt['path']}"
            if wt["prunable"]:
                out.append({"kind": "worktree-missing", "where": where, "blocking": False,
                            "detail": "the worktree directory is missing; its branch is bundled and the worktree is not recreated"
                                      + ("; its detached HEAD " + wt["head"][:10] + " is bundled and kept as refs/migrate/worktree-heads/" + wt["key"] if wt["detached"] else ""),
                            "action": f"git -C {repo['path']} worktree prune"})
                continue
            if wt["outside"]:
                out.append({"kind": "worktree-outside", "where": where, "blocking": False,
                            "detail": f"{wt['path']} is outside the repository directory: its uncommitted, untracked and local-only work is carried; on import it is "
                                      f"recreated under .claude/worktrees/{wt['key']} when that is free, else its work is kept as files under .baton/migrated-patches/{wt['key']}/",
                            "action": "nothing to do before the export; after the import, `bt migrate verify` lists what needs a person"})
            if wt["kind"] != "workstream":
                continue
            if wt["live"]:
                out.append({"kind": "live-session", "where": where, "blocking": True, "detail": "a live session owns this worktree",
                            "action": "end the session (handover, then close the tab); bt status shows which"})
            note = wt["note"]
            retire = " or, when nothing in it is needed, `bt wt retire " + wt["key"] + "`" if wt.get("on_mainline") and not wt.get("dirty") else ""
            if not note["exists"]:
                out.append({"kind": "handover-missing", "where": where, "blocking": True, "detail": ".baton/now.md is missing",
                            "action": "write the handover (bt close or the handover skill)" + retire})
            elif not note["complete"]:
                out.append({"kind": "handover-incomplete", "where": where, "blocking": True,
                            "detail": "now.md has no Objective: and Next: content",
                            "action": "fill Objective: and Next: (bt now check lints it)" + retire})
            elif note["stale"]:
                out.append({"kind": "handover-stale", "where": where, "blocking": True,
                            "detail": "now.md is older than the worktree's last commit",
                            "action": "update the handover (bt close or the handover skill)" + retire})
            out.extend(_compute_blockers(ctx, Path(wt["path"]), where))
            out.extend(_follower_blockers(Path(wt["path"]), where))
        if not ctx.include_secrets:
            for tree, items in [("(main)", repo.get("credentials", []))] + [(w["key"], w.get("credentials", [])) for w in repo["worktrees"]]:
                for x in items:
                    out.append({"kind": "uncommitted-credential", "where": f"{repo['name']}/{tree}", "blocking": True,
                                "detail": f"uncommitted change in {x['file']} of {repo['name']}/{tree} contains what looks like a credential: {x['rule']}",
                                "action": "commit or remove it, or export with --include-secrets (the change itself is carried either way)"})
            for x in repo.get("stash_credentials", []):
                out.append({"kind": "uncommitted-credential", "where": f"{repo['name']}/stash@{{{x['n']}}}", "blocking": True,
                            "detail": f"stash entry stash@{{{x['n']}}} ({x['message']}) of {repo['name']} contains what looks like a credential in "
                                      f"{x['file']}: {x['rule']}",
                            "action": "drop or rewrite the stash entry, or export with --include-secrets (the entry is carried either way)"})
        for tree, root, subs in [("", Path(repo["path"]), repo["submodules"])] + [
                (w["rel"] or w["path"], Path(w["path"]), w.get("submodules", [])) for w in repo["worktrees"] if "_local" in w]:
            for sub in subs:
                if (sub.get("state") or {}).get("lost"):
                    out.append({"kind": "submodule-unpushed", "where": f"{repo['name']}/" + (tree + "/" if tree else "") + sub["path"], "blocking": True,
                                "detail": "the submodule is not carried and holds work that exists nowhere else: " + sub["state"]["why"],
                                "action": f"in {root / sub['path']}: commit or stash the changes and `git push` every branch (then re-run the export)"})
        if repo.get("lfs_objects", {}).get("bytes", 0) > c.LFS_CARRY_LIMIT:
            out.append({"kind": "lfs-too-large", "where": repo["name"], "blocking": True,
                        "detail": f".git/lfs/objects holds {c.human(repo['lfs_objects']['bytes'])}, above the {c.human(c.LFS_CARRY_LIMIT)} that are carried",
                        "action": "push the objects (`git lfs push --all origin`) and `git lfs prune`, or list the repository in skip_repos"})
        out.extend(_compute_blockers(ctx, Path(repo["path"]), f"{repo['name']}/(main)"))
        decide = []
        for tree, l in [("(main)", repo["_local"])] + [(w["key"], w["_local"]) for w in repo["worktrees"] if "_local" in w]:
            _, _, left = loc.plan_copy(ctx, l)
            n = [x for x in left if x["decision"]]
            if n:
                decide.append(f"{tree}: {len(n)} path(s), {c.human(sum(x['size'] for x in n))}")
            by_rule = [f for f in l["carried"] if f["rule"] not in ("carry glob", "secret-looking, --include-secrets")]
            if sum(f["size"] for f in by_rule) > c.RULE_CARRY_LIMIT:
                top: dict[str, int] = {}
                for f in by_rule:
                    k = "/".join(f["rel"].split("/")[:3])
                    top[k] = top.get(k, 0) + f["size"]
                biggest = ", ".join(f"{k} {c.human(v)}" for k, v in sorted(top.items(), key=lambda kv: -kv[1])[:4])
                out.append({"kind": "large-carry", "where": f"{repo['name']}/{tree}", "blocking": True,
                            "detail": f"files carried by the built-in rules total {c.human(sum(f['size'] for f in by_rule))} "
                                      f"(limit {c.human(c.RULE_CARRY_LIMIT)}); largest: {biggest}",
                            "action": "settle the large paths with `carry` or `leave` globs in migrate.toml (an explicit carry is accepted at any size)"})
            if l["nested"]:
                names = ", ".join(n["rel"] for n in l["nested"][:3]) + (f", +{len(l['nested']) - 3} more" if len(l["nested"]) > 3 else "")
                out.append({"kind": "nested-repo", "where": f"{repo['name']}/{tree}", "blocking": False,
                            "detail": f"{len(l['nested'])} nested git repositories are not carried (their history is not in the bundle): {names}",
                            "action": "add one as an extra_repos entry in migrate.toml, or leave it"})
        if decide:
            out.append({"kind": "undecided-ignored", "where": repo["name"], "blocking": True,
                        "detail": "ignored files over the size limits need a decision: " + "; ".join(decide),
                        "action": "add `carry` or `leave` globs for them to migrate.toml (or [repos." + repo["name"] + "] carry/leave); `bt migrate audit` lists them"})
    for row in _enrolments(ctx):
        out.append(row)
    for p in ctx.problems:
        out.append({"kind": "could-not-read", "where": p["path"], "blocking": True, "detail": p["error"],
                    "action": "fix the permission or path problem, or settle it with --force (it is then recorded in the manifest)"})
    return out


def decide_checkout(local: dict) -> tuple[list[tuple[str, int]], int]:
    """(root-anchored glob patterns with the bytes each settles in this checkout, the bytes left undecided after them), the
    shortest list whose decision clears the checkout's ``undecided-ignored`` blocker. Empty when it has none. A file over the
    per-file limit is named by its own path even inside a wholly undecided directory (its small neighbours stay carried by
    default); a checkout over the per-checkout total then gets the largest groups until the rest falls under it."""
    files = local["undecided"]
    total = sum(f["size"] for f in files)
    big = [f for f in files if f["size"] >= c.UNDECIDED_FILE_LIMIT]
    if total < c.UNDECIDED_TOTAL_LIMIT and not big:
        return [], total
    settled = {f["rel"] for f in big}
    left = total - sum(f["size"] for f in big)
    groups = []
    if left >= c.UNDECIDED_TOTAL_LIMIT:
        cand = []
        for g in local["undecided_groups"]:
            members = [f for f in files if (f["rel"].startswith(g["path"]) if g["dir"] else f["rel"] == g["path"])]
            cand.append((sum(f["size"] for f in members if f["rel"] not in settled), g, members))
        for rem, g, members in sorted(cand, key=lambda t: (-t[0], t[1]["path"])):
            if left < c.UNDECIDED_TOTAL_LIMIT:
                break
            if rem:
                groups.append((rem, g, members))
                settled |= {f["rel"] for f in members}
                left -= rem
    in_groups = {f["rel"] for _, _, members in groups for f in members}
    picks = [("/" + g["path"], rem) for rem, g, _ in groups]
    picks += [("/" + f["rel"], f["size"]) for f in big if f["rel"] not in in_groups]
    return sorted(picks, key=lambda p: (-p[1], p[0])), left


def decide_snippets(ctx: c.Ctx, repos: list[dict]) -> dict[str, str]:
    """Per repository with a checkout that has an ``undecided-ignored`` blocker: ``[repos.<name>]`` lines to paste into
    migrate.toml. Every line is a pattern anchored at the checkout root (a leading ``/``), and its comment is computed by matching
    that pattern against the undecided files of every checkout of the repository: what it would settle and where. The owner moves
    each line into ``carry = [...]`` or ``leave = [...]``; once all are moved, every blocked checkout is clear. A line that a
    broader line already covers is dropped."""
    out: dict[str, str] = {}
    for repo in repos:
        if repo["skipped"]:
            continue
        trees = [("(main)", repo["_local"])] + [(w["key"], w["_local"]) for w in repo["worktrees"] if "_local" in w]
        patterns: dict[str, None] = {}
        notes = []
        for tree, l in trees:
            picks, left = decide_checkout(l)
            if picks:
                notes.append(f"{tree}: {c.human(l['undecided_bytes'])} undecided, {c.human(left)} would be left")
                patterns.update(dict.fromkeys(p for p, _ in picks))
        if not patterns:
            continue
        stats = {}
        for p in patterns:
            e = {"bytes": 0, "files": 0, "trees": [], "mtime": 0.0, "rels": set()}
            for tree, l in trees:
                hit = [f for f in l["undecided"] if c.match_any([p], f["rel"])]
                if hit:
                    e["bytes"] += sum(f["size"] for f in hit)
                    e["files"] += len(hit)
                    e["trees"].append(tree)
                    e["mtime"] = max(e["mtime"], max(f["mtime"] for f in hit))
                    e["rels"] |= {(tree, f["rel"]) for f in hit}
            stats[p] = e
        kept, covered = [], set()
        for p in sorted(stats, key=lambda p: (-len(stats[p]["rels"]), p)):        # broadest first
            if stats[p]["rels"] - covered:
                kept.append(p)
                covered |= stats[p]["rels"]
        body = [f"[repos.{repo['name']}]",
                f"# Limits per checkout: {c.human(c.UNDECIDED_TOTAL_LIMIT)} of undecided ignored files, {c.human(c.UNDECIDED_FILE_LIMIT)} per file. "
                + "; ".join(notes) + ".",
                "# A leading / anchors a pattern at the checkout root; each comment is what the line settles, in which checkouts.",
                "# Move each line into carry = [...] (copied, whatever its size) or leave = [...] (not copied, recorded). Pasted as it is, it changes nothing.",
                "decide = ["]
        for p in sorted(kept, key=lambda p: (-stats[p]["bytes"], p)):
            e = stats[p]
            when = time.strftime("%Y-%m-%d", time.localtime(e["mtime"])) if e["mtime"] else "-"
            body.append(f'  "{p}",  # settles {c.human(e["bytes"])} in {e["files"]} file(s), newest {when}; in {", ".join(e["trees"])}')
        body.append("]")
        out[repo["name"]] = chr(10).join(body)
    return out


def _compute_blockers(ctx: c.Ctx, checkout: Path, where: str) -> list[dict]:
    try:
        from .. import compute_pickup
        rows = compute_pickup.collect(checkout, ctx.state_dir / "compute", limit=128)
    except (OverflowError, OSError, ValueError):
        return []
    out = []
    for r in rows:
        if r["state"] in ("needs-review", "pending", "follow-up"):
            out.append({"kind": "compute-result", "where": where, "blocking": True,
                        "detail": f"deferred compute job {r['id']} is {r['state']} ({r.get('status', 'unknown')})",
                        "action": "wait for it to end; `bt compute --refresh`, inspect, then `bt compute --reviewed " + r["id"] + " --resolution checked`"})
    return out


def _follower_blockers(checkout: Path, where: str) -> list[dict]:
    out = []
    state = checkout / ".baton" / "follow-state"
    try:
        files = sorted(state.glob("*.json"))
    except OSError:
        return out
    for f in files:
        st = config.read_json(f)
        if st.get("status") == "running" and st.get("pid"):
            try:
                from .. import follow
                alive = follow.pid_alive(st["pid"])
            except Exception:  # noqa: BLE001
                alive = False
            if alive:
                out.append({"kind": "follower-running", "where": where, "blocking": True,
                            "detail": f"follower {f.stem} is running (pid {st['pid']})", "action": "wait for it to finish"})
    return out


def _enrolments(ctx: c.Ctx) -> list[dict]:
    try:
        from .. import night_schedule
        rows = night_schedule.read_enrolled(ctx.state_dir)
    except Exception:  # noqa: BLE001
        return []
    return [{"kind": "night-enrolment", "where": r["root"], "blocking": False,
             "detail": "night enrolments are keyed by path and are not carried",
             "action": "on the Mac, in that worktree: `bt night enroll`"} for r in rows]
