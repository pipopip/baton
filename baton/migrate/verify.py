"""``bt migrate verify``: re-check an import against the bundle's manifest without changing anything."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from . import common as c
from . import importer as imp


def _same_file(src: Path, dest: Path, lay: imp.Layout, *, rewrite: bool = True) -> str:
    """'' when ``dest`` (or its ``.migrated`` sibling) holds the rewritten source; else why not."""
    if os.stat(c.lp(src)).st_size >= imp.rw.MAX_TEXT_BYTES:                      # carried unchanged: compared in chunks
        for cand in (dest, dest.with_name(dest.name + ".migrated")):
            if os.path.lexists(c.lp(cand)) and c.same_content(src, cand):
                return ""
        return "is missing" if not os.path.lexists(c.lp(dest)) else "differs from the bundle's file and no .migrated file holds it"
    data = open(c.lp(src), "rb").read()
    want = imp.expected(lay.rewriter, data, dest.name) if rewrite else data
    for cand in (dest, dest.with_name(dest.name + ".migrated")):
        if os.path.lexists(c.lp(cand)):
            try:
                if open(c.lp(cand), "rb").read() == want:
                    return ""
            except OSError as exc:
                return f"unreadable: {exc}"
    return "is missing" if not os.path.lexists(c.lp(dest)) else "differs from the rewritten source and no .migrated file holds it"


def _head_and_branch(path: Path) -> tuple[str, str]:
    return imp.head_and_branch(path)


def _check_work(name: str, key: str, path: Path, d: dict, src: Path, gaps: list[str]) -> int:
    """The uncommitted work of one checkout: every tracked file that is not in the old machine's changed list matches HEAD, every
    changed file has the old machine's content (as git hashes it: line endings and filters as git sees them), a patch that was
    kept instead of applied is a gap, and the untracked files are there. Returns the number of files checked."""
    label = f"uncommitted work of {name}/{key}"
    n = 0
    kept = path / ".baton" / "migrated-patches" / "tracked.patch"
    if kept.is_file():
        gaps.append(f"{label}: the tracked changes did not apply and were kept at {kept}; a person must apply them")
    changed = d.get("changed")
    if c.git(path, "rev-parse", "--verify", "--quiet", "HEAD").returncode != 0:
        changed = []                       # a repository with no commits has no HEAD to compare with
    if changed is None:                    # a bundle from before the changed list: the old check
        if d["tracked_patch"] and not kept.is_file():
            patch = (src / d["tracked_patch"]["file"]).resolve()
            if c.git(path, *c.APPLY_CONFIG, "apply", "--binary", "-p1", "--reverse", "--check", str(patch)).returncode != 0:
                gaps.append(f"{label}: the tracked patch is not applied")
    else:
        cp = c.git(path, *c.PATCH_CONFIG, "diff", "--name-only", "-z", "--no-renames", "--no-ext-diff", "--no-textconv", "HEAD")
        if cp.returncode != 0 and not changed:
            pass                           # no HEAD, nothing recorded
        elif cp.returncode != 0:
            gaps.append(f"{label}: git diff HEAD failed: {cp.stderr.strip()[:200]}")
        else:
            recorded = {x["path"]: x for x in changed}
            for p in sorted(set(c.split_z(cp.stdout)) - set(recorded)):
                gaps.append(f"{name}/{key}: tracked file {p} differs from HEAD, but it was not changed on the old machine")
            for p, x in sorted(recorded.items()):
                n += 1
                full = path / p
                present = os.path.lexists(c.lp(full))
                if x["kind"] == "deleted":
                    if present:
                        gaps.append(f"{label}: {p} was deleted on the old machine and still exists")
                elif not present:
                    gaps.append(f"{label}: {p} is missing")
                elif x["kind"] == "file" and x.get("blob"):
                    h = c.git(path, "hash-object", f"--path={p}", "--", p)
                    if h.returncode == 0 and h.stdout.strip() != x["blob"]:
                        gaps.append(f"{label}: the content of {p} differs from the old machine's")
    for f in d["untracked"]:
        target = path.joinpath(*f["rel"].split("/"))
        ok = any(os.path.lexists(c.lp(t)) and c.sha256_file(t) == f["sha256"] for t in (target, target.with_name(target.name + ".migrated")))
        n += 1
        if not ok:
            gaps.append(f"untracked file {name}/{key}/{f['rel']}: missing or different")
    return n


def _verify_repo(src: Path, lay: imp.Layout, entry: dict, gaps: list[str], seen: dict) -> None:
    name = entry["name"]
    dest = lay.repo_dest(entry)
    if not (dest / ".git").exists():
        gaps.append(f"repository {name}: {dest} is missing")
        return
    seen["repos"] += 1
    for g in imp.compare_refs(dest, entry):
        gaps.append(f"repository {name}: {g}")
    seen["refs"] += len(entry["refs"])
    head, cur = _head_and_branch(dest)
    if entry["branch"] and cur != entry["branch"]:
        gaps.append(f"repository {name}: checked out branch is {cur or '(detached)'}, the manifest has {entry['branch']}")
    elif not entry["branch"] and cur and entry["head"]:
        gaps.append(f"repository {name}: checked out branch is {cur}, the manifest has a detached HEAD")
    if entry["head"] and head != entry["head"]:
        gaps.append(f"repository {name}: HEAD is {head[:10] or '(none)'}, the manifest has {entry['head'][:10]}")
    # stash entries: the same commits in the same order
    want = [(s["sha"], s["message"]) for s in entry["stashes"]]
    have = imp.stash_list(dest)
    if want or have:
        if len(have) != len(want):
            gaps.append(f"repository {name}: {len(have)} stash entries, the manifest has {len(want)}")
        for i, (w, h) in enumerate(zip(want, have)):
            if w[0] != h[0]:
                gaps.append(f"repository {name}: stash@{{{i}}} is {h[0][:10]}, the manifest has {w[0][:10]}")
            elif w[1] != h[1]:
                gaps.append(f"repository {name}: stash@{{{i}}} has the message {h[1]!r}, the manifest has {w[1]!r}")
    for s in entry["stashes"]:
        for k in ("patch", "untracked_patch"):
            meta = s.get(k)
            if meta and not (dest / ".baton" / "migrated-stashes" / Path(meta["file"]).name).is_file():
                gaps.append(f"stash {s['n']} of {name}: patch file {Path(meta['file']).name} is not kept")
    for wt in entry["worktrees"]:
        key = wt["key"]
        outside = imp.Layout.is_outside(wt)
        path = lay.tree_path(entry, key)               # a directory that is verifiably this worktree
        if path is None and not wt.get("prunable"):    # else the directory that should be it, to say what differs
            taken = {w["rel"] for w in entry["worktrees"] if not imp.Layout.is_outside(w)}
            path = next((dest.joinpath(*r.split("/")) for r in imp.Layout.candidate_rels(wt)
                         if (r not in taken or not outside) and (dest.joinpath(*r.split("/")) / ".git").exists()), None)
        label = f"{name}/{path.relative_to(dest).as_posix() if path else (wt['rel'] or '.claude/worktrees/' + key)}"
        recreated = path is not None
        if recreated:
            seen["worktrees"] += 1
            wh, branch = _head_and_branch(path)
            if wt["branch"] and branch != wt["branch"]:
                gaps.append(f"worktree {label}: is on {branch or '(detached)'}, the manifest has {wt['branch']}")
            if wt["head"] and wh != wt["head"]:
                gaps.append(f"worktree {label}: HEAD is {wh[:10]}, the manifest has {wt['head'][:10]}")
            continue
        if not wt.get("prunable") and not outside:
            gaps.append(f"worktree {label}: is missing")
        if wt.get("head"):                 # a worktree that is not recreated keeps its HEAD alive as a ref
            ref = c.git(dest, "rev-parse", "--verify", "--quiet", imp.WORKTREE_HEADS + key).stdout.strip()
            if ref != wt["head"]:
                gaps.append(f"worktree {label}: its HEAD {wt['head'][:10]} is not kept as {imp.WORKTREE_HEADS}{key}")
    for key, d in entry["dirty"].items():
        path = lay.tree_path(entry, key)
        if path is None or not path.is_dir():
            if d["tracked_patch"] or d["untracked"]:
                kept = imp.kept_dir(dest, key)
                gaps.append(f"uncommitted work of {name}/{key}: its checkout is missing; " +
                            (f"the work was kept under {kept} and a person must apply it" if kept.is_dir() else "and the work was not kept either"))
            continue
        seen["files"] += _check_work(name, key, path, d, src, gaps)
    for f in entry["overlay"]:
        path = lay.tree_path(entry, f["tree"])
        seen["files"] += 1
        if path is None or not path.is_dir():
            gaps.append(f"overlay file {name}/{f['tree']}/{f['rel']}: its checkout is missing")
            continue
        why = _same_file(src / f["file"], path.joinpath(*f["rel"].split("/")), lay)
        if why:
            gaps.append(f"overlay file {name}/{f['tree']}/{f['rel']}: {why}")
    meta = entry.get("lfs_objects")
    if meta:
        base = src / meta["prefix"]
        for dirpath, _, names in os.walk(c.lp(base)):
            for n in names:
                rel = os.path.relpath(os.path.join(dirpath, n), c.lp(base))
                target = dest / ".git" / "lfs" / "objects" / rel
                seen["files"] += 1
                if not os.path.isfile(c.lp(target)) or os.stat(c.lp(target)).st_size != os.stat(os.path.join(dirpath, n)).st_size:
                    gaps.append(f"git LFS object {name}/{rel.replace(os.sep, '/')}: missing or a different size")


def verify(src: Path, new_root: Path, new_home: Path, only: set[str] | None = None) -> tuple[list[str], dict]:
    manifest = imp.load_manifest(src)
    lay = imp.Layout(manifest, new_root, new_home)
    gaps: list[str] = []
    seen = {"repos": 0, "refs": 0, "worktrees": 0, "files": 0}
    for entry in manifest["repos"].values():
        if only and entry["name"].lower() not in only:
            continue
        try:
            _verify_repo(src, lay, entry, gaps, seen)
        except Exception as exc:  # noqa: BLE001 - one unreadable repository must not hide the gaps of the others
            gaps.append(f"repository {entry['name']}: could not be checked: {type(exc).__name__}: {exc}")
    if not only:
        for f in manifest["root"]["overlay"]:
            seen["files"] += 1
            why = _same_file(src / f["file"], lay.new_root.joinpath(*f["rel"].split("/")), lay)
            if why:
                gaps.append(f"root file {f['rel']}: {why}")
        for d in manifest.get("dirs", []):
            base = lay.base(d["base"]).joinpath(*d["target"].split("/"))
            for f in d["files"]:
                seen["files"] += 1
                why = _same_file(src / f["file"], base.joinpath(*f["rel"].split("/")), lay)
                if why:
                    gaps.append(f"directory {d['target']}: {f['rel']} {why}")
        for a in manifest.get("aliases", []):
            if not a["target_in_root"]:
                continue
            link = lay.new_root / a["name"]
            if not c.is_link(link):
                gaps.append(f"alias {a['name']}: the link to {a['target_in_root']} is missing")
            elif c.real(link) != c.real(lay.new_root / a["target_in_root"]):
                gaps.append(f"alias {a['name']}: it does not lead to {a['target_in_root']}")
    for f in manifest.get("baton_state", {}).get("files", []):
        seen["files"] += 1
        why = _same_file(src / f["file"], lay.new_root / imp.STAGE / Path(*f["file"].split("/")), lay)
        if why:
            gaps.append(f"baton configuration {f['file']}: {why}")
    stage = lay.new_root / imp.STAGE / "user"
    for f in manifest["user"]["files"]:
        seen["files"] += 1
        why = _same_file(src / f["file"], stage.joinpath(*lay.staged_user_rel(f["file"]).split("/")), lay)
        if why:
            gaps.append(f"user configuration {f['file']}: {why}")
    for m in manifest["user"]["memory"]:
        new = lay.slug_map().get(m["slug"], m["slug"])
        mem = stage / "claude" / m["account"] / "projects" / new / "memory"
        have = sum(1 for _ in mem.rglob("*") if _.is_file()) if mem.is_dir() else 0
        if have < m["files"]:
            gaps.append(f"memory directory {m['slug']} (as {new}): {have} of {m['files']} files present")
    return gaps, seen


def run(args) -> int:
    src, new_root, new_home = imp.resolve_args(args)
    try:
        gaps, seen = verify(src, new_root, new_home, {n.lower() for n in (getattr(args, "repo", None) or [])} or None)
    except imp.BundleError as exc:
        print(f"bt migrate verify: {exc}", file=sys.stderr)
        return 2
    if gaps:
        print(f"{len(gaps)} gap(s):")
        for g in gaps:
            print(f"  - {g}")
        return 1
    print(f"verified: {seen['repos']} repositories, {seen['refs']} refs, {seen['worktrees']} worktrees, {seen['files']} files; no gaps")
    return 0
