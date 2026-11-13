"""``bt migrate import``: verify a bundle, then rebuild repositories, worktrees and local-only files under a new root.

Runs on macOS, Linux and Windows. Never installs packages, never runs repository restore commands, never writes credentials,
never touches ssh keys. Existing files are never overwritten: a differing file gets a ``.migrated`` sibling.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from . import common as c
from . import notes
from . import secrets
from . import rewrite as rw
from . import user as userconf

STAGE = ".migration"


class BundleError(Exception):
    pass


# ------------------------------------------------------------------ bundle

def load_manifest(src: Path) -> dict:
    """The manifest of a bundle with every file's size and sha256 checked. Raises BundleError (nothing has been written)."""
    try:
        manifest = json.loads((src / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise BundleError(f"{src} has no readable manifest.json: {exc}")
    if not isinstance(manifest, dict) or manifest.get("tool") != c.TOOL:
        raise BundleError(f"{src} is not a bundle of this tool")
    if int(manifest.get("format", 0)) > c.FORMAT_VERSION:
        raise BundleError(f"bundle format {manifest['format']} is newer than this tool understands ({c.FORMAT_VERSION}); update Baton")
    bad = []
    for rel, meta in manifest.get("files", {}).items():
        path = src / rel
        try:
            if os.stat(c.lp(path)).st_size != meta["size"] or c.sha256_file(path) != meta["sha256"]:
                bad.append(f"{rel}: size or sha256 differs from the manifest")
        except OSError as exc:
            bad.append(f"{rel}: {exc}")
    if bad:
        raise BundleError("the bundle is damaged; nothing was written:\n  " + "\n  ".join(bad[:30]))
    return manifest


class Layout:
    """Where everything goes on the new machine, and the rewriter that maps source paths to it."""

    def __init__(self, manifest: dict, new_root: Path, new_home: Path):
        self.m = manifest
        self._slugs = None
        self.new_root, self.new_home = Path(new_root), Path(new_home)
        src = manifest["source"]
        pairs = [(src["root"], self.new_root.as_posix()), (src["home"], self.new_home.as_posix())]
        for repo in manifest["repos"].values():
            if repo["base"] == "home" or repo["target"] != repo["name"]:
                pairs.append((repo["source_path"], self.base(repo["base"]).joinpath(*repo["target"].split("/")).as_posix()))
        for d in manifest.get("dirs", []):
            pairs.append((d["source_path"], self.base(d["base"]).joinpath(*d["target"].split("/")).as_posix()))
        self.rewriter = rw.Rewriter(pairs, src["home"], self.new_home.as_posix())
        self.placed: dict[tuple[str, str], Path] = {}       # (repository, worktree key) -> checkout recreated in this run
        self.strict = False                                 # an import trusts only what it placed; verify looks and checks

    def base(self, which: str) -> Path:
        return self.new_home if which == "home" else self.new_root

    def repo_dest(self, repo: dict) -> Path:
        return self.base(repo["base"]).joinpath(*repo["target"].split("/"))

    @staticmethod
    def is_outside(wt: dict) -> bool:
        return bool(wt.get("outside") or not wt.get("rel"))

    @staticmethod
    def candidate_rels(wt: dict) -> list[str]:
        """Where a worktree is (or was put) below its repository: its own path when it lay inside the repository directory,
        else ``.claude/worktrees/<name>`` or, when that name was taken, ``<name>-outside``, ``<name>-outside-2`` ..."""
        if not Layout.is_outside(wt):
            return [wt["rel"]]
        natural = f".claude/worktrees/{wt['key']}"
        return [natural] + [natural + "-outside" + (f"-{n}" if n > 1 else "") for n in range(1, 10)]

    def checkout_matches(self, path: Path, repo: dict, key: str) -> str:
        """'' when the checkout at ``path`` is on the branch (or detached at the commit) the manifest records for ``key``, else
        what differs. Work is never applied to a checkout that is not the one it came from."""
        if key == "_main":
            want_branch, want_head = repo.get("branch"), repo.get("head")
        else:
            wt = next((w for w in repo["worktrees"] if w["key"] == key), None)
            if wt is None:
                return "is not a worktree of the manifest"
            want_branch, want_head = wt.get("branch"), wt.get("head")
        head, branch = head_and_branch(path)
        if want_branch and branch != want_branch:
            return f"is on {branch or 'a detached HEAD'}, the manifest records {want_branch}"
        if not want_branch and branch and want_head:
            return f"is on branch {branch}, the manifest records a detached HEAD"
        if want_head and head != want_head:
            return f"is at {head[:10] or '(no commit)'}, the manifest records {want_head[:10]}"
        return ""

    def tree_path(self, repo: dict, key: str) -> Path | None:
        """The checkout that holds ``key``'s work: the main one, a worktree recreated in this run, or (when verifying) a
        directory that is verifiably that worktree: same branch and same HEAD as the manifest records."""
        dest = self.repo_dest(repo)
        if key == "_main":
            return dest
        wt = next((w for w in repo["worktrees"] if w["key"] == key), None)
        if wt is None or wt.get("prunable"):
            return None
        got = self.placed.get((repo["name"], key))
        if got is not None or self.strict:
            return got
        for rel in self.candidate_rels(wt):
            p = dest.joinpath(*rel.split("/"))
            if (p / ".git").exists() and not self.checkout_matches(p, repo, key):
                return p
        return None

    def new_path(self, source_path: str) -> str:
        return self.rewriter.rewrite(source_path)[0]

    def slug_map(self) -> dict[str, str]:
        """old project slug -> new project slug, for every known path (repo, worktree, root, home, directory)."""
        if self._slugs is None:
            self._slugs = {c.project_slug(k["path"]): c.project_slug(self.new_path(k["path"])) for k in self.m.get("known_paths", [])}
        return self._slugs

    def account_dir(self, vendor: str, account: str, source_dir: str) -> Path:
        mapped = self.new_path(source_dir)
        if mapped != source_dir:
            return Path(mapped)
        stem = ".claude" if vendor == "claude" else ".codex"
        return self.new_home / (stem if account == "main" else f"{stem}-{account}")

    def staged_user_rel(self, rel: str) -> str:
        """Bundle path ``user/...`` -> path below ``.migration/user``: a memory directory takes its new project slug."""
        parts = rel.split("/")
        if len(parts) > 5 and parts[1] == "claude" and parts[3] == "projects" and parts[5] == "memory":
            parts[4] = self.slug_map().get(parts[4], parts[4])
        return "/".join(parts[1:]) if parts[0] == "user" else rel


def head_and_branch(path: Path) -> tuple[str, str]:
    """(HEAD commit, branch name or '' when detached) of a checkout."""
    return (c.git(path, "rev-parse", "HEAD").stdout.strip(), c.git(path, "symbolic-ref", "--quiet", "--short", "HEAD").stdout.strip())


def expected(rewriter: rw.Rewriter, data: bytes, name: str = "") -> bytes:
    return rewriter.rewrite_bytes(data, name)[0]


# ------------------------------------------------------------------ files

def place(src: Path, dest: Path, rewriter: rw.Rewriter, label: str, *, rewrite: bool = True) -> dict:
    """Copy one carried file to ``dest`` with path rewriting (text only). A differing existing file gets ``.migrated``."""
    if os.stat(c.lp(src)).st_size >= rw.MAX_TEXT_BYTES:                          # too large for the rewriter: streamed, never read whole
        rec = {"file": label, "dest": str(dest), "replacements": 0, "left_alone": "larger than 2 MB", "status": "", "needs": []}
        target = dest
        if os.path.lexists(c.lp(dest)):
            if c.same_content(src, dest):
                rec["status"] = "same"
                return rec
            target = dest.with_name(dest.name + ".migrated")
            rec["status"] = "migrated"
            rec["written"] = str(target)
        else:
            rec["status"] = "written"
        c.copy_file(src, target)
        c.exec_if_script(target)
        return rec
    data = open(c.lp(src), "rb").read()
    if rewrite:
        new, count, why = rewriter.rewrite_bytes(data, dest.name)
    else:
        new, count, why = data, 0, "not rewritten"
    rec = {"file": label, "dest": str(dest), "replacements": count, "left_alone": why, "status": "", "needs": []}
    if not why and count >= 0:
        rec["needs"] = rw.needs_person(new.decode("utf-8"))
    elif why == rw.JSON_BROKEN:
        rec["needs"] = [(1, why + ": edit its Windows paths by hand")]
    if not why and not dest.name == "mcpServers.redacted.json":
        keys = secrets.redacted_keys(new, dest.name)
        if keys:
            rec["needs"].append((1, "values were redacted in the bundle and are not set here; set again: " + ", ".join(keys[:12])
                                 + (f" (+{len(keys) - 12} more)" if len(keys) > 12 else "")))
    target = dest
    if os.path.lexists(c.lp(dest)):
        try:
            same = open(c.lp(dest), "rb").read() == new
        except OSError:
            same = False
        if same:
            rec["status"] = "same"
            return rec
        target = dest.with_name(dest.name + ".migrated")
        rec["status"] = "migrated"
        rec["written"] = str(target)
    else:
        rec["status"] = "written"
    c.write_bytes(target, new)
    c.exec_if_script(target)
    return rec


# ------------------------------------------------------------------ git

def _git(path: Path, *args, timeout: float = 600):
    return c.git(path, *args, timeout=timeout)


_FILES_FORMAT: bool | None = None


def init_repo(dest: Path) -> subprocess.CompletedProcess:
    """``git init``. The refs are written as a packed-refs file, which only the files ref format reads, so a git whose default
    is reftable is asked for the files format (probed once: an older git has only that format and no such option)."""
    global _FILES_FORMAT
    if _FILES_FORMAT is not False:
        cp = _git(dest, "init", "-q", "--ref-format=files")
        if cp.returncode == 0:
            _FILES_FORMAT = True
            return cp
        if _FILES_FORMAT is None and re.search(r"(?i)unknown option|ref-format|usage:", cp.stderr):
            _FILES_FORMAT = False
        else:
            return cp
    return _git(dest, "init", "-q")


def _write_packed_refs(dest: Path, refs: dict[str, str]) -> None:
    lines = ["# pack-refs with: sorted"]
    for ref in sorted(refs, key=lambda r: r.encode("utf-8")):
        lines.append(f"{refs[ref]} {ref}")
    c.write_bytes(dest / ".git" / "packed-refs", ("\n".join(lines) + "\n").encode("utf-8"))


def compare_refs(dest: Path, entry: dict) -> list[str]:
    """Gaps between the repository's refs and the manifest's: missing, different, or a different count."""
    cp = _git(dest, "for-each-ref", "--format=%(refname)%09%(objectname)")
    if cp.returncode != 0:
        return [f"git for-each-ref failed: {cp.stderr.strip()[:200]}"]
    have = dict(line.split("\t", 1) for line in cp.stdout.splitlines() if "\t" in line)
    gaps = []
    for ref, sha in entry["refs"].items():
        if ref not in have:
            gaps.append(f"ref {ref} is missing")
        elif have[ref] != sha:
            gaps.append(f"ref {ref} is at {have[ref][:10]}, the manifest has {sha[:10]}")
    if len(have) < len(entry["refs"]):
        gaps.append(f"{len(have)} refs present, {len(entry['refs'])} recorded")
    return gaps


def stash_list(dest: Path) -> list[tuple[str, str]]:
    """(sha, message) of every stash entry, newest first."""
    cp = _git(dest, "stash", "list", "--format=%H%x09%gs")
    out = []
    for line in cp.stdout.splitlines() if cp.returncode == 0 else []:
        sha, _, msg = line.partition("\t")
        out.append((sha, msg))
    return out


def _is_local_path(url: str) -> bool:
    return bool(re.match(r"^(?:[A-Za-z]:[\\/]|/|\\\\|\.)", url)) and "://" not in url


WORKTREE_HEADS = "refs/migrate/worktree-heads/"


def kept_dir(dest: Path, key: str) -> Path:
    """Where the work of a checkout that could not be recreated is kept (patches, untracked and local-only files)."""
    return dest / ".baton" / "migrated-patches" / key


def _discard_fresh(dest: Path, created: bool) -> str:
    """After a failure before the checkout: remove what this run made (the new directory, or the fresh ``.git`` in an empty one)
    so the import can be run again. Nothing is removed from a directory that holds anything else."""
    try:
        names = set(os.listdir(dest))
    except OSError:
        return ""
    if not names <= {".git"}:
        return f"{dest} holds more than the fresh .git and was left for inspection"
    try:
        c.rmtree(dest if created else dest / ".git")
    except OSError as exc:
        return f"could not remove what this run created in {dest}: {exc}"
    return "what this run created was removed, so the import can be run again"


def import_repo(lay: Layout, src: Path, entry: dict) -> dict:
    name = entry["name"]
    dest = lay.repo_dest(entry)
    res: dict = {"name": name, "dest": str(dest), "status": "", "refs": [], "worktrees": [], "dirty": {}, "stashes": [], "overlay": [],
                 "notes": [], "needs": [], "problems": []}
    if dest.exists() and (not dest.is_dir() or any(dest.iterdir())):
        res["status"] = "refused: the target directory exists and is not empty; nothing was touched"
        return res
    created = not dest.exists()

    def fail(why: str) -> dict:
        res["status"] = "failed: " + why
        res["notes"].append(_discard_fresh(dest, created))
        res["refs"] = []
        return res

    dest.mkdir(parents=True, exist_ok=True)
    cp = init_repo(dest)
    if cp.returncode != 0:
        return fail("git init: " + cp.stderr.strip()[:200])
    if entry["bundle"]:
        cp = _git(dest, "bundle", "unbundle", str(src / entry["bundle"]))
        if cp.returncode != 0:
            return fail("git bundle unbundle: " + cp.stderr.strip()[:300])
        # Objects only so far. Refs are written as packed refs (never loose files), so refs that differ only by case,
        # or one being a path prefix of another in a different case, cannot collide on a case-insensitive disk. refs/stash is
        # left to `git stash store` below, which gives the stash its whole reflog.
        skip = set(entry["symrefs"]) | ({"refs/stash"} if entry["stashes"] else set())
        _write_packed_refs(dest, {r: s for r, s in entry["refs"].items() if r not in skip})
        for ref, target in entry["symrefs"].items():
            _git(dest, "symbolic-ref", ref, target)
    for i, (k, v) in enumerate(entry["config"]):
        seen = [kk for kk, _ in entry["config"][:i]]
        _git(dest, "config", "--local", *(["--add"] if k in seen else []), k, v)
        if k.endswith(".url") and _is_local_path(v):
            res["needs"].append(f"{k} = {v} is a filesystem path of the old machine")
        if re.match(r"remote\..+\.(push)?url$", k) and userconf.REDACTED in v:
            res["needs"].append(f"{k} had credentials in it on the old machine; they were not carried, so it is set to {v}: "
                                f"put the credentials back (git remote set-url, or a credential helper)")
    for fname in ("exclude", "attributes"):
        meta = entry.get("info_" + fname)
        if meta:
            c.copy_file(src / meta["file"], dest / ".git" / "info" / fname)
    _import_lfs(src, entry, dest)
    if entry["branch"]:                      # also for a repository with no commits yet: HEAD names its unborn branch
        _git(dest, "symbolic-ref", "HEAD", f"refs/heads/{entry['branch']}")
    if entry["bundle"]:
        if not entry["branch"] and entry["head"]:
            _git(dest, "update-ref", "--no-deref", "HEAD", entry["head"])
        cp = _git(dest, "reset", "--hard", "-q", timeout=1800)
        if cp.returncode != 0:
            return fail("the checkout (git reset --hard) failed: " + cp.stderr.strip()[:300])
    for s in reversed(entry["stashes"]):     # oldest first, so `git stash list` shows the old machine's entries in the old order
        cp = _git(dest, "stash", "store", "-m", s["message"], s["sha"])
        if cp.returncode != 0:
            res["problems"].append(f"stash {s['n']} ({s['sha'][:10]}) could not be stored: {cp.stderr.strip()[:200]}")
    res["refs"] = compare_refs(dest, entry)
    if (dest / ".git").is_dir():
        res["hooks_not_carried"] = entry["hooks"]
    # worktrees inside the repository directory first, at their own paths, which stay reserved: one that lay outside it never
    # takes the place of an inside one
    reserved = {w["rel"] for w in entry["worktrees"] if not Layout.is_outside(w) and not w.get("prunable")}
    for wt in sorted(entry["worktrees"], key=lambda w: (Layout.is_outside(w), w.get("rel") or "", w["key"])):
        rec = _import_worktree(lay, dest, entry, wt, reserved)
        res["worktrees"].append(rec)
        if rec.get("problem"):
            res["problems"].append(f"worktree {rec['key']}: {rec['problem']}")
    # uncommitted work, stashes
    wrong: dict[str, str] = {}              # key -> why the checkout found is not the one the work came from
    for key in ["_main"] + [w["key"] for w in entry["worktrees"]]:
        p = lay.tree_path(entry, key)
        if p is not None and p.is_dir():
            why = lay.checkout_matches(p, entry, key) if entry["bundle"] else ""
            if why:
                wrong[key] = f"the checkout {p} {why}"
    for key, d in entry["dirty"].items():
        path = lay.tree_path(entry, key)
        has_work = bool(d["tracked_patch"] or d["untracked"])
        if path is None or not path.is_dir() or key in wrong:
            if has_work:
                kept = _keep_unrecreated(src, entry, dest, key, d)
                res["dirty"][key] = kept
                res["problems"].append(f"the work of {key} was not applied: " + (wrong[key] if key in wrong else "it has no checkout on this machine")
                                       + f"; it was kept as files under {kept['kept']}")
            else:
                res["dirty"][key] = {"tracked": "none", "kept": "", "untracked": 0, "untracked_migrated": []}
            continue
        res["dirty"][key] = _import_dirty(lay, src, entry, key, path, d)
        if res["dirty"][key]["tracked"] == "kept":
            res["problems"].append(f"the tracked changes of {key} did not apply and were kept at {res['dirty'][key]['kept']}")
    res["stashes"] = _keep_stashes(src, entry, dest)
    # local-only overlay
    for f in entry["overlay"]:
        path = lay.tree_path(entry, f["tree"])
        label = f"{name}/{f['tree']}/{f['rel']}"
        try:
            if path is None or not path.is_dir() or f["tree"] in wrong:
                rec = place(src / f["file"], kept_dir(dest, f["tree"]) / "overlay" / Path(*f["rel"].split("/")), lay.rewriter, label)
                rec["status"] = "kept: the checkout is missing (" + rec["status"] + ")"
            else:
                rec = place(src / f["file"], path.joinpath(*f["rel"].split("/")), lay.rewriter, label)
        except Exception as exc:  # noqa: BLE001 - one unwritable file must not stop the rest
            rec = {"file": label, "status": f"failed: {type(exc).__name__}: {exc}", "replacements": 0, "needs": []}
            res["problems"].append(f"overlay file {label} could not be placed: {type(exc).__name__}: {exc}")
        res["overlay"].append(rec)
    res["status"] = "imported" if not res["refs"] and not res["problems"] else "imported with gaps"
    return res


def _import_lfs(src: Path, entry: dict, dest: Path) -> None:
    """The LFS objects of the old machine's ``.git/lfs/objects``, so a checkout with git-lfs installed finds them."""
    meta = entry.get("lfs_objects")
    if not meta:
        return
    base = src / meta["prefix"]
    for dirpath, _, names in os.walk(c.lp(base)):
        for n in names:
            rel = os.path.relpath(os.path.join(dirpath, n), c.lp(base))
            target = dest / ".git" / "lfs" / "objects" / rel
            if not os.path.lexists(c.lp(target)):
                c.copy_file(os.path.join(dirpath, n), target)


def _import_worktree(lay: Layout, dest: Path, entry: dict, wt: dict, reserved: set[str] | None = None) -> dict:
    """Recreate one worktree. One outside the repository directory comes back under ``.claude/worktrees/<name>`` when that is
    free. A worktree that is not recreated keeps its HEAD alive as ``refs/migrate/worktree-heads/<name>``."""
    key = wt["key"]
    rec = {"key": key, "rel": wt.get("rel"), "branch": wt["branch"], "status": "", "recreated": False, "problem": ""}
    if wt.get("prunable"):
        rec["status"] = "not recreated: its directory was missing on the old machine" + (" (detached HEAD kept as a ref)" if wt["detached"] else "")
    else:
        outside = Layout.is_outside(wt)
        free = lambda r: not (dest.joinpath(*r.split("/")).exists() and (not dest.joinpath(*r.split("/")).is_dir()        # noqa: E731
                                                                         or any(dest.joinpath(*r.split("/")).iterdir())))
        if outside:
            rel = next((r for r in Layout.candidate_rels(wt) if r not in (reserved or ()) and free(r)), "")
        else:
            rel = wt["rel"]
        target = dest.joinpath(*rel.split("/")) if rel else dest
        if not rel or not free(rel):
            rec["status"] = "refused: " + ("no free place for it below .claude/worktrees" if outside else "target exists and is not empty")
            rec["problem"] = rec["status"]
        else:
            if wt["branch"]:
                args = ["worktree", "add", "-q", rel, wt["branch"]]
            else:
                args = ["worktree", "add", "-q", "--detach", rel, wt["head"]]
            cp = _git(dest, *args, timeout=1800)
            if cp.returncode == 0:
                rec["recreated"] = True
                lay.placed[(entry["name"], key)] = target
                rec["rel"] = rel
                rec["status"] = "created" + (f" (it was outside the repository directory; now {rel})" if outside else "")
            else:
                rec["status"] = "failed: " + cp.stderr.strip()[:300]
                rec["problem"] = rec["status"]
    if not rec["recreated"] and wt.get("head"):
        cp = _git(dest, "update-ref", WORKTREE_HEADS + key, wt["head"])
        rec["head_ref"] = WORKTREE_HEADS + key if cp.returncode == 0 else ""
        if cp.returncode != 0:
            rec["problem"] = rec["problem"] or f"its HEAD {wt['head'][:10]} could not be kept as a ref: {cp.stderr.strip()[:200]}"
    return rec


def _apply_patch(path: Path, patch: Path):
    return _git(path, *c.APPLY_CONFIG, "apply", "--binary", "-p1", str(patch))


def _import_dirty(lay: Layout, src: Path, entry: dict, key: str, path: Path, d: dict) -> dict:
    out = {"tracked": "none", "kept": "", "untracked": 0, "untracked_migrated": []}
    meta = d["tracked_patch"]
    if meta:
        patch = (src / meta["file"]).resolve()
        cp = _apply_patch(path, patch)
        if cp.returncode == 0:
            out["tracked"] = "applied"
        else:
            kept = path / ".baton" / "migrated-patches" / "tracked.patch"
            c.copy_file(patch, kept)
            out["tracked"] = "kept"
            out["kept"] = str(kept)
            out["error"] = cp.stderr.strip()[:300]
    for f in d["untracked"]:
        target = path.joinpath(*f["rel"].split("/"))
        if os.path.lexists(c.lp(target)):
            if c.sha256_file(target) == f["sha256"]:
                out["untracked"] += 1
                continue
            target = target.with_name(target.name + ".migrated")
            out["untracked_migrated"].append(f["rel"])
        c.copy_file(src / f["file"], target)
        c.exec_if_script(target)
        out["untracked"] += 1
    return out


def _keep_unrecreated(src: Path, entry: dict, dest: Path, key: str, d: dict) -> dict:
    """The work of a checkout that does not exist on this machine, kept as files under ``.baton/migrated-patches/<key>/``."""
    base = kept_dir(dest, key)
    out = {"tracked": "none", "kept": str(base), "untracked": 0, "untracked_migrated": []}
    if d["tracked_patch"]:
        c.copy_file(src / d["tracked_patch"]["file"], base / "tracked.patch")
        out["tracked"] = "kept"
    for f in d["untracked"]:
        c.copy_file(src / f["file"], base / "untracked" / Path(*f["rel"].split("/")))
        out["untracked"] += 1
    return out


def _keep_stashes(src: Path, entry: dict, dest: Path) -> list[dict]:
    """The stash entries as patches, a second copy beside the entries `git stash store` restored."""
    out = []
    for s in entry["stashes"]:
        rec = {"n": s["n"], "message": s["message"], "base": s["base"], "files": []}
        for key, label in (("patch", "tracked"), ("untracked_patch", "untracked")):
            meta = s.get(key)
            if meta:
                target = dest / ".baton" / "migrated-stashes" / Path(meta["file"]).name
                c.copy_file(src / meta["file"], target)
                rec["files"].append(str(target))
        out.append(rec)
    if out:
        lines = [f"stash {s['n']}: base {s['base']}: {s['message']}" for s in out]
        c.write_bytes(dest / ".baton" / "migrated-stashes" / "README.txt",
                      ("Stash entries of the old machine as patches. They were also stored again: `git stash list` shows them in the old order.\n"
                       "To use a patch instead: `git apply --binary <file>` on a checkout of its base.\n" + "\n".join(lines) + "\n").encode("utf-8"))
    return out


# ------------------------------------------------------------------ root, directories, aliases, user configuration

def import_root(lay: Layout, src: Path) -> list[dict]:
    out = []
    for f in lay.m["root"]["overlay"]:
        out.append(place(src / f["file"], lay.new_root.joinpath(*f["rel"].split("/")), lay.rewriter, f"(root)/{f['rel']}"))
    for d in lay.m.get("dirs", []):
        base = lay.base(d["base"]).joinpath(*d["target"].split("/"))
        for f in d["files"]:
            out.append(place(src / f["file"], base.joinpath(*f["rel"].split("/")), lay.rewriter, f"({d['kind']}) {d['target']}/{f['rel']}"))
    return out


def import_aliases(lay: Layout) -> list[dict]:
    out = []
    for a in lay.m.get("aliases", []):
        link = lay.new_root / a["name"]
        rec = {"name": a["name"], "target": a["target_in_root"], "status": ""}
        if not a["target_in_root"]:
            rec["status"] = f"not carried: points outside the root ({a['target']})"
        elif os.path.lexists(link):
            rec["status"] = "exists" if c.real(link) == c.real(lay.new_root / a["target_in_root"]) else "refused: a different file or directory is in the way"
        elif not (lay.new_root / a["target_in_root"]).exists():
            rec["status"] = f"failed: the target {a['target_in_root']} does not exist under the new root"
        else:
            rec["status"] = make_alias(link, a["target_in_root"], lay.new_root / a["target_in_root"])
        out.append(rec)
    return out


def make_alias(link: Path, rel_target: str, abs_target: Path) -> str:
    try:
        os.symlink(rel_target, link, target_is_directory=True)
        return "created (symlink)"
    except OSError as exc:
        if os.name == "nt":
            import subprocess
            cp = subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(abs_target)], capture_output=True, text=True,
                                stdin=subprocess.DEVNULL, creationflags=0x08000000)
            if cp.returncode == 0:
                return "created (junction)"
        return f"failed: {exc}"


def proposed_settings(rewritten: dict) -> tuple[dict, list[str]]:
    """settings.proposed.json: env, statusLine, enabledPlugins, additionalDirectories and the permission rules that no longer
    look Windows-specific; no hooks. Returns (document, dropped rules)."""
    out: dict = {}
    for k in ("env", "statusLine", "enabledPlugins"):
        if k in rewritten:
            out[k] = rewritten[k]
    if isinstance(out.get("env"), dict):          # a redacted value is not a setting: the name is listed in the report instead
        out["env"] = {k: v for k, v in out["env"].items() if v != userconf.REDACTED}
        if not out["env"]:
            del out["env"]
    perms = rewritten.get("permissions") if isinstance(rewritten.get("permissions"), dict) else {}
    dropped = []
    keep: dict = {}
    for k, v in perms.items():
        if k in ("allow", "deny", "ask") and isinstance(v, list):
            keep[k] = []
            for rule in v:
                if isinstance(rule, str) and userconf.WINDOWSY.search(rule):
                    dropped.append(rule)
                else:
                    keep[k].append(rule)
        else:
            keep[k] = v
    if keep:
        out["permissions"] = keep
    if isinstance(rewritten.get("additionalDirectories"), list):
        out["additionalDirectories"] = rewritten["additionalDirectories"]
    return out, dropped


def stage_user(lay: Layout, src: Path) -> dict:
    """User configuration into NEWROOT/.migration/user (rewritten), the proposed settings, then the report pieces."""
    res: dict = {"files": [], "proposed": [], "dropped_rules": {}, "old_hooks": {}, "mcp_to_login": [], "memory": [], "unmatched_memory": []}
    stage = lay.new_root / STAGE / "user"
    slugs = lay.slug_map()
    for f in lay.m["user"]["files"]:
        rel = lay.staged_user_rel(f["file"])
        res["files"].append(place(src / f["file"], stage.joinpath(*rel.split("/")), lay.rewriter, f["file"]))
    for m in lay.m["user"]["memory"]:
        new = slugs.get(m["slug"])
        res["memory"].append({"account": m["account"], "old": m["slug"], "new": new or m["slug"], "files": m["files"], "matched": bool(new)})
        if not new:
            res["unmatched_memory"].append(m["slug"])
    for acct in lay.m["user"]["accounts"]:
        if acct["vendor"] != "claude":
            continue
        base = stage / "claude" / acct["account"]
        path = base / "settings.json"
        if not path.is_file():
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            res["proposed"].append({"account": acct["account"], "status": "settings.json is not valid JSON after rewriting; no proposal"})
            continue
        proposal, dropped = proposed_settings(doc)
        c.write_bytes(base / "settings.proposed.json", (json.dumps(proposal, indent=2) + "\n").encode("utf-8"))
        res["proposed"].append({"account": acct["account"], "status": "written", "path": str(base / "settings.proposed.json")})
        res["dropped_rules"][acct["account"]] = dropped
        env = doc.get("env") if isinstance(doc.get("env"), dict) else {}
        res.setdefault("redacted_env", {})[acct["account"]] = sorted(k for k, v in env.items() if v == userconf.REDACTED)
        res["old_hooks"][acct["account"]] = userconf.hook_commands({"hooks": doc.get("hooks") or {}})
    for f in lay.m["user"]["files"]:
        if f["file"].endswith("mcpServers.redacted.json"):
            doc = json.loads((src / f["file"]).read_text(encoding="utf-8"))
            names = set()
            for scope in [doc.get("global", {}), *doc.get("projects", {}).values()]:
                for n, s in scope.items():
                    if REDACT_MARK in json.dumps(s):
                        names.add(n)
            res["mcp_to_login"] += [{"file": f["file"], "server": n} for n in sorted(names)]
    return res


REDACT_MARK = userconf.REDACTED


APPLY = {"claude": ("CLAUDE.md", "keybindings.json", "agents", "skills", "hooks", "commands", "output-styles"),
         "codex": ("AGENTS.md", "skills", "rules", "prompts")}


def baton_dest(lay: Layout) -> Path:
    """Where Baton's own checkout is on the new machine (its ``.state`` is below it)."""
    entry = lay.m["repos"].get("baton")
    return lay.repo_dest(entry) if entry else lay.new_root / "baton"


def stage_baton_state(lay: Layout, src: Path) -> list[dict]:
    """The queue's host configuration and ``startup.toml`` into ``<root>/.migration/baton/...`` (paths rewritten)."""
    stage = lay.new_root / STAGE
    return [place(src / f["file"], stage.joinpath(*f["file"].split("/")), lay.rewriter, f["file"])
            for f in lay.m.get("baton_state", {}).get("files", [])]


def apply_baton_state(lay: Layout, staged: list[dict]) -> list[dict]:
    """Place the staged files under ``<baton>/.state/`` only where nothing exists (never overwritten)."""
    out = []
    base = baton_dest(lay) / ".state"
    actual = {r["file"]: Path(r.get("written") or r["dest"]) for r in staged}       # where place() really put this run's content
    for f in lay.m.get("baton_state", {}).get("files", []):
        rel = f["file"][len("baton/"):]
        dest = base.joinpath(*rel.split("/"))
        label = f"baton state {rel}"
        if os.path.lexists(c.lp(dest)):
            out.append({"file": label, "status": "already present", "dest": str(dest)})
            continue
        s = actual.get(f["file"]) or lay.new_root / STAGE / Path(*f["file"].split("/"))
        c.write_bytes(dest, open(c.lp(s), "rb").read())
        out.append({"file": label, "status": "placed", "dest": str(dest)})
    return out


def apply_user(lay: Layout, staged: dict) -> list[dict]:
    """Copy staged configuration into the real account directories, never overwriting a differing file; settings.json only
    when the account has none."""
    out = []
    stage = lay.new_root / STAGE / "user"
    # A staged file that already differed got this run's content in a ``.migrated`` sibling (see ``place``): that sibling is
    # what is applied, in the place of the older staged file, and it is not applied a second time under its own name.
    actual = {str(r["dest"]): Path(r["written"]) for r in staged.get("files", []) if r.get("written")}
    siblings = {str(v) for v in actual.values()}
    for acct in lay.m["user"]["accounts"]:
        vendor, name = acct["vendor"], acct["account"]
        adir = lay.account_dir(vendor, name, acct["dir"])
        base = stage / vendor / name
        if not base.is_dir():
            continue
        for item in APPLY[vendor]:
            p = base / item
            files = [p] if p.is_file() else [x for x in p.rglob("*") if x.is_file()] if p.is_dir() else []
            for f in files:
                if str(f) in siblings:
                    continue
                rel = f.relative_to(base).as_posix()
                out.append(_apply_one(actual.get(str(f), f), adir / rel, f"{vendor}/{name}/{rel}"))
        if vendor == "claude":
            proj = base / "projects"
            matched = set(lay.slug_map().values())          # memory of unmatched workspaces stays staged
            if proj.is_dir():
                for f in proj.rglob("*"):
                    if f.is_file() and str(f) not in siblings and f.relative_to(proj).parts[0] in matched and "memory" in f.relative_to(proj).parts:
                        rel = f.relative_to(base).as_posix()
                        out.append(_apply_one(actual.get(str(f), f), adir / rel, f"claude/{name}/{rel}"))
            settings = adir / "settings.json"
            proposed = base / "settings.proposed.json"
            if proposed.is_file() and not settings.exists():
                out.append(_apply_one(proposed, settings, f"claude/{name}/settings.json (from the proposal)"))
            elif proposed.is_file():
                out.append({"file": f"claude/{name}/settings.json", "status": "left: settings.json exists; settings.proposed.json stays staged"})
    return out


def _apply_one(src: Path, dest: Path, label: str) -> dict:
    if os.stat(c.lp(src)).st_size >= rw.MAX_TEXT_BYTES:                          # streamed
        if os.path.lexists(c.lp(dest)):
            if c.same_content(src, dest):
                return {"file": label, "status": "same"}
            c.copy_file(src, dest.with_name(dest.name + ".migrated"))
            return {"file": label, "status": "migrated", "written": str(dest) + ".migrated"}
        c.copy_file(src, dest)
        return {"file": label, "status": "written"}
    data = open(c.lp(src), "rb").read()
    if os.path.lexists(c.lp(dest)):
        if open(c.lp(dest), "rb").read() == data:
            return {"file": label, "status": "same"}
        c.write_bytes(dest.with_name(dest.name + ".migrated"), data)
        return {"file": label, "status": "migrated", "written": str(dest) + ".migrated"}
    c.write_bytes(dest, data)
    return {"file": label, "status": "written"}


# ------------------------------------------------------------------ the run

def checklist(lay: Layout, src: Path, result: dict) -> list[str]:
    """What is left to do on the new machine, in order. (The transfer, `sh mac-start.sh` and the import itself are done.)"""
    m = lay.m
    items = []
    items.append("Before the first session on this Mac: every Claude Code session on the Windows machine must be closed. The compute queue's "
                 "leader is chosen per state directory, so a session open on Windows and one open here are two controllers submitting to "
                 "the same hosts. Remove the Windows schedule and start-at-login there: `bt night schedule remove` and `bt startup uninstall`.")
    items.append(f"Install the tools: `brew bundle --file {src / 'baton' / 'Brewfile'}` (review it first; it is a proposal).")
    audit = {}
    try:
        audit = json.loads((src / "baton" / "audit.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        pass
    restore = []
    for r in audit.get("repos", []):
        if r["name"] not in m["repos"]:
            continue
        dest = lay.repo_dest(m["repos"][r["name"]])
        for t in r.get("toolchains", []):
            restore.append(f"`cd {dest} && {t['restore']}`")
    items.append("Per-repository restore commands (suggestions from the audit; nothing was run):" + ("".join(f"\n   - {x}" for x in restore) or " none detected"))
    lfs = [lay.repo_dest(e) for e in m["repos"].values() if e.get("lfs")]
    if lfs:
        items.append("Git LFS: `git lfs install`, then `git lfs checkout` in " + ", ".join(str(d) for d in lfs)
                     + " (the objects that were on the old machine were carried into .git/lfs/objects; the rest are fetched with `git lfs pull`).")
    logins = ["`claude` (then /login) for each account", "`codex login` for each account", "`gh auth login`"]
    mcp = result["user"]["mcp_to_login"]
    logins += [f"MCP server `{x['server']}` ({x['file']}): environment values, headers and URL query strings were redacted; set them again" for x in mcp]
    items.append("Log in again: " + "; ".join(logins[:3]) + "".join(f"\n   - {x}" for x in logins[3:]))
    ssh = m["user"]["ssh"]
    items.append("ssh: copy these key files yourself (they are never carried): " + (", ".join(ssh["key_names"]) or "none")
                 + "; authorise this Mac's key on hosts: " + (", ".join(h["alias"] for h in ssh["hosts"]) or "none")
                 + f"; staged config: {lay.new_root / STAGE / 'user' / 'ssh' / 'config'}")
    by_hand = m.get("secrets_by_hand", [])
    items.append("Secret-looking files that were not carried (copy by hand): "
                 + ("".join(f"\n   - {s['where']}" + (f" ({s['why']})" if s.get("why") else "") for s in by_hand) or "none"))
    old = result["user"]["old_hooks"]
    hook_lines = [f"{acct}: {h['event']} {h['matcher']}: {h['command']}" for acct, hs in old.items() for h in hs]
    baton = lay.new_root / "baton" / "bt.py"
    items.append(f"Install Baton's hooks and agents with `bt install` (`bt` does not exist yet: `python3 {baton} install`; Baton's own repository "
                 f"was imported to {baton.parent}). It writes Baton's hooks, session-workflow's hooks and the `bt` line in ~/.zshrc (~/.bashrc "
                 f"when $SHELL is bash); open a new terminal afterwards. Then `bt routing apply`. settings.proposed.json contains no hooks; the old machine's hooks, for reference:"
                 + "".join(f"\n   - {x}" for x in hook_lines[:30]))
    enrolled = []
    try:
        enrolled = json.loads((src / "baton" / "night" / "enrolled.json").read_text(encoding="utf-8")).get("worktrees", [])
    except (OSError, ValueError):
        pass
    items.append("Re-make night enrolments (they are keyed by path): "
                 + ("".join(f"\n   - `cd {lay.new_path(e['root'])} && bt night enroll`" for e in enrolled) or "none were enrolled"))
    items.append("Run `bt doctor`.")
    items.append("Install the evening task and start-at-login on this Mac: `bt night schedule install` and `bt startup install` "
                 "(the Windows ones were removed in the first step).")
    items.append(f"Check the import against the bundle: `python3 {baton} migrate verify --from {src} --root {lay.new_root}` "
                 "(it must print `no gaps`; a patch that was kept instead of applied, or a checkout that does not match, is a gap).")
    needs = [f"{r['file']}:{n}: {line}" for r in result["files_all"] for n, line in r.get("needs", [])]
    needs += [f"{r['name']}: {line}" for r in result["repos"] for line in r["needs"]]
    needs += [f"{r['name']}: {line}" for r in result["repos"] for line in r.get("problems", [])]
    items.append("Needs a person (still look Windows-specific or could not be done; not auto-fixed): " + ("".join(f"\n   - {x}" for x in needs[:80]) or "none")
                 + (f"\n   - ... {len(needs) - 80} more in import.json" if len(needs) > 80 else ""))
    return items


def render_report(lay: Layout, result: dict, items: list[str]) -> str:
    L = ["# Import report", "", f"Bundle created {lay.m['created']} on {lay.m['source']['platform']}; imported under `{lay.new_root}`, home `{lay.new_home}`.", ""]
    L += ["## Repositories", ""]
    for r in result["repos"]:
        failed = r["status"].startswith(("failed", "refused"))
        tail = "" if failed else (f"; ref gaps: {r['refs']}" if r["refs"] else "; every ref matches the manifest")
        L.append(f"- `{r['name']}` -> `{r['dest']}`: {r['status']}{tail}")
        for w in r["worktrees"]:
            L.append(f"  - worktree {w['key']}: {w['status']}")
            if w.get("head_ref"):
                L.append(f"  - worktree {w['key']}: its HEAD is kept as `{w['head_ref']}` (nothing else references it)")
        for k, d in r["dirty"].items():
            if d["tracked"] != "none" or d["untracked"]:
                L.append(f"  - uncommitted work in {k}: tracked patch {d['tracked']}" + (f" (kept at {d['kept']}: {d.get('error', '')})" if d.get("kept") else "")
                         + f"; {d['untracked']} untracked file(s)")
        for s in r["stashes"]:
            L.append(f"  - stash {s['n']} kept as patch: {s['message']} (base {s['base'][:10]}): {', '.join(s['files'])}")
        for n in r["notes"]:
            L.append(f"  - note: {n}")
        for p in r.get("problems", []):
            L.append(f"  - NEEDS A PERSON: {p}")
        if r.get("notes_written"):
            L.append(f"  - .baton/migration-notes.md written in {len(r['notes_written'])} checkout(s): the first sessions there are pointed at it")
    L += ["", "## Files", ""]
    mig = [r for r in result["files_all"] if r["status"] == "migrated"]
    L.append(f"{len(result['files_all'])} files placed; {sum(r['replacements'] for r in result['files_all'])} path replacements; "
             f"{len(mig)} existing file(s) differed and were not overwritten (a `.migrated` file sits beside each):")
    L += [f"- {r['written']}" for r in mig]
    L += ["", "## Aliases", ""] + [f"- `{a['name']}` -> `{a['target']}`: {a['status']}" for a in result["aliases"]]
    L += ["", "## User configuration", "", f"Staged under `{lay.new_root / STAGE / 'user'}`.", ""]
    for p in result["user"]["proposed"]:
        L.append(f"- settings.proposed.json for {p['account']}: {p['status']}")
    for acct, dropped in result["user"]["dropped_rules"].items():
        L.append(f"- {acct}: {len(dropped)} permission rule(s) dropped because they still look Windows-specific")
    for acct, names in result["user"].get("redacted_env", {}).items():
        if names:
            L.append(f"- {acct}: settings.json env values were not carried; set these again on the Mac: {', '.join(names)}")
    for mm in result["user"]["memory"]:
        L.append(f"- memory {mm['old']} -> {mm['new']}" if mm["matched"] else f"- memory {mm['old']}: no matching workspace; staged under its old name")
    if result.get("applied"):
        L += ["", "Applied (`--apply-user-config`):", ""] + [f"- {a['file']}: {a['status']}" for a in result["applied"]]
    if result.get("state"):
        L += ["", "## Baton configuration (compute queue hosts, startup list)", "",
              f"Staged under `{lay.new_root / STAGE / 'baton'}`; `--apply-user-config` places each under `{baton_dest(lay) / '.state'}` only where no file exists."]
        placed = {a["file"]: a for a in result.get("state_applied", [])}
        for f in lay.m.get("baton_state", {}).get("files", []):
            a = placed.get("baton state " + f["file"][len("baton/"):])
            L.append(f"- {f['file']}: " + (f"{a['status']} ({a['dest']})" if a else "staged, not placed"))
    unread = lay.m.get("could_not_read") or []
    if unread:
        L += ["", "## Not readable on the old machine (the export was forced past them)", "",
              f"{len(unread)} path(s) could not be read, so nothing under them was carried. Settle each by hand on Windows, or add a `leave` "
              "entry for it to migrate.toml:", ""] + [f"- `{p['path']}`: {p['error']}" for p in unread[:80]]
        if len(unread) > 80:
            L.append(f"- ... {len(unread) - 80} more in the manifest (could_not_read)")
    if result.get("errors"):
        L += ["", "## Errors (the run went on past each)", ""] + [f"- {e}" for e in result["errors"]]
    L += ["", "## Checklist, in order", ""] + [f"{i}. {x}" for i, x in enumerate(items, 1)]
    return "\n".join(L) + "\n"


EMPTY_USER = {"files": [], "proposed": [], "dropped_rules": {}, "old_hooks": {}, "mcp_to_login": [], "memory": [], "unmatched_memory": []}


def _failed_repo(lay: Layout, entry: dict, exc: Exception) -> dict:
    return {"name": entry["name"], "dest": str(lay.repo_dest(entry)), "status": f"failed: unexpected {type(exc).__name__}: {exc}", "refs": [],
            "worktrees": [], "dirty": {}, "stashes": [], "overlay": [], "notes": [], "needs": [], "problems": []}


def absolute(path) -> Path:
    """An absolute path, whatever the working directory is later (git commands run with ``-C <repository>``)."""
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def resolve_args(args) -> tuple[Path, Path, Path]:
    """(bundle, new root, new home) as absolute paths."""
    return absolute(args.src), absolute(args.root), absolute(args.home) if args.home else c.source_home()


def run(args) -> int:
    src, new_root, new_home = resolve_args(args)
    try:
        manifest = load_manifest(src)
    except BundleError as exc:
        print(f"bt migrate import: {exc}", file=sys.stderr)
        return 2
    lay = Layout(manifest, new_root, new_home)
    lay.strict = True
    only = {n.lower() for n in (args.repo or [])}
    repos = [r for r in manifest["repos"].values() if not only or r["name"].lower() in only]
    if args.dry_run:
        print(f"bundle verified ({len(manifest['files'])} files). Would import under {new_root}:")
        for r in repos:
            dest = lay.repo_dest(r)
            busy = dest.exists() and (not dest.is_dir() or any(dest.iterdir()))
            print(f"  {r['name']} -> {dest}" + ("  (REFUSED: target exists and is not empty)" if busy else "") + f"; {len(r['worktrees'])} worktree(s), {len(r['overlay'])} overlay file(s)")
        return 0
    new_root.mkdir(parents=True, exist_ok=True)
    result: dict = {"repos": [], "root": [], "aliases": [], "user": dict(EMPTY_USER), "files_all": [], "applied": [], "errors": []}

    def guard(label, fn, default):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - a failure in one part must not stop the others or the report
            result["errors"].append(f"{label}: {type(exc).__name__}: {exc}")
            return default

    audit = notes.load_audit(src)
    for r in repos:
        try:
            rr = import_repo(lay, src, r)
            result["repos"].append(rr)
            rr["notes_written"] = guard(f"migration notes of {r['name']}", lambda: notes.write_all(lay, src, manifest, r, rr, audit), [])
        except Exception as exc:  # noqa: BLE001
            result["repos"].append(_failed_repo(lay, r, exc))
    if not only:
        result["root"] = guard("root files", lambda: import_root(lay, src), [])
        result["aliases"] = guard("aliases", lambda: import_aliases(lay), [])
    result["user"] = guard("user configuration", lambda: stage_user(lay, src), dict(EMPTY_USER))
    result["state"] = guard("baton state", lambda: stage_baton_state(lay, src), [])
    if args.apply_user_config:
        result["applied"] = guard("apply user configuration", lambda: apply_user(lay, result["user"]), [])
        result["state_applied"] = guard("apply baton state", lambda: apply_baton_state(lay, result["state"]), [])
    result["files_all"] = [f for r in result["repos"] for f in r["overlay"]] + result["root"] + result["user"]["files"] + result["state"]
    items = guard("checklist", lambda: checklist(lay, src, result), ["The checklist could not be written; see the errors above."])
    report = render_report(lay, result, items)
    stage = new_root / STAGE
    c.write_bytes(stage / "import-report.md", report.encode("utf-8"))
    c.write_bytes(stage / "import.json", json.dumps({**result, "checklist": items}, indent=1, default=str).encode("utf-8"))
    print(report)
    bad = [r for r in result["repos"] if r["status"].startswith(("failed", "refused")) or r["refs"] or r.get("problems")]
    return 1 if bad or result["errors"] else 0
