"""``bt migrate export``: write the bundle directory. Never modifies a repository (read-only git plumbing only)."""
from __future__ import annotations

import io
import json
import os
import re
import shutil
import sys
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .. import config
from . import audit as auditmod
from . import common as c
from . import macstart, secrets

HEADS = "refs/heads/"


def is_bundle(path: Path) -> bool:
    try:
        data = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(data, dict) and data.get("tool") == c.TOOL


class Bundle:
    """The bundle under construction (in a temporary directory); every file is hashed when the bundle is finished."""

    def __init__(self, tmp: Path, ctx: c.Ctx | None = None):
        self.tmp = tmp
        self.ctx = ctx
        self.held: list[dict] = []          # files not carried because of what they hold: {where, size, why}
        self.known: dict[str, tuple[int, str]] = {}      # rel -> (size, sha256) of what was written, so the inventory does not read it again

    def path(self, rel: str) -> Path:
        return self.tmp / rel

    def copy(self, src, rel: str, *, repo: str | None = None, where: str = "", check: bool = True) -> dict | None:
        """Copy one local-only or user file into the bundle. Credential files are held back (``self.held``, None is returned);
        configuration files are written with their secret values redacted (see ``secrets.py``). ``repo`` names the repository the
        file belongs to: one in ``no_scan`` is never opened for the content check."""
        ctx = self.ctx
        if check and ctx is not None and not ctx.include_secrets:
            name = rel.rsplit("/", 1)[-1]
            kind = secrets.is_structured(name, rel)
            scan = repo not in ctx.cfg["no_scan"]
            size = os.stat(c.lp(src)).st_size
            limit = 16 * 1024 * 1024 if kind else secrets.MAX_CONTENT_BYTES            # a file above it is streamed, never read whole
            if size > limit and kind:
                self.held.append({"where": where or rel, "size": size, "why": "a configuration file too large to redact"})
                return None
            if size <= limit and (kind or name.lower().endswith(".pem") or (scan and not secrets.is_opaque(name))):
                with open(c.lp(src), "rb") as fh:
                    data = fh.read(limit + 1)
                new, why = secrets.prepare(data, name, rel, content=scan)
                if new is None:
                    self.held.append({"where": where or rel, "size": size, "why": why})
                    return None
                return self.write(rel, new)
        size, sha = c.copy_file(src, self.path(rel))
        self.known[rel] = (size, sha)
        return {"file": rel, "size": size, "sha256": sha}

    def write(self, rel: str, data: bytes) -> dict:
        c.write_bytes(self.path(rel), data)
        self.known[rel] = (len(data), c.sha256_bytes(data))
        return {"file": rel, "size": len(data), "sha256": self.known[rel][1]}

    def inventory(self) -> dict:
        files = {}
        for dirpath, _, names in os.walk(c.lp(self.tmp)):
            for n in names:
                full = os.path.join(dirpath, n)
                rel = os.path.relpath(full, c.lp(self.tmp)).replace(os.sep, "/")
                if rel != "manifest.json":
                    size = os.stat(full).st_size
                    known = self.known.get(rel)
                    files[rel] = {"size": size, "sha256": known[1] if known and known[0] == size else c.sha256_file(full)}
        return dict(sorted(files.items()))


def _git_out(ctx, path, *args, what: str) -> bytes | None:
    cp = c.patch_git(path, *args)
    if cp.returncode != 0:
        ctx.problem(path, f"git {' '.join(args[:3])} failed ({what}): {cp.stderr.decode('utf-8', 'replace').strip()[:200]}")
        return None
    return cp.stdout


def changed_paths(ctx, path: Path) -> list[dict]:
    """Every tracked path that differs from HEAD in a checkout, with the blob id git would store for its working-tree content
    (``git hash-object --path``: line endings and filters as git sees them, so the same content hashes the same on the
    Mac) or None for a deleted file. Verify checks exactly this list."""
    cp = c.git(path, *c.PATCH_CONFIG, "diff", "--name-only", "-z", "--no-renames", "--no-ext-diff", "--no-textconv", "HEAD")
    out = []
    for p in sorted(c.split_z(cp.stdout)) if cp.returncode == 0 else []:
        full = path / p
        if c.is_link(full):
            out.append({"path": p, "blob": None, "kind": "link"})
        elif full.is_dir():
            out.append({"path": p, "blob": None, "kind": "dir"})
        elif os.path.lexists(c.lp(full)):
            h = c.git(path, "hash-object", f"--path={p}", "--", p)
            out.append({"path": p, "blob": h.stdout.strip() if h.returncode == 0 else None, "kind": "file"})
        else:
            out.append({"path": p, "blob": None, "kind": "deleted"})
    return out


def _export_dirty(ctx, b: Bundle, repo: str, key: str, path: Path, untracked: list[dict]) -> dict:
    out: dict = {"tracked_patch": None, "untracked": [], "status": [], "changed": []}
    out["status"] = [list(x) for x in c.porcelain(ctx, path)]
    if c.git(path, "rev-parse", "--verify", "--quiet", "HEAD").returncode == 0:
        data = _git_out(ctx, path, "diff", *c.DIFF_FLAGS, "HEAD", what="tracked changes")
        if data:
            out["tracked_patch"] = b.write(f"dirty/{repo}/{key}/tracked.patch", data)
        out["changed"] = changed_paths(ctx, path)
    for f in untracked:
        src = path / f["rel"]
        try:
            meta = b.copy(src, f"dirty/{repo}/{key}/untracked/{f['rel']}", repo=repo, where=f"{repo}/{key}/{f['rel']}")
            if meta:
                out["untracked"].append({"rel": f["rel"], **meta})
        except OSError as exc:
            ctx.problem(src, f"cannot copy untracked file: {exc}")
    return out


def _export_stashes(ctx, b: Bundle, repo: str, path: Path, stashes: list[dict]) -> list[dict]:
    """Every stash entry as a patch (a second copy: the entries themselves travel in the bundle, see ``_export_repo``)."""
    out = []
    for s in stashes:
        n, sha = s["n"], s["sha"]
        entry = {**s, "patch": None, "untracked_patch": None}
        data = _git_out(ctx, path, "diff", *c.DIFF_FLAGS, f"{sha}^1", sha, what=f"stash {n}")
        if data is not None:
            entry["patch"] = b.write(f"dirty/{repo}/_main/stash-{n}.patch", data)
        if s["has_untracked"]:
            data = _git_out(ctx, path, "show", *c.DIFF_FLAGS, "--format=", f"{sha}^3", what=f"stash {n} untracked part")
            if data is not None:
                entry["untracked_patch"] = b.write(f"dirty/{repo}/_main/stash-{n}.untracked.patch", data)
        out.append(entry)
    return out


def _extra_bundle_shas(ctx, path: Path, repo: dict, entry: dict) -> list[str]:
    """Commits that no ref reaches but the import needs: every stash entry (only the top one has a ref) and the HEAD of
    every worktree (a detached one has no ref). Commits missing from the object store are listed in the entry."""
    want = [s["sha"] for s in repo["stashes"]] + [w["head"] for w in repo["worktrees"] if w.get("head")]
    out, missing = [], []
    for sha in dict.fromkeys(want):
        if c.git(path, "cat-file", "-e", sha + "^{commit}").returncode == 0:
            out.append(sha)
        else:
            missing.append(sha)
    if missing:
        entry["missing_objects"] = missing
    return out


def _export_repo(ctx: c.Ctx, b: Bundle, repo: dict, plan: dict) -> dict:
    name, path = repo["name"], Path(repo["path"])
    refs = c.for_each_ref(ctx, path)
    entry: dict = {
        "name": name, "source_path": str(path), "origin": repo["origin"], "remotes": repo["remotes"], "branch": repo["branch"],
        "head": repo["head"], "refs": {r["ref"]: r["sha"] for r in refs}, "symrefs": {r["ref"]: r["symref"] for r in refs if r["symref"]},
        "tags": repo["tags"], "hooks": repo["hooks"], "submodules": repo["submodules"], "lfs": repo["lfs"],
        "target": (repo["extra"] or {}).get("target", name), "base": (repo["extra"] or {}).get("base", "root"),
        "bundle": None, "bundle_verified": False, "worktrees": [], "dirty": {}, "stashes": [], "overlay": [], "secrets_by_hand": [],
        "config": [], "config_other": repo["config_other"],
    }
    cfg = c.local_config(path)
    entry["config"] = [[k, secrets.redact_url(v) if re.match(r"remote\..+\.(push)?url$", k) else v]
                       for k, v in cfg if k.startswith(("remote.", "branch.")) or k in ("user.name", "user.email")]
    common_dir = c.git_common_dir(path)
    for fname, rel in (("exclude", f"repos/{name}.exclude"), ("attributes", f"repos/{name}.attributes")):
        src = common_dir / "info" / fname
        if src.is_file():
            entry["info_" + fname] = b.copy(src, rel, check=False)
    if refs:
        rel = f"repos/{name}.bundle"
        extra = _extra_bundle_shas(ctx, path, repo, entry)
        cp = c.git_bytes(path, "bundle", "create", str(b.path(rel)), "--all", *extra, timeout=3600)
        if cp.returncode != 0:
            ctx.problem(path, "git bundle create failed: " + cp.stderr.decode("utf-8", "replace").strip()[:300])
        else:
            entry["bundle"] = rel
            v = c.git(path, "bundle", "verify", str(b.path(rel)), timeout=3600)
            entry["bundle_verified"] = v.returncode == 0
            if v.returncode != 0:
                ctx.problem(path, "git bundle verify failed: " + v.stderr.strip()[:300])
    entry["stashes"] = _export_stashes(ctx, b, name, path, repo["stashes"])
    lfs_dir = common_dir / "lfs" / "objects"
    if lfs_dir.is_dir():
        count = total = 0
        for dirpath, _, names in os.walk(c.lp(lfs_dir)):
            for fn in names:
                full = os.path.join(dirpath, fn)
                relname = os.path.relpath(full, c.lp(lfs_dir)).replace(os.sep, "/")
                try:
                    meta = b.copy(full, f"repos/{name}.lfs/{relname}", check=False)
                except OSError as exc:
                    ctx.problem(full, f"cannot copy LFS object: {exc}")
                    continue
                count += 1
                total += meta["size"]
        if count:
            entry["lfs_objects"] = {"prefix": f"repos/{name}.lfs", "count": count, "bytes": total}
    p = plan["repos"][name]
    trees = {"_main": (path, p["trees"]["_main"])}
    for wt in repo["worktrees"]:
        info = {k: wt.get(k) for k in ("key", "rel", "path", "branch", "head", "detached", "kind", "prunable", "outside")}
        info["note"] = (wt.get("note") or {}).get("fields", {})
        entry["worktrees"].append(info)
        if wt["key"] in p["trees"]:
            trees[wt["key"]] = (Path(wt["path"]), p["trees"][wt["key"]])
    for key, (tpath, tp) in trees.items():
        entry["dirty"][key] = _export_dirty(ctx, b, name, key, tpath, tp["untracked"])
        for f in tp["overlay"]:
            try:
                meta = b.copy(tpath / f["rel"], f"overlay/{name}/{key}/{f['rel']}", repo=name, where=f"{name}/{key}/{f['rel']}")
                if meta:
                    entry["overlay"].append({"tree": key, "rel": f["rel"], "rule": f["rule"], **meta})
            except OSError as exc:
                ctx.problem(tpath / f["rel"], f"cannot copy overlay file: {exc}")
    entry["secrets_by_hand"] = [s for s in p["secrets"]]
    return entry


def _export_extras(ctx, b: Bundle, plan: dict, survey: dict) -> dict:
    out: dict = {"root": [], "dirs": [], "root_secrets": plan["root"]["secrets"]}
    for f in plan["root"]["overlay"]:
        try:
            meta = b.copy(ctx.root / f["rel"], f"root/{f['rel']}", where=f"(root)/{f['rel']}")
            if meta:
                out["root"].append({"rel": f["rel"], "rule": f["rule"], **meta})
        except OSError as exc:
            ctx.problem(ctx.root / f["rel"], f"cannot copy root file: {exc}")
    for d in plan["dirs"]:
        prefix = "nonrepo/" + ("@home/" if d["base"] == "home" else "") + d["target"]
        files = []
        for f in d["plan"]["files"]:
            try:
                meta = b.copy(Path(d["src"]) / f["rel"], f"{prefix}/{f['rel']}", where=f"({d['kind']}) {d['name']}/{f['rel']}")
                if meta:
                    files.append({"rel": f["rel"], **meta})
            except OSError as exc:
                ctx.problem(Path(d["src"]) / f["rel"], f"cannot copy file: {exc}")
        out["dirs"].append({"target": d["target"], "base": d["base"], "kind": d["kind"], "source_path": d["src"], "prefix": prefix,
                            "files": files, "links": d["plan"]["links"]})
    return out


def _export_user(ctx, b: Bundle, plan: dict, known: dict[str, str]) -> dict:
    u = plan["user"]
    out = {"accounts": u["accounts"], "files": [], "links": u["links"], "skipped": u["skipped"], "secrets": u["secrets"],
           "ssh": u["ssh"], "gitconfig": u["gitconfig"], "memory": []}
    for f in u["files"]:
        try:
            meta = b.copy(f["src"], f["rel"])
            if meta:
                out["files"].append(meta)
        except OSError as exc:
            ctx.problem(f["src"], f"cannot copy: {exc}")
    for rel, data in u["generated"].items():
        out["files"].append(b.write(rel, data))
    mem: dict = {}
    for f in out["files"]:
        parts = f["file"].split("/")
        if len(parts) > 5 and parts[0] == "user" and parts[1] == "claude" and parts[3] == "projects" and parts[5] == "memory":
            k = (parts[2], parts[4])
            mem[k] = mem.get(k, 0) + 1
    out["memory"] = [{"vendor": "claude", "account": a, "slug": s, "files": n, "source_path": known.get(s, "")} for (a, s), n in sorted(mem.items())]
    return out


def _known_paths(ctx: c.Ctx, survey: dict, plan: dict) -> list[dict]:
    out = [{"path": str(ctx.root), "kind": "root"}, {"path": str(ctx.home), "kind": "home"}]
    for r in survey["repos"]:
        out.append({"path": r["path"], "kind": "repo", "name": r["name"]})
        out += [{"path": w["path"], "kind": "worktree", "repo": r["name"]} for w in r["worktrees"]]
    out += [{"path": str(ctx.root / n["name"]), "kind": "nonrepo"} for n in survey["nonrepo"]]
    out += [{"path": str(ctx.root / a["name"]), "kind": "alias"} for a in survey["aliases"]]
    return out


def print_plan(data: dict, plan: dict, ctx: c.Ctx) -> None:
    print(f"export plan (nothing is written): root {ctx.root}")
    for name, p in plan["repos"].items():
        n_over = sum(len(t["overlay"]) for t in p["trees"].values())
        n_unt = sum(len(t["untracked"]) for t in p["trees"].values())
        print(f"  repos/{name}.bundle  about {c.human(p['bundle_bytes'])} (estimated from git count-objects, not a bundle run); "
              f"overlay {n_over} files, untracked {n_unt} files, {c.human(p['bytes'])}")
    print(f"  root: {len(plan['root']['overlay'])} files; directories: {len(plan['dirs'])}")
    u = plan["user"]
    print(f"  user configuration: {len(u['files'])} files, {c.human(sum(f['size'] for f in u['files']))}")
    print(f"  left behind: {len(plan['left'])} entries; secrets to copy by hand: {len(plan['secrets'])}")
    print(f"  projected total: {c.human(plan['bytes'])}")


TRANSFER_TEXT = """\
Before anything starts on the Mac: close every Claude Code session on this Windows machine and remove what starts work here
(`bt night schedule remove`, `bt startup uninstall`). The compute queue's leader is chosen per state directory, so a session open
here and one open on the Mac are two controllers submitting to the same hosts.

Next, in this order (encryption of the transfer is yours to choose, bt migrate does not encrypt):
  1. Transfer the bundle to the Mac, by either route.
     - Direct copy over ssh (OpenSSH Server enabled on the Windows machine): on the Mac
         rsync -a <windows-user>@<windows-host>:{bundle_posix}/ ~/migration/{name}/
     - One tar on an external disk{tar_hint}: copy {tar_name} and {tar_name}.sha256 to the disk, encrypting the disk or the file
       with a tool you choose; on the Mac
         shasum -a 256 -c {tar_name}.sha256 && tar -xf {tar_name} -C ~/migration
  2. On the Mac, the dry run (verifies the bundle, writes nothing; needs no tools installed beforehand):
         sh ~/migration/{name}/mac-start.sh
     It uses the copy of Baton inside the bundle (tool/, the checkout this bundle was made by) and a Python 3.11 or later; when there
     is none it tells you what to install and stops.
  3. The real import: the command step 2 prints, normally
         sh ~/migration/{name}/mac-start.sh --root ~/Code
  4. Install the tools: brew bundle --file ~/migration/{name}/baton/Brewfile  (review it first)
  5. Log in again (claude, codex, gh, the MCP servers) and copy the secret files named above by hand.
  6. Install Baton's hooks, session-workflow's hooks and the bt line in ~/.zshrc: python3 ~/Code/baton/bt.py install
  7. python3 ~/Code/baton/bt.py doctor
  8. python3 ~/Code/baton/bt.py migrate verify --from ~/migration/{name} --root ~/Code     (must print: no gaps)
  9. The evening task and start-at-login on the Mac: bt night schedule install, bt startup install
The import report (~/Code/.migration/import-report.md) repeats the steps after 3 with this machine's paths.
"""


def snapshot_tool(ctx: c.Ctx, b: Bundle) -> dict:
    """``tool/``: Baton's tracked source at the exporting checkout's HEAD (``git archive``), so the tool that imports is the tool
    that exported. Everything tracked is carried: the archive is compared with ``git ls-files`` and a difference is a problem."""
    home = config.BATON_HOME
    out: dict = {"source": str(home), "sha": "", "dirty": False, "files": 0, "symlinks": [], "ok": False, "error": ""}
    cp = c.git_bytes(home, "archive", "--format=tar", "HEAD", timeout=600)
    if cp.returncode != 0:
        out["error"] = "git archive of Baton failed: " + cp.stderr.decode("utf-8", "replace").strip()[:200]
        return out
    out["sha"] = c.git(home, "rev-parse", "HEAD").stdout.strip()
    out["dirty"] = bool(c.git(home, "status", "--porcelain", "--untracked-files=no").stdout.strip())
    seen = set()
    with tarfile.open(fileobj=io.BytesIO(cp.stdout)) as tf:
        for m in tf:
            if m.isfile():
                if m.name.startswith(("/", "../")) or "/../" in m.name:
                    continue
                b.write("tool/" + m.name, tf.extractfile(m).read())
                seen.add(m.name)
            elif m.issym() or m.islnk():
                out["symlinks"].append(m.name)
    out["files"] = len(seen)
    tracked = c.split_z(c.git(home, "ls-files", "-s", "-z").stdout)
    missing = [t.partition("\t")[2] for t in tracked if t.startswith(("100644", "100755"))]
    missing = [p for p in missing if p not in seen]
    if missing:
        out["error"] = f"{len(missing)} tracked Baton file(s) are not in the tool/ snapshot (export-ignore?): {', '.join(missing[:5])}"
    elif not seen:
        out["error"] = "the tool/ snapshot is empty"
    out["ok"] = not out["error"]
    return out


def run(args, ctx: c.Ctx) -> int:
    out = Path(args.out)
    if out.exists() and not is_bundle(out) and (not out.is_dir() or any(out.iterdir())):
        print(f"bt migrate export: {out} exists and is not an earlier bundle of this tool; refusing to touch it", file=sys.stderr)
        return 2
    data, survey, plan = auditmod.build_audit(ctx, scan=True, only=ctx.only)
    blockers = [r for r in data["readiness"] if r["blocking"]]
    if blockers and not args.force:
        print(f"bt migrate export: {len(blockers)} blocker(s); fix them or use --force (they are then recorded in the manifest):", file=sys.stderr)
        for r in blockers:
            print(f"  - {r['kind']} {r['where']}: {r['detail']}. Clears with: {r['action']}", file=sys.stderr)
        return 2
    if args.dry_run:
        print_plan(data, plan, ctx)
        if blockers:
            print(f"  forced blockers ({len(blockers)}): " + "; ".join(f"{r['kind']} {r['where']}" for r in blockers))
        return 0
    tmp = out.parent / f"{out.name}.partial-{os.getpid()}"
    if tmp.exists():
        shutil.rmtree(c.lp(tmp), ignore_errors=True)
    tmp.mkdir(parents=True)
    b = Bundle(tmp, ctx)
    try:
        manifest = build(ctx, b, data, survey, plan, blockers)
        if not manifest["baton_tool"].get("ok"):          # the Mac cannot import without tool/: not a problem --force can accept
            print(f"bt migrate export: the bundle has no usable tool/ (Baton's own source, which runs the import on the Mac): "
                  f"{manifest['baton_tool'].get('error')}. Nothing was finalised; --force does not cover this.", file=sys.stderr)
            return 2
        manifest["files"] = b.inventory()
        (tmp / "manifest.json").write_text(json.dumps(manifest, indent=1, default=str), encoding="utf-8", newline="\n")
        if ctx.problems and not args.force:
            print(f"bt migrate export: {len(ctx.problems)} path(s) could not be read; nothing was finalised (use --force to export anyway):", file=sys.stderr)
            for p in ctx.problems[:30]:
                print(f"  - {p['path']}: {p['error']}", file=sys.stderr)
            return 2
        old = None
        if out.exists():
            old = out.parent / f"{out.name}.old-{os.getpid()}"
            os.replace(out, old)
        os.replace(tmp, out)
        if old is not None:
            shutil.rmtree(c.lp(old), ignore_errors=True)
    finally:
        if tmp.exists():
            shutil.rmtree(c.lp(tmp), ignore_errors=True)
    tar_hint = ""
    if args.archive:
        tar_hint = f" ({args.archive})"
        archive(out, Path(args.archive))
    print(f"bundle written: {out} ({len(manifest['files'])} files, {c.human(sum(f['size'] for f in manifest['files'].values()))})")
    if blockers:
        print(f"forced past {len(blockers)} blocker(s); recorded in the manifest")
    if ctx.problems:
        print(f"{len(ctx.problems)} path(s) could not be read (recorded in the manifest)")
    by_hand = [s for s in manifest["secrets_by_hand"]]
    if by_hand:
        print(f"{len(by_hand)} secret-looking file(s) NOT carried; copy by hand:")
        for s in by_hand[:20]:
            print(f"  - {s['where']}" + (f" ({s['why']})" if s.get("why") else ""))
    print(f"left behind: {len(manifest['left_behind'])} entries (manifest.json, audit.md)")
    tar_name = Path(args.archive).name if args.archive else f"{out.name}.tar"
    print(TRANSFER_TEXT.format(bundle_posix=str(out).replace("\\", "/"), name=out.name, tar_hint=tar_hint, tar_name=tar_name))
    return 0


def archive(bundle_dir: Path, tar_path: Path) -> None:
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "w") as tf:
        tf.add(bundle_dir, arcname=bundle_dir.name)
    sha = c.sha256_file(tar_path)
    tar_path.with_name(tar_path.name + ".sha256").write_text(f"{sha}  {tar_path.name}\n", encoding="utf-8", newline="\n")


def build(ctx: c.Ctx, b: Bundle, data: dict, survey: dict, plan: dict, blockers: list[dict]) -> dict:
    repos = [r for r in survey["repos"] if not r["skipped"]]
    with ThreadPoolExecutor(max_workers=4) as pool:
        entries = list(pool.map(lambda r: _export_repo(ctx, b, r, plan), repos))
    extras = _export_extras(ctx, b, plan, survey)
    known = auditmod.known_slugs(ctx, survey)
    user = _export_user(ctx, b, plan, known)
    baton_files = []
    for src, rel in ((config.STATE_DIR / "accounts.local.toml", "baton/accounts.local.toml"),
                     (_enrolled_path(ctx), "baton/night/enrolled.json")):
        if src.is_file():
            meta = b.copy(src, rel)
            if meta:
                baton_files.append(meta)
    state_files = []
    for f in plan["state"]["files"]:
        meta = b.copy(f["src"], f["rel"], where=f["rel"])
        if meta:
            state_files.append(meta)
    text = auditmod.render_markdown(data)
    baton_files.append(b.write("baton/audit.md", text.encode("utf-8")))
    baton_files.append(b.write("baton/audit.json", json.dumps(data, indent=1, default=str).encode("utf-8")))
    baton_files.append(b.write("baton/Brewfile", data["brewfile"].encode("utf-8")))
    baton_files.append(b.write("mac-start.sh", macstart.SCRIPT.encode("ascii")))
    tool = snapshot_tool(ctx, b) if ctx.with_tool else {"source": "", "sha": "", "dirty": False, "files": 0, "symlinks": [], "skipped": True, "ok": True}
    by_hand = [{"where": f"{s['repo']}/{s['tree']}/{s['rel']}", "size": s["size"], "why": s.get("why", "")} for s in plan["secrets"]]
    by_hand += [{"where": s["path"], "size": s["size"], "why": s.get("why", "")} for s in plan["user"]["secrets"]]
    have = {x["where"] for x in by_hand}
    by_hand += [h for h in b.held if h["where"] not in have]
    condensed: dict = {}
    for x in plan["not_carried"]:
        k = (x["repo"], x["tree"], x["reason"])
        e = condensed.setdefault(k, {"repo": x["repo"], "tree": x["tree"], "reason": x["reason"], "entries": 0, "files": 0, "bytes": 0, "examples": []})
        e["entries"] += 1
        e["files"] += x["files"]
        e["bytes"] += x["bytes"]
        if len(e["examples"]) < 5:
            e["examples"].append(x["what"])
    user_left = [{"repo": "(user)", "what": x["path"], "why": f"{x['kind']} to {x['target']}: recorded, not followed", "decision": False}
                 for x in plan["user"]["links"]]
    user_left += [{"repo": "(user)", "what": x["path"], "why": x["why"], "decision": False} for x in plan["user"]["skipped"]]
    return {
        "tool": c.TOOL, "format": c.FORMAT_VERSION, "created": c.iso(),
        "source": {"platform": sys.platform, "root": str(ctx.root), "home": str(ctx.home)},
        "repos": {e["name"]: e for e in entries},
        "aliases": [a for a in survey["aliases"] if not ctx.only or a["target_in_root"].lower() in {n.lower() for n in ctx.only}], "nonrepo_listed": survey["nonrepo"],
        "root": {"overlay": extras["root"], "secrets": extras["root_secrets"]}, "dirs": extras["dirs"],
        "user": user, "baton": {"files": baton_files}, "baton_state": {"files": state_files}, "baton_tool": tool, "known_paths": _known_paths(ctx, survey, plan),
        "left_behind": plan["left"] + user_left,
        "not_carried": list(condensed.values()), "secrets_by_hand": by_hand,
        "blockers_forced": blockers, "could_not_read": ctx.problems,
        "notes": ["Stashes: every entry's commits are in the bundle and import stores them again in order; each entry is also carried as a patch.",
                  "Reflogs are not carried.",
                  "Secret values never enter the bundle: configuration files are carried with env, header and secret-named values replaced by REDACTED, "
                  "credential files and files holding a recognised key are listed in secrets_by_hand and must be copied by hand."],
    }


def _enrolled_path(ctx: c.Ctx) -> Path:
    from .. import night_schedule
    return night_schedule.enrolled_path(ctx.state_dir)
