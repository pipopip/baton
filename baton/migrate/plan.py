"""What an export would carry and what it would leave behind, computed from a survey (shared by audit and export)."""
from __future__ import annotations

import os
from pathlib import Path

from . import common as c
from . import local as loc
from . import user as userconf


def classify_root(ctx: c.Ctx, root_files: list[dict]) -> dict:
    """The code root itself: its top-level files by the carry rules (plus every ``*.md``) and its own ``.claude``, ``.codex``
    and ``.baton`` directories. Files that match nothing are named in ``other`` (not carried, never silently)."""
    out = loc._empty()
    out["_grouped"] = set()
    out["other"] = []
    for f in root_files:
        rel = f["name"]
        klass, why = loc.classify_rel(ctx, rel, None)
        if klass == "undecided" and rel.lower().endswith(".md"):
            klass, why = "carried", "top-level Markdown file"
        if klass == "carried":
            held = loc.hold_reason(ctx, None, ctx.root / rel, rel, f["size"])
            if held:
                klass, why = "secret", held
        if klass == "undecided":
            out["other"].append({"rel": rel, "size": f["size"]})
        elif klass == "carried":
            out["carried"].append({"rel": rel, "size": f["size"], "rule": why, "untracked": False})
        elif klass == "secret":
            out["secrets"].append({"rel": rel, "size": f["size"], "why": why})
        elif klass == "left":
            out["left"].append({"rel": rel, "size": f["size"], "why": why})
        else:
            out[klass].append({"path": rel, "files": 1, "bytes": f["size"], "approx": False, "why": why})
    for d in (".claude", ".codex", ".baton"):
        p = ctx.root / d
        if c.is_link(p):
            out["links"].append({"rel": d, "target": c.link_target(p), "kind": c.link_kind(p) or "symlink"})
        elif p.is_dir():
            loc._walk(ctx, ctx.root, None, d, out)
    out.pop("_grouped", None)
    return out


def extra_dirs(ctx: c.Ctx) -> list[dict]:
    """Plain directories to carry: ``nonrepo`` names under the root and ``extra_dirs`` entries of migrate.toml."""
    out = []
    for name in ctx.cfg["nonrepo"]:
        out.append({"src": ctx.root / name, "target": name.replace("\\", "/").strip("/"), "base": "root", "kind": "nonrepo", "name": name})
    for e in ctx.cfg["extra_dirs"]:
        out.append({"src": Path(e["path"]), "target": e["target"], "base": e["base"], "kind": "extra", "name": Path(e["path"]).name})
    return out


def _tree_entries(trees: list[tuple[str, Path, dict]], ctx: c.Ctx) -> dict:
    out = {"trees": {}, "secrets": [], "not_carried": [], "left": [], "bytes": 0}
    for key, path, l in trees:
        overlay, untracked, left = loc.plan_copy(ctx, l)
        out["trees"][key] = {"path": str(path), "overlay": overlay, "untracked": untracked}
        out["bytes"] += sum(f["size"] for f in overlay) + sum(f["size"] for f in untracked)
        for s in l["secrets"]:
            out["secrets"].append({"tree": key, "rel": s["rel"], "size": s["size"], "why": s.get("why", "secret-looking name")})
        for f in left:
            out["left"].append({"tree": key, "rel": f["rel"], "size": f["size"], "files": f.get("files", 1), "why": f["why"],
                                "decision": f["decision"]})
        for kind in ("rebuildable", "machine"):
            for item in l[kind]:
                out["not_carried"].append({"tree": key, "what": item["path"], "reason": item["why"] + " (" + kind + ")",
                                           "files": item["files"], "bytes": item["bytes"]})
        for item in l["links"]:
            out["left"].append({"tree": key, "rel": item["rel"], "size": 0, "decision": False,
                                "why": f"{item['kind']} to {item['target']}: links are recorded, not followed"})
        for item in l["nested"]:
            out["left"].append({"tree": key, "rel": item["rel"], "size": 0, "decision": False, "why": "nested git repository"})
    return out


COMPUTE_CONFIG = ("hosts.json", "host.conf.json")
COMPUTE_LEFT = {"runtime.local.json": "names helpers that exist only on Windows (the queue runtime of the old machine)",
                "laptop.conf.json": "describes the Windows laptop's own lane: Windows paths and helpers"}


def baton_state(ctx: c.Ctx) -> dict:
    """Baton's own configuration that lives in its ``.state`` directory (classed machine state otherwise): what the compute queue
    needs to reach its hosts (``compute/hosts.json``, ``compute/host.conf.json``, ``compute/hosts/<id>/host.conf.json``) and the
    pinned workstreams of ``startup.toml`` (names of workstreams, not paths: ``baton/startup.py`` reads only ``pinned``).
    ``runtime.local.json`` and ``laptop.conf.json`` are named as left behind. The secret rules apply to every file."""
    out: dict = {"files": [], "left": [], "secrets": []}
    cdir = ctx.state_dir / "compute"

    def add(src: Path, rel: str) -> None:
        if not src.is_file():
            return
        size = src.stat().st_size
        held = loc.hold_reason(ctx, None, src, rel, size)
        if held:
            out["secrets"].append({"repo": "(baton state)", "tree": "_state", "rel": rel, "size": size, "why": held})
        else:
            out["files"].append({"src": str(src), "rel": rel, "size": size})

    for name in COMPUTE_CONFIG:
        add(cdir / name, f"baton/compute/{name}")
    try:
        ids = sorted(e.name for e in os.scandir(c.lp(cdir / "hosts")) if e.is_dir())
    except OSError:
        ids = []
    for host_id in ids:
        add(cdir / "hosts" / host_id / "host.conf.json", f"baton/compute/hosts/{host_id}/host.conf.json")
    add(cdir / "fleet" / "config.json", "baton/compute/fleet/config.json")
    add(ctx.state_dir / "startup.toml", "baton/startup.toml")
    fleet = cdir / "fleet"
    if fleet.is_dir():
        s = c.summarize_dir(fleet)
        cfg = fleet / "config.json"
        files = s["files"] - (1 if cfg.is_file() else 0)
        if files > 0:
            out["left"].append({"repo": "(baton state)", "what": ".state/compute/fleet/ (everything but config.json)", "decision": False,
                                "why": f"fleet controller state ({files} file(s)): machine state, rebuilt on the new machine",
                                "size": s["bytes"] - (cfg.stat().st_size if cfg.is_file() else 0)})
    for name, why in COMPUTE_LEFT.items():
        p = cdir / name
        if p.is_file():
            out["left"].append({"repo": "(baton state)", "what": f".state/compute/{name}", "why": why, "decision": False, "size": p.stat().st_size})
    return out


def build_plan(ctx: c.Ctx, survey: dict) -> dict:
    """Plan of the bundle: per repository the overlay and untracked files and what is left behind, the root, plain
    directories, and the user configuration."""
    plan: dict = {"repos": {}, "left": [], "not_carried": [], "secrets": [], "bytes": 0}
    for repo in survey["repos"]:
        if repo["skipped"]:
            plan["left"].append({"repo": repo["name"], "what": "(repository)", "why": "listed in skip_repos of migrate.toml", "decision": False})
            continue
        trees = [("_main", Path(repo["path"]), repo["_local"])]
        for wt in repo["worktrees"]:
            if "_local" in wt:                          # every worktree that exists, inside the repository directory or not
                trees.append((wt["key"], Path(wt["path"]), wt["_local"]))
        sub = _tree_entries(trees, ctx)
        sub["bundle_bytes"] = repo["object_bytes"] + (repo.get("lfs_objects") or {}).get("bytes", 0)
        plan["repos"][repo["name"]] = sub
        plan["bytes"] += sub["bytes"] + sub["bundle_bytes"]
        for kind in ("left", "not_carried", "secrets"):
            for item in sub[kind]:
                plan[kind].append({"repo": repo["name"], **item})
        plan["left"].append({"repo": repo["name"], "what": "reflogs", "decision": False,
                             "why": "reflogs are not carried; commits reachable only from a reflog are not in the bundle"})
        if repo["submodules"]:
            plan["left"].append({"repo": repo["name"], "what": "submodule contents", "decision": False,
                                 "why": "only the superproject is bundled; run `git submodule update --init` on the Mac ("
                                        + ", ".join(s["path"] for s in repo["submodules"][:5]) + ")"})
        for wt in repo["worktrees"]:
            if wt["prunable"] or wt["outside"]:
                plan["left"].append({"repo": repo["name"], "what": f"worktree {wt['path']}", "decision": False,
                                     "why": "its directory is missing: its branch (or detached HEAD) is bundled, the worktree is not recreated" if wt["prunable"]
                                     else "it lies outside the repository directory: its work is carried and it is recreated under .claude/worktrees/ on import"})
    rl = classify_root(ctx, survey["root_files"])
    ov, _ut, left = loc.plan_copy(ctx, rl)
    plan["root"] = {"overlay": ov, "secrets": rl["secrets"], "other": rl.get("other", []), "links": rl["links"]}
    plan["bytes"] += sum(f["size"] for f in ov)
    for s in rl["secrets"]:
        plan["secrets"].append({"repo": "(root)", "tree": "_root", "rel": s["rel"], "size": s["size"], "why": s.get("why", "")})
    for f in rl.get("other", []):
        plan["left"].append({"repo": "(root)", "what": f["rel"], "why": "top-level file matching no carry rule", "decision": False, "size": f["size"]})
    for f in left:
        plan["left"].append({"repo": "(root)", "what": f["rel"], "why": f["why"], "decision": f["decision"], "size": f["size"]})
    for a in survey["aliases"]:
        if not a["target_in_root"]:
            plan["left"].append({"repo": "(root)", "what": f"alias {a['name']}", "decision": False,
                                 "why": f"{a['kind']} to {a['target']}, which is outside the root: the target is not carried"})
    plan["dirs"] = []
    for d in extra_dirs(ctx):
        if not d["src"].is_dir():
            ctx.problem(d["src"], "directory named in migrate.toml does not exist")
            continue
        t = loc.tree_plan(ctx, d["src"], prefix=d["target"])
        plan["dirs"].append({**d, "src": str(d["src"]), "plan": t})
        plan["bytes"] += sum(f["size"] for f in t["files"])
        for s in t["secrets"]:
            plan["secrets"].append({"repo": f"({d['kind']}) {d['name']}", "tree": "_dir", "rel": s["rel"], "size": s["size"], "why": s.get("why", "")})
        for f in t["left"]:
            plan["left"].append({"repo": f"({d['kind']}) {d['name']}", "what": f["rel"], "why": f["why"], "decision": False})
        for f in t["too_large"]:
            plan["left"].append({"repo": f"({d['kind']}) {d['name']}", "what": f["rel"], "why": "larger than 500 MiB", "decision": False})
        for item in t["links"]:
            plan["left"].append({"repo": f"({d['kind']}) {d['name']}", "what": item["rel"], "decision": False,
                                 "why": f"{item['kind']} to {item['target']}: links are recorded, not followed"})
    plan["state"] = baton_state(ctx)
    plan["bytes"] += sum(f["size"] for f in plan["state"]["files"])
    plan["left"].extend(plan["state"]["left"])
    plan["secrets"].extend(plan["state"]["secrets"])
    plan["user"] = userconf.collect_user(ctx)
    for x in plan["user"]["left"]:
        plan["left"].append({"repo": "(user)", "what": x["path"], "why": f"{x['why']} ({x['files']} file(s))", "decision": False, "size": x["size"]})
    plan["bytes"] += sum(f["size"] for f in plan["user"]["files"]) + sum(len(b) for b in plan["user"]["generated"].values())
    return plan
