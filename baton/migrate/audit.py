"""``bt migrate audit``: survey the workspace (read-only) and report what a move would lose."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from . import common as c
from . import machine, plan as planmod, repo as repomod, user as userconf
from .scan import CATEGORIES

TOP_BRANCHES = 10


def public(repo: dict) -> dict:
    """The repo dict without its internal full file lists (``_local``)."""
    out = {k: v for k, v in repo.items() if k != "_local"}
    out["worktrees"] = [{k: v for k, v in w.items() if k != "_local"} for w in repo["worktrees"]]
    return out


def known_slugs(ctx: c.Ctx, survey: dict) -> dict[str, str]:
    """Claude project slug -> workspace path, for every path under the root or home that is known (source spelling)."""
    paths = [str(ctx.root), str(ctx.home)]
    for r in survey["repos"]:
        paths.append(r["path"])
        paths.extend(w["path"] for w in r["worktrees"])
    paths.extend(str(ctx.root / n["name"]) for n in survey["nonrepo"])
    paths.extend(str(ctx.root / a["name"]) for a in survey["aliases"])
    return {c.project_slug(p): p for p in paths}


def lost_summary(repos: list[dict], plan: dict) -> dict:
    plan_counts = {n: (sum(len(t["overlay"]) + len(t["untracked"]) for t in p["trees"].values()), p["bytes"]) for n, p in plan["repos"].items()}
    out = {"no_remote": [], "repos": {}, "branches": 0, "branches_unique": 0, "dirty": 0, "stashes": 0, "local_files": 0, "local_bytes": 0,
           "undecided_left_files": sum(1 for x in plan["left"] if x.get("decision")),
           "undecided_left_bytes": sum(x.get("size", 0) for x in plan["left"] if x.get("decision"))}
    for r in repos:
        if r["skipped"]:
            continue
        lost = [b for b in r["branches"] if b["lost"]]
        dirty = len(r["dirty"]) + sum(w.get("dirty", 0) for w in r["worktrees"])
        files, size = plan_counts.get(r["name"], (0, 0))
        if not r["remotes"]:
            out["no_remote"].append(r["name"])
        out["repos"][r["name"]] = {"lost": len(lost), "unique": sum(1 for b in lost if b["unique"]),
                                   "newest": [{"name": b["name"], "status": b["status"], "date": b["date"]}
                                              for b in sorted(lost, key=lambda b: -b["date"])[:TOP_BRANCHES]],
                                   "dirty": dirty, "stashes": len(r["stashes"]), "files": files, "bytes": size}
        out["branches"] += len(lost)
        out["branches_unique"] += sum(1 for b in lost if b["unique"])
        out["dirty"] += dirty
        out["stashes"] += len(r["stashes"])
        out["local_files"] += files
        out["local_bytes"] += size
    return out


def example_toml(ctx: c.Ctx, survey: dict) -> str:
    lines = ["# migrate.toml: next to accounts.toml (or .state/migrate.local.toml). Every key is optional.",
             "# carry / leave: globs relative to each repository, worktree and the root; a pattern without / matches any depth.",
             "# carry = [\"notes/**\"]", "# leave = [\"scratch/**\"]", "# skip_repos = [\"brain\"]",
             "# no_scan = [\"brain\"]   # repositories whose file contents are never opened by the scan or the secret check (default)", "# [repos.<name>]", "# carry = [\"docs/local/**\"]",
             "# nonrepo: directories directly under the root to carry (listed below with their sizes).", "# nonrepo = ["]
    for n in survey["nonrepo"]:
        lines.append(f"#   \"{n['name']}\",  # {n['files']}{'+' if n['approx'] else ''} files, {c.human(n['bytes'])}")
    lines += ["# ]", "# extra_repos / extra_dirs: sources outside the root, with a target relative to NEWROOT (or NEWHOME with base = \"home\").",
              "# [[extra_repos]]", "# path = \"D:/AI/WanGP\"", "# target = \"AI/WanGP\"", "# [[extra_dirs]]", "# path = \"C:/Users/<you>/plugins/app\"",
              "# target = \"plugins/app\"", "# base = \"home\""]
    return "\n".join(lines) + "\n"


def build_audit(ctx: c.Ctx, *, scan: bool = True, only: list[str] | None = None) -> tuple[dict, dict, dict]:
    """(audit data for JSON, the internal survey with full file lists, the bundle plan)."""
    t0 = time.time()
    survey = repomod.survey_all(ctx, scan=scan, only=only)
    plan = planmod.build_plan(ctx, survey)
    userdata = userconf.survey_user(ctx, known_slugs(ctx, survey))
    mach = machine.survey_machine(ctx)
    ready = repomod.readiness(ctx, survey["repos"])
    data = {
        "generated": c.iso(), "platform": sys.platform, "root": str(ctx.root), "home": str(ctx.home), "scan": scan,
        "repos": [public(r) for r in survey["repos"]], "aliases": survey["aliases"], "nonrepo": survey["nonrepo"],
        "root_files": survey["root_files"],
        "root_local": {"overlay": plan["root"]["overlay"], "secrets": plan["root"]["secrets"], "other": plan["root"]["other"]},
        "lost": lost_summary(survey["repos"], plan), "user": userdata, "machine": mach, "readiness": ready,
        "brewfile": machine.brewfile(mach, survey["repos"]), "example_toml": example_toml(ctx, survey),
        "decide": repomod.decide_snippets(ctx, survey["repos"]),
        "left_behind": plan["left"], "not_carried": plan["not_carried"], "secrets": plan["secrets"], "could_not_read": ctx.problems,
        "projected_bytes": plan["bytes"],
        "projected": {"bundles": sum(p["bundle_bytes"] for p in plan["repos"].values()),
                      "overlay_and_untracked": sum(p["bytes"] for p in plan["repos"].values()),
                      "root_and_dirs": sum(f["size"] for f in plan["root"]["overlay"]) + sum(sum(f["size"] for f in d["plan"]["files"]) for d in plan["dirs"]),
                      "user": sum(f["size"] for f in plan["user"]["files"]) + sum(len(b) for b in plan["user"]["generated"].values()),
                      "baton_state": sum(f["size"] for f in plan["state"]["files"])},
        "elapsed_seconds": round(time.time() - t0, 1),
    }
    return data, survey, plan


# ------------------------------------------------------------------ Markdown

def _table(head: list[str], rows: list[list]) -> list[str]:
    out = ["| " + " | ".join(head) + " |", "|" + "|".join("---" for _ in head) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(x).replace("|", "\\|") for x in r) + " |")
    return out


def _date(ts) -> str:
    return time.strftime("%Y-%m-%d", time.localtime(ts)) if ts else "-"


def render_markdown(d: dict) -> str:
    L: list[str] = []
    ready = d["readiness"]
    blockers = [r for r in ready if r["blocking"]]
    repos = [r for r in d["repos"] if not r["skipped"]]
    lost = d["lost"]
    wts = [w for r in repos for w in r["worktrees"]]
    kinds = {k: sum(1 for w in wts if w["kind"] == k and not w["prunable"]) for k in ("workstream", "follower", "scratch")}
    stream_wts = [w for w in wts if not w["prunable"] and w["kind"] == "workstream"]
    unusable = [w for w in stream_wts if not w["note"]["exists"] or not w["note"]["complete"] or w["note"]["stale"]]
    secrets_n = len(d["secrets"])
    L += [f"# Migration audit", "", f"Generated {d['generated']} on {d['platform']}; root `{d['root']}`, home `{d['home']}`; "
          f"took {d['elapsed_seconds']} s. Read-only: nothing was written into any repository.", "",
          "## Summary", ""]
    L += [f"- Repositories: {len(repos)} ({len(lost['no_remote'])} without a remote: {', '.join(lost['no_remote']) or 'none'}); "
          f"aliases: {len(d['aliases'])}; non-repository directories: {len(d['nonrepo'])}",
          f"- Branches a plain re-clone would lose: {lost['branches']} ({lost['branches_unique']} with commits that exist nowhere else)",
          f"- Uncommitted changes: {lost['dirty']} dirty files; stashes: {lost['stashes']}",
          f"- Local-only files that would be carried (rules, ignored-undecided under the limits, untracked): {lost['local_files']} files, {c.human(lost['local_bytes'])}; "
          f"left for your decision (over the limits): {lost['undecided_left_files']} files, {c.human(lost['undecided_left_bytes'])}; secret-looking files (copy by hand): {secrets_n}",
          f"- Linked worktrees: {len(wts)} ({kinds['workstream']} workstream, {kinds['follower']} follower, {kinds['scratch']} scratch, "
          f"{sum(1 for w in wts if w['prunable'])} with a missing directory); of the {len(stream_wts)} workstream worktrees, "
          f"{len(unusable)} have a missing, incomplete or stale handover",
          f"- Export blockers: {len(blockers)}; could not read: {len(d['could_not_read'])}"]
    not_scanned = [r["name"] for r in d["repos"] if r.get("no_scan")]
    L += [f"- Contents never opened by the scan or the secret content check (`no_scan` in migrate.toml): {', '.join(not_scanned) or 'none'}", ""]
    p = d["projected"]
    L += [f"- Projected size on the Mac of what is carried: **{c.human(d['projected_bytes'])}** (bundles about {c.human(p['bundles'])} "
          f"estimated from `git count-objects`, overlay and untracked files {c.human(p['overlay_and_untracked'])}, "
          f"root and directories {c.human(p['root_and_dirs'])}, user configuration {c.human(p['user'])}, Baton's queue configuration "
          f"{c.human(p['baton_state'])}); working trees come on top.", ""]

    L += ["## Readiness for export", ""]
    if not ready:
        L += ["Nothing blocks the export.", ""]
    for r in ready:
        L.append(f"- {'BLOCKER' if r['blocking'] else 'note'} `{r['kind']}` {r['where']}: {r['detail']}. Clears with: {r['action']}")
    L.append("")

    if d.get("decide"):
        L += ["## What to decide", "",
              "Ignored files that no rule covers, over the per-checkout limit. The shortest list of the largest groups whose decision brings "
              "the rest under the limit, per repository, ready to paste into migrate.toml (next to accounts.toml):", ""]
        for name, snippet in d["decide"].items():
            L += ["```toml", snippet, "```", ""]
    L += ["## Would be lost by a plain re-clone", ""]
    if lost["no_remote"]:
        L.append(f"Repositories with no remote (everything in them is local): {', '.join(lost['no_remote'])}")
        L.append("")
    rows = []
    for name, v in lost["repos"].items():
        rows.append([name, v["lost"], v["unique"], v["dirty"], v["stashes"], v["files"], c.human(v["bytes"])])
    L += _table(["repo", "branches lost", "with unique commits", "dirty files", "stashes", "local-only files", "bytes"], rows)
    L.append("")
    for name, v in lost["repos"].items():
        if v["newest"]:
            L.append(f"- `{name}`: newest of {v['lost']}: " + ", ".join(f"{b['name']} ({b['status']}, {_date(b['date'])})" for b in v["newest"]))
    L.append("")

    L += ["## Repositories", ""]
    for r in d["repos"]:
        L += [f"### {r['name']}" + (" (skipped by migrate.toml)" if r["skipped"] else ""), ""]
        L.append(f"- origin: {r['origin'] or 'none'}; branch `{r['branch'] or '(detached)'}` at `{r['head'][:10]}`; "
                 f"{len(r['branches'])} branches, {r['tags']} tags (remote tags unknown offline), {len(r['stashes'])} stashes, "
                 f"{len(r['submodules'])} submodules, LFS {'yes' if r['lfs'] else 'no'}")
        if r.get("extra"):
            L.append(f"- extra repository: target `{r['extra']['target']}` under {r['extra']['base']}")
        rl = r["reflog"]
        L.append("- commits reachable only from reflogs: " + ("unknown (time limit reached)" if rl["cut_short"] else str(rl["commits"])) + " (reflogs are not carried)")
        cc = r["case_collisions"]
        if cc["refs"] or cc["tracked"] or cc["worktrees"]:
            L.append(f"- CASE COLLISIONS: refs {cc['refs']}; tracked paths {cc['tracked'][:5]}; worktrees {cc['worktrees']}")
        if r["dirty"]:
            L.append(f"- dirty in main checkout: {len(r['dirty'])}: " + ", ".join(x["path"] for x in r["dirty"][:8]))
        rows = [[b["name"], b["sha"][:10], b["status"], "unique" if b["lost"] and b["unique"] else "", _date(b["date"])] for b in r["branches"]]
        if rows:
            L += ["", *_table(["branch", "tip", "vs upstream", "commits", "date"], rows)]
        if r["worktrees"]:
            L += ["", *_table(["worktree", "kind", "branch", "head", "dirty", "live", "now.md", "Exit", "note vs last commit"],
                              [[w["rel"] or w["path"], w["kind"], w["branch"] or "(detached)", w["head"][:10], w.get("dirty", "-"),
                                "yes" if w.get("live") else "no",
                                "missing" if not w.get("note", {}).get("exists") else ("ok" if w["note"]["complete"] else "incomplete"),
                                w.get("exit", ""), "older" if w.get("note", {}).get("stale") else "ok"] for w in r["worktrees"]])]
        L.append("")

    L += ["## Local-only files", ""]
    for r in repos:
        parts = [("main", r["local"])] + [(w["key"], w["local"]) for w in r["worktrees"] if "local" in w]
        for tree, l in parts:
            line = [f"carried by rule {l['carried']['files']} ({c.human(l['carried']['bytes'])})",
                    f"rebuildable (left) {len(l['rebuildable'])} dirs/files", f"machine-bound {len(l['machine'])}",
                    f"secret-looking {len(l['secrets'])}", f"untracked {l['untracked']['files']}",
                    f"ignored, undecided {l['undecided']['files']} ({c.human(l['undecided']['bytes'])})"]
            if any(len(l[k]) if isinstance(l[k], list) else l[k]["files"] for k in ("rebuildable", "machine", "secrets", "left", "links", "nested")) or \
                    l["carried"]["files"] or l["undecided"]["files"] or l["untracked"]["files"]:
                L.append(f"- `{r['name']}/{tree}`: " + "; ".join(line))
                for s in l["secrets"]:
                    L.append(f"  - not carried, copy by hand: `{s['rel']}` {c.human(s['size'])}" + (f" ({s['why']})" if s.get("why") else ""))
                big = sorted(l["undecided"]["groups"], key=lambda g: -g["bytes"])
                for g in big[:15]:
                    L.append(f"  - undecided: `{g['path']}` {g['files']} file(s), {c.human(g['bytes'])}, {_date(g['mtime'])}")
                if len(big) > 15:
                    L.append(f"  - ... {len(big) - 15} more undecided entries (audit.json)")
                for item in sorted(l["rebuildable"], key=lambda x: -x["bytes"])[:5]:
                    L.append(f"  - left, rebuildable: `{item['path']}` {item['files']}{'+' if item['approx'] else ''} files, {c.human(item['bytes'])}")
                for item in l["links"] + l["nested"]:
                    L.append(f"  - not carried: `{item['rel']}` ({item.get('kind', 'nested git repository')})")
    L += ["", "Root:", ""]
    for f in d["root_local"]["overlay"]:
        L.append(f"- carry `{f['rel']}` ({f['rule']}, {c.human(f['size'])})")
    for f in d["root_local"]["other"]:
        L.append(f"- not carried (no rule): `{f['rel']}` {c.human(f['size'])}")
    L.append("")
    if d["aliases"]:
        L += ["Aliases at the root (recorded, not surveyed as repositories):", ""]
        for a in d["aliases"]:
            L.append(f"- `{a['name']}` -> `{a['target']}` ({a['kind']}); " + (f"recreated on the Mac as a symlink to `{a['target_in_root']}`" if a["target_in_root"] else "target is outside the root: not carried"))
        L.append("")
    if d["nonrepo"]:
        L += ["Non-repository directories under the root:", ""]
        L += _table(["directory", "files", "size"], [[n["name"], f"{n['files']}{'+' if n['approx'] else ''}", c.human(n["bytes"])] for n in d["nonrepo"]])
        L.append("")

    if d["scan"]:
        L += ["## Windows dependence in tracked files", ""]
        rows = []
        for r in repos:
            w = r.get("windows", {})
            sp = w.get("win_paths", {}).get("split", {})
            rows.append([r["name"], w.get("win_scripts", {}).get("count", 0), w.get("win_paths", {}).get("count", 0),
                         f"{sp.get('root', 0)}/{sp.get('home', 0)}", w.get("powershell", {}).get("count", 0), w.get("exe", {}).get("count", 0),
                         w.get("os_checks", {}).get("count", 0), w.get("scheduling", {}).get("count", 0), w.get("wsl", {}).get("count", 0),
                         w.get("ci", {}).get("count", 0)])
        L += _table(["repo", "scripts", "win paths", "root/home", "powershell", ".exe", "os checks", "scheduling", "wsl", "ci"], rows)
        L.append("")
        for r in repos:
            w = r.get("windows", {})
            lines = []
            for cat in CATEGORIES:
                v = w.get(cat, {})
                if v.get("count") and v.get("examples"):
                    lines.append(f"  - {cat}: " + ", ".join(f"`{e['loc']}` ({e['kind']})" for e in v["examples"]))
            if w.get("ci", {}).get("workflows"):
                lines.append("  - runs-on: " + "; ".join(f"{x['file'].split('/')[-1]}: {', '.join(x['runs_on']) or '?'}" for x in w["ci"]["workflows"][:6]))
            if lines:
                L += [f"- `{r['name']}`"] + lines
        L.append("")

    L += ["## Toolchains (restore commands are suggestions; the import never runs them)", ""]
    for r in repos:
        for t in r["toolchains"]:
            L.append(f"- `{r['name']}` {t['dir']}: {t['tool']}{' ' + t['version'] if t['version'] else ''} ({', '.join(t['evidence'])}): `{t['restore']}`")
    L.append("")

    u = d["user"]
    L += ["## User configuration", ""]
    for vendor in ("claude", "codex"):
        for a in u[vendor]:
            L.append(f"### {vendor} / {a['account']}: `{a['dir']}`" + ("" if a["exists"] else " (missing)"))
            if vendor == "claude":
                L.append(f"- permission rules: {a['permissions']['rules']}, of which Windows-specific: {a['permissions']['windows']}")
            else:
                L.append(f"- [projects.*] trust entries: {a['trust_entries']}")
            for h in a["hooks"]:
                L.append(f"- hook {h['event']} {h['matcher']}: `{h['command']}`")
            for scope, servers in (("global", a["mcp"]["global"]),) + tuple((f"project {k}", v) for k, v in a["mcp"]["projects"].items()):
                for name, s in servers.items():
                    cmd = " ".join([str(s.get("command", ""))] + [str(x) for x in s.get("args", [])]).strip() or s.get("url", "")
                    L.append(f"- MCP ({scope}) {name}: `{cmd}`" + (f" env keys: {', '.join(s['env'])}" if s.get("env") else ""))
            sk = a["skills"]
            L.append(f"- skills: {sk['dirs']} directories, {sk['skill_files']} SKILL.md, {sk['with_paths']} contain the root or home path"
                     + (f"; links: {', '.join(x['name'] + ' -> ' + x['target'] for x in sk['links'])}" if sk["links"] else ""))
            if vendor == "claude":
                L.append(f"- agents: {len(a['agents'])}")
                for m in a["memory"]:
                    if m["files"]:
                        L.append(f"- memory `{m['slug']}`: {m['files']} files, {c.human(m['bytes'])}; workspace: {m['workspace'] or 'no match'}")
                empty = sum(1 for m in a["memory"] if not m["files"])
                if empty:
                    L.append(f"- {empty} more memory directories are empty")
    s = u["ssh"]
    L += ["", "### ssh and git", ""]
    for h in s["hosts"]:
        L.append(f"- ssh host `{h['alias']}`: HostName {h['hostname']}, User {h['user']}, IdentityFile {', '.join(h['identity']) or '-'}")
    L.append(f"- key file names (never read): {', '.join(s['key_names']) or 'none'}")
    L.append(f"- ~/.gitconfig: {len(u['gitconfig'])} entries")
    for e in u["gitconfig"]:
        L.append(f"  - {e}")
    L.append("")

    m = d["machine"]
    L += ["## Machine", "", "Tools found: " + ", ".join(f"{k} ({v})" for k, v in m["tools"].items()), "",
          "Not found: " + ", ".join(m["missing"]), ""]
    if m["npm_global"]:
        L.append("npm global: " + ", ".join(m["npm_global"]))
    if m["uv_tools"]:
        L.append("uv tools: " + "; ".join(m["uv_tools"]))
    for t in m["scheduled_tasks"]:
        L.append(f"- scheduled task `{t['name']}` ({t['status']}): `{t['run']}`")
    L += ["", "### Proposed Brewfile", "", "```", d["brewfile"].rstrip(), "```", ""]

    L += ["## Left behind (named, with the reason)", ""]
    for x in d["left_behind"][:300]:
        L.append(f"- {x.get('repo', '')} {x.get('tree', '')} `{x.get('rel') or x.get('what')}`: {x['why']}" + (" (needs your decision)" if x.get("decision") else ""))
    if len(d["left_behind"]) > 300:
        L.append(f"- ... {len(d['left_behind']) - 300} more (audit.json)")
    L += ["", "## Could not read", ""]
    L += [f"- `{p['path']}`: {p['error']}" for p in d["could_not_read"]] or ["Nothing."]
    L += ["", "## Example migrate.toml", "", "```toml", d["example_toml"].rstrip(), "```", ""]
    return "\n".join(L)


def write_out(out_dir: Path, data: dict, text: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "audit.md").write_text(text, encoding="utf-8", newline="\n")
    (out_dir / "audit.json").write_text(json.dumps(data, indent=1, default=str), encoding="utf-8", newline="\n")
    (out_dir / "Brewfile").write_text(data["brewfile"], encoding="utf-8", newline="\n")
