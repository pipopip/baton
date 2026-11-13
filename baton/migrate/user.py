"""User-level configuration: ``~/.claude*``, ``~/.codex*``, ssh and git config. Survey (audit) and collection (export).

Nothing here reads a credential file: ``.credentials.json``, ``auth.json`` and every secret-looking name are skipped, ssh keys
are only listed by name, and MCP environment values and URL query strings are never printed or copied.
"""
from __future__ import annotations

import fnmatch
import json
import os
import re
import tomllib
from pathlib import Path

from . import common as c
from . import local as loc
from . import scan as scanmod
from . import secrets

REDACTED = secrets.REDACTED
ALWAYS_SKIP = frozenset({".credentials.json", "auth.json"})
SKIP_SUFFIXES = (".sqlite", ".sqlite3", ".db", ".db-wal", ".db-shm", ".log")
SKIP_USER_DIRS = frozenset({"cache", "tmp", "logs", "node_modules", ".venv", "sessions", "shell-snapshots", "todos", "ide",
                            "statsig", "telemetry", "debug", "paste-cache", "file-history", "backups", "plugins"})
# Names directly in an account directory that are neither carried nor listed: credentials, caches, transcripts, state that the
# tool recreates. Anything else is named in "left behind" with its size.
KNOWN_NOT_CARRIED = SKIP_USER_DIRS | frozenset({".credentials.json", "auth.json", ".claude.json", "history.jsonl", "stats-cache.json",
                                                "mcp-needs-auth-cache.json", "session-env", "local", "downloads", "image-cache", "usage-data",
                                                "log", "installation_id", "version.json", "models_cache.json", "shell_snapshots",
                                                "archived_sessions", ".DS_Store", "Thumbs.db"})
KNOWN_NOT_CARRIED_GLOBS = ("*.bak", "*.bak*", "*.backup*", "*.pre-*", "*.before-*", "*-backups", "*.lock", "*.lock.*", "*.tmp", "*.tmp.*", "*.tmp-*",
                           ".tmp", "*.sqlite-shm", "*.sqlite-wal", "*.guard", ".last-*", "*-locks", ".oauth_refresh*")       # backups, locks, scratch
WINDOWSY = re.compile(r"(?i)(?<![A-Za-z0-9_])[A-Za-z]:[\\/]|\bpowershell\b|\bpwsh\b|\.exe\b|\bcmd(?:\.exe)?\s*/c\b|%[A-Z]+%")


# ------------------------------------------------------------------ redaction

def redact_url(url: str) -> str:
    """URL without credentials and query string (see ``secrets.redact_url``)."""
    return secrets.redact_url(url)


def redact_args(args: list) -> list:
    return secrets.redact_args(args)


def redact_mcp_server(server: dict, *, for_bundle: bool = True) -> dict:
    """A copy of one MCP server entry with every env and header value, secret-named value, URL userinfo and query string
    replaced by ``REDACTED`` (names are kept). The report and the bundle show the same thing."""
    return secrets.redact_doc(server)


def redact_mcp_map(servers: dict, *, for_bundle: bool) -> dict:
    return {name: redact_mcp_server(s, for_bundle=for_bundle) if isinstance(s, dict) else s for name, s in (servers or {}).items()}


# ------------------------------------------------------------------ readers

def _json(path: Path) -> dict:
    try:
        with open(c.lp(path), encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _toml(path: Path) -> dict:
    try:
        with open(c.lp(path), "rb") as fh:
            return tomllib.load(fh)
    except (OSError, ValueError):
        return {}


def hook_commands(obj, event: str = "", matcher: str = "") -> list[dict]:
    """Every ``command`` found in a hooks structure (Claude settings.json ``hooks`` or Codex hooks.json), verbatim."""
    out = []
    if isinstance(obj, dict):
        if isinstance(obj.get("command"), str):
            args = [str(a) for a in obj["args"]] if isinstance(obj.get("args"), list) else []
            out.append({"event": event, "matcher": matcher, "command": secrets.redact_command_text(" ".join([obj["command"], *args]))})
        m = obj.get("matcher") if isinstance(obj.get("matcher"), str) else matcher
        for k, v in obj.items():
            if k == "hooks" and isinstance(v, dict):
                for ev, body in v.items():
                    out.extend(hook_commands(body, ev, ""))
            elif isinstance(v, (dict, list)):
                out.extend(hook_commands(v, event, m))
    elif isinstance(obj, list):
        for item in obj:
            out.extend(hook_commands(item, event, matcher))
    return out


def claude_json_path(ctx: c.Ctx, account: str, adir: Path) -> Path:
    inside = adir / ".claude.json"
    if inside.exists():
        return inside
    return ctx.home / ".claude.json" if account == "main" else inside


def mcp_from_claude_json(data: dict, *, for_bundle: bool) -> dict:
    out = {"global": redact_mcp_map(data.get("mcpServers") or {}, for_bundle=for_bundle), "projects": {}}
    for path, body in (data.get("projects") or {}).items():
        if isinstance(body, dict) and body.get("mcpServers"):
            out["projects"][path] = redact_mcp_map(body["mcpServers"], for_bundle=for_bundle)
    return out


def _skill_stats(ctx: c.Ctx, skills: Path) -> dict:
    root_re, home_re = scanmod._prefix_re(str(ctx.root)), scanmod._prefix_re(str(ctx.home))
    total = with_paths = 0
    links = []
    names = []
    try:
        entries = sorted(os.scandir(c.lp(skills)), key=lambda e: e.name)
    except OSError:
        return {"dirs": 0, "skill_files": 0, "with_paths": 0, "names": [], "links": []}
    for e in entries:
        p = skills / e.name
        if c.is_link(p):
            links.append({"name": e.name, "target": c.link_target(p), "kind": c.link_kind(p)})
            continue
        if not e.is_dir(follow_symlinks=False):
            continue
        names.append(e.name)
        for dirpath, dirnames, filenames in os.walk(c.lp(p)):
            dirnames[:] = [d for d in dirnames if not c.is_link(Path(dirpath) / d)]
            if "SKILL.md" in filenames:
                total += 1
                try:
                    text = (Path(dirpath) / "SKILL.md").read_text(encoding="utf-8", errors="replace")
                except OSError:
                    continue
                if (root_re and root_re.search(text)) or (home_re and home_re.search(text)):
                    with_paths += 1
    return {"dirs": len(names), "skill_files": total, "with_paths": with_paths, "names": names, "links": links}


def _memory_dirs(projects: Path, known_slugs: dict[str, str]) -> list[dict]:
    out = []
    try:
        entries = sorted(os.scandir(c.lp(projects)), key=lambda e: e.name)
    except OSError:
        return out
    for e in entries:
        mem = projects / e.name / "memory"
        if not mem.is_dir():
            continue
        files = size = 0
        for dirpath, _, filenames in os.walk(c.lp(mem)):
            for f in filenames:
                files += 1
                try:
                    size += os.stat(os.path.join(dirpath, f)).st_size
                except OSError:
                    pass
        out.append({"slug": e.name, "files": files, "bytes": size, "workspace": known_slugs.get(e.name, "")})
    return out


def ssh_info(home: Path) -> dict:
    cfg = home / ".ssh" / "config"
    hosts, cur = [], None
    try:
        text = cfg.read_text(encoding="utf-8", errors="replace")
    except OSError:
        text = ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"(?i)^(\w+)[\s=]+(.*)$", line)
        if not m:
            continue
        key, value = m.group(1).lower(), m.group(2).strip().strip('"')
        if key == "host":
            cur = {"alias": value, "hostname": "", "user": "", "identity": []}
            hosts.append(cur)
        elif cur is not None and key == "hostname":
            cur["hostname"] = value
        elif cur is not None and key == "user":
            cur["user"] = value
        elif cur is not None and key == "identityfile":
            cur["identity"].append(re.split(r"[\\/]", value)[-1])
    keys = []
    try:
        for name in sorted(os.listdir(c.lp(home / ".ssh"))):
            if name not in ("config", "known_hosts", "known_hosts.old", "authorized_keys") and (home / ".ssh" / name).is_file():
                keys.append(name)
    except OSError:
        pass
    return {"config": cfg.is_file(), "hosts": hosts, "key_names": keys}


def gitconfig_entries(home: Path) -> list[str]:
    path = home / ".gitconfig"
    if not path.is_file():
        return []
    cp = c.git(home, "config", "--file", str(path), "--list", "-z")
    out = []
    for item in cp.stdout.split("\0") if cp.returncode == 0 else []:
        key, _, value = item.partition("\n")
        if key:
            out.append(f"{secrets.redact_userinfo_text(key)}={secrets.gitconfig_value(key, value)}")
    return out


# ------------------------------------------------------------------ survey

def survey_user(ctx: c.Ctx, known_slugs: dict[str, str]) -> dict:
    out: dict = {"claude": [], "codex": []}
    for account, adir in ctx.accounts("claude"):
        adir = Path(adir)
        entry = {"account": account, "dir": str(adir), "exists": adir.is_dir()}
        settings = _json(adir / "settings.json")
        perms = settings.get("permissions") if isinstance(settings.get("permissions"), dict) else {}
        rules = [str(r) for key in ("allow", "deny", "ask") for r in (perms.get(key) or []) if isinstance(r, (str,))]
        entry["hooks"] = hook_commands({"hooks": settings.get("hooks") or {}})
        entry["permissions"] = {"rules": len(rules), "windows": sum(1 for r in rules if WINDOWSY.search(r))}
        entry["settings_keys"] = sorted(settings)
        mcp = mcp_from_claude_json(_json(claude_json_path(ctx, account, adir)), for_bundle=False)
        entry["mcp"] = mcp
        entry["skills"] = _skill_stats(ctx, adir / "skills")
        try:
            entry["agents"] = sorted(f.name for f in (adir / "agents").iterdir() if f.is_file())
        except OSError:
            entry["agents"] = []
        entry["memory"] = _memory_dirs(adir / "projects", known_slugs)
        out["claude"].append(entry)
    for account, adir in ctx.accounts("codex"):
        adir = Path(adir)
        entry = {"account": account, "dir": str(adir), "exists": adir.is_dir()}
        cfg = _toml(adir / "config.toml")
        entry["mcp"] = {"global": redact_mcp_map(cfg.get("mcp_servers") or {}, for_bundle=False), "projects": {}}
        entry["hooks"] = hook_commands(_json(adir / "hooks.json"))
        entry["trust_entries"] = len(cfg.get("projects") or {})
        entry["skills"] = _skill_stats(ctx, adir / "skills")
        out["codex"].append(entry)
    out["ssh"] = ssh_info(ctx.home)
    out["gitconfig"] = gitconfig_entries(ctx.home)
    return out


# ------------------------------------------------------------------ collection (export)

def _add_file(ctx: c.Ctx, out: dict, src: Path, rel: str, size: int) -> None:
    """Carry one user file, or list it for copying by hand when its content or an unparseable configuration says so."""
    held = loc.hold_reason(ctx, None, src, rel, size)
    if held:
        out["secrets"].append({"path": rel, "size": size, "why": held})
    else:
        out["files"].append({"src": str(src), "rel": rel, "size": size})


def _walk_user_tree(ctx: c.Ctx, base: Path, rel_prefix: str, out: dict, *, prune: frozenset = SKIP_USER_DIRS) -> None:
    """Add every file under ``base`` to ``out['files']`` as (source, bundle-relative path); links, secrets and other
    skipped paths are recorded."""
    stack = [""]
    while stack:
        rel = stack.pop()
        here = base / rel if rel else base
        try:
            entries = list(os.scandir(c.lp(here)))
        except OSError as exc:
            ctx.problem(here, f"cannot read directory: {exc}")
            continue
        for e in entries:
            r = (rel + "/" if rel else "") + e.name
            p = here / e.name
            if c.is_link(p):
                out["links"].append({"path": f"{rel_prefix}/{r}", "target": c.link_target(p), "kind": c.link_kind(p)})
            elif e.is_dir(follow_symlinks=False):
                if e.name in prune:
                    out["skipped"].append({"path": f"{rel_prefix}/{r}", "why": "cache, log or session directory"})
                else:
                    stack.append(r)
            elif e.name in ALWAYS_SKIP or e.name.endswith(SKIP_SUFFIXES):
                out["skipped"].append({"path": f"{rel_prefix}/{r}", "why": "credential, database or log file: never copied"})
            elif c.is_secret_name(e.name) and not ctx.include_secrets:
                out["secrets"].append({"path": f"{rel_prefix}/{r}", "size": e.stat(follow_symlinks=False).st_size, "why": "secret-looking name"})
            else:
                try:
                    size = e.stat(follow_symlinks=False).st_size
                except OSError as exc:
                    ctx.problem(p, f"cannot stat: {exc}")
                    continue
                if size > c.MAX_COPY_FILE:
                    out["skipped"].append({"path": f"{rel_prefix}/{r}", "why": "larger than 500 MiB"})
                else:
                    _add_file(ctx, out, p, f"{rel_prefix}/{r}", size)


def _list_left(ctx: c.Ctx, out: dict, adir: Path, base: str, carried: set) -> None:
    """Name every top-level entry of an account directory that is neither carried nor a known credential, cache or transcript."""
    try:
        entries = sorted(os.scandir(c.lp(adir)), key=lambda e: e.name)
    except OSError:
        return
    for e in entries:
        if e.name in carried or e.name in KNOWN_NOT_CARRIED or e.name.endswith(SKIP_SUFFIXES)                 or any(fnmatch.fnmatchcase(e.name, g) for g in KNOWN_NOT_CARRIED_GLOBS):
            continue
        p = adir / e.name
        if c.is_link(p):
            continue                                    # recorded with the other links where it matters
        try:
            if e.is_dir(follow_symlinks=False):
                s = c.summarize_dir(p)
                files, size = s["files"], s["bytes"]
            else:
                files, size = 1, e.stat(follow_symlinks=False).st_size
        except OSError:
            files, size = 0, 0
        out["left"].append({"path": f"{base}/{e.name}", "size": size, "files": files,
                            "why": "user configuration that no rule carries and no known class covers: copy it by hand, or ask for a rule"})


def collect_user(ctx: c.Ctx) -> dict:
    """Files and generated documents of the user configuration to carry, plus what is skipped and why."""
    out: dict = {"files": [], "generated": {}, "links": [], "skipped": [], "secrets": [], "accounts": [], "ssh": {}, "gitconfig": False,
                 "left": []}
    for vendor, singles, trees in (("claude", ("CLAUDE.md", "settings.json", "keybindings.json"), ("agents", "skills", "hooks", "commands", "output-styles")),
                                   ("codex", ("AGENTS.md", "config.toml", "hooks.json"), ("skills", "rules", "prompts"))):
        for account, adir in ctx.accounts(vendor):
            adir = Path(adir)
            base = f"user/{vendor}/{account}"
            out["accounts"].append({"vendor": vendor, "account": account, "dir": str(adir), "exists": adir.is_dir()})
            if not adir.is_dir():
                continue
            _list_left(ctx, out, adir, base, set(singles) | set(trees) | ({"projects"} if vendor == "claude" else set()))
            for name in singles:
                p = adir / name
                if p.is_file():
                    _add_file(ctx, out, p, f"{base}/{name}", p.stat().st_size)
            for name in trees:
                p = adir / name
                if c.is_link(p):
                    out["links"].append({"path": f"{base}/{name}", "target": c.link_target(p), "kind": c.link_kind(p)})
                elif p.is_dir():
                    _walk_user_tree(ctx, p, f"{base}/{name}", out)
            if vendor == "claude":
                projects = adir / "projects"
                try:
                    slugs = sorted(os.scandir(c.lp(projects)), key=lambda e: e.name)
                except OSError:
                    slugs = []
                for s in slugs:
                    mem = projects / s.name / "memory"
                    if c.is_link(mem):
                        out["links"].append({"path": f"{base}/projects/{s.name}/memory", "target": c.link_target(mem), "kind": c.link_kind(mem)})
                    elif mem.is_dir():
                        _walk_user_tree(ctx, mem, f"{base}/projects/{s.name}/memory", out, prune=frozenset())
                data = _json(claude_json_path(ctx, account, adir))
                if data.get("mcpServers") or any(isinstance(b, dict) and b.get("mcpServers") for b in (data.get("projects") or {}).values()):
                    doc = mcp_from_claude_json(data, for_bundle=True)
                    out["generated"][f"{base}/mcpServers.redacted.json"] = (json.dumps(doc, indent=2) + "\n").encode("utf-8")
    ssh = ctx.home / ".ssh"
    if (ssh / "config").is_file():
        _add_file(ctx, out, ssh / "config", "ssh/config", (ssh / "config").stat().st_size)
    info = ssh_info(ctx.home)
    out["ssh"] = {"key_names": info["key_names"], "hosts": info["hosts"]}
    out["generated"]["ssh/key-names.txt"] = ("\n".join(info["key_names"]) + "\n").encode("utf-8")
    if (ctx.home / ".gitconfig").is_file():
        _add_file(ctx, out, ctx.home / ".gitconfig", "gitconfig", (ctx.home / ".gitconfig").stat().st_size)
        out["gitconfig"] = True
    return out
