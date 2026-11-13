"""The machine as it is now (tool versions, global packages, scheduled tasks) and the Homebrew Brewfile it implies."""
from __future__ import annotations

import csv
import io
import json
import os
import shutil
import subprocess
import textwrap
from concurrent.futures import ThreadPoolExecutor

from .. import config
from . import common as c
from . import secrets

VERSION_ARGS = {"java": ["-version"], "go": ["version"], "adb": ["version"], "doctl": ["version"]}
TOOL_TIMEOUT = 20

# tool or toolchain key -> (kind, name, what it is for). One table: the Brewfile is generated from it.
BREW: dict[str, tuple[str, str, str]] = {
    "python": ("brew", "python@3.12", "Python 3.12 (Baton and the Python repositories)"),
    "node": ("brew", "node", "Node.js and npm"),
    "npm": ("brew", "node", "Node.js and npm"),
    "pnpm": ("brew", "pnpm", "pnpm"),
    "yarn": ("brew", "yarn", "yarn"),
    "bun": ("brew", "oven-sh/bun/bun", "bun"),
    "git": ("brew", "git", "git"),
    "gh": ("brew", "gh", "GitHub CLI"),
    "rg": ("brew", "ripgrep", "ripgrep (agents search with it)"),
    "fd": ("brew", "fd", "fd"),
    "fzf": ("brew", "fzf", "fzf"),
    "jq": ("brew", "jq", "jq"),
    "uv": ("brew", "uv", "uv (Python environments)"),
    "go": ("brew", "go", "Go toolchain"),
    "java": ("brew", "openjdk", "JDK (Gradle, Android)"),
    "gradle": ("brew", "gradle", "Gradle (projects also bring their own wrapper)"),
    "adb": ("cask", "android-platform-tools", "adb"),
    "android": ("cask", "android-studio", "Android Studio (an Android project is tracked)"),
    "ffmpeg": ("brew", "ffmpeg", "ffmpeg"),
    "blender": ("cask", "blender", "Blender"),
    "lean": ("brew", "elan-init", "elan: installs the Lean toolchain each project pins"),
    "lake": ("brew", "elan-init", "elan: installs the Lean toolchain each project pins"),
    "elan": ("brew", "elan-init", "elan: installs the Lean toolchain each project pins"),
    "cargo": ("brew", "rustup", "rustup (Rust toolchain)"),
    "rustup": ("brew", "rustup", "rustup (Rust toolchain)"),
    "docker": ("cask", "docker", "Docker Desktop"),
    "pwsh": ("cask", "powershell", "PowerShell (only while .ps1 workflows remain)"),
    "claude": ("cask", "claude-code", "Claude Code"),
    "codex": ("brew", "codex", "Codex CLI"),
    "tailscale": ("cask", "tailscale", "Tailscale (reaches ember and hestia)"),
    "aws": ("brew", "awscli", "AWS CLI"),
    "doctl": ("brew", "doctl", "DigitalOcean CLI"),
    "dotnet": ("cask", "dotnet-sdk", ".NET SDK"),
    "just": ("brew", "just", "just"),
    "git-lfs": ("brew", "git-lfs", "git-lfs (a repository uses LFS)"),
    "iterm2": ("cask", "iterm2", "iTerm2: the terminal Baton drives on the Mac"),
    "zoxide": ("brew", "zoxide", "zoxide (smarter cd)"),
    "delta": ("brew", "git-delta", "delta, the git pager (command `delta`)"),
    "pdftotext": ("brew", "poppler", "poppler (pdftotext and the other PDF tools)"),
    "restic": ("brew", "restic", "restic backups"),
    "gcloud": ("cask", "gcloud-cli", "Google Cloud CLI (command `gcloud`)"),
    "code": ("cask", "visual-studio-code", "Visual Studio Code (command `code`)"),
    "maestro": ("brew", "mobile-dev-inc/tap/maestro", "Maestro mobile UI tests (Homebrew tap mobile-dev-inc/tap)"),
}
NO_BREW = {"wrangler": "npm install -g wrangler", "xcode": "Xcode: install from the App Store (`mas install 497799835`)"}


def _version(tool: str, ctx: c.Ctx) -> tuple[str, str]:
    path = shutil.which(tool)
    if not path:
        return tool, ""
    if not ctx.run_tools:
        return tool, "(found)"
    try:
        cp = subprocess.run([path, *VERSION_ARGS.get(tool, ["--version"])], capture_output=True, text=True, encoding="utf-8",
                            errors="replace", timeout=TOOL_TIMEOUT, stdin=subprocess.DEVNULL, creationflags=config.CREATE_NO_WINDOW)
        lines = [l.strip() for l in (cp.stdout + "\n" + cp.stderr).splitlines() if l.strip()]
        return tool, lines[0][:120] if lines else "(no version output)"
    except (OSError, subprocess.SubprocessError) as exc:
        return tool, f"(found; version failed: {type(exc).__name__})"


def _run_json(cmd: list[str], timeout: int = 60):
    try:
        cp = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout,
                            stdin=subprocess.DEVNULL, creationflags=config.CREATE_NO_WINDOW)
    except (OSError, subprocess.SubprocessError):
        return None
    return cp.stdout if cp.returncode == 0 else None


def scheduled_tasks(ctx: c.Ctx) -> list[dict]:
    """Windows scheduled tasks whose action mentions the code root (schtasks); empty elsewhere or on failure."""
    if os.name != "nt" or not ctx.run_tools or not shutil.which("schtasks"):
        return []
    text = _run_json(["schtasks", "/query", "/fo", "csv", "/v"], 90)
    if not text:
        return []
    out = []
    needle = str(ctx.root).replace("\\", "/").lower()
    rows = list(csv.reader(io.StringIO(text)))
    header = next((r for r in rows if "TaskName" in r), None)
    if not header:
        return []
    col = {n: i for i, n in enumerate(header)}
    for r in rows:
        if r == header or len(r) < len(header):
            continue
        run = r[col["Task To Run"]] if "Task To Run" in col else ""
        if needle in run.replace("\\", "/").lower():
            out.append({"name": r[col["TaskName"]], "run": secrets.redact_command_text(run), "status": r[col["Status"]] if "Status" in col else ""})
    return out


def survey_machine(ctx: c.Ctx) -> dict:
    with ThreadPoolExecutor(max_workers=8) as pool:
        versions = dict(pool.map(lambda t: _version(t, ctx), c.TOOL_NAMES))
    if not versions.get("maestro") and any((ctx.root / "tools" / n).exists() for n in ("maestro", "maestro.cmd")):
        versions["maestro"] = "(found under tools/maestro)"
    out = {"tools": {k: v for k, v in versions.items() if v}, "missing": [k for k, v in versions.items() if not v],
           "npm_global": [], "uv_tools": [], "scheduled_tasks": scheduled_tasks(ctx)}
    if ctx.run_tools and shutil.which("npm"):
        text = _run_json([shutil.which("npm"), "ls", "-g", "--depth=0", "--json"])
        try:
            deps = (json.loads(text or "{}").get("dependencies") or {})
            out["npm_global"] = sorted(f"{k}@{v.get('version', '?')}" for k, v in deps.items() if isinstance(v, dict))
        except ValueError:
            pass
    if ctx.run_tools and shutil.which("uv"):
        text = _run_json([shutil.which("uv"), "tool", "list"])
        out["uv_tools"] = [l.strip() for l in (text or "").splitlines() if l.strip() and not l.startswith("-")]
    return out


def brewfile(machine: dict, repos: list[dict]) -> str:
    """Brewfile text from the mapping table: tools found on this machine and tools a repository's toolchain needs."""
    why: dict[tuple[str, str], list[str]] = {}
    manual: dict[str, list[str]] = {}

    def want(key: str, reason: str) -> None:
        if key in BREW:
            kind, name, _ = BREW[key]
            why.setdefault((kind, name), []).append(reason)
        elif key in NO_BREW:
            manual.setdefault(key, []).append(reason)

    for tool, version in sorted(machine.get("tools", {}).items()):
        if tool == "pwsh":
            continue
        want(tool, f"on this machine: {version[:40]}")
    ps1 = False
    for repo in repos:
        if (repo.get("windows") or {}).get("win_scripts", {}).get("count") and (repo["windows"]["win_scripts"]["without_sibling"]):
            ps1 = True
        if repo.get("lfs"):
            want("git-lfs", f"{repo['name']}: tracked files use LFS")
        for tc in repo.get("toolchains", []):
            for need in tc["needs"]:
                want(need, f"{repo['name']}: {tc['tool']} ({tc['evidence'][0]})")
    if ps1:
        want("pwsh", ".ps1 files without a .sh/.py counterpart remain in tracked files")
    want("iterm2", "Baton's terminal backend on the Mac")
    lines = ["# Proposed Brewfile, generated by `bt migrate audit`. Review before `brew bundle --file Brewfile`.", ""]
    for (kind, name), reasons in sorted(why.items(), key=lambda kv: (kv[0][0] != "brew", kv[0][1])):
        uniq = list(dict.fromkeys(reasons))
        note = "; ".join(uniq[:3]) + (f"; +{len(uniq) - 3} more" if len(uniq) > 3 else "")
        note = textwrap.shorten(note, 150, placeholder=" ...")
        lines.append(f'{kind} "{name}"  # {BREW_WHY.get(name, "")}{" — " if BREW_WHY.get(name) else ""}{note}')
    for key, reasons in sorted(manual.items()):
        lines.append(f"# {NO_BREW[key]}  # {textwrap.shorten('; '.join(dict.fromkeys(reasons)), 100, placeholder=' ...')}")
    return "\n".join(lines) + "\n"


BREW_WHY = {name: what for _, (_, name, what) in BREW.items()}
