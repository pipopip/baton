"""``bt backup``: push work that exists on no remote to ``origin`` as ``backup/<date>/...`` branches.

Candidates: every local branch with commits on no remote, plus ``refs/baton/snapshots/**``,
``refs/archive/**`` and ``refs/baton/retired/**`` that no remote has. One ``git push origin`` per
repo (hooks run, no ``--no-verify``); destinations never are master or main. Before pushing, every
``.github/workflows`` file is read: if one could trigger on a push to ``backup/**`` the backup is
refused and the workflow named. Stdlib only (the workflow files are read with a small YAML subset parser).
"""
from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path

from . import gitutil, worktrees

EXTRA_PREFIXES = ("refs/baton/snapshots/", "refs/archive/", "refs/baton/retired/")
PROTECTED = ("refs/heads/master", "refs/heads/main")


class BackupError(Exception):
    pass


# --------------------------------------------------------------------------- workflow triggers (YAML subset)


def _strip_comment(line: str) -> str:
    quote = ""
    for i, ch in enumerate(line):
        if quote:
            if ch == quote:
                quote = ""
        elif ch in "\"'":
            quote = ch
        elif ch == "#" and (i == 0 or line[i - 1] in " \t"):
            return line[:i]
    return line


def _scalar(text: str):
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "\"'":
        return text[1:-1]
    return None if text in ("", "~", "null") else text


def _flow(text: str, i: int = 0):
    """Parse a flow sequence/mapping/scalar starting at ``text[i]``; returns (value, next index)."""
    while i < len(text) and text[i] in " \t":
        i += 1
    if i < len(text) and text[i] == "[":
        items, i = [], i + 1
        while True:
            while i < len(text) and text[i] in " \t,":
                i += 1
            if i >= len(text):
                raise ValueError("unterminated flow sequence")
            if text[i] == "]":
                return items, i + 1
            value, i = _flow(text, i)
            items.append(value)
    if i < len(text) and text[i] == "{":
        out, i = {}, i + 1
        while True:
            while i < len(text) and text[i] in " \t,":
                i += 1
            if i >= len(text):
                raise ValueError("unterminated flow mapping")
            if text[i] == "}":
                return out, i + 1
            j = i
            while j < len(text) and text[j] not in ":,}":
                j += 1
            key = _scalar(text[i:j])
            i = j
            if i < len(text) and text[i] == ":":
                value, i = _flow(text, i + 1)
            else:
                value = None
            out[key] = value
    j = i
    if j < len(text) and text[j] in "\"'":
        end = text.find(text[j], j + 1)
        if end < 0:
            raise ValueError("unterminated quote")
        return text[j + 1:end], end + 1
    while j < len(text) and text[j] not in ",]}":
        j += 1
    return _scalar(text[i:j]), j


def _value(text: str):
    text = text.strip()
    if text[:1] in "[{":
        value, end = _flow(text)
        if text[end:].strip():
            raise ValueError("trailing text after flow value")
        return value
    return _scalar(text)


def _block(lines: list[tuple[int, str]], i: int, indent: int):
    """Parse the block of ``lines`` starting at ``i`` whose indent is ``indent``; returns (value, next index)."""
    if lines[i][1].startswith("-"):
        items = []
        while i < len(lines) and lines[i][0] == indent and lines[i][1].startswith("-"):
            rest = lines[i][1][1:].strip()
            i += 1
            if rest:
                items.append(_value(rest))
            elif i < len(lines) and lines[i][0] > indent:
                value, i = _block(lines, i, lines[i][0])
                items.append(value)
            else:
                items.append(None)
        return items, i
    out = {}
    while i < len(lines) and lines[i][0] == indent and not lines[i][1].startswith("-"):
        text = lines[i][1]
        key, sep, rest = text.partition(":")
        if not sep:
            raise ValueError(f"cannot parse line {text!r}")
        key = _scalar(key)
        i += 1
        if rest.strip():
            out[key] = _value(rest)
        elif i < len(lines) and (lines[i][0] > indent or (lines[i][0] == indent and lines[i][1].startswith("-"))):
            out[key], i = _block(lines, i, lines[i][0])
        else:
            out[key] = None
    return out, i


def workflow_events(text: str):
    """The ``on:`` section of a workflow as a dict {event: config}; raises ValueError when it cannot be read."""
    lines = []
    for raw in text.splitlines():
        stripped = _strip_comment(raw).rstrip()
        if stripped.strip():
            lines.append((len(stripped) - len(stripped.lstrip(" ")), stripped.strip()))
    start = None
    for n, (indent, line) in enumerate(lines):
        if indent == 0 and re.match(r"""^(['"]?)on\1\s*:""", line):
            start = n
            break
    if start is None:
        raise ValueError("no top-level 'on:' key")
    rest = lines[start][1].split(":", 1)[1].strip()
    if rest:
        value = _value(rest)
    else:
        end = start + 1
        while end < len(lines) and lines[end][0] > 0:
            end += 1
        section = lines[start + 1:end]
        if not section:
            raise ValueError("empty 'on:' section")
        value, _ = _block(section, 0, section[0][0])
    if isinstance(value, str):
        return {value: None}
    if isinstance(value, list):
        return {str(v): None for v in value}
    if isinstance(value, dict):
        return value
    raise ValueError("unsupported 'on:' value")


def _glob(pattern: str) -> re.Pattern:
    out, i = [], 0
    while i < len(pattern):
        ch = pattern[i]
        if pattern.startswith("**", i):
            out.append(".*")
            i += 2
            continue
        if ch == "*":
            out.append("[^/]*")
        elif ch == "?":
            out.append("[^/]")
        elif ch == "[":
            end = pattern.find("]", i + 1)
            if end > i:
                out.append(pattern[i:end + 1])
                i = end + 1
                continue
            out.append(r"\[")
        else:
            out.append(re.escape(ch))
        i += 1
    return re.compile("".join(out))


def _matches(patterns: list[str], branch: str) -> bool:
    matched = False
    for pat in patterns:
        negate = pat.startswith("!")
        if _glob(pat[1:] if negate else pat).fullmatch(branch):
            matched = not negate
    return matched


def _as_list(value) -> list[str]:
    if value is None:
        return []
    return [str(v) for v in value] if isinstance(value, list) else [str(value)]


def triggers_on_push(events: dict, branches: list[str]) -> str:
    """Why a push to any of ``branches`` could start this workflow ('' when it cannot)."""
    if "push" not in events:
        return ""
    cfg = events["push"]
    if not isinstance(cfg, dict):
        return "on.push has no branch filter"
    if "branches" in cfg:
        patterns = _as_list(cfg["branches"])
        hit = next((b for b in branches if _matches(patterns, b)), None)
        return f"on.push.branches {patterns} matches {hit}" if hit else ""
    if "branches-ignore" in cfg:
        patterns = _as_list(cfg["branches-ignore"])
        hit = next((b for b in branches if not _matches(patterns, b)), None)
        return f"on.push.branches-ignore {patterns} does not exclude {hit}" if hit else ""
    if "tags" in cfg or "tags-ignore" in cfg:
        return ""            # a tags-only filter never starts on a branch push
    return "on.push has no branch filter"


def workflow_conflicts(main: Path, branches: list[str]) -> list[str]:
    """Findings for workflows that could run on a push to one of ``branches``; unreadable ones count as findings."""
    found = []
    wf_dir = Path(main) / ".github" / "workflows"
    try:
        files = sorted(p for p in wf_dir.iterdir() if p.suffix in (".yml", ".yaml"))
    except OSError:
        return found
    for f in files:
        try:
            events = workflow_events(f.read_text(encoding="utf-8", errors="replace"))
        except ValueError as exc:
            found.append(f"{f.name}: cannot tell whether it runs on pushes ({exc})")
            continue
        why = triggers_on_push(events, branches)
        if why:
            found.append(f"{f.name}: {why}")
    return found


# --------------------------------------------------------------------------- planning and pushing


def needs_backup(main: Path, ref: str) -> bool:
    return bool(gitutil.out(main, "rev-list", "-n1", ref, "--not", "--remotes"))


def plan(main: Path, day: str) -> tuple[list[tuple[str, str]], list[str]]:
    """([(source ref, destination ref)], notes) for the refs that exist on no remote."""
    text = gitutil.out(main, "for-each-ref", "--format=%(refname)", "refs/heads", "refs/baton/snapshots", "refs/archive", "refs/baton/retired")
    pairs, notes = [], []
    for ref in text.splitlines():
        if ref.startswith("refs/heads/"):
            name = ref[len("refs/heads/"):]
            if name.startswith("backup/"):
                continue
        elif ref.startswith(EXTRA_PREFIXES):
            name = ref[len("refs/"):]
        else:
            continue
        dest = f"refs/heads/backup/{day}/{name}"
        if gitutil.git(main, "check-ref-format", dest).returncode != 0:
            notes.append(f"skipped {ref}: {dest} is not a valid ref name")
        elif dest in PROTECTED:
            notes.append(f"refused {ref}: destination {dest} is protected")
        elif needs_backup(main, ref):
            pairs.append((ref, dest))
    dests = {d for _, d in pairs}
    clean = []
    for src, dest in pairs:
        if any(other.startswith(dest + "/") for other in dests):
            notes.append(f"skipped {src}: {dest} would collide with a longer backup ref")
        else:
            clean.append((src, dest))
    return clean, notes


def backup_repo(main: Path, dry_run: bool, day: str | None = None) -> int:
    day = day or time.strftime("%Y-%m-%d")
    print(f"{main}")
    if not gitutil.out(main, "remote", "get-url", "origin"):
        print("  no remote 'origin'; nothing to do")
        return 1
    pairs, notes = plan(main, day)
    for note in notes:
        print(f"  {note}")
    if not pairs:
        print("  everything is already on a remote")
        return 0
    findings = workflow_conflicts(main, [d[len("refs/heads/"):] for _, d in pairs])
    if findings:
        print("  refused: a workflow could run on pushes to backup/** (CI minutes cost money):", file=sys.stderr)
        for finding in findings:
            print(f"    {finding}", file=sys.stderr)
        print("  add `branches-ignore: ['backup/**']` to those workflows (or restrict `branches`) first", file=sys.stderr)
        return 2
    for src, dest in pairs:
        print(f"  {src} -> {dest}")
    if dry_run:
        print(f"  dry run: {len(pairs)} ref(s) would be pushed with one `git push origin`")
        return 0
    cp = gitutil.git(main, "push", "origin", *[f"{s}:{d}" for s, d in pairs], timeout=1800)
    sys.stdout.write(cp.stdout)
    sys.stderr.write(cp.stderr)
    if cp.returncode != 0:
        print(f"  git push failed (exit {cp.returncode})", file=sys.stderr)
        return 1
    print(f"  pushed {len(pairs)} ref(s)")
    return 0


def run(args) -> int:
    if args.repo:
        main = worktrees.resolve_repo(args.repo)
        repos = [main] if main else []
    else:
        here = worktrees.resolve_repo(None)
        if here:
            repos = [here]
        else:
            skip = worktrees.read_only_remotes()
            repos = [r for r in worktrees.known_repos() if Path(r).name.lower() not in skip]
            for r in worktrees.known_repos():
                if Path(r).name.lower() in skip:
                    print(f"skipped {r}: read-only remote ([worktrees] read_only_remotes); name it with --repo to try anyway")
    if not repos:
        print("bt backup: no repository found; pass --repo PATH", file=sys.stderr)
        return 2
    worst = 0
    for main in repos:
        worst = max(worst, backup_repo(main, args.dry_run))
    return worst


def register(sub) -> None:
    p = sub.add_parser("backup", help="push branches and baton refs that exist on no remote to origin as backup/<date>/...")
    p.add_argument("--repo", help="repository (default: the current one, else every known repo)")
    p.add_argument("--dry-run", action="store_true", help="show what would be pushed")
    p.set_defaults(func=run)
