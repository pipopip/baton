"""Session-start capsule: a few hundred tokens of worktree state, injected by the SessionStart hook.

``build()`` is used by the hook (and ``bt now --print``); the section helpers are
shared with the UserPromptSubmit inbox notice. Stdlib only; hook-safe under
``python -S`` (argparse is not imported here).
"""
from __future__ import annotations

import importlib
import json
import os
import re
import stat
import sys
import threading
import time
from pathlib import Path

from . import config, gitutil, paths

NOW_MAX_LINES = 40
NOW_MAX_CHARS = 3500
LEGACY_CHARS = 2500
INBOX_BUDGET = 600           # chars of inbox lines in context
INBOX_LINE_CAP = 200
SUBJECT_CAP = 70
CAPS = {"claude": 9000, "codex": 6000}   # additionalContext caps
GIT_TIMEOUT = 4              # seconds per git call

# The now.md contract, defined once here: the template, the lint (``now_problems``), the capsule and the cycle
# prompts all use it. ``bt wt new`` writes the header lines; the body fields are filled in at the handover.
HEADER_FIELDS = ("Purpose", "Exit", "Base", "Parent")
BODY_FIELDS = ("Objective", "State", "In flight", "Decisions", "Next")
CONTRACT = (
    "Keep the header lines (`# Now — <ws>`, Purpose:, Exit: pr|merge:<branch>|discard, Base:, Parent:). Body: "
    "Objective: (one line); State: (what is done; each verification claim names the commit sha it was verified at); "
    "In flight: (queue job IDs, background watchers with their exact restart commands, open PRs, pending reviews, "
    "unresolved inbox notices, or `none`); Decisions:; Next: (the single next concrete action). "
    f"At most {NOW_MAX_LINES} lines and {NOW_MAX_CHARS} characters, replaced in place; history goes to .baton/log.md."
)

BODY_TEMPLATE = (
    "Objective:\n"
    "State:\n"
    "In flight:\n"
    "Decisions:\n"
    "Next:\n"
    "<!-- ≤40 lines, replace in place. State: name the commit sha each verification was run at. "
    "History → .baton/log.md (never auto-loaded). `bt now check` lints this file. -->\n"
)
TEMPLATE = "# Now — {ws}\nPurpose:\nExit:\n" + BODY_TEMPLATE

RULES = (
    "Work on the task; do not keep notes, logs or status files current while working. .baton/now.md is the handover: "
    "write it when the session ends or pauses, when the deliverable is complete, or when Baton asks for it (a context "
    "cycle); `bt now check` lints it. Git, the queue's own records and this capsule carry the rest. "
    "Explore freely inside this worktree. Do not create worktrees: only the lead creates them, "
    "with `bt wt new <name> --exit ...`, within the cap; retire finished ones with `bt wt retire`. "
    "Followers start from your commits and report here; merge "
    "them with `bt merge <role>`. Delegate by role: cheapest role that "
    "fits. Goals need a token budget. Compaction preserves the active task: continue from the "
    "objective and unfinished operations; choose a fresh session only for an intentional handoff. "
    "Release completed owned child agents through the client's supported close/unload/archive action "
    "once their result is consumed and no work remains; never stop a session solely because it is idle."
)

LOG_MAX_LINES = 200

MIGRATION_NOTES = "migration-notes.md"      # written by `bt migrate import` into a moved checkout's .baton
MIGRATION_DAYS = 30                         # the capsule mentions the notes for this long
MIGRATION_MAX_BYTES = 256 * 1024            # a notes file larger than this is not what the import wrote: no line

NOW_HEADING = "## Now  (.baton/now.md — the handover: Objective, State, In flight, Decisions, Next; written at session end, pause or completion; ≤40 lines)"
COMPACT_HEADING = "## Last compaction  (.baton/compact.md)"
NEW_NOTE = "(new — empty; it is written at the handover when the session ends)"
LEGACY_NOTE = "(legacy note; the handover moves its live part into .baton/now.md)"
REPAIR_CAP = 160             # chars of the problem named in the one-line stale-note notice
INBOX_HINT = "(merge with `bt merge <role>`, dismiss with `bt inbox done <name>`)"
TRUNCATED = "\n...[truncated to fit the context cap]"

# Lowest number = kept first when the cap forces truncation.
PRIORITY = ("warning", "header", "migration", "mismatch", "now", "compute", "inbox", "compact", "rules", "roles")
# Order in which the sections appear.
DISPLAY = ("warning", "header", "migration", "mismatch", "now", "compute", "compact", "inbox", "rules", "roles")


# --------------------------------------------------------------------------- file helpers


def _read(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace").lstrip("﻿")
    except OSError:
        return None


def now_path(root: Path) -> Path:
    return Path(root) / paths.BATON_DIRNAME / "now.md"


def ensure_now(root: Path, ws: str) -> bool:
    """Create ``.baton/now.md`` from the template if missing. Returns True when it was created."""
    path = now_path(root)
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "x", encoding="utf-8", newline="\n") as fh:
            fh.write(TEMPLATE.format(ws=ws))
    except FileExistsError:
        return False
    return True


def _is_blank_template(text: str) -> bool:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    labels = ("Objective", "Acceptance", "State", "In flight", "Decisions", "Next")
    return all(
        ln.startswith("# Now") or ln.startswith("<!--") or ln.rstrip(":") in labels
        or ln.split(":", 1)[0] in HEADER_FIELDS   # header lines `bt wt new` writes
        for ln in lines
    )


# --------------------------------------------------------------------------- the now.md contract: parse and lint

_NAMES = r"Purpose|Exit|Base|Parent|Objective|Acceptance|State|In[ -]?flight|Decisions?|Next(?:[ -](?:action|steps?))?"
# `Next: ...`, `- Next: ...`, `**Next:** ...`, `**Next**: ...`, `NEXT (nothing open): ...`
_LABEL = re.compile(rf"^[ \t]*(?:[-*][ \t]+)?(?:\*\*|__)?({_NAMES})(?:[ \t]*\([^)]*\))?(?:\*\*|__)?[ \t]*:(?:\*\*|__)?[ \t]*(.*)$", re.I)
# `## Next`, `### In flight:`
_HEADING_LABEL = re.compile(rf"^[ \t]*#{{1,6}}[ \t]+(?:\*\*)?({_NAMES})(?:\*\*)?[ \t]*:?[ \t]*$", re.I)
_HEADING = re.compile(r"^[ \t]{0,3}#{1,6}[ \t]")
_LIST_OR_INDENT = re.compile(r"^(?:[ \t]+\S|[-*+][ \t]|\d+[.)][ \t])")
# Month names as written (capitalised, whole words): under the heading patterns' re.I a bare prefix would read
# "decisions" as December and "Marking" as March, and "may" as May.
_MONTH = (r"(?-i:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sep(?:t(?:ember)?)?"
          r"|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)")
_DATE = rf"(?:\d{{4}}-\d{{2}}-\d{{2}}|\b\d{{1,2}}[ ]{_MONTH}\b|\b{_MONTH}[ ]\d{{1,2}}\b)"
_DIARY_HEADING = re.compile(rf"^[ \t]*#{{2,6}}[ \t]+.*(?:{_DATE}|\bSession[ ]\d+\b)", re.I)
_DIARY_LABEL = re.compile(rf"^[ \t]*(?:\*\*)?[A-Za-z][A-Za-z -]{{0,30}}\([^)]{{0,12}}{_DATE}[^)]{{0,12}}\)(?:\*\*)?:?[ \t]*$", re.I)
STALE_DAYS = 3
LINE_CAP = 300                  # chars of a line kept when the capsule must shorten the note
HEADER_LINE_CAP = 200
HEADER_KEYS = ("purpose", "exit", "base", "parent")
BLOCK_CAPS = {"next": (1400, 14), "inflight": (1000, 10)}   # chars, lines kept of a block that must be kept whole


def _key(name: str) -> str:
    key = re.sub(r"[ -]", "", name.lower())
    return "next" if key.startswith("next") else "decisions" if key.startswith("decision") else key


def fields(lines: list[str]) -> list[tuple[str, int, int, str]]:
    """``[(key, first line, end line (exclusive), text after the label)]`` for the contract labels in ``lines``.

    A block runs to the next label or heading; after a blank line it also ends unless the next line is a list item
    or indented, or the block has no text yet (a bare label, a blank line, then its paragraph). Keys: purpose, exit, base, parent, objective, acceptance, state, inflight, decisions, next."""
    found: list[tuple[str, int, bool, str]] = []
    for i, line in enumerate(lines):
        m = _LABEL.match(line)
        if m:
            found.append((_key(m.group(1)), i, False, m.group(2).strip()))
            continue
        m = _HEADING_LABEL.match(line)
        if m:
            found.append((_key(m.group(1)), i, True, ""))
    starts = {i for _, i, _, _ in found}
    out = []
    for key, i, heading, inline in found:
        j = i + 1
        while j < len(lines):
            line = lines[j]
            if j in starts or _HEADING.match(line):
                break
            if not heading and not line.strip():
                k = j + 1
                while k < len(lines) and not lines[k].strip():
                    k += 1
                bare = not inline and not any(ln.strip() for ln in lines[i + 1:j])
                if (k >= len(lines) or k in starts or _HEADING.match(lines[k])
                        or not (bare or _LIST_OR_INDENT.match(lines[k]))):
                    break
            j += 1
        out.append((key, i, j, inline))
    return out


def _has_content(lines: list[str], span: tuple[str, int, int, str]) -> bool:
    _key_, start, end, inline = span
    if inline and not inline.startswith("<!--"):
        return True
    return any(ln.strip() and not ln.strip().startswith("<!--") for ln in lines[start + 1:end])


def now_problems(text: str, *, mtime: float | None = None, head_time: float | None = None,
                 exit_required: bool = True) -> list[str]:
    """What is wrong with a now.md, as short actionable strings (empty list: clean).

    ``exit_required`` is False for a main checkout, which has no merge target. ``mtime`` and ``head_time`` (epoch
    seconds of the file and of HEAD's commit) enable the staleness check. Old notes that use ``Acceptance:`` or
    lack ``State:``/``In flight:`` are not a problem in themselves; a job id without an ``In flight:`` field is not
    either."""
    if _is_blank_template(text):
        return ["unfilled template: fill in Objective, State, In flight, Decisions and Next"]
    lines = text.splitlines()
    by_key: dict[str, list] = {}
    for span in fields(lines):
        by_key.setdefault(span[0], []).append(span)
    problems = []
    if exit_required:
        exits = by_key.get("exit", [])
        if not exits:
            problems.append("no `Exit:` line: add `Exit: pr|merge:<branch>|discard` under the title")
        elif not any(span[3] and not span[3].startswith("<!--") for span in exits):
            problems.append("`Exit:` is empty: say how this worktree ends (pr|merge:<branch>|discard)")
    if not any(_has_content(lines, span) for key in ("objective", "purpose") for span in by_key.get(key, [])):
        problems.append("no `Objective:` or `Purpose:` with content: state in one line what this worktree is for")
    if not any(_has_content(lines, span) for span in by_key.get("next", [])):
        problems.append("no `Next:` with content: name the single next concrete action")
    n_lines, n_chars = len(lines), len(text)
    if n_lines > NOW_MAX_LINES or n_chars > NOW_MAX_CHARS:
        problems.append(f"{n_lines} lines / {n_chars} chars (limit {NOW_MAX_LINES} / {NOW_MAX_CHARS}): the capsule cannot show "
                        "the rest; move history to .baton/log.md and keep State short")
    diary = [ln.strip() for ln in lines if _DIARY_HEADING.match(ln) or _DIARY_LABEL.match(ln)]
    if diary:
        shown = ", ".join(f"`{d[:40]}`" for d in diary[:2])
        problems.append(f"{len(diary)} dated or session heading(s) ({shown}): history belongs in .baton/log.md; "
                        "keep one current State")
    if mtime is not None and head_time is not None and head_time - mtime > STALE_DAYS * 86400:
        problems.append(f"now.md was last written {round((head_time - mtime) / 86400)} days before HEAD's last commit: "
                        "bring State, In flight and Next up to date")
    return problems


def repair_block(problems: list[str]) -> str:
    """The capsule's one-line notice for a stale or malformed now.md ("" when none). It asks for nothing now: the
    note is rewritten at the next handover, which is the only time a session is asked to write it."""
    if not problems:
        return ""
    first = _clip(problems[0], REPAIR_CAP)
    more = f" (+{len(problems) - 1} more: `bt now check`)" if len(problems) > 1 else ""
    return f"now.md is stale or malformed ({first}){more}; it will be rewritten at the next handover."


def _exit_required(root: Path) -> bool:
    try:
        return config.norm(paths.main_checkout(Path(root))) != config.norm(root)
    except Exception:  # noqa: BLE001
        return True


def problems_for(root: Path, *, head_time: float | None = None) -> list[str]:
    """Lint of the worktree's .baton/now.md; a missing file is one problem. Never raises."""
    try:
        path = now_path(root)
        text = _read(path)
        if text is None:
            return ["no .baton/now.md: run `bt now` to create it"]
        return now_problems(text, mtime=config.mtime(path), head_time=head_time, exit_required=_exit_required(root))
    except Exception:  # noqa: BLE001 - lint trouble must never hide the note
        return []


def _clip(line: str, cap: int) -> str:
    return line if len(line) <= cap else line[: cap - 4].rstrip() + " ..."


def fit_now(text: str) -> str:
    """``text`` cut to ``NOW_MAX_LINES``/``NOW_MAX_CHARS`` without losing the parts the next session needs.

    Always kept: the title, the header lines, the first Objective line, and the whole ``Next:`` and ``In flight:``
    blocks (each within ``BLOCK_CAPS``). The rest is kept from the top while it fits; what is left out is replaced
    by one marker line naming the count and the full file. Blank lines are dropped."""
    lines = text.rstrip().splitlines()
    if len(text) <= NOW_MAX_CHARS and len(lines) <= NOW_MAX_LINES:
        return text.rstrip()
    spans = fields(lines)
    budget_chars = NOW_MAX_CHARS - 150          # room for the marker line
    budget_lines = NOW_MAX_LINES - 1
    keep: dict[int, str] = {}
    used = [0, 0]                               # chars, lines

    def take(index: int, cap: int) -> bool:
        if index in keep or not lines[index].strip():
            return True
        line = _clip(lines[index].rstrip(), cap)
        if used[0] + len(line) + 1 > budget_chars or used[1] + 1 > budget_lines:
            return False
        keep[index] = line
        used[0] += len(line) + 1
        used[1] += 1
        return True

    first = next((i for i, ln in enumerate(lines) if ln.strip()), None)
    if first is not None and lines[first].lstrip().startswith("#"):
        take(first, LINE_CAP)
    seen: set[str] = set()
    for key, start, _end, _inline in spans:
        if key in HEADER_KEYS and key not in seen:
            seen.add(key)
            take(start, HEADER_LINE_CAP)
    for want in ("next", "inflight"):
        cap_chars, cap_lines = BLOCK_CAPS[want]
        for key, start, end, _inline in spans:
            if key != want:
                continue
            chars = n = 0
            for i in range(start, end):
                if not lines[i].strip():
                    continue
                chars += len(lines[i]) + 1
                n += 1
                if chars > cap_chars or n > cap_lines or not take(i, LINE_CAP):
                    break
    for key, start, _end, _inline in spans:
        if key == "objective":
            take(start, LINE_CAP)
            break
    for i, line in enumerate(lines):            # the rest, from the top, while it fits
        if line.strip() and i not in keep and not take(i, LINE_CAP):
            break
    out: list[str] = []
    left_out = 0
    marker_at = None
    for i, line in enumerate(lines):
        if i in keep:
            out.append(keep[i])
        elif line.strip():
            left_out += 1
            if marker_at is None:
                marker_at = len(out)
    shortened = sum(1 for i, kept in keep.items() if kept != lines[i].rstrip())
    if left_out or shortened:
        what = f"{left_out} line(s) left out" + (f", {shortened} shortened" if shortened else "")
        out.insert(len(out) if marker_at is None else marker_at,
                   f"(over budget: {what}; the full note is .baton/now.md; move history to .baton/log.md)")
    return "\n".join(out)


def rotate_log(root: Path, now: float | None = None) -> Path | None:
    """Move ``.baton/log.md`` to ``.baton/archive/log-<YYYYmmdd>.md`` once it is over 200 lines.

    The log is never auto-loaded, so this is hygiene. A same-day archive is appended to, never overwritten.
    Returns the archive path, or None when nothing moved. Never raises.
    """
    log = Path(root) / paths.BATON_DIRNAME / "log.md"
    try:
        data = log.read_bytes()
        if len(data.splitlines()) <= LOG_MAX_LINES:
            return None
        import time
        stamp = time.strftime("%Y%m%d", time.localtime(now))
        target = log.parent / "archive" / f"log-{stamp}.md"
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            with open(target, "ab") as fh:
                fh.write(b"\n" + data)
            os.remove(log)
        else:
            os.replace(log, target)
        return target
    except OSError:
        return None


def _legacy_note(root: Path, ws: str) -> str:
    for base in (Path(root), paths.main_checkout(Path(root))):
        text = _read(base / "docs" / "handovers" / f"{ws}.md")
        if text and text.strip():
            return text
    return ""


# --------------------------------------------------------------------------- header


def _head_branch(root: Path) -> str:
    gd = paths.git_dir(root)
    text = _read(gd / "HEAD") if gd else None
    if text and text.startswith("ref:"):
        ref = text.split(":", 1)[1].strip()
        return ref[len("refs/heads/"):] if ref.startswith("refs/heads/") else ref
    return ""


def _default_branch(root: Path) -> str:
    common = paths.git_common_dir(root)
    if common is None:
        return ""
    packed = _read(common / "packed-refs") or ""
    for name in ("master", "main"):
        if (common / "refs" / "heads" / name).is_file() or f" refs/heads/{name}\n" in packed + "\n":
            return name
    return ""


def _parse_dirty(stdout: str) -> list[str]:
    files = []
    for line in stdout.splitlines():
        path = line[3:].strip().strip('"')
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        if path and not path.startswith(".baton/"):
            files.append(path)
    return files


def _git_text(root: Path, *args: str) -> str | None:
    """stdout of a git call, None on failure or timeout (not stripped: porcelain lines start with a space)."""
    try:
        cp = gitutil.git(root, "--no-optional-locks", *args, timeout=GIT_TIMEOUT)
    except Exception:
        return None
    return cp.stdout if cp.returncode == 0 else None


def header(root: Path, ws: str, repo: str, use_git: bool = True) -> str:
    """One-line worktree status. Three git calls (log, status, rev-list), run concurrently."""
    root = Path(root)
    branch = _head_branch(root)
    default = _default_branch(root)
    results: dict[str, str] = {}
    if use_git:
        jobs = {
            "log": ("log", "-1", "--format=%H%x1f%s"),
            "status": ("-c", "core.quotepath=off", "status", "--porcelain", "--untracked-files=normal"),
        }
        if default:
            jobs["ab"] = ("rev-list", "--left-right", "--count", f"HEAD...{default}")

        def run(key: str, args: tuple[str, ...]) -> None:
            text = _git_text(root, *args)
            if text is not None:  # a failed or timed-out call omits its segment instead of reporting "clean"
                results[key] = text

        threads = [threading.Thread(target=run, args=(k, a), daemon=True) for k, a in jobs.items()]
        for t in threads:
            t.start()
        for t in threads:
            t.join(GIT_TIMEOUT + 2)

    parts = [f"[baton] ws={ws} repo={repo} branch={branch or 'detached'}"]
    sha, _, subject = results.get("log", "").strip().partition("\x1f")
    if sha:
        subject = subject.strip()
        if len(subject) > SUBJECT_CAP:
            subject = subject[: SUBJECT_CAP - 3] + "..."
        parts.append(f'head={sha[:7]} "{subject}"')
    segments = [" ".join(parts)]
    if "status" in results:
        dirty = _parse_dirty(results["status"])
        if dirty:
            shown = ", ".join(dirty[:3])
            more = f" (+{len(dirty) - 3} more)" if len(dirty) > 3 else ""
            segments.append(f"{len(dirty)} dirty: {shown}{more}")
        else:
            segments.append("clean")
    counts = results.get("ab", "").split()
    if len(counts) == 2 and all(c.isdigit() for c in counts):
        segments.append(f"+{counts[0]}/-{counts[1]} vs {default}")
    return " | ".join(segments)


# --------------------------------------------------------------------------- brief vs branch

BRANCH_IN_TEXT = re.compile(r"(?<![\w./-])(?:feat|fix|chore|docs|refactor|test|perf|hotfix|release|wt)/[\w.-]+(?:/[\w.-]+)*")
HEADER_LINE = re.compile(r"\s*(?:Purpose|Exit|Base|Parent):")
FILE_LIKE = re.compile(r"\.[A-Za-z][A-Za-z0-9]{0,7}$")


def recorded_branch(root: Path) -> str:
    """Branch ``bt wt new`` recorded for this worktree in worktrees.json ("" when unknown or retired)."""
    try:
        with open(config.STATE_DIR / "worktrees.json", encoding="utf-8") as fh:
            rec = json.load(fh).get(config.norm(root))
    except (OSError, ValueError, AttributeError):
        return ""
    branch = rec.get("branch") if isinstance(rec, dict) and not rec.get("retired") else None
    return branch if isinstance(branch, str) else ""


def names_branch(text: str, branch: str) -> bool:
    """Whether ``text`` mentions ``branch`` as a whole name: not as part of a longer branch or path
    (``X--explore``, ``X/sub``, ``X.md``). A sentence-final period does not count as part of the name."""
    return bool(branch) and re.search(r"(?<![\w./-])" + re.escape(branch) + r"(?![\w/-])(?!\.\w)", text) is not None


def branch_exists(root: Path, branch: str) -> bool:
    """Whether ``refs/heads/<branch>`` exists, as a loose ref or in packed-refs. True when that cannot be told."""
    common = paths.git_common_dir(root)
    if common is None:
        return True
    if (common / "refs" / "heads" / branch).is_file():
        return True
    packed = _read(common / "packed-refs")
    if packed is None:
        return False
    return any(line.rstrip().endswith(f" refs/heads/{branch}") for line in packed.splitlines())


def record_baseline(root: Path) -> None:
    """Keep the branch recorded for this worktree in worktrees.json in step with HEAD. Never raises.

    A worktree without a lineage record gets HEAD's branch recorded as its baseline (a ``baseline`` record
    without a repo, so ``bt wt`` ignores it). A recorded branch that is no longer HEAD's is re-recorded
    (``renamed_from`` keeps the old name) when its ref is gone (the lead renamed the branch) or when now.md
    names HEAD's branch (the lead rewrote the brief for it); otherwise it stays, and ``brief_mismatch``
    reports the difference."""
    try:
        head = _head_branch(root)
        if not head:
            return
        try:
            with open(config.STATE_DIR / "worktrees.json", encoding="utf-8") as fh:
                data = json.load(fh)
        except FileNotFoundError:
            data = {}
        if not isinstance(data, dict):
            return                      # unreadable store: leave it for `bt doctor`, never overwrite it
        rec = data.get(config.norm(root))
        if isinstance(rec, dict) and rec.get("retired"):
            return
        old = rec.get("branch") if isinstance(rec, dict) else None
        from . import worktrees
        if not old:
            worktrees.update_lineage(root, branch=head, baseline=True)
        elif old != head and (not branch_exists(root, old) or names_branch(_read(now_path(root)) or "", head)):
            worktrees.update_lineage(root, branch=head, renamed_from=old)
    except Exception:  # noqa: BLE001 - runs inside SessionStart
        return


def brief_mismatch(root: Path, heuristic: bool = False) -> str:
    """One line when .baton/now.md was written for another branch than HEAD's, else "".

    HEAD's branch is compared with the branch recorded for the worktree in worktrees.json (by ``bt wt new``,
    or as the baseline of its first SessionStart, kept in step by ``record_baseline``); a now.md that names
    HEAD's branch (the whole name, see ``names_branch``) is taken as rewritten for it. With ``heuristic`` (``bt doctor``) a worktree whose recorded branch is HEAD's, or that has no
    record, is also checked by the branch names (``feat/...``, ``fix/...``, ``wt/...`` and similar) that
    now.md mentions outside its Purpose/Exit/Base/Parent header; the SessionStart capsule does not use that, since a lead's now.md legitimately names
    its members' branches. Reads three small files, no git.
    """
    head = _head_branch(root)
    if not head:
        return ""
    text = _read(now_path(root)) or ""
    if names_branch(text, head):
        return ""
    described = recorded_branch(root)
    if heuristic and (not described or described == head):
        # The Purpose/Exit/Base/Parent header (written by `bt wt new` and for follower siblings) names the base,
        # parent and merge target, never the branch the brief is about.
        body = "\n".join(line for line in text.splitlines() if not HEADER_LINE.match(line))
        names = (b.rstrip(".") for b in BRANCH_IN_TEXT.findall(body))
        named = [b for b in names if b != head and not FILE_LIKE.search(b)]   # docs/RELEASING.md is a path
        described = named[0] if named else ""
    if not described or described == head:
        return ""
    return (f"Brief mismatch: now.md describes {described}, HEAD is {head}; rewrite now.md for the current branch "
            "or move this work to its own worktree.")


# --------------------------------------------------------------------------- sections


def now_section(root: Path, ws: str, create: bool = True) -> str:
    path = now_path(root)
    text = _read(path)
    if text is None:
        if create:
            try:
                ensure_now(root, ws)
            except OSError:
                pass
        text = TEMPLATE.format(ws=ws)
    if _is_blank_template(text):
        legacy = _legacy_note(root, ws)
        if legacy:
            body = f"{legacy[:LEGACY_CHARS].rstrip()}\n{LEGACY_NOTE}\n{NEW_NOTE}"
        else:
            body = f"{text.rstrip()}\n{NEW_NOTE}"
    else:
        # a note that is over budget is shortened in the middle, never at the end (Next and In flight stay whole)
        body = fit_now(text)
        try:
            repair = repair_block(now_problems(text, exit_required=_exit_required(root)))
        except Exception:  # noqa: BLE001 - the note itself is still worth showing
            repair = ""
        if repair:
            body = f"{repair}\n{body}"
    return f"{NOW_HEADING}\n{body}"


COMPACT_MAX_AGE = 2 * 86400   # a fresh capsule leaves out a compact.md older than this


def compact_is_stale(root: Path) -> bool:
    """compact.md is older than a filled-in now.md (the brief was rewritten since) or than ``COMPACT_MAX_AGE``.

    A blank template now.md does not count: SessionStart creates it just before this check."""
    compact = config.mtime(Path(root) / paths.BATON_DIRNAME / "compact.md")
    if compact is None:
        return False
    if time.time() - compact > COMPACT_MAX_AGE:
        return True
    now_md = config.mtime(now_path(root))
    return now_md is not None and compact < now_md and not _is_blank_template(_read(now_path(root)) or "")


def compact_section(root: Path) -> str:
    """The last compaction's harvest; empty when absent or stale (a compaction that predates the current now.md
    or is over two days old describes work the brief has since moved past)."""
    if compact_is_stale(root):
        return ""
    text = _read(Path(root) / paths.BATON_DIRNAME / "compact.md")
    if not text or not text.strip():
        return ""
    return f"{COMPACT_HEADING}\n{text.strip()}"


def inbox_notes(root: Path) -> list[tuple[str, str]]:
    """[(file name, first line)] of unread follower notes in ``.baton/inbox/*.md``, newest name first."""
    inbox = Path(root) / paths.BATON_DIRNAME / "inbox"
    try:
        names = sorted((p.name for p in inbox.glob("*.md") if p.is_file()), reverse=True)
    except OSError:
        return []
    notes = []
    for name in names:
        first = ""
        try:
            with open(inbox / name, encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if line.strip():
                        first = line.strip().lstrip("﻿")
                        break
        except OSError:
            continue
        notes.append((name, first))
    return notes


def inbox_lines(notes: list[tuple[str, str]], budget: int = INBOX_BUDGET) -> str:
    """``- <name>: <first line>`` per note, newest first, at most ``budget`` chars in total."""
    lines = [f"- {name}: {first}"[:INBOX_LINE_CAP] for name, first in notes]
    if len("\n".join(lines)) <= budget:
        return "\n".join(lines)
    tail_room = 24
    kept: list[str] = []
    used = 0
    for line in lines:
        cost = len(line) + (1 if kept else 0)
        if used + cost > budget - tail_room:
            break
        kept.append(line)
        used += cost
    if not kept and lines:
        kept = [lines[0][: budget - tail_room]]
    return "\n".join(kept + [f"(+{len(lines) - len(kept)} more: `bt inbox`)"])


def migration_section(root: Path, now: float | None = None) -> str:
    """One line while ``.baton/migration-notes.md`` (written by ``bt migrate import``) is less than 30 days old."""
    try:
        path = Path(root) / ".baton" / MIGRATION_NOTES
        st = path.stat()
        if not stat.S_ISREG(st.st_mode) or st.st_size > MIGRATION_MAX_BYTES:         # a directory, a device, a huge file: no line
            return ""
        now = time.time() if now is None else now
        if now - st.st_mtime >= MIGRATION_DAYS * 86400:
            return ""
        with open(path, "rb") as fh:
            head = fh.read(600)                                                         # only what the date needs
        if b"\0" in head:                                                                # binary
            return ""
        text = head.decode("utf-8", errors="replace")
        m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text)
        date = m.group(1) if m else time.strftime("%Y-%m-%d", time.localtime(st.st_mtime))
        return (f"This checkout was moved from Windows to this machine on {date}: "
                f"read .baton/{MIGRATION_NOTES} before running scripts or tests.")
    except Exception:  # noqa: BLE001 - nothing about this optional line may cost the session its capsule
        return ""


def inbox_section(root: Path) -> str:
    notes = inbox_notes(root)
    if not notes:
        return ""
    return f"## Inbox\n{len(notes)} unread follower result(s) {INBOX_HINT}:\n{inbox_lines(notes)}"


def roles_section(vendor: str, account: str | None = None) -> str:
    try:
        # Written by another module; absent or broken -> no Roles section. Codex agent files are per account home.
        routing = importlib.import_module("baton.routing")
        hint = routing.lead_hint(vendor, account) if vendor == "codex" and account and account != "main" else routing.lead_hint(vendor)
    except Exception:
        return ""
    return f"## Roles\n{hint.strip()}" if hint and hint.strip() else ""


def sections(root: Path, vendor: str, source: str, account: str | None = None, *,
             create: bool = True, use_git: bool = True) -> dict[str, str]:
    """The capsule sections that apply to ``source``, keyed by name (see PRIORITY / DISPLAY)."""
    root = Path(root)
    ws = paths.ws_name(root)
    out: dict[str, str] = {}
    from . import compute_pickup
    out['compute'] = compute_pickup.summary(root)
    if source == "resume":
        out["inbox"] = inbox_section(root)
    elif source == "compact":
        if create:
            record_baseline(root)
        out["migration"] = migration_section(root)
        out["mismatch"] = brief_mismatch(root)
        out["now"] = now_section(root, ws, create)
        out["inbox"] = inbox_section(root)
        out["rules"] = f"## Working rules\n{RULES}"
    else:  # startup, clear, fork, anything unknown
        out["header"] = header(root, ws, paths.repo_name(root), use_git)
        out["migration"] = migration_section(root)
        if create:
            record_baseline(root)
        out["mismatch"] = brief_mismatch(root)
        out["now"] = now_section(root, ws, create)
        out["compact"] = compact_section(root)
        out["inbox"] = inbox_section(root)
        out["rules"] = f"## Working rules\n{RULES}"
        out["roles"] = roles_section(vendor, account)
    return {k: v for k, v in out.items() if v}


def assemble(parts: dict[str, str], cap: int) -> str:
    """Join sections in DISPLAY order, truncating lower-priority sections first to stay within ``cap`` chars."""
    keep: dict[str, str] = {}
    used = 0
    for key in PRIORITY:
        text = parts.get(key)
        if not text:
            continue
        cost = len(text) + (1 if keep else 0)
        if used + cost <= cap:
            keep[key] = text
            used += cost
            continue
        room = cap - used - (1 if keep else 0) - len(TRUNCATED)
        if room >= 40:
            if key == "compact":
                # Summaries often put pending work at the end. Keep both the
                # opening objective and final continuation state within the cap.
                head = (room - 1) * 2 // 3
                tail = room - 1 - head
                keep[key] = text[:head].rstrip() + TRUNCATED + "\n" + text[-tail:].lstrip()
            else:
                keep[key] = text[:room].rstrip() + TRUNCATED
        break
    return "\n".join(keep[k] for k in DISPLAY if k in keep)


def build(root: Path, vendor: str, source: str = "startup", account: str | None = None, *,
          warning: str = "", cap: int | None = None) -> str:
    """The additionalContext for a SessionStart of ``source`` (empty string when there is nothing to say).

    ``warning`` (owner conflict) is placed first and is never truncated before the other sections.
    """
    parts = sections(root, vendor, source, account)
    if warning:
        parts["warning"] = warning
    return assemble(parts, cap or CAPS.get(vendor, CAPS["claude"]))


def estimate_tokens(root: Path, vendor: str, account: str | None = None) -> int:
    """Rough token size (4 chars/token) of a startup capsule, without git calls or file creation."""
    text = assemble(sections(root, vendor, "startup", account, create=False, use_git=False), CAPS.get(vendor, 9000))
    return (len(text) + 250) // 4  # + a typical header line


# --------------------------------------------------------------------------- bt now


def cmd_now_check(args) -> int:
    """``bt now check``: lint this worktree's .baton/now.md. Read-only; exit 0 when clean, 1 otherwise."""
    root = paths.find_worktree_root(args.worktree or os.getcwd())
    if root is None:
        print("bt now check: not inside a git worktree", file=sys.stderr)
        return 2
    head_time = None
    try:
        raw = gitutil.out(root, "log", "-1", "--format=%ct", timeout=GIT_TIMEOUT)
        head_time = float(raw) if raw else None
    except (ValueError, OSError):
        pass
    problems = problems_for(root, head_time=head_time)
    text = _read(now_path(root)) or ""
    if args.json:
        print(json.dumps({"worktree": str(root), "path": str(now_path(root)), "lines": len(text.splitlines()),
                          "chars": len(text), "problems": problems}))
    elif problems:
        print(f"{now_path(root)}: {len(problems)} problem(s)")
        for problem in problems:
            print(f"- {problem}")
        print(f"contract: {CONTRACT}")
    else:
        print(f"{now_path(root)}: ok ({len(text.splitlines())} lines, {len(text)} chars)")
    return 1 if problems else 0


def cmd_now(args) -> int:
    if getattr(args, "action", None) == "check":
        return cmd_now_check(args)
    root = paths.find_worktree_root(os.getcwd())
    if root is None:
        print("bt now: not inside a git worktree", file=sys.stderr)
        return 1
    try:
        paths.ensure_excluded(root)
    except OSError:
        pass
    paths.baton_dir(root, create=True)
    ensure_now(root, paths.ws_name(root))
    rotate_log(root)
    print(now_path(root))
    if args.print_capsule:
        from . import registry

        vendor = args.vendor or registry.entry(root).get("last_vendor") or "claude"
        account = config.account_from_env(vendor, os.environ)
        print()
        print(build(root, vendor, "startup", account))
    return 0


def register(subparsers) -> None:
    parser = subparsers.add_parser(
        "now", help="print the path of this worktree's .baton/now.md (created from the template if missing); "
                    "`bt now check` lints it"
    )
    parser.add_argument("action", nargs="?", choices=["check"],
                        help="check: lint .baton/now.md against the contract (exit 1 when it has problems; read-only)")
    parser.add_argument("--worktree", help="check: worktree directory (default: the current one)")
    parser.add_argument("--json", action="store_true", help="check: machine-readable output")
    parser.add_argument("--print", dest="print_capsule", action="store_true",
                        help="also print the capsule a startup SessionStart would inject")
    parser.add_argument("--vendor", choices=config.VENDORS, help="vendor to render for --print (default: last used, else claude)")
    parser.set_defaults(func=cmd_now)
