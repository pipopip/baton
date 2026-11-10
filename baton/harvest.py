"""Mechanical harvest of a compaction, plus transcript-tail readers used by the hooks.

Everything here reads transcripts from the tail only (transcripts reach hundreds
of MB) and never raises on malformed input: callers get ``None`` / ``""``.
Stdlib only; safe under ``python -S``.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

COMPACT_CAP = 5000          # total chars of compact.md
SECTION_CAP = 1200          # chars per kept Claude summary section
CODEX_TEXT_CAP = 400        # chars per kept Codex user request
CODEX_KEEP = 8              # number of Codex user requests kept
REPLACE_TRIES = 5            # os.replace attempts (PermissionError backoff)
CTX_TAIL_BYTES = 512 * 1024  # SessionEnd / resume token lookup window
COMPACT_TAIL_WINDOWS = (1 << 20, 8 << 20)  # compaction lookup: 1 MiB, then 8 MiB

# The nine sections of Claude Code's compaction summary, and the ones we keep.
CLAUDE_TITLES = {
    1: "Primary Request and Intent",
    2: "Key Technical Concepts",
    3: "Files and Code Sections",
    4: "Errors and fixes",
    5: "Problem Solving",
    6: "All user messages",
    7: "Pending Tasks",
    8: "Current Work",
    9: "Optional Next Step",
}
CLAUDE_KEEP = (1, 4, 7, 8, 9)

_SKIP_CODEX_PREFIXES = ("<environment_context>", "<user_instructions>", "# AGENTS.md instructions")


def iso_now(ts: float | None = None) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def parse_iso(text: object) -> float | None:
    """Epoch seconds of a ``YYYY-MM-DDTHH:MM:SS`` UTC stamp (as written by iso_now), else None.

    Hand-rolled (days-from-civil) so the hook does not import calendar/datetime.
    """
    if not isinstance(text, str) or len(text) < 19 or text[4] != "-" or text[10] != "T":
        return None
    try:
        y, mo, d = int(text[0:4]), int(text[5:7]), int(text[8:10])
        hh, mm, ss = int(text[11:13]), int(text[14:16]), int(text[17:19])
    except ValueError:
        return None
    if not (1 <= mo <= 12 and 1 <= d <= 31 and hh < 24 and mm < 60 and ss < 61):
        return None
    y -= mo <= 2
    era = y // 400
    yoe = y - era * 400
    doy = (153 * (mo + (-3 if mo > 2 else 9)) + 2) // 5 + d - 1
    days = era * 146097 + yoe * 365 + yoe // 4 - yoe // 100 + doy - 719468
    return float(days * 86400 + hh * 3600 + mm * 60 + ss)


def atomic_write(path: str | os.PathLike, text: str) -> None:
    """Write ``text`` to ``path`` via a pid-suffixed temp file and ``os.replace``.

    ``os.replace`` is retried on PermissionError (Windows: a reader holds the target open) like
    ``registry._save``; the temp file is removed and the error raised if the last attempt fails.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f"{target.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8", newline="\n")
        for attempt in range(REPLACE_TRIES):
            try:
                os.replace(tmp, target)
                return
            except PermissionError:
                if attempt == REPLACE_TRIES - 1:
                    raise
                time.sleep(0.05 * (attempt + 1))
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def id8(session_id: str | None) -> str:
    return (session_id or "")[:8] or "unknown"


# --------------------------------------------------------------------------- tail reading


def read_tail_lines(path: str | os.PathLike, max_bytes: int) -> list[bytes]:
    """Complete lines from the last ``max_bytes`` of a file (the first partial line is dropped)."""
    try:
        with open(path, "rb") as fh:
            size = fh.seek(0, os.SEEK_END)
            start = max(0, size - max_bytes)
            fh.seek(start)
            data = fh.read()
    except OSError:
        return []
    lines = data.split(b"\n")
    if start > 0 and lines:
        lines = lines[1:]
    return [ln for ln in lines if ln.strip()]


def _loads(line: bytes):
    try:
        return json.loads(line)
    except ValueError:
        return None


# --------------------------------------------------------------------------- context tokens


def ctx_info(vendor: str, transcript: str | os.PathLike | None) -> tuple[int | None, str | None]:
    """(context size of the last model call, model id of the session) from one read of the transcript tail (<= 512 KiB).

    Claude: the size is the last non-sidechain assistant ``message.usage``; the model is that entry's
    ``message.model`` (``<synthetic>`` entries are skipped). Codex: the size is the last ``token_count``
    ``last_token_usage.input_tokens`` after the latest compaction; the model is ``payload.model``
    of the last ``turn_context`` entry. A compaction without later usage leaves the size unknown.
    Either is None when the tail does not hold it.
    """
    if not transcript:
        return None, None
    lines = read_tail_lines(transcript, CTX_TAIL_BYTES)
    tokens: int | None = None
    model: str | None = None
    if vendor == "codex":
        search_tokens = True
        for line in reversed(lines):
            want_tokens = search_tokens and tokens is None and b"token_count" in line
            want_model = model is None and b"turn_context" in line
            want_boundary = search_tokens and tokens is None and b"compacted" in line
            if not (want_tokens or want_model or want_boundary):
                continue
            obj = _loads(line)
            if want_boundary and isinstance(obj, dict) and obj.get("type") == "compacted":
                search_tokens = False
                continue
            payload = obj.get("payload") if isinstance(obj, dict) else None
            if not isinstance(payload, dict):
                continue
            if want_tokens and payload.get("type") == "token_count":
                info = payload.get("info")
                usage = info.get("last_token_usage") if isinstance(info, dict) else None
                value = usage.get("input_tokens") if isinstance(usage, dict) else None
                if isinstance(value, int):
                    tokens = value
            elif want_model and obj.get("type") == "turn_context":
                value = payload.get("model")
                if isinstance(value, str) and value:
                    model = value
            if tokens is not None and model is not None:
                break
        return tokens, model
    for line in reversed(lines):
        if b'"usage"' not in line:
            continue
        obj = _loads(line)
        if not isinstance(obj, dict) or obj.get("type") != "assistant" or obj.get("isSidechain"):
            continue
        message = obj.get("message")
        if not isinstance(message, dict):
            continue
        usage = message.get("usage")
        if not isinstance(usage, dict):
            continue
        total = 0
        for key in ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"):
            value = usage.get(key)
            if isinstance(value, int):
                total += value
        if total > 0:  # synthetic/error messages carry zeros; keep looking
            value = message.get("model")
            return total, value if isinstance(value, str) and value and not value.startswith("<") else None
    return None, None


def ctx_tokens(vendor: str, transcript: str | os.PathLike | None) -> int | None:
    """Context size of the last model call, from the transcript tail (<= 512 KiB)."""
    return ctx_info(vendor, transcript)[0]


def session_model(vendor: str, transcript: str | os.PathLike | None) -> str | None:
    """Model id of the session, from the transcript tail (see ``ctx_info``)."""
    return ctx_info(vendor, transcript)[1]


# --------------------------------------------------------------------------- Claude harvest


def claude_summary_from_transcript(transcript: str | os.PathLike | None) -> str:
    """Text of the last ``isCompactSummary`` line in the transcript tail."""
    if not transcript:
        return ""
    for window in COMPACT_TAIL_WINDOWS:
        for line in reversed(read_tail_lines(transcript, window)):
            if b"isCompactSummary" not in line:
                continue
            obj = _loads(line)
            if not isinstance(obj, dict) or not obj.get("isCompactSummary"):
                continue
            message = obj.get("message")
            content = message.get("content") if isinstance(message, dict) else None
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                parts = []
                for part in content:
                    if isinstance(part, str):
                        parts.append(part)
                    elif isinstance(part, dict) and isinstance(part.get("text"), str):
                        parts.append(part["text"])
                return "\n".join(parts)
        try:
            if os.path.getsize(transcript) <= window:
                break
        except OSError:
            break
    return ""


def _heading(line: str) -> tuple[int, str] | None:
    """(section number, inline remainder) if ``line`` opens one of the nine summary sections."""
    stripped = line.strip().lstrip("#").strip().lstrip("*_").strip()
    match = re.match(r"(\d{1,2})\.\s+(.*)$", stripped)
    if not match:
        return None
    number = int(match.group(1))
    title = CLAUDE_TITLES.get(number)
    rest = match.group(2)
    if title is None or not rest.lower().startswith(title.lower()):
        return None
    return number, rest[len(title):].lstrip(" *_:").rstrip()


SUMMARY_LEAD = "This session is being continued from a previous conversation"
SUMMARY_TRAILER = "If you need specific details from before compaction"


def strip_wrapper(text: str) -> str:
    """Remove Claude Code's framing: the leading "session is being continued" paragraph and everything
    from the "If you need specific details from before compaction" trailer onward.

    The trailer is matched at its last occurrence so a summary that quotes the phrase keeps its later sections.
    """
    cut = text.rfind("\n" + SUMMARY_TRAILER)
    if cut >= 0:
        text = text[:cut]
    elif text.startswith(SUMMARY_TRAILER):
        text = ""
    lead = text.lstrip()
    if lead.startswith(SUMMARY_LEAD):
        text = lead.partition("\n\n")[2]
    return text


def clean_summary(text: str) -> str:
    """The summary body: wrapper stripped, then the text between the first ``<summary>`` and the last
    ``</summary>`` (or, without tags, the text minus its ``<analysis>`` blocks)."""
    text = strip_wrapper(text)
    start = text.find("<summary>")
    end = text.rfind("</summary>")
    if start >= 0 and end > start:
        return text[start + len("<summary>"):end]
    if start >= 0:
        return text[start + len("<summary>"):]
    return re.sub(r"<analysis>.*?</analysis>", "", text, flags=re.S)


def parse_claude_summary(text: str) -> dict[int, str]:
    """Split a Claude compaction summary into {section number: body}."""
    text = clean_summary(text)
    sections: dict[int, list[str]] = {}
    current = None
    for line in text.splitlines():
        found = _heading(line)
        if found and found[0] not in sections and (current is None or found[0] > current):
            current = found[0]
            sections[current] = [found[1]] if found[1] else []
        elif current is not None:
            sections[current].append(line)
    return {n: "\n".join(body).strip() for n, body in sections.items()}


def _clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _fit_sections(items: list[tuple[str, str]], budget: int, cap: int) -> list[tuple[str, str]]:
    """Clip bodies to <= cap chars each, then shrink the cap until the total fits ``budget``."""
    while True:
        clipped = [(title, _clip(body, cap)) for title, body in items]
        total = sum(len(title) + len(body) + 2 for title, body in clipped)
        if total <= budget or cap <= 40:
            return clipped
        cap = max(40, cap - 10)


def render_claude(summary: str, header: str) -> str:
    """compact.md body for a Claude summary: sections 1, 4, 7, 8, 9 (<= 1200 chars each, <= 5000 total)."""
    sections = parse_claude_summary(summary)
    items = []
    for number in CLAUDE_KEEP:
        body = sections.get(number, "")
        if body:
            items.append((f"## {number}. {CLAUDE_TITLES[number]}", body))
    if not items:
        stripped = clean_summary(summary).strip()
        if not stripped:
            return ""
        items = [("## Summary (unparsed)", stripped)]
    budget = COMPACT_CAP - len(header) - 1
    parts = [header]
    for title, body in _fit_sections(items, budget, SECTION_CAP):
        parts.append(f"{title}\n{body}")
    return _final("\n".join(parts))


# --------------------------------------------------------------------------- Codex harvest


def _codex_compaction(transcript: str | os.PathLike | None) -> tuple[bool, str, list[str]]:
    """Latest confirmed compaction, its summary and retained user requests."""
    if not transcript:
        return False, "", []
    for window in COMPACT_TAIL_WINDOWS:
        for line in reversed(read_tail_lines(transcript, window)):
            if b"compacted" not in line:
                continue
            obj = _loads(line)
            if not isinstance(obj, dict) or obj.get("type") != "compacted":
                continue
            payload = obj.get("payload")
            summary = payload.get("message") if isinstance(payload, dict) else None
            summary = summary if isinstance(summary, str) and summary.strip() else ""
            retained = payload.get("retained_context") if isinstance(payload, dict) else None
            messages = retained.get("user_messages") if isinstance(retained, dict) else None
            texts = []
            for item in messages if isinstance(messages, list) else []:
                text = item.get("text") if isinstance(item, dict) else item
                if not isinstance(text, str):
                    continue
                text = " ".join(text.split())
                if text and not text.startswith(_SKIP_CODEX_PREFIXES):
                    texts.append(_clip(text, CODEX_TEXT_CAP))
            return True, summary, texts[-CODEX_KEEP:]
        try:
            if os.path.getsize(transcript) <= window:
                break
        except OSError:
            break
    return False, "", []


def codex_user_requests(transcript: str | os.PathLike | None) -> list[str]:
    """User message texts retained by the last Codex compaction (last 8, each <= 400 chars)."""
    return _codex_compaction(transcript)[2]


def render_codex(texts: list[str], header: str) -> str:
    if not texts:
        return ""
    lines = [header, "## Recent user requests"] + [f"- {t}" for t in texts]
    return _final("\n".join(lines))


# --------------------------------------------------------------------------- entry point


def _final(text: str) -> str:
    return _clip(text, COMPACT_CAP).rstrip() + "\n"


def _codex_summary(summary: str, header: str) -> str:
    """Keep both ends of an oversized summary within the complete file budget."""
    prefix = f"{header}\n## Compaction summary\n"
    budget = COMPACT_CAP - len(prefix) - 1  # final newline
    if len(summary) > budget:
        marker = "\n\n[... middle of compaction summary truncated ...]\n\n"
        available = max(0, budget - len(marker))
        head = (available + 1) // 2
        tail = available // 2
        summary = summary[:head] + marker + (summary[-tail:] if tail else "")
    return prefix + summary.rstrip() + "\n"


def _render(vendor: str, session_id: str | None, transcript: str | os.PathLike | None,
            compact_summary: str | None = None) -> tuple[str, bool]:
    """Return text and whether an empty result confirms a Codex compaction."""
    header = f"<!-- harvested {iso_now()} from {vendor} session {id8(session_id)} -->"
    summary = compact_summary if isinstance(compact_summary, str) and compact_summary.strip() else ""
    if vendor == "codex":
        confirmed = False
        texts = []
        if not summary:
            confirmed, summary, texts = _codex_compaction(transcript)
        if summary:
            return _codex_summary(summary, header), confirmed
        return render_codex(texts, header), confirmed
    if not summary:
        summary = claude_summary_from_transcript(transcript)
    return (render_claude(summary, header) if summary.strip() else ""), False


def render(vendor: str, session_id: str | None, transcript: str | os.PathLike | None,
           compact_summary: str | None = None) -> str:
    """The compact.md text for a finished compaction, or '' when nothing could be harvested."""
    return _render(vendor, session_id, transcript, compact_summary)[0]


def write_compact(root: Path, vendor: str, session_id: str | None, transcript: str | os.PathLike | None,
                  compact_summary: str | None = None) -> Path | None:
    """Write ``<root>/.baton/compact.md`` atomically; returns its path, or None if nothing was harvested."""
    text, confirmed = _render(vendor, session_id, transcript, compact_summary)
    target = Path(root) / ".baton" / "compact.md"
    if not text:
        if confirmed:
            target.unlink(missing_ok=True)
        return None
    atomic_write(target, text)
    return target
