"""Context cycling: replace a long-running Claude Code session by a fresh one when its context is large.

Two hooks (installed by ``bt install``, entry ``cycle_hook.py``) and one command (``bt cycle``):

* ``Stop`` — at the end of every turn (a safe boundary). Above the worktree's threshold
  (``cycle.toml``), phase 1 blocks the stop once with an instruction: DRAIN while background
  subagents or workflows are running (the running ones finish, no new ones), else write the
  handover into ``.baton/now.md`` (the contract in ``capsule.CONTRACT``, with the lint problems of the
  current note). Phase 2, at the next stop with ``now.md`` updated and lint-clean and nothing
  running, starts the detached cycler. Each phase blocks at most once (twice for an ignored or
  still-unclean handover request), so the hook never loops.
  A drain is bounded (``drain_max_minutes``): after that the handover is requested although subagents
  still run, and they end with the session. Stale running entries are dropped (``agent_stale_minutes``).
* ``PreToolUse`` on ``Agent`` and ``Workflow`` — denies new launches while a drain or handover
  is pending for the session.
* The cycler (``bt cycle --run``, detached from the hook so hook timeouts do not touch it):
  stages a waiting Windows Terminal tab, verifies its readiness, and ends the exact old
  ``claude`` process while retaining its ownership until exit is proven. Only then may
  the new tab run ``bt <worktree> -Fresh -Prompt "Context cycle: ..."``. It verifies the
  successor's live claim, name and registration before reporting a completed cycle. One line per cycle goes to
  ``cycle.log``; a lane's cycle is announced to the lead through session-workflow.

Guards: only sessions whose worktree has a ``.baton/owner.json`` naming that session; never a
subagent (``agent_id`` present) or a follower (``BATON_ROLE``); a kill switch file; a dry-run
mode; a minimum interval between cycles of one worktree.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from . import capsule, config, handoff, harvest, hook, launch, locking, paths, procs, registry

CONFIG_NAME = "cycle.toml"
LOCAL_NAME = "cycle.local.toml"
FLAG_NAME = "cycle.json"            # <worktree>/.baton/cycle.json: the per-session phase
OFF_NAME = "cycle.off"              # STATE_DIR/cycle.off: kill switch
LOG_NAME = "cycle.log"              # BATON_HOME/cycle.log
LAST_STOP_NAME = "cycle-last-stop.json"   # STATE_DIR: the last Stop payload seen (diagnostics)
DEFAULT_THRESHOLD = 700_000
DEFAULT_MIN_INTERVAL = 20 * 60
DEFAULT_CONFIRM_SECONDS = 180
DEFAULT_LEAD = "lane_plan"
PREPARE_BLOCKS = 2                  # handover requests before cycling anyway
# A drain waits for background subagents, which can run for hours while the session keeps growing (the window is
# about 1M tokens, a busy lead adds ~90k an hour above the threshold). 30 minutes leaves most of that headroom for the
# handover and the cycle; cycle.toml `drain_max_minutes` changes it.
DEFAULT_DRAIN_MAX_MINUTES = 30
# A running entry whose subagent transcript has not been written for this long counts as ended without a notice
# (cycle.toml `agent_stale_minutes`). Only the transcript's own entries are judged this way, never the Stop payload's.
DEFAULT_AGENT_STALE_MINUTES = 60
POLL_SECONDS = 2.0
WORKFLOW_WAIT = 45                  # extra seconds to wait for the session-workflow registration

AGENT_TOOLS = ("Agent", "Workflow")
_TASK_ID = re.compile(rb"<task-id>([^<]{1,80})</task-id>")
_TOOL_USE_ID = re.compile(rb"<tool-use-id>([^<]{1,80})</tool-use-id>")
_STATUS = re.compile(rb"<status>([^<]{1,40})</status>")
_FINAL = (b"completed", b"failed", b"killed", b"stopped", b"error")
_AGENT_ID = re.compile(r"agentId:\s*([A-Za-z0-9_-]{4,64})")
_ASYNC_LAUNCH = re.compile(r"\s*Async agent launched\b")   # how a launch result begins when the agent runs on
_ASSISTANT = re.compile(rb'"role"\s*:\s*"assistant"')
_LOG_LINE = re.compile(r"^(\S+) cycled (\S+) ")

PROMPT = ("Context cycle: the previous session reached {tokens} tokens and was cycled by the baton cycle hook. "
          "Resume from .baton/now.md and the capsule; run the inbox receive; restart the background watchers "
          "listed in now.md; continue the work.")

DRAIN_REASON = (
    "baton cycle: this session's context is ~{k}k tokens (limit {limit}k) and background subagents are still "
    "running: {running}. DRAIN: launch no new subagents or workflows (the hook denies them), let the running ones "
    "finish and act on their results, then update .baton/now.md as your handover to this contract: {contract}{problems} "
    "At the first stop after now.md is updated and lint-clean with no subagent running, this session is replaced by a "
    "fresh `bt {ws}` session that resumes from now.md and the capsule. The drain ends after {drain} minutes: "
    "subagents still running then end with this session, so list them under In flight."
)
PREPARE_REASON = (
    "baton cycle: this session's context is ~{k}k tokens (limit {limit}k) and {why}. Write your handover NOW: update "
    ".baton/now.md to this contract: {contract}{problems} Then end the turn without starting anything new. At the "
    "next stop this session is replaced by a fresh `bt {ws}` session that resumes from now.md and the capsule.{again}"
)
PREPARE_WHY = "no subagent is running"
PREPARE_WHY_ABANDONED = ("its drain ended after {drain} minutes with {n} subagent(s) still running ({running}); they end "
                         "with this session, so list them under In flight")
PREPARE_AGAIN = " (.baton/now.md has not changed since the previous request; this is the last request before the cycle runs anyway.)"
PREPARE_AGAIN_PROBLEMS = (" (.baton/now.md changed but still has the problems listed; this is the last request before the "
                          "cycle runs anyway.)")
DENY_REASON = (
    "baton cycle: this session is being drained for a context cycle (~{k}k tokens, limit {limit}k). No new "
    "subagents or workflows: let the running ones finish, update .baton/now.md, then end the turn."
)


# ------------------------------------------------------------------------------------ config


def settings() -> dict:
    """cycle.toml overlaid by .state/cycle.local.toml; {} when neither exists."""
    try:
        base = config.load_toml(config.BATON_HOME / CONFIG_NAME)
        local = config.load_toml(config.STATE_DIR / LOCAL_NAME)
    except Exception:  # noqa: BLE001 - a broken toml must not break a hook
        return {}
    return config._merge(base, local)


def _posint(value, default: int) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else default


def ws_settings(ws: str, cfg: dict | None = None) -> dict:
    """``{"threshold": int, "model": str | None, "lead": str | None}`` for a workstream (case-insensitive key match)."""
    cfg = settings() if cfg is None else cfg
    default = _posint(cfg.get("default"), DEFAULT_THRESHOLD)
    table = cfg.get("workstreams") if isinstance(cfg.get("workstreams"), dict) else {}
    entry = None
    for key, value in table.items():
        if str(key).lower() == ws.lower():
            entry = value
            break
    if isinstance(entry, dict):
        model = entry.get("model")
        lead = entry.get("lead")
        return {"threshold": _posint(entry.get("threshold"), default), "model": model if isinstance(model, str) and model else None,
                "lead": lead if isinstance(lead, str) and lead else None}
    return {"threshold": _posint(entry, default), "model": None, "lead": None}


def threshold(ws: str, cfg: dict | None = None) -> int:
    return ws_settings(ws, cfg)["threshold"]


def drain_max_seconds(cfg: dict | None = None) -> float:
    """How long a drain may wait for background subagents (``drain_max_minutes``, default 30)."""
    cfg = settings() if cfg is None else cfg
    return _posint(cfg.get("drain_max_minutes"), DEFAULT_DRAIN_MAX_MINUTES) * 60.0


def agent_stale_seconds(cfg: dict | None = None) -> float:
    """Silence after which a transcript-derived running entry counts as ended (``agent_stale_minutes``, default 60)."""
    cfg = settings() if cfg is None else cfg
    return _posint(cfg.get("agent_stale_minutes"), DEFAULT_AGENT_STALE_MINUTES) * 60.0


def _lead_for(cfg: dict, ws: str) -> str:
    """The workstream notified when ``ws`` cycles: its own ``lead``, else the top-level ``lead``, else DEFAULT_LEAD."""
    return ws_settings(ws, cfg)["lead"] or str(cfg.get("lead") or DEFAULT_LEAD)


def off_path() -> Path:
    return config.STATE_DIR / OFF_NAME


def enabled(cfg: dict | None = None) -> bool:
    cfg = settings() if cfg is None else cfg
    return cfg.get("enabled", True) is not False and not off_path().exists()


def dry_run(cfg: dict | None = None) -> bool:
    cfg = settings() if cfg is None else cfg
    return cfg.get("dry_run") is True


def log_path() -> Path:
    return config.BATON_HOME / LOG_NAME


def log(text: str) -> None:
    """Append one timestamped line to cycle.log. Never raises."""
    try:
        with open(log_path(), "a", encoding="utf-8") as fh:
            fh.write(f"{harvest.iso_now()} {' '.join(str(text).split())}\n")
    except OSError:
        pass


def last_cycle_at(ws: str) -> float | None:
    """Epoch time of the last ``cycled <ws>`` line in cycle.log (the tail only); None when there is none."""
    try:
        lines = harvest.read_tail_lines(log_path(), 64 * 1024)
    except OSError:
        return None
    for raw in reversed(lines):
        text = raw.decode("utf-8", errors="replace")
        match = _LOG_LINE.match(text)
        if match and match.group(2).lower() == ws.lower():
            return harvest.parse_iso(match.group(1))
    return None


# -------------------------------------------------------------------------------------- flag


def flag_path(root: Path) -> Path:
    return paths.baton_dir(root) / FLAG_NAME


def read_flag(root: Path, session_id: str | None) -> dict | None:
    """The flag when it belongs to ``session_id``; None otherwise (another session's flag is stale)."""
    data = config.read_json(flag_path(root))
    if not data or not session_id or data.get("session_id") != session_id:
        return None
    return data


def write_flag(root: Path, data: dict | None) -> None:
    path = flag_path(root)
    if data is None:
        hook._remove(path)
        return
    paths.baton_dir(root, create=True)
    harvest.atomic_write(path, json.dumps(data, indent=1))


# ------------------------------------------------------------------------------- subagents


def running_from_payload(data: dict) -> list[str] | None:
    """Running agents/workflows from a Stop payload's ``background_tasks`` list; None when the field is absent.

    Entries are dicts; an entry counts when its type names an agent or workflow (not a shell command or
    monitor) and its status is not final.
    """
    tasks = data.get("background_tasks")
    if not isinstance(tasks, list):
        return None
    found = []
    for item in tasks:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("type") or item.get("kind") or "").lower()
        if not kind or not any(word in kind for word in ("agent", "workflow", "subagent", "team")):
            continue
        status = str(item.get("status") or "").lower()
        if status in ("completed", "complete", "done", "failed", "error", "killed", "stopped", "cancelled", "canceled"):
            continue
        label = item.get("description") or item.get("summary") or item.get("name") or item.get("id") or kind
        found.append(str(label)[:80])
    return found


def _agent_transcript(transcript: str | os.PathLike, agent: str) -> Path:
    """Claude keeps a background agent's own transcript at ``<session transcript minus .jsonl>/subagents/agent-<id>.jsonl``."""
    path = Path(transcript)
    return path.with_suffix("") / "subagents" / f"agent-{agent}.jsonl"


def running_from_transcript(transcript: str | os.PathLike | None, *, stale_after: float | None = None,
                            dropped: list[str] | None = None, now: float | None = None) -> list[str]:
    """Background Agent/Workflow launches in the main chain whose last activation (launch, or a SendMessage to the
    agent) is later than their last completion (a task-notification for the task or tool-use id, or a TaskStop).

    Only lines that can matter are JSON-parsed. Foreground launches are over by the time Stop fires, so only
    background launches count: ``run_in_background``, or an Agent call whose result begins "Async agent launched"
    (clients whose Agent tool has no such parameter launch every agent that way). Never raises; an unreadable
    transcript yields [].

    An entry can outlive its agent: the completion notice never reached the transcript (the agent was stopped from
    outside, its resume by SendMessage never ran, the session was restarted around it). With ``stale_after`` a
    running entry whose agent transcript exists and has not been written for that many seconds is left out and its
    label appended to ``dropped``. An entry without a known agent id or transcript file is kept.
    """
    if not transcript:
        return []
    launches: dict[str, dict] = {}       # tool_use_id -> {line, name, desc, agent}
    activations: dict[str, int] = {}     # key (tool_use_id or agent id) -> last line
    completions: dict[str, int] = {}     # key -> last line
    try:
        with open(transcript, "rb") as fh:
            for lineno, raw in enumerate(fh):
                # a completion notice (queued command / attachment), never an assistant line that merely quotes the tags
                if b"<task-notification>" in raw and b"<status>" in raw and not _ASSISTANT.search(raw):
                    status = _STATUS.search(raw)
                    if status is not None and status.group(1) in _FINAL:
                        for pattern in (_TASK_ID, _TOOL_USE_ID):
                            match = pattern.search(raw)
                            if match:
                                key = match.group(1).decode("ascii", errors="replace")
                                completions[key] = max(completions.get(key, -1), lineno)
                if not (b'"Agent"' in raw or b'"Workflow"' in raw or b'"SendMessage"' in raw or b'"TaskStop"' in raw
                        or b"agentId" in raw):
                    continue
                obj = harvest._loads(raw)
                if not isinstance(obj, dict) or obj.get("isSidechain"):
                    continue
                message = obj.get("message")
                content = message.get("content") if isinstance(message, dict) else None
                if not isinstance(content, list):
                    continue
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    if obj.get("type") == "assistant" and block.get("type") == "tool_use":
                        name = block.get("name")
                        inp = block.get("input") if isinstance(block.get("input"), dict) else {}
                        if name in AGENT_TOOLS:
                            tid = str(block.get("id") or "")
                            desc = str(inp.get("description") or inp.get("prompt") or "")[:60]
                            # An Agent call without the flag is foreground on some clients and background on
                            # others (no such parameter: every launch is asynchronous); its result says which.
                            flagged = name != "Agent" or str(inp.get("run_in_background", "")).lower() == "true"
                            launches[tid] = {"line": lineno, "name": name, "desc": desc, "agent": None, "bg": flagged}
                            activations[tid] = lineno
                        elif name == "SendMessage":
                            target = str(inp.get("to") or inp.get("recipient") or "")
                            if target:
                                activations[target] = max(activations.get(target, -1), lineno)
                        elif name == "TaskStop":
                            target = str(inp.get("task_id") or inp.get("taskId") or "")
                            if target:
                                completions[target] = max(completions.get(target, -1), lineno)
                    elif obj.get("type") == "user" and block.get("type") == "tool_result" and block.get("tool_use_id") in launches:
                        text = block.get("content")
                        if isinstance(text, list):
                            text = " ".join(str(part.get("text", "")) for part in text if isinstance(part, dict))
                        launch = launches[block["tool_use_id"]]
                        if not launch["bg"]:
                            if not _ASYNC_LAUNCH.match(str(text)):
                                del launches[block["tool_use_id"]]   # a foreground call: over when its result arrived
                                activations.pop(block["tool_use_id"], None)
                                continue
                            launch["bg"] = True
                        match = _AGENT_ID.search(str(text))
                        if match:
                            launch["agent"] = match.group(1)
    except OSError:
        return []
    running = []
    for tid, info in launches.items():
        if not info["bg"]:
            continue   # an unflagged Agent call whose result has not said it runs on: a foreground call
        keys = [tid] + ([info["agent"]] if info["agent"] else [])
        last_on = max(activations.get(k, -1) for k in keys)
        last_off = max((completions.get(k, -1) for k in keys), default=-1)
        if last_off < last_on:
            label = f"{info['name']} {info['desc']}".strip()
            if stale_after is not None and info["agent"]:
                written = config.mtime(_agent_transcript(transcript, info["agent"]))
                if written is not None and (time.time() if now is None else now) - written > stale_after:
                    if dropped is not None:
                        dropped.append(label)
                    continue
            running.append(label)
    return running


def running_subagents(data: dict, transcript: str | os.PathLike | None, *, stale_after: float | None = None,
                      dropped: list[str] | None = None) -> list[str]:
    """Union of what the Stop payload reports and what the transcript shows (either source saying "running" counts).
    ``stale_after``/``dropped`` as for ``running_from_transcript`` (the payload's entries are never judged stale)."""
    seen, out = set(), []
    for label in (running_from_payload(data) or []) + running_from_transcript(transcript, stale_after=stale_after, dropped=dropped):
        if label not in seen:
            seen.add(label)
            out.append(label)
    return out


# ------------------------------------------------------------------------------- decisions

ALLOW, CLEAR, BLOCK_DRAIN, BLOCK_PREPARE, CYCLE = "allow", "clear", "block_drain", "block_prepare", "cycle"
DRAIN_TIMEOUT = "drain_timeout"      # the drain reached its bound: ask for the handover although subagents still run


def decide(state: dict | None, *, session_id: str, over: bool, running: list[str], now_md_mtime: float | None,
           now: float, stop_hook_active: bool = False, now_md_clean: bool = True,
           drain_max: float = DEFAULT_DRAIN_MAX_MINUTES * 60.0) -> tuple[str, dict | None]:
    """The flag state machine: (action, new state). Pure, so it is unit-tested directly.

    Phases: ``requested`` (armed by ``bt cycle``: treated as over the threshold), ``draining`` (blocked once,
    subagents running), ``prepared`` (handover requested ``blocks`` times at ``since``), ``cycling`` (the cycler
    is running; allow everything). A flag of another session is ignored.

    A drain is bounded: once ``drain_max`` seconds have passed since it began, the next stop gets ``DRAIN_TIMEOUT``
    (the handover request, with the flag set to ``prepared`` and ``drain_abandoned``) whatever is still running, and
    a flag with ``drain_abandoned`` ignores running subagents from then on, so the drain cannot start again.
    ``prepared`` means now.md changed since the request and (``now_md_clean``) passes the lint; after
    ``PREPARE_BLOCKS`` requests the cycle runs anyway, so nothing can wait forever on the session.
    """
    if state is not None and state.get("session_id") != session_id:
        state = None
    phase = state.get("phase") if state else None
    if phase == "cycling":
        return ALLOW, state
    # A manual request remains armed across drain and prepare even when the
    # session is below the automatic context threshold.
    armed = phase == "requested" or bool(state and state.get("by") == "bt cycle")
    if not over and not armed:
        # below the limit (or back below it after a compaction): nothing pending
        return (CLEAR if state else ALLOW), None
    base = {"session_id": session_id}
    if armed:
        base["by"] = "bt cycle"
    # The bound runs from the first drain of this cycle: a subagent woken again after the handover request (a
    # SendMessage is not a launch, so the guard lets it through) re-enters the drain on the old clock, not a new one.
    first = state.get("drain_started") if state else None
    first = None if isinstance(first, bool) or not isinstance(first, (int, float)) else first
    if first is not None:
        base["drain_started"] = first
    if running and not (state and state.get("drain_abandoned")):
        if phase != "draining":
            if first is None:
                return BLOCK_DRAIN, dict(base, phase="draining", since=now, drain_started=now, blocks=1, running=running[:8])
            since = first
        else:
            since = state.get("since")
            since = 0.0 if isinstance(since, bool) or not isinstance(since, (int, float)) else since   # no usable start: expired
        if now - since < drain_max:
            return (ALLOW, state) if phase == "draining" else                 (BLOCK_DRAIN, dict(base, phase="draining", since=since, blocks=1, running=running[:8]))
        return DRAIN_TIMEOUT, dict(base, phase="prepared", since=now, blocks=1, drain_abandoned=True,
                                   abandoned_running=running[:8], drain_started=since)
    if phase != "prepared":
        return BLOCK_PREPARE, dict(base, phase="prepared", since=now, blocks=1)
    since = state.get("since") if isinstance(state.get("since"), (int, float)) else now
    blocks = state.get("blocks") if isinstance(state.get("blocks"), int) else 1
    changed = now_md_mtime is not None and now_md_mtime > since
    if changed and now_md_clean:
        return CYCLE, dict(state, phase="cycling", cycle_at=now)
    if blocks < PREPARE_BLOCKS:
        return BLOCK_PREPARE, dict(state, blocks=blocks + 1)
    return CYCLE, dict(state, phase="cycling", cycle_at=now, **({"now_md_problems": True} if changed else {"now_md_unchanged": True}))


# ------------------------------------------------------------------------------------ guard


def session_root(data: dict, environ=None) -> tuple[Path | None, str]:
    """(worktree, reason) for a hook payload: the worktree when this session owns it, else (None, why not)."""
    environ = os.environ if environ is None else environ
    if environ.get(config.FOLLOWER_ENV):
        return None, "follower"
    if data.get("agent_id"):
        return None, "subagent"
    session_id = data.get("session_id") or None
    transcript = data.get("transcript_path") or None
    if not session_id:
        return None, "no session id"
    cwd = data.get("cwd") if isinstance(data.get("cwd"), str) and data.get("cwd") else os.getcwd()
    root, _ = hook.resolve_root("claude", cwd, session_id, transcript)
    if root is None:
        return None, "not in a worktree"
    owner = config.read_json(paths.baton_dir(root) / "owner.json")
    if not owner or not owner.get("session_id"):
        return None, "no baton owner"
    if owner.get("session_id") != session_id:
        return None, "owned by another session"
    return root, ""


# ----------------------------------------------------------------------------------- hooks


def _problem_text(problems: list[str]) -> str:
    """The current lint problems as a sentence for the prompts ("" when clean)."""
    return " Problems in the current now.md: " + "; ".join(problems[:6]) + "." if problems else ""


def _k(tokens: int | None) -> str:
    return "?" if tokens is None else str(round(tokens / 1000))


def on_stop(data: dict, environ=None) -> dict | None:
    """The Stop hook: None (allow), a block decision, or a systemMessage when the cycler was started."""
    environ = os.environ if environ is None else environ
    _record_payload(data, environ)
    root, _why = session_root(data, environ)
    if root is None:
        return None
    cfg = settings()
    if not enabled(cfg):
        return None
    session_id = str(data.get("session_id"))
    transcript = data.get("transcript_path") or None
    ws = paths.ws_name(root)
    limit = threshold(ws, cfg)
    tokens, _model = harvest.ctx_info("claude", transcript)
    state = read_flag(root, session_id)
    over = tokens is not None and tokens >= limit
    if not over and state is None:
        return None
    now = time.time()
    # Enforce the cooldown before asking the session to drain or hand over.
    # Never clear a cycling flag: its detached worker may still be running.
    if not state or state.get("phase") != "cycling":
        last = last_cycle_at(ws)
        interval = _posint(cfg.get("min_interval_minutes"), DEFAULT_MIN_INTERVAL // 60) * 60
        if last is not None and now - last < interval:
            log(f"skip {ws} session {harvest.id8(session_id)}: cycled {round((now - last) / 60)} min ago (min_interval {interval // 60})")
            if state and not dry_run(cfg):
                write_flag(root, None)
            return None
    dropped: list[str] = []
    stale_after = agent_stale_seconds(cfg)
    running = running_subagents(data, transcript, stale_after=stale_after, dropped=dropped)
    if dropped and not dry_run(cfg):
        log(f"stale {ws} session {harvest.id8(session_id)}: dropped {len(dropped)} running entr{'y' if len(dropped) == 1 else 'ies'} "
            f"whose agent transcript was silent for over {round(stale_after / 60)} min: {dropped[:4]}")
    now_md = config.mtime(paths.baton_dir(root) / "now.md")
    problems = capsule.problems_for(root)          # [] when the lint itself fails: it must never hold a cycle back
    action, new_state = decide(state, session_id=session_id, over=over, running=running, now_md_mtime=now_md, now=now,
                               stop_hook_active=bool(data.get("stop_hook_active")), now_md_clean=not problems,
                               drain_max=drain_max_seconds(cfg))
    if dry_run(cfg):
        log(f"dry-run {ws} session {harvest.id8(session_id)} tokens={tokens} limit={limit} running={len(running)} "
            f"phase={state.get('phase') if state else None} -> {action}")
        return None
    if action == CLEAR:
        write_flag(root, None)
        return None
    if action == ALLOW:
        return None
    k, lim = _k(tokens), round(limit / 1000)
    listed = _problem_text(problems)
    drain_min = round(drain_max_seconds(cfg) / 60)
    if action == BLOCK_DRAIN:
        write_flag(root, new_state)
        log(f"drain {ws} session {harvest.id8(session_id)} tokens={tokens} running={running[:4]}")
        return {"decision": "block", "reason": DRAIN_REASON.format(k=k, limit=lim, ws=ws, running="; ".join(running[:6]),
                                                                     contract=capsule.CONTRACT, problems=listed, drain=drain_min)}
    if action in (BLOCK_PREPARE, DRAIN_TIMEOUT):
        write_flag(root, new_state)
        blocks = (new_state or {}).get("blocks", 1)
        why = PREPARE_WHY
        if action == DRAIN_TIMEOUT:
            waited = round((now - (new_state or {}).get("drain_started", now)) / 60)
            log(f"drain-abandoned {ws} session {harvest.id8(session_id)} tokens={tokens} after {waited} min "
                f"(drain_max_minutes {drain_min}); {len(running)} still running, they end with the cycle: {running[:4]}")
            why = PREPARE_WHY_ABANDONED.format(drain=drain_min, n=len(running), running="; ".join(running[:4]))
        else:
            log(f"prepare {ws} session {harvest.id8(session_id)} tokens={tokens} request={blocks}")
        again = ""
        if blocks > 1:
            changed = now_md is not None and now_md > ((new_state or {}).get("since") or 0)
            again = PREPARE_AGAIN_PROBLEMS if changed else PREPARE_AGAIN
        return {"decision": "block", "reason": PREPARE_REASON.format(k=k, limit=lim, ws=ws, why=why, contract=capsule.CONTRACT,
                                                                       problems=listed, again=again)}
    # CYCLE: identify the session's own process before handing over to the detached cycler
    pid = pid_for_session(session_id, root)
    env_pid = environ.get("CLAUDE_PID")
    if pid is None:
        log(f"abort {ws} session {harvest.id8(session_id)}: no running claude process found for the session; not cycling")
        write_flag(root, None)
        return None
    if env_pid and env_pid.isdigit() and int(env_pid) != pid:
        log(f"abort {ws} session {harvest.id8(session_id)}: sessions file pid {pid} != CLAUDE_PID {env_pid}; not cycling")
        write_flag(root, None)
        return None
    write_flag(root, new_state)
    reason = "stop hook" + (" (now.md unchanged)" if (new_state or {}).get("now_md_unchanged")
                            else " (now.md not lint-clean)" if (new_state or {}).get("now_md_problems") else "")
    ok, started = spawn_cycler(root, session_id, pid, tokens or 0, transcript, reason)
    if not ok:
        write_flag(root, None)
        log(f"fail {ws} session {harvest.id8(session_id)}: cycler {started}")
        return {"systemMessage": f"baton cycle: could not start the cycler ({started}); this session remains active."}
    log(f"start {ws} session {harvest.id8(session_id)} pid={pid} tokens={tokens}: cycler {started}")
    return {"systemMessage": f"baton cycle: context ~{k}k tokens (limit {lim}k); a fresh `bt {ws}` session is starting in a "
                             "new tab; this one will be closed after a replacement claims the worktree."}


def on_pre_tool_use(data: dict, environ=None) -> dict | None:
    """Deny Agent/Workflow launches while this session's flag says draining or prepared."""
    if data.get("tool_name") not in AGENT_TOOLS:
        return None
    session_id = data.get("session_id") or None
    if not session_id:
        return None
    cwd = data.get("cwd") if isinstance(data.get("cwd"), str) and data.get("cwd") else os.getcwd()
    root, _ = hook.resolve_root("claude", cwd, session_id, data.get("transcript_path") or None)
    if root is None:
        return None
    state = read_flag(root, str(session_id))
    if not state or state.get("phase") not in ("draining", "prepared", "cycling"):
        return None
    if not enabled():
        return None
    tokens, _ = harvest.ctx_info("claude", data.get("transcript_path") or None)
    limit = threshold(paths.ws_name(root))
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny",
                                   "permissionDecisionReason": DENY_REASON.format(k=_k(tokens), limit=round(limit / 1000))}}


def _record_payload(data: dict, environ) -> None:
    """Keep the last Stop payload (without the assistant text) for diagnosis. Never raises."""
    try:
        slim = {k: v for k, v in data.items() if k != "last_assistant_message"}
        slim["_env_claude_pid"] = environ.get("CLAUDE_PID")
        harvest.atomic_write(config.STATE_DIR / LAST_STOP_NAME, json.dumps(slim, indent=1, default=str))
    except Exception:  # noqa: BLE001
        pass


def handle(kind: str, data: dict) -> dict | None:
    event = str(data.get("hook_event_name") or "")
    if kind == "stop" and event == "Stop":
        return on_stop(data)
    if kind == "pretool" and event == "PreToolUse":
        return on_pre_tool_use(data)
    return None


def main(kind: str) -> int:
    """Entry used by cycle_hook.py: read one payload, print one JSON object or nothing, exit 0."""
    try:
        raw = sys.stdin.buffer.read()
        data = json.loads(raw.decode("utf-8-sig", errors="replace"))
        if not isinstance(data, dict):
            return 0
        out = handle(kind, data)
        if out is not None:
            sys.stdout.write(json.dumps(out))
            sys.stdout.flush()
    except Exception as exc:  # noqa: BLE001 - a hook never fails the session
        hook._log("claude", f"cycle:{kind}", exc)
    return 0


# ---------------------------------------------------------------------------------- cycler


def pid_for_session(session_id: str, root: Path | None = None) -> int | None:
    """Pid of the running Claude process whose ``sessions/<pid>.json`` names ``session_id`` (and, with ``root``,
    was launched in that worktree or attached to it). None when there is none."""
    root_key = config.norm(root) if root is not None else None
    attached = root_key is not None and hook._attached_to(session_id, root_key)
    for account in config.account_names("claude"):
        sessions = config.account_dir("claude", account) / "sessions"
        try:
            files = list(sessions.glob("*.json"))
        except OSError:
            continue
        for f in files:
            rec = config.read_json(f)
            if rec.get("sessionId") != session_id:
                continue
            if root_key is not None and not (attached or hook._session_in_root(rec, root_key)):
                continue
            try:
                pid = int(rec.get("pid") or f.stem)
            except (TypeError, ValueError):
                continue
            if hook._pid_alive(pid):
                if os.name == "nt":
                    try:
                        expected_start = int(rec["procStart"])
                    except (KeyError, TypeError, ValueError):
                        continue
                    if procs.process_start_time(pid) != expected_start:
                        continue
                return pid
    return None


def session_record(session_id: str) -> dict:
    """Claude's ``sessions/<pid>.json`` record naming ``session_id`` ({} when none)."""
    for account in config.account_names("claude"):
        try:
            files = list((config.account_dir("claude", account) / "sessions").glob("*.json"))
        except OSError:
            continue
        for f in files:
            rec = config.read_json(f)
            if rec.get("sessionId") == session_id:
                return rec
    return {}


def spawn_cycler(root: Path, session_id: str, pid: int, tokens: int, transcript: str | None, reason: str) -> tuple[bool, str]:
    """Start ``bt cycle --run`` detached; return success and its log path or error."""
    try:
        runs = paths.baton_dir(root, create=True) / "runs"
        runs.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        out = runs / f"cycle-{stamp}.log"
        argv = [config.PYTHON, str(config.BATON_HOME / "bt.py"), "cycle", "--run", "--root", str(root), "--old-session", session_id,
                "--old-pid", str(pid), "--tokens", str(tokens), "--reason", reason]
        if transcript:
            argv += ["--transcript", transcript]
        with open(out, "w", encoding="utf-8") as fh:
            procs.popen_detached(argv, cwd=str(root), env=config.clean_env(), stdin=subprocess.DEVNULL, stdout=fh,
                                 stderr=subprocess.STDOUT, close_fds=True)
        return True, f"log {out}"
    except OSError as exc:
        return False, str(exc)


_MODEL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:\[\]-]{0,99}")


def successor_model(ws: str, transcript: str | os.PathLike | None, cfg: dict | None = None) -> tuple[str | None, str]:
    """(model, where it came from) for the session that replaces a cycled one.

    The replacement continues the same work, so it runs on the model the old session was on when it stopped: the
    model of its last own model call in the transcript (which follows a `/model` change made during the session),
    not the routing lead model a new workstream starts on. A ``model`` set for the workstream in cycle.toml is an
    explicit instruction and wins. With neither (no transcript, no model call in its tail, an id that is not one)
    the launcher's default applies, as before."""
    configured = ws_settings(ws, cfg)["model"]
    if configured:
        return configured, "cycle.toml"
    try:
        _tokens, model = harvest.ctx_info("claude", transcript)
    except Exception:
        model = None
    if isinstance(model, str) and _MODEL_ID.fullmatch(model):
        return model, "the previous session"
    return None, "the routing lead model"


def _lit(text: str) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def tab_command(root: Path, vendor: str, account: str | None, prompt: str, model: str | None = None,
                handoff_token: str | None = None) -> str:
    """PowerShell text the new tab runs: the same `bt <worktree>` start path, fresh, with the continue prompt."""
    cmd = f". {_lit(config.INSTALL_HOME / 'bt.ps1')}; bt {_lit(str(root))} -Vendor {_lit(vendor)}"
    if account:
        cmd += f" -Account {_lit(account)}"
    cmd += f" -Fresh -Prompt {_lit(prompt)}"
    if model:
        cmd += f" -Model {_lit(model)}"
    if handoff_token:
        gate = (f"& {_lit(config.PYTHON)} -S {_lit(config.INSTALL_HOME / 'bt.py')} cycle --wait-handoff "
                f"{_lit(handoff_token)} --root {_lit(str(root))}")
        cmd = f"{gate}; if ($LASTEXITCODE -ne 0) {{ exit $LASTEXITCODE }}; {cmd}"
    return cmd


def wt_argv(root: Path, title: str, command: str, wt: str = "wt.exe") -> list[str]:
    encoded = base64.b64encode(launch.managed_tab_command(command).encode("utf-16-le")).decode("ascii")
    shell = "pwsh" if shutil.which("pwsh") else "powershell"
    return [wt, "-w", "0", "nt", "-d", str(root), "--title", title, "--suppressApplicationTitle",
            shell, "-EncodedCommand", encoded]


def workflow_registered(ws: str, thread: str) -> bool | None:
    """Whether session-workflow holds an unreleased registration of ``ws`` by ``thread``; None when unreadable."""
    db = hook.SESSION_WORKFLOW / ".state" / "workflow.sqlite3"
    if not db.exists():
        return None
    try:
        con = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True, timeout=5)
        try:
            row = con.execute("SELECT thread_id, released_at FROM registrations WHERE workstream = ?", (ws,)).fetchone()
        finally:
            con.close()
    except sqlite3.Error:
        return None
    return bool(row and row[0] == thread and row[1] is None)


def _workflow(*args: str) -> tuple[int, str]:
    """Run one session-workflow command; (returncode, output). Missing helper -> (-1, reason)."""
    script = hook.SESSION_WORKFLOW / "workflow.py"
    if not script.exists():
        return -1, "session-workflow not installed"
    argv = [config.PYTHON, "-S", str(script), "--state", str(hook.SESSION_WORKFLOW / ".state" / "workflow.sqlite3"), *args]
    try:
        cp = subprocess.run(argv, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30,
                            stdin=subprocess.DEVNULL, env=config.clean_env(), creationflags=config.CREATE_NO_WINDOW)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return -1, str(exc)
    return cp.returncode, (cp.stdout + cp.stderr).strip()


def end_process(pid: int, expected_start: int | None) -> bool:
    """End the old session's own process (its pid was verified against its session record twice).
    Returns whether it is gone."""
    from . import procs as _procs
    return _procs.end_process(pid, wait=5.0, expected_start=expected_start)


def _restore_old_claim(root: Path, owner: dict, pid: int) -> bool:
    """Restore only the same still-running owner when no successor has claimed the worktree."""
    sid = owner.get("session_id")
    if not isinstance(sid, str) or not sid:
        return False
    owner_path = paths.baton_dir(root) / "owner.json"
    try:
        with locking.exclusive(paths.baton_dir(root) / "owner.lock"):
            if config.read_json(owner_path) or pid_for_session(sid, root) != pid:
                return False
            if workflow_registered(paths.ws_name(root), sid) is not True:
                return False
            hook._write_json(owner_path, dict(owner, claimed_at=harvest.iso_now()))
            return True
    except (OSError, TimeoutError):
        return False


def terminal_retirement(root: Path, owner: dict, phase: str) -> None:
    """Only a bt invocation's own shell may retire, and only on an explicit marker."""
    token = owner.get("terminal_token")
    if isinstance(token, str) and re.fullmatch(r"[0-9a-f]{32}", token):
        terminal_root = Path(owner.get("terminal_root") or root)
        path = paths.baton_dir(terminal_root) / "terminal-retire" / (token + ".json")
        if phase == "canceled":
            hook._remove(path)
        else:
            harvest.atomic_write(path, json.dumps({"token": token, "phase": phase}))


def run_cycle(root: Path, old_session: str, old_pid: int | None, tokens: int, transcript: str | None, reason: str,
              *, confirm_seconds: int | None = None, dry: bool = False, echo=print) -> int:
    """The cycle itself (steps a-f of the design). Returns 0 when the old process was replaced."""
    cfg = settings()
    ws = paths.ws_name(root)
    confirm_seconds = _posint(cfg.get("confirm_seconds"), DEFAULT_CONFIRM_SECONDS) if confirm_seconds is None else confirm_seconds
    owner = config.read_json(paths.baton_dir(root) / "owner.json")
    if owner.get("session_id") != old_session:
        echo(f"[cycle] {ws} is no longer owned by the session being cycled; nothing changed")
        return 2
    vendor = str(owner.get("vendor") or "claude")
    account = owner.get("account") if isinstance(owner.get("account"), str) else None
    if vendor != "claude":
        echo("[cycle] automatic process replacement is supported for Claude only; use bt -Fresh for Codex")
        return 2
    transcript = transcript or (owner.get("transcript") if isinstance(owner.get("transcript"), str) else None)
    pid = pid_for_session(old_session, root)
    if pid is None:
        echo(f"[cycle] no running claude process for session {harvest.id8(old_session)} in {root}; nothing to cycle")
        log(f"abort {ws} session {harvest.id8(old_session)}: process not found ({reason})")
        write_flag(root, None)
        return 2
    if old_pid is not None and old_pid != pid:
        echo(f"[cycle] pid mismatch: sessions file says {pid}, caller said {old_pid}; not cycling")
        log(f"abort {ws} session {harvest.id8(old_session)}: pid mismatch {pid} vs {old_pid}")
        write_flag(root, None)
        return 2
    model, model_from = successor_model(ws, transcript, cfg)
    command = tab_command(root, vendor, account, PROMPT.format(tokens=tokens), model)
    argv = wt_argv(root, ws, command, shutil.which("wt.exe") or "wt.exe")
    echo(f"[cycle] {ws}: old session {harvest.id8(old_session)} pid {pid} ({tokens} tokens); new tab: {command}")
    log(f"model {ws} session {harvest.id8(old_session)}: successor on {model or 'the launcher default'} (from {model_from})")
    if dry:
        echo("[cycle] dry run: nothing released, launched or ended")
        return 0

    if workflow_registered(ws, old_session) is not True:
        write_flag(root, None)
        echo("[cycle] old session registration is unavailable or no longer current; nothing changed")
        return 1

    # Stage a terminal before ending the old process. It runs only a waiting
    # Python gate, never a replacement CLI or model, until the predecessor is gone.
    old_record = session_record(old_session)
    try:
        if int(old_record.get("pid", 0)) != pid:
            raise ValueError("session pid mismatch")
        expected_start = int(old_record["procStart"])
        token = handoff.prepare(root, old_session, pid, expected_start, 2 * confirm_seconds + 15)
    except (KeyError, TypeError, ValueError, OSError) as exc:
        write_flag(root, None)
        echo(f"[cycle] cannot establish staged handoff: {exc}; old session kept")
        return 1
    command = tab_command(root, vendor, account, PROMPT.format(tokens=tokens), model, token)
    argv = wt_argv(root, ws, command, shutil.which("wt.exe") or "wt.exe")
    try:
        subprocess.Popen(argv, env=config.clean_env(), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, close_fds=True)
    except OSError as exc:
        handoff.cancel(root, token)
        write_flag(root, None)
        echo(f"[cycle] wt.exe failed: {exc}; old session and ownership kept")
        return 1
    staged_at = time.time()          # the waiting tab was requested; every later timestamp is measured from here
    deadline = time.monotonic() + confirm_seconds
    while time.monotonic() < deadline and not handoff.ready(root, token):
        time.sleep(POLL_SECONDS)
    current_owner = config.read_json(paths.baton_dir(root) / "owner.json")
    if (not handoff.ready(root, token) or current_owner.get("session_id") != old_session
            or current_owner.get("vendor") != vendor or current_owner.get("account") != account
            or pid_for_session(old_session, root) != pid):
        handoff.cancel(root, token)
        write_flag(root, None)
        echo("[cycle] replacement terminal not ready or ownership changed; old session kept")
        log(f"fail {ws} session {harvest.id8(old_session)}: staged terminal not ready; old kept"
            + _timing(staged_at, failed=time.time()))
        return 1
    ready_at = time.time()           # the staged terminal answered: the old process may now be ended
    terminal_retirement(root, owner, "waiting")
    log(f"ending {ws} old={old_session} old_pid={pid} reason={reason}" + _timing(staged_at, ready=ready_at))
    if not end_process(pid, expected_start):
        terminal_retirement(root, owner, "canceled")
        handoff.cancel(root, token)
        write_flag(root, None)
        echo("[cycle] could not prove old process exited; replacement CLI was not started")
        return 1
    ended_at = time.time()           # the predecessor is proven gone; the successor is authorized below
    terminal_retirement(root, owner, "retired")
    hook.release_owner(root, old_session, transcript, vendor=vendor, account=account)
    write_flag(root, None)
    if config.read_json(paths.baton_dir(root) / "owner.json") or not handoff.authorize(root, token):
        handoff.cancel(root, token)
        echo("[cycle] predecessor ended but takeover could not be authorized; recover with bt -Fresh")
        return 1
    # Once granted, the successor is the sole possible writer. A startup failure
    # cannot resurrect the predecessor; report recovery instead of a false success.
    deadline = time.monotonic() + confirm_seconds
    new_session, new_pid, name, registered = None, None, None, False
    while time.monotonic() < deadline:
        time.sleep(POLL_SECONDS)
        candidate_owner = config.read_json(paths.baton_dir(root) / "owner.json")
        sid = candidate_owner.get("session_id")
        if (isinstance(sid, str) and sid and sid != old_session and candidate_owner.get("vendor") == vendor
                and candidate_owner.get("account") == account):
            candidate = pid_for_session(sid, root)
            name = session_record(sid).get("name")
            registered = workflow_registered(ws, sid)
            if candidate is not None and candidate != pid and name == ws and registered is True:
                new_session, new_pid = sid, candidate
                break
    if new_session is None:
        handoff.cancel(root, token)
        echo("[cycle] predecessor ended; replacement startup/name/registration unconfirmed; inspect the new tab or run bt -Fresh")
        log(f"fail {ws} old={old_session}: takeover unconfirmed; name={name!r} registered={registered}"
            + _timing(staged_at, ready=ready_at, ended=ended_at, failed=time.time()))
        return 1
    log(f"cycled {ws} old={old_session} new={new_session} old_pid={pid} new_pid={new_pid} tokens={tokens} "
        f"name={name} workflow_registered={registered} reason={reason}"
        + _timing(staged_at, ready=ready_at, ended=ended_at, confirmed=time.time()))
    # (f) tell the lead when a lane cycles
    lead = _lead_for(cfg, ws)
    if ws.lower() != lead.lower():
        summary = (f"{ws} cycled by the baton cycle hook at {tokens} tokens: session {harvest.id8(old_session)} -> "
                   f"{harvest.id8(new_session)} (same name, resumes from now.md).")
        rc, out = _workflow("notify", "--sender", ws, "--recipient", lead, "--commit", _head(root), "--node", "baton_cycle",
                            "--kind", "info", "--update-key", f"cycle-{new_session[:8]}", "--summary", summary)
        echo(f"[cycle] notify {lead}: rc={rc} {out[:200]}")
    return 0


def _timing(staged: float, *, ready: float | None = None, ended: float | None = None, confirmed: float | None = None,
            failed: float | None = None) -> str:
    """`` staged=<epoch> [ready=..] [ended=..] [confirmed=..|failed=..] elapsed=<s>`` for a transition's cycle.log lines.

    All values are epoch seconds; ``elapsed`` is the difference of the printed whole-second stamps (``staged`` to the last one), so the time a successor
    needs (``confirmed - ended`` is the part after the predecessor exited) can be read off the log."""
    parts = [f"staged={int(staged)}"]
    last = staged
    for name, value in (("ready", ready), ("ended", ended), ("confirmed", confirmed), ("failed", failed)):
        if value is not None:
            parts.append(f"{name}={int(value)}")
            last = value
    parts.append(f"elapsed={int(last) - int(staged)}s")
    return " " + " ".join(parts)


def _head(root: Path) -> str:
    try:
        from . import gitutil
        sha, _ = gitutil.head(root)
        return sha or "0" * 40
    except Exception:  # noqa: BLE001
        return "0" * 40


# ------------------------------------------------------------------------------------- CLI


def _resolve(query: str | None) -> Path:
    from . import launch
    return launch.resolve_worktree(query)


def _status_rows() -> list[dict]:
    rows = []
    for key, item in registry._load_quiet()["worktrees"].items():
        root = Path(item.get("path") or key)
        if not root.is_dir():
            continue
        owner = config.read_json(paths.baton_dir(root) / "owner.json")
        sid = owner.get("session_id")
        if not isinstance(sid, str) or not sid or owner.get("vendor") != "claude":
            continue
        ws = paths.ws_name(root)
        transcript = owner.get("transcript")
        tokens, _ = harvest.ctx_info("claude", transcript)
        flag = read_flag(root, sid)
        rows.append({"ws": ws, "root": root, "session": sid, "pid": pid_for_session(sid, root), "tokens": tokens,
                     "threshold": threshold(ws), "running": running_from_transcript(transcript),
                     "phase": flag.get("phase") if flag else "-", "last": last_cycle_at(ws)})
    return rows


def _cmd(args) -> int:
    if getattr(args, "wait_handoff", None):
        return 0 if handoff.wait_ready(Path(args.root), args.wait_handoff) else 1
    if args.run:
        root = Path(args.root) if args.root else _resolve(None)
        return run_cycle(root, args.old_session, int(args.old_pid) if args.old_pid else None, int(args.tokens or 0),
                         args.transcript, args.reason or "manual", dry=args.dry_run)
    if args.off or args.on:
        if args.off:
            off_path().parent.mkdir(parents=True, exist_ok=True)
            off_path().write_text(harvest.iso_now() + "\n", encoding="utf-8")
            print(f"[cycle] disabled: {off_path()} exists (bt cycle --on re-enables)")
        else:
            hook._remove(off_path())
            print(f"[cycle] enabled (config enabled = {settings().get('enabled', True)}, dry_run = {dry_run()})")
        return 0
    if args.status or not (args.ws or args.now):
        rows = _status_rows()
        print(f"cycle: enabled={enabled()} dry_run={dry_run()} default threshold {threshold('-')} tokens; log {log_path()}")
        for r in rows:
            last = "-" if r["last"] is None else time.strftime("%m-%d %H:%M", time.localtime(r["last"]))
            flag = "!" if r["tokens"] is not None and r["tokens"] >= r["threshold"] else " "
            print(f"  {r['ws']:16} {harvest.id8(r['session']):9} pid {str(r['pid'] or '-'):6} ctx {_k(r['tokens']):>4}k{flag} "
                  f"limit {round(r['threshold'] / 1000)}k  subagents {len(r['running'])}  phase {r['phase']:9} last cycle {last}")
            for label in r["running"][:6]:
                print(f"      running: {label}")
        return 0
    try:
        root = _resolve(args.ws)
    except LookupError as exc:
        print(f"bt cycle: {exc}", file=sys.stderr)
        return 2
    ws = paths.ws_name(root)
    owner = config.read_json(paths.baton_dir(root) / "owner.json")
    sid = owner.get("session_id")
    if not isinstance(sid, str) or not sid:
        print(f"bt cycle: {ws} has no owning session (.baton/owner.json)")
        return 2
    transcript = owner.get("transcript") if isinstance(owner.get("transcript"), str) else None
    tokens, _ = harvest.ctx_info("claude", transcript)
    running = running_from_transcript(transcript)
    pid = pid_for_session(sid, root)
    print(f"{ws}: session {harvest.id8(sid)} pid {pid} ctx ~{_k(tokens)}k (limit {round(threshold(ws) / 1000)}k), "
          f"{len(running)} background subagent(s) running" + (": " + "; ".join(running[:6]) if running else ""))
    if not args.now:
        if pid is None:
            print("no running process: nothing armed")
            return 2
        if args.dry_run:
            print(f"dry run: would arm {ws} for a cycle at its next stop; no flag written")
            return 0
        write_flag(root, {"session_id": sid, "phase": "requested", "since": time.time(), "by": "bt cycle"})
        log(f"armed {ws} session {harvest.id8(sid)} tokens={tokens}")
        print(f"armed: at its next stop the session is asked to drain/hand over, then cycled (flag {flag_path(root)}). "
              "Wake it with a message if it is idle.")
        return 0
    if running and not args.force:
        print("refusing --now while subagents are running (they would be stopped); wait for them or pass --force")
        return 2
    if pid is None:
        print("no running process for the owning session; nothing to cycle")
        return 2
    if args.dry_run:
        return run_cycle(root, sid, pid, tokens or 0, transcript, "manual --now", dry=True)
    ok, started = spawn_cycler(root, sid, pid, tokens or 0, transcript, "manual --now" + (" --force" if args.force else ""))
    if not ok:
        log(f"fail {ws} session {harvest.id8(sid)}: cycler {started}")
        print(f"cycler failed to start: {started}; the old session remains active", file=sys.stderr)
        return 1
    log(f"start {ws} session {harvest.id8(sid)} pid={pid} tokens={tokens}: manual --now; cycler {started}")
    print(f"cycler started ({started}); a waiting tab opens first, then the successor starts after this session exits.")
    return 0


def register(sub) -> None:
    p = sub.add_parser("cycle", help="cycle a large session to a fresh one (arm, --now, --status, --off/--on)")
    p.add_argument("--ws", help="worktree: ws name, repo/ws or unique substring (default: status of all)")
    p.add_argument("--now", action="store_true", help="cycle immediately (refuses while subagents run unless --force)")
    p.add_argument("--force", action="store_true")
    p.add_argument("--status", action="store_true", help="context size, subagents and phase per live baton session")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--off", action="store_true", help="kill switch on (.state/cycle.off)")
    p.add_argument("--on", action="store_true", help="kill switch off")
    # the detached cycler's own arguments
    p.add_argument("--wait-handoff", help=argparse.SUPPRESS)
    p.add_argument("--run", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--root", help=argparse.SUPPRESS)
    p.add_argument("--old-session", help=argparse.SUPPRESS)
    p.add_argument("--old-pid", help=argparse.SUPPRESS)
    p.add_argument("--tokens", help=argparse.SUPPRESS)
    p.add_argument("--transcript", help=argparse.SUPPRESS)
    p.add_argument("--reason", help=argparse.SUPPRESS)
    p.set_defaults(func=_cmd)
