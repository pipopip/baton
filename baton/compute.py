"""Route LANE shell compute through the queue's existing pure command policy.

This hook changes no scheduler or permissions. Non-LANE sessions are untouched;
queued commands are validated independently by the MCP adapter.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys

from . import config, hook, paths

SHELL_TOOLS = {"Bash", "PowerShell", "Monitor", "exec_command", "shell", "shell_command"}
POLICY = config.INSTALL_HOME / "mods/compute-queue/bin/policy.mjs"


def lane_root(data: dict) -> Path | None:
    root, _ = hook.resolve_root("codex", data.get("cwd") or os.getcwd(),
                                data.get("session_id"), data.get("transcript_path"))
    if root is None:
        return None
    if paths.ws_name(root).casefold().startswith("lane_"):
        return root
    try:
        with (paths.baton_dir(root) / "now.md").open(encoding="utf-8") as source:
            brief = source.read(8192)
        if re.search(r"^Workflow:\s*lane\s*$", brief, re.I | re.M):
            return root
    except OSError:
        pass
    return None


def classify(command: str) -> dict:
    result = subprocess.run([os.environ.get("CQ_NODE", "node"), str(POLICY)],
                            input=json.dumps({"command": command}), capture_output=True,
                            text=True, encoding="utf-8", timeout=5,
                            creationflags=config.CREATE_NO_WINDOW)
    answer = json.loads(result.stdout)
    verdict = answer.get("classification")
    if result.returncode or not isinstance(verdict, dict) or not isinstance(verdict.get("deny"), bool):
        raise ValueError("compute classifier returned no verdict")
    return verdict


def handle(data: dict) -> dict | None:
    if data.get("hook_event_name") != "PreToolUse" or data.get("tool_name") not in SHELL_TOOLS:
        return None
    tool_input = data.get("tool_input")
    if not isinstance(tool_input, dict):
        return None
    command = tool_input.get("command", tool_input.get("cmd"))
    if not isinstance(command, str) or not command or lane_root(data) is None:
        return None
    if os.environ.get("COMPUTE_QUEUE_BYPASS") == "1":
        # Match the existing Claude mod's explicit, logged escape hatch.
        script = config.INSTALL_HOME / "mods/compute-queue/bin/cq.py"
        subprocess.run([config.PYTHON, "-X", "utf8", str(script), "note"],
                       input=json.dumps({"kind": "bypass", "vendor": "codex",
                                         "session": data.get("session_id"), "command": command[:300]}),
                       capture_output=True, text=True, timeout=3, creationflags=config.CREATE_NO_WINDOW)
        return None
    try:
        verdict = classify(command)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        hook._log("codex", "PreToolUse", exc)
        # The same failure backstop as the Claude mod; classification is a
        # routing aid, not a claim to parse every possible shell program.
        if not re.search(r"(?:remoterun|heavyrun)\.py", command, re.I):
            return None
        verdict = {"deny": True, "reason": "Compute classifier unavailable; use the compute-queue run tool."}
    if verdict["deny"]:
        return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny",
                                       "permissionDecisionReason": str(verdict.get("reason") or "Use compute-queue run.")}}
    return None


def main() -> int:
    try:
        data = json.loads(sys.stdin.buffer.read().decode("utf-8-sig"))
        result = handle(data) if isinstance(data, dict) else None
        if result:
            print(json.dumps(result))
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        hook._log("codex", "PreToolUse", exc)
    return 0
