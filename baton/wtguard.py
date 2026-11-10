"""PreToolUse gate that stops agents from running ``git worktree add``; the lead uses ``bt wt new``.

``python -S wt_guard.py <claude|codex>`` reads the hook payload on stdin. Only an actual git invocation is
blocked: ``git`` (or ``git.exe``) as a command, optionally behind env assignments or wrappers, with git's own
options (``-C <path>``, ``-c k=v``, ``--git-dir ...``) before ``worktree add``. Quoted text, a search for the
phrase, and file reads pass. A command handed to a shell (``bash -c``, ``pwsh -Command``, ``cmd /c``) is
checked too. Fails open: any error allows the command. Stdlib only.
"""
from __future__ import annotations

import json
import os
import re
import sys

WRAPPERS = {"env", "command", "call", "exec", "sudo", "nohup", "time", "start", "builtin", "nice", "&", "then", "do", "else"}
SHELLS = {"bash", "sh", "zsh", "dash", "cmd", "powershell", "pwsh"}
SHELL_FLAGS = {"-c", "-lc", "-ic", "-command", "/c", "/k"}
GIT_VALUE_OPTS = {"-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path", "--super-prefix", "--config-env"}
ENV_ASSIGN = re.compile(r"^[A-Za-z_]\w*=")
SEPARATORS = "\r\n;|&`()"
MAX_DEPTH = 4


def message(install_home: str) -> str:
    bt = os.path.join(install_home, "bt.py").replace("\\", "/")
    return (f"baton: agents must not run git worktree add; the lead creates worktrees with python {bt} "
            "wt new <name> --exit ...")


def segments(command: str) -> list[list[str]]:
    """Split ``command`` into simple commands (token lists), quote-aware: separators inside quotes do not split."""
    out: list[list[str]] = []
    tokens: list[str] = []
    cur: list[str] = []
    has_cur = False
    quote = ""
    i, n = 0, len(command)

    def end_token() -> None:
        nonlocal cur, has_cur
        if has_cur:
            tokens.append("".join(cur))
        cur, has_cur = [], False

    def end_segment() -> None:
        nonlocal tokens
        end_token()
        if tokens:
            out.append(tokens)
        tokens = []

    while i < n:
        c = command[i]
        if quote:
            if c == quote:
                quote = ""
            elif c == "\\" and quote == '"' and i + 1 < n and command[i + 1] in '"\\':
                i += 1
                cur.append(command[i])
            else:
                cur.append(c)
        elif c in "\"'":
            quote, has_cur = c, True
        elif c in SEPARATORS:
            end_segment()
        elif c.isspace():
            end_token()
        else:
            cur.append(c)
            has_cur = True
        i += 1
    end_segment()
    return out


def _base(token: str) -> str:
    name = token.replace("\\", "/").rsplit("/", 1)[-1].lower()
    return name[:-4] if name.endswith(".exe") else name


def _is_worktree_add(args: list[str]) -> bool:
    """``args`` follow ``git``: skip git's global options, then expect ``worktree`` and, after its options, ``add``."""
    i = 0
    while i < len(args) and args[i].startswith("-"):
        i += 2 if args[i] in GIT_VALUE_OPTS else 1
    if i >= len(args) or args[i].lower() != "worktree":
        return False
    rest = [a for a in args[i + 1:] if not a.startswith("-")]
    return bool(rest) and rest[0].lower() == "add"


def blocks(command: str, depth: int = 0) -> bool:
    """Whether ``command`` runs ``git worktree add``."""
    if depth > MAX_DEPTH:
        return False
    for tokens in segments(command):
        i = 0
        while i < len(tokens) and (ENV_ASSIGN.match(tokens[i]) or _base(tokens[i]) in WRAPPERS):
            i += 1
        if i >= len(tokens):
            continue
        name = _base(tokens[i])
        if name == "git":
            if _is_worktree_add(tokens[i + 1:]):
                return True
        elif name in SHELLS:
            rest = tokens[i + 1:]
            for j, tok in enumerate(rest):
                if tok.lower() in SHELL_FLAGS and j + 1 < len(rest):
                    if blocks(" ".join(rest[j + 1:]), depth + 1):
                        return True
                    break
    return False


def payload_command(raw: bytes) -> str | None:
    """The Bash command of a PreToolUse payload; None when it has none."""
    try:
        data = json.loads(raw.decode("utf-8-sig", errors="replace"))
    except ValueError:
        return None
    tool_input = data.get("tool_input") if isinstance(data, dict) else None
    command = tool_input.get("command") if isinstance(tool_input, dict) else None
    return command if isinstance(command, str) else None


def main(vendor: str, install_home: str) -> int:
    """Claude blocks on exit code 2 with the message on stderr; Codex on a JSON decision (exit 0). Never raises."""
    try:
        command = payload_command(sys.stdin.buffer.read())
        if command is None or not blocks(command):
            return 0
        text = message(install_home)
        if vendor == "codex":
            sys.stdout.write(json.dumps({"decision": "block", "reason": text}))
            sys.stdout.flush()
            return 0
        sys.stderr.write(text + "\n")
        sys.stderr.flush()
        return 2
    except Exception:
        return 0
