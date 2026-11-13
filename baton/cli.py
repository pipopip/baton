"""``bt`` command-line entry point.

Each feature module exposes ``register(subparsers)`` and sets ``func`` on its
parsers; this file only wires them together.
"""
from __future__ import annotations

import argparse
import importlib
import sys

MODULES = ("launch", "accounts", "routing", "follow", "capsule", "install", "close", "doctor", "attach", "worktrees", "backup", "startup", "cycle", "compute_pickup", "compute_status", "night", "migrate", "shell")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bt", description="Baton: shared context, roles and followers for Claude Code and Codex worktrees.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in MODULES:
        try:
            module = importlib.import_module(f"baton.{name}")
        except ImportError:
            continue
        register = getattr(module, "register", None)
        if register is not None:
            register(sub)
    return parser


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv and argv[0] == "shell":
        # `bt shell` takes the user's launch options (-Fresh, --dry-run, ...) verbatim; argparse would reject them.
        from .shell import run_tokens
        return run_tokens(argv[1:])
    args = build_parser().parse_args(argv)
    return int(args.func(args) or 0)
