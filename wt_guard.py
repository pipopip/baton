"""PreToolUse gate entry point: ``python -S wt_guard.py <claude|codex>`` with the hook payload on stdin.

Thin shim like hook.py: puts BATON_HOME on sys.path and hands over to ``baton.wtguard.main``. Fails open.
"""
import os
import sys

BATON_HOME = os.path.dirname(os.path.abspath(__file__))


def run() -> int:
    if BATON_HOME not in sys.path:
        sys.path.insert(0, BATON_HOME)
    try:
        from baton.wtguard import main

        return main(sys.argv[1] if len(sys.argv) > 1 else "claude", BATON_HOME)
    except Exception:
        return 0


if __name__ == "__main__":
    sys.exit(run())
