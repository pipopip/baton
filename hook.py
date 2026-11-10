"""Hook entry point: ``python -S C:/Work/baton/hook.py <claude|codex>`` with a JSON object on stdin.

Thin shim: puts BATON_HOME on sys.path and hands over to ``baton.hook.main``.
Always exits 0 and never prints a traceback.
"""
import os
import sys

BATON_HOME = os.path.dirname(os.path.abspath(__file__))


def run() -> int:
    if BATON_HOME not in sys.path:
        sys.path.insert(0, BATON_HOME)
    try:
        from baton.hook import main

        main(sys.argv[1] if len(sys.argv) > 1 else "")
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    run()
    sys.exit(0)
