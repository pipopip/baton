"""Context-cycle hook entry point: ``python -S C:/Work/baton/cycle_hook.py <stop|pretool>`` with the hook's JSON on stdin.

Thin shim like hook.py: puts BATON_HOME on sys.path and hands over to ``baton.cycle.main``.
Always exits 0 and never prints a traceback (a failed hook must never stop a session).
"""
import os
import sys

BATON_HOME = os.path.dirname(os.path.abspath(__file__))


def run() -> int:
    if BATON_HOME not in sys.path:
        sys.path.insert(0, BATON_HOME)
    try:
        from baton.cycle import main

        main(sys.argv[1] if len(sys.argv) > 1 else "stop")
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    run()
    sys.exit(0)
