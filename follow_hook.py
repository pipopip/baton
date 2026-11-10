"""git post-commit shim: ``python -S follow_hook.py <worktree>`` (installed by ``bt follow on``).

Always exits 0 and prints nothing: a follower problem must never disturb a commit.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

if __name__ == "__main__":
    try:
        from baton.follow import hook_main

        hook_main(sys.argv[1:])
    except Exception:
        pass
    sys.exit(0)
