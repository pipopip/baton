"""Entry point: ``python C:/Work/baton/bt.py <command>`` (wrapped by the ``bt`` PowerShell function)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from baton.cli import main  # noqa: E402

if __name__ == "__main__":
    from baton.shell import entry_refusal  # noqa: E402
    refusal = entry_refusal(sys.argv[1:])
    if refusal:
        print(refusal, file=sys.stderr)
        sys.exit(2)
    sys.exit(main())
