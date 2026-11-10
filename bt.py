"""Entry point: ``python C:/Work/baton/bt.py <command>`` (wrapped by the ``bt`` PowerShell function)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from baton.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
