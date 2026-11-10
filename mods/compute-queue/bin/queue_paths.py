"""Portable queue state and machine-local helper configuration."""
import json
import os
from pathlib import Path
import stat

_GENERATION_MARKER = b'baton-queue-generation-2\n'


def is_generation2(state):
    path = Path(state)
    if path.name != 'queue-v2':
        return False
    marker = path / '.generation'
    try:
        st = marker.lstat()
    except FileNotFoundError:
        return False
    if path.is_symlink() or not stat.S_ISREG(st.st_mode):
        raise ValueError('unsafe queue generation marker')
    fd = os.open(marker, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode) or stream.read(100) != _GENERATION_MARKER:
            raise ValueError('invalid queue generation marker')
    return True


def ensure_generation2(state):
    path = Path(state)
    if path.name != 'queue-v2':
        raise ValueError('invalid generation-two state path')
    if path.exists() and (path.is_symlink() or not path.is_dir()):
        raise ValueError('unsafe generation-two state directory')
    path.mkdir(parents=True, exist_ok=True)
    if is_generation2(path):
        return
    if any(path.iterdir()):
        raise ValueError('generation-two state exists without a valid marker')
    try:
        fd = os.open(path / '.generation', os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    except FileExistsError:
        if is_generation2(path):
            return
        raise ValueError('invalid queue generation marker')
    with os.fdopen(fd, 'wb') as stream:
        stream.write(_GENERATION_MARKER)
        stream.flush()
        os.fsync(stream.fileno())


def config_state(state):
    """Configuration and shared legacy locks live above an isolated v2 store."""
    path = Path(state)
    return str(path.parent if is_generation2(path) else path)


def default_state():
    if os.environ.get("CQ_STATE"):
        return os.environ["CQ_STATE"]
    if os.environ.get("BATON_STATE"):
        return str(Path(os.environ["BATON_STATE"]) / "compute")
    root = Path(__file__).resolve().parents[3]
    # Baton-created worktrees live under the main checkout.
    if root.parent.name == "worktrees" and root.parent.parent.name == ".claude":
        root = root.parents[2]
    return str(root / ".state" / "compute")


def runtime_config(state):
    state = config_state(state)
    try:
        with open(os.path.join(state, "runtime.local.json"), encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, RecursionError):
        return {}


def runtime_paths(state):
    state = config_state(state)
    config = runtime_config(state)
    legacy = os.environ.get("CQ_BATON_DIR") or ""

    def value(name, default=""):
        v = os.environ.get("CQ_" + name) or config.get(name.lower()) or default
        return v if isinstance(v, str) else default

    runner = value("REMOTERUN", str(Path(legacy) / "remote" / "remoterun.py") if legacy else
                   str(Path(__file__).resolve().parent / "remote_stage.py"))
    heavy = value("HEAVYRUN", str(Path(legacy) / "heavyrun.py") if legacy else "")
    lock = value("HEAVY_LOCK", str(Path(heavy).parent / "heavy.lock") if heavy else str(Path(state) / "heavy.lock"))
    return runner, heavy, lock


def explicit_legacy_lock(state):
    """A configured lock or helper identifies the shared legacy lock convention."""
    state = config_state(state)
    config = runtime_config(state)
    return any(isinstance(v, str) and bool(v.strip()) for v in
               (os.environ.get("CQ_HEAVY_LOCK"), os.environ.get("CQ_HEAVYRUN"),
                os.environ.get("CQ_BATON_DIR"), config.get("heavy_lock"), config.get("heavyrun")))


_DEFAULT_LOCK_MARKER = "fleet-default-lock.v1"
_MARKER_CONTENT = b"state-heavy-lock-v1\n"


def default_lock_confirmed(state):
    state = config_state(state)
    if explicit_legacy_lock(state):
        return True
    try:
        return (Path(state) / _DEFAULT_LOCK_MARKER).read_bytes() == _MARKER_CONTENT
    except OSError:
        return False


def initialize_default_lock(state):
    """Under queue lock, establish the default only before any persisted work exists.

    Old queues must explicitly configure their existing lock; absence of a marker
    after persisted work is never interpreted as consent to a new lock.
    """
    state = config_state(state)
    marker = Path(state) / _DEFAULT_LOCK_MARKER
    if explicit_legacy_lock(state):
        # Removing an override later must not revive an earlier default convention.
        try:
            marker.unlink(missing_ok=True)
        except OSError:
            pass
        return
    if default_lock_confirmed(state):
        return
    try:
        if marker.exists():
            return
        for name in ("jobs", "deferred-jobs", "fleet/jobs", "run", "fleet/run",
                     "queue-v2/jobs", "queue-v2/deferred-jobs", "queue-v2/fleet/jobs",
                     "queue-v2/run", "queue-v2/fleet/run"):
            directory = Path(state) / name
            if directory.is_dir() and next(directory.iterdir(), None) is not None:
                return
        for name in ("history.jsonl", "queue-v2/history.jsonl"):
            history = Path(state) / name
            if history.exists() and history.stat().st_size:
                return
        marker.parent.mkdir(parents=True, exist_ok=True)
        with marker.open("xb") as f:
            f.write(_MARKER_CONTENT)
    except OSError:
        # Unknown or inaccessible state cannot safely establish a new convention.
        return
