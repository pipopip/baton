"""LaunchAgents on macOS: the one place baton writes a plist and calls ``launchctl``.

Two agents use it (labels in the ``me.baton`` namespace): ``me.baton.night`` runs ``bt night run`` every evening
(``bt night schedule``) and ``me.baton.startup`` reopens the live workstreams' tabs at login (``bt startup install``).
Plists live in ``~/Library/LaunchAgents`` and are written with ``plistlib``, atomically. ``launchctl`` is called with
the modern verbs only: ``bootstrap gui/<uid> <plist>``, ``bootout gui/<uid>/<label>``, ``print gui/<uid>/<label>``,
and only through ``launchctl()``, which tests replace. Nothing here runs on other platforms except the pure
functions (plist content, PATH), which tests use anywhere.
"""
from __future__ import annotations

import os
import plistlib
import posixpath
import re
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import config

NIGHT_LABEL = "me.baton.night"
STARTUP_LABEL = "me.baton.startup"
LAUNCHCTL_TIMEOUT = 30
BOOTSTRAP_TRIES = 3            # right after a bootout the old job can still be going away: bootstrap then says "I/O error"
BOOTSTRAP_WAIT = 1.0
# A LaunchAgent starts with only /usr/bin:/bin:/usr/sbin:/sbin; the night run needs git and ssh, which Homebrew's
# (or Xcode's) installs put in these directories.
EXTRA_PATH = ("/opt/homebrew/bin", "/opt/homebrew/sbin", "/usr/local/bin")
SYSTEM_PATH = ("/usr/bin", "/bin", "/usr/sbin", "/sbin")


def agents_dir() -> Path:
    return Path.home() / "Library" / "LaunchAgents"


def plist_path(label: str) -> Path:
    return agents_dir() / f"{label}.plist"


def log_paths(label: str) -> tuple[Path, Path]:
    """Where the agent's stdout and stderr go: under baton's state directory (created at install)."""
    folder = config.STATE_DIR / "launchd"
    return folder / f"{label}.out.log", folder / f"{label}.err.log"


def agent_path(python: str | os.PathLike) -> str:
    """The agent's ``PATH``: the recorded Python's directory first (the shell the user starts it from found it
    there), then Homebrew, then the system directories."""
    dirs = [posixpath.dirname(str(python)), *EXTRA_PATH, *SYSTEM_PATH]
    seen: list[str] = []
    for d in dirs:
        if d and d != "." and d not in seen:
            seen.append(d)
    return ":".join(seen)


def build_plist(label: str, arguments: list[str], python: str, **extra) -> dict:
    """The plist of an agent: ``arguments`` run in baton's checkout, in the background, logging to the state directory."""
    out, err = log_paths(label)
    plist = {"Label": label, "ProgramArguments": [str(a) for a in arguments],
             "WorkingDirectory": str(config.INSTALL_HOME), "ProcessType": "Background",
             "StandardOutPath": str(out), "StandardErrorPath": str(err),
             "EnvironmentVariables": {"PATH": agent_path(python)}}
    plist.update(extra)
    return plist


def python_exe() -> str:
    """Absolute path of the Python baton runs on (``config.PYTHON`` may be a bare name)."""
    import shutil
    return config.PYTHON if os.path.isabs(config.PYTHON) else (shutil.which(config.PYTHON) or config.PYTHON)


# ------------------------------------------------------------------ launchctl

def uid() -> int:
    return os.getuid()


def launchctl(*args: str) -> tuple[int, str]:
    """(returncode, output) of one ``launchctl`` call; (127, reason) when it cannot run or times out."""
    try:
        cp = subprocess.run(["launchctl", *args], capture_output=True, text=True, errors="replace",
                            timeout=LAUNCHCTL_TIMEOUT, stdin=subprocess.DEVNULL, env=config.clean_env())
    except (OSError, subprocess.SubprocessError) as exc:
        return 127, str(exc)
    return cp.returncode, ((cp.stdout or "") + (cp.stderr or "")).strip()


def _sleep(seconds: float) -> None:
    time.sleep(seconds)


def target(label: str) -> str:
    return f"gui/{uid()}/{label}"


def is_loaded(label: str) -> bool:
    return launchctl("print", target(label))[0] == 0


def output_says_running(text: str) -> bool:
    """Whether the output of ``launchctl print`` reports the job running: ``state = running``, or a ``pid = N`` line
    with N above 0 (a waiting job has no pid line). Tolerant: any other output means not running."""
    if re.search(r"^\s*state\s*=\s*running\s*$", text, re.MULTILINE):
        return True
    return any(int(n) > 0 for n in re.findall(r"^\s*pid\s*=\s*(\d+)\s*$", text, re.MULTILINE))


def is_running(label: str) -> bool:
    """Whether the agent's program is running now (False when it is not loaded or launchctl cannot say)."""
    code, out = launchctl("print", target(label))
    return code == 0 and output_says_running(out)


def bootout(label: str) -> str | None:
    """Unload the agent. None when it is unloaded afterwards (an agent that was not loaded is fine), else the error."""
    code, out = launchctl("bootout", target(label))
    if code == 0 or not is_loaded(label):
        return None
    return f"launchctl bootout {target(label)} failed ({code}): {out[:300]}"


def bootstrap(label: str) -> str | None:
    """Load the agent from its plist. None on success, else the error."""
    code, out = 0, ""
    for attempt in range(BOOTSTRAP_TRIES):
        code, out = launchctl("bootstrap", f"gui/{uid()}", str(plist_path(label)))
        if code == 0:
            return None
        if attempt + 1 < BOOTSTRAP_TRIES:
            _sleep(BOOTSTRAP_WAIT)
    return f"launchctl bootstrap gui/{uid()} {plist_path(label)} failed ({code}): {out[:300]}"


# ------------------------------------------------------------------ install, remove, status

def write_plist(label: str, data: dict) -> Path:
    """Write the plist atomically (temp file, then rename), readable by launchd (0644)."""
    path = plist_path(label)
    path.parent.mkdir(parents=True, exist_ok=True)
    out, err = log_paths(label)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_bytes(plistlib.dumps(data))
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return path


@dataclass
class Installed:
    path: Path
    replaced: bool
    error: str | None = None


def install(label: str, data: dict, *, load: bool = True, force: bool = False) -> Installed:
    """Re-installable: unload any loaded copy, write the plist, load it again (``load=False``: leave it for the
    next login). Stops at the first error and reports it; the plist is not written when the old copy cannot be unloaded.
    Unloading a job that is running kills it, so a running one is refused (nothing changed) unless ``force``."""
    path = plist_path(label)
    replaced = path.exists()
    if not force and is_running(label):
        return Installed(path, replaced, f"{label} is running now, and replacing it would stop it mid-run: try again when "
                                         "it has finished (or add --force to stop it)")
    error = bootout(label)
    if error:
        return Installed(path, replaced, error)
    try:
        write_plist(label, data)
    except OSError as exc:
        return Installed(path, replaced, f"could not write {path}: {exc}")
    return Installed(path, replaced, bootstrap(label) if load else None)


def uninstall(label: str) -> tuple[bool, str | None]:
    """(plist was there, error). Unloads, then removes the plist; both are fine when already gone."""
    error = bootout(label)
    if error:
        return False, error
    path = plist_path(label)
    try:
        path.unlink()
        return True, None
    except FileNotFoundError:
        return False, None
    except OSError as exc:
        return False, f"could not remove {path}: {exc}"


@dataclass
class Status:
    path: Path
    installed: bool
    loaded: bool = False
    last_exit: str | None = None
    plist: dict = field(default_factory=dict)
    detail: str = ""


def status(label: str) -> Status:
    path = plist_path(label)
    st = Status(path, path.is_file())
    if st.installed:
        try:
            st.plist = plistlib.loads(path.read_bytes())
        except (OSError, plistlib.InvalidFileException, ValueError):
            st.plist = {}
    code, out = launchctl("print", target(label))
    st.loaded = code == 0
    if st.loaded:
        found = re.search(r"last exit (?:code|status) = (\S+)", out)
        st.last_exit = found.group(1) if found else None
    else:
        st.detail = out[:200]
    return st
