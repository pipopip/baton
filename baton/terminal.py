"""One terminal backend for every place baton opens a terminal tab.

A tab is described neutrally (``Tab``: working directory, title and a ``Launch`` request, which says *what to run*,
never shell text) and each backend renders it for its own terminal:

* ``windows-terminal``: ``wt.exe`` with PowerShell text, exactly the command lines baton always built.
* ``iterm2``: ``osascript`` creating a tab (or window) whose command is ``/bin/zsh -lc '<text>'``.
* ``terminal-app``: ``osascript`` with ``do script`` (Terminal.app has no scripted new tab; each tab is a window).
* ``none`` (Linux, or a bad setting): opening a tab returns an error; every caller already keeps the old session.

Selection: ``[policy] terminal`` in accounts.toml (see there) wins; ``auto`` picks Windows Terminal on Windows, on
macOS iTerm2 when it is installed (``/Applications`` or ``~/Applications``) or this process runs in it
(``TERM_PROGRAM=iTerm.app``), else Terminal.app, else none.

Environment. ``wt.exe`` starts the tab as a child of the caller, so the tab inherits the caller's environment and
callers pass ``config.clean_env()``. iTerm2 and Terminal.app create the tab themselves: it inherits *nothing* from the
caller (the new shell is a login shell of the user's own environment), so everything a tab needs has to be in its
command text (``Launch.env``, the checkout and Python that ``bt.sh`` is sourced with). ``clean_env()`` is still what
``osascript`` itself runs with.

POSIX text. Every value is quoted with ``shlex.quote``/``shlex.join`` and nothing else, so a prompt with quotes, ``$``,
backticks, newlines or non-ASCII text reaches ``bt`` unchanged. The text starts with ``cd``, sources ``bt.sh`` with the
recorded checkout and Python (the line ``bt install`` writes to the rc file, so the tab does not depend on the rc file
defining ``bt``), runs the command and ends with ``exit $?`` (its status) unless the tab is to stay open, which ends
in ``exec /bin/zsh -l`` instead. A command that fails (non-zero status) also leaves that shell, after one line naming
the status, so its error is not closed away with the tab (in a context cycle the old session is already gone). The
text runs as ``/bin/zsh -lc``: ``-l`` so the login files (``.zprofile``, where
Homebrew puts its ``PATH``) are read and ``git``, ``claude`` and ``python`` are found as in any new terminal; no ``-i``,
because an interactive shell reads ``.zshrc``, whose prompt plugins and banners can stall or garble a tab that is
about to run one command and which is not needed since the text sources ``bt.sh`` itself.

AppleScript. The script text is constant; every variable string (command, title) is an ``on run argv`` argument
after ``--``, so no AppleScript escaping exists anywhere.
"""
from __future__ import annotations

import base64
import os
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from . import config

OSASCRIPT_TIMEOUT = 20
SETTING = "terminal"
LOGIN_SHELL = "/bin/zsh"      # the shell a macOS tab runs its text under and is left in when it stays open
NAMES = ("windows-terminal", "iterm2", "terminal-app", "none")
ITERM_APPS = ("/Applications/iTerm.app", "~/Applications/iTerm.app")
AUTOMATION_REMEDY = ("System Settings > Privacy & Security > Automation: allow the terminal baton runs from to "
                     "control {app}")
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class Word(str):
    """A fixed word of a command (program, subcommand, option) as opposed to a value: PowerShell takes it bare,
    where a value is always single-quoted; POSIX quoting treats both alike."""


@dataclass(frozen=True)
class Launch:
    """What a tab runs. Exactly one of ``bt`` and ``steps``.

    ``bt``: the arguments of the ``bt`` command (the function of bt.ps1 or bt.sh), see ``bt_args``.
    ``steps``: commands (argvs) run one after another, each ignoring the status of the one before, after ``env``
    is set and followed by ``message``. ``gate``: an argv that must exit 0 before the rest runs; its status is the
    tab's when it does not. ``keep_open``: leave an interactive shell in the tab afterwards, otherwise the tab's
    shell exits with the command's status."""
    bt: tuple[str, ...] | None = None
    steps: tuple[tuple[str, ...], ...] = ()
    env: tuple[tuple[str, str], ...] = ()
    message: str | None = None
    gate: tuple[str, ...] | None = None
    keep_open: bool = False

    def __post_init__(self):
        if (self.bt is None) == (not self.steps):
            raise ValueError("a launch is either bt arguments or steps")
        for name, _ in self.env:
            if not _ENV_NAME.fullmatch(name):
                raise ValueError(f"not an environment variable name: {name!r}")


@dataclass(frozen=True)
class Tab:
    cwd: str
    title: str
    launch: Launch


@dataclass(frozen=True)
class Opened:
    """Result of opening tabs: truthy when they were requested. ``session_ids``: the terminal's ids of the new
    sessions (iTerm2 ``unique id``, Terminal.app ``tty``) when it says them."""
    error: str | None = None
    session_ids: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return self.error is None


def bt_args(query: str, vendor: str | None = None, account: str | None = None, *, fresh: bool = False,
            prompt: str | None = None, model: str | None = None, switch: bool = False, resume: bool = False,
            force: bool = False, role: str | None = None) -> tuple[str, ...]:
    """``bt [switch] <query> [-Vendor v] [-Account a] [-Fresh] [-Resume] [-Force] [-Prompt p] [-Role r] [-Model m]``:
    the spellings and the order of bt.ps1's own ``Open-BatonTab``, which bt.ps1 and bt.sh both parse."""
    out: list[str] = [Word("switch")] if switch else []
    out.append(str(query))
    for flag, value in (("-Vendor", vendor), ("-Account", account)):
        if value:
            out += [Word(flag), value]
    out += [Word(flag) for flag, on in (("-Fresh", fresh), ("-Resume", resume), ("-Force", force)) if on]
    for flag, value in (("-Prompt", prompt), ("-Role", role), ("-Model", model)):
        if value:
            out += [Word(flag), value]
    return tuple(out)


def handoff_gate(token: str, root, vendor: str | None = None) -> tuple[str, ...]:
    """The argv a staged tab runs first: wait until the old session is gone (exit 0) or give up (non-zero).

    Off Windows ``vendor`` is added (``--vendor``): the gate then checks, before it reports ready, that the CLI the
    successor will run can be started in the tab's own environment (a macOS tab is a fresh login shell that has read
    ``.zprofile`` and not ``.zshrc``, so a ``PATH`` or ``BATON_CLAUDE_EXE`` set only in ``.zshrc`` is not there).
    The Windows argv is unchanged: its tab inherits the caller's environment, which has already been through that."""
    argv = (str(config.PYTHON), Word("-S"), str(config.INSTALL_HOME / "bt.py"), Word("cycle"), Word("--wait-handoff"),
            token, Word("--root"), str(root))
    return argv + (Word("--vendor"), vendor) if vendor and not config.WINDOWS else argv


# ------------------------------------------------------------------ PowerShell text (Windows Terminal)

def _lit(text) -> str:
    return "'" + str(text).replace("'", "''") + "'"


def _ps_word(item: str) -> str:
    return str(item) if isinstance(item, Word) else _lit(item)


def _ps_argv(argv) -> str:
    first = argv[0]
    head = str(first) if isinstance(first, Word) else f"& {_lit(first)}"
    return " ".join([head, *(_ps_word(a) for a in argv[1:])])


def powershell_text(launch: Launch) -> str:
    """The PowerShell text of a launch, as baton has always built it (the tests pin these strings)."""
    if launch.bt is not None:
        text = f". {_lit(config.INSTALL_HOME / 'bt.ps1')}; " + _ps_argv((Word("bt"), *launch.bt))
    else:
        parts = [f"$env:{name}={_lit(value)}" for name, value in launch.env] + [_ps_argv(s) for s in launch.steps]
        if launch.message is not None:
            parts.append(f"Write-Host {_lit(launch.message)}")
        text = "; ".join(parts)
    if launch.gate:
        text = f"{_ps_argv(launch.gate)}; if ($LASTEXITCODE -ne 0) {{ exit $LASTEXITCODE }}; {text}"
    return text


def _ps_shell() -> str:
    return "powershell" if not shutil.which("pwsh") and shutil.which("powershell") else "pwsh"


# ------------------------------------------------------------------ POSIX text (iTerm2, Terminal.app)

def _stay_on_failure(tab: Tab) -> str:
    """The end of a tab that is not to stay open: exit with the command's status when that is 0, else say so and
    leave a login shell, so the error stays on screen (iTerm2's default profile closes a tab when its command exits;
    Windows Terminal keeps one open after a non-zero exit). A confirmed retirement ends the script inside the ``bt``
    function with status 0 before this runs, and a failing gate ends it with ``exit $?``: both still close the tab."""
    if tab.launch.bt is not None:
        # the path is a printf argument (quoted twice: once for this shell, once so the printed command is pasteable)
        say = ("'bt exited with status %s; start the session by hand with: bt %s -Fresh\\n' \"$__baton_rc\" "
               + shlex.quote(shlex.quote(str(tab.cwd))))
    else:
        say = "'the command exited with status %s; this shell stays open\\n' \"$__baton_rc\""
    return (f'__baton_rc=$?; if [ "$__baton_rc" -ne 0 ]; then printf {say}; '
            f'exec {LOGIN_SHELL} -l; fi; exit "$__baton_rc"')


def posix_text(tab: Tab) -> str:
    """The shell text a macOS tab runs under ``/bin/zsh -lc`` (see the module docstring)."""
    from . import install            # install imports wtguard, never this module
    launch = tab.launch
    parts = [f"cd {shlex.quote(str(tab.cwd))} || exit $?"]
    parts += [f"export {name}={shlex.quote(value)}" for name, value in launch.env]
    if launch.gate:
        parts.append(f"{shlex.join(launch.gate)} || exit $?")
    if launch.bt is not None:
        parts.append(f"{install.posix_source_text()} || exit $?")
        parts.append(shlex.join(["bt", *launch.bt]))
    else:
        parts += [shlex.join(step) for step in launch.steps]
        if launch.message is not None:
            parts.append(f"printf '%s\\n' {shlex.quote(launch.message)}")
    if launch.keep_open:
        parts.append(f"exec {LOGIN_SHELL} -l")
    else:
        parts.append(_stay_on_failure(tab))
    return "; ".join(parts)


def zsh_command(tab: Tab) -> str:
    return shlex.join(["/bin/zsh", "-lc", posix_text(tab)])


# ------------------------------------------------------------------ backends

class Backend:
    name = "none"

    def command_text(self, tab: Tab) -> str:
        """What the tab runs, for the dry run."""
        return zsh_command(tab)

    def argv(self, tabs, *, new_window: bool = False) -> list[str]:
        """The process ``open`` starts, for the dry run."""
        return []

    def open(self, tabs, *, new_window: bool = False, env: dict | None = None) -> Opened:
        """Request one tab per entry of ``tabs`` (``new_window``: all in a new window). Never raises."""
        raise NotImplementedError


class NoTerminal(Backend):
    def __init__(self, reason: str | None = None):
        self.reason = reason or (f"baton opens terminal tabs on Windows (Windows Terminal) and macOS (iTerm2, Terminal.app); "
                                 f"this is {sys.platform}, so there is no tab to open")

    def open(self, tabs, *, new_window: bool = False, env: dict | None = None) -> Opened:
        return Opened(error=self.reason)


class WindowsTerminal(Backend):
    name = "windows-terminal"

    def __init__(self, wt: str | None = None, shell: str | None = None):
        self.wt = wt or shutil.which("wt.exe") or "wt.exe"
        self.shell = shell or _ps_shell()

    def command_text(self, tab: Tab) -> str:
        return powershell_text(tab.launch)

    def _tab(self, tab: Tab, verb: str) -> list[str]:
        from . import launch as launcher   # launch imports accounts, which imports this module
        text = powershell_text(tab.launch)
        if tab.launch.keep_open:           # an interactive tab: the shell stays, it is not wrapped to exit
            return [verb, "--title", tab.title, "-d", str(tab.cwd), self.shell, "-NoExit", "-EncodedCommand", encode(text)]
        return [verb, "-d", str(tab.cwd), "--title", tab.title, "--suppressApplicationTitle", self.shell,
                "-EncodedCommand", encode(launcher.managed_tab_command(text))]

    def argv(self, tabs, *, new_window: bool = False) -> list[str]:
        """``wt -w new`` with one ``new-tab`` per tab (separated by ``;``), or into window 0 (the most recently used)."""
        argv = [self.wt, "-w", "new" if new_window else "0"]
        for i, tab in enumerate(tabs):
            if i:
                argv.append(";")
            plain = not new_window and not tab.launch.keep_open
            argv += self._tab(tab, "nt" if plain else "new-tab")   # "nt" is wt's alias of "new-tab", kept as it was
        return argv

    def open(self, tabs, *, new_window: bool = False, env: dict | None = None) -> Opened:
        try:
            subprocess.Popen(self.argv(tabs, new_window=new_window), env=config.clean_env() if env is None else env,
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             close_fds=True)
        except OSError as exc:
            return Opened(error=str(exc))
        return Opened()


def encode(text: str) -> str:
    """PowerShell's ``-EncodedCommand``: base64 of UTF-16-LE."""
    return base64.b64encode(text.encode("utf-16-le")).decode("ascii")


# One constant script per application; the variable strings come in after `--` (`on run argv`), never inside it.
# argv: "current" or "new", then (command, title) pairs. Result: the new sessions' ids, one per line.
# iTerm2 is named by its bundle id: the application file is iTerm.app, and `tell application "iTerm2"` compiles only
# while iTerm2 is running (measured on macOS 15: installed but not started, the name is not found and its terms do not
# parse, registered with Launch Services or not). The id finds it either way and starts it when it is not running.
ITERM_BUNDLE_ID = "com.googlecode.iterm2"
ITERM_SCRIPT = (
    "on run argv",
    "set theIds to {}",
    f'tell application id "{ITERM_BUNDLE_ID}"',
    "set theWindow to missing value",
    'if (item 1 of argv) is "current" and (count of windows) > 0 then set theWindow to current window',
    "repeat with i from 2 to (count of argv) by 2",
    "set theCommand to item i of argv",
    "set theTitle to item (i + 1) of argv",
    "if theWindow is missing value then",
    "set theWindow to (create window with default profile command theCommand)",
    "set theSession to current session of current tab of theWindow",
    "else",
    "tell theWindow",
    "set theTab to (create tab with default profile command theCommand)",
    "end tell",
    "set theSession to current session of theTab",
    "end if",
    "tell theSession to set name to theTitle",
    "set end of theIds to (unique id of theSession)",
    "end repeat",
    'if (item 1 of argv) is "new" then activate',
    "end tell",
    "set AppleScript's text item delimiters to linefeed",
    "return theIds as text",
    "end run",
)
# argv: (command, title) pairs. Terminal.app opens a window per `do script`; the result is each new tab's tty.
TERMINAL_SCRIPT = (
    "on run argv",
    "set theIds to {}",
    'tell application "Terminal"',
    "repeat with i from 1 to (count of argv) by 2",
    "set theTab to (do script (item i of argv))",
    "set custom title of theTab to (item (i + 1) of argv)",
    "set end of theIds to (tty of theTab)",
    "end repeat",
    "activate",
    "end tell",
    "set AppleScript's text item delimiters to linefeed",
    "return theIds as text",
    "end run",
)


def osascript_argv(script, args) -> list[str]:
    argv = ["osascript"]
    for line in script:
        argv += ["-e", line]
    return argv + ["--", *args]


def run_osascript(script, args, env: dict | None = None) -> tuple[str | None, tuple[str, ...]]:
    """(error text, session ids). An error is the stderr text plus a one-line remedy; never an exception."""
    app = "iTerm2" if script is ITERM_SCRIPT else "Terminal"
    try:
        cp = subprocess.run(osascript_argv(script, args), capture_output=True, text=True, encoding="utf-8",
                            errors="replace", timeout=OSASCRIPT_TIMEOUT, stdin=subprocess.DEVNULL,
                            env=config.clean_env() if env is None else env)
    except subprocess.TimeoutExpired:
        return (f"osascript did not answer within {OSASCRIPT_TIMEOUT} s; {app} may be waiting for an Automation "
                f"permission prompt: answer it, or " + AUTOMATION_REMEDY.format(app=app)), ()
    except OSError as exc:
        return f"osascript could not be started: {exc}; it exists on macOS only (see [policy] terminal in accounts.toml)", ()
    if cp.returncode != 0:
        text = " ".join((cp.stderr or cp.stdout or "").split())[:400] or "no message"
        if "-1743" in text or "Not authorized" in text:
            remedy = "the Automation permission was refused: " + AUTOMATION_REMEDY.format(app=app)
        else:
            remedy = f"check that {app} is installed and can be started"
        return f"osascript failed ({cp.returncode}): {text}; {remedy}", ()
    return None, tuple(line.strip() for line in (cp.stdout or "").splitlines() if line.strip())


class _Osascript(Backend):
    script: tuple = ()

    def argv(self, tabs, *, new_window: bool = False) -> list[str]:
        return osascript_argv(self.script, self._args(tabs, new_window))

    def _args(self, tabs, new_window: bool) -> list[str]:
        raise NotImplementedError

    def open(self, tabs, *, new_window: bool = False, env: dict | None = None) -> Opened:
        error, ids = run_osascript(self.script, self._args(tabs, new_window), env)
        return Opened(error=error, session_ids=ids)


class ITerm2(_Osascript):
    name = "iterm2"
    script = ITERM_SCRIPT

    def _args(self, tabs, new_window: bool) -> list[str]:
        out = ["new" if new_window else "current"]
        for tab in tabs:
            out += [zsh_command(tab), tab.title]
        return out


class TerminalApp(_Osascript):
    name = "terminal-app"
    script = TERMINAL_SCRIPT

    def command_text(self, tab: Tab) -> str:
        return "exec " + zsh_command(tab)       # the window's interactive shell is replaced, so the window ends with it

    def _args(self, tabs, new_window: bool) -> list[str]:
        out: list[str] = []
        for tab in tabs:
            out += [self.command_text(tab), tab.title]
        return out


# ------------------------------------------------------------------ selection

def iterm_present(environ=None, exists=os.path.exists) -> bool:
    environ = os.environ if environ is None else environ
    return environ.get("TERM_PROGRAM") == "iTerm.app" or any(exists(os.path.expanduser(p)) for p in ITERM_APPS)


def select(setting: str | None = None, environ=None, exists=os.path.exists) -> Backend:
    """The backend to use: the ``[policy] terminal`` setting, else by platform (see the module docstring)."""
    name = (config.policy_str(SETTING, "auto") if setting is None else setting).strip().lower()
    if name == "auto":
        if config.WINDOWS:
            name = "windows-terminal"
        elif config.MACOS:
            name = "iterm2" if iterm_present(environ, exists) else "terminal-app"
        else:
            name = "none"
    if name == "windows-terminal":
        return WindowsTerminal()
    if name == "iterm2":
        return ITerm2()
    if name == "terminal-app":
        return TerminalApp()
    if name == "none":
        return NoTerminal()
    return NoTerminal(f"[policy] terminal = {name!r} is not one of auto, {', '.join(NAMES)}")


def backend() -> Backend:
    return select()


def describe(be: Backend, tabs, *, new_window: bool = False) -> str:
    """One line for a dry run: the process the backend would start."""
    argv = be.argv(tabs, new_window=new_window)
    if not argv:
        return f"{be.name}: " + getattr(be, "reason", "no command")
    shown = subprocess.list2cmdline(argv) if be.name == "windows-terminal" else shlex.join(argv)
    return f"{Path(argv[0]).stem} argv: {shown}"
