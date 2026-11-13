"""``bt shell``: the whole ``bt`` command for zsh and bash (the POSIX counterpart of ``bt.ps1``).

``bt.sh`` defines a ``bt`` function that runs ``python bt.py shell "$@"``; everything bt.ps1 does is done here:
subcommand pass-through, option parsing, the fzf picker, the launch plan, the dry run, the foreground run of the
agent CLI and the terminal-retire check.  The one thing Python cannot do is end its parent shell, so the shell
function passes a temp file in ``BATON_SHELL_EXIT_FILE``; Python writes ``RETIRED_WORD`` there only when the
retire marker confirms this invocation's context cycle, and the function exits the shell only on that word.

Stdlib plus baton; works on Windows too (tests), where ``bt.ps1`` stays the launcher.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from . import launch, terminal

EXIT_FILE_ENV = "BATON_SHELL_EXIT_FILE"
RETIRED_WORD = "retired"
TOKEN_ENV = "BATON_TERMINAL_TOKEN"
RETIRE_WAIT_SECONDS = 6.0
RETIRE_POLL_SECONDS = 0.1
HELP_FLAGS = ("-h", "--help", "-?", "/?")
_SUBCOMMANDS = None   # the live name -> parser map of the top-level parser, set by register()

# Option spellings exactly as in bt.ps1 (matched case-insensitively, whole token).
_VALUE_OPTIONS = (("vendor", re.compile(r"--?vendor", re.I)), ("account", re.compile(r"--?account", re.I)),
                  ("prompt", re.compile(r"--?prompt", re.I)), ("role", re.compile(r"--?role", re.I)),
                  ("model", re.compile(r"--?model", re.I)))
_FLAG_OPTIONS = (("fresh", re.compile(r"--?fresh", re.I)), ("resume", re.compile(r"--?resume", re.I)),
                 ("new_tab", re.compile(r"--?new-?tab", re.I)), ("dry_run", re.compile(r"--?dry-?run", re.I)),
                 ("force", re.compile(r"--?force", re.I)))


class OptionError(ValueError):
    """A launch option the user got wrong; the message is printed as ``bt: <message>``."""


@dataclass
class LaunchOptions:
    query: str | None = None
    vendor: str | None = None
    account: str | None = None
    prompt: str | None = None
    role: str | None = None
    model: str | None = None
    fresh: bool = False
    resume: bool = False
    new_tab: bool = False
    dry_run: bool = False
    force: bool = False
    switch: bool = False


def parse_options(tokens) -> LaunchOptions:
    """Port of ``Get-BatonLaunchOptions``. Raises OptionError."""
    opt = LaunchOptions()
    tokens = [str(t) for t in (tokens or ())]
    i = 0
    while i < len(tokens):
        token = tokens[i]
        for name, rx in _VALUE_OPTIONS:
            if rx.fullmatch(token):
                i += 1
                if i >= len(tokens):
                    raise OptionError(f"{token} needs a value")
                setattr(opt, name, tokens[i])
                break
        else:
            for name, rx in _FLAG_OPTIONS:
                if rx.fullmatch(token):
                    setattr(opt, name, True)
                    break
            else:
                if token.startswith("-"):
                    raise OptionError(f"unknown option {token}")
                if opt.query:
                    raise OptionError(f"unexpected argument {token}")
                opt.query = token
        i += 1
    if opt.fresh and opt.resume:
        raise OptionError("-Fresh and -Resume are mutually exclusive")
    return opt


def subcommand_names() -> set[str]:
    """Subcommands of bt.py, read from its argparse parser (never a hand copy), without ``shell`` itself."""
    names = _SUBCOMMANDS
    if names is None:
        from .cli import build_parser
        names = next(a.choices for a in build_parser()._actions if isinstance(a, argparse._SubParsersAction))
    return {n for n in names if n != "shell"}


# ----------------------------------------------------------------- output

def _say(text: str, color: str | None = None, *, err: bool = False) -> None:
    stream = sys.stderr if err else sys.stdout
    code = {"gray": "90", "yellow": "33", "red": "31"}.get(color or "")
    if code and stream.isatty() and not os.environ.get("NO_COLOR"):
        text = f"\x1b[{code}m{text}\x1b[0m"
    print(text, file=stream, flush=True)


def format_command(argv) -> str:
    """Port of ``Format-BatonCommand`` (the dry run's ``run:`` line)."""
    return " ".join('"' + a.replace('"', '\\"') + '"' if re.search(r'[\s"]', a) else a for a in argv)


# ----------------------------------------------------------------- the retire marker

def terminal_retired(root, token: str, *, timeout: float = RETIRE_WAIT_SECONDS, poll: float = RETIRE_POLL_SECONDS,
                     clock=time.monotonic, sleep=time.sleep) -> bool:
    """Port of ``Test-BatonTerminalRetired``: only the marker of this invocation's own token, in phase ``retired``,
    retires the shell; phase ``waiting`` is polled until ``timeout``; anything else (no file, bad JSON, another
    token, another phase) is no. A consumed marker is removed."""
    if not isinstance(token, str) or not re.fullmatch(r"[0-9a-f]{32}", token):
        return False
    path = Path(root) / ".baton" / "terminal-retire" / (token + ".json")
    deadline = clock() + timeout
    while True:
        try:
            if not path.is_file():
                return False
            marker = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(marker, dict) or marker.get("token") != token:
                return False
            phase = marker.get("phase")
            if phase == "retired":
                try:
                    path.unlink()
                except OSError:
                    pass
                return True
            if phase != "waiting" or clock() >= deadline:
                return False
        except (OSError, ValueError):
            return False
        sleep(poll)


# ----------------------------------------------------------------- running the agent CLI

def build_env(plan, token: str, environ=None) -> dict:
    """The agent CLI's environment: ours plus the plan's ``env_set`` minus its ``env_unset``, plus the terminal
    token. The exit-file variable is never passed on."""
    env = dict(os.environ if environ is None else environ)
    norm = str.upper if os.name == "nt" else (lambda name: name)   # Windows names are case-insensitive
    for name, value in plan.env_set.items():
        env[norm(name)] = str(value)
    for name in plan.env_unset:
        env.pop(norm(name), None)
    env.pop(EXIT_FILE_ENV, None)
    env[TOKEN_ENV] = token
    return env


@contextlib.contextmanager
def _sigint_to_the_child():
    """While the child runs, Ctrl-C belongs to it. A no-op *handler* (not SIG_IGN) keeps this process alive, and
    is reset to the default in the child on exec; an ignored signal would be inherited and the CLI could not be
    interrupted. The same holds for the Windows console control handler."""
    try:
        previous = signal.signal(signal.SIGINT, lambda signum, frame: None)
    except ValueError:   # not the main thread
        yield
        return
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous)


def _exit_code(returncode: int) -> int:
    return 128 - returncode if returncode < 0 else returncode   # killed by signal N: the shell convention 128+N


def run_foreground(argv, cwd: str, env: dict) -> int:
    """Run ``argv`` in ``cwd`` as the terminal's foreground job (same process group, same session) and wait."""
    exe = shutil.which(argv[0], path=env.get("PATH")) or argv[0]
    try:
        with _sigint_to_the_child():
            child = subprocess.Popen([exe, *argv[1:]], cwd=cwd, env=env)
            while True:
                try:
                    return _exit_code(child.wait())
                except KeyboardInterrupt:   # a Python without PEP 475 retry semantics; the child got the signal itself
                    continue
    except FileNotFoundError:
        _say(f"bt: cannot run {argv[0]}: not found", "red", err=True)
        return 127
    except OSError as exc:
        _say(f"bt: cannot run {argv[0]}: {exc}", "red", err=True)
        return 126


def open_new_tab(plan, opt: LaunchOptions, title: str) -> bool:
    """Open the launch in a new tab of the terminal backend (iTerm2: the current window); False, after printing why,
    when it cannot, and the caller runs inline. The tab runs ``bt`` on the resolved worktree, not on the raw query:
    a relative or ambiguous query could resolve differently there (as ``Open-BatonTab`` of bt.ps1 does)."""
    be = terminal.backend()
    tab = terminal.Tab(cwd=str(plan.cwd), title=title, launch=terminal.Launch(bt=terminal.bt_args(
        str(plan.cwd), opt.vendor, opt.account, fresh=opt.fresh, prompt=opt.prompt, model=opt.model, switch=opt.switch,
        resume=opt.resume, force=opt.force, role=opt.role)))
    if isinstance(be, terminal.NoTerminal):
        _say(f"[baton] cannot open a new tab: {be.reason}", "yellow")
        return False
    if opt.dry_run:
        print(terminal.describe(be, [tab]))
        print(f"  (tab command: {be.command_text(tab)})")
        return True
    opened = be.open([tab])
    if not opened:
        _say(f"[baton] cannot open a new tab ({be.name}): {opened.error}", "yellow")
    return bool(opened)


# ----------------------------------------------------------------- the launch

def _plan_for(opt: LaunchOptions):
    """The same plan ``bt plan --json`` prints, computed in-process."""
    wt = launch.resolve_worktree(opt.query)
    if not opt.force:
        blocked = launch.live_owner_message(wt)   # a second session would be a second writer
        if blocked:
            raise LookupError(blocked)
    mode = "fresh" if opt.fresh else "resume" if opt.resume else "auto"
    # Dry runs and new tabs must not sync the second account here (the tab's own launch does).
    return launch.plan(wt, opt.vendor or None, opt.account or None, mode, role=opt.role or "lead", switch=opt.switch,
                       sync=not (opt.dry_run or opt.new_tab), prompt=opt.prompt or None, model=opt.model or None)


def invoke_launch(opt: LaunchOptions, *, retire_timeout: float = RETIRE_WAIT_SECONDS) -> tuple[int, bool]:
    """Returns (exit code, True when a confirmed context retirement says the shell should end)."""
    try:
        plan = _plan_for(opt)
    except (LookupError, ValueError) as exc:
        _say(f"bt: {exc}", err=True)
        return 2, False
    for line in str(plan.reason).split("\n"):
        if line:
            _say(f"[baton] {line}", "gray")
    if opt.new_tab:
        if open_new_tab(plan, opt, Path(plan.cwd).name):
            return 0, False
        _say("[baton] running inline", "yellow")
    argv = [str(a) for a in plan.argv]
    if opt.dry_run:
        print(f"cwd:   {plan.cwd}")
        for name, value in plan.env_set.items():
            print(f"set:   {name}={value}")
        for name in plan.env_unset:
            print(f"unset: {name}")
        print(f"run:   {format_command(argv)}")
        return 0, False
    token = uuid.uuid4().hex
    code = run_foreground(argv, str(plan.cwd), build_env(plan, token))
    if terminal_retired(plan.cwd, token, timeout=retire_timeout):
        return 0, True   # a confirmed retirement is a success even when the old CLI was killed
    return code, False


# ----------------------------------------------------------------- picker and dispatch

def in_worktree(start=None) -> bool:
    """True when ``start`` (default: the current directory) is inside a git worktree (a .git file or directory)."""
    here = Path(start or os.getcwd()).resolve()
    return any((d / ".git").exists() for d in (here, *here.parents))


def _list_output(tsv: bool = False) -> str:
    """What `bt list` (or `bt list --tsv`) prints."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        launch._cmd_list(argparse.Namespace(json=False, tsv=tsv, all=False))
    return buf.getvalue()


def picker() -> tuple[int, bool]:
    """``bt`` with no arguments outside a worktree: choose from `bt list` and start `bt <key>`."""
    rows = [r for r in _list_output(tsv=True).splitlines() if r]
    if not rows:
        _say("[baton] no worktrees found under the code root", "yellow")
        return 0, False
    fzf = shutil.which("fzf")
    if not fzf:
        sys.stdout.write(_list_output())
        return 0, False
    with _sigint_to_the_child():
        cp = subprocess.run([fzf, "--delimiter", "\t", "--with-nth", "1,2,3", "--prompt", "Start agent > ",
                             "--header", "Most recent first. Enter starts an agent with baton (bt <worktree>)."],
                            input="\n".join(rows) + "\n", stdout=subprocess.PIPE, text=True, encoding="utf-8")
    choice = cp.stdout.strip("\r\n")
    if cp.returncode != 0 or not choice:
        return 0, False
    return invoke([choice.split("\t", 1)[0]])


def invoke(tokens, *, retire_timeout: float = RETIRE_WAIT_SECONDS) -> tuple[int, bool]:
    """The ``bt`` function of bt.ps1. Returns (exit code, shell should end)."""
    tokens = [str(t) for t in tokens]
    if not tokens and not in_worktree():
        return picker()
    first = tokens[0] if tokens else ""
    if first and first.lower() in subcommand_names():
        from .cli import main
        return main([first.lower(), *tokens[1:]]), False
    if first in HELP_FLAGS:
        from .cli import main
        return main(["--help"]), False
    is_switch = first.lower() == "switch"
    try:
        opt = parse_options(tokens[1:] if is_switch else tokens)
    except OptionError as exc:
        _say(f"bt: {exc}", "red", err=True)
        return 2, False
    opt.switch = is_switch
    return invoke_launch(opt, retire_timeout=retire_timeout)


WINDOWS_REFUSAL = ("bt: on Windows the launcher is the PowerShell `bt` function (bt.ps1, which also ends the shell after a "
                   "context retirement); `bt.py shell` is the engine of bt.sh for zsh and bash")


def entry_refusal(argv, environ=None, windows: bool | None = None) -> str | None:
    """Why the process entry (``bt.py``) must not run ``bt.py shell ...``: on Windows it would start the POSIX launcher
    path without the retire-and-exit of bt.ps1. None otherwise, and whenever bt.sh's exit-file variable is set (what
    the POSIX launcher always passes). Checked by ``bt.py`` itself, not by ``run_tokens``, which tests call directly."""
    from . import config
    environ = os.environ if environ is None else environ
    if (config.WINDOWS if windows is None else windows) and argv[:1] == ["shell"] and not environ.get(EXIT_FILE_ENV):
        return WINDOWS_REFUSAL
    return None


def run_tokens(tokens, *, retire_timeout: float = RETIRE_WAIT_SECONDS) -> int:
    """Entry point of ``bt.py shell <tokens>``: the exit code of the ``bt`` command."""
    exit_file = os.environ.pop(EXIT_FILE_ENV, None)   # not for the CLI, nor for any subcommand's children
    try:
        code, retired = invoke(tokens, retire_timeout=retire_timeout)
    except KeyboardInterrupt:
        return 130
    if retired and exit_file:
        try:
            Path(exit_file).write_text(RETIRED_WORD, encoding="ascii")
        except OSError:
            pass
    return code


def register(sub) -> None:
    global _SUBCOMMANDS
    _SUBCOMMANDS = sub.choices
    # cli.main hands `bt shell ...` to run_tokens before argparse sees it (the launch options start with "-");
    # this parser only makes the name known to --help and to the bt.ps1 subcommand-list test.
    p = sub.add_parser("shell", help="the bt command of bt.sh (zsh, bash); not for direct use", add_help=False)
    p.set_defaults(func=lambda args: run_tokens([]))
