"""Everything iTerm2-specific that is not opening a tab (that is ``terminal.ITERM_SCRIPT``).

* Baton's own profile: a dynamic profile ``bt install`` writes to iTerm2's ``DynamicProfiles`` folder: what Baton needs
  (close with the session, Baton's titles, notifications) and, unless ``[policy] iterm_look = "inherit"``, a look (font,
  colours, badge, keys). Every other attribute is inherited from the user's default profile.
* Tab state: a few escape sequences the hooks write to the session's tty (``BATON_TTY``, recorded by ``bt <ws>``): badge
  and tab title with the workstream name always. Then one of two modes (``tab_mode``). *status* (iTerm2 3.7.5 or later):
  the tab colour names the repository, and the session's status is the Program Status Protocol's (OSC 7501), which iTerm2
  shows on the tab: Claude Code reports it itself, and Baton reports it for Codex, which does not. *classic* (older
  iTerm2): the tab colour is the state (green: ready, amber: needs input) with a notification, for both agents.
* ``bt iterm prefs``: the few application-wide settings that keep many agent tabs readable, as ``defaults write`` lines.
* Jump to the tab of a workstream that is already open (``SELECT_SCRIPT``) and open an image pane (``bt img``,
  ``SPLIT_SCRIPT`` and the ``--draw`` side).

Stdlib plus ``config`` only at import time (``shlex``, ``shutil``, ``subprocess`` and ``terminal`` are imported where
they are used), so a hook (``python -S``) can import it cheaply. Every action that needs a Mac sits behind a small function the tests replace.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import zlib
from pathlib import Path

from . import config

ITERM_BUNDLE_ID = "com.googlecode.iterm2"      # iTerm2 is always named by this id (see terminal.ITERM_SCRIPT)
PROFILE_SETTING = "iterm_profile"              # [policy]: the profile Baton's tabs use; "" = the user's default profile
PROFILE_DEFAULT = "Baton"
TAB_STATE_SETTING = "iterm_tab_state"          # [policy]: "on" (status or classic by iTerm2's version), "status", "classic", "off"
LOOK_SETTING = "iterm_look"                    # [policy]: "baton" (dark), "auto" (follows the Mac's light/dark mode), "inherit"
LOOK_DEFAULT = "baton"
FONT_SETTING = "iterm_font"                    # [policy]: "<PostScript name> <size>"; "" = FONT_DEFAULT
FONT_DEFAULT = "Menlo-Regular 13"              # on every Mac
COLORS_TABLE = "iterm_colors"                  # [policy.iterm_colors]: repository name -> "#rrggbb" tab colour (status mode)
STATUS_MIN_VERSION = (3, 7, 5)                 # the first iTerm2 that shows the Program Status Protocol (OSC 7501) on a tab
SELF_REPORTING = ("claude",)                   # agents that report OSC 7501 themselves (Claude Code 2.1.295 or later)
PROFILE_GUID = "E6C9C545-9A68-4673-B6A2-AA55A2C96E5D"
SHELL_PROFILE = "Baton Shell"                  # the look for ordinary tabs (titles left to the shell); iTerm2's default
SHELL_GUID = "0B5D7A2E-6C1F-4E8B-9A3D-7F2C4B1E9D60"
PROFILE_FILE = "baton.json"

SESSION_VAR = "BATON_ITERM_SESSION"            # the ITERM_SESSION_ID of the session `bt <ws>` started the agent in
TTY_VAR = "BATON_TTY"                          # that session's tty, which the hooks write escape sequences to
TTY_DIR = "/dev"                               # a tty path must resolve to a terminal device here (tests move it to a temp dir)
LABEL_MAX = 80
IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp", ".tif", ".tiff")
MAX_IMAGE_BYTES = 30 * 1024 * 1024


# ------------------------------------------------------------------ the profile

def _rgb(text) -> tuple[int, int, int] | None:
    """``#rrggbb`` as three ints, None for anything else."""
    text = str(text).strip()
    if len(text) != 7 or text[0] != "#" or any(ch not in "0123456789abcdefABCDEF" for ch in text[1:]):
        return None
    return int(text[1:3], 16), int(text[3:5], 16), int(text[5:7], 16)


def _color(text: str, alpha: float = 1.0) -> dict:
    """A profile colour: the dictionary iTerm2 stores for ``#rrggbb``."""
    red, green, blue = _rgb(text)
    return {"Red Component": round(red / 255, 4), "Green Component": round(green / 255, 4), "Blue Component": round(blue / 255, 4),
            "Alpha Component": alpha, "Color Space": "sRGB"}


# Two palettes: [0] the dark one, [1] the light one (modelled on GitHub's light theme). The dark one is an ink
# background with soft, evenly bright colours: against #0e1117 every text colour has a WCAG contrast of at least 5:1
# (bright black, the "dim" colour many tools use, 5.3:1; the foreground 13.7:1), so dim output stays readable without
# any colour shouting. "White" is a grey in the light palette, so text printed in ANSI white stays readable on white.
_ANSI = (("#30363f", "#24292f"), ("#f47c7c", "#cf222e"), ("#7ed69a", "#116329"), ("#e8c57a", "#4d2d00"),
         ("#72aaf5", "#0969da"), ("#c49df5", "#8250df"), ("#5fd0cf", "#1b7c83"), ("#c3cad5", "#6e7781"),
         ("#7f8896", "#57606a"), ("#ff9e9e", "#a40e26"), ("#a3e8b6", "#1a7f37"), ("#f5d898", "#633c01"),
         ("#98c1ff", "#218bff"), ("#dabbff", "#a475f9"), ("#8fe3e1", "#3192aa"), ("#f3f5f8", "#8c959f"))
_NAMED = {"Background Color": ("#0e1117", "#ffffff"), "Foreground Color": ("#d7dce4", "#1f2328"),
          "Bold Color": ("#f3f5f8", "#1f2328"), "Cursor Color": ("#f2a65a", "#0969da"),
          "Cursor Text Color": ("#0e1117", "#ffffff"), "Selection Color": ("#28344a", "#b6e3ff"),
          "Selected Text Color": ("#f3f5f8", "#1f2328"), "Link Color": ("#7ab4ff", "#0969da")}
_BADGE = ("#8b949e", "#57606a")
_BADGE_ALPHA = 0.16          # a watermark: the tab title and the status bar carry the name; the badge only marks the pane
_FAINT_ALPHA = 0.72          # faint (SGR 2) text, which the agents use for secondary lines: iTerm2's 0.5 is too dim


def look_name() -> str:
    """``[policy] iterm_look``: "baton", "auto" or "inherit" (anything else counts as "baton")."""
    value = config.policy_text(LOOK_SETTING, LOOK_DEFAULT).lower()
    return value if value in ("baton", "auto", "inherit") else LOOK_DEFAULT


def look_entry(look: str) -> dict:
    """The look's profile attributes. "baton": the dark palette whatever the Mac's appearance (the agents' own themes
    are dark by default). "auto": both palettes, and iTerm2 follows the Mac's light or dark mode. Also here because a
    new Mac's default profile lacks it: the left Option key sends Esc+ (the agents' Option shortcuts need it) while
    the right one stays a normal Option key, so characters typed with Option (# on a British keyboard) can be typed."""
    font = config.policy_text(FONT_SETTING, "") or FONT_DEFAULT
    entry = {
        "Normal Font": font, "Use Non-ASCII Font": False, "Horizontal Spacing": 1, "Vertical Spacing": 1.1,
        "Use Bold Font": True, "Use Italic Font": True, "ASCII Ligatures": False, "Blinking Cursor": False,
        "Columns": 132, "Rows": 40, "Scrollback Lines": 20000, "Unlimited Scrollback": False,
        "Option Key Sends": 2, "Right Option Key Sends": 0,           # 0 normal, 1 meta, 2 Esc+
        "Silence Bell": True, "Visual Bell": False, "Flashing Bell": False,
        "Minimum Contrast": 0.12, "Transparency": 0, "Faint Text Alpha": _FAINT_ALPHA, "Cursor Type": 2,
        "Badge Max Width": 0.24, "Badge Max Height": 0.055, "Badge Top Margin": 12, "Badge Right Margin": 18,
        "Use Separate Colors for Light and Dark Mode": look == "auto",
    }
    colors = dict(_NAMED, **{f"Ansi {i} Color": pair for i, pair in enumerate(_ANSI)})
    for key, (dark, light) in colors.items():
        entry[key] = _color(dark)
        if look == "auto":
            entry[key + " (Dark)"] = _color(dark)
            entry[key + " (Light)"] = _color(light)
    entry["Badge Color"] = _color(_BADGE[0], _BADGE_ALPHA)
    entry["Show Status Bar"] = True
    if look == "auto":
        entry["Badge Color (Dark)"] = _color(_BADGE[0], _BADGE_ALPHA)
        entry["Badge Color (Light)"] = _color(_BADGE[1], _BADGE_ALPHA)
        entry["Faint Text Alpha (Dark)"] = entry["Faint Text Alpha (Light)"] = _FAINT_ALPHA
    return entry


# The tab's title: the workstream Baton named the session after (user.baton_ws, set at launch and by the hooks, cleared
# when the agent ends). Tab titles are evaluated in the tab's context, hence currentSession. While the variable is unset
# or empty, iTerm2 falls back to the tab's ordinary title (checked with iTerm2 3.7.8; its ternary operator fails on an
# unset variable, so there is no expression-level fallback). It also keeps iTerm2's Claude Code workgroup, which renames
# its sessions "Chat", from hiding which workstream a tab is.
TAB_TITLE = r"\(currentSession.user.baton_ws)"
# iTerm2 3.7's Claude Code workgroup (Settings > Workgroups): a session running `claude` gets the Chat / Diff / Code Review
# toolbar. iTerm2 adds these triggers to the profiles it knows when the workgroup is installed, and a dynamic profile loses
# them whenever Baton rewrites its file, so Baton's profiles carry them. Older iTerm2 ignores an unknown trigger.
WORKGROUP_TRIGGERS = [
    {"action": "iTermEnterWorkgroupTrigger", "matchType": 113, "parameter": "builtin.claudeCode", "partial": False,
     "eventParams": {"jobName": "claude"}, "contentregex": "", "disabled": False, "regex": ""},
    {"action": "iTermExitWorkgroupTrigger", "matchType": 114, "parameter": "", "partial": False,
     "eventParams": {"jobName": "claude", "leaderOnly": True}, "contentregex": "", "disabled": False, "regex": ""},
]

# The status bar (Settings > Profiles > Session), at the foot of the pane (StatusBarPosition in `bt iterm prefs`): who the
# pane is, its git state, and the machine's CPU and memory, so a runaway process shows before the fans do. iTerm2's system
# font at 11 pt sets it apart from the terminal text; components with nothing to show are dropped.
_BAR = {"background": "#0a0d12", "text": "#8f99a8", "strong": "#e3e7ee", "separator": "#1f2530",
        "cpu": "#72aaf5", "memory": "#c49df5", "git": "#7ed69a", "path": "#c3cad5"}


def _bar_advanced() -> dict:
    return {"algorithm": 0, "remove empty components": True, "font": ".AppleSystemUIFont 11", "auto-rainbow style": 0,
            "background color": _color(_BAR["background"]), "default text color": _color(_BAR["text"]),
            "separator color": _color(_BAR["separator"]), "draw separator between status bar and terminal": True}


def _bar_component(cls: str, priority: int, color: str | None = None, **knobs) -> dict:
    values = {"base: priority": priority, "base: compression resistance": 1, **knobs}
    if color:
        values["shared text color"] = _color(_BAR[color])
    return {"class": "iTermStatusBar" + cls + "Component",
            "configuration": {"knobs": values, "layout advanced configuration dictionary value": _bar_advanced()}}


def status_bar(agent: bool) -> dict:
    """The status bar layout: Baton's tabs lead with the workstream and the agent (user variables the hooks set); an
    ordinary tab with its directory. Both then show git and the CPU and memory graphs."""
    lead = ([_bar_component("SwiftyString", 9, "strong", expression=r"\(user.baton_ws)"),
             _bar_component("SwiftyString", 6, None, expression=r"\(user.baton_agent)")] if agent else
            [_bar_component("WorkingDirectory", 8, "path")])
    return {"components": lead + [_bar_component("Git", 7, "git"), _bar_component("CPUUtilization", 5, "cpu"),
                                  _bar_component("MemoryUtilization", 5, "memory")],
            "advanced configuration": _bar_advanced()}


def profile_entry(name: str, look: str | None = None) -> dict:
    """The dynamic profile: close the tab when its session ends, let Baton (not the agent) title it, post the
    notifications the hooks ask for; then the look (``look_entry``) unless it is "inherit". Everything not set here
    comes from the user's default profile."""
    entry = {
        "Name": name,
        "Guid": PROFILE_GUID,
        "Close Sessions On End": 1,         # iTermSessionEndAction: 1 = close
        "Allow Title Setting": False,
        "Title Components": 1,              # Session Name only
        "BM Growl": True,
        "Send Terminal Generated Alerts": True,
        "Tags": ["baton"],
        "Use Custom Tab Title": True,
        "Custom Tab Title": TAB_TITLE,
        "Triggers": WORKGROUP_TRIGGERS,
    }
    look = look_name() if look is None else look
    if look != "inherit":
        entry.update(look_entry(look))
        entry["Status Bar Layout"] = status_bar(agent=True)
    return entry


def shell_profile_entry(look: str) -> dict:
    """``Baton Shell``: the same look for ordinary tabs, which keep iTerm2's own title and end-of-session behaviour.
    ``bt iterm prefs`` makes it iTerm2's default profile, so a new window or tab (Cmd-T) looks like Baton's tabs."""
    entry = {"Name": SHELL_PROFILE, "Guid": SHELL_GUID, "Tags": ["baton"], "Use Custom Tab Title": True,
             "Custom Tab Title": TAB_TITLE, "Triggers": WORKGROUP_TRIGGERS}
    entry.update(look_entry(look))
    entry["Status Bar Layout"] = status_bar(agent=False)
    return entry


def profile_document(name: str) -> dict:
    """The file's content: ``{"Profiles": [<profile>, <shell profile>]}``; the shell profile only with a look of Baton's
    ("inherit" leaves the look, and so ordinary tabs, to the user's own profiles)."""
    look = look_name()
    profiles = [profile_entry(name, look)]
    if look != "inherit":
        profiles.append(shell_profile_entry(look))
    return {"Profiles": profiles}


def profile_text(name: str) -> str:
    """The file as ``bt install`` writes it: JSON, sorted keys, two-space indent, trailing newline."""
    return json.dumps(profile_document(name), indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def profiles_dir(home=None) -> Path:
    return Path(home or Path.home()) / "Library" / "Application Support" / "iTerm2" / "DynamicProfiles"


def profile_path(home=None) -> Path:
    return profiles_dir(home) / PROFILE_FILE


def profile_name() -> str:
    """``[policy] iterm_profile``: the name of the profile Baton's tabs use ("" = the user's default profile)."""
    return config.policy_text(PROFILE_SETTING, PROFILE_DEFAULT)


def profile_problem(path: Path, name: str | None = None) -> str | None:
    """Why the dynamic profile file at ``path`` does not give iTerm2 Baton's profile (the one with Baton's Guid, and
    called ``name`` when a name is given); None when it does."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return f"{path} does not exist"
    except (OSError, ValueError) as exc:
        return f"{path} cannot be read as JSON ({exc})"
    profiles = data.get("Profiles") if isinstance(data, dict) else None
    ours = [p for p in (profiles if isinstance(profiles, list) else ()) if isinstance(p, dict) and p.get("Guid") == PROFILE_GUID]
    if not ours:
        return f"{path} has no profile with Baton's Guid {PROFILE_GUID}"
    if name is not None and ours[0].get("Name") != name:
        return f"{path} names its profile {ours[0].get('Name')!r}, not {name!r} ([policy] iterm_profile)"
    return None


# ------------------------------------------------------------------ tab state (escape sequences)

def osc(body: str) -> str:
    """``ESC ] <body> BEL``."""
    return f"\x1b]{body}\x07"


def _b64(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def sanitise(text, limit: int = LABEL_MAX) -> str:
    """``text`` without any control character (below 0x20, 0x7f, the C1 range: so never a BEL, ESC or newline that could
    end or start a sequence), trimmed and cut at ``limit`` characters."""
    kept = "".join(ch for ch in str(text) if ch >= " " and not "\x7f" <= ch <= "\x9f")
    return kept.strip()[:limit].rstrip()


def badge(text: str) -> str:
    return osc("1337;SetBadgeFormat=" + _b64(text))


def user_var(name: str, value: str) -> str:
    return osc(f"1337;SetUserVar={name}=" + _b64(value))


def title(text: str) -> str:
    return osc("1;" + text)


def tab_color(red: int, green: int, blue: int) -> str:
    return "".join(osc(f"6;1;bg;{channel};brightness;{value}")
                   for channel, value in (("red", red), ("green", green), ("blue", blue)))


COLOR_RESET = osc("6;1;bg;*;default")
GREEN = (60, 160, 90)        # classic mode: ready for you
AMBER = (220, 130, 40)       # classic mode: needs your input
# status mode: a tab's colour names its repository (a stable choice from these, or [policy.iterm_colors] in accounts.toml).
# Deep, muted tones: in the Minimal theme the selected tab's colour also tints the window, and the tab's title and status
# line are drawn in white on it (contrast 7.7:1 or more): indigo, teal, plum, forest, wine, bronze, steel, slate.
PALETTE = ((44, 61, 115), (28, 85, 89), (85, 48, 107), (44, 86, 54), (106, 44, 55), (102, 80, 31),
           (36, 80, 122), (58, 65, 78))


def notify(message: str) -> str:
    return osc("9;" + message)


def attention(mode: str) -> str:
    """``RequestAttention``: yes (bounce until seen), once, no, fireworks."""
    return osc("1337;RequestAttention=" + mode)


def repo_color(repo: str) -> tuple[int, int, int] | None:
    """The tab colour of ``repo``: ``[policy.iterm_colors] <repo> = "#rrggbb"`` when given and valid, else a stable choice
    from PALETTE; None for no repository."""
    if not repo:
        return None
    try:
        table = config.accounts_config().get("policy", {}).get(COLORS_TABLE)
        chosen = _rgb(table.get(repo)) if isinstance(table, dict) and isinstance(table.get(repo), str) else None
    except Exception:  # noqa: BLE001 - a broken accounts.toml must not break a hook
        chosen = None
    return chosen or PALETTE[zlib.crc32(repo.encode("utf-8")) % len(PALETTE)]


STATUS_STATES = ("idle", "working", "done", "blocked", "error", "clear")
MSG_MAX = 120


def status_report(state: str, app: str = "", kind: str = "", msg: str = "") -> str:
    """One report of the Program Status Protocol: ``ESC ] 7501 ; state=<state>[:kind=...][:app=...][:msg=<base64>] BEL``
    for the terminal's root record. ``kind`` (permission, question, auth) goes with ``blocked`` only; ``app`` is kept
    when it is a legal identifier; ``msg`` is one sanitised line. A terminal that does not know the sequence ignores it."""
    if state not in STATUS_STATES:
        raise ValueError(f"not a program status: {state!r}")
    pairs = ["state=" + state]
    if state == "blocked" and kind in ("permission", "question", "auth"):
        pairs.append("kind=" + kind)
    if app and len(app) <= 32 and all(ch.isascii() and (ch.isalnum() or ch in "_.+-") for ch in app):
        pairs.append("app=" + app)
    msg = sanitise(msg, MSG_MAX)
    if msg and state != "clear":
        pairs.append("msg=" + _b64(msg))
    return osc("7501;" + ":".join(pairs))


def version_tuple(text) -> tuple[int, ...]:
    """The leading dotted numbers of a version string: "3.7.7" is (3, 7, 7), "3.7.0beta3" is (3, 7, 0), "" is ()."""
    out: list[int] = []
    for part in str(text or "").strip().split("."):
        digits = ""
        for ch in part:
            if not (ch.isascii() and ch.isdigit()):
                break
            digits += ch
        if not digits:
            break
        out.append(int(digits))
        if len(digits) != len(part):
            break
    return tuple(out)


def tab_mode(environ=None) -> str:
    """"off", "classic" or "status" from ``[policy] iterm_tab_state``: "off"/false; "classic"; "status"; anything else
    ("on", the default) is "status" when ``TERM_PROGRAM_VERSION`` names iTerm2 3.7.5 or later and "classic" otherwise."""
    if not config.policy_on(TAB_STATE_SETTING, True):
        return "off"
    value = config.policy_text(TAB_STATE_SETTING, "on").lower()
    if value in ("classic", "status"):
        return value
    environ = os.environ if environ is None else environ
    return "status" if version_tuple(environ.get("TERM_PROGRAM_VERSION")) >= STATUS_MIN_VERSION else "classic"


STATES = ("start", "working", "ready", "input", "idle", "end")


def state_sequences(state: str, label: str, mode: str = "classic", vendor: str = "", repo: str = "", detail: str = "") -> str:
    """The escape sequences of one tab state, concatenated (``label`` and ``detail``: sanitised here); "" when the
    state needs none. *classic*: the tab colour is the state. *status*: the tab colour is the repository's, and the
    state is a Program Status report, sent only for an agent that does not report for itself (``SELF_REPORTING``),
    with a notification when it finished or needs the user."""
    if state not in STATES:
        raise ValueError(f"not a tab state: {state!r}")
    label = sanitise(label) or "baton"
    identity = badge(label) + user_var("baton_ws", label) + title(label)
    if mode != "status":
        if state == "start":
            return identity + COLOR_RESET
        if state in ("working", "idle"):
            return COLOR_RESET + attention("no")
        if state == "ready":
            return tab_color(*GREEN) + notify(sanitise(f"{label}: ready for you")) + attention("once")
        if state == "input":
            return tab_color(*AMBER) + notify(sanitise(f"{label} needs your input")) + attention("once")
        return COLOR_RESET + badge("") + user_var("baton_ws", "") + attention("no")
    reports = bool(vendor) and vendor not in SELF_REPORTING
    if state == "start":                     # no report: a session also starts again in the middle of a turn (compaction)
        color = repo_color(sanitise(repo))
        agent = user_var("baton_agent", sanitise(vendor, 32)) if vendor else ""
        return identity + agent + (tab_color(*color) if color else COLOR_RESET)
    if state == "end":                       # the tab's title and status bar fall back to the shell's
        return (COLOR_RESET + badge("") + user_var("baton_ws", "") + user_var("baton_agent", "")
                + (status_report("clear") if reports else ""))
    if not reports:
        return ""
    if state == "working":
        return status_report("working", vendor)
    if state == "idle":
        return status_report("idle", vendor)
    if state == "ready":
        return status_report("done", vendor, msg="ready for you") + notify(sanitise(f"{label}: ready for you"))
    return (status_report("blocked", vendor, kind="permission", msg=detail or "needs your input")
            + notify(sanitise(f"{label} needs your input")))


def label_for(environ=None, fallback: str | os.PathLike = "") -> str:
    """The workstream name of this session: ``BATON_SESSION_NAME``, else the folder name of ``fallback``."""
    environ = os.environ if environ is None else environ
    return str(environ.get("BATON_SESSION_NAME") or Path(str(fallback)).name or "baton")


def tty_path(path) -> str | None:
    """``path`` resolved (``os.path.realpath``) when it is a terminal device: ``/dev/ttys*``, ``/dev/tty<letter or digit>*``
    or ``/dev/pts/*``; None otherwise, so ``/dev/stdout``, ``/dev/fd/1``, ``/dev/../x`` and a link to a file elsewhere
    are never written to."""
    real = os.path.realpath(str(path))
    folder, name = os.path.split(real)
    folder = os.path.normcase(folder)
    if folder == os.path.normcase(TTY_DIR):
        ok = len(name) > 3 and name.startswith("tty") and name[3].isascii() and name[3].isalnum()
    else:
        ok = folder == os.path.normcase(os.path.join(TTY_DIR, "pts")) and bool(name)
    return real if ok else None


def tab_state(state: str, label: str | None = None, environ=None, *, vendor: str = "", repo: str = "", detail: str = "") -> bool:
    """Write ``state`` to the tty named by ``BATON_TTY`` in one ``os.write``; True when all of it was written. Nothing
    happens (and nothing is raised) when the variable is unset, does not resolve to a terminal device (``tty_path``),
    the policy says off, the state needs no sequence in this mode (``tab_mode``, ``state_sequences``), the opened file
    is not a terminal or the write fails: a tab's colour is never worth a failed hook. A write that took only part of
    the bytes is closed with one BEL, so an unterminated sequence cannot swallow what the terminal prints next."""
    try:
        environ = os.environ if environ is None else environ
        tty = tty_path(environ[TTY_VAR]) if environ.get(TTY_VAR) else None
        mode = tab_mode(environ) if tty is not None else "off"
        if mode == "off":
            return False
        data = state_sequences(state, label if label is not None else label_for(environ), mode, vendor, repo, detail).encode("utf-8")
        if not data:
            return False
        flags = os.O_WRONLY | getattr(os, "O_NOCTTY", 0) | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)
        fd = os.open(tty, flags)
        try:
            if not os.isatty(fd):
                return False
            if os.write(fd, data) < len(data):
                os.write(fd, b"\x07")
                return False
        finally:
            os.close(fd)
        return True
    except Exception:  # noqa: BLE001 - OSError and anything stranger: never break the caller
        return False


def session_env(environ=None, isatty=None, ttyname=None) -> dict:
    """``BATON_ITERM_SESSION`` and ``BATON_TTY`` for a session started from an iTerm2 tab with a terminal on stdout or
    stdin; ``{}`` otherwise (the agent then runs exactly as before). Both ``TERM_PROGRAM=iTerm.app`` and
    ``ITERM_SESSION_ID`` are required: the second alone is inherited by any program started from an iTerm2 shell (VS
    Code's terminal, say), whose tty is not that tab."""
    environ = os.environ if environ is None else environ
    isatty = isatty or os.isatty
    sid = environ.get("ITERM_SESSION_ID") or ""
    if not sid or environ.get("TERM_PROGRAM") != "iTerm.app":
        return {}
    ttyname = ttyname or getattr(os, "ttyname", None)
    if ttyname is None:
        return {}
    for fd in (1, 0):
        try:
            if isatty(fd):
                return {SESSION_VAR: sid, TTY_VAR: ttyname(fd)}
        except (OSError, ValueError):
            continue
    return {}


def safe_tab_state(state: str, label: str | None = None, environ=None, **about) -> None:
    """``tab_state`` for call sites that must not know about it: swallows everything, returns nothing."""
    try:
        tab_state(state, label, environ, **about)
    except Exception:  # noqa: BLE001
        pass


# ------------------------------------------------------------------ finding a session

def session_candidates(value: str) -> tuple[str, str]:
    """The two forms a stored session id may take in AppleScript: the whole ``ITERM_SESSION_ID`` (``w0t0p0:<UUID>``) and
    the part after the last colon (the UUID); equal when there is no colon."""
    whole = str(value).strip()
    return whole, whole.rpartition(":")[2]


# argv: the two candidates. Result: "selected" (session, tab and window selected, iTerm2 in front) or "missing". The loop
# variables are by-index references and selecting a window reorders the windows, so the three objects are resolved
# (`contents of`) first and selected innermost first.
SELECT_SCRIPT = (
    "on run argv",
    "set wantA to item 1 of argv",
    "set wantB to item 2 of argv",
    f'tell application id "{ITERM_BUNDLE_ID}"',
    "repeat with theWindow in windows",
    "repeat with theTab in tabs of theWindow",
    "repeat with theSession in sessions of theTab",
    "set theId to (id of theSession)",
    "set theUnique to (unique id of theSession)",
    "if theId is wantA or theId is wantB or theUnique is wantA or theUnique is wantB then",
    "set winRef to contents of theWindow",
    "set tabRef to contents of theTab",
    "set sesRef to contents of theSession",
    "select sesRef",
    "select tabRef",
    "select winRef",
    "activate",
    'return "selected"',
    "end if",
    "end repeat",
    "end repeat",
    "end repeat",
    "end tell",
    'return "missing"',
    "end run",
)

# argv: the two candidates, then the command. The session is found as in SELECT_SCRIPT, else the current session of the
# current window is used; the new pane (same profile, beside it) runs the command.
SPLIT_SCRIPT = (
    "on run argv",
    "set wantA to item 1 of argv",
    "set wantB to item 2 of argv",
    "set theCommand to item 3 of argv",
    f'tell application id "{ITERM_BUNDLE_ID}"',
    "set theTarget to missing value",
    "repeat with theWindow in windows",
    "repeat with theTab in tabs of theWindow",
    "repeat with theSession in sessions of theTab",
    "set theId to (id of theSession)",
    "set theUnique to (unique id of theSession)",
    "if theId is wantA or theId is wantB or theUnique is wantA or theUnique is wantB then set theTarget to contents of theSession",
    "end repeat",
    "end repeat",
    "end repeat",
    "if theTarget is missing value then set theTarget to current session of current window",
    "tell theTarget to split vertically with same profile command theCommand",
    "end tell",
    'return "split"',
    "end run",
)


def select_session(value: str, run=None) -> tuple[bool, str | None]:
    """(selected, error): select the iTerm2 tab holding session ``value``. ``(False, None)`` when no session has that id
    (the tab was closed); ``(False, text)`` when osascript failed. Never raises."""
    from . import terminal
    run = terminal.run_osascript if run is None else run
    error, lines = run(SELECT_SCRIPT, list(session_candidates(value)))
    if error:
        return False, error
    return "selected" in lines, None


# ------------------------------------------------------------------ images

def inline_image(name: str, data: bytes, width: str = "auto", height: str = "auto") -> bytes:
    """iTerm2's inline image sequence for ``data`` (``ESC ] 1337 ; File=...:<base64> BEL``, no spaces)."""
    head = (f"\x1b]1337;File=name={_b64(name)};size={len(data)};inline=1;width={width};height={height};"
            "preserveAspectRatio=1:")
    return head.encode("ascii") + base64.b64encode(data) + b"\x07"


def check_images(paths) -> tuple[list[str], str | None]:
    """(absolute paths, error): every path must be an existing file with an image extension."""
    out = []
    for raw in paths:
        full = os.path.abspath(os.path.expanduser(str(raw)))
        if not os.path.isfile(full):
            return [], f"{raw}: no such file"
        if os.path.splitext(full)[1].lower() not in IMAGE_EXTENSIONS:
            return [], f"{raw}: not an image bt img knows ({', '.join(e.lstrip('.') for e in IMAGE_EXTENSIONS)})"
        out.append(full)
    return out, None


def draw_command(paths, close_after: float | None = None) -> str:
    """The command text of the pane ``SPLIT_SCRIPT`` opens: ``/bin/zsh -lc '<python> -S <bt.py> img --draw <paths>'``."""
    import shlex
    argv = [str(config.PYTHON), "-S", str(config.INSTALL_HOME / "bt.py"), "img", "--draw"]
    if close_after is not None:
        argv += ["--close-after", f"{close_after:g}"]
    return shlex.join(["/bin/zsh", "-lc", shlex.join(argv + [str(p) for p in paths])])


def rows_for_one_image(size=None) -> int:
    """Height in terminal rows for a single image: the terminal's height minus 3 (at least 1)."""
    import shutil
    return max(1, (size or shutil.get_terminal_size()).lines - 3)


def draw(paths, out=None, rows: int | None = None) -> int:
    """Write each image (its name, then the inline image sequence and a newline) to ``out`` (default: stdout's bytes).
    A file over ``MAX_IMAGE_BYTES`` is skipped with a message. Returns the number drawn."""
    out = sys.stdout.buffer if out is None else out
    height = str(rows if rows is not None else rows_for_one_image()) if len(paths) == 1 else "auto"
    drawn = 0
    for path in paths:
        name = sanitise(os.path.basename(str(path)), 200)       # a file name can hold a BEL or an ESC: never print it raw
        try:
            size = os.path.getsize(path)
            if size > MAX_IMAGE_BYTES:
                out.write(f"{name}: {size / 1048576:.1f} MB is over the {MAX_IMAGE_BYTES // 1048576} MB limit; not drawn\n".encode("utf-8"))
                continue
            with open(path, "rb") as fh:
                data = fh.read()
        except OSError as exc:
            out.write(f"{name}: cannot read it: {sanitise(exc, 200)}\n".encode("utf-8"))
            continue
        out.write(name.encode("utf-8") + b"\n" + inline_image(name, data, "auto", height) + b"\n")
        drawn += 1
    out.flush()
    return drawn


def _wait_key_posix(timeout: float | None, fd: int = 0) -> bool:
    """Wait for one key press on ``fd`` in raw mode (or ``timeout`` seconds); True when a key was pressed."""
    import select
    import termios
    import tty
    saved = termios.tcgetattr(fd)
    try:
        tty.setraw(fd, termios.TCSAFLUSH)   # keys typed before this moment are discarded on purpose
        ready, _, _ = select.select([fd], [], [], timeout)
        if ready:
            os.read(fd, 1)
        return bool(ready)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, saved)


def _wait_key_windows(timeout: float | None, msvcrt=None, clock=time.monotonic, sleep=time.sleep) -> bool:
    if msvcrt is None:
        import msvcrt
    deadline = None if timeout is None else clock() + timeout
    while deadline is None or clock() < deadline:
        if msvcrt.kbhit():
            msvcrt.getwch()
            return True
        sleep(0.05)
    return False


def wait_for_key(timeout: float | None) -> bool:
    """Block until a key is pressed or ``timeout`` seconds pass (None: only a key ends it). Without a terminal on
    stdin only the timeout applies."""
    try:
        interactive = os.isatty(0)
    except (OSError, ValueError):
        interactive = False
    if not interactive:
        if timeout:
            time.sleep(timeout)
        return False
    return _wait_key_windows(timeout) if os.name == "nt" else _wait_key_posix(timeout)


def open_split(session: str, paths, close_after: float | None, run=None) -> str | None:
    """Open the image pane beside ``session`` (an ``ITERM_SESSION_ID``); the error text, or None."""
    from . import terminal
    run = terminal.run_osascript if run is None else run
    error, _ = run(SPLIT_SCRIPT, [*session_candidates(session), draw_command(paths, close_after)])
    return error


def run_img(args, environ=None, run=None, runner=None) -> int:
    environ = os.environ if environ is None else environ
    if runner is None:
        import subprocess
        runner = subprocess.run
    paths, error = check_images(args.paths)
    if error:
        print(f"bt img: {error}", file=sys.stderr)
        return 2
    if args.draw:
        draw(paths)
        wait_for_key(args.close_after)
        return 0
    if config.WINDOWS:
        print("bt img: on Windows use C:/Work/tools/terminal-workflow/imgshow.ps1 <path> (a Sixel pane in Windows Terminal); "
              "bt img opens an iTerm2 pane or the default viewer on macOS", file=sys.stderr)
        return 2
    if not config.MACOS:
        print(f"bt img: there is no image pane on {sys.platform}; it works on macOS (iTerm2, or the default viewer)",
              file=sys.stderr)
        return 2
    for path in paths:
        print(path)
    # the session this shell runs in (ITERM_SESSION_ID counts only under iTerm2 itself), else the one `bt <ws>` recorded
    session = (environ.get("ITERM_SESSION_ID") if environ.get("TERM_PROGRAM") == "iTerm.app" else "") or environ.get(SESSION_VAR) or ""
    if session:
        error = open_split(session, paths, args.close_after, run)
        if error:
            print(f"bt img: could not open the pane: {error}", file=sys.stderr)
            return 1
        print("opened an image pane beside this session (any key closes it)")
        return 0
    try:
        runner(["open", *paths], check=False)
    except OSError as exc:
        print(f"bt img: could not start open: {exc}", file=sys.stderr)
        return 1
    print("opened in the default viewer")
    return 0


# ------------------------------------------------------------------ application settings (bt iterm prefs)

DEFAULTS_DOMAIN = ITERM_BUNDLE_ID
# (key, `defaults` type, value, why). iTerm2 keeps these for the whole application, not in a profile, so `bt install`
# never writes them: `bt iterm prefs` prints them and `--apply` writes them while iTerm2 is not running.
PREFS = (
    ("TabStyleWithAutomaticOption", "int", "5", "the Minimal theme: tabs and title bar in one strip"),
    ("TabViewType", "int", "0", "tabs along the top"),
    ("StretchTabsToFillBar", "bool", "false", "each tab as wide as its title, not the bar shared out among them"),
    ("HideTabNumber", "bool", "true", "no Cmd-number on each tab (Cmd-1 to Cmd-9 still switch)"),
    ("HideTabCloseButton", "bool", "true", "no close button on each tab (Cmd-W still closes)"),
    ("HideTab", "bool", "false", "the tab bar stays when only one tab is left"),
    ("HideActivityIndicator", "bool", "true", "no spinner on every tab that prints: the status says who is working"),
    ("ShowNewOutputIndicator", "bool", "false", "no new-output dot: agents print all the time"),
    ("AllowClipboardAccess", "bool", "true", "programs may write the clipboard (Claude Code's /copy)"),
    ("TerminalMargin", "int", "14", "room left and right of the text"),
    ("TerminalVMargin", "int", "10", "and above and below it"),
    ("HideScrollbar", "bool", "true", "no scroll bar beside every pane (the trackpad still scrolls)"),
    ("WindowNumber", "bool", "false", "no window number in the title"),
    ("MinimalTabStyleBackgroundColorDifference", "float", "0.2", "the selected tab stands out from the others"),
    ("StatusBarPosition", "int", "1", "the status bar at the foot of the pane, like an editor's"),
)
DEFAULT_PROFILE = ("Default Bookmark Guid", "string", SHELL_GUID, "new windows and tabs use the Baton Shell profile")
WAIT_QUIT_SECONDS = 15 * 60


def prefs() -> tuple:
    """The settings: PREFS, and Baton Shell as the default profile unless the look is "inherit"."""
    return PREFS + ((DEFAULT_PROFILE,) if look_name() != "inherit" else ())


def prefs_commands() -> list[list[str]]:
    """One ``defaults write`` argv per setting."""
    return [["defaults", "write", DEFAULTS_DOMAIN, key, "-" + kind, value] for key, kind, value, _why in prefs()]


def _iterm_running(runner) -> int:
    """pgrep's exit code: 0 = iTerm2 runs, 1 = it does not, anything else = it could not tell."""
    return runner(["pgrep", "-x", "iTerm2"], capture_output=True, text=True).returncode


def _start_waiter(spawn=None) -> Path:
    """Start ``bt iterm prefs --apply --wait-quit`` detached from this terminal (its own session, so quitting iTerm2
    does not end it) with its output in a log under the state folder; return the log's path."""
    import subprocess
    log = config.STATE_DIR / "iterm-prefs.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    argv = [sys.executable, str(config.BATON_HOME / "bt.py"), "iterm", "prefs", "--apply", "--wait-quit"]
    with open(log, "w", encoding="utf-8") as out:
        (spawn or subprocess.Popen)(argv, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
                                    start_new_session=True, close_fds=True)
    return log


# ------------------------------------------------------------------ groups and the arranger (iTerm2's Python API)
#
# A group is a set of workstreams that belong together (a lead and its lanes). Its tabs share a colour and, when there
# are two or more of them, a window of their own titled with the group's name. A workstream's group is, first, the
# `[policy.iterm_groups]` entry that names it or its repository (`LANE = ["lane_plan", "app", "app-proofs"]`,
# whose order is also the order of the tabs), then the group of the worktree its handover's `Parent:` line names, and
# otherwise its repository. `bt iterm arrange` (and, with `[policy] iterm_arrange = "auto"`, every session start) uses
# iTerm2's Python API through the `iterm2` package, in a virtualenv `bt install` makes under the state folder: it gives
# every agent tab Baton's profile, its workstream as title and its group's colour, restyles the remaining tabs with the
# Baton Shell profile, and moves each group of two or more into its window. It never closes or ends a session.

GROUPS_TABLE = "iterm_groups"
ARRANGE_SETTING = "iterm_arrange"           # "auto" (default), "manual" (only `bt iterm arrange`) or "off"
API_PACKAGE = "iterm2==2.30"
AGENTS = ("claude", "codex")
NEUTRAL = (58, 65, 78)                      # an agent tab outside any worktree: slate
PARENT_DEPTH = 6


def api_python() -> Path:
    """The interpreter of the virtualenv that holds the ``iterm2`` package."""
    return config.STATE_DIR / "iterm-venv" / "bin" / "python"


def groups_table() -> dict[str, list[str]]:
    """``[policy.iterm_groups]``: group name -> the workstreams and repositories in it, in tab order; malformed entries
    are dropped."""
    try:
        table = config.accounts_config().get("policy", {}).get(GROUPS_TABLE)
    except Exception:  # noqa: BLE001 - a broken accounts.toml must not break a hook
        return {}
    if not isinstance(table, dict):
        return {}
    return {str(name): [str(item) for item in items if isinstance(item, str) and item]
            for name, items in table.items() if isinstance(items, list)}


def parent_root(root: Path) -> Path | None:
    """The worktree or checkout that the ``Parent:`` line of ``root``'s .baton/now.md names by path (absolute or ``~``),
    when it is another git checkout; None for "none", a description, or a path that is not one."""
    try:
        text = (Path(root) / ".baton" / "now.md").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    for line in text.splitlines():
        if not line.startswith("Parent:"):
            continue
        for word in line[len("Parent:"):].replace("(", " ").replace(")", " ").replace(",", " ").split():
            if word.startswith(("/", "~")):
                candidate = Path(os.path.expanduser(word.rstrip(".;:")))
                try:
                    if (candidate / ".git").exists() and candidate.resolve() != Path(root).resolve():
                        return candidate
                except OSError:
                    return None
        return None
    return None


def group_of(root, table: dict | None = None) -> tuple[str, int]:
    """``(group, rank)`` of the worktree at ``root``: the first `[policy.iterm_groups]` entry naming the worktree (by its
    workstream name) or its repository, looked up along the chain of ``Parent:`` lines; else the repository at the top of
    that chain. ``rank`` orders the group's tabs: the position of the matching name in the entry, workstreams before
    repositories."""
    from . import paths
    table = groups_table() if table is None else table
    chain, node = [], Path(root)
    while node is not None and len(chain) < PARENT_DEPTH and node not in chain:
        chain.append(node)
        node = parent_root(node)
    for node in chain:
        ws, repo = paths.ws_name(node), paths.repo_name(node)
        for name, items in table.items():
            if ws in items:
                return name, items.index(ws)
        for name, items in table.items():
            if repo in items:
                return name, len(items) + items.index(repo)
    return paths.repo_name(chain[-1]), 0


def plan_arrangement(sessions: list[dict], table: dict | None = None, roots=None) -> dict:
    """What `bt iterm arrange` does, as data (pure: ``roots`` maps a session's path to its worktree root, by default
    ``paths.find_worktree_root`` under the code root). Each session: ``session``, ``tab``, ``window``, ``profile``,
    ``job``, ``path``, ``ws`` (user.baton_ws), ``group`` (user.baton_group), ``tab_title`` (the tab's title override
    format) and ``window_title``. Returns ``{"sessions": [{"session", "profile"?, "inject"?, "tab", "title"?}],
    "windows": [{"group", "tabs", "window"}]}``: profiles to switch to, escape sequences to inject (identity and colour,
    only when the session lacks them), tab titles to set, and the tabs of each group of two or more, in order, with the
    window already titled with the group's name (or None: move the first tab to a new window)."""
    from . import paths
    if roots is None:
        def roots(path):
            found = paths.find_worktree_root(path) if path else None
            try:
                inside = found is not None and config.CODE_ROOT.resolve() in found.resolve().parents
            except OSError:
                inside = False
            return found if inside else None
    out, members = [], {}
    for s in sessions:
        job = (s.get("job") or "").lower()
        root = roots(s.get("path")) if job in AGENTS or s.get("ws") else None
        item = {"session": s["session"], "tab": s["tab"]}
        if root is not None:
            ws = s.get("ws") or paths.ws_name(root)
            group, rank = group_of(root, table)
            baton = profile_name() or PROFILE_DEFAULT
            if s.get("profile") != baton:
                item["profile"] = baton
            if s.get("ws") != ws or s.get("group") != group:
                color = repo_color(group)
                item["inject"] = (badge(ws) + user_var("baton_ws", ws) + user_var("baton_group", group)
                                  + (user_var("baton_agent", job) if job in AGENTS else "")
                                  + (tab_color(*color) if color else ""))
            if s.get("tab_title") != TAB_TITLE:
                item["title"] = TAB_TITLE
            members.setdefault(group, []).append((rank, ws, s))
        elif job in AGENTS:
            folder = Path(s.get("path") or "~").name or "~"
            title = f"{job} · {folder}"
            if s.get("profile") not in (SHELL_PROFILE, PROFILE_DEFAULT):
                item["profile"] = SHELL_PROFILE
            if s.get("tab_title") != title:
                item["title"] = title
            if s.get("group") != "-":
                item["inject"] = user_var("baton_group", "-") + user_var("baton_agent", job) + tab_color(*NEUTRAL)
        elif s.get("profile") not in (SHELL_PROFILE, PROFILE_DEFAULT):
            item["profile"] = SHELL_PROFILE
        if len(item) > 2:
            out.append(item)
    windows = []
    for group, tabs in sorted(members.items()):
        seen, ordered = set(), []
        for _rank, _ws, s in sorted(tabs, key=lambda t: (t[0], t[1])):
            if s["tab"] not in seen:
                seen.add(s["tab"])
                ordered.append(s)
        if len(ordered) < 2:
            continue
        titled = [s["window"] for s in sessions if s.get("window_title") == group]
        windows.append({"group": group, "tabs": [s["tab"] for s in ordered], "window": titled[0] if titled else None})
    return {"sessions": out, "windows": windows}


def arrange_mode() -> str:
    value = config.policy_text(ARRANGE_SETTING, "auto").lower()
    return value if value in ("auto", "manual", "off") else "auto"


def _cookie(runner) -> str | None:
    """A Python API cookie from iTerm2 (AppleScript ``request cookie``): the script then connects without a prompt."""
    done = runner(["osascript", "-e", f'tell application id "{ITERM_BUNDLE_ID}" to request cookie'],
                  capture_output=True, text=True, timeout=10)
    cookie = done.stdout.strip() if done.returncode == 0 else ""
    return cookie or None


def run_api(action: str, extra=(), runner=None, detach: bool = False) -> int:
    """Run ``baton.iterm_api <action>`` in the iterm2 virtualenv with a cookie; 2 when the virtualenv is missing (run
    `bt install`), 1 when iTerm2 gives no cookie. ``detach`` starts it in its own session and returns at once (hooks)."""
    import subprocess
    runner = runner or subprocess.run
    python = api_python()
    if not python.exists():
        return 2
    cookie = _cookie(runner)
    if not cookie:
        return 1
    env = dict(os.environ, ITERM2_COOKIE=cookie, PYTHONPATH=str(config.BATON_HOME))
    argv = [str(python), "-m", "baton.iterm_api", action, *extra]
    if detach:
        log = config.STATE_DIR / "iterm-arrange.log"
        with open(log, "a", encoding="utf-8") as out:
            subprocess.Popen(argv, cwd=str(config.BATON_HOME), env=env, stdin=subprocess.DEVNULL, stdout=out,
                             stderr=subprocess.STDOUT, start_new_session=True, close_fds=True)
        return 0
    return runner(argv, cwd=str(config.BATON_HOME), env=env).returncode


def arrange_soon(environ=None) -> None:
    """At a session start in iTerm2 (``[policy] iterm_arrange = "auto"``): arrange the tabs in the background. Never
    raises, never waits; concurrent starts coalesce in the arranger (a lock and a second pass)."""
    try:
        environ = os.environ if environ is None else environ
        if not config.MACOS or environ.get("TERM_PROGRAM") != "iTerm.app" or arrange_mode() != "auto":
            return
        run_api("arrange", ("--quiet", "--settle", "1.5"), detach=True)
    except Exception:  # noqa: BLE001 - a tab's arrangement is never worth a failed hook
        pass


def run_iterm(args, runner=None, sleep=None, spawn=None) -> int:
    """``bt iterm prefs [--apply]``: print the settings; with ``--apply`` (macOS, iTerm2 not running: it writes its
    settings back when it quits, which would undo them) write each and print the value it replaced. ``--after-quit``
    (from inside iTerm2) starts a detached waiter instead: it writes them once iTerm2 has quit, then opens it again.
    ``--wait-quit`` is that waiter: it polls for up to WAIT_QUIT_SECONDS before writing, and reopens iTerm2 after."""
    if runner is None:
        import subprocess
        runner = subprocess.run
    if args.action == "arrange":
        if not config.MACOS:
            print("bt iterm arrange: iTerm2 exists on macOS only", file=sys.stderr)
            return 2
        code = run_api("arrange", ("--dry-run",) if getattr(args, "dry_run", False) else (), runner=runner)
        if code == 2:
            print(f"bt iterm arrange: no {api_python()}: run `bt install` (it installs {API_PACKAGE} there)", file=sys.stderr)
        elif code == 1 and not getattr(args, "dry_run", False):
            print("bt iterm arrange: iTerm2 gave no Python API cookie (Settings > General > Magic > Enable Python API)",
                  file=sys.stderr)
        return code
    if getattr(args, "live", False):
        if not config.MACOS:
            print("bt iterm prefs --live: iTerm2's settings exist on macOS only", file=sys.stderr)
            return 2
        code = run_api("prefs", runner=runner)
        if code == 2:
            print(f"bt iterm prefs --live: no {api_python()}: run `bt install`", file=sys.stderr)
        return code
    for argv, (_key, _kind, _value, why) in zip(prefs_commands(), prefs()):
        print(f"{' '.join(argv):<78}  # {why}")
    after_quit, wait_quit = getattr(args, "after_quit", False), getattr(args, "wait_quit", False)
    if not (args.apply or after_quit):
        print("printed only. `bt iterm prefs --after-quit` writes them as soon as you quit iTerm2 (Cmd-Q) and reopens it; "
              "or quit iTerm2 and run `bt iterm prefs --apply` from Terminal.app.")
        return 0
    if not config.MACOS:
        print("bt iterm prefs --apply: iTerm2's settings exist on macOS only", file=sys.stderr)
        return 2
    try:
        if after_quit:
            log = _start_waiter(spawn)
            print(f"waiting for iTerm2 to quit: press Cmd-Q when the sessions in it can end (resume them with `bt <ws>`). "
                  f"The settings are written once it has quit, then iTerm2 opens again. Log: {log}")
            return 0
        found = _iterm_running(runner)
        if wait_quit:
            if sleep is None:
                sleep = time.sleep
            waited = 0
            while found == 0 and waited < WAIT_QUIT_SECONDS:
                sleep(1)
                waited += 1
                found = _iterm_running(runner)
        if found != 1:                       # pgrep: 0 = running, 1 = no such process, anything else = it could not tell
            print("bt iterm prefs --apply: iTerm2 is running and would write its own settings back when it quits. "
                  "Quit it and run this from Terminal.app, or run `bt iterm prefs --after-quit` in it." if found == 0 else
                  f"bt iterm prefs --apply: could not tell whether iTerm2 is running (pgrep exit {found}); nothing was written.",
                  file=sys.stderr)
            return 1
        for argv in prefs_commands():
            before = runner(["defaults", "read", DEFAULTS_DOMAIN, argv[3]], capture_output=True, text=True)
            was = before.stdout.strip() if before.returncode == 0 else "(not set)"
            done = runner(argv, capture_output=True, text=True)
            if done.returncode != 0:
                print(f"bt iterm prefs --apply: {' '.join(argv)} failed: {done.stderr.strip()}", file=sys.stderr)
                return 1
            print(f"set {argv[3]} = {argv[5]} (was {was})")
        if wait_quit:
            runner(["open", "-b", ITERM_BUNDLE_ID], capture_output=True, text=True)
    except OSError as exc:
        print(f"bt iterm prefs --apply: {exc}", file=sys.stderr)
        return 1
    return 0


def register(sub) -> None:
    q = sub.add_parser("iterm", help="iTerm2 (macOS): `prefs` prints or applies the application settings that suit many "
                                     "agent tabs; `arrange` restyles, titles, colours and groups the open tabs")
    q.add_argument("action", choices=["prefs", "arrange"])
    q.add_argument("--live", action="store_true", help="prefs: apply the settings to the running iTerm2 (Python API)")
    q.add_argument("--dry-run", action="store_true", help="arrange: print what would change")
    q.add_argument("--apply", action="store_true", help="write the settings (iTerm2 must not be running)")
    q.add_argument("--after-quit", action="store_true",
                   help="from inside iTerm2: write the settings as soon as you quit it, then open it again")
    q.add_argument("--wait-quit", action="store_true", help=argparse.SUPPRESS)
    q.set_defaults(func=run_iterm)
    p = sub.add_parser("img", help="show images in a pane beside this iTerm2 session (macOS); `open` otherwise")
    p.add_argument("paths", nargs="+", metavar="path", help="image files (png jpg jpeg gif bmp webp tif tiff)")
    p.add_argument("--draw", action="store_true", help="internal: draw in this terminal and wait for a key")
    p.add_argument("--close-after", type=float, metavar="SECONDS", help="close the pane after this many seconds without a key")
    p.set_defaults(func=run_img)
