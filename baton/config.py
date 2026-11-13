"""Shared constants and small loaders.

Stdlib only and safe under ``python -S``: hooks import this module.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tomllib
from pathlib import Path

BATON_HOME = Path(__file__).resolve().parent.parent
STATE_DIR = Path(os.environ.get("BATON_STATE") or BATON_HOME / ".state")

def _code_root() -> Path:
    """Directory holding all repos: the parent of baton's main checkout (also from a worktree)."""
    if BATON_HOME.parent.name == "worktrees" and BATON_HOME.parent.parent.name == ".claude":
        return BATON_HOME.parents[2].parent
    return BATON_HOME.parent


CODE_ROOT = Path(os.environ.get("BATON_CODE_ROOT") or _code_root())


def _install_home() -> Path:
    """Baton's main checkout. Installed hooks must never point into a disposable worktree."""
    if BATON_HOME.parent.name == "worktrees" and BATON_HOME.parent.parent.name == ".claude":
        return BATON_HOME.parents[2]
    return BATON_HOME


INSTALL_HOME = Path(os.environ.get("BATON_INSTALL_HOME") or _install_home())
PYTHON = os.environ.get("BATON_PYTHON") or sys.executable
# The one platform decision for everything that differs between Windows and POSIX (hook wrappers, shell profile,
# CLI lookup, doctor checks). Read at call time, so tests patch it to exercise both forms on either platform.
WINDOWS = os.name == "nt"
MACOS = sys.platform == "darwin"   # likewise: terminal backend, LaunchAgents, `bt night schedule` choose by these two

VENDORS = ("claude", "codex")
# Env var that selects an account's config directory, per vendor.
ACCOUNT_ENV = {"claude": "CLAUDE_CONFIG_DIR", "codex": "CODEX_HOME"}
# Set in the environment of headless follower runs; hooks stay silent there.
FOLLOWER_ENV = "BATON_ROLE"
# Set by a running Claude Code / Codex session for its own child processes. A CLI started
# with these inherited believes it is nested inside that session (observed 2026-10-10: a
# Claude launched from an agent's shell could not save /login credentials). Every CLI that
# baton starts gets an environment without them.
SESSION_ENV = (
    "CLAUDECODE", "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_SESSION_ATTENDED",
    "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_CODE_ENTRYPOINT",
    "CLAUDE_CODE_EXECPATH", "CLAUDE_PID", "CLAUDE_EFFORT",
    "CODEX_THREAD_ID", "CODEX_SESSION_ID", "CODEX_CI",
    "BATON_SESSION_NAME", "BATON_LAUNCH_CWD", "BATON_TERMINAL_TOKEN",
)


def clean_env(environ: dict | None = None) -> dict:
    """Copy of ``environ`` (default: os.environ) without the parent session's variables."""
    source = os.environ if environ is None else environ
    return {k: v for k, v in source.items() if k.upper() not in SESSION_ENV}

CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0


def expand(path: str | os.PathLike) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(str(path))))


def norm(path: str | os.PathLike) -> str:
    """Canonical key for comparing paths and keying registries, lineage, ownership and liveness by them.

    Windows: ``normcase(abspath)``, case- and slash-insensitive; junctions are deliberately not resolved.
    macOS and Linux: symbolic links are resolved first, so every spelling of one directory is one key: git prints
    real paths, ``os.getcwd()`` returns the real path, ``$PWD`` and a hook payload's ``cwd`` may carry a link
    (macOS ``/var`` is ``/private/var``; the owner's ``~/Code/app`` is ``~/Code/app``). A path that does not
    exist is resolved as far as it does. On macOS the key is also put in one Unicode form (NFC) and lower-cased, as
    Windows lower-cases: the default volume ignores both case and the composed or decomposed form of a name, and
    ``normcase`` is the identity there (git prints NFC, a directory listing prints the form the name was made with).
    On a case-sensitive macOS volume two paths that differ only in case would share a key, and a key used as a path
    (a few places read a registry key back as one) would name the wrong spelling. Only comparison and keys use this
    form; stored and displayed paths keep the spelling they were given.
    """
    absolute = os.path.abspath(str(path))
    if os.name == "nt":
        return os.path.normcase(absolute)
    real = os.path.realpath(absolute)
    if sys.platform != "darwin":
        return real
    import unicodedata                         # here only: the hooks import this module on every platform
    return unicodedata.normalize("NFC", real).lower()


def lexical(path: str | os.PathLike) -> str:
    """The spelling-only form of a path (``normcase(abspath)``, no link resolved), for the few comparisons whose point is
    whether two *configured spellings* name the same directory, as opposed to what is there: an account directory that is
    a link to the primary one is a misconfiguration to refuse, not the primary account. Everything else uses ``norm``."""
    return os.path.normcase(os.path.abspath(str(path)))


def load_toml(path: Path) -> dict:
    try:
        with open(path, "rb") as fh:
            return tomllib.load(fh)
    except FileNotFoundError:
        return {}


def _merge(base: dict, extra: dict) -> dict:
    out = dict(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _merge(out[key], value)
        else:
            out[key] = value
    return out


def accounts_config() -> dict:
    """accounts.toml, overlaid by the machine-local .state/accounts.local.toml."""
    return _merge(load_toml(BATON_HOME / "accounts.toml"), load_toml(STATE_DIR / "accounts.local.toml"))


# Defaults of the [policy] thresholds that several modules read.
ROTATE_TOKENS = 250_000           # rotate_context_tokens: context size from which a lead is told to hand over (fallback)
ROTATE_COMPACTIONS = 10           # rotate_compactions: compactions after which `bt status` flags a session
ROTATE_AGE_SECONDS = 24 * 3600    # `bt status` flags a session older than this
CACHE_TTL_SECONDS = 3600          # cache_ttl_seconds_<vendor>: idle time after which the prompt cache counts as cold
OWNER_LIVE_SECONDS = 15 * 60      # an owner seen (transcript write, claim or registry touch) this recently is live


def policy_int(name: str, default: int) -> int:
    """Positive integer ``[policy] <name>`` from accounts.toml (and its local overlay); ``default`` when absent or invalid."""
    try:
        value = accounts_config().get("policy", {}).get(name)
    except Exception:  # noqa: BLE001 - a broken accounts.toml must not break a hook
        return default
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else default


def policy_str(name: str, default: str) -> str:
    """Non-empty string ``[policy] <name>`` from accounts.toml (and its local overlay); ``default`` when absent or invalid."""
    try:
        value = accounts_config().get("policy", {}).get(name)
    except Exception:  # noqa: BLE001 - a broken accounts.toml must not break a hook
        return default
    return value.strip() if isinstance(value, str) and value.strip() else default


def _rotate_table() -> int | dict:
    """``[policy] rotate_context_tokens`` as a positive int, a dict of limits, or ROTATE_TOKENS when absent or invalid."""
    try:
        value = accounts_config().get("policy", {}).get("rotate_context_tokens")
    except Exception:  # noqa: BLE001 - a broken accounts.toml must not break a hook
        return ROTATE_TOKENS
    if isinstance(value, dict):
        return value
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else ROTATE_TOKENS


def _limit(value) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def rotate_tokens(vendor: str | None, model: str | None = None) -> int:
    """Context size above which a ``vendor`` session running ``model`` should hand over.

    ``[policy] rotate_context_tokens`` is either one integer (every session) or a table whose keys are model ids,
    model-id prefixes, the vendor names (``claude``, ``codex``) and ``default``. The most specific key wins: the
    exact model id, then the longest model-id prefix, then the vendor key, then ``default``, then ``ROTATE_TOKENS``.
    Matching is case-insensitive; a prefix is a plain ``startswith`` of the model id.
    """
    table = _rotate_table()
    if not isinstance(table, dict):
        return table
    keys = {str(k).lower(): _limit(v) for k, v in table.items()}
    vendor_key = (vendor or "").lower()
    name = (model or "").lower()
    if name:
        if keys.get(name) is not None and name not in ("default", "claude", "codex"):
            return keys[name]
        prefixes = [k for k, v in keys.items() if v is not None and name.startswith(k) and k not in ("default", "claude", "codex")]
        if prefixes:
            return keys[max(prefixes, key=len)]
    for key in (vendor_key, "default"):
        if key and keys.get(key) is not None:
            return keys[key]
    return ROTATE_TOKENS


def rotate_summary() -> str:
    """Short statement of the context limits for ``bt status``, e.g. ``250k (every session)`` or ``claude 800k, codex 400k, ...``."""
    table = _rotate_table()
    if not isinstance(table, dict):
        return f"{round(table / 1000)}k"
    parts = [f"{k} {round(v / 1000)}k" for k, v in table.items() if _limit(v) is not None]
    return ", ".join(parts) if parts else f"{round(ROTATE_TOKENS / 1000)}k"


def cache_ttl_seconds(vendor: str) -> int:
    """Idle seconds after which ``vendor``'s prompt cache counts as cold (``[policy] cache_ttl_seconds_<vendor>``)."""
    return policy_int(f"cache_ttl_seconds_{vendor}", CACHE_TTL_SECONDS)


def read_json(path: str | os.PathLike) -> dict:
    """JSON object in ``path``; ``{}`` when missing, unreadable or not an object."""
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def mtime(path: str | os.PathLike | None) -> float | None:
    """Modification time of ``path``; None when ``path`` is empty or cannot be stat-ed."""
    if not path:
        return None
    try:
        return os.stat(path).st_mtime
    except OSError:
        return None


def routing_config() -> dict:
    """routing.toml, overlaid by the machine-local .state/routing.local.toml."""
    return _merge(load_toml(BATON_HOME / "routing.toml"), load_toml(STATE_DIR / "routing.local.toml"))


def account_dir(vendor: str, account: str) -> Path:
    """Config directory (Claude) or home (Codex) for a named account."""
    cfg = accounts_config().get(vendor, {}).get(account, {})
    key = "config_dir" if vendor == "claude" else "home"
    default = "~/.claude" if vendor == "claude" else "~/.codex"
    return expand(cfg.get(key) or (default if account == "main" else f"{default}-{account}"))


def account_names(vendor: str) -> list[str]:
    names = [k for k, v in accounts_config().get(vendor, {}).items() if isinstance(v, dict)]
    return names or ["main"]


def account_from_env(vendor: str, environ: dict | None = None) -> str:
    """Name of the account whose directory the given environment selects ('main' when unset)."""
    environ = os.environ if environ is None else environ
    value = environ.get(ACCOUNT_ENV[vendor])
    if not value:
        return "main"
    target = lexical(expand(value))
    for name in account_names(vendor):
        if lexical(account_dir(vendor, name)) == target:
            return name
    return Path(value).name or "main"


def exe(vendor: str) -> str:
    """Path of the vendor CLI (override with BATON_CLAUDE_EXE / BATON_CODEX_EXE)."""
    override = os.environ.get(f"BATON_{vendor.upper()}_EXE")
    if override:
        return override
    if vendor == "claude":
        native = Path.home() / ".local" / "bin" / ("claude.exe" if WINDOWS else "claude")
        if native.exists():
            return str(native)
    found = shutil.which(vendor)
    # The npm wrapper (a .cmd/.ps1 shim around a Windows .exe) exists only on Windows; elsewhere `codex` is the binary.
    if WINDOWS and vendor == "codex" and found and Path(found).suffix.lower() in (".cmd", ".ps1", ".bat"):
        native = _codex_native(Path(found))
        if native is not None:
            return str(native)
    return found or vendor


def _codex_native(wrapper: Path) -> Path | None:
    """Resolve only the npm wrapper's adjacent package tree, for the host architecture."""
    import platform
    arch = {"amd64": "x64", "x86_64": "x64", "arm64": "arm64", "aarch64": "arm64"}.get(platform.machine().lower())
    if arch is None:
        return None
    target = "aarch64" if arch == "arm64" else "x86_64"
    package = wrapper.parent / "node_modules" / "@openai" / "codex"
    # Current optional platform package, then the older bundled vendor layout.
    roots = [package / "node_modules" / "@openai" / f"codex-win32-{arch}", package]
    for root in roots:
        for dirname in ("bin", "codex"):
            candidate = root / "vendor" / f"{target}-pc-windows-msvc" / dirname / "codex.exe"
            if candidate.is_file():
                return candidate
    return None
