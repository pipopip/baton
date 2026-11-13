"""Shared pieces of ``bt migrate``: context, read-only git access, carry rules, hashing, slugs."""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .. import config, gitutil
from . import secrets

FORMAT_VERSION = 1
TOOL = "baton-migrate"
MAX_COPY_FILE = 500 * 1024 * 1024          # a single file above this is never copied (named instead)
UNDECIDED_FILE_LIMIT = 20 * 1024 * 1024    # ignored, undecided file carried by default only below this ...
UNDECIDED_TOTAL_LIMIT = 200 * 1024 * 1024  # ... and only while a checkout's undecided total stays below this
LFS_CARRY_LIMIT = 4 * 1024 * 1024 * 1024   # .git/lfs/objects is carried into the bundle up to this size per repository
RULE_CARRY_LIMIT = 500 * 1024 * 1024       # files carried by a built-in rule above this total in one checkout need an owner decision
SUMMARY_WALK_CAP = 20000                   # entries counted under one rebuildable directory before "at least"
UNDECIDED_WALK_CAP = 1000000                # files enumerated under one undecided directory

CARRY_NAMES = frozenset({"CLAUDE.md", "AGENTS.md", "CLAUDE.local.md"})
AGENT_DIRS = frozenset({".claude", ".codex"})
AGENT_CACHE_DIRS = frozenset({"cache", "tmp", "logs", "worktrees", "node_modules", ".venv"})
BATON_CARRY_SUFFIXES = (".md", ".toml", ".ps1", ".py", ".sh")
BATON_MACHINE = frozenset({"owner.json", "owner.lock", "inbox", "runs", "compute", "handoffs", "follow-state",
                           "follow-state.json", "locks", "lock"})
REBUILDABLE_DIRS = frozenset({"node_modules", ".venv", "venv", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
                              ".gradle", "build", "dist", "target", ".lake", ".next", ".astro", ".turbo", ".cache",
                              "DerivedData", ".test-tmp", ".test-build"})
REBUILDABLE_SUFFIXES = (".pyc", ".o", ".class")
TOOL_NAMES = ("python", "node", "npm", "pnpm", "git", "gh", "rg", "fd", "fzf", "jq", "uv", "go", "java", "gradle", "adb",
              "ffmpeg", "blender", "lean", "lake", "elan", "cargo", "docker", "pwsh", "claude", "codex", "tailscale",
              "wrangler", "aws", "doctl", "zoxide", "delta", "pdftotext", "restic", "gcloud", "code", "maestro")


def is_secret_name(name: str) -> bool:
    """A credential file by its name alone (see ``secrets.py``); a ``.pem`` is decided by its content."""
    return secrets.is_secret_name(name)


def is_rebuildable_dir(name: str) -> bool:
    return (name in REBUILDABLE_DIRS or name.endswith(".egg-info") or name.startswith((".test-build", ".pytest-temp", ".pytest_cache"))
            or name == "site-packages")


def is_venv_dir(path) -> bool:
    """A Python virtual environment, whatever its name or parent: it holds ``pyvenv.cfg``."""
    try:
        return os.path.isfile(lp(Path(path) / "pyvenv.cfg"))
    except OSError:
        return False


# ------------------------------------------------------------------ context

def source_home() -> Path:
    """The home directory whose configuration is surveyed or exported (tests replace this)."""
    return Path.home()


def default_accounts(vendor: str) -> list[tuple[str, Path]]:
    return [(n, config.account_dir(vendor, n)) for n in config.account_names(vendor)]


DEFAULT_NO_SCAN = ("brain",)        # repositories whose file contents the scan and the secret content check never open


def _as_list(value) -> list[str]:
    return [str(v) for v in value] if isinstance(value, list) else []


def normalize_cfg(raw: dict | None) -> dict:
    """``migrate.toml`` (documented in ``baton/migrate/__init__.py``) as a plain dict with every key present."""
    raw = raw or {}
    repos = {}
    for name, body in (raw.get("repos") or {}).items():
        if isinstance(body, dict):
            repos[str(name)] = {"carry": _as_list(body.get("carry")), "leave": _as_list(body.get("leave"))}

    def entries(value, kind):
        out = []
        for item in value if isinstance(value, list) else []:
            if isinstance(item, str):
                item = {"path": item}
            if isinstance(item, dict) and item.get("path"):
                path = str(item["path"])
                target = str(item.get("target") or Path(path.replace("\\", "/")).name)
                base = "home" if str(item.get("base", "root")).lower() == "home" else "root"
                out.append({"path": path, "target": target.replace("\\", "/").strip("/"), "base": base, "kind": kind})
        return out

    return {"carry": _as_list(raw.get("carry")), "leave": _as_list(raw.get("leave")), "skip_repos": _as_list(raw.get("skip_repos")),
            "nonrepo": _as_list(raw.get("nonrepo")), "repos": repos,
            "no_scan": _as_list(raw["no_scan"]) if "no_scan" in raw else list(DEFAULT_NO_SCAN),
            "extra_repos": entries(raw.get("extra_repos"), "repo"), "extra_dirs": entries(raw.get("extra_dirs"), "dir")}


def load_cfg() -> dict:
    """migrate.toml next to accounts.toml, with the machine-local ``.state/migrate.local.toml`` appended to its lists."""
    base = config.load_toml(config.BATON_HOME / "migrate.toml")
    local = config.load_toml(config.STATE_DIR / "migrate.local.toml")
    for key, value in local.items():
        if isinstance(value, list) and isinstance(base.get(key), list):
            base[key] = base[key] + value
        else:
            base[key] = value
    return normalize_cfg(base)


@dataclass
class Ctx:
    root: Path
    home: Path = field(default_factory=Path.home)
    cfg: dict = field(default_factory=lambda: normalize_cfg({}))
    accounts: Callable[[str], list] = default_accounts
    state_dir: Path = field(default_factory=lambda: config.STATE_DIR)
    run_tools: bool = True              # run external programs for versions (off in tests)
    with_tool: bool = True              # write the tool/ snapshot of Baton into an export bundle (off in most tests: 700 files)
    problems: list = field(default_factory=list)
    include_secrets: bool = False
    extra_carry: list = field(default_factory=list)
    only: list = field(default_factory=list)

    def problem(self, path, error) -> None:
        self.problems.append({"path": str(path), "error": secrets.redact_userinfo_text(str(error))})

    def carry_globs(self, repo: str | None) -> list[str]:
        return self.extra_carry + self.cfg["carry"] + (self.cfg["repos"].get(repo or "", {}).get("carry", []))

    def leave_globs(self, repo: str | None) -> list[str]:
        return self.cfg["leave"] + (self.cfg["repos"].get(repo or "", {}).get("leave", []))


# ------------------------------------------------------------------ globs

_glob_cache: dict[str, re.Pattern] = {}


def glob_re(pattern: str) -> re.Pattern:
    """gitignore-like glob: ``*`` and ``?`` stay inside one path segment, ``**`` crosses segments; a pattern without
    ``/`` matches the final name at any depth; a trailing ``/`` takes everything below; a leading ``/`` anchors the pattern
    at the checkout root (``/artifacts/`` is that one directory, not every ``artifacts`` below the root)."""
    got = _glob_cache.get(pattern)
    if got is not None:
        return got
    pat = pattern.replace("\\", "/")
    rooted = pat.startswith("/")
    pat = pat.lstrip("/") if rooted else pat
    anywhere = not rooted and "/" not in pat.rstrip("/")
    if pat.endswith("/"):
        pat += "**"
    out, i = [], 0
    while i < len(pat):
        c = pat[i]
        if pat.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pat.startswith("**", i):
            out.append(".*")
            i += 2
        elif c == "*":
            out.append("[^/]*")
            i += 1
        elif c == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(c))
            i += 1
    body = "".join(out)
    got = re.compile(("(?:.*/)?" if anywhere else "") + body + r"\Z", re.DOTALL)
    _glob_cache[pattern] = got
    return got


def match_any(patterns: list[str], rel: str) -> bool:
    return any(glob_re(p).match(rel) for p in patterns)


# ------------------------------------------------------------------ carry rules

def baton_rule(parts: list[str]) -> str | None:
    """Class of a path below the root ``.baton`` directory: ``carry``, ``machine`` or None (undecided)."""
    if len(parts) < 2:
        return None
    name = parts[1]
    if len(parts) == 2 and name.endswith(BATON_CARRY_SUFFIXES):
        return "carry"
    if name == "remote":
        return "carry"
    if name in BATON_MACHINE or name.endswith(".lock"):
        return "machine"
    return None


def carry_rule(rel: str) -> str | None:
    """Name of the built-in carry rule a repo-relative path matches (None when none)."""
    parts = rel.split("/")
    if parts[-1] in CARRY_NAMES:
        return parts[-1]
    if parts[0] == ".baton":
        return ".baton" if baton_rule(parts) == "carry" else None
    for i, part in enumerate(parts[:-1]):
        if part in AGENT_DIRS:
            if any(p in AGENT_CACHE_DIRS for p in parts[i + 1:-1]):
                return None
            return part + "/**"
    return None


# ------------------------------------------------------------------ files

def lp(path) -> str:
    """Extended-length form of an absolute path on Windows (the 260-character limit), the path itself elsewhere."""
    text = os.path.abspath(str(path))
    if os.name == "nt" and not text.startswith("\\\\?\\"):
        return "\\\\?\\UNC\\" + text[2:] if text.startswith("\\\\") else "\\\\?\\" + text
    return text


CHUNK = 1 << 20          # every read and write of a carried file is this big at most: a 5 GiB file costs 1 MiB of memory


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(lp(path), "rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def same_content(a, b) -> bool:
    """Whether two files hold the same bytes, read in chunks (size first)."""
    try:
        if os.stat(lp(a)).st_size != os.stat(lp(b)).st_size:
            return False
        with open(lp(a), "rb") as fa, open(lp(b), "rb") as fb:
            while True:
                x = fa.read(CHUNK)
                if x != fb.read(CHUNK):
                    return False
                if not x:
                    return True
    except OSError:
        return False


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def is_link(path) -> bool:
    """A symlink or (Windows) junction."""
    try:
        if os.path.islink(str(path)):
            return True
        isj = getattr(os.path, "isjunction", None)
        return bool(isj and isj(str(path)))
    except OSError:
        return False


def link_target(path) -> str:
    """Where a symlink or junction points ('' when unknown); a junction's target comes from realpath."""
    try:
        target = os.readlink(str(path))
    except (OSError, ValueError):
        target = ""
    if target.startswith("\\\\?\\"):
        target = target[4:]
    if not target:
        try:
            target = os.path.realpath(str(path))
        except OSError:
            target = ""
    return target


def link_kind(path) -> str:
    isj = getattr(os.path, "isjunction", None)
    try:
        if isj and isj(str(path)):
            return "junction"
        if os.path.islink(str(path)):
            return "symlink"
    except OSError:
        pass
    return ""


def copy_file(src, dst) -> tuple[int, str]:
    """Copy ``src`` to ``dst`` (parents created); returns (size, sha256) of the copy."""
    os.makedirs(lp(Path(dst).parent), exist_ok=True)
    h, size = hashlib.sha256(), 0
    with open(lp(src), "rb") as fi, open(lp(dst), "wb") as fo:                # one pass: copied and hashed chunk by chunk
        for chunk in iter(lambda: fi.read(CHUNK), b""):
            fo.write(chunk)
            h.update(chunk)
            size += len(chunk)
    return size, h.hexdigest()


def write_bytes(dst, data: bytes) -> None:
    os.makedirs(lp(Path(dst).parent), exist_ok=True)
    with open(lp(dst), "wb") as fh:
        fh.write(data)


def exec_if_script(path) -> None:
    """A carried file that starts with ``#!`` is executable again on a POSIX system (Windows does not keep the bit)."""
    if os.name != "posix":
        return
    try:
        with open(path, "rb") as fh:
            head = fh.read(2)
        if head == b"#!":
            mode = os.stat(path).st_mode
            os.chmod(path, mode | ((mode & 0o444) >> 2))
    except OSError:
        pass


def rmtree(path) -> None:
    """Remove a directory tree, read-only files (git's objects on Windows) included."""
    def clear(func, p, _exc):
        os.chmod(p, stat.S_IWRITE)
        func(p)
    if sys.version_info >= (3, 12):
        shutil.rmtree(lp(path), onexc=clear)
    else:
        shutil.rmtree(lp(path), onerror=clear)


def stat_info(path) -> tuple[int, float]:
    st = os.stat(lp(path))
    return st.st_size, st.st_mtime


def summarize_dir(path, cap: int = SUMMARY_WALK_CAP, skip=None) -> dict:
    """File count and bytes below ``path`` (links not followed); stops counting at ``cap`` entries (``approx``). ``skip`` is a
    predicate on a directory's path below ``path`` (with a trailing slash): a directory it accepts is not entered."""
    files = size = seen = 0
    approx = False
    errors: list[str] = []
    stack = [(str(path), "")]
    while stack and not approx:
        cur, rel = stack.pop()
        try:
            with os.scandir(lp(cur)) as it:
                for entry in it:
                    seen += 1
                    if seen > cap:
                        approx = True
                        break
                    try:
                        if entry.is_symlink():
                            files += 1
                        elif entry.is_dir(follow_symlinks=False):
                            sub = (rel + "/" if rel else "") + entry.name
                            if skip is None or not skip(sub + "/"):
                                stack.append((entry.path, sub))
                        else:
                            files += 1
                            size += entry.stat(follow_symlinks=False).st_size
                    except OSError as exc:
                        errors.append(f"{entry.path}: {exc}")
        except OSError as exc:
            errors.append(f"{cur}: {exc}")
    return {"files": files, "bytes": size, "approx": approx, "errors": errors[:5]}


# ------------------------------------------------------------------ git (read-only)

def git(root, *args, timeout: float = 60) -> subprocess.CompletedProcess:
    """Text git; ``-c core.quotepath=off`` so paths are not escaped. Never raises."""
    try:
        return gitutil.git(root, "--no-optional-locks", "-c", "core.quotepath=off", *args, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as exc:
        return subprocess.CompletedProcess(args, 255, "", str(exc))


def git_ok(ctx: Ctx | None, root, *args, timeout: float = 60, what: str = "") -> str:
    """stdout of a git command that should succeed; a failure is recorded in ``ctx.problems`` and gives ''."""
    cp = git(root, *args, timeout=timeout)
    if cp.returncode != 0:
        if ctx is not None:
            ctx.problem(root, f"git {' '.join(args[:3])} failed ({what or cp.returncode}): {cp.stderr.strip()[:200]}")
        return ""
    return cp.stdout


def git_bytes(root, *args, timeout: float = 300) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["git", "-C", str(root), "--no-optional-locks", "-c", "core.quotepath=off", *args], capture_output=True, timeout=timeout,
                              stdin=subprocess.DEVNULL, creationflags=config.CREATE_NO_WINDOW)
    except (OSError, subprocess.SubprocessError) as exc:
        return subprocess.CompletedProcess(args, 255, b"", str(exc).encode())


# Patches must not follow the user's diff configuration (noprefix and mnemonicPrefix change the file names git apply expects,
# color.ui=always puts escape codes in the patch), so every patch-producing call carries these and every apply matches them.
PATCH_CONFIG = ("-c", "diff.noprefix=false", "-c", "diff.mnemonicPrefix=false", "-c", "diff.srcPrefix=a/", "-c", "diff.dstPrefix=b/",
                "-c", "diff.relative=false", "-c", "color.ui=never", "-c", "color.diff=never", "-c", "core.quotePath=true")
DIFF_FLAGS = ("--no-color", "--no-ext-diff", "--no-textconv", "--binary", "--full-index")
APPLY_CONFIG = ("-c", "apply.whitespace=nowarn")


def patch_git(root, *args, timeout: float = 300) -> subprocess.CompletedProcess:
    """Bytes of a git command that produces a patch (``diff``, ``show``), under ``PATCH_CONFIG``."""
    return git_bytes(root, *PATCH_CONFIG, *args, timeout=timeout)


def split_z(text: str) -> list[str]:
    return [p for p in text.split("\0") if p]


def porcelain(ctx: Ctx | None, root) -> list[tuple[str, str]]:
    """(status, path) of ``git status`` entries; status is the two porcelain columns, '??' for untracked files."""
    text = git_ok(ctx, root, "status", "--porcelain=v1", "-z", "--untracked-files=normal", what="status")
    parts = text.split("\0")
    out, i = [], 0
    while i < len(parts):
        item = parts[i]
        i += 1
        if len(item) < 4:
            continue
        status, path = item[:2], item[3:]
        if status[0] in "RC":
            i += 1  # the rename source follows as its own field
        out.append((status, path))
    return out


def for_each_ref(ctx: Ctx | None, root) -> list[dict]:
    fmt = "%(refname)%09%(objectname)%09%(symref)%09%(upstream)%09%(upstream:track)%09%(committerdate:unix)%09%(objecttype)"
    text = git_ok(ctx, root, "for-each-ref", f"--format={fmt}", what="for-each-ref")
    refs = []
    for line in text.splitlines():
        f = line.split("\t")
        if len(f) < 7:
            continue
        refs.append({"ref": f[0], "sha": f[1], "symref": f[2], "upstream": f[3], "track": f[4],
                     "date": int(f[5]) if f[5].isdigit() else 0, "type": f[6]})
    return refs


def local_config(root) -> list[tuple[str, str]]:
    cp = git(root, "config", "--local", "-z", "--list")
    out = []
    for item in cp.stdout.split("\0") if cp.returncode == 0 else []:
        if "\n" in item:
            k, v = item.split("\n", 1)
            out.append((k, v))
        elif item:
            out.append((item, ""))
    return out


def git_common_dir(checkout: Path) -> Path:
    text = gitutil.out(checkout, "rev-parse", "--git-common-dir")
    p = Path(text) if text else checkout / ".git"
    return p if p.is_absolute() else (checkout / p).resolve()


def real(path) -> str:
    """Normalised real path (links resolved): the identity of a directory however it was reached."""
    try:
        return config.norm(os.path.realpath(str(path)))
    except OSError:
        return config.norm(path)


# ------------------------------------------------------------------ slugs and formatting

def project_slug(path: str) -> str:
    """Claude Code's project directory name: every character that is not ASCII alphanumeric becomes ``-``
    (a character outside the BMP counts as two, like the UTF-16 units Claude Code iterates)."""
    out = []
    for ch in str(path):
        if ch.isascii() and ch.isalnum():
            out.append(ch)
        else:
            out.append("-" * (len(ch.encode("utf-16-le")) // 2))
    return "".join(out)


def human(n: float) -> str:
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GiB"


def iso(ts: float | None = None) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts if ts is not None else time.time()))


def posix_rel(path, base) -> str:
    return os.path.relpath(str(path), str(base)).replace(os.sep, "/")
