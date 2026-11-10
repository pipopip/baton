"""Accounts: usage lookup, linking a second account's config to the primary, and the CLI.

Credential files (``.credentials.json``, ``auth.json``) are never copied or printed. Their existence is
tested, and ``login_expiry`` reads the two expiry timestamps from Claude's ``.credentials.json`` (nothing else
in it).
"""
from __future__ import annotations

import copy
import hashlib
import itertools
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import tomllib
from datetime import datetime
from pathlib import Path

from . import config, procs
from .hook import _pid_alive

CLAUDE_JUNCTIONS = (
    "projects", "skills", "agents", "plugins", "hooks", "workflows", "themes", "commands",
    "output-styles", "todos", "plans", "file-history", "project-ctx", "session-env",
)
CLAUDE_CREATE_IN_PRIMARY = ("agents",)
CLAUDE_FILES = ("settings.json", "CLAUDE.md", "keybindings.json")
CLAUDE_PROJECT_KEYS = (
    "hasTrustDialogAccepted", "allowedTools", "mcpServers", "enabledMcpjsonServers",
    "disabledMcpjsonServers", "hasCompletedProjectOnboarding",
)
# Not "agents": each Codex home gets its own agent files, rendered for its own models (`bt routing apply`).
CODEX_JUNCTIONS = ("skills", "rules", "themes", "plugins")
CODEX_FILE_PATTERNS = ("config.toml", "hooks.json", "AGENTS.md", "*.config.toml", "model-catalog*.json")
PROTECTED_FILES = frozenset({".credentials.json", "auth.json"})
UTILISATION_KEYS = ("session", "weekly", "primary", "secondary")
CREDENTIAL_FILE = {"claude": ".credentials.json", "codex": "auth.json"}
WIDGET_CONFIG = Path("C:/Work/usage-widget/config.json")
USAGE_TIMEOUT = 45
SNAPSHOT_MAX_AGE_FACTOR = 2     # the widget snapshot is usable up to usage_cache_seconds * this
NEGATIVE_CACHE_SECONDS = 60     # no retry of Get-Usage.ps1 after a failed or timed-out run
STALE_ENTRY_SECONDS = 30 * 60   # utilisation() ignores entries fetched longer ago than this
LOGIN_WARN_DAYS = 5             # doctor warns, and keepalive acts, this long before a refresh token expires
KEEPALIVE_TIMEOUT = 120
KEEPALIVE_MODEL = "claude-haiku-5-5"
STATUS_TIMEOUT = 30             # ``claude auth status --json`` when the email is not cached
LOGIN_NEXT = {
    "claude": "Done. In a new session under this account run /mcp to authorise learnings-graph if needed.",
    "codex": "Done. Start `codex` once in this tab and approve /hooks so baton's hooks run for this account.",
}
_ACCOUNT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


# ---------------------------------------------------------------------- usage

def _policy() -> dict:
    try:
        policy = config.accounts_config().get("policy", {})
    except Exception:  # noqa: BLE001 - a malformed accounts file must not break usage lookups
        return {}
    return policy if isinstance(policy, dict) else {}


def _usage_cache_path() -> Path:
    return config.STATE_DIR / "usage.json"


def _usage_ttl() -> float:
    return _as_float(_policy().get("usage_cache_seconds", 300)) or 300.0


def _tmp_name(path: Path) -> Path:
    return path.with_name(f"{path.name}.{os.getpid()}.tmp")


def _atomic_json(path: Path, data) -> None:
    """Write JSON next to ``path`` and rename it into place; the temp file never outlives a failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_name(path)
    try:
        with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=True)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _as_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _read_usage_file() -> dict:
    try:
        with open(_usage_cache_path(), encoding="utf-8") as fh:
            raw = json.load(fh)
        return raw if isinstance(raw, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_usage_cache(data: dict, fetched_at: float | None = None, failed_at: float | None = None) -> None:
    payload = {"fetched_at": time.time() if fetched_at is None else fetched_at, "data": data}
    if failed_at:
        payload["failed_at"] = failed_at
    _atomic_json(_usage_cache_path(), payload)


def _credential_mtime(vendor: str, account: str) -> float | None:
    """Modification time of the account's credential file; None when absent. The contents are never read."""
    try:
        return os.path.getmtime(config.account_dir(vendor, account) / CREDENTIAL_FILE[vendor])
    except OSError:
        return None


def _credential_newer(vendor: str, account: str, fetched_at: float) -> bool:
    """True when the account's credential file changed after a cache entry fetched at ``fetched_at`` (a login since)."""
    mtime = _credential_mtime(vendor, account)
    return mtime is not None and mtime > fetched_at


def _stale_usage_ids(fetched_at: float) -> set[str]:
    """Usage ids of the accounts that logged in after a usage or identity entry fetched at ``fetched_at``."""
    return {
        uid for vendor in config.VENDORS for name in config.account_names(vendor)
        if (uid := usage_id(vendor, name)) and _credential_newer(vendor, name, fetched_at)
    }


def _without_stale(data: dict, fetched_at: float) -> dict:
    """``data`` without the entries of accounts whose credentials changed after ``fetched_at``."""
    stale = _stale_usage_ids(fetched_at)
    return {k: v for k, v in data.items() if k not in stale} if stale else data


def _run_usage_script() -> str | None:
    """Run Get-Usage.ps1 -Json; None when no script is configured."""
    script = _policy().get("usage_script")
    if not script or not config.expand(script).is_file():
        return None
    cp = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(config.expand(script)), "-Json"],
        capture_output=True,
        timeout=USAGE_TIMEOUT,
        stdin=subprocess.DEVNULL,
        creationflags=config.CREATE_NO_WINDOW,
    )
    # Get-Usage.ps1 exits 1 when any account reports an error but still prints the JSON for all of them.
    return cp.stdout.decode("utf-8-sig", errors="replace")


def _parse_usage(text: str | None) -> dict:
    """Index Get-Usage.ps1 -Json output (an array, or a bare object for one account) by id.

    Banner text before the JSON may itself contain ``[`` or ``{``, so each candidate start is tried until one parses.
    """
    if not text:
        return {}
    decoder = json.JSONDecoder()
    for match in itertools.islice(re.finditer(r"[\[{]", text), 200):
        try:
            obj = decoder.raw_decode(text, match.start())[0]
        except ValueError:
            continue
        rows = obj if isinstance(obj, list) else [obj]
        found = {row["id"]: row for row in rows if isinstance(row, dict) and isinstance(row.get("id"), str) and row["id"]}
        if found:
            return found
    return {}


def widget_snapshot_path() -> Path:
    """The usage widget's own last result (read-only for us): %LOCALAPPDATA%\\UsageWidget\\last.json."""
    base = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
    return Path(base) / "UsageWidget" / "last.json"


def _read_widget_snapshot(max_age: float) -> dict:
    path = widget_snapshot_path()
    try:
        fetched_at = path.stat().st_mtime
        if time.time() - fetched_at >= max_age:
            return {}
        entries = _parse_usage(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return {}
    if _stale_usage_ids(fetched_at).intersection(entries):
        return {}                          # the widget's result predates a login of one of these accounts: refetch
    for entry in entries.values():
        entry.pop("sourceKey", None)       # the widget's credential-derived dedupe key: not needed, never passed on
    return entries


def usage(refresh: bool = False) -> dict:
    """``{usage_id: {id, provider, windows, error, stale, ...}}``; never raises.

    Order: fresh own cache, then the widget's snapshot (when younger than twice the cache TTL), then
    Get-Usage.ps1. A failed or timed-out run is not retried for 60 s. ``refresh`` skips all three shortcuts.
    An account whose credential file changed after the cache was fetched has its cached entry ignored:
    the own cache and the snapshot are bypassed for it and Get-Usage.ps1 runs.
    """
    ttl = _usage_ttl()
    raw = _read_usage_file()
    data = raw.get("data")
    cached = data if isinstance(data, dict) else {}
    fetched_at = _as_float(raw.get("fetched_at"))
    now = time.time()
    if not refresh:
        if cached and now - fetched_at < ttl and not _stale_usage_ids(fetched_at):
            return cached
        snapshot = _read_widget_snapshot(ttl * SNAPSHOT_MAX_AGE_FACTOR)
        if snapshot:
            return snapshot
        if now - _as_float(raw.get("failed_at")) < NEGATIVE_CACHE_SECONDS:
            return _without_stale(cached, fetched_at)
    try:
        text = _run_usage_script()
        if text is None:
            return _without_stale(cached, fetched_at)      # nothing configured: not a failure
        fresh = _parse_usage(text)
    except Exception:  # noqa: BLE001 - usage is advisory; callers fall back to the cache
        fresh = {}
    try:
        if fresh:
            _write_usage_cache(fresh)
        else:
            _write_usage_cache(cached, fetched_at=fetched_at, failed_at=time.time())
    except OSError:
        pass
    return fresh or _without_stale(cached, fetched_at)


def usage_id(vendor: str, account: str) -> str | None:
    value = config.accounts_config().get(vendor, {}).get(account, {}).get("usage_id")
    return value if isinstance(value, str) and value else None


def _entry_age(entry: dict) -> float | None:
    """Seconds since the entry's ``fetchedAt`` (ISO 8601), or None when absent/unparseable."""
    value = entry.get("fetchedAt")
    if not isinstance(value, str):
        return None
    try:
        return time.time() - datetime.fromisoformat(value).timestamp()
    except ValueError:
        return None


def utilisation(vendor: str, account: str, data: dict | None = None) -> int | None:
    """Highest percent over the account's session/weekly/primary/secondary windows, or None.

    None as well when the entry reports an error, is flagged stale, or was fetched over 30 minutes ago:
    a remembered number is not a reason to switch accounts.
    """
    uid = usage_id(vendor, account)
    if uid is None:
        return None
    entry = (usage() if data is None else data).get(uid)
    if not isinstance(entry, dict) or entry.get("error") or entry.get("stale"):
        return None
    age = _entry_age(entry)
    if age is not None and age > STALE_ENTRY_SECONDS:
        return None
    best: int | None = None
    for window in entry.get("windows") or []:
        if not isinstance(window, dict) or window.get("key") not in UTILISATION_KEYS:
            continue
        percent = window.get("percent")
        if isinstance(percent, (int, float)) and not isinstance(percent, bool):
            value = int(round(percent))
            best = value if best is None else max(best, value)
    return best


def logged_in(vendor: str, account: str) -> bool:
    """True when the account's credential file exists (contents are never read)."""
    return (config.account_dir(vendor, account) / CREDENTIAL_FILE[vendor]).is_file()


def login_expiry(vendor: str, account: str) -> tuple[float | None, float | None]:
    """(refresh-token expiry, access-token expiry) as epoch seconds, ``None`` where unknown.

    Claude only: reads ``claudeAiOauth.refreshTokenExpiresAt`` and ``claudeAiOauth.expiresAt``
    (epoch ms) from the account's credentials file and nothing else in it. Codex's auth.json has
    no expiry field baton relies on, so it returns ``(None, None)``. Never raises.
    """
    if vendor != "claude":
        return None, None
    try:
        with open(config.account_dir(vendor, account) / CREDENTIAL_FILE[vendor], encoding="utf-8") as fh:
            oauth = json.load(fh).get("claudeAiOauth")
    except (OSError, ValueError, AttributeError):
        return None, None
    if not isinstance(oauth, dict):
        return None, None

    def seconds(key: str) -> float | None:
        value = oauth.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            return None
        return float(value) / 1000

    return seconds("refreshTokenExpiresAt"), seconds("expiresAt")


def login_expired(vendor: str, account: str, now: float | None = None) -> bool:
    """True when the account's refresh token is known to have expired (it needs an interactive login)."""
    refresh = login_expiry(vendor, account)[0]
    return refresh is not None and refresh <= (time.time() if now is None else now)


def login_remedy(vendor: str, account: str) -> str:
    """The command that logs an account in again."""
    if vendor != "claude":
        return "run `codex login`" if account == "main" else f"run `codex login` with CODEX_HOME set to {config.account_dir(vendor, account)}"
    if account == "main":
        return "run `claude auth login`"
    return f'run `$env:CLAUDE_CONFIG_DIR="{config.account_dir(vendor, account)}"; claude auth login; Remove-Item Env:CLAUDE_CONFIG_DIR`'


def fmt_date(epoch: float | None) -> str:
    return "unknown" if epoch is None else time.strftime("%Y-%m-%d", time.localtime(epoch))


# --------------------------------------------------------------------- linking

class _Run:
    """Collects actions; performs them unless ``dry_run``. Failures become ``error:`` entries.

    ``written`` maps a copied file's name to the sha256 baton last wrote there (persisted in sync.json).
    """

    def __init__(self, dry_run: bool, written: dict | None = None):
        self.dry_run = dry_run
        self.actions: list[str] = []
        self.written = written if written is not None else {}
        self.written_changed = False

    def do(self, message: str, fn=None) -> bool:
        if self.dry_run:
            self.actions.append(f"would {message}")
            return True
        try:
            if fn is not None:
                fn()
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            self.actions.append(f"error: {message}: {exc}")
            return False
        self.actions.append(message)
        return True

    def note(self, message: str) -> None:
        self.actions.append(message)

    def record(self, name: str, digest: str) -> None:
        if not self.dry_run and self.written.get(name) != digest:
            self.written[name] = digest
            self.written_changed = True


def has_errors(actions: list[str]) -> bool:
    return any(a.startswith("error:") for a in actions)


def _is_junction(path: Path) -> bool:
    try:
        return os.path.isjunction(path)
    except (OSError, AttributeError):
        return False


def _strip_extended(value: str) -> str:
    return value[4:] if value.startswith("\\\\?\\") else value


def _same_target(link: Path, target: Path) -> bool:
    try:
        resolved = _strip_extended(os.readlink(link))
    except OSError:
        resolved = os.path.realpath(link)
    return config.norm(resolved) == config.norm(target)


def _mklink_junction(link: Path, target: Path) -> None:
    cp = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(target)],
        capture_output=True,
        text=True,
        errors="replace",
        timeout=30,
        stdin=subprocess.DEVNULL,
        creationflags=config.CREATE_NO_WINDOW,
    )
    if cp.returncode != 0:
        raise OSError((cp.stdout + cp.stderr).strip() or f"mklink exited {cp.returncode}")


def _backup_name(path: Path, stamp: str) -> Path:
    candidate = path.with_name(f"{path.name}.pre-baton-{stamp}")
    n = 1
    while os.path.lexists(candidate):
        n += 1
        candidate = path.with_name(f"{path.name}.pre-baton-{stamp}-{n}")
    return candidate


def _real(path: Path) -> str:
    return config.norm(os.path.realpath(path))


def _inside(child: str, parent: str) -> bool:
    return child != parent and child.startswith(parent.rstrip("\\/") + os.sep)


def _alias_problem(primary: Path, second: Path) -> str | None:
    """Why linking primary -> second would loop back on itself (or None). Checked before any change."""
    if os.path.lexists(second) and (_is_junction(second) or second.is_symlink()):
        return f"{second} is itself a junction or symlink (it would resolve to {_real(second)})"
    real_primary, real_second = _real(primary), _real(second)
    if real_primary == real_second:
        return f"{primary} and {second} are the same directory"
    if _inside(real_primary, real_second) or _inside(real_second, real_primary):
        return f"{primary} and {second} overlap (one lies inside the other)"
    return None


def _target_problem(target: Path, second: Path) -> str | None:
    """A junction target must exist and must not resolve into the second account's own tree."""
    if not target.exists():
        return f"{target} does not exist"
    real, real_second = _real(target), _real(second)
    if real == real_second or _inside(real, real_second):
        return f"{target} resolves to {real}, inside the second account"
    return None


def _ensure_junction(run: _Run, link: Path, target: Path, stamp: str, second: Path) -> None:
    if os.path.lexists(link):
        if _is_junction(link):
            if _same_target(link, target):
                return
            if not run.do(f"remove stale junction {link.name}", lambda: os.rmdir(link)):
                return
        elif link.is_dir() and not link.is_symlink():
            if not any(link.iterdir()):
                if not run.do(f"remove empty dir {link.name}", lambda: os.rmdir(link)):
                    return
            else:
                backup = _backup_name(link, stamp)
                if not run.do(f"rename {link.name} -> {backup.name}", lambda: os.rename(link, backup)):
                    return
        else:
            backup = _backup_name(link, stamp)
            if not run.do(f"rename {link.name} -> {backup.name}", lambda: os.rename(link, backup)):
                return

    def make() -> None:
        # Re-check after the rename/rmdir above: they can change what the target resolves to.
        problem = _target_problem(target, second)
        if problem:
            raise OSError(problem)
        _mklink_junction(link, target)

    run.do(f"junction {link.name} -> {target}", make)


def _link_junctions(run: _Run, primary: Path, second: Path, names, create_in_primary=()) -> None:
    stamp = datetime.now().strftime("%Y%m%d%H%M")
    for name in names:
        target = primary / name
        would_create = False
        if not target.exists():
            if name not in create_in_primary:
                continue
            if not run.do(f"mkdir {target}", lambda t=target: t.mkdir(parents=True, exist_ok=True)):
                continue
            would_create = run.dry_run
        if not would_create:
            problem = _target_problem(target, second)
            if problem:
                run.note(f"error: junction {name}: {problem}")
                continue
        _ensure_junction(run, second / name, target, stamp, second)


def _atomic_copy(src: Path, dst: Path) -> None:
    tmp = _tmp_name(dst)
    try:
        shutil.copy2(src, tmp)
        os.replace(tmp, dst)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_bytes(data: bytes, dst: Path) -> None:
    tmp = _tmp_name(dst)
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
        os.replace(tmp, dst)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


BACKUPS_KEPT = 3


def _prune_backups(run: _Run, dst: Path) -> None:
    """Keep the newest ``BACKUPS_KEPT`` timestamped ``<name>.pre-baton-*`` backups of ``dst``; the first-overwrite
    ``<name>.pre-baton`` is never touched."""
    if run.dry_run:
        return
    try:
        old = sorted(dst.parent.glob(f"{dst.name}.pre-baton-*"), key=lambda p: (p.stat().st_mtime, p.name), reverse=True)
    except OSError:
        return
    for stale in old[BACKUPS_KEPT:]:
        run.do(f"remove old backup {stale.name}", stale.unlink)


def _copy_if_different(run: _Run, src: Path, dst: Path, content: bytes | None = None) -> None:
    """Copy ``src`` over ``dst`` when the content differs, keeping whatever the second account had.

    The first overwrite of a file keeps one ``<name>.pre-baton`` backup. Later overwrites compare ``dst``
    with the hash baton recorded when it last wrote it; a mismatch means someone edited it locally, so that
    version is saved as ``<name>.pre-baton-<YYYYmmddHHMMSS>`` first.
    """
    if src.name in PROTECTED_FILES or not src.is_file():
        return
    name = src.name
    new = hashlib.sha256(content).hexdigest() if content is not None else _sha256(src)
    recorded = run.written.get(name)
    if dst.exists():
        current = _sha256(dst)
        if current == new:
            run.record(name, new)          # already identical; remember it so later local edits are noticed
            return
        if recorded is None:
            backup = dst.with_name(f"{dst.name}.pre-baton")
            if not backup.exists() and not run.do(f"backup {dst.name} -> {backup.name}", lambda: shutil.copy2(dst, backup)):
                return
        elif recorded != current:
            backup = _backup_name(dst, datetime.now().strftime("%Y%m%d%H%M%S"))
            if not run.do(f"backup locally edited {dst.name} -> {backup.name}", lambda: shutil.copy2(dst, backup)):
                return
            run.note(f"note: {dst.name} had local edits in the second account; saved as {backup.name} before syncing")
            _prune_backups(run, dst)
    write = (lambda: _atomic_write_bytes(content, dst)) if content is not None else (lambda: _atomic_copy(src, dst))
    if run.do(f"copy {name}", write):
        run.record(name, new)


def _is_superset(have, want) -> bool:
    if isinstance(have, list) and isinstance(want, list):
        return all(item in have for item in want)
    if isinstance(have, dict) and isinstance(want, dict):
        return all(k in have and have[k] == v for k, v in want.items())
    return have == want


def _merge_project(have: dict, want: dict) -> bool:
    """Fold the primary's per-project settings into the second's without discarding the second's own."""
    changed = False
    for key in CLAUDE_PROJECT_KEYS:
        if key not in want:
            continue
        new = want[key]
        if key not in have:
            have[key] = copy.deepcopy(new)
            changed = True
            continue
        cur = have[key]
        if key in ("hasTrustDialogAccepted", "hasCompletedProjectOnboarding"):
            if new is True and cur is not True:        # a trust / onboarding grant is never revoked
                have[key] = True
                changed = True
        elif key == "allowedTools" and isinstance(cur, list) and isinstance(new, list):
            extra = [item for item in new if item not in cur]
            if extra:
                cur.extend(copy.deepcopy(extra))       # ordered union: the second's order, then the primary's additions
                changed = True
        elif cur != new and not _is_superset(cur, new):
            have[key] = {**cur, **copy.deepcopy(new)} if isinstance(cur, dict) and isinstance(new, dict) else copy.deepcopy(new)
            changed = True
    return changed


def merge_claude_json(prim: dict, sec: dict) -> list[str]:
    """Merge the primary's shareable settings into ``sec`` in place; returns what changed."""
    changed: list[str] = []
    servers = prim.get("mcpServers")
    if isinstance(servers, dict):
        mine = sec.get("mcpServers") if isinstance(sec.get("mcpServers"), dict) else {}
        merged = {**mine, **copy.deepcopy(servers)}    # the primary wins per server; the second's own servers stay
        if merged != sec.get("mcpServers"):
            sec["mcpServers"] = merged
            changed.append("mcpServers")
    projects = prim.get("projects")
    if isinstance(projects, dict):
        touched = 0
        for path, pvalue in projects.items():
            if not isinstance(pvalue, dict):
                continue
            wanted = {k: pvalue[k] for k in CLAUDE_PROJECT_KEYS if k in pvalue}
            if not wanted:
                continue
            if not isinstance(sec.get("projects"), dict):
                sec["projects"] = {}
            have = sec["projects"].get(path)
            created = not isinstance(have, dict)
            have = {} if created else have
            if _merge_project(have, wanted):
                sec["projects"][path] = have
                touched += 1
        if touched:
            changed.append(f"{touched} project{'s' if touched != 1 else ''}")
    return changed


def _link_claude_json(run: _Run, primary_json: Path, second_json: Path) -> None:
    if not primary_json.is_file():
        return
    if not second_json.is_file():
        run.note(f"skip {second_json.name}: the second account has none yet (log in once, then re-run)")
        return
    try:
        prim = json.loads(primary_json.read_text(encoding="utf-8"))
        sec = json.loads(second_json.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        run.note(f"error: {second_json.name}: cannot parse ({exc})")
        return
    if not isinstance(prim, dict) or not isinstance(sec, dict):
        return
    changed = merge_claude_json(prim, sec)
    if not changed:
        return
    backup = second_json.with_name(f"{second_json.name}.pre-baton")
    if not backup.exists() and not run.do(f"backup {second_json.name} -> {backup.name}", lambda: shutil.copy2(second_json, backup)):
        return
    run.do(f"update {second_json.name} ({', '.join(changed)})", lambda: _atomic_json(second_json, sec))


def _toml_header(line: str) -> tuple[str, ...] | None:
    """The key path of a TOML table header line (``[projects.'c:\\x']`` -> ('projects', 'c:\\x')), else None."""
    text = line.strip()
    if not text.startswith("[") or text.startswith("[["):
        return None
    try:
        node = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return None
    key: list[str] = []
    while isinstance(node, dict) and len(node) == 1:
        ((name, node),) = node.items()
        key.append(name)
    return tuple(key) if key and node == {} else None


def _toml_sections(text: str) -> list[tuple[tuple[str, ...] | None, list[str]]]:
    """(header key or None for the preamble, lines including the header) per table of a TOML file."""
    sections: list[tuple[tuple[str, ...] | None, list[str]]] = [(None, [])]
    for line in text.splitlines(keepends=True):
        key = _toml_header(line)
        if key is not None:
            sections.append((key, [line]))
        else:
            sections[-1][1].append(line)
    return sections


def _toml_scalar(value) -> str | None:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    return None


def merge_codex_config(primary_text: str, second_text: str) -> tuple[str, list[str]]:
    """The primary's config.toml with the second home's own tables carried over (union): ``[projects.*]`` (folder
    trust) and ``[hooks.state.*]`` (the hook approvals the owner gave in that home, keyed by its hooks.json). A table the
    primary lacks is appended as it is; in a table both have, scalar keys only the second has are added. Only text
    that already exists in the second home's file is written, nothing is invented. Returns (text, tables changed)."""
    try:
        tomllib.loads(second_text)
        tomllib.loads(primary_text)
    except tomllib.TOMLDecodeError:
        return primary_text, []
    mine = _toml_sections(primary_text)
    index = {key: n for n, (key, _) in enumerate(mine) if key is not None}
    if mine[-1][1] and not mine[-1][1][-1].endswith("\n"):
        mine[-1][1][-1] += "\n"
    changed: list[str] = []
    for key, lines in _toml_sections(second_text):
        if not key or not (key[0] == "projects" or key[:2] == ("hooks", "state")):
            continue
        label = ".".join(key[1:]) or "projects"
        if key not in index:
            block = list(lines)
            if block and not block[-1].endswith("\n"):
                block[-1] += "\n"
            tail = mine[-1][1]
            mine.append((key, ([] if not tail or tail[-1].strip() == "" else ["\n"]) + block))
            index[key] = len(mine) - 1
            changed.append(label)
            continue
        have = tomllib.loads("".join(mine[index[key]][1]))
        want = tomllib.loads("".join(lines))
        for part in key:
            have, want = have.get(part, {}), want.get(part, {})
        extra = []
        for name, value in want.items():
            text = _toml_scalar(value)
            if name not in have and text is not None:
                extra.append(f"{name} = {text}\n")
        if extra:
            mine[index[key]][1][1:1] = extra
            changed.append(label)
    return "".join("".join(lines) for _, lines in mine), changed


def _link_codex_config(run: _Run, src: Path, dst: Path) -> None:
    """Sync the primary's config.toml to the second home without dropping the second's own ``[projects.*]`` and
    ``[hooks.state.*]`` tables (a wholesale copy revoked the hook approvals given in the second home)."""
    if src.name in PROTECTED_FILES or not src.is_file():
        return
    try:
        text = src.read_text(encoding="utf-8")
        mine = dst.read_text(encoding="utf-8") if dst.is_file() else ""
    except (OSError, UnicodeDecodeError) as exc:
        run.note(f"error: {src.name}: cannot read ({exc})")
        return
    merged, changed = merge_codex_config(text, mine)
    if changed and merged != mine:
        run.note(f"keeping {len(changed)} project/hook table(s) of the second account's {dst.name}: " + ", ".join(changed[:6]))
    _copy_if_different(run, src, dst, content=merged.encode("utf-8"))


def _codex_files(primary: Path) -> list[Path]:
    names: set[str] = set()
    for pattern in CODEX_FILE_PATTERNS:
        try:
            names.update(p.name for p in primary.glob(pattern) if p.is_file())
        except OSError:
            pass
    return [primary / n for n in sorted(names)]


def _paths(vendor: str, account: str, primary, second, primary_json):
    if vendor not in config.VENDORS:
        raise ValueError(f"unknown vendor {vendor!r}")
    primary = Path(primary) if primary else config.account_dir(vendor, "main")
    second = Path(second) if second else config.account_dir(vendor, account)
    if primary_json is None and vendor == "claude":
        primary_json = Path.home() / ".claude.json"
    return primary, second, (Path(primary_json) if primary_json else None)


def _sync_path() -> Path:
    return config.STATE_DIR / "sync.json"


def _read_json(path: Path) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def link(vendor: str, account: str, dry_run: bool = False, *, primary=None, second=None, primary_json=None) -> list[str]:
    """Share the primary account's config with ``account``; returns the actions taken.

    Refuses (one ``error:`` action, nothing changed) when the two directories alias each other.
    ``primary`` / ``second`` / ``primary_json`` default from the configuration and
    exist so tests can use temporary directories.
    """
    primary, second, primary_json = _paths(vendor, account, primary, second, primary_json)
    key = f"{vendor}/{account}"
    written = _read_json(_sync_path()).get("written")
    written = written.get(key) if isinstance(written, dict) else None
    run = _Run(dry_run, dict(written) if isinstance(written, dict) else {})
    if account == "main" or config.norm(primary) == config.norm(second):
        run.note(f"{vendor}:{account} is the primary account; nothing to link")
        return run.actions
    if not primary.is_dir():
        run.note(f"error: primary directory {primary} does not exist")
        return run.actions
    problem = _alias_problem(primary, second)
    if problem:
        run.note(f"error: refusing to link {vendor}:{account}: {problem}")
        return run.actions
    if not second.is_dir():
        run.do(f"mkdir {second}", lambda: second.mkdir(parents=True, exist_ok=True))
        if run.actions[-1].startswith("error:"):
            return run.actions
    if vendor == "claude":
        _link_junctions(run, primary, second, CLAUDE_JUNCTIONS, CLAUDE_CREATE_IN_PRIMARY)
        for name in CLAUDE_FILES:
            _copy_if_different(run, primary / name, second / name)
        _link_claude_json(run, primary_json, second / ".claude.json")
    else:
        _link_junctions(run, primary, second, CODEX_JUNCTIONS)
        for src in _codex_files(primary):
            if src.name == "config.toml":
                _link_codex_config(run, src, second / src.name)
            else:
                _copy_if_different(run, src, second / src.name)
    if run.written_changed:
        try:
            state = _read_json(_sync_path())                  # fresh: another process may have written meanwhile
            if not isinstance(state.get("written"), dict):
                state["written"] = {}
            state["written"][key] = run.written
            _atomic_json(_sync_path(), state)
        except OSError as exc:
            run.note(f"error: could not record what was written to {second}: {exc}")
    return run.actions


# ------------------------------------------------------------------------ sync

def _stamp(path: Path):
    try:
        st = path.stat()
    except OSError:
        return None
    return [st.st_mtime_ns, st.st_size]


def _watched_files(vendor: str, primary: Path, primary_json: Path | None) -> list[Path]:
    if vendor == "claude":
        return [primary / n for n in CLAUDE_FILES] + ([primary_json] if primary_json else [])
    return _codex_files(primary)


def _fingerprint(vendor: str, primary: Path, second: Path, primary_json: Path | None, files: dict | None = None) -> dict:
    names = CLAUDE_JUNCTIONS if vendor == "claude" else CODEX_JUNCTIONS
    return {
        "files": files if files is not None else {str(p): _stamp(p) for p in _watched_files(vendor, primary, primary_json)},
        # which junctions are expected and in place (cheap lstat calls)
        "junctions": [n for n in names if (primary / n).exists() and _is_junction(second / n)],
        "targets": [n for n in names if (primary / n).exists()],
        "second_json": (second / ".claude.json").exists() if vendor == "claude" else None,
    }


def sync(vendor: str, account: str, *, primary=None, second=None, primary_json=None) -> list[str]:
    """Silent, cheap ``link``: skipped while the primary's files are unchanged since the last sync."""
    if account == "main":
        return []
    try:
        primary, second, primary_json = _paths(vendor, account, primary, second, primary_json)
        if not primary.is_dir():
            return []
        key = f"{vendor}/{account}"
        if _read_json(_sync_path()).get(key) == _fingerprint(vendor, primary, second, primary_json):
            return []
        # Stamp the primary's files before linking: an edit made while link() runs then shows up as a change next time.
        files = {str(p): _stamp(p) for p in _watched_files(vendor, primary, primary_json)}
        actions = link(vendor, account, primary=primary, second=second, primary_json=primary_json)
        if not has_errors(actions):
            state = _read_json(_sync_path())                  # link() may have recorded written hashes
            state[key] = _fingerprint(vendor, primary, second, primary_json, files=files)
            _atomic_json(_sync_path(), state)
        return actions
    except Exception as exc:  # noqa: BLE001 - never block a launch on a sync problem
        return [f"error: sync {vendor}:{account}: {exc}"]


# ------------------------------------------------------------- add codex account

def _bracket_end(text: str, start: int) -> int:
    """Index of the ``]`` matching the ``[`` at ``start`` (string-aware)."""
    depth, i, in_str = 0, start, False
    while i < len(text):
        ch = text[i]
        if in_str:
            if ch == "\\":
                i += 1
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def widget_add_account(path: Path, entry: dict) -> bool:
    """Append ``entry`` to ``accounts`` in the widget config unless its id exists. True if the file changed.

    The file is PowerShell-formatted JSON; the entry is inserted textually so the
    rest of the file (indentation, CRLF, key spacing) is left byte-for-byte alone.
    """
    raw = path.read_bytes()
    text = raw.decode("utf-8")
    data = json.loads(text.lstrip("\ufeff"))
    existing = data.get("accounts")
    if not isinstance(existing, list):
        raise ValueError(f"{path}: 'accounts' is not a list")
    if any(isinstance(a, dict) and a.get("id") == entry["id"] for a in existing):
        return False
    new_text = None
    match = re.search(r'"accounts"\s*:\s*\[', text)
    if match and existing:
        close = _bracket_end(text, match.end() - 1)
        last = len(text[:close].rstrip())
        if close > 0 and text[last - 1] == "}":
            nl = "\r\n" if "\r\n" in text else "\n"
            line_start = text.rfind("\n", 0, last) + 1
            outer = re.match(r"[ \t]*", text[line_start:last]).group(0)
            inner = outer + "    "
            colon = ":  " if '":  ' in text else ": "
            body = f",{nl}".join(f"{inner}{json.dumps(k)}{colon}{json.dumps(v)}" for k, v in entry.items())
            new_text = f"{text[:last]},{nl}{outer}{{{nl}{body}{nl}{outer}}}{text[last:]}"
            try:
                if json.loads(new_text.lstrip("\ufeff")).get("accounts", [])[-1] != entry:
                    new_text = None
            except ValueError:
                new_text = None
    if new_text is None:
        data["accounts"] = [*existing, entry]
        new_text = json.dumps(data, indent=4)
    tmp = _tmp_name(path)
    try:
        tmp.write_bytes(new_text.encode("utf-8"))
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
    return True


def _widget_auth_path(home: Path) -> str:
    try:
        rel = home.resolve().relative_to(Path.home().resolve())
        return "%USERPROFILE%\\" + str(rel / "auth.json").replace("/", "\\")
    except (ValueError, OSError):
        return str(home / "auth.json").replace("/", "\\")


def _register_local(vendor: str, name: str) -> bool:
    """Make a new account known to baton via the machine-local overlay (accounts.local.toml)."""
    if name in config.accounts_config().get(vendor, {}):
        return False
    path = config.STATE_DIR / "accounts.local.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    sep = "" if not existing or existing.endswith("\n") else "\n"
    block = f'[{vendor}.{name}]\nhome = "~/.codex-{name}"\nusage_id = "chatgpt-{name}"\n'
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(f"{sep}{block}")
    return True


def add_codex_account(name: str, widget: bool = False, *, widget_config=None, out=print) -> int:
    """Create the account's Codex home, link it to the primary and print the login command."""
    if not _ACCOUNT_NAME.match(name) or name == "main":
        out(f"error: invalid account name {name!r} (letters, digits, '-' and '_'; not 'main')")
        return 2
    home = config.account_dir("codex", name)
    if _register_local("codex", name):
        out(f"registered codex:{name} in {config.STATE_DIR / 'accounts.local.toml'}")
    home.mkdir(parents=True, exist_ok=True)
    actions = link("codex", name)
    for action in actions:
        out(f"  {action}")
    if widget:
        entry = {
            "id": f"chatgpt-{name}",
            "provider": "chatgpt",
            "label": f"ChatGPT ({name})",
            "authPath": _widget_auth_path(home),
            "enabled": True,
            "allowTokenRefresh": True,
        }
        target = Path(widget_config) if widget_config else WIDGET_CONFIG
        try:
            changed = widget_add_account(target, entry)
            out(f"widget: {'added ' + entry['id'] + ' to' if changed else entry['id'] + ' already in'} {target}")
        except (OSError, ValueError) as exc:
            out(f"error: widget config {target}: {exc}")
            return 1
    out("Log in to the new account (run in PowerShell):")
    out(f'$env:CODEX_HOME="{home}"; codex login; Remove-Item Env:CODEX_HOME')
    return 1 if has_errors(actions) else 0


# ------------------------------------------------------------- identity and login

def _identity_cache_path() -> Path:
    return config.STATE_DIR / "account-identity.json"


def _claude_email(account: str) -> str | None:
    """``email`` from ``claude auth status --json`` run as ``account``; None when logged out or unknown. Never raises."""
    try:
        cp = subprocess.run([config.exe("claude"), "auth", "status", "--json"], env=account_env("claude", account),
                            cwd=tempfile.gettempdir(), capture_output=True, text=True, errors="replace",
                            timeout=STATUS_TIMEOUT, stdin=subprocess.DEVNULL, creationflags=config.CREATE_NO_WINDOW)
        status = json.loads(cp.stdout or "")
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    if not isinstance(status, dict) or status.get("loggedIn") is not True:
        return None
    email = status.get("email")
    return email if isinstance(email, str) and email else None


def _cached_email(vendor: str, account: str) -> str | None:
    if vendor != "claude":
        return None
    entry = _read_json(_identity_cache_path()).get(f"{vendor}:{account}")
    if not isinstance(entry, dict) or entry.get("dir") != str(config.account_dir(vendor, account)):
        return None
    if _credential_newer(vendor, account, _as_float(entry.get("fetched_at"))):
        return None                                # logged in again since: the remembered email may be another one
    return entry.get("email") if isinstance(entry.get("email"), str) else None


def account_email(vendor: str, account: str, refresh: bool = False) -> str | None:
    """Login email of a logged-in account, or None.

    Claude only: the status call is cached per account for ``usage_cache_seconds`` (``refresh`` bypasses the cache),
    and an entry older than the account's credential file is not used. Codex always returns None: its email sits
    in the ``id_token`` JWT of auth.json, which baton never decodes.
    """
    if vendor != "claude" or not logged_in(vendor, account):
        return None
    key, home, now = f"{vendor}:{account}", str(config.account_dir(vendor, account)), time.time()
    entry = _read_json(_identity_cache_path()).get(key)
    entry = entry if isinstance(entry, dict) and entry.get("dir") == home else {}
    fetched_at = _as_float(entry.get("fetched_at"))
    if not refresh and entry and now - fetched_at < _usage_ttl() and not _credential_newer(vendor, account, fetched_at):
        return entry.get("email") if isinstance(entry.get("email"), str) else None
    email = _claude_email(account)
    try:
        cache = _read_json(_identity_cache_path())            # fresh read: another process may have written meanwhile
        cache[key] = {"dir": home, "fetched_at": now, "email": email}
        _atomic_json(_identity_cache_path(), cache)
    except OSError:
        pass
    return email


def _sessions_files(directory: Path, pattern: str) -> list[Path]:
    try:
        return sorted(p for p in directory.glob(pattern) if p.is_file())
    except OSError:
        return []


def live_sessions(vendor: str, account: str) -> int:
    """Running sessions on the account's directory.

    Claude: ``sessions/<pid>.json`` whose pid is alive. Codex: ``thread-writer-locks/*.lock`` still held by a running Codex.
    """
    home = config.account_dir(vendor, account)
    if vendor == "claude":
        count = 0
        for f in _sessions_files(home / "sessions", "*.json"):
            try:
                count += _pid_alive(int(_read_json(f).get("pid") or f.stem))
            except (TypeError, ValueError):
                continue
        return count
    from .startup import _lock_held          # startup imports launch, which imports this module
    return sum(1 for f in _sessions_files(home / "thread-writer-locks", "*.lock") if _lock_held(f))


def login_refusal(vendor: str, account: str, force: bool = False) -> str | None:
    """Why ``vendor:account`` must not log in right now, or None when it may.

    A login replaces the credential file that every session on the directory reads. A logged-in account is
    therefore refused unless ``force``, and even then while any session is still running on it.
    """
    if not logged_in(vendor, account):
        return None
    label, home = f"{vendor}:{account}", config.account_dir(vendor, account)
    live = live_sessions(vendor, account)
    if force and not live:
        return None
    email = _cached_email(vendor, account)          # cache only: a refusal must not start another CLI call
    who = f" ({email})" if email else ""
    if force:
        return (f"{label} is already logged in{who} and {live} live session(s) use {home}; "
                "--force does not replace a login while sessions run on it. Close them first.")
    return (f"{label} is already logged in{who}; logging in again replaces that login for every session using {home}. "
            f"Use --force only if no session is running on it. {live} live session(s) found on {home}.")


def login_tab_command(vendor: str, account: str) -> str:
    """PowerShell the login tab runs: select the account's directory (none for main), vendor login, status, next steps."""
    def lit(text: str) -> str:
        return "'" + str(text).replace("'", "''") + "'"
    steps = ["claude auth login", "claude auth status"] if vendor == "claude" else ["codex login", "codex login status"]
    steps.append(f"Write-Host {lit(LOGIN_NEXT[vendor])}")
    prefix = []
    if account != "main":
        var = config.ACCOUNT_ENV[vendor]
        prefix.append(f"$env:{var}={lit(config.account_dir(vendor, account))}")
    return "; ".join(prefix + steps)


def login_wt_argv(vendor: str, account: str, wt: str = "wt.exe") -> list[str]:
    from .startup import encode_command      # startup imports launch, which imports this module
    return [wt, "-w", "0", "new-tab", "--title", f"login {vendor}:{account}", "-d", str(Path.home()),
            "pwsh", "-NoExit", "-EncodedCommand", encode_command(login_tab_command(vendor, account))]


def account_login(vendor: str, account: str, *, force: bool = False, dry_run: bool = False, out=print) -> int:
    """Open one Windows Terminal tab that logs ``vendor:account`` in; refuse when that would disturb a running session."""
    names = config.account_names(vendor) if vendor in config.VENDORS else []
    if account not in names:
        out(f"unknown {vendor} account {account!r}; configured: {', '.join(names) or 'none'}")
        return 2
    refusal = login_refusal(vendor, account, force)
    if refusal:
        out(f"refused: {refusal}")
        return 1
    home = config.account_dir(vendor, account)
    if dry_run:
        out(f"dry run: would open a login tab for {vendor}:{account} ({home})")
        out(f"  tab command: {login_tab_command(vendor, account)}")
        return 0
    try:
        home.mkdir(parents=True, exist_ok=True)
        env = config.clean_env()
        env.pop(config.ACCOUNT_ENV[vendor], None)     # the tab's own variable is set inside its command
        subprocess.Popen(login_wt_argv(vendor, account, shutil.which("wt.exe") or "wt.exe"), env=env,
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)
    except OSError as exc:
        out(f"error: could not open a login tab for {vendor}:{account}: {exc}")
        return 1
    out(f"opened a login tab for {vendor}:{account}; finish the login there")
    return 0


# ----------------------------------------------------------------------- labels

def _labels_path() -> Path:
    return config.STATE_DIR / "account-labels.json"


LABEL_SET_AT = "_set_at"      # account-labels.json: {"vendor.name": text, "_set_at": {"vendor.name": epoch seconds}}


def label_of(vendor: str, account: str) -> str | None:
    """The manual label set by ``bt account label`` ({"vendor.name": text} in account-labels.json), or None."""
    value = _read_json(_labels_path()).get(f"{vendor}.{account}")
    return value if isinstance(value, str) and value else None


def label_stale(vendor: str, account: str) -> bool:
    """True when the account logged in again after its label was set (the label may name the previous login).
    A label from before set times were kept is never reported stale."""
    stamps = _read_json(_labels_path()).get(LABEL_SET_AT)
    set_at = _as_float(stamps.get(f"{vendor}.{account}")) if isinstance(stamps, dict) else 0.0
    return bool(set_at) and label_of(vendor, account) is not None and _credential_newer(vendor, account, set_at)


def account_label(vendor: str, account: str, value: str | None = None, *, clear: bool = False, out=print) -> int:
    """Set the label of ``vendor:account`` to ``value``, or remove it with ``clear``. Exit code 0, 1 or 2."""
    names = config.account_names(vendor) if vendor in config.VENDORS else []
    if account not in names:
        out(f"unknown {vendor} account {account!r}; configured: {', '.join(names) or 'none'}")
        return 2
    text = (value or "").strip()
    if clear == bool(text):                        # exactly one of a label and --clear
        out(f"usage: bt account label {vendor} {account} <email-or-label>   |   bt account label {vendor} {account} --clear")
        return 2
    path, key = _labels_path(), f"{vendor}.{account}"
    try:
        labels = _read_json(path)
        stamps = labels.get(LABEL_SET_AT) if isinstance(labels.get(LABEL_SET_AT), dict) else {}
        if clear:
            labels.pop(key, None)
            stamps.pop(key, None)
            done = f"cleared the label of {vendor}:{account}"
        else:
            labels[key] = text
            stamps[key] = time.time()
            done = f"labelled {vendor}:{account} as {text}"
        if stamps:
            labels[LABEL_SET_AT] = stamps
        else:
            labels.pop(LABEL_SET_AT, None)
        _atomic_json(path, labels)
    except OSError as exc:
        out(f"error: could not write {path}: {exc}")
        return 1
    out(done)
    return 0


# -------------------------------------------------------------------------- CLI

def _short_dir(path: Path) -> str:
    try:
        return "~\\" + str(path.resolve().relative_to(Path.home().resolve()))
    except (ValueError, OSError):
        return str(path)


def accounts_rows(refresh: bool = False) -> list[dict]:
    data = usage(refresh=refresh)
    rows = []
    for vendor in config.VENDORS:
        for name in config.account_names(vendor):
            home = config.account_dir(vendor, name)
            entry = data.get(usage_id(vendor, name) or "", {})
            percent = utilisation(vendor, name, data)
            error = entry.get("error") if isinstance(entry, dict) else None
            rows.append({
                "vendor": vendor,
                "account": name,
                "dir": _short_dir(home),
                "logged_in": logged_in(vendor, name),
                "email": account_email(vendor, name, refresh=refresh),
                "label": label_of(vendor, name),
                "label_stale": label_stale(vendor, name),
                "utilisation": percent,
                "error": error or ("no usage data" if not entry else ""),
            })
    return rows


def format_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    lines = ["  ".join(h.ljust(w) for h, w in zip(headers, widths)).rstrip()]
    lines += ["  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip() for r in rows]
    return "\n".join(lines)


# ------------------------------------------------------------------- keep-alive

def keepalive_due(days: float = LOGIN_WARN_DAYS, only: str | None = None, now: float | None = None) -> list[tuple[str, float]]:
    """[(account, refresh expiry)] of Claude accounts whose refresh token expires within ``days`` but has not expired."""
    now = time.time() if now is None else now
    out = []
    for name in config.account_names("claude"):
        if (only and name != only) or not logged_in("claude", name):
            continue
        refresh = login_expiry("claude", name)[0]
        if refresh is not None and now < refresh <= now + days * 86400:
            out.append((name, refresh))
    return out


def keepalive_enabled() -> bool:
    return _policy().get("keepalive", True) is not False


def account_env(vendor: str, account: str) -> dict:
    """The parent session's variables removed and the account's config dir selected (``main``: the CLI default)."""
    env = config.clean_env()
    if account == "main":
        env.pop(config.ACCOUNT_ENV[vendor], None)
    else:
        env[config.ACCOUNT_ENV[vendor]] = str(config.account_dir(vendor, account))
    return env


def _keepalive_run(account: str) -> str:
    """One minimal ``claude -p`` call under the account's config dir; returns '' on success, else the problem."""
    env = account_env("claude", account)
    env[config.FOLLOWER_ENV] = "keepalive"          # hooks stay silent
    argv =[config.exe("claude"), "-p", "Reply OK", "--model", KEEPALIVE_MODEL, "--effort", "low",
            "--no-session-persistence", "--output-format", "text"]
    with tempfile.TemporaryDirectory(prefix="baton-keepalive-") as cwd:   # outside any repo
        try:
            cp = subprocess.run(argv, env=env, cwd=cwd, capture_output=True, text=True, errors="replace", timeout=KEEPALIVE_TIMEOUT,
                                stdin=subprocess.DEVNULL, creationflags=config.CREATE_NO_WINDOW)
        except subprocess.TimeoutExpired:
            return f"timed out after {KEEPALIVE_TIMEOUT} s"
        except OSError as exc:
            return f"could not run claude: {exc}"
    if cp.returncode != 0:
        tail = " ".join((cp.stderr or cp.stdout or "").split())[:160]
        return f"claude exited {cp.returncode}" + (f": {tail}" if tail else "")
    return ""


def keepalive(days: float = LOGIN_WARN_DAYS, only: str | None = None, dry_run: bool = False, out=print) -> int:
    """Refresh the login of each Claude account whose refresh token is about to expire (see ``keepalive_due``)."""
    if only and only not in config.account_names("claude"):
        out(f"unknown claude account {only!r}; configured: {', '.join(config.account_names('claude'))}")
        return 2
    due = keepalive_due(days, only)
    if not due:
        out(f"keepalive: no claude login expires within {days:g} days")
        return 0
    failed = 0
    for name, before in due:
        label = f"claude:{name}"
        if dry_run:
            out(f"would refresh {label} (refresh token expires {fmt_date(before)})")
            continue
        problem = _keepalive_run(name)
        after = login_expiry("claude", name)[0]
        if problem:
            failed += 1
            out(f"{label}: {problem}")
        elif after is None or after <= before:
            failed += 1
            out(f"{label}: ran, but the refresh token expiry did not move ({fmt_date(before)})")
        else:
            out(f"{label}: refresh token expiry {fmt_date(before)} -> {fmt_date(after)}")
    return 1 if failed else 0


def spawn_keepalive() -> bool:
    """Start ``bt keepalive`` detached from the caller's console, process group and job. Best effort."""
    argv = [config.PYTHON, "-S", str(config.BATON_HOME / "bt.py"), "keepalive"]
    try:
        procs.popen_detached(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             close_fds=True, cwd=str(config.BATON_HOME))
    except OSError:
        return False
    return True


def _cmd_keepalive(args) -> int:
    return keepalive(args.days, args.account, args.dry_run)


def _cmd_accounts(args) -> int:
    rows = accounts_rows(refresh=args.refresh)
    table, hinted = [], False
    for r in rows:
        email = r["email"] or r["label"]           # Claude: derived email first; Codex: its label (never derived)
        if not r["email"] and r["label"] and r["label_stale"]:
            email = f"{r['label']} (stale: logged in since; relabel)"
        if not email and r["vendor"] == "codex" and not hinted:
            email, hinted = f"- (bt account label codex {r['account']} <email>)", True
        table.append([r["vendor"], r["account"], r["dir"], "yes" if r["logged_in"] else "no", email or "-",
                      "-" if r["utilisation"] is None else f"{r['utilisation']}%", str(r["error"])[:70]])
    print(format_table(["vendor", "account", "dir", "logged-in", "email", "utilisation", "error"], table))
    return 0


def _cmd_account_link(args) -> int:
    actions = link(args.vendor, args.name, dry_run=args.dry_run)
    for action in actions:
        print(f"  {action}")
    if not actions:
        print("  already linked; nothing to do")
    return 1 if has_errors(actions) else 0


def _cmd_account_add(args) -> int:
    return add_codex_account(args.name, widget=args.widget)


def _cmd_account_login(args) -> int:
    return account_login(args.vendor, args.name, force=args.force, dry_run=args.dry_run)


def _cmd_account_label(args) -> int:
    return account_label(args.vendor, args.name, args.value, clear=args.clear)


def register(subparsers) -> None:
    p = subparsers.add_parser("accounts", help="list accounts with login state and usage")
    p.add_argument("--refresh", action="store_true", help="ignore the usage cache")
    p.set_defaults(func=_cmd_accounts)

    account = subparsers.add_parser("account", help="manage accounts")
    sub = account.add_subparsers(dest="account_cmd", required=True)
    p = sub.add_parser("link", help="share the primary account's config with another account")
    p.add_argument("vendor", choices=config.VENDORS)
    p.add_argument("name")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_cmd_account_link)
    p = sub.add_parser("add", help="add an account (Codex only)")
    p.add_argument("vendor", choices=["codex"])
    p.add_argument("name")
    p.add_argument("--widget", action="store_true", help="also add it to the usage widget config")
    p.set_defaults(func=_cmd_account_add)
    p = sub.add_parser("login", help="open a terminal tab that logs an account in (refuses while a session uses it)")
    p.add_argument("vendor", choices=config.VENDORS)
    p.add_argument("name")
    p.add_argument("--force", action="store_true", help="log in again although it is logged in (only with no session running on it)")
    p.add_argument("--dry-run", action="store_true", help="print the decision and the tab command; launch nothing")
    p.set_defaults(func=_cmd_account_login)
    p = sub.add_parser("label", help="set or clear the email/label shown for an account (Codex needs one)")
    p.add_argument("vendor", choices=config.VENDORS)
    p.add_argument("name")
    p.add_argument("value", nargs="?", help="the account's email or a label")
    p.add_argument("--clear", action="store_true", help="remove the label")
    p.set_defaults(func=_cmd_account_label)

    p = subparsers.add_parser("keepalive", help="refresh Claude logins whose refresh token is about to expire")
    p.add_argument("--account", help="only this Claude account")
    p.add_argument("--days", type=float, default=LOGIN_WARN_DAYS, help="act when the refresh token expires within this many days (default 5)")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=_cmd_keepalive)
