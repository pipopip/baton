"""Machine-wide record of the last session per worktree and vendor.

Written by hooks (SessionStart/SessionEnd) and read by the launcher. One JSON
file, replaced atomically; concurrent writers may lose an update, which only
costs a slightly stale "last session" hint.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from . import config, harvest, paths

REGISTRY_NAME = "registry.json"
ATTACHED_NAME = "attached.json"
ATTACHED_RETENTION = 14 * 86400
STARTUP_MAX_AGE = 7 * 86400              # a startup-list entry not refreshed for this long is dropped on the next write


def _path() -> Path:
    return config.STATE_DIR / REGISTRY_NAME


class RegistryUnavailable(RuntimeError):
    """The registry exists but could not be read; callers must not overwrite it."""


def load() -> dict:
    """Registry contents; empty only when the file does not exist yet.

    A read racing another process's replace can fail transiently on Windows, so
    retry; if it still fails, raise rather than let ``update`` save an empty copy.
    """
    for attempt in range(4):
        try:
            with open(_path(), encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                data.setdefault("worktrees", {})
                return data
            break
        except FileNotFoundError:
            return {"worktrees": {}}
        except (OSError, ValueError):
            time.sleep(0.05 * (attempt + 1))
    raise RegistryUnavailable(str(_path()))


def _load_quiet() -> dict:
    try:
        return load()
    except RegistryUnavailable:
        return {"worktrees": {}}


def _save(data: dict) -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1, sort_keys=True)  # ensure_ascii default: no encode errors
    for attempt in range(5):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            time.sleep(0.05 * (attempt + 1))
    try:
        os.remove(tmp)
    except OSError:
        pass


def entry(worktree: str | Path) -> dict:
    """Registry entry for a worktree: {'path', 'ws', 'last_vendor', 'claude': {...}, 'codex': {...}}."""
    return _load_quiet()["worktrees"].get(config.norm(worktree), {})


def session(worktree: str | Path, vendor: str) -> dict:
    """Last recorded session for (worktree, vendor): session_id, transcript, account, last_seen, ctx_tokens."""
    return entry(worktree).get(vendor, {})


def update(worktree: str | Path, vendor: str, ws: str | None = None, **fields) -> None:
    """Merge ``fields`` into the (worktree, vendor) session record and mark the vendor as last used."""
    data = load()
    key = config.norm(worktree)
    item = data["worktrees"].setdefault(key, {"path": str(worktree)})
    if ws:
        item["ws"] = ws
    item["last_vendor"] = vendor
    rec = item.setdefault(vendor, {})
    if fields.get("session_id") and rec.get("session_id") != fields["session_id"]:
        rec.clear()
    rec.update({k: v for k, v in fields.items() if v is not None})
    rec.setdefault("last_seen", time.time())
    _save(data)


def remove(worktree: str | Path) -> bool:
    """Drop a worktree's registry entry (``bt wt retire``). Returns True when there was one."""
    data = load()
    if data["worktrees"].pop(config.norm(worktree), None) is None:
        return False
    _save(data)
    return True


def detach_session(session_id: str, keep: str | Path, *, vendor: str | None = None,
                   account: str | None = None) -> list[str]:
    """Drop ``session_id`` from the vendor records of every worktree except ``keep`` (``bt attach`` moved it).

    A worktree left without any vendor record loses its entry; otherwise ``last_vendor`` falls back to a
    remaining vendor when it named the dropped one. Returns the paths of the worktrees touched.
    """
    data = load()
    keep_key = config.norm(keep)
    touched = []
    for key, item in list(data["worktrees"].items()):
        if key == keep_key or not isinstance(item, dict):
            continue
        dropped = False
        for record_vendor in config.VENDORS:
            rec = item.get(record_vendor)
            if (isinstance(rec, dict) and rec.get("session_id") == session_id
                    and (vendor is None or record_vendor == vendor)
                    and (account is None or rec.get("account") == account)):
                del item[record_vendor]
                dropped = True
        if not dropped:
            continue
        touched.append(str(item.get("path") or key))
        remaining = [v for v in config.VENDORS if isinstance(item.get(v), dict)]
        if not remaining:
            del data["worktrees"][key]
        elif item.get("last_vendor") not in remaining:
            item["last_vendor"] = remaining[0]
    if touched:
        _save(data)
    return touched


# ------------------------------------------------------------------- startup list
#
# startup.json is the list of workstreams to reopen after a reboot, maintained by the hooks: SessionStart adds
# or refreshes the entry of its worktree, SessionEnd removes it when this session owns it. A tab killed by a
# reboot fires no SessionEnd, so its entry stays: that is what ``bt startup run`` reopens.
# {"workstreams": [{path, key, vendor, account, session_id, updated_at}]}, one entry per worktree and per session.
# Pinned workstreams (startup.toml) are not in this file, so pruning never touches them.


def startup_path() -> Path:
    return config.STATE_DIR / "startup.json"


def startup_entries() -> list[dict]:
    """Entries of startup.json (dicts with a path); ``[]`` when missing or unreadable. Hooks call this, so it never raises."""
    try:
        with open(startup_path(), encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return []
    items = data.get("workstreams") if isinstance(data, dict) else None
    return [e for e in items if isinstance(e, dict) and isinstance(e.get("path"), str)] if isinstance(items, list) else []


def startup_save(entries: list[dict], now: float | None = None) -> None:
    now = time.time() if now is None else now
    harvest.atomic_write(startup_path(), json.dumps({"updated_at": now, "workstreams": entries}, indent=1))


def startup_add(worktree: str | Path, vendor: str, account: str, session_id: str | None,
                now: float | None = None, min_age: float = 0) -> bool:
    """Add or refresh the entry of ``worktree`` for this session, dropping the session's entry elsewhere.

    With ``min_age`` an unchanged entry younger than that many seconds is left alone (no write). A write also
    drops the entries whose directory is gone or that were last refreshed over ``STARTUP_MAX_AGE`` ago (or never).
    Returns whether it wrote.
    """
    now = time.time() if now is None else now
    wkey = config.norm(worktree)
    entries = startup_entries()
    mine = next((e for e in entries if config.norm(e["path"]) == wkey), None)
    if (min_age and mine is not None and mine.get("session_id") == session_id and mine.get("vendor") == vendor and mine.get("account") == account
            and isinstance(mine.get("updated_at"), (int, float)) and 0 <= now - mine["updated_at"] < min_age):
        return False
    kept = [e for e in entries if config.norm(e["path"]) != wkey and not (session_id and e.get("session_id") == session_id and e.get("vendor") == vendor and e.get("account") == account)
            and _startup_live(e, now)]
    kept.append({"path": str(worktree), "key": paths.key_for(Path(worktree)), "vendor": vendor, "account": account,
                 "session_id": session_id, "updated_at": now})
    startup_save(kept, now)
    return True


def _startup_live(entry: dict, now: float) -> bool:
    stamp = entry.get("updated_at")
    return (isinstance(stamp, (int, float)) and not isinstance(stamp, bool) and now - stamp <= STARTUP_MAX_AGE
            and os.path.isdir(entry["path"]))


def startup_forget(worktree: str | Path) -> bool:
    """Drop the worktree's entry whoever owns it (``bt wt retire`` removed the directory). Never raises."""
    wkey = config.norm(worktree)
    try:
        entries = startup_entries()
        kept = [e for e in entries if config.norm(e["path"]) != wkey]
        if len(kept) == len(entries):
            return False
        startup_save(kept)
        return True
    except OSError:
        return False


def startup_remove(worktree: str | Path, session_id: str | None, *,
                   vendor: str | None = None, account: str | None = None) -> bool:
    """Drop the worktree's entry when it belongs to ``session_id`` (a deliberate exit). Returns whether it removed one."""
    wkey = config.norm(worktree)
    entries = startup_entries()
    kept = [e for e in entries if not (config.norm(e["path"]) == wkey and session_id and e.get("session_id") == session_id
            and (vendor is None or e.get("vendor") == vendor) and (account is None or e.get("account") == account))]
    if len(kept) == len(entries):
        return False
    startup_save(kept)
    return True


def startup_drop_session(session_id: str, keep: str | Path | None = None, *,
                         vendor: str | None = None, account: str | None = None) -> bool:
    """Drop the entries of ``session_id`` outside ``keep`` (``bt attach`` moved the session)."""
    keep_key = config.norm(keep) if keep is not None else None
    entries = startup_entries()
    kept = [e for e in entries if not (e.get("session_id") == session_id and config.norm(e["path"]) != keep_key
            and (vendor is None or e.get("vendor") == vendor) and (account is None or e.get("account") == account))]
    if len(kept) == len(entries):
        return False
    startup_save(kept)
    return True


# ------------------------------------------------------------------- attached sessions
#
# ``bt attach`` maps a session that started outside a worktree (for example in C:/Work) to the
# worktree it works in, so hooks can resolve that worktree from the session id instead of the cwd.
# attached.json: {session_id: {worktree, vendor, account, transcript, attached_at}}.


def attached_path() -> Path:
    return config.STATE_DIR / ATTACHED_NAME


def attached_load() -> dict:
    """attached.json contents; ``{}`` when missing or unreadable. Hooks call this, so it never raises."""
    try:
        with open(attached_path(), encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def attached_worktree(session_id: str | None, data: dict | None = None) -> Path | None:
    """Worktree ``session_id`` was attached to, when that directory still exists."""
    if not isinstance(session_id, str) or not session_id:
        return None
    rec = (attached_load() if data is None else data).get(session_id)
    target = rec.get("worktree") if isinstance(rec, dict) else None
    if isinstance(target, str) and target and os.path.isdir(target):
        return Path(target)
    return None


def attached_worktrees() -> set[str]:
    """``config.norm`` keys of every worktree some attached session maps to."""
    return {config.norm(rec["worktree"]) for rec in attached_load().values()
            if isinstance(rec, dict) and isinstance(rec.get("worktree"), str) and rec["worktree"]}


def attach_record(session_id: str, worktree: str | Path, vendor: str, account: str,
                  transcript: str | None, now: float | None = None) -> None:
    """Add or replace one attached session and drop entries older than 14 days (atomic write with retries)."""
    now = time.time() if now is None else now
    data = attached_load()
    kept = {}
    for sid, rec in data.items():
        stamp = harvest.parse_iso(rec.get("attached_at")) if isinstance(rec, dict) else None
        if stamp is not None and now - stamp <= ATTACHED_RETENTION:
            kept[sid] = rec
    kept[session_id] = {"worktree": str(worktree), "vendor": vendor, "account": account,
                        "transcript": transcript, "attached_at": harvest.iso_now(now)}
    harvest.atomic_write(attached_path(), json.dumps(kept, indent=1, sort_keys=True))
