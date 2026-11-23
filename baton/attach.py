"""``bt attach``: bring a session that started outside a worktree (typically in ``C:/Work``) under baton.

SessionStart sees the session's cwd, so a session started outside any worktree never loads the capsule;
the owner then types ``/pickup <ws>``. This command resolves the worktree, records the mapping
``(vendor, account, session id) -> worktree`` in ``STATE_DIR/attached.json`` (hooks consult it for SessionStart, PostCompact,
SessionEnd and UserPromptSubmit), claims ``owner.json``, registers the session, releases the claim and registry record it held on any
other worktree, and prints the startup capsule. Run from inside the agent's shell, where the vendor exports its session id.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from . import capsule, config, harvest, hook, launch, locking, paths, registry, worktrees

SESSION_ENV = {"claude": ("CLAUDE_CODE_SESSION_ID",), "codex": ("CODEX_THREAD_ID", "CODEX_SESSION_ID")}
NO_SESSION_NOTE = ("note: no session id (run this from the agent's shell, or pass --session and --vendor): "
                   "compaction and closeout will not follow this session.")


def env_session(vendor: str, environ=None) -> str | None:
    environ = os.environ if environ is None else environ
    for name in SESSION_ENV[vendor]:
        if environ.get(name):
            return environ[name]
    return None


def detect(vendor: str | None, session: str | None, environ=None) -> tuple[str | None, str | None]:
    """(vendor, session id) from the flags, else from the agent's environment (Claude wins when both exist)."""
    if vendor is None:
        vendor = next((v for v in config.VENDORS if env_session(v, environ)), None)
    if vendor is None:
        return None, session
    return vendor, session or env_session(vendor, environ)


def find_transcript(vendor: str, session_id: str, account: str) -> Path | None:
    """The newest transcript with this session ID in the selected account."""
    selected_home = os.environ.get(config.ACCOUNT_ENV[vendor])
    home = (config.expand(selected_home) if selected_home and config.account_from_env(vendor) == account
            else config.account_dir(vendor, account))
    if vendor == "codex":
        return worktrees.find_rollout(session_id, [home], time.monotonic() + worktrees.LIVE_LOOKUP_S)
    try:
        hits = [p for p in (home / "projects").glob(f"*/{session_id}.jsonl") if p.is_file()]
    except OSError:
        return None
    return max(hits, key=lambda p: config.mtime(p) or 0.0, default=None)


def release_elsewhere(root: Path, session_id: str, previous: str | None, *,
                      vendor: str, account: str) -> list[str]:
    """Release this identity's old owner and registry records under each worktree's owner lock."""
    keep = config.norm(root)
    others = registry.session_worktrees(session_id, vendor=vendor, account=account)
    if previous:
        others.setdefault(config.norm(previous), previous)
    others.pop(keep, None)
    released = []
    for path in others.values():
        old_root = Path(path)
        if old_root.is_dir():
            try:
                with locking.exclusive(paths.baton_dir(old_root) / "owner.lock"):
                    hook._release_owner_locked(old_root, session_id, None, vendor, account)
                    registry.detach_session(old_root, session_id, vendor=vendor, account=account)
            except (OSError, registry.RegistryUnavailable) as exc:
                hook._log(vendor, "attach", exc)
                continue
        else:
            hook._guard(vendor, "attach", lambda: registry.detach_session(
                old_root, session_id, vendor=vendor, account=account))
        released.append(path)
    return released


def _move(root: Path, vendor: str, account: str, session_id: str, transcript: str | None) -> tuple[str, list[str]]:
    """Claim, publish, and release in that order under the attachment lock."""
    try:
        with registry.attached_lock():
            data = registry.attached_load(strict=True)
            bound = registry.attached_record(session_id, vendor=vendor, account=account, data=data)
            now = time.time()
            owner_path = paths.baton_dir(root) / "owner.json"
            with locking.exclusive(owner_path.with_suffix(".lock")):
                previous_owner = config.read_json(owner_path)
                warning = hook._claim_owner_locked(root, vendor, account, session_id, transcript, "attach", now)
                if warning:
                    return warning, []
                try:
                    registry._attach_record_locked(data, session_id, root, vendor, account, transcript, now)
                except OSError:
                    if previous_owner:
                        hook._write_json(owner_path, previous_owner)
                    else:
                        hook._remove(owner_path)
                    raise
                hook._guard(vendor, "attach", lambda: registry.update(
                    root, vendor, ws=paths.ws_name(root), session_id=session_id, transcript=transcript, account=account,
                    source="attach", started_at=harvest.iso_now(now), last_seen=now))
                released = release_elsewhere(root, session_id, bound["worktree"] if bound else None,
                                             vendor=vendor, account=account)
                hook._guard(vendor, "attach", lambda: registry.startup_add(root, vendor, account, session_id, now))
                hook.register_session_workflow(root, vendor, session_id, transcript, launch_cwd=os.getcwd())
                return "", released
    except OSError as exc:
        hook._log(vendor, "attach", exc)
        return "WARNING: cannot update worktree attachment. Do not edit files here; tell the user.", []


def run(args) -> int:
    try:
        root = launch.resolve_worktree(args.query)
    except LookupError as exc:
        print(f"bt: {exc}", file=sys.stderr)
        return 2
    vendor, session_id = detect(args.vendor, args.session)
    if session_id and vendor is None:  # --session without --vendor and no env: whichever vendor has the transcript
        vendor = next((v for v in config.VENDORS if find_transcript(v, session_id, config.account_from_env(v))), "claude")
    vendor = vendor or registry.entry(root).get("last_vendor") or "claude"
    account = config.account_from_env(vendor)
    found = find_transcript(vendor, session_id, account) if session_id else None
    transcript = str(found) if found else None

    paths.ensure_excluded(root)
    paths.baton_dir(root, create=True)
    if session_id:
        warning, released = _move(root, vendor, account, session_id, transcript)
        if warning:
            print(warning)
            return 2
        print(f"Attached {vendor} session {harvest.id8(session_id)} to {root} — work in that directory.")
        if released:
            print("moved this session off: " + ", ".join(released))
        if transcript is None:
            print("note: transcript not found; liveness falls back to the claim time.")
    else:
        print(f"Capsule for {root} — work in that directory.")
        print(NO_SESSION_NOTE)
    print()

    # Same as SessionStart: listing the inbox in the capsule consumes the unread flag unless the cap cuts it.
    flag = paths.baton_dir(root) / "inbox" / ".unread"
    had_flag = flag.exists()
    hook._remove(flag)
    delivered = False
    try:
        parts = capsule.sections(root, vendor, "startup", account)
        context = capsule.assemble(parts, capsule.CAPS.get(vendor, capsule.CAPS["claude"]))
        print(context)
        delivered = hook._inbox_complete(root, context) if had_flag else True
    finally:
        if had_flag and not delivered:
            hook._restore_unread(flag)
    return 0


def register(sub) -> None:
    p = sub.add_parser("attach", help="attach this session (started outside a worktree) to a workstream")
    p.add_argument("query", help="worktree: ws name, repo/ws or unique substring")
    p.add_argument("--vendor", choices=config.VENDORS, help="default: from CLAUDE_CODE_SESSION_ID / CODEX_THREAD_ID")
    p.add_argument("--session", help="session id (default: from the agent's environment)")
    p.set_defaults(func=run)
