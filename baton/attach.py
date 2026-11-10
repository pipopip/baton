"""``bt attach``: bring a session that started outside a worktree (typically in ``C:/Work``) under baton.

SessionStart sees the session's cwd, so a session started outside any worktree never loads the capsule;
the owner then types ``/pickup <ws>``. This command resolves the worktree, records the mapping
``session id -> worktree`` in ``STATE_DIR/attached.json`` (hooks consult it for SessionStart, PostCompact,
SessionEnd and UserPromptSubmit), claims ``owner.json``, registers the session, releases the claim and registry record it held on any
other worktree, and prints the startup capsule. Run from inside the agent's shell, where the vendor exports its session id.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from . import capsule, config, harvest, hook, launch, paths, registry, worktrees

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


def find_transcript(vendor: str, session_id: str) -> Path | None:
    """The session's transcript under any account of ``vendor`` (newest when several match), else None."""
    hits: list[Path] = []
    for account in config.account_names(vendor):
        home = config.account_dir(vendor, account)
        if vendor == "claude":
            try:
                hits += [p for p in (home / "projects").glob(f"*/{session_id}.jsonl") if p.is_file()]
            except OSError:
                pass
        else:
            found = worktrees.find_rollout(session_id, [home], time.monotonic() + worktrees.LIVE_LOOKUP_S)
            if found is not None:
                hits.append(found)
    return max(hits, key=lambda p: config.mtime(p) or 0.0, default=None)


def release_elsewhere(root: Path, session_id: str, previous: str | None = None, *,
                      vendor: str | None = None, account: str | None = None) -> list[str]:
    """Let go of this session's hold on every worktree but ``root``; returns the paths released.

    The old holds are the worktrees whose registry vendor record names this session and the worktree attached.json
    bound it to before (``previous``). In each, ``owner.json`` is removed when it is this session's, the session is
    dropped from the registry record and from the startup list. Each step is independent and never raises.
    """
    keep = config.norm(root)
    others: dict[str, str] = {}
    try:
        for path in registry.detach_session(session_id, root, vendor=vendor, account=account):
            others[config.norm(path)] = path
    except Exception:  # noqa: BLE001 - an unreadable registry must not undo the attach
        pass
    if previous and config.norm(previous) != keep:
        others.setdefault(config.norm(previous), previous)
    for path in others.values():
        try:
            hook.release_owner(Path(path), session_id, None, vendor=vendor, account=account)
        except Exception:  # noqa: BLE001
            pass
    try:
        registry.startup_drop_session(session_id, root, vendor=vendor, account=account)
    except Exception:  # noqa: BLE001
        pass
    return list(others.values())


def run(args) -> int:
    try:
        root = launch.resolve_worktree(args.query)
    except LookupError as exc:
        print(f"bt: {exc}", file=sys.stderr)
        return 2
    vendor, session_id = detect(args.vendor, args.session)
    if session_id and vendor is None:  # --session without --vendor and no env: whichever vendor has the transcript
        vendor = next((v for v in config.VENDORS if find_transcript(v, session_id)), "claude")
    vendor = vendor or registry.entry(root).get("last_vendor") or "claude"
    account = config.account_from_env(vendor)
    transcript = find_transcript(vendor, session_id) if session_id else None
    now = time.time()

    paths.ensure_excluded(root)
    paths.baton_dir(root, create=True)
    warning = ""
    if session_id:
        warning = hook.claim_owner(root, vendor, account, session_id, str(transcript) if transcript else None, "attach", now)
        if warning:
            print(warning)
            return 2
        bound = registry.attached_load().get(session_id)
        previous = bound.get("worktree") if isinstance(bound, dict) and isinstance(bound.get("worktree"), str) else None
        registry.attach_record(session_id, root, vendor, account, str(transcript) if transcript else None, now)
        registry.update(root, vendor, ws=paths.ws_name(root), session_id=session_id,
                        transcript=str(transcript) if transcript else None, account=account,
                        source="attach", started_at=harvest.iso_now(now), last_seen=now)
        hook.register_session_workflow(root, vendor, session_id, str(transcript) if transcript else None,
                                       launch_cwd=os.getcwd())
        registry.startup_add(root, vendor, account, session_id, now)
        released = release_elsewhere(root, session_id, previous, vendor=vendor, account=account)
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
