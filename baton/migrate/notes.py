"""``.baton/migration-notes.md``: what a moved checkout's first sessions on the new machine must know.

Written by ``bt migrate import`` for every repository's main checkout and every recreated worktree, from the audit in the bundle
(``baton/audit.json``: the Windows-dependence scan, toolchains, scheduled tasks, hooks) and from what the import itself did
(patches kept, lines that need a person). The session capsule (``baton/capsule.py``) points at the file for 30 days. The file
stays under about 80 lines; ``<root>/.migration/import-report.md`` has the rest.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from . import common as c

NOTES = "migration-notes.md"
MAX_LINES = 80
LABELS = (("win_paths", "Windows paths"), ("powershell", "PowerShell / cmd"), ("exe", ".exe programs"), ("os_checks", "OS checks"),
          ("scheduling", "Windows scheduling"), ("wsl", "WSL"), ("ci", "CI workflow runners"))


def _norm(text: str) -> str:
    return text.replace("\\", "/").lower()


def load_audit(src: Path) -> dict:
    try:
        return json.loads((src / "baton" / "audit.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _first(examples: list, n: int = 3) -> str:
    return ", ".join(f"{e['loc']}" for e in examples[:n])


def _named(audit: dict, names: list[str]) -> list[str]:
    """Scheduled tasks and hooks of the old machine whose command names one of ``names`` (source paths)."""
    needles = [_norm(n) for n in names if n]
    out = []
    for t in (audit.get("machine") or {}).get("scheduled_tasks", []):
        if any(n in _norm(t.get("run", "")) for n in needles):
            out.append(f"scheduled task `{t['name']}`: {t.get('run', '')[:110]}")
    user = audit.get("user") or {}
    for vendor in ("claude", "codex"):
        for acct in user.get(vendor, []):
            for h in acct.get("hooks", []):
                if any(n in _norm(h.get("command", "")) for n in needles):
                    out.append(f"{vendor}/{acct['account']} hook {h.get('event', '')}: {h.get('command', '')[:110]}")
    return out


def _mine(problem: str, key: str, other_keys: set[str]) -> bool:
    if key == "_main":
        return not any(f"of {k} " in problem or f"worktree {k}:" in problem or f"/{k}/" in problem for k in other_keys)
    return f"of {key} " in problem or f"worktree {key}:" in problem or f"/{key}/" in problem


def checkout_notes(lay, entry: dict, res: dict, audit: dict, key: str, old: str, new: Path, manifest: dict, report: Path,
                   today: str) -> str:
    name = entry["name"]
    arepo = next((r for r in audit.get("repos", []) if r["name"] == name), {})
    sections: list[tuple[str, list[str]]] = []
    win = arepo.get("windows") or {}
    lines = []
    ws = win.get("win_scripts") or {}
    if ws.get("count"):
        lines.append(f"- {ws['count']} Windows script file(s) (.ps1 .cmd .bat ...), {ws.get('without_sibling', 0)} with no .sh or .py counterpart; "
                     f"first: {', '.join(e['loc'] for e in ws.get('examples', [])[:3])}")
    for cat, label in LABELS:
        v = win.get(cat) or {}
        if v.get("count"):
            lines.append(f"- {label}: {v['count']} in {v.get('files', '?')} file(s); first: {_first(v.get('examples', []))}")
    if lines:
        sections.append(("Windows dependence in tracked files (counted in the main checkout; run before trusting scripts or tests)", lines))
    tc = [f"- {t['tool']} in {t['dir']}: `{t['restore']}`" for t in arepo.get("toolchains", [])]
    if tc:
        sections.append(("Restore commands (suggestions; the import ran none of them)", tc[:8] + ([f"- ... {len(tc) - 8} more"] if len(tc) > 8 else [])))
    named = _named(audit, [entry["source_path"], old, arepo.get("path", "")])
    if named:
        sections.append(("Scheduled tasks and hooks of the old machine that named this checkout (not carried)", [f"- {x}" for x in named[:8]]))
    held = [s["where"][len(f"{name}/{key}/"):] for s in manifest.get("secrets_by_hand", []) if s["where"].startswith(f"{name}/{key}/")]
    if held:
        sections.append(("Secret-looking files that were not carried (copy by hand; names only)", [f"- {x}" for x in held[:10]]
                         + ([f"- ... {len(held) - 10} more"] if len(held) > 10 else [])))
    kept = []
    d = res["dirty"].get(key) or {}
    if d.get("tracked") == "kept":
        kept.append(f"- tracked changes did not apply; kept at {d.get('kept')}")
    kept += [f"- {x}.migrated: an untracked file already existed, the old one is beside it" for x in d.get("untracked_migrated", [])[:5]]
    other_keys = {w["key"] for w in entry["worktrees"]} | {"_main"}
    other_keys.discard(key)
    kept += [f"- {p}" for p in res.get("problems", []) if _mine(p, key, other_keys) and "did not apply" not in p]
    if key == "_main" and res.get("stashes"):
        kept.append(f"- {len(res['stashes'])} stash entr(ies) restored (`git stash list`); patch copies in .baton/migrated-stashes/")
    if kept:
        sections.append(("Kept for a person", kept))
    needs = []
    for e, rec in zip(entry["overlay"], res.get("overlay", [])):
        if e["tree"] == key:
            needs += [f"- {e['rel']}:{n}: {line}" for n, line in rec.get("needs", [])]
    if key == "_main":
        needs += [f"- {x}" for x in res.get("needs", [])]
    if needs:
        sections.append(("Still looks Windows-specific or needs a person in this checkout", needs[:14] + ([f"- ... {len(needs) - 14} more"] if len(needs) > 14 else [])))
    out = ["# Moved from Windows", "",
           f"Moved on {today} (bundle created {manifest.get('created', '?')}). Old path: {old}. New path: {new}.",
           f"Read this before running scripts or tests here. Everything the import knows: {report}", ""]
    if not sections:
        out.append("Nothing was found to report for this checkout: no Windows-dependent tracked files, nothing kept, nothing that needs a person.")
    for title, body in sections:
        out += [f"## {title}", *body, ""]
    if len(out) > MAX_LINES:
        extra = len(out) - (MAX_LINES - 2)
        out = out[:MAX_LINES - 2] + ["", f"... {extra} more line(s): see {report}"]
    return "\n".join(out).rstrip("\n") + "\n"


def _ensure_ignored(checkout: Path, rel: str) -> None:
    """Keep the notes out of ``git status`` (a checkout that does not ignore ``.baton`` gets an entry in the shared info/exclude)."""
    if c.git(checkout, "check-ignore", "-q", rel).returncode == 0:
        return
    exclude = c.git_common_dir(checkout) / "info" / "exclude"
    try:
        old = exclude.read_text(encoding="utf-8") if exclude.is_file() else ""
        exclude.parent.mkdir(parents=True, exist_ok=True)
        with open(exclude, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(old + ("" if old.endswith("\n") or not old else "\n") + "/.baton/" + NOTES + "\n")
    except OSError:
        pass


def write_all(lay, src: Path, manifest: dict, entry: dict, res: dict, audit: dict) -> list[str]:
    """Write the notes for the main checkout and every recreated worktree of one repository; returns the files written."""
    if res["status"].startswith(("failed", "refused")):
        return []
    dest = Path(res["dest"])
    report = lay.new_root / ".migration" / "import-report.md"
    today = time.strftime("%Y-%m-%d")
    targets = [("_main", entry["source_path"], dest)]
    wts = {w["key"]: w for w in entry["worktrees"]}
    for rec in res["worktrees"]:
        if rec.get("recreated") and rec["key"] in wts:
            targets.append((rec["key"], wts[rec["key"]]["path"], dest.joinpath(*rec["rel"].split("/"))))
    written = []
    for key, old, path in targets:
        if not path.is_dir():
            continue
        target = path / ".baton" / NOTES
        c.write_bytes(target, checkout_notes(lay, entry, res, audit, key, old, path, manifest, report, today).encode("utf-8"))
        _ensure_ignored(path, f".baton/{NOTES}")
        written.append(str(target))
    return written
