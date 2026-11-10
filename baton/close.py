"""``bt close``: run the workstream closeout on a cheap headless model.

The lead's context is large and expensive; the closeout (handover note, graph
update) needs only the baton capsule and git history. This hands both to a
headless ``claude -p`` running the ``scribe`` role and the user's handover skill.
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from . import config, gitutil, paths, procs

PROMPT = """\
/handover {ws}

You are the scribe for workstream `{ws}` ({root}). The lead agent that did the
work has stopped; you are doing its closeout so its large context is not
reloaded. Work only from the baton capsule and git history below plus targeted
reads of the files they name. Do not edit code, commit, push, or start other
agents. Skip steps that only the original session can do (releasing its own
registration, answering its notices); everything else in the handover skill
applies, including its rules on minimal, replace-in-place graph updates.

## Baton capsule
{capsule}

## Commits on {branch} since {base7}
{log}

## Diffstat
{stat}
"""


def build_prompt(root: Path, vendor: str = "claude") -> str:
    try:
        from . import capsule
        parts = capsule.sections(root, vendor, "startup", config.account_from_env(vendor), create=False)
        parts = {k: v for k, v in parts.items() if k not in ("rules", "roles")}  # lead-only guidance
        capsule_text = capsule.assemble(parts, capsule.CAPS["claude"])
    except Exception as exc:  # capsule is best-effort context
        capsule_text = f"(capsule unavailable: {exc})"
    base_branch = gitutil.default_branch(root)
    base = gitutil.merge_base(root, "HEAD", base_branch) or base_branch
    log = gitutil.out(root, "log", "--format=%h %s", "-n", "60", f"{base}..HEAD") or "(none)"
    stat = gitutil.out(root, "diff", "--stat=100", "--stat-count=40", f"{base}..HEAD") or "(none)"
    return PROMPT.format(ws=paths.ws_name(root), root=root, capsule=capsule_text,
                         branch=gitutil.branch(root) or "HEAD", base7=base[:7], log=log, stat=stat)


def command() -> list[str]:
    """The scribe's claude argv; the prompt goes on stdin (no command-line length or quoting limits)."""
    role = config.routing_config().get("roles", {}).get("scribe", {}).get("claude", {})
    argv = [config.exe("claude"), "-p", "--output-format", "text", "--no-session-persistence"]
    if role.get("model"):
        argv += ["--model", role["model"]]
    if role.get("effort"):
        argv += ["--effort", role["effort"]]
    return argv + ["--permission-mode", "auto"]


def run(args) -> int:
    root = paths.find_worktree_root(args.worktree or os.getcwd())
    if root is None:
        print("not inside a git worktree")
        return 1
    prompt = build_prompt(root)
    if args.dry_run:
        print(prompt)
        return 0
    argv = command()
    env = dict(config.clean_env(), **{config.FOLLOWER_ENV: "scribe"})
    if not args.bg:
        return subprocess.run(argv, cwd=root, env=env, input=prompt, text=True, encoding="utf-8").returncode
    runs = paths.baton_dir(root, create=True) / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    prompt_file, log = runs / f"close-{stamp}.prompt.md", runs / f"close-{stamp}.log"
    prompt_file.write_text(prompt, encoding="utf-8")
    with open(prompt_file, encoding="utf-8") as stdin, open(log, "w", encoding="utf-8") as fh:
        procs.popen_detached(argv, cwd=root, env=env, stdin=stdin, stdout=fh, stderr=subprocess.STDOUT, close_fds=True)
    print(f"closeout running in the background; log: {log}")
    return 0


def register(sub) -> None:
    p = sub.add_parser("close", help="run the workstream closeout (handover skill) on a cheap headless model")
    p.add_argument("--worktree", help="worktree path (default: current directory)")
    p.add_argument("--bg", action="store_true", help="run detached; output to .baton/runs/close-*.log")
    p.add_argument("--dry-run", action="store_true", help="print the prompt only")
    p.set_defaults(func=run)
