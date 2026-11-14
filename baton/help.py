"""``bt help``: a task-ordered guide to the bt command, and ``bt help <command>`` for one command's options.

The option detail comes from each command's own argparse parser; this file holds only the guide's grouping and
one-line descriptions. tests/test_help.py checks that every bt command appears in the guide.
"""
from __future__ import annotations

import argparse
import sys

GUIDE = """\
bt: start agents on worktrees, see what is running, and hand work over.

START AND SWITCH SESSIONS
  bt                          outside a worktree: pick one (fzf, most recent first) and start an agent
  bt <worktree>               start or resume the agent for a worktree (any unique part of its name)
      -Fresh | -Resume          force a new session, or resume the last one
      -Vendor claude|codex      -Account <name>   -Model <id>   -Role <role>   -Prompt <text>
      -NewTab                   open in a new terminal tab
      -DryRun                   print the directory, environment and command without starting
      -Force                    launch even though another session looks live in the worktree
  bt switch [<worktree>]      fresh session on the other account
  bt attach <worktree>        link a session started outside a worktree to a workstream
  bt plan <worktree>          show what `bt <worktree>` would decide (vendor, account, fresh or resume)
  bt cycle --status           hand a large session over to a fresh one (arm, --now, --off, --on)
  bt startup show|run|pin|unpin
                              reopen live workstreams as terminal tabs at logon (install, uninstall, snapshot)

SEE WHAT IS GOING ON
  bt status                   one row per workstream: session, idle time, context, compactions, owner (--all)
  bt list                     worktrees under the code root, most recent activity first
  bt fleet                    compute fleet: slots, RAM, running and queued jobs (--details, --watch)
  bt compute                  this workstream's deferred jobs (--refresh; --reviewed <id> --resolution ...)
  bt doctor                   check hooks, formats, accounts and routing for drift (--fix)

WORKTREES
  bt wt new <name> --exit pr|merge:<branch>|discard
                              create a worktree (the only sanctioned way; capped, records its Exit)
  bt wt retire <name>         remove a finished worktree; the branch is kept
  bt wt list                  lineage tree of linked worktrees per repo
  bt backup                   push branches that exist on no remote to origin as backup/<date>/...

HANDOVER
  bt now                      path of this worktree's .baton/now.md; `bt now check` lints it
  bt close                    run the closeout (handover note) on a cheap model (--bg, --dry-run)

FOLLOWERS (review, docs and optimise runs after your commits)
  bt follow on|off|now|status|reset
  bt inbox                    follower results: `bt inbox show <name>`, `bt inbox done <name|all>`
  bt merge <role>             merge a follower's branch into this branch

OVERNIGHT COMPUTE
  bt night plan|submit|report        what would run, submit it, the morning report
  bt night enroll|unenroll|schedule  let the evening task submit this worktree's checks

ACCOUNTS AND MODELS
  bt accounts                 accounts with login state and usage (--refresh)
  bt account link|add|login|label
  bt keepalive                refresh Claude logins whose token is about to expire
  bt routing show|apply|check
                              role table, write the bt-<role> agents, check model availability

SETUP AND MOVING MACHINES
  bt install                  wire hooks, role agents and the bt shell command
  bt migrate audit|export|import|verify

MORE
  bt help <command> [<sub>]   options for one command, e.g. `bt help wt new`
"""

# Commands the guide does not list: internal plumbing.
UNLISTED = frozenset({"shell", "help"})


def _subparsers(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return dict(action.choices)
    return {}


def run(args) -> int:
    words = list(args.topic or [])
    if not words:
        sys.stdout.write(GUIDE)
        return 0
    from .cli import build_parser
    parser = build_parser()
    for i, word in enumerate(words):
        choices = _subparsers(parser)
        if word not in choices:
            where = "bt " + " ".join(words[:i]) if i else "bt"
            known = ", ".join(sorted(n for n in choices if n not in UNLISTED)) or "none"
            print(f"bt help: {where} has no command {word!r}; its commands: {known}", file=sys.stderr)
            return 2
        parser = choices[word]
    sys.stdout.write(parser.format_help())
    return 0


def register(sub) -> None:
    p = sub.add_parser("help", help="a guide to the bt commands; `bt help <command>` for one command's options")
    p.add_argument("topic", nargs="*", help="a command, and optionally its subcommand (e.g. wt new)")
    p.set_defaults(func=run)
