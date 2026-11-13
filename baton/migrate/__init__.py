"""``bt migrate``: one-way migration of a development workspace to another machine (Windows to a Mac).

Subcommands
  audit   survey the workspace read-only and report what a plain re-clone would lose, what is Windows-specific and what the
          new machine needs (Markdown, or ``--json``; ``--out DIR`` writes audit.md, audit.json and a proposed Brewfile)
  export  write a self-describing bundle directory: git bundles (every stash entry and every worktree's HEAD included), the
          uncommitted and untracked work and local-only files of every worktree wherever it lies, user configuration, a
          snapshot of Baton itself (``tool/``) and ``mac-start.sh``. Secret values never enter the bundle (see below)
  import  verify a bundle, then rebuild the repositories, worktrees and files under a new root (runs on macOS, Linux, Windows);
          on the Mac start it with ``sh mac-start.sh``: it finds a Python 3.11 or later, uses ``tool/`` and, with no arguments,
          runs ``--dry-run``. Exits non-zero when a repository failed or anything needs a person
  verify  re-check an import against the bundle's manifest without changing anything: refs, stash list, HEAD of every checkout,
          every tracked file against HEAD, the dirty files' content, a patch that was kept instead of applied (a gap)

migrate.toml (next to accounts.toml; ``.state/migrate.local.toml`` is appended to its lists). Every key is optional:

    carry = ["notes/**"]            # globs, relative to every repository, worktree and the root, matching ignored files
                                    # that are carried (a pattern without "/" matches the file name at any depth; a leading
                                    # "/" anchors it at the checkout root: "/artifacts/" is the root's artifacts directory only)
    leave = ["scratch/**"]          # globs for ignored files deliberately left behind (recorded in the manifest). For a plain
                                    # folder (nonrepo, extra_dirs) a glob is matched against the path below the folder and
                                    # against its path below the code root or home: "/scratch/old/" and "old/" both settle the
                                    # directory scratch/old without entering it (an unreadable directory needs no other fix)
    skip_repos = ["brain"]          # repositories directly under the root that are not exported
    no_scan = ["brain"]             # repositories whose file contents the Windows-dependence scan and the secret content check
                                    # never open (names, sizes and git metadata are still used); default ["brain"]; the audit
                                    # names them. A configuration file there that would need redacting is held back unread
    nonrepo = ["tools"]             # directories directly under the root, not repositories, to carry whole
    [repos.app]               # per repository: the same carry / leave
    carry = ["docs/local/**"]
    [[extra_repos]]                 # git repositories outside the root
    path = "E:/Elsewhere/some-repo"
    target = "Elsewhere/some-repo"  # where the import puts it, relative to NEWROOT (or NEWHOME with base = "home")
    [[extra_dirs]]                  # plain directories outside the root
    path = "C:/Users/<you>/plugins/app"
    target = "plugins/app"
    base = "home"

Secrets (``secrets.py``). A credential file by name (``.env``, ``.npmrc``, ``id_rsa``, ``*.pfx``, ``token.json`` ...) is never carried.
``settings*.json``, ``.mcp.json``, ``~/.claude.json`` extracts and Codex ``config.toml`` are carried with every value under ``env``,
``headers`` and ``http_headers``, every secret-named key, URL userinfo and query string replaced by ``REDACTED`` (names kept); one
that does not parse is not carried. Any other text file up to 2 MiB holding a private key block or a recognised token is held back
too. Held files are named in the audit and the manifest (``secrets_by_hand``) with the reason, never with a value. Remote URLs lose
their userinfo everywhere they are written, and import sets the redacted URL and says so. ``--include-secrets`` turns these checks off.

Ignored files that match no carry rule are never dropped silently: rebuildable directories are summarised and left, secrets
are named for copying by hand, and the rest ("ignored, undecided") is carried below 20 MiB per file and 200 MiB per checkout
and otherwise named as needing a ``carry`` or ``leave`` decision.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .. import config
from . import common as c


def make_ctx(args, **kw) -> c.Ctx:
    root = Path(args.root) if getattr(args, "root", None) else config.CODE_ROOT
    return c.Ctx(root=root, home=c.source_home(), cfg=c.load_cfg(), accounts=lambda v: c.default_accounts(v),
                 only=list(getattr(args, "repo", None) or []), **kw)


def cmd_audit(args) -> int:
    from . import audit
    ctx = make_ctx(args)
    data, _survey, _plan = audit.build_audit(ctx, scan=not args.no_scan, only=ctx.only)
    text = audit.render_markdown(data)
    if args.out:
        audit.write_out(Path(args.out), data, text)
        print(f"wrote {Path(args.out) / 'audit.md'} ({len(text.encode('utf-8'))} bytes), audit.json and Brewfile", file=sys.stderr)
    print(json.dumps(data, indent=1, default=str) if args.json else text)
    return 0


def cmd_export(args) -> int:
    from . import export
    return export.run(args, make_ctx(args, include_secrets=args.include_secrets, extra_carry=list(args.carry or [])))


def cmd_import(args) -> int:
    from . import importer
    return importer.run(args)


def cmd_verify(args) -> int:
    from . import verify
    return verify.run(args)


def register(sub) -> None:
    p = sub.add_parser("migrate", help="one-way migration of the workspace to another machine (audit, export, import, verify)")
    s = p.add_subparsers(dest="migrate_cmd", required=True)

    a = s.add_parser("audit", help="read-only survey: what a plain re-clone would lose, Windows dependence, tools")
    a.add_argument("--root", help="code root (default: Baton's)")
    a.add_argument("--json", action="store_true", help="machine-readable output")
    a.add_argument("--out", help="write audit.md, audit.json and Brewfile into this directory")
    a.add_argument("--repo", action="append", help="only this repository (repeatable)")
    a.add_argument("--no-scan", action="store_true", help="skip the Windows-dependence scan of tracked files")
    a.set_defaults(func=cmd_audit)

    e = s.add_parser("export", help="write a self-describing bundle directory")
    e.add_argument("--root", help="code root (default: Baton's)")
    e.add_argument("--out", required=True, help="bundle directory to create")
    e.add_argument("--repo", action="append", help="only this repository (repeatable)")
    e.add_argument("--carry", action="append", help="extra glob of ignored files to carry (repeatable)")
    e.add_argument("--include-secrets", action="store_true", help="carry secret-looking files too")
    e.add_argument("--force", action="store_true", help="export despite readiness blockers (recorded in the manifest)")
    e.add_argument("--dry-run", action="store_true", help="print what would be written, with sizes; write nothing")
    e.add_argument("--archive", help="also write the finished bundle as one uncompressed tar, with a sha256 beside it")
    e.set_defaults(func=cmd_export)

    i = s.add_parser("import", help="rebuild the workspace from a bundle under a new root")
    i.add_argument("--from", dest="src", required=True, help="bundle directory")
    i.add_argument("--root", default=str(Path.home() / "Code"), help="new code root (default ~/Code)")
    i.add_argument("--home", help="new home directory (default: this machine's)")
    i.add_argument("--repo", action="append", help="only this repository (repeatable)")
    i.add_argument("--apply-user-config", action="store_true", help="copy user configuration into the real account directories")
    i.add_argument("--dry-run", action="store_true", help="verify the bundle and print the plan; write nothing")
    i.set_defaults(func=cmd_import)

    v = s.add_parser("verify", help="re-check an import against the bundle's manifest")
    v.add_argument("--from", dest="src", required=True, help="bundle directory")
    v.add_argument("--root", default=str(Path.home() / "Code"), help="new code root (default ~/Code)")
    v.add_argument("--home", help="new home directory (default: this machine's)")
    v.set_defaults(func=cmd_verify)
