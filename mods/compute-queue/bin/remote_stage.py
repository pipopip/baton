#!/usr/bin/env python3
"""Stage a committed Git snapshot and run it on a queue host over one SSH connection."""
import argparse
import hashlib
import os
import re
import shlex
import subprocess
import sys
import tempfile
import uuid
from pathlib import PurePosixPath

SSH_OPTIONS = ("-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
               "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15")
FLAGS = 0x08000000 if os.name == "nt" else 0


def system_ssh(environ=None, name=None, isfile=os.path.isfile):
    """Windows' own OpenSSH client when this helper runs without CQ_SSH: a bare `ssh` there resolves through PATH,
    where Git for Windows' ssh.exe (which cannot reach the Windows ssh-agent) may come first. None elsewhere."""
    environ = os.environ if environ is None else environ
    if (os.name if name is None else name) != "nt":
        return None
    path = (environ.get("SystemRoot") or r"C:\Windows") + r"\System32\OpenSSH\ssh.exe"
    return path if isfile(path) else None


def host_value(value):
    if not value or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@-]*", value) or value.count("@") > 1 or value.endswith("@"):
        raise ValueError("invalid LANE_HOST")
    return value


def subdir_value(value):
    if not value or "\\" in value or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError("invalid --subdir")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("--subdir must stay inside the work tree")
    return str(path)


def revision_value(value):
    if not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", value):
        raise ValueError("--rev must be a full lowercase commit hash")
    return value


def machine_value(value):
    if value and not re.fullmatch(r"[0-9a-f]{32}", value):
        raise ValueError("invalid CQ_REMOTE_MACHINE_ID")
    return value


def repo_id(common_dir):
    absolute = os.path.normcase(os.path.realpath(common_dir))
    name = re.sub(r"[^A-Za-z0-9_-]", "-", os.path.basename(os.path.dirname(absolute)))[:40] or "repo"
    return name + "-" + hashlib.sha256(absolute.encode()).hexdigest()[:16]


TAG_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/+-]{0,199}")
MAX_TAGS = 64          # the newest tags reachable from the snapshot
MAX_TAG_BYTES = 6000   # of names and ids together: the script is one argument of ssh, and Windows caps a command line at 32,767 characters


def tag_name_value(name):
    return (TAG_NAME.fullmatch(name) is not None and ".." not in name and "//" not in name
            and not name.endswith(("/", ".", ".lock")) and not any(part.startswith(".") for part in name.split("/")))


def merged_tags(git, top, sha):
    """The newest tags merged into sha as (name, oid) pairs sorted by name: MAX_TAGS at most and MAX_TAG_BYTES of
    names and ids, newest first when there are more (so `git describe` finds the nearest). Tags are a convenience:
    a failed listing gives none."""
    try:
        listing = git("-C", top, "for-each-ref", "--merged", sha, "--sort=-creatordate",
                      "--format=%(objectname) %(refname)", "refs/tags")
    except Exception:
        return []
    tags, size = [], 0
    for line in listing.splitlines():
        oid, _, ref = line.partition(" ")
        name = ref[len("refs/tags/"):] if ref.startswith("refs/tags/") else ""
        try:
            revision_value(oid)
        except ValueError:
            continue
        if not tag_name_value(name):
            continue
        size += len(name) + len(oid)
        if len(tags) >= MAX_TAGS or size > MAX_TAG_BYTES:
            break
        tags.append((name, oid))
    tags.sort(key=lambda tag: tag[0])
    return tags


def remote_script(repo, stage_name, sha, subdir, command, machine_id, tags=()):
    # Every interpolated value is shell-quoted; paths are anchored to $HOME.
    q = shlex.quote
    # Tags go into the stage clone only: the shared mirror must not hold snapshot-specific tag refs. A tag that
    # cannot be set does not fail the staging.
    tag_lines = "".join(f'git -C "$stage" update-ref {q("refs/tags/" + name)} {q(oid)} || true\n' for name, oid in tags)
    return f'''set -eu
expected={q(machine_id)}
if [ -n "$expected" ]; then
  actual=$(cat /etc/machine-id)
  [ "$actual" = "$expected" ] || {{ echo "[remote_stage] machine identity mismatch" >&2; exit 1; }}
fi
base="$HOME/.local/share/baton"
bare="$base/repos/"{q(repo + '.git')}
stage="$base/staging/"{q(repo)}/{q(stage_name)}
mkdir -p "$base/repos" "$base/staging/"{q(repo)}
if [ ! -d "$bare" ]; then git init -q --bare --object-format={"sha256" if len(sha) == 64 else "sha1"} "$bare"; fi
# The pack contains only objects reachable from the requested commit.
git --git-dir="$bare" index-pack --stdin >/dev/null
git --git-dir="$bare" update-ref refs/baton-snapshots/{q(sha)} {q(sha)}
trap 'rm -rf -- "$stage"' EXIT
git clone -q --shared --no-checkout "$bare" "$stage"
git -C "$stage" checkout -q --detach {q(sha)}
{tag_lines}stage=$(realpath -e -- "$stage")
work=$(realpath -e -- "$stage/"{q(subdir)})
case "$work/" in "$stage/"*) ;; *) echo "[remote_stage] subdir escapes staging" >&2; exit 1;; esac
[ -d "$work" ] || {{ echo "[remote_stage] subdir is not a directory" >&2; exit 1; }}
"$HOME/bin/lanerun" "$work" -- {shlex.join(command)}
'''


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rev", required=True)
    parser.add_argument("--subdir", default=".")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("a command after -- is required")
    try:
        sha = revision_value(args.rev)
        subdir = subdir_value(args.subdir)
        host = host_value(os.environ.get("LANE_HOST", ""))
        machine_id = machine_value(os.environ.get("CQ_REMOTE_MACHINE_ID", ""))
        def git(*arguments):
            return subprocess.run(["git", *arguments], check=True, capture_output=True,
                                  text=True, creationflags=FLAGS).stdout.strip()
        top = git("rev-parse", "--show-toplevel")
        common = git("-C", top, "rev-parse", "--path-format=absolute", "--git-common-dir")
        resolved = git("-C", top, "rev-parse", "--verify", sha + "^{commit}")
        if resolved != sha:
            raise ValueError("--rev must name an exact commit")
        tags = merged_tags(git, top, sha)
        script = remote_script(repo_id(common), sha[:12] + "-" + uuid.uuid4().hex,
                               sha, subdir, command, machine_id, tags=tags)
        ssh = os.environ.get("CQ_SSH") or system_ssh() or "ssh"
        revs = "".join(f"{rev}\n" for rev in [sha, *(oid for _, oid in tags)])
        with tempfile.TemporaryFile() as pack:
            subprocess.run(["git", "-C", top, "pack-objects", "--stdout", "--revs"],
                           input=revs.encode(), stdout=pack, check=True, creationflags=FLAGS)
            pack.seek(0)
            result = subprocess.run([ssh, *SSH_OPTIONS, host, "bash -c " + shlex.quote(script)],
                                    stdin=pack, creationflags=FLAGS)
        return result.returncode
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        print(f"[remote_stage] {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
