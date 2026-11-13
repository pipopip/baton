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


def sandboxed_value(value):
    """CQ_REMOTE_SANDBOXED: "1" when the job runs in the job sandbox, absent or empty when it does not."""
    if value not in ("", "1"):
        raise ValueError("invalid CQ_REMOTE_SANDBOXED")
    return value == "1"


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


CANDIDATES = 50            # the newest first-parent ancestors offered to the host's mirror
QUESTION_SECONDS = 20      # the question is a convenience: past this the full history is sent
SMALL_HISTORY_KIB = 64 * 1024  # a repository whose objects take less than this on disk is sent whole, unasked
INCREMENTAL_REFUSED = 86   # exit status of a staging script whose incremental pack was refused, before the job starts


def hash_value(value, length):
    return re.fullmatch("[0-9a-f]{%d}" % length, value) is not None


def candidate_revisions(git, top, sha):
    """The snapshot commit and its newest first-parent ancestors, newest first (the snapshot is usually a temporary
    commit on top of HEAD, so: snapshot, HEAD, HEAD~1, ...). Every id is a full lowercase hash of sha's length. A
    failed listing gives none, which means no question and the full history."""
    try:
        listing = git("-C", top, "rev-list", "--first-parent", "-n", str(CANDIDATES), sha)
    except Exception:
        return []
    ids = []
    for line in listing.splitlines():
        if hash_value(line, len(sha)) and line not in ids:
            ids.append(line)
    return ids if ids[:1] == [sha] else []


def history_is_small(git, top):
    """Whether the submitting repository's objects take less than SMALL_HISTORY_KIB on disk (loose and packed, by
    `git count-objects -v`). The question costs one more ssh connection, which was measured at about 6 s to a
    remote host: more than sending a small history costs (a 5.5 MB pack was no measurable part of an 11 s
    staging). False when the size cannot be read: the question is then asked."""
    try:
        sizes = {}
        for line in git("-C", top, "count-objects", "-v").splitlines():
            key, _, value = line.partition(":")
            if key in ("size", "size-pack"):
                sizes[key] = int(value)
        return len(sizes) == 2 and sum(sizes.values()) < SMALL_HISTORY_KIB
    except Exception:
        return False


def question_script(repo):
    """What the host says about the ids on stdin: one `cat-file --batch-check` line each, from its mirror of repo.
    It only reads, so it needs no machine-identity check: the staging script that follows still makes that check
    before it writes anything, and a mirror on the wrong machine could only make the incremental pack fail the
    check after index-pack, which sends the full history instead (see main)."""
    return f'''set -eu
bare="$HOME/.local/share/baton/repos/"{shlex.quote(repo + '.git')}
[ -d "$bare" ] || exit 0
git --git-dir="$bare" cat-file --batch-check
'''


def answer_base(candidates, text):
    """The newest candidate the answer says the mirror has as a commit, or None for none or for anything unexpected:
    the answer must be exactly one line per candidate, in order, each `<that id> commit <size>` or `<that id> missing`."""
    if not candidates or not isinstance(text, str) or not text.endswith("\n"):
        return None
    lines = text[:-1].split("\n")
    if len(lines) != len(candidates):
        return None
    base = None
    for want, line in zip(candidates, lines):
        if re.fullmatch(re.escape(want) + r" (?:(commit) [0-9]{1,20}|missing)", line) is None:
            return None
        if base is None and line != want + " missing":
            base = want
    return base


def mirror_base(ssh, host, repo, candidates):
    """Ask the host (one ssh call, the same client, options and host as the staging) which of the candidates its
    mirror has; the newest one it has, else None. A failed, timed-out or odd answer is None: the staging is then
    what it was without the question."""
    if not candidates:
        return None
    try:
        result = subprocess.run([ssh, *SSH_OPTIONS, host, "bash -c " + shlex.quote(question_script(repo))],
                                input="".join(c + "\n" for c in candidates).encode(), capture_output=True,
                                timeout=QUESTION_SECONDS, creationflags=FLAGS)
        return answer_base(candidates, result.stdout.decode("ascii")) if result.returncode == 0 else None
    except Exception:
        return None


def pack_file():
    """The temporary file a pack is written to and then handed to ssh as its standard input. Unbuffered on purpose:
    the child reads from the file descriptor's own position, and a buffered Python file answers seek(0) from its
    buffer without moving that position once a read has filled the buffer, so after the header was read a small
    pack reached the host as an empty stream (`fatal: early EOF`) and every incremental staging was refused."""
    return tempfile.TemporaryFile(buffering=0)


def pack_object_count(pack):
    """The object count in the header of a pack file object (read from its start), None when it is not a pack header."""
    header = pack.read(12)
    if len(header) != 12 or header[:4] != b"PACK" or int.from_bytes(header[4:8], "big") not in (2, 3):
        return None
    return int.from_bytes(header[8:], "big")


def remote_script(repo, stage_name, sha, subdir, command, machine_id, tags=(), sandboxed=False, base=None, empty_pack=False):
    """base: a commit the host's mirror has, which the pack on stdin was made relative to (None: the pack holds the
    whole history); empty_pack: the pack holds no object, so there is nothing to index."""
    # Every interpolated value is shell-quoted; paths are anchored to $HOME.
    q = shlex.quote
    # Tags go into the stage clone only: the shared mirror must not hold snapshot-specific tag refs. A tag that
    # cannot be set does not fail the staging.
    tag_lines = "".join(f'git -C "$stage" update-ref {q("refs/tags/" + name)} {q(oid)} || true\n' for name, oid in tags)
    # The clone copies the mirror's refs/tags and refs/heads as its own refs. For a sandboxed job those copies are
    # deleted right after the clone, so the stage keeps no ref of the mirror; the job's tags are set later.
    drop_copied = ('copied=$(git -C "$stage" for-each-ref --format=\'delete %(refname)\')\n'
                   'if [ -n "$copied" ]; then printf \'%s\\n\' "$copied" | git -C "$stage" update-ref --no-deref --stdin; fi\n'
                   '[ -z "$(git -C "$stage" for-each-ref)" ] || { echo "[remote_stage] stage clone still holds refs of the mirror" >&2; exit 1; }\n'
                   ) if sandboxed else ""
    # The stage clone borrows its objects from the mirror (alternates), which the job sandbox hides along with the real
    # home. For a sandboxed job the clone takes them over first: the refs the clone copied from the mirror are deleted
    # and the reflogs emptied, so what repack -a (without -l) keeps is reachable only from the clone's own HEAD (the
    # detached snapshot commit) and the job's tags, borrowed objects included. That is the job's history and nothing
    # else of the mirror. Under set -eu a failing repack, a missing alternates file or a commit that still cannot be
    # read ends the staging before the job starts; the trap removes the stage.
    self_contained = (f'git -C "$stage" reflog expire --expire=now --all\n'
                      f'git -C "$stage" repack -a -d -q\n'
                      f'rm -- "$stage/.git/objects/info/alternates"\n'
                      f'git -C "$stage" rev-list --quiet {q(sha)}\n') if sandboxed else ""
    # The mirror is written by this script only (index-pack --stdin moves a pack into place only when it is whole, then
    # update-ref), and nothing prunes or garbage-collects it. Every pack it holds is the closure of a snapshot commit:
    # a full one, or one relative to a commit that was complete already. So a commit the mirror has is complete, and
    # a pack of what the commit lacks, made relative to it, completes the mirror. So a commit the mirror has is normally
    # complete. The script does not rely on that: before the job starts, the incremental form checks that every object
    # reachable from the snapshot is in the mirror, whatever the mirror held before. It is self-contained (not thin), so
    # index-pack needs no --fix-thin. Anything missing exits INCREMENTAL_REFUSED, which main answers with one more
    # attempt that sends the full history; that also repairs the mirror. Only this script can end that way before the
    # job starts: a job's own exit status 86 becomes 1, so the retry can never start the job twice.
    if base is None:
        ingest = ("# The pack contains only objects reachable from the requested commit.\n"
                  'git --git-dir="$bare" index-pack --stdin >/dev/null\n')
        run = f'"$HOME/bin/lanerun" "$work" -- {shlex.join(command)}\n'
    else:
        check = f'git --git-dir="$bare" rev-list --objects --quiet {q(sha)}'
        steps = check if empty_pack else 'git --git-dir="$bare" index-pack --stdin >/dev/null && ' + check
        ingest = (f"# The pack holds what the requested commit has beyond {base}, which the mirror has.\n"
                  "{ " + steps + '; } || { echo "[remote_stage] incremental pack refused by the host" >&2; '
                  f"exit {INCREMENTAL_REFUSED}; }}\n")
        run = (f'rc=0\n"$HOME/bin/lanerun" "$work" -- {shlex.join(command)} || rc=$?\n'
               f'[ "$rc" -ne {INCREMENTAL_REFUSED} ] || rc=1\nexit "$rc"\n')
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
{ingest}git --git-dir="$bare" update-ref refs/baton-snapshots/{q(sha)} {q(sha)}
trap 'rm -rf -- "$stage"' EXIT
git clone -q --shared --no-checkout "$bare" "$stage"
{drop_copied}git -C "$stage" checkout -q --detach {q(sha)}
{tag_lines}{self_contained}stage=$(realpath -e -- "$stage")
work=$(realpath -e -- "$stage/"{q(subdir)})
case "$work/" in "$stage/"*) ;; *) echo "[remote_stage] subdir escapes staging" >&2; exit 1;; esac
[ -d "$work" ] || {{ echo "[remote_stage] subdir is not a directory" >&2; exit 1; }}
{run}'''


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
        sandboxed = sandboxed_value(os.environ.get("CQ_REMOTE_SANDBOXED", ""))
        def git(*arguments):
            return subprocess.run(["git", *arguments], check=True, capture_output=True,
                                  text=True, creationflags=FLAGS).stdout.strip()
        top = git("rev-parse", "--show-toplevel")
        common = git("-C", top, "rev-parse", "--path-format=absolute", "--git-common-dir")
        resolved = git("-C", top, "rev-parse", "--verify", sha + "^{commit}")
        if resolved != sha:
            raise ValueError("--rev must name an exact commit")
        tags = merged_tags(git, top, sha)
        repo = repo_id(common)
        stage_name = sha[:12] + "-" + uuid.uuid4().hex
        ssh = os.environ.get("CQ_SSH") or system_ssh() or "ssh"

        def stage(base):
            """One ssh call that stages and runs. With a base the pack holds only what the host's mirror lacks."""
            revs = [sha] + ([f"^{base}"] if base else []) + [oid for _, oid in tags]
            with pack_file() as pack:
                subprocess.run(["git", "-C", top, "pack-objects", "--stdout", "--revs"],
                               input="".join(f"{rev}\n" for rev in revs).encode(), stdout=pack, check=True,
                               creationflags=FLAGS)
                pack.seek(0)
                empty = base is not None and pack_object_count(pack) == 0
                pack.seek(0)
                script = remote_script(repo, stage_name, sha, subdir, command, machine_id, tags=tags,
                                       sandboxed=sandboxed, base=base, empty_pack=empty)
                return subprocess.run([ssh, *SSH_OPTIONS, host, "bash -c " + shlex.quote(script)],
                                      stdin=subprocess.DEVNULL if empty else pack, creationflags=FLAGS).returncode

        # a small history is sent whole: the question's ssh connection would cost more than the upload it saves
        base = None if history_is_small(git, top) else mirror_base(ssh, host, repo, candidate_revisions(git, top, sha))
        if base:
            try:
                code = stage(base)
            except subprocess.CalledProcessError:
                code = INCREMENTAL_REFUSED
            if code != INCREMENTAL_REFUSED:
                return code
            print("[remote_stage] incremental staging failed; sending the full history", file=sys.stderr)
        return stage(None)
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        print(f"[remote_stage] {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
