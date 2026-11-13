import contextlib
import importlib.util
import io
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gnu_shims_for_tests  # noqa: E402

gnu_shims_for_tests.install()   # the staging script runs locally as the fake host: a Mac has no `realpath -e`

spec = importlib.util.spec_from_file_location("remote_stage", Path(__file__).with_name("remote_stage.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def git_bash():
    """Git for Windows' bash.exe, found above the git on PATH (a bare `bash` may be WSL's launcher)."""
    git = shutil.which("git")
    for parent in Path(git).parents if git else ():
        if (parent / "bin" / "bash.exe").exists():
            return str(parent / "bin" / "bash.exe")
    return None


class RemoteStageTests(unittest.TestCase):
    def test_inputs(self):
        for host in ("-oProxyCommand=x", "a b", "host\nwhoami", "user@"):
            with self.subTest(host=host), self.assertRaises(ValueError):
                runner.host_value(host)
        self.assertEqual(runner.host_value("build@host.example"), "build@host.example")
        for path in ("../x", "a/../b", "/x", "a\\b", "a\nx", ""):
            with self.subTest(path=path), self.assertRaises(ValueError):
                runner.subdir_value(path)
        self.assertEqual(runner.subdir_value("a/b"), "a/b")
        for sha in ("a" * 39, "G" * 40, "a" * 41):
            with self.assertRaises(ValueError):
                runner.revision_value(sha)
        with self.assertRaises(ValueError):
            runner.machine_value("abc")

    def test_script_quotes_and_identity_precedes_writes(self):
        cmd = ["echo", "$(touch /tmp/unsafe)", "a'b"]
        script = runner.remote_script("repo-123", "stage-123", "a" * 40, "dir with space", cmd, "b" * 32)
        self.assertIn(shlex.join(cmd), script)
        self.assertIn("$HOME/.local/share/baton", script)
        self.assertIn('"$HOME/bin/lanerun"', script)
        self.assertLess(script.index("machine identity mismatch"), script.index("mkdir -p"))
        self.assertLess(script.index("machine identity mismatch"), script.index('"$HOME/bin/lanerun"'))
        self.assertIn('git -C "$stage" checkout -q --detach', script)
        self.assertNotIn("/srv/example-user", script)

    def test_end_to_end_single_ssh_with_fake_host(self):
        # Run the generated remote shell locally against an isolated HOME.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "source"
            repo.mkdir()
            def git(*args):
                return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()
            git("init", "-q")
            git("config", "user.name", "Test")
            git("config", "user.email", "test@example.invalid")
            (repo / "file").write_text("committed", encoding="utf-8")
            git("add", "file")
            git("commit", "-qm", "test")
            sha = git("rev-parse", "HEAD")
            pack = subprocess.run(["git", "-C", str(repo), "pack-objects", "--stdout", "--revs"], input=(sha + "\n").encode(), capture_output=True, check=True).stdout
            home = root / "home"
            (home / "bin").mkdir(parents=True)
            lanerun = home / "bin" / "lanerun"
            lanerun.write_text('#!/bin/sh\ncd "$1"\nshift 2\nexec "$@"\n', encoding="utf-8")
            bash = "bash"
            home_env = str(home)
            if os.name == "nt":
                bash = git_bash()
                if bash is None:
                    self.skipTest("Git Bash is unavailable")
                home_env = "/" + home.drive[0].lower() + home.as_posix()[2:]
            else:
                lanerun.chmod(0o755)
            script = runner.remote_script("repo-id", "stage-id", sha, ".", ['sh', '-c', 'cat file; git rev-parse HEAD'], "")
            result = subprocess.run([bash, "-c", script], input=pack, capture_output=True, env={**os.environ, "HOME": home_env})
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(result.stdout, b"committed" + sha.encode() + b"\n")
            self.assertFalse((home / ".local/share/baton/staging/repo-id/stage-id").exists())
            fail_script = runner.remote_script("repo-id", "other-stage", sha, ".", ["sh", "-c", "exit 75"], "")
            result = subprocess.run([bash, "-c", fail_script], input=pack, capture_output=True, env={**os.environ, "HOME": home_env})
            self.assertEqual(result.returncode, 75, result.stderr.decode())
            self.assertFalse((home / ".local/share/baton/staging/repo-id/other-stage").exists())
            mismatch_home = root / "mismatch"
            mismatch_home.mkdir()
            mismatch_env = str(mismatch_home)
            if os.name == "nt":
                mismatch_env = "/" + mismatch_home.drive[0].lower() + mismatch_home.as_posix()[2:]
            mismatch = runner.remote_script("repo-id", "stage", sha, ".", ["true"], "0" * 32)
            result = subprocess.run([bash, "-c", mismatch], input=pack, capture_output=True, env={**os.environ, "HOME": mismatch_env})
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((mismatch_home / ".local").exists())

    def test_merged_tags_filters_names_and_oids(self):
        oid = "a" * 40
        bad_names = ["../x", "a..b", "x.lock", ".hidden", "a/.b", "a b", "$(id)", ";rm", "a" * 201, "end/", "end.", "a//b"]
        good = {"v1": oid, "freeze/lane-base-2026-10-15": "b" * 64, "rel+1": "c" * 40, "a" * 200: "d" * 40}
        lines = [f"{oid} refs/tags/{name}" for name in bad_names]
        lines += [f"{good_oid} refs/tags/{name}" for name, good_oid in good.items()]
        lines += [f"{'a' * 39} refs/tags/short", f"{'a' * 41} refs/tags/long", f"{'A' * 40} refs/tags/upper", f"{'g' * 40} refs/tags/nothex"]
        calls = []
        def fake_git(*args):
            calls.append(args)
            return "\n".join(lines)
        tags = runner.merged_tags(fake_git, "/top", "c" * 40)
        self.assertEqual(calls, [("-C", "/top", "for-each-ref", "--merged", "c" * 40, "--sort=-creatordate",
                                  "--format=%(objectname) %(refname)", "refs/tags")])
        self.assertEqual(tags, [("a" * 200, "d" * 40), ("freeze/lane-base-2026-10-15", "b" * 64), ("rel+1", "c" * 40), ("v1", oid)])

    def test_merged_tags_keeps_the_newest_within_a_count_and_a_size(self):
        oid = "a" * 40
        # git lists them newest first: the first 64 are kept, whatever their names
        lines = [f"{oid} refs/tags/t{i:03d}" for i in range(299, -1, -1)]
        tags = runner.merged_tags(lambda *args: "\n".join(lines), "/top", "c" * 40)
        self.assertEqual(len(tags), runner.MAX_TAGS)
        self.assertEqual((tags[0], tags[-1]), (("t236", oid), ("t299", oid)))
        # long names stop the list at the size bound instead
        long = [f"{oid} refs/tags/{'n' * 190}{i:02d}" for i in range(60)]
        kept = runner.merged_tags(lambda *args: "\n".join(long), "/top", "c" * 40)
        self.assertEqual(len(kept), runner.MAX_TAG_BYTES // (192 + 40))
        self.assertLessEqual(sum(len(n) + len(o) for n, o in kept), runner.MAX_TAG_BYTES)
        # and the whole remote script stays far inside one Windows command line with the largest tag list
        worst = [("x" * 53, oid)] * runner.MAX_TAGS
        script = runner.remote_script("repo-123", "stage-123", oid, ".", ["bash", "-c", "y" * 6000], "", tags=worst)
        self.assertLess(len(script), 24000)

    def test_merged_tags_gives_nothing_when_git_fails(self):
        def failing_git(*args):
            raise subprocess.CalledProcessError(128, ["git", *args])
        self.assertEqual(runner.merged_tags(failing_git, "/top", "c" * 40), [])

    def test_script_stages_tags_after_checkout_and_not_in_mirror(self):
        sha = "a" * 40
        tags = [("v1", "b" * 40), ("odd name", "c" * 40)]
        script = runner.remote_script("repo-123", "stage-123", sha, ".", ["true"], "", tags=tags)
        lines = script.splitlines()
        checkout = lines.index(f'git -C "$stage" checkout -q --detach {shlex.quote(sha)}')
        for name, oid in tags:
            line = f'git -C "$stage" update-ref {shlex.quote("refs/tags/" + name)} {shlex.quote(oid)} || true'
            self.assertIn(line, lines)
            self.assertGreater(lines.index(line), checkout)
        self.assertIn("'refs/tags/odd name'", script)
        for line in lines:
            if "refs/tags" in line:
                self.assertNotIn("$bare", line)
        without = runner.remote_script("repo-123", "stage-123", sha, ".", ["true"], "")
        self.assertEqual(without, runner.remote_script("repo-123", "stage-123", sha, ".", ["true"], "", tags=[]))
        self.assertNotIn("refs/tags", without)

    def test_sandboxed_flag_is_validated(self):
        self.assertIs(runner.sandboxed_value(""), False)
        self.assertIs(runner.sandboxed_value("1"), True)
        for bad in ("0", "true", "yes", " 1", "1\n", "11"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                runner.sandboxed_value(bad)

    def test_sandboxed_script_makes_the_clone_self_contained_after_checkout_and_tags(self):
        sha = "a" * 40
        tags = [("v1", "b" * 40)]
        script = runner.remote_script("repo-123", "stage-123", sha, ".", ["true"], "", tags=tags, sandboxed=True)
        lines = script.splitlines()
        checkout = lines.index(f'git -C "$stage" checkout -q --detach {sha}')
        tag = lines.index('git -C "$stage" update-ref refs/tags/v1 ' + "b" * 40 + " || true")
        repack = lines.index('git -C "$stage" repack -a -d -q')
        expire = lines.index('git -C "$stage" reflog expire --expire=now --all')
        remove = lines.index('rm -- "$stage/.git/objects/info/alternates"')
        verify = lines.index(f'git -C "$stage" rev-list --quiet {sha}')
        started = next(i for i, line in enumerate(lines) if line.startswith('"$HOME/bin/lanerun"'))
        self.assertLess(checkout, tag)
        self.assertLess(tag, expire)
        self.assertLess(expire, repack)
        self.assertLess(repack, remove)
        self.assertLess(repack, remove)
        self.assertLess(remove, verify)
        self.assertLess(verify, started)
        # nothing here fails softly: no repack or removal line carries an `|| true`
        for line in lines[repack:verify + 1]:
            self.assertNotIn("||", line)
        # the mirror is never exposed to the job by another route
        self.assertNotIn("bind", script)
        self.assertNotIn("--link", script)
        self.assertEqual(script.count("repack"), 1)

    def test_unsandboxed_script_keeps_the_shared_clone(self):
        sha = "a" * 40
        for kwargs in ({}, {"sandboxed": False}):
            script = runner.remote_script("repo-123", "stage-123", sha, ".", ["true"], "", tags=[("v1", "b" * 40)], **kwargs)
            self.assertIn('git clone -q --shared --no-checkout "$bare" "$stage"', script)
            self.assertNotIn("repack", script)
            self.assertNotIn("alternates", script)
            self.assertNotIn("rev-list", script)
        # a sandboxed script differs from the shared one by those lines only
        plain = runner.remote_script("repo-123", "stage-123", sha, ".", ["true"], "")
        boxed = runner.remote_script("repo-123", "stage-123", sha, ".", ["true"], "", sandboxed=True)
        self.assertEqual([line for line in boxed.splitlines() if line not in plain.splitlines()],
                         ['copied=$(git -C "$stage" for-each-ref --format=\'delete %(refname)\')',
                          'if [ -n "$copied" ]; then printf \'%s\\n\' "$copied" | git -C "$stage" update-ref --no-deref --stdin; fi',
                          '[ -z "$(git -C "$stage" for-each-ref)" ] || { echo "[remote_stage] stage clone still holds refs of the mirror" >&2; exit 1; }',
                          'git -C "$stage" reflog expire --expire=now --all',
                          'git -C "$stage" repack -a -d -q', 'rm -- "$stage/.git/objects/info/alternates"',
                          f'git -C "$stage" rev-list --quiet {sha}'])

    def test_sandboxed_script_drops_the_refs_the_clone_copied_between_clone_and_checkout(self):
        sha = "a" * 40
        drop = ['copied=$(git -C "$stage" for-each-ref --format=\'delete %(refname)\')',
                'if [ -n "$copied" ]; then printf \'%s\\n\' "$copied" | git -C "$stage" update-ref --no-deref --stdin; fi',
                '[ -z "$(git -C "$stage" for-each-ref)" ] || { echo "[remote_stage] stage clone still holds refs of the mirror" >&2; exit 1; }']
        boxed = runner.remote_script("repo-123", "stage-123", sha, ".", ["true"], "", tags=[("v1", "b" * 40)], sandboxed=True)
        lines = boxed.splitlines()
        clone = lines.index('git clone -q --shared --no-checkout "$bare" "$stage"')
        checkout = lines.index(f'git -C "$stage" checkout -q --detach {sha}')
        self.assertEqual(lines[clone + 1:checkout], drop)
        self.assertLess(lines.index('git -C "$stage" update-ref refs/tags/v1 ' + "b" * 40 + " || true"), lines.index('git -C "$stage" reflog expire --expire=now --all'))
        # the unsandboxed script has none of these lines, and its clone-to-checkout span is unchanged
        plain = runner.remote_script("repo-123", "stage-123", sha, ".", ["true"], "", tags=[("v1", "b" * 40)])
        self.assertEqual(plain.splitlines()[plain.splitlines().index('git clone -q --shared --no-checkout "$bare" "$stage"') + 1], f'git -C "$stage" checkout -q --detach {sha}')
        for line in drop + ["reflog expire"]:
            self.assertFalse(any(line in other for other in plain.splitlines()))

    def test_end_to_end_sandboxed_stage_holds_no_ref_of_the_mirror(self):
        if shutil.which("git") is None:
            self.skipTest("git is unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "source"
            repo.mkdir()

            def git(*args):
                return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()
            git("init", "-q")
            git("config", "user.name", "Test")
            git("config", "user.email", "test@example.invalid")
            (repo / "file").write_text("A", encoding="utf-8")
            git("add", "file")
            git("commit", "-qm", "A")
            snap_a = git("rev-parse", "HEAD")
            # B is a history of its own (no common ancestor with A): two commits on an orphan line
            git("checkout", "-q", "--orphan", "line-b")
            (repo / "file").write_text("B1", encoding="utf-8")
            git("add", "file")
            git("commit", "-qm", "B1")
            (repo / "file").write_text("B2", encoding="utf-8")
            git("commit", "-qam", "B2")
            snap_b = git("rev-parse", "HEAD")
            home = root / "home"
            (home / "bin").mkdir(parents=True)
            lanerun = home / "bin" / "lanerun"
            lanerun.write_text('#!/bin/sh\ncd "$1"\nshift 2\nexec "$@"\n', encoding="utf-8")
            bash, home_env = "bash", str(home)
            if os.name == "nt":
                bash = git_bash()
                if bash is None:
                    self.skipTest("Git Bash is unavailable")
                home_env = "/" + home.drive[0].lower() + home.as_posix()[2:]
            else:
                lanerun.chmod(0o755)
            env = {**os.environ, "HOME": home_env}
            # the mirror as earlier jobs left it: both snapshots, and besides them a tag and a branch on A with HEAD on main
            mirror = home / ".local" / "share" / "baton" / "repos" / "repo-id.git"
            mirror.parent.mkdir(parents=True)
            subprocess.run(["git", "init", "-q", "--bare", str(mirror)], check=True)
            subprocess.run(["git", "-C", str(repo), "push", "-q", str(mirror), f"{snap_a}:refs/baton-snapshots/A",
                            f"{snap_b}:refs/baton-snapshots/B", f"{snap_a}:refs/heads/main", f"{snap_a}:refs/heads/other",
                            f"{snap_a}:refs/tags/old"], check=True, capture_output=True)
            subprocess.run(["git", "--git-dir", str(mirror), "symbolic-ref", "HEAD", "refs/heads/main"], check=True)
            subprocess.run(["git", "--git-dir", str(mirror), "update-ref", "refs/heads/main", snap_a], check=True)

            def pack(*revs):
                return subprocess.run(["git", "-C", str(repo), "pack-objects", "--stdout", "--revs"],
                                      input="".join(r + "\n" for r in revs).encode(), capture_output=True, check=True).stdout

            def stage(sandboxed, job):
                script = runner.remote_script("repo-id", "job-stage", snap_b, ".", ["sh", "-c", job], "",
                                              tags=[("job", snap_b)], sandboxed=sandboxed)
                return subprocess.run([bash, "-c", script], input=pack(snap_b), capture_output=True, env=env)

            result = stage(True, f'echo "a:$(git cat-file -e {snap_a} 2>/dev/null && echo visible || echo absent)" && '
                                 "git log --format=%s && git for-each-ref --format='%(refname)' && git fsck --no-dangling && "
                                 'test ! -e .git/objects/info/alternates && echo no-alternates')
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(result.stdout.decode().splitlines(), ["a:absent", "B2", "B1", "refs/tags/job", "no-alternates"])
            self.assertFalse((home / ".local/share/baton/staging/repo-id/job-stage").exists())
            # the same mirror staged unsandboxed still borrows from it
            result = stage(False, 'test -e .git/objects/info/alternates && echo shared && git log --format=%s')
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(result.stdout.decode().splitlines(), ["shared", "B2", "B1"])

    def test_main_reads_the_sandbox_signal_from_the_environment(self):
        scripts = []

        def fake_run(argv, **kwargs):
            if argv[0] == "git":
                out = {"rev-parse": "a" * 40, "pack-objects": b""}
                if "pack-objects" in argv:
                    return subprocess.CompletedProcess(argv, 0)
                return subprocess.CompletedProcess(argv, 0, stdout=out["rev-parse"] + "\n")
            if "cat-file --batch-check" in argv[-1]:  # the question about the host's mirror: no answer
                return subprocess.CompletedProcess(argv, 1)
            scripts.append(argv[-1])
            return subprocess.CompletedProcess(argv, 0)
        base = {"LANE_HOST": "host.example", "CQ_SSH": "ssh"}
        argv = ["--rev", "a" * 40, "--", "true"]
        for extra, expected in (({}, False), ({"CQ_REMOTE_SANDBOXED": ""}, False), ({"CQ_REMOTE_SANDBOXED": "1"}, True)):
            with self.subTest(extra=extra):
                scripts.clear()
                with unittest.mock.patch.dict(os.environ, {**base, **extra}, clear=False), \
                        unittest.mock.patch.object(runner.subprocess, "run", side_effect=fake_run):
                    if not extra:
                        os.environ.pop("CQ_REMOTE_SANDBOXED", None)
                    self.assertEqual(runner.main(argv), 0)
                self.assertEqual(len(scripts), 1)
                self.assertEqual('git -C "$stage" repack -a -d -q' in scripts[0], expected)
        with unittest.mock.patch.dict(os.environ, {**base, "CQ_REMOTE_SANDBOXED": "yes"}), \
                unittest.mock.patch.object(runner.subprocess, "run", side_effect=fake_run):
            scripts.clear()
            with contextlib.redirect_stderr(io.StringIO()) as err:
                self.assertEqual(runner.main(argv), 2)
            self.assertIn("invalid CQ_REMOTE_SANDBOXED", err.getvalue())
            self.assertEqual(scripts, [])

    def staged(self, tmp, sandboxed, job, edit=None):
        """Run the staging script locally against an isolated HOME: the mirror holds the job's commit and a later commit
        that is not in the job's history; the job command runs in the stage clone. Returns (result, home)."""
        root = Path(tmp)
        repo = root / "source"
        repo.mkdir()

        def git(*args):
            return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()
        git("init", "-q")
        git("config", "user.name", "Test")
        git("config", "user.email", "test@example.invalid")
        for name in ("first", "second", "third"):
            (repo / "file").write_text(name, encoding="utf-8")
            git("add", "file")
            git("commit", "-qm", name)
            if name == "first":
                git("tag", "-a", "v1", "-m", "annotated")
        shas = git("rev-list", "--reverse", "HEAD").split()
        job_sha, other = shas[1], shas[2]  # the job runs the second commit; the third is another snapshot in the mirror
        home = root / "home"
        (home / "bin").mkdir(parents=True)
        lanerun = home / "bin" / "lanerun"
        lanerun.write_text('#!/bin/sh\ncd "$1"\nshift 2\nexec "$@"\n', encoding="utf-8")
        bash, home_env = "bash", str(home)
        if os.name == "nt":
            bash = git_bash()
            if bash is None:
                self.skipTest("Git Bash is unavailable")
            home_env = "/" + home.drive[0].lower() + home.as_posix()[2:]
        else:
            lanerun.chmod(0o755)
        env = {**os.environ, "HOME": home_env}

        def pack(*revs):
            return subprocess.run(["git", "-C", str(repo), "pack-objects", "--stdout", "--revs"],
                                  input="".join(r + "\n" for r in revs).encode(), capture_output=True, check=True).stdout
        tags = runner.merged_tags(lambda *a: subprocess.run(["git", *a], check=True, capture_output=True, text=True).stdout.strip(),
                                  str(repo), job_sha)
        self.assertEqual([name for name, _ in tags], ["v1"])
        # the other snapshot reaches the mirror first, as an earlier job's would
        other_script = runner.remote_script("repo-id", "other-stage", other, ".", ["true"], "")
        done = subprocess.run([bash, "-c", other_script], input=pack(other), capture_output=True, env=env)
        self.assertEqual(done.returncode, 0, done.stderr.decode())
        script = runner.remote_script("repo-id", "job-stage", job_sha, ".", ["sh", "-c", job.format(other=other, first=shas[0])], "",
                                      tags=tags, sandboxed=sandboxed)
        script = edit(script) if edit else script
        result = subprocess.run([bash, "-c", script], input=pack(job_sha, *(oid for _, oid in tags)), capture_output=True, env=env)
        return result, home, (job_sha, other, shas[0])

    HISTORY_JOB = ('mv "$HOME/.local/share/baton/repos" "$HOME/moved-away" && '
                   'git log --format=%H && git describe --tags && git cat-file -t v1 && git cat-file -p HEAD:file && echo && '
                   'echo "other:$(git cat-file -e {other} 2>/dev/null && echo visible || echo absent)" && '
                   'git fsck --no-dangling && test ! -e .git/objects/info/alternates && echo no-alternates')

    def test_end_to_end_sandboxed_stage_keeps_its_history_without_the_mirror(self):
        if shutil.which("git") is None:
            self.skipTest("git is unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            result, home, (job_sha, other, first) = self.staged(tmp, True, self.HISTORY_JOB)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            # the job's own history and tag, read after the mirror was moved away; the other snapshot is not in the clone
            lines = result.stdout.decode().splitlines()
            self.assertEqual(lines[:2], [job_sha, first])
            self.assertEqual(lines[2], "v1-1-g" + job_sha[:7])
            self.assertEqual(lines[3:], ["tag", "second", "other:absent", "no-alternates"])
            self.assertFalse((home / ".local/share/baton/staging/repo-id/job-stage").exists())
            # the mirror itself is untouched and still holds both snapshots (the job moved it, so look where it went)
            moved = home / "moved-away" / "repo-id.git"
            for sha in (job_sha, other):
                self.assertEqual(subprocess.run(["git", "--git-dir", str(moved), "cat-file", "-t", sha],
                                                capture_output=True, text=True).stdout.strip(), "commit")

    def test_end_to_end_unsandboxed_stage_still_borrows_from_the_mirror(self):
        if shutil.which("git") is None:
            self.skipTest("git is unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            result, home, _ = self.staged(tmp, False, 'test -e .git/objects/info/alternates && echo shared && '
                                                      'mv "$HOME/.local/share/baton/repos" "$HOME/moved-away" && ! git log --format=%H')
            self.assertEqual(result.stdout.decode().splitlines(), ["shared"])

    def test_end_to_end_a_failing_repack_fails_the_staging_before_the_job(self):
        if shutil.which("git") is None:
            self.skipTest("git is unavailable")
        with tempfile.TemporaryDirectory() as tmp:
            # the repack line made to fail: the job command must not run and the stage must be removed
            result, home, _ = self.staged(tmp, True, "echo job-started",
                                          edit=lambda script: script.replace("repack -a -d -q", "repack --no-such-option"))
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(b"job-started", result.stdout)
            self.assertIn(b"no-such-option", result.stderr)
            self.assertFalse((home / ".local/share/baton/staging/repo-id/job-stage").exists())
        with tempfile.TemporaryDirectory() as tmp:
            # an alternates file that is not there (the clone was not shared): the removal fails loudly as well
            result, home, _ = self.staged(tmp, True, "echo job-started",
                                          edit=lambda script: script.replace("clone -q --shared", "clone -q"))
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(b"job-started", result.stdout)
            self.assertFalse((home / ".local/share/baton/staging/repo-id/job-stage").exists())

    def test_end_to_end_stages_merged_tags_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "source"
            repo.mkdir()
            def git(*args):
                return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout.strip()
            def git_top(*args):
                return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout.strip()
            git("init", "-q")
            git("config", "user.name", "Test")
            git("config", "user.email", "test@example.invalid")
            (repo / "file").write_text("first", encoding="utf-8")
            git("add", "file")
            git("commit", "-qm", "first")
            sha = git("rev-parse", "HEAD")
            git("tag", "light")
            git("tag", "-a", "ann", "-m", "annotated")
            (repo / "file").write_text("second", encoding="utf-8")
            git("commit", "-qam", "second")
            git("tag", "later")
            tags = runner.merged_tags(git_top, str(repo), sha)
            self.assertEqual([name for name, _ in tags], ["ann", "light"])
            revs = "".join(f"{rev}\n" for rev in [sha, *(oid for _, oid in tags)])
            pack = subprocess.run(["git", "-C", str(repo), "pack-objects", "--stdout", "--revs"], input=revs.encode(), capture_output=True, check=True).stdout
            home = root / "home"
            (home / "bin").mkdir(parents=True)
            lanerun = home / "bin" / "lanerun"
            lanerun.write_text('#!/bin/sh\ncd "$1"\nshift 2\nexec "$@"\n', encoding="utf-8")
            bash = "bash"
            home_env = str(home)
            if os.name == "nt":
                bash = git_bash()
                if bash is None:
                    self.skipTest("Git Bash is unavailable")
                home_env = "/" + home.drive[0].lower() + home.as_posix()[2:]
            else:
                lanerun.chmod(0o755)
            script = runner.remote_script("repo-id", "tag-stage", sha, ".",
                                          ["sh", "-c", "git tag; git cat-file -t ann; git describe --tags"], "", tags=tags)
            result = subprocess.run([bash, "-c", script], input=pack, capture_output=True, env={**os.environ, "HOME": home_env})
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            output = result.stdout.decode().splitlines()
            self.assertEqual(output[:3], ["ann", "light", "tag"])
            self.assertIn(output[3], ("ann", "light"))
            self.assertEqual(len(output), 4)
            bare = home / ".local/share/baton/repos/repo-id.git"
            self.assertEqual(subprocess.run(["git", "--git-dir", str(bare), "tag"], capture_output=True, text=True, check=True).stdout, "")


def pack_objects_in(data):
    return runner.pack_object_count(io.BytesIO(data))


class IncrementalStagingTests(unittest.TestCase):
    """main() against a real source repository and a real mirror: the ssh calls are replaced by running the generated
    scripts in bash against an isolated HOME, the way the other main tests replace them."""

    def setUp(self):
        if shutil.which("git") is None:
            self.skipTest("git is unavailable")
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.repo = self.root / "source"
        self.repo.mkdir()
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.invalid")
        # these repositories are tiny: without this every one would be sent whole, unasked (history_is_small)
        patch = unittest.mock.patch.object(runner, "SMALL_HISTORY_KIB", 0)
        patch.start()
        self.addCleanup(patch.stop)
        self.home = self.root / "home"
        (self.home / "bin").mkdir(parents=True)
        lanerun = self.home / "bin" / "lanerun"
        lanerun.write_text('#!/bin/sh\ncd "$1"\nshift 2\nexec "$@"\n', encoding="utf-8")
        self.bash, self.home_env = "bash", str(self.home)
        if os.name == "nt":
            self.bash = git_bash()
            if self.bash is None:
                self.skipTest("Git Bash is unavailable")
            self.home_env = "/" + self.home.drive[0].lower() + self.home.as_posix()[2:]
        else:
            lanerun.chmod(0o755)
        self.real_run = subprocess.run
        self.stages = []     # one entry per staging call: the script and the pack that went with it
        self.questions = []  # one entry per question: the ssh argv, its keyword arguments and its script
        self.answer = None   # replaces the question's answer: a callable (candidates) -> CompletedProcess, or raising
        self.fail_incremental_pack = False

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.repo), *args], check=True, capture_output=True, text=True).stdout.strip()

    def commit(self, name):
        (self.repo / name).write_text(name, encoding="utf-8")
        self.git("add", name)
        self.git("commit", "-qm", name)
        return self.git("rev-parse", "HEAD")

    def mirror(self):
        found = list((self.home / ".local/share/baton/repos").glob("*.git"))
        self.assertEqual(len(found), 1)
        return found[0]

    def mirror_git(self, *args):
        return subprocess.run(["git", "--git-dir", str(self.mirror()), *args], check=True, capture_output=True, text=True).stdout.strip()

    def assert_mirror_complete(self, *shas):
        self.mirror_git("fsck", "--full", "--no-dangling")
        for sha in shas:
            self.mirror_git("rev-list", "--objects", "--quiet", sha)
            self.assertEqual(self.mirror_git("cat-file", "-t", sha), "commit")

    def fake_run(self, argv, **kwargs):
        if argv[0] == "git":
            if "pack-objects" in argv and self.fail_incremental_pack and b"^" in kwargs["input"]:
                raise subprocess.CalledProcessError(128, argv)
            return self.real_run(argv, cwd=self.repo, **kwargs)
        script = shlex.split(argv[-1])[2]
        env = {**os.environ, "HOME": self.home_env}
        if "cat-file --batch-check" in script and "index-pack" not in script:
            self.questions.append((argv, kwargs, script))
            if self.answer is not None:
                return self.answer(kwargs["input"].decode().split())
            done = self.real_run([self.bash, "-c", script], input=kwargs["input"], capture_output=True, env=env)
            return subprocess.CompletedProcess(argv, done.returncode, done.stdout, done.stderr)
        stdin = kwargs["stdin"]
        data = b"" if stdin is subprocess.DEVNULL else stdin.read()
        self.stages.append({"script": script, "pack": data, "devnull": stdin is subprocess.DEVNULL, "argv": argv})
        done = self.real_run([self.bash, "-c", script], input=data, capture_output=True, env=env)
        sys.stderr.write(done.stderr.decode())
        return subprocess.CompletedProcess(argv, done.returncode)

    def stage(self, sha, job='echo ran >> "$HOME/ran"', sandboxed=False):
        """main() for sha; returns (exit code, its stderr). The job appends a line to $HOME/ran unless it is replaced."""
        env = {"LANE_HOST": "host.example", "CQ_SSH": "ssh"}
        with unittest.mock.patch.dict(os.environ, env), unittest.mock.patch.object(runner.subprocess, "run", side_effect=self.fake_run):
            os.environ.pop("CQ_REMOTE_MACHINE_ID", None)
            os.environ.pop("CQ_REMOTE_SANDBOXED", None)
            if sandboxed:
                os.environ["CQ_REMOTE_SANDBOXED"] = "1"
            with contextlib.redirect_stderr(io.StringIO()) as err:
                code = runner.main(["--rev", sha, "--", "sh", "-c", job])
        return code, err.getvalue()

    def runs(self):
        ran = self.home / "ran"
        return ran.read_text().splitlines() if ran.exists() else []

    def test_first_staging_sends_the_whole_history_and_the_next_only_what_is_new(self):
        a, b, c = self.commit("a"), self.commit("b"), self.commit("c")
        self.assertEqual(self.stage(c)[0], 0)
        # an empty mirror has none of the candidates: the full pack, three commits with a tree and a blob each
        self.assertEqual(len(self.questions), 1)
        self.assertEqual(pack_objects_in(self.stages[0]["pack"]), 9)
        self.assertNotIn("--not", self.stages[0]["script"])
        d = self.commit("d")
        full_size = len(self.stages[0]["pack"])
        self.assertEqual(self.stage(d)[0], 0)
        # the mirror had c: the pack holds the commit, its tree and its blob only
        self.assertEqual(pack_objects_in(self.stages[1]["pack"]), 3)
        self.assertLess(len(self.stages[1]["pack"]), full_size)
        self.assertIn(f"rev-list --objects --quiet {d}; }} || ", self.stages[1]["script"])
        self.assertEqual(self.runs(), ["ran", "ran"])
        self.assert_mirror_complete(c, d)
        self.assertEqual(self.mirror_git("rev-list", "--count", d), "4")
        self.assertEqual(self.mirror_git("rev-parse", f"refs/baton-snapshots/{d}"), d)

    def test_the_newest_candidate_the_mirror_has_is_the_base(self):
        a, b, c, d = (self.commit(name) for name in "abcd")
        self.stage(c)
        # the snapshot commit is a temporary child of HEAD: e is HEAD, the snapshot s sits on top of it
        e = self.commit("e")
        s = self.commit("snapshot")
        self.assertEqual(self.stage(s)[0], 0)
        asked = self.questions[-1][1]["input"].decode().split()
        self.assertEqual(asked, [s, e, d, c, b, a])
        self.assertIn(f"rev-list --objects --quiet {s}; }} || ", self.stages[-1]["script"])
        self.assertEqual(pack_objects_in(self.stages[-1]["pack"]), 9)  # d, e and s
        self.assert_mirror_complete(s)

    def test_candidates_unknown_to_the_mirror_fall_back_to_the_full_pack(self):
        a = self.commit("a")
        self.git("checkout", "-q", "--orphan", "other")
        self.git("rm", "-q", "-f", "a")
        x = self.commit("x")
        self.assertEqual(self.stage(x)[0], 0)
        self.git("checkout", "-q", "main")
        b, c = self.commit("b"), self.commit("c")
        self.assertEqual(self.stage(c)[0], 0)
        self.assertEqual(len(self.questions), 2)
        self.assertNotIn("--not", self.stages[1]["script"])
        self.assertEqual(pack_objects_in(self.stages[1]["pack"]), 9)  # a, b, c: none of x's history is shared
        self.assertEqual(self.runs(), ["ran", "ran"])
        self.assert_mirror_complete(x, c)

    def test_a_failed_timed_out_or_odd_answer_sends_the_full_history(self):
        a, b = self.commit("a"), self.commit("b")
        self.stage(b)
        c = self.commit("c")

        def raising(exc):
            def answer(candidates):
                raise exc
            return answer

        def reply(code, make):
            return lambda ids: subprocess.CompletedProcess([], code, make(ids), b"")
        odd = [
            ("exit status", reply(255, lambda ids: b"")),
            ("exit status with a good answer", reply(1, lambda ids: "".join(f"{i} commit 1\n" for i in ids).encode())),
            ("timeout", raising(subprocess.TimeoutExpired("ssh", 20))),
            ("no ssh", raising(FileNotFoundError("ssh"))),
            ("garbage", reply(0, lambda ids: b"hello\n")),
            ("binary", reply(0, lambda ids: b"\xff\xfe\n")),
            ("no output", reply(0, lambda ids: None)),
            ("too few lines", reply(0, lambda ids: f"{ids[0]} missing\n".encode())),
            ("other ids", reply(0, lambda ids: "".join(f"{'0' * 40} commit 1\n" for _ in ids).encode())),
            ("a tree", reply(0, lambda ids: "".join(f"{i} tree 1\n" for i in ids).encode())),
            ("carriage returns", reply(0, lambda ids: "".join(f"{i} commit 1\r\n" for i in ids).encode())),
        ]
        for label, answer in odd:
            with self.subTest(label):
                self.answer = answer
                before = len(self.stages)
                self.assertEqual(self.stage(c)[0], 0)
                self.assertEqual(len(self.stages), before + 1)
                self.assertNotIn("--not", self.stages[-1]["script"])
                self.assertEqual(pack_objects_in(self.stages[-1]["pack"]), 9)  # a, b and c, as without the question
                self.assert_mirror_complete(c)
        self.assertEqual(len(self.runs()), 1 + len(odd))

    def test_the_question_uses_the_staging_client_options_and_host(self):
        a = self.commit("a")
        self.stage(a)
        argv, kwargs, script = self.questions[0]
        self.assertEqual(argv[:-1], ["ssh", *runner.SSH_OPTIONS, "host.example"])
        self.assertEqual(self.stages[0]["argv"][:-1], argv[:-1])
        self.assertEqual(kwargs["timeout"], runner.QUESTION_SECONDS)
        self.assertLessEqual(runner.QUESTION_SECONDS, 20)
        self.assertEqual(kwargs["input"], (a + "\n").encode())
        repo = runner.repo_id(self.git("rev-parse", "--path-format=absolute", "--git-common-dir"))
        name = f'"$HOME/.local/share/baton/repos/"{shlex.quote(repo + ".git")}'
        self.assertIn(name, script)
        self.assertIn(f'bare="$base/repos/"{shlex.quote(repo + ".git")}', self.stages[0]["script"])

    def test_restaging_the_same_snapshot_sends_no_pack_and_does_not_grow_the_mirror(self):
        a, b = self.commit("a"), self.commit("b")
        self.stage(b)
        packs = sorted(p.name for p in (self.mirror() / "objects/pack").glob("*.pack"))
        self.assertEqual(self.stage(b)[0], 0)
        again = self.stages[1]
        self.assertTrue(again["devnull"])
        self.assertEqual(again["pack"], b"")
        self.assertNotIn("index-pack", again["script"])
        self.assertIn(f"rev-list --objects --quiet {b}; }} || ", again["script"])
        self.assertEqual(sorted(p.name for p in (self.mirror() / "objects/pack").glob("*.pack")), packs)
        self.assertEqual(self.runs(), ["ran", "ran"])
        self.assert_mirror_complete(b)

    def test_git_makes_a_valid_empty_pack_and_index_pack_accepts_it(self):
        # the restage skips indexing to leave no empty pack file in the mirror, not because git refuses it
        a = self.commit("a")
        empty = subprocess.run(["git", "-C", str(self.repo), "pack-objects", "--stdout", "--revs"],
                               input=f"{a}\n^{a}\n".encode(), capture_output=True, check=True).stdout
        self.assertEqual(pack_objects_in(empty), 0)
        bare = self.root / "bare.git"
        subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True)
        done = subprocess.run(["git", "--git-dir", str(bare), "index-pack", "--stdin"], input=empty, capture_output=True)
        self.assertEqual(done.returncode, 0, done.stderr.decode())
        self.assertEqual(len(list((bare / "objects/pack").glob("*.pack"))), 1)

    def test_a_merge_whose_second_parent_is_new_to_the_mirror_arrives_complete(self):
        a = self.commit("a")
        self.git("branch", "side")
        b = self.commit("b")
        self.stage(b)
        self.git("checkout", "-q", "side")
        s1, s2 = self.commit("s1"), self.commit("s2")
        self.git("checkout", "-q", "main")
        self.git("merge", "-q", "--no-ff", "-m", "merge", "side")
        m = self.git("rev-parse", "HEAD")
        self.assertEqual(self.git("rev-parse", "HEAD^2"), s2)
        self.assertEqual(self.stage(m)[0], 0)
        self.assertIn(f"rev-list --objects --quiet {m}; }} || ", self.stages[-1]["script"])
        # s1 and s2 with their trees and blobs, and the merge with its tree: 8 objects (its other blobs are known)
        self.assertEqual(pack_objects_in(self.stages[-1]["pack"]), 8)
        self.assert_mirror_complete(m, s1, s2)
        self.assertEqual(self.mirror_git("rev-list", "--count", m), "5")

    def test_tag_objects_arrive_when_their_commit_is_already_on_the_host(self):
        a = self.commit("a")
        self.stage(a)
        self.git("tag", "-a", "v1", "-m", "annotated", a)
        b = self.commit("b")
        job = 'git cat-file -t "$TAG" > "$HOME/tagtype"; git describe --tags > "$HOME/describe"; echo ran >> "$HOME/ran"'
        self.assertEqual(self.stage(b, job.replace('"$TAG"', "v1"))[0], 0)
        self.assertIn(f"rev-list --objects --quiet {b}; }} || ", self.stages[-1]["script"])
        self.assertEqual(pack_objects_in(self.stages[-1]["pack"]), 4)  # b with its tree and blob, and the tag object
        self.assertEqual((self.home / "tagtype").read_text().strip(), "tag")
        self.assertEqual((self.home / "describe").read_text().strip(), "v1-1-g" + b[:7])
        # restaging the snapshot itself (the base is the snapshot) still sends a tag object the host lacks
        self.git("tag", "-a", "v2", "-m", "another", a)
        self.assertEqual(self.stage(b, job.replace('"$TAG"', "v2"))[0], 0)
        self.assertFalse(self.stages[-1]["devnull"])
        self.assertIn(f"rev-list --objects --quiet {b}; }} || ", self.stages[-1]["script"])
        self.assertEqual(pack_objects_in(self.stages[-1]["pack"]), 2)  # the two tag objects
        self.assertEqual((self.home / "tagtype").read_text().strip(), "tag")
        self.assert_mirror_complete(b)

    def test_a_base_the_host_does_not_really_have_gets_the_full_history_in_a_second_attempt(self):
        a, b, c = self.commit("a"), self.commit("b"), self.commit("c")
        # the host claims to have b, but its mirror is empty (as if it were wiped between the question and the staging)
        self.answer = lambda ids: subprocess.CompletedProcess([], 0, "".join(
            f"{i} {'commit 1' if i == b else 'missing'}\n" for i in ids).encode(), b"")
        code, err = self.stage(c)
        self.assertEqual(code, 0, err)
        self.assertIn("sending the full history", err)
        self.assertEqual(len(self.stages), 2)
        self.assertIn(f"rev-list --objects --quiet {c}; }} || ", self.stages[0]["script"])
        self.assertEqual(pack_objects_in(self.stages[0]["pack"]), 3)
        self.assertNotIn("--not", self.stages[1]["script"])
        self.assertEqual(pack_objects_in(self.stages[1]["pack"]), 9)
        self.assertEqual(self.runs(), ["ran"])  # the job started once, in the second attempt
        self.assertEqual(len(self.questions), 1)
        self.assertFalse(any((self.home / ".local/share/baton/staging").glob("*/*")))
        self.assert_mirror_complete(c)

    def test_a_mirror_holding_a_commit_without_its_blobs_is_repaired_by_the_second_attempt(self):
        a, b = self.commit("a"), self.commit("b")
        self.stage(b)
        # damage the mirror by hand: the commit b stays and the objects below it go (no staging does that)
        loose = self.root / "loose.git"
        subprocess.run(["git", "init", "-q", "--bare", str(loose)], check=True)
        raw = subprocess.run(["git", "-C", str(self.repo), "cat-file", "commit", b], capture_output=True, check=True).stdout
        subprocess.run(["git", "--git-dir", str(loose), "hash-object", "-t", "commit", "-w", "--stdin", "--literally"],
                       input=raw, capture_output=True, check=True)
        shutil.rmtree(self.mirror() / "objects", onexc=lambda func, path, exc: (os.chmod(path, 0o666), func(path)))
        shutil.copytree(loose / "objects", self.mirror() / "objects")
        self.assertEqual(self.mirror_git("cat-file", "-t", b), "commit")
        c = self.commit("c")
        code, err = self.stage(c)
        self.assertEqual(code, 0, err)
        self.assertIn("sending the full history", err)
        self.assertEqual(len(self.stages), 3)  # b, then c twice
        self.assertEqual(self.runs(), ["ran", "ran"])  # b's job, and c's once
        self.assert_mirror_complete(c)

    def drop_from_mirror(self, victim):
        """Rewrites the mirror's objects as one pack without victim: the mirror keeps the commits and trees that name it."""
        mirror = self.mirror()
        listed = self.mirror_git("rev-list", "--objects", "--all").splitlines()
        keep = "".join(f"{line.split()[0]}\n" for line in listed if line.split()[0] != victim)
        packed = subprocess.run(["git", "--git-dir", str(mirror), "pack-objects", "--stdout"], input=keep.encode(),
                                capture_output=True, check=True).stdout
        for old in (mirror / "objects/pack").iterdir():
            os.chmod(old, 0o666)
            old.unlink()
        subprocess.run(["git", "--git-dir", str(mirror), "index-pack", "--stdin"], input=packed, capture_output=True, check=True)
        gone = subprocess.run(["git", "--git-dir", str(mirror), "cat-file", "-e", victim], capture_output=True)
        self.assertNotEqual(gone.returncode, 0)

    def test_a_mirror_missing_a_blob_of_its_commit_is_repaired_when_the_snapshot_is_staged(self):
        # the mirror holds b complete except the blob of a, which the snapshot s (a child of b) does not change
        a, b = self.commit("a"), self.commit("b")
        self.stage(b)
        self.drop_from_mirror(self.git("rev-parse", f"{b}:a"))
        s = self.commit("snapshot")
        code, err = self.stage(s, 'cat a > "$HOME/a.copy"; echo ran >> "$HOME/ran"')
        self.assertEqual(code, 0, err)
        self.assertIn("incremental pack refused by the host", err)
        self.assertEqual(len(self.stages), 3)  # b, s incrementally (refused before the job), s in full
        self.assertEqual(pack_objects_in(self.stages[1]["pack"]), 3)  # s, its tree and its blob
        self.assertEqual(pack_objects_in(self.stages[2]["pack"]), 9)
        self.assertEqual(self.runs(), ["ran", "ran"])  # the job ran once, after the full history arrived
        self.assertEqual((self.home / "a.copy").read_text().strip(), "a")
        self.assert_mirror_complete(b, s)

    def test_a_restaged_snapshot_whose_mirror_lacks_one_of_its_blobs_is_repaired(self):
        a, b, s = self.commit("a"), self.commit("b"), self.commit("snapshot")
        self.stage(s)
        self.drop_from_mirror(self.git("rev-parse", f"{s}:snapshot"))
        # the mirror holds s itself, so the answer names s as the base: the pack is empty and only the check runs
        code, err = self.stage(s)
        self.assertEqual(code, 0, err)
        self.assertIn("sending the full history", err)
        self.assertEqual(len(self.stages), 3)  # s, s with the check alone (refused), s in full
        self.assertTrue(self.stages[1]["devnull"])
        self.assertEqual(pack_objects_in(self.stages[2]["pack"]), 9)
        self.assertEqual(self.runs(), ["ran", "ran"])
        self.assert_mirror_complete(s)

    def test_a_missing_blob_only_an_older_commit_references_is_repaired(self):
        a = self.commit("a")
        x = self.commit("x")
        self.git("rm", "-q", "x")
        self.git("commit", "-qm", "remove x")
        b = self.git("rev-parse", "HEAD")
        self.stage(b)
        self.drop_from_mirror(self.git("rev-parse", f"{x}:x"))
        s = self.commit("snapshot")
        self.assertNotIn(self.git("rev-parse", f"{x}:x"), self.git("ls-tree", "-r", s))  # s does not hold x's blob
        code, err = self.stage(s)
        self.assertEqual(code, 0, err)
        self.assertIn("sending the full history", err)
        self.assertEqual(len(self.stages), 3)
        self.assertEqual(pack_objects_in(self.stages[1]["pack"]), 3)  # s, its tree and its blob
        self.assertEqual(self.runs(), ["ran", "ran"])
        self.assert_mirror_complete(b, s)

    def test_a_job_exiting_86_is_not_mistaken_for_a_refusal(self):
        a, b = self.commit("a"), self.commit("b")
        self.stage(b)
        c = self.commit("c")
        code, err = self.stage(c, 'echo ran >> "$HOME/ran"; exit 86')
        self.assertEqual(code, 1)
        self.assertEqual(len(self.stages), 2)  # no second attempt
        self.assertEqual(self.runs(), ["ran", "ran"])
        # a full staging keeps its job's status as it is
        d = self.commit("d")
        self.answer = lambda ids: subprocess.CompletedProcess([], 255, b"", b"")
        self.assertEqual(self.stage(d, "exit 86")[0], 86)
        self.assertEqual(len(self.stages), 3)

    def test_a_pack_that_cannot_be_built_relative_to_the_base_is_built_in_full(self):
        a, b = self.commit("a"), self.commit("b")
        self.stage(b)
        c = self.commit("c")
        self.fail_incremental_pack = True
        code, err = self.stage(c)
        self.assertEqual(code, 0, err)
        self.assertEqual(len(self.stages), 2)
        self.assertEqual(pack_objects_in(self.stages[-1]["pack"]), 9)
        self.assert_mirror_complete(c)

    def test_sandboxed_staging_of_an_incremental_pack_keeps_the_whole_history_in_the_stage(self):
        a, b = self.commit("a"), self.commit("b")
        self.stage(b)
        c = self.commit("c")
        job = ('mv "$HOME/.local/share/baton/repos" "$HOME/moved-away" && git log --format=%s > "$HOME/log" && '
               'git fsck --no-dangling && test ! -e .git/objects/info/alternates && echo ok > "$HOME/ok"')
        self.assertEqual(self.stage(c, job, sandboxed=True)[0], 0)
        self.assertEqual(pack_objects_in(self.stages[-1]["pack"]), 3)
        self.assertEqual((self.home / "log").read_text().split(), ["c", "b", "a"])
        self.assertEqual((self.home / "ok").read_text().strip(), "ok")


class IncrementalHelpersTests(unittest.TestCase):
    def test_a_child_process_reads_the_whole_pack_after_its_header_was_read(self):
        # The way main uses the file: git writes it, the header is read, the file is rewound and given to ssh as
        # its standard input. The child reads from the descriptor, not through Python's buffer.
        data = b"PACK" + (2).to_bytes(4, "big") + (3).to_bytes(4, "big") + b"x" * 500
        with runner.pack_file() as pack:
            subprocess.run([sys.executable, "-c", "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())"],
                           input=data, stdout=pack, check=True)
            pack.seek(0)
            self.assertEqual(runner.pack_object_count(pack), 3)
            pack.seek(0)
            seen = subprocess.run([sys.executable, "-c", "import sys; print(len(sys.stdin.buffer.read()))"],
                                  stdin=pack, capture_output=True, text=True, check=True).stdout.strip()
        self.assertEqual(int(seen), len(data))

    def test_a_small_history_is_sent_whole_and_an_unreadable_size_asks(self):
        def sized(loose, packed):
            return lambda *args: "\n".join(("count: 9", f"size: {loose}", "in-pack: 3", "packs: 1",
                                            f"size-pack: {packed}", "prune-packable: 0"))
        limit = runner.SMALL_HISTORY_KIB
        self.assertTrue(runner.history_is_small(sized(100, 200), "top"))
        self.assertTrue(runner.history_is_small(sized(limit - 1, 0), "top"))
        self.assertFalse(runner.history_is_small(sized(limit // 2, limit // 2), "top"))
        self.assertFalse(runner.history_is_small(sized(0, limit * 9), "top"))
        self.assertFalse(runner.history_is_small(lambda *args: "\n".join(("size: many", "size-pack: 3")), "top"))
        self.assertFalse(runner.history_is_small(lambda *args: "size: 3", "top"))
        def failing(*args):
            raise OSError("git")
        self.assertFalse(runner.history_is_small(failing, "top"))

    def test_candidate_revisions_keeps_valid_ids_with_the_snapshot_first(self):
        sha = "a" * 40
        calls = []

        def git(*args):
            calls.append(args)
            return "\n".join([sha, "b" * 40, "B" * 40, "c" * 39, "d" * 64, "e" * 40 + " x", "", "b" * 40, "f" * 40, "g" * 40])
        self.assertEqual(runner.candidate_revisions(git, "/top", sha), [sha, "b" * 40, "f" * 40])
        self.assertEqual(calls, [("-C", "/top", "rev-list", "--first-parent", "-n", "50", sha)])
        wide = "a" * 64
        self.assertEqual(runner.candidate_revisions(lambda *a: "\n".join([wide, "b" * 40, "c" * 64]), "/top", wide), [wide, "c" * 64])
        self.assertEqual(runner.candidate_revisions(lambda *a: "\n".join(["b" * 40, sha]), "/top", sha), [])
        self.assertEqual(runner.candidate_revisions(lambda *a: "", "/top", sha), [])

        def failing(*args):
            raise subprocess.CalledProcessError(128, ["git", *args])
        self.assertEqual(runner.candidate_revisions(failing, "/top", sha), [])

    def test_answer_base_takes_the_newest_present_commit_and_nothing_unexpected(self):
        ids = ["1" * 40, "2" * 40, "3" * 40]

        def line(i, what):
            return f"{i} {what}\n"
        self.assertEqual(runner.answer_base(ids, line(ids[0], "missing") + line(ids[1], "commit 250") + line(ids[2], "commit 9")), ids[1])
        self.assertEqual(runner.answer_base(ids, line(ids[0], "commit 1") + line(ids[1], "missing") + line(ids[2], "missing")), ids[0])
        self.assertIsNone(runner.answer_base(ids, "".join(line(i, "missing") for i in ids)))
        bad = [
            "", "\n", "".join(line(i, "missing") for i in ids[:2]), "".join(line(i, "missing") for i in ids) + line(ids[0], "missing"),
            line(ids[1], "missing") + line(ids[0], "missing") + line(ids[2], "missing"),
            line(ids[0], "commit 1") + line(ids[1], "tree 1") + line(ids[2], "missing"),
            line(ids[0], "commit") + line(ids[1], "missing") + line(ids[2], "missing"),
            line(ids[0], "commit -1") + line(ids[1], "missing") + line(ids[2], "missing"),
            line(ids[0], "commit 1 extra") + line(ids[1], "missing") + line(ids[2], "missing"),
            "".join(f"{i} missing\r\n" for i in ids), "".join(f"{i}  missing\n" for i in ids), "".join(f" {i} missing\n" for i in ids),
            "".join(line(i.upper(), "missing") for i in ids), "".join(line("0" * 40, "missing") for _ in ids),
            "".join(line(i, "missing") for i in ids)[:-1], "fatal: not a git repository\n",
            "".join(line(i, "ambiguous") for i in ids), None, b"1" * 40,
        ]
        for text in bad:
            with self.subTest(text=text):
                self.assertIsNone(runner.answer_base(ids, text))
        self.assertIsNone(runner.answer_base([], ""))
        wide = ["a" * 64]
        self.assertEqual(runner.answer_base(wide, f"{wide[0]} commit 3\n"), wide[0])
        self.assertIsNone(runner.answer_base(wide, f"{'a' * 40} commit 3\n"))

    def test_mirror_base_asks_once_with_a_timeout_and_never_raises(self):
        ids = ["1" * 40, "2" * 40]
        seen = []

        def run(argv, **kwargs):
            seen.append((argv, kwargs))
            return subprocess.CompletedProcess(argv, 0, f"{ids[0]} missing\n{ids[1]} commit 4\n".encode(), b"")
        with unittest.mock.patch.object(runner.subprocess, "run", side_effect=run):
            self.assertEqual(runner.mirror_base("ssh", "me@host", "repo-1", ids), ids[1])
            self.assertIsNone(runner.mirror_base("ssh", "me@host", "repo-1", []))
        self.assertEqual(len(seen), 1)
        argv, kwargs = seen[0]
        self.assertEqual(argv[:-1], ["ssh", *runner.SSH_OPTIONS, "me@host"])
        self.assertEqual(shlex.split(argv[-1])[:2], ["bash", "-c"])
        self.assertEqual(kwargs["input"], f"{ids[0]}\n{ids[1]}\n".encode())
        self.assertEqual(kwargs["timeout"], 20)
        for failure in (subprocess.TimeoutExpired("ssh", 20), OSError("no ssh")):
            with self.subTest(failure=failure), unittest.mock.patch.object(runner.subprocess, "run", side_effect=failure):
                self.assertIsNone(runner.mirror_base("ssh", "me@host", "repo-1", ids))
        with unittest.mock.patch.object(runner.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, b"\xff\n", b"")):
            self.assertIsNone(runner.mirror_base("ssh", "me@host", "repo-1", ids))

    def test_the_question_script_reads_only_and_quotes_the_repo_name(self):
        script = runner.question_script("it's-repo")
        self.assertIn('bare="$HOME/.local/share/baton/repos/"' + shlex.quote("it's-repo.git"), script)
        self.assertIn('[ -d "$bare" ] || exit 0', script)
        self.assertIn('git --git-dir="$bare" cat-file --batch-check', script)
        for writer in ("index-pack", "update-ref", "mkdir", "init", "clone", "rm "):
            self.assertNotIn(writer, script)

    def test_pack_object_count(self):
        header = b"PACK" + (2).to_bytes(4, "big") + (7).to_bytes(4, "big")
        self.assertEqual(runner.pack_object_count(io.BytesIO(header + b"x" * 20)), 7)
        self.assertEqual(runner.pack_object_count(io.BytesIO(b"PACK" + (3).to_bytes(4, "big") + bytes(4))), 0)
        for bad in (b"", b"PACK", b"JUNK" + header[4:], b"PACK" + (9).to_bytes(4, "big") + bytes(4)):
            self.assertIsNone(runner.pack_object_count(io.BytesIO(bad)))

    def test_scripts_differ_by_the_base_only(self):
        sha, base = "a" * 40, "b" * 40
        full = runner.remote_script("repo", "stage", sha, ".", ["true"], "")
        self.assertEqual(full, runner.remote_script("repo", "stage", sha, ".", ["true"], "", base=None))
        self.assertIn('git --git-dir="$bare" index-pack --stdin >/dev/null\n', full)
        for text in ("--not", "86", "--fix-thin"):
            self.assertNotIn(text, full)
        check = f'git --git-dir="$bare" rev-list --objects --quiet {sha}'
        inc = runner.remote_script("repo", "stage", sha, ".", ["true"], "", base=base)
        self.assertIn('{ git --git-dir="$bare" index-pack --stdin >/dev/null && ' + check + "; } || ", inc)
        self.assertNotIn("--not", inc)  # the check walks the snapshot's whole closure, not only what is new
        self.assertNotIn("--fix-thin", inc)
        lines = inc.splitlines()
        gate = next(i for i, text in enumerate(lines) if check in text and not text.startswith("#"))
        self.assertLess(gate, lines.index(f'git --git-dir="$bare" update-ref refs/baton-snapshots/{sha} {sha}'))
        self.assertLess(gate, lines.index("trap 'rm -rf -- \"$stage\"' EXIT"))
        self.assertTrue(lines[gate].endswith(f"exit {runner.INCREMENTAL_REFUSED}; }}"))
        empty = runner.remote_script("repo", "stage", sha, ".", ["true"], "", base=base, empty_pack=True)
        self.assertNotIn("index-pack", empty)
        self.assertIn("{ " + check + "; } || ", empty)
        self.assertIn('"$HOME/bin/lanerun" "$work" -- true || rc=$?', inc)
        self.assertIn('[ "$rc" -ne 86 ] || rc=1', inc)
        self.assertTrue(inc.endswith('exit "$rc"\n'))
        # the sandbox lines are the same with or without a base
        def sandbox_lines(**kwargs):
            plain = runner.remote_script("repo", "stage", sha, ".", ["true"], "", **kwargs)
            boxed = runner.remote_script("repo", "stage", sha, ".", ["true"], "", sandboxed=True, **kwargs)
            return [text for text in boxed.splitlines() if text not in plain.splitlines()]
        self.assertEqual(sandbox_lines(), sandbox_lines(base=base))
        self.assertIn('git -C "$stage" repack -a -d -q', sandbox_lines(base=base))


if __name__ == "__main__":
    unittest.main()
