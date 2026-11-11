import importlib.util
import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
