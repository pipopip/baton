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
                bash = str(Path(shutil.which("git")).parent.parent / "bin" / "bash.exe")
                if not Path(bash).exists():
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


if __name__ == "__main__":
    unittest.main()
