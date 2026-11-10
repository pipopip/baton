"""The ssh client the queue starts, and the git-free worktree top; no network access."""
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cq


class DefaultSsh(unittest.TestCase):
    SYSTEM = r"C:\Windows\System32\OpenSSH\ssh.exe"
    GIT_SSH = r"C:\Program Files\Git\usr\bin\ssh.EXE"

    def test_explicit_setting_wins(self):
        self.assertEqual(cq.default_ssh({"CQ_SSH": "X:/ssh.exe", "SystemRoot": r"C:\Windows"}, "nt",
                                        isfile=lambda p: True, which=lambda n: self.GIT_SSH), "X:/ssh.exe")

    def test_windows_uses_the_system_client_whatever_path_says(self):
        # the failure this pins: Git's ssh first on PATH, which cannot reach the Windows ssh-agent service
        self.assertEqual(cq.default_ssh({"SystemRoot": r"C:\Windows"}, "nt", isfile=lambda p: p == self.SYSTEM,
                                        which=lambda n: self.GIT_SSH), self.SYSTEM)

    def test_windows_without_systemroot_assumes_the_usual_directory(self):
        self.assertEqual(cq.default_ssh({}, "nt", isfile=lambda p: p == self.SYSTEM, which=lambda n: None), self.SYSTEM)

    def test_windows_without_the_system_client_falls_back_to_path(self):
        self.assertEqual(cq.default_ssh({"SystemRoot": r"C:\Windows"}, "nt", isfile=lambda p: False,
                                        which=lambda n: self.GIT_SSH), self.GIT_SSH)

    def test_other_platforms_use_path(self):
        self.assertEqual(cq.default_ssh({}, "posix", isfile=lambda p: True, which=lambda n: "/usr/bin/ssh"), "/usr/bin/ssh")
        self.assertEqual(cq.default_ssh({}, "posix", isfile=lambda p: True, which=lambda n: None), "ssh")

    def test_module_constant_uses_the_resolver(self):
        self.assertEqual(cq.SSH, cq.default_ssh())

    def test_launch_helper_is_given_the_same_client_as_the_probes(self):
        # every fleet launch goes through the helper; it must not fall back to its own search for `ssh`
        seen = {}

        def fake_run(argv, **kw):
            seen.update(env=kw["env"], argv=argv)
            return subprocess.CompletedProcess(argv, 0, b"", b"")
        with mock.patch.object(cq, "_run", side_effect=fake_run), mock.patch.object(cq, "SSH", "X:/the/ssh.exe"), \
                mock.patch.dict(os.environ, {"CQ_SSH": "", "PATH": os.environ.get("PATH", "")}):
            rc, _out, _err = cq.real_run_remoterun(["--", "true"], cwd=".", host="user@host.example",
                                                   binding={"portable_runner": True})
        self.assertEqual(rc, 0)
        self.assertEqual(seen["env"]["CQ_SSH"], "X:/the/ssh.exe")
        self.assertTrue(any(str(a).endswith("remote_stage.py") for a in seen["argv"]))

    def test_helper_alone_prefers_the_windows_system_client(self):
        import remote_stage
        self.assertEqual(remote_stage.system_ssh({"SystemRoot": r"C:\Windows"}, "nt", isfile=lambda p: True), self.SYSTEM)
        self.assertEqual(remote_stage.system_ssh({}, "nt", isfile=lambda p: True), self.SYSTEM)
        self.assertIsNone(remote_stage.system_ssh({"SystemRoot": r"C:\Windows"}, "nt", isfile=lambda p: False))
        self.assertIsNone(remote_stage.system_ssh({}, "posix", isfile=lambda p: True))


class TopOf(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(os.path.realpath(tempfile.mkdtemp(prefix="cq-top-")))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    @staticmethod
    def _spelled(path):
        return str(path).replace("\\", "/")

    def test_repository_top_from_a_nested_directory_without_starting_a_process(self):
        (self.tmp / "repo" / ".git").mkdir(parents=True)
        nested = self.tmp / "repo" / "a" / "b"
        nested.mkdir(parents=True)
        with mock.patch.object(cq, "_run", side_effect=AssertionError("no process may start")), \
                mock.patch.object(subprocess, "run", side_effect=AssertionError("no process may start")):
            self.assertEqual(cq._top_of(str(nested)), self._spelled(self.tmp / "repo"))
            self.assertEqual(cq._top_of(str(self.tmp / "repo")), self._spelled(self.tmp / "repo"))

    def test_linked_worktree_inside_the_main_checkout_is_its_own_top(self):
        (self.tmp / "repo" / ".git").mkdir(parents=True)
        linked = self.tmp / "repo" / ".claude" / "worktrees" / "w"
        (linked / "src").mkdir(parents=True)
        (linked / ".git").write_text("gitdir: ../../../.git/worktrees/w\n", encoding="utf-8")
        self.assertEqual(cq._top_of(str(linked / "src")), self._spelled(linked))

    def test_removed_worktree_does_not_resolve_to_the_enclosing_repository(self):
        (self.tmp / "repo" / ".git").mkdir(parents=True)
        gone = str(self.tmp / "repo" / ".claude" / "worktrees" / "gone")
        self.assertEqual(cq._top_of(gone), gone)

    def test_directory_outside_any_repository_is_returned_unchanged(self):
        cur = self.tmp
        while True:
            if os.path.lexists(cur / ".git"):
                self.skipTest("the temporary directory is inside a repository")
            if cur.parent == cur:
                break
            cur = cur.parent
        plain = self.tmp / "plain"
        plain.mkdir()
        self.assertEqual(cq._top_of(str(plain)), str(plain))

    def test_unusable_value_is_returned_unchanged(self):
        self.assertIsNone(cq._top_of(None))

    @unittest.skipUnless(shutil.which("git"), "git is not installed")
    def test_agrees_with_git_for_a_real_repository(self):
        repo = self.tmp / "real"
        (repo / "x" / "y").mkdir(parents=True)
        subprocess.run(["git", "init", "-q", str(repo)], check=True, capture_output=True)
        expected = subprocess.run(["git", "-C", str(repo / "x" / "y"), "rev-parse", "--show-toplevel"], check=True,
                                  capture_output=True).stdout.decode("utf-8").strip()
        self.assertTrue(cq.same_path(cq._top_of(str(repo / "x" / "y")), expected))
        self.assertNotIn("\\", cq._top_of(str(repo / "x" / "y")))


class AttachedRequest(unittest.TestCase):
    """A session started outside a repository and linked to a worktree with `bt attach`."""
    SESSION = "11111111-2222-3333-4444-555555555555"

    def setUp(self):
        self.tmp = Path(os.path.realpath(tempfile.mkdtemp(prefix="cq-attach-")))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        cur = self.tmp
        while True:  # the scenario needs a start directory with no repository above it
            if os.path.lexists(cur / ".git"):
                self.skipTest("the temporary directory is inside a repository")
            if cur.parent == cur:
                break
            cur = cur.parent
        self.start = self.tmp / "code"  # where the session started: not a checkout
        self.start.mkdir()
        self.work = self.tmp / "code" / "repo" / ".claude" / "worktrees" / "w"
        (self.work / ".baton").mkdir(parents=True)
        (self.work / ".git").write_text("gitdir: elsewhere\n", encoding="utf-8")
        self.state = self.tmp / "baton" / ".state" / "compute"
        self.state.mkdir(parents=True)
        self.ctx = cq.Ctx(state=str(self.state))
        self.book(self.SESSION, self.work)
        self.own(self.SESSION)

    def book(self, session, worktree):
        cq.write_json(str(self.state.parent / "attached.json"), {session: {"worktree": str(worktree), "vendor": "claude"}})

    def own(self, session):
        cq.write_json(str(self.work / ".baton" / "owner.json"), {"session_id": session, "vendor": "claude"})

    def req(self, **extra):
        return {"root": str(self.start), "session": self.SESSION, **extra}

    def test_root_becomes_the_attached_worktree(self):
        out = cq.attached_request(self.ctx, self.req(command="bash x.sh"))
        self.assertTrue(cq.same_path(out["root"], str(self.work)))
        self.assertEqual(out["command"], "bash x.sh")
        self.assertEqual(self.req()["root"], str(self.start))  # the caller's request is not changed in place

    def test_tick_uses_the_watching_session(self):
        out = cq.attached_request(self.ctx, {"root": str(self.start), "session": "leader-of-old", "watch_session": self.SESSION})
        self.assertTrue(cq.same_path(out["root"], str(self.work)))

    def test_generation_two_state_reads_the_same_book(self):
        v2 = self.state / "queue-v2"
        v2.mkdir()
        (v2 / ".generation").write_bytes(b"baton-queue-generation-2\n")
        out = cq.attached_request(cq.Ctx(state=str(v2)), self.req())
        self.assertTrue(cq.same_path(out["root"], str(self.work)))

    def test_a_root_inside_a_repository_is_never_replaced(self):
        inside = self.tmp / "code" / "repo" / "src"
        inside.mkdir(parents=True)
        (self.tmp / "code" / "repo" / ".git").mkdir()
        req = {"root": str(inside), "session": self.SESSION}
        self.assertIs(cq.attached_request(self.ctx, req), req)

    def test_unchanged_when_the_worktree_is_owned_by_another_session(self):
        self.own("someone-else")
        req = self.req()
        self.assertIs(cq.attached_request(self.ctx, req), req)

    def test_unchanged_without_a_record_owner_file_or_worktree(self):
        req = self.req()
        self.book("another-session", self.work)
        self.assertIs(cq.attached_request(self.ctx, req), req)
        self.book(self.SESSION, self.tmp / "missing")
        self.assertIs(cq.attached_request(self.ctx, req), req)
        self.book(self.SESSION, self.work)
        os.remove(self.work / ".baton" / "owner.json")
        self.assertIs(cq.attached_request(self.ctx, req), req)

    def test_unchanged_for_requests_without_root_or_session_and_for_bad_books(self):
        for req in ({}, {"root": str(self.start)}, {"session": self.SESSION}, {"root": 5, "session": self.SESSION}):
            self.assertIs(cq.attached_request(self.ctx, req), req)
        req = self.req()
        (self.state.parent / "attached.json").write_text("[1, 2]", encoding="utf-8")
        self.assertIs(cq.attached_request(self.ctx, req), req)
        cq.write_json(str(self.state.parent / "attached.json"), {self.SESSION: "not a record"})
        self.assertIs(cq.attached_request(self.ctx, req), req)


if __name__ == "__main__":
    unittest.main()
