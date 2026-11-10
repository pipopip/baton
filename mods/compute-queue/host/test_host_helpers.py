import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('bootstrap', HERE / 'bootstrap.py')
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)


@unittest.skipUnless(os.name == 'posix', 'POSIX ownership and flock required')
class HostTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.home = self.root / 'home'
        self.home.mkdir()
        self.shm = self.root / 'shm'
        self.env = {**os.environ, 'HOME': str(self.home), 'HEAVY_SLOTS': '2'}

    def test_bootstrap_create_only(self):
        missing = bootstrap.install(self.home, self.shm, HERE, check=True)
        self.assertTrue(missing)
        self.assertFalse((self.home / 'bin').exists())
        bootstrap.install(self.home, self.shm, HERE)
        self.assertEqual(bootstrap.install(self.home, self.shm, HERE, check=True), [])
        self.assertEqual((self.home / 'bin/heavy').stat().st_mode & 0o777, 0o700)
        bootstrap.install(self.home, self.shm, HERE)
        (self.home / 'bin/heavy').write_text('not ours')
        with self.assertRaises(ValueError):
            bootstrap.install(self.home, self.shm, HERE)
        self.assertEqual((self.home / 'bin/heavy').read_text(), 'not ours')

    def test_bootstrap_rejects_symlink_before_writing(self):
        (self.home / 'q').symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ValueError):
            bootstrap.install(self.home, self.shm, HERE)
        self.assertFalse((self.home / 'bin').exists())

    def test_heavy_rejects_bad_arguments(self):
        (self.home / 'q').mkdir()
        for args in [('-n', '0', '--', 'true'), ('-n', '3', '--', 'true'),
                     ('-n', 'abc', '--', 'true'), ('-n', '1'), ('--',)]:
            result = subprocess.run(['bash', str(HERE / 'heavy'), *args], env=self.env, capture_output=True)
            self.assertEqual(result.returncode, 2, (args, result.stderr))

    def test_heavy_waits_until_all_requested_slots_free(self):
        (self.home / 'q').mkdir()
        ready, release, second = (self.root / n for n in ('ready', 'release', 'second'))
        code = 'import pathlib,sys,time; pathlib.Path(sys.argv[1]).touch();\nwhile not pathlib.Path(sys.argv[2]).exists(): time.sleep(.02)'
        first = subprocess.Popen(['bash', str(HERE / 'heavy'), '-n', '1', '--', sys.executable, '-c', code, str(ready), str(release)], env=self.env)
        self.addCleanup(lambda: first.kill() if first.poll() is None else None)
        deadline = time.monotonic() + 3
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertTrue(ready.exists())
        other = subprocess.Popen(['bash', str(HERE / 'heavy'), '-n', '2', '--', sys.executable, '-c', 'import pathlib,sys; pathlib.Path(sys.argv[1]).touch()', str(second)], env=self.env)
        self.addCleanup(lambda: other.kill() if other.poll() is None else None)
        time.sleep(.3)
        self.assertFalse(second.exists())
        release.touch()
        self.assertEqual(first.wait(timeout=3), 0)
        self.assertEqual(other.wait(timeout=3), 0)
        self.assertTrue(second.exists())

    def test_lanerun_disk_and_boundary(self):
        stage = self.home / '.local/share/baton/staging/repo/ref'
        stage.mkdir(parents=True)
        fake = self.root / 'fake'
        fake.mkdir()
        df = fake / 'df'
        df.write_text('#!/bin/sh\nprintf "Filesystem 1024-blocks Used Available Capacity Mounted\\nfs 100 0 100 0%% /\\n"\n')
        df.chmod(0o700)
        env = {**self.env, 'PATH': str(fake) + ':' + self.env['PATH']}
        result = subprocess.run(['bash', str(HERE / 'lanerun'), str(stage), '--', 'true'], env=env, capture_output=True)
        self.assertEqual(result.returncode, 75, result.stderr)
        result = subprocess.run(['bash', str(HERE / 'lanerun'), str(self.home), '--', 'true'], env=env, capture_output=True)
        self.assertEqual(result.returncode, 2, result.stderr)


if __name__ == '__main__':
    unittest.main()
