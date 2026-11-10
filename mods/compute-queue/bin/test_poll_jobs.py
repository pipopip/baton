"""Offline controller scans: no live state, hosts, processes or notices."""
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

import poll_jobs


class PollJobsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'jobs').mkdir()
        self.ctx = types.SimpleNamespace(p=lambda *p: str(self.root.joinpath(*p)))
        self.reads = []
        self.now = 100
        def read(path):
            self.reads.append(Path(path).name)
            try:
                return json.loads(Path(path).read_text())
            except FileNotFoundError:
                return None
        self.cache = poll_jobs.SettledJobs(read, ('done', 'failed', 'timeout', 'cancelled'),
                                           clock=lambda: self.now)

    def write(self, jid, **fields):
        path = self.root / 'jobs' / (jid + '.json')
        tmp = path.with_suffix('.tmp')
        tmp.write_text(json.dumps(dict(id=jid, status='done', notified=True, **fields)))
        tmp.replace(path)
        return path

    def scan(self):
        with self.cache.generation():
            return list(self.cache.scan(self.ctx))

    def test_settled_body_not_retained_and_unchanged_file_not_read(self):
        self.write('old', tail=['x' * 200_000], artifacts=['unused'])
        self.assertEqual(self.scan(), [('old', None)])
        self.assertEqual(self.scan(), [('old', None)])
        self.assertEqual(self.reads, ['old.json'])
        self.assertLess(len(repr(self.cache.cache)), 300)

    def test_active_and_outstanding_notices_always_read_fresh(self):
        cases = [('queued', False), ('launching', False), ('running', False), ('done', False)]
        for i, (status, notified) in enumerate(cases):
            (self.root / 'jobs' / f'{i}.json').write_text(json.dumps(dict(id=str(i), status=status, notified=notified)))
        self.assertEqual(len(self.scan()), 4)
        self.assertEqual(len(self.scan()), 4)
        self.assertEqual(len(self.reads), 8)
        self.assertEqual(self.cache.cache, {})

    def test_notify_false_and_terminal_child_are_settled(self):
        self.write('silent', notify=False)
        self.write('child', parent='p')
        self.scan(); self.scan()
        self.assertEqual(len(self.reads), 2)

    def test_atomic_replacement_reactivates_notice(self):
        path = self.write('old')
        self.scan()
        tmp = path.with_suffix('.tmp')
        tmp.write_text(json.dumps(dict(id='old', status='done', notified=False)))
        tmp.replace(path)
        self.assertFalse(self.scan()[0][1]['notified'])
        self.assertEqual(self.cache.cache, {})

    def test_metadata_collision_eventually_refreshes(self):
        path = self.write('old')
        self.scan()
        # Simulate a writer preserving all metadata instead of using save_job.
        previous = self.cache.cache[str(path)]
        path.write_text(json.dumps(dict(id='old', status='done', notified=False)))
        info = os.stat(path)
        self.cache.cache[str(path)] = (self.cache.signature(info), previous[1], previous[2])
        self.assertEqual(self.scan(), [('old', None)])
        self.now += self.cache.refresh_after
        self.assertFalse(self.scan()[0][1]['notified'])

    def test_replacement_during_read_is_not_cached(self):
        path = self.write('old')
        original = self.cache.read_json
        def replace(path):
            job = original(path)
            tmp = Path(path).with_suffix('.tmp')
            tmp.write_text(json.dumps(dict(id='old', status='running')))
            tmp.replace(path)
            return job
        self.cache.read_json = replace
        self.scan()
        self.assertEqual(self.cache.cache, {})
        self.cache.read_json = original
        self.assertEqual(self.scan()[0][1]['status'], 'running')

    def test_capacity_and_deleted_files_and_removed_hosts(self):
        self.cache.limit = 3
        paths = [self.write(str(i)) for i in range(8)]
        self.scan(); self.scan()
        self.assertEqual(len(self.cache.cache), 3)
        for path in paths:
            path.unlink()
        self.assertEqual(self.scan(), [])
        self.assertEqual(self.cache.cache, {})
        self.write('old'); self.scan()
        with self.cache.generation():
            pass  # A disabled/removed shard is no longer scanned.
        self.assertEqual(self.cache.cache, {})

    def test_missing_jobs_directory_and_invalid_document(self):
        (self.root / 'jobs' / 'invalid.json').write_text('null')
        (self.root / 'jobs' / 'no-id.json').write_text('{}')
        self.assertEqual(self.scan(), [])
        for p in (self.root / 'jobs').iterdir():
            p.unlink()
        (self.root / 'jobs').rmdir()
        self.assertEqual(self.scan(), [])

    def test_symlink_target_is_never_hidden_by_link_metadata(self):
        target = self.root / 'target.json'
        target.write_text(json.dumps(dict(id='linked', status='done', notified=True)))
        link = self.root / 'jobs' / 'link.json'
        try:
            link.symlink_to(target)
        except OSError:
            self.skipTest('symlink privilege unavailable')
        self.scan()
        self.assertEqual(self.cache.cache, {})
        target.write_text(json.dumps(dict(id='linked', status='running')))
        self.assertEqual(self.scan()[0][1]['status'], 'running')


if __name__ == '__main__':
    unittest.main()
