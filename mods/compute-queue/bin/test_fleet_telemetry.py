"""Fleet telemetry tests use only synthetic temporary state."""
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fleet_telemetry as ft


class FleetTelemetryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = Path(self.tmp.name)
        (self.state / 'jobs').mkdir()
        self.now = 1700000000

    def job(self, name, **values):
        (self.state / 'jobs' / (name + '.json')).write_text(json.dumps(values), encoding='utf-8')

    def test_snapshot_alerts_and_no_sensitive_text(self):
        caps = {'at': self.now, 'devices': {
            'laptop': {'up': True, 'eligible': True, 'at': self.now, 'slots_used': 1, 'slots_total': 4},
            'hearth': {'up': False, 'at': self.now - 301, 'slots_used': 0, 'slots_total': 8},
            'ember': {'up': True, 'at': self.now, 'slots_used': 17, 'slots_total': 16}}}
        (self.state / 'capacity.json').write_text(json.dumps(caps), encoding='utf-8')
        self.job('a', status='queued', t_submitted=self.now-901, command='secret-command', reason='secret-reason')
        self.job('b', status='running', launch_uncertain=True, cancel_requested=True, memory_max_gb=1,
                 metrics={'peak_memory_bytes': 2 * 1024**3})
        self.job('c', status='failed', t_finished=self.now-10, reason='out of memory', artifact_missing=True)
        self.job('d', status='failed', t_finished=self.now-86401, reason='OOM')
        summary = ft.summarize(self.state, now=self.now)
        self.assertTrue(summary['line'].startswith('CQ L1/4 H0/8 E17/16 · q1'))
        self.assertEqual(summary['alerts'], {'stale': 1, 'down': 1, 'overrun': 2, 'queue_delay': 1,
                         'uncertain': 1, 'cancel_pending': 1, 'artifact_missing': 1, 'failure': 1, 'oom': 1})
        self.assertNotIn('secret', json.dumps(summary))
        self.assertEqual(summary['counts']['failed'], 2)

    def test_additional_device_and_safe_name(self):
        (self.state / 'capacity.json').write_text(json.dumps({'at': self.now, 'devices': {'zephyr': {
            'at': self.now, 'up': True, 'name': 'Desk\nRemote', 'slots_used': 1, 'slots_total': 5}}}), encoding='utf-8')
        result = ft.summarize(self.state, self.now)
        self.assertEqual(result['devices']['zephyr']['name'], 'Desk Remote')
        self.assertIn('zephyr:1/5', result['line'])

    def test_generation_summary_preserves_active_legacy_and_shared_capacity(self):
        legacy = self.state / 'fleet'
        current = self.state / 'queue-v2' / 'fleet'
        for root in (legacy, current):
            (root / 'jobs').mkdir(parents=True)
        (self.state / 'queue-v2' / '.generation').write_bytes(b'baton-queue-generation-2\n')
        (legacy / 'jobs' / 'old.json').write_text(json.dumps(
            dict(status='running', host_id='ember', slots=3)), encoding='utf-8')
        (current / 'jobs' / 'new.json').write_text(json.dumps(
            dict(status='running', host_id='ember', slots=2)), encoding='utf-8')
        for root, used in ((legacy, 3), (current, 2)):
            (root / 'capacity.json').write_text(json.dumps({'at': self.now, 'devices': {
                'ember': {'at': self.now, 'up': True, 'eligible': True,
                          'slots_used': used, 'slots_total': 8}}}), encoding='utf-8')
        summary = ft.summarize_generations(self.state, self.now)
        self.assertEqual(summary['counts']['running'], 2)
        self.assertEqual(summary['devices']['ember']['slots_total'], 8)
        self.assertEqual(summary['devices']['ember']['slots_used'], 5)
        self.assertIn('E5/8', summary['line'])
        for root in (legacy, current):
            cap = json.loads((root / 'capacity.json').read_text(encoding='utf-8'))
            cap['devices']['ember']['at'] = self.now - 301
            (root / 'capacity.json').write_text(json.dumps(cap), encoding='utf-8')
        summary = ft.summarize_generations(self.state, self.now)
        self.assertFalse(summary['devices']['ember']['eligible'])
        self.assertTrue(summary['devices']['ember']['stale'])

    def test_registered_deferred_jobs_waiting_for_their_window_hold_no_slots(self):
        current = self.state / 'queue-v2' / 'fleet'
        (current / 'jobs').mkdir(parents=True)
        (self.state / 'queue-v2' / '.generation').write_bytes(b'baton-queue-generation-2\n')
        (current / 'capacity.json').write_text(json.dumps({'at': self.now, 'devices': {
            'ember': {'at': self.now, 'up': True, 'eligible': True, 'slots_used': 12, 'slots_total': 14}}}), encoding='utf-8')

        def job(name, **values):
            (current / 'jobs' / (name + '.json')).write_text(json.dumps(values), encoding='utf-8')
        job('run', status='running', host_id='ember', slots=12)
        job('night1', status='launching', host_id='ember', slots=3, deferred_registered=True, deferred_phase='queued')
        job('night2', status='launching', host_id='ember', slots=4, deferred_registered=True, deferred_phase='queued')
        summary = ft.summarize_generations(self.state, self.now)
        self.assertEqual(summary['devices']['ember']['slots_used'], 12)
        self.assertIn('E12/14', summary['line'])
        self.assertNotIn('overrun', summary['alerts'])
        # an unconfirmed staging, and a deferred job that has started, still count
        job('staging', status='launching', host_id='ember', slots=2, deferred_registered=False, deferred_phase='staging_pending')
        job('started', status='running', host_id='ember', slots=1, deferred_registered=True, deferred_phase='running')
        summary = ft.summarize_generations(self.state, self.now)
        self.assertEqual(summary['devices']['ember']['slots_used'], 15)
        self.assertEqual(summary['alerts'].get('overrun'), 1)

    def test_missing_corrupt_oversize_and_bounded_input(self):
        (self.state / 'capacity.json').write_text('{', encoding='utf-8')
        (self.state / 'jobs' / 'a.json').write_text('{', encoding='utf-8')
        (self.state / 'jobs' / 'b.json').write_bytes(b'x' * (ft.MAX_JSON_BYTES + 1))
        self.job('c', status='queued')
        with patch.object(ft, 'MAX_JOB_FILES', 2):
            result = ft.summarize(self.state, now=self.now)
        self.assertTrue(result['truncated'])
        self.assertEqual(result['counts']['queued'], 0)
        self.assertEqual(result['alerts']['stale'], 3)
        self.assertIn('L?/?', result['line'])

    def test_events_are_allowlisted_bounded_and_rotated(self):
        event = {'type': 'started', 'id': 'cq-1', 'at': 10, 'command': 'secret', 'reason': 'secret', 'slots': 1}
        with patch.object(ft, 'MAX_EVENT_BYTES', 100):
            for _ in range(10):
                ft.append_event(self.state, event)
            current = self.state / 'events.jsonl'
            previous = self.state / 'events.jsonl.1'
            for path in (current, previous):
                self.assertLessEqual(path.stat().st_size, 100)
                self.assertNotIn('secret', path.read_text(encoding='utf-8'))
                for line in path.read_text(encoding='utf-8').splitlines():
                    self.assertEqual(json.loads(line)['type'], 'started')

    def test_events_cross_process_do_not_lose_writes(self):
        module_dir = str(Path(ft.__file__).parent)
        code = ('import sys; sys.path.insert(0,sys.argv[1]); import fleet_telemetry as f; '
                '[f.append_event(sys.argv[2], {"type":"done","id":str(i)}) for i in range(40)]')
        ps = [subprocess.Popen([sys.executable, '-c', code, module_dir, str(self.state)]) for _ in range(3)]
        for p in ps:
            self.assertEqual(p.wait(timeout=30), 0)
        lines = (self.state / 'events.jsonl').read_text(encoding='utf-8').splitlines()
        self.assertEqual(len(lines), 120)
        self.assertTrue(all(json.loads(line)['type'] == 'done' for line in lines))

    def test_exact_complete_success_and_conservative_cap(self):
        base = dict(status='done', exit=0, profile_key='p', command_key='c', platform='linux', host_id='hearth',
                    slots=2, t_started=1, t_finished=2, metrics={'memory_kind': 'cgroup', 'peak_memory_bytes': 2*1024**3})
        args = ('p', 'c', 'linux', 'hearth', 2, 1)
        rows = [base, dict(base, status='failed'), dict(base, exit=1), dict(base, host_id='ember'),
                dict(base, t_finished=None), dict(base, reason='OOM'),
                dict(base, metrics={'memory_kind': 'cgroup', 'peak_memory_bytes': math.nan})]
        result = ft.estimate_memory(rows, *args)
        self.assertEqual(result, {'memory_gb': 2.5, 'sample_count': 1, 'memory_kind': 'cgroup', 'method': 'max_plus_25_percent'})
        mixed = [base, dict(base, metrics={'memory_kind': 'rss', 'peak_memory_bytes': 3*1024**3})]
        self.assertEqual(ft.estimate_memory(mixed, *args)['method'], 'default')
        self.assertEqual(ft.estimate_memory([base], 'p', 'c', 'linux', 'hearth', 2, 8)['memory_gb'], 8)
        self.assertEqual(ft.estimate_memory([], *args)['memory_gb'], 1)
        self.assertEqual(ft.estimate_memory([base], None, 'c', 'linux', 'hearth', 2, 1)['method'], 'default')


if __name__ == '__main__':
    unittest.main()
