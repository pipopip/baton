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
        ft._JOB_CACHE.clear()
        self.addCleanup(ft._JOB_CACHE.clear)
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
        # the old line was 'CQ L1/4 H0/8 E17/16 · q1 · <lifetime alerts>'; the new one is device names, live work, nothing lifetime
        self.assertEqual(summary['line'], 'ember 17/16 · hearth STALE · laptop 1/4 · 1 running · 1 queued')
        self.assertEqual(summary['alerts'], {'stale': 1, 'down': 1, 'overrun': 2, 'queue_delay': 1,
                         'uncertain': 1, 'cancel_pending': 1, 'artifact_missing': 1, 'failure': 1, 'oom': 1})
        self.assertNotIn('secret', json.dumps(summary))
        self.assertEqual(summary['counts']['failed'], 2)

    def test_additional_device_and_safe_name(self):
        (self.state / 'capacity.json').write_text(json.dumps({'at': self.now, 'devices': {'zephyr': {
            'at': self.now, 'up': True, 'name': 'Desk\nRemote', 'slots_used': 1, 'slots_total': 5}}}), encoding='utf-8')
        result = ft.summarize(self.state, self.now)
        self.assertEqual(result['devices']['zephyr']['name'], 'Desk Remote')
        self.assertIn('Desk Remote 1/5', result['line'])

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
        self.assertIn('ember 5/8', summary['line'])
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
        self.assertIn('ember 12/14', summary['line'])
        self.assertNotIn('overrun', summary['alerts'])
        # an unconfirmed staging, and a deferred job that has started, still count
        job('staging', status='launching', host_id='ember', slots=2, deferred_registered=False, deferred_phase='staging_pending')
        job('started', status='running', host_id='ember', slots=1, deferred_registered=True, deferred_phase='running')
        summary = ft.summarize_generations(self.state, self.now)
        self.assertEqual(summary['devices']['ember']['slots_used'], 15)
        self.assertEqual(summary['alerts'].get('overrun'), 1)

    def settle(self, name, seconds=60):
        """Put a job file's times `seconds` into the past, as a file nothing has written for that long."""
        import time
        old = time.time() - seconds
        os.utime(self.state / 'jobs' / (name + '.json'), (old, old))

    def opened(self):
        """A patch of _read that records the job files opened."""
        names, real = [], ft._read

        def counted(path, size=None):
            if path.parent.name == 'jobs':
                names.append(path.name)
            return real(path, size)
        patcher = patch.object(ft, '_read', counted)
        patcher.start()
        self.addCleanup(patcher.stop)
        return names

    def test_a_settled_job_file_is_opened_once_however_often_the_snapshot_is_built(self):
        self.job('a', status='running', host_id='ember', slots=2)
        self.job('b', status='queued')
        self.settle('a')
        self.settle('b')
        opened = self.opened()
        first = ft.summarize(self.state, now=self.now)
        self.assertEqual(sorted(opened), ['a.json', 'b.json'])
        del opened[:]
        self.assertEqual(ft.summarize(self.state, now=self.now), first)
        self.assertEqual(ft._read_jobs(self.state / 'jobs')[0], 2)
        self.assertEqual(opened, [])
        self.assertEqual((first['counts']['running'], first['counts']['queued']), (1, 1))

    def test_a_job_file_that_changed_is_read_again(self):
        self.job('a', status='queued')
        self.settle('a')
        self.assertEqual(ft.summarize(self.state, now=self.now)['counts']['queued'], 1)
        opened = self.opened()
        self.job('a', status='running', host_id='ember', slots=1)  # written now: read on every call until it settles
        for _ in range(2):
            self.assertEqual(ft.summarize(self.state, now=self.now)['counts']['running'], 1)
        self.assertEqual(opened, ['a.json', 'a.json'])
        self.settle('a')
        ft.summarize(self.state, now=self.now)
        del opened[:]
        ft.summarize(self.state, now=self.now)
        self.assertEqual(opened, [])
        with patch.object(ft, 'JOB_REFRESH_S', 0):  # and again once the refresh time has passed
            ft.summarize(self.state, now=self.now)
        self.assertEqual(opened, ['a.json'])

    def test_a_removed_job_file_and_a_missing_directory_leave_nothing_behind(self):
        self.job('a', status='queued')
        self.job('b', status='queued')
        self.settle('a')
        self.settle('b')
        ft.summarize(self.state, now=self.now)
        (self.state / 'jobs' / 'a.json').unlink()
        self.assertEqual(ft.summarize(self.state, now=self.now)['counts']['queued'], 1)
        self.assertEqual(list(ft._JOB_CACHE[str(self.state / 'jobs')]), ['b.json'])
        (self.state / 'jobs' / 'b.json').unlink()
        (self.state / 'jobs').rmdir()
        self.assertEqual(ft._read_jobs(self.state / 'jobs'), (0, []))
        self.assertNotIn(str(self.state / 'jobs'), ft._JOB_CACHE)

    def test_an_unreadable_job_file_is_tried_again_next_time(self):
        (self.state / 'jobs' / 'a.json').write_text('{', encoding='utf-8')
        self.settle('a')
        opened = self.opened()
        ft.summarize(self.state, now=self.now)
        ft.summarize(self.state, now=self.now)
        self.assertEqual(opened, ['a.json', 'a.json'])

    def test_the_generation_summary_opens_each_job_file_once(self):
        fleet = self.state / 'fleet'
        (fleet / 'jobs').mkdir(parents=True)
        for name in ('a', 'b', 'c'):
            path = fleet / 'jobs' / (name + '.json')
            path.write_text(json.dumps({'status': 'running', 'host_id': 'ember', 'slots': 1}), encoding='utf-8')
            import time
            old = time.time() - 60
            os.utime(path, (old, old))
        opened = self.opened()
        result = ft.summarize_generations(self.state, now=self.now)
        self.assertEqual(sorted(opened), ['a.json', 'b.json', 'c.json'])
        self.assertEqual(result['counts']['running'], 3)
        self.assertEqual(result['devices']['ember']['slots_used'], 3)

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
        self.assertIn('no capacity data', result['line'])

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


class StatusLineTests(unittest.TestCase):
    """The status line: devices with a reason when they cannot take work, live work by worktree, current attention only."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = Path(self.tmp.name)
        (self.state / 'jobs').mkdir()
        self.now = 1700000000

    def capacity(self, **devices):
        base = {'at': self.now, 'up': True, 'eligible': True, 'slots_used': 0, 'slots_total': 4, 'reason': ''}
        (self.state / 'capacity.json').write_text(json.dumps({'at': self.now, 'devices': {
            host: {**base, **values} for host, values in devices.items()}}), encoding='utf-8')

    def job(self, name, **values):
        (self.state / 'jobs' / (name + '.json')).write_text(json.dumps(values), encoding='utf-8')

    def line(self):
        return ft.summarize(self.state, self.now)['line']

    def test_busy_idle_ineligible_and_stale_devices_read_in_words(self):
        self.capacity(
            ember={'name': 'ember', 'slots_used': 10, 'slots_total': 14},
            hearth={'name': 'hestia', 'slots_used': 1, 'slots_total': 6, 'eligible': False, 'reason': 'legacy work active or waiting'},
            laptop={'name': 'laptop', 'slots_total': 4, 'eligible': False, 'reason': 'physical or commit RAM floor'})
        self.assertEqual(self.line(), 'ember 10/14 · hestia 1/6 legacy busy · laptop 0/4 RAM floor · idle')
        # a device that is only full, or idle and fine, shows its numbers and nothing else
        self.capacity(ember={'slots_used': 14, 'slots_total': 14, 'eligible': False, 'reason': 'CPU slots unavailable'},
                      laptop={'slots_used': 0})
        self.assertEqual(self.line(), 'ember 14/14 · laptop 0/4 · idle')
        # down, paused and stale are the words that ask for action
        self.capacity(ember={'up': False}, hearth={'eligible': False, 'reason': 'host paused'},
                      laptop={'at': self.now - 400})
        summary = ft.summarize(self.state, self.now)
        self.assertEqual(summary['line'], 'ember DOWN · hearth 0/4 PAUSED · laptop STALE · idle')
        self.assertEqual(summary['attention'], ['ember down', 'hearth paused', 'laptop stale'])
        self.capacity(ember={'eligible': False, 'reason': 'host load cap'}, laptop={'eligible': False, 'reason': 'RAM floor'})
        self.assertEqual(self.line(), 'ember 0/4 load cap · laptop 0/4 RAM floor · idle')

    def test_running_and_queued_with_distinct_worktrees_from_live_job_records(self):
        self.capacity(ember={'slots_used': 3, 'slots_total': 14})
        base = 'C:/Work/r/.claude/worktrees/'
        self.job('a', status='running', top=base + 'one', session='s1')
        self.job('b', status='running', top=base + 'one', session='s1')
        self.job('c', status='queued', top=base + 'Two', session='s2')
        self.job('d', status='queued', top=(base + 'two').replace('/', chr(92)), session='s3')  # the same worktree, spelled differently
        self.job('e', status='launching', top=base + 'three', session='s4', deferred_registered=True, deferred_phase='queued')
        self.job('f', status='done', top=base + 'four', session='s5', t_finished=self.now - 5)  # finished work is not live
        summary = ft.summarize(self.state, self.now)
        self.assertEqual(summary['line'], 'ember 3/14 · 2 running · 3 queued · 3 worktrees')
        self.assertEqual(summary['activity'], {'running': 2, 'waiting': 3, 'worktrees': 3, 'sessions': 4})
        # a fleet job keeps the status 'launching' for its whole run: once its host reports it running, it is running
        self.job('e', status='launching', top=base + 'three', session='s4', deferred_registered=True, deferred_phase='running')
        started = ft.summarize(self.state, self.now)
        self.assertEqual(started['line'], 'ember 3/14 · 3 running · 2 queued · 3 worktrees')
        self.assertEqual((started['activity']['running'], started['activity']['waiting'], started['counts']['launching']), (3, 2, 1))
        self.job('e', status='launching', top=base + 'three', session='s4', deferred_registered=True, deferred_phase='queued')
        # the numbers move as work moves
        self.job('a', status='done', top=base + 'one', t_finished=self.now)
        self.job('b', status='done', top=base + 'one', t_finished=self.now)
        self.assertEqual(self.line(), 'ember 3/14 · 0 running · 3 queued · 2 worktrees')
        # a record with no worktree is still work, and no worktree count is invented for it
        for name in 'cde':
            self.job(name, status='done', t_finished=self.now)
        self.job('g', status='running')
        self.assertEqual(self.line(), 'ember 3/14 · 1 running · 0 queued')

    def test_lifetime_totals_of_failures_and_missing_artifacts_are_not_in_the_line(self):
        self.capacity(ember={'slots_used': 1})
        for i in range(30):
            self.job(f'f{i}', status='failed', t_finished=self.now - 60, artifact_missing=[{'path': 'x'}], reason='out of memory')
        summary = ft.summarize(self.state, self.now)
        self.assertEqual(summary['line'], 'ember 1/4 · idle')
        for word in ('fail', 'artifact', 'oom', 'memory', '30'):
            self.assertNotIn(word, summary['line'])
        # the fleet view keeps the totals, and every key it had
        self.assertEqual(summary['alerts'], {'artifact_missing': 30, 'failure': 30, 'oom': 30})
        self.assertEqual(summary['counts']['failed'], 30)
        for key in ('line', 'devices', 'counts', 'alerts', 'truncated'):
            self.assertIn(key, summary)
        self.assertEqual(summary['attention'], [])

    def test_attention_is_for_current_conditions(self):
        # work waits and no device can take it: dispatch is blocked
        self.capacity(ember={'eligible': False, 'reason': 'legacy work active or waiting'},
                      laptop={'eligible': False, 'reason': 'physical or commit RAM floor'})
        self.job('q', status='queued', top='D:/x/one')
        summary = ft.summarize(self.state, self.now)
        self.assertTrue(summary['dispatch_blocked'])
        self.assertEqual(summary['attention'], ['dispatch blocked'])
        self.assertTrue(summary['line'].endswith('0 running · 1 queued · 1 worktree · DISPATCH BLOCKED'), summary['line'])
        # one device that can take it, or an empty queue, and nothing is blocked
        self.capacity(ember={'eligible': True}, laptop={'eligible': False, 'reason': 'RAM floor'})
        self.assertFalse(ft.summarize(self.state, self.now)['dispatch_blocked'])
        self.capacity(ember={'eligible': False, 'reason': 'CPU slots unavailable', 'slots_used': 4})  # full, not blocked
        self.assertFalse(ft.summarize(self.state, self.now)['dispatch_blocked'])
        self.job('q', status='done', t_finished=self.now)
        self.capacity(ember={'eligible': False, 'reason': 'legacy work active or waiting'})
        summary = ft.summarize(self.state, self.now)
        self.assertEqual((summary['dispatch_blocked'], summary['attention']), (False, []))

    def test_a_stale_snapshot_says_so_and_the_work_counts_still_move(self):
        self.capacity(ember={'at': self.now - 1000, 'slots_used': 9}, hearth={'at': self.now - 1000}, laptop={'at': self.now - 1000})
        self.job('q', status='queued', top='D:/x/one')
        self.job('r', status='running', top='D:/x/two')
        summary = ft.summarize(self.state, self.now)
        self.assertEqual(summary['line'], 'ember STALE · hearth STALE · laptop STALE · 1 running · 1 queued · 2 worktrees')
        self.assertNotIn('9', summary['line'])  # a stale sample's numbers are not shown as current
        self.assertFalse(summary['dispatch_blocked'])  # nothing fresh to judge by; the STALE words carry it
        # nothing at all in the cache
        (self.state / 'capacity.json').unlink()
        self.assertEqual(self.line(), 'no capacity data · 1 running · 1 queued · 2 worktrees')

    def test_the_line_stays_short(self):
        self.capacity(ember={'name': 'ember', 'slots_used': 13, 'slots_total': 14},
                      hearth={'name': 'hestia-build', 'slots_used': 1, 'slots_total': 6, 'eligible': False, 'reason': 'legacy work active or waiting'},
                      laptop={'name': 'laptop', 'eligible': False, 'reason': 'physical or commit RAM floor'})
        for i in range(17):
            self.job(f'r{i}', status='running' if i < 12 else 'queued', top=f'C:/Work/r/.claude/worktrees/w{i}')
        line = self.line()
        self.assertLessEqual(len(line), ft.LINE_WIDTH, line)
        self.assertIn('hestia-build 1/6 legacy busy', line)
        self.assertIn('12 run', line)
        self.assertIn('17 wt', line)
        # an ordinary line, with all devices usable, is well inside the width
        self.capacity(ember={'slots_used': 13, 'slots_total': 14}, hearth={'name': 'hestia', 'slots_total': 6}, laptop={})
        self.assertLessEqual(len(self.line()), 100)
        # a name is cut and cleaned
        self.capacity(ember={'name': 'a very long device name\nwith a break'})
        self.assertTrue(self.line().startswith('a very long 0/4'), self.line())

    def test_both_generations_count_one_set_of_worktrees(self):
        legacy, current = self.state / 'fleet', self.state / 'queue-v2' / 'fleet'
        for root in (legacy, current):
            (root / 'jobs').mkdir(parents=True)
        (self.state / 'queue-v2' / '.generation').write_bytes(b'baton-queue-generation-2\n')
        (legacy / 'jobs' / 'a.json').write_text(json.dumps(dict(status='running', host_id='ember', slots=2, top='D:/x/one')), encoding='utf-8')
        (current / 'jobs' / 'b.json').write_text(json.dumps(dict(status='running', host_id='ember', slots=1, top='D:/x/one')), encoding='utf-8')
        (current / 'jobs' / 'c.json').write_text(json.dumps(dict(status='queued', top='D:/x/two')), encoding='utf-8')
        # the older generation's cache is old; only the live one is current
        for root, at in ((legacy, self.now - 3000), (current, self.now)):
            (root / 'capacity.json').write_text(json.dumps({'at': at, 'devices': {'ember': {
                'at': at, 'up': True, 'eligible': True, 'slots_used': 1, 'slots_total': 14, 'name': 'ember'}}}), encoding='utf-8')
        summary = ft.summarize_generations(self.state, self.now)
        self.assertEqual(summary['line'], 'ember 3/14 · 2 running · 1 queued · 2 worktrees')
        self.assertEqual(summary['activity']['worktrees'], 2)


if __name__ == '__main__':
    unittest.main()
