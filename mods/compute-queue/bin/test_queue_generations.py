import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import cq
import fleet_telemetry
import multi_host
import queue_generations as generations
import queue_paths


class Generations(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.ctx = cq.Ctx(state=str(self.root), now=lambda: 1000, sync=True)
        (self.root / 'hosts.json').write_text(json.dumps({'version': 1, 'hosts': {
            'hearth': {'state': 'hosts/hearth', 'enabled': True}}}))
        self.legacy = 'cq-hearth-20261110T120000-0123456789abcdef'
        self.new = 'cq-v2--hearth-20261110T120000-0123456789abcdef'
        self.primary = 'cq-v2--20261110T120000-0123456789abcdef'
        self.fleet = 'cq-v2--fleet-20261110T120000-0123456789abcdef'

    def write(self, state, jid):
        path = Path(state) / 'jobs' / (jid + '.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({'id': jid, 'status': 'done'}))

    def test_context_and_shard_config(self):
        v2 = generations.context(cq, self.ctx, 2)
        self.assertTrue(cq.same_path(v2.state, str(self.root / 'queue-v2')))
        registry = multi_host._registry(cq, v2)
        self.assertEqual(os.path.normcase(os.path.realpath(registry['hearth'][0])),
                         os.path.normcase(os.path.realpath(self.root / 'hosts' / 'hearth')))
        self.assertIsNone(multi_host._location(cq, v2, self.new, registry))
        self.assertFalse((self.root / 'hosts/hearth/queue-v2').exists())
        queue_paths.ensure_generation2(self.root / 'hosts/hearth/queue-v2')
        self.assertTrue(cq.same_path(multi_host._location(cq, v2, self.new, registry).state,
                                    str(self.root / 'hosts' / 'hearth' / 'queue-v2')))
        self.assertEqual(multi_host._location(cq, v2, self.fleet, registry).state, v2.state)
        self.assertEqual(multi_host._location(cq, v2, self.primary, registry).state, v2.state)
        self.assertEqual(Path(queue_paths.config_state(v2.state)), self.root)

    def test_old_reader_cannot_see_v2_and_both_generations_read(self):
        self.write(self.root / 'hosts/hearth', self.legacy)
        generations.context(cq, self.ctx, 2)
        queue_paths.ensure_generation2(self.root / 'hosts/hearth/queue-v2')
        self.write(self.root / 'hosts/hearth/queue-v2', self.new)
        self.assertEqual([j['id'] for j in multi_host.all_jobs(cq, self.ctx)], [self.legacy])
        self.assertEqual({j['id'] for j in generations.all_jobs(cq, self.ctx)}, {self.legacy, self.new})
        self.assertEqual(generations.load_job(cq, self.ctx, self.new)['id'], self.new)
        self.assertEqual(generations.load_job(cq, self.ctx, self.legacy)['id'], self.legacy)

    def test_wrong_generation_wrong_shard_and_malformed_refused(self):
        self.write(self.root / 'hosts/hearth', self.new)
        with self.assertRaisesRegex(cq.CqError, 'wrong queue generation'):
            generations.load_job(cq, self.ctx, self.new)
        with self.assertRaisesRegex(cq.CqError, 'wrong queue generation'):
            generations.all_jobs(cq, self.ctx)
        with self.assertRaisesRegex(cq.CqError, 'invalid generation-two'):
            generations.job_generation(cq, 'cq-v2--hearth-malformed')
        unknown = 'cq-v2--unknown-20261110T120000-0123456789abcdef'
        with self.assertRaisesRegex(cq.CqError, 'unknown host'):
            generations.load_job(cq, self.ctx, unknown)

    def test_mixed_notices_route_once_and_submit_selection(self):
        self.write(self.root / 'hosts/hearth', self.legacy)
        generations.context(cq, self.ctx, 2)
        queue_paths.ensure_generation2(self.root / 'hosts/hearth/queue-v2')
        self.write(self.root / 'hosts/hearth/queue-v2', self.new)
        calls = []
        def leaf(ctx, cmd, req):
            calls.append((ctx.state, cmd, dict(req)))
            return {'ok': True, 'jobs': [], 'acknowledged_ids': list(req.get('ids', []))}
        result = generations.execute(cq, self.ctx, 'notified', {'ids': [self.legacy, self.new, self.new], 'notice_protocol': 2}, leaf)
        self.assertEqual(result['acknowledged_ids'], [self.legacy, self.new])
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][2]['ids'], [self.legacy])
        self.assertEqual(calls[1][2]['ids'], [self.new])
        who = {'root': str(self.root), 'session': 'sess-A'}
        for req, expected in (({'where': 'fleet'}, 2), ({'gets': ['out']}, 2),
                              ({'artifact_total_mb': 10}, 2), ({'where': 'host'}, 1)):
            generations.execute(cq, self.ctx, 'submit', dict(req, **who), leaf)
            self.assertEqual(calls[-1][0].endswith('queue-v2'), expected == 2)
        generations.execute(cq, self.ctx, 'cancel', dict({'job': self.new}, **who), leaf)
        self.assertTrue(calls[-1][0].endswith('queue-v2'))
        count = len(calls)
        for command, req in (('submit', {'where': 'fleet'}), ('cancel', {'job': self.new})):
            for partial in ({}, {'root': str(self.root)}, {'session': 'sess-A'}):
                with self.assertRaisesRegex(cq.CqError, '(submission|cancellation) requires'):
                    generations.execute(cq, self.ctx, command, dict(req, **partial), leaf)
        self.assertEqual(len(calls), count)

    def test_a_heavy_where_host_job_becomes_a_fleet_job_pinned_to_its_host(self):
        generations.context(cq, self.ctx, 2)
        calls = []
        def leaf(ctx, cmd, req):
            calls.append((ctx.state.endswith('queue-v2'), dict(req)))
            return {'ok': True, 'job': self.fleet, 'note': 'fleet job accepted'}
        base = {'root': str(self.root), 'session': 'sess-A', 'command': 'bash build.sh'}
        submit = lambda **fields: generations.execute(cq, self.ctx, 'submit', dict(base, **fields), leaf)
        with unittest.mock.patch.object(cq, 'load_job', return_value={'memory_max_gb': 12.0}):
            result = submit(where='host', slots=3, est_minutes=20, priority='verify', _default_memory_max_gb=50)
        v2, sent = calls[-1]
        self.assertTrue(v2)
        self.assertEqual((sent['where'], sent['candidates'], sent['platform'], sent['slots']), ('fleet', ['ember'], 'linux', 3))
        self.assertEqual(sent['_default_memory_max_gb'], 12)          # 4 GB a slot; a figure sent by the client is dropped
        self.assertEqual((sent['est_minutes'], sent['priority'], sent['command']), (20, 'verify', 'bash build.sh'))
        self.assertEqual(result['note'], 'fleet job accepted; this where: host job runs as a contained fleet job on ember: '
                                         'CPU held to 3 slots, memory capped at 12 GB (state memory_max_gb to change it)')
        self.assertEqual(sent['label'], 'bash build.sh')              # named after its command, as an ordinary job was
        submit()                                                       # where and slots left out: the host, one slot
        self.assertEqual((calls[-1][0], calls[-1][1]['candidates'], calls[-1][1]['slots'], calls[-1][1]['_default_memory_max_gb']),
                         (True, ['ember'], 1, 8))                      # never under the 8 GB floor
        # the ordinary backend ignored `requires`; a stated label stands; NAME=value stays out of a derived one
        submit(slots=2, requires=['no-such-tool'], label='mine', command='A=1 B=2 go test ./... -run X -count 1 -v -race')
        self.assertEqual((calls[-1][0], calls[-1][1]['label']), (True, 'mine'))
        self.assertNotIn('requires', calls[-1][1])
        submit(slots=2, command='A=1 B=2 go test ./... -run X -count 1 -v -race')
        self.assertEqual(calls[-1][1]['label'], 'go test ./... -run X -count')
        # a job larger than the host's fleet pool would be accepted there and never placed: it stays ordinary.
        # With no settings file the pool is the engine's default 8 slots, less 2 kept for foreground work.
        submit(slots=6)
        self.assertTrue(calls[-1][0])
        submit(slots=7)
        self.assertFalse(calls[-1][0])
        (self.root / 'host.conf.json').write_text(json.dumps({'name': 'ember', 'slots': 16}))
        submit(slots=14)
        self.assertEqual((calls[-1][0], calls[-1][1]['_default_memory_max_gb']), (True, 42))   # the memory budget bounds it
        submit(slots=15)
        self.assertFalse(calls[-1][0])
        (self.root / 'host.conf.json').write_text(json.dumps({'name': 'ember', 'slots': 16, 'foreground_slots': 4,
                                                             'memory_budget_gb': 30}))
        submit(slots=12)
        self.assertEqual((calls[-1][0], calls[-1][1]['_default_memory_max_gb']), (True, 30))
        submit(slots=13)
        self.assertFalse(calls[-1][0])
        (self.root / 'host.conf.json').unlink()
        # what stays an ordinary job: an explicitly chosen host (its own queue; GPU work is qualified there), light
        # work, and what the fleet backend does not do
        for fields in ({'host_id': 'hestia', 'slots': 2}, {'host_id': 'ember'},
                       {'slots': 0}, {'slots': 2, 'self_heavy': True}, {'slots': 2, 'split': {'max_parts': 2}},
                       {'temp_storage': 'ram', 'memory_max_gb': 4}, {'defer': 'overnight', 'host_id': 'ember', 'memory_max_gb': 4},
                       {'where': 'laptop'}, {'where': 'any'}, {'host_id': 'laptop'}, {'est_minutes': 600},
                       {'command': 'bash part.sh {part}'}, {'slots': '2'}, {'timeout_minutes': 900}):
            submit(**fields)
            self.assertFalse(calls[-1][0], fields)
            self.assertNotIn('_default_memory_max_gb', calls[-1][1])
        # the queue's own setting turns it off; the engine's loaded settings (the last host served) do not decide
        (self.root / 'host.conf.json').write_text(json.dumps({'name': 'ember', 'host_jobs_to_fleet': False}))
        submit(where='host', slots=3)
        self.assertFalse(calls[-1][0])
        (self.root / 'host.conf.json').write_text(json.dumps({'name': 'ember'}))
        with unittest.mock.patch.dict(cq._CONF, {'host_jobs_to_fleet': False}):
            submit(where='host', slots=3)
            self.assertTrue(calls[-1][0])
        # settings that cannot be read or make no sense: the request keeps the path it always had
        for text in ('not json', '[]', json.dumps({'slots': '16'}), json.dumps({'slots': 16, 'foreground_slots': 'two'}),
                     json.dumps({'memory_budget_gb': True}), '[' * 5000):
            (self.root / 'host.conf.json').write_text(text)
            submit(where='host', slots=3)
            self.assertFalse(calls[-1][0], text[:40])
        (self.root / 'host.conf.json').unlink()
        # the settings file is not opened for a request that would not convert anyway
        with unittest.mock.patch('builtins.open', side_effect=AssertionError('settings read')):
            submit(slots=0)
            submit(where='laptop')
        # generation two unusable (its state overlaps a shard, or cannot be opened): ordinary, as before
        with unittest.mock.patch.object(generations, '_v2_availability', return_value=(False, 'overlaps a shard')):
            submit(slots=2)
            self.assertFalse(calls[-1][0])
        with unittest.mock.patch.object(generations, '_v2_availability', return_value=(False, None)):   # not created yet
            submit(slots=2)
            self.assertTrue(calls[-1][0])
        opened = generations.context
        def broken(engine, ctx, generation):
            if generation == 2:
                raise ValueError('generation-two state exists without a valid marker')
            return opened(engine, ctx, generation)
        with unittest.mock.patch.object(generations, 'context', side_effect=broken):
            submit(slots=2, requires=['go'])
            self.assertFalse(calls[-1][0])
            self.assertEqual((calls[-1][1].get('requires'), calls[-1][1].get('where')), (['go'], None))   # the request as it came
        # a direct fleet submission cannot pick the roomier default for itself
        submit(where='fleet', _default_memory_max_gb=50)
        self.assertNotIn('_default_memory_max_gb', calls[-1][1])
        # a refusal says why a where: host job met a fleet rule
        def refuse(ctx, cmd, req):
            raise cq.CqError('fleet submission requires the current worktree owner session')
        with self.assertRaisesRegex(cq.CqError, r'owner session \(a heavy where: host job that names no host now runs as a '
                                                r'contained fleet job on ember, so the host can share its capacity\)$'):
            generations.execute(cq, self.ctx, 'submit', dict(base, slots=2), refuse)
        # the note is a courtesy: a job whose record cannot be read back is still accepted
        with unittest.mock.patch.object(cq, 'load_job', side_effect=cq.CqError('gone')):
            self.assertEqual(submit(slots=2), {'ok': True, 'job': self.fleet, 'note': 'fleet job accepted'})

    def test_views_merge_and_flag_generation_errors(self):
        generations.context(cq, self.ctx, 2)
        def leaf(ctx, cmd, req):
            v2 = ctx.state.endswith('queue-v2')
            if cmd == 'list':
                return {'ok': True, 'jobs': [{'id': self.new if v2 else self.legacy, 'age_min': 2 if v2 else 4}]}
            if cmd == 'status':
                return {'ok': True, 'queue': [{'id': self.new if v2 else self.legacy}], 'recent': []}
            return {'ok': True, 'watch': {'queued': 2 if v2 else 1, 'to_notify': []}}
        self.assertEqual(len(generations.execute(cq, self.ctx, 'list', {}, leaf)['jobs']), 2)
        self.assertEqual(len(generations.execute(cq, self.ctx, 'status', {}, leaf)['queue']), 2)
        self.assertEqual(generations.execute(cq, self.ctx, 'watch', {}, leaf)['watch']['queued'], 3)
        def broken(ctx, command, req):
            return {'ok': not ctx.state.endswith('queue-v2')}
        self.assertFalse(generations.execute(cq, self.ctx, 'list', {}, broken)['ok'])

    def test_tick_dispatches_v2_primary_under_its_own_lease(self):
        v2 = generations.context(cq, self.ctx, 2)
        cq.write_json(v2.p('leader.json'), {'session': 'owner', 'heartbeat': 999})
        calls = []
        def leaf(ctx, cmd, req):
            calls.append((ctx.state, cmd))
            return {'ok': True, 'leader': True, 'can_start': True, 'watch': {}}
        generations.execute(cq, self.ctx, 'tick', {'session': 'owner'}, leaf)
        self.assertEqual([(Path(state).name, cmd) for state, cmd in calls if cmd == 'apply'], [('queue-v2', 'apply')])
        calls.clear()
        generations.execute(cq, self.ctx, 'tick', {'session': 'another'}, leaf)
        self.assertNotIn('apply', [cmd for _, cmd in calls])

    def test_internal_v2_context_does_not_reenter_both_generations(self):
        v2 = generations.context(cq, self.ctx, 2)
        calls = []
        def leaf(ctx, cmd, req):
            calls.append((ctx.state, cmd))
            return {'ok': True}
        generations.execute(cq, v2, 'tick', {}, leaf)
        self.assertEqual(calls, [(v2.state, 'tick')])

    def test_a_snapshot_that_cannot_be_built_does_not_fail_the_tick(self):
        # the tick also schedules the queue and delivers notices; the status line is the only thing that may suffer
        generations.context(cq, self.ctx, 2)   # generation two exists, so the combined view is asked for
        def leaf(ctx, cmd, req):
            return {'ok': True, 'watch': {'queued': 1, 'to_notify': [{'id': 'cq-1'}]}, 'fleet': {'line': 'own generation'}}
        with mock.patch.object(fleet_telemetry, 'summarize_generations', side_effect=KeyError('devices'), create=True):
            result = generations.execute(cq, self.ctx, 'tick', {}, leaf)
        self.assertTrue(result['ok'])
        self.assertEqual(result['fleet'], {'line': 'own generation'})
        self.assertEqual(result['fleet_warning'], 'combined fleet snapshot unavailable: KeyError')
        self.assertEqual([j['id'] for j in result['watch']['to_notify']][:1], ['cq-1'])

    def test_tick_and_status_carry_one_fleet_view_of_both_generations_and_merged_attention(self):
        v2 = generations.context(cq, self.ctx, 2)
        def leaf(ctx, cmd, req):
            old = not ctx.state.endswith('queue-v2')
            return {'ok': True, 'watch': {'queued': 1, 'to_notify': [], 'mine_attention': {'failed': 1 if old else 0, 'missing': 0 if old else 2}},
                    'fleet': {'line': 'older generation only' if old else 'newer generation only'}}
        with mock.patch.object(fleet_telemetry, 'summarize_generations', return_value={'line': 'combined'}, create=True) as summary:
            for command in ('tick', 'watch', 'status'):
                result = generations.execute(cq, self.ctx, command, {}, leaf)
                # the primary result's own fleet block is the older generation's, whose cache is no longer written
                self.assertEqual(result['fleet'], {'line': 'combined'}, command)
                self.assertEqual(result['generations']['v2']['fleet'], {'line': 'newer generation only'})
                if command != 'status':
                    self.assertEqual(result['watch'].get('mine_attention'), {'failed': 1, 'missing': 2})

    def test_fleet_view_uses_aggregated_telemetry(self):
        with mock.patch.object(fleet_telemetry, 'summarize_generations', return_value={'combined': True}, create=True) as summary:
            result = generations.execute(cq, self.ctx, 'status', {'view': 'fleet'}, lambda *a: None)
        self.assertEqual(result['fleet'], {'combined': True})
        self.assertEqual(Path(summary.call_args.args[0]), self.root)
        self.assertEqual(summary.call_args.args[1], 1000)
        state = self.root / 'queue-v2'
        state.mkdir()
        (state / 'unrelated').write_text('legacy data')
        with mock.patch.object(fleet_telemetry, 'summarize_generations', return_value={}, create=True):
            result = generations.execute(cq, self.ctx, 'status', {'view': 'fleet'}, lambda *a: None)
        self.assertIn('generation2_unavailable', result)

    def test_runtime_paths_shared_but_jobs_isolated(self):
        v2 = self.root / 'queue-v2'
        queue_paths.ensure_generation2(v2)
        (self.root / 'runtime.local.json').write_text(json.dumps({'heavyrun': '/shared/heavy.py'}))
        with mock.patch.dict(os.environ, {'CQ_HEAVY_LOCK': '', 'CQ_HEAVYRUN': '', 'CQ_BATON_DIR': ''}):
            self.assertEqual(queue_paths.runtime_paths(str(self.root)), queue_paths.runtime_paths(str(v2)))
        self.write(v2, self.primary)
        queue_paths.initialize_default_lock(str(v2))
        self.assertFalse((self.root / 'fleet-default-lock.v1').exists())

    def test_legacy_queue_v2_name_and_marker_safety(self):
        legacy = self.root / 'legacy' / 'queue-v2'
        legacy.mkdir(parents=True)
        self.assertEqual(Path(queue_paths.config_state(legacy)), legacy)
        nested = legacy / 'queue-v2'
        queue_paths.ensure_generation2(nested)
        self.assertEqual(Path(queue_paths.config_state(nested)), legacy)
        unmarked = self.root / 'unmarked' / 'queue-v2'
        unmarked.mkdir(parents=True)
        (unmarked / 'jobs').mkdir()
        with self.assertRaisesRegex(ValueError, 'without a valid marker'):
            queue_paths.ensure_generation2(unmarked)
        invalid = self.root / 'invalid' / 'queue-v2'
        invalid.mkdir(parents=True)
        (invalid / '.generation').write_text('wrong')
        with self.assertRaisesRegex(ValueError, 'invalid queue generation marker'):
            queue_paths.config_state(invalid)

    def test_primary_generation_cannot_overlap_a_legacy_shard(self):
        for state in ('queue-v2', 'queue-v2/host'):
            (self.root / 'hosts.json').write_text(json.dumps({'version': 1, 'hosts': {
                'hearth': {'state': state, 'enabled': True}}}))
            with self.subTest(state=state), self.assertRaisesRegex(cq.CqError, 'overlaps'):
                generations.context(cq, self.ctx, 2)
            result = generations.execute(cq, self.ctx, 'status', {}, lambda *a: {'ok': True, 'queue': []})
            self.assertIn('overlaps', result['generation2_unavailable'])
        self.assertFalse((self.root / 'queue-v2' / '.generation').exists())

    def test_read_only_views_preserve_legacy_and_do_not_create_markers(self):
        calls = []
        def leaf(ctx, command, req):
            calls.append((ctx.state, command))
            return {'ok': True, 'jobs': [{'id': self.legacy}], 'queue': [{'id': self.legacy}]}
        result = generations.execute(cq, self.ctx, 'list', {}, leaf)
        self.assertEqual(result['jobs'][0]['id'], self.legacy)
        self.assertFalse((self.root / 'queue-v2').exists())
        state = self.root / 'queue-v2'
        state.mkdir()
        (state / 'unrelated').write_text('legacy data')
        self.write(self.root / 'hosts/hearth', self.legacy)
        result = generations.execute(cq, self.ctx, 'status', {}, leaf)
        self.assertIn('generation2_unavailable', result)
        self.assertEqual(result['queue'][0]['id'], self.legacy)
        self.assertEqual(generations.load_job(cq, self.ctx, self.legacy)['id'], self.legacy)
        self.assertEqual(len(generations.all_jobs(cq, self.ctx)), 1)
        self.assertFalse((state / '.generation').exists())

    def test_scan_includes_fleet_jobs_in_both_generations(self):
        generations.context(cq, self.ctx, 2)
        legacy_id = 'cq-fleet-20261110T120000-0123456789abcdef'
        for path, jid in ((self.root / 'fleet/jobs', legacy_id),
                          (self.root / 'queue-v2/fleet/jobs', self.fleet)):
            path.mkdir(parents=True)
            (path / (jid + '.json')).write_text(json.dumps({'id': jid, 'fleet': True}))
        self.assertEqual({j['id'] for j in generations.all_jobs(cq, self.ctx)}, {legacy_id, self.fleet})

    def test_v2_reads_do_not_initialize_missing_shard_stores(self):
        v2 = generations.context(cq, self.ctx, 2)
        self.assertEqual(generations.all_jobs(cq, self.ctx), [])
        self.assertIsNone(generations.load_job(cq, self.ctx, self.new))
        leaf = lambda ctx, command, req: {'ok': True, 'queue': [], 'recent': [], 'capacity': {}, 'summary': {}}
        multi_host.execute(cq, v2, 'status', {}, leaf)
        self.assertFalse((self.root / 'hosts/hearth/queue-v2').exists())


if __name__ == '__main__':
    unittest.main()
