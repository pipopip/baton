import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import cq
import multi_host as mh

class Shards(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.ctx = cq.Ctx(state=str(self.root), now=lambda: 1000)
        self.config = {'version': 1, 'hosts': {'hearth': {'state': 'hosts/hearth', 'enabled': True}}}
        self.write_config()
        self.calls = []
    def write_config(self):
        (self.root / 'hosts.json').write_text(json.dumps(self.config), encoding='utf-8')
    def leaf(self, ctx, command, req):
        host = getattr(ctx, 'host_id', None) or 'ember'
        self.calls.append((host, command, dict(req)))
        if command == 'status':
            return {'ok': True, 'queue': [{'id': host}], 'recent': [], 'capacity': {'host': {'name': host}}, 'summary': {'host': host}}
        if command == 'tick':
            return {'ok': True, 'leader': host == 'hearth', 'can_start': True}
        return {'ok': True, 'host': host}
    def run_cmd(self, command, req):
        return mh.execute(cq, self.ctx, command, req, self.leaf)
    def test_submit_selector(self):
        self.assertEqual(self.run_cmd('submit', {'where': 'any'})['host'], 'ember')
        self.assertEqual(self.run_cmd('submit', {'host_id': 'ember'})['host'], 'ember')
        self.assertEqual(self.run_cmd('submit', {'host_id': 'hearth'})['host'], 'hearth')
        self.assertNotIn('host_id', self.calls[-1][2])
        for req in ({'host_id': 'hearth', 'where': 'any'}, {'host_id': 'missing'}):
            with self.assertRaises(cq.CqError): self.run_cmd('submit', req)
    def test_reserved_host_ids_and_primary_alias(self):
        self.config['aliases'] = {'main': 'ember'}
        self.write_config()
        self.assertEqual(self.run_cmd('submit', {'host_id': 'main'})['host'], 'ember')
        for host in ('ember', 'fleet', 'laptop', 'bad-name'):
            self.config['hosts'][host] = {'state': 'hosts/bad', 'enabled': True}
            self.write_config()
            with self.assertRaises(cq.CqError):
                self.run_cmd('submit', {})
            del self.config['hosts'][host]

    def test_disabled_and_batch(self):
        self.config['hosts']['hearth']['enabled'] = False
        self.write_config()
        jid = 'cq-hearth-20261107T123456-1234'
        with self.assertRaises(cq.CqError): self.run_cmd('submit', {'host_id': 'hearth'})
        self.assertEqual(self.run_cmd('cancel', {'job': jid})['host'], 'hearth')
        self.assertEqual(self.run_cmd('get', {'job': 'cq-20261107T123456-1234'})['host'], 'ember')
        self.assertTrue(self.run_cmd('notified', {'ids': [jid, 'cq-20261107T123456-1234']})['ok'])
        with self.assertRaises(cq.CqError): self.run_cmd('get', {'job': 'cq-unknown-20261107T123456-1234'})
    def test_renamed_host_alias_keeps_one_queue_and_old_jobs(self):
        self.config['aliases'] = {'hestia': 'hearth'}
        self.write_config()
        for name in ('hestia', 'hearth'):
            self.assertEqual(self.run_cmd('submit', {'host_id': name, 'where': 'host'})['host'], 'hearth')
        with self.assertRaises(cq.CqError):
            self.run_cmd('submit', {'host_id': 'hestia', 'where': 'any'})
        self.assertEqual(self.run_cmd('get', {'job': 'cq-hearth-20261107T123456-1234'})['host'], 'hearth')
        self.assertEqual(self.run_cmd('notified', {'host_id': 'hestia', 'ack_alert': True})['host'], 'hearth')
        self.assertEqual(list(self.run_cmd('status', {})['capacity']['hosts']), ['hearth'])
        self.config['hosts']['hearth']['enabled'] = False
        self.write_config()
        for name in ('hestia', 'hearth'):
            with self.assertRaises(cq.CqError): self.run_cmd('submit', {'host_id': name})
    def test_aliases_cannot_shadow_hosts_or_chain(self):
        for names in ([], {'ember': 'hearth'}, {'hearth': 'hearth'}, {'bad-name': 'hearth'},
                      {'hestia': []}, {'hestia': 'missing'}, {'hestia': 'other', 'other': 'hearth'}):
            self.config['aliases'] = names
            self.write_config()
            with self.subTest(aliases=names), self.assertRaises(cq.CqError):
                self.run_cmd('status', {})
    def test_status(self):
        out = self.run_cmd('status', {})
        self.assertEqual([r['id'] for r in out['queue']], ['ember', 'hearth'])
        self.assertEqual(out['capacity']['host']['name'], 'ember')
        self.assertEqual(out['capacity']['hosts']['hearth']['name'], 'hearth')
        self.assertEqual(out['summary']['hosts']['hearth'], 'hearth')
    def test_tick_lease_and_drain(self):
        state = self.root / 'hosts/hearth'
        state.mkdir(parents=True)
        (state / 'leader.json').write_text(json.dumps({'session': 'me', 'heartbeat': 999}), encoding='utf-8')
        result = self.run_cmd('tick', {'session': 'me'})
        self.assertFalse(result['leader'])
        self.assertEqual(self.calls[-1], ('hearth', 'apply', {'planner': 'code'}))
        self.calls.clear()
        self.config['hosts']['hearth']['enabled'] = False
        self.write_config()
        self.run_cmd('tick', {'session': 'me'})
        self.assertEqual([c[1] for c in self.calls], ['tick', 'tick'])
        self.calls.clear()
        self.config['hosts']['hearth']['enabled'] = True
        self.write_config()
        self.run_cmd('tick', {'session': 'other'})
        self.assertNotIn('apply', [c[1] for c in self.calls])
    def test_partial_notices(self):
        ids = ['cq-20261107T123456-1234', 'cq-hearth-20261107T123456-1234']
        def leaf(ctx, command, req):
            self.assertEqual(ctx.qlock_timeout, .1)
            if ctx.state == self.ctx.state:
                raise cq.LockTimeout('busy')
            return {'ok': True, 'jobs': [{'id': ids[1]}]}
        for command in ('notice_authorize', 'notified'):
            result = mh.execute(cq, self.ctx, command, {'ids': ids, 'notice_protocol': 2}, leaf)
            self.assertEqual(result['deferred_ids'], ids[:1])
            self.assertEqual(result['jobs'], [{'id': ids[1]}])
            self.assertEqual(result['acknowledged_ids'], ids[1:] if command == 'notified' else [])
            self.assertFalse(hasattr(self.ctx, 'qlock_timeout'))

    def test_tick_apply_failure_does_not_block_later_host(self):
        self.config['hosts']['later'] = {'state': 'hosts/later', 'enabled': True}
        self.write_config()
        for host in ('hearth', 'later'):
            state = self.root / 'hosts' / host
            state.mkdir(parents=True)
            (state / 'leader.json').write_text(json.dumps({'session': 'me', 'heartbeat': 999}))
        def leaf(ctx, command, req):
            host = getattr(ctx, 'host_id', None) or 'ember'
            if command == 'apply' and host == 'hearth':
                raise cq.LockTimeout('busy')
            return {'ok': True, 'leader': host != 'ember', 'can_start': True, 'host': host}
        result = mh.execute(cq, self.ctx, 'tick', {'session': 'me'}, leaf)
        self.assertFalse(result['per_host']['hearth']['ok'])
        self.assertEqual(result['per_host']['later']['apply']['host'], 'later')

    def test_list_global_filters_order_and_limit(self):
        jobs = [{'id': 'cq-20261107T123456-1234', 'status': 'done', 'top': 'A', 'rank': 2},
                {'id': 'cq-hearth-20261107T123456-1234', 'status': 'done', 'top': 'A', 'rank': 3},
                {'id': 'cq-hearth-20261107T123456-5678', 'status': 'running', 'top': 'A', 'rank': 4},
                {'id': 'cq-20261107T123456-5678', 'status': 'done', 'top': 'B', 'rank': 5}]
        with patch.object(mh, 'all_jobs', return_value=jobs), patch.object(cq, '_top_of', side_effect=lambda x: x), patch.object(cq, 'same_path', side_effect=lambda a, b: a == b), patch.object(cq, 'order_key', side_effect=lambda j: j['rank']), patch.object(cq, 'load_capacity', return_value={}), patch.object(cq, 'status_row', side_effect=lambda j, now, cap: {'id': j['id']}):
            result = self.run_cmd('list', {'status': 'done', 'root': 'A', 'limit': 1})
        self.assertEqual(result['jobs'], [{'id': jobs[1]['id']}])

    def test_shard_filesystem_failure_does_not_abort_primary_tick(self):
        def leaf(ctx, command, req):
            if getattr(ctx, 'host_id', None) == 'hearth':
                raise PermissionError('read-only shard state')
            return {'ok': True, 'leader': False, 'can_start': False}
        result = mh.execute(cq, self.ctx, 'tick', {'session': 'me'}, leaf)
        self.assertTrue(result['ok'])
        self.assertIn('read-only', result['per_host']['hearth']['error'])

    def test_watch_protocol(self):
        def leaf(ctx, command, req):
            host = getattr(ctx, 'host_id', None) or 'ember'
            return {'ok': True, 'watch': {'to_notify': [{'id': host}], 'mine_running': 1}}
        old = mh.execute(cq, self.ctx, 'watch', {}, leaf)
        new = mh.execute(cq, self.ctx, 'watch', {'notice_protocol': 2}, leaf)
        self.assertEqual([j['id'] for j in old['watch']['to_notify']], ['ember'])
        self.assertEqual([j['id'] for j in new['watch']['to_notify']], ['ember', 'hearth'])

    def test_paths(self):
        for path in ('../escape', '/absolute', 'C:/outside', 'hosts/../bad', '.', ''):
            self.config['hosts']['hearth']['state'] = path
            self.write_config()
            with self.subTest(path=path), self.assertRaises(cq.CqError): self.run_cmd('status', {})
        self.config['hosts'] = {'hearth': {'state': 'hosts/h', 'enabled': True}, 'other': {'state': 'hosts/h/nested', 'enabled': True}}
        self.write_config()
        with self.assertRaises(cq.CqError): self.run_cmd('status', {})
    def test_duplicate_ids(self):
        jid = 'cq-hearth-20261107T123456-1234'
        for path in (self.root / 'jobs', self.root / 'hosts/hearth/jobs'):
            path.mkdir(parents=True)
            (path / (jid + '.json')).write_text(json.dumps({'id': jid}), encoding='utf-8')
        with self.assertRaises(cq.CqError): mh.load_job(cq, self.ctx, jid)
        with self.assertRaises(cq.CqError): mh.all_jobs(cq, self.ctx)
        with self.assertRaises(cq.CqError): self.run_cmd('cancel', {'job': jid})

    def test_poll_reader_still_validates_settled_ids(self):
        jid = 'cq-hearth-20261107T123456-1234'
        def wrong_shard(candidate):
            return [(jid, None)] if candidate.state == self.ctx.state else []
        with self.assertRaisesRegex(cq.CqError, 'wrong queue shard'):
            mh.all_jobs(cq, self.ctx, reader=wrong_shard)
        def duplicate(candidate):
            return [('cq-20261107T123456-1234', None)]
        with self.assertRaises(cq.CqError):
            mh.all_jobs(cq, self.ctx, reader=duplicate)

    def test_poll_reader_omits_only_settled_bodies(self):
        jid = 'cq-hearth-20261107T123456-1234'
        job = {'id': jid, 'status': 'running'}
        def reader(candidate):
            return [(jid, job)] if getattr(candidate, 'host_id', None) else [('old', None)]
        self.assertEqual(mh.all_jobs(cq, self.ctx, reader=reader), [job])

if __name__ == '__main__': unittest.main()
