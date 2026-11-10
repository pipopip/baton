"""Host isolation and immutable execution bindings, with no network access."""
import os
import subprocess
import unittest
from unittest import mock

from test_cq import Base, cap
import cq
import multi_host


class HostContracts(Base):
    def setUp(self):
        super().setUp()
        cq.write_json(self.ctx.p('host.conf.json'), {'name': 'ember', 'address': 'build@ember', 'slots': 4})
        self.remote = cq.Ctx(state=self.ctx.p('hosts', 'hearth'), now=self.ctx.now,
                             ssh=self.ctx.ssh, run_remoterun=self.ctx.run_remoterun,
                             spawn=self.ctx.spawn, laptop=self.laptop, sync=True)
        cq.write_json(self.remote.p('host.conf.json'), {
            'host_id': 'hearth', 'name': 'hearth', 'address': 'worker@lan', 'slots': 8,
            'slot_ram_gb': 2, 'tmp_root': '/dev/shm/cq', 'portable_runner': True,
            'caps_override': False, 'machine_id': 'a' * 32,
        })
        self.remote.host_id = 'hearth'
        cq.write_json(self.ctx.p('hosts.json'), {'version': 1, 'hosts': {
            'hearth': {'state': 'hosts/hearth', 'enabled': True}}})
        self.addCleanup(cq.refresh_host_config, os.environ['CQ_STATE'])

    def remote_job(self, **kw):
        return cq.execute(self.ctx, 'submit', {'command': 'echo test', 'root': self.repo,
                                              'session': 'sess-A', 'host_id': 'hearth', **kw})['job']

    def test_shard_submission_is_invisible_to_legacy_and_address_override(self):
        with mock.patch.dict(os.environ, {'LANE_HOST': 'wrong@ember', 'CQ_HOST': 'also@wrong'}):
            jid = self.remote_job()
            cq._execute_single(self.remote, 'dispatch', {'job': jid, 'where': 'host'})
        self.assertRegex(jid, r'^cq-hearth-\d{8}T\d{6}-[0-9a-f]{16}$')
        self.assertEqual(cq.all_jobs(self.ctx), [])
        job = multi_host.load_job(cq, self.ctx, jid)
        self.assertEqual(job['host'], 'worker@lan')
        self.assertEqual(job['host_config']['machine_id'], 'a' * 32)
        self.assertEqual(cq.execute(self.ctx, 'get', {'job': jid})['job']['id'], jid)

    def test_bound_launcher_survives_config_change(self):
        jid = self.remote_job()
        cq._execute_single(self.remote, 'dispatch', {'job': jid, 'where': 'host'})
        job = cq.load_job(self.remote, jid)
        cq.refresh_host_config(self.ctx.state)
        script = cq.make_launcher(job)
        self.assertIn('/dev/shm/cq', script)
        self.assertIn('HEAVY_SLOTS', script)
        self.assertTrue(next(line for line in script.splitlines() if 'export HEAVY_SLOTS' in line).endswith(' 8'))
        self.assertEqual(cq.memory_reservation(job), 2)

    def test_name_change_and_selector_alias_preserve_dispatched_jobs(self):
        old_id = self.remote_job()
        cq._execute_single(self.remote, 'dispatch', {'job': old_id, 'where': 'host'})
        bound = multi_host.load_job(cq, self.ctx, old_id)
        registry = cq.read_json(self.ctx.p('hosts.json'))
        registry['aliases'] = {'hestia': 'hearth'}
        cq.write_json(self.ctx.p('hosts.json'), registry)
        conf = cq.host_conf(self.remote.state)
        conf['name'] = 'hestia'
        cq.write_json(self.remote.p('host.conf.json'), conf)
        cq.refresh_host_config(self.remote.state, 'hearth')
        self.assertTrue(cq.binding_matches_current(bound))
        new_id = self.remote_job(host_id='hestia', where='host')
        cq._execute_single(self.remote, 'dispatch', {'job': new_id, 'where': 'host'})
        self.assertEqual({j['id'] for j in multi_host.all_jobs(cq, self.ctx)}, {old_id, new_id})
        self.assertEqual(cq.execute(self.ctx, 'get', {'job': old_id})['job']['host_config'], bound['host_config'])
        self.assertEqual(multi_host.load_job(cq, self.ctx, new_id)['host_config']['name'], 'hestia')
        self.assertEqual(cq.all_jobs(self.ctx), [])

    def test_split_children_never_enter_legacy_store(self):
        jid = self.remote_job(command='echo {part}', split={'max_parts': 2})
        cq._execute_single(self.remote, 'dispatch', {'job': jid, 'where': 'host', 'parts': 2})
        parent = multi_host.load_job(cq, self.ctx, jid)
        for child in parent['children']:
            self.assertEqual(multi_host.load_job(cq, self.ctx, child)['host'], 'worker@lan')
        self.assertEqual(cq.all_jobs(self.ctx), [])

    def test_invalid_shard_config_cannot_use_default_ember(self):
        cq.write_json(self.remote.p('host.conf.json'), {})
        with self.assertRaisesRegex(cq.CqError, 'explicit address'):
            self.remote_job()

    def test_remote_cannot_spend_laptop_resources(self):
        with self.assertRaises(cq.CqError):
            self.remote_job(where='any')

    def test_reused_address_is_not_proof_an_old_job_vanished(self):
        jid = self.remote_job()
        cq._execute_single(self.remote, 'dispatch', {'job': jid, 'where': 'host'})
        job = cq.load_job(self.remote, jid)
        job['status'] = 'running'
        cq.save_job(self.remote, job)
        conf = cq.host_conf(self.remote.state)
        conf['machine_id'] = 'b' * 32
        cq.write_json(self.remote.p('host.conf.json'), conf)
        cq.refresh_host_config(self.remote.state, 'hearth')
        with mock.patch.object(cq, 'host_snapshot', return_value=(None, 'identity mismatch')) as probe:
            cq.reap(self.remote, {'jobs': {}})
        self.assertEqual(probe.call_args.kwargs['binding']['machine_id'], 'a' * 32)
        current = cq.load_job(self.remote, jid)
        self.assertEqual(current['status'], 'running')
        self.assertNotIn('missing_since', current)

    def test_hearth_never_overrides_memory_floor_when_idle(self):
        cq.refresh_host_config(self.remote.state, 'hearth')
        sample = cap(free=8, mem=1, idle=3)
        job = {'where': 'host', 'slots': 1}
        self.assertFalse(cq.caps_override(job, sample))

    def test_ambiguous_launch_is_held_and_cannot_switch_hosts(self):
        jid = self.remote_job()
        self.remote.run_remoterun = lambda *args: (255, '', 'Connection reset by peer')
        cq._execute_single(self.remote, 'dispatch', {'job': jid, 'where': 'host'})
        job = cq.load_job(self.remote, jid)
        self.assertEqual(job['status'], 'launching')
        self.assertTrue(job['launch_uncertain'])
        self.advance(cq.LAUNCH_STALE + 1)
        cq.reap(self.remote, {'jobs': {}})
        self.assertEqual(cq.load_job(self.remote, jid)['status'], 'launching')
        partial = {'alive': False, 'started': None, 'launched': None, 'exit': None}
        cq.reap_host(self.remote, cq.load_job(self.remote, jid), partial, self.clock[0], [])
        self.assertEqual(cq.load_job(self.remote, jid)['status'], 'launching')
        cancelled = cq.cmd_cancel(self.remote, {'job': jid, 'root': self.repo, 'session': 'sess-A'})
        self.assertEqual(cancelled['status'], 'launching')
        self.assertTrue(cq.load_job(self.remote, jid)['cancel_requested'])
        # Even an explicit retry retains the old execution identity.
        job['status'] = 'queued'
        cq.save_job(self.remote, job)
        conf = cq.host_conf(self.remote.state)
        conf.update(address='other@new-machine', machine_id='b' * 32)
        cq.write_json(self.remote.p('host.conf.json'), conf)
        result = cq._execute_single(self.remote, 'dispatch', {'job': jid, 'where': 'host'})
        self.assertFalse(result['ok'])
        self.assertEqual(cq.load_job(self.remote, jid)['host'], 'worker@lan')

    def test_unconfirmed_launch_reconciles_running_without_replaying(self):
        jid = self.remote_job()
        cq._execute_single(self.remote, 'dispatch', {'job': jid, 'where': 'host'})
        job = cq.load_job(self.remote, jid)
        job['launch_uncertain'] = True
        job['status'] = 'launching'
        cq.save_job(self.remote, job)
        ent = {'alive': True, 'started': self.clock[0] - 10, 'launched': self.clock[0] - 11, 'exit': None}
        cq.reap_host(self.remote, job, ent, self.clock[0], [])
        current = cq.load_job(self.remote, jid)
        self.assertEqual(current['status'], 'running')
        self.assertNotIn('launch_uncertain', current)
        self.assertEqual(current['t_started'], ent['started'])
        script = cq.make_launcher(current)
        self.assertLess(script.index('trap - EXIT'), script.index('echo "CQ_LAUNCHED'))


class RouteContracts(unittest.TestCase):
    def test_only_precommand_failure_uses_backup_route_and_identity_guard(self):
        failure = subprocess.CompletedProcess([], 255, b'', b'ssh: connect to host lan port 22: refused')
        success = subprocess.CompletedProcess([], 0, b'ok', b'')
        with mock.patch.object(cq, '_run', side_effect=[failure, failure, success]) as run:
            result = cq.real_ssh('echo action', host='u@lan', binding={
                'machine_id': 'a' * 32, 'fallback_address': 'u@tail'})
        self.assertEqual(result[0], 0)
        self.assertIn('u@tail', run.call_args.args[0])
        self.assertIn('machine identity mismatch', run.call_args.args[0][-1])
        with mock.patch.object(cq, '_run', return_value=subprocess.CompletedProcess(
                [], 255, b'started', b'Connection reset by peer')) as run:
            cq.real_ssh('echo action', host='u@lan', binding={'fallback_address': 'u@tail'})
        self.assertEqual(run.call_count, 1)
