"""Fleet adapter contracts with temporary state; no host or model calls."""
import json
import base64
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cq
import codex_bridge
import multi_host


class FleetContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.ctx = cq.Ctx(state=str(self.base / 'state'))
        cq.write_json(self.ctx.p('hosts.json'), {'version': 1,
            'hosts': {'hearth': {'enabled': True, 'state': 'hosts/hearth'}},
            'aliases': {'hestia': 'hearth'}})
        self.jid = 'cq-fleet-20261110T080000-test'

    def test_one_primary_record_remains_hidden_from_legacy_planning(self):
        job = {'id': self.jid, 'fleet': True, 'defer': 'immediate', 'status': 'queued', 'host_id': None}
        cq.save_job(self.ctx, job)
        self.assertEqual(cq.load_job(self.ctx, self.jid), job)
        self.assertEqual(multi_host.load_job(cq, self.ctx, self.jid), job)
        self.assertEqual(cq.all_jobs(self.ctx), [])
        job.update(host_id='hearth', host='test@hestia', status='launching')
        cq.save_job(self.ctx, job)
        self.assertEqual(multi_host.load_job(cq, self.ctx, self.jid)['host_id'], 'hearth')
        self.assertFalse(Path(self.ctx.p('jobs', self.jid + '.json')).exists())
        self.assertFalse(Path(self.ctx.p('deferred-jobs', self.jid + '.json')).exists())

    def test_compact_status_is_cache_only_and_does_not_wake_controller(self):
        import fleet_queue
        with patch.object(cq, 'real_ssh') as ssh, patch.object(fleet_queue, 'ensure_controller') as wake:
            result = cq.execute(self.ctx, 'status', {'view': 'fleet'})
        self.assertTrue(result['ok'])
        # nothing has been probed in this fixture: the line says so instead of showing empty numbers
        self.assertEqual(result['fleet']['line'], 'no capacity data · idle')
        ssh.assert_not_called()
        wake.assert_not_called()

    def test_host_waiting_queue_is_not_an_occupied_memory_reservation(self):
        import fleet_queue
        job = {'id': self.jid, 'status': 'launching', 'host_id': 'hearth',
               'deferred_registered': True, 'deferred_phase': 'queued', 'memory_max_gb': 8}
        self.assertEqual(fleet_queue._pending([job], 'hearth', set()), [])
        job['deferred_registered'] = False
        self.assertEqual(fleet_queue._pending([job], 'hearth', set()), [job])
        job.update(host_id='laptop', deferred_registered=True)
        self.assertEqual(fleet_queue._pending([job], 'laptop', set()), [job])

    def test_codex_carries_independent_resources_and_compatibility(self):
        root = str(self.base / 'repo')
        Path(root).mkdir()
        calls = []
        owners = types.SimpleNamespace(env={}, current=lambda *a, **kw: {'root': root, 'session': 'owned'})
        b = codex_bridge.Bridge(ctx=self.ctx, owners=owners, interactive=False,
            check_policy=lambda command: None,
            engine=lambda command, req: calls.append((command, req)) or {'ok': True})
        b.call('run', dict(root=root, command='python3 scripts/check.py', where='fleet',
            platform='portable', candidates=['laptop', 'hestia'], requires=['python'],
            slots=2, memory_gb=0.5, memory_max_gb=1.0, resource_key='small-check'))
        command, req = calls[-1]
        self.assertEqual(command, 'submit')
        self.assertEqual(req['slots'], 2)
        self.assertEqual(req['memory_gb'], 0.5)
        self.assertEqual(req['candidates'], ['laptop', 'hestia'])
        self.assertEqual(req['session'], 'owned')
        cq.save_job(self.ctx, {'id': self.jid, 'fleet': True, 'root': root})
        b.call('cancel', {'root': root, 'job': self.jid})
        self.assertEqual(calls[-1], ('cancel', {'job': self.jid, 'root': root, 'session': 'owned'}))

    @unittest.skipIf(os.name == 'nt', 'remote artifact reader is Linux-only')
    def test_remote_artifacts_reject_links_and_fifos_and_quote_log_framing(self):
        home = self.base / 'home'
        job_dir = home / 'q' / self.jid
        src = job_dir / 'src'
        src.mkdir(parents=True)
        (src / 'good.bin').write_bytes(b'\0\xfffixture')
        outside = home / 'outside'; outside.mkdir()
        (outside / 'data').write_bytes(b'outside fixture')
        (src / 'link').symlink_to(outside / 'data')
        (src / 'parent-link').symlink_to(outside, target_is_directory=True)
        os.mkfifo(src / 'fifo')
        with (src / 'large').open('wb') as f: f.truncate(8 * 1024 * 1024 + 1)
        (job_dir / 'run.log').write_text('==GET bogus==\nhello\n', encoding='utf-8')
        job = dict(id=self.jid, fleet=True, gets=['good.bin', 'link', 'parent-link/data', 'fifo', 'large'])
        result = subprocess.run(['bash', '-c', cq.fetch_script(job)], env=dict(os.environ, HOME=str(home)),
                                text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        tail, data = cq.parse_fetch(result.stdout)
        self.assertEqual(base64.b64decode(data[0]), b'\0\xfffixture')
        self.assertEqual([data[i] for i in range(1, 5)], [None] * 4)
        self.assertIn('> ==GET bogus==', tail)


if __name__ == '__main__':
    unittest.main()
