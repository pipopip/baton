"""Submission, descriptor and shared-client contracts; isolated queue fixtures."""
import base64
import json
import os
import re
from unittest import mock

from test_cq import Base
import cq
import codex_bridge
import fleet_queue


class PolicyContractTests(Base):
    def test_custom_deferred_policy_and_artifact_limits_are_pinned(self):
        policy = {'foreground_slots': 1, 'timezone': 'UTC', 'window_start': '22:30', 'window_end': '06:15'}
        with mock.patch.object(cq, '_CONF', policy):
            jid = self.submit(where='host', host_id='ember', defer='overnight', memory_max_gb=2,
                              slots=3)
        job = self.job(jid)
        self.assertNotIn('artifact_policy', job)
        job.update(host_config={**policy, 'slots': 4}, deferred_load_cap=8)
        script = cq.make_launcher(job)
        encoded = re.search(r'printf %s ([A-Za-z0-9+/=]+) \| base64 -d > "\$D/deferred.json"', script).group(1)
        descriptor = json.loads(base64.b64decode(encoded))
        self.assertEqual({k: descriptor[k] for k in policy}, policy)
        self.assertEqual(descriptor['resource_policy_version'], 2)
        self.assertIn('host helper does not support configurable scheduling policy', script)

    def test_artifact_host_jobs_use_broker_and_ordinary_v2_cannot_dispatch(self):
        import queue_generations
        calls = []
        req = dict(root=self.repo, session='sess-A', command='python3 render.py', where='host',
                   host_id='hestia', gets=['image.png'], slots=2, memory_max_gb=3)
        def leaf(ctx, command, request):
            calls.append((ctx.generation, command, request))
            return {'ok': True}
        queue_generations.execute(cq, self.ctx, 'submit', req, leaf)
        generation, _, routed = calls[0]
        self.assertEqual(generation, 2)
        self.assertEqual(routed['where'], 'fleet')
        self.assertEqual(routed['candidates'], ['hestia'])
        self.assertEqual(routed['memory_max_gb'], 3)
        self.assertNotIn('host_id', routed)
        v2 = queue_generations.context(cq, self.ctx, 2)
        with self.assertRaisesRegex(cq.CqError, 'contained fleet'):
            cq.cmd_submit(v2, req)
        # Persisted queued rows from a pre-release runtime must not bypass the
        # same fence through tick/apply/direct dispatch.
        self.assertFalse(cq.dispatch(v2, {'job': 'old-ordinary', 'where': 'host'})['ok'])
        for changes in ({'self_heavy': True}, {'temp_storage': 'ram'}, {'split': {'max_parts': 2}}, {'where': 'laptop'}):
            with self.assertRaises(cq.CqError):
                queue_generations.execute(cq, self.ctx, 'submit', {**req, **changes}, leaf)

    def test_bad_local_policy_is_not_silently_defaulted(self):
        cq.write_json(self.ctx.p('host.conf.json'), {'slots': 4, 'foreground_slots': 4})
        conf = cq.host_conf(self.ctx.state)
        self.assertIn('_policy_error', conf)
        fleet_queue.bind(cq)
        with mock.patch.object(self.ctx, 'ssh') as ssh:
            self.assertIsNone(fleet_queue._remote_probe(self.ctx, 'ember', self.ctx.state, conf))
            ssh.assert_not_called()
        with mock.patch.object(cq, '_CONF', conf), self.assertRaises(cq.CqError):
            self.submit(where='host', host_id='ember', defer='overnight', memory_max_gb=2)

    def test_documented_primary_host_identity_routes_artifacts_to_fleet(self):
        cq.write_json(self.ctx.p('host.conf.json'), {'host_id': 'ember'})
        ctx = cq.Ctx(state=self.ctx.state, sync=True)
        self.assertIsNone(ctx.host_id)
        result = cq.execute(ctx, 'submit', dict(root=self.repo, session='sess-A',
                            command='python3 render.py', where='host', gets=['image.png'], slots=1))
        self.assertTrue(result['ok'])
        self.assertTrue(result['job'].startswith('cq-v2--fleet-'))
        job = cq.execute(ctx, 'get', {'job': result['job']})['job']
        self.assertEqual(job['candidates'], ['ember'])
        self.assertEqual(job['where'], 'fleet')

    def test_artifact_output_collisions_are_refused_before_snapshot(self):
        for paths in (['a', 'A'], ['a', 'a/b'], ['x/y', 'x'], ['.'], ['x:stream'], ['nul.txt'], ['x.'], ['ok', 3],
                      ['a/../b'], ['a//b'], ['x?y'], ['x'*1025], [str(i)+'x'*1020 for i in range(9)]):
            with self.subTest(paths=paths), self.assertRaises(cq.CqError):
                self.submit(gets=paths)
        self.assertEqual(cq.all_jobs(self.ctx), [])
        self.assertEqual(cq.normalize_gets(['frames\\one.png', 'video.mp4']), ['frames/one.png', 'video.mp4'])

    def test_shared_schema_carries_the_sandbox_fields_for_claude_and_codex(self):
        schema = next(t['inputSchema'] for t in codex_bridge.TOOLS if t['name'] == 'run')
        for name in ('sandbox', 'network'):
            self.assertEqual(schema['properties'][name]['type'], 'boolean')
        request = {'root': self.repo, 'command': 'go test ./...', 'sandbox': True, 'network': False}
        codex_bridge.validate(request, schema)
        for changes in ({'sandbox': 'yes'}, {'network': 1}, {'sandbox': None}):
            with self.subTest(changes=changes), self.assertRaises(codex_bridge.Refusal):
                codex_bridge.validate({**request, **changes}, schema)

    def test_shared_schema_accepts_configured_ids_and_bounded_artifacts(self):
        schema = next(t['inputSchema'] for t in codex_bridge.TOOLS if t['name'] == 'run')
        request = {'root': self.repo, 'command': 'python3 render.py', 'where': 'fleet',
                   'candidates': ['renderfarm'], 'artifact_max_mb': 256, 'artifact_total_mb': 1024}
        codex_bridge.validate(request, schema)
        for changes in ({'candidates': ['../other']}, {'candidates': []}, {'candidates': ['a']*65},
                        {'artifact_max_mb': True}, {'artifact_total_mb': 4097}):
            with self.subTest(changes=changes), self.assertRaises(codex_bridge.Refusal):
                codex_bridge.validate({**request, **changes}, schema)
