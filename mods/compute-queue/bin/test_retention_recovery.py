"""Regressions for shared-host cleanup and ordinary-host completion races."""
from pathlib import Path
import json
from unittest import mock

import cq
import queue_generations
from test_cq import Base


class RetentionRecoveryTests(Base):
    def running(self, **request):
        jid = self.submit(where='host', **request)
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host', 'slots': 1})
        cq._launch_result(self.ctx, jid, 'running', t_started=self.clock[0] - 120)
        return jid

    def test_other_generation_and_unknown_remote_sources_are_never_pruned(self):
        jid = self.running(gets=['result'])
        job = self.job(jid)
        job.update(status='done', exit=0, artifact_missing=[{'path': 'result', 'reason': 'missing'}])
        cq.save_job(self.ctx, job)
        old = self.clock[0] - 9 * 86400
        self.host.jobs[jid] = dict(exit=0, started=old, launched=old, finished=old, alive=False, src=True, pid=123)
        self.host.jobs['cq-unknown'] = dict(self.host.jobs[jid])
        queue_generations.context(cq, self.ctx, 2)
        self.host.calls.clear()
        cq.execute(self.ctx, 'reap', {})
        self.assertFalse(any('rm -rf' in script for script in self.host.calls))
        self.assertIn(jid, self.host.jobs)
        self.assertTrue(self.job(jid)['artifact_missing'])

    def test_cancellation_during_capture_wins_under_completion_lock(self):
        jid = self.running()
        stale = self.job(jid)
        ent = dict(exit=0, started=self.clock[0]-120, launched=self.clock[0]-120,
                   finished=self.clock[0], alive=False, src=True, pid=123)
        def fetch(*args, **kwargs):
            with mock.patch.object(cq, 'kill_running', return_value=True):
                result = cq.cmd_cancel(self.ctx, {'job': jid})
            self.assertEqual(result['status'], 'running')
            return [], []
        with mock.patch.object(cq, 'fetch_results', side_effect=fetch):
            cq.reap_host(self.ctx, stale, ent, self.clock[0], [])
        current = self.job(jid)
        self.assertEqual(current['status'], 'cancelled')
        self.assertNotIn('pending', current['reason'])
        self.assertEqual(json.loads(cq.read_lines(self.ctx.p('history.jsonl'), 1)[0])['status'], 'cancelled')

    def test_stopped_without_exit_can_recover_after_rechecking_empty_session(self):
        jid = self.running(gets=['result'])
        job = self.job(jid)
        job['cancel_requested'] = True
        cq.save_job(self.ctx, job)
        ent = dict(exit=None, started=self.clock[0]-120, launched=self.clock[0]-120,
                   finished=None, alive=False, src=True, pid=123)
        cq.reap_host(self.ctx, job, ent, self.clock[0], [])
        current = self.job(jid)
        self.assertEqual(current['status'], 'cancelled')
        self.assertTrue(current['host_empty_confirmed'])
        self.assertTrue(current['artifact_missing'])
        with mock.patch.object(cq, 'job_ssh', return_value=(4, '', 'session live')), \
             mock.patch.object(cq, 'fetch_results') as fetch:
            self.assertFalse(cq.refresh_host_artifacts(self.ctx, current)['ok'])
            fetch.assert_not_called()
        self.host.files[(jid, 'result')] = b'recovered'
        def stopped(ctx, job, script, timeout):
            self.assertIn('pgrep -s', script)
            self.assertNotIn('[ -f "$d/exit" ]', script)
            return 0, 'CQ_STOPPED\n', ''
        with mock.patch.object(cq, 'job_ssh', side_effect=stopped):
            result = cq.refresh_host_artifacts(self.ctx, current)
        self.assertEqual(result['remaining'], 0)
        self.assertEqual(Path(self.job(jid)['out_paths'][0]).read_bytes(), b'recovered')
