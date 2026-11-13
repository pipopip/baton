"""Deferred pickup tests use isolated directories and never probe hosts."""
import argparse
import atexit
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import shutil
import subprocess
import unittest
from unittest.mock import patch

_STATE = tempfile.mkdtemp(prefix='baton-compute-pickup-tests-')
os.environ["BATON_STATE"] = _STATE
atexit.register(shutil.rmtree, _STATE, True)

from baton import compute_pickup as compute, config


class ComputeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.root = self.base / 'work'
        (self.root / '.git').mkdir(parents=True)
        self.receipts = self.root / '.baton/compute'
        self.receipts.mkdir(parents=True)
        self.state = self.base / 'state'
        (self.state / 'compute/deferred-jobs').mkdir(parents=True)
        self.patcher = patch.object(config, 'STATE_DIR', self.state)
        self.patcher.start(); self.addCleanup(self.patcher.stop)
        self.owner = self.root / '.baton/owner.json'
        self.owner.write_text(json.dumps({'session_id': 'owner', 'vendor': 'codex'}))
        self.job = dict(id='cq-example', top=str(self.root), root=str(self.root),
                        worktree_identity=compute.identity(self.root), defer='overnight',
                        deferred_registered=True, deferred_phase='queued', status='launching',
                        sha='a' * 40, notified=True)
        self.write()

    def write(self):
        (self.receipts / 'cq-example.json').write_text(json.dumps(dict(version=1, id='cq-example',
             host_id='ember', worktree_identity=compute.identity(self.root))))
        (self.state / 'compute/deferred-jobs/cq-example.json').write_text(json.dumps(self.job))

    def args(self, **kwargs):
        return argparse.Namespace(worktree=str(self.root), session='owner', refresh=False,
            reviewed='cq-example', resolution='checked', note='', json=True, **kwargs)

    def test_a_reviewed_result_is_marked_notified_so_no_notice_follows(self):
        record = self.state / 'compute/deferred-jobs/cq-example.json'
        self.job.update(status='done', exit=0, notified=False); self.write()
        with contextlib.redirect_stdout(io.StringIO()):
            compute.run(self.args())
        self.assertEqual(compute.collect(self.root)[0]['state'], 'reviewed')
        self.assertIs(json.loads(record.read_text())['notified'], True)
        # the mark is best effort: a queue that cannot be reached does not undo or fail the review
        self.job.update(notified=False); self.write()
        with patch.object(compute.config, 'INSTALL_HOME', self.base / 'nowhere'), \
                patch.dict('sys.modules', {'cq': None, 'quiet_helper': None}), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(compute.run(self.args()), 0)
        self.assertEqual(compute.collect(self.root)[0]['state'], 'reviewed')

    def test_morning_result_remains_unreviewed_even_when_notified(self):
        self.assertEqual(compute.collect(self.root)[0]['state'], 'pending')
        self.job.update(status='done', exit=0, out_paths=['a.png']); self.write()
        self.assertIn('needs-review', compute.summary(self.root))
        with contextlib.redirect_stdout(io.StringIO()):
            compute.run(self.args())
        self.assertEqual(compute.collect(self.root)[0]['state'], 'reviewed')
        self.assertEqual(compute.summary(self.root), '')
        self.job['out_paths'].append('late.mp4'); self.write()
        self.assertEqual(compute.collect(self.root)[0]['state'], 'needs-review')

    def test_a_delivered_immediate_job_needs_no_review_and_is_not_listed(self):
        self.job.update(defer='immediate', requested_defer=None, status='done', exit=0, notified=True); self.write()
        self.assertFalse((self.receipts / 'reviewed').exists())          # no ack file
        self.assertEqual(compute.collect(self.root)[0]['state'], 'reviewed')
        self.assertEqual(compute.summary(self.root), '')
        self.assertEqual(compute.prompt_summary(self.root, 'owner'), '')
        self.job.update(defer=None, status='failed', exit=1); self.write()   # a host job record has no defer at all
        self.assertEqual(compute.summary(self.root), '')

    def test_an_undelivered_immediate_job_is_listed(self):
        self.job.update(defer='immediate', requested_defer=None, status='done', exit=0, notified=False); self.write()
        self.assertEqual(compute.collect(self.root)[0]['state'], 'needs-review')
        self.assertIn('needs-review', compute.summary(self.root))
        self.assertIn('needs-review', compute.prompt_summary(self.root, 'owner'))
        self.job.update(notified=True); self.write()                     # the notice is delivered: it clears itself
        self.assertEqual(compute.summary(self.root), '')

    def test_a_running_immediate_job_is_listed_as_pending(self):
        self.job.update(defer='immediate', requested_defer=None, status='running', notified=False); self.write()
        self.assertEqual(compute.collect(self.root)[0]['state'], 'pending')
        self.assertIn('cq-example: pending', compute.summary(self.root))

    def test_a_finished_deferred_job_still_needs_the_explicit_review_even_when_notified(self):
        for defer in ('overnight', 'spare'):
            self.job.update(defer=defer, requested_defer=defer, status='done', exit=0, notified=True); self.write()
            self.assertEqual(compute.collect(self.root)[0]['state'], 'needs-review', defer)
            self.assertIn('needs-review', compute.summary(self.root), defer)
        # a fleet job that asked for a deferral keeps `requested_defer` whatever `defer` says
        self.job.update(defer='immediate', requested_defer='overnight'); self.write()
        self.assertEqual(compute.collect(self.root)[0]['state'], 'needs-review')

    def test_follow_up_still_shows_for_a_delivered_immediate_job(self):
        self.job.update(defer='immediate', requested_defer=None, status='done', exit=0, notified=True); self.write()
        with contextlib.redirect_stdout(io.StringIO()):
            compute.run(argparse.Namespace(
                worktree=str(self.root), session='owner', refresh=False, reviewed='cq-example',
                resolution='follow-up', note='check the render', json=True))
        self.assertEqual(compute.collect(self.root)[0]['state'], 'follow-up')
        self.assertIn('follow-up', compute.summary(self.root))

    def test_the_list_lead_line_asks_for_nothing_routine(self):
        self.job.update(status='done', exit=0); self.write()
        text = compute.summary(self.root)
        self.assertIn('not on every turn', text)
        self.assertNotIn('queued/delivered is not reviewed', text)

    def test_generation_two_receipts_find_only_their_isolated_job(self):
        for host, suffix, folder in [('ember', 'example', 'deferred-jobs'),
                                     ('fleet', 'fleet-example', 'fleet/jobs')]:
            jid = 'cq-v2--' + suffix
            job = dict(self.job, id=jid, status='done', exit=0, sha='b' * 40)
            target = self.state / 'compute/queue-v2' / folder
            target.mkdir(parents=True, exist_ok=True)
            (target / (jid + '.json')).write_text(json.dumps(job))
            (self.receipts / (jid + '.json')).write_text(json.dumps(dict(
                version=1, id=jid, host_id=host, worktree_identity=compute.identity(self.root))))
            wrong = self.state / 'compute' / folder
            wrong.mkdir(parents=True, exist_ok=True)
            (wrong / (jid + '.json')).write_text(json.dumps(dict(job, sha='c' * 40)))
            row = next(row for row in compute.collect(self.root) if row['id'] == jid)
            self.assertEqual(row['state'], 'needs-review')
            self.assertEqual(row['source'], 'b' * 40)

    def test_directory_incarnation_and_foreign_root_are_held(self):
        self.job['worktree_identity']['inode'] += 1; self.write()
        self.assertEqual(compute.collect(self.root)[0]['state'], 'held')
        self.job['worktree_identity'] = compute.identity(self.root)
        self.job['top'] = str(self.base / 'other'); self.write()
        self.assertEqual(compute.collect(self.root)[0]['state'], 'held')

    def test_cli_resolves_an_alias_of_the_same_directory(self):
        alias = self.base / 'alias'
        if os.name == 'nt':
            subprocess.run(['cmd.exe', '/d', '/c', 'mklink', '/J', str(alias), str(self.root)],
                           check=True, capture_output=True, creationflags=0x08000000)
            self.addCleanup(os.rmdir, alias)
        else:
            alias.symlink_to(self.root, target_is_directory=True)
            self.addCleanup(alias.unlink)
        self.job.update(top=str(alias), root=str(alias), status='done', exit=0)
        self.write()
        args = self.args()
        args.worktree = str(alias)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(compute.run(args), 0)
        self.assertEqual(compute.collect(self.root)[0]['state'], 'reviewed')

    def test_review_binds_captured_bytes_logs_and_local_file_state(self):
        artifact = self.base / 'render.png'
        artifact.write_bytes(b'first')
        self.job.update(status='done', exit=0, out_paths=[str(artifact)],
                        artifact_digests=[{'path': str(artifact), 'sha256': 'a' * 64, 'size_bytes': 5}],
                        tail_digest={'sha256': 'c' * 64})
        self.write()
        with contextlib.redirect_stdout(io.StringIO()): compute.run(self.args())
        self.assertEqual(compute.collect(self.root)[0]['state'], 'reviewed')
        self.job['artifact_digests'][0]['sha256'] = 'b' * 64
        self.write()
        self.assertEqual(compute.collect(self.root)[0]['state'], 'needs-review')
        with contextlib.redirect_stdout(io.StringIO()): compute.run(self.args())
        self.job['tail_digest']['sha256'] = 'd' * 64; self.write()
        self.assertEqual(compute.collect(self.root)[0]['state'], 'needs-review')
        with contextlib.redirect_stdout(io.StringIO()): compute.run(self.args())
        artifact.write_bytes(b'changed bytes')
        self.assertEqual(compute.collect(self.root)[0]['state'], 'needs-review')

    def test_fleet_receipt_retains_primary_identity_after_placement(self):
        jid = 'cq-fleet-test'
        target = self.state / 'compute/fleet/jobs'
        target.mkdir(parents=True)
        fleet = dict(self.job, id=jid, fleet=True, host_id='hearth', status='done', exit=0)
        (target / (jid + '.json')).write_text(json.dumps(fleet), encoding='utf-8')
        pointer = dict(version=1, id=jid, host_id='fleet', worktree_identity=compute.identity(self.root))
        (self.receipts / (jid + '.json')).write_text(json.dumps(pointer), encoding='utf-8')
        row = next(r for r in compute.collect(self.root) if r['id'] == jid)
        self.assertEqual(row['state'], 'needs-review')
        self.assertEqual(row['placed_host'], 'hearth')

    def test_handoff_can_pick_up_but_wrong_owner_cannot_ack(self):
        self.job.update(status='failed', exit=1); self.write()
        self.owner.write_text(json.dumps({'session_id': 'successor', 'vendor': 'codex'}))
        self.assertEqual(compute.collect(self.root)[0]['state'], 'needs-review')
        with self.assertRaises(ValueError): compute.run(self.args())
        self.assertEqual(compute.prompt_summary(self.root, 'owner'), '')
        first = compute.prompt_summary(self.root, 'successor')
        self.assertIn('needs-review', first)
        self.assertEqual(compute.prompt_summary(self.root, 'successor'), '')
        self.job['out_paths'] = ['result.json']; self.write()
        self.assertTrue(compute.prompt_summary(self.root, 'successor'))

    def test_follow_up_stays_visible_and_active_jobs_cannot_be_reviewed(self):
        with self.assertRaises(ValueError): compute.run(self.args())
        self.job['status'] = 'failed'; self.write()
        args = self.args(); args.resolution = 'follow-up'
        with contextlib.redirect_stdout(io.StringIO()): compute.run(args)
        self.assertIn('follow-up', compute.summary(self.root))

    def test_host_state_escape_and_missing_job_are_held(self):
        p = self.receipts / 'cq-example.json'; ptr = json.loads(p.read_text()); ptr['host_id'] = 'bad'; p.write_text(json.dumps(ptr))
        (self.state / 'compute/hosts.json').write_text(json.dumps({'hosts': {'bad': {'state': '../../escape'}}}))
        self.assertEqual(compute.collect(self.root)[0]['state'], 'held')

    def test_hook_receipt_budget_is_explicit_and_offline(self):
        for i in range(129): (self.receipts / f'cq-{i}.json').write_text('{}')
        self.assertIn('full inbox', compute.summary(self.root))

    def test_retirement_refuses_pending_results_even_with_force(self):
        from baton import worktrees
        args = argparse.Namespace(target='work', repo=str(self.base), force=True, discard_compute=False)
        with patch.object(worktrees, 'resolve_repo', return_value=self.base), \
                patch.object(worktrees, 'find_target', return_value={'path': str(self.root)}), \
                patch.object(worktrees, 'is_live', return_value=False):
            with self.assertRaisesRegex(worktrees.WtError, 'unreviewed deferred compute'):
                worktrees.cmd_retire(args)


if __name__ == '__main__': unittest.main()
