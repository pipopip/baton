import errno
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import host_stop


class HostStopTests(unittest.TestCase):
    def make_proc(self, root, pid, sid, job, source):
        path = Path(root) / str(pid)
        path.mkdir()
        (path / 'stat').write_bytes(f'{pid} (name with ) parenthesis) S 1 2 {sid} 0\n'.encode())
        (path / 'environ').write_bytes(b'CQ_JOB=' + os.fsencode(job) + b'\0CQ_SRC=' + os.fsencode(source) + b'\0')
        return path

    def test_only_exact_job_and_session_are_signalled_through_pidfd(self):
        with tempfile.TemporaryDirectory() as root:
            self.make_proc(root, 101, 40, 'cq-owned', '/source')
            self.make_proc(root, 102, 40, 'cq-other', '/source')
            self.make_proc(root, 103, 41, 'cq-owned', '/source')
            opened, sent, closed = [], [], []
            def bind(pid, flags):
                opened.append(pid)
                return pid + 1000
            def send(fd, sig, info, flags):
                self.assertIn(fd - 1000, opened)
                sent.append((fd, sig))
            result = host_stop._scan(40, 'cq-owned', '/source', signal.SIGTERM, proc_root=root,
                                     pidfd_open=bind, pidfd_send=send, close=closed.append)
            self.assertEqual(sent, [(1101, signal.SIGTERM)])
            self.assertEqual(result, {'matched': 1, 'sent': 1, 'unverified': 1})
            self.assertEqual(sorted(closed), [1101, 1102, 1103])

    def test_identity_change_after_pidfd_binding_cannot_signal_other_process(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.make_proc(root, 101, 40, 'cq-owned', '/source')
            sent = []
            def bind(pid, flags):
                (path / 'environ').write_bytes(b'CQ_JOB=cq-other\0CQ_SRC=/source\0')
                return 1001
            result = host_stop._scan(40, 'cq-owned', '/source', signal.SIGTERM, proc_root=root,
                                     pidfd_open=bind, pidfd_send=lambda *args: sent.append(args), close=lambda _: None)
            self.assertEqual(sent, [])
            self.assertEqual(result['unverified'], 1)

    def test_reused_pidfd_cannot_signal_new_process(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.make_proc(root, 101, 40, 'cq-other', '/source')
            attempted = []
            def bind(pid, flags):
                # The proc entry is reused after this fd was bound to the old process.
                (path / 'environ').write_bytes(b'CQ_JOB=cq-owned\0CQ_SRC=/source\0')
                return 1001
            def send(fd, sig, info, flags):
                attempted.append(fd)
                raise OSError(errno.ESRCH, 'old pidfd target exited')
            result = host_stop._scan(40, 'cq-owned', '/source', signal.SIGTERM, proc_root=root,
                                     pidfd_open=bind, pidfd_send=send, close=lambda _: None)
            self.assertEqual(attempted, [1001])
            self.assertEqual(result['sent'], 0)

    def test_missing_pidfd_support_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / 'q' / 'cq-owned'
            directory.mkdir(parents=True)
            (directory / 'pid').write_text('1234')
            (directory / 'pgid').write_text('1234')
            with mock.patch.dict(os.environ, {'HOME': root}), \
                 mock.patch.object(host_stop.os, 'pidfd_open', create=True,
                                   side_effect=OSError(errno.ENOSYS, 'not supported')), \
                 mock.patch.object(host_stop.signal, 'pidfd_send_signal', create=True) as send, \
                 mock.patch.object(host_stop, '_scan') as scan:
                self.assertFalse(host_stop.stop('cq-owned')['ok'])
                scan.assert_not_called()
                send.assert_not_called()

    def test_unconfirmed_stop_stays_reserved_and_cannot_relaunch(self):
        import cq
        for marker in ('cancel_requested', 'host_timeout_requested'):
            with self.subTest(marker=marker), tempfile.TemporaryDirectory() as root:
                transport = mock.Mock()
                ctx = cq.Ctx(state=os.path.join(root, 'state'), now=lambda: 2000,
                             run_remoterun=transport)
                job = {'id': 'cq-fixture', 'status': 'launching', 'placed_where': 'host',
                       'top': root, 't_launch': 1, 'attempts': 1, 'slots': 2, marker: True}
                cq.save_job(ctx, job)
                cq.reap_host(ctx, job, None, 2000, [])
                cq.requeue_or_fail(ctx, job['id'], 'launcher missing', transient=True)
                cq.launch_host(ctx, job)
                current = cq.load_job(ctx, job['id'])
                self.assertEqual(current['status'], 'launching')
                self.assertTrue(current[marker])
                self.assertEqual([j['id'] for j in cq.running_jobs(ctx)], [job['id']])
                transport.assert_not_called()

    def test_failed_host_cancel_request_keeps_job_active_and_ref(self):
        import cq
        with tempfile.TemporaryDirectory() as root:
            ssh = mock.Mock(return_value=(255, '', 'unavailable'))
            ctx = cq.Ctx(state=os.path.join(root, 'state'), now=lambda: 2000, ssh=ssh)
            os.makedirs(os.path.join(root, '.git'))  # a worktree marker: the cancel names its root and owner session
            cq.write_json(os.path.join(root, '.baton', 'owner.json'), {'vendor': 'claude', 'session_id': 'sess-A'})
            job = {'id': 'cq-fixture', 'status': 'running', 'placed_where': 'host',
                   'top': root, 'slots': 2, 'worktree_identity': cq.worktree_identity(root)}
            cq.save_job(ctx, job)
            with mock.patch.object(cq, 'drop_ref') as drop:
                result = cq.cmd_cancel(ctx, {'job': job['id'], 'root': root, 'session': 'sess-A'})
                drop.assert_not_called()
            self.assertEqual(result['status'], 'running')
            self.assertTrue(cq.load_job(ctx, job['id'])['cancel_requested'])
            self.assertEqual([j['id'] for j in cq.running_jobs(ctx)], [job['id']])
            ssh.assert_called_once()

    def test_unavailable_timeout_stop_stays_held_without_capture(self):
        import cq
        with tempfile.TemporaryDirectory() as root:
            ctx = cq.Ctx(state=os.path.join(root, 'state'), now=lambda: 1000)
            job = {'id': 'cq-fixture', 'status': 'running', 'placed_where': 'host',
                   'top': root, 'slots': 2, 'timeout_minutes': 1, 'gets': ['result']}
            ent = {'exit': None, 'alive': True, 'pid': 123, 'started': 800, 'launched': 800,
                   'finished': None, 'cpu': 0, 'src': True}
            cq.save_job(ctx, job)
            with mock.patch.object(cq, 'kill_running', return_value=False) as stop, \
                 mock.patch.object(cq, 'fetch_results') as fetch, mock.patch.object(cq, 'drop_ref') as drop:
                cq.reap_host(ctx, job, ent, 1000, [])
                current = cq.load_job(ctx, job['id'])
                self.assertEqual(current['status'], 'running')
                self.assertTrue(current['host_timeout_requested'])
                self.assertEqual([j['id'] for j in cq.running_jobs(ctx)], [job['id']])
                cq.reap_host(ctx, current, None, 1100, [])
                self.assertEqual(cq.load_job(ctx, job['id'])['status'], 'running')
                stop.assert_called_once()
                fetch.assert_not_called()
                drop.assert_not_called()

    @unittest.skipUnless(sys.platform.startswith('linux') and hasattr(os, 'pidfd_open') and
                         hasattr(signal, 'pidfd_send_signal'), 'Linux pidfd support required')
    def test_real_shared_session_only_matching_child_stops(self):
        with tempfile.TemporaryDirectory() as root:
            jid = 'cq-pidfd-fixture'
            directory = Path(root) / 'q' / jid
            source = directory / 'src'
            source.mkdir(parents=True)
            release = Path(root) / 'release'
            owned_ready, other_ready, owned_done = (Path(root) / p for p in ('owned-ready', 'other-ready', 'owned-done'))
            child = ('import pathlib,sys,time\npathlib.Path(sys.argv[1]).write_text("ready")\n'
                     'p=pathlib.Path(sys.argv[2])\nwhile not p.exists(): time.sleep(0.02)\n')
            wrapper = '''import os,pathlib,subprocess,sys,time
child,src,jid,one,two,done,release=sys.argv[1:]
base=dict(os.environ); base.pop('CQ_JOB',None); base.pop('CQ_SRC',None)
a=subprocess.Popen([sys.executable,'-c',child,one,release],env=dict(base,CQ_JOB=jid,CQ_SRC=src))
b=subprocess.Popen([sys.executable,'-c',child,two,release],env=dict(base,CQ_JOB='cq-unrelated',CQ_SRC=src))
pathlib.Path(done+'.pids').write_text(str(a.pid)+' '+str(b.pid))
while not pathlib.Path(release).exists():
 if a.poll() is not None: pathlib.Path(done).write_text(str(a.returncode))
 time.sleep(0.02)
a.wait(); b.wait()
'''
            env = dict(os.environ, HOME=root)
            env.pop('CQ_JOB', None)
            env.pop('CQ_SRC', None)
            proc = subprocess.Popen([sys.executable, '-c', wrapper, child, str(source), jid, str(owned_ready),
                                     str(other_ready), str(owned_done), str(release)], env=env,
                                    start_new_session=True, stdin=subprocess.DEVNULL,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            try:
                (directory / 'pid').write_text(str(proc.pid))
                (directory / 'pgid').write_text(str(proc.pid))
                deadline = time.monotonic() + 5
                while not (owned_ready.exists() and other_ready.exists()) and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(owned_ready.exists() and other_ready.exists(), 'fixture children did not start')
                with mock.patch.dict(os.environ, {'HOME': root}):
                    result = host_stop.stop(jid, grace=0)
                self.assertEqual(result['term_sent'], 1)
                self.assertFalse(result['ok'])  # unrelated and wrapper identities remain unverified
                deadline = time.monotonic() + 5
                while not owned_done.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(owned_done.exists(), 'owned child did not stop')
                _, other_pid = map(int, Path(str(owned_done) + '.pids').read_text().split())
                self.assertTrue(Path('/proc', str(other_pid)).exists(), 'unrelated child was stopped')
            finally:
                release.touch()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    fd = os.pidfd_open(proc.pid, 0)
                    try:
                        signal.pidfd_send_signal(fd, signal.SIGKILL, None, 0)
                    finally:
                        os.close(fd)
                    proc.wait(timeout=5)
                proc.stderr.close()


if __name__ == '__main__':
    unittest.main()
