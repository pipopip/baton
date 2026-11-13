import base64
import io
import json
import os
import re
import shlex
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import artifact_transfer as transfer


class Sink:
    def __init__(self):
        self.files = {}
        self.pending = None

    def begin(self, index, path, size):
        self.pending = io.BytesIO()

    def write(self, data):
        self.pending.write(data)

    def commit(self, index, path, size, digest):
        self.files[path] = self.pending.getvalue()
        self.pending = None

    def abort(self):
        self.pending = None


class ArtifactTransferTests(unittest.TestCase):
    @unittest.skipUnless(os.name == 'nt', 'requires Windows DOS path aliases')
    def test_short_alias_source_and_publication_preserve_identity(self):
        import ctypes
        from ctypes import wintypes
        api = ctypes.WinDLL('kernel32', use_last_error=True).GetShortPathNameW
        api.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
        api.restype = wintypes.DWORD
        with tempfile.TemporaryDirectory(prefix='baton-artifact-long-name-') as name:
            buffer = ctypes.create_unicode_buffer(32768)
            self.assertTrue(api(name, buffer, len(buffer)))
            short = buffer.value
            if os.path.normcase(short) == os.path.normcase(name):
                self.skipTest('volume does not provide DOS short aliases')
            (Path(name) / 'source').write_bytes(b'fixture')
            file, _ = transfer.open_source_windows(short, 'source')
            with file:
                self.assertEqual(file.read(), b'fixture')
            with transfer.destination_parent(short, 'out/result') as (_, parent, leaf):
                with transfer.windows_create_temp(parent, 'temporary') as file:
                    file.write(b'captured'); file.flush()
                    transfer.windows_publish(file, parent, leaf)
            self.assertEqual((Path(name) / 'out/result').read_bytes(), b'captured')
            target = Path(name) / 'target'
            (target / 'child').mkdir(parents=True)
            (target / 'child/data').write_bytes(b'outside')
            junction = Path(name) / 'junction'
            subprocess.run(['cmd', '/c', 'mklink', '/J', str(junction), str(target)],
                           capture_output=True, check=True, creationflags=0x08000000)
            try:
                with self.assertRaises(OSError):
                    transfer.open_source_windows(str(junction / 'child'), 'data')
                with self.assertRaises(OSError):
                    with transfer.destination_parent(str(junction / 'child'), 'result'):
                        self.fail('junction ancestor was accepted')
            finally:
                os.rmdir(junction)

    def test_policy_defaults_bounds_and_persisted_validation(self):
        self.assertEqual(transfer.normalize_policy({}), {'version': 2, 'max_file_bytes': 256 * transfer.MIB,
                                                         'total_bytes': 1024 * transfer.MIB})
        for request in ({'artifact_max_mb': True}, {'artifact_total_mb': 4097},
                        {'artifact_max_mb': 4, 'artifact_total_mb': 3}, {'artifact_max_mb': 1.5}):
            with self.assertRaises(ValueError):
                transfer.normalize_policy(request)
        with self.assertRaises(ValueError):
            transfer.validate_policy({'version': 2, 'max_file_bytes': True, 'total_bytes': 5})

    def test_binary_quota_and_symlink_rejection(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root / 'binary').write_bytes(b'\x00\xff\n' * 100)
            (root / 'other').write_bytes(b'a' * 10)
            wanted = [(0, 'binary'), (1, 'other')]
            output = io.BytesIO()
            transfer.send(str(root), wanted, 300, 300, output)
            sink = Sink()
            missing = transfer.receive(io.BytesIO(output.getvalue()), wanted, 300, 300, sink)
            self.assertEqual(sink.files, {'binary': b'\x00\xff\n' * 100})
            self.assertEqual(missing, ['other'])

    @unittest.skipIf(not hasattr(transfer.os, 'O_NOFOLLOW'), 'requires openat no-follow')
    def test_posix_source_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root / 'data').write_bytes(b'fixture')
            (root / 'link').symlink_to(root / 'data')
            with self.assertRaises(OSError):
                transfer.open_source_posix(str(root), 'link')

    @unittest.skipIf(not hasattr(transfer.os, 'O_NOFOLLOW'), 'requires openat no-follow')
    def test_posix_root_may_sit_behind_a_link_but_may_not_be_one(self):
        with tempfile.TemporaryDirectory() as name:
            base = Path(name)
            (base / 'disk' / 'job' / 'src').mkdir(parents=True)
            (base / 'disk' / 'job' / 'src' / 'data').write_bytes(b'fixture')
            (base / 'state').symlink_to(base / 'disk', target_is_directory=True)        # the controller's own link, above the root
            handle, _ = transfer.open_source_posix(str(base / 'state' / 'job' / 'src'), 'data')
            handle.close()
            (base / 'elsewhere').mkdir()
            (base / 'elsewhere' / 'data').write_bytes(b'not the job')
            (base / 'disk' / 'job' / 'swapped').symlink_to(base / 'elsewhere', target_is_directory=True)   # a job put a link where its root was
            for root in (base / 'disk' / 'job' / 'swapped', base / 'state' / 'job' / 'swapped'):
                with self.assertRaises(OSError):
                    transfer.open_source_posix(str(root), 'data')

    @unittest.skipIf(not hasattr(transfer.os, 'O_NOFOLLOW'), 'requires openat no-follow')
    def test_posix_state_directory_may_itself_be_a_link_and_nothing_below_it_may(self):
        with tempfile.TemporaryDirectory() as name:
            base = Path(name)
            (base / 'disk').mkdir()
            (base / 'state').symlink_to(base / 'disk', target_is_directory=True)        # the state directory on another disk
            with transfer.destination_parent(str(base / 'state'), 'out/job/a.txt') as (_, directory, leaf):
                self.assertEqual(leaf, 'a.txt')
            self.assertTrue((base / 'disk' / 'out' / 'job').is_dir())
            (base / 'elsewhere').mkdir()
            (base / 'disk' / 'out' / 'planted').symlink_to(base / 'elsewhere', target_is_directory=True)
            with self.assertRaises(OSError):
                with transfer.destination_parent(str(base / 'state'), 'out/planted/a.txt'):
                    pass
            self.assertEqual(list((base / 'elsewhere').iterdir()), [])

    def test_rejects_truncation_and_digest_mismatch(self):
        wanted = [(0, 'a')]
        sink = Sink()
        with self.assertRaises(ValueError):
            transfer.receive(io.BytesIO(b'{"index":0,"size":10}\n123'), wanted, 10, 10, sink)
        self.assertIsNone(sink.pending)
        stream = b'{"index":0,"size":1}\nx{"stable":true,"sha256":"bad"}\n{"end":1}\n'
        self.assertEqual(transfer.receive(io.BytesIO(stream), wanted, 10, 10, sink), ['a'])
        self.assertEqual(sink.files, {})

    def test_changed_source_is_not_published(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root / 'result').write_bytes(b'original')
            opener = transfer.open_source_windows if os.name == 'nt' else transfer.open_source_posix
            class Changed:
                def __init__(self, file):
                    self.file = file
                    self.changed = False
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    self.file.close()
                def fileno(self):
                    return self.file.fileno()
                def read(self, size):
                    result = self.file.read(size)
                    if result and not self.changed:
                        self.changed = True
                        with open(root / 'result', 'ab') as writer:
                            writer.write(b'changed')
                    return result
            def changed_source(path, rel):
                file, info = opener(path, rel)
                return Changed(file), info
            wire = io.BytesIO()
            transfer.send(str(root), [(0, 'result')], 100, 100, wire, open_source=changed_source)
            sink = Sink()
            self.assertEqual(transfer.receive(io.BytesIO(wire.getvalue()), [(0, 'result')], 100, 100, sink), ['result'])
            self.assertEqual(sink.files, {})

    def test_receiver_enforces_quota_and_strict_frames(self):
        for data in (b'{"index":1,"missing":true}\n', b'{"index":0,"size":11}\n',
                     b'{"index":0,"size":1,"size":1}\n',
                     b'{' + b'x' * transfer.MAX_HEADER + b'}\n'):
            with self.assertRaises(ValueError):
                transfer.receive(io.BytesIO(data), [(0, 'a')], 10, 10, Sink())

    def test_capture_sink_records_before_publish_and_retains_failed_commit(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            source = root / 'source'
            source.mkdir()
            (source / 'result').write_bytes(b'fixture')
            wire = io.BytesIO()
            transfer.send(str(source), [(0, 'result')], 100, 100, wire)
            pending = []
            def record(value):
                pending[:] = [value]
            def clear(value):
                pending.clear()
            def failed_done(value):
                self.assertEqual(pending[0]['phase'], 'verified')
                self.assertEqual((root / 'out' / 'cq-123' / 'result').read_bytes(), b'fixture')
                raise OSError('simulated metadata failure')
            sink = transfer.CaptureSink(name, 'cq-123', record, failed_done, clear, reserve=0)
            with self.assertRaises(OSError):
                transfer.receive(io.BytesIO(wire.getvalue()), [(0, 'result')], 100, 100, sink)
            self.assertEqual(pending[0]['phase'], 'verified')
            self.assertEqual((root / 'out' / 'cq-123' / 'result').read_bytes(), b'fixture')
            adopted = []
            self.assertTrue(transfer.recover_pending(name, 'cq-123', pending[0],
                                                    lambda value: adopted.append(value), clear))
            self.assertEqual(adopted[0]['sha256'], pending[0]['sha256'])
            self.assertEqual((root / 'out' / 'cq-123' / 'result').read_bytes(), b'fixture')

    def test_queue_local_capture_retries_missing_without_overwrite(self):
        import cq
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            source = root / 'source'
            source.mkdir()
            (source / 'first').write_bytes(b'first version')
            ctx = cq.Ctx(state=name)
            job = {'id': 'cq-test', 'gets': ['first', 'later'], 'defer': None,
                   'artifact_policy': transfer.normalize_policy({})}
            cq.save_job(ctx, job)
            paths, digests, missing = cq.capture_local_job(ctx, job, str(source))
            self.assertEqual([d['path'] for d in digests], ['first'])
            self.assertEqual([m['path'] for m in missing], ['later'])
            (source / 'first').write_bytes(b'changed')
            (source / 'later').write_bytes(b'recovered')
            paths, digests, missing = cq.capture_local_job(ctx, cq.load_job(ctx, job['id']), str(source))
            self.assertEqual(missing, [])
            self.assertEqual([d['path'] for d in digests], ['first', 'later'])
            self.assertEqual(Path(paths[0]).read_bytes(), b'first version')
            self.assertEqual(Path(paths[1]).read_bytes(), b'recovered')

    def test_recovery_cleans_staging_and_rejects_conflicting_final(self):
        with tempfile.TemporaryDirectory() as name:
            directory = Path(name) / 'out' / 'cq-123'
            directory.mkdir(parents=True)
            temp = '.cq-artifact-' + 'a' * 32
            (directory / temp).write_bytes(b'partial')
            pending = {'path': 'result', 'temp': temp, 'size_bytes': 7, 'phase': 'staging'}
            cleared = []
            self.assertTrue(transfer.recover_pending(name, 'cq-123', pending,
                                                    lambda _: self.fail('staging cannot be adopted'), cleared.append))
            self.assertFalse((directory / temp).exists())
            self.assertEqual(cleared, [pending])
            (directory / 'result').write_bytes(b'other')
            pending = {'path': 'result', 'temp': temp, 'size_bytes': 5, 'phase': 'verified',
                       'sha256': '0' * 64}
            self.assertFalse(transfer.recover_pending(name, 'cq-123', pending,
                                                     lambda _: self.fail('conflict cannot be adopted'),
                                                     lambda _: self.fail('conflict cannot be cleared')))
            self.assertEqual((directory / 'result').read_bytes(), b'other')

    def test_queue_host_fetch_stream_and_resume(self):
        import cq
        class Process:
            def __init__(self, data):
                self.stdout = io.BytesIO(data)
                self.stderr = io.BytesIO(b'')
                self.returncode = None
            def wait(self):
                self.returncode = 0
                return 0
            def poll(self):
                return self.returncode
            def kill(self):
                self.returncode = -1
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            source = root / 'source'
            source.mkdir()
            (source / 'first').write_bytes(b'binary\x00\xff')
            requests = []
            def stream(job, script, timeout):
                args = shlex.split(script)
                self.assertEqual(args[:2], ['python3', '-c'])
                compile(args[2], '<remote artifact sender>', 'exec')
                self.assertLess(len(script), 30000)
                requests.append(script)
                wire = io.BytesIO()
                wire.write(b'{"tail":["completed"]}\n')
                wanted = [(i, rel) for i, rel in enumerate(job['gets'])
                          if rel not in {d['path'] for d in job.get('artifact_digests', [])}]
                transfer.send(str(source), wanted, 100, 100, wire)
                return Process(wire.getvalue())
            ctx = cq.Ctx(state=name, artifact_stream=stream)
            job = {'id': 'cq-test', 'gets': ['first', 'later'], 'defer': None,
                   'artifact_policy': {'version': 2, 'max_file_bytes': 100, 'total_bytes': 100}}
            cq.save_job(ctx, job)
            tail, paths = cq.fetch_results(ctx, job)
            self.assertEqual(tail, ['completed'])
            self.assertEqual(Path(paths[0]).read_bytes(), b'binary\x00\xff')
            (source / 'first').write_bytes(b'changed')
            (source / 'later').write_bytes(b'recovered')
            tail, paths = cq.fetch_results(ctx, cq.load_job(ctx, job['id']))
            self.assertEqual(Path(paths[0]).read_bytes(), b'binary\x00\xff')
            self.assertEqual(Path(paths[1]).read_bytes(), b'recovered')
            self.assertEqual(len(requests), 2)

    def test_queue_quota_persists_across_local_retries(self):
        import cq
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            source = root / 'source'
            source.mkdir()
            (source / 'first').write_bytes(b'a' * 7)
            (source / 'second').write_bytes(b'b' * 4)
            ctx = cq.Ctx(state=name)
            job = {'id': 'cq-test', 'gets': ['first', 'second'], 'defer': None,
                   'artifact_policy': {'version': 2, 'max_file_bytes': 10, 'total_bytes': 10}}
            cq.save_job(ctx, job)
            _, digests, missing = cq.capture_local_job(ctx, job, str(source))
            self.assertEqual([d['path'] for d in digests], ['first'])
            self.assertEqual([m['path'] for m in missing], ['second'])
            _, digests, missing = cq.capture_local_job(ctx, cq.load_job(ctx, job['id']), str(source))
            self.assertEqual([d['path'] for d in digests], ['first'])
            self.assertEqual([m['path'] for m in missing], ['second'])

    def test_host_stream_fallback_only_after_preconnection_failure(self):
        import cq
        from unittest import mock
        class Process:
            def __init__(self, output, error, code):
                self.stdout, self.stderr, self.code = io.BytesIO(output), io.BytesIO(error), code
            def poll(self):
                return self.code
            def wait(self, timeout=None):
                return self.code
            def kill(self):
                self.code = -1
        with tempfile.TemporaryDirectory() as name:
            ctx = cq.Ctx(state=name)
            job = {'id': 'cq-test', 'gets': [], 'defer': None, 'host': 'primary',
                   'host_config': {'fallback_address': 'fallback'}}
            cq.save_job(ctx, job)
            failed = Process(b'', b'ssh: connect to host primary failed', 255)
            succeeded = Process(b'{"tail":[]}\n{"end":0}\n', b'', 0)
            with mock.patch.object(cq.subprocess, 'Popen', side_effect=[failed, succeeded]) as popen:
                self.assertEqual(cq.fetch_results(ctx, job), ([], []))
                self.assertIn('primary', popen.call_args_list[0].args[0])
                self.assertIn('fallback', popen.call_args_list[1].args[0])
            failed = Process(b'partial', b'ssh: connect to host primary failed', 255)
            with mock.patch.object(cq.subprocess, 'Popen', return_value=failed) as popen:
                self.assertIsNone(cq.fetch_results(ctx, job))
                self.assertEqual(popen.call_count, 1)

    def test_host_refresh_requires_confirmed_shutdown(self):
        import cq
        from unittest import mock
        with tempfile.TemporaryDirectory() as name:
            ctx = cq.Ctx(state=name)
            job = {'id': 'cq-test', 'gets': ['result'], 'defer': None, 'host': 'primary',
                   'status': 'done', 'artifact_missing': [{'path': 'result'}]}
            cq.save_job(ctx, job)
            with mock.patch.object(cq, 'job_ssh', return_value=(4, '', '')), \
                 mock.patch.object(cq, 'fetch_results') as fetch:
                result = cq.refresh_host_artifacts(ctx, job)
                self.assertFalse(result['ok'])
                fetch.assert_not_called()

    def test_exited_host_runner_stops_descendants_before_capture(self):
        import cq
        from unittest import mock
        for cancelled in (False, True):
            with self.subTest(cancelled=cancelled), tempfile.TemporaryDirectory() as name:
                ctx = cq.Ctx(state=name, now=lambda: 1000)
                job = {'id': 'cq-test', 'status': 'running', 'placed_where': 'host', 'defer': None,
                       'gets': [], 'timeout_minutes': 1, 'command': 'build', 'workstream': 'test',
                       'est_minutes': 1, 'slots': 1, 'top': name, 'cancel_requested': cancelled}
                cq.save_job(ctx, job)
                started = 990 if cancelled else 800
                ent = {'exit': 0, 'alive': True, 'pid': 123, 'started': started, 'launched': started,
                       'finished': started + 1, 'cpu': 0, 'src': True}
                ended = []
                with mock.patch.object(cq, 'kill_running', return_value=True) as kill, \
                     mock.patch.object(cq, 'fetch_results', return_value=([], [])) as fetch, \
                     mock.patch.object(cq, 'drop_ref'):
                    cq.reap_host(ctx, job, ent, 1000, ended)
                    kill.assert_called_once()
                    fetch.assert_not_called()
                    self.assertEqual(cq.load_job(ctx, job['id'])['status'], 'running')
                    self.assertEqual(ended, [])
                    ent['alive'] = False
                    cq.reap_host(ctx, cq.load_job(ctx, job['id']), ent, 1001, ended)
                    self.assertEqual(cq.load_job(ctx, job['id'])['status'], 'cancelled' if cancelled else 'timeout')
                    self.assertEqual(ended, [job['id']])
                    fetch.assert_called_once()

    def test_exited_host_without_session_receipt_never_captures(self):
        import cq
        from unittest import mock
        with tempfile.TemporaryDirectory() as name:
            ctx = cq.Ctx(state=name, now=lambda: 1000)
            job = {'id': 'cq-test', 'status': 'running', 'placed_where': 'host', 'defer': None,
                   'gets': ['result'], 'timeout_minutes': 1}
            cq.save_job(ctx, job)
            ent = {'exit': 0, 'alive': False, 'pid': None, 'started': 800, 'launched': 800,
                   'finished': 810, 'cpu': 0, 'src': True}
            with mock.patch.object(cq, 'kill_running', return_value=True) as kill, \
                 mock.patch.object(cq, 'fetch_results') as fetch:
                cq.reap_host(ctx, job, ent, 1000, [])
                kill.assert_called_once()
                fetch.assert_not_called()
            self.assertEqual(cq.load_job(ctx, job['id'])['status'], 'running')

    def test_legacy_host_refresh_preserves_old_output_and_counts_quota(self):
        import cq
        from unittest import mock
        class Process:
            def __init__(self, data):
                self.stdout, self.stderr, self.code = io.BytesIO(data), io.BytesIO(), None
            def poll(self):
                return self.code
            def wait(self, timeout=None):
                self.code = 0
                return 0
            def kill(self):
                self.code = -1
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            source, out = root / 'source', root / 'out' / 'cq-test'
            source.mkdir()
            out.mkdir(parents=True)
            (out / 'first').write_bytes(b'1234567')
            (source / 'first').write_bytes(b'do not overwrite')
            (source / 'later').write_bytes(b'four')
            requested = []
            def stream(job, script, timeout):
                program = shlex.split(script)[-1]
                match = re.search(r"jid,wanted,maximum,remaining=json.loads\(base64.b64decode\('([^']+)'\)\)", program)
                _, wanted, maximum, remaining = json.loads(base64.b64decode(match.group(1)))
                requested.append((wanted, remaining))
                wire = io.BytesIO(b'{"tail":[]}\n')
                wire.seek(0, 2)
                transfer.send(str(source), wanted, maximum, remaining, wire)
                return Process(wire.getvalue())
            ctx = cq.Ctx(state=name, artifact_stream=stream)
            job = {'id': 'cq-test', 'gets': ['first', 'later'], 'defer': None, 'host': 'primary',
                   'status': 'done', 'out_paths': [str(out / 'first')],
                   'artifact_missing': [{'path': 'later'}], 'top': name}
            cq.save_job(ctx, job)
            with mock.patch.object(cq, 'job_ssh', return_value=(0, 'CQ_STOPPED\n', '')), \
                 mock.patch.object(cq, 'drop_ref'), mock.patch.object(transfer, 'DEFAULT_TOTAL', 10):
                result = cq.refresh_host_artifacts(ctx, job)
                self.assertEqual(result['remaining'], 1)
                self.assertEqual(requested[-1], ([[1, 'later']], 3))
                self.assertEqual(cq.load_job(ctx, job['id'])['artifact_missing'][0]['path'], 'later')
                self.assertEqual((out / 'first').read_bytes(), b'1234567')
                (source / 'later').write_bytes(b'new')
                result = cq.refresh_host_artifacts(ctx, cq.load_job(ctx, job['id']))
            self.assertEqual(result['remaining'], 0)
            self.assertEqual((out / 'first').read_bytes(), b'1234567')
            self.assertEqual((out / 'later').read_bytes(), b'new')
            self.assertEqual([d['path'] for d in cq.load_job(ctx, job['id'])['artifact_digests']], ['later'])

    def test_legacy_host_refresh_refuses_unverifiable_old_output(self):
        import cq
        from unittest import mock
        with tempfile.TemporaryDirectory() as name:
            ctx = cq.Ctx(state=name, artifact_stream=mock.Mock())
            job = {'id': 'cq-test', 'gets': ['first', 'later'], 'defer': None, 'host': 'primary',
                   'status': 'done', 'out_paths': [ctx.p('out', 'cq-test', 'first')],
                   'artifact_missing': [{'path': 'later'}]}
            cq.save_job(ctx, job)
            with mock.patch.object(cq, 'job_ssh', return_value=(0, 'CQ_STOPPED\n', '')):
                result = cq.refresh_host_artifacts(ctx, job)
            self.assertFalse(result['ok'])
            self.assertEqual(cq.load_job(ctx, job['id'])['artifact_missing'], [{'path': 'later'}])
            ctx.artifact_stream.assert_not_called()

    @unittest.skipUnless(os.name == 'nt', 'Windows command-line limit')
    def test_oversized_ssh_command_reports_missing_and_retains_source(self):
        import cq
        from unittest import mock
        with tempfile.TemporaryDirectory() as name:
            stream = mock.Mock(side_effect=AssertionError('must not launch oversized command'))
            ctx = cq.Ctx(state=name, now=lambda: 1000, artifact_stream=stream)
            job = {'id': 'cq-test', 'gets': ['result'], 'defer': None, 'host': 'primary',
                   'status': 'running', 'command': 'build', 'workstream': 'test', 'est_minutes': 1,
                   'slots': 1, 'timeout_minutes': 5, 'top': name}
            cq.save_job(ctx, job)
            ent = {'exit': 0, 'pid': 123, 'alive': False, 'src': True, 'started': 990,
                   'launched': 990, 'finished': 1000, 'cpu': 1}
            with mock.patch.object(cq.subprocess, 'list2cmdline', return_value='x' * 30000):
                cq.reap_host(ctx, job, ent, 1000, [])
            done = cq.load_job(ctx, job['id'])
            self.assertEqual(done['status'], 'done')
            self.assertEqual(done['artifact_missing'][0]['reason'], cq.ARTIFACT_COMMAND_ERROR)
            stream.assert_not_called()
            with mock.patch.object(cq, 'job_ssh') as ssh:
                cq.cleanup_host(ctx, {'jobs': {job['id']: ent}}, 1001)
                ssh.assert_not_called()

    def test_pipe_reader_stops_after_parent_exit_with_inherited_pipe(self):
        import cq
        child = ('import subprocess,sys,os; '
                 'subprocess.Popen([sys.executable,"-c","import time;time.sleep(2)"],'
                 'stdout=sys.stdout,stderr=sys.stderr,close_fds=False,'
                 'creationflags=0x08000000 if os.name=="nt" else 0); '
                 'print("ready",flush=True)')
        options = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE, 'stdin': subprocess.DEVNULL}
        if os.name == 'nt':
            options['creationflags'] = 0x08000000
        process = subprocess.Popen([sys.executable, '-c', child], **options)
        try:
            self.assertEqual(process.wait(timeout=5), 0)
            reader = cq._DeadlinePipeReader(process, 5)
            self.assertEqual(reader.readline(100).rstrip(b'\r\n'), b'ready')
            if os.name == 'nt':
                self.assertEqual(reader._available_windows(reader.out_fd), 0)
            start = time.monotonic()
            self.assertEqual(reader.readline(100), b'')
            self.assertLess(time.monotonic() - start, 1.0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            process.stdout.close()
            process.stderr.close()

    def test_pipe_reader_deadline_with_live_silent_process(self):
        import cq
        options = {'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE, 'stdin': subprocess.DEVNULL}
        if os.name == 'nt':
            options['creationflags'] = 0x08000000
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(5)'], **options)
        try:
            reader = cq._DeadlinePipeReader(process, 0.2)
            start = time.monotonic()
            with self.assertRaises(TimeoutError):
                reader.readline(100)
            self.assertLess(time.monotonic() - start, 1.0)
        finally:
            process.kill()
            process.wait(timeout=5)
            process.stdout.close()
            process.stderr.close()

    def test_host_fetch_returns_with_inherited_helper_pipes(self):
        import cq
        from unittest import mock
        real_popen = subprocess.Popen
        child = ('import subprocess,sys,os; '
                 'subprocess.Popen([sys.executable,"-c","import time;time.sleep(2)"],'
                 'stdout=sys.stdout,stderr=sys.stderr,close_fds=False,'
                 'creationflags=0x08000000 if os.name=="nt" else 0); '
                 'print("invalid frame",flush=True)')
        def launch(*args, **kwargs):
            kwargs.pop('creationflags', None)
            if os.name == 'nt':
                kwargs['creationflags'] = 0x08000000
            return real_popen([sys.executable, '-c', child], **kwargs)
        with tempfile.TemporaryDirectory() as name:
            ctx = cq.Ctx(state=name)
            job = {'id': 'cq-test', 'gets': [], 'defer': None, 'host': 'primary'}
            cq.save_job(ctx, job)
            with mock.patch.object(cq.subprocess, 'Popen', side_effect=launch):
                start = time.monotonic()
                self.assertIsNone(cq.fetch_results(ctx, job))
                self.assertLess(time.monotonic() - start, 1.0)

    def test_host_fetch_drains_large_stderr_without_buffering_it(self):
        import cq
        from unittest import mock
        real_popen = subprocess.Popen
        child = ('import os,sys; '
                 'os.write(2,b"x"*200000); '
                 'sys.stdout.buffer.write(b"{\\"tail\\":[]}\\n{\\"end\\":0}\\n"); '
                 'sys.stdout.buffer.flush()')
        def launch(*args, **kwargs):
            kwargs.pop('creationflags', None)
            if os.name == 'nt':
                kwargs['creationflags'] = 0x08000000
            return real_popen([sys.executable, '-c', child], **kwargs)
        with tempfile.TemporaryDirectory() as name:
            ctx = cq.Ctx(state=name)
            job = {'id': 'cq-test', 'gets': [], 'defer': None, 'host': 'primary'}
            cq.save_job(ctx, job)
            with mock.patch.object(cq.subprocess, 'Popen', side_effect=launch):
                self.assertEqual(cq.fetch_results(ctx, job), ([], []))

    @unittest.skipUnless(os.name == 'nt', 'requires Windows handle APIs')
    def test_windows_source_handle_and_invalid_names(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root / 'dir').mkdir()
            (root / 'dir' / 'file').write_bytes(b'fixture')
            source, info = transfer.open_source_windows(name, 'dir/file')
            with source:
                self.assertEqual(source.read(), b'fixture')
            for bad in ('../file', 'dir/../file', 'dir/file:stream', 'NUL', 'dir/file.'):
                with self.assertRaises(ValueError):
                    transfer.open_source_windows(name, bad)
            link = subprocess.run(['cmd', '/c', 'mklink', '/J', str(root / 'linked'), str(root / 'dir')],
                                  capture_output=True)
            if link.returncode:
                self.fail('could not create test junction')
            with self.assertRaises(OSError):
                transfer.open_source_windows(name, 'linked/file')
            with self.assertRaises(OSError):
                transfer.open_source_windows(str(root / 'linked'), 'file')

    @unittest.skipUnless(os.name == 'nt', 'requires Windows handle APIs')
    def test_windows_pinned_destination_blocks_rename_and_junction_swap(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name) / 'queue'
            root.mkdir()
            outside = Path(name) / 'outside'
            outside.mkdir()
            sentinel = outside / 'result'
            sentinel.write_bytes(b'untouched')
            with transfer.destination_parent(str(root), 'out/job/result') as (parent, fd, leaf):
                self.assertIsInstance(fd, int)
                self.assertEqual(leaf, 'result')
                for directory in (root, root / 'out', root / 'out' / 'job'):
                    with self.assertRaises(OSError):
                        directory.rename(directory.with_name(directory.name + '-moved'))
                    with self.assertRaises(OSError):
                        directory.rmdir()
                link = subprocess.run(['cmd', '/c', 'mklink', '/J', parent, str(outside)], capture_output=True)
                self.assertNotEqual(link.returncode, 0)
                (Path(parent) / leaf).write_bytes(b'captured')
            self.assertEqual(sentinel.read_bytes(), b'untouched')
            self.assertEqual((root / 'out' / 'job' / 'result').read_bytes(), b'captured')

    @unittest.skipUnless(os.name == 'nt', 'requires Windows handle APIs')
    def test_windows_destination_allows_existing_directory_writer(self):
        import ctypes
        from ctypes import wintypes
        api = ctypes.WinDLL('kernel32', use_last_error=True)
        create = api.CreateFileW
        create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                           wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        create.restype = wintypes.HANDLE
        close = api.CloseHandle
        close.argtypes = [wintypes.HANDLE]
        with tempfile.TemporaryDirectory() as name:
            handle = create(name, 0x40000000, 7, None, 3, 0x02000000, None)
            self.assertNotEqual(handle, ctypes.c_void_p(-1).value)
            try:
                with transfer.destination_parent(name, 'result') as (_, parent, leaf):
                    with transfer.windows_create_temp(parent, 'capture.tmp') as temp:
                        temp.write(b'fixture')
                        transfer.windows_publish(temp, parent, leaf)
                self.assertEqual((Path(name) / 'result').read_bytes(), b'fixture')
                with transfer.destination_parent(name, '😀😀😀') as (_, parent, leaf):
                    with transfer.windows_create_temp(parent, 'capture.tmp') as temp:
                        temp.write(b'unicode')
                        transfer.windows_publish(temp, parent, leaf)
                self.assertEqual((Path(name) / '😀😀😀').read_bytes(), b'unicode')
            finally:
                close(handle)

    @unittest.skipUnless(os.name == 'nt', 'requires Windows handle APIs')
    def test_windows_relative_create_refuses_retagged_parent(self):
        import ctypes
        from ctypes import wintypes
        api = ctypes.WinDLL('kernel32', use_last_error=True)
        create = api.CreateFileW
        create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                           wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        create.restype = wintypes.HANDLE
        close = api.CloseHandle
        close.argtypes = [wintypes.HANDLE]
        ioctl = api.DeviceIoControl
        ioctl.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD,
                          wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID]
        ioctl.restype = wintypes.BOOL
        with tempfile.TemporaryDirectory() as name:
            root = Path(name) / 'root'
            root.mkdir()
            outside = Path(name) / 'outside'
            outside.mkdir()
            sentinel = outside / 'result'
            sentinel.write_bytes(b'untouched')
            writer = create(str(root), 0x100, 7, None, 3, 0x02200000, None)
            self.assertNotEqual(writer, ctypes.c_void_p(-1).value)
            try:
                with transfer.destination_parent(str(root), 'result') as (_, parent, _):
                    substitute = ('\\??\\' + str(outside)).encode('utf-16le')
                    display = str(outside).encode('utf-16le')
                    path = substitute + b'\0\0' + display + b'\0\0'
                    payload = struct.pack('<HHHH', 0, len(substitute), len(substitute) + 2, len(display)) + path
                    data = struct.pack('<IHH', 0xA0000003, len(payload), 0) + payload
                    buffer = ctypes.create_string_buffer(data)
                    returned = wintypes.DWORD()
                    self.assertTrue(ioctl(writer, 0x900A4, buffer, len(data), None, 0,
                                          ctypes.byref(returned), None))
                    with self.assertRaises(OSError):
                        transfer.windows_create_temp(parent, 'temp')
                self.assertEqual(sentinel.read_bytes(), b'untouched')
            finally:
                close(writer)

    @unittest.skipUnless(os.name == 'nt', 'requires Windows handle APIs')
    def test_windows_relative_publication_above_old_limit(self):
        import ctypes
        from ctypes import wintypes
        api = ctypes.WinDLL('kernel32', use_last_error=True)
        create = api.CreateFileW
        create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                           wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        create.restype = wintypes.HANDLE
        close = api.CloseHandle
        close.argtypes = [wintypes.HANDLE]
        class WindowsSink:
            def __init__(self, parent):
                self.parent = parent
                self.file = None
            def begin(self, index, path, size):
                self.file = transfer.windows_create_temp(self.parent, 'capture.tmp')
            def write(self, data):
                self.file.write(data)
            def commit(self, index, path, size, digest):
                transfer.windows_publish(self.file, self.parent, path)
                self.file.close()
                self.file = None
            def abort(self):
                if self.file is not None:
                    transfer.windows_discard(self.file)
                    self.file.close()
                    self.file = None
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            source, dest = root / 'source', root / 'dest'
            source.mkdir()
            dest.mkdir()
            data = b'\x00\xffdata' * (transfer.LEGACY_FILE // 6 + 1)
            self.assertGreater(len(data), transfer.LEGACY_FILE)
            (source / 'result').write_bytes(data)
            wire = io.BytesIO()
            transfer.send(str(source), [(0, 'result')], len(data), len(data), wire)
            handle = create(str(dest), 0x80000000, 3, None, 3, 0x02200000, None)
            self.assertNotEqual(handle, ctypes.c_void_p(-1).value)
            try:
                self.assertEqual(transfer.receive(io.BytesIO(wire.getvalue()), [(0, 'result')],
                                                  len(data), len(data), WindowsSink(handle)), [])
                self.assertEqual((dest / 'result').read_bytes(), data)
                # The same file must never be replaced on a repeated capture.
                with self.assertRaises(FileExistsError):
                    transfer.receive(io.BytesIO(wire.getvalue()), [(0, 'result')], len(data),
                                     len(data), WindowsSink(handle))
                self.assertEqual((dest / 'result').read_bytes(), data)
            finally:
                close(handle)

    @unittest.skipUnless(os.name == 'nt', 'requires Windows handle APIs')
    def test_windows_retag_after_temp_creation_fails(self):
        import ctypes
        from ctypes import wintypes
        api = ctypes.WinDLL('kernel32', use_last_error=True)
        create = api.CreateFileW
        create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                           wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        create.restype = wintypes.HANDLE
        close = api.CloseHandle
        close.argtypes = [wintypes.HANDLE]
        ioctl = api.DeviceIoControl
        ioctl.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD,
                          wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID]
        ioctl.restype = wintypes.BOOL
        with tempfile.TemporaryDirectory() as name:
            root, outside = Path(name) / 'root', Path(name) / 'outside'
            root.mkdir()
            outside.mkdir()
            sentinel = outside / 'result'
            sentinel.write_bytes(b'untouched')
            writer = create(str(root), 0x100, 7, None, 3, 0x02200000, None)
            self.assertNotEqual(writer, ctypes.c_void_p(-1).value)
            try:
                with transfer.destination_parent(str(root), 'result') as (_, parent, leaf):
                    with transfer.windows_create_temp(parent, 'capture.tmp') as temp:
                        temp.write(b'captured')
                        substitute = ('\\??\\' + str(outside)).encode('utf-16le')
                        display = str(outside).encode('utf-16le')
                        path = substitute + b'\0\0' + display + b'\0\0'
                        payload = struct.pack('<HHHH', 0, len(substitute), len(substitute) + 2, len(display)) + path
                        data = struct.pack('<IHH', 0xA0000003, len(payload), 0) + payload
                        buffer = ctypes.create_string_buffer(data)
                        returned = wintypes.DWORD()
                        self.assertFalse(ioctl(writer, 0x900A4, buffer, len(data), None, 0,
                                               ctypes.byref(returned), None))
                        transfer.windows_publish(temp, parent, leaf)
                self.assertEqual(sentinel.read_bytes(), b'untouched')
                self.assertEqual((root / 'result').read_bytes(), b'captured')
            finally:
                close(writer)


if __name__ == '__main__':
    unittest.main()
