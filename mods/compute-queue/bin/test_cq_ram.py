"""RAM opt-in queue lifecycle contracts; fake SSH, no host workloads."""
import json
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest import mock
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_cq import Base, cap, J
import cq
import codex_bridge


class RamQueueTests(Base):
    def setUp(self):
        super().setUp()
        patch = mock.patch.object(cq, 'RAM_JOBS', True)
        patch.start()
        self.addCleanup(patch.stop)
        self.ram = {'version': 1, 'empty': False, 'exit': 0, 'result': 'success'}
        self.ops = []
        ssh = self.ctx.ssh

        def control(script, timeout=60):
            if '.local/bin/cq-job"' in script:
                op = script.split('cq-job" ', 1)[1].split()[0]
                self.ops.append(op)
                if self.ram is None:
                    return 255, '', 'unreachable'
                return 0, json.dumps(self.ram), ''
            return ssh(script, timeout)
        self.ctx.ssh = control

    def ram_job(self, **kwargs):
        return self.submit(temp_storage='ram', memory_max_gb=1, **kwargs)

    def start_job(self, **kwargs):
        jid = self.ram_job(**kwargs)
        self.assertTrue(cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})['ok'])
        cq.launch(self.ctx, jid)
        return jid

    def test_disk_launcher_parity_and_no_concurrency_override(self):
        job = self.job(self.submit())
        launcher = cq.make_launcher(job)
        self.assertIn('setsid nohup bash', launcher)
        self.assertNotIn('cq-job', launcher)
        self.assertNotIn('GOTMPDIR', launcher)
        self.assertEqual(job['temp_storage'], 'disk')
        ram = cq.make_launcher(self.job(self.ram_job()))
        self.assertIn('export TMPDIR="${CQ_RAM_TMP:?RAM service required}"', ram)
        self.assertIn('export GOTMPDIR="$TMPDIR"', ram)
        self.assertIn('cp -a "$TOP" "$D/src"', ram)
        self.assertIn('cq-job" start', ram)
        self.assertNotIn('setsid nohup', ram)
        self.assertNotIn('GOMAXPROCS', ram)
        self.assertNotIn('rm -rf "$D"', ram)

    def test_opt_in_validation(self):
        for kw in ({'temp_storage': 'other'}, {'temp_storage': None},
                   {'temp_storage': 'ram'}, {'temp_storage': 'ram', 'memory_max_gb': True},
                   {'temp_storage': 'ram', 'memory_max_gb': float('nan')},
                   {'temp_storage': 'ram', 'memory_max_gb': 51},
                   {'temp_storage': 'ram', 'memory_max_gb': 1, 'where': 'any'},
                   {'memory_max_gb': 1},
                   {'temp_storage': 'ram', 'memory_max_gb': 1, 'split': {'max_parts': 2}}):
            with self.subTest(kw=kw), self.assertRaises(cq.CqError):
                self.submit(**kw)
        with mock.patch.object(cq, 'RAM_JOBS', False), self.assertRaisesRegex(cq.CqError, 'no disk fallback'):
            self.ram_job()

    def test_admission_combined_budget_includes_cancelling(self):
        first = self.ram_job()
        job = self.job(first)
        job['memory_max_gb'] = cq.RAM_BUDGET_GB - 0.5
        cq.save_job(self.ctx, job)
        self.assertTrue(cq.dispatch(self.ctx, {'job': first, 'where': 'host'})['ok'])
        cq.cmd_cancel(self.ctx, {'job': first})
        second = self.ram_job()
        self.assertFalse(cq.dispatch(self.ctx, {'job': second, 'where': 'host'})['ok'])
        with mock.patch.object(cq, 'RAM_JOBS', False):
            self.assertFalse(cq.dispatch(self.ctx, {'job': second, 'where': 'host'})['ok'])
        self.assertFalse(cq.dispatch(self.ctx, {'job': second, 'where': 'laptop'})['ok'])

    def test_mixed_disk_ram_budget_charges_each_job_once(self):
        jid = self.ram_job()
        job = self.job(jid)
        job['memory_max_gb'] = cq.RAM_BUDGET_GB - cq.SLOT_RAM_GB
        cq.save_job(self.ctx, job)
        self.assertTrue(cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})['ok'])
        disk = self.submit()
        self.assertTrue(cq.dispatch(self.ctx, {'job': disk, 'where': 'host'})['ok'])
        light = self.submit(slots=0)
        self.assertFalse(cq.dispatch(self.ctx, {'job': light, 'where': 'host'})['ok'])
        snapshot, _ = cq.host_snapshot(self.ctx)
        capacity = cq.probe(self.ctx, snapshot)['capacity']['host']
        self.assertEqual(capacity['memory_reserved_gb'], cq.RAM_BUDGET_GB)
        self.assertEqual(capacity['memory_budget_gb'], cq.RAM_BUDGET_GB)

    def test_terminal_ram_memory_remains_reserved_until_cleanup_acknowledgement(self):
        jid = self.ram_job()
        job = self.job(jid)
        job['memory_max_gb'] = cq.RAM_BUDGET_GB
        cq.save_job(self.ctx, job)
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        cq.launch(self.ctx, jid)
        self.ram.update(empty=True)
        control = cq.ram_control
        with mock.patch.object(cq, 'ram_control', side_effect=lambda ctx, job, op: None if op == 'cleanup' else control(ctx, job, op)):
            cq.reap(self.ctx, {'jobs': self.host.jobs})
        self.assertEqual(self.job(jid)['status'], 'done')
        self.assertFalse(self.job(jid).get('ram_cleaned', False))
        self.assertEqual(cq.reserved_memory(self.ctx), cq.RAM_BUDGET_GB)
        snapshot, _ = cq.host_snapshot(self.ctx)
        capacity = cq.probe(self.ctx, snapshot)['capacity']['host']
        self.assertEqual(capacity['free'], cq.HOST_SLOTS)  # process slots and scratch memory have separate lifetimes
        self.assertEqual(capacity['memory_reserved_gb'], cq.RAM_BUDGET_GB)
        adjusted = cq.adjust_for_launches(self.ctx, {'at': self.clock[0], 'host': {}})
        self.assertEqual(adjusted['host']['memory_reserved_gb'], cq.RAM_BUDGET_GB)
        disk, ram = self.submit(), self.ram_job()
        for next_job in (disk, ram):
            self.assertFalse(cq.dispatch(self.ctx, {'job': next_job, 'where': 'host'})['ok'])
        cq.cleanup_ram(self.ctx)
        self.assertTrue(self.job(jid)['ram_cleaned'])
        self.assertEqual(cq.reserved_memory(self.ctx), 0)
        self.assertTrue(cq.dispatch(self.ctx, {'job': disk, 'where': 'host'})['ok'])

    def test_guard_uses_declared_ram_budget_alongside_disk_slots(self):
        capacity = cap(mem=100, nproc=100)
        capacity['host'].update(memory_budget_gb=42, memory_reserved_gb=10)
        jobs = [J('ram', temp_storage='ram', memory_max_gb=31), J('disk')]
        result = cq.guard([{'job': job['id'], 'where': 'host'} for job in jobs], jobs,
                          capacity, self.clock[0])
        self.assertEqual([entry['job'] for entry in result['accepted']], ['ram'])
        self.assertIn('memory reservation', result['refused'][-1]['reason'])

    def test_slots_remain_reserved_after_session_and_lock_disappear(self):
        jid = self.start_job(slots=4)
        self.host.set_done(jid, 0)
        cq.reap(self.ctx, {'jobs': self.host.jobs})
        snapshot, _ = cq.host_snapshot(self.ctx)
        capacity = cq.probe(self.ctx, snapshot)['capacity']['host']
        self.assertEqual(capacity['free'], 0)
        next_job = self.submit()
        self.assertFalse(cq.dispatch(self.ctx, {'job': next_job, 'where': 'host'})['ok'])

    def test_cancel_before_worker_does_not_launch_or_release_early(self):
        jid = self.ram_job()
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        result = cq.cmd_cancel(self.ctx, {'job': jid})
        self.assertEqual(result['status'], 'launching')
        cq.launch(self.ctx, jid)
        self.assertEqual(self.host.launches, [])
        cq.reap(self.ctx, {'jobs': {}})
        self.assertEqual(self.job(jid)['status'], 'launching')
        self.ram.update(empty=True, cancelled=True)
        cq.reap(self.ctx, {'jobs': {}})
        self.assertEqual(self.job(jid)['status'], 'cancelled')
        self.assertTrue(self.job(jid)['ram_cleaned'])

    def test_cancel_during_launch_stops_service_and_holds_reservation(self):
        jid = self.ram_job()
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        remoterun = self.ctx.run_remoterun

        def race(argv, cwd):
            cq.cmd_cancel(self.ctx, {'job': jid})
            return remoterun(argv, cwd)
        self.ctx.run_remoterun = race
        cq.launch(self.ctx, jid)
        self.assertIn('stop', self.ops)
        self.assertNotIn(self.job(jid)['status'], cq.TERMINAL)
        self.ram.update(empty=True)
        cq.reap(self.ctx, {'jobs': {}})
        self.assertEqual(self.job(jid)['status'], 'cancelled')

    def test_detached_grandchild_and_failed_queries_do_not_settle(self):
        jid = self.start_job()
        self.host.set_done(jid, 0)
        cq.reap(self.ctx, {'jobs': self.host.jobs})
        self.assertEqual(self.job(jid)['status'], 'running')
        self.assertNotIn('cleanup', self.ops)
        self.ram = None
        cq.reap(self.ctx, None)
        self.assertEqual(self.job(jid)['status'], 'running')

    def test_bound_host_cleanup_does_not_depend_on_current_host_probe(self):
        jid = self.start_job()
        self.ram.update(empty=True)
        with mock.patch.object(cq, 'HOST', 'builder@203.0.113.99'):
            cq.reap(self.ctx, None)
        self.assertEqual(self.job(jid)['status'], 'done')
        self.assertTrue(self.job(jid)['ram_cleaned'])

    def test_exit_timeout_oom_reboot_and_persistence(self):
        for code, result, expected in ((0, 'success', 'done'), (9, 'success', 'failed'),
                                        (124, 'timeout', 'timeout'), (137, 'oom-kill', 'failed'),
                                        (255, 'reboot', 'failed')):
            with self.subTest(result=result, code=code):
                jid = self.start_job(gets=['result.txt'])
                self.host.files[jid, 'result.txt'] = b'persistent output'
                self.ram = {'version': 1, 'empty': True, 'exit': code, 'result': result,
                            'properties': {'MemoryPeak': '1234', 'CPUUsageNSec': '456'}}
                cq.reap(self.ctx, {'jobs': self.host.jobs})
                job = self.job(jid)
                self.assertEqual(job['status'], expected)
                self.assertEqual(job['ram_status']['properties']['MemoryPeak'], '1234')
                self.assertEqual(Path(job['out_paths'][0]).read_bytes(), b'persistent output')
                self.assertTrue(job['ram_cleaned'])
                self.ram = {'version': 1, 'empty': False, 'exit': 0, 'result': 'success'}

    def test_ambiguous_launch_is_stopped_before_failure(self):
        jid = self.ram_job()
        cq.dispatch(self.ctx, {'job': jid, 'where': 'host'})
        self.host.remoterun_rc = 255
        cq.launch(self.ctx, jid)
        self.assertEqual(self.job(jid)['status'], 'launching')
        self.assertIn('launch_error', self.job(jid))
        cq.reap(self.ctx, {'jobs': {}})
        self.assertIn('stop', self.ops)
        self.assertEqual(self.job(jid)['status'], 'launching')
        self.ram.update(empty=True, cancelled=True)
        cq.reap(self.ctx, {'jobs': {}})
        self.assertEqual(self.job(jid)['status'], 'failed')

    def test_killed_controller_requests_stop_without_local_launch_error(self):
        jid = self.start_job()
        self.ram.update(needs_stop=True)
        cq.reap(self.ctx, None)
        self.assertIn('stop', self.ops)
        self.assertEqual(self.job(jid)['status'], 'running')
        self.ram.update(empty=True, cancelled=True)
        cq.reap(self.ctx, None)
        self.assertEqual(self.job(jid)['status'], 'failed')
        self.assertIn('controller interrupted', self.job(jid)['reason'])

    def test_bridge_numeric_schema(self):
        for value in (0.25, 1, 8.5, 50):
            codex_bridge.validate(value, codex_bridge.RUN_PROPERTIES['memory_max_gb'])
        for value in (True, None, 0.1, 51, float('nan'), float('inf'), '1'):
            with self.assertRaises(codex_bridge.Refusal):
                codex_bridge.validate(value, codex_bridge.RUN_PROPERTIES['memory_max_gb'])


class ConfigRefreshTests(Base):
    def setUp(self):
        super().setUp()
        names = ('_CONF', 'HOST', 'HOST_NAME', 'HOST_PAUSED', 'HOST_SLOTS', 'SLOT_VCPU', 'SLOT_RAM_GB',
                 'LOAD_CAP', 'HOST_RAM_FLOOR_GB', 'PLANNER', 'RAM_JOBS', 'RAM_BUDGET_GB')
        previous = {name: getattr(cq, name) for name in names}
        self.addCleanup(lambda: [setattr(cq, name, value) for name, value in previous.items()])

    def test_long_lived_bridge_refreshes_host_pause_and_named_state(self):
        bridge = codex_bridge.Bridge(ctx=self.ctx, interactive=False)
        conf = {'address': '203.0.113.1', 'name': 'before', 'slots': 8}
        cq.write_json(self.ctx.p(cq.HOST_CONF_FILE), conf)
        bridge.engine('probe', {})
        self.assertEqual(cq.HOST, '203.0.113.1')
        old = cq.host_state(self.ctx)
        old.update(next_probe=self.clock[0] + 9999, disk_paused=True)
        cq.save_host_state(self.ctx, old)
        jid = self.submit()
        conf.update(address='203.0.113.2', name='after', paused=True, ram_jobs=True,
                    memory_budget_gb=39, slots=10, slot_ram_gb=3)
        cq.write_json(self.ctx.p(cq.HOST_CONF_FILE), conf)
        result = bridge.engine('dispatch', {'job': jid, 'where': 'host'})
        self.assertFalse(result['ok'])
        self.assertIn('paused', result['error'])
        self.assertEqual(cq.HOST, '203.0.113.2')
        self.assertEqual(cq.RAM_BUDGET_GB, 39)
        self.assertEqual(cq.HOST_SLOTS, 10)
        self.assertFalse(cq.load_capacity(self.ctx)['host']['usable'])
        bridge.engine('probe', {})
        self.assertEqual(cq.host_state(self.ctx)['address'], cq.HOST)
        self.assertNotIn('next_probe', cq.host_state(self.ctx))
        self.assertFalse(cq.host_state(self.ctx).get('disk_paused', False))
        conf['paused'] = False
        cq.write_json(self.ctx.p(cq.HOST_CONF_FILE), conf)
        result = bridge.engine('dispatch', {'job': jid, 'where': 'host'})
        self.assertTrue(result['ok'])
        self.assertEqual(self.job(jid)['host'], '203.0.113.2')

    def test_cancel_and_result_fetch_use_dispatched_host_after_switch(self):
        jid = self.submit()
        job = self.job(jid)
        job.update(placed_where='host', host='builder@203.0.113.1', status='running')
        cq.save_job(self.ctx, job)
        self.ctx.ssh = cq.real_ssh
        self.ctx.artifact_stream = None
        response = mock.Mock(returncode=0, stdout=b'==TAIL==\n==END==\n', stderr=b'')
        process = mock.Mock(stdout=io.BytesIO(b'{"tail":[]}\n{"end":0}\n'), stderr=io.BytesIO(),
                            wait=mock.Mock(return_value=0), poll=mock.Mock(return_value=0))
        with mock.patch.object(cq, 'HOST', 'builder@203.0.113.2'), \
             mock.patch.object(cq, '_run', return_value=response) as run, \
             mock.patch.object(cq.subprocess, 'Popen', return_value=process) as popen:
            cq.kill_running(self.ctx, job)
            cq.fetch_results(self.ctx, job)
            self.assertEqual(run.call_count, 1)
            self.assertEqual(popen.call_count, 1)
            for call in [*run.call_args_list, *popen.call_args_list]:
                self.assertIn('builder@203.0.113.1', call.args[0])
                self.assertNotIn('builder@203.0.113.2', call.args[0])

    def test_reaper_samples_original_host_for_dispatched_disk_job(self):
        jid = self.submit()
        job = self.job(jid)
        job.update(placed_where='host', host='builder@203.0.113.1', status='running')
        cq.save_job(self.ctx, job)
        old = {'jobs': {jid: {'exit': 0, 'started': int(self.clock[0] - 60),
                              'launched': int(self.clock[0] - 60), 'finished': int(self.clock[0]),
                              'cpu': 1, 'pid': 4242, 'alive': False, 'src': True}}}
        with mock.patch.object(cq, 'HOST', 'builder@203.0.113.2'), mock.patch.object(cq, 'host_snapshot', return_value=(old, '')) as snapshot:
            cq.reap(self.ctx, {'jobs': {}})
            snapshot.assert_called_once_with(self.ctx, host='builder@203.0.113.1', binding={})
        self.assertEqual(self.job(jid)['status'], 'done')


class TmpRootTests(Base):
    """host.conf.json tmp_root: validation, the refreshed global, the launcher's TMP_BLOCK and TMP_ENV_* and ramtmp_rm."""
    GIT_BASH = 'C:/Program Files/Git/bin/bash.exe'

    def setUp(self):
        super().setUp()
        patch = mock.patch.object(cq, 'RAM_JOBS', True)
        patch.start()
        self.addCleanup(patch.stop)
        patch = mock.patch.object(cq, 'TMP_ROOT', '')
        patch.start()
        self.addCleanup(patch.stop)

    def ram_job(self):
        return self.submit(temp_storage='ram', memory_max_gb=1)

    def test_host_conf_keeps_valid_tmp_root_and_drops_invalid(self):
        with tempfile.TemporaryDirectory() as state:
            def conf(value):
                with open(os.path.join(state, cq.HOST_CONF_FILE), 'w', encoding='utf-8') as f:
                    json.dump({'tmp_root': value}, f)
                return cq.host_conf(state)
            self.assertEqual(conf('/dev/shm/cq'), {'tmp_root': '/dev/shm/cq'})
            self.assertEqual(conf('/' + 'a' * 19), {'tmp_root': '/' + 'a' * 19})
            for value in ('relative/x', '/dev/shm/../etc', '/a b', '/' + 'a' * 20, 7):
                with self.subTest(value=value):
                    self.assertEqual(conf(value), {})

    def test_refresh_host_config_sets_tmp_root_from_file_and_clears_when_absent(self):
        names = ('_CONF', 'HOST', 'HOST_NAME', 'HOST_PAUSED', 'HOST_SLOTS', 'SLOT_VCPU', 'SLOT_RAM_GB',
                 'LOAD_CAP', 'HOST_RAM_FLOOR_GB', 'PLANNER', 'RAM_JOBS', 'RAM_BUDGET_GB', 'TMP_ROOT')
        previous = {name: getattr(cq, name) for name in names}
        self.addCleanup(lambda: [setattr(cq, name, value) for name, value in previous.items()])
        with tempfile.TemporaryDirectory() as state:
            path = os.path.join(state, cq.HOST_CONF_FILE)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({'tmp_root': '/dev/shm/cq'}, f)
            with cq.CONFIG_LOCK:
                cq.refresh_host_config(state)
            self.assertEqual(cq.TMP_ROOT, '/dev/shm/cq')
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({}, f)
            with cq.CONFIG_LOCK:
                cq.refresh_host_config(state)
            self.assertEqual(cq.TMP_ROOT, '')

    def test_disk_launcher_with_tmp_root_puts_temp_dirs_on_tmpfs(self):
        with mock.patch.object(cq, 'TMP_ROOT', '/dev/shm/cq'):
            launcher = cq.make_launcher(self.job(self.submit()))
        self.assertIn('TR=/dev/shm/cq', launcher)
        self.assertIn('mktemp -d "$TR/XXXXXXXX"', launcher)
        self.assertIn(r"printf 'export GOTMPDIR=%q\nexport CQ_RAM_TMP=%q\n'", launcher)
        self.assertIn('rm -rf "$D" "$T"', launcher)
        self.assertNotIn('@TMP_', launcher)

    def test_disk_launcher_without_tmp_root_keeps_job_tmp(self):
        with mock.patch.object(cq, 'TMP_ROOT', ''):
            launcher = cq.make_launcher(self.job(self.submit()))
        self.assertIn(r'''printf 'export TMPDIR=%q\n' "$D/tmp"''', launcher)
        for text in ('TR=', 'GOTMPDIR', '@TMP_'):
            self.assertNotIn(text, launcher)

    def test_ram_launcher_has_no_tmp_root_block(self):
        with mock.patch.object(cq, 'TMP_ROOT', '/dev/shm/cq'):
            launcher = cq.make_launcher(self.job(self.ram_job()))
        for text in ('TR=', 'CQ_RAM_TMP=%q', '@TMP_'):
            self.assertNotIn(text, launcher)
        self.assertIn('export TMPDIR="${CQ_RAM_TMP:?RAM service required}"', launcher)

    def test_length_budget_for_tmp_root_and_mktemp_suffix(self):
        suffix = '/XXXXXXXX'
        self.assertLessEqual(len('/dev/shm/cq') + len(suffix), 29)
        longest = '/' + 'a' * 19
        self.assertTrue(cq.HOST_CONF_VALID['tmp_root'](longest))
        self.assertLessEqual(len(longest) + len(suffix), 29)

    def test_snapshot_script_substitutes_slot_count_at_call_time(self):
        for slots in (14, 3):
            with self.subTest(slots=slots), mock.patch.object(cq, 'HOST_SLOTS', slots):
                script = cq.snapshot_script()
                self.assertIn(f'N={slots}', script)
                self.assertNotIn('@N@', script)

    def test_ramtmp_rm_reads_job_ramtmp_and_matches_only_root_children(self):
        with mock.patch.object(cq, 'TMP_ROOT', '/dev/shm/cq'):
            shell = cq.ramtmp_rm('cq-a')
        self.assertIn('"$HOME/q/cq-a/ramtmp"', shell)
        self.assertIn('/dev/shm/cq/' + '[A-Za-z0-9]' * 8 + ')', shell)
        with mock.patch.object(cq, 'TMP_ROOT', ''):
            self.assertEqual(cq.ramtmp_rm('cq-a'), '')

    @unittest.skipUnless(os.path.isfile(GIT_BASH), 'Git Bash is not installed')
    def test_ramtmp_rm_in_git_bash_removes_only_eight_char_roots(self):
        def posix(path):
            drive, rest = os.path.splitdrive(os.path.abspath(path).replace(os.sep, '/'))
            return f'/{drive[0].lower()}{rest}' if drive else rest
        home, root = os.path.join(self.tmp, 'home'), os.path.join(self.tmp, 'root')
        env = dict(os.environ, HOME=posix(home))
        ramtmp = os.path.join(home, 'q', 'cq-a', 'ramtmp')
        os.makedirs(os.path.dirname(ramtmp))

        def point_at(content):
            with open(ramtmp, 'w', encoding='utf-8') as f:
                f.write(content)

        def run(script):
            return subprocess.run([self.GIT_BASH, '-c', script], env=env, capture_output=True, text=True, timeout=60)

        patch = mock.patch.object(cq, 'TMP_ROOT', posix(root))
        patch.start()
        self.addCleanup(patch.stop)
        victim = os.path.join(root, 'abcdefgh')
        os.makedirs(victim)
        point_at(posix(victim))
        result = run(cq.ramtmp_rm('cq-a') + 'echo DONE')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('DONE', result.stdout)
        self.assertFalse(os.path.exists(victim))

        other = os.path.join(self.tmp, 'other', 'abcdefgh')  # an 8-character dir outside the root
        kept = [os.path.join(root, 'abcdefg'), os.path.join(root, 'abcdefghi'), os.path.join(root, 'abc.efgh'), other]
        for path in kept:
            os.makedirs(path)
        for content in ('/', *map(posix, kept), posix(root) + '/abcdefgh/..', posix(root) + '/..'):
            with self.subTest(content=content):
                point_at(content)
                # rm is shadowed to report instead of delete, so a wrong match shows as an RM line rather than a loss
                result = run('rm() { printf "RM %s\\n" "$*"; }\n' + cq.ramtmp_rm('cq-a') + 'echo DONE')
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('DONE', result.stdout)
                self.assertNotIn('RM ', result.stdout)
                self.assertTrue(all(os.path.isdir(path) for path in kept))


class GoTrimpathTests(Base):
    """host.conf.json go_trimpath: validation, the refreshed global, and the GOFLAGS line of the disk and RAM launchers."""
    GOFLAGS_LINE = r'''printf 'export GOFLAGS=%q\n' "-trimpath -count=1${GOFLAGS:+ $GOFLAGS}"'''

    def setUp(self):
        super().setUp()
        patch = mock.patch.object(cq, 'RAM_JOBS', True)
        patch.start()
        self.addCleanup(patch.stop)
        patch = mock.patch.object(cq, 'TMP_ROOT', '')
        patch.start()
        self.addCleanup(patch.stop)

    def ram_job(self):
        return self.submit(temp_storage='ram', memory_max_gb=1)

    def test_host_conf_keeps_go_trimpath_bool_and_drops_non_bool(self):
        with tempfile.TemporaryDirectory() as state:
            def conf(value):
                with open(os.path.join(state, cq.HOST_CONF_FILE), 'w', encoding='utf-8') as f:
                    json.dump({'go_trimpath': value}, f)
                return cq.host_conf(state)
            self.assertEqual(conf(True), {'go_trimpath': True})
            for value in ('yes', 1):
                with self.subTest(value=value):
                    self.assertEqual(conf(value), {})

    def test_refresh_host_config_sets_go_trimpath_from_file_and_clears_when_absent(self):
        names = ('_CONF', 'HOST', 'HOST_NAME', 'HOST_PAUSED', 'HOST_SLOTS', 'SLOT_VCPU', 'SLOT_RAM_GB',
                 'LOAD_CAP', 'HOST_RAM_FLOOR_GB', 'PLANNER', 'RAM_JOBS', 'RAM_BUDGET_GB', 'TMP_ROOT', 'GO_TRIMPATH')
        previous = {name: getattr(cq, name) for name in names}
        self.addCleanup(lambda: [setattr(cq, name, value) for name, value in previous.items()])
        with tempfile.TemporaryDirectory() as state:
            path = os.path.join(state, cq.HOST_CONF_FILE)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({'go_trimpath': True}, f)
            with cq.CONFIG_LOCK:
                cq.refresh_host_config(state)
            self.assertIs(cq.GO_TRIMPATH, True)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({}, f)
            with cq.CONFIG_LOCK:
                cq.refresh_host_config(state)
            self.assertIs(cq.GO_TRIMPATH, False)

    def test_launchers_export_goflags_trimpath_only_when_go_trimpath_is_set(self):
        for go_trimpath in (True, False):
            with self.subTest(go_trimpath=go_trimpath), mock.patch.object(cq, 'GO_TRIMPATH', go_trimpath):
                launchers = (cq.make_launcher(self.job(self.submit())), cq.make_launcher(self.job(self.ram_job())))
                for launcher in launchers:
                    if go_trimpath:
                        self.assertIn(self.GOFLAGS_LINE, launcher)
                    else:
                        self.assertNotIn('GOFLAGS', launcher)
                        self.assertNotIn('@GOFLAGS@', launcher)

    @unittest.skipUnless(os.path.isfile(TmpRootTests.GIT_BASH), 'Git Bash is not installed')
    def test_goflags_line_in_git_bash_exports_trimpath_then_appends_existing_goflags(self):
        def run(script, **extra):
            env = {k: v for k, v in os.environ.items() if k != 'GOFLAGS'}
            env.update(extra)
            return subprocess.run([TmpRootTests.GIT_BASH, '-c', script], env=env, capture_output=True, text=True, timeout=60)
        # no go on PATH: GOFLAGS only; a stub go answering `go env GOROOT` adds the GOROOT export
        line = 'set -eu\nPATH=/usr/bin:/bin\n' + cq.GOFLAGS_TRIMPATH
        result = run(line)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, 'export GOFLAGS=-trimpath\\ -count=1\n')
        result = run(line, GOFLAGS='-p=2')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, 'export GOFLAGS=-trimpath\\ -count=1\\ -p=2\n')
        result = run('set -eu\n' + result.stdout + 'echo "$GOFLAGS"\n')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '-trimpath -count=1 -p=2\n')
        with tempfile.TemporaryDirectory() as stub:
            with open(os.path.join(stub, 'go'), 'w', newline='\n') as f:
                f.write('#!/bin/sh\n[ "$1 $2" = "env GOROOT" ] && echo /opt/go1.27.2\n')
            os.chmod(os.path.join(stub, 'go'), 0o755)
            bin_dir = '/' + stub[0].lower() + stub[2:].replace('\\', '/')
            result = run(f'set -eu\nPATH={bin_dir}:/usr/bin:/bin\n' + cq.GOFLAGS_TRIMPATH)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, 'export GOFLAGS=-trimpath\\ -count=1\nexport GOROOT=/opt/go1.27.2\n')


if __name__ == '__main__':
    unittest.main()
