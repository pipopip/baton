import contextlib
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

HERE = Path(__file__).parent
# Allow policy/state tests to run on the development Windows machine. Real flock
# tests below run only on Linux and never use this stub.
try:
    import fcntl  # noqa: F401
except ImportError:
    sys.modules['fcntl'] = types.SimpleNamespace(LOCK_EX=2, LOCK_SH=1, LOCK_NB=4, flock=lambda *a: None)
spec = importlib.util.spec_from_file_location('deferred', HERE / 'deferred.py')
deferred = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deferred)


class PolicyTests(unittest.TestCase):
    @unittest.skipIf(os.name == 'nt', 'requires actual POSIX flock')
    def test_slot_gate_uses_the_configured_foreground_reserve(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(deferred, 'Q', Path(directory)):
            self.assertIsNone(deferred.slots(2, 2))  # legacy two-slot reserve
            held = deferred.slots(2, 2, foreground=0)
            self.assertEqual(len(held), 2)
            for fd in held:
                os.close(fd)
            held = deferred.slots(2, 1, foreground=1)
            self.assertEqual(len(held), 1)
            for fd in held:
                os.close(fd)

    def test_fleet_memory_reservations_count_others_and_self_once(self):
        value = {'fleet': True, 'mode': 'immediate', 'load_cap': 8, 'ram_floor_gb': 2,
                 'memory_max_gb': 3, 'pool_slots': 8, 'slots': 2}
        active = [{'id': 'cq-a', 'phase': 'running', 'slots': 2, 'reserved_memory_bytes': 4 * deferred.GIB},
                  {'id': 'cq-b', 'phase': 'starting', 'slots': 2, 'reserved_memory_bytes': 3 * deferred.GIB}]
        with mock.patch.object(deferred, 'reservations', return_value=(active, False, False)), \
             mock.patch.object(deferred, 'legacy_busy', return_value=False), \
             mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True), \
             mock.patch.object(deferred, 'available', return_value=9 * deferred.GIB):
            self.assertIsNone(deferred.fleet_reason(value, 'cq-b'))
            self.assertIn('memory', deferred.fleet_reason(value))
            self.assertIn('CPU', deferred.fleet_reason({**value, 'slots': 3}))
        with mock.patch.object(deferred, 'reservations', return_value=(active, False, True)):
            self.assertIn('waiting', deferred.fleet_reason(value, 'cq-b'))

    def test_immediate_requires_fleet_and_light_fleet_is_rejected(self):
        value = {'version': 1, 'id': 'cq-test', 'mode': 'immediate', 'timezone': 'Europe/London',
                 'window_start': 0, 'window_end': 8, 'slots': 0, 'pool_slots': 8,
                 'est_minutes': 5, 'timeout_minutes': 10, 'load_cap': 8, 'ram_floor_gb': 2,
                 'memory_max_gb': 1, 'source_sha': 'a' * 40}
        with mock.patch.object(deferred, 'jobdir', return_value=Path('x')), \
             mock.patch.object(deferred, 'read', return_value=value), \
             mock.patch.object(deferred, 'host_policy', return_value={}):
            with self.assertRaisesRegex(RuntimeError, 'scheduling policy'):
                deferred.desc('cq-test')
            value['fleet'] = True
            with self.assertRaisesRegex(RuntimeError, 'slots'):
                deferred.desc('cq-test')
        self.assertTrue(deferred.window(value))

    def test_nonblocking_gate_reports_contention(self):
        with mock.patch.object(deferred, 'open_lock', return_value=42), \
             mock.patch.object(deferred.fcntl, 'flock', side_effect=BlockingIOError) as flock, \
             mock.patch.object(deferred.os, 'close') as close:
            self.assertIsNone(deferred.try_gate(shared=True))
            flock.assert_called_once_with(42, deferred.fcntl.LOCK_SH | deferred.fcntl.LOCK_NB)
            close.assert_called_once_with(42)

    def test_probe_is_read_only_and_reports_gate_and_active_reservations(self):
        policy = {'pool_slots': 8, 'load_cap': 10, 'ram_floor_gb': 2}
        active = [{'id': 'cq-a', 'phase': 'starting', 'slots': 2, 'reserved_memory_bytes': 3 * deferred.GIB}]
        with tempfile.TemporaryDirectory() as temp, mock.patch.object(deferred, 'Q', Path(temp)), \
             mock.patch.object(deferred, 'check', return_value={'ok': True}), \
             mock.patch.object(deferred, 'host_policy', return_value=policy), \
             mock.patch.object(deferred, 'reservations', return_value=(active, False, False)), \
             mock.patch.object(deferred, 'legacy_busy', return_value=False), \
             mock.patch.object(deferred, 'existing_lock_free', return_value=False), \
             mock.patch.object(deferred, 'run_facts', return_value={'src_exists': True}), \
             mock.patch.object(deferred, 'machine_id', return_value='machine'), \
             mock.patch.object(deferred, 'available', return_value=100), \
             mock.patch.object(deferred, 'total_memory', return_value=200), \
             mock.patch.object(deferred.shutil, 'which', side_effect=lambda name: '/bin/' + name if name in ('python3', 'ffmpeg') else None) as which, \
             mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True):
            result = deferred.fleet_probe()
            self.assertTrue(result['legacy_busy'])
            self.assertEqual(result['active_fleet_slots'], 2)
            self.assertEqual(result['reserved_memory_bytes'], 3 * deferred.GIB)
            self.assertTrue(result['active_fleet'][0]['src_exists'])
            self.assertEqual(result['capabilities'], ['linux', 'python', 'ffmpeg'])
            which.side_effect = lambda name: '/bin/python' if name == 'python' else None
            self.assertEqual(deferred.fleet_probe()['capabilities'], ['linux', 'python'])
            self.assertEqual(list(Path(temp).iterdir()), [])
            peaks = {'cq-valid': 1234, 'cq-zero': 0, 'cq-bool': True, 'cq-float': 1.5, 'cq-negative': -1}
            for jid in peaks:
                (Path(temp) / jid).mkdir()
            def state(path):
                return {'id': path.parent.name, 'fleet': True, 'phase': 'done',
                        'memory_peak_bytes': peaks[path.parent.name]}
            with mock.patch.object(deferred, 'safe'), mock.patch.object(deferred, 'read', side_effect=state):
                rows = {row['id']: row for row in deferred.fleet_probe()['terminal_fleet']}
            self.assertEqual(rows['cq-valid']['memory_peak_bytes'], 1234)
            self.assertEqual(rows['cq-valid']['memory_kind'], 'cgroup_memory')
            self.assertEqual(rows['cq-zero']['memory_peak_bytes'], 0)
            for jid in ('cq-bool', 'cq-float', 'cq-negative'):
                self.assertNotIn('memory_peak_bytes', rows[jid])
                self.assertNotIn('memory_kind', rows[jid])

    def test_reservations_fail_closed_on_bad_active_record(self):
        with tempfile.TemporaryDirectory() as temp:
            d = Path(temp) / 'cq-a'
            d.mkdir()
            state = {'id': 'cq-a', 'phase': 'running', 'fleet': True, 'slots': 1}
            with mock.patch.object(deferred, 'Q', Path(temp)), mock.patch.object(deferred, 'safe'), \
                 mock.patch.object(deferred, 'read', return_value=state):
                with self.assertRaisesRegex(RuntimeError, 'invalid active fleet reservation'):
                    deferred.reservations()

    def test_held_foreground_slots_and_required_headroom(self):
        def try_pool(pool, busy, need):
            def lock(fd, operation):
                if fd - 100 in busy:
                    raise BlockingIOError()
            with mock.patch.object(deferred, 'open_lock', side_effect=lambda path: 100 + int(path.name.rsplit('.', 1)[1])), \
                 mock.patch.object(deferred.fcntl, 'flock', side_effect=lock), mock.patch.object(deferred.os, 'close'):
                return deferred.slots(pool, need)
        self.assertEqual(len(try_pool(16, {0, 1}, 2)), 2)
        self.assertIsNone(try_pool(6, {0, 1, 2, 3}, 1))

    def test_dst_window_uses_actual_elapsed_time(self):
        v = {'mode': 'overnight', 'est_minutes': 480}
        # Spring clock change leaves seven real hours from midnight to 08:00.
        self.assertFalse(deferred.window(v, dt.datetime(2026, 3, 29, 0, 0, tzinfo=dt.timezone.utc)))
        # Autumn clock change leaves nine real hours.
        self.assertTrue(deferred.window(v, dt.datetime(2026, 10, 24, 23, 0, tzinfo=dt.timezone.utc)))
        self.assertFalse(deferred.window(v, dt.datetime(2026, 10, 25, 8, 0, tzinfo=dt.timezone.utc)))
        self.assertTrue(deferred.window({'mode': 'spare', 'est_minutes': 15, 'timeout_minutes': 20}, dt.datetime(2026, 10, 25, 12, 0, tzinfo=dt.timezone.utc)))

    def test_spare_long_batches_wait_for_night_and_fit(self):
        noon = dt.datetime(2026, 11, 9, 12, 0, tzinfo=dt.timezone.utc)
        late = dt.datetime(2026, 11, 9, 7, 50, tzinfo=dt.timezone.utc)
        night = dt.datetime(2026, 11, 9, 0, 0, tzinfo=dt.timezone.utc)
        short = {'mode': 'spare', 'est_minutes': 15, 'timeout_minutes': 20}
        self.assertTrue(deferred.window(short, noon))
        self.assertTrue(deferred.window(short, late))
        for v in ({**short, 'est_minutes': 16}, {**short, 'timeout_minutes': 21}):
            self.assertFalse(deferred.window(v, noon))
            self.assertFalse(deferred.window(v, late))
            self.assertTrue(deferred.window(v, night))

    @mock.patch.object(deferred.os, 'getloadavg', return_value=(0.1, 0.1, 0.1), create=True)
    def test_full_reservation_above_floor_even_when_idle(self, _):
        v = {'mode': 'spare', 'est_minutes': 5, 'timeout_minutes': 10, 'load_cap': 10, 'ram_floor_gb': 4, 'memory_max_gb': 6}
        with mock.patch.object(deferred, 'available', return_value=10 * deferred.GIB - 1):
            self.assertIn('insufficient', deferred.eligibility(v))
        with mock.patch.object(deferred, 'available', return_value=10 * deferred.GIB):
            self.assertIsNone(deferred.eligibility(v))
        with mock.patch.object(deferred.os, 'getloadavg', return_value=(11, 0, 0), create=True):
            self.assertIn('load', deferred.eligibility(v))

    def test_pool_leaves_two_configured_slots(self):
        value = {'version': 1, 'id': 'cq-test', 'mode': 'spare', 'timezone': 'Europe/London',
                 'window_start': 0, 'window_end': 8, 'slots': 7, 'pool_slots': 8,
                 'est_minutes': 5, 'timeout_minutes': 10, 'load_cap': 8, 'ram_floor_gb': 2,
                 'memory_max_gb': 1, 'source_sha': 'a' * 40}
        policy = {k: value[k] for k in ('pool_slots', 'load_cap', 'ram_floor_gb')}
        with mock.patch.object(deferred, 'jobdir', return_value=Path('x')), mock.patch.object(deferred, 'read', return_value=value), mock.patch.object(deferred, 'host_policy', return_value=policy):
            with self.assertRaisesRegex(RuntimeError, 'configured pool slots'):
                deferred.desc('cq-test')


class StateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.job = self.root / 'cq-test'
        self.job.mkdir()
        self.oldq = deferred.Q
        deferred.Q = self.root
        self.addCleanup(setattr, deferred, 'Q', self.oldq)
        self.records = {}
        self.base = {'version': 1, 'id': 'cq-test', 'phase': 'starting', 'boot': 'boot',
                     'nonce': 'claim', 'submitted': False}
        self.records['cq-test'] = dict(self.base)
        def read(path):
            return dict(self.records['cq-test']) if path.name == 'deferred-state.json' else None
        def save(jid, state):
            self.records[jid] = dict(state)
        self.patches = [mock.patch.object(deferred, 'jobdir', return_value=self.job),
                        mock.patch.object(deferred, 'safe'),
                        mock.patch.object(deferred, 'read', side_effect=read),
                        mock.patch.object(deferred, 'save', side_effect=save),
                        mock.patch.object(deferred, 'boot', return_value='boot')]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)

    def test_ambiguous_submission_never_replays(self):
        props = {'LoadState': 'not-found', 'ActiveState': 'inactive', 'Job': '0'}
        with mock.patch.object(deferred, 'show', return_value=props), mock.patch.object(deferred, 'command') as cmd:
            self.assertEqual(deferred.status('cq-test')['phase'], 'starting')
            self.assertEqual(deferred.tick()['reason'], 'another deferred reservation is active')
            cmd.assert_not_called()

    def test_cancellation_waits_for_empty_cgroup(self):
        self.records['cq-test'].update(phase='running', submitted=True)
        props = {'LoadState': 'loaded', 'ActiveState': 'deactivating', 'Job': '0'}
        result = types.SimpleNamespace(returncode=0, stderr='')
        with mock.patch.object(deferred, 'show', return_value=props), mock.patch.object(deferred, 'command', return_value=result):
            s = deferred.cancel('cq-test')
            self.assertEqual(s['phase'], 'running')
            self.assertTrue(s['cancel_requested'])
            props['ActiveState'] = 'inactive'
            self.assertEqual(deferred.status('cq-test')['phase'], 'cancelled')

    def test_cgroup_descendant_blocks_cancel_confirmation(self):
        self.records['cq-test'].update(phase='running', submitted=True, control_group='/user.slice/job')
        props = {'LoadState': 'loaded', 'ActiveState': 'inactive', 'Job': '0', 'ControlGroup': '/user.slice/job'}
        group = self.root / 'user.slice' / 'job'
        group.mkdir(parents=True)
        events = group / 'cgroup.events'
        events.write_text('populated 1\nfrozen 0\n')
        result = types.SimpleNamespace(returncode=0, stderr='')
        with mock.patch.object(deferred, 'CGROUP', self.root), mock.patch.object(deferred, 'show', return_value=props), mock.patch.object(deferred, 'command', return_value=result):
            self.assertEqual(deferred.cancel('cq-test')['phase'], 'running')
            events.write_text('populated 0\nfrozen 0\n')
            self.assertEqual(deferred.status('cq-test')['phase'], 'cancelled')

    def test_remain_after_exit_waits_for_empty_cgroup_then_finishes_and_retires(self):
        self.records['cq-test'].update(phase='running', submitted=True,
                                        control_group='/user.slice/job', memory_peak_bytes=8388608)
        (self.job / 'exit').write_text('0\n')
        group = self.root / 'user.slice' / 'job'
        group.mkdir(parents=True)
        events = group / 'cgroup.events'
        events.write_text('populated 1\nfrozen 0\n')
        props = {'LoadState': 'loaded', 'ActiveState': 'active', 'SubState': 'exited',
                 'Result': 'success', 'Job': '', 'ControlGroup': '/user.slice/job'}
        with mock.patch.object(deferred, 'CGROUP', self.root), mock.patch.object(deferred, 'show', return_value=props), \
             mock.patch.object(deferred, 'command', return_value=types.SimpleNamespace(returncode=0)) as stop:
            self.assertEqual(deferred.status('cq-test')['phase'], 'running')
            stop.assert_not_called()
            events.write_text('populated 0\nfrozen 0\n')
            state = deferred.status('cq-test')
            self.assertEqual(state['phase'], 'done')
            self.assertEqual(state['exit'], 0)
            self.assertEqual(state['memory_peak_bytes'], 8388608)
            self.assertTrue(state['unit_retired'])
            stop.assert_called_once_with('systemctl', '--user', 'stop', deferred.unit('cq-test'), check=False)

    def test_active_exited_with_pending_systemd_job_is_not_empty(self):
        props = {'ActiveState': 'active', 'SubState': 'exited'}
        for value in ('42 stop', 'unknown', '0 garbage'):
            self.assertFalse(deferred.empty({**props, 'Job': value}))
        self.assertFalse(deferred.empty(props))
        for value in ('', '0'):
            self.assertTrue(deferred.empty({**props, 'Job': value}))

    def test_check_uses_nonsecret_manager_query(self):
        (self.root / 'cgroup.controllers').write_text('cpu memory\n')
        def invoke(*args):
            stdout = 'yes\n' if args[0] == 'loginctl' else ('255\n' if 'show' in args else '')
            return types.SimpleNamespace(stdout=stdout, returncode=0)
        with mock.patch.object(deferred, 'CGROUP', self.root), mock.patch.object(deferred, 'host_policy', return_value={}), \
             mock.patch.object(deferred, 'machine_id', return_value='machine'), mock.patch.object(deferred, 'available', return_value=123), \
             mock.patch.object(deferred.os, 'getuid', return_value=1000, create=True), \
             mock.patch.object(deferred, 'command', side_effect=invoke) as run:
            self.assertEqual(deferred.check()['user_manager_version'], '255')
            self.assertFalse(any('show-environment' in call.args for call in run.call_args_list))

    def test_reboot_never_replays(self):
        self.records['cq-test']['boot'] = 'old'
        with mock.patch.object(deferred, 'show', return_value={'LoadState': 'not-found', 'ActiveState': 'inactive'}):
            self.assertEqual(deferred.status('cq-test')['phase'], 'failed')

    def test_cancel_before_registration_persists_tombstone(self):
        tombstone = {}
        def read(path):
            return tombstone.get(str(path))
        def atomic(path, value):
            tombstone[str(path)] = value
        with mock.patch.object(deferred, 'read', side_effect=read), mock.patch.object(deferred, 'atomic', side_effect=atomic), \
             mock.patch.object(deferred, 'desc') as descriptor:
            self.assertEqual(deferred.cancel('cq-test')['phase'], 'cancelled')
            self.assertEqual(deferred.status('cq-test')['phase'], 'cancelled')
            self.assertEqual(deferred.register('cq-test')['phase'], 'cancelled')
            descriptor.assert_not_called()

    def test_foreground_race_is_requeued_only_after_unit_stops(self):
        v = {'pool_slots': 8, 'slots': 2}
        with mock.patch.object(deferred, 'staged_state', return_value=(dict(self.base), v)), \
             mock.patch.object(deferred, 'locked', side_effect=lambda p: contextlib.nullcontext()), \
             mock.patch.object(deferred, 'try_gate', return_value=None), \
             mock.patch.object(deferred, 'self_group', return_value='/user.slice/job'), \
             mock.patch.object(deferred, 'eligibility', return_value=None), \
             mock.patch.object(deferred, 'foreground_waiter', return_value=False), \
             mock.patch.object(deferred, 'slots', return_value=None), \
             mock.patch.object(deferred.subprocess, 'Popen') as spawn:
            deferred.runner('cq-test', 'claim')
            self.assertEqual(self.records['cq-test']['phase'], 'starting')
            self.assertTrue(self.records['cq-test']['retry_safe'])
            spawn.assert_not_called()
        self.records['cq-test']['submitted'] = True
        props = {'LoadState': 'loaded', 'ActiveState': 'active', 'Job': '0'}
        result = types.SimpleNamespace(returncode=0, stderr='')
        with mock.patch.object(deferred, 'show', return_value=props), mock.patch.object(deferred, 'command', return_value=result):
            self.assertEqual(deferred.status('cq-test')['phase'], 'starting')
            props['ActiveState'] = 'inactive'
            self.assertEqual(deferred.status('cq-test')['phase'], 'queued')

    def test_fleet_first_runner_rejects_preexisting_legacy_slot(self):
        value = {'fleet': True, 'pool_slots': 8, 'slots': 2}
        active = [{'id': 'cq-test', 'phase': 'starting', 'slots': 2, 'reserved_memory_bytes': deferred.GIB}]
        with mock.patch.object(deferred, 'staged_state', return_value=(dict(self.base), value)), \
             mock.patch.object(deferred, 'locked', side_effect=lambda p: contextlib.nullcontext()), \
             mock.patch.object(deferred, 'self_group', return_value='/user.slice/job'), \
             mock.patch.object(deferred, 'try_gate', return_value=99), \
             mock.patch.object(deferred.os, 'close') as close, \
             mock.patch.object(deferred, 'reservations', return_value=(active, False, False)), \
             mock.patch.object(deferred, 'fleet_reason', return_value=None), \
             mock.patch.object(deferred, 'pool_empty', return_value=False), \
             mock.patch.object(deferred.subprocess, 'Popen') as spawn:
            deferred.runner('cq-test', 'claim')
            self.assertTrue(self.records['cq-test']['retry_safe'])
            self.assertIn('legacy heavy slots held', self.records['cq-test']['reason'])
            close.assert_called_once_with(99)
            spawn.assert_not_called()

    def test_large_live_foreground_waiter_blocks_but_light_does_not(self):
        (self.job / 'pid').write_text('12345')
        env = self.job / 'job.env'
        with mock.patch.object(deferred, 'process_matches', return_value=True):
            env.write_text('SLOTS=14\n')
            self.assertTrue(deferred.foreground_waiter())
            env.write_text('SLOTS=0\n')
            self.assertFalse(deferred.foreground_waiter())
            env.write_text('SLOTS=$(touch should-never-run)\n')
            self.assertTrue(deferred.foreground_waiter())
            self.assertFalse((self.job / 'should-never-run').exists())

    def test_tick_defers_to_foreground_waiter(self):
        self.records['cq-test']['phase'] = 'queued'
        v = {'pool_slots': 16, 'slots': 2}
        with mock.patch.object(deferred, 'staged_state', return_value=(dict(self.records['cq-test']), v)), \
             mock.patch.object(deferred, 'ordinary_waiter', return_value=(True, None)), \
             mock.patch.object(deferred, 'host_policy', return_value={'x': 1}), \
             mock.patch.object(deferred, 'total_memory', return_value=8 * deferred.GIB), \
             mock.patch.object(deferred, 'locked', side_effect=lambda p: contextlib.nullcontext()), \
             mock.patch.object(deferred, 'try_gate', return_value=99), mock.patch.object(deferred.os, 'close'), \
             mock.patch.object(deferred, 'eligibility', return_value=None), \
             mock.patch.object(deferred, 'foreground_waiter', return_value=True), \
             mock.patch.object(deferred, 'command') as launch:
            deferred.tick()
            self.assertEqual(self.records['cq-test']['phase'], 'queued')
            self.assertIn('foreground heavy job is waiting', self.records['cq-test']['reason'])
            launch.assert_not_called()


class FleetBatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.states = {}
        self.values = {}
        for index in range(3):
            jid = 'cq-batch-' + str(index)
            (self.root / jid).mkdir()
            self.states[jid] = {'id': jid, 'phase': 'queued', 'fleet': True, 'registered': 1}
            self.values[jid] = {'fleet': True, 'mode': 'immediate', 'slots': 1, 'pool_slots': 8,
                                'memory_max_gb': 1, 'ram_floor_gb': 1, 'load_cap': 8,
                                'est_minutes': 5, 'timeout_minutes': 10}
        def read(path):
            state = self.states.get(path.parent.name)
            if path.name == 'deferred-state.json':
                return dict(state) if state else None
            value = self.values.get(path.parent.name)
            return dict(value) if path.name == 'deferred.json' and value else None
        def save(jid, state):
            self.states[jid] = dict(state)
        self.patches = [mock.patch.object(deferred, 'Q', self.root),
                        mock.patch.object(deferred, 'safe'),
                        mock.patch.object(deferred, 'read', side_effect=read),
                        mock.patch.object(deferred, 'save', side_effect=save),
                        mock.patch.object(deferred, 'staged_state', side_effect=lambda jid: (dict(self.states[jid]), self.values[jid])),
                        mock.patch.object(deferred, 'legacy_busy', return_value=False),
                        mock.patch.object(deferred, 'foreground_waiter', return_value=False),
                        mock.patch.object(deferred, 'available', return_value=3 * deferred.GIB),
                        mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True),
                        mock.patch.object(deferred, 'try_gate', return_value=99),
                        mock.patch.object(deferred.os, 'close'),
                        mock.patch.object(deferred, 'pool_empty', return_value=True),
                        mock.patch.object(deferred, 'slots', return_value=[98]),
                        mock.patch.object(deferred, 'boot', return_value='boot')]
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)

    def test_two_fleet_claims_and_third_blocked_by_memory(self):
        with mock.patch.object(deferred, 'command') as launch:
            result = deferred.tick()
        self.assertEqual(result['id'], 'cq-batch-0')
        self.assertEqual(result['phase'], 'starting')
        self.assertEqual(result['started'], ['cq-batch-0', 'cq-batch-1'])
        self.assertEqual(result['blocked_id'], 'cq-batch-2')
        self.assertIn('memory', result['reason'])
        self.assertEqual(launch.call_count, 2)
        self.assertEqual(self.states['cq-batch-2']['phase'], 'queued')

    def test_ordinary_waiter_blocks_entire_fleet_batch(self):
        jid = 'cq-ordinary'
        (self.root / jid).mkdir()
        self.states[jid] = {'id': jid, 'phase': 'queued', 'fleet': False, 'registered': 1}
        self.values[jid] = {'fleet': False, 'pool_slots': 8, 'slots': 1}
        with mock.patch.object(deferred, 'eligibility', return_value='outside window'), \
             mock.patch.object(deferred, 'ordinary_waiter', return_value=(True, None)), \
             mock.patch.object(deferred, 'host_policy', return_value={'x': 1}), \
             mock.patch.object(deferred, 'total_memory', return_value=8 * deferred.GIB), \
             mock.patch.object(deferred, 'command') as launch:
            result = deferred.tick()
        self.assertIn('waiting', result['reason'])
        launch.assert_not_called()
        self.assertTrue(all(self.states[jid]['phase'] == 'queued' for jid in self.values))

    def test_ambiguous_second_submission_keeps_first_and_stops_batch(self):
        with mock.patch.object(deferred, 'command', side_effect=[None, subprocess.TimeoutExpired('systemd-run', 30)]) as launch:
            result = deferred.tick()
        self.assertEqual(result['started'], ['cq-batch-0'])
        self.assertEqual(result['error_id'], 'cq-batch-1')
        self.assertIn('timed out', result['error'])
        self.assertFalse(self.states['cq-batch-1']['submitted'])
        self.assertEqual(self.states['cq-batch-2']['phase'], 'queued')
        self.assertEqual(launch.call_count, 2)

    def add_overnight(self, memory_gb=1):
        jid = 'cq-ordinary'
        (self.root / jid).mkdir()
        policy = {'pool_slots': 8, 'load_cap': 8, 'ram_floor_gb': 1}
        value = {'version': 1, 'id': jid, 'mode': 'overnight', 'timezone': 'Europe/London',
                 'window_start': 0, 'window_end': 8, 'slots': 1, **policy,
                 'est_minutes': 5, 'timeout_minutes': 10, 'memory_max_gb': memory_gb,
                 'source_sha': 'a' * 40}
        self.values[jid] = value
        self.states[jid] = {'id': jid, 'phase': 'queued', 'registered': 1,
                            'descriptor_hash': deferred.fingerprint(value),
                            'policy_hash': deferred.fingerprint(policy)}
        return policy

    def test_overnight_waiter_only_drains_during_window(self):
        policy = self.add_overnight()
        real_window = deferred.window
        noon = dt.datetime(2026, 11, 9, 12, tzinfo=dt.timezone.utc)
        night = dt.datetime(2026, 11, 9, 1, tzinfo=dt.timezone.utc)
        with mock.patch.object(deferred, 'host_policy', return_value=policy), \
             mock.patch.object(deferred, 'total_memory', return_value=8 * deferred.GIB), \
             mock.patch.object(deferred, 'command') as launch:
            with mock.patch.object(deferred, 'window', side_effect=lambda v: real_window(v, noon)):
                self.assertFalse(deferred.reservations()[2])
                result = deferred.tick()
                self.assertEqual(result['started'], ['cq-batch-0', 'cq-batch-1'])
            self.assertEqual(launch.call_count, 2)
            with mock.patch.object(deferred, 'window', side_effect=lambda v: real_window(v, night)):
                self.assertTrue(deferred.reservations()[2])
                self.assertIn('waiting', deferred.fleet_reason(self.values['cq-batch-2']))

    def test_impossible_ordinary_memory_does_not_block_fleet(self):
        policy = self.add_overnight(memory_gb=8)
        real_window = deferred.window
        night = dt.datetime(2026, 11, 9, 1, tzinfo=dt.timezone.utc)
        with mock.patch.object(deferred, 'host_policy', return_value=policy), \
             mock.patch.object(deferred, 'total_memory', return_value=8 * deferred.GIB), \
             mock.patch.object(deferred, 'window', side_effect=lambda v: real_window(v, night)), \
             mock.patch.object(deferred, 'command') as launch:
            self.assertFalse(deferred.reservations()[2])
            result = deferred.tick()
        self.assertEqual(result['started'], ['cq-batch-0', 'cq-batch-1'])
        self.assertIn('exceeds total host capacity', self.states['cq-ordinary']['reason'])
        self.assertEqual(launch.call_count, 2)

    def test_changed_ordinary_descriptor_fails_closed(self):
        policy = self.add_overnight()
        self.values['cq-ordinary']['memory_max_gb'] = 2
        with mock.patch.object(deferred, 'host_policy', return_value=policy), \
             mock.patch.object(deferred, 'total_memory', return_value=8 * deferred.GIB):
            with self.assertRaisesRegex(RuntimeError, 'descriptor or policy changed'):
                deferred.reservations()


@unittest.skipUnless(sys.platform.startswith('linux'), 'real Linux flock required')
class LinuxLockTests(unittest.TestCase):
    def test_shared_fleet_gate_excludes_legacy_exclusive_and_drains(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(deferred, 'Q', Path(tmp)):
                shared = deferred.try_gate(shared=True)
                self.assertIsNotNone(shared)
                other = deferred.try_gate(shared=True)
                self.assertIsNotNone(other)
                self.assertIsNone(deferred.try_gate())
                os.close(shared)
                self.assertIsNone(deferred.try_gate())
                os.close(other)
                exclusive = deferred.try_gate()
                self.assertIsNotNone(exclusive)
                self.assertIsNone(deferred.try_gate(shared=True))
                os.close(exclusive)
    def test_foreground_lock_and_concurrent_claim(self):
        with tempfile.TemporaryDirectory() as tmp:
            oldq = deferred.Q
            deferred.Q = Path(tmp)
            try:
                first = []
                for i in range(2):
                    fd = deferred.open_lock(deferred.Q / ('.heavy.' + str(i)))
                    deferred.fcntl.flock(fd, deferred.fcntl.LOCK_EX | deferred.fcntl.LOCK_NB)
                    first.append(fd)
                with deferred.locked(deferred.Q / '.heavy.gate'):
                    taken = deferred.slots(16, 2)
                self.assertEqual(len(taken), 2)
                with deferred.locked(deferred.Q / '.heavy.gate'):
                    self.assertIsNone(deferred.slots(6, 1))  # only two free; needs three
                for fd in first + taken:
                    os.close(fd)
                active = 0
                high = 0
                def contender():
                    nonlocal active, high
                    with deferred.locked(deferred.Q / '.deferred.lock'):
                        active += 1
                        high = max(high, active)
                        time.sleep(.03)
                        active -= 1
                threads = [threading.Thread(target=contender) for _ in range(8)]
                for t in threads: t.start()
                for t in threads: t.join()
                self.assertEqual(high, 1)
            finally:
                deferred.Q = oldq

    def test_concurrent_ticks_submit_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            job = root / 'cq-test'
            job.mkdir()
            oldq = deferred.Q
            deferred.Q = root
            state = {'version': 1, 'id': 'cq-test', 'phase': 'queued'}
            calls = []
            def read(path):
                return dict(state) if path.name == 'deferred-state.json' else None
            def save(jid, record):
                state.clear()
                state.update(record)
            def launch(*args, **kwargs):
                calls.append(args)
                time.sleep(.03)
                return types.SimpleNamespace(returncode=0)
            v = {'pool_slots': 4, 'slots': 1, 'memory_max_gb': 1, 'timeout_minutes': 1}
            try:
                with mock.patch.object(deferred, 'read', side_effect=read), mock.patch.object(deferred, 'save', side_effect=save), \
                     mock.patch.object(deferred, 'staged_state', side_effect=lambda jid: (dict(state), v)), \
                     mock.patch.object(deferred, 'ordinary_waiter', return_value=(True, None)), \
                     mock.patch.object(deferred, 'host_policy', return_value={'pool_slots': 4}), \
                     mock.patch.object(deferred, 'total_memory', return_value=8 * deferred.GIB), \
                     mock.patch.object(deferred, 'eligibility', return_value=None), mock.patch.object(deferred, 'boot', return_value='boot'), \
                     mock.patch.object(deferred, 'foreground_waiter', return_value=False), \
                     mock.patch.object(deferred, 'command', side_effect=launch), \
                     mock.patch.object(deferred, 'status', side_effect=lambda jid: dict(state)):
                    def run_tick():
                        with deferred.locked(root / '.deferred.lock'):
                            deferred.tick()
                    threads = [threading.Thread(target=run_tick) for _ in range(8)]
                    for t in threads: t.start()
                    for t in threads: t.join()
                self.assertEqual(len(calls), 1)
                self.assertEqual(state['phase'], 'starting')
            finally:
                deferred.Q = oldq


class ConfigurablePolicyTests(unittest.TestCase):
    def test_fleet_admission_counts_configured_foreground_reserve(self):
        value = {'fleet': True, 'mode': 'immediate', 'load_cap': 8, 'ram_floor_gb': 2,
                 'memory_max_gb': 1, 'pool_slots': 8, 'slots': 2, 'foreground_slots': 3}
        active = [{'id': 'cq-a', 'phase': 'running', 'slots': 4, 'reserved_memory_bytes': deferred.GIB}]
        with mock.patch.object(deferred, 'reservations', return_value=(active, False, False)), \
             mock.patch.object(deferred, 'legacy_busy', return_value=False), \
             mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True), \
             mock.patch.object(deferred, 'available', return_value=10 * deferred.GIB):
            self.assertIn('CPU', deferred.fleet_reason(value))
            self.assertIsNone(deferred.fleet_reason({**value, 'slots': 1}))

    def test_installer_preserves_existing_raw_policy_and_refuses_changes(self):
        spec = importlib.util.spec_from_file_location('installer', HERE / 'install_deferred.py')
        installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(installer)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = root / '.local/share/baton/compute-deferred'
            target.mkdir(parents=True)
            config = target / 'config.json'
            raw = '{"version":1,"machine_id":"' + 'a' * 32 + '","pool_slots":8,"load_cap":10,"ram_floor_gb":2}\n'
            config.write_text(raw)
            original_read = Path.read_text
            def read_text(path, *args, **kwargs):
                if str(path).replace('\\', '/') == '/etc/machine-id':
                    return 'a' * 32
                return original_read(path, *args, **kwargs)
            argv = ['install_deferred.py', '--pool-slots', '8', '--load-cap', '10', '--ram-floor-gb', '2']
            with mock.patch.object(Path, 'home', return_value=root), \
                 mock.patch.object(Path, 'read_text', read_text), \
                 mock.patch.object(installer.os, 'cpu_count', return_value=16), \
                 mock.patch.object(installer.os, 'getuid', return_value=1000, create=True), \
                 mock.patch.object(installer.subprocess, 'check_output', return_value='yes'), \
                 mock.patch.object(installer.subprocess, 'run'), mock.patch.object(sys, 'argv', argv):
                installer.main()
                self.assertEqual(config.read_text(), raw)
                self.assertTrue((target / 'queue_policy.py').is_file())
                with mock.patch.object(sys, 'argv', argv + ['--update-policy']):
                    installer.main()
                self.assertEqual(config.read_text(), raw)
                with mock.patch.object(sys, 'argv', argv + ['--foreground-slots', '1']):
                    with self.assertRaisesRegex(SystemExit, 'pass --update-policy'):
                        installer.main()
                self.assertEqual(config.read_text(), raw)

    def test_update_refuses_unresolved_and_preserves_terminal_history(self):
        spec = importlib.util.spec_from_file_location('installer', HERE / 'install_deferred.py')
        installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(installer)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = root / '.local/share/baton/compute-deferred'
            target.mkdir(parents=True)
            queue = root / 'q'
            queue.mkdir()
            config = target / 'config.json'
            raw = '{"version":1,"machine_id":"' + 'a' * 32 + '","pool_slots":8,"load_cap":10,"ram_floor_gb":2}\n'
            config.write_text(raw)
            original_read = Path.read_text
            def read_text(path, *args, **kwargs):
                if str(path).replace('\\', '/') == '/etc/machine-id':
                    return 'a' * 32
                return original_read(path, *args, **kwargs)
            argv = ['install_deferred.py', '--pool-slots', '8', '--load-cap', '10', '--ram-floor-gb', '2']
            # Stub only OS-specific locking and directory fsync on Windows; job
            # evidence and policy-file behavior remain real filesystem checks.
            def update_policy(path, policy):
                path.write_text(json.dumps(policy, sort_keys=True) + '\n')
            fixture_uid = os.getuid() if hasattr(os, 'getuid') else 0
            with mock.patch.object(Path, 'home', return_value=root), \
                 mock.patch.object(Path, 'read_text', read_text), \
                 mock.patch.object(installer.os, 'cpu_count', return_value=16), \
                 mock.patch.object(installer.os, 'getuid', return_value=fixture_uid, create=True), \
                 mock.patch.object(installer.subprocess, 'check_output', return_value='yes'), \
                 mock.patch.object(installer.subprocess, 'run'), \
                 mock.patch.object(installer, 'update_locks', side_effect=lambda *a: contextlib.nullcontext()), \
                 mock.patch.object(installer, 'check_no_helper_process'), \
                 mock.patch.object(installer, '_read_json', side_effect=lambda path: json.loads(path.read_text())), \
                 mock.patch.object(installer, 'atomic_policy', side_effect=update_policy), \
                 mock.patch.object(sys, 'argv', argv):
                installer.main()
                update = argv + ['--update-policy', '--foreground-slots', '1', '--window-start', '22:30']
                with mock.patch.object(sys, 'argv', update), \
                     mock.patch.object(installer, 'update_locks', side_effect=RuntimeError('queue or heavy work is active')):
                    with self.assertRaisesRegex(SystemExit, 'heavy work is active'):
                        installer.main()
                self.assertEqual(config.read_text(), raw)
                job = queue / 'cq-example'
                job.mkdir()
                with mock.patch.object(sys, 'argv', update):
                    with self.assertRaisesRegex(SystemExit, 'unknown or unresolved'):
                        installer.main()
                    (job / 'deferred.json').write_text(json.dumps({'version': 1, 'id': job.name}))
                    with self.assertRaisesRegex(SystemExit, 'unresolved staged'):
                        installer.main()
                    descriptor = json.loads((job / 'deferred.json').read_text())
                    record = {'version': 1, 'id': job.name, 'phase': 'queued',
                              'descriptor_hash': deferred.fingerprint(descriptor), 'policy_hash': 'a' * 64}
                    (job / 'deferred-state.json').write_text(json.dumps(record))
                    with self.assertRaisesRegex(SystemExit, 'unresolved or invalid'):
                        installer.main()
                    self.assertEqual(config.read_text(), raw)
                    record['phase'] = 'done'
                    record['descriptor_hash'] = 'corrupt'
                    (job / 'deferred-state.json').write_text(json.dumps(record))
                    with self.assertRaisesRegex(SystemExit, 'unresolved or invalid'):
                        installer.main()
                    record['descriptor_hash'] = deferred.fingerprint(descriptor)
                    (job / 'deferred-state.json').write_text(json.dumps(record))
                    history = (job / 'deferred-state.json').read_bytes()
                    installer.main()
                    self.assertEqual(json.loads(config.read_text())['foreground_slots'], 1)
                    self.assertEqual(json.loads(config.read_text())['window_start'], '22:30')
                    self.assertEqual((job / 'deferred-state.json').read_bytes(), history)
                updated = config.read_text()
                with mock.patch.object(sys, 'argv', update + ['--pool-slots', '9']):
                    with self.assertRaisesRegex(SystemExit, 'base policy differs'):
                        installer.main()
                self.assertEqual(config.read_text(), updated)

    @unittest.skipIf(os.name == 'nt', 'requires POSIX flock and ownership')
    def test_update_locks_refuse_busy_heavy_gate(self):
        spec = importlib.util.spec_from_file_location('installer', HERE / 'install_deferred.py')
        installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(installer)
        with tempfile.TemporaryDirectory() as temp:
            queue = Path(temp)
            fd = os.open(queue / '.heavy.gate', os.O_CREAT | os.O_RDWR, 0o600)
            try:
                deferred.fcntl.flock(fd, deferred.fcntl.LOCK_EX | deferred.fcntl.LOCK_NB)
                with self.assertRaisesRegex(RuntimeError, 'active'):
                    with installer.update_locks(queue, 4):
                        pass
            finally:
                os.close(fd)

    def test_legacy_descriptor_and_raw_policy_are_not_rewritten(self):
        policy = {'version': 1, 'machine_id': 'a' * 32, 'pool_slots': 8, 'load_cap': 10, 'ram_floor_gb': 2}
        descriptor = {'version': 1, 'id': 'cq-test', 'mode': 'overnight', 'timezone': 'Europe/London',
                      'window_start': 0, 'window_end': 8, 'slots': 6, 'pool_slots': 8,
                      'est_minutes': 10, 'timeout_minutes': 30, 'load_cap': 10, 'ram_floor_gb': 2,
                      'memory_max_gb': 1, 'source_sha': 'a' * 40}
        old_policy, old_desc = deferred.fingerprint(policy), deferred.fingerprint(descriptor)
        deferred.validate_descriptor(descriptor, 'cq-test', policy)
        self.assertEqual((deferred.fingerprint(policy), deferred.fingerprint(descriptor)), (old_policy, old_desc))
        with self.assertRaisesRegex(RuntimeError, 'does not match'):
            deferred.validate_descriptor(descriptor, 'cq-test', {**policy, 'foreground_slots': 1})
        updated = {**descriptor, 'foreground_slots': 1, 'slots': 7}
        deferred.validate_descriptor(updated, 'cq-test', {**policy, 'foreground_slots': 1})
        with self.assertRaisesRegex(RuntimeError, 'configured pool slots'):
            deferred.validate_descriptor({**updated, 'slots': 8}, 'cq-test', {**policy, 'foreground_slots': 1})

    def test_custom_window_and_dst_boundaries(self):
        utc = dt.timezone.utc
        value = {'mode': 'overnight', 'est_minutes': 30, 'timezone': 'UTC',
                 'window_start': '22:15', 'window_end': '02:30'}
        self.assertTrue(deferred.window(value, dt.datetime(2026, 1, 2, 1, 59, tzinfo=utc)))
        self.assertFalse(deferred.window(value, dt.datetime(2026, 1, 2, 2, 1, tzinfo=utc)))
        self.assertFalse(deferred.window(value, dt.datetime(2026, 1, 2, 22, 14, tzinfo=utc)))
        # London spring gap: 01:30 advances to 02:00 local (01:00 UTC).
        gap = {**value, 'timezone': 'Europe/London', 'window_start': '01:30', 'window_end': '03:00'}
        self.assertFalse(deferred.window(gap, dt.datetime(2026, 3, 29, 0, 59, tzinfo=utc)))
        self.assertTrue(deferred.window(gap, dt.datetime(2026, 3, 29, 1, 0, tzinfo=utc)))
        # London autumn fold: start takes first occurrence, end the last.
        fold = {**value, 'timezone': 'Europe/London', 'window_start': '01:15', 'window_end': '01:45'}
        self.assertTrue(deferred.window(fold, dt.datetime(2026, 10, 25, 0, 15, tzinfo=utc)))
        self.assertTrue(deferred.window(fold, dt.datetime(2026, 10, 25, 1, 15, tzinfo=utc)))
        self.assertFalse(deferred.window(fold, dt.datetime(2026, 10, 25, 1, 16, tzinfo=utc)))

    def test_probe_returns_effective_policy_and_capability(self):
        policy = {'pool_slots': 8, 'load_cap': 10, 'ram_floor_gb': 2, 'foreground_slots': 3}
        with tempfile.TemporaryDirectory() as temp, mock.patch.object(deferred, 'Q', Path(temp)), \
             mock.patch.object(deferred, 'check', return_value={'ok': True}), \
             mock.patch.object(deferred, 'host_policy', return_value=policy), \
             mock.patch.object(deferred, 'reservations', return_value=([], False, False)), \
             mock.patch.object(deferred, 'legacy_busy', return_value=False), \
             mock.patch.object(deferred, 'existing_lock_free', return_value=True), \
             mock.patch.object(deferred, 'machine_id', return_value='machine'), \
             mock.patch.object(deferred, 'available', return_value=100), \
             mock.patch.object(deferred, 'total_memory', return_value=200), \
             mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True):
            result = deferred.fleet_probe()
        self.assertEqual(result['resource_policy_version'], 2)
        self.assertEqual(result['policy']['foreground_slots'], 3)
        self.assertEqual(result['policy']['window_end'], '08:00')
        self.assertNotIn('window_end', policy)


if __name__ == '__main__':
    unittest.main()
