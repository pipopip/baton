import contextlib
import copy
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent / 'bin'))
from queue_policy import OOM_REASON_PREFIX
# Allow policy/state tests to run on the development Windows machine. Real flock
# tests below run only on Linux and never use this stub.
try:
    import fcntl  # noqa: F401
except ImportError:
    sys.modules['fcntl'] = types.SimpleNamespace(LOCK_EX=2, LOCK_SH=1, LOCK_NB=4, flock=lambda *a: None)
spec = importlib.util.spec_from_file_location('deferred', HERE / 'deferred.py')
deferred = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deferred)


REAL_POOL_LIMIT = deferred.pool_limit
REAL_QUEUE_FREE = deferred.queue_free


def setUpModule():
    # Nothing here may ask the real user manager about the fleet slice: tests that need its limit patch pool_limit.
    patch = mock.patch.object(deferred, 'pool_limit', return_value=None)
    patch.start()
    unittest.addModuleCleanup(patch.stop)
    # ... nor depend on how full the disk under the test's temporary directory is: tests of the floor patch queue_free.
    disk = mock.patch.object(deferred, 'queue_free', return_value=100 * deferred.GIB)
    disk.start()
    unittest.addModuleCleanup(disk.stop)


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
            self.assertEqual(result['queued_fleet'], [])
            # registered fleet jobs that have not started are listed, for the controller to read instead of asking
            states = {'cq-w': {'id': 'cq-w', 'phase': 'queued', 'fleet': True, 'reason': 'fleet CPU pool exhausted'},
                      'cq-n': {'id': 'cq-n', 'phase': 'queued', 'fleet': False, 'reason': 'outside window'},
                      'cq-d': {'id': 'cq-d', 'phase': 'done', 'fleet': True, 'exit': 0}}
            for name in states:
                (Path(temp) / name).mkdir()
            with mock.patch.object(deferred, 'safe'), \
                 mock.patch.object(deferred, 'read', side_effect=lambda path: states.get(path.parent.name)):
                listed = deferred.fleet_probe()
            self.assertEqual(listed['queued_fleet'], [{'id': 'cq-w', 'reason': 'fleet CPU pool exhausted'}])
            self.assertEqual([row['id'] for row in listed['terminal_fleet']], ['cq-d'])
            for name in states:
                (Path(temp) / name).rmdir()
            self.assertEqual(result['reserved_memory_bytes'], 3 * deferred.GIB)
            self.assertEqual(result['held_memory_bytes'], 0)
            active[0]['held_memory_bytes'] = deferred.GIB
            with mock.patch.object(deferred, 'read', return_value={'id': 'cq-a'}), \
                 mock.patch.object(deferred, 'held_memory', return_value=2 * deferred.GIB):
                self.assertEqual(deferred.fleet_probe()['held_memory_bytes'], deferred.GIB)
                self.assertEqual(deferred.fleet_probe()['active_fleet'][0]['held_memory_bytes'], deferred.GIB)
            with mock.patch.object(deferred, 'read', return_value={'id': 'cq-a'}), \
                 mock.patch.object(deferred, 'held_memory', return_value=deferred.GIB // 2):
                self.assertEqual(deferred.fleet_probe()['held_memory_bytes'], deferred.GIB // 2)  # the second look was lower
            del active[0]['held_memory_bytes']
            self.assertTrue(result['active_fleet'][0]['src_exists'])
            self.assertEqual(result['capabilities'], ['linux', 'python', 'ffmpeg'])
            which.side_effect = lambda name: '/bin/python' if name == 'python' else None
            self.assertEqual(deferred.fleet_probe()['capabilities'], ['linux', 'python'])
            self.assertEqual(list(Path(temp).iterdir()), [])
            peaks = {'cq-valid': 1234, 'cq-zero': 0, 'cq-bool': True, 'cq-float': 1.5, 'cq-negative': -1}
            for jid in peaks:
                (Path(temp) / jid).mkdir()
            held = {'cq-valid': (99, 2), 'cq-zero': (99, 0), 'cq-bool': (True, 2), 'cq-float': (9.5, 2), 'cq-negative': (-1, 2)}
            def state(path):
                jid = path.parent.name
                return {'id': jid, 'fleet': True, 'phase': 'done', 'memory_peak_bytes': peaks[jid],
                        'memory_held_peak_bytes': held[jid][0], 'memory_held_samples': held[jid][1]}
            with mock.patch.object(deferred, 'safe'), mock.patch.object(deferred, 'read', side_effect=state):
                rows = {row['id']: row for row in deferred.fleet_probe()['terminal_fleet']}
            # what the job held, only as a whole number of bytes with at least one reading behind it
            self.assertEqual((rows['cq-valid']['memory_held_peak_bytes'], rows['cq-valid']['memory_held_samples']), (99, 2))
            for jid in ('cq-zero', 'cq-bool', 'cq-float', 'cq-negative'):
                self.assertNotIn('memory_held_peak_bytes', rows[jid])
                self.assertNotIn('memory_held_samples', rows[jid])
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

    def test_held_memory_is_the_running_units_anonymous_lists_within_its_cap(self):
        gib = deferred.GIB
        with tempfile.TemporaryDirectory() as temp, mock.patch.object(deferred, 'CGROUP', Path(temp)):
            group = Path(temp) / 'user.slice' / 'cq-deferred-cq-a.service'
            group.mkdir(parents=True)
            stat = group / 'memory.stat'
            # 3 GiB mapped anonymous, 2 of them freed lazily and so on the file lists with the page cache
            stat.write_text(f'anon {3 * gib}\nfile {5 * gib}\nkernel {gib}\nshmem {gib // 2}\nanon_thp {gib}\n'
                            f'inactive_anon {gib}\nactive_anon {gib // 2}\ninactive_file {6 * gib}\nactive_file {gib}\n'
                            f'unevictable {gib}\n')
            state = {'id': 'cq-a', 'phase': 'running', 'memory_bytes': 4 * gib,
                     'control_group': '/user.slice/cq-deferred-cq-a.service'}
            # not the lazily freed pages, the page cache, kernel or locked memory: MemAvailable counts some of those
            self.assertEqual(deferred.held_memory(state), gib + gib // 2)
            self.assertEqual(deferred.held_memory(None), 0)
            self.assertEqual(deferred.held_memory({**state, 'memory_bytes': gib}), gib)  # never more than the cap
            # Anything short of this job's own running unit is charged the whole cap.
            for change in ({'phase': 'starting'}, {'control_group': None}, {'control_group': '/user.slice'},
                           {'control_group': '/user.slice/../user.slice/cq-deferred-cq-a.service'},
                           {'control_group': 'user.slice/cq-deferred-cq-a.service'},
                           {'id': 'cq-b'}, {'memory_bytes': None}):
                self.assertEqual(deferred.held_memory({**state, **change}), 0, change)
            self.assertEqual(deferred.held_memory({k: v for k, v in state.items() if k != 'id'}), 0)
            stat.write_text('active_anon many\ninactive_anon 1\n')
            self.assertEqual(deferred.held_memory(state), 0)
            stat.write_text(f'active_anon -{gib}\ninactive_anon {2 * gib}\n')
            self.assertEqual(deferred.held_memory(state), 0)
            stat.write_text(f'active_anon {gib}\n')   # one of the two figures alone credits nothing
            self.assertEqual(deferred.held_memory(state), 0)
            stat.write_text(f'anon {gib}\nshmem {gib}\n')   # a kernel without the list figures credits nothing
            self.assertEqual(deferred.held_memory(state), 0)
            stat.unlink()
            self.assertEqual(deferred.held_memory(state), 0)

    def test_the_sampler_keeps_the_largest_reading_of_the_anonymous_lists(self):
        mib = 2 ** 20
        with tempfile.TemporaryDirectory() as temp:
            group = Path(temp)
            self.assertIsNone(deferred.anon_lists(group))                      # no file
            (group / 'memory.stat').write_text(f'anon {9 * mib}\nactive_anon {2 * mib}\n')
            self.assertIsNone(deferred.anon_lists(group))                      # one of the two figures is missing
            (group / 'memory.stat').write_text(f'active_anon {2 * mib}\ninactive_anon {mib}\nfile {50 * mib}\n')
            self.assertEqual(deferred.anon_lists(group), 3 * mib)
            readings = iter([3 * mib, None, 7 * mib, 5 * mib, 4 * mib])
            stop = mock.Mock()
            stop.wait.side_effect = [False, False, False, False, False, True]
            result = {}
            with mock.patch.object(deferred, 'anon_lists', side_effect=lambda path: next(readings)):
                deferred.sample_held(group, stop, result)
            self.assertEqual(result, {'samples': 4, 'peak': 7 * mib})          # the unreadable moment is not a reading
            # it waits before every reading (the command has to exist first): closely at the start, then every 5 s
            self.assertEqual([call.args[0] for call in stop.wait.call_args_list], [.2, .5, 1, 2, 5, 5])
            ended_at_once = {}
            stop = mock.Mock()
            stop.wait.return_value = True
            deferred.sample_held(group, stop, ended_at_once)
            self.assertEqual(ended_at_once, {})                                # a job gone before the first wait: no reading
            # the runner's two calls, with a real thread: readings arrive, it stops when told
            (group / 'job').mkdir()
            (group / 'job' / 'memory.stat').write_text(f'active_anon {2 * mib}\ninactive_anon {mib}\n')
            with mock.patch.object(deferred, 'CGROUP', group), mock.patch.object(deferred, 'self_group', return_value='/job'), \
                 mock.patch.object(deferred, 'HELD_SAMPLE_START', (.01,)), mock.patch.object(deferred, 'HELD_SAMPLE_S', .01):
                sampled, stop, thread = deferred.start_sampler()
                deadline = time.time() + 5
                while sampled.get('samples', 0) < 2 and time.time() < deadline:
                    time.sleep(.01)
                peak, samples = deferred.finish_sampler(sampled, stop, thread)
            self.assertFalse(thread.is_alive())
            self.assertEqual(peak, 3 * mib)
            self.assertGreaterEqual(samples, 2)
            # a sampler that cannot start, or read nothing, or was caught half way, costs the job nothing
            with mock.patch.object(deferred, 'self_group', side_effect=RuntimeError('no cgroup')):
                sampled, stop, thread = deferred.start_sampler()
            self.assertIsNone(thread)
            self.assertIsNone(deferred.finish_sampler(sampled, stop, thread))
            with mock.patch.object(deferred.threading, 'Thread', side_effect=ValueError('anything')):
                self.assertIsNone(deferred.start_sampler()[2])
            self.assertIsNone(deferred.finish_sampler({'samples': 1}, threading.Event(), None))
            self.assertIsNone(deferred.finish_sampler({'peak': mib, 'samples': 0}, threading.Event(), None))
            self.assertIsNone(deferred.finish_sampler(None, None, None))
            self.assertEqual(deferred.finish_sampler({'peak': mib, 'samples': 3}, threading.Event(), None), (mib, 3))

    def test_reservations_report_what_each_running_job_holds(self):
        gib = deferred.GIB
        with tempfile.TemporaryDirectory() as temp, tempfile.TemporaryDirectory() as groups:
            for jid in ('cq-a', 'cq-b'):
                (Path(temp) / jid).mkdir()
                group = Path(groups) / ('cq-deferred-' + jid + '.service')
                group.mkdir()
                (group / 'memory.stat').write_text(f'active_anon {2 * gib}\ninactive_anon {gib}\n')
            def state(path):
                jid = path.parent.name
                return {'id': jid, 'fleet': True, 'slots': 1, 'memory_bytes': 4 * gib,
                        'phase': 'running' if jid == 'cq-a' else 'starting',
                        'control_group': '/cq-deferred-' + jid + '.service'}
            with mock.patch.object(deferred, 'Q', Path(temp)), mock.patch.object(deferred, 'CGROUP', Path(groups)), \
                 mock.patch.object(deferred, 'safe'), mock.patch.object(deferred, 'read', side_effect=state):
                active = {row['id']: row for row in deferred.reservations()[0]}
            self.assertEqual(active['cq-a']['held_memory_bytes'], 3 * gib)
            self.assertEqual(active['cq-b']['held_memory_bytes'], 0)  # not running yet: its whole cap is charged
            self.assertEqual(active['cq-a']['reserved_memory_bytes'], 4 * gib)

    def test_admission_charges_a_running_job_only_what_it_does_not_hold_yet(self):
        gib = deferred.GIB
        value = {'fleet': True, 'mode': 'immediate', 'load_cap': 8, 'ram_floor_gb': 2,
                 'memory_max_gb': 3, 'pool_slots': 8, 'slots': 2}
        def reason(held, available, later=None):
            active = [{'id': 'cq-a', 'phase': 'running', 'slots': 2, 'reserved_memory_bytes': 4 * gib,
                       'held_memory_bytes': held}]
            order = []
            def first():
                order.append('held')
                return active, False, False
            def free():
                order.append('available')
                return available
            def second(state):
                order.append('held again')
                return held if later is None else later
            with mock.patch.object(deferred, 'reservations', side_effect=first), \
                 mock.patch.object(deferred, 'legacy_busy', return_value=False), \
                 mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True), \
                 mock.patch.object(deferred, 'available', side_effect=free), \
                 mock.patch.object(deferred, 'read', return_value={'id': 'cq-a'}), \
                 mock.patch.object(deferred, 'held_memory', side_effect=second):
                result = deferred.fleet_reason(value)
            # available memory is read between the two looks at what the job holds; a job holding nothing is not reread
            self.assertEqual(order, ['held', 'available', 'held again'] if held else ['held', 'available'])
            return result
        # A 16 GiB host with 1 GiB in other use: 2 floor + 4 cap + 3 cap fit whatever the running job holds.
        self.assertIn('memory', reason(0, 9 * gib - 1))       # holds nothing: floor 2 + its cap 4 + this cap 3
        self.assertIsNone(reason(0, 9 * gib))
        self.assertIsNone(reason(3 * gib, 6 * gib))           # holds 3 of its 4: available is 3 lower, and so is its charge
        self.assertIn('memory', reason(3 * gib, 6 * gib - 1))
        self.assertIsNone(reason(4 * gib, 5 * gib))           # holds all of it: floor 2 + this cap 3
        # it let go of 2 GiB while available memory was read: that shows as available, so it is not credited too
        self.assertIn('memory', reason(3 * gib, 6 * gib, later=gib))
        self.assertIsNone(reason(3 * gib, 8 * gib, later=gib))
        self.assertIn('memory', reason(3 * gib, 8 * gib - 1, later=gib))
        self.assertIsNone(reason(3 * gib, 6 * gib, later=4 * gib))   # it grew: the earlier, smaller figure stands
        # its record gone or unreadable at the second look: the whole cap is charged
        with mock.patch.object(deferred, 'reservations', return_value=(
                [{'id': 'cq-a', 'phase': 'running', 'slots': 2, 'reserved_memory_bytes': 4 * gib, 'held_memory_bytes': 3 * gib}],
                False, False)), \
             mock.patch.object(deferred, 'legacy_busy', return_value=False), \
             mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True), \
             mock.patch.object(deferred, 'available', return_value=9 * gib - 1), \
             mock.patch.object(deferred, 'read', side_effect=[None, RuntimeError('unsafe')]):
            self.assertIn('memory', deferred.fleet_reason(value))
            self.assertIn('memory', deferred.fleet_reason(value))

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

    def oom_job(self, **state):
        """A fleet job whose unit the kernel killed for memory, in a cgroup tree under self.root: its own cgroup is gone,
        the pool slice's counters remain."""
        pool = self.root / 'user.slice' / 'cq-fleet.slice'
        pool.mkdir(parents=True, exist_ok=True)
        (pool / 'memory.max').write_text(str(20 * deferred.GIB) + '\n')
        group = '/user.slice/cq-fleet.slice/' + deferred.unit('cq-test')
        state.setdefault('pool_ino', pool.stat().st_ino)   # recorded with the counter when the command started
        self.records['cq-test'].update(phase='running', submitted=True, control_group=group, memory_bytes=4 * deferred.GIB, **state)
        props = {'LoadState': 'loaded', 'ActiveState': 'failed', 'SubState': 'failed', 'Job': '0', 'ControlGroup': '',
                 'Result': 'oom-kill'}
        return pool, props

    def reason_of(self, props):
        with mock.patch.object(deferred, 'CGROUP', self.root), mock.patch.object(deferred, 'show', return_value=props):
            state = deferred.status('cq-test')
        self.assertEqual(state['phase'], 'failed')
        self.assertLessEqual(len(state['reason']), 200)   # the probe cuts a terminal job's reason there
        return state['reason']

    def test_a_unit_killed_while_the_pool_limit_was_reached_says_so(self):
        oom = re.compile(r'\boom\b|out[ -]of[ -]memory|memory limit', re.I)   # fleet_telemetry._OOM
        pool, props = self.oom_job(pool_oom_start=2)
        (pool / 'memory.events.local').write_text('low 0\nhigh 0\nmax 9\noom 3\noom_kill 1\n')
        reason = self.reason_of(props)
        self.assertRegex(reason, oom)
        self.assertTrue(reason.startswith(OOM_REASON_PREFIX))
        self.assertIn("pool limit (20.0 GB) was reached between the job's start and the check after its end", reason)
        self.assertIn('its cap was 4 GB', reason)
        self.assertNotIn('while it ran', reason)   # what is known is the interval to the check, not the job's run
        self.assertNotIn('own cap of', reason)

    def test_a_unit_killed_without_the_pool_limit_being_reached_was_at_its_own_cap(self):
        oom = re.compile(r'\boom\b|out[ -]of[ -]memory|memory limit', re.I)
        pool, props = self.oom_job(pool_oom_start=2)
        (pool / 'memory.events.local').write_text('low 0\nhigh 0\nmax 9\noom 2\noom_kill 0\n')
        reason = self.reason_of(props)
        self.assertRegex(reason, oom)
        self.assertIn('at its own cap of 4 GB', reason)
        self.assertIn('pool limit was not reached', reason)

    def test_a_killed_unit_without_a_pool_reading_is_not_blamed_on_either_limit(self):
        # outside the pool (no count recorded at its start)
        pool, props = self.oom_job()
        (pool / 'memory.events.local').write_text('oom 5\n')
        reason = self.reason_of(props)
        self.assertTrue(reason.startswith(OOM_REASON_PREFIX))
        self.assertIn('no fleet pool reading', reason)
        self.assertIn('not known whether', reason)
        # a count below the recorded one: the slice was made again after it emptied
        self.records['cq-test'] = dict(self.base)
        pool, props = self.oom_job(pool_oom_start=7)
        (pool / 'memory.events.local').write_text('oom 1\n')
        self.assertIn('no fleet pool reading', self.reason_of(props))
        # the slice's counters unreadable
        self.records['cq-test'] = dict(self.base)
        pool, props = self.oom_job(pool_oom_start=7)
        (pool / 'memory.events.local').unlink()
        self.assertIn('no fleet pool reading', self.reason_of(props))

    def test_a_slice_made_again_is_not_read_as_one_whose_limit_was_not_reached(self):
        # the counter restarts at 0 in a slice cgroup that was removed when it emptied: 0 at the start and 0 now say nothing
        pool, props = self.oom_job(pool_oom_start=0, pool_ino=-1)
        (pool / 'memory.events.local').write_text('oom 0\n')
        reason = self.reason_of(props)
        self.assertIn('no fleet pool reading', reason)
        self.assertNotIn('was not reached', reason)
        # the same inode: the readings are comparable, and equal counts mean the limit was not reached
        self.records['cq-test'] = dict(self.base)
        pool, props = self.oom_job(pool_oom_start=0)
        (pool / 'memory.events.local').write_text('oom 0\n')
        self.assertIn('pool limit was not reached', self.reason_of(props))
        # an inode that was not recorded (a unit started before it was)
        self.records['cq-test'] = dict(self.base)
        pool, props = self.oom_job(pool_oom_start=0, pool_ino=None)
        (pool / 'memory.events.local').write_text('oom 5\n')
        self.assertIn('no fleet pool reading', self.reason_of(props))
        # a slice directory that is gone
        self.records['cq-test'] = dict(self.base)
        pool, props = self.oom_job(pool_oom_start=0)
        (pool / 'memory.events.local').write_text('oom 5\n')
        (pool / 'memory.events.local').unlink()
        (pool / 'memory.max').unlink()
        pool.rmdir()
        self.assertIn('no fleet pool reading', self.reason_of(props))

    def test_the_longest_oom_reasons_stay_within_the_probes_cut(self):
        pool, props = self.oom_job(pool_oom_start=0)
        (pool / 'memory.events.local').write_text('oom 1\n')
        (pool / 'memory.max').write_text(str(1000 * deferred.GIB) + '\n')
        for cap in (50 * deferred.GIB, 12345 * deferred.GIB // 100, None):
            with self.subTest(cap=cap):
                state = dict(self.records['cq-test'], memory_bytes=cap)
                with mock.patch.object(deferred, 'CGROUP', self.root):
                    reason = deferred.failure_reason(state, props)
                self.assertTrue(reason.startswith(OOM_REASON_PREFIX))
                self.assertLessEqual(len(reason), 200)

    def test_another_failure_keeps_its_old_reason(self):
        pool, props = self.oom_job(pool_oom_start=0)
        (pool / 'memory.events.local').write_text('oom 4\n')
        props['Result'] = 'exit-code'
        self.assertEqual(self.reason_of(props), 'unit exited without success')

    def test_pool_counters_are_read_only_from_a_unit_inside_the_fleet_slice(self):
        pool = self.root / 'user.slice' / 'cq-fleet.slice'
        pool.mkdir(parents=True)
        (pool / 'memory.events.local').write_text('low 0\nmax 3\noom 6\noom_kill 1\n')
        inside = '/user.slice/cq-fleet.slice/cq-deferred-cq-test.service'
        with mock.patch.object(deferred, 'CGROUP', self.root):
            self.assertEqual(deferred.pool_event(inside, 'oom'), 6)
            self.assertEqual(deferred.pool_event(inside, 'oom_kill'), 1)
            self.assertIsNone(deferred.pool_event(inside, 'high'))
            self.assertIsNone(deferred.pool_event('/user.slice/cq-deferred-cq-test.service', 'oom'))  # started outside it
            self.assertIsNone(deferred.pool_event('/user.slice/cq-fleet.slice/../cq-fleet.slice/x.service', 'oom'))
            self.assertIsNone(deferred.pool_event(None, 'oom'))
            (pool / 'memory.events.local').unlink()
            self.assertIsNone(deferred.pool_event(inside, 'oom'))

    def test_the_slice_directorys_inode_is_read_only_for_a_unit_inside_it(self):
        pool = self.root / 'user.slice' / 'cq-fleet.slice'
        pool.mkdir(parents=True)
        inside = '/user.slice/cq-fleet.slice/cq-deferred-cq-test.service'
        with mock.patch.object(deferred, 'CGROUP', self.root):
            self.assertEqual(deferred.pool_inode(inside), pool.stat().st_ino)
            self.assertIsNone(deferred.pool_inode('/user.slice/cq-deferred-cq-test.service'))
            self.assertIsNone(deferred.pool_inode(None))
            pool.rmdir()
            self.assertIsNone(deferred.pool_inode(inside))

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
        with mock.patch.object(deferred, 'staged_state', return_value=(dict(self.base, slice=deferred.FLEET_SLICE), v)), \
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
            self.assertNotIn('slice', self.records['cq-test'])   # the runner is not in the pool slice: the intent is dropped
            spawn.assert_not_called()
        self.records['cq-test'].update(submitted=True, slice=deferred.FLEET_SLICE, pool_ino=7, pool_oom_start=1,
                                         runtime_max_s=630, estimate_s=300, unit_started_by=5.0, passed='cq-other',
                                         held_bound=9.0, held_since=1.0)
        props = {'LoadState': 'loaded', 'ActiveState': 'active', 'Job': '0'}
        result = types.SimpleNamespace(returncode=0, stderr='')
        with mock.patch.object(deferred, 'show', return_value=props), mock.patch.object(deferred, 'command', return_value=result):
            self.assertEqual(deferred.status('cq-test')['phase'], 'starting')
            props['ActiveState'] = 'inactive'
            self.assertEqual(deferred.status('cq-test')['phase'], 'queued')
        for key in ('slice', 'pool_ino', 'pool_oom_start', 'runtime_max_s', 'estimate_s', 'unit_started_by', 'passed',
                    'held_bound', 'held_since'):
            self.assertNotIn(key, self.records['cq-test'])   # a requeued job starts again with none of the old marks

    def test_the_runner_keeps_the_pool_intent_only_when_its_unit_is_in_the_pool_slice(self):
        v = {'pool_slots': 8, 'slots': 2}
        for group, kept in (('/user.slice/user@1000.service/cq-fleet.slice/cq-deferred-cq-test.service', True),
                            ('/user.slice/user@1000.service/cq-deferred-cq-test.service', False)):
            with self.subTest(group=group):
                self.records['cq-test'] = dict(self.base, slice=deferred.FLEET_SLICE)
                with mock.patch.object(deferred, 'staged_state', return_value=(dict(self.records['cq-test']), v)), \
                     mock.patch.object(deferred, 'locked', side_effect=lambda p: contextlib.nullcontext()), \
                     mock.patch.object(deferred, 'try_gate', return_value=None), \
                     mock.patch.object(deferred, 'self_group', return_value=group):
                    deferred.runner('cq-test', 'claim')
                self.assertEqual(self.records['cq-test']['control_group'], group)
                self.assertEqual(self.records['cq-test'].get('slice'), deferred.FLEET_SLICE if kept else None)

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

    def test_a_unit_takes_its_charged_slots_from_the_pool_and_keeps_its_declared_ones_as_its_limit(self):
        self.values['cq-batch-0'].update(slots=4, cpu_expected=1.2)
        with mock.patch.object(deferred, 'command') as launch:
            deferred.tick()
        first, second = (call.args for call in launch.call_args_list[:2])
        self.assertIn('--property=CPUQuota=400%', first)                       # the limit is what the job declared
        self.assertEqual(deferred.slots.call_args_list[0].args[1], 2)          # the locks are what it is expected to use
        self.assertEqual((self.states['cq-batch-0']['slots'], self.states['cq-batch-0']['quota_slots']), (2, 4))
        self.assertIn('--property=CPUQuota=100%', second)                      # no expectation: charged as declared
        self.assertEqual((self.states['cq-batch-1']['slots'], self.states['cq-batch-1']['quota_slots']), (1, 1))

    def test_fleet_units_start_in_the_pool_slice_with_their_expectation_stored(self):
        self.values['cq-batch-0'].update(memory_max_gb=1, memory_expected_gb=.5)
        with mock.patch.object(deferred, 'command') as launch:
            deferred.tick()
        first, second = (call.args for call in launch.call_args_list[:2])
        self.assertIn('--slice=cq-fleet.slice', first)
        self.assertIn('--slice=cq-fleet.slice', second)
        self.assertEqual(self.states['cq-batch-0']['memory_bytes'], deferred.GIB)
        self.assertEqual(self.states['cq-batch-0']['memory_expected_bytes'], deferred.GIB // 2)
        self.assertNotIn('memory_expected_bytes', self.states['cq-batch-1'])   # no expectation in its descriptor: its cap
        self.assertEqual(self.states['cq-batch-0']['slice'], 'cq-fleet.slice')   # reservations counts it as pooled until its runner says

    def test_an_ordinary_unit_stays_out_of_the_pool_slice(self):
        jid = 'cq-ordinary'
        (self.root / jid).mkdir()
        self.states[jid] = {'id': jid, 'phase': 'queued', 'fleet': False, 'registered': 1}
        self.values[jid] = {'fleet': False, 'pool_slots': 8, 'slots': 1, 'memory_max_gb': 1, 'timeout_minutes': 10}
        with mock.patch.object(deferred, 'eligibility', return_value=None), \
             mock.patch.object(deferred, 'ordinary_waiter', return_value=(True, None)), \
             mock.patch.object(deferred, 'host_policy', return_value={'x': 1}), \
             mock.patch.object(deferred, 'total_memory', return_value=8 * deferred.GIB), \
             mock.patch.object(deferred, 'command') as launch:
            result = deferred.tick()
        self.assertEqual(result['started'], [jid])
        self.assertFalse(any(arg.startswith('--slice') for arg in launch.call_args.args))
        self.assertNotIn('slice', self.states[jid])

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


class DiskFloorTests(unittest.TestCase):
    """No fleet job starts while the job directories' file system has less than DISK_FLOOR_BYTES free."""
    VALUE = {'fleet': True, 'mode': 'immediate', 'load_cap': 20, 'ram_floor_gb': 4, 'memory_max_gb': 2, 'pool_slots': 8, 'slots': 1}

    def reason(self, free):
        with mock.patch.object(deferred, 'reservations', return_value=([], False, False)), \
             mock.patch.object(deferred, 'legacy_busy', return_value=False), \
             mock.patch.object(deferred, 'available_after', return_value=60 * deferred.GIB), \
             mock.patch.object(deferred, 'expected_mode', return_value=(False, 'test')), \
             mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True), \
             mock.patch.object(deferred, 'queue_free', return_value=free):
            return deferred.fleet_reason(dict(self.VALUE))

    def test_fleet_reason_names_a_nearly_full_job_disk(self):
        self.assertEqual(self.reason(deferred.DISK_FLOOR_BYTES - 1), 'queue disk below floor')
        self.assertEqual(self.reason(0), 'queue disk below floor')
        self.assertIsNone(self.reason(deferred.DISK_FLOOR_BYTES))
        self.assertIsNone(self.reason(None))   # a file system that cannot be read holds nothing back
        # host-wide: nothing may be started past a job that waits for it
        self.assertNotIn('queue disk below floor', deferred.BACKFILL_REASONS)

    def test_queue_free_reads_the_job_directory_and_survives_its_absence(self):
        with tempfile.TemporaryDirectory() as temp:   # the real function: setUpModule patches the module's name for it
            with mock.patch.object(deferred, 'Q', Path(temp)):
                self.assertGreater(REAL_QUEUE_FREE(), 0)
            with mock.patch.object(deferred, 'Q', Path(temp) / 'missing'):
                self.assertIsNone(REAL_QUEUE_FREE())


class BackfillTests(unittest.TestCase):
    """A queued fleet job blocked only by its own size is held; a later one may start past it when that costs the held
    job nothing beyond the moment the running jobs' hard ends guarantee it room. The pool has 8 slots and 2 are
    foreground, so 6 are for the fleet; memory is plentiful unless a test says otherwise (slots decide)."""
    NOW = 1_000_000.0

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.states, self.values = {}, {}
        self.now = self.NOW
        self.gate = 99
        self.pool_free = True
        self.free_gb = 20

        def read(path):
            if path.name == 'deferred-state.json':
                state = self.states.get(path.parent.name)
                return dict(state) if state else None
            value = self.values.get(path.parent.name)
            return dict(value) if path.name == 'deferred.json' and value else None

        def save(jid, state):
            self.states[jid] = dict(state)
        self.patches = [mock.patch.object(deferred, 'Q', self.root),
                        mock.patch.object(deferred, 'safe'),
                        mock.patch.object(deferred, 'read', side_effect=read),
                        mock.patch.object(deferred, 'save', side_effect=save),
                        mock.patch.object(deferred, 'status'),
                        mock.patch.object(deferred, 'staged_state', side_effect=lambda jid: (dict(self.states[jid]), self.values[jid])),
                        mock.patch.object(deferred, 'legacy_busy', return_value=False),
                        mock.patch.object(deferred, 'foreground_waiter', return_value=False),
                        mock.patch.object(deferred, 'available', side_effect=lambda: self.free_gb * deferred.GIB),
                        mock.patch.object(deferred, 'held_memory', return_value=0),
                        mock.patch.object(deferred, 'pool_dir', return_value=self.root),
                        mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True),
                        mock.patch.object(deferred, 'try_gate', side_effect=lambda shared=False: self.gate),
                        mock.patch.object(deferred.os, 'close'),
                        mock.patch.object(deferred, 'pool_empty', side_effect=lambda pool: self.pool_free),
                        mock.patch.object(deferred, 'slots', return_value=[98]),
                        mock.patch.object(deferred, 'boot', return_value='boot'),
                        mock.patch.object(deferred.time, 'time', side_effect=lambda: self.now)]
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)

    def queue(self, jid, slots, registered, memory_gb=2, timeout_minutes=10, **extra):
        (self.root / jid).mkdir(exist_ok=True)
        self.states[jid] = {'id': jid, 'phase': 'queued', 'fleet': True, 'registered': registered}
        self.values[jid] = {'fleet': True, 'mode': 'immediate', 'slots': slots, 'pool_slots': 8, 'memory_max_gb': memory_gb,
                            'ram_floor_gb': 1, 'load_cap': 8, 'est_minutes': 5, 'timeout_minutes': timeout_minutes, **extra}

    def running(self, jid, slots, ended_in, memory_gb=2, phase='running', expected_in=None):
        """A fleet job whose hard end (RuntimeMaxSec plus the grace) is `ended_in` seconds from now, and which is
        expected to end `expected_in` seconds from now (by default a minute before its hard end; 0 or less: it is
        past its estimate)."""
        (self.root / jid).mkdir(exist_ok=True)
        started = self.now - 100   # it started 100 s ago, with the RuntimeMaxSec that ends it `ended_in` from now
        runtime = ended_in + 100 - deferred.HARD_END_GRACE_S
        estimate = runtime if expected_in is None else max(1, int(expected_in) + 100)
        self.states[jid] = {'id': jid, 'phase': phase, 'fleet': True, 'slots': slots, 'registered': 0,
                            'memory_bytes': memory_gb * deferred.GIB, 'runtime_max_s': runtime, 'estimate_s': estimate,
                            'unit_started_by': started, 'command_started': started + 2}
        if phase == 'running':
            self.states[jid]['control_group'] = '/x/' + deferred.unit(jid)
        else:
            self.states[jid]['slice'] = deferred.FLEET_SLICE

    def tick(self):
        with mock.patch.object(deferred, 'command') as launch:
            result = deferred.tick()
        return result, launch

    def hold_bound(self, jid):
        return deferred.backfill_hold(self.values[jid], self.now)

    def two_running(self, **kwargs):
        """4 of the 6 fleet slots busy: the first job ends in an hour, the other in two. cq-big (3 slots) fits only
        once the first has gone."""
        self.running('cq-run-1', 2, 3600, **kwargs)
        self.running('cq-run-2', 2, 7200, **kwargs)
        self.queue('cq-big', 3, 1)

    def test_a_small_job_whose_hard_end_is_within_the_bound_passes_a_blocked_larger_one(self):
        self.two_running()
        self.queue('cq-small', 2, 2, timeout_minutes=10)
        result, launch = self.tick()
        self.assertEqual(result['started'], ['cq-small'])
        self.assertEqual(result['blocked_id'], 'cq-big')
        self.assertEqual(result['reason'], 'fleet CPU pool exhausted')
        self.assertEqual(self.states['cq-big']['reason'], 'fleet CPU pool exhausted')   # the held job keeps its capacity reason
        self.assertEqual(self.states['cq-big']['phase'], 'queued')
        self.assertEqual(self.states['cq-small']['phase'], 'starting')
        self.assertEqual(self.states['cq-small']['passed'], 'cq-big')
        self.assertEqual(self.states['cq-small']['runtime_max_s'], 630)
        self.assertEqual(self.states['cq-small']['unit_started_by'], self.NOW)
        self.assertIn('--property=RuntimeMaxSec=630', launch.call_args.args)
        self.assertNotIn('passed', self.states['cq-run-1'])

    def test_a_job_that_can_outlast_the_bound_waits_when_it_would_take_the_room_the_held_job_needs(self):
        self.two_running()
        self.queue('cq-long', 2, 2, timeout_minutes=120)   # ends after the first running job: 2 + 2 + 3 slots at that moment
        result, launch = self.tick()
        launch.assert_not_called()
        self.assertEqual(result['id'], 'cq-big')
        self.assertEqual(result['reason'], 'fleet CPU pool exhausted')
        self.assertEqual(self.states['cq-long']['phase'], 'queued')
        self.assertIn('waiting behind cq-big', self.states['cq-long']['reason'])
        self.assertNotIn('passed', self.states['cq-long'])

    def test_a_job_that_can_outlast_the_bound_passes_when_the_held_job_still_fits_beside_it(self):
        self.two_running()
        self.queue('cq-long', 1, 2, timeout_minutes=120)   # 2 + 1 + 3 slots of 6 at the bound
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-long'])
        self.assertEqual(self.states['cq-long']['passed'], 'cq-big')

    def test_memory_counts_against_the_bound_by_caps(self):
        # 12 GiB available less the 1 GiB floor leaves 11 GiB of caps. At the bound the first running job (1 GiB) is gone:
        # the second (2) and the held job (6) leave 3 GiB for a job that outlasts the bound, though 8 GiB are free now
        for memory_gb, passes in ((3, True), (4, False)):
            with self.subTest(memory_gb=memory_gb):
                self.setUp()
                self.free_gb = 12
                self.running('cq-run-1', 2, 3600, memory_gb=1)
                self.running('cq-run-2', 2, 7200, memory_gb=2)
                self.queue('cq-big', 3, 1, memory_gb=6)
                self.queue('cq-long', 1, 2, memory_gb=memory_gb, timeout_minutes=120)
                result, _ = self.tick()
                self.assertEqual(result.get('started', []), ['cq-long'] if passes else [])
                self.assertEqual(self.states['cq-big']['reason'], 'fleet CPU pool exhausted')
                if not passes:
                    self.assertIn('waiting behind cq-big', self.states['cq-long']['reason'])

    def test_nothing_passes_for_a_host_wide_reason(self):
        for name in ('legacy or ordinary deferred work active or waiting', 'load above cap', 'heavy gate busy',
                     'legacy heavy slots held'):
            with self.subTest(name):
                self.setUp()
                self.running('cq-run-1', 2, 3600, phase='starting')   # a pool that has no running job: pool_empty is asked
                self.running('cq-run-2', 2, 7200, phase='starting')
                self.queue('cq-big', 3, 1)
                self.queue('cq-small', 2, 2)
                self.queue('cq-after', 1, 3)
                if name.startswith('legacy or'):
                    deferred.legacy_busy.side_effect = [False, True]   # cq-big is asked first, cq-small second
                elif name == 'load above cap':
                    self.values['cq-small']['load_cap'] = .5
                elif name == 'heavy gate busy':
                    deferred.try_gate.side_effect = [99, None]
                else:
                    deferred.pool_empty.side_effect = lambda pool: False
                result, launch = self.tick()
                launch.assert_not_called()
                self.assertEqual(result['reason'], name)
                self.assertEqual(result['id'], 'cq-small')
                self.assertEqual(self.states['cq-small']['reason'], name)
                self.assertEqual(self.states['cq-small']['phase'], 'queued')
                self.assertEqual(self.states['cq-after']['phase'], 'queued')
                self.assertEqual(self.states['cq-big']['reason'], 'fleet CPU pool exhausted')

    def test_an_error_stops_everything_behind_the_held_job(self):
        self.two_running()
        self.queue('cq-small', 2, 2)
        self.queue('cq-after', 1, 3)
        with mock.patch.object(deferred, 'command', side_effect=subprocess.TimeoutExpired('systemd-run', 30)):
            result = deferred.tick()
        self.assertEqual(result['id'], 'cq-small')
        self.assertIn('timed out', result['error'])
        self.assertEqual(self.states['cq-after']['phase'], 'queued')
        self.assertFalse(self.states['cq-small']['submitted'])

    def test_no_bound_no_pass(self):
        cases = {'no saved runtime': lambda s: s.pop('runtime_max_s'),
                 'no start time': lambda s: (s.pop('unit_started_by'), s.pop('command_started')),
                 'a start in the future': lambda s: s.update(unit_started_by=self.now + 3600, command_started=self.now + 3600),
                 'a malformed runtime': lambda s: s.update(runtime_max_s='3600'),
                 'no saved estimate': lambda s: s.pop('estimate_s')}
        for name, damage in cases.items():
            with self.subTest(name):
                self.setUp()
                self.two_running()
                self.queue('cq-small', 2, 2)
                damage(self.states['cq-run-1'])
                result, launch = self.tick()
                launch.assert_not_called()
                self.assertEqual(result['id'], 'cq-big')   # as before: the blocked job stops the batch
                self.assertEqual(result['reason'], 'fleet CPU pool exhausted')
                self.assertEqual(self.states['cq-small']['phase'], 'queued')
                self.assertNotIn('reason', self.states['cq-small'])   # not even looked at

    def test_a_row_outside_the_pool_or_past_its_hard_end_gives_no_bound(self):
        self.two_running()
        self.assertIsNotNone(self.hold_bound('cq-big'))
        with mock.patch.object(deferred, 'pool_dir', return_value=None):
            self.assertIsNone(self.hold_bound('cq-big'))
        self.running('cq-run-1', 2, -10)   # still listed after its hard end plus the grace
        self.assertIsNone(self.hold_bound('cq-big'))

    def test_a_held_job_that_would_not_fit_even_with_everything_gone_gives_no_bound(self):
        self.two_running()
        self.values['cq-big']['memory_max_gb'] = 30
        self.assertIsNone(self.hold_bound('cq-big'))

    def test_the_bound_is_the_hard_end_that_makes_room(self):
        self.two_running()
        self.assertEqual(self.hold_bound('cq-big')['bound'], self.NOW + 3600)
        self.queue('cq-huge', 5, 1)   # needs both gone
        self.assertEqual(self.hold_bound('cq-huge')['bound'], self.NOW + 7200)
        self.queue('cq-fits', 2, 1)   # room already by the rows: whatever else keeps it out, there is no bound to give
        self.assertIsNone(self.hold_bound('cq-fits'))

    def test_only_the_first_held_job_defines_the_bound(self):
        self.two_running()
        self.queue('cq-big-2', 3, 2)   # also too big; it is skipped, not a new bound
        self.queue('cq-small', 2, 3)
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-small'])
        self.assertEqual(result['blocked_id'], 'cq-big')
        self.assertEqual(self.states['cq-small']['passed'], 'cq-big')
        self.assertEqual(self.states['cq-big-2']['phase'], 'queued')
        self.assertEqual(self.states['cq-big-2']['reason'], 'fleet CPU pool exhausted')
        self.assertNotIn('passed', self.states['cq-big-2'])

    def test_the_bound_never_moves_later_across_passes_and_ticks(self):
        self.two_running()
        bound = self.hold_bound('cq-big')['bound']
        self.queue('cq-small', 1, 2, timeout_minutes=10)
        self.queue('cq-long', 1, 3, timeout_minutes=120)   # past the bound, but leaves the held job its room
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-small', 'cq-long'])
        self.assertEqual(self.states['cq-long']['passed'], 'cq-big')
        self.assertLessEqual(self.hold_bound('cq-big')['bound'], bound)
        self.now += 300
        self.queue('cq-small-2', 1, 4, timeout_minutes=5)
        result, _ = self.tick()
        self.assertEqual(result.get('started', []), [])   # 4 + 2 + 1 slots in use: nothing more fits
        self.states['cq-small'].update(phase='done')
        self.queue('cq-small-3', 1, 5, timeout_minutes=5)
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-small-2'])   # the freed slot; cq-small-3 is too big for now and is skipped
        self.assertLessEqual(self.hold_bound('cq-big')['bound'], bound)

    def test_a_bound_that_has_moved_later_lets_nothing_pass(self):
        self.two_running()
        hold = self.hold_bound('cq-big')
        self.queue('cq-small', 2, 2)
        active, _, _ = deferred.reservations()
        self.assertTrue(deferred.may_pass(hold, active, self.values['cq-small'], self.now))
        self.assertFalse(deferred.may_pass(dict(hold, bound=hold['bound'] - 1), active, self.values['cq-small'], self.now))

    def test_the_held_job_starts_first_once_it_fits(self):
        self.two_running()
        self.queue('cq-small', 2, 2)
        self.tick()
        self.assertEqual(self.states['cq-big']['held_bound'], self.NOW + 3600)   # kept while it is held
        self.assertEqual(self.states['cq-big']['held_since'], self.NOW)
        self.states['cq-small'].update(phase='done')
        self.states['cq-run-1'].update(phase='done')   # the first running job has ended
        self.now += 3000
        self.queue('cq-small-2', 1, 3)
        result, _ = self.tick()
        self.assertEqual(result['started'][0], 'cq-big')
        self.assertNotIn('passed', self.states['cq-big'])
        self.assertNotIn('blocked_id', result)
        for key in ('held_bound', 'held_since'):
            self.assertNotIn(key, self.states['cq-big'])   # no longer held

    def test_ordinary_jobs_keep_their_behaviour(self):
        self.two_running()
        jid = 'cq-ordinary'
        (self.root / jid).mkdir()
        self.states[jid] = {'id': jid, 'phase': 'queued', 'fleet': False, 'registered': 1}
        self.values[jid] = {'fleet': False, 'pool_slots': 8, 'slots': 1, 'memory_max_gb': 1, 'timeout_minutes': 10}
        with mock.patch.object(deferred, 'ordinary_waiter', return_value=(True, None)), \
             mock.patch.object(deferred, 'host_policy', return_value={'x': 1}), \
             mock.patch.object(deferred, 'total_memory', return_value=32 * deferred.GIB):
            result, launch = self.tick()
        launch.assert_not_called()
        self.assertEqual(self.states[jid]['reason'], 'fleet reservation active; waiting for pool to drain')
        self.assertNotIn('passed', self.states[jid])
        self.assertNotIn('runtime_max_s', self.states[jid])

    def test_a_waiting_ordinary_job_still_stops_all_fleet_admission(self):
        self.two_running()
        self.queue('cq-small', 2, 2)
        jid = 'cq-ordinary'
        (self.root / jid).mkdir()
        self.states[jid] = {'id': jid, 'phase': 'queued', 'fleet': False, 'registered': 1}
        self.values[jid] = {'fleet': False, 'pool_slots': 8, 'slots': 1}
        with mock.patch.object(deferred, 'ordinary_waiter', return_value=(True, None)), \
             mock.patch.object(deferred, 'host_policy', return_value={'x': 1}), \
             mock.patch.object(deferred, 'total_memory', return_value=32 * deferred.GIB):
            result, launch = self.tick()
        launch.assert_not_called()
        self.assertEqual(result['reason'], 'legacy or ordinary deferred work active or waiting')
        self.assertEqual(self.states['cq-big']['reason'], 'legacy or ordinary deferred work active or waiting')
        self.assertEqual(self.states['cq-small']['phase'], 'queued')

    def test_small_jobs_fill_the_free_slots_past_the_held_one(self):
        self.running('cq-run-1', 1, 7200)
        self.queue('cq-big', 6, 1)
        for index in range(9):
            self.queue('cq-s-%d' % index, 1, 2 + index, memory_gb=1, timeout_minutes=5)
        result, _ = self.tick()
        self.assertEqual(len(result['started']), 5)   # the 6 fleet slots less the running job's 1
        self.assertEqual(result['blocked_id'], 'cq-big')

    # ---- start latency, the bound kept across ticks, the estimate rule and the other guards ----

    def test_a_job_whose_unit_is_recorded_as_started_late_does_not_move_the_bound_later(self):
        # systemd-run may take up to COMMAND_TIMEOUT_S; unit_started_by is saved after it returns. cq-pass ends 20 s
        # before the bound by its hard end as of the tick's own time, and 5 s later by the time it is recorded.
        self.running('cq-run-1', 2, 3590)
        self.running('cq-run-2', 2, 7200)
        self.queue('cq-big', 3, 1)
        self.queue('cq-pass', 2, 2, timeout_minutes=58)   # 58 * 60 + 30 + 60 s of grace: ends at NOW + 3570
        before = self.hold_bound('cq-big')['bound']
        self.assertEqual(before, self.NOW + 3590)
        self.assertGreaterEqual(before - (self.NOW + 3570), 1)
        self.assertLess(before - (self.NOW + 3570), deferred.START_SLACK_S)

        def slow_systemd_run(*args):
            self.now += 25   # the call took 25 s: the start is recorded 25 s after the job was judged
        with mock.patch.object(deferred, 'command', side_effect=slow_systemd_run) as launch:
            result = deferred.tick()
        self.assertEqual(launch.call_count, 0)
        self.assertEqual(result.get('started', []), [])   # refused: its hard end plus the allowed latency is past the bound
        self.assertLessEqual(self.hold_bound('cq-big')['bound'], before)
        # a job with that much margin goes, and the bound recomputed with its recorded start is not later
        self.queue('cq-pass-2', 2, 3, timeout_minutes=50)
        with mock.patch.object(deferred, 'command', side_effect=slow_systemd_run):
            result = deferred.tick()
        self.assertEqual(result['started'], ['cq-pass-2'])
        self.assertEqual(self.states['cq-pass-2']['unit_started_by'], self.NOW + 25)
        self.assertLessEqual(deferred.backfill_hold(self.values['cq-big'], self.now)['bound'], before)

    def test_the_bound_is_kept_on_the_held_job_and_a_later_one_lets_nothing_pass(self):
        self.two_running()
        self.queue('cq-small', 2, 2)
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-small'])
        self.assertEqual((self.states['cq-big']['held_bound'], self.states['cq-big']['held_since']), (self.NOW + 3600, self.NOW))
        # memory outside the fleet grows: the fresh bound would be the second job's end, an hour later. Nothing passes.
        self.states['cq-small'].update(phase='done')
        self.queue('cq-small-2', 1, 3, memory_gb=1)
        self.running('cq-run-1', 2, 3600, memory_gb=1)
        self.running('cq-run-2', 2, 7200, memory_gb=2)
        self.values['cq-big']['memory_max_gb'] = 6
        self.free_gb = 8
        self.now += 10
        result, launch = self.tick()
        launch.assert_not_called()
        self.assertEqual(result['id'], 'cq-big')
        self.assertEqual(self.states['cq-big']['held_bound'], self.NOW + 3600)   # the stored value stays
        self.assertEqual(self.states['cq-small-2']['phase'], 'queued')

    def test_an_earlier_bound_replaces_the_stored_one(self):
        self.two_running()
        self.tick()
        self.assertEqual(self.states['cq-big']['held_bound'], self.NOW + 3600)
        self.running('cq-run-1', 2, 1800)   # its hard end is earlier than it was thought
        self.tick()
        self.assertEqual(self.states['cq-big']['held_bound'], self.NOW + 1800)
        self.assertEqual(self.states['cq-big']['held_since'], self.NOW)

    def test_once_the_stored_bound_has_passed_nothing_passes_a_job_still_held(self):
        self.running('cq-run-1', 2, 3)   # ends in 3 s, within the slack of the stored bound
        self.running('cq-run-2', 2, 7200)
        self.queue('cq-big', 3, 1)
        self.queue('cq-long', 1, 2, timeout_minutes=120)   # would pass through the room the held job keeps beside it
        self.states['cq-big'].update(held_bound=self.NOW - 1, held_since=self.NOW - 600)
        result, launch = self.tick()
        launch.assert_not_called()
        self.assertEqual(result['id'], 'cq-big')
        self.assertEqual(self.states['cq-big']['held_bound'], self.NOW - 1)
        # the same without a stored bound: it passes
        self.states['cq-big'].pop('held_bound')
        self.states['cq-big'].pop('held_since')
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-long'])

    def test_a_clock_stepped_back_fails_closed(self):
        self.two_running()
        self.queue('cq-small', 2, 2)
        self.states['cq-big'].update(held_bound=self.NOW + 3600, held_since=self.NOW + 60)   # saved a minute "in the future"
        result, launch = self.tick()
        launch.assert_not_called()
        self.assertEqual(result['id'], 'cq-big')

    def test_a_short_estimate_passes_and_a_long_one_that_takes_the_held_jobs_room_does_not(self):
        # the first running job's hard end is an hour away but it is expected to end in ten minutes
        self.running('cq-run-1', 2, 3600, expected_in=600)
        self.running('cq-run-2', 2, 7200, expected_in=7000)
        self.queue('cq-big', 3, 1)
        self.queue('cq-long-estimate', 2, 2, timeout_minutes=10, est_minutes=20)   # its hard end fits the bound, its estimate not
        self.queue('cq-short-estimate', 2, 3, timeout_minutes=10, est_minutes=5)
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-short-estimate'])
        self.assertIn('waiting behind cq-big', self.states['cq-long-estimate']['reason'])
        # the hard rule alone would have let the long estimate through
        self.assertEqual(self.states['cq-big']['held_bound'], self.NOW + 3600)

    def test_a_job_that_would_not_take_the_held_jobs_room_passes_whatever_its_estimate(self):
        self.running('cq-run-1', 2, 3600, expected_in=600)
        self.running('cq-run-2', 2, 7200, expected_in=7000)
        self.queue('cq-big', 3, 1)
        self.queue('cq-narrow', 1, 2, timeout_minutes=120, est_minutes=60)   # 2 + 1 + 3 slots of 6 when the first is gone
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-narrow'])

    def test_a_running_job_past_its_estimate_lets_nothing_pass_that_takes_the_held_jobs_room(self):
        self.running('cq-run-1', 2, 3600, expected_in=0)   # past its estimate: it may end at any moment
        self.running('cq-run-2', 2, 7200)
        self.queue('cq-big', 3, 1)
        self.queue('cq-wide', 2, 2)
        self.queue('cq-narrow', 1, 3)
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-narrow'])
        self.assertIn('waiting behind cq-big', self.states['cq-wide']['reason'])

    def test_a_held_job_blocked_by_the_slot_locks_lets_nothing_pass_under_a_constant_clock(self):
        # the accounting sees room for it (4 of 6 fleet slots are free beside one running job), so it has no bound
        self.running('cq-run-1', 2, 3600)
        self.queue('cq-big', 3, 1)
        self.queue('cq-small', 1, 2)
        deferred.slots.side_effect = [None, [98]]   # cq-big finds the heavy slot locks held, cq-small does not
        result, launch = self.tick()
        launch.assert_not_called()
        self.assertEqual(result['id'], 'cq-big')
        self.assertEqual(result['reason'], 'insufficient free heavy slots and foreground headroom')
        self.assertNotIn('held_bound', self.states['cq-big'])
        self.assertEqual(self.states['cq-small']['phase'], 'queued')

    def test_the_memory_reason_uses_what_the_running_jobs_hold(self):
        # 8 GiB are available and the first running job holds 3 of its 4: the held job, 6 GiB beside the floor of 1, needs
        # 1 + (4 - 3) + 2 + 6 = 10. With what is held added back, 8 + 3 - 1 = 10 GiB are there once the first job is gone.
        deferred.held_memory.side_effect = lambda state: 3 * deferred.GIB if state.get('id') == 'cq-run-1' else 0
        self.free_gb = 8
        self.running('cq-run-1', 2, 3600, memory_gb=4)
        self.running('cq-run-2', 2, 7200, memory_gb=2)
        self.queue('cq-big', 1, 1, memory_gb=6)
        self.queue('cq-small', 1, 2, memory_gb=1)
        self.queue('cq-fat', 1, 3, memory_gb=3, timeout_minutes=120)   # 2 + 3 + 6 = 11 GiB beside the held job at the bound
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-small'])
        self.assertEqual(result['blocked_id'], 'cq-big')
        self.assertEqual(result['reason'], 'insufficient available memory for fleet reservations and floor')
        self.assertEqual(self.states['cq-big']['held_bound'], self.NOW + 3600)
        self.assertIn('waiting behind cq-big', self.states['cq-fat']['reason'])

    def test_the_start_cap_is_reached_while_a_job_is_held_and_the_result_keeps_the_blocked_job(self):
        self.running('cq-run-1', 2, 7200)
        self.queue('cq-big', 9, 1, pool_slots=12, memory_gb=1)   # 10 fleet slots: 2 + 9 do not fit
        for index in range(9):
            self.queue('cq-s-%d' % index, 1, 2 + index, memory_gb=1, timeout_minutes=5, pool_slots=12)
        result, launch = self.tick()
        self.assertEqual(len(result['started']), 8)   # min(8, pool_slots)
        self.assertEqual(result['blocked_id'], 'cq-big')
        self.assertEqual(result['reason'], 'fleet CPU pool exhausted')
        self.assertEqual(launch.call_count, 8)
        self.assertEqual(self.states['cq-s-8']['phase'], 'queued')

    def test_a_newer_higher_priority_job_that_is_not_yet_aged_is_ordered_first_and_takes_the_held_place(self):
        # the order is queue_order's, as before: the older job is held first, then a newer verify job (an hour not yet
        # passed) goes ahead of it, is held itself with a bound of its own, and the older job is a later job
        self.two_running()
        self.states['cq-big'].update(registered=self.NOW - 600, priority='lane')
        self.queue('cq-new-verify', 3, self.NOW - 10)
        self.states['cq-new-verify']['priority'] = 'verify'
        self.queue('cq-small', 2, self.NOW - 5)
        result, _ = self.tick()
        self.assertEqual(result['blocked_id'], 'cq-new-verify')
        self.assertEqual(result['started'], ['cq-small'])
        self.assertEqual(self.states['cq-small']['passed'], 'cq-new-verify')
        self.assertIn('held_bound', self.states['cq-new-verify'])
        self.assertNotIn('held_bound', self.states['cq-big'])   # it was skipped: it makes no bound of its own

    def test_a_job_ages_from_its_submission_when_its_controller_gave_it(self):
        # 4 of the 6 fleet slots are busy and neither 3-slot job fits. The lane job reached this host ten seconds ago,
        # handed over after waiting over an hour with its controller; the verify job came twenty seconds ago.
        self.running('cq-run-1', 2, 3600)
        self.running('cq-run-2', 2, 7200)
        self.queue('cq-lane', 3, self.NOW - 10)
        self.states['cq-lane'].update(priority='lane')
        self.queue('cq-verify', 3, self.NOW - 20)
        self.states['cq-verify'].update(priority='verify')
        result, _ = self.tick()
        self.assertEqual(result['id'], 'cq-verify')                                   # by registration it is the newer work's turn
        for state in self.states.values():
            state.pop('held_bound', None)
            state.pop('held_since', None)
        self.states['cq-lane']['submitted_at'] = self.NOW - 3700
        result, _ = self.tick()
        self.assertEqual(result['id'], 'cq-lane')                                     # it has waited its hour: first, as with its controller
        self.assertEqual(deferred.waiting_since(self.states['cq-lane']), self.NOW - 3700)
        # a time after the registration is another clock's: the registration stands
        self.assertEqual(deferred.waiting_since({'registered': 100, 'submitted_at': 101}), 100)
        self.assertEqual(deferred.waiting_since({'registered': 100, 'submitted_at': 'soon'}), 100)
        self.assertEqual(deferred.waiting_since({'registered': 100}), 100)
        self.assertEqual(deferred.waiting_since({'registered': 100, 'submitted': True, 'submitted_at': 40}), 40)
        with self.assertRaisesRegex(RuntimeError, 'invalid registration time'):
            deferred.waiting_since({'registered': 'x'})

    def test_registration_keeps_the_submission_time_apart_from_the_units_submitted_mark(self):
        (self.root / 'cq-new').mkdir()
        value = {'fleet': True, 'priority': 'lane', 'submitted': self.NOW - 500.7}
        with mock.patch.object(deferred, 'desc', return_value=value), mock.patch.object(deferred, 'check'), \
                mock.patch.object(deferred, 'host_policy', return_value={}):
            state = deferred.register('cq-new')
        self.assertEqual((state['registered'], state['submitted_at']), (int(self.NOW), int(self.NOW - 500.7)))
        self.assertNotIn('submitted', state)                                                  # the tick's mark, set at the claim
        # the claim sets that mark and a start that did not happen takes it away again: the time stays
        self.states['cq-new']['submitted'] = True
        self.assertEqual(deferred.waiting_since(self.states['cq-new']), int(self.NOW - 500.7))
        del self.states['cq-new']['submitted']
        self.assertEqual(deferred.waiting_since(self.states['cq-new']), int(self.NOW - 500.7))
        (self.root / 'cq-late').mkdir()                                                       # a controller's clock ahead of this one
        with mock.patch.object(deferred, 'desc', return_value=dict(value, submitted=self.NOW + 90)), \
                mock.patch.object(deferred, 'check'), mock.patch.object(deferred, 'host_policy', return_value={}):
            self.assertEqual(deferred.register('cq-late')['submitted_at'], int(self.NOW))

    def test_recall_takes_only_a_fleet_job_that_has_not_started(self):
        self.queue('cq-waits', 2, self.NOW - 300)
        self.states['cq-waits'].update(held_bound=self.NOW + 50, held_since=self.NOW - 5)
        state = deferred.recall('cq-waits')
        self.assertEqual((state['phase'], state['reason']), ('cancelled', deferred.RECALLED))
        self.assertEqual(self.states['cq-waits']['phase'], 'cancelled')
        self.assertNotIn('held_bound', self.states['cq-waits'])
        result, launch = self.tick()                                                          # and it never starts here
        launch.assert_not_called()
        self.assertEqual(deferred.recall('cq-waits')['reason'], deferred.RECALLED)            # asked again: the same answer
        self.running('cq-runs', 2, 3600)
        self.assertEqual(deferred.recall('cq-runs')['phase'], 'running')                      # started: left alone
        self.assertEqual(self.states['cq-runs']['phase'], 'running')
        self.queue('cq-night', 2, self.NOW - 300)
        self.states['cq-night']['fleet'] = False                                              # an ordinary deferred job is not the fleet's
        self.assertEqual(deferred.recall('cq-night')['phase'], 'queued')
        (self.root / 'cq-unknown').mkdir()
        self.assertEqual(deferred.recall('cq-unknown')['phase'], 'absent')

    def test_an_odd_row_or_descriptor_means_may_not_pass_and_does_not_raise(self):
        self.two_running()
        hold = self.hold_bound('cq-big')
        self.queue('cq-small', 2, 2)
        active, _, _ = deferred.reservations()
        self.assertTrue(deferred.may_pass(hold, active, self.values['cq-small'], self.now))
        del self.values['cq-small']['est_minutes']
        self.assertFalse(deferred.may_pass(hold, active, self.values['cq-small'], self.now))
        self.assertFalse(deferred.may_pass({}, active, self.values['cq-big'], self.now))
        self.assertFalse(deferred.may_pass(hold, [active[0], dict(active[1], slots=None)], self.values['cq-big'], self.now))
        # and the tick survives it
        self.queue('cq-small-2', 2, 3)
        result, _ = self.tick()
        self.assertEqual(result['started'], ['cq-small-2'])


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
                if str(path).replace('\\', '/') == '/proc/meminfo':
                    return 'MemTotal:       16777216 kB\nMemFree:  1 kB\n'
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
                # the pool every fleet job runs in: 16 GiB less the 2 GiB floor, no swap, and the CPU of its 6 fleet slots
                self.assertEqual((root / '.config/systemd/user/cq-fleet.slice').read_text(),
                                 '[Unit]\nDescription=Baton fleet jobs memory pool\n\n[Slice]\nMemoryMax='
                                 + str(14 * deferred.GIB) + '\nMemorySwapMax=0\nCPUQuota=600%\n')
                with mock.patch.object(sys, 'argv', argv + ['--update-policy']):
                    installer.main()
                self.assertEqual(config.read_text(), raw)
                with mock.patch.object(sys, 'argv', argv + ['--foreground-slots', '1']):
                    with self.assertRaisesRegex(SystemExit, 'pass --update-policy'):
                        installer.main()
                self.assertEqual(config.read_text(), raw)

    def test_a_pool_grows_under_running_jobs_but_not_past_an_unstarted_one(self):
        spec = importlib.util.spec_from_file_location('installer', HERE / 'install_deferred.py')
        installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(installer)
        base = {'version': 1, 'machine_id': 'a' * 32, 'pool_slots': 16, 'load_cap': 24.0, 'ram_floor_gb': 6.0}
        fields = installer.policy_fields({}, 20, validate_zone=False)
        new = dict(base, pool_slots=20, **fields)
        self.assertTrue(installer.pool_grows(base, new))
        self.assertFalse(installer.pool_grows(dict(base, pool_slots=20), new))               # the same pool is not growth
        self.assertFalse(installer.pool_grows(dict(base, pool_slots=24), new))               # and a pool never shrinks
        self.assertFalse(installer.pool_grows(dict(base, load_cap=20.0), new))
        self.assertFalse(installer.pool_grows(dict(base, machine_id='b' * 32), new))
        self.assertFalse(installer.pool_grows(dict(base, foreground_slots=1), new))
        self.assertFalse(installer.pool_grows(dict(base, surprise=1), new))
        self.assertFalse(installer.pool_grows(None, new))
        if os.name == 'nt':
            return   # the queue checks below read file ownership
        with tempfile.TemporaryDirectory() as temp:
            queue = Path(temp)

            def job(name, phase=None, descriptor=True):
                d = queue / name
                d.mkdir()
                if descriptor:
                    (d / 'deferred.json').write_text('{}')
                if phase:
                    (d / 'deferred-state.json').write_text(json.dumps({'version': 1, 'id': name, 'phase': phase}))
                return d
            job('cq-running', 'running')
            job('cq-done', 'done')
            for name, mark in (('cq-immediate', 'pid'), ('cq-ended', 'exit')):               # ordinary jobs have neither file,
                (queue / name).mkdir()                                                       # and their wrapper has run
                (queue / name / mark).write_text('7\n')
            installer.check_nothing_unstarted(queue)
            staging = queue / 'cq-staging'                                                   # a source push still on its way:
            staging.mkdir()                                                                  # its descriptor would name the old pool
            (staging / 'src').mkdir()
            with self.assertRaisesRegex(RuntimeError, 'is being staged, or was left half staged: cq-staging'):
                installer.check_nothing_unstarted(queue)
            (staging / 'src').rmdir()
            staging.rmdir()
            installer.check_nothing_unstarted(queue)
            for phase in ('queued', 'starting', 'absent'):
                waiting = job('cq-wait-' + phase, phase)
                with self.assertRaisesRegex(RuntimeError, 'has not started yet: cq-wait-' + phase):
                    installer.check_nothing_unstarted(queue)
                for item in waiting.iterdir():
                    item.unlink()
                waiting.rmdir()
            job('cq-half')                                                                   # a descriptor and no state
            with self.assertRaisesRegex(RuntimeError, 'unresolved staged deferred job: cq-half'):
                installer.check_nothing_unstarted(queue)
            # cancelled before its registration: the helper's record of that answers every later `register`, so the
            # directory can never become a job (ember, 13 Nov 2026: one such, two days old, stopped the growth)
            record = queue / '.deferred-cancel-cq-half'
            record.write_text(json.dumps({'version': 1, 'id': 'cq-other', 'phase': 'cancelled'}))
            with self.assertRaisesRegex(RuntimeError, 'unresolved staged deferred job: cq-half'):
                installer.check_nothing_unstarted(queue)                                     # another job's record
            record.write_text(json.dumps({'version': 1, 'id': 'cq-half', 'phase': 'cancelled', 'reason': 'cancelled before registration'}))
            installer.check_nothing_unstarted(queue)
            bare = queue / 'cq-bare'                                                         # its push had not got as far as the descriptor
            bare.mkdir()
            (queue / '.deferred-cancel-cq-bare').write_text(json.dumps({'version': 1, 'id': 'cq-bare', 'phase': 'cancelled'}))
            installer.check_nothing_unstarted(queue)
            for name in ('cq-running', 'cq-immediate'):                                      # a drained host has neither of these
                for item in (queue / name).iterdir():
                    item.unlink()
                (queue / name).rmdir()
            (queue / 'cq-done' / 'deferred.json').unlink()
            (queue / 'cq-done' / 'deferred-state.json').unlink()
            (queue / 'cq-done' / 'exit').write_text('0\n')
            installer.check_drained(queue)                                                   # the same holds for a policy update
            record.unlink()
            (queue / '.deferred-cancel-cq-bare').unlink()
            with self.assertRaisesRegex(RuntimeError, 'unresolved'):
                installer.check_drained(queue)

    @unittest.skipIf(os.name == 'nt', 'the installer takes POSIX locks and reads file ownership')
    def test_the_installer_grows_the_pool_only_when_asked_and_only_with_nothing_unstarted(self):
        spec = importlib.util.spec_from_file_location('installer', HERE / 'install_deferred.py')
        installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(installer)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            target = root / '.local/share/baton/compute-deferred'
            target.mkdir(parents=True)
            (root / 'q').mkdir()
            config = target / 'config.json'
            raw = '{"version":1,"machine_id":"' + 'a' * 32 + '","pool_slots":8,"load_cap":10,"ram_floor_gb":2}' + chr(10)
            config.write_text(raw)
            for name, source in (('deferred.py', HERE / 'deferred.py'), ('queue_policy.py', HERE.parent / 'bin' / 'queue_policy.py')):
                (target / name).write_bytes(source.read_bytes())                             # this helper version is installed
            original_read = Path.read_text

            def read_text(path, *args, **kwargs):
                if str(path) == '/etc/machine-id':
                    return 'a' * 32
                if str(path) == '/proc/meminfo':
                    return 'MemTotal:       16777216 kB' + chr(10)
                return original_read(path, *args, **kwargs)
            grown = ['install_deferred.py', '--pool-slots', '12', '--load-cap', '10', '--ram-floor-gb', '2']
            with mock.patch.object(Path, 'home', return_value=root), mock.patch.object(Path, 'read_text', read_text), \
                    mock.patch.object(installer.os, 'cpu_count', return_value=16), \
                    mock.patch.object(installer.subprocess, 'check_output', return_value='yes'), \
                    mock.patch.object(installer.subprocess, 'run'):
                with mock.patch.object(sys, 'argv', grown), self.assertRaisesRegex(SystemExit, 'only grows, with --grow-pool'):
                    installer.main()
                self.assertEqual(config.read_text(), raw)
                waiting = root / 'q' / 'cq-waiting'
                waiting.mkdir()
                (waiting / 'deferred.json').write_text('{}')
                (waiting / 'deferred-state.json').write_text(json.dumps({'version': 1, 'id': 'cq-waiting', 'phase': 'queued'}))
                with mock.patch.object(sys, 'argv', grown + ['--grow-pool']), self.assertRaisesRegex(SystemExit, 'pool not grown: a registered job has not started yet'):
                    installer.main()
                self.assertEqual(config.read_text(), raw)
                (waiting / 'deferred-state.json').write_text(json.dumps({'version': 1, 'id': 'cq-waiting', 'phase': 'running'}))
                with mock.patch.object(sys, 'argv', grown + ['--grow-pool']):
                    installer.main()
                self.assertEqual(json.loads(config.read_text())['pool_slots'], 12)
                self.assertIn('CPUQuota=1000%', (root / '.config/systemd/user/cq-fleet.slice').read_text())   # 12 less 2 for the foreground
                with mock.patch.object(sys, 'argv', ['install_deferred.py', '--pool-slots', '8', '--load-cap', '10', '--ram-floor-gb', '2', '--grow-pool']), \
                        self.assertRaisesRegex(SystemExit, 'only grows'):
                    installer.main()                                                         # a pool never shrinks

    def test_the_fleet_slice_has_no_limit_when_the_pool_would_be_under_two_gib(self):
        spec = importlib.util.spec_from_file_location('installer', HERE / 'install_deferred.py')
        installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(installer)
        gib = deferred.GIB
        # the limit is the host's memory less the RAM floor, nothing more
        self.assertIn('MemoryMax=' + str(6 * gib), installer.fleet_slice_unit(8 * gib, 2))
        self.assertIn('MemoryMax=' + str(int(15.5 * gib)), installer.fleet_slice_unit(16 * gib, .5))
        self.assertIn('MemorySwapMax=0', installer.fleet_slice_unit(8 * gib, 2))
        self.assertIn('MemoryMax=' + str(2 * gib), installer.fleet_slice_unit(4 * gib, 2))   # exactly 2 GiB
        for total in (4 * gib - 1, 3 * gib, 2 * gib):
            text = installer.fleet_slice_unit(total, 2)
            self.assertNotIn('MemoryMax', text)
            self.assertNotIn('MemorySwapMax', text)
            self.assertIn('[Slice]', text)

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
                if str(path).replace('\\', '/') == '/proc/meminfo':
                    return 'MemTotal:       16777216 kB\nMemFree:  1 kB\n'
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


class ExpectedMemoryTests(unittest.TestCase):
    """Admission on the memory a job is expected to use, only while the fleet slice's limit is in effect."""
    GIB = deferred.GIB

    def base_descriptor(self, **extra):
        return {'version': 1, 'id': 'cq-x', 'fleet': True, 'mode': 'immediate', 'slots': 1, 'pool_slots': 8,
                'est_minutes': 5, 'timeout_minutes': 10, 'load_cap': 8, 'ram_floor_gb': 2, 'memory_max_gb': 4,
                'source_sha': 'a' * 40, **extra}

    def test_the_descriptor_may_carry_an_expectation_within_the_cap(self):
        policy = {'pool_slots': 8, 'load_cap': 8, 'ram_floor_gb': 2}
        deferred.validate_descriptor(self.base_descriptor(), 'cq-x', policy)
        for good in (.25, 1, 3.5, 4):
            deferred.validate_descriptor(self.base_descriptor(memory_expected_gb=good), 'cq-x', policy)
        for bad in (0, -1, 4.01, 5, True, '1', [1], float('nan'), float('inf')):
            with self.subTest(bad=bad), self.assertRaisesRegex(RuntimeError, 'memory_expected_gb'):
                deferred.validate_descriptor(self.base_descriptor(memory_expected_gb=bad), 'cq-x', policy)

    def test_a_job_is_charged_its_expectation_until_it_holds_more(self):
        gib = self.GIB
        row = {'reserved_memory_bytes': 8 * gib, 'expected_memory_bytes': 2 * gib}
        for held, expected, charged in ((0, True, 2 * gib), (gib, True, gib), (2 * gib, True, 0),
                                        (2 * gib + 1, True, 6 * gib - 1),   # over it: the cap less what it holds
                                        (5 * gib, True, 3 * gib), (8 * gib, True, 0),
                                        (gib, False, 7 * gib), (0, False, 8 * gib)):
            with self.subTest(held=held, expected=expected):
                self.assertEqual(deferred.charge(dict(row, held_memory_bytes=held), expected), charged)
        self.assertEqual(deferred.charge({'reserved_memory_bytes': 8 * gib, 'held_memory_bytes': gib}, True), 7 * gib)  # none
        self.assertEqual(deferred.charge({'reserved_memory_bytes': 8 * gib}, True), 8 * gib)

    def test_reservations_carry_a_valid_expectation_only(self):
        gib = self.GIB
        with tempfile.TemporaryDirectory() as temp:
            for jid in ('cq-a', 'cq-b', 'cq-c', 'cq-d', 'cq-e'):
                (Path(temp) / jid).mkdir()
            expected = {'cq-a': 2 * gib, 'cq-b': None, 'cq-c': 5 * gib, 'cq-d': 0, 'cq-e': True}
            def state(path):
                jid = path.parent.name
                row = {'id': jid, 'fleet': True, 'slots': 1, 'memory_bytes': 4 * gib, 'phase': 'starting'}
                if expected[jid] is not None:
                    row['memory_expected_bytes'] = expected[jid]
                return row
            with mock.patch.object(deferred, 'Q', Path(temp)), mock.patch.object(deferred, 'safe'), \
                 mock.patch.object(deferred, 'read', side_effect=state):
                rows = {row['id']: row for row in deferred.reservations()[0]}
        self.assertEqual(rows['cq-a']['expected_memory_bytes'], 2 * gib)
        for jid in ('cq-b', 'cq-c', 'cq-d', 'cq-e'):   # missing, above the cap, zero, not an integer: charged its cap
            self.assertNotIn('expected_memory_bytes', rows[jid])

    def reason(self, value, active, free_gib, limit=20, total_gib=32, jid=None):
        gib = self.GIB
        with mock.patch.object(deferred, 'reservations', return_value=(active, False, False)), \
             mock.patch.object(deferred, 'legacy_busy', return_value=False), \
             mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True), \
             mock.patch.object(deferred, 'available_after', return_value=int(free_gib * gib)), \
             mock.patch.object(deferred, 'total_memory', return_value=total_gib * gib), \
             mock.patch.object(deferred, 'expected_mode', return_value=(bool(limit), None if limit else 'fleet slice limit not set, or above memory less the floor')):
            return deferred.fleet_reason(value, jid)

    def job(self, **extra):
        return {'fleet': True, 'mode': 'immediate', 'load_cap': 8, 'ram_floor_gb': 2, 'memory_max_gb': 8,
                'pool_slots': 8, 'slots': 1, **extra}

    def running(self, held_gib, expected_gib=2, jid='cq-a', cap_gib=8):
        gib = self.GIB
        row = {'id': jid, 'phase': 'running', 'slots': 1, 'reserved_memory_bytes': cap_gib * gib,
               'held_memory_bytes': int(held_gib * gib), 'pooled': True,
               'control_group': '/user.slice/cq-fleet.slice/' + deferred.unit(jid)}
        if expected_gib is not None:
            row['expected_memory_bytes'] = int(expected_gib * gib)
        return row

    def test_admission_under_the_expectation_of_a_running_job_and_of_the_new_one(self):
        gib = self.GIB
        active = [self.running(1)]
        new = self.job(memory_expected_gb=2)
        # floor 2 + the running job's expectation less what it holds (2 - 1) + the new job's expectation 2
        self.assertIsNone(self.reason(new, active, 5))
        self.assertIn('insufficient available memory', self.reason(new, active, 5 - 1 / gib))
        # the same jobs on caps (no slice limit): floor 2 + (8 - 1) + 8
        self.assertIsNone(self.reason(new, active, 17, limit=None))
        self.assertIn('insufficient available memory', self.reason(new, active, 17 - 1 / gib, limit=None))

    def test_a_running_job_over_its_expectation_is_charged_its_cap(self):
        gib = self.GIB
        active = [self.running(3)]   # holds 3 of an expected 2: the expectation was wrong, so 8 - 3 stays charged
        new = self.job(memory_expected_gb=2)
        self.assertIsNone(self.reason(new, active, 9))               # floor 2 + 5 + 2
        self.assertIn('insufficient', self.reason(new, active, 9 - 1 / gib))

    def test_a_job_without_an_expectation_is_charged_its_cap_whatever_it_holds_or_is(self):
        gib = self.GIB
        # the running job's record has none: its cap less what it holds; the new descriptor has none: its cap
        self.assertIsNone(self.reason(self.job(), [self.running(1, expected_gib=None)], 2 + 7 + 8))
        self.assertIn('insufficient', self.reason(self.job(), [self.running(1, expected_gib=None)], 2 + 7 + 8 - 1 / gib))
        # a running job with an expectation, a new job without: the new one is charged 8
        self.assertIsNone(self.reason(self.job(), [self.running(1)], 2 + 1 + 8))
        self.assertIn('insufficient', self.reason(self.job(), [self.running(1)], 2 + 1 + 8 - 1 / gib))

    def test_the_job_itself_is_not_charged_twice(self):
        active = [self.running(0, jid='cq-self'), self.running(1)]
        self.assertIsNone(self.reason(self.job(memory_expected_gb=2), active, 5, jid='cq-self'))

    def test_caps_may_add_up_to_twice_the_pool_and_no_more(self):
        # 32 GiB host, 2 GiB floor: the pool is 30 GiB, so the caps together may be 60 GiB
        active = [self.running(0, expected_gib=1, jid='cq-a', cap_gib=28), self.running(0, expected_gib=1, jid='cq-b', cap_gib=28)]
        self.assertIsNone(self.reason(self.job(memory_max_gb=4, memory_expected_gb=1), active, 100))
        reason = self.reason(self.job(memory_max_gb=4.5, memory_expected_gb=1), active, 100)
        self.assertEqual(reason, 'fleet memory caps exceed the overcommit bound')
        # without the slice's limit the bound does not apply: caps are charged in full, as before
        self.assertIsNone(self.reason(self.job(memory_max_gb=4.5, memory_expected_gb=1), active, 100 + 56 + 5, limit=None))

    def test_a_pool_limit_that_is_not_set_charges_caps_even_for_jobs_with_expectations(self):
        active = [self.running(0)]
        new = self.job(memory_expected_gb=2)
        self.assertIn('insufficient', self.reason(new, active, 12, limit=None))   # 2 + 8 + 8 = 18 on caps
        self.assertIsNone(self.reason(new, active, 12))                             # 2 + 2 + 2 on expectations

    def probe(self, active, limit, outside_gib=0):
        gib = self.GIB
        avail = 200 * gib - sum(row.get('held_memory_bytes', 0) for row in active) - int(outside_gib * gib)
        policy = {'pool_slots': 8, 'load_cap': 10, 'ram_floor_gb': 2}
        with tempfile.TemporaryDirectory() as temp, mock.patch.object(deferred, 'Q', Path(temp)), \
             mock.patch.object(deferred, 'check', return_value={'ok': True}), \
             mock.patch.object(deferred, 'host_policy', return_value=policy), \
             mock.patch.object(deferred, 'reservations', return_value=(active, False, False)), \
             mock.patch.object(deferred, 'legacy_busy', return_value=False), \
             mock.patch.object(deferred, 'existing_lock_free', return_value=True), \
             mock.patch.object(deferred, 'run_facts', return_value={}), \
             mock.patch.object(deferred, 'machine_id', return_value='machine'), \
             mock.patch.object(deferred, 'available_after', return_value=avail), \
             mock.patch.object(deferred, 'total_memory', return_value=200 * gib), \
             mock.patch.object(deferred, 'pool_limit', return_value=limit), \
             mock.patch.object(deferred, 'pool_memory_max', return_value=limit), \
             mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True):
            return deferred.fleet_probe()

    def test_the_probe_advertises_expected_memory_admission_only_with_the_pool_limit(self):
        gib = self.GIB
        active = [self.running(1), self.running(3, jid='cq-b'), self.running(0, expected_gib=None, jid='cq-c')]
        with_pool = self.probe(active, 20 * gib)
        self.assertIs(with_pool['expected_memory_admission'], True)
        self.assertEqual(with_pool['pool_limit_bytes'], 20 * gib)
        self.assertIsNone(with_pool['expected_memory_blocker'])
        # (2 - 1) + (8 - 3: over its expectation) + 8 (none)
        self.assertEqual(with_pool['charged_memory_bytes'], 14 * gib)
        self.assertEqual(with_pool['reserved_memory_bytes'], 24 * gib)   # unchanged: the caps
        self.assertEqual(with_pool['held_memory_bytes'], 4 * gib)
        self.assertEqual(with_pool['active_fleet'][0]['expected_memory_bytes'], 2 * gib)
        self.assertNotIn('expected_memory_bytes', with_pool['active_fleet'][2])
        self.assertNotIn('expected_memory_admission', with_pool['capabilities'])   # that list is for tools
        without = self.probe(active, None)
        self.assertIs(without['expected_memory_admission'], False)
        self.assertIsNone(without['pool_limit_bytes'])
        self.assertEqual(without['expected_memory_blocker'], 'fleet slice limit not set, or above memory less the floor')
        self.assertEqual(without['charged_memory_bytes'], 20 * gib)   # every cap less what it holds
        self.assertEqual(self.probe([], None)['charged_memory_bytes'], 0)

    def test_the_pool_limit_is_what_systemd_reports_below_the_hosts_memory(self):
        gib = self.GIB
        real = REAL_POOL_LIMIT
        def shown(out='', returncode=0, error=None):
            result = types.SimpleNamespace(stdout=out, returncode=returncode, stderr='')
            with mock.patch.object(deferred, 'pool_limit', real), \
                 mock.patch.object(deferred, 'command', side_effect=error, return_value=result) as run, \
                 mock.patch.object(deferred, 'total_memory', return_value=32 * gib):
                return deferred.pool_limit(2 * gib), run
        value, run = shown(str(20 * gib) + '\n')
        self.assertEqual(value, 20 * gib)
        self.assertEqual(shown(str(30 * gib) + '\n')[0], 30 * gib)    # exactly the host's memory less the floor
        slack = deferred.POOL_SLACK_BYTES                             # MemTotal moves a little between kernels
        self.assertEqual(shown(str(30 * gib + slack) + '\n')[0], 30 * gib + slack)
        self.assertIsNone(shown(str(30 * gib + slack + 1) + '\n')[0])  # above that: the floor would not be kept
        run.assert_called_once_with('systemctl', '--user', 'show', 'cq-fleet.slice', '--property=MemoryMax', '--value', check=False)
        for out in ('infinity\n', '', '[not set]\n', '18446744073709551615\n', str(32 * gib) + '\n', '0\n', '-5\n',
                    str(31 * gib) + '\n'):
            self.assertIsNone(shown(out)[0], out)
        self.assertIsNone(shown(str(20 * gib), returncode=1)[0])
        self.assertIsNone(shown(error=FileNotFoundError('systemctl'))[0])
        self.assertIsNone(shown(error=subprocess.TimeoutExpired('systemctl', 30))[0])


class ExpectedModeTests(unittest.TestCase):
    """expected_mode: the one decision fleet_reason and fleet_probe share. A 32 GiB host with a 2 GiB RAM floor has a
    30 GiB pool; memory outside the fleet jobs may take at most the floor less 1 GiB, so at most 1 GiB."""
    GIB = deferred.GIB
    GROUP = '/user.slice/user@1000.service/cq-fleet.slice/'

    def row(self, held=0, jid='cq-a', phase='running', pooled=True, expected=2, cap=8):
        gib = self.GIB
        row = {'id': jid, 'phase': phase, 'slots': 1, 'reserved_memory_bytes': cap * gib,
               'held_memory_bytes': int(held * gib), 'pooled': pooled}
        if expected is not None:
            row['expected_memory_bytes'] = int(expected * gib)
        if pooled or phase == 'running':
            row['control_group'] = (self.GROUP if pooled else '/user.slice/user@1000.service/') + deferred.unit(jid)
        return row

    @contextlib.contextmanager
    def host(self, limit=30, total=32, memory_max='limit'):
        """The user manager's MemoryMax for the slice (GiB, None: infinity), the host's memory and what the kernel's
        memory.max of the slice says (GiB; None: unreadable; 'real': read it from self.cgroups)."""
        gib = self.GIB
        shown = 'infinity\n' if limit is None else str(int(limit * gib)) + '\n'
        patches = [mock.patch.object(deferred, 'pool_limit', REAL_POOL_LIMIT),
                   mock.patch.object(deferred, 'command', return_value=types.SimpleNamespace(stdout=shown, returncode=0, stderr='')),
                   mock.patch.object(deferred, 'total_memory', return_value=int(total * gib))]
        if memory_max != 'real':
            value = limit if memory_max == 'limit' else memory_max
            patches.append(mock.patch.object(deferred, 'pool_memory_max', return_value=None if value is None else int(value * gib)))
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            yield

    def mode(self, active, free, floor=2, **host):
        with self.host(**host):
            return deferred.expected_mode(active, int(free * self.GIB), {'ram_floor_gb': floor})

    def test_it_holds_when_the_limit_is_set_every_job_is_pooled_and_the_outside_leaves_the_margin(self):
        self.assertEqual(self.mode([self.row(held=1)], 30), (True, None))
        self.assertEqual(self.mode([], 31), (True, None))
        self.assertEqual(self.mode([self.row(phase='starting', held=0)], 31), (True, None))

    def test_a_limit_that_is_not_set_or_above_the_memory_less_the_floor_is_not_a_pool(self):
        for limit in (None, 30.5, 32):
            with self.subTest(limit=limit):
                self.assertEqual(self.mode([], 31, limit=limit), (False, 'fleet slice limit not set, or above memory less the floor'))
        self.assertEqual(self.mode([], 31, limit=30), (True, None))          # exactly the memory less the floor
        self.assertEqual(self.mode([], 31, limit=30.2), (True, None))        # within POOL_SLACK_BYTES of it: MemTotal moved
        self.assertEqual(self.mode([], 30, floor=3, limit=30)[0], False)     # a larger floor: 30 is now above 29
        self.assertEqual(self.mode([], 30, floor=3, limit=29), (True, None))

    def test_a_job_outside_the_slice_turns_it_off_and_it_comes_back_when_that_job_is_gone(self):
        old = self.row(held=1, jid='cq-old', pooled=False)
        self.assertEqual(self.mode([old], 30), (False, 'a fleet job runs outside the fleet slice'))
        self.assertEqual(self.mode([self.row(), old], 30), (False, 'a fleet job runs outside the fleet slice'))
        self.assertEqual(self.mode([self.row(held=1)], 30), (True, None))
        self.assertEqual(self.mode([], 31), (True, None))

    def test_a_job_outside_the_pool_makes_admission_charge_caps_and_the_pool_comes_back_with_it_gone(self):
        # a 64 GiB host (pool 62 GiB), the new job's cap is 70 GiB and its expectation 2 GiB: only expectations fit
        value = {'fleet': True, 'mode': 'immediate', 'load_cap': 8, 'ram_floor_gb': 2, 'memory_max_gb': 70,
                 'memory_expected_gb': 2, 'pool_slots': 8, 'slots': 1}
        def reason(active):
            with self.host(limit=62, total=64), \
                 mock.patch.object(deferred, 'reservations', return_value=(active, False, False)), \
                 mock.patch.object(deferred, 'legacy_busy', return_value=False), \
                 mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True), \
                 mock.patch.object(deferred, 'available_after', return_value=63 * self.GIB):
                return deferred.fleet_reason(value)
        self.assertIsNone(reason([]))
        self.assertIsNone(reason([self.row(cap=8, expected=2)]))   # charged 2 and 2: 2 + 2 + 2 of 63
        self.assertIn('insufficient', reason([self.row(cap=8, expected=2, pooled=False)]))   # caps: 2 + 8 + 70 of 63
        self.assertIsNone(reason([self.row(cap=8, expected=2)]))   # that job is gone (or pooled): expectations again

    def test_a_starting_job_counts_as_pooled_only_with_the_intent_mark_or_a_cgroup_in_the_slice(self):
        gib = self.GIB
        states = {'cq-a': dict(phase='starting', slice=deferred.FLEET_SLICE),
                  'cq-b': dict(phase='starting'),
                  'cq-c': dict(phase='starting', control_group=self.GROUP + 'cq-deferred-cq-c.service'),
                  'cq-d': dict(phase='running', control_group='/user.slice/user@1000.service/cq-deferred-cq-d.service'),
                  'cq-e': dict(phase='running'),
                  'cq-f': dict(phase='starting', slice=deferred.FLEET_SLICE, control_group='/user.slice/cq-deferred-cq-f.service'),
                  'cq-g': dict(phase='running', slice=deferred.FLEET_SLICE)}
        with tempfile.TemporaryDirectory() as temp:
            for jid in states:
                (Path(temp) / jid).mkdir()
            def state(path):
                jid = path.parent.name
                return {'id': jid, 'fleet': True, 'slots': 1, 'memory_bytes': 4 * gib, **states[jid]}
            with mock.patch.object(deferred, 'Q', Path(temp)), mock.patch.object(deferred, 'safe'), \
                 mock.patch.object(deferred, 'read', side_effect=state), mock.patch.object(deferred, 'held_memory', return_value=0):
                rows = {row['id']: row for row in deferred.reservations()[0]}
        self.assertEqual({jid: row['pooled'] for jid, row in rows.items()},
                         {'cq-a': True, 'cq-b': False, 'cq-c': True, 'cq-d': False, 'cq-e': False, 'cq-f': False, 'cq-g': False})
        self.assertEqual(rows['cq-c']['control_group'], self.GROUP + 'cq-deferred-cq-c.service')

    def test_the_kernels_memory_max_of_the_slice_must_be_the_number_the_property_shows(self):
        gib = self.GIB
        with tempfile.TemporaryDirectory() as temp:
            cgroups = Path(temp)
            slice_dir = cgroups / 'user.slice' / 'user@1000.service' / 'cq-fleet.slice'
            slice_dir.mkdir(parents=True)
            def at(written, active=None, free=30):
                if written is None:
                    (slice_dir / 'memory.max').unlink(missing_ok=True)
                else:
                    (slice_dir / 'memory.max').write_text(written + '\n')
                with mock.patch.object(deferred, 'CGROUP', cgroups):
                    return self.mode(active if active is not None else [self.row(held=1)], free, memory_max='real')
            self.assertEqual(at(str(30 * gib)), (True, None))
            for written in (str(29 * gib), str(31 * gib), 'max', 'garbage', '', None):
                with self.subTest(written=written):
                    self.assertEqual(at(written), (False, 'fleet slice limit not in effect'))
            # no pooled job runs: there is no cgroup to read, the property alone decides
            self.assertEqual(at(None, [self.row(phase='starting')], 31), (True, None))
            self.assertEqual(at(None, [], 31), (True, None))
            self.assertEqual(at('max', [self.row(phase='starting')], 31), (True, None))

    def test_memory_outside_the_fleet_jobs_may_take_the_floor_less_the_margin_and_no_more(self):
        gib = self.GIB
        # outside = total - available - held by the fleet jobs; at most the 2 GiB floor less 1 GiB
        self.assertEqual(self.mode([self.row(held=1)], 30), (True, None))                   # 32 - 30 - 1 = 1: exactly
        self.assertEqual(self.mode([self.row(held=1)], 30 - 1 / gib),
                         (False, 'memory outside the fleet pool exceeds the floor'))     # one byte over
        self.assertEqual(self.mode([self.row(held=5)], 26), (True, None))                   # what the fleet holds is not the outside
        self.assertEqual(self.mode([self.row(held=5), self.row(held=3, jid='cq-b')], 24), (True, None))
        self.assertEqual(self.mode([self.row(held=3)], 26)[0], False)                       # 3 GiB outside
        # a floor under the margin leaves nothing for the outside to take
        self.assertEqual(self.mode([], 31.9, floor=.5, limit=31.5)[0], False)

    def test_the_order_in_which_the_reasons_are_given(self):
        outside = self.row(pooled=False)
        self.assertEqual(self.mode([outside], 5, limit=None), (False, 'fleet slice limit not set, or above memory less the floor'))
        self.assertEqual(self.mode([outside], 5, memory_max=None), (False, 'a fleet job runs outside the fleet slice'))
        self.assertEqual(self.mode([self.row()], 5, memory_max=None), (False, 'fleet slice limit not in effect'))
        self.assertEqual(self.mode([self.row()], 5), (False, 'memory outside the fleet pool exceeds the floor'))

    def test_the_probe_flag_and_blocker_are_what_admission_uses_on_the_same_state(self):
        gib = self.GIB
        value = {'fleet': True, 'mode': 'immediate', 'load_cap': 8, 'ram_floor_gb': 2, 'memory_max_gb': 4,
                 'pool_slots': 8, 'slots': 1}
        policy = {'pool_slots': 8, 'load_cap': 10, 'ram_floor_gb': 2}
        cases = {'on': ([self.row(held=1)], 30, {}, True, None),
                 'no limit': ([self.row(held=1)], 30, dict(limit=None), False, 'fleet slice limit not set, or above memory less the floor'),
                 'above the memory less the floor': ([self.row(held=1)], 30, dict(limit=31), False, 'fleet slice limit not set, or above memory less the floor'),
                 'job outside the slice': ([self.row(held=1, pooled=False)], 30, {}, False, 'a fleet job runs outside the fleet slice'),
                 'kernel shows another number': ([self.row(held=1)], 30, dict(memory_max=25), False, 'fleet slice limit not in effect'),
                 'kernel shows none': ([self.row(held=1)], 30, dict(memory_max=None), False, 'fleet slice limit not in effect'),
                 'outside memory': ([self.row(held=1)], 28, {}, False, 'memory outside the fleet pool exceeds the floor'),
                 'nothing running': ([], 31, {}, True, None)}
        for name, (active, free, host, flag, blocker) in cases.items():
            with self.subTest(name), tempfile.TemporaryDirectory() as temp:
                used = []
                real = deferred.expected_mode
                def spy(*args):
                    used.append(real(*args))
                    return used[-1]
                with self.host(**host), mock.patch.object(deferred, 'expected_mode', spy), \
                     mock.patch.object(deferred, 'Q', Path(temp)), \
                     mock.patch.object(deferred, 'check', return_value={'ok': True}), \
                     mock.patch.object(deferred, 'host_policy', return_value=policy), \
                     mock.patch.object(deferred, 'reservations', side_effect=lambda: (copy.deepcopy(active), False, False)), \
                     mock.patch.object(deferred, 'legacy_busy', return_value=False), \
                     mock.patch.object(deferred, 'existing_lock_free', return_value=True), \
                     mock.patch.object(deferred, 'run_facts', return_value={}), \
                     mock.patch.object(deferred, 'machine_id', return_value='machine'), \
                     mock.patch.object(deferred, 'available_after', return_value=int(free * gib)), \
                     mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True):
                    probed = deferred.fleet_probe()
                    deferred.fleet_reason(value)
                self.assertEqual(len(used), 2)
                self.assertEqual(used[0], used[1])   # the same answer to the probe and to admission
                self.assertEqual((probed['expected_memory_admission'], probed['expected_memory_blocker']), (flag, blocker))
                self.assertEqual(used[0], (flag, blocker))
                self.assertIs(type(probed['expected_memory_admission']), bool)
                # charged memory follows the flag: expectations when on, caps otherwise
                charged = sum(deferred.charge(row, flag) for row in active)
                self.assertEqual(probed['charged_memory_bytes'], charged)


class ExpectedCpuTests(unittest.TestCase):
    """A job the controller says uses fewer cores than it declares takes that many pool slots; its limit stays."""

    VALUE = {'fleet': True, 'mode': 'immediate', 'load_cap': 8, 'ram_floor_gb': 2, 'memory_max_gb': 1, 'pool_slots': 8, 'slots': 4}

    def test_the_charge_is_the_expectation_in_whole_slots_and_never_more_than_declared(self):
        for cores, charged in ((None, 4), (.2, 1), (1.0, 1), (1.2, 2), (3.01, 4), (4, 4)):
            value = dict(self.VALUE) if cores is None else dict(self.VALUE, cpu_expected=cores)
            self.assertEqual(deferred.charged_slots(value), charged, cores)
        for bad in (0, -1, 4.5, '2', float('nan'), float('inf')):
            self.assertEqual(deferred.charged_slots(dict(self.VALUE, cpu_expected=bad)), 4, bad)

    def test_admission_counts_charges_and_bounds_the_quotas(self):
        def reason(value, active):
            with mock.patch.object(deferred, 'reservations', return_value=(active, False, False)), \
                 mock.patch.object(deferred, 'legacy_busy', return_value=False), \
                 mock.patch.object(deferred, 'disk_low', return_value=False), \
                 mock.patch.object(deferred.os, 'getloadavg', return_value=(1, 0, 0), create=True), \
                 mock.patch.object(deferred, 'available', return_value=20 * deferred.GIB):
                return deferred.fleet_reason(value)
        row = lambda jid, slots, quota=None: dict({'id': jid, 'phase': 'running', 'slots': slots, 'reserved_memory_bytes': deferred.GIB},
                                                  **({} if quota is None else {'quota_slots': quota}))
        # 8 pool slots, 2 kept for the foreground: 6. Four are charged; a job that declares 4 does not fit, one expected to use 1.2 does
        self.assertIn('CPU pool', reason(dict(self.VALUE), [row('cq-a', 4)]))
        self.assertIsNone(reason(dict(self.VALUE, cpu_expected=1.2), [row('cq-a', 4)]))
        self.assertIn('CPU pool', reason(dict(self.VALUE, cpu_expected=1.2), [row('cq-a', 5)]))
        # declared slots of all of them: at most CPU_OVERCOMMIT x 6
        self.assertIsNone(reason(dict(self.VALUE, cpu_expected=1.2), [row('cq-a', 2, 4), row('cq-b', 2, 4)]))            # 8 + 4
        self.assertIn('quotas', reason(dict(self.VALUE, cpu_expected=1.2), [row('cq-a', 2, 5), row('cq-b', 2, 4)]))      # 9 + 4
        self.assertEqual(deferred.CPU_OVERCOMMIT, 2)

    def test_expected_cpu_is_offered_only_where_the_slice_bounds_the_fleets_cpu(self):
        def shown(text, code=0):
            return mock.patch.object(deferred, 'command', return_value=types.SimpleNamespace(stdout=text, returncode=code, stderr=''))
        for text, slots, bounded in (('14s', 14, True), ('6s', 6, True), ('6s', 14, True), ('14s', 6, False),
                                     ('1min 30s', 90, True), ('500ms', 1, True), ('infinity', 14, False), ('', 14, False),
                                     ('0', 14, False), ('junk', 14, False)):
            with shown(text + chr(10)):
                self.assertIs(deferred.pool_cpu_bounded(slots), bounded, (text, slots))
        with shown('14s', code=1):
            self.assertFalse(deferred.pool_cpu_bounded(14))
        with mock.patch.object(deferred, 'command', side_effect=OSError('no systemctl')):
            self.assertFalse(deferred.pool_cpu_bounded(14))
        # once a job runs in the slice, the kernel's own figure must agree with the unit's
        with tempfile.TemporaryDirectory() as name, mock.patch.object(deferred, 'CGROUP', Path(name)), shown('14s'):
            pool = Path(name) / 'user.slice' / deferred.FLEET_SLICE
            pool.mkdir(parents=True)
            rows = [{'id': 'cq-a', 'control_group': '/user.slice/' + deferred.FLEET_SLICE + '/cq-deferred-cq-a.service'}]
            self.assertFalse(deferred.pool_cpu_bounded(14, rows))                # no cpu.max: the controller is not on for the slice
            for text, bounded in (('1400000 100000', True), ('max 100000', False), ('2000000 100000', False), ('junk', False)):
                (pool / 'cpu.max').write_text(text + chr(10))
                self.assertIs(deferred.pool_cpu_bounded(14, rows), bounded, text)
            self.assertTrue(deferred.pool_cpu_bounded(14, [{'id': 'cq-b'}]))     # nothing in the slice yet: the configuration
        spec = importlib.util.spec_from_file_location('installer', HERE / 'install_deferred.py')
        installer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(installer)
        gib = deferred.GIB
        self.assertIn('CPUQuota=1400%', installer.fleet_slice_unit(64 * gib, 6, 16, 2))     # ember: 16 pool slots, 2 for the foreground
        self.assertIn('CPUQuota=600%', installer.fleet_slice_unit(24 * gib, 6, 8, 2))
        self.assertNotIn('CPUQuota', installer.fleet_slice_unit(64 * gib, 6))

    def test_a_job_held_by_the_quota_bound_does_not_hold_the_queue_behind_it(self):
        self.assertEqual(deferred.CPU_QUOTA_WAIT, 'fleet CPU quotas exceed the overcommit bound')
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            states = {'cq-a': {'id': 'cq-a', 'phase': 'queued', 'fleet': True, 'registered': time.time() - 60},
                      'cq-b': {'id': 'cq-b', 'phase': 'queued', 'fleet': True, 'registered': time.time() - 50}}
            value = {'fleet': True, 'mode': 'immediate', 'slots': 1, 'pool_slots': 8, 'memory_max_gb': 1, 'ram_floor_gb': 1,
                     'load_cap': 8, 'est_minutes': 5, 'timeout_minutes': 10}
            for jid in states:
                (root / jid).mkdir()

            def read(path):
                state = states.get(path.parent.name)
                return dict(state) if path.name == 'deferred-state.json' and state else None
            reasons = {'cq-a': deferred.CPU_QUOTA_WAIT, 'cq-b': None}
            current = []
            patches = [mock.patch.object(deferred, 'Q', root), mock.patch.object(deferred, 'safe'),
                       mock.patch.object(deferred, 'read', side_effect=read),
                       mock.patch.object(deferred, 'save', side_effect=lambda jid, state: states.__setitem__(jid, dict(state))),
                       mock.patch.object(deferred, 'staged_state', side_effect=lambda jid: (current.append(jid) or dict(states[jid]), value)),
                       mock.patch.object(deferred, 'reservations', return_value=([], False, False)),
                       mock.patch.object(deferred, 'fleet_reason', side_effect=lambda v: reasons[current[-1]]),
                       mock.patch.object(deferred, 'try_gate', return_value=99), mock.patch.object(deferred.os, 'close'),
                       mock.patch.object(deferred, 'pool_empty', return_value=True),
                       mock.patch.object(deferred, 'slots', return_value=[98]),
                       mock.patch.object(deferred, 'boot', return_value='boot'), mock.patch.object(deferred, 'command')]
            with contextlib.ExitStack() as stack:
                for item in patches:
                    stack.enter_context(item)
                result = deferred.tick()
                self.assertEqual(result.get('started'), ['cq-b'])        # the job behind it starts
                self.assertEqual(states['cq-a']['phase'], 'queued')
                self.assertEqual(states['cq-a']['reason'], deferred.CPU_QUOTA_WAIT)
                # but only until it has waited an hour: then nothing passes it, so it cannot be kept out for good
                states['cq-a']['registered'] = time.time() - 3700
                states['cq-b'].update(phase='queued', registered=time.time() - 50)
                del current[:]
                result = deferred.tick()
                self.assertNotIn('started', result)
                self.assertEqual((result.get('id'), result.get('reason')), ('cq-a', deferred.CPU_QUOTA_WAIT))
                self.assertEqual(states['cq-b']['phase'], 'queued')

    def test_the_descriptor_may_carry_an_expectation_within_its_slots(self):
        policy = {'pool_slots': 8, 'load_cap': 8, 'ram_floor_gb': 2}
        base = dict(self.VALUE, version=deferred.VERSION, id='cq-test', priority='lane', est_minutes=5, timeout_minutes=10,
                    source_sha='a' * 40)
        deferred.validate_descriptor(dict(base, cpu_expected=1.2), 'cq-test', policy)
        deferred.validate_descriptor(dict(base, cpu_expected=4), 'cq-test', policy)
        for bad in (0, -1, 4.5, '2', True, float('nan')):
            with self.assertRaises(RuntimeError, msg=bad):
                deferred.validate_descriptor(dict(base, cpu_expected=bad), 'cq-test', policy)

    def test_cpu_usage_is_read_from_the_cgroup_and_the_sampler_keeps_the_busiest_stretch(self):
        with tempfile.TemporaryDirectory() as name:
            group = Path(name)
            self.assertIsNone(deferred.cpu_usage(group))
            (group / 'cpu.stat').write_text('usage_usec 2500000\nuser_usec 2000000\nsystem_usec 500000\n')
            self.assertEqual(deferred.cpu_usage(group), 2500000)
            (group / 'cpu.stat').write_text('usage_usec x\n')
            self.assertIsNone(deferred.cpu_usage(group))
        readings = iter([0, 1_000_000, 9_000_000, 10_000_000])           # microseconds at 0, 2, 6 and 11 s
        clock = iter([0.0, 2.0, 6.0, 11.0])
        stop = mock.Mock()
        stop.wait.side_effect = [False, False, False, False, True]
        result = {}
        with mock.patch.object(deferred, 'anon_lists', return_value=None), \
             mock.patch.object(deferred, 'cpu_usage', side_effect=lambda group: next(readings)), \
             mock.patch.object(deferred.time, 'monotonic', side_effect=lambda: next(clock)):
            deferred.sample_held(Path('unused'), stop, result)
        self.assertEqual(result['cpu_peak_cores'], 1.5)                  # 9 s of CPU in the 6 s from the first reading; then 0.2


class TickSoonTests(unittest.TestCase):
    """A runner's exit is followed by a scheduler tick about a second later, not by the minute timer's next one."""

    def test_the_tick_is_a_transient_timer_of_the_user_manager_and_never_raises(self):
        with mock.patch.object(deferred.subprocess, 'run') as run:
            deferred.tick_soon()
        self.assertEqual(deferred.TICK_SOON_S, (1, 6))         # at once, and again in case the unit had not yet gone
        self.assertEqual([call.args[0][4] for call in run.call_args_list], ['--on-active=1', '--on-active=6'])
        argv = run.call_args.args[0]
        self.assertEqual(argv[:2], ['systemd-run', '--user'])
        self.assertIn('--collect', argv)                       # a failed tick leaves no unit behind
        self.assertEqual(argv[-3:], [sys.executable, str(deferred.SCRIPT), 'tick'])
        self.assertFalse(run.call_args.kwargs['check'])
        with mock.patch.object(deferred.subprocess, 'run') as run:
            deferred.tick_soon(deferred.TICK_RETRY_S)
        self.assertEqual([call.args[0][4] for call in run.call_args_list], ['--on-active=15'])
        for error in (OSError('no systemd-run'), subprocess.TimeoutExpired('systemd-run', 30)):
            with mock.patch.object(deferred.subprocess, 'run', side_effect=error):
                deferred.tick_soon()                           # the timer's own tick settles the job then

    def test_every_way_out_of_the_runner_asks_for_it(self):
        # a runner that started its command: at once. One that did not (the claim goes back to the queue), or that
        # refused a stale claim: after TICK_RETRY_S, so a flapping condition is not retried every second
        for outcome, delays in ((True, deferred.TICK_SOON_S), (None, deferred.TICK_RETRY_S),
                                (RuntimeError('stale or cancelled claim'), deferred.TICK_RETRY_S)):
            how = {'side_effect': outcome} if isinstance(outcome, Exception) else {'return_value': outcome}
            argv = mock.patch.object(sys, 'argv', ['deferred.py', 'run', 'cq-test', 'claim'])
            with self.subTest(outcome=outcome), argv, mock.patch.object(deferred, 'safe'):
                with mock.patch.object(deferred, 'runner', **how) as runner, mock.patch.object(deferred, 'tick_soon') as soon:
                    if isinstance(outcome, Exception):
                        with self.assertRaises(RuntimeError):
                            deferred.main()
                    else:
                        deferred.main()
                runner.assert_called_once_with('cq-test', 'claim')
                soon.assert_called_once_with(delays)

    def test_other_operations_do_not(self):
        argv = mock.patch.object(sys, 'argv', ['deferred.py', 'tick'])
        unlocked = mock.patch.object(deferred, 'locked', side_effect=lambda p: contextlib.nullcontext())
        with argv, unlocked, mock.patch.object(deferred, 'safe'), mock.patch('builtins.print'):
            with mock.patch.object(deferred, 'tick', return_value={'version': 1}), mock.patch.object(deferred, 'tick_soon') as soon:
                deferred.main()
        soon.assert_not_called()


if __name__ == '__main__':
    unittest.main()
