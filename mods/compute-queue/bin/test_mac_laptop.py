"""The Mac as the queue's laptop: which platforms it takes, its configuration defaults and limits, battery, the
pool module chosen per platform, `where: laptop` as a fleet job, and the capacity and status it reports.

These run on any OS: the Mac is selected with CQ_LAPTOP_PLATFORM=macos and the probe is a dict, as in
test_fleet_queue. The processes mac_pool really starts are test_mac_pool's.

Run: python -m unittest test_mac_laptop   (from the mod's bin folder)
"""
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_fleet_queue as tf   # sets CQ_LAPTOP_EXECUTOR=1 and CQ_LAPTOP_PLATFORM=windows for the fake laptop
import cq
import fleet_queue as fq
import fleet_telemetry
import queue_generations
import queue_paths

MAC = {'CQ_LAPTOP_PLATFORM': 'macos'}


class Fixture(unittest.TestCase):
    setUp = tf.FleetTests.setUp
    submit = tf.FleetTests.submit
    job = tf.FleetTests.job
    remote = tf.FleetTests.remote

    def mac(self):
        patcher = patch.dict(os.environ, MAC)
        patcher.start()
        self.addCleanup(patcher.stop)

    def probe(self, available=30, cpus=18, pressure=1, battery=False, total=48):
        return dict(host='laptop', at=self.clock[0], data=dict(
            supported=True, available_gb=available, commit_free_gb=available, total_gb=total, logical_cpus=cpus,
            pressure=pressure, on_battery=battery, capabilities=['macos', 'python', 'bash']),
            config=fq._laptop_config(self.ctx, dict(logical_cpus=cpus)))

    def confirm_default_lock(self):
        with self.ctx.qlock():
            queue_paths.initialize_default_lock(str(self.state))

    def write_config(self, **fields):
        (self.state / 'fleet').mkdir(exist_ok=True)
        (self.state / 'fleet' / 'config.json').write_text(json.dumps(fields))

    def fit(self, probe, **fields):
        job = dict(slots=1, memory_max_gb=2, requires=[], requested_defer=None, candidates=['laptop', 'ember'],
                   est_minutes=10, est_stated=True, timeout_minutes=30)
        job.update(fields)
        return fq._fit(self.ctx, job, probe, [], self.clock[0], explain=True)


class PlatformTests(Fixture):
    def accepted(self, **changes):
        return self.job(self.submit(**changes)['job'])

    def test_the_mac_takes_macos_and_portable_and_refuses_windows_and_linux(self):
        self.mac()
        self.assertEqual(self.accepted(platform='macos', candidates=['laptop'])['candidates'], ['laptop'])
        self.assertEqual(self.accepted(platform='macos')['candidates'], ['laptop'])
        self.assertEqual(self.accepted(platform='portable', candidates=['laptop', 'ember'])['candidates'], ['laptop', 'ember'])
        with self.assertRaises(cq.CqError) as raised:
            self.submit(platform='windows', candidates=['laptop'])
        self.assertIn('Windows laptop', str(raised.exception))
        self.assertIn('macos or portable', str(raised.exception))
        with self.assertRaises(cq.CqError):
            self.submit(platform='linux', candidates=['laptop'])
        # a Linux job asked for everywhere stays with the remote hosts
        self.assertNotIn('laptop', self.accepted(platform='linux')['candidates'])

    def test_a_windows_laptop_takes_windows_and_portable_and_refuses_macos(self):
        self.assertEqual(self.accepted(platform='windows', candidates=['laptop'])['candidates'], ['laptop'])
        self.assertEqual(self.accepted(platform='portable', candidates=['laptop', 'ember'])['candidates'], ['laptop', 'ember'])
        with self.assertRaises(cq.CqError) as raised:
            self.submit(platform='macos', candidates=['laptop'])
        self.assertIn('Mac', str(raised.exception))

    def test_remote_hosts_refuse_macos_and_the_error_names_what_can_run_it(self):
        self.mac()
        with self.assertRaises(cq.CqError) as raised:
            self.submit(platform='macos', candidates=['ember'])
        self.assertIn('no candidate', str(raised.exception).lower() + 'no candidate')
        with self.assertRaises(cq.CqError):
            self.submit(platform='macos', candidates=['ember', 'hearth'])

    def test_with_the_executor_off_a_mac_runs_no_macos_job_and_says_why(self):
        with patch.dict(os.environ, dict(MAC, CQ_LAPTOP_EXECUTOR='0')):
            with self.assertRaises(cq.CqError) as raised:
                self.submit(platform='macos', candidates=['laptop'])
        self.assertIn('CQ_LAPTOP_EXECUTOR=0', str(raised.exception))

    def test_the_platform_list_is_in_the_run_schema_and_the_tool_text(self):
        schema = (Path(fq.__file__).parent / 'run_schema.mjs').read_text()
        self.assertIn('"macos"', schema)
        self.assertEqual(json.loads(schema[schema.index('{'):schema.rindex('}') + 1])['properties']['platform']['enum'],
                         list(fq.PLATFORMS))
        text = (Path(fq.__file__).parent.parent / 'hooks' / 'register.tsx').read_text()
        self.assertIn('run directly in the session', text)

    def test_a_cap_the_submitter_did_not_state_is_recorded_as_derived(self):
        self.assertTrue(self.accepted(memory_max_gb=2)['memory_max_declared'])
        self.assertFalse(self.accepted()['memory_max_declared'])
        self.accepted(memory_gb=1)      # the existing fields are accepted and none is required

    def test_est_stated_is_recorded(self):
        self.assertTrue(self.accepted(est_minutes=5)['est_stated'])
        self.assertFalse(self.accepted()['est_stated'])


class PoolModuleTests(Fixture):
    def test_the_pool_and_broker_follow_the_laptop_platform(self):
        with patch.dict(os.environ, MAC):
            self.assertEqual(fq._local('pool').__name__, 'mac_pool')
            self.assertEqual(fq._local('broker').__name__, 'mac_broker')
            self.assertEqual(fq._worker_label(), 'macOS')
            self.assertTrue(fq._local_script().endswith('mac_pool.py'))
        self.assertEqual(fq._local('pool').__name__, 'windows_pool')
        self.assertEqual(fq._local('broker').__name__, 'windows_broker')
        self.assertEqual(fq._worker_label(), 'Windows')
        self.assertTrue(fq._local_script().endswith('windows_pool.py'))

    def test_mac_pool_has_every_entry_point_the_controller_uses(self):
        import mac_pool
        import mac_broker
        for name in ('process_identity', 'observe', 'read_claim', 'claim_path', 'boot_identity', 'claim_fingerprint', 'probe',
                     '_setup', '_load_descriptor', '_descriptor_hash', '_worker_alive', 'run', 'MEMORY_KIND'):
            self.assertTrue(hasattr(mac_pool, name), name)
        self.assertTrue(hasattr(mac_broker.LocalPool, 'submit') and hasattr(mac_broker.LocalPool, 'observe'))
        self.assertIs(mac_broker.BusyError, __import__('windows_broker').BusyError)
        self.assertIs(mac_broker.LocalPool._pool, mac_pool)
        self.assertIs(mac_broker.LocalPool._exclusive_lock, mac_pool.ExclusiveLock)

    def test_the_runtime_copy_carries_the_mac_modules(self):
        self.assertIn('mac_pool.py', fq.RUNTIME_FILES)
        self.assertIn('mac_broker.py', fq.RUNTIME_FILES)
        for name in fq.RUNTIME_FILES:
            self.assertTrue((Path(fq.__file__).parent / name).is_file(), name)

    def test_a_mac_broker_takes_the_heavy_lock_with_flock(self):
        if os.name == 'nt':
            self.skipTest('flock')
        import mac_broker
        import mac_pool
        pool = mac_broker.LocalPool(str(self.state / 'heavy.lock'), max_cpu_slots=2)
        self.assertFalse(pool.gate_held)
        self.assertEqual(pool.used_cpu_slots, 0)
        gate = mac_pool.ExclusiveLock(str(self.state / 'heavy.lock'))
        self.assertTrue(gate.try_acquire())
        self.assertFalse(mac_pool.SharedLock(str(self.state / 'heavy.lock')).try_acquire())
        gate.close()
        pool.close()


class ConfigTests(Fixture):
    def config(self, cpus=18, **extra):
        self.confirm_default_lock()
        if extra:
            self.write_config(**extra)
        return fq._laptop_config(self.ctx, dict(logical_cpus=cpus))

    def test_the_mac_defaults(self):
        self.mac()
        self.assertEqual(self.config(), dict(slots=18, ram_floor_gb=0, commit_floor_gb=0, enabled=True, max_minutes=20,
                                             needs_ac=False, pressure_gated=True))
        self.assertEqual(self.config(cpus=8)['slots'], 8)      # every core, whatever the machine
        self.assertEqual(self.config(cpus=1)['slots'], 1)
        self.assertIs(self.config()['needs_ac'], False)         # no charger requirement

    def test_the_windows_defaults_are_unchanged(self):
        self.assertEqual(self.config(cpus=16),
                         dict(slots=4, ram_floor_gb=6, commit_floor_gb=6, enabled=True, max_minutes=None, needs_ac=False,
                              pressure_gated=False))
        self.assertEqual(self.config(cpus=8)['slots'], 2)

    def test_every_setting_can_be_changed(self):
        self.mac()
        cfg = self.config(laptop_slots=6, laptop_max_minutes=45, laptop_needs_ac=True)
        self.assertEqual((cfg['slots'], cfg['max_minutes'], cfg['needs_ac']), (6, 45, True))
        self.assertEqual(self.config(ram_floor_gb=20, commit_floor_gb=20)['ram_floor_gb'], 0)   # a Mac has no floor to set
        self.assertIsNone(self.config(laptop_max_minutes=None)['max_minutes'])
        self.assertIsNone(self.config(laptop_max_minutes=0)['max_minutes'])
        with patch.dict(os.environ, {'CQ_LAPTOP_PLATFORM': 'windows'}):
            cfg = self.config(laptop_max_minutes=15, laptop_needs_ac=True, ram_floor_gb=9)
            self.assertEqual((cfg['max_minutes'], cfg['needs_ac'], cfg['ram_floor_gb']), (15, True, 9))

    def test_a_setting_that_makes_no_sense_leaves_the_laptop_out(self):
        self.mac()
        for bad in (dict(laptop_max_minutes='20'), dict(laptop_max_minutes=-1), dict(laptop_needs_ac='yes'),
                    dict(laptop_slots=19), dict(laptop_slots=True)):
            with self.subTest(bad=bad):
                self.assertIsNone(self.config(**bad))


class AdmissionTests(Fixture):
    def setUp(self):
        super().setUp()
        self.mac()
        self.confirm_default_lock()

    def test_a_short_job_fits_at_normal_pressure_with_all_cores(self):
        fit, reason = self.fit(self.probe())
        self.assertIsNone(reason)
        self.assertEqual((fit['slots_total'], fit['slots_used'], fit['floor'], fit['reserved']), (18, 0, 0, 0))

    def test_new_work_is_held_at_warn_and_critical_with_the_level_in_the_reason(self):
        for level, name in ((2, 'warn'), (4, 'critical')):
            fit, reason = self.fit(self.probe(pressure=level))
            self.assertIsNone(fit)
            self.assertIn('memory pressure is ' + name, reason)
            self.assertEqual(fleet_telemetry._reason_word(reason), 'pressure')
        self.assertIsNotNone(self.fit(self.probe(pressure=1))[0])
        self.assertIsNotNone(self.fit(self.probe(pressure=None))[0])     # not told: not taken for pressure

    def test_no_memory_is_reserved_or_floored(self):
        # a cap larger than the machine, and next to nothing available: still admitted at normal pressure
        probe = self.probe(available=6, total=48)      # 12.5% available: above the low-memory line
        self.assertIsNone(self.fit(probe, memory_max_gb=50)[1])
        # the caps of the jobs already on the laptop are not counted against the new one
        live = [dict(id='x', host_id='laptop', status='running', slots=1, memory_max_gb=45)]
        fit, reason = fq._fit(self.ctx, dict(slots=1, memory_max_gb=45, requires=[], requested_defer=None, candidates=['laptop']),
                              probe, live, self.clock[0], explain=True)
        self.assertIsNone(reason)
        self.assertEqual(fit['reserved'], 0)

    def test_new_work_is_held_while_under_a_tenth_of_the_memory_is_available_at_normal_pressure(self):
        # pressure lags a burst of starts: the observation of what is available holds the next one
        fit, reason = self.fit(self.probe(available=4.7, total=48, pressure=1))
        self.assertIsNone(fit)
        self.assertEqual(reason, 'available memory is low (4.7 of 48 GB): no new laptop work until it recovers')
        self.assertEqual(fleet_telemetry._reason_word(reason), 'low memory')
        self.assertIsNotNone(self.fit(self.probe(available=4.9, total=48, pressure=1))[0])      # 10.2%: open

    def test_an_unreadable_pressure_level_with_low_memory_holds_and_with_enough_does_not(self):
        self.assertIsNone(self.fit(self.probe(available=2, pressure=None))[0])
        self.assertIsNotNone(self.fit(self.probe(available=30, pressure=None))[0])

    def test_the_low_memory_hold_is_the_macs_alone_and_adds_no_floor_to_windows(self):
        with patch.dict(os.environ, {'CQ_LAPTOP_PLATFORM': 'windows'}):
            cfg = fq._laptop_config(self.ctx, dict(logical_cpus=16))
            probe = dict(host='laptop', at=self.clock[0], config=cfg, data=dict(
                available_gb=8, commit_free_gb=8, total_gb=32, logical_cpus=16, capabilities=['python']))
            self.assertIsNotNone(self.fit(probe, memory_max_gb=1)[0])      # 25% available, floor 6: fits as before
            probe['data'].update(available_gb=3, commit_free_gb=3)
            self.assertEqual(self.fit(probe, memory_max_gb=1)[1], 'physical or commit RAM floor')

    def test_the_idle_check_has_no_memory_term_on_a_mac(self):
        probe = self.probe()
        job = dict(slots=1, memory_max_gb=50, requires=[], requested_defer=None)
        self.assertTrue(fq._fits_idle(job, probe))              # 50 GB beside a 48 GB machine: no reservation, so it fits
        self.assertFalse(fq._fits_idle(dict(job, slots=19), probe))
        with patch.dict(os.environ, {'CQ_LAPTOP_PLATFORM': 'windows'}):
            win = dict(host='laptop', at=self.clock[0], config=fq._laptop_config(self.ctx, dict(logical_cpus=16)),
                       data=dict(total_gb=32, capabilities=['python']))
            self.assertFalse(fq._fits_idle(job, win))           # Windows keeps the cap-plus-floor test

    def test_slots_still_bound_admission_and_a_job_larger_than_the_pool_never_fits(self):
        self.write_config(laptop_slots=2)
        probe = self.probe()
        self.assertEqual(self.fit(probe, slots=3)[1], fq.SLOTS_NEVER)
        live = [dict(id='x', host_id='laptop', status='running', slots=2, memory_max_gb=1)]
        self.assertEqual(fq._fit(self.ctx, dict(slots=1, memory_max_gb=1, requires=[], requested_defer=None), probe, live,
                                 self.clock[0], explain=True)[1], 'CPU slots unavailable')

    def test_a_pressure_hold_does_not_touch_a_running_job(self):
        # admission is only asked about new work: the running job's record is not read or changed by _fit
        live = [dict(id='x', host_id='laptop', status='running', slots=1, memory_max_gb=1)]
        before = json.dumps(live)
        self.assertIsNone(fq._fit(self.ctx, dict(slots=1, memory_max_gb=1, requires=[], requested_defer=None),
                                  self.probe(pressure=4), live, self.clock[0], explain=True)[0])
        self.assertEqual(json.dumps(live), before)

    def test_the_battery_is_information_not_a_condition_by_default(self):
        fit, reason = self.fit(self.probe(battery=True))
        self.assertIsNone(reason)
        self.assertIsNotNone(fit)

    def test_on_battery_no_new_work_is_admitted_when_the_setting_says_so(self):
        self.write_config(laptop_needs_ac=True)
        fit, reason = self.fit(self.probe(battery=True))
        self.assertIsNone(fit)
        self.assertIn('battery', reason)
        self.assertIn('laptop_needs_ac', reason)
        self.assertEqual(fleet_telemetry._reason_word(reason), 'battery')
        self.assertIsNotNone(self.fit(self.probe(battery=False))[0])
        self.assertIsNotNone(self.fit(self.probe(battery=None))[0])   # not told: not taken for battery

    def test_a_long_job_is_left_to_the_hosts_unless_the_laptop_is_its_only_candidate(self):
        probe = self.probe()
        fit, reason = self.fit(probe, est_minutes=30)
        self.assertIsNone(fit)
        self.assertIn('laptop_max_minutes', reason)
        self.assertIsNotNone(self.fit(probe, est_minutes=20)[0])
        self.assertIsNotNone(self.fit(probe, est_minutes=30, candidates=['laptop'])[0])
        self.assertIsNone(self.fit(probe, est_minutes=30, candidates=['laptop', 'hearth'])[0])

    def test_without_an_estimate_the_timeout_is_the_duration(self):
        probe = self.probe()
        self.assertIsNone(self.fit(probe, est_minutes=10, est_stated=False, timeout_minutes=30)[0])
        self.assertIsNotNone(self.fit(probe, est_minutes=10, est_stated=False, timeout_minutes=15)[0])
        self.assertIsNotNone(self.fit(probe, est_minutes=10, est_stated=True, timeout_minutes=480)[0])

    def test_the_limit_is_configurable_and_can_be_off(self):
        self.write_config(laptop_max_minutes=60)
        self.assertIsNotNone(self.fit(self.probe(), est_minutes=45)[0])
        self.write_config(laptop_max_minutes=None)
        self.assertIsNotNone(self.fit(self.probe(), est_minutes=400)[0])

    def test_a_windows_laptop_has_neither_limit_by_default(self):
        with patch.dict(os.environ, {'CQ_LAPTOP_PLATFORM': 'windows'}):
            cfg = fq._laptop_config(self.ctx, dict(logical_cpus=16))
            probe = dict(host='laptop', at=self.clock[0], config=cfg, data=dict(
                available_gb=20, commit_free_gb=20, total_gb=32, logical_cpus=16, capabilities=['python'], on_battery=True))
            self.assertIsNotNone(self.fit(probe, est_minutes=300)[0])

    def test_the_exemption_is_for_a_job_submitted_to_the_laptop_alone(self):
        probe = self.probe()
        recalled = dict(candidates=['laptop'], candidates_submitted=['laptop', 'ember'], est_minutes=45)
        self.assertIn('laptop_max_minutes', self.fit(probe, **recalled)[1])      # a recall shrank the list: the limit stands
        pinned = dict(candidates=['laptop'], candidates_submitted=['laptop'], est_minutes=45)
        self.assertIsNotNone(self.fit(probe, **pinned)[0])

    def test_a_capacity_row_check_is_not_a_job(self):
        # the device row asks with a placeholder job: no estimate, no candidates
        fit, reason = fq._fit(self.ctx, dict(slots=1, memory_max_gb=.25, requires=[], requested_defer=None), self.probe(),
                              [], self.clock[0], explain=True)
        self.assertIsNone(reason)


class LongJobPlacementTests(Fixture):
    """A job over laptop_max_minutes whose other candidates can never run it is not left waiting for them."""

    def setUp(self):
        super().setUp()
        self.mac()
        self.confirm_default_lock()
        self.write_config(laptop_needs_ac=False, ram_floor_gb=2)

    def reserve(self, laptop, **job):
        jid = self.submit(platform='portable', candidates=['laptop', 'ember'], est_minutes=60, memory_gb=1, memory_max_gb=2, **job)['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            placed = fq._reserve(self.ctx, {'laptop': laptop, 'ember': self.remote('ember', slots=4)})
        return jid, placed

    def test_the_laptop_takes_a_long_job_no_host_can_ever_run(self):
        jid, placed = self.reserve(self.probe(), slots=8)       # ember's pool is 4 less its reserve of 2: never
        self.assertEqual([(j['id'], j['host_id']) for j in placed], [(jid, 'laptop')])

    def test_a_long_job_that_fits_a_host_still_waits_for_it(self):
        jid, placed = self.reserve(self.probe(), slots=1)       # ember has room: the laptop's limit sends it there
        self.assertEqual([(j['id'], j['host_id']) for j in placed], [(jid, 'ember')])

    def test_a_long_job_that_cannot_be_placed_anywhere_ever_is_refused_at_plan_time(self):
        jid, placed = self.reserve(self.probe(cpus=4), slots=6)   # the laptop's pool is 4, ember's 2
        self.assertEqual(placed, [])
        job = self.job(jid)
        self.assertEqual(job['status'], 'failed')
        self.assertIn('no candidate can ever run this job', job['reason'])
        self.assertIn(fq.SLOTS_NEVER, job['reason'])

    def test_a_host_that_is_merely_down_does_not_send_a_long_job_to_the_laptop(self):
        jid = self.submit(platform='portable', candidates=['laptop', 'ember'], est_minutes=60, memory_gb=1, memory_max_gb=2)['job']
        with patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False):
            placed = fq._reserve(self.ctx, {'laptop': self.probe()})       # no ember probe: offline, a temporary state
        self.assertEqual(placed, [])
        self.assertEqual(self.job(jid)['status'], 'queued')
        self.assertIn('laptop_max_minutes', self.job(jid)['placement_reasons']['laptop'])

    def test_candidates_as_submitted_are_kept_when_a_recall_shrinks_the_list(self):
        job = self.job(self.submit(platform='portable', candidates=['laptop', 'ember'])['job'])
        self.assertEqual(job['candidates_submitted'], ['laptop', 'ember'])


class RoutingTests(Fixture):
    """`where: laptop` on a Mac is a fleet job pinned to the laptop."""

    def run_submit(self, **changes):
        calls = []

        def leaf(ctx, command, req):
            calls.append((ctx, req))
            return {'ok': True, 'job': 'x', 'note': 'fleet job accepted'}
        req = dict(where='laptop', root=str(self.root), command='make check', session='s1', slots=1)
        req.update(changes)
        ctx = cq.Ctx(state=str(self.state / 'v1'), now=lambda: self.clock[0], sync=True)
        with patch.object(queue_generations, 'context', side_effect=lambda engine, c, gen: ('ctx', gen)):
            result = queue_generations.execute(cq, ctx, 'submit', req, leaf)
        return result, calls

    def test_where_laptop_becomes_a_fleet_job_pinned_to_the_laptop(self):
        self.mac()
        result, calls = self.run_submit()
        (ctx, req), = calls
        self.assertEqual(ctx, ('ctx', 2))
        self.assertEqual((req['where'], req['candidates'], req['platform'], req['slots']), ('fleet', ['laptop'], 'macos', 1))
        self.assertIn('contained fleet job on the Mac', result['note'])
        self.assertTrue(req['label'])
        _, calls = self.run_submit(platform='portable', slots=3)
        self.assertEqual((calls[0][1]['platform'], calls[0][1]['slots']), ('portable', 3))

    def test_what_the_fleet_cannot_do_is_refused_with_the_fleet_form(self):
        self.mac()
        for fields in (dict(slots=0), dict(self_heavy=True), dict(split={'max_parts': 2}), dict(temp_storage='ram'),
                       dict(platform='windows'), dict(platform='linux'), dict(host_id='ember'), dict(candidates=['ember'])):
            with self.subTest(fields=fields), self.assertRaises(cq.CqError) as raised:
                self.run_submit(**fields)
            self.assertIn('candidates: ["laptop"]', str(raised.exception))
            self.assertIn('macos', str(raised.exception))

    def test_off_a_mac_where_laptop_keeps_the_ordinary_path(self):
        _, calls = self.run_submit()
        self.assertEqual(calls[0][1]['where'], 'laptop')

    def test_the_ordinary_queue_refuses_a_macs_laptop_with_the_fleet_form(self):
        self.mac()
        with patch.dict(os.environ, {'CQ_LAPTOP_EXECUTOR': '1'}):
            ctx = cq.Ctx(state=str(self.state), now=lambda: self.clock[0], sync=True)
            with self.assertRaises(cq.CqError) as raised:
                cq.cmd_submit(ctx, dict(where='laptop', root=str(self.root), command='true', session='s1'))
        self.assertIn("where 'fleet'", str(raised.exception))
        self.assertIn('macos', str(raised.exception))


class FiguresTests(Fixture):
    def test_the_figures_are_the_ones_admission_reads(self):
        self.mac()
        self.confirm_default_lock()
        self.write_config(laptop_needs_ac=True, laptop_slots=5)
        raw = dict(supported=True, logical_cpus=18, available_gb=30, commit_free_gb=30, total_gb=48,
                   capabilities=['macos'], boot_identity='b', pressure=1)
        jid = self.submit(platform='macos', candidates=['laptop'], slots=2)['job']
        job = self.job(jid)
        job.update(status='launching', host_id='laptop')
        cq.save_job(self.ctx, job)
        with patch('mac_pool.probe', return_value=raw):
            self.assertEqual(fq.laptop_figures(self.ctx), dict(slots_total=5, slots_used=2, ram_floor_gb=0, needs_ac=True))
        with patch('mac_pool.probe', return_value=dict(raw, supported=False)):
            self.assertEqual(fq.laptop_figures(self.ctx), {})


class FiguresFromOneProbeTests(Fixture):
    def test_laptop_figures_uses_the_probe_it_is_given(self):
        self.mac()
        self.confirm_default_lock()
        raw = dict(supported=True, logical_cpus=18, available_gb=30, commit_free_gb=30, total_gb=48, boot_identity='b', pressure=1)
        with patch('mac_pool.probe', side_effect=AssertionError('probed again')):
            self.assertEqual(fq.laptop_figures(self.ctx, raw), dict(slots_total=18, slots_used=0, ram_floor_gb=0, needs_ac=False))


class HeavyLockFolderTests(Fixture):
    """cq.Laptop.heavy_lock_held creates a missing lock folder only at the queue's own default place."""

    def setUp(self):
        super().setUp()
        if os.name == 'nt':
            self.skipTest('flock')

    def held(self, lock, **env):
        with patch.object(cq, 'HEAVY_LOCK', str(lock)), patch.dict(os.environ, dict(CQ_STATE=str(self.state), **env)):
            return cq.Laptop().heavy_lock_held()

    def test_a_missing_default_folder_is_created_and_not_held(self):
        for name in ('CQ_HEAVY_LOCK', 'CQ_HEAVYRUN', 'CQ_BATON_DIR'):
            os.environ.pop(name, None)
        state = self.state / 'fresh'
        with patch.object(cq, 'HEAVY_LOCK', str(state / 'heavy.lock')), patch.dict(os.environ, {'CQ_STATE': str(state)}):
            self.assertFalse(cq.Laptop().heavy_lock_held())
        self.assertTrue((state / 'heavy.lock').exists())

    def test_a_missing_folder_of_an_explicit_path_is_held_and_not_created(self):
        for env in ({'CQ_HEAVY_LOCK': str(self.state / 'elsewhere' / 'heavy.lock')},
                    {'CQ_BATON_DIR': str(self.state / 'baton')}):
            with self.subTest(env=env):
                lock = Path(env.get('CQ_HEAVY_LOCK') or Path(env['CQ_BATON_DIR']) / 'heavy.lock')
                with patch.object(cq, 'HEAVY_LOCK', str(lock)), patch.dict(os.environ, dict(CQ_STATE=str(self.state), **env)):
                    self.assertTrue(cq.Laptop().heavy_lock_held())
                self.assertFalse(lock.parent.exists())

    def test_a_default_path_that_is_not_the_state_folder_is_not_created_either(self):
        lock = self.state / 'sub' / 'heavy.lock'
        for name in ('CQ_HEAVY_LOCK', 'CQ_HEAVYRUN', 'CQ_BATON_DIR'):
            os.environ.pop(name, None)
        self.assertTrue(self.held(lock))
        self.assertFalse(lock.parent.exists())


class CapacityTests(Fixture):
    class FakeLaptop(cq.Laptop):
        def free_ram_gb(self):
            return 31.5

        def heavy_lock_held(self):
            return False

        def on_battery(self):
            return True

    def test_the_ordinary_capacity_carries_a_macs_real_numbers_and_no_legacy_laptop(self):
        self.mac()
        ctx = cq.Ctx(state=str(self.state), now=lambda: self.clock[0], sync=True, laptop=self.FakeLaptop())
        lp = {"legacy": False, "platform": "macos", "free_ram_gb": 31.5, "on_battery": True, "heavy_busy": False}
        cap = {'laptop': lp}
        self.assertTrue(cq._laptop_off(cap))
        line = cq._laptop_line(lp)
        self.assertIn('31.5 GB free', line)
        self.assertIn('on battery', line)
        self.assertIn('fleet jobs only', line)
        self.assertNotIn(cq.LAPTOP_NOT_EXECUTOR, line)
        self.assertIs(ctx.laptop.on_battery(), True)

    def test_the_fleet_device_row_shows_slots_free_ram_pressure_and_battery_as_information(self):
        self.mac()
        self.confirm_default_lock()
        fq._capacity(self.ctx, {'laptop': self.probe(pressure=2, battery=True, available=7.5)}, [])
        row = json.loads((self.state / 'fleet' / 'capacity.json').read_text())['devices']['laptop']
        self.assertEqual((row['up'], row['eligible'], row['slots_total'], row['slots_used']), (True, False, 18, 0))
        self.assertIsNone(row['memory_floor_gb'])            # no floor on a Mac
        self.assertEqual((row['memory_available_gb'], row['memory_reserved_gb'], row['memory_total_gb']), (7.5, 0, 48))
        self.assertEqual((row['memory_pressure'], row['on_battery']), ('warn', True))
        self.assertIn('memory pressure is warn', row['reason'])
        line = fleet_telemetry.summarize(str(self.state / 'fleet'), self.clock[0])
        self.assertIn('laptop 0/18 pressure', line['line'])
        self.assertEqual(line['devices']['laptop']['memory_pressure'], 'warn')
        fq._capacity(self.ctx, {'laptop': self.probe(pressure=1, battery=True)}, [])
        row = json.loads((self.state / 'fleet' / 'capacity.json').read_text())['devices']['laptop']
        self.assertTrue(row['eligible'])                     # on battery, normal pressure: open
        self.assertEqual(row['memory_pressure'], 'normal')


@unittest.skipUnless(sys.platform == 'darwin', 'macOS processes')
class LaptopNativeTests(Fixture):
    """cq.Laptop, the ordinary queue's side, on a real Mac."""

    def test_free_ram_is_the_vm_stat_figure(self):
        free = cq.Laptop().free_ram_gb()
        self.assertGreater(free, 0)
        self.assertLessEqual(free, 1024)
        import mac_pool
        self.assertAlmostEqual(free, mac_pool.available_gb(), delta=4)

    def test_the_heavy_lock_is_read_with_flock(self):
        import mac_pool
        path = str(self.state / 'heavy.lock')
        with patch.object(cq, 'HEAVY_LOCK', path):
            laptop = cq.Laptop()
            self.assertFalse(laptop.heavy_lock_held())
            shared = mac_pool.SharedLock(path)
            self.assertTrue(shared.try_acquire())
            self.assertTrue(laptop.heavy_lock_held())
            shared.close()
            self.assertFalse(laptop.heavy_lock_held())

    def test_kill_tree_is_a_tree_kill(self):
        import subprocess
        import time
        import mac_pool
        child = subprocess.Popen(['sh', '-c', 'sleep 120 & sleep 120'], start_new_session=True)
        try:
            deadline = time.monotonic() + 10
            while len(mac_pool.tree_of(mac_pool.snapshot(0), child.pid)) < 3 and time.monotonic() < deadline:
                time.sleep(0.05)
            tree = mac_pool.tree_of(mac_pool.snapshot(0), child.pid)
            self.assertGreaterEqual(len(tree), 3)
            cq.Laptop().kill_tree(child.pid)
            child.wait(10)
            for pid in tree:
                deadline = time.monotonic() + 5
                while mac_pool._bsd(pid) is not None and mac_pool._bsd(pid)[0] != mac_pool._SZOMB and time.monotonic() < deadline:
                    time.sleep(0.05)
                info = mac_pool._bsd(pid)
                self.assertTrue(info is None or info[0] == mac_pool._SZOMB, pid)
        finally:
            if child.poll() is None:
                child.kill()


@unittest.skipUnless(sys.platform == 'darwin', 'macOS processes')
class EndToEndTests(Fixture):
    """A macos fleet job through the controller's own steps with the real mac_pool: reserve, stage (the pool is spawned
    detached, as for a first-generation queue), reconcile to the finished record."""

    def run_job(self, command, **changes):
        import subprocess
        import time
        import mac_pool
        self.mac()
        self.confirm_default_lock()
        self.write_config(laptop_needs_ac=False)
        jid = self.submit(platform='macos', candidates=['laptop'], command=command, memory_gb=.5, memory_max_gb=1, **changes)['job']
        data = mac_pool.probe()
        probe = dict(host='laptop', data=data, config=fq._laptop_config(self.ctx, data), at=self.clock[0])
        self.assertIsNotNone(probe['config'])
        heavy = patch.object(self.ctx.laptop, 'heavy_lock_held', return_value=False)
        heavy.start()
        self.addCleanup(heavy.stop)
        job = fq._reserve(self.ctx, {'laptop': probe})[0]
        self.assertEqual((job['id'], job['host_id']), (jid, 'laptop'))

        def checkout(top, sha, dest):
            os.makedirs(dest, exist_ok=True)
            return []
        with patch.object(cq, 'checkout_tree', side_effect=checkout):
            fq._stage_laptop(self.ctx, job)
        staged = self.job(jid)
        self.assertTrue(staged['stage_attempted'])
        self.assertEqual(Path(staged['descriptor_path']).parent.name[:7], 'launch-')
        self.assertRegex(staged['boot_identity'], r'^[0-9a-f-]{36}$')
        descriptor = cq.read_json(staged['descriptor_path'])
        self.assertIs(descriptor['memory_cap_declared'], True)     # the submitter stated memory_max_gb
        self.assertEqual(descriptor['ram_floor_gb'], 0)
        self.assertEqual(descriptor['cpu_slots'], 1)
        self.addCleanup(lambda: Path(staged['descriptor_path']).parent.joinpath('cancel').touch())
        deadline = time.monotonic() + 40
        with patch.object(cq, 'drop_ref'), patch.object(cq, 'capture_local_job', return_value=([], [], [])):
            while time.monotonic() < deadline and self.job(jid)['status'] not in fq.TERMINAL:
                fq._local_reconcile(self.ctx, self.job(jid))
                time.sleep(.2)
        return self.job(jid), staged

    def test_a_macos_job_runs_and_finishes(self):
        job, staged = self.run_job('echo hello-from-the-mac; pwd')
        self.assertEqual(job['status'], 'done')
        self.assertTrue(job['worker_pid'] and job['worker_start_identity'])
        self.assertIn('macOS worker', job.get('reason', 'macOS worker') or 'macOS worker')
        self.assertIn('hello-from-the-mac', Path(staged['log_path']).read_text())

    def test_a_failing_macos_job_reports_its_exit_code(self):
        job, _ = self.run_job('exit 7')
        self.assertEqual((job['status'], job.get('exit')), ('failed', 7))


if __name__ == '__main__':
    unittest.main()
