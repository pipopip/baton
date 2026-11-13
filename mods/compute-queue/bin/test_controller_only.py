"""The queue on a machine that only controls (a Mac): the laptop is not an executor there, so `where: "laptop"` is
refused, `where: "any"` is placed on a host only, a stored laptop job is never started and says why, and the capacity
and status say so instead of reporting RAM. The platform is decided by cq.laptop_executes, which these tests patch.

Run: python -m unittest test_controller_only   (from the mod's bin folder)
"""
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_cq import Base, J, cap
import cq
import fleet_queue as fq
import test_fleet_queue


class ExecutesTests(unittest.TestCase):
    def test_windows_runs_laptop_jobs(self):
        self.assertTrue(cq.laptop_executes(name="nt", environ={}))

    def test_elsewhere_it_does_not_unless_opted_in(self):
        for name in ("posix", "darwin"):
            self.assertFalse(cq.laptop_executes(name=name, environ={}))
            self.assertFalse(cq.laptop_executes(name=name, environ={"CQ_LAPTOP_EXECUTOR": ""}))
            self.assertTrue(cq.laptop_executes(name=name, environ={"CQ_LAPTOP_EXECUTOR": "1"}))

    def test_an_explicit_zero_turns_it_off_anywhere(self):
        self.assertFalse(cq.laptop_executes(name="nt", environ={"CQ_LAPTOP_EXECUTOR": "0"}))

    def test_the_environment_and_os_name_are_the_defaults(self):
        with mock.patch.dict(os.environ, {"CQ_LAPTOP_EXECUTOR": "0"}):
            self.assertFalse(cq.laptop_executes())
        with mock.patch.dict(os.environ, {"CQ_LAPTOP_EXECUTOR": "1"}):
            self.assertTrue(cq.laptop_executes())


class ControllerOnly(Base):
    """Every test runs with the laptop executor off, unless it says it turns it on to submit what Windows would have."""

    def setUp(self):
        super().setUp()
        patch = mock.patch.object(cq, "laptop_executes", return_value=False)
        self.executes = patch.start()
        self.addCleanup(patch.stop)

    def windows_submit(self, command="echo hi", **kw):
        """A job as a Windows controller would have stored it (a store carried over)."""
        self.executes.return_value = True
        try:
            return self.submit(command, **kw)
        finally:
            self.executes.return_value = False

    def apply(self):
        return cq.cmd_apply(self.ctx, {"plan": None, "reason": "x"})


class SubmitTests(ControllerOnly):
    def test_laptop_is_refused_with_the_reason_and_the_alternative(self):
        for slots in (0, 1):
            with self.assertRaises(cq.CqError) as raised:
                cq.cmd_submit(self.ctx, {"command": "echo hi", "root": self.repo, "session": "sess-A", "where": "laptop", "slots": slots})
            self.assertIn("not an executor on this machine", str(raised.exception))
            self.assertIn("where 'host' or 'fleet'", str(raised.exception))
        self.assertEqual(cq.cmd_list(self.ctx, {})["jobs"], [])

    def test_where_host_and_where_any_are_still_accepted(self):
        host, anywhere = self.submit(where="host"), self.submit("echo two", where="any")
        self.assertEqual((self.job(host)["where"], self.job(anywhere)["where"]), ("host", "any"))

    def test_a_laptop_job_is_accepted_where_the_laptop_executes(self):
        self.executes.return_value = True
        self.assertEqual(self.job(self.submit(where="laptop", slots=0))["where"], "laptop")


class CapacityTests(ControllerOnly):
    def test_capacity_says_not_an_executor_and_reads_no_laptop_figure(self):
        self.laptop.heavy_lock_held = mock.Mock(side_effect=AssertionError("the heavy lock was read"))
        self.laptop.free_ram_gb = mock.Mock(side_effect=AssertionError("free RAM was read"))
        lp = self.tick()["capacity"]["laptop"]
        self.assertIs(lp["executor"], False)
        self.assertIsNone(lp["free_ram_gb"])
        self.assertFalse(lp["ram_ok"])
        self.assertEqual(lp["reason"], "not an executor on this machine")
        self.assertEqual(cq.read_json(self.ctx.p("capacity.json"))["laptop"], lp)

    def test_status_summary_says_so_instead_of_a_ram_figure(self):
        self.tick()
        summary = cq.cmd_status(self.ctx, {})["summary"]
        self.assertEqual(summary["laptop"], "not an executor on this machine")

    def test_the_summary_line_of_an_executor_is_unchanged(self):
        self.assertEqual(cq.capacity_summary({"laptop": {"heavy_busy": False, "free_ram_gb": 16.0}})["laptop"],
                         "16 GB free (heavy jobs need 6)")

    def test_capacity_on_an_executor_is_as_before(self):
        self.executes.return_value = True
        lp = self.tick()["capacity"]["laptop"]
        self.assertNotIn("executor", lp)
        self.assertEqual((lp["free_ram_gb"], lp["heavy_busy"], lp["ram_ok"]), (16.0, False, True))


def off(**kw):
    c = cap(**kw)
    c["laptop"] = {"executor": False, "heavy_busy": False, "free_ram_gb": None, "ram_ok": False, "light_running": 0,
                   "reason": "not an executor on this machine"}
    return c


class PlacementTests(ControllerOnly):
    """The pure planner: a capacity sample that says the laptop is not an executor never offers it."""

    def test_any_is_placed_on_the_host_and_never_on_the_laptop(self):
        for slots in (0, 1):
            job = J("a", where="any", slots=slots)
            self.assertEqual(cq._fits(job, off(free=4), 4, cq._laptop_heavy_free(off()), cq._light_room(off())), ["host"])
            # the host is down: nothing fits (a laptop that executes would take it)
            down = off(up=False)
            self.assertEqual(cq._fits(job, down, 0, cq._laptop_heavy_free(down), cq._light_room(down)), [])
            on = cap(up=False)
            self.assertEqual(cq._fits(job, on, 0, cq._laptop_heavy_free(on), cq._light_room(on)), ["laptop"])

    def test_a_laptop_job_fits_nowhere(self):
        for slots in (0, 1):
            job = J("l", where="laptop", slots=slots)
            self.assertEqual(cq._fits(job, off(), 4, cq._laptop_heavy_free(off()), cq._light_room(off())), [])

    def test_fits_does_not_trust_a_caller_that_says_the_laptop_is_free(self):
        for slots in (0, 1):
            job = J("l", where="any", slots=slots)
            self.assertEqual(cq._fits(job, off(up=False), 0, True, {"host": 4, "laptop": 4}), [])

    def test_the_laptop_has_no_heavy_room_and_no_light_room(self):
        self.assertFalse(cq._laptop_heavy_free(off()))
        self.assertEqual(cq._light_room(off())["laptop"], 0)

    def test_no_reservation_is_made_on_the_laptop(self):
        res = cq.reservations([J("a", where="any"), J("l", where="laptop")], off(free=0), None, 2000.0)
        self.assertIsNone(res["laptop"])

    def test_the_guard_refuses_a_laptop_entry(self):
        res = cq.guard([{"job": "l", "where": "laptop", "slots": 1}], [J("l", where="laptop")], off(), 2000.0)
        self.assertEqual(res["accepted"], [])
        self.assertIn("not an executor on this machine", res["refused"][0]["reason"])

    def test_the_fallback_plan_puts_any_on_the_host_only(self):
        queued = [J("a", where="any", slots=1), J("b", where="any", slots=0)]
        self.assertEqual({(e["job"], e["where"]) for e in cq.fallback_entries(queued, off(free=4), None, 2000.0)},
                         {("a", "host"), ("b", "host")})
        self.assertEqual(cq.fallback_entries(queued, off(free=4, up=False), None, 2000.0), [])
        self.assertEqual(cq.fallback_entries(queued[:1], off(free=0), None, 2000.0), [])


class StoredJobTests(ControllerOnly):
    def test_a_stored_laptop_job_is_never_started_and_its_status_says_why(self):
        jid = self.windows_submit(where="laptop", slots=0)
        self.tick()
        self.assertEqual(self.apply()["accepted"], [])
        self.assertEqual(self.job(jid)["status"], "queued")
        row = cq.cmd_status(self.ctx, {})["queue"][0]
        self.assertIn("not an executor on this machine", row["wait_reason"])
        self.assertIn("where 'host' or 'fleet'", row["wait_reason"])

    def test_a_stored_laptop_job_is_not_started_even_when_the_planner_names_it(self):
        jid = self.windows_submit(where="laptop", slots=0)
        self.tick()
        r = cq.dispatch(self.ctx, {"job": jid, "where": "laptop", "slots": 0})
        self.assertFalse(r["ok"], r)
        self.assertIn("not an executor", r["error"])
        self.assertEqual(self.job(jid)["status"], "queued")

    def test_a_launching_laptop_job_is_failed_not_run(self):
        jid = self.windows_submit(where="laptop", slots=0)
        job = self.job(jid)
        job.update(status="launching", placed_where="laptop")
        cq.save_job(self.ctx, job)
        with mock.patch.object(cq, "launch_laptop") as launch:
            cq.launch(self.ctx, jid)
        launch.assert_not_called()
        self.assertNotEqual(self.job(jid)["status"], "running")

    def test_a_stored_any_job_waits_for_the_host_and_never_the_laptop(self):
        jid = self.windows_submit(where="any", slots=0)
        self.host.up = False
        self.tick()
        self.assertEqual(self.apply()["accepted"], [])
        self.assertEqual(self.job(jid)["status"], "queued")
        why = cq.cmd_status(self.ctx, {})["queue"][0]["wait_reason"]
        self.assertTrue(why.startswith("host down"), why)
        self.assertNotIn("laptop", why)

    def test_a_stored_any_job_starts_on_the_host_when_it_can(self):
        jid = self.windows_submit(where="any", slots=1)
        self.tick()
        accepted = self.apply()["accepted"]
        self.assertEqual([(a["job"], a["where"]) for a in accepted], [(jid, "host")])

    def test_a_busy_host_does_not_push_an_any_job_onto_the_laptop(self):
        jid = self.windows_submit(where="any", slots=1)
        self.host.held = 4
        self.tick()
        self.assertEqual(self.apply()["accepted"], [])
        self.assertEqual(self.job(jid)["status"], "queued")


class FleetOff(test_fleet_queue.FleetTests):
    """Fleet jobs on a controller that is not the Windows laptop. Inherits the fleet fixture; the inherited tests are
    left to test_fleet_queue (`load_tests` below keeps only this class's own)."""

    def setUp(self):
        super().setUp()
        patch = patch_executes = mock.patch.object(cq, "laptop_executes", return_value=False)
        self.executes = patch_executes.start()
        self.addCleanup(patch.stop)

    def stored_as_windows(self, **changes):
        """A fleet job as a Windows controller would have stored it."""
        self.executes.return_value = True
        try:
            return self.submit(**changes)["job"]
        finally:
            self.executes.return_value = False

    def test_a_windows_job_is_refused_at_admission_with_the_reason(self):
        for changes in (dict(platform="windows", candidates=["laptop"]), dict(platform="windows"),
                        dict(platform="windows", candidates=["laptop", "ember"])):
            with self.subTest(changes=changes), self.assertRaises(cq.CqError) as raised:
                self.submit(**changes)
            text = str(raised.exception)
            self.assertIn("no configured device can run this job", text)
            self.assertIn("not an executor on this machine", text)
            self.assertIn("platform linux or portable", text)
        self.assertEqual(self.snapshot_mock.call_count, 0)
        self.assertEqual(fq.all_jobs(self.ctx), [])

    def test_a_portable_job_is_placed_on_a_remote_host_and_the_laptop_is_no_candidate(self):
        jid = self.submit(platform="portable", memory_gb=2, memory_max_gb=4)["job"]
        self.assertNotIn("laptop", self.job(jid)["candidates"])
        self.assertEqual(sorted(self.job(jid)["candidates"]), ["ember", "hearth"])
        chosen = fq._reserve(self.ctx, {"ember": self.remote()})
        self.assertEqual([c["host_id"] for c in chosen], ["ember"])
        self.assertEqual(self.job(jid)["status"], "launching")

    def test_a_portable_job_that_names_only_the_laptop_is_refused(self):
        with self.assertRaises(cq.CqError) as raised:
            self.submit(platform="portable", candidates=["laptop"])
        self.assertIn("not an executor on this machine", str(raised.exception))

    def test_a_portable_job_with_the_laptop_among_its_candidates_keeps_the_others(self):
        jid = self.submit(platform="portable", candidates=["laptop", "ember"])["job"]
        self.assertEqual(self.job(jid)["candidates"], ["ember"])

    def test_a_linux_job_is_unaffected(self):
        jid = self.submit(candidates=["ember"])["job"]
        self.assertEqual([c["host_id"] for c in fq._reserve(self.ctx, {"ember": self.remote()})], ["ember"])
        self.assertEqual(self.job(jid)["status"], "launching")

    def test_a_stored_windows_job_is_marked_never_fits_with_the_reason(self):
        jid = self.stored_as_windows(platform="windows", candidates=["laptop"])
        self.assertEqual(fq._reserve(self.ctx, {"ember": self.remote()}), [])
        job = self.job(jid)
        self.assertEqual(job["status"], "failed")
        self.assertIn("no candidate can ever run this job", job["reason"])
        self.assertIn(fq.NO_LAPTOP, job["reason"])

    def test_a_stored_windows_job_fails_the_same_when_no_probe_answers(self):
        jid = self.stored_as_windows(platform="windows", candidates=["laptop"])
        self.assertEqual(fq._reserve(self.ctx, {}), [])
        self.assertEqual(self.job(jid)["status"], "failed")

    def test_a_stored_portable_job_with_the_laptop_waits_for_the_hosts_and_never_fails(self):
        jid = self.stored_as_windows(platform="portable", candidates=["laptop", "ember"])
        self.assertEqual(self.job(jid)["candidates"], ["laptop", "ember"])
        self.assertEqual(fq._reserve(self.ctx, {}), [])
        job = self.job(jid)
        self.assertEqual(job["status"], "queued")
        self.assertEqual(job["placement_reasons"]["laptop"], fq.NO_LAPTOP)
        self.assertEqual([c["host_id"] for c in fq._reserve(self.ctx, {"ember": self.remote()})], ["ember"])

    def test_the_laptop_is_never_picked_even_when_a_probe_for_it_arrives(self):
        jid = self.stored_as_windows(platform="portable", candidates=["laptop", "ember"])
        with mock.patch.object(self.ctx.laptop, "heavy_lock_held", return_value=False):
            chosen = fq._reserve(self.ctx, {"laptop": self.laptop(), "ember": self.remote(avail=2)})
        self.assertEqual(chosen, [])
        self.assertEqual(self.job(jid)["status"], "queued")

    def test_the_capacity_row_of_the_laptop_says_why(self):
        fq._capacity(self.ctx, {"ember": self.remote()}, fq.all_jobs(self.ctx))
        devices = cq.read_json(self.ctx.p("fleet", "capacity.json"))["devices"]
        self.assertEqual((devices["laptop"]["up"], devices["laptop"]["eligible"]), (False, False))
        self.assertEqual(devices["laptop"]["reason"], fq.NO_LAPTOP)
        self.assertEqual(devices["hearth"]["reason"], "probe unavailable")


def load_tests(loader, tests, pattern):
    """FleetOff inherits FleetTests' fixture, not its tests: only the methods FleetOff defines run here."""
    suite = unittest.TestSuite()
    for name, case in list(globals().items()):
        if isinstance(case, type) and issubclass(case, unittest.TestCase) and case.__module__ == __name__:
            own = [n for n in loader.getTestCaseNames(case) if n in case.__dict__] if case is FleetOff else loader.getTestCaseNames(case)
            suite.addTests(case(n) for n in own)
    return suite


if __name__ == "__main__":
    unittest.main()
