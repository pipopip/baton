"""The job stores under load: _jobs_in opens a job file only when its size or times changed, archive_jobs moves
finished jobs nothing has written for days out of the scanned stores, and load_job still finds them by ID."""
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cq
import cq_stats

FLEET = "cq-v2--fleet-20261101T000000-0123456789abcdef"


class StoreCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqa")
        self.addCleanup(cq.rmtree_force, self.tmp)
        self.addCleanup(cq._SCAN_CACHE.clear)
        cq._SCAN_CACHE.clear()
        self.ctx = cq.Ctx(state=os.path.join(self.tmp, "state"))

    def put(self, jid, age_s=0.0, **fields):
        """Save one job and set its file `age_s` seconds into the past."""
        job = {"id": jid, "status": "done", "notified": True, **fields}
        cq.save_job(self.ctx, job)
        path = cq.job_file(self.ctx, jid, bool(job.get("defer")))
        if age_s:
            old = time.time() - age_s
            os.utime(path, (old, old))
        return path

    def reads(self):
        """A patch of _job_text that records the file names opened."""
        opened = []
        real = cq._job_text

        def counted(path):
            opened.append(os.path.basename(path))
            return real(path)
        patch = mock.patch.object(cq, "_job_text", counted)
        patch.start()
        self.addCleanup(patch.stop)
        return opened


class ScanTests(StoreCase):
    def test_a_settled_file_is_opened_once(self):
        self.put("cq-a", age_s=60)
        self.put("cq-b", age_s=60, status="failed")
        opened = self.reads()
        first = cq.all_jobs(self.ctx)
        self.assertEqual([j["id"] for j in first], ["cq-a", "cq-b"])
        self.assertEqual(opened, ["cq-a.json", "cq-b.json"])
        del opened[:]
        second = cq.all_jobs(self.ctx)
        self.assertEqual(opened, [])
        self.assertEqual(second, first)

    def test_the_caller_may_change_what_it_gets(self):
        self.put("cq-a", age_s=60, parts=[1, 2])
        got = cq.all_jobs(self.ctx)[0]
        got["status"] = "changed"
        got["parts"].append(3)
        again = cq.all_jobs(self.ctx)[0]
        self.assertEqual((again["status"], again["parts"]), ("done", [1, 2]))
        self.assertIsNot(again, got)

    def test_a_saved_job_is_read_again_and_only_that_one(self):
        self.put("cq-a", age_s=60)
        self.put("cq-b", age_s=60)
        cq.all_jobs(self.ctx)
        opened = self.reads()
        job = cq.load_job(self.ctx, "cq-b")
        job["status"] = "failed"
        cq.save_job(self.ctx, job)
        self.assertEqual([j["status"] for j in cq.all_jobs(self.ctx)], ["done", "failed"])
        self.assertEqual(opened, ["cq-b.json"])

    def test_a_file_that_changed_moments_ago_is_read_every_time(self):
        self.put("cq-a")  # written now: a second write in the same clock tick could keep its size and time
        opened = self.reads()
        cq.all_jobs(self.ctx)
        cq.all_jobs(self.ctx)
        self.assertEqual(opened, ["cq-a.json", "cq-a.json"])

    def test_a_rewrite_with_the_same_size_and_time_is_seen_after_the_refresh_time(self):
        path = self.put("cq-a", age_s=60, status="done")
        stamp = os.stat(path)
        self.assertEqual(cq.all_jobs(self.ctx)[0]["status"], "done")
        with open(path, "rb") as f:
            raw = f.read()
        with open(path, "r+b") as f:  # in place, same length, times put back: the one change size and times cannot show
            f.write(raw.replace(b'"done"', b'"dead"'))
        os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
        after = os.stat(path)
        self.assertEqual((after.st_size, after.st_mtime_ns), (stamp.st_size, stamp.st_mtime_ns))
        if after.st_ctime_ns == stamp.st_ctime_ns:  # where the change time moved (Linux), the next scan already sees it
            self.assertEqual(cq.all_jobs(self.ctx)[0]["status"], "done")
        with mock.patch.object(cq, "SCAN_REFRESH_S", 0):
            self.assertEqual(cq.all_jobs(self.ctx)[0]["status"], "dead")

    def test_a_removed_file_and_a_missing_store(self):
        path = self.put("cq-a", age_s=60)
        self.put("cq-b", age_s=60)
        cq.all_jobs(self.ctx)
        os.remove(path)
        self.assertEqual([j["id"] for j in cq.all_jobs(self.ctx)], ["cq-b"])
        self.assertNotIn("cq-a.json", cq._SCAN_CACHE[os.path.normpath(self.ctx.p("jobs"))])
        cq.rmtree_force(self.ctx.p("jobs"))
        self.assertEqual(cq._jobs_in(self.ctx, "jobs"), [])
        self.assertNotIn(os.path.normpath(self.ctx.p("jobs")), cq._SCAN_CACHE)

    def test_files_that_are_not_jobs_are_left_out(self):
        self.put("cq-a", age_s=60)
        d = self.ctx.p("jobs")
        for name, text in (("list.json", "[1]"), ("noid.json", '{"status": "done"}'), ("notes.txt", "x")):
            with open(os.path.join(d, name), "w") as f:
                f.write(text)
        with open(os.path.join(d, "broken.json"), "w") as f:
            f.write("{")
        with mock.patch.object(cq.time, "sleep"):
            self.assertEqual([j["id"] for j in cq.all_jobs(self.ctx)], ["cq-a"])

    def test_two_stores_and_two_states_do_not_share_entries(self):
        self.put("cq-a", age_s=60)
        self.put("cq-d", age_s=60, defer="overnight")
        self.put(FLEET, age_s=60)
        other = cq.Ctx(state=os.path.join(self.tmp, "other"))
        cq.save_job(other, {"id": "cq-a", "status": "queued"})
        self.assertEqual([j["id"] for j in cq.all_jobs(self.ctx)], ["cq-a", "cq-d"])
        self.assertEqual([j["id"] for j in cq.all_fleet_jobs(self.ctx)], [FLEET])
        self.assertEqual([j["status"] for j in cq.all_jobs(other)], ["queued"])
        self.assertEqual([j["status"] for j in cq.all_jobs(self.ctx)], ["done", "done"])


class ArchiveTests(StoreCase):
    OLD = cq.ARCHIVE_AFTER_S + 3600
    VERY_OLD = cq.ARCHIVE_HELD_S + 3600

    def stored(self, directory="jobs"):
        d = self.ctx.p(directory)
        return sorted(os.listdir(d)) if os.path.isdir(d) else []

    def test_finished_jobs_untouched_for_days_leave_every_store(self):
        self.put("cq-old", age_s=self.OLD)
        self.put("cq-old-failed", age_s=self.OLD, status="failed")
        self.put("cq-recent", age_s=3600)
        self.put("cq-defer", age_s=self.OLD, defer="overnight")
        self.put(FLEET, age_s=self.OLD)
        self.assertEqual(cq.archive_jobs(self.ctx), 4)
        self.assertEqual(self.stored(), ["cq-recent.json"])
        self.assertEqual(self.stored("jobs-archive"), ["cq-old-failed.json", "cq-old.json"])
        self.assertEqual(self.stored("deferred-jobs"), [])
        self.assertEqual(self.stored("deferred-jobs-archive"), ["cq-defer.json"])
        self.assertEqual(self.stored(os.path.join("fleet", "jobs-archive")), [FLEET + ".json"])
        self.assertEqual([j["id"] for j in cq.all_jobs(self.ctx)], ["cq-recent"])
        self.assertEqual(cq.all_fleet_jobs(self.ctx), [])

    def test_a_job_that_is_not_finished_stays_however_old(self):
        for status in ("queued", "launching", "running"):
            self.put("cq-" + status, age_s=self.VERY_OLD, status=status)
        self.assertEqual(cq.archive_jobs(self.ctx), 0)
        self.assertEqual(len(self.stored()), 3)

    def test_a_job_something_waits_on_stays_for_the_longer_time(self):
        held = {"cq-notice": dict(notified=False), "cq-notice-default": dict(notified=None),
                "cq-missing": dict(artifact_missing=["out.txt"]), "cq-pending": dict(artifact_pending=True)}
        free = {"cq-part": dict(notified=False, parent="cq-x"), "cq-silent": dict(notified=False, notify=False),
                "cq-ram-clean": dict(temp_storage="ram", placed_where="host", ram_cleaned=True),
                "cq-ram-never-placed": dict(temp_storage="ram", placed_where=None)}
        for jid, fields in {**held, **free}.items():
            self.put(jid, age_s=self.OLD, **fields)
        self.assertEqual(cq.archive_jobs(self.ctx), len(free))
        self.assertEqual(self.stored(), sorted(j + ".json" for j in held))
        for jid in held:
            old = time.time() - self.VERY_OLD
            os.utime(cq.job_file(self.ctx, jid), (old, old))
        self.assertEqual(cq.archive_jobs(self.ctx, force=True), len(held))
        self.assertEqual(self.stored(), [])

    def test_a_ram_job_whose_host_copy_is_not_cleaned_never_leaves(self):
        self.put("cq-ram", age_s=self.VERY_OLD * 4, temp_storage="ram", placed_where="host")
        self.assertEqual(cq.archive_jobs(self.ctx), 0)
        self.assertEqual(self.stored(), ["cq-ram.json"])  # reserved_memory and cleanup_ram find it by scanning
        job = cq.load_job(self.ctx, "cq-ram")
        job["ram_cleaned"] = True
        cq.save_job(self.ctx, job)
        old = time.time() - self.OLD
        os.utime(cq.job_file(self.ctx, "cq-ram"), (old, old))
        self.assertEqual(cq.archive_jobs(self.ctx, force=True), 1)

    def test_the_sweep_runs_once_an_hour_unless_forced(self):
        self.put("cq-a", age_s=self.OLD)
        self.assertEqual(cq.archive_jobs(self.ctx), 1)
        self.put("cq-b", age_s=self.OLD)
        self.assertEqual(cq.archive_jobs(self.ctx), 0)
        self.assertEqual(self.stored(), ["cq-b.json"])
        old = time.time() - cq.ARCHIVE_EVERY_S - 5
        os.utime(self.ctx.p("archive.at"), (old, old))
        self.assertEqual(cq.archive_jobs(self.ctx), 1)
        self.put("cq-c", age_s=self.OLD)
        self.assertEqual(cq.archive_jobs(self.ctx, force=True), 1)

    def test_an_archived_job_is_still_found_by_id(self):
        self.put("cq-old", age_s=self.OLD, exit=0)
        self.put("cq-defer", age_s=self.OLD, defer="overnight")
        self.put(FLEET, age_s=self.OLD)
        cq.archive_jobs(self.ctx)
        self.assertEqual(cq.load_job(self.ctx, "cq-old")["exit"], 0)
        self.assertEqual(cq.load_job(self.ctx, "cq-defer")["defer"], "overnight")
        self.assertEqual(cq.load_job(self.ctx, FLEET)["id"], FLEET)
        self.assertIsNone(cq.load_job(self.ctx, "cq-never"))
        self.assertIsNone(cq.load_job(self.ctx, FLEET[:-1] + "0"))

    def test_a_store_file_that_cannot_be_read_just_now_does_not_give_the_older_archived_copy(self):
        for jid, defer in (("cq-old", None), ("cq-defer", "overnight"), (FLEET, None)):
            with self.subTest(jid):
                fields = {"defer": defer} if defer else {}
                path = self.put(jid, age_s=self.OLD, version="archived", **fields)
                cq.archive_jobs(self.ctx, force=True)
                self.assertEqual(cq.load_job(self.ctx, jid)["version"], "archived")
                with open(path, "w") as f:  # the record was saved again and is mid-write or locked: not valid JSON now
                    f.write("{")
                with mock.patch.object(cq.time, "sleep"):
                    self.assertIsNone(cq.load_job(self.ctx, jid))
                os.remove(path)
                self.assertEqual(cq.load_job(self.ctx, jid)["version"], "archived")

    def test_an_archive_can_be_read_up_to_an_age(self):
        self.put(FLEET, age_s=self.OLD)
        older = FLEET[:-1] + "0"
        self.put(older, age_s=self.OLD)
        cq.archive_jobs(self.ctx)
        store = os.path.join("fleet", "jobs") + cq.ARCHIVE_SUFFIX
        old = time.time() - 40 * 86400
        os.utime(cq.archived_file(cq.job_file(self.ctx, older)), (old, old))
        self.assertEqual([j["id"] for j in cq._jobs_in(self.ctx, store)], [older, FLEET])
        self.assertEqual([j["id"] for j in cq._jobs_in(self.ctx, store, 30 * 86400)], [FLEET])
        self.assertEqual([j["id"] for j in cq._jobs_in(self.ctx, store)], [older, FLEET])

    def test_saving_an_archived_job_puts_it_back_and_the_store_wins(self):
        self.put("cq-old", age_s=self.OLD)
        cq.archive_jobs(self.ctx)
        job = cq.load_job(self.ctx, "cq-old")
        job["reviewed"] = True
        cq.save_job(self.ctx, job)
        self.assertEqual(self.stored(), ["cq-old.json"])
        self.assertTrue(cq.load_job(self.ctx, "cq-old")["reviewed"])
        self.assertEqual([j["id"] for j in cq.all_jobs(self.ctx)], ["cq-old"])
        old = time.time() - self.OLD
        os.utime(cq.job_file(self.ctx, "cq-old"), (old, old))
        self.assertEqual(cq.archive_jobs(self.ctx, force=True), 1)  # the newer copy replaces the archived one
        self.assertEqual(self.stored(), [])
        self.assertTrue(cq.load_job(self.ctx, "cq-old")["reviewed"])

    def test_a_file_that_is_not_a_job_is_left_where_it_is(self):
        d = self.ctx.p("jobs")
        os.makedirs(d)
        for name, text in (("broken.json", "{"), ("list.json", "[]"), ("nostatus.json", '{"id": "x"}')):
            path = os.path.join(d, name)
            with open(path, "w") as f:
                f.write(text)
            old = time.time() - self.VERY_OLD
            os.utime(path, (old, old))
        self.assertEqual(cq.archive_jobs(self.ctx), 0)
        self.assertEqual(len(self.stored()), 3)

    def test_the_report_reads_the_archive_too(self):
        self.put("cq-old", age_s=self.OLD, t_submitted=1.0)
        self.put("cq-recent", age_s=60, t_submitted=2.0)
        cq.archive_jobs(self.ctx)
        tally = cq_stats.new_tally()
        got = (cq_stats.read_jobs(self.ctx.p("jobs"), tally) + cq_stats.read_jobs(self.ctx.p("jobs") + "-archive", tally))
        self.assertEqual(sorted(j["id"] for j in got), ["cq-old", "cq-recent"])
        reading = {}
        jobs = cq_stats.read_all_jobs(self.ctx.state, None, reading, "ember", {}, [])
        self.assertEqual(sorted(j["id"] for j in jobs), ["cq-old", "cq-recent"])
        # a job saved again after it was archived is in both places: one job, the store's copy, and no repeated id
        job = cq.load_job(self.ctx, "cq-old")
        job["status"] = "failed"
        cq.save_job(self.ctx, job)
        reading = {}
        jobs = cq_stats.read_all_jobs(self.ctx.state, None, reading, "ember", {}, [])
        self.assertEqual(sorted((j["id"], j["status"]) for j in jobs), [("cq-old", "failed"), ("cq-recent", "done")])
        self.assertEqual(reading["jobs"]["duplicates"], 0)


class SamePathTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqp")
        self.addCleanup(cq.rmtree_force, self.tmp)
        self.addCleanup(cq._REAL_PATHS.clear)
        cq._REAL_PATHS.clear()
        self.a = os.path.join(self.tmp, "Work")
        os.makedirs(self.a)

    def resolutions(self):
        seen = []
        real = os.path.realpath
        patch = mock.patch.object(cq.os.path, "realpath", lambda p: (seen.append(p), real(p))[1])
        patch.start()
        self.addCleanup(patch.stop)
        return seen

    def test_a_path_is_resolved_once_for_many_comparisons(self):
        seen = self.resolutions()
        other = os.path.join(self.tmp, "work", ".", "")  # the same directory, written differently
        for _ in range(500):
            self.assertTrue(cq.same_path(self.a, other) if os.name == "nt" else cq.same_path(self.a, self.a + os.sep))
            self.assertFalse(cq.same_path(self.a, self.tmp))
        self.assertLessEqual(len(seen), 3)

    def test_the_answer_is_resolved_again_after_the_keep_time(self):
        seen = self.resolutions()
        cq.same_path(self.a, self.tmp)
        with mock.patch.object(cq, "REAL_PATH_S", 0):
            cq.same_path(self.a, self.tmp)
        self.assertEqual(len(seen), 4)

    def test_values_that_are_not_paths_compare_unequal_and_are_not_kept(self):
        for bad in (None, 5, ["x"], {"a": 1}):
            self.assertFalse(cq.same_path(bad, self.a))
            self.assertFalse(cq.same_path(self.a, bad))
        self.assertEqual(list(cq._REAL_PATHS), [self.a])
        self.assertTrue(cq.same_path("", os.getcwd()))  # as before: an empty path is the current directory

    def test_the_table_does_not_grow_without_bound(self):
        for i in range(1100):
            cq.same_path(os.path.join(self.tmp, "p%d" % i), self.a)
        self.assertLessEqual(len(cq._REAL_PATHS), 1024)


class TickTests(unittest.TestCase):
    def test_the_leading_tick_sweeps_and_a_failed_sweep_does_not_fail_it(self):
        from test_cq import Base

        class One(Base):
            def runTest(self):
                pass
        case = One()
        case.setUp()
        self.addCleanup(case.doCleanups)
        cq.save_job(case.ctx, {"id": "cq-old", "status": "done", "notified": True})
        old = time.time() - cq.ARCHIVE_AFTER_S - 3600
        os.utime(cq.job_file(case.ctx, "cq-old"), (old, old))
        out = case.tick()
        self.assertTrue(out["leader"])
        self.assertEqual(out.get("archived"), 1)
        self.assertEqual(cq.load_job(case.ctx, "cq-old")["status"], "done")
        self.assertNotIn("archived", case.tick())
        with mock.patch.object(cq, "archive_jobs", side_effect=RuntimeError("disk")):
            self.assertTrue(case.tick()["ok"])


if __name__ == "__main__":
    unittest.main()
