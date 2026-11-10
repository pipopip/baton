"""Unit tests for cq_stats.py. Deterministic: synthetic state dirs only.

Run with: python -I test_cq_stats.py
Fixture timeline (minutes after B, host pool of 8 slots):
  A   verify, 5 slots, failed    sub 0,  start 0,   fin 100
  L1  lane, 1 slot               sub 10, start 14,  fin 24
  L2  lane, 2 slots              sub 20, start 60,  fin 90
  L3  fixcheck, 1 slot           sub 30, start 100, fin 130
  X   lane, 1 slot, cores 1.2    sub 40, start 130, fin 140
  Y   lane, 1 slot, failed       sub 140, start 140, fin 150, cpu_s 1200
  P   split parent (2 slots)     sub 0, start 0, fin 60, children P.p1 and P.p2 (excluded)
  P.p1 1 slot                    sub 0, start 0,   fin 60
  P.p2 1 slot                    sub 0, start 20,  fin 60
  C   cancelled, never started   sub 5, finished 60
B is 22:00 UTC, so the timeline crosses UTC midnight at minute 120.
"""

import contextlib
import hashlib
import io
import json
import datetime as dt
import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import cq_stats  # noqa: E402

DAY0 = dt.datetime(2023, 12, 9, tzinfo=dt.timezone.utc).timestamp()
B = DAY0 + 22 * 3600
NOW = B + 1000 * 60


def at(minutes):
    return B + 60.0 * minutes


def iso(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def job(job_id, **kw):
    base = {"id": job_id, "label": job_id, "status": "done", "priority": "lane",
            "workstream": "lane_a", "where": "host", "placed_where": "host",
            "slots": 1, "slots_eff": 1, "est_minutes": 10, "command": "go test ./...",
            "exit": 0}
    base.update(kw)
    return {k: v for k, v in base.items() if v is not None}


FIXTURE_JOBS = [
    job("cq-A", label="integ verify k", status="failed", priority="verify", slots=5,
        slots_eff=5, t_submitted=at(0), t_started=at(0), t_finished=at(100), minutes=100.0,
        command="bash integ-verify-k.sh", exit=1),
    job("cq-L1", t_submitted=at(10), t_started=at(14), t_finished=at(24), minutes=10.0),
    job("cq-L2", slots=2, slots_eff=2, t_submitted=at(20), t_started=at(60),
        t_finished=at(90), minutes=30.0),
    job("cq-L3", priority="fixcheck", t_submitted=at(30), t_started=at(100),
        t_finished=at(130), minutes=30.0),
    job("cq-X", t_submitted=at(40), t_started=at(130), t_finished=at(140), minutes=10.0,
        cores=1.2),
    job("cq-Y", status="failed", exit=1, t_submitted=at(140), t_started=at(140),
        t_finished=at(150), minutes=10.0, cpu_s=1200),
    job("cq-P", slots=2, slots_eff=2, children=["cq-P.p1", "cq-P.p2"], t_submitted=at(0),
        t_started=at(0), t_finished=at(60), minutes=60.0),
    job("cq-P.p1", t_submitted=at(0), t_started=at(0), t_finished=at(60), minutes=60.0),
    job("cq-P.p2", t_submitted=at(0), t_started=at(20), t_finished=at(60), minutes=40.0),
    job("cq-C", status="cancelled", placed_where=None, t_submitted=at(5), t_finished=at(60)),
]

FIXTURE_HISTORY = [
    {"kind": "go vet", "est": 6, "actual": 0.6, "slots": 1, "status": "done",
     "where": "host", "workstream": "lane_a", "exit": 0, "t": "2023-12-09T23:00:00Z"},
    {"kind": "go vet", "est": 5, "actual": 6.0, "slots": 1, "status": "done",
     "where": "host", "workstream": "lane_a", "exit": 0, "t": "2023-12-09T23:10:00Z"},
    {"kind": "python3 mutate.py", "est": 20, "actual": 40, "slots": 2, "status": "failed",
     "where": "host", "workstream": "lane_a", "exit": 1, "t": "2023-12-09T23:20:00Z"},
    {"kind": "python3 mutate.py", "est": 10, "actual": 4, "slots": 2, "status": "done",
     "where": "host", "workstream": "lane_a", "exit": 0, "t": "2023-12-09T23:30:00Z"},
]

FIXTURE_DECISIONS = [
    {"kind": "plan", "mode": "haiku", "t": "2023-12-09T22:10:00Z", "free_host": 3,
     "queued": 3, "note": "", "why": "",
     "accepted": [{"job": "cq-L1", "slots": 1, "where": "host", "dispatched": True}],
     "refused": [{"job": "cq-L2", "reason": "its slots are reserved for the head job that fits now"},
                 {"job": "cq-L3", "reason": "needs 2 slots, 0 free"}]},
    {"kind": "plan", "mode": "haiku", "t": "2023-12-09T22:40:00Z", "free_host": 2,
     "queued": 3, "note": "", "why": "",
     "accepted": [{"job": "cq-X", "slots": 1, "where": "host", "backfill": True},
                  {"job": "cq-Y", "slots": 1, "where": "host", "backfill": False}],
     "refused": [{"job": "cq-P", "reason": "load cap: load 8.3 + 1.5*3 slots > 12"},
                 {"job": "cq-A", "reason": "laptop heavy lock busy or free RAM below 6 GB"},
                 {"job": "cq-C", "reason": "unknown or not queued"}]},
]


def write_jsonl(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(row if isinstance(row, str) else json.dumps(row))
            fh.write("\n")


def write_state(state, jobs, history=(), decisions=()):
    os.makedirs(os.path.join(state, "jobs"), exist_ok=True)
    for j in jobs:
        with open(os.path.join(state, "jobs", j["id"] + ".json"), "w", encoding="utf-8") as fh:
            json.dump(j, fh)
    write_jsonl(os.path.join(state, "history.jsonl"), list(history))
    write_jsonl(os.path.join(state, "decisions.jsonl"), list(decisions))


def snapshot(root):
    out = {}
    for dirpath, _, names in os.walk(root):
        for name in names:
            path = os.path.join(dirpath, name)
            with open(path, "rb") as fh:
                digest = hashlib.sha256(fh.read()).hexdigest()
            out[os.path.relpath(path, root)] = (digest, os.path.getsize(path),
                                                os.path.getmtime(path))
    return out


def find(report, prefix):
    for sec in report["sections"]:
        if sec["title"].startswith(prefix):
            return sec
    raise AssertionError("no section " + prefix)


def table(report, prefix, index=0):
    return find(report, prefix)["tables"][index]["rows"]


def rows_by(rows, *keys):
    return {tuple(r[:len(keys)]): r for r in rows}


class PercentileTests(unittest.TestCase):
    def test_linear_interpolation(self):
        xs = [1, 2, 3, 4]
        self.assertEqual(cq_stats.percentile(xs, 50), 2.5)
        self.assertEqual(cq_stats.percentile(xs, 75), 3.25)
        self.assertAlmostEqual(cq_stats.percentile(xs, 90), 3.7)
        self.assertEqual(cq_stats.percentile(xs, 0), 1)
        self.assertEqual(cq_stats.percentile(xs, 100), 4)

    def test_empty_is_none(self):
        self.assertIsNone(cq_stats.percentile([], 50))

    def test_refusal_buckets(self):
        bucket = cq_stats.refusal_bucket
        self.assertEqual(bucket("its slots are reserved for the head job"), "reservation")
        self.assertEqual(bucket("needs 3 slots, 1 free"), "slots")
        self.assertEqual(bucket("load cap: load 7.84 + 1.5*3 slots > 12"), "load cap")
        self.assertEqual(bucket("laptop heavy lock busy or free RAM below 6 GB"), "RAM floor")
        self.assertEqual(bucket("light cap reached"), "light cap")
        self.assertEqual(bucket("stale sample"), "stale sample")
        self.assertEqual(bucket("unknown or not queued"), "other")
        self.assertEqual(bucket(None), "other")


class FixtureReportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="cqs")
        cls.state = os.path.join(cls.tmp, "state")
        write_state(cls.state, FIXTURE_JOBS, FIXTURE_HISTORY, FIXTURE_DECISIONS)
        with open(os.path.join(cls.state, "jobs", "broken.json"), "w") as fh:
            fh.write("{")
        with open(os.path.join(cls.state, "history.jsonl"), "a", encoding="utf-8") as fh:
            fh.write("not json\n")
            fh.write('{"kind": "go vet", "est": 5}\n')
        with open(os.path.join(cls.state, "decisions.jsonl"), "a", encoding="utf-8") as fh:
            fh.write("{not json\n")
        cls.report = cq_stats.build_report(cls.state, None, 8, NOW, "cmd")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_job_counts(self):
        rows = rows_by(table(self.report, "Jobs"), "status", "priority")
        self.assertEqual(rows[("status", "done")][2], 7)
        self.assertEqual(rows[("status", "failed")][2], 2)
        self.assertEqual(rows[("status", "cancelled")][2], 1)
        self.assertEqual(rows[("priority", "verify")][2], 1)
        self.assertEqual(rows[("placed_where", "unplaced")][2], 1)
        self.assertEqual(rows[("slots", "1")][2], 7)
        self.assertIn("split parents: 1", find(self.report, "Jobs")["notes"][0])

    def test_wait_and_run_percentiles(self):
        rows = rows_by(table(self.report, "Queue wait"), "metric", "group")
        wait = rows[("wait", "all")]
        # waits (min): 0, 0, 0, 4, 20, 40, 70, 90
        self.assertEqual(wait[2], 8)
        for got, want in zip(wait[3:], [12.0, 47.5, 76.0, 88.6, 90]):
            self.assertAlmostEqual(got, want)
        run = rows[("run", "all")]
        # runs (min): 10, 10, 10, 30, 30, 40, 60, 100 (parent P excluded)
        self.assertEqual(run[2], 8)
        for got, want in zip(run[3:], [30.0, 45.0, 72.0, 97.2, 100]):
            self.assertAlmostEqual(got, want)
        self.assertEqual(rows[("run", "priority=verify")][3], 100)
        self.assertEqual(rows[("wait", "slots=2")][2], 1)

    def test_estimate_accuracy(self):
        rows = rows_by(table(self.report, "Estimate accuracy"), "kind")
        allrow = rows[("all",)]
        self.assertEqual(allrow[1], 4)
        self.assertEqual(allrow[2], 8.0)
        self.assertEqual(allrow[3], 5.0)
        self.assertAlmostEqual(allrow[4], 0.8)
        self.assertEqual(allrow[5:], [0.5, 0.5])
        vet = rows[("go vet",)]
        self.assertEqual(vet[1:3], [2, 5.5])
        self.assertAlmostEqual(vet[3], 3.3)
        self.assertAlmostEqual(vet[4], 0.65)
        self.assertEqual(vet[5:], [0.5, 0.5])
        mutate = rows[("python3 mutate.py",)]
        self.assertEqual(mutate[2:4], [15.0, 22.0])
        self.assertAlmostEqual(mutate[4], 1.2)

    def test_kinds_slot_minutes(self):
        rows = rows_by(table(self.report, "Kinds"), "kind")
        mutate = rows[("python3 mutate.py",)]
        self.assertEqual(mutate[1:], [2, 44.0, 88.0, 22.0])
        vet = rows[("go vet",)]
        self.assertEqual(vet[1], 2)
        for got, want in zip(vet[2:], [6.6, 6.6, 3.3]):
            self.assertAlmostEqual(got, want)
        self.assertEqual(table(self.report, "Kinds")[0][0], "python3 mutate.py")

    def test_daily_split_across_midnight(self):
        rows = table(self.report, "Utilisation per UTC day")
        day1, day2, allrow = rows[0], rows[1], rows[-1]
        self.assertEqual(day1[0], "2023-12-09")
        self.assertEqual(day1[1], "partial (first day)")
        self.assertEqual(day1[2], 120.0)
        self.assertAlmostEqual(day1[4], 690.0)   # 500 + 60 + 40 + 10 + 60 + 20
        self.assertEqual(day1[5], 960.0)         # 8 x 120
        self.assertAlmostEqual(day1[6], 690 / 960)
        self.assertEqual(day2[0], "2023-12-10")
        self.assertEqual(day2[1], "partial (last day)")
        self.assertEqual(day2[2], 30.0)
        self.assertAlmostEqual(day2[4], 30.0)
        self.assertEqual(day2[5], 240.0)
        self.assertEqual(allrow[4], 720.0)
        self.assertAlmostEqual(allrow[6], 0.6)

    def test_concurrency_grid(self):
        rows = rows_by(table(self.report, "Concurrency"), "measure")
        self.assertEqual(rows[("grid points (1 min)",)][1], 150)
        self.assertAlmostEqual(rows[("held >= 7 slots",)][1], 76 / 150)
        self.assertAlmostEqual(rows[("held == 8 slots",)][1], 4 / 150)
        self.assertAlmostEqual(rows[("a queued job waiting",)][1], 130 / 150)
        stranded_label = "fitting job waiting (stranded): free slots while a job that fits was waiting"
        self.assertAlmostEqual(rows[(stranded_label,)][1], 120 / 150)
        self.assertEqual(rows[("fitting job waiting (stranded) minutes",)][1], 120)
        self.assertEqual(rows[("idle slot-minutes",)][1], 480.0)
        self.assertEqual(rows[("idle slot-minutes while a fitting job waited",)][1], 334.0)
        self.assertAlmostEqual(rows[("share of idle slot-minutes that were stranded",)][1],
                               334 / 480)
        self.assertEqual(rows[("max held slots",)][1], 8)

    def test_failed_waste(self):
        rows = table(self.report, "Failed-run waste")
        self.assertEqual(rows[0][:3], ["failed or timeout", 2, 510.0])
        self.assertAlmostEqual(rows[0][3], 510 / 720)
        self.assertEqual(rows[1][:3], ["failed or timeout, slots >= 5", 1, 500.0])
        self.assertAlmostEqual(rows[1][3], 500 / 720)
        top = table(self.report, "Failed-run waste", 1)
        self.assertEqual(top[0][0], "cq-A")
        self.assertEqual(top[0][2:], [5, 100.0, 500.0])
        self.assertEqual(top[1][0], "cq-Y")
        self.assertEqual(len(top), 2)

    def test_refusals(self):
        rows = rows_by(table(self.report, "Refusals"), "bucket")
        self.assertEqual(rows[("reservation",)][1], 1)
        self.assertEqual(rows[("slots",)][1], 1)
        self.assertEqual(rows[("load cap",)][1], 1)
        self.assertEqual(rows[("RAM floor",)][1], 1)
        self.assertEqual(rows[("other",)][1], 1)
        self.assertEqual(rows[("light cap",)][1], 0)
        self.assertAlmostEqual(rows[("slots",)][2], 0.2)
        notes = find(self.report, "Refusals")["notes"]
        self.assertIn("accepted entries: 3", notes[0])
        self.assertIn("backfill true: 1 (field present on 2 of 3", notes[2])

    def test_cpu_per_slot(self):
        rows = rows_by(table(self.report, "CPU use"), "slots")
        one = rows[(1,)]
        self.assertEqual(one[1], 2)
        self.assertAlmostEqual(one[2], 1.6)
        self.assertAlmostEqual(one[3], 1.92)
        self.assertEqual(one[6], 0.5)
        self.assertEqual(rows[("all",)][1], 2)

    def test_footer_counts_malformed_rows(self):
        notes = find(self.report, "Footer")["notes"]
        self.assertIn("jobs: 10 rows read, 1 malformed skipped", " ".join(notes))
        self.assertIn("history.jsonl: 4 rows read, 2 malformed skipped", " ".join(notes))
        self.assertIn("decisions.jsonl: 2 rows read, 1 malformed skipped", " ".join(notes))
        self.assertIn("Covered host span: 2023-12-09 22:00 UTC to 2023-12-10 00:30 UTC",
                      " ".join(notes))
        self.assertEqual(notes[-1], "Reproduce: `cmd`")

    def test_markdown_renders_all_sections(self):
        text = cq_stats.render_markdown(self.report)
        for number in range(1, 12):
            self.assertIn(f"## {number}. ", text)
        self.assertIn("## 2. Queue wait and run time (minutes)", text)
        self.assertIn("| 8 | 12.00 | 47.50 | 76.00 | 88.60 | 90.00 |", text)

    def test_json_round_trip(self):
        data = json.loads(json.dumps(self.report))
        self.assertEqual(len(data["sections"]), 16)
        self.assertEqual(data["slots"], 8)


SAMPLE_ROWS = [
    {"t": iso(at(0)), "at": at(0), "load1": 0.5, "mem_avail_gb": 10.0, "free": 4, "room": 4, "queued": 0,
     "waiting_fit": 0, "head_state": None},
    {"t": iso(at(1)), "at": at(1), "load1": 1.0, "mem_avail_gb": 12.0, "free": 0, "room": 0, "queued": 2,
     "waiting_fit": 0, "head_state": "slots"},
    "not json",
    {"t": iso(at(2)), "at": at(2), "load1": 2.0, "mem_avail_gb": 14.0, "free": 2, "room": 0, "queued": 1,
     "waiting_fit": 0, "head_state": "caps"},
    {"t": iso(at(5)), "at": at(5), "load1": 3.0, "mem_avail_gb": 16.0, "free": 3, "room": 3, "queued": 1,
     "waiting_fit": 1, "head_state": "fits"},
    {"t": iso(at(6)), "queued": 0},  # no "at": malformed
    {"t": iso(at(9)), "at": at(9), "load1": 4.0, "mem_avail_gb": 18.0, "free": 4, "room": 4, "queued": 0,
     "waiting_fit": 0, "head_state": None},
]


class SampleSectionTests(unittest.TestCase):
    """Section 11 weights each row by its gap to the next row, capped at 180 s; the last row has no gap."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqs")
        self.state = os.path.join(self.tmp, "state")
        write_state(self.state, [])
        write_jsonl(os.path.join(self.state, "samples.jsonl"), SAMPLE_ROWS)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_shares_are_weighted_by_gap_capped_at_180_seconds(self):
        # gaps to the next valid row: 60, 60, 180, and 240 capped to 180; the last row (at 540 s) has no gap: weight 0
        report = cq_stats.build_report(self.state, None, 8, NOW, "cmd")
        shares = [row[1] for row in table(report, "Capacity samples", 1)]
        expected = [60, 60, 180, 180, 60, 180]  # idle, saturated, cap-blocked, dispatch, slots, caps (out of 480)
        self.assertEqual(len(shares), len(expected))
        for got, weight in zip(shares, expected):
            self.assertAlmostEqual(got, weight / 480.0)
        self.assertAlmostEqual(shares[2], 0.375)

    def test_rotated_file_is_read_first_and_the_gaps_straddle_the_boundary(self):
        # samples.jsonl.1 holds the first three lines (two rows and a malformed one), samples.jsonl the rest
        write_jsonl(os.path.join(self.state, "samples.jsonl.1"), SAMPLE_ROWS[:3])
        write_jsonl(os.path.join(self.state, "samples.jsonl"), SAMPLE_ROWS[3:])
        report = cq_stats.build_report(self.state, None, 8, NOW, "cmd")
        # the same rows in the same order as one file: the at(1) to at(2) gap (60 s, slots) crosses the boundary
        shares = [row[1] for row in table(report, "Capacity samples", 1)]
        for got, weight in zip(shares, [60, 60, 180, 180, 60, 180]):
            self.assertAlmostEqual(got, weight / 480.0)
        notes = " ".join(find(report, "Capacity samples")["notes"])
        self.assertIn("Rows read: 5, malformed skipped: 2, dropped by --since: 0.", notes)
        self.assertIn("Covered span: 2023-12-09 22:00 UTC to 2023-12-09 22:09 UTC.", notes)

    def test_load_and_memory_medians_and_p90(self):
        report = cq_stats.build_report(self.state, None, 8, NOW, "cmd")
        load, mem = table(report, "Capacity samples", 0)
        self.assertEqual(load[:2], ["load1", 5])
        self.assertAlmostEqual(load[2], 2.0)
        self.assertAlmostEqual(load[3], 3.6)
        self.assertEqual(mem[:2], ["mem_avail_gb", 5])
        self.assertAlmostEqual(mem[2], 14.0)
        self.assertAlmostEqual(mem[3], 17.2)

    def test_notes_count_rows_span_and_average_queue(self):
        report = cq_stats.build_report(self.state, None, 8, NOW, "cmd")
        notes = " ".join(find(report, "Capacity samples")["notes"])
        self.assertIn("Rows read: 5, malformed skipped: 2, dropped by --since: 0.", notes)
        self.assertIn("Covered span: 2023-12-09 22:00 UTC to 2023-12-09 22:09 UTC.", notes)
        # queued > 0 for 60 + 180 + 180 s, with 2, 1 and 1 queued: (120 + 180 + 180) / 420
        self.assertIn("(weighted by gap, rows with queued > 0): 1.14.", notes)
        self.assertEqual(len(find(report, "Capacity samples")["footnotes"]), 1)

    def test_skipped_when_absent_or_empty(self):
        report = cq_stats.build_report(os.path.join(self.tmp, "nope"), None, 8, NOW, "cmd")
        self.assertEqual(find(report, "Capacity samples")["skipped"], "no samples.jsonl rows")
        write_jsonl(os.path.join(self.state, "samples.jsonl"), [])
        report = cq_stats.build_report(self.state, None, 8, NOW, "cmd")
        self.assertEqual(find(report, "Capacity samples")["skipped"], "no samples.jsonl rows")

    def test_json_and_markdown_carry_the_section(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            cq_stats.main(["--state", self.state, "--json"])
        titles = [sec["title"] for sec in json.loads(buf.getvalue())["sections"]]
        self.assertEqual(titles[-7:], ["Capacity samples (samples.jsonl)", "Host CPU (samples)", "Shared host (benchmark)",
                                       "Laptop RAM (samples)", "Night window use (00:00-08:00 Europe/London)",
                                       "Deferred work", "Footer"])
        text = cq_stats.render_markdown(cq_stats.build_report(self.state, None, 8, NOW, "cmd"))
        self.assertIn("## 10. Capacity samples (samples.jsonl)", text)
        self.assertIn("| dispatch latency: a fitting job waits, nothing blocks | 37.5% |", text)


def sample(minutes, **kw):
    """A sample row at `minutes` after B, with the fields section 10 reads."""
    row = {"t": iso(at(minutes)), "at": at(minutes), "queued": 0, "free": 0, "room": 0,
           "waiting_fit": 0, "head_state": None}
    row.update(kw)
    return row


class HostAndLaptopSampleTests(unittest.TestCase):
    """Sections 11 to 13: gap weights as in section 10, idle-while-caps, the benchmark bands, slopes, odd values."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqs")
        self.state = os.path.join(self.tmp, "state")
        write_state(self.state, [])

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def report(self, rows, jobs=()):
        write_state(self.state, list(jobs))
        write_jsonl(os.path.join(self.state, "samples.jsonl"), rows)
        return cq_stats.build_report(self.state, None, 8, NOW, "cmd")

    def test_idle_while_caps_refuse(self):
        # minute 0 is another state (weight 60 s); minutes 1-4 are caps with idle 30, 5, 25, 0 (60 s each);
        # minute 5 closes the interval and has weight 0: caps time 240 s of 300 s
        rows = [sample(0, cpu_busy=50.0, cpu_idle=70.0, head_state="slots", queued=2, free=0)]
        for minute, idle in zip((1, 2, 3, 4), (30.0, 5.0, 25.0, 0.0)):
            rows.append(sample(minute, cpu_busy=90.0, cpu_idle=idle, head_state="caps", queued=1, free=2, room=0))
        rows.append(sample(5, cpu_busy=50.0, cpu_idle=70.0))
        decisive = table(self.report(rows), "Host CPU (samples)", 1)
        self.assertEqual([r[0] for r in decisive], ["caps time: share with cpu_idle >= 20",
                                                   "caps time: share with cpu_idle >= 10",
                                                   "caps time: weighted mean cpu_idle",
                                                   "all time: share in caps"])
        self.assertAlmostEqual(decisive[0][1], 0.5)
        self.assertAlmostEqual(decisive[1][1], 0.5)
        self.assertAlmostEqual(decisive[2][1], 15.0)
        self.assertAlmostEqual(decisive[3][1], 240.0 / 300.0)

    def test_measure_table_uses_only_rows_that_have_the_key(self):
        rows = [sample(0, cpu_busy=10.0), sample(1, cpu_busy=None), sample(2, cpu_busy=30.0),
                sample(3, cpu_busy="50"), sample(4, cpu_busy=20.0, runq=3)]
        measures = {r[0]: r for r in table(self.report(rows), "Host CPU (samples)", 0)}
        self.assertEqual(measures["cpu_busy"][1:3], [3, 20.0])
        self.assertEqual(measures["runq"][1:3], [1, 3.0])
        self.assertEqual(measures["cpu_idle"][1:], [0, None, None])

    def test_class_table_orders_by_mean_cores(self):
        tops = [[["verify", 4.0], ["lane", 1.0]], [["lane", 2.0]], [["lane", 3.0], ["fixcheck", 0.5]], None]
        rows = [sample(i, cpu_busy=60.0, top=top) for i, top in enumerate(tops)]
        classes = table(self.report(rows), "Host CPU (samples)", 2)
        # three rows carry a top list (the null row does not): lane 6/3, verify 4/3, fixcheck 0.5/3
        self.assertEqual([name for name, _ in classes], ["lane", "verify", "fixcheck"])
        for (_, got), want in zip(classes, [2.0, 4 / 3, 1 / 6]):
            self.assertAlmostEqual(got, want)

    def test_go_build_classes_get_a_summed_row_across_the_go_tool_split(self):
        # an old row (go-tool) and a new one (go-compile, go-link, go-vet): the sum row reads (4 + 3 + 0.5 + 0.5) / 2
        tops = [[["go-tool", 4.0], ["go-test", 1.0]], [["go-compile", 3.0], ["go-link", 0.5], ["go-vet", 0.5]]]
        rows = [sample(i, cpu_busy=60.0, top=top) for i, top in enumerate(tops)]
        classes = dict(table(self.report(rows), "Host CPU (samples)", 2))
        self.assertAlmostEqual(classes["go build tools, all (go-tool + go-compile + go-link + go-vet)"], 4.0)
        self.assertAlmostEqual(classes["go-tool"], 2.0)
        no_go = table(self.report([sample(0, cpu_busy=60.0, top=[["lean", 2.0]])]), "Host CPU (samples)", 2)
        self.assertEqual([name for name, _ in no_go], ["lean"])

    def test_job_cores_joined_by_id_and_kept_when_in_three_rows(self):
        jobs = [job("cq-J1", label="integ verify", slots=2, slots_eff=2), job("cq-J2", label="lane")]
        cores = [{"cq-J1": 2.0, "cq-J2": 1.0, "cq-J3": 0.5}, {"cq-J1": 4.0, "cq-J3": 0.5},
                 {"cq-J1": 3.0, "cq-J3": "x"}, {"cq-J3": 0.5}, {"cq-J3": 0.5}]
        rows = [sample(i, cpu_busy=60.0, job_cores=c) for i, c in enumerate(cores)]
        per_job = table(self.report(rows, jobs), "Host CPU (samples)", 3)
        # cq-J1: 3 rows, mean 3.0, joined to its job file; cq-J2 has 1 row (dropped);
        # cq-J3: 4 numeric rows, no job file
        self.assertEqual(per_job, [["cq-J1", "integ verify", 2, 3, 3.0], ["cq-J3", "-", "-", 4, 0.5]])

    def test_laptop_slope_per_day_on_a_straight_line(self):
        # 13 rows every 4 h over two days; nonpaged pool rises 0.1 GB per day
        rows = [sample(240 * i, laptop_free_ram_gb=8.0, laptop_np_gb=1.0 + 0.1 * i * 4 / 24) for i in range(13)]
        np_row = rows_by(table(self.report(rows), "Laptop RAM (samples)", 1), "measure")[("laptop_np_gb",)]
        self.assertEqual(np_row[1], 13)
        self.assertAlmostEqual(np_row[3], 1.0)
        self.assertAlmostEqual(np_row[4], 1.2)
        self.assertAlmostEqual(np_row[5], 0.1)
        # under 10 rows, or a span under an hour: n/a
        few = [sample(240 * i, laptop_free_ram_gb=8.0, laptop_np_gb=float(i)) for i in range(9)]
        short = [sample(i, laptop_free_ram_gb=8.0, laptop_np_gb=float(i)) for i in range(12)]
        for rows_case in (few, short):
            kernel = rows_by(table(self.report(rows_case), "Laptop RAM (samples)", 1), "measure")
            self.assertEqual(kernel[("laptop_np_gb",)][5], "n/a")

    def test_share_of_time_below_six_gb(self):
        # gap weights 60 s for minutes 0-5, 0 for minute 6: below 6 GB at minutes 1, 2, 5 -> 180 of 360 s
        values = [8.0, 5.0, 5.5, 7.0, 9.0, 4.0, 3.0]
        rows = [sample(i, laptop_free_ram_gb=v) for i, v in enumerate(values)]
        ram = table(self.report(rows), "Laptop RAM (samples)", 0)[0]
        self.assertEqual(ram[:2], ["laptop_free_ram_gb", 7])
        self.assertAlmostEqual(ram[2], 5.5)
        self.assertAlmostEqual(ram[3], 3.6)
        self.assertAlmostEqual(ram[4], 3.0)
        self.assertAlmostEqual(ram[5], 0.5)

    def test_process_groups_and_pool_tags(self):
        rows = [sample(i, laptop_free_ram_gb=7.0, laptop_unreadable=i % 2,
                       laptop_top=[["Code", 3 + i, 900.0 + i, 300.0], ["Chrome", 2, 100.0, 50.0 * i]],
                       laptop_pool=[["NonPagedPool", 500.0 + i, 10000 + i], ["Filter", 20.0, 3 - i]])
                for i in range(4)]
        report = self.report(rows)
        procs = table(report, "Laptop RAM (samples)", 2)
        self.assertEqual([r[0] for r in procs], ["Code", "Chrome"])
        self.assertEqual(procs[0][1:], [4, 901.5, 300.0, 4.5])
        pool = table(report, "Laptop RAM (samples)", 3)
        self.assertEqual(pool[0][:4], ["NonPagedPool", 4, 500.0, 503.0])
        self.assertEqual(pool[0][4:6], [10000, 10003])
        # four rows 60 s apart: the span is under an hour, so the slope is n/a
        self.assertEqual(pool[0][6], "n/a")

    def test_both_sections_skipped_without_new_keys(self):
        report = self.report([sample(i, load1=1.0) for i in range(5)])
        self.assertEqual(find(report, "Host CPU (samples)")["skipped"], "no cpu_busy values in samples.jsonl")
        self.assertEqual(find(report, "Laptop RAM (samples)")["skipped"],
                         "no laptop_free_ram_gb values in samples.jsonl")
        self.assertEqual(find(report, "Shared host (benchmark)")["skipped"], "no bench_real_ms values in samples.jsonl")

    def test_shared_host_bands_by_cpu_idle_and_the_wait_share_of_the_benchmark(self):
        rows = [sample(0, cpu_idle=70.0, bench_real_ms=500, bench_cpu_ms=500),  # band 50+, wait 0
                sample(1, cpu_idle=60.0, bench_real_ms=600, bench_cpu_ms=500),  # band 50+, wait 1/6
                sample(2, cpu_idle=30.0, bench_real_ms=800, bench_cpu_ms=400),  # band 20-50, wait 1/2
                sample(3, cpu_idle=5.0, bench_real_ms=1000, bench_cpu_ms=500),  # band under 20, wait 1/2
                sample(4, bench_real_ms=900, bench_cpu_ms=450),  # no cpu_idle: in all only
                sample(5, cpu_idle=10.0, bench_real_ms=None, bench_cpu_ms=3)]  # no real time: not a benchmark row
        report = self.report(rows)
        by = rows_by(table(report, "Shared host (benchmark)", 0), "cpu_idle")
        self.assertEqual(by[("cpu_idle >= 50",)][1], 2)
        self.assertAlmostEqual(by[("cpu_idle >= 50",)][2], 550.0)
        self.assertAlmostEqual(by[("cpu_idle >= 50",)][3], 590.0)
        self.assertAlmostEqual(by[("cpu_idle >= 50",)][4], 500.0)
        self.assertAlmostEqual(by[("cpu_idle >= 50",)][5], 1 / 12)
        self.assertAlmostEqual(by[("cpu_idle >= 50",)][6], 0.15)
        self.assertEqual(by[("cpu_idle 20-50",)][1:], [1, 800.0, 800.0, 400.0, 0.5, 0.5])
        self.assertEqual(by[("cpu_idle < 20",)][1:], [1, 1000.0, 1000.0, 500.0, 0.5, 0.5])
        all_row = by[("all",)]
        self.assertEqual(all_row[1], 5)  # the row with no cpu_idle counts here
        self.assertAlmostEqual(all_row[2], 800.0)
        self.assertAlmostEqual(all_row[3], 960.0)
        self.assertAlmostEqual(all_row[4], 500.0)
        self.assertAlmostEqual(all_row[5], 0.5)
        self.assertIn("runnable but not running", " ".join(find(report, "Shared host (benchmark)")["footnotes"]))

    def test_shared_host_band_without_rows_is_empty_not_missing(self):
        rows = [sample(0, cpu_idle=70.0, bench_real_ms=500, bench_cpu_ms=500)]
        by = rows_by(table(self.report(rows), "Shared host (benchmark)", 0), "cpu_idle")
        self.assertEqual(by[("cpu_idle < 20",)][1:], [0, None, None, None, None, None])

    def test_none_and_string_values_are_ignored(self):
        rows = [
            sample(0, cpu_busy=10.0, cpu_idle=None, top="lane", job_cores=[1, 2], laptop_free_ram_gb="7",
                   laptop_top="x", laptop_pool=[["a", None, 1]], laptop_unreadable="2", laptop_np_gb=None),
            sample(1, cpu_busy="50", cpu_idle=20.0, top=[["lane", "x"], "bad", ["lane"]],
                   job_cores={"cq-J1": None}, laptop_free_ram_gb=None, laptop_top=[["p", "3", 1, 2]]),
            sample(2, cpu_busy=30.0, laptop_free_ram_gb=5.0, laptop_top=[["p", 2, 1.0, 4.0], [5, 1, 1, 1]],
                   laptop_pool=[["a", 100, 2], ["b", 5]], laptop_np_gb=True, head_state="caps"),
        ]
        report = self.report(rows)
        json.loads(json.dumps(report))
        measures = {r[0]: r for r in table(report, "Host CPU (samples)", 0)}
        self.assertEqual(measures["cpu_busy"][1:3], [2, 20.0])
        self.assertEqual(table(report, "Host CPU (samples)", 1)[2], ["caps time: weighted mean cpu_idle", None])
        ram = table(report, "Laptop RAM (samples)", 0)[0]
        self.assertEqual(ram[1], 1)
        self.assertEqual(table(report, "Laptop RAM (samples)", 2), [["p", 1, 1.0, 4.0, 2.0]])
        self.assertEqual(table(report, "Laptop RAM (samples)", 3)[0][:2], ["a", 1])

    def test_full_rows_render_and_round_trip(self):
        rows = [sample(i, cpu_busy=50.0 + i, cpu_sys=5.0, cpu_iowait=1.0, cpu_idle=50.0 - i, runq=i % 3,
                       psi_cpu10=2.0, head_state="caps" if i % 2 else None, top=[["lane", 2.0]],
                       job_cores={"cq-J1": 2.0}, laptop_free_ram_gb=7.0 - 0.1 * i,
                       laptop_commit_gb=20.0, laptop_commit_limit_gb=40.0, laptop_np_gb=1.0 + 0.01 * i,
                       laptop_paged_gb=0.5, laptop_top=[["Code", 3, 900.0, 300.0]], laptop_unreadable=0,
                       laptop_pool=[["NonPagedPool", 500.0, 10000]])
                for i in range(12)]
        report = self.report(rows)
        json.loads(json.dumps(report))
        self.assertNotIn("skipped", find(report, "Host CPU (samples)"))
        self.assertNotIn("skipped", find(report, "Laptop RAM (samples)"))
        text = cq_stats.render_markdown(report)
        self.assertIn("## 11. Host CPU (samples)", text)
        self.assertIn("## 12. Shared host (benchmark)", text)
        self.assertIn("## 13. Laptop RAM (samples)", text)
        self.assertIn("## 16. Footer", text)


class LaunchAndPlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqs")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_launch_holds_slots_without_stranding(self):
        # Two 1-slot jobs launch at B and start 90 s later; nothing else is queued.
        state = os.path.join(self.tmp, "launch")
        jobs = [job("cq-U1", t_submitted=B, t_launch=B, t_started=B + 90, t_finished=at(10)),
                job("cq-U2", t_submitted=B, t_launch=B, t_started=B + 90, t_finished=at(10))]
        write_state(state, jobs)
        report = cq_stats.build_report(state, None, 8, NOW, "cmd")
        rows = rows_by(table(report, "Concurrency"), "measure")
        # Grid points at B, B+1 min, ..., B+9 min: 10 points, both jobs held, 6 slots free each.
        self.assertEqual(rows[("grid points (1 min)",)][1], 10)
        self.assertEqual(rows[("held == 8 slots",)][1], 0.0)
        self.assertEqual(rows[("a queued job waiting",)][1], 0.0)
        self.assertEqual(rows[("fitting job waiting (stranded) minutes",)][1], 0)
        self.assertEqual(rows[("idle slot-minutes while a fitting job waited",)][1], 0.0)
        self.assertEqual(rows[("idle slot-minutes",)][1], 60.0)
        self.assertEqual(rows[("max held slots",)][1], 2)
        section = find(report, "Concurrency")
        self.assertEqual(section["footnotes"], [
            "Hold and wait intervals use t_launch (slots are taken while launching); "
            "fitting job waiting (stranded) includes minutes the reservation rule or the caps "
            "held a fitting job back on purpose."])
        text = cq_stats.render_markdown(report)
        self.assertLess(text.index("| grid points (1 min) |"), text.index(section["footnotes"][0]))
        # Section 2 still measures t_started - t_submitted: 90 s for both jobs.
        wait = rows_by(table(report, "Queue wait"), "metric", "group")[("wait", "all")]
        self.assertEqual(wait[2:4], [2, 1.5])

    def test_hold_window_starts_at_launch(self):
        def window(**kw):
            return cq_stats.hold_window(cq_stats.normalise_job(job("cq-H", **kw)), at(7))
        self.assertEqual(window(status="launching", t_launch=at(0)), (at(0), at(7)))
        self.assertEqual(window(status="cancelled", t_launch=at(0), t_finished=at(5)),
                         (at(0), at(5)))
        self.assertIsNone(window(status="cancelled", t_launch=at(0)))
        self.assertEqual(window(status="running", t_launch=at(0), t_started=at(2)),
                         (at(0), at(7)))
        self.assertEqual(window(status="done", t_started=at(2), t_finished=at(5)),
                         (at(2), at(5)))

    def test_caps_override_is_not_a_plan(self):
        state = os.path.join(self.tmp, "caps")
        decisions = [
            {"kind": "caps_override", "note": "idle host", "job": "cq-Q", "slots": 1,
             "load": 2.0, "mem_avail_gb": 20, "idle_samples": 3, "t": iso(at(0))},
            {"kind": "plan", "mode": "haiku", "t": iso(at(1)), "free_host": 1, "queued": 1,
             "note": "", "why": "", "accepted": [],
             "refused": [{"job": "cq-R", "reason": "needs 2 slots, 0 free"}]},
        ]
        write_state(state, [job("cq-Q", t_submitted=at(0), t_started=at(0), t_finished=at(5),
                                minutes=5.0)], decisions=decisions)
        report = cq_stats.build_report(state, None, 8, NOW, "cmd")
        refusals = find(report, "Refusals")
        self.assertIn("Plan rows: 1; accepted entries: 0; refused entries: 1.", refusals["notes"][0])
        self.assertEqual(refusals["notes"][-1],
                         "caps_override records (not plans, not in the counts above): 1.")
        rows = rows_by(table(report, "Refusals"), "bucket")
        self.assertEqual(rows[("slots",)][1], 1)
        self.assertEqual(sum(r[1] for r in table(report, "Refusals")), 1)


class EmptyAndFilterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqs")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_empty_state_skips_cleanly(self):
        state = os.path.join(self.tmp, "empty")
        os.makedirs(state)
        report = cq_stats.build_report(state, None, 8, NOW, "cmd")
        for sec in report["sections"][:-1]:
            self.assertIn("skipped", sec, sec["title"])
        self.assertNotIn("skipped", report["sections"][-1])
        text = cq_stats.render_markdown(report)
        self.assertIn("Skipped: no job files.", text)
        self.assertIn("## 16. Footer", text)
        json.loads(json.dumps(report))

    def test_missing_state_dir_is_empty(self):
        report = cq_stats.build_report(os.path.join(self.tmp, "nope"), None, 8, NOW, "cmd")
        self.assertIn("skipped", report["sections"][0])

    def test_since_filter_drops_earlier_rows(self):
        state = os.path.join(self.tmp, "state")
        write_state(state, FIXTURE_JOBS, FIXTURE_HISTORY, FIXTURE_DECISIONS)
        since = cq_stats.parse_since("2023-12-10")
        report = cq_stats.build_report(state, since, 8, NOW, "cmd")
        # Y is submitted at minute 140 (00:20 UTC on the 10th), so only nine jobs predate it.
        self.assertEqual(report["reading"]["jobs"]["dropped_since"], 9)
        self.assertEqual(report["reading"]["history.jsonl"]["dropped_since"], 4)
        self.assertEqual(report["reading"]["decisions.jsonl"]["dropped_since"], 2)
        self.assertIn("1 job files", find(report, "Jobs")["notes"][0])
        self.assertNotIn("skipped", find(report, "Queue wait"))

    def test_running_job_clipped_at_report_time(self):
        state = os.path.join(self.tmp, "running")
        running = job("cq-R", status="running", t_submitted=at(0), t_started=at(0))
        write_state(state, [running])
        report = cq_stats.build_report(state, None, 8, at(30), "cmd")
        rows = table(report, "Utilisation per UTC day")
        self.assertEqual(rows[-1][2], 30.0)
        self.assertEqual(rows[-1][4], 30.0)
        self.assertEqual(rows[-1][5], 8 * 30.0)
        self.assertEqual(find(report, "Utilisation per UTC day")["notes"][1],
                         "Running jobs clipped at report time: 1.")

    def test_cpu_skipped_without_fields(self):
        state = os.path.join(self.tmp, "plain")
        write_state(state, [job("cq-P1", t_submitted=at(0), t_started=at(0), t_finished=at(5),
                                minutes=5.0)])
        report = cq_stats.build_report(state, None, 8, NOW, "cmd")
        self.assertEqual(find(report, "CPU use per slot")["skipped"],
                         "no cores or cpu_s fields in job files")


def utc(year, month, day, hour=0, minute=0):
    return dt.datetime(year, month, day, hour, minute, tzinfo=dt.timezone.utc).timestamp()


def write_store(base, rel, jobs):
    """Write job files under base/rel (a path of components)."""
    folder = os.path.join(base, *rel.split("/"))
    os.makedirs(folder, exist_ok=True)
    for j in jobs:
        with open(os.path.join(folder, j["id"] + ".json"), "w", encoding="utf-8") as fh:
            json.dump(j, fh)


def write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)


def fleet_job(job_id, host_id="ember", **kw):
    kw.setdefault("t_submitted", at(0))
    kw.setdefault("t_launch", at(1))
    kw.setdefault("t_finished", at(11))
    return job(job_id, where="fleet", fleet=True, host_id=host_id, defer="immediate",
               slots_eff=None, **kw)


def have_tz():
    return cq_stats.local_tz()[1] == "Europe/London"


class StoreReadingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqs")
        self.state = os.path.join(self.tmp, "state")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def report(self, state=None, **kw):
        return cq_stats.build_report(state or self.state, kw.get("since"), 8, NOW, "cmd")

    def jobs_rows(self, report, field):
        return {r[1]: r[2] for r in table(report, "Jobs") if r[0] == field}

    def test_legacy_only_state_gets_only_default_fields_and_empty_new_sections(self):
        write_state(self.state, FIXTURE_JOBS, FIXTURE_HISTORY, FIXTURE_DECISIONS)
        report = self.report()
        self.assertEqual(self.jobs_rows(report, "store"), {"jobs": 10})
        self.assertEqual(self.jobs_rows(report, "host"), {"ember": 9, "unplaced": 1})
        self.assertEqual(self.jobs_rows(report, "defer"), {"immediate": 10})
        self.assertEqual(list(report["reading"]), ["jobs", "history.jsonl", "decisions.jsonl"])
        self.assertEqual(find(report, "Deferred work")["skipped"], "no deferred jobs")
        self.assertEqual(table(report, "Utilisation per UTC day")[-1][4], 720.0)
        self.assertEqual(table(report, "Utilisation per UTC day")[-1][5], 1200.0)

    def test_fleet_v2_jobs_are_found_from_the_parent_and_from_the_v2_directory(self):
        write_store(self.state, "queue-v2/fleet/jobs", [fleet_job("cq-v2--fleet-1"), fleet_job("cq-v2--fleet-2")])
        write_store(self.state, "fleet/jobs", [fleet_job("cq-fleet-1")])
        report = self.report()
        self.assertEqual(self.jobs_rows(report, "store"), {"v2/fleet": 2, "fleet": 1})
        self.assertEqual(self.jobs_rows(report, "host"), {"ember": 3})
        self.assertEqual(table(report, "Utilisation per UTC day")[-1][4], 30.0)  # 3 jobs x 10 min x 1 slot
        notes = " ".join(find(report, "Footer")["notes"])
        self.assertIn("v2/fleet: 2 rows read", notes)
        self.assertIn("fleet: 1 rows read", notes)
        report = self.report(os.path.join(self.state, "queue-v2"))
        self.assertEqual(self.jobs_rows(report, "store"), {"v2/fleet": 2})
        self.assertNotIn("skipped", find(report, "Jobs"))

    def test_v2_directory_does_not_double_count_its_history(self):
        row = {"kind": "go vet", "est": 6, "actual": 1.0, "slots": 1, "t": "2023-12-09T23:00:00Z"}
        later = dict(row, t="2023-12-09T22:30:00Z")
        write_state(self.state, [], [row])
        os.makedirs(os.path.join(self.state, "queue-v2"))
        write_jsonl(os.path.join(self.state, "queue-v2", "history.jsonl"), [later])
        report = self.report()
        self.assertEqual(report["reading"]["history.jsonl"]["read"], 2)
        self.assertEqual(table(report, "Estimate accuracy")[0][1], 2)
        report = self.report(os.path.join(self.state, "queue-v2"))
        self.assertEqual(report["reading"]["history.jsonl"]["read"], 1)

    def test_history_of_both_generations_is_in_time_order(self):
        rows = [dict(kind="k", est=1, actual=1.0, t="2023-12-09T23:00:00Z", n=1),
                dict(kind="k", est=1, actual=1.0, t="2023-12-09T23:20:00Z", n=3)]
        write_state(self.state, [], rows)
        os.makedirs(os.path.join(self.state, "queue-v2"))
        write_jsonl(os.path.join(self.state, "queue-v2", "history.jsonl"),
                    [dict(kind="k", est=1, actual=1.0, t="2023-12-09T23:10:00Z", n=2)])
        history = cq_stats.read_in_time_order(
            [os.path.join(self.state, "history.jsonl"), os.path.join(self.state, "queue-v2", "history.jsonl")],
            cq_stats.new_tally(), None, cq_stats.valid_history, lambda row: cq_stats.parse_time(row.get("t")))
        self.assertEqual([h["n"] for h in history], [1, 2, 3])

    def test_deferred_jobs_are_found(self):
        write_store(self.state, "deferred-jobs", [job("cq-D1", defer="spare", deferred_registered=True,
                                                      t_submitted=at(0), t_started=at(3), t_finished=at(9))])
        report = self.report()
        self.assertEqual(self.jobs_rows(report, "store"), {"deferred-jobs": 1})
        self.assertEqual(self.jobs_rows(report, "defer"), {"spare": 1})

    def test_a_repeated_id_counts_once_and_the_first_store_wins(self):
        write_store(self.state, "jobs", [job("cq-same", t_submitted=at(0), t_started=at(0), t_finished=at(5))])
        write_store(self.state, "queue-v2/fleet/jobs", [fleet_job("cq-same"), fleet_job("cq-other")])
        report = self.report()
        self.assertEqual(self.jobs_rows(report, "store"), {"jobs": 1, "v2/fleet": 1})
        self.assertEqual(report["reading"]["v2/fleet"]["duplicates"], 1)
        self.assertEqual(report["reading"]["v2/fleet"]["read"], 2)
        self.assertIn("1 repeated ids dropped", " ".join(find(report, "Footer")["notes"]))

    def write_shard(self, jobs, host_id=None):
        write_json(os.path.join(self.state, "hosts.json"), {
            "aliases": {"hestia": "hearth"},
            "hosts": {"hearth": {"enabled": True, "state": "hosts/hearth"}}})
        write_json(os.path.join(self.state, "hosts", "hearth", "host.conf.json"), {"slots": 4, "name": "hestia"})
        write_store(os.path.join(self.state, "hosts", "hearth"), "jobs", jobs)

    def test_shard_jobs_have_their_own_host_and_stay_out_of_the_primary_pool(self):
        write_store(self.state, "jobs", [job("cq-p", t_submitted=at(0), t_started=at(0), t_finished=at(10))])
        self.write_shard([job("cq-s1", t_submitted=at(0), t_started=at(0), t_finished=at(100)),
                          job("cq-s2", host_id="hestia", t_submitted=at(0), t_started=at(0), t_finished=at(100))])
        write_store(self.state, "fleet/jobs", [fleet_job("cq-f", host_id="hestia")])
        report = self.report()
        self.assertEqual(self.jobs_rows(report, "host"), {"ember": 1, "hearth": 3})
        self.assertEqual(self.jobs_rows(report, "store"), {"jobs": 1, "hearth:jobs": 2, "fleet": 1})
        daily = table(report, "Utilisation per UTC day")
        self.assertEqual(daily[-1][4], 10.0)  # only the primary host's 10 slot-minutes
        self.assertIn("(ember)", " ".join(find(report, "Utilisation per UTC day")["notes"]))
        self.assertEqual(report["span"], [cq_stats.iso(at(0)), cq_stats.iso(at(10))])
        self.assertEqual(table(report, "Failed-run waste")[0][1], 0)
        hosts = {r[0] for r in table(report, "Night window use")}
        self.assertEqual(hosts, {"ember", "hearth"})

    def test_primary_name_comes_from_host_conf(self):
        write_json(os.path.join(self.state, "host.conf.json"), {"name": "box", "slots": 6})
        write_store(self.state, "jobs", [job("cq-a", t_submitted=at(0), t_started=at(0), t_finished=at(10)),
                                         job("cq-b", host_id="ember", t_submitted=at(0), t_started=at(0),
                                             t_finished=at(10))])
        report = self.report()
        self.assertEqual(self.jobs_rows(report, "host"), {"box": 1, "ember": 1})
        self.assertEqual(table(report, "Utilisation per UTC day")[-1][4], 10.0)

    def test_unsafe_shard_paths_are_ignored(self):
        outside = os.path.join(self.tmp, "outside")
        write_store(outside, "jobs", [job("cq-out", t_submitted=at(0), t_started=at(0), t_finished=at(5))])
        write_store(self.state, "jobs", [job("cq-p", t_submitted=at(0), t_started=at(0), t_finished=at(5))])
        for rel in (outside, "../outside", "hosts/../../outside", "..\\outside", "missing/dir", ""):
            write_json(os.path.join(self.state, "hosts.json"),
                       {"hosts": {"evil": {"enabled": True, "state": rel}}})
            report = self.report()
            self.assertEqual(self.jobs_rows(report, "host"), {"ember": 1}, rel)
            self.assertEqual(list(report["reading"])[:1], ["jobs"], rel)
        for text in ("not json", "[]", '{"hosts": []}', '{"hosts": {"a": 3}}'):
            with open(os.path.join(self.state, "hosts.json"), "w", encoding="utf-8") as fh:
                fh.write(text)
            self.assertEqual(self.jobs_rows(self.report(), "host"), {"ember": 1}, text)

    def test_a_disabled_shard_is_ignored(self):
        self.write_shard([job("cq-s1", t_submitted=at(0), t_started=at(0), t_finished=at(5))])
        write_json(os.path.join(self.state, "hosts.json"),
                   {"hosts": {"hearth": {"enabled": False, "state": "hosts/hearth"}}})
        self.assertIn("skipped", find(self.report(), "Jobs"))

    def test_reading_the_layout_never_writes(self):
        write_store(self.state, "queue-v2/fleet/jobs", [fleet_job("cq-v2--fleet-1")])
        self.write_shard([job("cq-s1", t_submitted=at(0), t_started=at(0), t_finished=at(5))])
        before = snapshot(self.state)
        self.report()
        self.report(os.path.join(self.state, "queue-v2"))
        self.assertEqual(snapshot(self.state), before)


class NightWindowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqs")
        self.state = os.path.join(self.tmp, "state")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def night(self, jobs, now, slots=4):
        write_state(self.state, jobs)
        report = cq_stats.build_report(self.state, None, slots, now, "cmd")
        return report, {r[1]: r for r in table(report, "Night window use")}

    @unittest.skipUnless(have_tz(), "tz database unavailable")
    def test_clipping_at_the_window_edges_for_a_running_job_and_a_day_job(self):
        # December: London is UTC. Night of 12-05 is 00:00-08:00Z.
        jobs = [
            job("cq-a", t_launch=utc(2026, 12, 4, 23, 30), t_started=utc(2026, 12, 4, 23, 31),
                t_finished=utc(2026, 12, 5, 0, 45)),                    # 45 min in the window, 1 slot
            job("cq-b", slots=2, slots_eff=2, defer="spare", t_started=utc(2026, 12, 5, 7, 50),
                t_finished=utc(2026, 12, 5, 8, 20)),                    # 10 min in the window, 2 slots
            job("cq-c", t_started=utc(2026, 12, 5, 12), t_finished=utc(2026, 12, 5, 13)),  # day only
            job("cq-d", status="running", defer="overnight", t_started=utc(2026, 12, 6, 7, 30)),
        ]
        now = utc(2026, 12, 6, 7, 45)
        report, rows = self.night(jobs, now)
        full = rows["2026-12-05"]
        self.assertEqual(full[0], "ember")
        self.assertEqual(full[2], "full")
        self.assertEqual(full[3], 480.0)
        self.assertAlmostEqual(full[4], 45 + 2 * 10)
        self.assertEqual(full[5], 4 * 480.0)
        self.assertAlmostEqual(full[6], 65 / 1920)
        self.assertEqual(full[7:], [1, 1])  # cq-b deferred, cq-a immediate; cq-c is outside the window
        # cq-d is still running at the report time: clipped there, in a night the span only partly covers.
        part = rows["2026-12-06"]
        self.assertEqual(part[2], "partial (ends inside the window)")
        self.assertEqual(part[3], 465.0)
        self.assertAlmostEqual(part[4], 15.0)
        self.assertEqual(part[5], 4 * 465.0)
        self.assertEqual(part[7:], [1, 0])
        self.assertNotIn("2026-12-04", rows)
        allrow = rows["all nights"]
        self.assertEqual(allrow[2], "full nights only: 1")
        self.assertEqual(allrow[3:6], [480.0, 65.0, 1920.0])
        self.assertEqual(allrow[7:], [1, 1])
        notes = find(report, "Night window use")["notes"]
        self.assertIn("Average idle slot-hours per night, ember: 30.92 over 1 full night; "
                      "partial nights excluded: 1.", notes)

    @unittest.skipUnless(have_tz(), "tz database unavailable")
    def test_the_window_is_local_wall_clock_across_the_clock_changes(self):
        # 2026-10-25 has 25 hours (BST ends 01:00Z): 00:00 BST = 23:00Z, 08:00 GMT = 08:00Z.
        # 2027-03-28 has 23 hours (BST starts 01:00Z): 00:00 GMT = 00:00Z, 08:00 BST = 07:00Z.
        cases = [("2026-10-25", utc(2026, 10, 24, 22), utc(2026, 10, 25, 12), 540.0),
                 ("2027-03-28", utc(2027, 3, 27, 22), utc(2027, 3, 28, 12), 420.0),
                 ("2026-10-24", utc(2026, 10, 23, 21), utc(2026, 10, 24, 12), 480.0)]
        for night, start, end, minutes in cases:
            shutil.rmtree(self.state, ignore_errors=True)
            _, rows = self.night([job("cq-w", t_started=start, t_finished=end)], end + 3600)
            row = rows[night]
            self.assertEqual(row[2], "full", night)
            self.assertEqual(row[3], minutes, night)
            self.assertEqual(row[4], minutes, night)  # 1 slot held through the whole window
            self.assertEqual(row[5], 4 * minutes, night)
            self.assertEqual(row[6], 0.25, night)

    def test_unavailable_tz_database_falls_back_to_utc(self):
        original = cq_stats.local_tz
        cq_stats.local_tz = lambda: (dt.timezone.utc, "UTC")
        try:
            jobs = [job("cq-a", t_started=utc(2026, 10, 24, 22), t_finished=utc(2026, 10, 25, 12))]
            report, rows = self.night(jobs, utc(2026, 10, 25, 13))
        finally:
            cq_stats.local_tz = original
        self.assertEqual(rows["2026-10-25"][3], 480.0)
        self.assertIn("tz database is unavailable", find(report, "Night window use")["notes"][0])

    @unittest.skipUnless(have_tz(), "tz database unavailable")
    def test_local_date_follows_the_zone_in_summer(self):
        # July: London is UTC+1, so the night of 07-02 is 23:00Z (07-01) to 07:00Z.
        jobs = [job("cq-a", t_started=utc(2026, 7, 1, 20), t_finished=utc(2026, 7, 2, 12))]
        _, rows = self.night(jobs, utc(2026, 7, 2, 13))
        self.assertEqual(rows["2026-07-02"][2:5], ["full", 480.0, 480.0])

    @unittest.skipUnless(have_tz(), "tz database unavailable")
    def test_hosts_have_their_own_rows_and_slots_and_the_laptop_is_skipped(self):
        write_state(self.state, [
            job("cq-a", t_started=utc(2026, 12, 4, 22), t_finished=utc(2026, 12, 5, 12)),
            job("cq-l", placed_where="laptop", t_started=utc(2026, 12, 4, 22), t_finished=utc(2026, 12, 5, 12)),
            fleet_job("cq-f", host_id="hearth", t_launch=utc(2026, 12, 4, 22), t_started=None,
                      t_finished=utc(2026, 12, 5, 12))])
        write_json(os.path.join(self.state, "hosts.json"),
                   {"hosts": {"hearth": {"enabled": True, "state": "hosts/hearth"}}})
        write_json(os.path.join(self.state, "hosts", "hearth", "host.conf.json"), {"slots": 2})
        report = cq_stats.build_report(self.state, None, 4, utc(2026, 12, 5, 13), "cmd")
        rows = {(r[0], r[1]): r for r in table(report, "Night window use")}
        self.assertEqual(set(h for h, _ in rows), {"ember", "hearth"})
        self.assertEqual(rows[("ember", "2026-12-05")][5], 4 * 480.0)
        self.assertEqual(rows[("hearth", "2026-12-05")][5], 2 * 480.0)
        self.assertEqual(rows[("hearth", "2026-12-05")][4], 480.0)

    def test_skipped_without_host_runs(self):
        write_state(self.state, [job("cq-q", status="queued", placed_where=None, t_submitted=at(0))])
        report = cq_stats.build_report(self.state, None, 8, NOW, "cmd")
        self.assertEqual(find(report, "Night window use")["skipped"], "no host runs")


class DeferredWorkTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqs")
        self.state = os.path.join(self.tmp, "state")
        write_state(self.state, [
            job("cq-i", t_submitted=at(0), t_started=at(1), t_finished=at(5)),
            job("cq-s1", defer="spare", deferred_registered=True, t_submitted=at(0), t_started=at(10),
                t_finished=at(15)),
            job("cq-s2", defer="spare", status="failed", t_submitted=at(0), t_started=at(30),
                t_finished=at(31)),
            job("cq-o", defer="overnight", deferred_registered=True, status="cancelled",
                t_submitted=at(0), t_finished=at(40)),
            job("cq-x", host_id="hearth", defer="spare", deferred_registered=True, t_submitted=at(0),
                t_launch=at(20), t_finished=at(25)),
        ])
        self.report = cq_stats.build_report(self.state, None, 8, NOW, "cmd")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_counts_by_mode_status_and_host(self):
        rows = {tuple(r[:3]): r[3] for r in table(self.report, "Deferred work")}
        self.assertEqual(rows, {("immediate", "done", "ember"): 1, ("spare", "done", "ember"): 1,
                                ("spare", "failed", "ember"): 1, ("overnight", "cancelled", "ember"): 1,
                                ("spare", "done", "hearth"): 1})

    def test_median_wait_and_registered_count(self):
        waits = {r[0]: r for r in table(self.report, "Deferred work", 1)}
        # waits (min): spare 10, 30, 20 (cq-x from t_launch); overnight has no start
        self.assertEqual(waits["all deferred"][1:3], [3, 20.0])
        self.assertEqual(waits["spare"][1:3], [3, 20.0])
        self.assertNotIn("overnight", waits)
        measures = {r[0]: r[1] for r in table(self.report, "Deferred work", 2)}
        self.assertEqual(measures, {"deferred jobs": 4, "deferred_registered true": 3})

    def test_json_and_markdown_carry_the_new_sections(self):
        data = json.loads(json.dumps(self.report))
        titles = [s["title"] for s in data["sections"]]
        self.assertIn("Night window use (00:00-08:00 Europe/London)", titles)
        self.assertIn("Deferred work", titles)
        night = find(data, "Night window use")
        self.assertEqual(night["tables"][0]["headers"][0], "host")
        text = cq_stats.render_markdown(self.report)
        self.assertIn("## 14. Night window use", text)
        self.assertIn("## 15. Deferred work", text)
        self.assertIn("| spare | failed | ember | 1 |", text)


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqs")
        self.state = os.path.join(self.tmp, "state")
        write_state(self.state, FIXTURE_JOBS, FIXTURE_HISTORY, FIXTURE_DECISIONS)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_main(self, argv):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            code = cq_stats.main(argv)
        self.assertEqual(code, 0)
        return buf.getvalue()

    def test_json_output_parses(self):
        out = self.run_main(["--state", self.state, "--json"])
        data = json.loads(out)
        self.assertEqual(data["state"], self.state)
        self.assertEqual(len(data["sections"]), 16)
        self.assertIn("--json", data["sections"][-1]["notes"][-1])

    def test_env_var_selects_state(self):
        old = os.environ.get("CQ_STATE")
        os.environ["CQ_STATE"] = self.state
        try:
            data = json.loads(self.run_main(["--json"]))
        finally:
            if old is None:
                del os.environ["CQ_STATE"]
            else:
                os.environ["CQ_STATE"] = old
        self.assertEqual(data["state"], self.state)

    def test_markdown_default(self):
        out = self.run_main(["--state", self.state, "--slots", "8"])
        self.assertTrue(out.startswith("# Compute queue utilisation and wait report"))
        self.assertIn("## 6. Concurrency", out)

    def test_bad_arguments_exit(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                cq_stats.main(["--slots", "0"])
            with self.assertRaises(SystemExit):
                cq_stats.main(["--since", "2023/12/10"])

    def test_slots_default_comes_from_host_conf_and_an_explicit_slots_wins(self):
        self.assertEqual(json.loads(self.run_main(["--state", self.state, "--json"]))["slots"], 8)
        with open(os.path.join(self.state, "host.conf.json"), "w", encoding="utf-8") as fh:
            json.dump({"slots": 13}, fh)
        self.assertEqual(json.loads(self.run_main(["--state", self.state, "--json"]))["slots"], 13)
        self.assertEqual(json.loads(self.run_main(["--state", self.state, "--json", "--slots", "5"]))["slots"], 5)

    def test_an_invalid_host_conf_slots_falls_back_to_8(self):
        path = os.path.join(self.state, "host.conf.json")
        for text in ('{"slots": 33}', '{"slots": true}', '{"slots": "13"}', '[13]', "not json"):
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)
            self.assertEqual(json.loads(self.run_main(["--state", self.state, "--json"]))["slots"], 8, text)

    def test_a_host_conf_with_a_huge_integer_or_nested_too_deep_falls_back_to_8(self):
        path = os.path.join(self.state, "host.conf.json")
        for text in ('{"slots": 1' + "0" * 400 + "}", "[" * 200000 + "]" * 200000, '{"slots": 13, "x": ' + "[" * 200000 + "]" * 200000 + "}"):
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)
            self.assertEqual(cq_stats.host_slots(self.state), 8, text[:20])

    def test_never_modifies_state_dir(self):
        before = snapshot(self.state)
        self.run_main(["--state", self.state])
        self.run_main(["--state", self.state, "--json", "--since", "2023-12-09", "--slots", "4"])
        self.assertEqual(snapshot(self.state), before)


if __name__ == "__main__":
    unittest.main()
