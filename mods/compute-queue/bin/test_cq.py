"""Tests for cq.py: a temp state dir, a temp git repo, and a simulated host (ssh and remoterun injected).

Run: python -m unittest bin/test_cq.py   (from the mod folder)
The stress scenarios S1-S20 of compute-queue-spec.md are named in the test names.
"""
import atexit
import base64
import importlib.util
import io
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest
import warnings
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# the tests never read the live state directory (a real host.conf.json there would change the host the tests see)
STATE_DIR = tempfile.mkdtemp(prefix="cqtest-state")
atexit.register(shutil.rmtree, STATE_DIR, True)
os.environ["CQ_STATE"] = STATE_DIR
# these tests run the laptop executor on any OS (cq.laptop_executes is off elsewhere than Windows by default)
os.environ.setdefault("CQ_LAPTOP_EXECUTOR", "1")
for _name in ("CQ_HOST", "LANE_HOST", "CQ_LOAD_CAP", "CQ_HOST_RAM_FLOOR_GB"):
    os.environ.pop(_name, None)
import cq  # noqa: E402
import gnu_shims_for_tests  # noqa: E402

gnu_shims_for_tests.install()   # a Mac has no GNU timeout or realpath -e, which the host scripts these tests run locally use
BASH = cq.bash_exe()
HAVE_BASH = os.path.isfile(BASH) or shutil.which("bash") is not None


def setUpModule():
    # the runner resets the filters; the tests read small files inline
    warnings.simplefilter("ignore", ResourceWarning)


def sh(cwd, *args):
    r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True)
    return r.stdout.strip()


class FakeLaptop:
    def __init__(self):
        self.busy = False
        self.ram = 16.0
        self.alive = set()
        self.killed = []
        self.mem = {"commit_gb": 9.5, "commit_limit_gb": 30.0, "np_gb": 0.4, "paged_gb": 0.6, "cache_gb": 1.2}
        self.procs = {"groups": [{"name": "chrome", "n": 20, "ws_mb": 900, "priv_mb": 700}], "ws_gb_total": 0.9,
                      "n_procs": 200, "unreadable": 3}
        self.pool = [{"tag": "Ntfs", "mb": 120.0, "outstanding": 5000}]
        self.failing = set()  # the names of the collectors that raise

    def heavy_lock_held(self):
        return self.busy

    def free_ram_gb(self):
        return self.ram

    def pid_alive(self, pid):
        return pid in self.alive

    def kill_tree(self, pid):
        self.killed.append(pid)
        self.alive.discard(pid)

    def _collect(self, name, value):
        if name in self.failing:
            raise RuntimeError(f"{name} failed")
        return value

    def mem_stats(self):
        return self._collect("mem_stats", self.mem)

    def top_procs(self, n=6):
        return self._collect("top_procs", self.procs)

    def pool_top(self, n=3):
        return self._collect("pool_top", self.pool)


class OldLaptop:
    """A laptop object with only the methods from before the memory telemetry."""

    def heavy_lock_held(self):
        return False

    def free_ram_gb(self):
        return 16.0

    def pid_alive(self, pid):
        return False

    def kill_tree(self, pid):
        pass


class FakeHost:
    """Plays lane-cpx52 for ssh() and remoterun: ~/q/cq-* dirs, heavy slots held by others, disk, reachability."""

    def __init__(self, clock):
        self.clock = clock
        self.up = True
        self.held = 0
        self.disk_kb = 300 * 1024 * 1024
        self.jobs = {}  # id -> ent
        self.tails = {}
        self.files = {}
        self.calls = []
        self.kills = []
        self.removed = []
        self.launches = []
        self.remoterun_rc = None
        self.remoterun_err = ""
        self.remoterun_ok_err = ""  # what a launch that succeeds writes to stderr (remote_stage's fallback notes)
        self.slots = None  # the host's HEAVY_SLOTS line, when set
        self.mem_kb = 20000000
        self.cpu = None  # the CPUSTAT jiffies (10 numbers) the host reports; None: a host with no CPU lines
        self.runq = None  # (running, blocked) for the RUNQ line
        self.psi = None  # resource -> (some10, some60) for the PSI lines
        self.procs = {}  # pid -> (session, ticks in pass A, ticks in pass B, rss pages, comm): the per-process pass
        self.cticks = {}  # pid -> (children's ticks reaped by pass A, by pass B): the cticks column
        self.bench = None  # "real user sys" seconds of the BENCH line, on a profiled probe

    def set_done(self, jid, code, started=None, finished=None):
        e = self.jobs[jid]
        now = int(self.clock[0])
        e.update(exit=code, alive=False, started=started or now - 60, finished=finished or now)

    def stream(self, job, script, timeout):
        class Process:
            def __init__(self, data, code):
                self.stdout, self.stderr, self.code, self.result = io.BytesIO(data), io.BytesIO(), None, code
            def wait(self):
                self.code = self.result
                return self.code
            def poll(self):
                return self.code
            def kill(self):
                self.code = -1
        self.calls.append(script)
        if not self.up:
            return Process(b'', 255)
        program = shlex.split(script)[-1]
        match = re.search(r"jid,wanted,maximum,remaining=json.loads\(base64.b64decode\('([^']+)'\)\)", program)
        jid, wanted, maximum, remaining = json.loads(base64.b64decode(match.group(1)))
        wire = io.BytesIO()
        raw_log = '\n'.join(self.tails.get(jid, [])).encode('utf-8')
        tail = raw_log[-131072:].decode('utf-8', 'replace').splitlines()[-40:]
        wire.write((json.dumps({'tail': [line[:300] for line in tail]}) + '\n').encode())
        for i, rel in wanted:
            data = self.files.get((jid, rel))
            if data is None or len(data) > maximum or len(data) > remaining:
                wire.write((json.dumps({'index': i, 'missing': True}) + '\n').encode())
            else:
                wire.write((json.dumps({'index': i, 'size': len(data)}) + '\n').encode())
                wire.write(data)
                wire.write((json.dumps({'sha256': cq.hashlib.sha256(data).hexdigest(), 'stable': True}) + '\n').encode())
                remaining -= len(data)
        wire.write((json.dumps({'end': len(wanted)}) + '\n').encode())
        return Process(wire.getvalue(), 0)

    def ssh(self, script, timeout=60):
        self.calls.append(script)
        if not self.up:
            return 255, "", "ssh: connect to host 198.51.100.17 port 22: Connection timed out"
        if "echo PROBE" in script:
            lines = ["PROBE"] + ([f"SLOTS {self.slots}"] if self.slots else []) + [f"HELD {i}" for i in range(self.held)]
            lines += ["LOAD 0.5 0.4 0.3", f"MEMAVAIL {self.mem_kb}", f"DISKKB {self.disk_kb}", "NPROC 12"]
            if self.cpu is not None:
                lines += ["TICKHZ 100", "CPUSTAT " + " ".join(map(str, self.cpu))]
            if self.runq is not None:
                lines.append(f"RUNQ {self.runq[0]} {self.runq[1]}")
            for r, (some10, some60) in (self.psi or {}).items():
                lines.append(f"PSI {r} some avg10={some10:.2f} avg60={some60:.2f} avg300=0.00 total=1 full avg10=0.00 avg60=0.00 avg300=0.00 total=0")
            if "sleep 1" in script:
                lines += ["PAGEKB 4", "PROFMS 1000"]
                for pid, (sid, a, b, rss, comm) in self.procs.items():
                    ca, cb = self.cticks.get(pid, (0, 0))
                    lines += [f"PA {pid} {sid} {a} {rss} {ca} {comm}", f"PB {pid} {sid} {b} {rss} {cb} {comm}"]
                if self.bench is not None:
                    lines.append(f"BENCH {self.bench}")
            lines.append("JOBS")
            now = int(self.clock[0])
            for jid, e in self.jobs.items():
                # An executed fake job has the session PID written by run.sh.
                # Explicit pid=None remains available to model a missing receipt.
                pid = e.get('pid', 4242 if e.get('exit') is not None else None)
                lines.append(
                    f"CQJOB {jid} exit={'' if e['exit'] is None else e['exit']} started={e['started'] or ''} "
                    f"launched={e['launched'] or ''} finished={e['finished'] or ''}"
                    + ("" if e.get("cpu") is None else f" cpu={e['cpu']}")
                    + f" pid={pid or ''}"
                    + f" alive={1 if e['alive'] else 0} src={1 if e['src'] else 0} now={now}")
            lines.append("END")
            return 0, "\n".join(lines) + "\n", ""
        m = re.search(r'D="\$HOME/q/(cq-[^"]+)"', script)
        if "==TAIL==" in script and m:
            jid = m.group(1)
            out = ["==TAIL=="] + self.tails.get(jid, [])
            for i, rel in enumerate(re.findall(r'f="\$D/src"/(\S+);', script)):
                data = self.files.get((jid, rel.strip("'")))
                if data is None:
                    out.append(f"==MISSING {i}==")
                else:
                    out += [f"==GET {i}==", base64.b64encode(data).decode()]
            out.append("==END==")
            return 0, "\n".join(out) + "\n", ""
        stop_ids = re.findall(r'# CQ_PIDFD_STOP (cq-[A-Za-z0-9_.-]+)', script)
        if stop_ids:
            self.kills.extend(stop_ids)  # a stop request is not proof of shutdown
            return 0, 'CQ_PIDFD_STOP {"ok":true}\n', ''
        for jid in re.findall(r'rm -rf "\$HOME/q/(cq-[\w.-]+)/src"', script):
            self.removed.append(jid)
            if jid in self.jobs:
                self.jobs[jid]["src"] = False
        return 0, "", ""

    def remoterun(self, argv, cwd):
        script = base64.b64decode(re.search(r"echo (\S+) \|", argv[-1]).group(1)).decode()
        jid = re.search(r"^ID=(\S+)", script, re.M).group(1)
        self.launches.append((jid, argv, cwd, script))
        if not self.up:
            return 255, "", "ssh: connect to host 198.51.100.17 port 22: Connection timed out\n[remoterun] push failed"
        if self.remoterun_rc is not None:
            return self.remoterun_rc, "", self.remoterun_err
        if jid in self.jobs:
            return 17, f"CQ_EXISTS {jid}\n", ""
        now = int(self.clock[0])
        self.jobs[jid] = dict(exit=None, started=None, launched=now, finished=None, alive=True, src=True, pid=4242)
        return 0, f"CQ_LAUNCHED {jid}\n", self.remoterun_ok_err


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="cqt")
        self.addCleanup(cq.rmtree_force, self.tmp)
        self.repo = os.path.join(self.tmp, "wt", "lane_x")
        os.makedirs(self.repo)
        sh(self.repo, "init", "-q")
        sh(self.repo, "config", "user.email", "t@t")
        sh(self.repo, "config", "user.name", "t")
        for n, c in (("a.txt", "one\n"), (".gitignore", "*.log\n")):
            with open(os.path.join(self.repo, n), "w") as f:
                f.write(c)
        sh(self.repo, "add", "-A")
        sh(self.repo, "commit", "-q", "-m", "init")
        with open(os.path.join(self.repo, ".git", "info", "exclude"), "a") as exclude:
            exclude.write("\n.baton/\n")
        cq.write_json(os.path.join(self.repo, ".baton", "owner.json"), {"vendor": "claude", "session_id": "sess-A"})
        self.clock = [1_800_000_000.0]
        self.host = FakeHost(self.clock)
        self.laptop = FakeLaptop()
        self.spawned = []
        # the older tests hand the guard hand-made samples and arbitrary clocks: the sample age is tested in CapTests
        patch = mock.patch.object(cq, "SAMPLE_MAX_AGE", 1e12)
        patch.start()
        self.addCleanup(patch.stop)
        # the older tests are written for the pool of 4; PoolTests / CapTests run the 8-slot pool
        patch = mock.patch.object(cq, "HOST_SLOTS", 4)
        patch.start()
        self.addCleanup(patch.stop)
        self.host.slots = 4
        self.ctx = self.make_ctx()

    def make_ctx(self, sync=True):
        return cq.Ctx(state=os.path.join(self.tmp, "state"), now=lambda: self.clock[0], ssh=self.host.ssh,
                      run_remoterun=self.host.remoterun, spawn=lambda ids: self.spawned.append(ids),
                      laptop=self.laptop, sync=sync, artifact_stream=self.host.stream)

    def submit(self, command="go test ./pkg", **kw):
        req = {"command": command, "root": self.repo, "session": "sess-A", **kw}
        r = cq.cmd_submit(self.ctx, req)
        self.assertTrue(r["ok"], r)
        return r["job"]

    def cancel(self, jid, **kw):
        return cq.cmd_cancel(self.ctx, {"job": jid, "root": self.repo, "session": "sess-A", **kw})

    def job(self, jid):
        return cq.load_job(self.ctx, jid)

    def tick(self, session="sess-A", root=None):
        return cq.cmd_tick(self.ctx, {"session": session, "root": root or self.repo})

    def advance(self, s):
        self.clock[0] += s

    def decisions(self):
        return [json.loads(l) for l in cq.read_lines(self.ctx.p("decisions.jsonl"))]


def cap(free=4, held=0, up=True, usable=True, busy=False, ram=16.0, load="0.5 0.4 0.3", mem=20.0, nproc=12, committed=0, at=0, idle=0):
    return {"at": at, "host": {"up": up, "usable": usable, "total": 4, "held": held, "free": free, "paused": not usable,
                               "load": load, "mem_avail_gb": mem, "nproc": nproc, "mem_total_gb": 22.0, "committed": committed,
                               "idle_samples": idle},
            "laptop": {"heavy_busy": busy, "free_ram_gb": ram, "ram_ok": ram >= 6}}


def J(jid, **kw):
    base = dict(id=jid, status="queued", where="host", slots=1, priority="lane", t_submitted=1000.0, split=None,
                self_heavy=False, workstream="w", label=jid, est_minutes=10, command="x")
    base.update(kw)
    return base


def R(jid, slots, est, elapsed_min, status="running", now=2000.0):
    """A running host job holding `slots`, `elapsed_min` into its `est`-minute estimate."""
    return dict(id=jid, status=status, placed_where="host", slots_eff=slots, est_minutes=est, t_started=now - elapsed_min * 60,
                t_launch=now - elapsed_min * 60)


class GuardTests(Base):
    """The pure guard and the fallback."""

    def test_S2_overcommit_is_trimmed_to_free(self):
        queued = [J("a", slots=2), J("b", slots=2), J("c", slots=2)]
        entries = [{"job": j, "where": "host", "slots": 2} for j in "abc"]
        res = cq.guard(entries, queued, cap(free=2), 2000.0)
        self.assertEqual([a["job"] for a in res["accepted"]], ["a"])
        self.assertEqual(sorted(r["job"] for r in res["refused"]), ["b", "c"])
        self.assertTrue(all("free" in r["reason"] for r in res["refused"]))

    def test_S2_slots_above_free_for_one_job(self):
        res = cq.guard([{"job": "a", "where": "host", "slots": 4}], [J("a", slots=4)], cap(free=3), 2000.0)
        self.assertEqual(res["accepted"], [])
        self.assertIn("free", res["refused"][0]["reason"])

    def test_haiku_slots_above_the_jobs_own_are_clamped_to_the_jobs_own(self):
        res = cq.guard([{"job": "a", "where": "host", "slots": 4}], [J("a")], cap(free=3), 2000.0)
        self.assertEqual(res["accepted"], [{"job": "a", "where": "host", "slots": 1}])

    def test_a_haiku_entry_without_slots_takes_the_jobs_own(self):
        for entry in ({"job": "a", "where": "host"}, {"job": "a", "where": "host", "slots": None}):
            res = cq.guard([entry], [J("a", slots=2)], cap(free=4), 2000.0)
            self.assertEqual(res["accepted"], [{"job": "a", "where": "host", "slots": 2}], entry)

    def test_slots_below_the_jobs_own_are_refused(self):
        res = cq.guard([{"job": "a", "where": "host", "slots": 1}], [J("a", slots=3)], cap(), 2000.0)
        self.assertEqual(res["accepted"], [])

    def test_unknown_duplicate_and_non_queued_ids_dropped(self):
        entries = [{"job": "zz", "where": "host", "slots": 1}, {"job": "a", "where": "host", "slots": 1},
                   {"job": "a", "where": "host", "slots": 1}]
        res = cq.guard(entries, [J("a")], cap(), 2000.0)
        self.assertEqual([a["job"] for a in res["accepted"]], ["a"])
        self.assertEqual({r["reason"] for r in res["refused"]}, {"unknown or not queued", "duplicate"})

    def test_S3_host_only_job_sent_to_laptop_is_refused(self):
        res = cq.guard([{"job": "a", "where": "laptop", "slots": 1}], [J("a", where="host")], cap(), 2000.0)
        self.assertEqual(res["accepted"], [])
        self.assertIn("not allowed", res["refused"][0]["reason"])

    def test_S4_starved_job_goes_first(self):
        old = J("old", priority="background", t_submitted=1000.0)
        v1 = J("v1", priority="verify", t_submitted=1000.0 + 3700)
        v2 = J("v2", priority="verify", t_submitted=1000.0 + 3710)
        now = 1000.0 + 61 * 60
        entries = [{"job": "v1", "where": "host", "slots": 1}, {"job": "v2", "where": "host", "slots": 1}]
        res = cq.guard(entries, [old, v1, v2], cap(free=2), now)
        self.assertEqual([a["job"] for a in res["accepted"]], ["old", "v1"])  # Haiku never named it
        self.assertTrue(res["accepted"][0].get("starved"))

    def test_S4_not_starved_below_60_minutes(self):
        old = J("old", priority="background", t_submitted=1000.0)
        v1 = J("v1", priority="verify", t_submitted=1000.0 + 1000)
        res = cq.guard([{"job": "v1", "where": "host", "slots": 1}], [old, v1], cap(free=2), 1000.0 + 59 * 60)
        self.assertEqual([a["job"] for a in res["accepted"]], ["v1"])  # what Haiku did not name is not started

    def test_S4_starved_job_whose_slots_do_not_fit_waits(self):
        old = J("old", priority="background", t_submitted=1000.0, slots=3)
        v1 = J("v1", priority="verify", t_submitted=1000.0 + 3700)
        res = cq.guard([{"job": "v1", "where": "host", "slots": 1}], [old, v1], cap(free=2), 1000.0 + 61 * 60)
        self.assertEqual([a["job"] for a in res["accepted"]], ["v1"])

    def test_S8_split_parts_bounded_by_max_parts_and_free(self):
        j = J("s", split={"max_parts": 4})
        ok = cq.guard([{"job": "s", "where": "host", "slots": 1, "parts": 4}], [j], cap(free=4), 2000.0)
        self.assertEqual(ok["accepted"][0]["parts"], 4)
        over = cq.guard([{"job": "s", "where": "host", "slots": 1, "parts": 5}], [j], cap(free=4), 2000.0)
        self.assertEqual(over["accepted"][0]["parts"], 4)  # an invalid count is not used: max_parts, within the free slots
        j2 = J("s2", slots=2, split={"max_parts": 4})
        tight = cq.guard([{"job": "s2", "where": "host", "slots": 2, "parts": 4}], [j2], cap(free=4), 2000.0)
        self.assertEqual(tight["accepted"][0]["parts"], 2)  # 4 x 2 slots do not fit; 2 x 2 do
        none = cq.guard([{"job": "s2", "where": "host", "slots": 2, "parts": 1}], [j2], cap(free=1), 2000.0)
        self.assertEqual(none["accepted"], [])
        self.assertIn("free", none["refused"][0]["reason"])

    def test_S8_the_guard_sets_the_parts_whatever_haiku_says(self):
        j = J("s", split={"max_parts": 8})
        for hint in (1, None, 2, 4):
            entry = {"job": "s", "where": "host", "slots": 1, **({} if hint is None else {"parts": hint})}
            res = cq.guard([entry], [j], cap(free=4), 2000.0)
            self.assertEqual(res["accepted"][0]["parts"], 4, hint)  # parts 1 with 4 free -> 4
        res = cq.guard([{"job": "s", "where": "host", "slots": 1, "parts": 1}], [J("s", split={"max_parts": 3})], cap(free=4), 2000.0)
        self.assertEqual(res["accepted"][0]["parts"], 3)  # capped at max_parts

    def test_S8_a_split_job_planned_first_does_not_starve_a_verify_haiku_also_named(self):
        s = J("s", split={"max_parts": 8}, priority="background")
        v = J("v", slots=3, priority="verify", self_heavy=True)
        entries = [{"job": "s", "where": "host", "slots": 1, "parts": 4}, {"job": "v", "where": "host", "slots": 3}]
        res = cq.guard(entries, [s, v], cap(free=4), 2000.0)
        self.assertEqual([(a["job"], a.get("parts")) for a in res["accepted"]], [("s", 1), ("v", None)])  # plan order kept
        self.assertEqual(res["refused"], [])
        # two split jobs: the first planned takes what the others left, the second what the first left
        a, b = J("a", split={"max_parts": 8}), J("b", split={"max_parts": 8})
        res = cq.guard([{"job": "a", "where": "host", "slots": 1}, {"job": "b", "where": "host", "slots": 1}], [a, b], cap(free=4), 2000.0)
        self.assertEqual([(x["job"], x["parts"]) for x in res["accepted"]], [("a", 4)])
        self.assertIn("free", res["refused"][0]["reason"])

    def test_light_split_jobs_get_four_parts_and_light_jobs_are_capped_per_machine(self):
        j = J("l", slots=0, split={"max_parts": 8}, where="any")
        res = cq.guard([{"job": "l", "where": "host", "slots": 0}], [j], cap(free=0), 2000.0)
        self.assertEqual(res["accepted"][0]["parts"], 4)
        res = cq.guard([{"job": "l", "where": "laptop", "slots": 0}], [j], cap(free=0), 2000.0)
        self.assertEqual(res["accepted"][0]["parts"], 4)
        two = J("l2", slots=0, split={"max_parts": 2})
        self.assertEqual(cq.guard([{"job": "l2", "where": "host", "slots": 0}], [two], cap(free=0), 2000.0)["accepted"][0]["parts"], 2)
        busy = cap(free=0)
        busy["host"]["light_running"] = 3
        res = cq.guard([{"job": "l", "where": "host", "slots": 0}], [j], busy, 2000.0)
        self.assertEqual(res["accepted"][0]["parts"], 1)  # three light jobs run: room for one
        busy["host"]["light_running"] = 4
        res = cq.guard([{"job": "l", "where": "host", "slots": 0}], [j], busy, 2000.0)
        self.assertEqual((res["accepted"], len(res["refused"])), ([], 1))
        lights = [J(f"x{i}", slots=0, where="any") for i in range(6)]
        res = cq.guard([{"job": x["id"], "where": "host", "slots": 0} for x in lights], lights, cap(free=0), 2000.0)
        self.assertEqual(len(res["accepted"]), 4)  # the fifth and sixth are over the cap
        self.assertEqual(cq.fallback_entries(lights, cap(free=0))[4]["where"], "laptop")  # the fallback moves on to the laptop
        self.assertEqual(len(cq.fallback_entries(lights, cap(free=0))), 6)
        self.assertEqual(cq.fallback_entries([J("h", slots=0)], busy), [])  # nothing fits: no candidate for Haiku

    def test_fallback_orders_by_priority_then_time_and_splits_to_free(self):
        queued = [J("bg", priority="background", t_submitted=1.0), J("l2", t_submitted=3.0), J("l1", t_submitted=2.0),
                  J("v", priority="verify", t_submitted=9.0), J("sp", t_submitted=4.0, split={"max_parts": 8})]
        res = cq.guard(None, queued, cap(free=4), 10.0, mode="fallback")
        order = [a["job"] for a in res["accepted"]]
        self.assertEqual(order[:3], ["v", "l1", "l2"])
        self.assertEqual(order[3], "sp")
        self.assertEqual(res["accepted"][3]["parts"], 1)  # one slot left
        self.assertNotIn("bg", order)

    def test_fallback_splits_into_all_free_slots(self):
        res = cq.guard(None, [J("sp", split={"max_parts": 8})], cap(free=4), 10.0, mode="fallback")
        self.assertEqual(res["accepted"][0]["parts"], 4)
        res = cq.guard(None, [J("sp", split={"max_parts": 3})], cap(free=4), 10.0, mode="fallback")
        self.assertEqual(res["accepted"][0]["parts"], 3)

    def test_S9_host_down_refuses_host_placement_but_any_goes_to_laptop(self):
        res = cq.guard([{"job": "a", "where": "host", "slots": 1}], [J("a")], cap(up=False, free=0), 10.0)
        self.assertEqual(res["accepted"], [])
        res = cq.guard(None, [J("a"), J("b", where="any")], cap(up=False, free=0), 10.0, mode="fallback")
        self.assertEqual([(a["job"], a["where"]) for a in res["accepted"]], [("b", "laptop")])

    def test_S15_self_heavy_counted_as_its_slots(self):
        j = J("v", slots=3, self_heavy=True, priority="verify")
        res = cq.guard([{"job": "v", "where": "host", "slots": 3}], [j, J("o")], cap(free=4), 10.0)
        self.assertEqual(res["accepted"][0]["slots"], 3)
        res = cq.guard([{"job": "v", "where": "host", "slots": 3}, {"job": "o", "where": "host", "slots": 1}], [j, J("o", slots=2)],
                       cap(free=4), 10.0)
        self.assertEqual([a["job"] for a in res["accepted"]], ["v"])  # 3 + 2 > 4

    def test_S16_laptop_one_heavy_at_a_time_and_a_light_alongside(self):
        queued = [J("h1", where="laptop"), J("h2", where="laptop"), J("light", where="laptop", slots=0)]
        entries = [{"job": j["id"], "where": "laptop", "slots": j["slots"]} for j in queued]
        res = cq.guard(entries, queued, cap(), 10.0)
        self.assertEqual([a["job"] for a in res["accepted"]], ["h1", "light"])
        self.assertIn("laptop", res["refused"][0]["reason"])

    def test_S16_laptop_busy_or_low_ram_refuses_heavy(self):
        e = [{"job": "h", "where": "laptop", "slots": 1}]
        self.assertEqual(cq.guard(e, [J("h", where="laptop")], cap(busy=True), 10.0)["accepted"], [])
        self.assertEqual(cq.guard(e, [J("h", where="laptop")], cap(ram=4.0), 10.0)["accepted"], [])
        light = [{"job": "l", "where": "laptop", "slots": 0}]
        self.assertEqual(len(cq.guard(light, [J("l", where="laptop", slots=0)], cap(busy=True), 10.0)["accepted"]), 1)

    def test_light_job_stays_light_whatever_haiku_says(self):
        res = cq.guard([{"job": "l", "where": "host", "slots": 3}], [J("l", slots=0)], cap(free=1), 10.0)
        self.assertEqual(res["accepted"][0]["slots"], 0)


class ReservationTests(Base):
    """Head-of-line reservation: the oldest job of the highest priority waiting for a machine holds its slots."""

    def plan_for(self, queued, where="host", cap_=None, now=2000.0):
        """A Haiku plan that names every queued job (the worst case), through the guard."""
        entries = [{"job": j["id"], "where": where, "slots": j["slots"]} for j in queued]
        return cq.guard(entries, queued, cap_ or cap(), now)

    def test_a_blocked_verify_holds_the_host_while_lane_jobs_end_then_starts_at_three_free(self):
        v = J("v", priority="verify", slots=3, self_heavy=True, t_submitted=1000.0)
        lanes = [J(f"l{i}", t_submitted=1001.0 + i) for i in range(4)]
        queued = [v] + lanes
        for free in (1, 2):  # 3 of 4 slots held, then one, then two lane jobs end
            res = self.plan_for(queued, cap_=cap(free=free))
            self.assertEqual(res["accepted"], [], free)
            self.assertTrue(all("reserved for v" in r["reason"] for r in res["refused"] if r["job"] != "v"), free)
            fb = cq.guard(None, queued, cap(free=free), 2000.0, mode="fallback")
            self.assertEqual(fb["accepted"], [], free)
        res = self.plan_for(queued, cap_=cap(free=3))
        self.assertEqual([(a["job"], a["slots"]) for a in res["accepted"]], [("v", 3)])
        fb = cq.guard(None, queued, cap(free=3), 2000.0, mode="fallback")
        self.assertEqual([a["job"] for a in fb["accepted"]], ["v"])

    def test_the_clip_is_logged_in_refused_with_the_head_job(self):
        v = J("v", priority="verify", slots=3, t_submitted=1000.0)
        res = self.plan_for([v, J("l")], cap_=cap(free=2))
        self.assertEqual(res["refused"], [{"job": "v", "reason": "needs 3 slots, 2 free"},
                                          {"job": "l", "reason": "reserved for v (verify, 3 slots) waiting on the host"}])

    def test_a_head_job_that_fits_is_added_and_the_others_cannot_take_its_slots(self):
        v = J("v", priority="verify", slots=3, t_submitted=1000.0)
        l1, l2 = J("l1", t_submitted=1001.0), J("l2", t_submitted=1002.0)
        entries = [{"job": "l1", "where": "host", "slots": 1}, {"job": "l2", "where": "host", "slots": 1}]
        res = cq.guard(entries, [v, l1, l2], cap(free=4), 2000.0)  # Haiku left the verify out
        self.assertEqual([a["job"] for a in res["accepted"]], ["l1", "v"])  # one lane job fits beside the 3 slots owed
        self.assertTrue(res["accepted"][1].get("reserved"))
        self.assertEqual([r["job"] for r in res["refused"]], ["l2"])
        # the same with the verify named last
        res = cq.guard(entries + [{"job": "v", "where": "host", "slots": 3}], [v, l1, l2], cap(free=4), 2000.0)
        self.assertEqual([a["job"] for a in res["accepted"]], ["l1", "v"])
        # three free and Haiku names only lane jobs: none of them takes the verify's slots (cmd_apply then runs the
        # fallback, which starts the verify: see the end-to-end test)
        res = cq.guard(entries, [v, l1, l2], cap(free=3), 2000.0)
        self.assertEqual(res["accepted"], [])
        self.assertEqual({r["reason"] for r in res["refused"]}, {"its slots are reserved for the head job that fits now"})

    def test_a_plan_that_starts_nothing_is_not_filled_with_the_head_job(self):
        v = J("v", priority="verify", slots=3, t_submitted=1000.0)
        self.assertEqual(cq.guard([], [v], cap(free=4), 2000.0)["accepted"], [])

    def test_a_split_head_job_keeps_its_first_part_against_split_jobs_planned_before_it(self):
        v = J("v", priority="verify", slots=2, split={"max_parts": 4}, t_submitted=1000.0)
        sp = J("sp", slots=1, split={"max_parts": 4}, t_submitted=1002.0)
        entries = [{"job": "sp", "where": "host", "slots": 1}, {"job": "v", "where": "host", "slots": 2}]
        res = cq.guard(entries, [v, sp], cap(free=2), 2000.0)
        self.assertEqual([(a["job"], a["parts"]) for a in res["accepted"]], [("v", 1)])  # sp gets none of v's 2 slots
        self.assertEqual(res["refused"][0]["job"], "sp")
        res = cq.guard(entries, [v, sp], cap(free=4), 2000.0)
        got = {a["job"]: a["parts"] for a in res["accepted"]}
        self.assertEqual(got, {"sp": 2, "v": 1})  # sp, planned first, leaves v's first part (2 slots); v takes it

    def test_light_jobs_still_start_while_the_verify_waits(self):
        v = J("v", priority="verify", slots=3, t_submitted=1000.0)
        light = J("li", slots=0, priority="background", t_submitted=1005.0)
        lane = J("l")
        res = self.plan_for([v, light, lane], cap_=cap(free=1))
        self.assertEqual([a["job"] for a in res["accepted"]], ["li"])
        fb = cq.guard(None, [v, light, lane], cap(free=1), 2000.0, mode="fallback")
        self.assertEqual([a["job"] for a in fb["accepted"]], ["li"])
        capped = cap(free=1)  # within the light cap: with LIGHT_CAP light jobs already running none more starts
        capped["host"]["light_running"] = cq.LIGHT_CAP
        self.assertEqual(self.plan_for([v, light], cap_=capped)["accepted"], [])

    def test_equal_priority_jobs_queue_fifo_behind_the_head_job(self):
        a = J("a", priority="lane", slots=3, t_submitted=1000.0)
        b = J("b", priority="lane", slots=1, t_submitted=1001.0)
        c = J("c", priority="lane", slots=3, t_submitted=1002.0)
        for free in (1, 2):  # no equal-priority backfill: b fits the free slot but would delay a
            self.assertEqual(self.plan_for([a, b, c], cap_=cap(free=free))["accepted"], [], free)
            self.assertEqual(cq.guard(None, [a, b, c], cap(free=free), 2000.0, mode="fallback")["accepted"], [], free)
        res = self.plan_for([a, b, c], cap_=cap(free=4))
        self.assertEqual([x["job"] for x in res["accepted"]], ["a", "b"])  # a fits: it starts, b takes the slot left
        self.assertEqual([r["job"] for r in res["refused"]], ["c"])
        res = self.plan_for([b, c], cap_=cap(free=2))  # a gone: b is the head and starts; c waits for its slots
        self.assertEqual([x["job"] for x in res["accepted"]], ["b"])

    def test_a_higher_priority_job_arriving_later_becomes_the_head(self):
        old = J("old", priority="lane", slots=2, t_submitted=1000.0)
        v = J("v", priority="verify", slots=3, t_submitted=1900.0)
        res = self.plan_for([v, old], cap_=cap(free=2))
        self.assertEqual(res["accepted"], [])  # old fits in 2 but the verify (3) is the head: the slots accumulate for it

    def test_a_down_host_reserves_nothing(self):
        v = J("v", priority="verify", slots=3, t_submitted=1000.0)
        res = cq.guard([{"job": "l", "where": "laptop", "slots": 1}], [v, J("l", where="any")], cap(up=False, free=0), 2000.0)
        self.assertEqual([a["job"] for a in res["accepted"]], ["l"])

    def test_the_laptop_pool_is_reserved_the_same_way(self):
        v = J("v", priority="verify", where="laptop", slots=1, t_submitted=1000.0)
        l1 = J("l1", where="laptop", t_submitted=1001.0)
        busy = cap(free=0, busy=True)  # the laptop's heavy slot is taken: the verify waits, l1 must not queue-jump
        res = cq.guard([{"job": "l1", "where": "laptop", "slots": 1}], [v, l1], busy, 2000.0)
        self.assertEqual(res["accepted"], [])
        self.assertEqual(res["refused"][0]["reason"], "reserved for v (verify, 1 slots) waiting on the laptop")
        self.assertEqual(cq.guard(None, [v, l1], busy, 2000.0, mode="fallback")["accepted"], [])
        lap = cap(free=0)  # the slot frees: the verify takes it even though Haiku named only the lane job
        res = cq.guard([{"job": "l1", "where": "laptop", "slots": 1}], [v, l1], lap, 2000.0)
        self.assertEqual(res["accepted"], [])
        self.assertEqual(res["refused"][0]["reason"], "the laptop is reserved for the head job that fits now")
        res = cq.guard([{"job": "l1", "where": "laptop", "slots": 1}, {"job": "x", "where": "laptop", "slots": 0}],
                       [v, l1, J("x", where="laptop", slots=0)], lap, 2000.0)
        self.assertEqual([a["job"] for a in res["accepted"]], ["x", "v"])  # the light job and then the added verify
        fb = cq.guard(None, [v, l1], lap, 2000.0, mode="fallback")
        self.assertEqual([a["job"] for a in fb["accepted"]], ["v"])
        # a lane job that may also use the host is not held by the laptop's reservation: it goes to the host
        l2 = J("l2", where="any", t_submitted=1002.0)
        fb = cq.guard(None, [v, l2], cap(free=2, busy=True), 2000.0, mode="fallback")
        self.assertEqual([(a["job"], a["where"]) for a in fb["accepted"]], [("l2", "host")])

    def test_a_head_job_that_fits_elsewhere_does_not_hold_this_machine(self):
        v = J("v", priority="verify", where="any", slots=1, t_submitted=1000.0)  # fits the free laptop
        l = J("l", t_submitted=1001.0)
        res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, l], cap(free=0), 2000.0)
        self.assertEqual(res["accepted"], [])  # the host has no slot at all for l
        res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, l], cap(free=2, busy=True), 2000.0)
        # v is blocked on the busy laptop but fits the host: it does not hold the host, it takes a slot there, l the other
        self.assertEqual([a["job"] for a in res["accepted"]], ["l", "v"])
        self.assertEqual(res["refused"], [])

    def test_end_to_end_through_apply_the_waiting_verify_starts_when_the_third_slot_frees(self):
        v = self.submit("verify", slots=3, self_heavy=True, priority="verify")
        lanes = [self.submit(f"lane {i}") for i in range(3)]
        plan = lambda: {"plan": {"start": [{"job": j, "where": "host", "slots": 1} for j in lanes], "why": "keep busy"}}
        for held in (3, 2):  # lane jobs of other workstreams hold the slots, then end one by one
            self.host.held = held
            self.tick()
            r = cq.cmd_apply(self.ctx, plan())
            self.assertEqual(r["accepted"], [], held)
            self.assertTrue(all(self.job(j)["status"] == "queued" for j in lanes), held)
            refused = [x for x in self.decisions() if x.get("kind") == "plan"][-1]["refused"]
            self.assertTrue(all(f"reserved for {v}" in x["reason"] for x in refused), refused)
            self.advance(30)
        self.host.held = 1
        self.tick()
        r = cq.cmd_apply(self.ctx, plan())
        self.assertEqual([a["job"] for a in r["accepted"]], [v])
        self.assertEqual(self.job(v)["status"], "running")
        self.assertTrue(all(self.job(j)["status"] == "queued" for j in lanes))

    def test_tick_reports_nothing_to_start_while_only_reserved_jobs_wait(self):
        self.submit("verify", slots=3, self_heavy=True, priority="verify")
        self.submit("lane")
        self.host.held = 3
        self.assertFalse(self.tick()["can_start"])
        self.host.held = 0
        self.assertTrue(self.tick()["can_start"])


class StartOrderTests(Base):
    """Start order (start_key): priority, then jobs queued over AGE_FIRST_MIN oldest first, then the shortest estimate."""

    def order(self, queued, now=5000.0):
        return [j["id"] for j in sorted(queued, key=lambda j: cq.start_key(j, now))]

    def test_priority_beats_estimate(self):
        lane = J("lane", est_minutes=1, t_submitted=4900.0)
        verify = J("verify", priority="verify", est_minutes=60, t_submitted=4900.0)
        self.assertEqual(self.order([lane, verify]), ["verify", "lane"])

    def test_shortest_estimate_first_within_a_priority(self):
        a = J("a", est_minutes=30, t_submitted=4900.0)
        b = J("b", est_minutes=5, t_submitted=4901.0)
        c = J("c", est_minutes=5, t_submitted=4902.0)
        self.assertEqual(self.order([a, b, c]), ["b", "c", "a"])

    def test_a_job_older_than_45_min_goes_first_oldest_first(self):
        now = 5000.0
        a = J("a", est_minutes=90, t_submitted=now - 50 * 60)
        b = J("b", est_minutes=80, t_submitted=now - 60 * 60)
        c = J("c", est_minutes=1, t_submitted=4900.0)
        self.assertEqual(self.order([a, b, c]), ["b", "a", "c"])

    def test_exactly_45_min_is_not_aged(self):
        old = J("old", est_minutes=90, t_submitted=5000.0 - 45 * 60)
        short = J("short", est_minutes=5, t_submitted=4900.0)
        self.assertEqual(self.order([old, short]), ["short", "old"])

    def test_missing_or_bad_estimate_counts_as_10(self):
        none = J("none", est_minutes=None, t_submitted=4900.0)
        bad = J("bad", est_minutes="x", t_submitted=4901.0)
        five = J("five", est_minutes=5, t_submitted=4902.0)
        twenty = J("twenty", est_minutes=20, t_submitted=4903.0)
        self.assertEqual(self.order([none, bad, five, twenty]), ["five", "none", "bad", "twenty"])

    def test_reservation_head_is_the_first_in_start_order(self):
        queued = [J("long", slots=1, est_minutes=60, t_submitted=4900.0), J("short3", slots=3, est_minutes=5, t_submitted=4901.0)]
        head = cq.reservations(queued, cap(free=2), now=5000.0)["host"]
        self.assertEqual((head["job"]["id"], head["blocked"]), ("short3", True))
        aged = [J("long", slots=1, est_minutes=60, t_submitted=5000.0 - 50 * 60), queued[1]]
        head = cq.reservations(aged, cap(free=2), now=5000.0)["host"]
        self.assertEqual((head["job"]["id"], head["blocked"]), ("long", False))

    def test_fallback_starts_the_shortest_fitting_job_first(self):
        long_ = J("long", est_minutes=60, t_submitted=4900.0)
        short = J("short", est_minutes=5, t_submitted=4901.0)
        self.assertEqual([e["job"] for e in cq.fallback_entries([long_, short], cap(free=1), None, 5000.0)], ["short"])
        self.assertEqual([e["job"] for e in cq.fallback_entries([long_, short], cap(free=2), None, 5000.0)], ["short", "long"])

    def test_queued_jobs_position_follows_start_order(self):
        a = cq.cmd_submit(self.ctx, {"command": "long", "root": self.repo, "session": "sess-A", "est_minutes": 60})
        self.advance(1)
        b = cq.cmd_submit(self.ctx, {"command": "short", "root": self.repo, "session": "sess-A", "est_minutes": 5})
        self.assertEqual(b["position"], 1)
        self.assertEqual([j["id"] for j in cq.queued_jobs(self.ctx)], [b["job"], a["job"]])

    def test_the_tick_rows_and_the_status_list_follow_start_order(self):
        cq.write_json(self.ctx.p("capacity.json"), cap(at=self.clock[0]))
        long_ = self.submit("long", est_minutes=60)
        self.advance(1)
        short = self.submit("short", est_minutes=5)
        self.assertEqual([r["id"] for r in self.tick()["queued"]], [short, long_])
        st = cq.cmd_status(self.ctx, {})["queue"]
        self.assertEqual([(r["id"], r["position"]) for r in st], [(short, 1), (long_, 2)])

    def test_an_aged_job_does_not_beat_a_fresh_job_of_a_higher_priority(self):
        aged = J("aged", est_minutes=90, t_submitted=5000.0 - 90 * 60)
        fresh = J("fresh", priority="fixcheck", est_minutes=60, t_submitted=4900.0)
        self.assertEqual(self.order([aged, fresh]), ["fresh", "aged"])

    def test_an_aged_blocked_head_still_holds_the_host_against_a_short_job_of_its_priority(self):
        aged = J("aged", slots=3, est_minutes=60, t_submitted=5000.0 - 50 * 60)
        short = J("short", slots=1, est_minutes=1, t_submitted=4990.0)
        res = cq.guard([{"job": "short", "where": "host", "slots": 1}], [aged, short], cap(free=2), 5000.0)
        self.assertEqual(res["accepted"], [])
        self.assertIn("reserved for aged", res["refused"][-1]["reason"])

    def test_the_laptop_head_is_the_shortest_job_too(self):
        a = J("a", where="laptop", est_minutes=30, t_submitted=4900.0)
        b = J("b", where="laptop", est_minutes=5, t_submitted=4901.0)
        self.assertEqual(cq.reservations([a, b], cap(), now=5000.0)["laptop"]["job"]["id"], "b")


class BackfillTests(Base):
    """Bounded EASY backfill: while the host head job waits for slots, a short job may start if it ends before the head could."""

    def head(self, slots=3):
        return J("v", priority="verify", slots=slots, t_submitted=1000.0)

    def test_a_short_lane_job_backfills_in_both_modes_and_the_window_bounds_it(self):
        v = self.head()
        run = [R("r", 3, est=120, elapsed_min=20)]  # 100 min left: the window is 50 min
        for entries, mode in (([{"job": "l", "where": "host", "slots": 1}], "haiku"), (None, "fallback")):
            res = cq.guard(entries, [v, J("l", est_minutes=10)], cap(free=1), 2000.0, mode=mode, running=run)
            self.assertEqual([(a["job"], a.get("backfill")) for a in res["accepted"]], [("l", True)], mode)
        edge = [R("r", 3, est=82, elapsed_min=20)]  # a window of 31 min: the margin decides between 29 and 30
        fits = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, J("l", est_minutes=29)], cap(free=1), 2000.0, running=edge)
        self.assertEqual([a["job"] for a in fits["accepted"]], ["l"])  # 29 + 2 = 31 = the window
        late = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, J("l", est_minutes=30)], cap(free=1), 2000.0, running=edge)
        self.assertEqual(late["accepted"], [])  # 30 + 2 ends after the window
        self.assertTrue(late["refused"][0]["reason"].startswith("reserved for v"))
        big = [R("r", 3, est=300, elapsed_min=20)]  # a window of 140 min: still no job over BACKFILL_MAX_MIN
        res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, J("l", est_minutes=cq.BACKFILL_MAX_MIN + 1)], cap(free=1),
                       2000.0, running=big)
        self.assertTrue(res["refused"][0]["reason"].startswith("reserved for v"))

    def test_without_the_running_jobs_nothing_backfills(self):
        v, lane = self.head(), J("l", est_minutes=5)
        res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, lane], cap(free=1), 2000.0)
        self.assertEqual(res["accepted"], [])
        self.assertTrue(res["refused"][0]["reason"].startswith("reserved for v"))
        self.assertEqual(cq.guard(None, [v, lane], cap(free=1), 2000.0, mode="fallback")["accepted"], [])
        self.assertIsNone(cq.reservations([v, lane], cap(free=1))["host"]["window_min"])

    def test_an_overdue_running_job_gives_no_window(self):
        v = self.head()
        run = [R("r", 3, est=10, elapsed_min=20)]  # past its estimate: 0 min left
        self.assertEqual(cq.reservations([v, J("l")], cap(free=1), run, 2000.0)["host"]["window_min"], 0)
        res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, J("l", est_minutes=1)], cap(free=1), 2000.0, running=run)
        self.assertEqual(res["accepted"], [])

    def test_a_head_blocked_by_the_caps_only_gets_no_window(self):
        v, lane = self.head(), J("l", est_minutes=5)
        c = cap(free=4, load="11.5 0.4 0.3")  # four slots free, but the load cap leaves no room for three
        self.assertGreaterEqual(c["host"]["free"], 3)
        self.assertLess(cq.host_free(c), 3)
        res = cq.reservations([v, lane], c, [R("r", 1, est=300, elapsed_min=0)], 2000.0)
        self.assertIsNone(res["host"]["window_min"])
        out = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, lane], c, 2000.0, running=[R("r", 1, est=300, elapsed_min=0)])
        self.assertEqual(out["accepted"], [])

    def test_split_and_self_heavy_jobs_never_backfill(self):
        v = self.head()
        run = [R("r", 3, est=300, elapsed_min=20)]  # a large window
        for extra in ({"split": {"max_parts": 2}}, {"self_heavy": True}):
            lane = J("l", est_minutes=5, **extra)
            res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, lane], cap(free=1), 2000.0, running=run)
            self.assertEqual(res["accepted"], [], extra)
            self.assertTrue(res["refused"][0]["reason"].startswith("reserved for v"), extra)
            self.assertEqual(cq.guard(None, [v, lane], cap(free=1), 2000.0, mode="fallback", running=run)["accepted"], [], extra)

    def test_running_jobs_that_cannot_free_enough_slots_give_no_window(self):
        v = self.head()
        run = [R("r", 1, est=60, elapsed_min=0)]  # one slot freed with one free: 2 of the 3 the head needs
        self.assertIsNone(cq.reservations([v, J("l")], cap(free=1), run, 2000.0)["host"]["window_min"])
        res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, J("l", est_minutes=5)], cap(free=1), 2000.0, running=run)
        self.assertEqual(res["accepted"], [])

    def test_the_window_comes_from_the_running_job_whose_end_frees_enough_for_the_head(self):
        v = self.head()
        self.assertEqual(cq.head_window(v, cap(free=0), [R("a", 2, est=20, elapsed_min=10), R("b", 2, est=100, elapsed_min=40)],
                                        2000.0), 30.0)  # A ends in 10 min with 2 of 3 slots; B in 60 min frees the third
        v4 = self.head(slots=4)  # one free slot: A (1 slot, 10 min left) gives 2 of 4, B (2 slots, 60 min) the rest: window 30
        run = [R("a", 1, est=20, elapsed_min=10), R("b", 2, est=100, elapsed_min=40)]
        res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v4, J("l", est_minutes=10)], cap(free=1), 2000.0, running=run)
        self.assertEqual([(a["job"], a.get("backfill")) for a in res["accepted"]], [("l", True)])  # A's 5-min window would refuse it

    def test_a_launching_job_counts_from_its_launch_time(self):
        v, run = self.head(), [R("r", 3, est=120, elapsed_min=20, status="launching")]
        run[0]["t_started"] = 2000.0  # stale or absent while launching: only t_launch (20 min ago) counts
        self.assertEqual(cq.reservations([v, J("l")], cap(free=1), run, 2000.0)["host"]["window_min"], 50)
        res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, J("l", est_minutes=49)], cap(free=1), 2000.0, running=run)
        self.assertEqual(res["accepted"], [])

    def test_split_parts_count_one_nth_of_the_inherited_estimate(self):
        v = self.head()
        parts = [dict(R("p.p1", 1, est=60, elapsed_min=1), parent="p", part="1/3"),
                 dict(R("p.p2", 1, est=60, elapsed_min=1), parent="p", part="2/3"),
                 dict(R("p.p3", 1, est=60, elapsed_min=1), parent="p", part="3/3")]
        self.assertAlmostEqual(cq.reservations([v, J("l")], cap(free=0), parts, 2000.0)["host"]["window_min"], (60 / 3 - 1) * 0.5)
        res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, J("l", est_minutes=25)], cap(free=1), 2000.0, running=parts)
        self.assertEqual(res["accepted"], [])
        self.assertTrue(res["refused"][0]["reason"].startswith("reserved for v"))
        res = cq.guard([{"job": "l", "where": "host", "slots": 1}], [v, J("l", est_minutes=5)], cap(free=1), 2000.0, running=parts)
        self.assertEqual([(a["job"], a.get("backfill")) for a in res["accepted"]], [("l", True)])
        whole = [dict(p) for p in parts]
        for p in whole:
            del p["parent"], p["part"]
        self.assertAlmostEqual(cq.reservations([v, J("l")], cap(free=0), whole, 2000.0)["host"]["window_min"], (60 - 1) * 0.5)

    def test_the_laptop_reservation_never_backfills(self):
        v = J("v", priority="verify", where="laptop", slots=1, t_submitted=1000.0)
        l1 = J("l1", where="laptop", est_minutes=5, t_submitted=1001.0)
        busy = cap(free=0, busy=True)
        run = [R("r", 3, est=300, elapsed_min=20)]
        self.assertNotIn("window_min", cq.reservations([v, l1], busy, run, 2000.0)["laptop"])
        res = cq.guard([{"job": "l1", "where": "laptop", "slots": 1}], [v, l1], busy, 2000.0, running=run)
        self.assertEqual(res["accepted"], [])
        self.assertEqual(res["refused"][0]["reason"], "reserved for v (verify, 1 slots) waiting on the laptop")
        self.assertEqual(cq.guard(None, [v, l1], busy, 2000.0, mode="fallback", running=run)["accepted"], [])

    def test_tick_marks_the_backfill_row_and_the_plan_starts_it_beside_the_blocked_head(self):
        v = self.submit("verify", slots=3, self_heavy=True, priority="verify")
        lane = self.submit("lane", slots=1, est_minutes=10, where="host")
        big = self.submit("big", slots=3, self_heavy=True, est_minutes=120)
        job = self.job(big)
        job.update(status="running", placed_where="host", slots_eff=3, t_started=self.clock[0] - 20 * 60,
                   t_launch=self.clock[0] - 20 * 60)  # 100 min left: a 50 min window
        cq.save_job(self.ctx, job)
        self.host.held = 0  # the probe counts the unseen running job's three slots as pending: one free, the verify (3) blocked
        r = self.tick()
        rows = {row["id"]: row for row in r["queued"]}
        self.assertTrue(rows[lane]["backfill_ok"])
        self.assertNotIn("backfill_ok", rows[v])
        self.assertTrue(r["can_start"])
        cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": lane, "where": "host", "slots": 1}], "why": "backfill"}})
        self.assertEqual(self.job(lane)["status"], "running")
        self.assertEqual(self.job(v)["status"], "queued")
        accepted = [x for x in self.decisions() if x.get("kind") == "plan"][-1]["accepted"]
        self.assertEqual([(a["job"], a.get("backfill")) for a in accepted], [(lane, True)])


class SampleLogTests(Base):
    """samples.jsonl: one row a minute from the leader's tick, never raising, moved aside when it grows past the cap."""

    def rows(self):
        return [json.loads(l) for l in cq.read_lines(self.ctx.p("samples.jsonl"))]

    def row(self, queued=(), free=4, load="0.5 0.4 0.3", holding=(), can=False, **kw):
        """Log a sample 61 s after the last one (so it is written) and return the row."""
        self.advance(61)
        c = cap(free=free, load=load, **kw)
        host = cq.reservations(list(queued), c, list(holding), self.clock[0])["host"]
        cq.log_sample(self.ctx, self.clock[0], c, list(queued), list(holding), can, host)
        return self.rows()[-1]

    def test_first_row_has_the_fields_and_a_second_within_a_minute_is_skipped(self):
        q = [J("a", slots=1)]
        cq.log_sample(self.ctx, self.clock[0], cap(), q, [], False, cq.reservations(q, cap(), [], self.clock[0])["host"])
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(set(rows[0]), {"t", "at", "load1", "mem_avail_gb", "held", "pending", "free", "room", "nproc", "queued",
                                        "queued_heavy", "waiting_fit", "running", "can_start", "head", "head_slots", "head_state",
                                        "laptop_free_ram_gb", "laptop_commit_gb", "laptop_commit_limit_gb", "laptop_np_gb",
                                        "laptop_paged_gb", "laptop_cache_gb", "laptop_top", "laptop_unreadable", "laptop_pool",
                                        "cpu_busy", "cpu_user", "cpu_nice", "cpu_sys", "cpu_iowait",
                                        "cpu_steal", "cpu_idle", "runq", "psi_cpu10", "psi_cpu60", "psi_mem10", "psi_io10", "top",
                                        "job_cores", "bench_real_ms", "bench_cpu_ms"})
        self.assertEqual(rows[0]["t"], cq.iso(self.clock[0]))
        self.assertEqual((rows[0]["at"], rows[0]["load1"], rows[0]["mem_avail_gb"], rows[0]["free"], rows[0]["room"],
                          rows[0]["nproc"]), (1_800_000_000.0, 0.5, 20.0, 4, 4, 12))
        self.assertEqual((rows[0]["queued"], rows[0]["queued_heavy"], rows[0]["waiting_fit"], rows[0]["running"],
                          rows[0]["can_start"], rows[0]["head"], rows[0]["head_slots"], rows[0]["head_state"],
                          rows[0]["laptop_free_ram_gb"]), (1, 1, 1, 0, False, "a", 1, "fits", 16.0))
        cq.log_sample(self.ctx, self.clock[0] + 30, cap(), q, [], False, None)
        self.assertEqual(len(self.rows()), 1)
        self.advance(61)
        cq.log_sample(self.ctx, self.clock[0], cap(), q, [], False, None)
        self.assertEqual([r["at"] for r in self.rows()], [1_800_000_000.0, 1_800_000_061.0])

    def test_the_throttle_keeps_its_time_in_a_sidecar_file(self):
        q = [J("a", slots=1)]
        cq.log_sample(self.ctx, self.clock[0], cap(), q, [], False, None)
        with open(self.ctx.p("samples.at"), encoding="utf-8") as fh:
            self.assertEqual(float(fh.read()), self.clock[0])
        os.remove(self.ctx.p("samples.at"))  # without the sidecar the next call writes a row, even 30 s later
        cq.log_sample(self.ctx, self.clock[0] + 30, cap(), q, [], False, None)
        self.assertEqual(len(self.rows()), 2)

    def test_a_corrupt_sidecar_does_not_stop_logging(self):
        self.row()  # the first row writes the sidecar
        with open(self.ctx.p("samples.at"), "w", encoding="utf-8") as fh:
            fh.write("x")
        self.row()
        self.assertEqual(len(self.rows()), 2)

    def test_head_state_says_why_the_head_waits(self):
        self.assertIsNone(self.row()["head_state"])  # nothing queued: no host head
        self.assertEqual(self.row([J("a", slots=1)])["head_state"], "fits")
        slots = self.row([J("a", slots=3)], free=1)
        self.assertEqual((slots["head_state"], slots["head_slots"], slots["free"], slots["room"]), ("slots", 3, 1, 1))
        caps = self.row([J("a", slots=1)], load="11.5 0.4 0.3")  # 12 cpus less 11.5 leaves no room for a slot of 1.5
        self.assertEqual((caps["head_state"], caps["free"], caps["room"]), ("caps", 4, 0))

    def test_waiting_fit_counts_the_queued_jobs_that_fit_the_free_slots(self):
        q = [J("a", slots=1), J("b", slots=3, t_submitted=1001.0), J("c", slots=2, where="laptop", t_submitted=1002.0),
             J("d", slots=0, t_submitted=1003.0), J("e", slots=2, where="any", t_submitted=1004.0)]
        row = self.row(q, free=2)
        self.assertEqual((row["queued"], row["queued_heavy"], row["waiting_fit"]), (5, 4, 2))

    def test_never_raises(self):
        q = [J("a", slots=1)]
        with mock.patch.object(cq, "append_line", side_effect=OSError("disk full")):
            cq.log_sample(self.ctx, self.clock[0], cap(), q, [], False, None)  # the write fails: the tick goes on
        self.assertEqual(self.rows(), [])
        self.advance(61)  # the failed row still wrote samples.at, so the next calls need a due sample
        cq.log_sample(self.ctx, self.clock[0], None, q, [], False, None)  # no cap at all
        self.assertEqual(self.rows(), [])
        self.advance(61)
        cq.log_sample(self.ctx, self.clock[0], {}, q, [], False, None)  # the host key is missing: a row with blanks
        self.assertEqual((self.rows()[0]["load1"], self.rows()[0]["free"], self.rows()[0]["head_state"]), (None, 0, None))

    def test_the_sidecar_is_written_when_the_row_fails_so_the_next_ticks_do_not_profile_again(self):
        q = [J("a", slots=1)]
        with mock.patch.object(cq, "host_free", side_effect=RuntimeError("the row fails")):
            cq.log_sample(self.ctx, self.clock[0], cap(), q, [], False, None)  # raises inside the row's dict: no row
        self.assertEqual(self.rows(), [])
        with open(self.ctx.p("samples.at"), encoding="utf-8") as fh:
            self.assertEqual(float(fh.read()), self.clock[0])
        self.advance(30)
        with mock.patch.object(self.laptop, "top_procs", wraps=self.laptop.top_procs) as scans:
            cq.log_sample(self.ctx, self.clock[0], cap(), q, [], False, None)  # within SAMPLE_LOG_EVERY: nothing runs
        scans.assert_not_called()
        self.assertEqual(self.rows(), [])

    def test_a_file_past_the_cap_moves_to_dot_one_before_the_next_row(self):
        path = self.ctx.p("samples.jsonl")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        old = json.dumps({"at": self.clock[0] - 3600, "t": "old"}) + "\n"
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(old * 20)
        with mock.patch.object(cq, "SAMPLE_LOG_MAX_BYTES", 100):
            cq.log_sample(self.ctx, self.clock[0], cap(), [J("a")], [], False, None)
        with open(path + ".1", encoding="utf-8") as fh:
            self.assertEqual(fh.read(), old * 20)
        self.assertEqual([r["queued"] for r in self.rows()], [1])

    def test_a_tick_row_carries_the_laptop_memory_split_and_the_scans(self):
        self.tick()
        r = self.rows()[-1]
        self.assertEqual((r["laptop_commit_gb"], r["laptop_commit_limit_gb"], r["laptop_np_gb"], r["laptop_paged_gb"],
                          r["laptop_cache_gb"]), (9.5, 30.0, 0.4, 0.6, 1.2))
        self.assertEqual(r["laptop_top"], [["chrome", 20, 900, 700]])
        self.assertEqual(r["laptop_unreadable"], 3)
        self.assertEqual(r["laptop_pool"], [["Ntfs", 120.0, 5000]])

    def test_a_collector_that_raises_leaves_only_its_own_keys_empty(self):
        self.laptop.failing = {"top_procs", "mem_stats"}
        self.tick()
        r = self.rows()[-1]
        self.assertEqual((r["laptop_top"], r["laptop_unreadable"], r["laptop_commit_gb"]), (None, None, None))
        self.assertEqual(r["laptop_pool"], [["Ntfs", 120.0, 5000]])
        self.assertEqual(r["queued"], 0)
        self.laptop.failing = {"pool_top"}
        self.advance(61)
        self.tick()
        self.assertIsNone(self.rows()[-1]["laptop_pool"])
        self.assertEqual(self.rows()[-1]["laptop_top"], [["chrome", 20, 900, 700]])

    def test_a_laptop_without_the_memory_methods_gives_blank_keys(self):
        self.ctx.laptop = OldLaptop()
        self.assertNotIn("commit_gb", self.tick()["capacity"]["laptop"])
        r = self.rows()[-1]
        self.assertEqual([r[k] for k in ("laptop_commit_gb", "laptop_commit_limit_gb", "laptop_np_gb", "laptop_paged_gb",
                                         "laptop_cache_gb", "laptop_top", "laptop_unreadable", "laptop_pool")], [None] * 8)
        self.assertEqual(r["laptop_free_ram_gb"], 16.0)

    def test_a_tick_logs_the_queue_it_reports(self):
        self.submit("lane one", slots=1, est_minutes=10, where="host")
        r = self.tick()
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["queued"], len(r["queued"]))
        self.assertEqual(rows[0]["queued"], 1)


class HostTelemetryTests(Base):
    """The probe's CPU split, run queue, PSI and per-process pass: parsed, then reduced to the host block and the sample row."""

    CPU_ROW_KEYS = ("cpu_busy", "cpu_user", "cpu_nice", "cpu_sys", "cpu_iowait", "cpu_steal", "cpu_idle", "runq", "psi_cpu10",
                    "psi_cpu60", "psi_mem10", "psi_io10", "top", "job_cores")
    JIFFIES_1 = [1000, 0, 500, 8000, 500, 0, 0, 0, 0, 0]
    JIFFIES_2 = [1100, 0, 550, 8700, 600, 0, 50, 0, 0, 0]  # 1000 jiffies on the 8 fields: 10 user, 10 sys, 70 idle, 10 iowait

    def rows(self):
        return [json.loads(l) for l in cq.read_lines(self.ctx.p("samples.jsonl"))]

    def job_on_host(self, pid=4321):
        """A submitted job whose host run leads the session `pid` (the run.sh pid)."""
        jid = self.submit("go test ./pkg")
        now = int(self.clock[0])
        self.host.jobs[jid] = dict(exit=None, started=now - 60, launched=now - 60, finished=None, alive=True, src=True, pid=pid)
        return jid

    def profiled_host(self):
        """Session 4321 (run.sh, 3.0 cores in bash, 1.5 in a pkg.test, 1 GB RSS together) and one 0-tick process of session 4400."""
        self.host.procs = {4321: (4321, 0, 300, 131072, "bash"), 4400: (4321, 100, 250, 131072, "pkg.test"),
                           4500: (4500, 40, 40, 256, "idle")}

    def test_parse_snapshot_reads_every_new_line_and_drops_a_malformed_one(self):
        out = textwrap.dedent("""\
            PROBE
            SLOTS 8
            TICKHZ 250
            CPUSTAT 100 0 50 800 20 1 2 3 0 0
            RUNQ 5 1
            PSI cpu some avg10=1.10 avg60=1.62 avg300=4.55 total=1000 full avg10=0.00 avg60=0.00 avg300=0.00 total=0
            PSI memory some avg10=2.50 avg60=3.00 avg300=1.00 total=5 full avg10=0.25 avg60=0.10 avg300=0.05 total=1
            PSI io some avg10=0.50 avg60=x avg300=0.30 total=9 full avg10=0.10 avg60=0.05 avg300=0.01 total=2
            PAGEKB 4
            PROFMS 1000
            PA 100 100 300 2000 40 python3
            PB 100 100 350 2010 45 python3
            PA 200 200 10 50 0 Web Content
            PB 200 200 40 60 0 Web Content
            PA 300 300 x 50 0 bad
            PB 300 300 9 50 0 bad
            PB 500 500 1 1
            CQJOB cq-a exit= started=1 launched=1 finished= cpu=3 pid=4321 alive=1 src=1 now=3
            CQJOB cq-b exit= started=1 launched=1 finished= cpu= pid= alive=0 src=0 now=3
            JOBS
            END
            """)
        s = cq.parse_snapshot(out)
        self.assertEqual(s["tickhz"], 250)
        self.assertEqual(s["cpu_jiffies"], [100, 0, 50, 800, 20, 1, 2, 3, 0, 0])
        self.assertEqual(s["runq"], [5, 1])
        # io has a malformed avg60, so it is left out; cpu has no full10
        self.assertEqual(s["psi"], {"cpu": {"some10": 1.1, "some60": 1.62},
                                    "memory": {"some10": 2.5, "some60": 3.0, "full10": 0.25}})
        self.assertEqual((s["prof"]["ms"], s["prof"]["pagekb"]), (1000, 4))
        self.assertEqual(s["prof"]["a"], {100: (100, 300, 2000, 40, "python3"), 200: (200, 10, 50, 0, "Web Content")})
        self.assertEqual(set(s["prof"]["b"]), {100, 200, 300})  # a pid only in pass B is kept by the parser, ignored by the usage; PB 500 is short
        self.assertEqual((s["jobs"]["cq-a"]["pid"], s["jobs"]["cq-a"]["cpu"]), (4321, 3))
        self.assertIsNone(s["jobs"]["cq-b"]["pid"])

    def test_a_malformed_line_leaves_its_field_none_and_an_old_host_leaves_them_all_none(self):
        bad = ("PROBE\nCPUSTAT 1 2 x 4 5 6 7 8\nRUNQ 5\nPSI cpu some avg10=q avg60=1.00 avg300=1.00 total=1\nTICKHZ x\n"
               "PAGEKB 4\nPROFMS 0\nPA 1 1 1 1 0 a\nPB 1 1 2 1 0 a\nEND\n")
        s = cq.parse_snapshot(bad)
        self.assertEqual((s["cpu_jiffies"], s["runq"], s["psi"], s["tickhz"], s["prof"]), (None, None, None, 100, None))
        old = "PROBE\nSLOTS 8\nLOAD 0.5 0.4 0.3\nMEMAVAIL 100\nMEMTOTAL 200\nDISKKB 5\nNPROC 12\nJOBS\nEND\n"
        s = cq.parse_snapshot(old)
        self.assertEqual((s["tickhz"], s["cpu_jiffies"], s["runq"], s["psi"], s["prof"]), (100, None, None, None, None))
        self.assertEqual((s["load"], s["nproc"], s["slots"], s["mem_avail_kb"]), ("0.5 0.4 0.3", 12, 8, 100))

    def test_a_zero_negative_or_epoch_sized_profile_interval_gives_no_profile(self):
        body = "PAGEKB 4\nPA 1 1 1 1 0 a\nPB 1 1 2 1 0 a\nEND\n"
        for ms in ("0", "-5", "1700000000000", str(cq.PROF_MS_MAX + 1)):  # the last two: an empty date reading
            self.assertIsNone(cq.parse_snapshot("PROBE\nPROFMS " + ms + "\n" + body)["prof"], ms)
        self.assertIsNotNone(cq.parse_snapshot("PROBE\nPROFMS 1000\n" + body)["prof"])
        self.assertIsNotNone(cq.parse_snapshot("PROBE\nPROFMS %d\n" % cq.PROF_MS_MAX + body)["prof"])

    def test_control_characters_in_a_command_name_do_not_start_a_line_and_crlf_is_a_line_end(self):
        out = ("PROBE\r\nSLOTS 8\r\nHELD 0\nMEMAVAIL 5000\nPAGEKB 4\nPROFMS 1000\n"
               "PA 100 100 300 2000 0 \x0bMEMAVAIL 0\nPB 100 100 350 2010 0 \x0bMEMAVAIL 0\n"
               "PA 200 200 10 50 0 \x85HELD 9\nPB 200 200 40 60 0 \x85HELD 9\nPA 300 300 1 1 0 \x0c\x1c\x1d\x1e HELD 7\n"
               "PB 300 300 2 1 0 \x0c\x1c\x1d\x1e HELD 7\nEND\n")
        s = cq.parse_snapshot(out)
        self.assertEqual((s["slots"], s["held"], s["mem_avail_kb"]), (8, 1, 5000))  # HELD 9 and HELD 7 are comm text
        self.assertEqual(set(s["prof"]["a"]), {100, 200, 300})  # the processes are still parsed
        self.assertEqual(set(s["prof"]["b"]), {100, 200, 300})

    def test_cpu_usage_is_the_percent_split_of_the_change_in_the_jiffies(self):
        prev = {"at": 100.0, "j": [1000, 0, 500, 8000, 500, 0, 0, 0]}
        cur = {"at": 115.0, "j": [1100, 0, 550, 8700, 600, 0, 50, 0]}
        self.assertEqual(cq.cpu_usage(prev, cur), {"busy": 20.0, "user": 10.0, "nice": 0.0, "sys": 10.0, "iowait": 10.0,
                                                    "steal": 0.0, "idle": 70.0, "dt": 15.0})
        # deltas user 60, nice 20, system 30, idle 500, iowait 40, irq 10, softirq 20, steal 20: sys takes irq and softirq
        base = [1000] * 8
        mixed = [b + d for b, d in zip(base, [60, 20, 30, 500, 40, 10, 20, 20])]
        u = cq.cpu_usage({"at": 0.0, "j": base}, {"at": 10.0, "j": mixed})
        self.assertEqual(u, {"busy": 22.9, "user": 8.6, "nice": 2.9, "sys": 8.6, "iowait": 5.7, "steal": 2.9, "idle": 71.4, "dt": 10.0})

    def test_cpu_usage_is_none_for_a_short_or_long_interval_a_counter_back_or_no_ticks(self):
        prev = {"at": 100.0, "j": [1000, 0, 500, 8000, 500, 0, 0, 0]}
        cur = {"at": 115.0, "j": [1100, 0, 550, 8700, 600, 0, 50, 0]}
        self.assertIsNotNone(cq.cpu_usage(prev, dict(cur, at=103.0)))  # 3 s is the least
        self.assertIsNone(cq.cpu_usage(prev, dict(cur, at=102.0)))
        self.assertIsNotNone(cq.cpu_usage(prev, dict(cur, at=400.0)))  # 300 s is the most
        self.assertIsNone(cq.cpu_usage(prev, dict(cur, at=401.0)))
        self.assertEqual(cq.cpu_usage(prev, {"at": 115.0, "j": [990, 0, 550, 8700, 600, 0, 50, 0]})["user"], 0.0)  # user went back: 0 ticks
        self.assertIsNone(cq.cpu_usage(prev, dict(prev, at=115.0)))  # nothing ticked
        self.assertIsNone(cq.cpu_usage(None, cur))
        self.assertIsNone(cq.cpu_usage(prev, {"at": 115.0, "j": [1100, 0, 550]}))

    def test_cpu_usage_clamps_a_counter_that_went_back_and_takes_no_ticks_from_it(self):
        prev = {"at": 100.0, "j": [1000, 0, 500, 8000, 500, 0, 0, 0]}
        cur = {"at": 115.0, "j": [1100, 0, 550, 8700, 400, 0, 50, 0]}  # iowait went back by 100, the others rose
        self.assertEqual(cq.cpu_usage(prev, cur), {"busy": 22.2, "user": 11.1, "nice": 0.0, "sys": 11.1, "iowait": 0.0,
                                                    "steal": 0.0, "idle": 77.8, "dt": 15.0})

    def test_cpu_usage_is_none_for_a_reboot_the_counters_sum_below_the_reference(self):
        prev = {"at": 100.0, "j": [1000, 0, 500, 8000, 500, 0, 0, 0]}
        self.assertIsNone(cq.cpu_usage(prev, {"at": 115.0, "j": [10, 0, 5, 80, 5, 0, 0, 0]}))
        self.assertTrue(cq.cpu_rebooted(prev, {"at": 115.0, "j": [10, 0, 5, 80, 5, 0, 0, 0]}))
        self.assertFalse(cq.cpu_rebooted(prev, {"at": 115.0, "j": [1100, 0, 550, 8700, 400, 0, 50, 0]}))

    def test_a_reboot_gives_no_split_and_the_next_split_is_against_the_new_counters(self):
        self.host.cpu = self.JIFFIES_2
        cq.cmd_probe(self.ctx, {})
        self.advance(15)
        self.host.cpu = [10, 0, 5, 80, 5, 0, 0, 0, 0, 0]  # the host rebooted: its counters start again
        h = cq.cmd_probe(self.ctx, {})["capacity"]["host"]
        self.assertNotIn("cpu", h)
        self.assertEqual(cq.read_json(self.ctx.p("host.json"))["cpu_prev"], {"at": self.clock[0], "j": self.host.cpu})
        self.advance(15)
        self.host.cpu = [20, 0, 10, 160, 10, 0, 0, 0, 0, 0]
        h = cq.cmd_probe(self.ctx, {})["capacity"]["host"]
        self.assertEqual(h["cpu"], {"busy": 15.0, "user": 10.0, "nice": 0.0, "sys": 5.0, "iowait": 5.0, "steal": 0.0,
                                    "idle": 80.0, "dt": 15.0})

    def test_a_malformed_telemetry_value_in_host_json_is_dropped_and_the_probe_is_still_written(self):
        cq.write_json(self.ctx.p("host.json"), {"cpu_prev": {"at": 1.0, "j": None}, "cpu_last": 5, "prof": {"at": "x"}})
        self.host.cpu = self.JIFFIES_1
        self.assertTrue(cq.cmd_probe(self.ctx, {})["ok"])
        self.assertIsNotNone(cq.read_json(self.ctx.p("capacity.json")))
        self.assertFalse({"cpu_prev", "cpu_last", "prof"} & set(cq.read_json(self.ctx.p("host.json"))))
        self.advance(15)
        self.host.cpu = self.JIFFIES_1  # the reference is taken again from the healed host.json
        cq.cmd_probe(self.ctx, {})
        self.advance(15)
        self.host.cpu = self.JIFFIES_2
        h = cq.cmd_probe(self.ctx, {})["capacity"]["host"]
        self.assertEqual(h["cpu"]["busy"], 20.0)

    def test_profile_usage_gives_cores_from_the_tick_delta_and_ram_per_class_and_session(self):
        prof = {"ms": 1000, "pagekb": 4,
                "a": {1: (10, 1000, 0, 0, "worker"), 2: (10, 0, 0, 0, "worker"), 3: (20, 0, 0, 0, "pkg.test"),
                      4: (11, 50, 0, 0, "compile"), 6: (13, 10, 0, 0, "gone"), 7: (14, 500, 0, 0, "backwards"),
                      8: (15, 300, 0, 0, "idle")},
                "b": {1: (10, 1100, 256, 0, "worker"), 2: (10, 50, 512, 0, "worker"), 3: (20, 200, 256, 0, "pkg.test"),
                      4: (11, 75, 256, 0, "compile"), 5: (12, 90, 999, 0, "ghost"), 7: (14, 100, 0, 0, "backwards"),
                      8: (15, 300, 4, 0, "idle")}}
        u = cq.profile_usage(prof, 100)
        # 100 ticks and 50 ticks in 1 s at 100 Hz: 1.0 and 0.5 cores; pid 6 (A only), 7 (fewer ticks in B) and 5 (B only) are not
        # classes; pid 8 has no tick change. Sessions: 10 is 1000 -> 1150 ticks, 11 is 50 -> 75, 12 is new in B with 90 ticks, 14 is
        # 500 -> 100 (clamped to 0), 15 is 300 -> 300, 20 is new with 200 ticks; 13 is only in A and is dropped
        self.assertEqual(u["classes"], [{"name": "go-test", "cores": 2.0, "rss_mb": 1, "n": 1},
                                        {"name": "worker", "cores": 1.5, "rss_mb": 3, "n": 2},
                                        {"name": "go-compile", "cores": 0.25, "rss_mb": 1, "n": 1}])
        self.assertEqual(u["sessions"], {"10": {"cores": 1.5, "rss_mb": 3, "n": 2}, "11": {"cores": 0.25, "rss_mb": 1, "n": 1},
                                         "12": {"cores": 0.9, "rss_mb": 3, "n": 1}, "14": {"cores": 0.0, "rss_mb": 0, "n": 1},
                                         "15": {"cores": 0.0, "rss_mb": 0, "n": 1}, "20": {"cores": 2.0, "rss_mb": 1, "n": 1}})

    def test_compile_link_and_vet_are_classes_of_their_own_and_other_go_tools_share_go_tool(self):
        prof = {"ms": 1000, "pagekb": 4,
                "a": {1: (10, 0, 0, 0, "compile"), 2: (10, 0, 0, 0, "compile"), 3: (11, 0, 0, 0, "link"), 4: (12, 0, 0, 0, "vet"),
                      5: (13, 0, 0, 0, "asm"), 6: (13, 0, 0, 0, "cover")},
                "b": {1: (10, 100, 256, 0, "compile"), 2: (10, 50, 256, 0, "compile"), 3: (11, 80, 256, 0, "link"),
                      4: (12, 20, 256, 0, "vet"), 5: (13, 10, 256, 0, "asm"), 6: (13, 10, 256, 0, "cover")}}
        classes = {c["name"]: (c["cores"], c["n"]) for c in cq.profile_usage(prof, 100)["classes"]}
        self.assertEqual(classes, {"go-compile": (1.5, 2), "go-link": (0.8, 1), "go-vet": (0.2, 1), "go-tool": (0.2, 2)})

    def test_a_child_reaped_inside_the_window_counts_once_in_its_session(self):
        # the make (pid 30) ran no ticks of its own; its child compile (pid 31) had 50 ticks in A, took 30 more and was reaped:
        # make's cticks went 0 -> 80. The session total is 100 + 0 + 50 = 150 in A and 100 + 80 = 180 in B: 0.3 cores
        prof = {"ms": 1000, "pagekb": 4,
                "a": {30: (30, 100, 1000, 0, "make"), 31: (30, 50, 500, 0, "compile")},
                "b": {30: (30, 100, 1000, 80, "make")}}
        u = cq.profile_usage(prof, 100)
        self.assertEqual(u["sessions"], {"30": {"cores": 0.3, "rss_mb": 3, "n": 1}})
        self.assertEqual(u["classes"], [])  # make ran no ticks of its own, and the child is not in both passes

    def test_a_session_seen_only_in_pass_a_is_dropped_and_a_total_that_falls_clamps_to_zero(self):
        prof = {"ms": 1000, "pagekb": 4, "a": {40: (40, 100, 0, 0, "gone"), 50: (50, 300, 1000, 0, "x")},
                "b": {50: (50, 100, 1000, 0, "x")}}
        u = cq.profile_usage(prof, 100)
        self.assertEqual(u["sessions"], {"50": {"cores": 0.0, "rss_mb": 3, "n": 1}})
        self.assertEqual(u["classes"], [])

    def test_a_session_whose_reaped_child_lands_ten_minutes_of_ticks_in_one_window_is_capped_at_nproc(self):
        # the parent (pid 60) is read in A before its child is reaped; the child's 10 minutes of ticks (60000 at 100 Hz) reach
        # the parent's cticks in B: 600 cores in one second, which the host's 12 vCPU cap to 12
        prof = {"ms": 1000, "pagekb": 4, "a": {60: (60, 0, 1000, 0, "make")}, "b": {60: (60, 0, 1000, 60000, "make")}}
        self.assertEqual(cq.profile_usage(prof, 100, 12)["sessions"]["60"]["cores"], 12.0)
        self.assertEqual(cq.profile_usage(prof, 100, None)["sessions"]["60"]["cores"], 600.0)
        self.assertEqual(cq.profile_usage(prof, 100)["sessions"]["60"]["cores"], 600.0)
        self.assertEqual(cq.profile_usage(prof, 100, 0)["sessions"]["60"]["cores"], 600.0)  # 0 is not a vCPU count: no cap

    def test_profile_usage_keeps_the_eight_busiest_classes(self):
        prof = {"ms": 1000, "pagekb": 4, "a": {i: (1, 0, 0, 0, f"c{i}") for i in range(1, 10)},
                "b": {i: (1, 10 * i, 1, 0, f"c{i}") for i in range(1, 10)}}
        classes = cq.profile_usage(prof, 100)["classes"]
        self.assertEqual([c["name"] for c in classes], [f"c{i}" for i in range(9, 1, -1)])
        self.assertIsNone(cq.profile_usage(None, 100))

    def test_benchmark_line_gives_real_and_cpu_milliseconds_or_none(self):
        good = cq.parse_snapshot("PROBE\nBENCH 0.512 0.500 0.010\nEND\n")["bench"]
        self.assertEqual(good, {"real_ms": 512, "cpu_ms": 510})
        for bad in ("BENCH 0.5 x 0.1", "BENCH 0.5 0.4", "BENCH 0.5 0.4 0.1 0.2", "BENCH", "BENCH nan 0 0", "BENCH 1e400 0 0",
                    "BENCH -1 0 0"):
            self.assertIsNone(cq.parse_snapshot("PROBE\n" + bad + "\nEND\n")["bench"], bad)
        self.assertIsNone(cq.parse_snapshot("PROBE\nEND\n")["bench"])

    def test_the_plain_probe_has_no_benchmark_and_no_process_pass_and_the_profiled_one_has_both(self):
        plain = cq.snapshot_script(False)
        self.assertNotIn("BENCH", plain)
        self.assertNotIn("pstat", plain)
        self.assertNotIn("TIMEFORMAT", plain)
        profiled = cq.snapshot_script(True)
        self.assertIn('echo "BENCH $b"', profiled)
        self.assertIn("b=$( LC_ALL=C; TIMEFORMAT='%R %U %S'; { time awk", profiled)  # the decimal point of the seconds is '.'
        self.assertLess(profiled.index("PROFMS"), profiled.index('echo "BENCH $b"'))

    def test_job_cpu_matches_a_live_job_pid_to_its_profiled_session(self):
        sessions = {"4321": {"cores": 2.0, "rss_mb": 2048, "n": 3}, "77": {"cores": 1.0, "rss_mb": 1, "n": 1}}
        jobs = {"cq-a": {"pid": 4321, "alive": True, "exit": None}, "cq-b": {"pid": None, "alive": True, "exit": None},
                "cq-c": {"pid": 9, "alive": True, "exit": None}, "cq-d": {}}
        self.assertEqual(cq.job_cpu(jobs, sessions), {"cq-a": {"cores": 2.0, "rss_gb": 2.0}})

    def test_job_cpu_keeps_the_sixteen_busiest_of_twenty_live_jobs_in_descending_cores_order(self):
        jobs = {f"cq-{i:02d}": {"pid": 1000 + i, "alive": True, "exit": None} for i in range(20)}
        sessions = {str(1000 + i): {"cores": float(i), "rss_mb": 1024, "n": 1} for i in range(20)}
        got = cq.job_cpu(jobs, sessions)
        self.assertEqual(list(got), [f"cq-{i:02d}" for i in range(19, 3, -1)])  # cores 19 down to 4: the 16 busiest
        self.assertEqual(got["cq-19"], {"cores": 19.0, "rss_gb": 1.0})

    def test_job_cpu_ignores_a_finished_job_whose_pid_was_reused_by_a_busy_session(self):
        out = ("PROBE\nPAGEKB 4\nPROFMS 1000\nPA 4400 4400 0 131072 0 bash\nPB 4400 4400 300 131072 0 bash\n"
               "PA 4321 4321 0 1024 0 bash\nPB 4321 4321 100 1024 0 bash\n"
               "CQJOB cq-live exit= started=1 launched=1 finished= pid=4321 alive=1 src=1 now=3\n"
               "CQJOB cq-stale exit=0 started=1 launched=1 finished=2 pid=4400 alive=0 src=0 now=3\nJOBS\nEND\n")
        s = cq.parse_snapshot(out)
        use = cq.profile_usage(s["prof"], s["tickhz"])
        self.assertEqual(set(use["sessions"]), {"4321", "4400"})  # the busy session 4400 is there to be matched
        self.assertEqual(set(cq.job_cpu(s["jobs"], use["sessions"])), {"cq-live"})

    def test_the_cpu_split_is_the_change_between_two_probes_and_is_saved_in_host_json(self):
        self.host.cpu, self.host.runq = self.JIFFIES_1, (3, 1)
        self.host.psi = {"cpu": (1.5, 2.0), "memory": (2.5, 1.0), "io": (0.5, 0.4)}
        h = self.tick()["capacity"]["host"]
        self.assertNotIn("cpu", h)  # the first probe has no earlier counters to difference
        self.assertEqual((h["runq"], h["psi"]["cpu"]), ([3, 1], {"some10": 1.5, "some60": 2.0}))
        self.assertEqual(cq.read_json(self.ctx.p("host.json"))["cpu_prev"], {"at": self.clock[0], "j": self.JIFFIES_1})
        self.advance(15)
        self.host.cpu = self.JIFFIES_2
        h = self.tick()["capacity"]["host"]
        self.assertEqual(h["cpu"], {"busy": 20.0, "user": 10.0, "nice": 0.0, "sys": 10.0, "iowait": 10.0, "steal": 0.0,
                                    "idle": 70.0, "dt": 15.0})
        self.assertEqual(h["psi"]["io"], {"some10": 0.5, "some60": 0.4, "full10": 0.0})

    def test_a_profiled_tick_gives_the_top_classes_and_the_job_that_leads_the_busiest_session(self):
        jid = self.job_on_host()
        self.profiled_host()
        h = self.tick()["capacity"]["host"]
        self.assertEqual([(c["name"], c["cores"], c["rss_mb"]) for c in h["top"]], [("bash", 3.0, 512), ("go-test", 1.5, 512)])
        self.assertEqual(h["job_cpu"], {jid: {"cores": 4.5, "rss_gb": 1.0}})

    def test_the_benchmark_reaches_the_host_block_and_the_sample_row_only_from_a_profiled_probe(self):
        self.host.bench = "0.512 0.500 0.010"
        self.profiled_host()
        h = self.tick()["capacity"]["host"]  # the first tick is profiled: no sample yet
        self.assertEqual(h["bench"], {"real_ms": 512, "cpu_ms": 510})
        self.assertEqual((self.rows()[-1]["bench_real_ms"], self.rows()[-1]["bench_cpu_ms"]), (512, 510))
        self.advance(10)
        self.assertNotIn("bench", self.tick()["capacity"]["host"])  # not profiled: no benchmark, and none kept from the last one
        self.advance(51)
        self.host.bench = None
        self.assertNotIn("bench", self.tick()["capacity"]["host"])  # profiled, but the probe printed no BENCH line
        self.assertEqual((self.rows()[-1]["bench_real_ms"], self.rows()[-1]["bench_cpu_ms"]), (None, None))

    def test_the_probe_profiles_only_when_asked_and_the_leader_profiles_once_a_minute(self):
        self.profiled_host()
        self.host.calls.clear()
        self.tick()  # no sample yet: profiled
        self.advance(10)
        self.tick()  # inside SAMPLE_LOG_EVERY: not profiled
        self.advance(51)
        self.tick()  # 61 s after the first: profiled again
        scripts = [c for c in self.host.calls if "echo PROBE" in c]
        self.assertEqual(["sleep 1" in s for s in scripts], [True, False, True])
        self.host.calls.clear()
        cq.cmd_probe(self.ctx, {})
        self.assertFalse(any("sleep 1" in c for c in self.host.calls))

    def test_the_sample_rows_carry_the_cpu_fields_and_an_old_host_leaves_them_none(self):
        self.tick()  # the first row: no CPU lines from this host
        self.assertEqual({k: self.rows()[-1][k] for k in self.CPU_ROW_KEYS}, dict.fromkeys(self.CPU_ROW_KEYS))
        self.advance(61)
        self.host.cpu, self.host.runq, self.host.psi = self.JIFFIES_1, (3, 1), {"cpu": (1.5, 2.0), "memory": (2.5, 1.0), "io": (0.5, 0.4)}
        self.profiled_host()
        jid = self.job_on_host()
        self.tick()  # the split needs two probes
        self.advance(61)
        self.host.cpu = self.JIFFIES_2
        self.tick()
        r = self.rows()[-1]
        self.assertEqual((r["cpu_busy"], r["cpu_user"], r["cpu_nice"], r["cpu_sys"], r["cpu_iowait"], r["cpu_steal"], r["cpu_idle"]),
                         (20.0, 10.0, 0.0, 10.0, 10.0, 0.0, 70.0))
        self.assertEqual((r["runq"], r["psi_cpu10"], r["psi_cpu60"], r["psi_mem10"], r["psi_io10"]), (3, 1.5, 2.0, 2.5, 0.5))
        self.assertEqual((r["top"], r["job_cores"]), ([["bash", 3.0], ["go-test", 1.5]], {jid: 4.5}))

    def test_a_profiled_session_is_capped_at_the_probes_nproc_in_the_host_block(self):
        jid = self.job_on_host(pid=50)  # the job's session 50: its parent 50 reaps a child with 60000 ticks in the window
        self.host.procs = {50: (50, 0, 0, 1000, "run.sh")}
        self.host.cticks = {50: (0, 60000)}
        h = self.tick()["capacity"]["host"]  # the first tick is profiled; the probe reports NPROC 12
        self.assertEqual(h["job_cpu"][jid]["cores"], 12.0)

    def test_a_profiled_probe_is_shown_on_the_probes_for_three_minutes_after_it(self):
        jid = self.job_on_host()
        self.profiled_host()
        h = self.tick()["capacity"]["host"]
        self.assertEqual((h["prof_age_s"], h["job_cpu"]), (0, {jid: {"cores": 4.5, "rss_gb": 1.0}}))
        self.advance(15)
        h = cq.cmd_probe(self.ctx, {})["capacity"]["host"]  # the probe takes no per-process pass
        self.assertEqual((h["prof_age_s"], [c["name"] for c in h["top"]], h["job_cpu"]),
                         (15, ["bash", "go-test"], {jid: {"cores": 4.5, "rss_gb": 1.0}}))
        self.advance(185)  # 200 s after the profile
        h = cq.cmd_probe(self.ctx, {})["capacity"]["host"]
        self.assertFalse({"top", "job_cpu", "prof_age_s"} & set(h))

    def test_probes_close_together_keep_the_reference_and_the_last_split_is_held(self):
        self.host.cpu = self.JIFFIES_1
        cq.cmd_probe(self.ctx, {})
        self.advance(15)
        self.host.cpu = self.JIFFIES_2
        h = cq.cmd_probe(self.ctx, {})["capacity"]["host"]
        ref = self.clock[0]
        self.assertEqual(h["cpu"]["busy"], 20.0)
        held = {"busy": 20.0, "user": 10.0, "nice": 0.0, "sys": 10.0, "iowait": 10.0, "steal": 0.0, "idle": 70.0, "dt": 15.0}
        for jiffies in ([1150, 0, 560, 8800, 610, 0, 60, 0, 0, 0], [1160, 0, 565, 8810, 615, 0, 62, 0, 0, 0]):
            self.advance(1)  # 1 s and 2 s after the reference: too short to difference, so the reference stays
            self.host.cpu = jiffies
            h = cq.cmd_probe(self.ctx, {})["capacity"]["host"]
            self.assertEqual(h["cpu"], held)
            self.assertEqual(cq.read_json(self.ctx.p("host.json"))["cpu_prev"]["at"], ref)
        self.advance(13)  # 15 s after the reference: the split is taken against the reference, not the probes in between
        self.host.cpu = [1400, 0, 650, 9200, 700, 0, 50, 0, 0, 0]
        h = cq.cmd_probe(self.ctx, {})["capacity"]["host"]
        self.assertEqual((h["cpu"]["busy"], h["cpu"]["user"], h["cpu"]["idle"], h["cpu"]["dt"]), (40.0, 30.0, 50.0, 15.0))
        self.assertNotIn("at", h["cpu"])


class HostAddressTests(Base):
    """host.json is one machine's history: a probe that finds another address in it starts it again."""

    OLD = "builder@198.51.100.17"
    NEW = ""
    JIFFIES = [1000, 0, 500, 8000, 500, 0, 0, 0, 0, 0]
    JIFFIES_LATER = [1100, 0, 550, 8700, 600, 0, 50, 0, 0, 0]

    def old_history(self):
        now = self.clock[0]
        return {"address": self.OLD, "mem_hist": {str(int(now // 3600)): 60.0}, "idle_samples": 4,
                "cpu_prev": {"at": now - 15, "j": self.JIFFIES},
                "prof": {"at": now - 30, "top": [{"name": "bash", "cores": 3.0, "rss_mb": 512, "n": 1}], "job_cpu": {}},
                "down_since": "2026-10-01T00:00:00Z", "down_reason": "old host"}

    def test_a_changed_address_drops_the_old_history_and_the_probe_starts_host_json_again(self):
        cq.write_json(self.ctx.p("host.json"), self.old_history())
        self.host.cpu = self.JIFFIES_LATER
        with mock.patch.object(cq, "HOST", self.NEW):
            cap = cq.cmd_probe(self.ctx, {})["capacity"]
        hs = cq.read_json(self.ctx.p("host.json"))
        self.assertEqual(cap["host"]["mem_idle_gb"], round(self.host.mem_kb / 2**20, 1))  # not the old 60 GB baseline
        self.assertNotIn("cpu", cap["host"])  # the old CPU reference is gone, so there is no split yet
        self.assertNotIn("top", cap["host"])  # nor the old profile
        self.assertEqual(hs["idle_samples"], 1)
        self.assertEqual(hs["cpu_prev"], {"at": self.clock[0], "j": self.JIFFIES_LATER})
        self.assertNotIn("prof", hs)
        self.assertEqual(hs["address"], self.NEW)

    def test_a_changed_address_with_the_host_down_starts_the_outage_at_this_probe(self):
        cq.write_json(self.ctx.p("host.json"), self.old_history())
        self.host.up = False
        with mock.patch.object(cq, "HOST", self.NEW):
            cq.cmd_probe(self.ctx, {})
        hs = cq.read_json(self.ctx.p("host.json"))
        self.assertEqual(hs["down_since"], cq.iso(self.clock[0]))  # not the old host's 2026-10-01
        self.assertNotIn("old host", hs["down_reason"])
        self.assertNotIn("mem_hist", hs)
        self.assertEqual(hs["address"], self.NEW)

    def test_a_host_json_with_no_address_keeps_its_history_and_records_the_address(self):
        old = self.old_history()
        del old["address"]
        cq.write_json(self.ctx.p("host.json"), old)
        self.host.cpu = self.JIFFIES_LATER
        with mock.patch.object(cq, "HOST", self.NEW):
            cap = cq.cmd_probe(self.ctx, {})["capacity"]
        hs = cq.read_json(self.ctx.p("host.json"))
        self.assertEqual(cap["host"]["mem_idle_gb"], 60.0)  # the baseline is kept
        self.assertEqual(cap["host"]["cpu"]["dt"], 15.0)  # so is the CPU reference
        self.assertEqual(hs["idle_samples"], 5)
        self.assertEqual(hs["prof"]["at"], self.clock[0] - 30)
        self.assertEqual(hs["address"], self.NEW)


class DiskLineTests(unittest.TestCase):
    """The DISKKB line of SNAPSHOT_TEMPLATE run by bash over a stub df: the smaller free space of $HOME and $HOME/q is reported."""

    # a df function in front of the line: one row per path argument, in argument order, as df --output=avail prints them
    STUB_DF = ('df() { echo Avail; for a in "$@"; do case "$a" in "$HOME") echo "$HOME_KB";; "$HOME/q") echo "$Q_KB";; esac; done; }\n')

    def setUp(self):
        if not HAVE_BASH:
            self.skipTest("bash needed")
        self.home = tempfile.mkdtemp(prefix="cqdisk")
        self.addCleanup(cq.rmtree_force, self.home)

    def disk_kb(self, home_kb, q_kb):
        line = re.search(r'^echo "DISKKB .*$', cq.SNAPSHOT_TEMPLATE, re.M).group(0)
        env = dict(os.environ, HOME=self.home.replace("\\", "/"), HOME_KB=str(home_kb), Q_KB=str(q_kb))
        r = subprocess.run([BASH, "-c", self.STUB_DF + 'mkdir -p "$HOME/q"\n' + line], capture_output=True, text=True, env=env, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        return cq.parse_snapshot(r.stdout)["disk_kb"]

    def test_the_disk_line_reports_the_smaller_of_home_and_home_q(self):
        self.assertEqual(self.disk_kb(50_000_000, 7_000_000), 7_000_000)  # ~/q on the other drive is the short one
        self.assertEqual(self.disk_kb(3_000_000, 9_000_000), 3_000_000)  # $HOME is the short one


class PstatTests(unittest.TestCase):
    """The pstat() of SNAPSHOT_TEMPLATE itself, run by bash and awk over synthetic /proc/PID/stat files."""

    STATS = {
        101: "101 (bash) S 1 101 101 0 -1 4194560 100 0 0 0 55 45 7 3 20 0 3 0 100 1000000 250 0 0",  # cutime 7, cstime 3
        102: "102 (kthreadd) S 0 0 0 0 -1 2129984 0 0 0 0 0 0 0 0 20 0 1 0 1 0 0",  # a kernel thread: session 0, dropped
        103: "103 (name with ) paren) S 1 103 103 0 -1 4194560 100 0 0 0 10 20 0 0 20 0 1 0 100 1000 300 0 0",
        104: "104 (bad\x01name) R 1 104 104 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 100 1000 40 0 0",
        105: "105 (idle) S 1 105 105 0 -1 4194560 0 0 0 0 0 0 0 0 20 0 1 0 100 1000 0 0 0",  # no RSS: printed now, rss 0
    }

    def setUp(self):
        if not (HAVE_BASH and shutil.which("awk")):
            self.skipTest("bash and awk needed")
        self.tmp = tempfile.mkdtemp(prefix="cqpstat")
        self.addCleanup(cq.rmtree_force, self.tmp)
        for pid, text in self.STATS.items():
            os.makedirs(os.path.join(self.tmp, str(pid)))
            with open(os.path.join(self.tmp, str(pid), "stat"), "w", encoding="utf-8") as fh:
                fh.write(text + "\n")

    def run_pstat(self):
        root = self.tmp.replace("\\", "/")
        fn = re.search(r"^[ \t]*(pstat\(\) \{.*\})[ \t]*$", cq.SNAPSHOT_TEMPLATE, re.M).group(1)
        script = fn.replace("/proc/[0-9]*/stat", root + "/[0-9]*/stat") + "\npstat PA\n"
        r = subprocess.run([BASH, "-c", script], capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout.splitlines()

    def test_pstat_prints_tag_pid_session_ticks_rss_reaped_child_ticks_and_the_command(self):
        self.assertEqual(self.run_pstat(), ["PA 101 101 100 250 10 bash", "PA 103 103 30 300 0 name with ) paren",
                                            "PA 104 104 3 40 0 bad?name", "PA 105 105 0 0 0 idle"])


class SubmitTests(Base):
    def test_notice_alias_matches_only_the_original_directory_identity(self):
        alias = os.path.join(self.tmp, 'repo-alias')
        if os.name == 'nt':
            subprocess.run(['cmd.exe', '/d', '/c', 'mklink', '/J', alias, self.repo],
                           check=True, capture_output=True, creationflags=0x08000000)
            self.addCleanup(os.rmdir, alias)
        else:
            os.symlink(self.repo, alias, target_is_directory=True)
            self.addCleanup(os.unlink, alias)
        original = cq.worktree_identity(self.repo)
        job = {'top': alias, 'worktree_identity': original}
        self.assertTrue(cq.same_path(alias, self.repo))
        self.assertTrue(cq.notice_matches_worktree(job, self.repo))
        job['worktree_identity'] = dict(original, inode=original['inode'] + 1)
        self.assertFalse(cq.notice_matches_worktree(job, self.repo))

    def test_baton_owner_lock_serializes_agent_submission_and_cancellation(self):
        spec = importlib.util.spec_from_file_location('baton_locking_test', Path(__file__).resolve().parents[3] / 'baton' / 'locking.py')
        baton_locking = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(baton_locking)
        owner = Path(self.repo) / '.baton' / 'owner.json'
        for operation in ('submit', 'cancel'):
            with self.subTest(operation=operation):
                cq.write_json(str(owner), {'vendor': 'claude', 'session_id': 'sess-A'})
                jid = self.submit() if operation == 'cancel' else None
                started, acquired = threading.Event(), threading.Event()
                errors = []
                def handoff():
                    try:
                        started.set()
                        with baton_locking.exclusive(owner.parent / 'owner.lock'):
                            acquired.set()
                            cq.write_json(str(owner), {'vendor': 'codex', 'session_id': 'sess-B'})
                    except Exception as exc:
                        errors.append(exc)
                thread = None
                original = cq.save_job
                def save(ctx, job):
                    nonlocal thread
                    if thread is None:
                        thread = threading.Thread(target=handoff)
                        thread.start()
                        self.assertTrue(started.wait(1))
                        self.assertFalse(acquired.wait(.15), 'owner changed during queue mutation')
                    return original(ctx, job)
                with mock.patch.object(cq, 'save_job', side_effect=save):
                    if operation == 'submit':
                        self.submit()
                    else:
                        cq.cmd_cancel(self.ctx, {'job': jid, 'root': self.repo, 'session': 'sess-A'})
                thread.join(3)
                self.assertFalse(thread.is_alive())
                self.assertTrue(acquired.is_set())
                self.assertEqual(errors, [])

    def test_agent_submit_requires_owner_before_and_after_snapshot(self):
        owner_path = os.path.join(self.repo, '.baton', 'owner.json')
        cq.write_json(owner_path, {'vendor': 'claude', 'session_id': 'new'})
        with mock.patch.object(cq, 'snapshot', side_effect=AssertionError('must not snapshot')):
            with self.assertRaisesRegex(cq.CqError, 'owner'):
                self.submit()
        cq.write_json(owner_path, {'vendor': 'claude', 'session_id': 'sess-A'})
        original = cq.snapshot
        def handoff(*args):
            result = original(*args)
            cq.write_json(owner_path, {'vendor': 'codex', 'session_id': 'new'})
            return result
        with mock.patch.object(cq, 'snapshot', side_effect=handoff):
            with self.assertRaisesRegex(cq.CqError, 'owner'):
                self.submit()
        self.assertEqual(cq.queued_jobs(self.ctx), [])

    def test_agent_cancel_requires_matching_owner_and_directory_identity(self):
        jid = self.submit()
        owner_path = os.path.join(self.repo, '.baton', 'owner.json')
        req = {'job': jid, 'root': self.repo, 'session': 'sess-A'}
        with self.assertRaisesRegex(cq.CqError, 'does not match'):
            cq.cmd_cancel(self.ctx, dict(req, root=self.tmp))
        cq.write_json(owner_path, {'vendor': 'codex', 'session_id': 'sess-B'})
        with self.assertRaisesRegex(cq.CqError, 'owner'):
            cq.cmd_cancel(self.ctx, req)
        req['session'] = 'sess-B'
        job = self.job(jid)
        original = job['worktree_identity']
        for identity in (None, {'device': -1, 'inode': -1, 'birth_ns': None}):
            job['worktree_identity'] = identity
            cq.save_job(self.ctx, job)
            with self.assertRaisesRegex(cq.CqError, 'identity'):
                cq.cmd_cancel(self.ctx, req)
        job['worktree_identity'] = original
        cq.save_job(self.ctx, job)
        self.assertEqual(cq.cmd_cancel(self.ctx, req)['status'], 'cancelled')

    def test_submit_without_session_or_root_is_refused_and_queues_nothing(self):
        for missing in ('session', 'root'):
            for value in (None, ''):
                req = {'command': 'echo hi', 'root': self.repo, 'session': 'sess-A'}
                if value is None:
                    del req[missing]
                else:
                    req[missing] = value
                with self.subTest(missing=missing, value=value):
                    for call in (lambda: cq.cmd_submit(self.ctx, req), lambda: cq.execute(self.ctx, 'submit', req)):
                        with self.assertRaisesRegex(cq.CqError, 'submission requires ' + missing):
                            call()
        self.assertEqual(cq.queued_jobs(self.ctx), [])

    def test_cancel_requires_root_and_session_and_keeps_the_job_queued(self):
        jid = self.submit()
        for req in ({'job': jid}, {'job': jid, 'root': self.repo}, {'job': jid, 'session': 'sess-A'}):
            for call in (lambda: cq.cmd_cancel(self.ctx, req), lambda: cq.execute(self.ctx, 'cancel', req)):
                with self.assertRaisesRegex(cq.CqError, 'cancellation requires'):
                    call()
            self.assertEqual(self.job(jid)['status'], 'queued')
        with self.assertRaisesRegex(cq.CqError, 'does not match'):
            self.cancel(jid, root=self.tmp)
        with self.assertRaisesRegex(cq.CqError, 'owner'):
            self.cancel(jid, session='sess-other')
        self.assertEqual(self.job(jid)['status'], 'queued')
        self.assertEqual(self.cancel(jid)['status'], 'cancelled')

    def test_standalone_protocol_two_notified_returns_acknowledged_ids(self):
        jid = self.submit()
        job = self.job(jid)
        job['status'] = 'done'
        cq.save_job(self.ctx, job)
        reply = cq.execute(self.ctx, 'notified', {'ids': [jid], 'owner_root': self.repo,
                           'owner_session': 'sess-A', 'notice_protocol': 2})
        self.assertEqual(reply['acknowledged_ids'], [jid])
        self.assertTrue(self.job(jid)['notified'])

    def test_equal_timestamp_submissions_follow_arrival_not_random_job_ids(self):
        with mock.patch.object(cq, "new_id", side_effect=["cq-20261018T000000-ffff", "cq-20261018T000000-0000"]):
            first = self.submit("first")
            second = self.submit("second")
        self.assertEqual(self.job(first)["t_submitted"], self.job(second)["t_submitted"])
        self.assertLess(self.job(first)["queue_seq"], self.job(second)["queue_seq"])
        self.assertEqual([job["id"] for job in cq.queued_jobs(self.ctx)], [first, second])

    def test_S18_part_without_split_refused(self):
        with self.assertRaisesRegex(cq.CqError, r"\{part\}"):
            cq.cmd_submit(self.ctx, {"command": "mutate.py --part {part}", "root": self.repo, "session": "sess-A"})

    def test_S18_split_without_part_refused(self):
        with self.assertRaisesRegex(cq.CqError, r"\{part\}"):
            cq.cmd_submit(self.ctx, {"command": "go test ./...", "root": self.repo, "session": "sess-A", "split": {"max_parts": 3}})

    def test_validation_errors(self):
        for bad in ({"slots": 5}, {"slots": -1}, {"where": "s22"}, {"priority": "urgent"}, {"subdir": "../x"},
                    {"gets": ["/etc/passwd"]}, {"self_heavy": True, "slots": 0}, {"timeout_minutes": 481},
                    {"where": "laptop", "slots": 2}, {"split": {"max_parts": 9}}, {"source": "not-a-sha!"}):
            with self.assertRaises(cq.CqError, msg=str(bad)):
                cq.cmd_submit(self.ctx, {"command": "echo {part}" if "split" in bad else "echo hi", "root": self.repo, "session": "sess-A", **bad})

    def test_defaults_and_record(self):
        jid = self.submit("go test ./pkg")
        j = self.job(jid)
        self.assertRegex(jid, r"^cq-\d{8}T\d{6}-[0-9a-f]{16}$")
        self.assertEqual((j["slots"], j["est_minutes"], j["priority"], j["where"], j["timeout_minutes"], j["subdir"]),
                         (1, 10, "lane", "host", 30, "."))
        self.assertEqual(j["workstream"], "lane_x")
        self.assertEqual(j["session"], "sess-A")
        self.assertTrue(cq.same_path(j["top"], self.repo))
        self.assertEqual(self.job(self.submit("x", est_minutes=100))["timeout_minutes"], 300)
        self.assertEqual(self.job(self.submit("x", est_minutes=500))["timeout_minutes"], 480)

    def test_position_follows_priority_then_fifo(self):
        a = cq.cmd_submit(self.ctx, {"command": "a", "root": self.repo, "session": "sess-A"})
        self.advance(1)
        b = cq.cmd_submit(self.ctx, {"command": "b", "root": self.repo, "session": "sess-A", "priority": "verify"})
        self.advance(1)
        c = cq.cmd_submit(self.ctx, {"command": "c", "root": self.repo, "session": "sess-A"})
        self.assertEqual((a["position"], b["position"], c["position"]), (1, 1, 3))

    def test_S7_snapshot_is_the_dirty_state_at_submit_and_leaves_the_worktree_alone(self):
        with open(os.path.join(self.repo, "a.txt"), "w") as f:
            f.write("dirty at submit\n")
        with open(os.path.join(self.repo, "new.txt"), "w") as f:
            f.write("untracked\n")
        with open(os.path.join(self.repo, "skip.log"), "w") as f:
            f.write("ignored\n")
        head = sh(self.repo, "rev-parse", "HEAD")
        index_before = open(os.path.join(self.repo, ".git", "index"), "rb").read()
        jid = self.submit("go test ./pkg")
        self.assertEqual(open(os.path.join(self.repo, ".git", "index"), "rb").read(), index_before)
        self.assertEqual(sh(self.repo, "rev-parse", "HEAD"), head)
        status = sh(self.repo, "status", "--porcelain")
        self.assertIn("M a.txt", status)
        self.assertIn("?? new.txt", status)
        self.assertNotIn("A  ", status)  # nothing staged
        j = self.job(jid)
        self.assertTrue(j["snapshot"])
        self.assertNotEqual(j["sha"], head)
        self.assertEqual(sh(self.repo, "show", j["sha"] + ":a.txt"), "dirty at submit")
        self.assertEqual(sh(self.repo, "show", j["sha"] + ":new.txt"), "untracked")
        self.assertNotIn("skip.log", sh(self.repo, "ls-tree", "-r", "--name-only", j["sha"]))
        self.assertEqual(sh(self.repo, "rev-parse", f"refs/cq/{jid}"), j["sha"])
        # the lane edits files after submitting: the job still runs the snapshot
        with open(os.path.join(self.repo, "a.txt"), "w") as f:
            f.write("edited later\n")
        self.assertEqual(sh(self.repo, "show", j["sha"] + ":a.txt"), "dirty at submit")
        # the ref goes when the job ends
        cq.finish(self.ctx, jid, "done", 0)
        r = subprocess.run(["git", "rev-parse", "--verify", "-q", f"refs/cq/{jid}"], cwd=self.repo, capture_output=True)
        self.assertNotEqual(r.returncode, 0)

    def test_clean_worktree_uses_head_and_makes_no_ref(self):
        jid = self.submit("x")
        j = self.job(jid)
        self.assertFalse(j["snapshot"])
        self.assertEqual(j["sha"], sh(self.repo, "rev-parse", "HEAD"))
        self.assertEqual(sh(self.repo, "for-each-ref", "refs/cq"), "")

    def test_source_head_ignores_dirty_files(self):
        with open(os.path.join(self.repo, "a.txt"), "w") as f:
            f.write("dirty\n")
        j = self.job(self.submit("x", source="head"))
        self.assertEqual(j["sha"], sh(self.repo, "rev-parse", "HEAD"))

    def carry_dir(self):
        d = os.path.join(self.repo, ".baton", "remote")
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(self.repo, ".git", "info", "exclude"), "a") as f:
            f.write("/.baton/\n")  # as in the app checkout: outside git, not even listed as untracked
        return d

    def test_baton_remote_is_carried_at_the_same_path_for_a_clean_worktree(self):
        d = self.carry_dir()
        with open(os.path.join(d, "run.sh"), "w", newline="\n") as f:
            f.write("echo hi\n")
        os.makedirs(os.path.join(d, "__pycache__"))
        open(os.path.join(d, "__pycache__", "x.pyc"), "wb").write(b"\0\1")
        open(os.path.join(d, "x.pyc"), "wb").write(b"\0\1")
        head = sh(self.repo, "rev-parse", "HEAD")
        status = sh(self.repo, "status", "--porcelain")
        jid = self.submit("bash .baton/remote/run.sh")
        j = self.job(jid)
        self.assertTrue(j["snapshot"])
        self.assertEqual((j["base"], sh(self.repo, "rev-parse", j["sha"] + "^")), (head, head))
        self.assertEqual(sh(self.repo, "ls-tree", "-r", "--name-only", j["sha"]).split(), [".baton/remote/run.sh", ".gitignore", "a.txt"])
        self.assertEqual(sh(self.repo, "show", j["sha"] + ":.baton/remote/run.sh"), "echo hi")
        # the worktree, HEAD and the branch are untouched: nothing was committed or staged
        self.assertEqual(sh(self.repo, "rev-parse", "HEAD"), head)
        self.assertEqual(sh(self.repo, "status", "--porcelain"), status)
        self.assertEqual(sh(self.repo, "diff", "--cached", "--name-only"), "")
        self.assertEqual(sh(self.repo, "branch", "--contains", j["sha"]), "")

    def test_baton_remote_is_carried_with_source_head_and_a_sha_too(self):
        d = self.carry_dir()
        open(os.path.join(d, "run.sh"), "w").write("echo hi\n")
        with open(os.path.join(self.repo, "a.txt"), "w") as f:
            f.write("dirty\n")
        head = sh(self.repo, "rev-parse", "HEAD")
        for src in ("head", head[:10]):
            j = self.job(self.submit("x", source=src))
            self.assertTrue(j["snapshot"], src)
            self.assertEqual(j["base"], head)
            self.assertEqual(sh(self.repo, "show", j["sha"] + ":a.txt"), "one")  # the dirty file is not part of it
            self.assertEqual(sh(self.repo, "show", j["sha"] + ":.baton/remote/run.sh"), "echo hi")

    def test_no_baton_remote_means_head_itself(self):
        j = self.job(self.submit("x"))
        self.assertEqual((j["sha"], j["base"], j["snapshot"]), (sh(self.repo, "rev-parse", "HEAD"),) * 2 + (False,))

    def test_baton_remote_refuses_a_link_an_oversize_tree_and_a_link_inside(self):
        d = self.carry_dir()
        open(os.path.join(d, "ok.sh"), "w").write("echo\n")
        big = os.path.join(d, "big.bin")
        with open(big, "wb") as f:
            f.write(b"x" * (cq.CARRY_MAX_BYTES + 1))
        with self.assertRaisesRegex(cq.CqError, r"over 2048 KiB"):
            cq.cmd_submit(self.ctx, {"command": "x", "root": self.repo, "session": "sess-A"})
        os.remove(big)
        with open(big, "wb") as f:
            f.write(b"x" * (cq.CARRY_MAX_BYTES - 10))
        self.assertTrue(cq.cmd_submit(self.ctx, {"command": "x", "root": self.repo, "session": "sess-A"})["ok"])  # under the cap in all
        os.remove(big)
        open(os.path.join(d, "link.sh"), "w").write("x")  # symlinks need a privilege on Windows: the link is simulated
        real = os.path.islink
        with mock.patch("os.path.islink", side_effect=lambda p: p.endswith("link.sh") or real(p)):
            with self.assertRaisesRegex(cq.CqError, r"not a regular file"):
                cq.cmd_submit(self.ctx, {"command": "x", "root": self.repo, "session": "sess-A"})
        self.assertEqual(sh(self.repo, "for-each-ref", "refs/cq").count("refs/cq"), 1)  # only the accepted one holds a ref


class LauncherTests(Base):
    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_scripts_parse_under_bash_n(self):
        jid = self.submit("echo 'it''s' \"x\"\nsecond line", gets=["out dir/it's.txt"], slots=2)
        j = self.job(jid)
        j.update(slots_eff=2)
        self.assertNotIn("sleep 1", cq.snapshot_script(False))  # the plain probe takes no per-process pass
        self.assertIn("pstat PA", cq.snapshot_script(True))
        self.assertIn("pstat PB", cq.snapshot_script(True))
        for name, text in (("launcher", cq.make_launcher(j)), ("snapshot", cq.snapshot_script(False)),
                           ("snapshot-profile", cq.snapshot_script(True)), ("fetch", cq.fetch_script(j)),
                           ("kill", cq.host_kill_script(jid))):
            p = os.path.join(self.tmp, name + ".sh")
            with open(p, "w", newline="\n") as f:
                f.write(text)
            r = subprocess.run([BASH, "-n", p], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, f"{name}: {r.stderr}")

    def test_launcher_carries_the_command_as_base64_and_the_atomic_mkdir(self):
        jid = self.submit("echo \"quoted 'x'\" && ls", subdir="go")
        j = self.job(jid)
        j["slots_eff"] = 1
        s = cq.make_launcher(j)
        self.assertIn(base64.b64encode(j["command"].encode()).decode(), s)
        self.assertIn('mkdir "$D"', s)
        self.assertIn("exit 17", s)
        self.assertIn("9>&-", s)  # the lane flock must not stay held by the detached job
        self.assertRegex(s, r"export TMPDIR=%q")
        self.assertIn("export CQ_SHA=%q", s)
        self.assertIn("export CQ_SRC=%q", s)
        self.assertIn(f"{j['sha']}", s)

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_run_sh_writes_the_cpu_seconds_of_the_finished_tree_and_keeps_the_exit(self):
        body = cq.LAUNCHER.split("<<'RUNEOF'\n")[1].split("\nRUNEOF")[0]
        spin = 's=$SECONDS; while [ $((SECONDS - s)) -lt 3 ]; do :; done\n'  # ~3 CPU seconds in the child tree
        for name, cmd, code in (("burn", spin, "0"), ("fail", "exit 3\n", "3")):
            d = os.path.join(self.tmp, name)
            os.makedirs(os.path.join(d, "src"))
            os.makedirs(os.path.join(d, "bin"))
            with open(os.path.join(d, "cmd.sh"), "w", newline="\n") as f:
                f.write(cmd)
            with open(os.path.join(d, "job.env"), "w", newline="\n") as f:
                f.write("SLOTS=3\nSELFHEAVY=0\nTIMEOUT_S=60\nSUBDIR=.\n")
            with open(os.path.join(d, "bin", "heavy"), "w", newline="\n") as f:
                f.write(f'#!/bin/bash\nif [ "$1" = -n ]; then shift 2; fi\nexec "$@"\n')
            os.chmod(os.path.join(d, "bin", "heavy"), 0o755)
            with open(os.path.join(d, "run.sh"), "w", newline="\n") as f:
                f.write(body + "\n")
            unix = lambda p: p.replace("\\", "/")
            env = dict(os.environ, PATH=unix(os.path.join(d, "bin")) + os.pathsep + os.environ["PATH"])
            r = subprocess.run([BASH, unix(os.path.join(d, "run.sh"))], capture_output=True, text=True, env=env, timeout=60)
            self.assertEqual(open(os.path.join(d, "exit")).read().strip(), code, r.stderr)
            cpu = open(os.path.join(d, "cpu")).read().strip()
            self.assertTrue(cpu.isdigit(), cpu)
            if name == "burn":
                self.assertGreaterEqual(int(cpu), 1)  # a `times | awk` pipeline would record 0 here
            self.assertFalse(os.path.exists(os.path.join(d, "cpu.raw")))

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_S15_run_sh_wraps_heavy_unless_self_heavy_or_light(self):
        body = cq.LAUNCHER.split("<<'RUNEOF'\n")[1].split("\nRUNEOF")[0]
        for slots, selfheavy, expect in ((3, 0, "-n 3"), (3, 1, None), (0, 0, None)):
            d = os.path.join(self.tmp, f"d{slots}{selfheavy}")
            os.makedirs(os.path.join(d, "src"))
            os.makedirs(os.path.join(d, "bin"))
            with open(os.path.join(d, "cmd.sh"), "w", newline="\n") as f:
                f.write("echo ran\n")
            with open(os.path.join(d, "job.env"), "w", newline="\n") as f:
                f.write(f"SLOTS={slots}\nSELFHEAVY={selfheavy}\nTIMEOUT_S=60\nSUBDIR=.\n")
            with open(os.path.join(d, "bin", "heavy"), "w", newline="\n") as f:
                f.write(f'#!/bin/bash\necho "HEAVY $@" >> "{d}/heavy.calls"\nif [ "$1" = -n ]; then shift 2; fi\nexec "$@"\n')
            os.chmod(os.path.join(d, "bin", "heavy"), 0o755)
            with open(os.path.join(d, "run.sh"), "w", newline="\n") as f:
                f.write(body + "\n")
            unix = lambda p: p.replace("\\", "/")
            env = dict(os.environ, PATH=unix(os.path.join(d, "bin")) + os.pathsep + os.environ["PATH"])
            r = subprocess.run([BASH, unix(os.path.join(d, "run.sh"))], capture_output=True, text=True, env=env, timeout=60)
            self.assertEqual(open(os.path.join(d, "exit")).read().strip(), "0", r.stderr)
            calls = open(os.path.join(d, "heavy.calls")).read() if os.path.exists(os.path.join(d, "heavy.calls")) else None
            if expect:
                self.assertIn(expect, calls)
            else:
                self.assertIsNone(calls)
            self.assertTrue(os.path.exists(os.path.join(d, "started")))


class PoolTests(Base):
    """The 8-slot pool (one slot = 1.5 vCPU / 2.5 GB) and the measured hard caps on top of the free slots."""

    def setUp(self):
        super().setUp()
        for name, value in (("HOST_SLOTS", 8), ("SAMPLE_MAX_AGE", 120.0)):
            patch = mock.patch.object(cq, name, value)
            patch.start()
            self.addCleanup(patch.stop)
        self.host.slots = 8

    def entries(self, queued):
        return [{"job": j["id"], "where": "host", "slots": j["slots"]} for j in queued]

    def accepted(self, queued, c, now=500.0, mode="haiku"):
        return cq.guard(self.entries(queued) if mode == "haiku" else None, queued, c, now, mode)

    # ---- the load cap: load1 + 1.5 * slots <= nproc

    def test_load_cap_clips_a_start(self):
        c = lambda load: cap(free=8, load=load, mem=22.0, at=500.0)
        ok = self.accepted([J("a", slots=1)], c("10.5 9 9"))  # 10.5 + 1.5 = 12 <= 12
        self.assertEqual([a["job"] for a in ok["accepted"]], ["a"])
        no = self.accepted([J("a", slots=1)], c("10.6 9 9"))
        self.assertEqual(no["accepted"], [])
        self.assertEqual(no["refused"][0]["reason"], "load cap: load 10.6 + 1.5*1 slots > 12")
        two = self.accepted([J("a", slots=2)], c("10.5 9 9"))  # 10.5 + 3 > 12
        self.assertEqual(two["accepted"], [])
        self.assertIn("load cap", two["refused"][0]["reason"])

    def test_load_cap_counts_what_the_plan_already_starts(self):
        queued = [J("a", slots=2), J("b", slots=2), J("c", slots=1), J("d", slots=1), J("e", slots=1), J("f", slots=1)]
        res = self.accepted(queued, cap(free=8, mem=30.0, at=500.0))  # load 0.5: room floor(11.5 / 1.5) = 7 slots
        self.assertEqual([a["job"] for a in res["accepted"]], ["a", "b", "c", "d", "e"])
        self.assertEqual([r["job"] for r in res["refused"]], ["f"])
        self.assertIn("load cap: load 0.5 + 1.5*8 slots > 12", res["refused"][0]["reason"])

    def test_load_cap_counts_slots_launched_since_the_sample(self):
        c = cap(free=8, mem=30.0, committed=5, at=500.0)  # room 7, 5 of it committed: 2 left
        res = self.accepted([J("a", slots=2), J("b", slots=1)], c)
        self.assertEqual([a["job"] for a in res["accepted"]], ["a"])
        self.assertEqual(cq.host_free(c), 2)

    def test_launches_after_the_sample_are_committed_by_adjust_for_launches(self):
        a = self.submit("go test ./pkg", slots=2)
        self.tick()
        base = cq.load_capacity(self.ctx)
        self.advance(1)
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 2})
        c = cq.adjust_for_launches(self.ctx, base)
        self.assertEqual(c["host"]["committed"], 2)
        self.assertEqual(cq.host_free(c), cq.host_free(base) - 2)

    # ---- the RAM floor: mem_avail - 2.5 * slots >= 4

    def test_ram_floor_clips(self):
        c = lambda mem: cap(free=8, mem=mem, at=500.0)
        self.assertEqual(len(self.accepted([J("a", slots=1)], c(6.5))["accepted"]), 1)  # 6.5 - 2.5 = 4
        no = self.accepted([J("a", slots=1)], c(6.4))
        self.assertEqual(no["accepted"], [])
        self.assertEqual(no["refused"][0]["reason"], "RAM floor: 6.4 GB available - 2.5*1 slots < 4 GB")
        self.assertEqual(self.accepted([J("a", slots=2)], c(8.9))["accepted"], [])
        self.assertEqual(len(self.accepted([J("a", slots=2)], c(9.0))["accepted"]), 1)

    # ---- a stale or empty sample is no headroom

    def test_a_stale_sample_clips(self):
        old = cap(free=8, at=500.0 - 121)
        res = self.accepted([J("a", slots=1)], old)
        self.assertEqual(res["accepted"], [])
        self.assertIn("stale", res["refused"][0]["reason"])
        self.assertEqual(len(self.accepted([J("a", slots=1)], cap(free=8, at=500.0 - 120))["accepted"]), 1)
        self.assertEqual(cq.host_free(cq.stamp_capacity(old, 500.0)), 0)

    def test_a_sample_without_load_or_ram_is_no_headroom(self):
        c = cap(free=8, at=500.0)
        c["host"].pop("mem_avail_gb")
        res = self.accepted([J("a", slots=1)], c)
        self.assertEqual(res["accepted"], [])
        self.assertIn("no load/RAM", res["refused"][0]["reason"])
        c = cap(free=8, at=500.0, load="")
        self.assertEqual(self.accepted([J("a", slots=1)], c)["accepted"], [])

    def test_the_tick_uses_the_age_of_the_sample(self):
        a = self.submit("go test ./pkg")
        self.tick()
        self.advance(121)  # the leader did not probe for two minutes
        self.assertEqual(cq.host_free(cq.load_capacity(self.ctx)), 0)
        res = cq.cmd_guard(self.ctx, {"entries": [{"job": a, "where": "host", "slots": 1}]})
        self.assertEqual(res["accepted"], [])
        self.assertIn("stale", res["refused"][0]["reason"])

    # ---- light jobs: load < nproc + 2

    def test_light_jobs_start_under_the_light_cap_only(self):
        light = [J("l", slots=0)]
        self.assertEqual(len(self.accepted(light, cap(free=0, load="13.9 5 5", at=500.0))["accepted"]), 1)
        no = self.accepted(light, cap(free=0, load="14.0 5 5", at=500.0))
        self.assertEqual(no["accepted"], [])
        self.assertEqual(no["refused"][0]["reason"], "light cap: load 14 >= 12 + 2")
        self.assertIn("stale", self.accepted(light, cap(free=0, at=100.0))["refused"][0]["reason"])
        # a heavy job at load 13.9 is clipped, a light one is not
        mixed = self.accepted([J("h", slots=1), J("l", slots=0)], cap(free=8, load="13.9 5 5", at=500.0))
        self.assertEqual([a["job"] for a in mixed["accepted"]], ["l"])

    def test_the_fallback_starts_nothing_on_a_loaded_host_and_logs_why(self):
        queued = [J("a", slots=1), J("l", slots=0)]
        res = self.accepted(queued, cap(free=8, load="14.5 5 5", at=500.0), mode="fallback")
        self.assertEqual(res["accepted"], [])
        reasons = {r["job"]: r["reason"] for r in res["refused"]}
        self.assertIn("load cap", reasons["a"])
        self.assertIn("light cap", reasons["l"])

    # ---- the reservation head obeys the caps

    def test_the_reservation_head_waits_on_the_caps_and_holds_the_host(self):
        head = J("head", slots=4, t_submitted=1000.0)
        small = [J("s1", slots=1, t_submitted=1001.0), J("s2", slots=1, t_submitted=1002.0)]
        c = cap(free=8, load="8 5 5", mem=30.0, at=500.0)  # slots are free, the load leaves room for 2
        self.assertEqual(cq.host_free(c), 2)
        res = cq.reservations([head, *small], c)
        self.assertTrue(res["host"]["blocked"])
        self.assertEqual(res["host"]["job"]["id"], "head")
        out = cq.guard(self.entries(small), [head, *small], c, 500.0)
        self.assertEqual(out["accepted"], [])  # no small job slips past the waiting head
        self.assertTrue(all(r["reason"].startswith("reserved for head") for r in out["refused"]))
        self.assertEqual(cq.fallback_entries([head, *small], c), [])
        light = cq.guard([{"job": "l", "where": "host", "slots": 0}], [head, J("l", slots=0)], c, 500.0)
        self.assertEqual(len(light["accepted"]), 1)  # light jobs are exempt from the reservation
        # the load drops: the head starts, and takes its slots first
        c2 = cap(free=8, load="3 3 3", mem=30.0, at=500.0)
        out = cq.guard(self.entries(small), [head, *small], c2, 500.0)
        self.assertIn("head", [a["job"] for a in out["accepted"]])

    def test_a_head_job_the_caps_could_never_admit_holds_nothing(self):
        big = J("big", slots=8, t_submitted=1000.0)  # 8 slots = 20 GB + 4 GB floor > the idle RAM
        small = J("s", slots=1, t_submitted=1001.0)
        c = cap(free=8, mem=21.0, at=500.0)
        self.assertEqual(cq.host_max_slots(c["host"]), 6)  # (21 - 4) / 2.5
        head = cq.reservations([big, small], c)["host"]
        self.assertEqual((head["job"]["id"], head["blocked"]), ("s", False))  # big is no candidate; s fits
        self.assertEqual([a["job"] for a in self.accepted([small], c)["accepted"]], ["s"])

    def test_the_max_slots_come_from_the_idle_ram_baseline_not_memtotal(self):
        h = cap(mem=12.0)["host"]
        h["mem_total_gb"] = 22.0
        self.assertEqual(cq.host_max_slots(h), 3)  # no history: the current MemAvailable, (12 - 4) / 2.5
        h["mem_idle_gb"] = 19.5
        self.assertEqual(cq.host_max_slots(h), 6)  # the 24 h maximum wins over the current sample

    def test_submit_refuses_slots_above_the_idle_baseline_of_the_last_24_hours(self):
        self.tick()  # MemAvailable 19.1 GB: (19.1 - 4) / 2.5 = 6
        self.submit("go test ./...", slots=6)
        with self.assertRaises(cq.CqError) as cm:
            self.submit("go test ./...", slots=7)
        self.assertIn("at most 6", str(cm.exception))
        self.advance(3600 * 5)
        self.host.mem_kb = 12 * 2**20
        self.tick()  # the busy hour does not lower a baseline still inside the 24 h window
        self.assertEqual(cq.load_capacity(self.ctx)["host"]["mem_idle_gb"], 19.1)
        self.submit("go test ./...", slots=6)
        self.advance(3600 * 25)
        self.tick()  # 25 h later the high samples are out of the window: the baseline is the 12 GB seen since
        self.assertEqual(cq.load_capacity(self.ctx)["host"]["mem_idle_gb"], 12.0)
        with self.assertRaises(cq.CqError) as cm:
            self.submit("go test ./...", slots=4)
        self.assertIn("at most 3", str(cm.exception))

    # ---- a head job the caps keep out of an idle host starts anyway

    def stuck(self, **kw):
        """6 slots on a 17 GB host: the RAM floor leaves room for 5 (13 / 2.5), the head fits the free slots."""
        c = cap(free=8, mem=17.0, at=500.0, idle=2, **kw)
        c["host"]["total"] = 8
        c["host"]["mem_idle_gb"] = 19.5  # seen when the host was idle: 6 slots are admissible then
        return c

    def test_caps_override_starts_the_head_when_no_heavy_job_runs_for_two_samples(self):
        head = J("head", slots=6, t_submitted=1000.0)
        small = J("s", slots=1, t_submitted=1001.0)
        c = self.stuck()
        self.assertEqual(cq.host_free(c), 5)
        res = cq.reservations([head, small], c)["host"]
        self.assertEqual((res["job"]["id"], res["blocked"], res.get("override")), ("head", False, True))
        out = cq.guard(self.entries([head, small]), [head, small], c, 500.0)
        self.assertEqual([(a["job"], a.get("caps_override")) for a in out["accepted"]], [("head", "caps override: host idle")])
        fb = cq.guard(None, [head, small], c, 500.0, "fallback")
        self.assertEqual([a["job"] for a in fb["accepted"]], ["head"])  # the fallback starts it too, and only it
        self.assertEqual(fb["accepted"][0]["caps_override"], "caps override: host idle")
        named = cq.guard([{"job": "s", "where": "host", "slots": 1}], [head, small], c, 500.0)
        self.assertEqual(named["accepted"], [])  # the small job may not take the head's slots; apply then runs the fallback
        self.assertIn("reserved for the head job", named["refused"][0]["reason"])

    def test_caps_override_needs_two_idle_samples_nothing_else_heavy_a_fresh_sample_and_free_slots(self):
        head = J("head", slots=6, t_submitted=1000.0)
        for name, c in (("one sample", cap(free=8, mem=17.0, at=500.0, idle=1)),
                        ("a heavy job holds a slot", cap(free=7, held=1, mem=17.0, at=500.0, idle=2)),
                        ("a launch since the sample", cap(free=8, mem=17.0, at=500.0, idle=2, committed=1)),
                        ("a stale sample", cap(free=8, mem=17.0, at=100.0, idle=2)),
                        ("the slots are not free", cap(free=5, held=3, mem=17.0, at=500.0, idle=2))):
            c["host"]["total"], c["host"]["mem_idle_gb"] = 8, 19.5
            res = cq.guard(self.entries([head]), [head], c, 500.0)
            self.assertEqual(res["accepted"], [], name)
        # a head within the caps needs no override and carries no mark
        ok = cq.guard(self.entries([J("h", slots=2)]), [J("h", slots=2)], self.stuck(), 500.0)
        self.assertNotIn("caps_override", ok["accepted"][0])

    def test_caps_override_for_a_split_head_runs_one_part(self):
        head = J("head", slots=6, split={"max_parts": 4}, t_submitted=1000.0)
        out = cq.guard(self.entries([head]), [head], self.stuck(), 500.0)
        self.assertEqual([(a["job"], a["parts"]) for a in out["accepted"]], [("head", 1)])
        self.assertEqual(out["accepted"][0]["caps_override"], "caps override: host idle")

    def test_probe_counts_consecutive_idle_samples_and_apply_logs_the_override(self):
        self.tick()  # MemAvailable 19.1 GB sets the baseline: 6 slots can be admitted at all
        a = self.submit("go test ./...", slots=6)
        self.host.mem_kb = 17 * 2**20  # 17 GB available: 5 slots by the RAM floor
        self.advance(15)
        t = self.tick()
        self.assertEqual(t["capacity"]["host"]["idle_samples"], 2)
        self.assertEqual(t["capacity"]["host"]["room"], 5)
        self.assertTrue(t["can_start"])
        r = cq.cmd_apply(self.ctx, {"plan": None})
        self.assertEqual([x["job"] for x in r["accepted"]], [a])
        self.assertEqual(self.job(a)["status"], "running")
        rows = [d for d in self.decisions() if d.get("kind") == "caps_override"]
        self.assertEqual([(d["job"], d["note"]) for d in rows], [(a, "caps override: host idle")])
        self.assertEqual(rows[0]["idle_samples"], 2)
        self.host.held = 1  # a heavy job now: the counter restarts
        self.advance(15)
        self.assertEqual(self.tick()["capacity"]["host"]["idle_samples"], 0)

    def test_a_held_heavy_slot_resets_the_idle_count_so_the_tick_waits(self):
        self.tick()
        a = self.submit("go test ./...", slots=6)
        self.host.mem_kb = 17 * 2**20
        self.host.held = 1  # another lane's heavy job holds .heavy.0
        self.advance(15)
        t = self.tick()
        self.assertEqual(t["capacity"]["host"]["idle_samples"], 0)
        self.assertFalse(t["can_start"])  # 6 slots fit the 7 free, the caps leave 5: it waits for the host to idle
        self.assertEqual(self.job(a)["status"], "queued")

    # ---- the 8-slot pool

    def test_the_pool_has_eight_slots_and_the_probe_counts_all_of_them(self):
        s = cq.SNAPSHOT_SCRIPT
        self.assertIn("N=8", s)
        self.assertIn("MEMTOTAL", s)
        self.host.slots = None  # the host says nothing: HOST_SLOTS applies
        self.assertEqual(cq.parse_snapshot("PROBE\nEND\n")["slots"], 8)
        self.host.slots = 8
        self.host.held = 3
        h = self.tick()["capacity"]["host"]
        self.assertEqual((h["total"], h["held"], h["free"]), (8, 3, 5))

    def test_one_and_two_slot_jobs_fill_the_eight_slots(self):
        queued = [J(f"j{i}", slots=1 + i % 2, t_submitted=1000.0 + i) for i in range(6)]  # 1,2,1,2,1,2 = 9 slots
        res = self.accepted(queued, cap(free=8, mem=40.0, load="0 0 0", at=500.0))
        self.assertEqual([a["job"] for a in res["accepted"]], ["j0", "j1", "j2", "j3", "j4"])  # 1+2+1+2+1 = 7 slots; j5 needs 2, 1 left
        self.assertEqual(res["refused"][0]["job"], "j5")
        self.assertIn("2 slots, 1 free", res["refused"][0]["reason"])
        fb = cq.fallback_entries(queued, cap(free=8, mem=40.0, load="0 0 0", at=500.0))
        self.assertEqual([e["job"] for e in fb], ["j0", "j1", "j2", "j3", "j4"])

    def test_a_split_job_takes_parts_within_the_caps(self):
        j = J("s", slots=1, split={"max_parts": 8})
        res = self.accepted([j], cap(free=8, mem=11.0, at=500.0))  # RAM floor: (11 - 4) / 2.5 = 2 slots
        self.assertEqual(res["accepted"][0]["parts"], 2)

    def test_job_env_carries_HEAVY_SLOTS_8_and_run_sh_exports_it_to_heavy(self):
        jid = self.submit("go test ./pkg", slots=2)
        j = self.job(jid)
        j["slots_eff"] = 2
        s = cq.make_launcher(j)
        self.assertIn("printf 'export HEAVY_SLOTS=%q\\n' 8\n", s)
        self.assertIn("export CQ_DOCPARSE_PYTHON=%q", s)  # the host's frozen interpreter, when it exists
        if not HAVE_BASH:
            return
        body = cq.LAUNCHER.split("<<'RUNEOF'\n")[1].split("\nRUNEOF")[0]
        d = os.path.join(self.tmp, "hs")
        os.makedirs(os.path.join(d, "src"))
        os.makedirs(os.path.join(d, "bin"))
        line = [l for l in cq.LAUNCHER.splitlines() if "HEAVY_SLOTS" in l][0].replace("@HEAVY_SLOTS@", "8")
        unix = lambda p: p.replace("\\", "/")
        env_text = subprocess.run([BASH, "-c", line.strip()], capture_output=True, text=True).stdout
        self.assertEqual(env_text.strip(), "export HEAVY_SLOTS=8")
        with open(os.path.join(d, "cmd.sh"), "w", newline="\n") as f:
            f.write("echo ran\n")
        with open(os.path.join(d, "job.env"), "w", newline="\n") as f:
            f.write(env_text + "SLOTS=2\nSELFHEAVY=0\nTIMEOUT_S=60\nSUBDIR=.\n")
        with open(os.path.join(d, "bin", "heavy"), "w", newline="\n") as f:
            f.write(f'#!/bin/bash\necho "HEAVY_SLOTS=$HEAVY_SLOTS $@" >> "{unix(d)}/heavy.calls"\nif [ "$1" = -n ]; then shift 2; fi\nexec "$@"\n')
        os.chmod(os.path.join(d, "bin", "heavy"), 0o755)
        with open(os.path.join(d, "run.sh"), "w", newline="\n") as f:
            f.write(body + "\n")
        env = dict(os.environ, PATH=unix(os.path.join(d, "bin")) + os.pathsep + os.environ["PATH"])
        env.pop("HEAVY_SLOTS", None)
        subprocess.run([BASH, unix(os.path.join(d, "run.sh"))], capture_output=True, text=True, env=env, timeout=60)
        self.assertIn("HEAVY_SLOTS=8 -n 2", open(os.path.join(d, "heavy.calls")).read())


class DispatchTests(Base):
    def probe(self):
        return self.tick()

    def test_apply_dispatches_a_haiku_plan_and_logs_it(self):
        a = self.submit("go test ./pkg")
        self.probe()
        r = cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": a, "where": "host", "slots": 1}], "why": "only job"}})
        self.assertEqual(r["mode"], "haiku")
        self.assertEqual(self.job(a)["status"], "running")
        (jid, argv, cwd, script), = self.host.launches
        self.assertEqual(argv[:4], ["--rev", self.job(a)["sha"], "--subdir", "."])
        self.assertTrue(cq.same_path(cwd, self.repo))
        d = self.decisions()[-1]
        self.assertEqual((d["mode"], d["why"]), ("haiku", "only job"))

    def test_the_tick_reply_carries_the_planner(self):
        self.submit()
        self.assertEqual(self.tick()["planner"], cq.PLANNER)
        with mock.patch.object(cq, "PLANNER", "haiku"):
            self.assertEqual(self.tick()["planner"], "haiku")

    def test_planner_code_starts_the_head_job_that_fits_with_no_plan_and_logs_mode_code(self):
        a = self.submit("go test ./pkg")
        self.probe()
        r = cq.cmd_apply(self.ctx, {"planner": "code"})
        self.assertEqual((r["mode"], [x["job"] for x in r["accepted"]], r["refused"]), ("code", [a], []))
        self.assertEqual(self.job(a)["status"], "running")
        (jid, argv, cwd, script), = self.host.launches
        d = self.decisions()[-1]
        self.assertEqual((d["kind"], d["mode"], d["why"], d["note"]), ("plan", "code", "", ""))
        self.assertEqual([x["job"] for x in d["accepted"]], [a])
        self.assertNotIn("usage", d)

    def test_planner_code_writes_no_row_when_nothing_can_start(self):
        a = self.submit("go test ./pkg")
        self.host.held = 4  # every host slot held: the job waits, nothing is accepted or refused
        self.probe()
        before = len(self.decisions())
        r = cq.cmd_apply(self.ctx, {"planner": "code"})
        self.assertEqual((r["mode"], r["accepted"], r["refused"]), ("code", [], []))
        self.assertEqual(self.job(a)["status"], "queued")
        self.assertEqual(len(self.decisions()), before)

    def test_a_model_call_that_starts_nothing_logs_its_usage_and_one_without_usage_logs_nothing(self):
        self.submit("go test ./pkg")  # queued just now: not waiting long enough for the idle override
        self.probe()
        before = len(self.decisions())
        usage = {"input_tokens": 1200, "output_tokens": 40}
        r = cq.cmd_apply(self.ctx, {"plan": {"start": [], "why": "wait"}, "usage": usage})
        self.assertEqual((r["mode"], r["accepted"], r["refused"]), ("haiku", [], []))
        self.assertEqual(len(self.decisions()), before + 1)
        d = self.decisions()[-1]
        self.assertEqual((d["kind"], d["mode"], d["accepted"], d["usage"]), ("plan", "haiku", [], usage))
        cq.cmd_apply(self.ctx, {"plan": {"start": [], "why": "wait"}})
        self.assertEqual(len(self.decisions()), before + 1)

    def test_S1_unusable_haiku_reply_runs_the_fallback(self):
        a = self.submit()
        self.probe()
        r = cq.cmd_apply(self.ctx, {"plan": None, "reason": "model call failed: aborted"})
        self.assertEqual(r["mode"], "fallback")
        self.assertEqual(self.job(a)["status"], "running")
        d = self.decisions()[-1]
        self.assertEqual(d["mode"], "fallback")
        self.assertIn("aborted", d["note"])

    def test_S1_every_entry_refused_runs_the_fallback(self):
        a = self.submit()
        self.probe()
        r = cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": "ghost", "where": "host", "slots": 1}], "why": "x"}})
        self.assertEqual(r["mode"], "fallback")
        self.assertEqual(self.job(a)["status"], "running")
        self.assertEqual(self.decisions()[-1]["refused"][0]["job"], "ghost")

    def test_S3_partial_refusal_keeps_the_job_queued_total_refusal_falls_back(self):
        a = self.submit("a")
        b = self.submit("b")
        self.probe()
        plan = {"start": [{"job": a, "where": "host", "slots": 1}, {"job": b, "where": "laptop", "slots": 1}], "why": "x"}
        r = cq.cmd_apply(self.ctx, {"plan": plan})
        self.assertEqual(r["mode"], "haiku")
        self.assertEqual((self.job(a)["status"], self.job(b)["status"]), ("running", "queued"))
        # only the refused entry: every entry refused -> the fallback places it on the host
        r = cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": b, "where": "laptop", "slots": 1}], "why": "x"}})
        self.assertEqual(r["mode"], "fallback")
        self.assertEqual(self.job(b)["status"], "running")
        self.assertEqual(self.job(b)["placed_where"], "host")

    def test_haiku_starting_nothing_is_honoured_then_overridden_after_two_minutes(self):
        a = self.submit()
        self.probe()
        r = cq.cmd_apply(self.ctx, {"plan": {"start": [], "why": "wait"}})
        self.assertEqual((r["mode"], r["accepted"]), ("haiku", []))
        self.assertEqual(self.job(a)["status"], "queued")
        self.advance(150)
        self.probe()
        r = cq.cmd_apply(self.ctx, {"plan": {"start": [], "why": "wait"}})
        self.assertEqual(r["mode"], "fallback")
        self.assertEqual(self.job(a)["status"], "running")

    def test_S5_two_leaders_one_dispatch_no_double_run(self):
        a = self.submit()
        self.probe()
        plan = {"start": [{"job": a, "where": "host", "slots": 1}], "why": "x"}
        r1 = cq.cmd_apply(self.ctx, plan)
        r2 = cq.cmd_apply(self.ctx, plan)
        self.assertEqual(len(r1["accepted"]), 1)
        self.assertEqual(r2["accepted"], [])  # no longer queued
        self.assertEqual(len(self.host.launches), 1)
        # a second dispatcher that raced past the lock: the host mkdir answers 17 -> already dispatched, not a failure
        self.host.jobs.pop(a)
        b = self.submit()
        self.host.jobs[b] = dict(exit=None, started=None, launched=1, finished=None, alive=True, src=True)
        self.ctx.sync = True
        cq.dispatch(self.ctx, {"job": b, "where": "host", "slots": 1})
        cq.launch(self.ctx, b)
        self.assertEqual(self.job(b)["status"], "running")
        self.assertEqual(self.host.launches[-1][0], b)

    def test_S5_dispatch_of_a_job_that_is_not_queued_is_refused(self):
        a = self.submit()
        self.assertTrue(cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})["ok"])
        self.assertFalse(cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})["ok"])

    def test_S5_only_one_session_leads(self):
        r1 = self.tick("sess-A")
        r2 = self.tick("sess-B")
        self.assertTrue(r1["leader"])
        self.assertFalse(r2["leader"])
        self.assertTrue(self.tick("sess-A")["leader"])

    def test_S6_S13_leader_ends_mid_job_new_leader_reaps_and_the_cycled_session_is_notified(self):
        a = self.submit("go test ./pkg", label="the job")
        self.tick("sess-A")
        cq.cmd_apply(self.ctx, {"plan": None})
        self.assertEqual(self.job(a)["status"], "running")
        cq.cmd_leader(self.ctx, {"session": "sess-A", "root": self.repo, "release": True})
        self.host.tails[a] = ["PASS", "ok  pkg 1.2s"]
        self.host.set_done(a, 0)
        self.advance(5)
        cq.write_json(os.path.join(self.repo, ".baton", "owner.json"), {"vendor": "claude", "session_id": "sess-B"})
        t = self.tick("sess-B")  # the new leader, within the minute
        self.assertTrue(t["leader"])
        self.assertEqual(self.job(a)["status"], "done")
        self.assertIn(a, t["ended"])
        # session A was cycled: a new session id in the same worktree gets the notice
        w = t["watch"]["to_notify"]
        self.assertEqual([x["id"] for x in w], [a])
        self.assertEqual(w[0]["tail"], ["PASS", "ok  pkg 1.2s"])
        cq.cmd_notified(self.ctx, {"ids": [a]})
        self.assertEqual(self.tick("sess-B")["watch"]["to_notify"], [])

    def test_watch_counts_only_undelivered_failures_and_missing_artifacts_as_attention(self):
        failed, missing, fine = self.submit("go test ./a"), self.submit("go test ./b"), self.submit("go test ./c")
        for jid, status, extra in ((failed, "failed", {}), (missing, "done", {"artifact_missing": [{"path": "x"}]}), (fine, "done", {})):
            job = self.job(jid)
            job.update(status=status, exit=1 if status == "failed" else 0, **extra)
            cq.save_job(self.ctx, job)
        watch = cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")
        self.assertEqual(watch["mine_attention"], {"failed": 1, "missing": 1})
        # a result the owner has been handed no longer asks for attention
        cq.cmd_notified(self.ctx, {"ids": [failed]})
        self.assertEqual(cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")["mine_attention"], {"failed": 0, "missing": 1})
        cq.cmd_notified(self.ctx, {"ids": [missing, fine]})
        self.assertEqual(cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")["mine_attention"], {"failed": 0, "missing": 0})
        # nor does a session that is not the worktree's owner see them (it could not deliver them)
        job = self.job(failed)
        job.update(notified=False)
        cq.save_job(self.ctx, job)
        self.assertEqual(cq.watch(self.ctx, self.repo, self.clock[0], "sess-other")["mine_attention"], {"failed": 0, "missing": 0})

    def test_vendor_handoff_leaves_completion_for_current_codex_owner(self):
        job_id = self.submit("go test ./pkg")
        job = self.job(job_id)
        job.update(status="done", exit=0)
        cq.save_job(self.ctx, job)
        self.assertEqual([j["id"] for j in cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")["to_notify"]], [job_id])
        cq.write_json(os.path.join(self.repo, ".baton", "owner.json"), {"vendor": "codex", "session_id": "new-owner"})
        self.assertEqual(cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")["to_notify"], [])
        self.assertFalse(self.job(job_id)["notified"])

    def test_claude_handoff_routes_to_current_owner_and_preserves_unacknowledged_job(self):
        job_id = self.submit("go test ./pkg")
        job = self.job(job_id)
        job.update(status="done", exit=0)
        cq.save_job(self.ctx, job)
        cq.write_json(os.path.join(self.repo, ".baton", "owner.json"), {"vendor": "claude", "session_id": "sess-B"})
        self.assertEqual(cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")["to_notify"], [])
        self.assertEqual([j["id"] for j in cq.watch(self.ctx, self.repo, self.clock[0], "sess-B")["to_notify"]], [job_id])
        self.assertFalse(cq.cmd_notice_authorize(self.ctx, {"root": self.repo, "session": "sess-A", "ids": [job_id]})["ok"])
        self.assertFalse(cq.cmd_notified(self.ctx, {"ids": [job_id], "owner_root": self.repo, "owner_session": "sess-A"})["ok"])
        self.assertFalse(self.job(job_id)["notified"])
        allowed = cq.cmd_notice_authorize(self.ctx, {"root": self.repo, "session": "sess-B", "ids": [job_id]})
        self.assertEqual([j["id"] for j in allowed["jobs"]], [job_id])
        self.assertTrue(cq.cmd_notified(self.ctx, {"ids": [job_id], "owner_root": self.repo, "owner_session": "sess-B"})["ok"])
        self.assertTrue(self.job(job_id)["notified"])

    def test_reused_worktree_path_holds_old_completion_without_marking_it(self):
        job_id = self.submit("go test ./pkg")
        job = self.job(job_id)
        self.assertEqual(job["worktree_identity"], cq.worktree_identity(self.repo))
        job.update(status="done", exit=0)
        job["worktree_identity"]["inode"] += 1  # old incarnation of the same path
        cq.save_job(self.ctx, job)
        self.assertEqual(cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")["to_notify"], [])
        allowed = cq.cmd_notice_authorize(self.ctx, {"root": self.repo, "session": "sess-A", "ids": [job_id]})
        self.assertEqual(allowed["jobs"], [])
        ack = cq.cmd_notified(self.ctx, {"ids": [job_id], "owner_root": self.repo, "owner_session": "sess-A"})
        self.assertFalse(ack["ok"])
        self.assertFalse(self.job(job_id)["notified"])
        self.assertIn("worktree identity unverified or changed", cq.status_row(self.job(job_id), self.clock[0])["notice_hold_reason"])

    def test_legacy_job_older_than_directory_birth_is_held(self):
        job_id = self.submit("go test ./pkg")
        job = self.job(job_id)
        job.pop("worktree_identity")
        job.update(status="done", exit=0)
        cq.save_job(self.ctx, job)
        fake = {"device": 1, "inode": 2, "birth_ns": int((job["t_submitted"] + 60) * 1e9)}
        with mock.patch.object(cq, "worktree_identity", return_value=fake):
            self.assertEqual(cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")["to_notify"], [])
            self.assertFalse(self.job(job_id)["notified"])

    def test_submission_fails_when_worktree_identity_unavailable_or_changes(self):
        original = cq.worktree_identity(self.repo)
        for identities in ([None], [original, {**original, "inode": original["inode"] + 1}]):
            with self.subTest(identities=identities), mock.patch.object(cq, "worktree_identity", side_effect=identities):
                before = len(cq.all_jobs(self.ctx))
                with self.assertRaises(cq.CqError):
                    self.submit("go test ./pkg")
                self.assertEqual(len(cq.all_jobs(self.ctx)), before)

    def test_legacy_active_job_remains_visible_after_handoff(self):
        job_id = self.submit("go test ./pkg")
        job = self.job(job_id)
        job.pop("worktree_identity")
        job["status"] = "running"
        cq.save_job(self.ctx, job)
        cq.write_json(os.path.join(self.repo, ".baton", "owner.json"), {"vendor": "claude", "session_id": "sess-B"})
        watch = cq.watch(self.ctx, self.repo, self.clock[0], "sess-B")
        self.assertEqual(watch["mine_running"], 1)
        self.assertEqual(watch["mine_unverified"], 1)
        self.assertEqual(watch["to_notify"], [])

    def test_legacy_completion_is_held_after_session_handoff(self):
        job_id = self.submit("go test ./pkg")
        job = self.job(job_id)
        job.pop("worktree_identity")
        job.update(status="done", exit=0)
        cq.save_job(self.ctx, job)
        self.assertEqual([j["id"] for j in cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")["to_notify"]], [job_id])
        cq.write_json(os.path.join(self.repo, ".baton", "owner.json"), {"vendor": "claude", "session_id": "sess-B"})
        with mock.patch.object(cq, "worktree_identity", return_value={"device": 1, "inode": 2, "birth_ns": None}):
            self.assertEqual(cq.watch(self.ctx, self.repo, self.clock[0], "sess-B")["to_notify"], [])
            self.assertEqual(cq.cmd_notice_authorize(self.ctx, {"root": self.repo, "session": "sess-B", "ids": [job_id]})["jobs"], [])
            self.assertFalse(cq.cmd_notified(self.ctx, {"ids": [job_id], "owner_root": self.repo, "owner_session": "sess-B"})["ok"])
            self.assertIn("identity unverified", cq.status_row(self.job(job_id), self.clock[0])["notice_hold_reason"])
        self.assertFalse(self.job(job_id)["notified"])

    def test_real_directory_recreation_changes_worktree_identity(self):
        path = os.path.join(self.tmp, "identity-test")
        os.mkdir(path)
        identity = cq.worktree_identity(path)
        job = {"top": path, "worktree_identity": identity}
        self.assertTrue(cq.notice_matches_worktree(job, path))
        os.rename(path, path + "-old")
        os.mkdir(path)
        self.assertNotEqual(identity, cq.worktree_identity(path))
        self.assertFalse(cq.notice_matches_worktree(job, path))

    def test_unknown_owner_and_invalid_notice_ids_are_not_delivered(self):
        job_id = self.submit("go test ./pkg")
        job = self.job(job_id)
        job.update(status="done", exit=0)
        cq.save_job(self.ctx, job)
        os.remove(os.path.join(self.repo, ".baton", "owner.json"))
        self.assertEqual(cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")["to_notify"], [])
        cq.write_json(os.path.join(self.repo, ".baton", "owner.json"), {"vendor": "claude", "session_id": "sess-A"})
        self.assertFalse(cq.cmd_notice_authorize(self.ctx, {"root": self.repo, "session": "sess-A", "ids": ["../bad"]})["ok"])

    def test_watch_uses_current_session_even_when_leader_identity_is_old(self):
        job_id = self.submit("go test ./pkg")
        job = self.job(job_id)
        job.update(status="done", exit=0)
        cq.save_job(self.ctx, job)
        cq.write_json(os.path.join(self.repo, ".baton", "owner.json"), {"vendor": "claude", "session_id": "sess-B"})
        t = cq.cmd_tick(self.ctx, {"session": "sess-A", "watch_session": "sess-B", "root": self.repo})
        self.assertEqual([j["id"] for j in t["watch"]["to_notify"]], [job_id])

    def test_S6_stale_heartbeat_hands_over_after_45_seconds(self):
        self.tick("sess-A")
        self.advance(30)
        self.assertFalse(self.tick("sess-B")["leader"])
        self.advance(20)  # 50 s since A's last heartbeat
        r = self.tick("sess-B")
        self.assertTrue(r["leader"])
        self.assertTrue(r["took_over"])

    def test_S8_split_job_four_parts_one_notice_with_the_worst_exit(self):
        a = self.submit("python mutate.py --part {part}", split={"max_parts": 4}, label="mut")
        self.tick()
        r = cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": a, "where": "host", "slots": 1, "parts": 4}], "why": "split"}})
        self.assertEqual(len(r["accepted"]), 1)
        parent = self.job(a)
        self.assertEqual(len(parent["children"]), 4)
        cmds = sorted(self.job(c)["command"] for c in parent["children"])
        self.assertEqual(cmds, [f"python mutate.py --part {k}/4" for k in range(1, 5)])
        self.assertEqual(len(self.host.launches), 4)
        for k, c in enumerate(parent["children"]):
            self.host.tails[c] = [f"part {k + 1} line"]
            self.host.set_done(c, 0 if k != 2 else 3)
        t = self.tick()
        parent = self.job(a)
        self.assertEqual((parent["status"], parent["exit"]), ("failed", 3))
        self.assertEqual(len(parent["parts_info"]), 4)
        self.assertLessEqual(len(parent["tail"]), 40)
        notes = t["watch"]["to_notify"]
        self.assertEqual([n["id"] for n in notes], [a])  # one notice, for the parent
        self.assertEqual(len(notes[0]["parts_info"]), 4)
        self.assertTrue(all(i["log"] for i in notes[0]["parts_info"]))

    def test_S8_parts_all_done_means_done_exit_zero(self):
        a = self.submit("x {part}", split={"max_parts": 2})
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": a, "where": "host", "slots": 1, "parts": 2}]}})
        for c in self.job(a)["children"]:
            self.host.set_done(c, 0)
        self.tick()
        self.assertEqual((self.job(a)["status"], self.job(a)["exit"]), ("done", 0))

    def test_S9_host_unreachable_jobs_stay_queued_and_the_leader_backs_off(self):
        a = self.submit()
        self.host.up = False
        t = self.tick()
        self.assertFalse(t["capacity"]["host"]["up"])
        self.assertIn("down_since", t["capacity"]["host"])
        self.assertFalse(t["can_start"])
        r = cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": a, "where": "host", "slots": 1}]}})
        self.assertEqual(self.job(a)["status"], "queued")
        self.advance(30)
        self.tick()  # the first failure re-probed on this tick (15 s later in the leader); this second failure backs off
        calls = len(self.host.calls)
        self.advance(30)
        self.tick()
        self.assertEqual(len(self.host.calls), calls)  # backing off: no ssh within 60 s
        self.advance(40)
        self.host.up = True
        t = self.tick()
        self.assertTrue(t["capacity"]["host"]["up"])
        self.assertTrue(t["can_start"])

    def test_S9_first_failed_probe_after_a_good_one_reprobes_next_tick_and_a_second_backs_off_60_s(self):
        self.assertTrue(cq.cmd_probe(self.ctx, {})["capacity"]["host"]["up"])
        self.host.up = False
        self.assertFalse(cq.cmd_probe(self.ctx, {})["capacity"]["host"]["up"])
        hs = cq.host_state(self.ctx)
        self.assertIn("down_since", hs)
        self.assertLessEqual(hs["next_probe"], self.clock[0])  # the first failure: the next tick probes again
        self.advance(15)
        calls = len(self.host.calls)
        cq.cmd_probe(self.ctx, {})
        self.assertEqual(len(self.host.calls), calls + 1)
        self.assertEqual(cq.host_state(self.ctx)["next_probe"], self.clock[0] + 60)  # already down: this one backs off
        self.advance(30)
        calls = len(self.host.calls)
        cq.cmd_probe(self.ctx, {})
        self.assertEqual(len(self.host.calls), calls)

    def test_S9_laptop_eligible_job_runs_on_the_laptop_while_the_host_is_down(self):
        a = self.submit("echo hi", where="any", slots=0)
        self.host.up = False
        t = self.tick()
        self.assertTrue(t["can_start"])
        r = cq.cmd_apply(self.ctx, {"plan": None, "reason": "x"})
        self.assertEqual(r["accepted"][0]["where"], "laptop")
        self.assertEqual(self.job(a)["placed_where"], "laptop")

    def test_S9_launch_fails_on_a_dead_host_requeues_then_fails_after_three_attempts(self):
        a = self.submit()
        self.tick()
        self.host.up = False
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
        cq.launch(self.ctx, a)
        self.assertEqual(self.job(a)["status"], "queued")
        self.assertIn("launch failed", self.job(a)["reason"])
        self.assertLessEqual(cq.host_state(self.ctx)["next_probe"], self.clock[0])  # a dropped connection: re-probe next tick
        self.assertIn("down_since", cq.host_state(self.ctx))
        for _ in range(2):
            cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
            cq.launch(self.ctx, a)
        self.assertEqual(self.job(a)["status"], "failed")

    def test_S9_a_push_failure_on_a_reachable_host_keeps_the_60_s_pause(self):
        a = self.submit()
        self.tick()
        self.host.remoterun_rc, self.host.remoterun_err = 1, "error: cannot lock ref 'refs/heads/x'\n[remoterun] push failed"
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
        cq.launch(self.ctx, a)
        self.assertEqual(self.job(a)["status"], "queued")
        self.assertEqual(cq.host_state(self.ctx)["next_probe"], self.clock[0] + 60)

    def test_S10_disk_low_fails_the_job_pauses_the_host_and_alerts_once(self):
        a = self.submit()
        b = self.submit("other")
        self.tick()
        self.host.remoterun_rc = 75
        self.host.remoterun_err = "[lanerun] host disk low: 4000 MiB free; not starting (tell lane_plan)"
        cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": a, "where": "host", "slots": 1}], "why": "x"}})
        j = self.job(a)
        self.assertEqual((j["status"], j["exit"]), ("failed", 75))
        self.assertIn("disk low", j["reason"])
        self.host.disk_kb = 4000 * 1024
        t = self.tick()
        self.assertTrue(t["capacity"]["host"]["paused"])
        self.assertFalse(t["can_start"])
        self.assertIsNotNone(t["alert"])
        cq.cmd_notified(self.ctx, {"ids": [], "ack_alert": True})
        self.assertIsNone(self.tick()["alert"])  # notified once
        res = cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": b, "where": "host", "slots": 1}]}})
        self.assertEqual(self.job(b)["status"], "queued")  # dispatch paused
        # the disk recovers: the next probe resumes the host
        self.host.disk_kb = 300 * 1024 * 1024
        t = self.tick()
        self.assertFalse(t["capacity"]["host"]["paused"])
        self.assertTrue(t["can_start"])

    def test_S11_cancel_queued_and_running(self):
        a = self.submit("a")
        b = self.submit("b", slots=2)
        self.tick()
        self.assertEqual(self.cancel(a)["status"], "cancelled")
        self.assertEqual(self.job(a)["status"], "cancelled")
        self.assertNotIn(a, [q["id"] for q in self.tick()["queued"]])
        cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": b, "where": "host", "slots": 2}]}})
        self.assertEqual(self.job(b)["status"], "running")
        t = self.tick()
        self.assertEqual(t["capacity"]["host"]["pending"], 2)  # launched, not yet holding its slots
        self.cancel(b)
        self.assertEqual(self.job(b)["status"], "running")
        self.assertTrue(self.job(b)['cancel_requested'])
        self.assertIn(b, self.host.kills)
        self.tick()
        self.assertEqual(self.job(b)['status'], 'running')  # unconfirmed stop stays held
        self.host.set_done(b, 0)
        self.assertEqual(self.tick()["capacity"]["host"]["free"], 4)
        self.assertEqual(self.job(b)['status'], 'cancelled')
        self.assertTrue(self.cancel(b)["ok"])  # idempotent

    def test_S11_cancel_a_split_parent_cancels_the_parts(self):
        a = self.submit("x {part}", split={"max_parts": 3})
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": {"start": [{"job": a, "where": "host", "slots": 1, "parts": 3}]}})
        self.cancel(a)
        children = self.job(a)['children']
        self.assertTrue(all(self.job(c)['cancel_requested'] for c in children))
        self.assertEqual({self.job(c)['status'] for c in children}, {'running'})
        self.assertEqual(len(self.host.kills), 3)
        for child in children:
            self.host.set_done(child, 0)
        self.tick()
        self.assertEqual({self.job(c)['status'] for c in children}, {'cancelled'})

    def test_S17_job_past_its_timeout_is_killed_and_noticed(self):
        a = self.submit("sleep 9999", timeout_minutes=30)
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        start = int(self.clock[0])
        self.host.jobs[a].update(started=start)
        self.advance(30 * 60 + 30)
        self.tick()
        self.assertEqual(self.job(a)["status"], "running")  # the host `timeout` has its grace
        self.advance(60)
        t = self.tick()
        self.assertEqual(self.job(a)["status"], "running")
        self.assertTrue(self.job(a)['host_timeout_requested'])
        self.assertIn(a, self.host.kills)
        self.assertEqual(t['watch']['to_notify'], [])
        self.host.set_done(a, 124, started=start)
        t = self.tick()
        self.assertEqual(self.job(a)["status"], "timeout")
        self.assertEqual([x["id"] for x in t["watch"]["to_notify"]], [a])

    def test_S17_timeout_exit_124_from_the_host_timeout_command(self):
        a = self.submit("sleep 9999", timeout_minutes=30)
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        now = int(self.clock[0])
        self.host.set_done(a, 124, started=now - 1800, finished=now)
        self.tick()
        self.assertEqual((self.job(a)["status"], self.job(a)["exit"]), ("timeout", 124))

    def test_reap_done_job_fetches_gets_records_history_and_cleans_the_copy(self):
        a = self.submit("go test ./pkg -run X", gets=["go/out/result.json"], label="t")
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        self.host.files[(a, "go/out/result.json")] = b'{"ok": true}'
        self.host.tails[a] = ["line"] * 60
        self.host.set_done(a, 0)
        self.tick()
        j = self.job(a)
        self.assertEqual((j["status"], j["exit"]), ("done", 0))
        self.assertEqual(len(j["tail"]), 40)
        out = os.path.join(self.ctx.state, "out", a, "go", "out", "result.json")
        self.assertEqual(open(out, "rb").read(), b'{"ok": true}')
        self.assertEqual(j["out_paths"], [out])
        hist = json.loads(cq.read_lines(self.ctx.p("history.jsonl"))[-1])
        self.assertEqual((hist["kind"], hist["workstream"], hist["exit"], hist["where"]), ("go test", "lane_x", 0, "host"))
        self.tick()
        self.assertIn(a, self.host.removed)  # src and tmp removed, run.log and exit kept

    def test_reap_records_cpu_seconds_and_cores_from_the_snapshot(self):
        a = self.submit("go test ./pkg -run X")
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        self.host.set_done(a, 0)  # started 60 s before finished
        self.host.jobs[a]["cpu"] = 120
        self.tick()
        j = self.job(a)
        self.assertEqual((j["cpu_s"], j["cores"]), (120, 2.0))
        hist = json.loads(cq.read_lines(self.ctx.p("history.jsonl"))[-1])
        self.assertEqual((hist["cpu_s"], hist["cores"]), (120, 2.0))

    def test_reap_without_cpu_in_the_snapshot_adds_no_cpu_keys(self):
        a = self.submit("go test ./pkg -run X")
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        self.host.set_done(a, 0)
        self.tick()
        j = self.job(a)
        self.assertEqual(j["status"], "done")
        self.assertNotIn("cpu_s", j)
        self.assertNotIn("cores", j)
        hist = json.loads(cq.read_lines(self.ctx.p("history.jsonl"))[-1])
        self.assertNotIn("cpu_s", hist)
        self.assertNotIn("cores", hist)

    def test_reap_marks_a_dead_runner_failed(self):
        a = self.submit()
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        self.host.jobs[a].update(alive=False)
        self.tick()
        self.assertEqual(self.job(a)["status"], "failed")
        self.assertIn("died", self.job(a)["reason"])

    def test_launching_job_with_no_host_dir_is_requeued_after_ten_minutes(self):
        a = self.submit()
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})  # the worker never ran
        self.advance(300)
        self.tick()
        self.assertEqual(self.job(a)["status"], "launching")
        self.advance(400)
        self.tick()
        self.assertEqual(self.job(a)["status"], "queued")

    def test_S20_notice_data_is_bounded_and_scoped_to_the_sessions_worktree(self):
        other = os.path.join(self.tmp, "wt", "lane_other")
        os.makedirs(other)
        sh(other, "init", "-q")
        sh(other, "config", "user.email", "t@t")
        sh(other, "config", "user.name", "t")
        open(os.path.join(other, "f"), "w").write("x")
        sh(other, "add", "-A")
        sh(other, "commit", "-q", "-m", "i")
        with open(os.path.join(other, ".git", "info", "exclude"), "a") as exclude:
            exclude.write("\n.baton/\n")
        cq.write_json(os.path.join(other, ".baton", "owner.json"), {"vendor": "claude", "session_id": "sess-O"})
        mine = self.submit("a")
        theirs = cq.cmd_submit(self.ctx, {"command": "b", "root": other, "session": "sess-O"})["job"]
        for j in (mine, theirs):
            self.host.tails[j] = [("x" * 500)] * 100
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        for j in (mine, theirs):
            self.host.set_done(j, 0)
        t = self.tick()
        got = t["watch"]["to_notify"]
        self.assertEqual([x["id"] for x in got], [mine])
        self.assertLessEqual(len(got[0]["tail"]), 40)
        self.assertTrue(all(len(l) <= 300 for l in got[0]["tail"]))
        t2 = cq.cmd_watch(self.ctx, {"root": other, "session": "sess-O"})["watch"]
        self.assertEqual([x["id"] for x in t2["to_notify"]], [theirs])

    def test_S14_nothing_to_start_means_no_candidates(self):
        t = self.tick()
        self.assertEqual((t["queued"], t["can_start"]), ([], False))
        self.submit("x", slots=4)
        self.host.held = 1
        t = self.tick()
        self.assertEqual(len(t["queued"]), 1)
        self.assertFalse(t["can_start"])  # 3 free, the job needs 4
        self.host.held = 0
        self.assertTrue(self.tick()["can_start"])

    def test_history_medians_reach_the_planner_input(self):
        for m in (5, 7, 9):
            cq.append_line(self.ctx.p("history.jsonl"), {"kind": "go test", "workstream": "lane_x", "actual": m, "status": "done"})
        self.submit("go test ./pkg")
        t = self.tick()
        self.assertEqual(t["medians"], [{"kind": "go test", "workstream": "lane_x", "median_min": 7, "n": 3}])

    def test_a_job_reaped_as_finished_before_its_worker_records_the_launch_stays_finished(self):
        a = self.submit()
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
        self.host.jobs[a] = dict(exit=0, started=1, launched=1, finished=2, alive=False, src=True)
        self.tick()  # the reaper sees the finished run while the job is still 'launching'
        self.assertEqual(self.job(a)["status"], "done")
        cq.launch(self.ctx, a)  # a late worker finds nothing to launch
        self.assertEqual(self.job(a)["status"], "done")
        cq._launch_result(self.ctx, a, "running")
        self.assertEqual(self.job(a)["status"], "done")

    def test_an_explicit_cancel_is_not_announced_back(self):
        a = self.submit()
        self.cancel(a)
        self.assertEqual(self.tick()["watch"]["to_notify"], [])


class LaptopTests(Base):
    def test_laptop_job_runs_in_an_exported_copy_and_is_reaped(self):
        if not HAVE_BASH:
            self.skipTest("bash needed")
        with open(os.path.join(self.repo, "a.txt"), "w") as f:
            f.write("dirty\n")
        a = self.submit("cat a.txt; echo done-$CQ_JOB; echo fine > made.txt", where="laptop", slots=0, gets=["made.txt"])
        self.tick()
        r = cq.cmd_apply(self.ctx, {"plan": None})
        self.assertEqual(r["accepted"][0]["where"], "laptop")
        run = self.ctx.p("run", a)
        self.assertEqual(open(os.path.join(run, "src", "a.txt")).read().strip(), "dirty")
        self.assertTrue(os.path.isdir(os.path.join(run, "src", ".git")))  # a clone of the source repo, never a linked worktree
        self.assertEqual(open(os.path.join(run, "exit")).read(), "0")
        self.tick()
        j = self.job(a)
        self.assertEqual((j["status"], j["exit"]), ("done", 0))
        self.assertTrue(any("done-" + a in l for l in j["tail"]))
        self.assertEqual(open(os.path.join(self.ctx.state, "out", a, "made.txt")).read().strip(), "fine")
        self.assertFalse(os.path.exists(os.path.join(run, "src")))  # the exported tree is removed, log and exit stay
        self.assertTrue(os.path.exists(os.path.join(run, "run.log")))

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_laptop_job_sees_the_carried_baton_remote_and_CQ_SHA_CQ_SRC(self):
        d = os.path.join(self.repo, ".baton", "remote")
        os.makedirs(d)
        with open(os.path.join(self.repo, ".git", "info", "exclude"), "a") as f:
            f.write("/.baton/\n")
        with open(os.path.join(d, "hello.sh"), "w", newline="\n") as f:
            f.write('echo "hello sha=$CQ_SHA base=$CQ_BASE src=$CQ_SRC"; ls .baton/remote > ls.out\n')
        a = self.submit("bash .baton/remote/hello.sh", where="laptop", slots=0, gets=["ls.out"])
        j = self.job(a)
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        run = self.ctx.p("run", a)
        log = open(os.path.join(run, "run.log")).read()
        self.assertIn("sha=" + j["sha"], log)
        self.assertIn("base=" + sh(self.repo, "rev-parse", "HEAD"), log)
        self.assertRegex(log.replace("\\", "/"), r"src=\S*/run/" + a + "/src")
        self.assertEqual(open(os.path.join(run, "src", "ls.out")).read().split(), ["hello.sh"])

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_laptop_job_src_is_a_git_checkout_at_CQ_SHA_with_the_source_repos_tags(self):
        sh(self.repo, "tag", "freeze/lane-base-test")
        sh(self.tmp, "init", "-q")  # an enclosing repo around the state dir, as baton is around .state/compute
        with open(os.path.join(self.repo, "a.txt"), "w") as f:
            f.write("dirty\n")  # the snapshot sha is then not HEAD
        cmd = ("git rev-parse HEAD > head.out; git tag -l > tags.out; git rev-parse --show-toplevel > top.out; "
               "git status --short --untracked-files=no > status.out; mkdir -p sub; cd sub; git rev-parse HEAD > ../sub.out; "
               "echo \"ceil=$GIT_CEILING_DIRECTORIES\" > ../ceil.out")
        a = self.submit(cmd, where="laptop", slots=0)
        j = self.job(a)
        self.assertNotEqual(j["sha"], sh(self.repo, "rev-parse", "HEAD"))
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        run = self.ctx.p("run", a)

        def out(name):
            return open(os.path.join(run, "src", name)).read().strip()

        self.assertEqual(out("head.out"), j["sha"])  # HEAD == CQ_SHA, not the enclosing repo's
        self.assertEqual(out("sub.out"), j["sha"])
        self.assertIn("freeze/lane-base-test", out("tags.out").split())
        self.assertEqual(os.path.realpath(out("top.out")), os.path.realpath(os.path.join(run, "src")))
        self.assertEqual(out("status.out"), "")  # the checkout is the snapshot, files and index agree
        self.assertEqual(out("ceil.out"), "ceil=" + run.replace("\\", "/"))
        self.assertEqual(open(os.path.join(run, "src", "a.txt"), newline="").read(), "dirty\n")

    def test_S17_laptop_wrapper_past_timeout_is_killed(self):
        a = self.submit("sleep 99", where="laptop", slots=0, timeout_minutes=1)
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "laptop", "slots": 0})
        cq._launch_result(self.ctx, a, "running")
        run = self.ctx.p("run", a)
        os.makedirs(run, exist_ok=True)
        open(os.path.join(run, "pid"), "w").write("4242")
        open(os.path.join(run, "started"), "w").write(str(int(self.clock[0])))
        self.laptop.alive.add(4242)
        self.advance(100)
        self.tick()
        self.assertEqual(self.job(a)["status"], "running")
        self.advance(40)
        self.tick()
        self.assertEqual(self.job(a)["status"], "timeout")
        self.assertEqual(self.laptop.killed, [4242])

    @unittest.skipUnless(HAVE_BASH, "bash needed")
    def test_laprun_writes_pid_started_log_and_exit(self):
        a = self.submit("echo hello; exit 3", where="laptop", slots=0)
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "laptop", "slots": 0})
        cq.launch(self.ctx, a)  # sync: runs the wrapper inline
        run = self.ctx.p("run", a)
        self.assertEqual(open(os.path.join(run, "exit")).read(), "3")
        self.assertIn("hello", open(os.path.join(run, "run.log")).read())
        self.assertTrue(os.path.exists(os.path.join(run, "pid")))
        self.tick()
        self.assertEqual((self.job(a)["status"], self.job(a)["exit"]), ("failed", 3))

    def test_S16_laptop_heavy_lock_busy_is_in_the_capacity(self):
        self.laptop.busy = True
        self.assertTrue(self.tick()["capacity"]["laptop"]["heavy_busy"])
        self.laptop.busy = False
        self.laptop.ram = 3.0
        t = self.tick()
        self.assertFalse(t["capacity"]["laptop"]["ram_ok"])

    def test_the_probe_writes_the_memory_split_and_ram_ok_still_comes_from_free_ram(self):
        self.laptop.ram = 4.0
        lp = self.tick()["capacity"]["laptop"]
        self.assertFalse(lp["ram_ok"])
        self.assertEqual({k: lp[k] for k in ("commit_gb", "commit_limit_gb", "np_gb", "paged_gb", "cache_gb")},
                         {"commit_gb": 9.5, "commit_limit_gb": 30.0, "np_gb": 0.4, "paged_gb": 0.6, "cache_gb": 1.2})
        written = cq.read_json(self.ctx.p("capacity.json"))["laptop"]
        self.assertEqual((written["commit_gb"], written["cache_gb"], written["free_ram_gb"]), (9.5, 1.2, 4.0))
        self.laptop.mem = dict(self.laptop.mem, commit_gb=31.0)  # a full commit does not change the RAM floor
        self.laptop.ram = 16.0
        self.assertTrue(self.tick()["capacity"]["laptop"]["ram_ok"])

    def test_a_probe_without_the_memory_split_adds_no_keys(self):
        self.ctx.laptop = OldLaptop()
        lp = self.tick()["capacity"]["laptop"]
        self.assertTrue(lp["ram_ok"])
        self.assertNotIn("commit_gb", lp)
        self.laptop.mem = None  # a failed query (mem_stats returns None) adds no keys either
        self.ctx.laptop = self.laptop
        self.assertNotIn("commit_gb", self.tick()["capacity"]["laptop"])


@unittest.skipUnless(os.name == "nt", "reads the Windows memory and process counters")
class LaptopMemoryTests(unittest.TestCase):
    def test_mem_stats_reads_the_commit_and_the_pools(self):
        m = cq.Laptop().mem_stats()
        self.assertIsInstance(m, dict)
        self.assertGreater(m["total_gb"], 0)
        self.assertGreater(m["commit_gb"], 0)
        self.assertGreaterEqual(m["commit_limit_gb"], m["commit_gb"])

    def test_top_procs_lists_the_largest_groups_first(self):
        t = cq.Laptop().top_procs(3)
        self.assertIsInstance(t, dict)
        self.assertLessEqual(len(t["groups"]), 3)
        sizes = [g["ws_mb"] for g in t["groups"]]
        self.assertEqual(sizes, sorted(sizes, reverse=True))
        self.assertGreater(t["ws_gb_total"], 0)

    def test_pool_top_lists_the_tags_by_size(self):
        p = cq.Laptop().pool_top()
        if p is None:
            self.skipTest("the pool tag query is blocked on this system")
        self.assertLessEqual(len(p), 3)
        self.assertTrue(p, "the pool holds no tags")
        self.assertTrue(all(len(r["tag"]) == 4 and r["tag"].isascii() and r["tag"].isprintable() for r in p), p)
        self.assertGreater(p[0]["mb"], 0)
        self.assertTrue(all(r["outstanding"] >= 0 for r in p), p)
        self.assertTrue(all(r["mb"] >= 0 for r in p))
        self.assertEqual([r["mb"] for r in p], sorted((r["mb"] for r in p), reverse=True))


class FixBatchTests(Base):
    """cq-review-1 and its examination: the fix batch."""

    def test_S11_the_stop_script_uses_identity_checked_pidfds(self):
        s = cq.host_kill_script("cq-x", grace=3)
        self.assertIn('# CQ_PIDFD_STOP cq-x', s)
        self.assertNotIn('pkill', s)
        self.assertNotIn('systemctl', s)
        for bad in ('cq-x;touch-pwned', 'cq-x\nother', 'cq-x.p9'):
            with self.assertRaises(cq.CqError):
                cq.host_kill_script(bad)
        with self.assertRaises(cq.CqError):
            cq.host_kill_script('cq-x', grace=11)

    def test_parse_snapshot_reads_cpu_seconds_or_none(self):
        line = "CQJOB cq-a exit=0 started=1 launched=1 finished=2 {} alive=0 src=0 now=3\n"
        self.assertEqual(cq.parse_snapshot("PROBE\n" + line.format("cpu=42") + "END\n")["jobs"]["cq-a"]["cpu"], 42)
        self.assertIsNone(cq.parse_snapshot("PROBE\n" + line.format("") + "END\n")["jobs"]["cq-a"]["cpu"])
        self.assertIsNone(cq.parse_snapshot("PROBE\n" + line.format("cpu=") + "END\n")["jobs"]["cq-a"]["cpu"])

    def test_the_probe_asks_for_a_live_session_and_reads_the_exit_file_after_it(self):
        s = cq.SNAPSHOT_SCRIPT
        self.assertIn('ps -e -o sid=', s)
        self.assertNotIn("kill -0", s)
        # liveness (the listing, then the pgrep for a pid the listing misses) is decided before the exit file is read
        self.assertLess(s.index('pgrep -s "$pid"'), s.index("rv exit"))
        self.assertIn("HEAVY_SLOTS", s)

    def test_the_job_loop_forks_nothing_per_finished_dir(self):
        keys = ["CQJOB", "exit=$ex", "started=$st", "launched=$la", "finished=$fin", "cpu=$cp", "pid=$pid",
                "alive=$alive", "src=$src", "now=$now"]
        for s in (cq.snapshot_script(False), cq.snapshot_script(True)):
            loop = s.split("echo JOBS\n", 1)[1]
            code = "\n".join(line for line in loop.splitlines() if not line.lstrip().startswith("#"))
            for banned in ("$(rd", "tr -d '\\n '"):
                self.assertNotIn(banned, code)
            self.assertEqual(code.count("pgrep"), 1)  # only the guarded fallback: no unconditional fork per directory
            pg = next(line for line in code.splitlines() if "pgrep" in line)
            self.assertTrue('"$nolive" = 1' in pg and '! -f "$d/exit"' in pg, pg)
            self.assertIn("ps -e -o sid=", code)
            self.assertIn("read -r v <", code)
            echo_line = next(line for line in code.splitlines() if "CQJOB" in line)
            pos = [echo_line.index(k) for k in keys]
            self.assertEqual(pos, sorted(pos), echo_line)

    @unittest.skipUnless(HAVE_BASH, "needs bash")
    def test_the_job_loop_keeps_the_pgrep_answer_where_the_listing_cannot_be_trusted(self):
        """Run the real loop under bash with stub ps and pgrep (only pid 123 is alive for pgrep; ps lists session 77 only)."""
        script = cq.snapshot_script(False)
        loop = script[script.index('cd "$HOME/q"'):script.index("echo END")]
        home = tempfile.mkdtemp(prefix="cq-probe-")
        self.addCleanup(shutil.rmtree, home, True)
        q = os.path.join(home, "q")

        def job(name, pid, exit_code=None, src=False):
            d = os.path.join(q, name)
            os.makedirs(os.path.join(d, "src") if src else d)
            for fname, val in (("pid", pid), ("exit", exit_code), ("launched", "99")):
                if val is not None:
                    with open(os.path.join(d, fname), "w", encoding="utf-8", newline="\n") as f:  # LF, as on the host
                        f.write(val + "\n")

        job("cq-a-late", "123", src=True)       # started after the listing: not in it, no exit file: pgrep says alive
        job("cq-b-listed", "77")                # in the listing: alive without any pgrep
        job("cq-c-dead", "999")                 # no exit file, not listed, pgrep says no: dead
        job("cq-d-done", "123", exit_code="0")  # finished, pid not listed, exit file present: no pgrep, not alive
        job("cq-e-empty", None)                 # nothing written yet

        def rows(ps_body):
            prog = "ps() { " + ps_body + "; }\npgrep() { [ \"$2\" = 123 ]; }\nnow=1000\n" + loop
            r = subprocess.run([BASH, "-c", prog], env=dict(os.environ, HOME=home), capture_output=True, timeout=60)
            self.assertEqual(r.returncode, 0, r.stderr.decode("utf-8", "replace"))
            return {ln.split()[1]: dict(kv.split("=", 1) for kv in ln.split()[2:]) for ln in r.stdout.decode().splitlines()
                    if ln.startswith("CQJOB ")}

        got = rows('echo "   77"; echo "    1"')
        self.assertEqual({k: v["alive"] for k, v in got.items()},
                         {"cq-a-late": "1", "cq-b-listed": "1", "cq-c-dead": "0", "cq-d-done": "0", "cq-e-empty": "0"})
        self.assertEqual((got["cq-d-done"]["exit"], got["cq-a-late"]["src"], got["cq-a-late"]["launched"]), ("0", "1", "99"))
        # ps fails or prints nothing: every pid gets the pgrep answer, so a running job is never reported dead for it
        got = rows(":")
        self.assertEqual({k: v["alive"] for k, v in got.items()},
                         {"cq-a-late": "1", "cq-b-listed": "0", "cq-c-dead": "0", "cq-d-done": "1", "cq-e-empty": "0"})

    def test_cleanup_never_removes_the_copy_under_a_live_command(self):
        a = self.submit()
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        cq.finish(self.ctx, a, 'cancelled', None)
        self.host.jobs[a].update(alive=True, src=True)  # run.sh is gone but the command still runs in the session
        snap, _ = cq.host_snapshot(self.ctx)
        self.host.calls.clear()
        cq.cleanup_host(self.ctx, snap, self.clock[0])
        script = self.host.calls[-1]
        self.assertIn(f'CQ_PIDFD_STOP {a}', script)
        self.assertNotIn("rm -rf", script)

    def test_cleanup_kills_a_lingering_session_of_every_terminal_status(self):
        for status, code in (("done", 0), ("failed", 3)):
            self.host.jobs.clear()  # the other status's job is a week-old dir to prune: not this one's script
            a = self.submit()
            self.tick()
            cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
            cq._launch_result(self.ctx, a, "running")
            cq.finish(self.ctx, a, status, code)
            self.host.jobs[a] = dict(exit=code, started=1, launched=1, finished=2, alive=True, src=True)  # a child holds a flock
            snap, _ = cq.host_snapshot(self.ctx)
            self.host.calls.clear()
            cq.cleanup_host(self.ctx, snap, self.clock[0])
            script = self.host.calls[-1]
            self.assertIn(f'CQ_PIDFD_STOP {a}', script, status)
            self.assertNotIn("rm -rf", script, status)  # the copy stays until nothing lives in it
            self.host.jobs[a]["alive"] = False
            snap, _ = cq.host_snapshot(self.ctx)
            self.host.calls.clear()
            cq.cleanup_host(self.ctx, snap, self.clock[0])
            self.assertIn(f'rm -rf "$HOME/q/{a}/src"', self.host.calls[-1], status)
            self.assertNotIn("pkill", self.host.calls[-1], status)

    def test_cleanup_of_a_long_backlog_goes_in_commands_a_command_line_holds(self):
        # 300 finished jobs with a source copy and 300 week-old directories: one command for all of them was about
        # 150,000 characters, more than ssh can be started with, so nothing was ever removed again
        now = self.clock[0]
        names = [f"cq-20261107T{i:06d}-{i:016x}" for i in range(600)]
        jobs = {n: dict(exit=0, started=1, launched=1, finished=now - (8 * 86400 if i % 2 else 60), cpu=1, alive=False,
                        src=not i % 2) for i, n in enumerate(names)}
        record = {"status": "done", "host": cq.HOST, "host_config": {"tmp_root": "/dev/shm/cq"}}
        sent = []

        def ssh(ctx, job, script, timeout=60):
            sent.append(script)
            return 0, "", ""
        with mock.patch.object(cq, "load_job", side_effect=lambda ctx, name: dict(record, id=name)), \
                mock.patch.object(cq, "job_ssh", side_effect=ssh):
            report = {}
            self.assertEqual(cq.cleanup_host(self.ctx, {"jobs": jobs}, now, report=report), cq.CLEANUP_CALLS)
            self.assertEqual(len(sent), cq.CLEANUP_CALLS)
            self.assertGreater(report["left"], 0)   # more than one call's worth
            sent.clear()
            self.assertEqual(cq.cleanup_host(self.ctx, {"jobs": jobs}, now, calls=1), 1)   # the leader's tick: one command
            self.assertEqual(len(sent), 1)
            sent.clear()
            self.assertEqual(cq.cleanup_host(self.ctx, {"jobs": jobs}, now), cq.CLEANUP_CALLS)
            self.assertTrue(all(len(s) <= cq.CLEANUP_SCRIPT_CHARS for s in sent))
            self.assertTrue(all(s.count("\n") > 20 for s in sent))  # a full command, not a job at a time
            self.assertTrue(all('/src"' in line for s in sent for line in s.splitlines()))  # source copies come first
            # the host's listing shrinks as it removes; every job is reached in a bounded number of calls
            seen, rounds = set(), 0
            while jobs and rounds < 40:
                sent.clear()
                cq.cleanup_host(self.ctx, {"jobs": jobs}, now)
                for script in sent:
                    for name in re.findall(r'rm -rf "\$HOME/q/(cq-[\w.-]+?)(?:/src)?"', script):
                        seen.add(name)
                        jobs.pop(name, None)
                rounds += 1
            self.assertEqual((seen, jobs), (set(names), {}))
            self.assertLess(rounds, 40)
            # a command the host did not take ends the call: nothing further is sent on that connection's failure
            jobs.update({n: dict(exit=0, started=1, launched=1, finished=now - 60, cpu=1, alive=False, src=True) for n in names})
            sent.clear()
            with mock.patch.object(cq, "job_ssh", side_effect=lambda *a, **k: (sent.append(a[2]), (255, "", "ssh timed out"))[1]):
                self.assertEqual(cq.cleanup_host(self.ctx, {"jobs": jobs}, now, report=report), 1)
            self.assertGreater(report["left"], cq.CLEANUP_CALLS)   # the refused command counts as left
            few = dict(list(jobs.items())[:3])
            self.assertEqual((cq.cleanup_host(self.ctx, {"jobs": few}, now, report=report), report["left"]), (1, 0))

        # a record the store cannot read is skipped; the others are still cleaned
        def load(ctx, name):
            if name == names[0]:
                raise cq.CqError("duplicate job ID across ordinary and deferred stores")
            return "not a record" if name == names[1] else dict(record, id=name)
        sent.clear()
        fresh = {n: dict(exit=0, started=1, launched=1, finished=now - 60, cpu=1, alive=False, src=True) for n in names[:6]}
        with mock.patch.object(cq, "load_job", side_effect=load), mock.patch.object(cq, "job_ssh", side_effect=ssh):
            self.assertEqual(cq.cleanup_host(self.ctx, {"jobs": fresh}, now), 1)
        self.assertEqual(re.findall(r'rm -rf "\$HOME/q/(cq-[\w.-]+?)/src"', sent[0]), names[2:6])
        self.assertEqual(list(cq.script_chunks(["aaaa", "bb", "c" * 30, "d"], 8)), [["aaaa", "bb"], ["c" * 30], ["d"]])
        self.assertEqual(cq.cleanup_host(self.ctx, {"jobs": {}}, now), 0)

    def test_probe_reads_the_hosts_heavy_slots(self):
        self.host.slots = 6
        self.host.held = 1
        h = self.tick()["capacity"]["host"]
        self.assertEqual((h["total"], h["free"]), (6, 5))
        self.assertEqual(cq.parse_snapshot("PROBE\nSLOTS x\nEND\n")["slots"], cq.HOST_SLOTS)

    def test_host_address_follows_remoterun_and_cq_host_wins(self):
        self.assertEqual(cq.host_address({}), "")
        self.assertEqual(cq.host_address({"LANE_HOST": "198.51.100.11"}), "198.51.100.11")
        self.assertEqual(cq.host_address({"LANE_HOST": "me@box"}), "me@box")
        self.assertEqual(cq.host_address({"LANE_HOST": "198.51.100.11", "CQ_HOST": "x@y"}), "x@y")

    def test_S10_the_disk_alert_is_on_every_tick_not_only_the_leaders(self):
        self.assertTrue(self.tick("sess-A")["leader"])
        hs = cq.host_state(self.ctx)
        hs["disk_alert"] = {"at": "2026-10-17T10:00:00Z", "job": "cq-2", "msg": "host disk low", "acked": False}
        cq.save_host_state(self.ctx, hs)
        t = self.tick("sess-B")
        self.assertFalse(t["leader"])
        self.assertEqual(t["alert"]["job"], "cq-2")
        cq.cmd_notified(self.ctx, {"ids": [], "ack_alert": True})
        self.assertIsNone(self.tick("sess-B")["alert"])
        self.assertIsNone(self.tick("sess-A")["alert"])

    def test_S8_tick_shows_parts_now_for_a_split_job(self):
        self.submit("python mutate.py --part {part}", split={"max_parts": 8})
        self.submit("echo {part}", slots=0, split={"max_parts": 8})
        rows = {r["slots"]: r for r in self.tick()["queued"]}
        self.assertEqual((rows[1]["parts_now"], rows[0]["parts_now"]), (4, 4))
        self.host.held = 3
        rows = {r["slots"]: r for r in self.tick()["queued"]}
        self.assertEqual(rows[1]["parts_now"], 1)

    def test_a_slow_launch_is_not_reaped_while_its_worker_refreshes_t_touch(self):
        a = self.submit()
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
        self.advance(560)  # the worker was slow to start: still inside LAUNCH_STALE (600 s)
        seen = []

        def slow_remoterun(argv, cwd):
            self.assertEqual(self.job(a)["t_touch"], self.clock[0])  # refreshed once the dispatch lock was taken
            self.advance(500)  # 1,060 s since the dispatch
            self.tick()
            seen.append(self.job(a)["status"])
            return self.host.remoterun(argv, cwd)

        self.ctx.run_remoterun = slow_remoterun
        cq.launch(self.ctx, a)
        self.assertEqual(seen, ["launching"])  # the reaper left it alone
        self.assertEqual(self.job(a)["status"], "running")

    def test_a_split_jobs_waiting_parts_are_kept_fresh(self):
        a = self.submit("x {part}", split={"max_parts": 2})
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1, "parts": 2})
        kids = self.job(a)["children"]
        self.advance(500)
        real = self.host.remoterun
        waiting = []

        def first_part_slow(argv, cwd):
            self.advance(500)
            waiting.append(self.job(kids[1]).get("t_touch"))
            return real(argv, cwd)

        self.ctx.run_remoterun = first_part_slow
        cq.launch(self.ctx, kids[0], kids)
        self.assertEqual(waiting, [self.clock[0] - 500])  # the second part was touched when the first took its turn

    def test_launch_lock_timeout_requeues_instead_of_failing(self):
        a = self.submit()
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
        with mock.patch.object(cq, "launch_host", side_effect=cq.LockTimeout("could not take dispatch lock")):
            cq.launch(self.ctx, a)
        j = self.job(a)
        self.assertEqual(j["status"], "queued")
        self.assertIn("lock busy", j["reason"])

    def test_laptop_launch_rechecks_the_status_before_spawning_the_wrapper(self):
        a = self.submit("echo hi", where="laptop", slots=0)
        self.tick()
        self.ctx.sync = False
        cq.dispatch(self.ctx, {"job": a, "where": "laptop", "slots": 0})
        real_export = cq.checkout_tree
        spawned = []

        def export_then_cancel(top, sha, dest):
            r = real_export(top, sha, dest)
            self.cancel(a)  # the cancel arrives while the tree is exported
            return r

        with mock.patch.object(cq, "checkout_tree", export_then_cancel), \
                mock.patch.object(cq, "spawn_detached", lambda *args, **kw: spawned.append(args)):
            cq.launch(self.ctx, a)
        self.assertEqual(spawned, [])
        self.assertEqual(self.job(a)["status"], "cancelled")
        b = self.submit("echo hi", where="laptop", slots=0)
        self.tick()
        cq.dispatch(self.ctx, {"job": b, "where": "laptop", "slots": 0})
        with mock.patch.object(cq, "spawn_detached", lambda *args, **kw: spawned.append(args)):
            cq.launch(self.ctx, b)
        self.assertEqual(len(spawned), 1)
        self.assertEqual(self.job(b)["status"], "running")

    def test_laptop_export_longer_than_two_minutes_is_not_a_died_wrapper(self):
        a = self.submit("echo hi", where="laptop", slots=0)
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "laptop", "slots": 0})
        self.advance(300)  # no pid yet, no touch: a launching job gets LAUNCH_STALE (600 s), not 120 s
        self.tick()
        self.assertEqual(self.job(a)["status"], "launching")
        self.advance(310)
        self.tick()
        self.assertEqual(self.job(a)["status"], "failed")

    def test_a_running_laptop_job_without_a_pid_still_fails_after_120_seconds(self):
        a = self.submit("echo hi", where="laptop", slots=0)
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "laptop", "slots": 0})
        cq._launch_result(self.ctx, a, "running")
        self.advance(130)
        self.tick()
        self.assertEqual(self.job(a)["status"], "failed")

    def test_host_launch_with_a_dir_but_no_launched_file_gets_the_launch_grace(self):
        a = self.submit()
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
        self.host.jobs[a] = dict(exit=None, started=None, launched=None, finished=None, alive=False, src=True)
        self.advance(200)
        self.tick()
        self.assertEqual(self.job(a)["status"], "launching")  # the launcher may still be pushing
        self.advance(420)  # 620 s
        self.tick()
        self.assertEqual(self.job(a)["status"], "queued")  # transient: requeued, as with no host dir
        self.assertIn("launcher lost", self.job(a)["reason"])

    def test_host_running_job_whose_run_sh_never_started_fails_after_120_seconds(self):
        a = self.submit()
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
        cq._launch_result(self.ctx, a, "running")
        self.host.jobs[a] = dict(exit=None, started=None, launched=None, finished=None, alive=False, src=True)
        self.advance(130)
        self.tick()
        j = self.job(a)
        self.assertEqual(j["status"], "failed")
        self.assertIn("died without an exit file", j["reason"])

    def test_reap_laptop_reads_the_exit_file_after_the_liveness_check(self):
        a = self.submit("echo hi", where="laptop", slots=0)
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "laptop", "slots": 0})
        cq._launch_result(self.ctx, a, "running")
        run = self.ctx.p("run", a)
        os.makedirs(run, exist_ok=True)
        with open(os.path.join(run, "pid"), "w") as f:
            f.write("4242")

        def dies_after_writing_its_exit(pid):  # the wrapper ends between the two reads
            with open(os.path.join(run, "exit"), "w") as f:
                f.write("0")
            return False

        self.laptop.pid_alive = dies_after_writing_its_exit
        self.tick()
        j = self.job(a)
        self.assertEqual((j["status"], j["exit"]), ("done", 0))  # not "wrapper died without an exit file"

    def test_default_label_leaves_out_NAME_value_tokens(self):
        j = self.job(self.submit("LANE_HOST=198.51.100.10 GOFLAGS=-race go test ./pkg"))
        self.assertEqual(j["label"], "go test ./pkg")
        self.assertEqual(self.job(self.submit("FOO=1"))["label"], "FOO=1")

    def test_light_jobs_on_the_host_count_against_the_cap_from_the_probe(self):
        for i in range(3):
            self.submit(f"echo {i}", slots=0)
        self.tick()
        cq.cmd_apply(self.ctx, {"plan": None})
        self.assertEqual(self.tick()["capacity"]["host"]["light_running"], 3)
        self.assertEqual(cq.light_running(self.ctx), {"host": 3, "laptop": 0})


class RobustnessTests(Base):
    def test_S19_an_interrupted_write_leaves_the_job_file_valid(self):
        a = self.submit()
        before = open(cq.job_file(self.ctx, a), "rb").read()
        real = os.replace

        def boom(src, dst):
            raise OSError("killed mid-write")

        cq.os.replace = boom
        try:
            with self.assertRaises(OSError):
                j = self.job(a)
                j["label"] = "changed"
                cq.save_job(self.ctx, j)
        finally:
            cq.os.replace = real
        self.assertEqual(open(cq.job_file(self.ctx, a), "rb").read(), before)
        self.assertEqual(json.loads(before)["id"], a)
        names = os.listdir(os.path.dirname(cq.job_file(self.ctx, a)))
        self.assertEqual([n for n in names if ".tmp" in n], [])  # the temp file was cleaned up
        self.assertEqual([j["id"] for j in cq.all_jobs(self.ctx)], [a])

    def test_S19_a_torn_temp_file_is_ignored_by_the_store(self):
        a = self.submit()
        torn = cq.job_file(self.ctx, "cq-torn") + ".tmp-1-ab"
        open(torn, "w").write('{"id": "cq-torn", "stat')
        self.assertEqual([j["id"] for j in cq.all_jobs(self.ctx)], [a])

    def test_S19_a_dead_lock_holder_does_not_wedge_the_queue(self):
        self.submit()
        lock = self.ctx.p("queue.lock")
        code = textwrap.dedent(f"""
            import os, sys, time
            sys.path.insert(0, {os.path.dirname(os.path.abspath(cq.__file__))!r})
            import cq
            with cq.FileLock({lock!r}, 5):
                print("held", flush=True)
                time.sleep(60)
        """)
        p = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(p.stdout.readline().strip(), "held")
            with self.assertRaises(cq.LockTimeout):  # a live holder does block
                with cq.FileLock(lock, 0.3):
                    pass
        finally:
            p.kill()
            p.wait()
        t0 = time.monotonic()
        with self.ctx.qlock(timeout=5):  # the dead holder's lock is gone with the process
            pass
        self.assertLess(time.monotonic() - t0, 3)
        self.assertEqual(len(cq.queued_jobs(self.ctx)), 1)

    def test_atomic_write_survives_concurrent_readers(self):
        path = self.ctx.p("x.json")
        cq.write_json(path, {"n": 0})
        for n in range(1, 30):
            cq.write_json(path, {"n": n})
            self.assertEqual(cq.read_json(path)["n"], n)

    def test_cli_round_trip_submit_status_cancel(self):
        state = os.path.join(self.tmp, "clistate")
        env = dict(os.environ, CQ_STATE=state, PYTHONIOENCODING="utf-8")

        def run(cmd, payload):
            r = subprocess.run([sys.executable, cq.__file__, cmd], input=json.dumps(payload).encode(), capture_output=True, env=env)
            self.assertEqual(r.stdout.decode().strip().count("\n"), 0, r.stderr.decode())
            return json.loads(r.stdout.decode())

        s = run("submit", {"command": "echo hi", "root": self.repo, "session": "sess-A"})
        self.assertTrue(s["ok"])
        st = run("status", {})
        self.assertEqual([q["id"] for q in st["queue"]], [s["job"]])
        self.assertFalse(run("cancel", {"job": s["job"]})["ok"])  # a bare job id names no worktree or owner
        self.assertTrue(run("cancel", {"job": s["job"], "root": self.repo, "session": "sess-A"})["ok"])
        self.assertFalse(run("submit", {"command": "echo {part}", "root": self.repo, "session": "sess-A"})["ok"])
        self.assertFalse(run("nonsense", {})["ok"])

    def test_wait_returns_the_job_when_it_ends_or_at_the_timeout(self):
        a = self.submit()
        os.environ["CQ_WAIT_INTERVAL"] = "0.05"
        self.addCleanup(os.environ.pop, "CQ_WAIT_INTERVAL", None)
        r = cq.cmd_wait(self.ctx, {"job": a, "timeout_s": 1})
        self.assertFalse(r["ended"])
        cq.finish(self.ctx, a, "done", 0, tail=["ok"])
        r = cq.cmd_wait(self.ctx, {"job": a, "timeout_s": 1})
        self.assertTrue(r["ended"])
        self.assertEqual(r["job"]["status"], "done")

    def test_a_result_answered_to_its_owner_is_not_announced_again(self):
        # one delivery per result: the wait or status answer counts, for the session the notice would have gone to
        a, b, c = self.submit(), self.submit(), self.submit()
        for job in (a, b, c):
            cq.finish(self.ctx, job, "failed", 1, tail=["boom"])
        pending = lambda: sorted(j["id"] for j in cq.watch(self.ctx, self.repo, self.clock[0], "sess-A")["to_notify"])
        self.assertEqual(pending(), sorted([a, b, c]))
        mine = {"seen_by": {"root": self.repo, "session": "sess-A"}}
        self.assertIs(cq.cmd_wait(self.ctx, {"job": a, "timeout_s": 1, **mine})["delivered"], True)
        self.assertEqual(pending(), sorted([b, c]))
        self.assertEqual(cq.load_job(self.ctx, a)["notified_via"], "wait")
        self.assertIs(cq.cmd_get(self.ctx, {"job": b, "compact": True, **mine})["delivered"], True)
        self.assertEqual(pending(), [c])
        # a job already marked is answered without the queue lock, and still reads as delivered
        with mock.patch.object(self.ctx, "qlock", side_effect=AssertionError("no lock needed")):
            self.assertIs(cq.cmd_wait(self.ctx, {"job": a, "timeout_s": 1, **mine})["delivered"], True)
        # a queue lock that cannot be had does not turn the result into an error: the notice simply stays due
        with mock.patch.object(self.ctx, "qlock", side_effect=cq.CqError("queue.lock busy")):
            held = cq.cmd_wait(self.ctx, {"job": c, "timeout_s": 1, **mine})
        self.assertEqual((held["ok"], held["delivered"], held["job"]["exit"]), (True, False, 1))
        # another session, an unnamed caller and a caller that is not the worktree's owner leave the notice pending
        self.assertIs(cq.cmd_wait(self.ctx, {"job": c, "timeout_s": 1})["delivered"], False)
        cq.cmd_wait(self.ctx, {"job": c, "timeout_s": 1, "seen_by": {"root": self.repo, "session": "sess-other"}})
        cq.cmd_get(self.ctx, {"job": c, "seen_by": {"root": self.tmp, "session": "sess-A"}})
        cq.cmd_get(self.ctx, {"job": c, "seen_by": "sess-A"})
        self.assertEqual(pending(), [c])
        # a job still running is not marked by being looked at
        d = self.submit()
        cq.cmd_get(self.ctx, {"job": d, "compact": True, **mine})
        self.assertFalse(cq.load_job(self.ctx, d).get("notified"))

    def test_the_answer_for_one_job_is_brief_and_the_stored_record_is_there_on_request(self):
        a = self.submit()
        brief = cq.cmd_get(self.ctx, {"job": a, "compact": True})["job"]
        self.assertEqual((brief["id"], brief["status"]), (a, cq.load_job(self.ctx, a)["status"]))
        self.assertIn("command", brief)
        self.assertFalse({"host_config", "worktree_identity", "artifact_policy", "t_submitted"} & set(brief))
        self.assertTrue(all(v is not None and v not in ("", [], {}) for v in brief.values()))
        self.assertEqual(brief["age_min"], 0.0)   # a zero is a value, not an empty field
        self.assertNotIn("delivered", cq.cmd_get(self.ctx, {"job": a, "compact": True}))   # nothing to deliver yet
        self.assertIn("t_submitted", cq.cmd_get(self.ctx, {"job": a})["job"])
        cq.finish(self.ctx, a, "done", 0, tail=["ok"],
                  artifact_digests=[{"path": "r.txt", "sha256": "a" * 64, "size_bytes": 3}])
        done = cq.cmd_get(self.ctx, {"job": a, "compact": True})["job"]
        self.assertEqual((done["status"], done["exit"], done["tail"], done["artifacts_verified"]), ("done", 0, ["ok"], 1))
        self.assertNotIn("artifact_digests", done)
        self.assertNotIn("artifact_digests", cq.cmd_wait(self.ctx, {"job": a, "timeout_s": 1})["job"])

    def test_small_text_artifacts_ride_in_the_notice(self):
        out = os.path.join(self.tmp, "out")
        os.makedirs(out)
        def put(name, data):
            path = os.path.join(out, name)
            with open(path, "wb") as fh:
                fh.write(data)
            return path
        results = put("results.tsv", b"".join(b"mod.%d\t0\t1\n" % i for i in range(40)))
        summary = put("summary.txt", b"suites=mod modules=24 failed=1\r\nmod.test_x  exit 1\x1b[0m\n\n")
        big = put("big.log", b"x" * (cq.NOTICE_TEXT_FILE_BYTES + 1))
        binary = put("frame.txt", b"\x00\x01\x02")
        image = put("frame.png", b"\x89PNG")
        job = {"status": "done", "out_paths": [results, big, binary, image, summary, os.path.join(out, "gone.txt"), 7]}
        shown = cq.artifact_text(job)
        self.assertEqual([os.path.basename(a["path"]) for a in shown], ["summary.txt", "results.tsv"])   # the summary first
        self.assertEqual(shown[0], {"path": summary, "lines": ["suites=mod modules=24 failed=1", "mod.test_x  exit 1 [0m"], "cut": False})
        self.assertEqual((len(shown[1]["lines"]), shown[1]["cut"]), (cq.NOTICE_TEXT_LINES - 2, True))
        self.assertEqual(cq.notice_data(job)["artifact_text"], shown)
        # the notice calls fetched files verified: a file that no longer has the recorded bytes is not quoted
        good = cq.hashlib.sha256(open(summary, "rb").read()).hexdigest()
        checked = dict(job, artifact_digests=[{"path": "elsewhere/summary.txt", "sha256": "1" * 64}, {"path": "results.tsv", "sha256": "0" * 64}])
        # results.tsv does not have its recorded bytes; the other record is not this summary.txt
        self.assertEqual([os.path.basename(a["path"]) for a in cq.artifact_text(checked)], ["summary.txt"])
        checked["artifact_digests"][0].update(path="summary.txt", sha256=good)
        self.assertEqual([os.path.basename(a["path"]) for a in cq.artifact_text(checked)], ["summary.txt"])
        put("summary.txt", b"edited after the fetch\n")
        self.assertEqual(cq.artifact_text(checked), [])
        self.assertNotIn("artifact_text", cq.notice_data({"status": "running", "out_paths": [summary]}))
        self.assertEqual(cq.artifact_text({"status": "done"}), [])

    def test_note_logs_a_bypass(self):
        cq.cmd_note(self.ctx, {"kind": "bypass", "session": "s", "command": "python remoterun.py"})
        self.assertEqual(self.decisions()[-1]["kind"], "bypass")


class StatusTests(Base):
    """status and list rows (times, exit, cores), the wait reason of a queued job by the planner's rules, the capacity
    summary and the submit note's estimate hint."""

    def setUp(self):
        super().setUp()
        for name, value in (("HOST_SLOTS", 8), ("SAMPLE_MAX_AGE", 120.0)):
            patch = mock.patch.object(cq, name, value)
            patch.start()
            self.addCleanup(patch.stop)
        self.host.slots = 8

    def why(self, job, c, queued=None, running=(), now=500.0):
        queued = [job] if queued is None else queued
        return cq.wait_reason(job, c, cq.reservations(queued, c, list(running), now), now)

    def hrow(self, actual, est, workstream="lane_x"):
        return {"kind": "go test", "workstream": workstream, "est": est, "actual": actual, "status": "done"}

    def history(self, rows, raw=()):
        path = self.ctx.p("history.jsonl")
        for r in rows:
            cq.append_line(path, r)
        with open(path, "a", encoding="utf-8") as fh:
            for line in raw:
                fh.write(line + "\n")

    def test_a_done_job_has_its_wait_run_age_and_estimate_ratio(self):
        t0 = self.clock[0]
        jid = self.submit("go test ./pkg", est_minutes=8)
        j = self.job(jid)
        j.update(t_launch=t0 + 60, t_started=t0 + 60)
        cq.save_job(self.ctx, j)
        cq.finish(self.ctx, jid, "done", 0, started=t0 + 60, finished=t0 + 660)
        self.advance(900)
        row = cq.status_row(self.job(jid), self.clock[0])
        self.assertEqual((row["wait_min"], row["run_min"], row["ago_min"], row["est_ratio"]), (1.0, 10.0, 4.0, 1.25))
        self.assertEqual(row["age_min"], 15.0)
        self.assertIsNone(row["elapsed_min"])
        self.assertEqual(row["reason"], "")

    def test_a_failed_job_with_an_empty_reason_shows_its_exit_and_stores_none(self):
        jid = self.submit()
        cq.finish(self.ctx, jid, "failed", 3, reason="")
        row = cq.status_row(self.job(jid), self.clock[0])
        self.assertEqual(row["reason"], "exit 3")
        self.assertEqual(self.job(jid)["reason"], "")

    def test_a_running_host_job_shows_the_cores_and_ram_the_profile_saw(self):
        t0 = self.clock[0]
        jid = self.submit()
        j = self.job(jid)
        j.update(status="running", placed_where="host", t_launch=t0 + 60, t_started=t0 + 60)
        cq.save_job(self.ctx, j)
        self.advance(360)
        cap_ = {"host": {"job_cpu": {jid: {"cores": 2.34, "rss_gb": 1.26}}}}
        row = cq.status_row(self.job(jid), self.clock[0], cap_)
        self.assertEqual((row["cores_now"], row["rss_gb_now"], row["elapsed_min"], row["wait_min"]), (2.3, 1.3, 5.0, 1.0))
        other = cq.status_row(self.job(jid), self.clock[0], {"host": {"job_cpu": {}}})
        self.assertEqual((other["cores_now"], other["rss_gb_now"]), (None, None))

    def test_a_launching_job_has_an_elapsed_time_from_its_start(self):
        t0 = self.clock[0]
        jid = self.submit()
        j = self.job(jid)
        j.update(status="launching", placed_where="host", t_launch=t0 + 90, t_started=t0 + 240)
        cq.save_job(self.ctx, j)
        self.advance(300)
        row = cq.status_row(self.job(jid), self.clock[0])
        self.assertEqual((row["elapsed_min"], row["wait_min"]), (1.0, 1.5))

    def test_list_and_status_rows_carry_the_keys_and_the_tick_rows_do_not(self):
        cq.write_json(self.ctx.p("capacity.json"), cap(at=self.clock[0]))
        queued = self.submit()
        done = self.submit("echo done")
        cq.finish(self.ctx, done, "done", 0)
        keys = {"wait_min", "run_min", "ago_min", "est_ratio", "cores_now", "rss_gb_now"}
        listed = {r["id"]: r for r in cq.cmd_list(self.ctx, {})["jobs"]}
        self.assertTrue(keys <= set(listed[queued]) and keys <= set(listed[done]))
        st = cq.cmd_status(self.ctx, {})
        self.assertEqual([r["id"] for r in st["queue"]], [queued])
        self.assertTrue(keys <= set(st["queue"][0]) and keys <= set(st["recent"][0]))
        self.assertEqual((st["queue"][0]["position"], st["queue"][0]["wait_reason"]),
                         (1, "fits now: starts at the next planning tick"))
        self.assertFalse({"position", "wait_reason"} & set(st["recent"][0]))
        out = self.tick()
        self.assertTrue(out["queued"])
        for row in out["queued"]:
            self.assertFalse(((keys | {"position", "wait_reason"}) & set(row)), row)

    def test_a_queued_job_with_no_capacity_sample_says_the_host_is_down(self):
        self.submit()
        row = cq.cmd_status(self.ctx, {})["queue"][0]
        self.assertEqual(row["wait_reason"], "host down: no sample")
        self.assertEqual(cq.cmd_status(self.ctx, {})["summary"], {"host": "no sample", "laptop": "no sample"})

    def test_host_down_paused_and_short_of_slots(self):
        down = cap(up=False, at=500.0)
        down["host"]["down_reason"] = "ssh: connect timed out"
        self.assertEqual(self.why(J("a", slots=1), down), "host down: ssh: connect timed out")
        self.assertEqual(self.why(J("a", slots=1), cap(usable=False, at=500.0)), "host paused (disk)")
        self.assertEqual(self.why(J("a", slots=2), cap(free=1, at=500.0)), "needs 2 slots, 1 free")

    def test_the_caps_name_the_load_and_the_ram_floor_as_the_planner_does(self):
        job = J("a", slots=1)
        load = cap(free=8, load="11 5 5", at=500.0)
        self.assertEqual(self.why(job, load), "load cap: load 11 + 1.5*1 slots > 12")
        self.assertEqual(self.why(job, load), cq.cap_clip_reason(load["host"], 1))
        self.assertEqual(self.why(job, cap(free=8, mem=5.0, at=500.0)), "RAM floor: 5 GB available - 2.5*1 slots < 4 GB")

    def test_a_head_held_only_by_the_caps_on_an_idle_host_fits_now(self):
        head = J("h", slots=2)
        self.assertEqual(self.why(head, cap(free=4, load="14 5 5", at=500.0, idle=2)), "fits now: starts at the next planning tick")
        self.assertIn("load cap", self.why(head, cap(free=4, load="14 5 5", at=500.0, idle=1)))

    def test_a_job_behind_a_blocked_head_waits_on_the_reservation(self):
        head = J("head", slots=4, priority="verify", t_submitted=900.0)
        small = J("small", slots=1, t_submitted=1000.0)
        c = cap(free=2, at=500.0)
        self.assertEqual(self.why(head, c, queued=[head, small]), "needs 4 slots, 2 free")
        self.assertEqual(self.why(small, c, queued=[head, small]), "reserved for head (verify, 4 slots) waiting on the host")

    def test_a_light_job_waits_on_the_light_cap_or_the_load(self):
        light = J("li", slots=0)
        full = cap(free=0, at=500.0)
        full["host"]["light_running"] = cq.LIGHT_CAP
        self.assertEqual(self.why(light, full), f"light jobs at the cap on the host ({cq.LIGHT_CAP} running)")
        self.assertEqual(self.why(light, cap(free=0, load="14 5 5", at=500.0)), "light cap: load 14 >= 12 + 2")

    def test_the_laptop_gives_its_ram_and_lock_reasons_and_its_light_cap(self):
        lap = J("l", slots=1, where="laptop")
        self.assertEqual(self.why(lap, cap(ram=4.0, at=500.0)), "laptop free RAM 4 GB < 6 GB")
        self.assertEqual(self.why(lap, cap(busy=True, ram=16.0, at=500.0)), "laptop heavy lock busy")
        self.assertEqual(self.why(lap, cap(ram=16.0, at=500.0)), "fits now: starts at the next planning tick")
        full = cap(ram=16.0, at=500.0)
        full["laptop"]["light_running"] = cq.LIGHT_CAP
        self.assertEqual(self.why(J("li", slots=0, where="laptop"), full), f"light jobs at the cap on the laptop ({cq.LIGHT_CAP} running)")

    def test_an_any_job_waits_only_when_both_machines_refuse_it(self):
        both = cap(up=False, busy=True, at=500.0)
        both["host"]["down_reason"] = "no route"
        self.assertEqual(self.why(J("x", slots=1, where="any"), both), "host down: no route | laptop: laptop heavy lock busy")
        self.assertEqual(self.why(J("x", slots=1, where="any"), cap(up=False, at=500.0)), "fits now: starts at the next planning tick")
        # a 2-slot job never fits the laptop (_fits), so a free laptop does not make it start
        down = cap(up=False, ram=16.0, at=500.0)
        down["host"]["down_reason"] = "no route"
        self.assertEqual(self.why(J("x", slots=2, where="any"), down), "host down: no route | laptop: the laptop takes only light and 1-slot jobs")

    def test_capacity_summary_gives_the_parts_the_sample_holds(self):
        c = cap(held=3, load="10.1 9 8", at=500.0)
        c["host"].update(cpu={"busy": 91.0, "sys": 14.0, "iowait": 1.0, "idle": 0.0}, runq=[40, 2],
                         psi={"cpu": {"some10": 53.0, "some60": 40.0}}, prof_age_s=0,
                         top=[{"name": "lean", "cores": 7.6, "rss_mb": 900, "n": 2}, {"name": "go-test", "cores": 1.2, "rss_mb": 300, "n": 1},
                              {"name": "python", "cores": 0.3, "rss_mb": 50, "n": 1}])
        c["laptop"].update(free_ram_gb=4.2, commit_gb=45.5, commit_limit_gb=58.3, np_gb=4.1, heavy_busy=False)
        s = cq.capacity_summary(c)
        self.assertEqual(s["host"], "3/4 slots held, load 10.1 (cap 12), cpu 91% busy (sys 14, iowait 1, idle 0), runq 40, "
                                    "psi cpu 53, top: lean 7.6, go-test 1.2, python 0.3 cores")
        self.assertEqual(s["laptop"], "4.2 GB free (heavy jobs need 6), commit 45.5/58.3 GB, nonpaged 4.1 GB")

    def test_capacity_summary_says_how_old_a_profiled_top_is_and_drops_absent_parts(self):
        c = cap(load="0.5 0.4 0.3", at=500.0)
        c["host"].update(top=[{"name": "lean", "cores": 7.6, "rss_mb": 900, "n": 2}], prof_age_s=42)
        c["laptop"] = {"free_ram_gb": 16.0, "heavy_busy": True}
        s = cq.capacity_summary(c)
        self.assertTrue(s["host"].endswith("top: lean 7.6 cores (as of 42s ago)"), s["host"])
        self.assertEqual(s["laptop"], "16 GB free (heavy jobs need 6), heavy lock busy")
        bare = cq.capacity_summary({"host": {"up": True, "usable": True, "held": 0, "total": 4, "load": "0.5 0.4 0.3", "nproc": 12}})
        self.assertEqual(bare, {"host": "0/4 slots held, load 0.5 (cap 12)", "laptop": "no sample"})
        self.assertEqual(cq.capacity_summary(None), {"host": "no sample", "laptop": "no sample"})
        down = cq.capacity_summary({"host": {"up": False, "down_reason": "ssh timed out"}})
        self.assertEqual(down["host"], "host down: ssh timed out")

    def test_est_hint_is_empty_without_history_and_with_seven_rows(self):
        self.assertEqual(cq.est_hint(self.ctx, "lane_x", 10), "")
        self.history([self.hrow(2.0, 10)] * 7)
        self.assertEqual(cq.est_hint(self.ctx, "lane_x", 10), "")

    def test_est_hint_gives_the_median_ratio_at_eight_rows_far_from_the_estimate(self):
        self.history([self.hrow(2.0, 10)] * 8)
        self.assertEqual(cq.est_hint(self.ctx, "lane_x", 10),
                         "; history: lane_x jobs ran a median 0.20x of their declared estimate (n=8), you declared 10 min")

    def test_est_hint_is_empty_when_the_median_is_near_the_estimate(self):
        self.history([self.hrow(10.0, 10, workstream="w2")] * 8)
        self.assertEqual(cq.est_hint(self.ctx, "w2", 10), "")

    def test_est_hint_skips_a_corrupt_line_and_other_workstreams(self):
        self.history([self.hrow(2.0, 10)] * 8, raw=["{not json", "[1, 2]", json.dumps(self.hrow(9.0, 10, workstream="other"))])
        self.assertIn("(n=8)", cq.est_hint(self.ctx, "lane_x", 10))

    def test_the_submit_note_gets_the_hint_only_when_one_applies(self):
        jid = self.submit()
        note = cq.cmd_submit(self.ctx, {"command": "echo x", "root": self.repo, "session": "sess-A"})["note"]
        self.assertEqual(note, f"queued at position 2 of 2; at {self.job(jid)['sha'][:12]}")
        self.history([self.hrow(2.0, 10)] * 8)
        note = cq.cmd_submit(self.ctx, {"command": "echo y", "root": self.repo, "session": "sess-A", "est_minutes": 10})["note"]
        self.assertTrue(note.endswith("; history: lane_x jobs ran a median 0.20x of their declared estimate (n=8), you declared 10 min"), note)

    def test_a_job_behind_a_head_that_takes_the_free_slots_does_not_say_fits_now(self):
        head = J("H", slots=2, priority="verify", t_submitted=900.0)
        behind = J("B", slots=2, t_submitted=1000.0)
        c, queued = cap(free=3, at=500.0), [head, behind]
        started = {e["job"] for e in cq.fallback_entries(queued, c, [], 500.0)}
        self.assertEqual(started, {"H"})
        res = cq.reservations(queued, c, [], 500.0)
        self.assertEqual(cq.wait_reason(head, c, res, 500.0, started), "fits now: starts at the next planning tick")
        self.assertEqual(cq.wait_reason(behind, c, res, 500.0, started), "waits for the jobs ahead of it: their slots are taken first")
        self.assertEqual(cq.wait_reason(behind, c, res, 500.0), "fits now: starts at the next planning tick")  # no planner set: helpers only

    def test_wait_reason_says_fits_now_exactly_for_the_jobs_the_planner_starts(self):
        def q(*specs):  # (id, slots, where, priority, submitted)
            return [J(i, slots=s, where=w, priority=p, t_submitted=t) for i, s, w, p, t in specs]

        mixes = [
            q(("H", 2, "host", "verify", 900.0), ("B", 2, "host", "lane", 1000.0), ("S", 1, "any", "lane", 1001.0),
              ("L", 0, "host", "lane", 1002.0), ("K", 1, "laptop", "lane", 1003.0)),
            q(("A", 3, "host", "lane", 900.0), ("B", 1, "host", "lane", 1000.0), ("C", 2, "any", "lane", 1001.0),
              ("D", 0, "laptop", "lane", 1002.0)),
            q(("H", 4, "host", "verify", 900.0), ("S", 1, "host", "lane", 1000.0), ("L", 0, "any", "lane", 1001.0)),
            q(("P", 1, "any", "lane", 900.0), ("Q", 1, "laptop", "lane", 1000.0), ("R", 2, "any", "lane", 1001.0)),
        ]
        running = [R("r1", 2, 30, 5)]
        scenarios = [(cap(free=free, at=500.0), mix, []) for free in range(5) for mix in mixes]
        scenarios += [(cap(free=free, at=500.0), mix, running) for free in range(3) for mix in mixes[:2]]
        scenarios += [(cap(free=4, load="11 5 5", at=500.0), mix, []) for mix in mixes]  # a capped load
        scenarios += [(cap(free=4, busy=True, at=500.0), mix, []) for mix in mixes]  # the laptop's heavy lock is held
        scenarios += [(cap(free=4, load="14 5 5", at=500.0, idle=2), mixes[2], [])]  # a head held by the caps on an idle host
        seen = set()
        for n, (c, queued, running_now) in enumerate(scenarios):
            started = {e["job"] for e in cq.fallback_entries(queued, c, running_now, 500.0)}
            res = cq.reservations(queued, c, running_now, 500.0)
            for job in queued:
                with self.subTest(scenario=n, job=job["id"]):
                    self.assertEqual(cq.wait_reason(job, c, res, 500.0, started).startswith("fits now"), job["id"] in started)
                seen.add(job["id"] in started)
        self.assertEqual(seen, {True, False})  # the scenarios hold both kinds of job

    def test_est_hint_leaves_out_failed_runs_and_split_parts(self):
        self.history([self.hrow(2.0, 10)] * 3 + [dict(self.hrow(2.0, 10), status="failed")] * 2
                     + [dict(self.hrow(2.0, 10), parent="cq-parent")] * 3)
        self.assertEqual(cq.est_hint(self.ctx, "lane_x", 10), "")  # 8 rows, 3 of them done: too few

    def crow(self, actual, est, **kw):
        return dict(self.hrow(actual, est), cmd="go test ./...", where="host", **kw)

    def test_est_calibration_needs_three_rows_of_the_command_workstream_and_machine_and_reads_the_last_eight(self):
        cal = lambda where="host": cq.est_calibration(self.ctx, "go test ./...", "lane_x", where)
        self.assertIsNone(cal())
        self.history([self.crow(5.0, 10)] * 2 + [self.crow(1.0, 10, status="failed"), self.crow(1.0, 10, parent="cq-p"),
                                                 dict(self.crow(1.0, 10), cmd="go test -race ./..."), dict(self.crow(1.0, 10), workstream="w2"),
                                                 dict(self.crow(1.0, 10), where="laptop"), self.hrow(1.0, 10)], raw=["{not json"])
        self.assertIsNone(cal())  # 2 matching rows: the others differ in status, part, command, workstream, machine or lack the key
        self.assertEqual(cal("any")[1], 3)  # a job that may run on either machine counts the laptop row too
        self.history([self.crow(5.0, 10)])
        self.assertEqual(cal(), (0.5, 3, 0.5))
        self.history([self.crow(20.0, 10)] * 8)  # the older 0.5x rows fall out of the last eight
        self.assertEqual(cal(), (2.0, 8, 2.0))

    def test_est_calibration_clamps_the_scale_and_keeps_the_median(self):
        self.history([self.crow(0.1, 10)] * 3)
        self.assertEqual(cq.est_calibration(self.ctx, "go test ./...", "lane_x", "host"), (0.25, 3, 0.01))
        self.history([self.crow(500.0, 10)] * 8)
        self.assertEqual(cq.est_calibration(self.ctx, "go test ./...", "lane_x", "host"), (4.0, 8, 50.0))

    def test_cmd_key_drops_assignments_and_keeps_six_words(self):
        self.assertEqual(cq.cmd_key("GOFLAGS=-p=2 go test -race -count=1 -run X ./a/... ./b"), "go test -race -count=1 -run X")
        self.assertNotEqual(cq.cmd_key("go test -run X ./a"), cq.cmd_key("go test -race ./..."))

    def test_submit_sets_est_sched_from_the_calibration_and_says_so(self):
        plain = self.submit("go test ./...", est_minutes=40)
        self.assertNotIn("est_sched", self.job(plain))
        self.history([self.crow(5.0, 10)] * 3)
        res = cq.cmd_submit(self.ctx, {"command": "go test ./...", "root": self.repo, "session": "sess-A", "est_minutes": 40})
        job = self.job(res["job"])
        self.assertEqual((job["est_minutes"], job["est_sched"]), (40, 20))
        self.assertIn("; ordered as 20 min: the last 3 runs of this command in lane_x took a median 0.50x of their estimates", res["note"])
        self.assertNotIn("clamped", res["note"])
        self.assertEqual(cq.job_summary(job, self.clock[0])["est_sched"], 20)
        self.history([self.crow(0.1, 10)] * 8)
        note = cq.cmd_submit(self.ctx, {"command": "go test ./...", "root": self.repo, "session": "sess-A", "est_minutes": 40})["note"]
        self.assertIn("took a median 0.01x of their estimates (scale clamped to 0.25x)", note)

    def test_finish_records_the_cmd_key_that_calibration_reads(self):
        jid = self.submit("GOFLAGS=-p=2 go test ./...", est_minutes=10)
        cq.finish(self.ctx, jid, "done", 0, started=self.clock[0] - 300, finished=self.clock[0])
        row = json.loads(cq.read_lines(self.ctx.p("history.jsonl"))[-1])
        self.assertEqual((row["cmd"], row["kind"]), ("go test ./...", "go test"))

    def test_the_start_order_uses_est_sched_and_backfill_the_declared_estimate(self):
        short, long_ = J("a", est_minutes=60, est_sched=5, t_submitted=1000.0), J("b", est_minutes=10, t_submitted=999.0)
        self.assertEqual([j["id"] for j in sorted([long_, short], key=lambda j: cq.start_key(j, 1000.0))], ["a", "b"])
        self.assertEqual(cq.sched_est(J("c", est_minutes=7)), 7)
        r = {"window_min": 100}
        self.assertFalse(cq.can_backfill(J("l", est_minutes=60, est_sched=20), r))  # a median is no bound on the run
        self.assertTrue(cq.can_backfill(J("l", est_minutes=10, est_sched=40), r))

    def test_the_head_window_takes_the_smaller_of_the_two_estimates(self):
        head = J("h", slots=3)
        for sched, left in ((60, 40), (400, 280), (None, 280)):
            running = R("r", 3, est=300, elapsed_min=20)
            if sched is not None:
                running["est_sched"] = sched
            with self.subTest(sched=sched):
                self.assertEqual(cq.head_window(head, cap(free=1), [running], 2000.0), left * cq.BACKFILL_SHADOW_FACTOR)

    def test_finish_marks_a_split_part_in_the_history_and_leaves_an_ordinary_row_alone(self):
        ordinary, part = self.submit("echo a", est_minutes=5), self.submit("echo b", est_minutes=5)
        j = self.job(part)
        j["parent"] = "cq-parent"
        cq.save_job(self.ctx, j)
        cq.finish(self.ctx, ordinary, "done", 0, started=self.clock[0] - 60, finished=self.clock[0])
        cq.finish(self.ctx, part, "done", 0, started=self.clock[0] - 60, finished=self.clock[0])
        rows = [json.loads(l) for l in cq.read_lines(self.ctx.p("history.jsonl"))]
        self.assertEqual(set(rows[0]), {"kind", "cmd", "workstream", "est", "actual", "slots", "where", "exit", "status", "t"})
        self.assertEqual(rows[1]["parent"], "cq-parent")
        self.assertEqual(set(rows[1]), set(rows[0]) | {"parent"})  # the part row is the ordinary row plus the mark

    def test_an_estimate_hint_that_raises_leaves_the_submit_note_intact(self):
        with mock.patch.object(cq, "est_hint", side_effect=RuntimeError("bad history")):
            r = cq.cmd_submit(self.ctx, {"command": "echo x", "root": self.repo, "session": "sess-A"})
        self.assertTrue(r["ok"])
        self.assertTrue(r["note"].startswith("queued at position 1 of 1; at "), r["note"])
        self.assertEqual(self.job(r["job"])["status"], "queued")

    def krow(self, cores, where="host", slots=1, status="done", kind="go test", **kw):
        return dict({"kind": kind, "workstream": "lane_x", "est": 10, "actual": 10, "slots": slots, "where": where,
                     "exit": 0, "status": status, "t": "2026-10-20T10:00:00Z", "cores": cores, "cpu_s": 60}, **kw)

    def test_kind_hint_says_when_the_reserved_slots_are_far_above_the_cores_used(self):
        self.history([self.krow(1.0)] * 8)
        with mock.patch.object(cq, "SLOT_VCPU", 1.5):
            self.assertEqual(cq.kind_hint(self.ctx, "go test", 4, 10),
                             "; history: 8 recent 'go test' runs used p75 1.0 cores, you reserved 4 slots (~6 vCPU): about 1 slots fits")

    def test_kind_hint_is_empty_when_the_reserved_slots_are_below_the_cores_used(self):
        self.history([self.krow(4.0)] * 8)
        with mock.patch.object(cq, "SLOT_VCPU", 1.5):
            self.assertEqual(cq.kind_hint(self.ctx, "go test", 1, 10), "")  # reserved 1.5 vCPU under p75 4.0: not advised

    def test_kind_hint_adds_the_median_ratio_when_it_is_outside_the_band(self):
        self.history([self.krow(1.0, actual=2.0)] * 8)
        with mock.patch.object(cq, "SLOT_VCPU", 1.5):
            self.assertEqual(cq.kind_hint(self.ctx, "go test", 4, 10),
                             "; history: 8 recent 'go test' runs used p75 1.0 cores, you reserved 4 slots (~6 vCPU): about 1 slots fits"
                             " and ran a median 0.20x of their estimate")

    def test_kind_hint_leaves_out_the_ratio_when_it_is_inside_the_band(self):
        self.history([self.krow(1.0)] * 8)  # actual equals est
        with mock.patch.object(cq, "SLOT_VCPU", 1.5):
            self.assertEqual(cq.kind_hint(self.ctx, "go test", 4, 10),
                             "; history: 8 recent 'go test' runs used p75 1.0 cores, you reserved 4 slots (~6 vCPU): about 1 slots fits")

    def test_kind_hint_is_empty_when_the_reservation_fits_and_for_slots_0(self):
        self.history([self.krow(3.0)] * 8)
        with mock.patch.object(cq, "SLOT_VCPU", 1.5):
            self.assertEqual(cq.kind_hint(self.ctx, "go test", 2, 10), "")  # reserved 3 vCPU, p75 3.0
            self.assertEqual(cq.kind_hint(self.ctx, "go test", 0, 10), "")  # a light job holds no slot

    def test_kind_hint_needs_eight_done_host_rows_of_its_own_kind(self):
        self.history([self.krow(1.0)] * 7)
        self.assertEqual(cq.kind_hint(self.ctx, "go test", 4, 10), "")
        self.history([self.krow(1.0)])
        self.assertEqual(cq.kind_hint(self.ctx, "go test", 4, 10),
                         "; history: 8 recent 'go test' runs used p75 1.0 cores, you reserved 4 slots (~6 vCPU): about 1 slots fits")

    def test_kind_hint_does_not_count_laptop_failed_or_other_kind_rows(self):
        self.history([self.krow(1.0, where="laptop")] * 8 + [self.krow(1.0, status="failed")] * 8
                     + [self.krow(1.0, kind="other")] * 8 + [self.krow(1.0)] * 7)
        self.assertEqual(cq.kind_hint(self.ctx, "go test", 4, 10), "")  # only 7 host done rows of this kind

    def test_the_submit_note_gives_no_kind_hint_to_a_laptop_job(self):
        self.history([self.krow(0.5)] * 8)  # host runs of this kind used half a core, so 1 slot (1.5 vCPU) is over-reserved
        with mock.patch.object(cq, "SLOT_VCPU", 1.5):
            host = cq.cmd_submit(self.ctx, {"command": "go test ./pkg", "root": self.repo, "session": "sess-A", "slots": 1, "est_minutes": 10})
            laptop = cq.cmd_submit(self.ctx, {"command": "go test ./pkg", "root": self.repo, "session": "sess-A", "slots": 1, "where": "laptop",
                                              "est_minutes": 10})
        self.assertIn("; history: 8 recent 'go test' runs used p75 0.5 cores", host["note"])
        self.assertNotIn("history:", laptop["note"])

    def test_the_submit_note_gets_the_kind_hint_past_malformed_history_and_never_fails(self):
        self.history([self.krow(1.0)] * 8, raw=["{not json", "[1, 2]", json.dumps(self.krow("x")), json.dumps(self.krow(0)),
                                                '{"kind": "go test", "where": "host", "status": "done", "cores": NaN}'])
        with mock.patch.object(cq, "SLOT_VCPU", 1.5):
            r = cq.cmd_submit(self.ctx, {"command": "go test ./pkg", "root": self.repo, "session": "sess-A", "slots": 4, "est_minutes": 10})
        self.assertTrue(r["note"].endswith("; history: 8 recent 'go test' runs used p75 1.0 cores, you reserved 4 slots (~6 vCPU): about 1 slots fits"), r["note"])
        with mock.patch.object(cq, "kind_hint", side_effect=RuntimeError("bad history")):
            r = cq.cmd_submit(self.ctx, {"command": "go test ./pkg", "root": self.repo, "session": "sess-A", "slots": 4, "est_minutes": 10})
        self.assertTrue(r["ok"])
        self.assertTrue(r["note"].startswith("queued at position 2 of 2; at "), r["note"])

    def test_cmd_status_stays_json_serialisable_with_a_sample_and_every_state(self):
        self.submit()
        done = self.submit("echo done")
        cq.finish(self.ctx, done, "failed", 3)
        cq.write_json(self.ctx.p("capacity.json"), cap(held=1, at=self.clock[0]))
        st = cq.cmd_status(self.ctx, {})
        json.dumps(st)
        self.assertEqual(st["queue"][0]["wait_reason"], "fits now: starts at the next planning tick")
        self.assertEqual(st["recent"][0]["reason"], "exit 3")
        self.assertIn("slots held", st["summary"]["host"])


class HostConfTests(Base):
    """host.conf.json: the keys that validate, the precedence env over file over default, the capacity host block that
    the planner text follows, and the SLOTS line of the probe."""

    VALID = {"name": "lane-ex44", "address": "198.51.100.11", "slots": 13, "slot_vcpu": 1.5, "slot_ram_gb": 2.5,
             "ram_floor_gb": 6, "load_cap": 20}

    def write_conf(self, text, state="conf"):
        d = os.path.join(self.tmp, state)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, cq.HOST_CONF_FILE), "w", encoding="utf-8") as f:
            f.write(text)
        return d

    def test_a_missing_file_gives_no_settings_and_an_unreadable_one_only_refuses_the_sandbox(self):
        # missing: every default. There but unreadable: every default too, except that a sandbox the file may have
        # required must not read as off, so the one setting left says "invalid" and sandbox_plan refuses every job
        unreadable = {"sandbox": "invalid"}
        self.assertEqual(cq.host_conf(os.path.join(self.tmp, "absent")), {})
        os.makedirs(os.path.join(self.tmp, "dir", cq.HOST_CONF_FILE))  # a directory where the file should be
        self.assertEqual(cq.host_conf(os.path.join(self.tmp, "dir")), unreadable)
        with self.assertRaises(cq.SandboxError):
            cq.sandbox_plan({}, cq.host_conf(os.path.join(self.tmp, "dir")))
        for text in ("{", "not json", "", "[13]", '"lane-ex44"', "13", "null", "NaN"):
            self.assertEqual(cq.host_conf(self.write_conf(text)), unreadable, text)
        with open(os.path.join(self.tmp, "conf", cq.HOST_CONF_FILE), "wb") as f:
            f.write(b"\xff\xfe{}")  # not UTF-8
        self.assertEqual(cq.host_conf(os.path.join(self.tmp, "conf")), unreadable)

    def test_every_valid_key_is_kept_and_unknown_keys_are_ignored(self):
        self.assertEqual(cq.host_conf(self.write_conf(json.dumps(self.VALID))), self.VALID)
        self.assertEqual(cq.host_conf(self.write_conf('{"name": "  lane-ex44 ", "ram_floor_gb": 0, "slots": 1}')),
                         {"name": "lane-ex44", "ram_floor_gb": 0, "slots": 1})
        self.assertEqual(cq.host_conf(self.write_conf('{"slots": 32, "color": "red"}')), {"slots": 32})

    def test_each_invalid_value_is_ignored_on_its_own_and_the_other_keys_survive(self):
        bad = {"name": ["", "   ", 5, None], "address": ["", 5, ["x"]], "slots": [0, 33, 2.5, "13", True, None],
               "slot_vcpu": [0, -1, "1.5", True, float("nan")], "slot_ram_gb": [0, -2.5, "2.5", False],
               "ram_floor_gb": [-1, "6", True, float("inf")], "load_cap": [0, -3, "20", False]}
        for key, values in bad.items():
            for v in values:
                self.assertEqual(cq.host_conf(self.write_conf(json.dumps({key: v}))), {}, (key, v))
                if key != "slots":
                    self.assertEqual(cq.host_conf(self.write_conf(json.dumps({key: v, "slots": 13}))), {"slots": 13}, (key, v))

    def test_booleans_are_not_numbers_and_the_slot_range_is_1_to_32(self):
        self.assertEqual(cq.host_conf(self.write_conf('{"slots": true, "load_cap": true, "ram_floor_gb": false}')), {})
        self.assertEqual(cq.host_conf(self.write_conf('{"ram_floor_gb": 0, "slot_vcpu": 0.5}')), {"ram_floor_gb": 0, "slot_vcpu": 0.5})
        self.assertEqual(cq.host_conf(self.write_conf('{"slots": 1}')), {"slots": 1})
        self.assertEqual(cq.host_conf(self.write_conf('{"slots": 33}')), {})

    def test_paused_is_a_boolean_and_anything_else_is_ignored(self):
        self.assertEqual(cq.host_conf(self.write_conf('{"paused": true}')), {"paused": True})
        self.assertEqual(cq.host_conf(self.write_conf('{"paused": false}')), {"paused": False})
        for v in ('"true"', '"yes"', "1", "0", "null", "[true]", '{"on": true}'):
            self.assertEqual(cq.host_conf(self.write_conf('{"paused": %s}' % v)), {}, v)
        self.assertEqual(cq.host_conf(self.write_conf('{"paused": 1, "slots": 13}')), {"slots": 13})

    def test_the_planner_is_haiku_only_when_host_conf_says_so_and_anything_else_is_code(self):
        self.assertEqual(cq.host_planner({}), "code")
        self.assertEqual(cq.host_planner({"planner": "code"}), "code")
        self.assertEqual(cq.host_planner({"planner": "haiku"}), "haiku")
        self.assertEqual(cq.host_conf(self.write_conf('{"planner": " haiku "}')), {"planner": "haiku"})
        for v in ("Haiku", "gpt", "", None, 1, True, ["haiku"]):
            self.assertEqual(cq.host_conf(self.write_conf(json.dumps({"planner": v}))), {}, repr(v))
            self.assertEqual(cq.host_planner(cq.host_conf(self.write_conf(json.dumps({"planner": v})))), "code", repr(v))

    def test_precedence_for_the_address_is_env_then_file_then_default(self):
        conf = cq.host_conf(self.write_conf('{"address": "me@198.51.100.11"}'))
        self.assertEqual(cq.host_address({}, conf), "me@198.51.100.11")
        self.assertEqual(cq.host_address({"LANE_HOST": "198.51.100.12"}, conf), "198.51.100.12")
        self.assertEqual(cq.host_address({"LANE_HOST": "198.51.100.12", "CQ_HOST": "x@y"}, conf), "x@y")
        self.assertEqual(cq.host_address({}, cq.host_conf(os.path.join(self.tmp, "none"))), "")
        self.assertEqual(cq.host_address({}, {"address": "198.51.100.13"}), "198.51.100.13")

    def test_precedence_for_the_load_cap_and_the_ram_floor_is_env_then_file_then_default(self):
        conf = {"load_cap": 20.0, "ram_floor_gb": 6}
        self.assertEqual(cq.host_load_cap({"CQ_LOAD_CAP": "16"}, conf), 16.0)
        self.assertEqual(cq.host_load_cap({"CQ_LOAD_CAP": ""}, conf), 20.0)
        self.assertEqual(cq.host_load_cap({}, conf), 20.0)
        self.assertIsNone(cq.host_load_cap({}, {}))  # None: the host's nproc
        self.assertEqual(cq.host_ram_floor_gb({"CQ_HOST_RAM_FLOOR_GB": "3"}, conf), 3.0)
        self.assertEqual(cq.host_ram_floor_gb({}, conf), 6.0)
        self.assertEqual(cq.host_ram_floor_gb({}, {}), 4.0)
        self.assertEqual(cq.host_ram_floor_gb({}, {"ram_floor_gb": 0}), 0.0)

    def test_the_capacity_host_block_carries_the_five_planner_facts(self):
        patches = [mock.patch.object(cq, name, value) for name, value in
                   (("HOST_NAME", "lane-ex44"), ("LOAD_CAP", None), ("SLOT_VCPU", 1.25), ("SLOT_RAM_GB", 3.0), ("HOST_RAM_FLOOR_GB", 6.0))]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        h = cq.cmd_probe(self.ctx, {})["capacity"]["host"]
        self.assertEqual((h["name"], h["slot_vcpu"], h["slot_ram_gb"], h["ram_floor_gb"], h["load_cap"]), ("lane-ex44", 1.25, 3.0, 6.0, 12))
        with mock.patch.object(cq, "LOAD_CAP", 16.0):
            self.assertEqual(cq.cmd_probe(self.ctx, {})["capacity"]["host"]["load_cap"], 16.0)

    def test_parse_snapshot_accepts_1_to_32_slots(self):
        self.assertEqual(cq.parse_snapshot("PROBE\nSLOTS 13\nEND\n")["slots"], 13)
        self.assertEqual(cq.parse_snapshot("PROBE\nSLOTS 32\nEND\n")["slots"], 32)
        self.assertEqual(cq.parse_snapshot("PROBE\nSLOTS 33\nEND\n")["slots"], cq.HOST_SLOTS)
        self.assertEqual(cq.parse_snapshot("PROBE\nSLOTS 0\nEND\n")["slots"], cq.HOST_SLOTS)

    def test_a_down_host_carries_the_five_planner_facts_too(self):
        patches = [mock.patch.object(cq, name, value) for name, value in
                   (("HOST_NAME", "lane-ex44"), ("LOAD_CAP", None), ("SLOT_VCPU", 1.25), ("SLOT_RAM_GB", 3.0), ("HOST_RAM_FLOOR_GB", 6.0))]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.host.up = False
        h = cq.cmd_probe(self.ctx, {})["capacity"]["host"]
        self.assertFalse(h["up"])
        self.assertEqual((h["name"], h["slot_vcpu"], h["slot_ram_gb"], h["ram_floor_gb"], h["load_cap"]), ("lane-ex44", 1.25, 3.0, 6.0, None))
        with mock.patch.object(cq, "LOAD_CAP", 16.0):
            self.assertEqual(cq.cmd_probe(self.ctx, {})["capacity"]["host"]["load_cap"], 16.0)

    def test_the_launch_gets_the_probe_host_as_lane_host_whatever_the_environment_says(self):
        # remoterun.py launches on LANE_HOST; a launch on another host than the probe's and kill's would split the queue
        with mock.patch.object(cq, "HOST", "me@192.0.2.7"), mock.patch.dict(os.environ, {"LANE_HOST": "198.51.100.14"}):
            with mock.patch.object(cq, "_run", return_value=subprocess.CompletedProcess([], 0, b"", b"")) as run:
                self.assertEqual(cq.real_run_remoterun(["--rev", "abc"], self.tmp), (0, "", ""))
        self.assertEqual(run.call_args.kwargs["env"]["LANE_HOST"], "me@192.0.2.7")
        argv = run.call_args.args[0]
        self.assertEqual(argv[0], sys.executable)
        self.assertEqual(argv[-3:], [cq.REMOTERUN, "--rev", "abc"])
        if os.name == "nt":
            self.assertEqual(os.path.basename(argv[1]), "quiet_helper.py")
        self.assertEqual(run.call_args.kwargs["cwd"], self.tmp)
        self.assertEqual(run.call_args.kwargs["stdin"], subprocess.DEVNULL)
        self.assertEqual(run.call_args.kwargs["timeout"], 540)

    def test_a_value_the_checks_cannot_compare_or_a_file_nested_too_deep_gives_no_settings(self):
        big = "1" + "0" * 400  # math.isfinite raises OverflowError on this integer
        for key in ("slot_vcpu", "slot_ram_gb", "ram_floor_gb", "load_cap"):
            self.assertEqual(cq.host_conf(self.write_conf('{"%s": %s}' % (key, big))), {}, key)
        self.assertEqual(cq.host_conf(self.write_conf('{"slot_vcpu": %s, "slots": 13}' % big)), {"slots": 13})
        deep = "[" * 200000 + "]" * 200000  # json.load raises RecursionError on this
        self.assertEqual(cq.host_conf(self.write_conf(deep)), {"sandbox": "invalid"})  # unreadable: see the test above
        self.assertEqual(cq.host_conf(self.write_conf('{"slots": 13, "extra": %s}' % deep)), {"sandbox": "invalid"})

    def test_the_bounds_are_inclusive_and_stop_absurd_values(self):
        cases = [("slot_vcpu", 0.1, True), ("slot_vcpu", 64, True), ("slot_vcpu", 0.09, False), ("slot_vcpu", 64.5, False),
                 ("slot_ram_gb", 0.1, True), ("slot_ram_gb", 1024, True), ("slot_ram_gb", 0.05, False), ("slot_ram_gb", 1024.5, False),
                 ("ram_floor_gb", 0, True), ("ram_floor_gb", 1024, True), ("ram_floor_gb", -0.5, False), ("ram_floor_gb", 1025, False),
                 ("load_cap", 1, True), ("load_cap", 1024, True), ("load_cap", 0.5, False), ("load_cap", 1025, False),
                 ("slots", 1, True), ("slots", 32, True), ("slots", 0, False), ("slots", 33, False),
                 ("slot_vcpu", 5e-324, False), ("slot_ram_gb", 5e-324, False), ("ram_floor_gb", 5e-324, True), ("load_cap", 5e-324, False)]
        for key, v, ok in cases:
            self.assertEqual(cq.host_conf(self.write_conf(json.dumps({key: v}))), {key: v} if ok else {}, (key, v))
        for key in ("slot_vcpu", "slot_ram_gb", "ram_floor_gb", "load_cap"):
            for v in (1e308, -1e308):
                self.assertEqual(cq.host_conf(self.write_conf(json.dumps({key: v}))), {}, (key, v))

    def test_an_address_that_would_read_as_an_option_or_holds_whitespace_or_control_characters_is_ignored(self):
        for v in ("-oProxyCommand=x", "-", "me @198.51.100.11", "me@198.51.100.11 -v", "me@\t198.51.100.11", "me@198.51.100.11\x00", "me@ 198.51.100.11"):
            self.assertEqual(cq.host_conf(self.write_conf(json.dumps({"address": v}))), {}, repr(v))
        self.assertEqual(cq.host_conf(self.write_conf(json.dumps({"address": " me@198.51.100.11\n"}))), {"address": "me@198.51.100.11"})


class StagingStderrTests(Base):
    """A staging helper that exits 0 may still say something on stderr (remote_stage falls back to the full history and
    says why); the job's record keeps the end of it as staging_stderr, and status shows it."""
    FALLBACK = "[remote_stage] incremental staging failed; sending the full history"

    def launch_one(self):
        a = self.submit()
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
        cq.launch(self.ctx, a)
        return a

    def test_stderr_of_a_helper_that_succeeded_is_kept_and_shown(self):
        self.host.remoterun_ok_err = self.FALLBACK + "\n"
        a = self.launch_one()
        self.assertEqual(self.job(a)["status"], "running")
        self.assertEqual(self.job(a)["staging_stderr"], self.FALLBACK)
        self.assertEqual(cq.cmd_get(self.ctx, {"job": a, "compact": True})["job"]["staging_stderr"], self.FALLBACK)
        self.assertEqual(cq.cmd_get(self.ctx, {"job": a})["job"]["staging_stderr"], self.FALLBACK)

    def test_a_finished_job_still_shows_it(self):
        view = cq.result_view({"id": "x", "status": "done", "exit": 0, "staging_stderr": self.FALLBACK})
        self.assertEqual(view["staging_stderr"], self.FALLBACK)
        self.assertNotIn("staging_stderr", cq.result_view({"id": "x", "status": "done", "exit": 0}))

    def test_a_helper_that_said_nothing_adds_no_key(self):
        for err in ("", "  \n\r\n"):
            with self.subTest(err=err):
                self.host.jobs.clear()
                self.host.remoterun_ok_err = err
                a = self.launch_one()
                self.assertEqual(self.job(a)["status"], "running")
                self.assertNotIn("staging_stderr", self.job(a))
                self.assertNotIn("staging_stderr", cq.cmd_get(self.ctx, {"job": a, "compact": True})["job"])

    def test_a_failed_staging_is_unchanged_and_keeps_nothing(self):
        self.host.remoterun_rc, self.host.remoterun_err = 1, "error: cannot lock ref 'refs/heads/x'\n[remoterun] push failed"
        a = self.submit()
        self.tick()
        cq.dispatch(self.ctx, {"job": a, "where": "host", "slots": 1})
        cq.launch(self.ctx, a)
        self.assertEqual(self.job(a)["status"], "queued")
        self.assertNotIn("staging_stderr", self.job(a))
        self.assertEqual(cq.host_state(self.ctx)["next_probe"], self.clock[0] + 60)

    def test_the_note_is_bounded_to_its_end_and_control_characters_are_made_safe(self):
        long = "".join(f"line {i}\n" for i in range(1000))
        note = cq.staging_note(0, long)["staging_stderr"]
        self.assertEqual(len(note), cq.STAGING_STDERR_CHARS)
        self.assertTrue(note.endswith("line 999"))
        self.assertEqual(cq.staging_note(0, "a\x1b[31mb\x00c\r\nd\te\x7f\x85")["staging_stderr"], "a?[31mb?c\nd\te??")
        self.assertEqual(cq.staging_note(0, "xxxxx")["staging_stderr"], "xxxxx")

    def test_only_an_exit_of_zero_makes_a_note(self):
        for rc in (1, 17, 75, 255):
            self.assertEqual(cq.staging_note(rc, "something\n"), {})
        self.assertEqual(cq.staging_note(0, None), {})

    def test_the_helper_runner_hands_back_stderr_of_a_zero_exit(self):
        done = subprocess.CompletedProcess([], 0, b"CQ_LAUNCHED x\n", (self.FALLBACK + "\n").encode())
        with mock.patch.object(cq, "HOST", "me@192.0.2.7"), mock.patch.object(cq, "_run", return_value=done):
            rc, out, err = cq.real_run_remoterun(["--rev", "abc"], self.tmp)
        self.assertEqual((rc, cq.staging_note(rc, err)), (0, {"staging_stderr": self.FALLBACK}))


class HostPauseTests(Base):
    """host.conf.json "paused": no job is dispatched to the host while it is set, a where=any job still goes to the laptop,
    a host job already running is reaped, and the status text names the config pause."""

    def setUp(self):
        super().setUp()
        patch = mock.patch.object(cq, "HOST_PAUSED", True)
        patch.start()
        self.addCleanup(patch.stop)

    def test_the_guard_starts_nothing_on_the_host_and_a_where_any_job_still_goes_to_the_laptop(self):
        h = self.submit("go test ./pkg")
        b = self.submit("echo hi", where="any", slots=0)
        t = self.tick()
        self.assertTrue(t["capacity"]["host"]["paused"])
        self.assertEqual(t["capacity"]["host"]["pause"], "host.conf.json")
        self.assertFalse(t["capacity"]["host"]["usable"])
        r = cq.cmd_apply(self.ctx, {"plan": None, "reason": "x"})
        self.assertEqual([a["job"] for a in r["accepted"]], [b])
        self.assertEqual(r["accepted"][0]["where"], "laptop")
        self.assertEqual(self.job(h)["status"], "queued")
        self.assertEqual(self.host.launches, [])

    def test_a_host_job_already_running_is_reaped_when_it_finishes(self):
        a = self.submit("go test ./pkg")
        with mock.patch.object(cq, "HOST_PAUSED", False):
            self.tick()
            cq.cmd_apply(self.ctx, {"plan": None})
        self.assertEqual(self.job(a)["status"], "running")
        self.host.tails[a] = ["PASS", "ok  pkg 1.2s"]
        self.host.set_done(a, 0)
        self.advance(5)
        self.assertIn(a, self.tick()["ended"])
        self.assertEqual(self.job(a)["status"], "done")

    def test_the_status_names_the_config_pause_and_a_disk_pause_stays_disk(self):
        h = self.submit("go test ./pkg")
        self.tick()
        st = cq.cmd_status(self.ctx, {})
        self.assertIn("paused (host.conf.json)", st["summary"]["host"])
        row = next(r for r in st["queue"] if r["id"] == h)
        self.assertEqual(row["wait_reason"], "host paused (host.conf.json)")
        with mock.patch.object(cq, "HOST_PAUSED", False):
            hs = cq.host_state(self.ctx)  # the disk pause a lanerun exit 75 sets; the disk stays below the floor, so it holds
            hs["disk_paused"] = True
            cq.save_host_state(self.ctx, hs)
            self.host.disk_kb = 1024
            self.tick()
            # the pause is the disk's, and no config pause is named
            st = cq.cmd_status(self.ctx, {})
            self.assertIn("paused (disk)", st["summary"]["host"])
            self.assertNotIn("host.conf.json", st["summary"]["host"])


class SshRetryTests(unittest.TestCase):
    """real_ssh retries once, at once, on a connection-level 255 (a brief drop on the laptop's path); never on a refusal or a hung command."""

    @staticmethod
    def completed(rc, err=b"", out=b""):
        return subprocess.CompletedProcess([], rc, out, err)

    def run_ssh(self, timeout, *results):
        with mock.patch.object(cq, "_run", side_effect=list(results)) as run:
            out = cq.real_ssh("echo PROBE", timeout=timeout, host="test-host")
        return out, run

    def test_a_connection_timeout_is_retried_once_with_the_same_argv_and_the_second_result_is_returned(self):
        (rc, out, err), run = self.run_ssh(60, self.completed(255, b"ssh: connect to host h port 22: Connection timed out"),
                                           self.completed(0, b"", b"PROBE\n"))
        self.assertEqual((rc, out, err), (0, "PROBE\n", ""))
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args_list[0].args, run.call_args_list[1].args)
        self.assertEqual(run.call_args_list[0].kwargs["timeout"], 60)
        self.assertTrue(15 <= run.call_args_list[1].kwargs["timeout"] <= 60)

    def test_the_retry_gets_at_least_15_s_when_the_original_timeout_is_nearly_spent(self):
        msg = b"ssh: connect to host h port 22: Connection timed out"
        _, run = self.run_ssh(10, self.completed(255, msg), self.completed(255, msg))
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args_list[1].kwargs["timeout"], 15)

    def test_each_transient_stderr_is_retried_and_a_second_failure_is_returned(self):
        for msg in (b"ssh: connect to host 198.51.100.17 port 22: Connection timed out",
                    b"ssh: connect to host 198.51.100.17 port 22: Connection refused",
                    b"kex_exchange_identification: read: Connection reset"):
            with self.subTest(msg=msg):
                (rc, out, err), run = self.run_ssh(60, self.completed(255, msg), self.completed(255, msg))
                self.assertEqual((rc, err), (255, msg.decode()))
                self.assertEqual(run.call_count, 2)

    def test_a_drop_after_the_command_may_have_started_is_not_retried(self):
        for msg in (b"Connection reset by 198.51.100.17 port 22", b"Connection closed by 198.51.100.17 port 22",
                    b"client_loop: send disconnect: Connection reset"):
            with self.subTest(msg=msg):
                (rc, _, _), run = self.run_ssh(60, self.completed(255, msg))
                self.assertEqual((rc, run.call_count), (255, 1))

    def test_a_refusal_is_not_retried(self):
        (rc, _, _), run = self.run_ssh(60, self.completed(255, b"Permission denied (publickey)."))
        self.assertEqual(rc, 255)
        self.assertEqual(run.call_count, 1)

    def test_a_nonzero_remote_exit_is_not_retried(self):
        (rc, _, _), run = self.run_ssh(60, self.completed(1, b"Connection timed out"))
        self.assertEqual(rc, 1)
        self.assertEqual(run.call_count, 1)

    def test_a_hung_command_that_times_out_is_not_retried(self):
        (rc, out, err), run = self.run_ssh(60, subprocess.TimeoutExpired("ssh", 60), self.completed(0))
        self.assertEqual((rc, out, err), (255, "", "ssh timed out"))
        self.assertEqual(run.call_count, 1)


class ImportWiringTests(unittest.TestCase):
    """cq.py reads host.conf.json at import: what a fresh process gets for the host constants from the state it is given."""

    PRINT = "import sys; sys.path.insert(0, %r); import cq; print(cq.HOST, cq.HOST_NAME, cq.HOST_SLOTS, cq.SLOT_VCPU, cq.SLOT_RAM_GB, cq.HOST_RAM_FLOOR_GB, cq.LOAD_CAP, cq.PLANNER)"

    def run_import(self, state):
        env = {k: os.environ[k] for k in ("PATH", "SYSTEMROOT", "TEMP", "TMP") if k in os.environ}  # a clean environment otherwise
        env["CQ_STATE"] = state
        bin_dir = os.path.dirname(os.path.abspath(__file__))
        r = subprocess.run([sys.executable, "-c", self.PRINT % bin_dir], env=env, cwd=state, capture_output=True, text=True, timeout=120)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout.strip()

    def test_a_host_conf_file_sets_the_constants_and_its_absence_or_a_malformed_file_gives_the_defaults(self):
        state = tempfile.mkdtemp(prefix="cqtest-import")
        self.addCleanup(cq.rmtree_force, state)
        with open(os.path.join(state, cq.HOST_CONF_FILE), "w", encoding="utf-8") as f:
            json.dump({"name": "lane-ex44", "address": "builder@203.0.113.9", "slots": 13, "slot_vcpu": 2, "slot_ram_gb": 3,
                       "ram_floor_gb": 6, "load_cap": 20, "planner": "haiku"}, f)
        self.assertEqual(self.run_import(state), "builder@203.0.113.9 lane-ex44 13 2.0 3.0 6.0 20.0 haiku")
        with open(os.path.join(state, cq.HOST_CONF_FILE), "w", encoding="utf-8") as f:
            f.write("not json")
        self.assertEqual(self.run_import(state), "ember 8 1.5 2.5 4.0 None code")
        os.remove(os.path.join(state, cq.HOST_CONF_FILE))
        self.assertEqual(self.run_import(state), "ember 8 1.5 2.5 4.0 None code")


if __name__ == "__main__":
    unittest.main()
