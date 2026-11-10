#!/usr/bin/env python3
"""Read-only utilisation and queue-wait report for the compute queue.

Reads from the state directory (--state, CQ_STATE, BATON_STATE/compute, or
the main checkout .state/compute):
  jobs/*.json            one file per job
  deferred-jobs/*.json   ordinary deferred ("spare" and "overnight") jobs
  fleet/jobs/*.json      generation-1 fleet jobs
  queue-v2/              the current generation: its own jobs/, deferred-jobs/, fleet/jobs/,
                         history.jsonl and samples.jsonl, read as well unless --state is
                         itself a queue-v2 directory (which then supplies its own files)
  hosts.json             optional shard registry; each enabled shard directory under the state
                         directory holds the same job stores for another host
  history.jsonl     one row per finished run (estimate and actual minutes)
  decisions.jsonl   one row per scheduler plan (accepted and refused jobs)
  samples.jsonl     one row a minute from the leader's tick (capacity and queue state); samples.jsonl.1 is read first when present
  host.conf.json    optional; its slots give the default --slots (see host_slots) and its name the primary host

Never writes: the state directory is only read. Output is GitHub-markdown
tables on stdout, or one JSON object with --json. Times are UTC.

Conventions: percentiles interpolate linearly between order statistics
(numpy's default). Run minutes are t_finished - t_started; running jobs end at
the report time. Held slot-minutes start at t_launch (slots are taken while a
job launches), else t_started, and end at t_finished. The host pool is
placed_where == "host" with slots_eff > 0. Split parents (a non-empty
"children" list) are counted in job tallies but excluded from every timing and
slot sum. A job's host is its host_id (aliases from hosts.json applied), else its shard,
else the primary host named by host.conf.json (default "ember"); the host pool sections
describe the primary host only. Waiting means t_submitted <= t < t_launch (else t_started); jobs that
never launched or started have no waiting interval.
"""

import argparse
import collections
import datetime as dt
import json
import math
import os
import re
import sys
import time
import zoneinfo

import queue_paths

DEFAULT_STATE = queue_paths.default_state()
DAY = 86400.0
MINUTE = 60.0
SAMPLE_GAP_CAP = 3 * MINUTE  # 3 x cq.SAMPLE_LOG_EVERY: a sample row is weighted by its gap to the next row, at most this
FAILED = ("failed", "timeout")
BUCKETS = ("reservation", "slots", "load cap", "RAM floor", "light cap",
           "stale sample", "other")
SAMPLE_STATES = ("idle: nothing queued", "slot-saturated: queued, free 0", "cap-blocked: queued, free > 0, room 0",
                 "dispatch latency: a fitting job waits, nothing blocks", "reservation: head waits for a bigger job's slots",
                 "caps: head would fit the free slots, the caps refuse")
CPU_MEASURES = ("cpu_busy", "cpu_sys", "cpu_iowait", "cpu_idle", "runq", "psi_cpu10")
TOP_CLASSES = 8
GO_BUILD_CLASSES = ("go-tool", "go-compile", "go-link", "go-vet")
TOP_JOBS = 8
JOB_CORES_MIN_ROWS = 3
TOP_PROCESS_GROUPS = 8
TOP_POOL_TAGS = 5
SLOPE_MIN_ROWS = 10
SLOPE_MIN_SPAN = 3600.0
V2_DIR = "queue-v2"
JOB_STORES = (("jobs", ("jobs",)), ("deferred-jobs", ("deferred-jobs",)), ("fleet", ("fleet", "jobs")))
DEFERRED_MODES = ("spare", "overnight")
NIGHT_TZ = "Europe/London"
NIGHT_START_HOUR, NIGHT_END_HOUR = 0, 8
LAPTOP_RAM_MIN_GB = 6.0  # mirrors LAPTOP_RAM_MIN_GB in cq.py (the laptop job floor); cq.py is not imported


class Pct(float):
    """A fraction, rendered as a percentage in markdown and as a number in JSON."""


def is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def parse_time(value):
    """Epoch seconds from a number or an ISO-8601 string (no offset means UTC)."""
    if is_number(value):
        return float(value)
    if not isinstance(value, str):
        return None
    try:
        when = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=dt.timezone.utc)
    return when.timestamp()


def as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def share(part, whole):
    return Pct(part / whole) if whole else None


def iso(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def percentile(xs, p):
    """Linear interpolation between order statistics; None when xs is empty."""
    if not xs:
        return None
    s = sorted(xs)
    k = (len(s) - 1) * p / 100.0
    lo = math.floor(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def dist_cells(xs):
    """n, median, p75, p90, p99, max."""
    return [len(xs), percentile(xs, 50), percentile(xs, 75), percentile(xs, 90),
            percentile(xs, 99), max(xs) if xs else None]


def section(title, tables=(), notes=(), footnotes=()):
    return {"title": title,
            "tables": [{"headers": h, "rows": r} for h, r in tables],
            "notes": list(notes),
            "footnotes": list(footnotes)}


def skipped(title, reason):
    return {"title": title, "skipped": reason}


# ---------------------------------------------------------------- reading

def new_tally():
    return {"read": 0, "skipped": 0, "dropped_since": 0}


def job_host(raw, shard, primary, aliases):
    """The host a job belongs to: 'laptop' or 'unplaced' by placement, else host_id, shard, primary host."""
    placed = raw.get("placed_where") or "unplaced"
    if placed in ("laptop", "unplaced"):
        return placed
    host_id = raw.get("host_id")
    if isinstance(host_id, str) and host_id:
        return aliases.get(host_id, host_id)
    return shard or primary


def normalise_job(raw, store="jobs", shard=None, primary="ember", aliases=None):
    eff = raw.get("slots_eff")
    slots = as_int(raw.get("slots"))
    return {
        "id": str(raw["id"]),
        "store": (shard + ":" if shard else "") + store,
        "host": job_host(raw, shard, primary, aliases or {}),
        "defer": str(raw.get("defer") or "immediate"),
        "registered": raw.get("deferred_registered") is True,
        "label": str(raw.get("label") or ""),
        "status": str(raw["status"]),
        "priority": str(raw.get("priority") or "none"),
        "workstream": str(raw.get("workstream") or "none"),
        "placed_where": raw.get("placed_where") or "unplaced",
        "slots": slots,
        "slots_eff": slots if eff is None else as_int(eff),
        "parent": bool(raw.get("children")),
        "t_sub": parse_time(raw.get("t_submitted")),
        "t_launch": parse_time(raw.get("t_launch")),
        "t_start": parse_time(raw.get("t_started")),
        "t_fin": parse_time(raw.get("t_finished")),
        "minutes": raw.get("minutes"),
        "cpu_s": raw.get("cpu_s"),
        "cores": raw.get("cores"),
    }


def read_jobs(jobs_dir, tally, store="jobs", shard=None, primary="ember", aliases=None):
    jobs = []
    if not os.path.isdir(jobs_dir):
        return jobs
    for name in sorted(os.listdir(jobs_dir)):
        if not name.endswith(".json"):
            continue
        try:
            with open(os.path.join(jobs_dir, name), encoding="utf-8") as fh:
                raw = json.load(fh)
        except (OSError, ValueError):
            raw = None
        if not isinstance(raw, dict) or "id" not in raw or "status" not in raw:
            tally["skipped"] += 1
            continue
        tally["read"] += 1
        jobs.append(normalise_job(raw, store, shard, primary, aliases))
    return jobs


def is_v2_state(state):
    return os.path.basename(os.path.abspath(state)) == V2_DIR


def conf_dir(state):
    """Where host.conf.json lives: the state directory, or the parent of a queue-v2 directory that has none."""
    if is_v2_state(state) and not os.path.isfile(os.path.join(state, "host.conf.json")):
        return os.path.dirname(os.path.abspath(state))
    return state


def read_conf(directory):
    """The parsed <directory>/host.conf.json when it is a JSON object, else {}."""
    try:
        with open(os.path.join(directory, "host.conf.json"), encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception:  # OSError, ValueError, or RecursionError for a file nested too deep: the report still runs
        return {}
    return raw if isinstance(raw, dict) else {}


def conf_slots(directory):
    """The slots of <directory>/host.conf.json when valid (an int from 1 to 32), else None."""
    n = read_conf(directory).get("slots")
    return n if isinstance(n, int) and not isinstance(n, bool) and 1 <= n <= 32 else None


def primary_name(state):
    name = read_conf(conf_dir(state)).get("name")
    return name if isinstance(name, str) and name else "ember"


def store_dirs(base, nested, prefix=""):
    """(label, directory) of each job store under base; the queue-v2 ones too when nested."""
    out = [(prefix + label, os.path.join(base, *parts)) for label, parts in JOB_STORES]
    if nested:
        out += [("v2/" + label, os.path.join(base, V2_DIR, *parts)) for label, parts in JOB_STORES]
    return out


def unsafe_relative(path):
    """True for an absolute, drive-qualified or '..' path: a shard must stay inside the state directory."""
    if not path or os.path.isabs(path) or os.path.splitdrive(path)[0] or path[0] in "/\\":
        return True
    return ".." in re.split(r"[\\/]+", path)


def read_registry(state):
    """(aliases, shards) from hosts.json: its aliases, and [(name, directory)] of the enabled shards
    whose state path is relative, stays inside the state directory and exists. Anything else is ignored."""
    try:
        with open(os.path.join(state, "hosts.json"), encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception:
        return {}, []
    if not isinstance(raw, dict):
        return {}, []
    aliases = raw.get("aliases")
    aliases = ({k: v for k, v in aliases.items() if isinstance(k, str) and isinstance(v, str)}
               if isinstance(aliases, dict) else {})
    hosts = raw.get("hosts")
    shards = []
    root = os.path.realpath(state)
    for name, entry in sorted(hosts.items() if isinstance(hosts, dict) else []):
        if not isinstance(entry, dict) or not name or entry.get("enabled") is False:
            continue
        rel = entry.get("state")
        if not isinstance(rel, str) or unsafe_relative(rel):
            continue
        path = os.path.realpath(os.path.join(state, rel))
        try:
            inside = os.path.commonpath([root, path]) == root
        except ValueError:
            inside = False
        if inside and os.path.isdir(path):
            shards.append((name, os.path.join(state, rel)))
    return aliases, shards


def read_all_jobs(state, since, reading, primary, aliases, shards):
    """Jobs from every store reachable from state; the first store holding an id wins.

    Each store gets a reading tally under its store label (shard stores 'shard:label'), with a
    count of the repeated ids it held; the 'jobs' tally is always present."""
    v2 = is_v2_state(state)
    stores = [(label, path, None) for label, path in
              store_dirs(state, nested=not v2, prefix="v2/" if v2 else "")]
    for name, path in shards:
        stores += [(label, p, name) for label, p in store_dirs(path, nested=True)]
    jobs, seen = [], set()
    for label, path, shard in stores:
        key = (shard + ":" if shard else "") + label
        if key != "jobs" and not os.path.isdir(path):
            continue
        tally = reading[key] = {**new_tally(), "duplicates": 0}
        found = []
        for job in read_jobs(path, tally, label, shard, primary, aliases):
            if job["id"] in seen:
                tally["duplicates"] += 1
                continue
            seen.add(job["id"])
            found.append(job)
        if since is not None:
            kept = [j for j in found if j["t_sub"] is not None and j["t_sub"] >= since]
            tally["dropped_since"] = len(found) - len(kept)
            found = kept
        jobs += found
    return jobs


def valid_history(row):
    return (isinstance(row.get("kind"), str) and is_number(row.get("est"))
            and is_number(row.get("actual")))


def read_jsonl(path, tally, since, valid=lambda row: True):
    """Rows of a JSONL file; malformed lines are skipped and counted."""
    rows = []
    if not os.path.isfile(path):
        return rows
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError:
                row = None
            if not isinstance(row, dict) or not valid(row):
                tally["skipped"] += 1
                continue
            tally["read"] += 1
            if since is not None:
                when = parse_time(row.get("t"))
                if when is None or when < since:
                    tally["dropped_since"] += 1
                    continue
            rows.append(row)
    return rows


def read_in_time_order(paths, tally, since, valid, key):
    """Rows of several JSONL files, one after the other; when two or more files have rows they are
    sorted by key (stable; a row without a time keeps the position of the row before it)."""
    groups = [read_jsonl(path, tally, since, valid) for path in paths]
    rows = [row for group in groups for row in group]
    if sum(bool(group) for group in groups) > 1:
        last = [-math.inf]

        def carried(row):
            when = key(row)
            if when is not None:
                last[0] = when
            return last[0]
        rows.sort(key=carried)
    return rows


# ---------------------------------------------------------- derived views

def hold_window(job, now):
    """(start, end) epoch seconds while a host job holds pool slots; None if it never held any.

    The hold starts at t_launch (the queue takes the slots when it launches), else at
    t_started. A job that never started holds until t_finished, or until now while it is
    still launching; a job with neither t_started nor a launch in progress holds nothing
    after t_finished."""
    start = job["t_launch"] if job["t_launch"] is not None else job["t_start"]
    if start is None:
        return None
    if job["t_fin"] is not None:
        end = job["t_fin"]
    elif job["t_start"] is not None or job["status"] == "launching":
        end = now
    else:
        return None
    return start, max(end, start)


def run_minutes(job):
    """Length of a finished run in minutes; None if never started or still running."""
    if job["t_start"] is None or job["t_fin"] is None:
        return None
    return max(job["t_fin"] - job["t_start"], 0.0) / MINUTE


def pool_jobs(jobs):
    for job in jobs:
        if not job["parent"] and job["placed_where"] == "host" and job["slots_eff"] > 0:
            yield job


def host_runs(jobs, now):
    """(start, end, slots, job) for each host hold on pool slots (see hold_window)."""
    out = []
    for job in pool_jobs(jobs):
        win = hold_window(job, now)
        if win:
            out.append((win[0], win[1], job["slots_eff"], job))
    return out


def host_waits(jobs):
    """(submitted, launched, slots, job) for each host job that waited before its slots were taken.

    'launched' is t_launch, else t_started; a job whose launch was cancelled still waited up
    to its launch."""
    out = []
    for j in pool_jobs(jobs):
        ready = j["t_launch"] if j["t_launch"] is not None else j["t_start"]
        if j["t_sub"] is not None and ready is not None:
            out.append((j["t_sub"], ready, j["slots_eff"], j))
    return out


def slot_minutes(runs):
    return sum(k * (e - s) for s, e, k, _ in runs) / MINUTE


def covered_span(runs):
    if not runs:
        return None
    return min(r[0] for r in runs), max(r[1] for r in runs)


def refusal_bucket(reason):
    text = (reason or "").lower()
    if "reserved for" in text:
        return "reservation"
    if re.search(r"needs \d+ slots?", text):
        return "slots"
    if "load cap" in text:
        return "load cap"
    if re.search(r"\bram\b", text):
        return "RAM floor"
    if "light" in text:
        return "light cap"
    if "stale" in text:
        return "stale sample"
    return "other"


def cores_of(job):
    """Mean cores of the job's process tree: 'cores', else cpu_s over the run's wall time."""
    if is_number(job["cores"]):
        return float(job["cores"])
    run = run_minutes(job)
    if is_number(job["cpu_s"]) and run:
        return job["cpu_s"] / (run * MINUTE)
    return None


def host_pool_note(ctx):
    return (f"Host pool = jobs whose host is the primary host ({ctx['primary']}); jobs on other hosts, "
            "on the laptop and unplaced jobs are not counted here.")


def local_tz():
    """(tzinfo, name) of the night window's zone; UTC when the tz database is unavailable."""
    try:
        return zoneinfo.ZoneInfo(NIGHT_TZ), NIGHT_TZ
    except Exception:  # ZoneInfoNotFoundError (no tzdata) or an unreadable database
        return dt.timezone.utc, "UTC"


def night_window(tz, day):
    """(start, end) epoch seconds of local NIGHT_START_HOUR to NIGHT_END_HOUR on the local date day.

    Both ends are local wall-clock times converted to instants, so the window is 540 real minutes on
    the 25-hour day, 420 on the 23-hour day and 480 otherwise (London changes clocks at 01:00 UTC)."""
    start = dt.datetime(day.year, day.month, day.day, NIGHT_START_HOUR, tzinfo=tz)
    end = dt.datetime(day.year, day.month, day.day, NIGHT_END_HOUR, tzinfo=tz)
    return start.timestamp(), end.timestamp()


# ------------------------------------------------------------- sections

def section_jobs(ctx):
    title = "Jobs"
    jobs = ctx["jobs"]
    if not jobs:
        return skipped(title, "no job files")
    rows = []
    for field in ("status", "priority", "workstream", "placed_where", "slots", "store", "host", "defer"):
        counts = collections.Counter(str(j[field]) for j in jobs)
        for value, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
            rows.append([field, value, n])
    parents = sum(j["parent"] for j in jobs)
    return section(title, [(["field", "value", "jobs"], rows)],
                   [f"{len(jobs)} job files; split parents: {parents} (counted here, "
                    "excluded from timing and slot sums)."])


def section_queue_times(ctx):
    title = "Queue wait and run time (minutes)"
    jobs = [j for j in ctx["jobs"] if not j["parent"]]
    waits = [(j, (j["t_start"] - j["t_sub"]) / MINUTE) for j in jobs
             if j["t_start"] is not None and j["t_sub"] is not None]
    runs = [(j, run_minutes(j)) for j in jobs]
    runs = [(j, m) for j, m in runs if m is not None]
    if not waits and not runs:
        return skipped(title, "no started jobs")
    rows = []
    for metric, pairs in (("wait", waits), ("run", runs)):
        rows.append([metric, "all"] + dist_cells([m for _, m in pairs]))
        for key in ("priority", "slots"):
            for value in sorted({j[key] for j, _ in pairs}):
                group = [m for j, m in pairs if j[key] == value]
                rows.append([metric, f"{key}={value}"] + dist_cells(group))
    headers = ["metric", "group", "n", "median", "p75", "p90", "p99", "max"]
    return section(title, [(headers, rows)], [
        "wait = t_started - t_submitted over started jobs; run = t_finished - t_started "
        "over finished starts (cancelled-after-start included); split parents excluded.",
        "Percentiles: linear interpolation between order statistics."])


def estimate_row(name, rows):
    ratios = [h["actual"] / h["est"] for h in rows if h["est"] > 0]
    return [name, len(rows),
            percentile([h["est"] for h in rows], 50),
            percentile([h["actual"] for h in rows], 50),
            percentile(ratios, 50),
            share(sum(r < 0.5 for r in ratios), len(ratios)),
            share(sum(r > 1 for r in ratios), len(ratios))]


def section_estimates(ctx):
    title = "Estimate accuracy (history.jsonl)"
    hist = ctx["history"]
    if not hist:
        return skipped(title, "no history rows")
    by_kind = collections.defaultdict(list)
    for h in hist:
        by_kind[h["kind"]].append(h)
    rows = [estimate_row("all", hist)]
    for kind in sorted(by_kind, key=lambda k: (-len(by_kind[k]), k)):
        rows.append(estimate_row(kind, by_kind[kind]))
    headers = ["kind", "n", "median est", "median actual", "median actual/est",
               "share < 0.5x est", "share > 1x est"]
    return section(title, [(headers, rows)], [
        "Ratios use rows with est > 0. Kind is the history row's kind (first two words of the command).",
        "Rows come from the primary host's history.jsonl and queue-v2/history.jsonl (in time order); "
        "shard histories are not read."])


def section_kinds(ctx):
    title = "Kinds (history.jsonl)"
    hist = ctx["history"]
    if not hist:
        return skipped(title, "no history rows")
    by_kind = collections.defaultdict(list)
    for h in hist:
        by_kind[h["kind"]].append(h)
    rows = []
    for kind, group in by_kind.items():
        slot_min = sum(as_int(h.get("slots")) * h["actual"] for h in group)
        rows.append([kind, len(group), sum(h["actual"] for h in group), slot_min,
                     percentile([h["actual"] for h in group], 50)])
    rows.sort(key=lambda r: (-r[3], r[0]))
    headers = ["kind", "runs", "wall min", "slot-min", "median run min"]
    total = sum(r[3] for r in rows)
    return section(title, [(headers, rows)], [f"Total slot-min across kinds: {total:.2f}."])


def section_daily(ctx):
    title = "Utilisation per UTC day (host pool)"
    runs = ctx["runs"]
    if not runs:
        return skipped(title, "no host runs")
    t0, t1 = ctx["span"]
    slots = ctx["slots"]
    rows = []
    used_all = denom_all = 0.0
    day = math.floor(t0 / DAY) * DAY
    while day < t1:
        lo, hi = max(t0, day), min(t1, day + DAY)
        if hi > lo:
            used = 0.0
            touching = 0
            for s, e, k, _ in runs:
                overlap = min(e, hi) - max(s, lo)
                if overlap > 0:
                    used += k * overlap
                    touching += 1
            cover = (hi - lo) / MINUTE
            if lo > day and hi < day + DAY:
                label = "partial (span inside one day)"
            elif lo > day:
                label = "partial (first day)"
            elif hi < day + DAY:
                label = "partial (last day)"
            else:
                label = "full"
            denom = slots * cover
            used_min = used / MINUTE
            used_all += used_min
            denom_all += denom
            rows.append([dt.datetime.fromtimestamp(day, dt.timezone.utc).strftime("%Y-%m-%d"),
                         label, cover, touching, used_min, denom, share(used_min, denom)])
        day += DAY
    rows.append(["all", "covered span", (t1 - t0) / MINUTE, len(runs), used_all, denom_all,
                 share(used_all, denom_all)])
    headers = ["day", "coverage", "covered min", "runs", "slot-min used", "slot-min available",
               "% used"]
    running = sum(1 for _, _, _, j in runs if j["t_fin"] is None)
    return section(title, [(headers, rows)], [
        f"Available = --slots ({slots}) x covered minutes of that day; full days cover 1440 min.",
        f"Running jobs clipped at report time: {running}.",
        "Used = held slot-minutes, from t_launch (else t_started) to t_finished; "
        "launch time counts.",
        host_pool_note(ctx)])


def section_concurrency(ctx):
    title = "Concurrency on a 1-minute grid (host pool)"
    runs, waits, slots = ctx["runs"], ctx["waits"], ctx["slots"]
    if not runs:
        return skipped(title, "no host runs")
    t0, t1 = ctx["span"]
    points = held_hi = held_full = queued = stranded = max_held = 0
    idle = idle_fit = 0.0
    t = t0
    while t < t1:
        held = sum(k for s, e, k, _ in runs if s <= t < e)
        free = max(slots - held, 0)
        waiting = [k for a, b, k, _ in waits if a <= t < b]
        fits = any(k <= free for k in waiting)
        points += 1
        held_hi += int(held >= slots - 1)
        held_full += int(held == slots)
        queued += int(bool(waiting))
        stranded += int(fits)
        idle += free
        idle_fit += free if fits else 0
        max_held = max(max_held, held)
        t += MINUTE
    rows = [
        ["grid points (1 min)", points],
        [f"held >= {slots - 1} slots", share(held_hi, points)],
        [f"held == {slots} slots", share(held_full, points)],
        ["a queued job waiting", share(queued, points)],
        ["fitting job waiting (stranded): free slots while a job that fits was waiting",
         share(stranded, points)],
        ["fitting job waiting (stranded) minutes", stranded],
        ["idle slot-minutes", idle],
        ["idle slot-minutes while a fitting job waited", idle_fit],
        ["share of idle slot-minutes that were stranded", share(idle_fit, idle)],
        ["max held slots", max_held],
    ]
    return section(title, [(["measure", "value"], rows)], [
        "Waiting job: t_submitted <= t < t_launch (t_started when t_launch is absent). "
        f"Fitting job waiting (stranded) minute: a waiting job has slots_eff <= {slots} - held.",
        "Grid points run from the first host start to the last host end (running: report time).",
        host_pool_note(ctx)],
        footnotes=[
            "Hold and wait intervals use t_launch (slots are taken while launching); "
            "fitting job waiting (stranded) includes minutes the reservation rule or the caps "
            "held a fitting job back on purpose."])


def section_failed(ctx):
    title = "Failed-run waste (host pool)"
    runs = ctx["runs"]
    if not runs:
        return skipped(title, "no host runs")
    total = slot_minutes(runs)
    failed = [r for r in runs if r[3]["status"] in FAILED and r[3]["t_fin"] is not None]
    big = [r for r in failed if r[2] >= 5]
    rows = []
    for name, group in (("failed or timeout", failed), ("failed or timeout, slots >= 5", big)):
        sm = slot_minutes(group)
        rows.append([name, len(group), sm, share(sm, total)])
    top = sorted(failed, key=lambda r: -(r[2] * (r[1] - r[0])))[:5]
    top_rows = [[r[3]["id"], r[3]["label"][:60], r[2], (r[1] - r[0]) / MINUTE,
                 r[2] * (r[1] - r[0]) / MINUTE] for r in top]
    return section(title,
                   [(["group", "runs", "slot-min", "share of host slot-min"], rows),
                    (["id", "label", "slots", "minutes", "slot-min"], top_rows)],
                   [f"All host slot-minutes: {total:.2f}.", host_pool_note(ctx)])


def section_refusals(ctx):
    title = "Refusals (decisions.jsonl)"
    dec = ctx["decisions"]
    plans = [row for row in dec if row.get("kind") == "plan"]
    caps = sum(row.get("kind") == "caps_override" for row in dec)
    if not plans:
        return skipped(title, "no plan rows")
    counts = collections.Counter()
    accepted = backfill_true = backfill_present = 0
    for plan in plans:
        for r in plan.get("refused") or []:
            reason = r.get("reason") if isinstance(r, dict) else None
            counts[refusal_bucket(reason)] += 1
        for a in plan.get("accepted") or []:
            if not isinstance(a, dict):
                continue
            accepted += 1
            if "backfill" in a:
                backfill_present += 1
                backfill_true += int(bool(a["backfill"]))
    total = sum(counts.values())
    rows = [[b, counts[b], share(counts[b], total)] for b in BUCKETS]
    backfill = (f"{backfill_true} (field present on {backfill_present} of {accepted} accepted entries)"
                if backfill_present else f"field absent on all {accepted} accepted entries")
    return section(title, [(["bucket", "refusals", "share"], rows)], [
        f"Plan rows: {len(plans)}; accepted entries: {accepted}; refused entries: {total}.",
        "Counted per refused entry, so a job refused in several plans counts several times.",
        f"Accepted entries with backfill true: {backfill}.",
        f"caps_override records (not plans, not in the counts above): {caps}."])


def section_cpu(ctx):
    title = "CPU use per slot"
    jobs = [j for j in ctx["pool"] if not j["parent"] and j["slots_eff"] > 0]
    measured = [(j, cores_of(j)) for j in jobs]
    measured = [(j, c) for j, c in measured if c is not None]
    if not measured:
        return skipped(title, "no cores or cpu_s fields in job files")
    rows = []
    for value in sorted({j["slots_eff"] for j, _ in measured}):
        group = [(j, c) for j, c in measured if j["slots_eff"] == value]
        rows.append(cpu_row(value, group))
    rows.append(cpu_row("all", measured))
    headers = ["slots", "n", "median cores", "p90 cores", "median cores/slot",
               "p90 cores/slot", "share cores > 1.5 x slots"]
    return section(title, [(headers, rows)], [
        "cores: the job field, else cpu_s over the run's wall time. "
        "The last column uses 1.5 vCPU per slot (the pre-2026-10-21 size; host.conf.json now sets 1.0). "
        "job_cores samples before 2026-10-21 16:30Z come from the older per-process method that missed short-lived children.",
        host_pool_note(ctx)])


def cpu_row(name, group):
    cores = [c for _, c in group]
    per_slot = [c / j["slots_eff"] for j, c in group]
    over = sum(c > 1.5 * j["slots_eff"] for j, c in group)
    return [name, len(group), percentile(cores, 50), percentile(cores, 90),
            percentile(per_slot, 50), percentile(per_slot, 90), share(over, len(group))]


def section_samples(ctx):
    title = "Capacity samples (samples.jsonl)"
    rows = ctx["samples"]
    if not rows:
        return skipped(title, "no samples.jsonl rows")
    tally = ctx["samples_tally"]
    ats = [float(row["at"]) for row in rows]
    weights = [min(max(nxt - at, 0.0), SAMPLE_GAP_CAP) for at, nxt in zip(ats, ats[1:])] + [0.0]
    total = sum(weights)
    weighted = [0.0] * len(SAMPLE_STATES)
    wait_weight = wait_sum = 0.0
    for row, w in zip(rows, weights):
        queued, free, room = as_int(row.get("queued")), as_int(row.get("free")), as_int(row.get("room"))
        state, fitting = row.get("head_state"), as_int(row.get("waiting_fit"))
        flags = (queued == 0, queued > 0 and free == 0, queued > 0 and free > 0 and room == 0,
                 queued > 0 and fitting > 0 and state in (None, "fits") and room > 0,
                 state == "slots", state == "caps")
        for i, on in enumerate(flags):
            if on:
                weighted[i] += w
        if queued > 0:
            wait_weight += w
            wait_sum += w * queued
    load = [float(row["load1"]) for row in rows if is_number(row.get("load1"))]
    mem = [float(row["mem_avail_gb"]) for row in rows if is_number(row.get("mem_avail_gb"))]
    headers = ["measure", "rows", "median", "p90"]
    measures = [["load1", len(load), percentile(load, 50), percentile(load, 90)],
                ["mem_avail_gb", len(mem), percentile(mem, 50), percentile(mem, 90)]]
    shares = [[name, share(weighted[i], total)] for i, name in enumerate(SAMPLE_STATES)]
    average = f"{wait_sum / wait_weight:.2f}" if wait_weight else "-"
    return section(title, [(headers, measures), (["state", "share of time"], shares)], [
        f"Rows read: {len(rows)}, malformed skipped: {tally['skipped']}, dropped by --since: {tally['dropped_since']}.",
        f"Covered span: {iso(min(ats))} to {iso(max(ats))}.",
        f"Average queue length while waiting (weighted by gap, rows with queued > 0): {average}."],
        ["Shares are weighted by the gap to the next row (the last row has none); states can overlap, so they need not total 100%."])


def gap_weights(rows):
    """Seconds each sample row stands for: the gap to the next row, capped; the last row has none."""
    ats = [float(row["at"]) for row in rows]
    return [min(max(nxt - at, 0.0), SAMPLE_GAP_CAP) for at, nxt in zip(ats, ats[1:])] + [0.0]


def numeric_series(rows, key):
    """(at, value) pairs in time order for the rows where key holds a number."""
    pairs = [(float(row["at"]), float(row[key])) for row in rows if is_number(row.get(key))]
    return sorted(pairs, key=lambda pair: pair[0])


def slope_per_day(pairs):
    """Least-squares slope of value on time, per day; "n/a" with under 10 rows or a span under an hour."""
    if len(pairs) < SLOPE_MIN_ROWS or pairs[-1][0] - pairs[0][0] < SLOPE_MIN_SPAN:
        return "n/a"
    mean_at = sum(at for at, _ in pairs) / len(pairs)
    mean_value = sum(value for _, value in pairs) / len(pairs)
    var = sum((at - mean_at) ** 2 for at, _ in pairs)
    cov = sum((at - mean_at) * (value - mean_value) for at, value in pairs)
    return cov / var * DAY


def measure_rows(rows, keys):
    out = []
    for key in keys:
        xs = [float(row[key]) for row in rows if is_number(row.get(key))]
        out.append([key, len(xs), percentile(xs, 50), percentile(xs, 90)])
    return out


def section_host_cpu(ctx):
    title = "Host CPU (samples)"
    rows = ctx["samples"]
    if not any(is_number(row.get("cpu_busy")) for row in rows):
        return skipped(title, "no cpu_busy values in samples.jsonl")
    weights = gap_weights(rows)
    total = sum(weights)
    caps = [(row, w) for row, w in zip(rows, weights) if row.get("head_state") == "caps"]
    caps_weight = sum(w for _, w in caps)
    idle = [(float(row["cpu_idle"]), w) for row, w in caps if is_number(row.get("cpu_idle"))]
    idle_weight = sum(w for _, w in idle)
    mean_idle = sum(v * w for v, w in idle) / idle_weight if idle_weight else None
    decisive = [["caps time: share with cpu_idle >= 20", share(sum(w for v, w in idle if v >= 20), idle_weight)],
                ["caps time: share with cpu_idle >= 10", share(sum(w for v, w in idle if v >= 10), idle_weight)],
                ["caps time: weighted mean cpu_idle", mean_idle],
                ["all time: share in caps", share(caps_weight, total)]]

    top_rows = [row["top"] for row in rows if isinstance(row.get("top"), list)]
    class_sums = collections.Counter()
    for entries in top_rows:
        for entry in entries:
            if isinstance(entry, list) and len(entry) == 2 and isinstance(entry[0], str) and is_number(entry[1]):
                class_sums[entry[0]] += float(entry[1])
    classes = sorted(((name, cores / len(top_rows)) for name, cores in class_sums.items()),
                     key=lambda item: (-item[1], item[0]))[:TOP_CLASSES]
    # The probe split go-tool into go-compile, go-link and go-vet (2026-10-23); their sum reads the same across the switch
    go_build = sum(class_sums.get(n, 0.0) for n in GO_BUILD_CLASSES)
    if go_build and top_rows:
        classes.append(("go build tools, all (" + " + ".join(GO_BUILD_CLASSES) + ")", go_build / len(top_rows)))

    job_samples = collections.defaultdict(list)
    for row in rows:
        if isinstance(row.get("job_cores"), dict):
            for job_id, cores in row["job_cores"].items():
                if is_number(cores):
                    job_samples[job_id].append(float(cores))
    jobs = {job["id"]: job for job in ctx["jobs"]}
    per_job = []
    for job_id, xs in job_samples.items():
        if len(xs) >= JOB_CORES_MIN_ROWS:
            job = jobs.get(job_id)
            label, slots = (job["label"] or "-", job["slots"]) if job else ("-", "-")
            per_job.append([job_id, label, slots, len(xs), sum(xs) / len(xs)])
    per_job = sorted(per_job, key=lambda item: (-item[4], item[0]))[:TOP_JOBS]

    headers = ["measure", "rows", "median", "p90"]
    measures = measure_rows(rows, CPU_MEASURES)
    busy = sum(is_number(row.get("cpu_busy")) for row in rows)
    return section(title, [
        (headers, measures),
        (["measure", "value"], decisive),
        (["class", "mean cores"], [[name, cores] for name, cores in classes]),
        (["job", "label", "slots", "rows", "mean cores"], per_job)], [
        f"Rows read: {len(rows)}, rows with cpu_busy: {busy}.",
        f"Time weights: gap to the next row, capped at {SAMPLE_GAP_CAP:.0f} s. Idle shares use only caps rows with cpu_idle: "
        f"{idle_weight:.0f} s of {caps_weight:.0f} s in caps.",
        f"Class means: over the {len(top_rows)} rows with a top list; a class missing from a row counts as 0 for that row.",
        f"Job means: over the rows where the job appears, for jobs in at least {JOB_CORES_MIN_ROWS} rows."],
        ["Idle while the caps refuse: a high share means the load average lags behind the real CPU, "
         "so the cap could be relaxed. A near-zero share means the host is saturated and the caps are right."])


def bench_row(name, rows):
    real = [float(row["bench_real_ms"]) for row in rows]
    cpu = [float(row["bench_cpu_ms"]) for row in rows]
    waits = [(r - c) / r for r, c in zip(real, cpu) if r > 0]
    return [name, len(rows), percentile(real, 50), percentile(real, 90), percentile(cpu, 50),
            percentile(waits, 50), percentile(waits, 90)]


def section_shared_host(ctx):
    title = "Shared host (benchmark)"
    rows = [row for row in ctx["samples"] if is_number(row.get("bench_real_ms")) and is_number(row.get("bench_cpu_ms"))]
    if not rows:
        return skipped(title, "no bench_real_ms values in samples.jsonl")
    bands = (("cpu_idle >= 50", lambda idle: idle >= 50), ("cpu_idle 20-50", lambda idle: 20 <= idle < 50),
             ("cpu_idle < 20", lambda idle: idle < 20))
    table = []
    for name, keep in bands:
        group = [row for row in rows if is_number(row.get("cpu_idle")) and keep(float(row["cpu_idle"]))]
        table.append(bench_row(name, group))
    table.append(bench_row("all", rows))
    headers = ["cpu_idle", "n", "median real ms", "p90 real ms", "median cpu ms", "median wait share", "p90 wait share"]
    return section(title, [(headers, table)], [
        f"Rows read with a benchmark: {len(rows)}. A row falls in a band by its own cpu_idle; rows without one count only in all."],
        ["Wait share is (real - cpu) / real, the time the single-threaded loop was runnable but not running: with idle vCPUs that "
         "is hypervisor contention, and cpu ms above its idle-host baseline is slowdown from a shared core."])


def section_laptop_ram(ctx):
    title = "Laptop RAM (samples)"
    rows = ctx["samples"]
    if not any(is_number(row.get("laptop_free_ram_gb")) for row in rows):
        return skipped(title, "no laptop_free_ram_gb values in samples.jsonl")
    weights = gap_weights(rows)
    free = [(float(row["laptop_free_ram_gb"]), w) for row, w in zip(rows, weights)
            if is_number(row.get("laptop_free_ram_gb"))]
    free_xs = [v for v, _ in free]
    free_weight = sum(w for _, w in free)
    below = sum(w for v, w in free if v < LAPTOP_RAM_MIN_GB)
    ram = [["laptop_free_ram_gb", len(free_xs), percentile(free_xs, 50), percentile(free_xs, 10),
            min(free_xs), share(below, free_weight)]]

    kernel = []
    for key in ("laptop_commit_gb", "laptop_commit_limit_gb", "laptop_np_gb", "laptop_paged_gb"):
        pairs = numeric_series(rows, key)
        values = [v for _, v in pairs]
        if key in ("laptop_np_gb", "laptop_paged_gb"):
            kernel.append([key, len(values), percentile(values, 50), pairs[0][1] if pairs else None,
                           pairs[-1][1] if pairs else None, slope_per_day(pairs)])
        else:
            kernel.append([key, len(values), percentile(values, 50), "-", "-", "-"])

    groups = collections.defaultdict(list)
    top_rows = 0
    for row in rows:
        if not isinstance(row.get("laptop_top"), list):
            continue
        top_rows += 1
        for entry in row["laptop_top"]:
            if (isinstance(entry, list) and len(entry) == 4 and isinstance(entry[0], str)
                    and all(is_number(v) for v in entry[1:])):
                groups[entry[0]].append([float(v) for v in entry[1:]])  # processes, ws_mb, priv_mb
    processes = []
    for name, vals in groups.items():
        processes.append([name, len(vals), percentile([v[1] for v in vals], 50),
                          percentile([v[2] for v in vals], 50), percentile([v[0] for v in vals], 50)])
    processes = sorted(processes, key=lambda item: (-item[2], item[0]))[:TOP_PROCESS_GROUPS]

    unreadable = [float(row["laptop_unreadable"]) for row in rows if is_number(row.get("laptop_unreadable"))]

    pool = collections.defaultdict(list)
    for row in rows:
        if not isinstance(row.get("laptop_pool"), list):
            continue
        for entry in row["laptop_pool"]:
            if (isinstance(entry, list) and len(entry) == 3 and isinstance(entry[0], str)
                    and is_number(entry[1]) and is_number(entry[2])):
                pool[entry[0]].append((float(row["at"]), float(entry[1]), float(entry[2])))
    pool_rows = []
    for tag, points in pool.items():
        points = sorted(points, key=lambda point: point[0])
        outstanding = [(at, out) for at, _, out in points]
        pool_rows.append([tag, len(points), points[0][1], points[-1][1], points[0][2], points[-1][2],
                          slope_per_day(outstanding)])
    pool_rows = sorted(pool_rows, key=lambda item: (-item[3], item[0]))[:TOP_POOL_TAGS]

    return section(title, [
        (["measure", "rows", "median", "p10", "min", f"share of time below {LAPTOP_RAM_MIN_GB:g} GB"], ram),
        (["measure", "rows", "median", "first", "last", "slope GB per day"], kernel),
        (["process group", "rows", "median ws_mb", "median priv_mb", "median processes"], processes),
        (["pool tag", "rows", "first mb", "last mb", "first outstanding", "last outstanding",
          "outstanding slope per day"], pool_rows)], [
        f"Rows read: {len(rows)}. Free RAM time weights: gap to the next row, capped at {SAMPLE_GAP_CAP:.0f} s, "
        f"over rows where the value is present.",
        f"Process groups: rows with a laptop_top list: {top_rows}; medians are over the rows where each group appears.",
        (f"laptop_unreadable: median {percentile(unreadable, 50):.2f} over {len(unreadable)} rows."
         if unreadable else "laptop_unreadable: no values."),
        f"Slopes need at least {SLOPE_MIN_ROWS} rows spanning {SLOPE_MIN_SPAN:.0f} s or more; otherwise n/a."],
        ["Nonpaged pool growth over days with a flat process list points at a driver or filter leak, not at a process.",
         "Pool tag rows come only from the 3 largest tags logged in each row, so a tag's slope uses the rows where it was among "
         "them: a tag near the cut-off is under-sampled and its slope is less reliable."])


def night_host_order(hosts, primary):
    return sorted(hosts, key=lambda h: (h != primary, h))


def night_rows(host, runs, slots, tz):
    """Table rows, and the full-night totals, for one host's holds (see section_night)."""
    t0, t1 = covered_span(runs)
    first = dt.datetime.fromtimestamp(t0, tz).date() - dt.timedelta(days=1)
    last = dt.datetime.fromtimestamp(t1, tz).date()
    rows = []
    full = {"nights": 0, "window": 0.0, "used": 0.0, "ids": {True: set(), False: set()}}
    partial = 0
    day = first
    while day <= last:
        night = day
        day += dt.timedelta(days=1)
        ws, we = night_window(tz, night)
        lo, hi = max(t0, ws), min(t1, we)
        if hi <= lo:
            continue
        used = 0.0
        ids = {True: set(), False: set()}
        for s, e, k, j in runs:
            overlap = min(e, hi) - max(s, lo)
            if overlap > 0:
                used += k * overlap
                ids[j["defer"] in DEFERRED_MODES].add(j["id"])
        used_min = used / MINUTE
        avail = slots * (hi - lo) / MINUTE if slots else None
        if lo == ws and hi == we:
            label = "full"
            full["nights"] += 1
            full["window"] += (hi - lo) / MINUTE
            full["used"] += used_min
            for flag in ids:
                full["ids"][flag] |= ids[flag]
        else:
            partial += 1
            if lo > ws and hi < we:
                label = "partial (span inside one night)"
            elif lo > ws:
                label = "partial (starts inside the window)"
            else:
                label = "partial (ends inside the window)"
        rows.append([host, night.isoformat(), label, (hi - lo) / MINUTE, used_min,
                     avail, share(used_min, avail), len(ids[True]), len(ids[False])])
    n = full["nights"]
    avail_all = slots * full["window"] if slots else None
    rows.append([host, "all nights", f"full nights only: {n}", full["window"], full["used"],
                 avail_all, share(full["used"], avail_all) if avail_all else None,
                 len(full["ids"][True]), len(full["ids"][False])])
    idle = f"{(avail_all - full['used']) / 60.0 / n:.2f}" if avail_all and n else "-"
    return rows, (f"Average idle slot-hours per night, {host}: {idle} over {n} full "
                  f"night{'s' if n != 1 else ''}; partial nights excluded: {partial}.")


def section_night(ctx):
    title = f"Night window use ({NIGHT_START_HOUR:02d}:00-{NIGHT_END_HOUR:02d}:00 Europe/London)"
    tz, tz_name = local_tz()
    by_host = collections.defaultdict(list)
    for s, e, k, j in host_runs([j for j in ctx["jobs"] if j["host"] not in ("laptop", "unplaced")], ctx["now"]):
        by_host[j["host"]].append((s, e, k, j))
    if not by_host:
        return skipped(title, "no host runs")
    rows, idle_lines = [], []
    for host in night_host_order(by_host, ctx["primary"]):
        host_rows, idle_line = night_rows(host, by_host[host], ctx["host_slots"].get(host), tz)
        rows += host_rows
        idle_lines.append(idle_line)
    headers = ["host", "night (local date)", "coverage", "window min", "slot-min used",
               "slot-min available", "% used", "deferred jobs", "immediate jobs"]
    notes = [
        f"Night = local {NIGHT_START_HOUR:02d}:00 to {NIGHT_END_HOUR:02d}:00 in {tz_name}, dated by the day it starts on; "
        "the window is bounded by local wall-clock times, so it is 540 real minutes on the 25-hour day "
        "and 420 on the 23-hour day." if tz_name == NIGHT_TZ else
        f"The tz database is unavailable, so nights are 00:00 to 08:00 UTC ({NIGHT_TZ} not used).",
        "Used = slot-minutes held inside the window (slots_eff, else slots), from t_launch (else t_started) to "
        "t_finished, running jobs to report time; split parents excluded. Hosts: every host with a hold, "
        "the laptop and unplaced jobs excluded.",
        "Covered span per host: its first hold start to its last hold end. Only nights wholly inside it are "
        "full; partial nights are listed, their available slot-minutes cover the covered part of the window only, "
        "and they are left out of the all-nights row and the averages.",
        "Available = pool slots x window minutes: the primary host uses --slots "
        f"({ctx['slots']}), another host the slots in its shard's host.conf.json ('-' when unknown).",
        "Deferred = jobs whose defer is spare or overnight, immediate = the rest, among the jobs holding slots "
        "in the window (all nights row: distinct jobs).",
        "Job files are read from every store (see the footer); shard histories, decisions and samples are not read."]
    return section(title, [(headers, rows)], notes + idle_lines)


def section_deferred(ctx):
    title = "Deferred work"
    jobs = [j for j in ctx["jobs"] if not j["parent"]]
    deferred = [j for j in jobs if j["defer"] in DEFERRED_MODES]
    if not deferred:
        return skipped(title, "no deferred jobs")
    counts = collections.Counter((j["defer"], j["status"], j["host"]) for j in jobs)
    count_rows = [[d, s, h, n] for (d, s, h), n in sorted(
        counts.items(), key=lambda kv: (kv[0][0] != "immediate", kv[0][0] != "spare", kv[0]))]
    waits = []
    for j in deferred:
        start = j["t_start"] if j["t_start"] is not None else j["t_launch"]
        if start is not None and j["t_sub"] is not None:
            waits.append((j["defer"], max(start - j["t_sub"], 0.0) / MINUTE))
    wait_rows = [["all deferred"] + dist_cells([w for _, w in waits])]
    for mode in DEFERRED_MODES:
        group = [w for m, w in waits if m == mode]
        if group:
            wait_rows.append([mode] + dist_cells(group))
    registered = sum(j["registered"] for j in deferred)
    measures = [["deferred jobs", len(deferred)], ["deferred_registered true", registered]]
    return section(title, [
        (["defer", "status", "host", "jobs"], count_rows),
        (["group", "n", "median", "p75", "p90", "p99", "max"], wait_rows),
        (["measure", "value"], measures)], [
        "Counts cover every job but split parents, all defer modes (absent or null = immediate).",
        "Wait (minutes) = start - t_submitted over deferred jobs (spare, overnight), where start is t_started, "
        "else t_launch (queue-v2 fleet records carry no t_started); jobs with neither are left out.",
        f"{registered} of {len(deferred)} deferred jobs have deferred_registered true."])


def section_footer(ctx, state, reading, command):
    title = "Footer"
    span = ctx["span"]
    span_text = (f"{iso(span[0])} to {iso(span[1])}" if span else "none (no host runs)")
    notes = [
        f"State directory: {state}",
        f"Primary host: {ctx['primary']}",
        f"Covered host span: {span_text}",
        f"Report time (running jobs clipped here): {iso(ctx['now'])}",
    ]
    for name, t in reading.items():
        extra = f", {t['duplicates']} repeated ids dropped" if t.get("duplicates") else ""
        notes.append(f"{name}: {t['read']} rows read, {t['skipped']} malformed skipped, "
                     f"{t['dropped_since']} dropped by --since{extra}")
    notes.append(f"Reproduce: `{command}`")
    return section(title, notes=notes)


# ----------------------------------------------------------------- report

def build_report(state, since, slots, now, command):
    primary = primary_name(state)
    aliases, shards = read_registry(state)
    reading = {}
    jobs = read_all_jobs(state, since, reading, primary, aliases, shards)
    reading.update({"history.jsonl": new_tally(), "decisions.jsonl": new_tally()})
    # The queue-v2 files continue the primary host's; a queue-v2 --state already is that directory.
    dirs = [state] if is_v2_state(state) else [state, os.path.join(state, V2_DIR)]
    by_time = lambda row: parse_time(row.get("t"))
    history = read_in_time_order([os.path.join(d, "history.jsonl") for d in dirs],
                                 reading["history.jsonl"], since, valid_history, by_time)
    decisions = read_in_time_order([os.path.join(d, "decisions.jsonl") for d in dirs],
                                   reading["decisions.jsonl"], since, lambda row: True, by_time)
    samples_tally = new_tally()
    # each rotated file holds the older rows: one chronological sequence per directory
    samples = read_in_time_order([os.path.join(d, name) for d in dirs for name in ("samples.jsonl.1", "samples.jsonl")],
                                 samples_tally, since, lambda row: is_number(row.get("at")),
                                 lambda row: float(row["at"]))
    pool = [j for j in jobs if j["host"] == primary]
    runs = host_runs(pool, now)
    span = covered_span(runs)
    host_slots_by_name = {primary: slots}
    for name, path in shards:
        n = conf_slots(path)
        if n is not None and name != primary:
            host_slots_by_name[name] = n
    ctx = {"jobs": jobs, "pool": pool, "primary": primary, "history": history, "decisions": decisions,
           "slots": slots, "host_slots": host_slots_by_name,
           "now": now, "runs": runs, "waits": host_waits(pool), "span": span,
           "samples": samples, "samples_tally": samples_tally}
    sections = [section_jobs(ctx), section_queue_times(ctx), section_estimates(ctx),
                section_kinds(ctx), section_daily(ctx), section_concurrency(ctx),
                section_failed(ctx), section_refusals(ctx), section_cpu(ctx),
                section_samples(ctx), section_host_cpu(ctx), section_shared_host(ctx), section_laptop_ram(ctx),
                section_night(ctx), section_deferred(ctx),
                section_footer(ctx, state, reading, command)]
    return {"state": state,
            "since": dt.datetime.fromtimestamp(since, dt.timezone.utc).strftime("%Y-%m-%d")
            if since is not None else None,
            "slots": slots,
            "now": iso(now),
            "span": [iso(span[0]), iso(span[1])] if span else None,
            "reading": reading,
            "sections": sections}


# --------------------------------------------------------------- rendering

def fmt(value):
    if value is None:
        return "-"
    if isinstance(value, Pct):
        return f"{value * 100:.1f}%"
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, str):
        return value.replace("|", "\\|")
    return str(value)


def md_table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |",
             "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(fmt(c) for c in row) + " |" for row in rows]
    return lines


def render_markdown(report):
    out = ["# Compute queue utilisation and wait report", ""]
    for number, sec in enumerate(report["sections"], 1):
        out += [f"## {number}. {sec['title']}", ""]
        if "skipped" in sec:
            out += [f"Skipped: {sec['skipped']}.", ""]
            continue
        out += [f"- {note}" for note in sec["notes"]] + ([""] if sec["notes"] else [])
        for table in sec["tables"]:
            out += md_table(table["headers"], table["rows"]) + [""]
        out += sec["footnotes"] + ([""] if sec["footnotes"] else [])
    return "\n".join(out).rstrip() + "\n"


# -------------------------------------------------------------------- CLI

def parse_since(text):
    try:
        day = dt.datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {text!r}") from exc
    return day.replace(tzinfo=dt.timezone.utc).timestamp()


def positive_int(text):
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return value


def host_slots(state):
    """The slots of <state>/host.conf.json when it gives a valid one (an int from 1 to 32), else the host pool's 8.

    A queue-v2 directory without its own host.conf.json uses its parent's."""
    n = conf_slots(conf_dir(state))
    return 8 if n is None else n


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Read-only utilisation and queue-wait report for the compute queue.")
    parser.add_argument("--state", help="state directory (default: env CQ_STATE, "
                                        f"else {DEFAULT_STATE})")
    parser.add_argument("--since", type=parse_since, metavar="YYYY-MM-DD",
                        help="only rows at or after this UTC date")
    parser.add_argument("--slots", type=positive_int, default=None,
                        help="host slot pool size for denominators (default: slots in host.conf.json under the state dir, else 8)")
    parser.add_argument("--json", action="store_true", help="print one JSON object")
    args = parser.parse_args(argv)

    state = args.state or os.environ.get("CQ_STATE") or DEFAULT_STATE
    slots = args.slots if args.slots is not None else host_slots(state)
    command = f'python -I "{os.path.abspath(__file__)}" --state "{state}" --slots {slots}'
    if args.since is not None:
        command += " --since " + dt.datetime.fromtimestamp(
            args.since, dt.timezone.utc).strftime("%Y-%m-%d")
    if args.json:
        command += " --json"

    report = build_report(state, args.since, slots, time.time(), command)
    text = json.dumps(report, indent=2) if args.json else render_markdown(report)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stdout.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
