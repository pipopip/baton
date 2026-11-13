#!/usr/bin/env python3
"""compute-queue: the queue store, executors, leader lock and plan guard behind the Claude Code mod.

Python 3.12, standard library only. One subcommand per operation; JSON in on stdin, JSON out on stdout:

  submit  list  get  cancel  status        the queue (every mutation under state/queue.lock)
  leader  probe  reap  tick                the leader's tick (host/laptop probe, reaping, notices)
  guard  apply  dispatch                   the plan guard, the launch (marks the job first, then a detached worker)
  wait  watch  notified  note               polling, a session's own view, bookkeeping
  _launch  _laprun                         detached workers (not for people)

State dir: <main checkout>/.state/compute (override: CQ_STATE, BATON_STATE or --state). Jobs never run inside a git worktree:
host jobs run in ~/q/cq-<id>/src clones, laptop jobs in <state>/run/<id>/src clones (git clone --shared at the snapshot sha).
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import datetime as dt
import hashlib
import json
import math
import ntpath
import os
import posixpath
import re
import select
import secrets
import shlex
import shutil
import stat
import statistics
import subprocess
import sys
import tarfile
import time
import unicodedata
import threading
import zlib
import artifact_transfer
import host_stop
import multi_host
import queue_policy
import artifact_transfer
import queue_paths

try:  # the lock is msvcrt on Windows (dies with the process); fcntl elsewhere keeps the module importable
    import msvcrt
    fcntl = None
except ImportError:  # pragma: no cover
    msvcrt = None
    import fcntl

DEFAULT_STATE = queue_paths.default_state()


def default_ssh(environ=None, name=None, isfile=os.path.isfile, which=shutil.which):
    """The ssh client: CQ_SSH, else on Windows the system OpenSSH client, else the first ssh on PATH.

    PATH order differs between sessions. Git for Windows ships its own ssh.exe, which cannot reach the Windows
    ssh-agent service that holds the queue key: a leader that resolved it failed publickey authentication on every
    probe, marked the host down and left running jobs uncollected."""
    environ = os.environ if environ is None else environ
    if environ.get("CQ_SSH"):
        return environ["CQ_SSH"]
    if (os.name if name is None else name) == "nt":
        system = ntpath.join(environ.get("SystemRoot") or r"C:\Windows", "System32", "OpenSSH", "ssh.exe")
        if isfile(system):
            return system
    return which("ssh") or "ssh"


SSH = default_ssh()


LAPTOP_NOT_EXECUTOR = "not an executor on this machine"


def laptop_executes(name=None, environ=None):
    """Whether this machine runs `where: "laptop"` work. The laptop executor is Windows' (Job Objects, the heavy lock,
    GlobalMemoryStatusEx); elsewhere it would run the job's script directly with no limit, so it is off unless
    CQ_LAPTOP_EXECUTOR=1 (the Linux test suite exercises the laptop paths that way). CQ_LAPTOP_EXECUTOR=0 turns it off
    anywhere. A controller on another machine (a Mac) places jobs on the remote hosts only."""
    environ = os.environ if environ is None else environ
    forced = environ.get("CQ_LAPTOP_EXECUTOR")
    if forced in ("0", "1"):
        return forced == "1"
    return (os.name if name is None else name) == "nt"


def norm_path(p):
    return os.path.abspath(p).replace("\\", "/")


# <state>/host.conf.json: the build host's settings. Refreshed for each engine command, including long-lived MCP bridges.
# Deleting the file restores the defaults. A key that is out of range or of the wrong type
# is ignored and its default applies. Precedence for each setting: environment, then this file, then the default.
HOST_CONF_FILE = "host.conf.json"


def _text(v):
    return isinstance(v, str) and v.strip() != ""


def _number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _in(lo, hi):
    return lambda v: _number(v) and lo <= v <= hi


def _address(v):
    """An ssh destination: no leading "-" (it would read as an option), no whitespace or control characters."""
    return _text(v) and not v.startswith("-") and v.isprintable() and not any(c.isspace() for c in v)


HOST_CONF_VALID = {
    "host_id": lambda v: isinstance(v, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,31}", v) is not None,
    "name": _text,
    "address": _address,
    "fallback_address": _address,
    "machine_id": lambda v: isinstance(v, str) and re.fullmatch(r"[0-9a-f]{32}", v) is not None,
    "portable_runner": lambda v: isinstance(v, bool),
    "caps_override": lambda v: isinstance(v, bool),
    "slots": lambda v: isinstance(v, int) and not isinstance(v, bool) and 1 <= v <= 32,
    "slot_vcpu": _in(0.1, 64),
    "slot_ram_gb": _in(0.1, 1024),
    "ram_floor_gb": _in(0, 1024),
    "load_cap": _in(1, 1024),
    "foreground_slots": lambda v: type(v) is int and 0 <= v <= 31,
    "timezone": lambda v: queue_policy.policy_fields({"timezone": v})["timezone"] == v,
    "window_start": lambda v: (type(v) is int and 0 <= v <= 23) or (isinstance(v, str) and re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", v) is not None),
    "window_end": lambda v: (type(v) is int and 0 <= v <= 23) or (isinstance(v, str) and re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", v) is not None),
    "planner": lambda v: v in ("code", "haiku"),
    "paused": lambda v: isinstance(v, bool),
    "ram_jobs": lambda v: isinstance(v, bool),
    "memory_budget_gb": _in(1, 50),
    # an absolute path of at most 20 characters, no dots: CQ_RAM_TMP (this plus /XXXXXXXX) stays within the 29 bytes
    # lane_plan's socket tests leave under the 108-byte unix socket path limit
    "tmp_root": lambda v: isinstance(v, str) and re.fullmatch(r"/[A-Za-z0-9_/-]{1,19}", v) is not None,
    "go_trimpath": lambda v: isinstance(v, bool),
    # the Linux job sandbox (host/jobbox): off (the default), optional (a job asks with sandbox: true) or required
    "sandbox": lambda v: v in ("off", "optional", "required"),
    # false: fleet jobs on this host are admitted on their caps, though its helper has the fleet memory pool in effect
    # (fleet_queue._remote_probe); the default is to admit on the memory a job is expected to use
    "expected_memory_admission": lambda v: isinstance(v, bool),
    # true: fleet jobs on this host take the pool slots their earlier runs show they use, not the slots they declare,
    # which stay their CPU limit (fleet_queue._job_slots, deferred.py charged_slots); the default is the declared slots
    "expected_cpu_admission": lambda v: isinstance(v, bool),
    # false: a fleet job that has waited for room is not registered on this host to wait there (fleet_queue._reserve,
    # hand-over); the default is that it is, so that it starts when room appears even with no controller running
    "queue_on_host": lambda v: isinstance(v, bool),
}


def _valid(check, v):
    try:
        return check(v)
    except Exception:  # a value the check cannot compare (a 400-digit integer in math.isfinite) is not valid
        return False


def host_conf(state_dir):
    """The valid keys of <state_dir>/host.conf.json, strings trimmed; {} when the file is missing. A file that is there
    but unreadable, not JSON, not an object or nested too deep to parse gives only {"sandbox": "invalid"}: every other
    setting falls back to its default, but a sandbox the file may have required must not silently read as off
    (sandbox_plan then refuses every job on the host). cq.py reads it at import, so nothing here may raise."""
    unreadable = {"sandbox": "invalid"}
    try:
        try:
            f = open(os.path.join(queue_paths.config_state(state_dir), HOST_CONF_FILE), encoding="utf-8")
        except FileNotFoundError:
            return {}
        with f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return unreadable
        vals = {k: v.strip() if isinstance(v, str) else v for k, v in raw.items() if k in HOST_CONF_VALID}
        result = {k: v for k, v in vals.items() if _valid(HOST_CONF_VALID[k], v)}
        if "sandbox" in vals and "sandbox" not in result:
            result["sandbox"] = "invalid"  # a mistyped setting must not read as "off": sandbox_plan refuses every job
        policy_keys = set(queue_policy.DEFAULTS)
        if policy_keys.intersection(raw):
            try:
                queue_policy.policy_fields(raw, pool_slots=result.get("slots"))
            except ValueError:
                result["_policy_error"] = "invalid configured host scheduling policy"
        return result
    except Exception:
        return unreadable


def host_address(env, conf=None):
    """Explicit SSH destination: CQ_HOST, LANE_HOST, then host.conf.json."""
    addr = env.get("CQ_HOST") or env.get("LANE_HOST") or (conf or {}).get("address") or ""
    return addr if _address(addr) else ""


def host_load_cap(env, conf):
    """CQ_LOAD_CAP, then the host.conf.json load_cap; None when neither is set (the host's nproc is the cap)."""
    return float(env.get("CQ_LOAD_CAP") or conf.get("load_cap") or 0) or None


def host_ram_floor_gb(env, conf):
    """CQ_HOST_RAM_FLOOR_GB, then the host.conf.json ram_floor_gb, then 4."""
    return float(env.get("CQ_HOST_RAM_FLOOR_GB") or conf.get("ram_floor_gb", 4))


def host_planner(conf):
    """The host.conf.json planner: "haiku" restores the model planner; anything else (absent, junk) is "code", the
    deterministic start-order path that plans every round."""
    return "haiku" if (conf or {}).get("planner") == "haiku" else "code"


_CONF = host_conf(norm_path(os.environ.get("CQ_STATE") or DEFAULT_STATE))
HOST = host_address(os.environ, _CONF)
REMOTERUN, HEAVYRUN, HEAVY_LOCK = queue_paths.runtime_paths(norm_path(DEFAULT_STATE))
GIT_BASH = "C:/Program Files/Git/bin/bash.exe"

PRIORITIES = ["verify", "fixcheck", "lane", "background"]
PRIO_RANK = {p: i for i, p in enumerate(PRIORITIES)}
HOST_NAME = _CONF.get("name", "ember")
HOST_PAUSED = bool(_CONF.get("paused", False))  # the drain switch: no job is dispatched to the host (a move to a new host); reaping goes on
RAM_JOBS = bool(_CONF.get("ram_jobs", False))  # explicit host capability, independently checked by the host helper
RAM_BUDGET_GB = float(_CONF.get("memory_budget_gb", 42))
# Go hashes a package's directory into its compile cache key unless -trimpath is set (cmd/go/internal/work/exec.go,
# buildActionID), and each job builds in its own copy ~/q/<id>/src: without it every job recompiles the whole module
GO_TRIMPATH = bool(_CONF.get("go_trimpath", False))
TMP_ROOT = _CONF.get("tmp_root", "")  # a host tmpfs dir: each disk job's TMPDIR, GOTMPDIR and CQ_RAM_TMP are a fresh dir in it
HOST_SLOTS = _CONF.get("slots", 8)  # the host pool: one slot ~ SLOT_VCPU vCPU / SLOT_RAM_GB GB; the probe counts ~/q/.heavy.0..N-1 and every job runs
#                 heavy with HEAVY_SLOTS=HOST_SLOTS (job.env), the one place the number lives. 0..3 of the old pool stay a subset.
SLOT_VCPU = float(_CONF.get("slot_vcpu", 1.5))
SLOT_RAM_GB = float(_CONF.get("slot_ram_gb", 2.5))
# Measured hard caps on the host, on top of free slots. A heavy job (slots >= 1) starts only if
#   load1 + SLOT_VCPU * slots <= LOAD_CAP  and  mem_avail_gb - SLOT_RAM_GB * slots >= HOST_RAM_FLOOR_GB,
# counting the slots launched but not yet visible in the sample. A light job (slots 0) starts only if load1 < LOAD_CAP +
# LIGHT_LOAD_MARGIN. A sample older than SAMPLE_MAX_AGE (the leader probes every 15 s) or without load/RAM is no headroom.
LOAD_CAP = host_load_cap(os.environ, _CONF)  # None: the host's nproc
HOST_RAM_FLOOR_GB = host_ram_floor_gb(os.environ, _CONF)
PLANNER = host_planner(_CONF)  # "code" (default): the deterministic path plans every round; "haiku": the model plans (cmd_apply)
LIGHT_LOAD_MARGIN = 2.0
SAMPLE_MAX_AGE = 120.0
IDLE_OVERRIDE_SAMPLES = 2  # consecutive probes with no heavy job on the host after which a head job blocked only by the caps starts anyway
MEM_HISTORY_HOURS = 24  # the idle-RAM baseline is the largest MemAvailable seen in this many hours of samples
CAPS_OVERRIDE_NOTE = "caps override: host idle"
HOST_DISK_MIN_KB = 10485760  # lanerun refuses to start below this (exit 75)
LAPTOP_RAM_MIN_GB = 6.0
STARVE_SECONDS = 60 * 60
AGE_FIRST_MIN = 45  # a queued job older than this starts ahead of the shorter ones of its priority (oldest first)
LEADER_STALE = 45.0
LAUNCH_STALE = 600.0  # a job 'launching' this long (since t_launch or the worker's t_touch) with no host dir lost its launcher
LIGHT_CAP = 4  # concurrent light (slots 0) jobs or parts per machine
SAMPLE_LOG_EVERY = 60.0  # seconds between rows of samples.jsonl (written by the leader's tick)
SAMPLE_LOG_MAX_BYTES = 20 * 1024 * 1024  # samples.jsonl moves to samples.jsonl.1 once it is larger than this
PROF_MS_MAX = 60_000  # the two process passes are about 1 s apart; a larger PROFMS is a broken clock reading
PROF_MAX_AGE = 180.0  # seconds a profiled probe's top classes and job cores stay in capacity.json
PROF_SHOWN_AGE = 5.0  # a status line says how old its top classes are beyond this many seconds
EST_HINT_ROWS = 80  # history rows of a workstream that est_hint reads
EST_HINT_MIN_ROWS = 8  # ...the fewest of them that make a hint
EST_HINT_BAND = (0.5, 1.5)  # a median actual / est outside this band gets the submit note's hint
KIND_HINT_ROWS = 80  # history rows of a command kind that kind_hint reads
EST_CAL_ROWS = 8  # latest done runs of the same command key, workstream and machine that est_calibration reads
EST_CAL_MIN_ROWS = 3  # ...the fewest of them that calibrate a job's scheduling estimate
EST_CAL_CLAMP = (0.25, 4.0)  # the scale applied to a declared estimate stays inside this range
KIND_HINT_MIN_ROWS = 8  # ...the fewest of them that make a hint
CPU_PREV_MAX_AGE = 300.0  # the CPU counters a split is differenced against are replaced once older than this
CPU_LAST_MAX_AGE = 60.0  # a split is held in capacity.json this long while the probes cannot compute one
BACKFILL_MAX_MIN = 30  # a job backfills only if its declared estimate is at most this many minutes
BACKFILL_MARGIN_MIN = 2  # ...and ends at least this long before the blocked head job could start
BACKFILL_SHADOW_FACTOR = 0.5  # a running job is assumed to end in this share of its remaining estimate
SPLIT_LIGHT_PARTS = 4  # parts of a light split job
VANISH_SECONDS = 120.0
MAX_ATTEMPTS = 3
TAIL_LINES = 40
LINE_CUT = 300
TERMINAL = ("done", "failed", "timeout", "cancelled")
JOB_STORES = ("jobs", "deferred-jobs", os.path.join("fleet", "jobs"))  # under a state directory; each may have an archive
ARCHIVE_SUFFIX = "-archive"     # <store>-archive/ holds the finished jobs archive_jobs moved out of <store>/
ARCHIVE_AFTER_S = 2 * 86400     # a finished job nothing has written for this long leaves the scanned store
ARCHIVE_HELD_S = 14 * 86400     # the same for one something still waits on (_waited_on)
ARCHIVE_EVERY_S = 3600          # the sweep runs at most this often per state directory
SCAN_SETTLE_S = 10.0            # _jobs_in: a file that changed this recently is read again next time
SCAN_REFRESH_S = 300.0          # _jobs_in: and every file is read again this often, whatever its size and times say
_SCAN_CACHE = {}                # _jobs_in: store directory -> {file name: (signature, bytes, monotonic time read)}; this process only
CREATE_NO_WINDOW = 0x08000000
DETACHED_PROCESS = 0x00000008
CREATE_NEW_PROCESS_GROUP = 0x00000200
CREATE_BREAKAWAY_FROM_JOB = 0x01000000


class CqError(Exception):
    """A refusal with a reason for the caller (printed as {ok: false, error})."""


class SandboxError(CqError):
    """A job's sandbox request that its host's setting cannot take (at submit, and again when the launcher is made)."""


# --------------------------------------------------------------------------- files, locks, atomic writes

class LockTimeout(CqError):
    pass


class FileLock:
    """A byte-range lock on a file: msvcrt on Windows, so it dies with the holding process (no stale lock)."""

    def __init__(self, path, timeout=30.0):
        self.path = path
        self.timeout = timeout
        self.fd = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                if msvcrt:
                    os.lseek(self.fd, 0, os.SEEK_SET)
                    msvcrt.locking(self.fd, msvcrt.LK_NBLCK, 1)
                else:  # pragma: no cover
                    fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except OSError:
                if time.monotonic() > deadline:
                    os.close(self.fd)
                    self.fd = None
                    raise LockTimeout(f"could not take {self.path} within {self.timeout:.0f}s")
                time.sleep(0.05)

    def __exit__(self, *exc):
        if self.fd is not None:
            try:
                if msvcrt:
                    os.lseek(self.fd, 0, os.SEEK_SET)
                    msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
                else:  # pragma: no cover
                    fcntl.flock(self.fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(self.fd)
            self.fd = None
        return False


def full_fsync(fd, fcntl_module=None):
    """Flush fd to the drive: F_FULLFSYNC where the platform has it (on macOS fsync leaves the data in the drive's cache),
    else os.fsync, which is also the fallback when F_FULLFSYNC fails (a file system that does not support it)."""
    module = fcntl if fcntl_module is None else fcntl_module  # None on Windows, which never has the constant
    command = getattr(module, "F_FULLFSYNC", None)
    if command is not None:
        try:
            module.fcntl(fd, command)
            return
        except OSError:
            pass
    os.fsync(fd)


def atomic_write(path, data: bytes, durable=False):
    """Write temp + os.replace: a reader sees the old file or the new one, never half of it (S19)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{secrets.token_hex(2)}"
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            if durable:
                full_fsync(f.fileno())
        last = None
        for _ in range(60):  # Windows refuses the replace while a reader has the target open
            try:
                if durable and os.name == 'nt':
                    import ctypes
                    from ctypes import wintypes
                    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
                    move = kernel.MoveFileExW
                    move.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
                    move.restype = wintypes.BOOL
                    if not move(str(tmp), str(path), 0x1 | 0x8):  # replace + write-through
                        raise ctypes.WinError(ctypes.get_last_error())
                else:
                    os.replace(tmp, path)
                    if durable:
                        directory = os.open(os.path.dirname(path), os.O_RDONLY | os.O_DIRECTORY)
                        try:
                            full_fsync(directory)
                        finally:
                            os.close(directory)
                return
            except PermissionError as e:
                last = e
                time.sleep(0.025)
        raise last
    finally:
        if os.path.exists(tmp):
            with contextlib.suppress(OSError):
                os.remove(tmp)


def write_json(path, obj):
    atomic_write(path, (json.dumps(obj, indent=1, sort_keys=True) + "\n").encode("utf-8"))


def read_json(path, default=None):
    for _ in range(40):
        try:
            with open(path, "rb") as f:
                return json.loads(f.read().decode("utf-8"))
        except FileNotFoundError:
            return default
        except (PermissionError, json.JSONDecodeError):
            time.sleep(0.025)
    return default


def append_line(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "ab") as f:
        f.write((json.dumps(obj, sort_keys=True) + "\n").encode("utf-8"))


def read_lines(path, last=None):
    try:
        with open(path, "rb") as f:
            lines = f.read().decode("utf-8", "replace").splitlines()
    except FileNotFoundError:
        return []
    return lines[-last:] if last else lines


REAL_PATH_S = 2.0  # same_path: a path's resolved form is reused this long within one process
_REAL_PATHS = {}   # same_path: path as given -> (monotonic time resolved, resolved form)


def _resolved(path):
    """The form same_path compares: the real path, normalised and case-folded. Resolving opens the path in the kernel
    (every component when it does not exist), and a watch compares the worktree with the `top` of every job in the
    stores: over 5,000 opens per tick for a handful of distinct strings. So the answer for a string is kept for
    REAL_PATH_S. A failure is not kept."""
    now = time.monotonic()
    hit = _REAL_PATHS.get(path)
    if hit is not None and 0 <= now - hit[0] < REAL_PATH_S:
        return hit[1]
    value = norm_path(os.path.realpath(path)).casefold()
    if len(_REAL_PATHS) >= 1024:
        _REAL_PATHS.clear()
    _REAL_PATHS[path] = (now, value)
    return value


def same_path(a, b):
    try:
        return _resolved(a) == _resolved(b)
    except (OSError, ValueError, TypeError):
        return False


def worktree_identity(path):
    """Identity of the directory incarnation, stable across edits and session handovers."""
    try:
        st = os.stat(path)
    except OSError:
        return None
    birth_ns = getattr(st, "st_birthtime_ns", None)
    if birth_ns is None and os.name == "nt":
        birth_ns = st.st_ctime_ns  # Windows Python before st_birthtime_ns
    return {"device": st.st_dev, "inode": st.st_ino, "birth_ns": birth_ns}


def require_worktree_owner(top, session):
    """Check local coordination for requests carrying an agent session."""
    owner = read_json(os.path.join(top, ".baton", "owner.json"))
    if (not isinstance(session, str) or not session or not isinstance(owner, dict)
            or owner.get("session_id") != session or owner.get("vendor") not in ("claude", "codex")):
        raise CqError("operation requires the current worktree owner session")


def require_request_identity(req, action):
    """A submission or cancellation always names the worktree root and the owner session; the engine then verifies both."""
    missing = [name for name in ("root", "session") if not isinstance(req.get(name), str) or not req.get(name)]
    if missing:
        raise CqError(f"{action} requires {' and '.join(missing)}: the current worktree root and owner session")


def owner_lock(top):
    """Baton's persistent owner.lock; acquire before queue.lock, never in reverse."""
    return FileLock(os.path.join(top, ".baton", "owner.lock"), 2.0)


def notice_matches_worktree(job, top, identity=None, owner_session=None):
    """Do not adopt a result from an earlier directory that used the same path."""
    if not top or not same_path(job.get("top", ""), top):
        return False
    current = identity if identity is not None else worktree_identity(top)
    if current is None:
        return False
    original = job.get("worktree_identity")
    if original is not None:
        return original == current
    # A legacy job cannot prove that a new owner has the original worktree.
    # Hold it across session changes, including when no owner is known.
    if not owner_session or job.get("session") != owner_session:
        return False
    # Also reject a directory clearly created after the submission. A clock
    # margin avoids false positives on filesystems with coarse timestamps.
    submitted = job.get("t_submitted")
    birth_ns = current.get("birth_ns")
    if isinstance(submitted, (int, float)) and isinstance(birth_ns, int) and birth_ns / 1e9 > submitted + 10:
        return False
    return True


def rmtree_force(path):
    def onerr(func, p, _exc):
        with contextlib.suppress(OSError):
            os.chmod(p, stat.S_IWRITE)
            func(p)
    shutil.rmtree(path, onerror=onerr)


def iso(t):
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def cut_lines(lines, n=TAIL_LINES):
    return [l[:LINE_CUT] for l in lines[-n:]]


# --------------------------------------------------------------------------- backends (injected in tests)

def _run(argv, **kw):
    if os.name == "nt":
        kw.setdefault("creationflags", CREATE_NO_WINDOW)
    # Queue helpers are noninteractive. In a live stdio MCP process, inheriting
    # its input pipe can leave Git-for-Windows waiting for the MCP stream's EOF.
    # Preserve deliberate input payloads and explicitly supplied stdin handles.
    if kw.get("input") is None and "stdin" not in kw:
        kw.pop("input", None)
        kw["stdin"] = subprocess.DEVNULL
    return subprocess.run(argv, **kw)


# only failures before the remote command can have started: a drop later on ("Connection reset by", "Connection closed by")
# may follow a script that already acted, so it is not retried
SSH_TRANSIENT = ("ssh: connect to host", "kex_exchange_identification", "during banner exchange", "Could not resolve hostname")


def real_ssh(script, timeout=60, host=None, binding=None):
    """Run a bash script on the host. Returns (rc, stdout, stderr); 255 on timeout or no connection."""
    binding = binding or {}
    if not _address(host or HOST):
        return 255, "", "cq: configure CQ_HOST, LANE_HOST, or host.conf.json address before SSH"
    if binding.get("machine_id"):
        script = ('[ "$(cat /etc/machine-id)" = ' + shlex.quote(binding["machine_id"]) +
                  ' ] || { echo "cq: machine identity mismatch" >&2; exit 78; }\n' + script)
    argv = [SSH, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=3", host or HOST, "bash -c " + shlex.quote(script)]
    deadline = time.monotonic() + timeout
    # a brief drop on the laptop's path to the host (measured: ~0.6% of new connections) fails one ssh; one immediate retry
    # rides it out, where marking the host down would hold dispatch for a backoff. A hung command (TimeoutExpired) is not retried.
    for attempt in range(2):
        try:
            r = _run(argv, stdin=subprocess.DEVNULL, capture_output=True,
                     timeout=timeout if attempt == 0 else max(15, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            return 255, "", "ssh timed out"
        except OSError as e:
            return 255, "", f"ssh failed to start: {e}"
        err = r.stderr.decode("utf-8", "replace")
        if attempt == 0 and r.returncode == 255 and any(s in err for s in SSH_TRANSIENT):
            continue
        break
    fallback = binding.get("fallback_address")
    if (fallback and fallback != (host or HOST) and r.returncode == 255
            and any(s in err for s in SSH_TRANSIENT)):
        # Only pre-command connection failures can switch routes. A timeout/reset
        # after the command may already have acted must never repeat its effects.
        return real_ssh(script, max(1, deadline - time.monotonic()), fallback)
    return r.returncode, r.stdout.decode("utf-8", "replace"), err


def real_run_remoterun(argv, cwd, timeout=540, host=None, binding=None, sandboxed=False):
    """sandboxed: the job this stages runs in the job sandbox (make_launcher's decision, from job_sandbox). The bundled
    helper then makes the staged clone self-contained, because the sandbox hides the mirror its objects are borrowed
    from; the external REMOTERUN helper ignores it."""
    if not _address(host or HOST):
        return 255, "", "cq: configure CQ_HOST, LANE_HOST, or host.conf.json address before remote execution"
    # dispatch binds the execution address, and the helper its ssh client: it would otherwise start whichever
    # `ssh` its own search finds, which is not the client the probes authenticate with
    env = dict(os.environ, LANE_HOST=host or HOST, CQ_SSH=SSH)
    binding = binding or {}
    env.pop("CQ_REMOTE_MACHINE_ID", None)
    if binding.get("machine_id"):
        env["CQ_REMOTE_MACHINE_ID"] = binding["machine_id"]
    env.pop("CQ_REMOTE_SANDBOXED", None)  # an inherited value must not decide for this job
    if sandboxed:
        env["CQ_REMOTE_SANDBOXED"] = "1"
    try:
        # The external helper starts Git and SSH itself; hiding only the helper
        # does not reliably hide the consoles those children may create.
        runner = (os.path.join(os.path.dirname(os.path.abspath(__file__)), "remote_stage.py")
                  if binding.get("portable_runner") else REMOTERUN)
        helper = [sys.executable, runner, *argv]
        if os.name == "nt":
            helper.insert(1, os.path.join(os.path.dirname(os.path.abspath(__file__)), "quiet_helper.py"))
        r = _run(helper, cwd=cwd, env=env, stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 255, "", "[remoterun] timed out (cq)"
    except OSError as e:
        return 255, "", f"[remoterun] failed to start: {e}"
    err = r.stderr.decode("utf-8", "replace")
    fallback = binding.get("fallback_address")
    if (os.path.basename(runner) == "remote_stage.py" and fallback and fallback != (host or HOST)
            and r.returncode == 255 and any(s in err for s in SSH_TRANSIENT)):
        return real_run_remoterun(argv, cwd, timeout, fallback,
                                  {k: v for k, v in binding.items() if k != "fallback_address"}, sandboxed)
    return r.returncode, r.stdout.decode("utf-8", "replace"), err


STAGING_STDERR_CHARS = 2000


def staging_note(rc, err):
    """What a staging helper that exited 0 said on stderr, for the job's record: {"staging_stderr": the last
    STAGING_STDERR_CHARS characters, control characters other than newline and tab shown as "?"}, or {} when it exited
    otherwise or said nothing. A helper that succeeds after falling back (remote_stage's "incremental staging failed;
    sending the full history") would otherwise leave no trace of why. Failure paths keep using err as they did."""
    if rc != 0 or not isinstance(err, str):
        return {}
    text = "".join(c if c in "\n\t" or unicodedata.category(c)[0] != "C" else "?"
                   for c in err.replace("\r\n", "\n")).strip()
    return {"staging_stderr": text[-STAGING_STDERR_CHARS:]} if text else {}


def spawn_detached(argv, env=None, stdout=None, cwd=None):
    """Start argv detached from this process (baton/procs.py popen_detached flags); returns the Popen."""
    kw = dict(stdin=subprocess.DEVNULL, stdout=stdout or subprocess.DEVNULL,
              stderr=subprocess.STDOUT if stdout else subprocess.DEVNULL, env=env, cwd=cwd)
    if os.name != "nt":
        return subprocess.Popen(argv, start_new_session=True, **kw)
    flags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
    try:
        return subprocess.Popen(argv, creationflags=flags | CREATE_BREAKAWAY_FROM_JOB, **kw)
    except OSError:
        return subprocess.Popen(argv, creationflags=flags, **kw)


class Laptop:
    """The laptop's side: heavy lock, free RAM, process liveness, tree kill."""

    def heavy_lock_held(self):
        try:
            fd = os.open(HEAVY_LOCK, os.O_RDWR | os.O_CREAT)
        except OSError:
            return True
        try:
            if msvcrt:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            return False
        except OSError:
            return True
        finally:
            os.close(fd)

    def free_ram_gb(self):
        if os.name != "nt":  # pragma: no cover
            return 99.0
        import ctypes

        class MEMSTATUS(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("sullAvailExtendedVirtual", ctypes.c_ulonglong)]
        st = MEMSTATUS()
        st.dwLength = ctypes.sizeof(MEMSTATUS)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st))
        return round(st.ullAvailPhys / 2**30, 1)

    def mem_stats(self):
        """GetPerformanceInfo in GB: commit and its limit, kernel nonpaged and paged pools, system cache; None on failure."""
        if os.name != "nt":  # pragma: no cover
            return None
        import ctypes
        from ctypes import wintypes
        try:
            class PERF(ctypes.Structure):
                _fields_ = [("cb", wintypes.DWORD)] + [(f, ctypes.c_size_t) for f in (
                    "CommitTotal", "CommitLimit", "CommitPeak", "PhysicalTotal", "PhysicalAvailable", "SystemCache",
                    "KernelTotal", "KernelPaged", "KernelNonpaged", "PageSize")] + [
                    ("HandleCount", wintypes.DWORD), ("ProcessCount", wintypes.DWORD), ("ThreadCount", wintypes.DWORD)]
            p = PERF()
            p.cb = ctypes.sizeof(PERF)
            if not ctypes.windll.psapi.GetPerformanceInfo(ctypes.byref(p), p.cb):
                return None
            gb = lambda pages: round(pages * p.PageSize / 2**30, 2)
            return {"avail_gb": gb(p.PhysicalAvailable), "total_gb": gb(p.PhysicalTotal), "commit_gb": gb(p.CommitTotal),
                    "commit_limit_gb": gb(p.CommitLimit), "np_gb": gb(p.KernelNonpaged), "paged_gb": gb(p.KernelPaged),
                    "cache_gb": gb(p.SystemCache), "procs": p.ProcessCount, "threads": p.ThreadCount, "handles": p.HandleCount}
        except Exception:
            return None

    def top_procs(self, n=6):
        """Working set and private bytes of the n largest executable names; `unreadable` counts the processes not opened."""
        if os.name != "nt":  # pragma: no cover
            return None
        import ctypes
        from ctypes import wintypes
        try:
            # private DLL objects: the argtypes set here change no other caller's calls in the process
            k32, ps = ctypes.WinDLL("kernel32"), ctypes.WinDLL("psapi")

            class PMC(ctypes.Structure):
                _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [(f, ctypes.c_size_t) for f in (
                    "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage",
                    "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage", "PrivateUsage")]
            k32.OpenProcess.restype = wintypes.HANDLE
            k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            k32.CloseHandle.argtypes = [wintypes.HANDLE]
            ps.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(PMC), wintypes.DWORD]
            k32.QueryFullProcessImageNameW.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD)]
            n_pids = 1024
            while True:  # EnumProcesses reports the bytes it filled: grow the list until it leaves room (65536 at most)
                pids = (wintypes.DWORD * n_pids)()
                got = wintypes.DWORD()
                if not ps.EnumProcesses(pids, ctypes.sizeof(pids), ctypes.byref(got)):
                    return None
                if got.value < ctypes.sizeof(pids) or n_pids >= 65536:
                    break
                n_pids *= 2
            groups, unreadable, total_ws, seen = {}, 0, 0, 0
            buf = ctypes.create_unicode_buffer(1024)
            for pid in pids[: got.value // ctypes.sizeof(wintypes.DWORD)]:
                if pid == 0:
                    continue
                seen += 1
                h = k32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
                if not h:
                    unreadable += 1
                    continue
                try:
                    pmc = PMC()
                    pmc.cb = ctypes.sizeof(PMC)
                    size = wintypes.DWORD(len(buf))
                    if not ps.GetProcessMemoryInfo(h, ctypes.byref(pmc), pmc.cb) or not k32.QueryFullProcessImageNameW(
                            h, 0, buf, ctypes.byref(size)):
                        unreadable += 1
                        continue
                    name = os.path.basename(buf.value).lower()
                    name = name[:-4] if name.endswith(".exe") else name
                    g = groups.setdefault(name, [0, 0, 0])
                    g[0] += 1
                    g[1] += pmc.WorkingSetSize
                    g[2] += pmc.PrivateUsage
                    total_ws += pmc.WorkingSetSize
                finally:
                    k32.CloseHandle(h)
            top = sorted(groups.items(), key=lambda kv: -kv[1][1])[:n]
            return {"groups": [{"name": k, "n": v[0], "ws_mb": round(v[1] / 2**20), "priv_mb": round(v[2] / 2**20)} for k, v in top],
                    "ws_gb_total": round(total_ws / 2**30, 2), "n_procs": seen, "unreadable": unreadable}
        except Exception:
            return None

    def pool_top(self, n=3):
        """The n largest nonpaged pool tags (NtQuerySystemInformation SystemPoolTagInformation = 22, no admin needed); None on failure."""
        if os.name != "nt":  # pragma: no cover
            return None
        import ctypes
        from ctypes import wintypes
        try:
            nt = ctypes.windll.ntdll
            size = 1 << 20
            for _ in range(8):
                buf = ctypes.create_string_buffer(size)
                ret = wintypes.ULONG()
                st = nt.NtQuerySystemInformation(22, buf, size, ctypes.byref(ret)) & 0xFFFFFFFF
                if st == 0:
                    break
                if st != 0xC0000004:  # STATUS_INFO_LENGTH_MISMATCH: grow and retry
                    return None
                size = ret.value + (64 << 10)
            else:
                return None
            raw = buf.raw  # one copy: every .raw access copies the whole buffer
            rows = []
            for i in range(int.from_bytes(raw[0:4], "little")):
                # entries start at offset 8 (x64). SYSTEM_POOLTAG is 40 bytes: Tag[4], PagedAllocs, PagedFrees, pad,
                # PagedUsed (8 bytes), NonPagedAllocs, NonPagedFrees (unsigned 32-bit), NonPagedUsed (8 bytes)
                o = 8 + 40 * i
                tag = raw[o:o + 4].decode("latin-1")
                outstanding = (int.from_bytes(raw[o + 24:o + 28], "little") - int.from_bytes(raw[o + 28:o + 32], "little")) & 0xFFFFFFFF
                used = int.from_bytes(raw[o + 32:o + 40], "little")
                rows.append((used, tag, outstanding))
            rows.sort(reverse=True)
            return [{"tag": t, "mb": round(u / 2**20, 1), "outstanding": o} for u, t, o in rows[:n]]
        except Exception:
            return None

    def pid_alive(self, pid):
        if not pid:
            return False
        if os.name != "nt":  # pragma: no cover
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False
        import ctypes
        h = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))  # PROCESS_QUERY_LIMITED_INFORMATION
        if not h:
            return False
        try:
            code = ctypes.c_ulong()
            ctypes.windll.kernel32.GetExitCodeProcess(h, ctypes.byref(code))
            return code.value == 259  # STILL_ACTIVE
        finally:
            ctypes.windll.kernel32.CloseHandle(h)

    def kill_tree(self, pid):
        if os.name == "nt":
            _run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)
        else:  # pragma: no cover
            with contextlib.suppress(OSError):
                os.kill(pid, 15)


class Ctx:
    """Everything with side effects outside the state dir, so tests can inject it."""

    def __init__(self, state=None, now=None, ssh=None, run_remoterun=None, spawn=None, laptop=None, sync=False,
                 artifact_stream=None):
        self.state = norm_path(state or os.environ.get("CQ_STATE") or DEFAULT_STATE)
        self.legacy_state = norm_path(queue_paths.config_state(self.state))
        self.generation = 2 if queue_paths.is_generation2(self.state) else 1
        self.host_id = host_conf(self.state).get("host_id")
        if self.host_id == "ember":
            self.host_id = None  # primary identity; actual shard contexts set their registry ID
        self.now = now or time.time
        self.ssh = ssh or real_ssh
        self.run_remoterun = run_remoterun or real_run_remoterun
        self.spawn = spawn  # fn(ids) -> None; None = start the detached worker (or run inline when sync)
        self.laptop = laptop or Laptop()
        self.sync = sync
        self.artifact_stream = artifact_stream

    def p(self, *parts):
        if parts and (parts[0] in ("hosts.json", "host.conf.json", "runtime.local.json", "laptop.conf.json")
                      or parts == ("fleet", "config.json")):
            return os.path.join(self.legacy_state, *parts)
        return os.path.join(self.state, *parts)

    def qlock(self, timeout=30.0):
        return FileLock(self.p("queue.lock"), min(timeout, getattr(self, "qlock_timeout", timeout)))


# --------------------------------------------------------------------------- git

def git(cwd, *args, env=None, check=True, input=None):
    e = dict(os.environ, **(env or {}))
    r = _run(["git", "-c", "core.quotepath=off", *args], cwd=cwd, env=e, capture_output=True, input=input)
    out = r.stdout.decode("utf-8", "replace").strip()
    if check and r.returncode != 0:
        raise CqError(f"git {' '.join(args)} failed in {cwd}: {r.stderr.decode('utf-8', 'replace').strip()[:300]}")
    return out if check else (r.returncode, out)


CARRY_DIR = ".baton/remote"
CARRY_MAX_BYTES = 2 * 1024 * 1024


def carry_files(top):
    """The submitting worktree's .baton/remote/ files (repo-relative, '/'-separated), or [] when it is absent. Local
    session scripts that git ignores: regular files only (a link, a device or an unreadable entry refuses the submit),
    at most CARRY_MAX_BYTES in all; __pycache__ directories and *.pyc files are left out."""
    root = os.path.join(top, *CARRY_DIR.split("/"))
    if os.path.islink(root):
        raise CqError(f"{CARRY_DIR} is a link: it is not carried")
    if not os.path.isdir(root):
        return []
    out, total = [], 0
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d != "__pycache__")
        for d in dirnames:
            if os.path.islink(os.path.join(dirpath, d)):
                raise CqError(f"{CARRY_DIR} holds a link ({d}): only regular files are carried")
        for n in sorted(filenames):
            if n.endswith(".pyc"):
                continue
            full = os.path.join(dirpath, n)
            if os.path.islink(full) or not os.path.isfile(full):
                raise CqError(f"{CARRY_DIR} holds {n}, which is not a regular file: only regular files are carried")
            total += os.path.getsize(full)
            if total > CARRY_MAX_BYTES:
                raise CqError(f"{CARRY_DIR} is over {CARRY_MAX_BYTES // 1024} KiB: it is not carried (trim it)")
            out.append(posixpath.join(CARRY_DIR, os.path.relpath(full, root).replace("\\", "/")))
    return out


def snapshot(ctx, top, job_id, source):
    """Return (sha, snapshot_made). 'worktree' = HEAD + tracked diff + untracked non-ignored files, built with a
    temporary index so the worktree, HEAD and the real index are untouched; the commit is held by refs/cq/<id>.
    Whatever the source, the worktree's .baton/remote/ files (git-ignored, local) are added to the snapshot at the same
    relative path; they exist only in that held commit, never on a branch."""
    head = git(top, "rev-parse", "HEAD")
    if source == "head":
        base = head
    elif source == "worktree":
        base = None
    elif re.fullmatch(r"[0-9a-fA-F]{7,40}", source):
        base = git(top, "rev-parse", "--verify", "-q", source + "^{commit}")
    else:
        raise CqError(f"source must be 'worktree', 'head' or a commit sha, got {source!r}")
    carry = carry_files(top)
    if base is not None and not carry:
        return base, False
    tmp_index = ctx.p("tmp", f"index-{job_id}")
    os.makedirs(os.path.dirname(tmp_index), exist_ok=True)
    env = {"GIT_INDEX_FILE": tmp_index, "GIT_AUTHOR_NAME": "compute-queue", "GIT_AUTHOR_EMAIL": "cq@localhost",
           "GIT_COMMITTER_NAME": "compute-queue", "GIT_COMMITTER_EMAIL": "cq@localhost", "GIT_LITERAL_PATHSPECS": "1"}
    try:
        git(top, "read-tree", base or "HEAD", env=env)
        if base is None:
            git(top, "add", "-A", env=env)
        if carry:
            git(top, "add", "-f", "--pathspec-from-file=-", "--pathspec-file-nul", env=env,
                input="\0".join(carry).encode("utf-8"))
        tree = git(top, "write-tree", env=env)
        if tree == git(top, "rev-parse", (base or head) + "^{tree}"):
            return (base or head), False  # nothing to add to the base commit: the base itself
        commit = git(top, "-c", "commit.gpgsign=false", "commit-tree", tree, "-p", base or head, "-m", f"cq-{job_id}", env=env)
        git(top, "update-ref", f"refs/cq/{job_id}", commit)
        return commit, True
    finally:
        for suffix in ("", ".lock"):
            with contextlib.suppress(OSError):
                os.remove(tmp_index + suffix)


def drop_ref(top, job_id):
    if not top:
        return
    with contextlib.suppress(Exception):
        git(top, "update-ref", "-d", f"refs/cq/{job_id}", check=False)


# --------------------------------------------------------------------------- job store

def new_id(now, host_id=None):
    prefix = "cq-" + (host_id + "-" if host_id else "")
    return prefix + time.strftime("%Y%m%dT%H%M%S", time.gmtime(now)) + "-" + secrets.token_hex(8)


def job_file(ctx, job_id, deferred=False):
    if is_fleet_id(job_id):
        return ctx.p("fleet", "jobs", job_id + ".json")
    return ctx.p("deferred-jobs" if deferred else "jobs", job_id + ".json")


def is_fleet_id(job_id):
    return isinstance(job_id, str) and re.fullmatch(r"cq-(?:v2--)?fleet-[A-Za-z0-9_-]+", job_id) is not None


def archived_file(path):
    """Where archive_jobs puts a job file: <store>-archive/<same name>, beside the store."""
    return os.path.join(os.path.dirname(path) + ARCHIVE_SUFFIX, os.path.basename(path))


def load_job(ctx, job_id):
    """One job by ID, from its store or, when archive_jobs has moved it out, from that store's archive. A file in the
    store wins: saving an archived job puts it back in the store, and the next sweep moves it again."""
    if is_fleet_id(job_id):
        path = job_file(ctx, job_id)
        job = read_json(path)
        return job if job is not None or os.path.lexists(path) else read_json(archived_file(path))
    paths = (job_file(ctx, job_id, True), job_file(ctx, job_id))
    deferred, normal = read_json(paths[0]), read_json(paths[1])
    if normal is not None and deferred is not None:
        raise CqError("duplicate job ID across ordinary and deferred stores")
    if deferred is not None or normal is not None:
        return deferred if deferred is not None else normal
    if any(os.path.lexists(p) for p in paths):
        return None  # in a store but locked or unreadable just now: an archived copy would be an older record
    for path in paths:
        job = read_json(archived_file(path))
        if job is not None:
            return job
    return None


def save_job(ctx, job):
    path = job_file(ctx, job["id"], bool(job.get("defer")))
    if is_fleet_id(job["id"]):
        atomic_write(path, (json.dumps(job, indent=1, sort_keys=True) + '\n').encode('utf-8'), durable=True)
    else:
        write_json(path, job)


def _job_text(path):
    """(bytes, parsed) of one job file, with read_json's patience for a file that is locked or half written;
    (None, None) when it is missing or stays unreadable."""
    for _ in range(40):
        try:
            with open(path, "rb") as f:
                raw = f.read()
            return raw, json.loads(raw.decode("utf-8"))
        except FileNotFoundError:
            return None, None
        except (PermissionError, json.JSONDecodeError):
            time.sleep(0.025)
    return None, None


def _jobs_in(ctx, directory, newer_than_s=None):
    """Every job of one store, in file-name order, as fresh objects the caller may change. With `newer_than_s`, only
    the files written within that many seconds (for a bounded read of an archive).

    One directory listing gives each file's size and times. A file is opened only when those differ from what this
    process saw when it last read the file, when the file had changed within SCAN_SETTLE_S of that read (two writes in
    one clock tick can leave size and time the same), or once SCAN_REFRESH_S has passed. Otherwise the bytes read
    before are parsed again. A tick used to open every job file of every store some sixteen times: with thousands of
    finished jobs that was tens of thousands of file opens a second across the sessions, each one scanned by the
    virus scanner, and Windows keeps a small share of the kernel objects of that many opens for good."""
    d = os.path.normpath(ctx.p(directory))
    try:
        with os.scandir(d) as it:
            entries = sorted(((e.name, e) for e in it if e.name.endswith(".json")), key=lambda pair: pair[0])
    except FileNotFoundError:
        _SCAN_CACHE.pop(d, None)
        return []
    cache, kept, jobs = _SCAN_CACHE.get(d, {}), {}, []
    for name, entry in entries:
        try:
            st = entry.stat()
            sig = (st.st_mtime_ns, st.st_ctime_ns, st.st_size) if stat.S_ISREG(st.st_mode) else None
        except OSError:
            sig = None  # never hide a job because its metadata could not be read
        if newer_than_s is not None and sig is not None and time.time() - sig[0] / 1e9 > newer_than_s:
            if name in cache:
                kept[name] = cache[name]
            continue
        hit, mono = cache.get(name), time.monotonic()
        if hit is not None and sig is not None and hit[0] == sig and 0 <= mono - hit[2] < SCAN_REFRESH_S:
            kept[name] = hit
            j = json.loads(hit[1].decode("utf-8"))
        else:
            raw, j = _job_text(os.path.join(d, name))
            # file times are on the real clock, so the settle test is too (ctx.now is the queue's injectable clock)
            if raw is not None and sig is not None and time.time() - sig[0] / 1e9 >= SCAN_SETTLE_S:
                kept[name] = (sig, raw, mono)
        if isinstance(j, dict) and "id" in j:
            jobs.append(j)
    _SCAN_CACHE[d] = kept
    return jobs


def _waited_on(job):
    """A finished job something still finds by scanning its store: a completion notice not yet delivered or artifacts
    not yet fetched."""
    return bool((job.get("notify", True) and not job.get("notified") and not job.get("parent"))
                or job.get("artifact_missing") or job.get("artifact_pending"))


def _ram_held(job):
    """A finished RAM job whose copy on the host is not yet confirmed cleaned: reserved_memory still counts its
    declaration and cleanup_ram still retries, both by scanning the store, so it never leaves it."""
    return job.get("temp_storage") == "ram" and job.get("placed_where") == "host" and not job.get("ram_cleaned")


def archive_jobs(ctx, force=False):
    """Move finished jobs nothing has written for ARCHIVE_AFTER_S out of the stores every tick scans, into
    <store>-archive/. At most once per ARCHIVE_EVERY_S per state directory unless `force`. A job something still
    waits on (_waited_on) stays for ARCHIVE_HELD_S, and one whose RAM copy is not cleaned (_ram_held) stays until it
    is. load_job still finds an archived job by its ID, so status, review
    and the host clean-up of its directory go on working; lists and scans no longer include it. Returns the number
    moved. File times are compared with the real clock."""
    wall = time.time()
    stamp = ctx.p("archive.at")
    if not force:
        with contextlib.suppress(OSError):
            # abs: a file time can be a few milliseconds ahead of time.time() (Windows reads a coarser clock here)
            if abs(wall - os.path.getmtime(stamp)) < ARCHIVE_EVERY_S:
                return 0
    atomic_write(stamp, b"")
    moved = 0
    for directory in JOB_STORES:
        if ctx.host_id and directory != "jobs" and directory != "deferred-jobs":
            continue  # a host's shard keeps no fleet store (all_fleet_jobs)
        d = ctx.p(directory)
        old = []
        try:
            with os.scandir(d) as it:
                for e in it:
                    with contextlib.suppress(OSError):  # one entry that cannot be read does not skip the store
                        if e.name.endswith(".json") and wall - e.stat().st_mtime >= ARCHIVE_AFTER_S:
                            old.append(e.name)
        except OSError:
            continue
        old.sort()
        for i in range(0, len(old), 200):  # short holds of the queue lock: a submit or a reap gets in between
            with ctx.qlock():
                for name in old[i:i + 200]:
                    path = os.path.join(d, name)
                    try:
                        with open(path, "rb") as f:
                            job = json.loads(f.read().decode("utf-8"))
                        age = wall - os.path.getmtime(path)  # again under the lock: the listing may be stale
                    except (OSError, ValueError):
                        continue
                    if (not isinstance(job, dict) or job.get("status") not in TERMINAL or age < ARCHIVE_AFTER_S
                            or _ram_held(job) or (_waited_on(job) and age < ARCHIVE_HELD_S)):
                        continue
                    with contextlib.suppress(OSError):
                        os.makedirs(os.path.dirname(archived_file(path)), exist_ok=True)
                        os.replace(path, archived_file(path))
                        moved += 1
    return moved


def all_deferred_jobs(ctx):
    return _jobs_in(ctx, "deferred-jobs")


def all_fleet_jobs(ctx):
    return [] if ctx.host_id else _jobs_in(ctx, "fleet/jobs")


def all_jobs(ctx):
    return _jobs_in(ctx, "jobs") + all_deferred_jobs(ctx)


def running_jobs(ctx):
    """The leaf jobs holding host slots now (launching or running): what the backfill window is measured on."""
    return [j for j in all_jobs(ctx) if j.get("status") in ("launching", "running") and j.get("placed_where") == "host" and not j.get("children") and not (j.get("defer") and j.get("status") == "launching")]


def order_key(job):
    return (PRIO_RANK.get(job.get("priority"), 2), job.get("t_submitted", 0), job.get("queue_seq", 0), job["id"])


def start_key(job, now=None):
    """The order the queue starts in: priority first; within a priority, jobs queued longer than AGE_FIRST_MIN go first,
    oldest first (short jobs cannot pass a long one for ever); then the shortest scheduling estimate (sched_est) first,
    submitted time breaking ties. `order_key` stays the plain priority-then-submitted order of listings."""
    now = time.time() if now is None else now
    prio, sub, seq = PRIO_RANK.get(job.get("priority"), 2), job.get("t_submitted", 0), job.get("queue_seq", 0)
    if now - sub > AGE_FIRST_MIN * 60:
        return (prio, 0, sub, seq, job["id"])
    est = sched_est(job)
    return (prio, 1, float(est) if _number(est) else 10.0, sub, seq, job["id"])


def sched_est(job):
    """The minutes the start order plans a job by: est_sched (set at submit by est_calibration) when the job has one, else
    its declared est_minutes. The declared estimate stays the one shown, recorded in history.jsonl and compared with, and
    backfill keeps it: a median is overrun by half the runs it was taken from (README, start order)."""
    est = job.get("est_sched")
    return est if _number(est) else job.get("est_minutes")


def queued_jobs(ctx, now=None):
    """The queued jobs in start order, aged at `now` (default: ctx.now()); callers that also plan pass their own `now`."""
    now = ctx.now() if now is None else now
    return sorted((j for j in all_jobs(ctx) if j.get("status") == "queued" and not j.get("parent")), key=lambda j: start_key(j, now))


def decision(ctx, rec):
    rec = dict(rec, t=iso(ctx.now()))
    append_line(ctx.p("decisions.jsonl"), rec)


# --------------------------------------------------------------------------- submit / list / get / cancel

def _int(v, name, lo, hi, default):
    if v is None:
        return default
    if isinstance(v, bool) or not isinstance(v, (int, float)) or int(v) != v:
        raise CqError(f"{name} must be an integer")
    v = int(v)
    if not lo <= v <= hi:
        raise CqError(f"{name} must be {lo}..{hi}, got {v}")
    return v


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def est_hint(ctx, workstream, est):
    """The submit note's hint when this workstream's recent jobs ran far from their declared estimates: the median of
    actual / est (both in minutes, from history.jsonl) over its last EST_HINT_ROWS rows. '' when there are too few rows
    or the median is inside EST_HINT_BAND. It is advice only: nothing is scheduled from it."""
    ratios = []
    for line in read_lines(ctx.p("history.jsonl")):
        try:
            h = json.loads(line)
        except json.JSONDecodeError:
            continue
        # only a done run says something about its estimate; a split part's est is its whole job's, so it is left out too
        if not isinstance(h, dict) or h.get("workstream") != workstream or h.get("status") != "done" or h.get("parent"):
            continue
        e, a = h.get("est"), h.get("actual")
        if _is_num(e) and _is_num(a) and e > 0 and a > 0:
            ratios.append(a / e)
    ratios = ratios[-EST_HINT_ROWS:]
    if len(ratios) < EST_HINT_MIN_ROWS:
        return ""
    ratio = statistics.median(ratios)
    if EST_HINT_BAND[0] <= ratio <= EST_HINT_BAND[1]:
        return ""
    return (f"; history: {workstream} jobs ran a median {ratio:.2f}x of their declared estimate (n={len(ratios)}), "
            f"you declared {est} min")


def _est_note(ctx, workstream, est):
    """est_hint for the submit note, or '' when it raises: the job is saved before the note is made, so the note never fails."""
    try:
        return est_hint(ctx, workstream, est)
    except Exception:
        return ""


def cmd_key(command):
    """The calibration key of a command: its first six words that are not NAME=value, so `go test -run X ./a` and
    `go test -race ./...` stay apart (history_kind's two words pool them)."""
    return " ".join([w for w in command.split() if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", w)][:6])[:200]


def est_calibration(ctx, key, workstream, where):
    """(scale, n, median): the median actual / est of the last EST_CAL_ROWS done, non-part history rows with this cmd_key,
    workstream and machine (`where` as placed; a job submitted with where 'any' matches either), the scale being that
    median clamped to EST_CAL_CLAMP; None with fewer than EST_CAL_MIN_ROWS. The scale applies to the declared estimate
    (history keeps declared estimates, so it never compounds). Rows written before the cmd key existed never match."""
    ratios = []
    for line in read_lines(ctx.p("history.jsonl"), 2000):
        try:
            h = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (not isinstance(h, dict) or h.get("cmd") != key or h.get("workstream") != workstream or h.get("status") != "done"
                or h.get("parent") or (where != "any" and h.get("where") != where)):
            continue
        e, a = h.get("est"), h.get("actual")
        if _fin(e) and _fin(a) and e > 0 and a > 0:
            ratios.append(a / e)
    ratios = ratios[-EST_CAL_ROWS:]
    if len(ratios) < EST_CAL_MIN_ROWS:
        return None
    m = statistics.median(ratios)
    return min(max(m, EST_CAL_CLAMP[0]), EST_CAL_CLAMP[1]), len(ratios), m


def _est_cal(ctx, key, workstream, where):
    """est_calibration, or None when it raises: a submit never fails over the history file."""
    try:
        return est_calibration(ctx, key, workstream, where)
    except Exception:
        return None


def kind_hint(ctx, kind, slots, est, where="host"):
    """The submit note's advice when the recent host runs of this command kind reserved far more than they used: p75 of their
    cores (history.jsonl `cores`) against slots * SLOT_VCPU, over the last KIND_HINT_ROWS done host rows of the kind (at least
    KIND_HINT_MIN_ROWS). A hint needs a host job (where not 'laptop'), slots >= 1 and reserved > 2 * p75. Under-reservation is
    not advised: the kind (the first two words of the command) mixes targeted and full runs, so a `go test -run X ./pkg/` would
    be told to take the slots of `-race ./...`. The median actual / est of those rows is appended when it is outside
    EST_HINT_BAND. '' otherwise. It is advice only: nothing is scheduled from it, and est is not used (the estimate is the
    job's own)."""
    rows = []
    for line in read_lines(ctx.p("history.jsonl")):
        try:
            h = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(h, dict) and h.get("kind") == kind and h.get("where") == "host" and h.get("status") == "done" and _fin(h.get("cores")) and h["cores"] > 0:
            rows.append(h)
    rows = rows[-KIND_HINT_ROWS:]
    if where == "laptop" or slots < 1 or len(rows) < KIND_HINT_MIN_ROWS:
        return ""
    p75 = statistics.quantiles([h["cores"] for h in rows], n=4)[2]
    reserved = slots * SLOT_VCPU
    if reserved <= 2 * p75:
        return ""
    ratios = [h["actual"] / h["est"] for h in rows if not h.get("parent") and _fin(h.get("est")) and _fin(h.get("actual")) and h["est"] > 0 and h["actual"] > 0]
    text = (f"; history: {len(rows)} recent '{kind}' runs used p75 {p75:.1f} cores, you reserved {slots} slots "
            f"(~{reserved:g} vCPU): about {math.ceil(p75 / SLOT_VCPU - 1e-9)} slots fits")
    if ratios:
        r = statistics.median(ratios)
        if not EST_HINT_BAND[0] <= r <= EST_HINT_BAND[1]:
            text += f" and ran a median {r:.2f}x of their estimate"
    return text


def _fin(v):
    return _is_num(v) and math.isfinite(v)


def _kind_note(ctx, command, slots, est, where):
    """kind_hint for the submit note, or '' when it raises: the job is saved before the note is made, so the note never fails."""
    try:
        return kind_hint(ctx, history_kind(command), slots, est, where)
    except Exception:
        return ""


def create_deferred_receipt(top, identity, receipt):
    """Create an atomic, non-replacing receipt in real worktree-owned directories."""
    if worktree_identity(top) != identity:
        raise CqError("worktree identity changed before receipt")
    directory = top
    for component in (".baton", "compute"):
        directory = os.path.join(directory, component)
        try:
            os.mkdir(directory)
        except FileExistsError:
            pass
        st = os.lstat(directory)
        if (not stat.S_ISDIR(st.st_mode) or stat.S_ISLNK(st.st_mode)
                or getattr(st, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)):
            raise CqError("deferred receipt directory must be a real directory, not a link or reparse point")
    if worktree_identity(top) != identity:
        raise CqError("worktree identity changed before receipt")
    path = os.path.join(directory, receipt["id"] + ".json")
    tmp = os.path.join(directory, ".receipt-" + secrets.token_hex(16) + ".tmp")
    linked = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(tmp, flags, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write((json.dumps(receipt, ensure_ascii=False) + "\n").encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        os.link(tmp, path)  # atomic and fails if any file or link already occupies the ID
        result = os.lstat(path)
        linked = (result.st_dev, result.st_ino)
        if worktree_identity(top) != identity:
            raise CqError("worktree identity changed during receipt creation")
        return path, linked
    except Exception:
        if linked:
            remove_deferred_receipt(path, linked)
        raise
    finally:
        with contextlib.suppress(OSError):
            os.unlink(tmp)


def remove_deferred_receipt(path, identity):
    try:
        st = os.lstat(path)
        if not stat.S_ISLNK(st.st_mode) and (st.st_dev, st.st_ino) == identity:
            os.unlink(path)
    except FileNotFoundError:
        pass


def normalize_gets(raw):
    """Portable file names with no duplicate or file/directory output collision."""
    if raw is None:
        return []
    if not isinstance(raw, list) or len(raw) > 64:
        raise CqError("gets must be a list of at most 64 file paths")
    result, keys, total_bytes = [], set(), 0
    for value in raw:
        if not isinstance(value, str) or not value or any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise CqError("gets entries must be nonempty relative file paths")
        path = value.replace("\\", "/")
        try:
            size = len(path.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise CqError("gets paths must be valid Unicode") from exc
        total_bytes += size
        if size > 1024 or total_bytes > 8192:
            raise CqError("gets paths exceed the 1024-byte per-path or 8192-byte total limit")
        parts = path.split("/")
        if (posixpath.isabs(path) or any(p in ("", ".", "..") or any(c in p for c in ':<>"|?*') or p.rstrip(" .") != p for p in parts)
                or any(re.fullmatch(r"(?i)(?:con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\..*)?", p) for p in parts)):
            raise CqError("gets entries must name portable files inside the source tree")
        key = path.casefold()
        if any(key == old or key.startswith(old + "/") or old.startswith(key + "/") for old in keys):
            raise CqError("gets paths collide or contain one another")
        keys.add(key)
        result.append(path)
    return result


JOBBOX = "/usr/local/libexec/baton/jobbox"  # installed on a host by host/install_jobbox.sh; run.sh runs a boxed job through it
JOBBOX_MISSING = f"host has no job sandbox helper ({JOBBOX})"
SANDBOX_LINUX_ONLY = "the job sandbox exists only on Linux hosts"


def sandbox_request(req):
    """(sandbox, network) as a submit request gives them: sandbox True, False or None (not given), network a bool."""
    sandbox, network = req.get("sandbox"), req.get("network")
    if sandbox is not None and not isinstance(sandbox, bool):
        raise CqError("sandbox must be true or false")
    if network is not None and not isinstance(network, bool):
        raise CqError("network must be true or false")
    return sandbox, bool(network)


SANDBOX_RANK = {"off": 0, "optional": 1, "required": 2, "invalid": 3}


def stricter_sandbox(*modes):
    """The strictest of the given host settings (None is no opinion); an unknown value counts as "invalid"."""
    modes = [m if m in SANDBOX_RANK else "invalid" for m in modes if m is not None]
    return max(modes, key=SANDBOX_RANK.__getitem__) if modes else "off"


def sandbox_plan(job, binding):
    """(boxed, network) for a job on the host `binding` describes: its "sandbox" setting (off when absent) and the job's
    sandbox/network request. Raises SandboxError when the request and the host's setting disagree, or when the setting
    itself is invalid. network matters only to a boxed job; for any other it is accepted and ignored."""
    mode = (binding or {}).get("sandbox", "off")
    asked = job.get("sandbox")
    if mode not in ("off", "optional", "required"):
        raise SandboxError('host setting "sandbox" must be off, optional or required')  # or host.conf.json is unreadable
    if mode == "off":
        if asked is True:
            raise SandboxError('this host has no job sandbox (host setting "sandbox" is off)')
        return False, False
    if mode == "required":
        if asked is False:
            raise SandboxError("this host requires the job sandbox")
        return True, job.get("network") is True
    return asked is True, asked is True and job.get("network") is True


def cmd_submit(ctx, req):
    if getattr(ctx, 'generation', 1) == 2:
        raise CqError('generation-two jobs require the contained fleet submission path')
    command = req.get("command")
    if not isinstance(command, str) or not command.strip():
        raise CqError("command is required")
    require_request_identity(req, "submission")
    root = req.get("root")
    if not root or not os.path.isdir(root):
        raise CqError(f"root {root!r} is not a directory")
    subdir = posixpath.normpath(str(req.get("subdir") or ".").replace("\\", "/"))
    if subdir.startswith("..") or posixpath.isabs(subdir) or re.match(r"^[A-Za-z]:", subdir):
        raise CqError("subdir must be a relative path inside the work tree")
    where = req.get("where") or "host"
    if where not in ("host", "laptop", "any"):
        raise CqError("where must be host, laptop or any")
    if ctx.host_id and where != "host":
        raise CqError("a remote queue shard requires where host")
    if where == "laptop" and not laptop_executes():
        raise CqError(f"where 'laptop' is refused: the laptop is {LAPTOP_NOT_EXECUTOR} (only a Windows controller runs "
                      "laptop jobs); use where 'host' or 'fleet' to run it on a remote Linux device")
    sandbox, network = sandbox_request(req)
    if sandbox is True and where == "laptop":
        raise CqError(SANDBOX_LINUX_ONLY)
    if sandbox is True and where == "any":
        where = "host"  # the laptop has no sandbox: a sandboxed job is pinned to the host
    if where != "laptop":  # refused here when this host's setting cannot take the request; make_launcher decides again
        sandbox_plan({"sandbox": sandbox, "network": network}, host_binding())
    defer = req.get("defer")
    if defer is not None and defer not in ("spare", "overnight"):
        raise CqError("defer must be spare or overnight")
    if defer and (req.get("where") != "host" or not (req.get("host_id") or req.get("_deferred_host_selected"))):
        raise CqError("deferred jobs require explicit where host and host_id")
    temp_storage = req.get("temp_storage", "disk")
    if temp_storage not in ("disk", "ram"):
        raise CqError("temp_storage must be disk or ram")
    memory_max_gb = req.get("memory_max_gb")
    if temp_storage == "ram":
        if where != "host" or not RAM_JOBS:
            raise CqError("RAM jobs require where host and host.conf.json ram_jobs enabled; no disk fallback")
        if not _valid(_in(0.25, RAM_BUDGET_GB), memory_max_gb):
            raise CqError(f"RAM jobs require memory_max_gb in 0.25..{RAM_BUDGET_GB:g} (processes plus tmpfs)")
        if req.get("split"):
            raise CqError("RAM jobs do not support split; submit bounded jobs separately")
    elif defer:
        if not _valid(_in(0.25, 50), memory_max_gb):
            raise CqError("deferred disk jobs require memory_max_gb in 0.25..50")
    elif memory_max_gb is not None:
        raise CqError("memory_max_gb applies only to temp_storage ram")
    slots = _int(req.get("slots"), "slots", 0, HOST_SLOTS, 1)
    self_heavy = bool(req.get("self_heavy", False))
    if defer and (temp_storage != "disk" or self_heavy or req.get("split") is not None or slots < 1):
        raise CqError("deferred jobs require disk, slots >= 1, no split and no self_heavy")
    if defer:
        if _CONF.get("_policy_error"):
            raise CqError(_CONF["_policy_error"])
        try:
            foreground = queue_policy.policy_fields(_CONF, HOST_SLOTS)["foreground_slots"]
        except ValueError as exc:
            raise CqError(str(exc)) from exc
        if slots > HOST_SLOTS - foreground:
            raise CqError(f"deferred slots must leave {foreground} foreground slots in the configured host pool")
    if self_heavy and slots < 1:
        raise CqError("self_heavy needs slots >= 1 (the slots the command takes itself)")
    if self_heavy and where != "host":
        raise CqError("self_heavy jobs take host slots: use where 'host'")
    if where == "laptop" and slots > 1:
        raise CqError("the laptop runs one heavy job at a time: use slots 0 or 1 with where 'laptop'")
    if where == "host" and slots > 0:
        mx = host_max_slots(((load_capacity(ctx) or {}).get("host")) or {})
        if mx is not None and slots > mx:  # the measured caps could never admit it, so it would sit queued for good
            raise CqError(f"slots {slots} can never start on the host: one slot is {SLOT_VCPU:g} vCPU / {SLOT_RAM_GB:g} GB and the "
                          f"load and RAM caps leave at most {mx} slots even when idle (use at most {mx})")
    est = _int(req.get("est_minutes"), "est_minutes", 1, 100000, 10)
    if defer and est > 480:
        raise CqError("deferred estimate must be at most 480 minutes")
    priority = req.get("priority") or "lane"
    if priority not in PRIORITIES:
        raise CqError(f"priority must be one of {PRIORITIES}")
    if defer:
        priority = "background"
    timeout_min = _int(req.get("timeout_minutes"), "timeout_minutes", 1, 480, min(480, max(30, 3 * est)))
    split = req.get("split")
    has_part = "{part}" in command
    if split is not None:
        if not isinstance(split, dict) or not isinstance(split.get("max_parts"), int) or isinstance(split.get("max_parts"), bool):
            raise CqError("split must be {max_parts: n}")
        if not 2 <= split["max_parts"] <= 8:
            raise CqError("split.max_parts must be 2..8")
        if not has_part:
            raise CqError("split given but the command has no {part} placeholder")
        if where == "laptop" and slots > 0:
            raise CqError("a split job runs on the host (the laptop takes one heavy job at a time)")
        split = {"max_parts": split["max_parts"]}
    elif has_part:
        raise CqError("the command holds {part} but no split was given: add split {max_parts: n}")
    gets = normalize_gets(req.get("gets"))
    if "artifact_max_mb" in req or "artifact_total_mb" in req:
        raise CqError("artifact limits require the generation-aware queue entry point")
    words = [w for w in command.split() if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", w)]  # NAME=value stays out of the label
    label = (req.get("label") or " ".join(words[:6]) or " ".join(command.split()[:6]))[:60]

    top = git(root, "rev-parse", "--show-toplevel")
    identity = worktree_identity(top)
    if identity is None:
        raise CqError("cannot identify the submitting worktree")
    require_worktree_owner(top, req["session"])
    now = ctx.now()
    job_id = new_id(now, ctx.host_id) if ctx.host_id else new_id(now)
    if load_job(ctx, job_id):
        raise CqError("job ID collision; no job submitted")
    sha, snap = snapshot(ctx, top, job_id, req.get("source") or "worktree")
    base_sha = git(top, "rev-parse", sha + "^") if snap else sha  # the commit the snapshot was built on
    if identity != worktree_identity(top):
        raise CqError("submitting worktree changed during snapshot")
    job = {
        "id": job_id, "status": "queued", "command": command, "subdir": subdir, "source": req.get("source") or "worktree",
        "sha": sha, "base": base_sha, "snapshot": snap, "where": where, "slots": slots, "self_heavy": self_heavy, "est_minutes": est,
        "priority": priority, "timeout_minutes": timeout_min, "label": label, "gets": gets, "split": split,
        "temp_storage": temp_storage, "memory_max_gb": memory_max_gb, "defer": defer,
        "sandbox": sandbox, "network": network,
        **({"deferred_registered": False, "deferred_phase": "staging_pending"} if defer else {}),
        "session": req.get("session") or "", "root": norm_path(root), "top": norm_path(top),
        "worktree_identity": identity,
        "host_id": ctx.host_id or "ember",
        "workstream": os.path.basename(norm_path(top)), "submitted": iso(now), "t_submitted": now,
        "notified": False, "notify": True, "attempts": 0,
    }
    cal = _est_cal(ctx, cmd_key(command), job["workstream"], where)
    if cal:
        job["est_sched"] = max(1, round(est * cal[0]))
    receipt_file = None
    try:
        with owner_lock(top), ctx.qlock():
            if identity != worktree_identity(top):
                raise CqError("submitting worktree changed before acceptance")
            require_worktree_owner(top, req["session"])
            # Clock ties are common in concurrent sessions and deterministic tests.
            sequence_path = ctx.p("queue-sequence.json")
            if load_job(ctx, job_id):
                raise CqError("job ID collision; existing job retained")
            previous = (read_json(sequence_path) or {}).get("value", 0)
            previous = previous if isinstance(previous, int) and not isinstance(previous, bool) and previous >= 0 else 0
            job["queue_seq"] = previous + 1
            write_json(sequence_path, {"value": job["queue_seq"]})
            if defer:
                receipt = {"version": 1, "id": job_id, "host_id": job["host_id"], "worktree_identity": identity,
                           "root": norm_path(top), "session": job["session"], "source_sha": sha,
                           "defer": defer, "created": job["submitted"]}
                receipt_file = create_deferred_receipt(top, identity, receipt)
                if worktree_identity(top) != identity:
                    raise CqError("worktree identity changed before saving job")
            try:
                save_job(ctx, job)
            except Exception:
                # Atomic replacement may have succeeded before a late error. If so,
                # the matching durable job and receipt constitute an acceptance.
                if not defer or load_job(ctx, job_id) != job:
                    raise
            try:
                queue = queued_jobs(ctx)
            except Exception:
                queue = [job]  # acceptance is already durable; do not erase its receipt
    except Exception as e:
        if receipt_file:
            remove_deferred_receipt(*receipt_file)
        drop_ref(top, job_id)
        if defer:
            raise CqError(f"deferred submission rejected; receipt or job could not be saved: {e}") from e
        raise
    pos = [j["id"] for j in queue].index(job_id) + 1
    receipt_warning = None
    if defer:
        try:
            ensure_deferred_controller(ctx, force=True)
        except Exception as e:
            receipt_warning = (receipt_warning + "; " if receipt_warning else "") + f"job accepted but staging controller could not start: {e}; retain job ID"
    return {"ok": True, "job": job_id, "position": pos, "queued": len(queue),
            **({"receipt_warning": receipt_warning} if receipt_warning else {}),
            "note": f"queued at position {pos} of {len(queue)}" + (f"; snapshot {sha[:12]}" if snap else f"; at {sha[:12]}")
            + (f"; ordered as {job['est_sched']} min: the last {cal[1]} runs of this command in {job['workstream']} took a "
               f"median {cal[2]:.2f}x of their estimates" + (f" (scale clamped to {cal[0]:g}x)" if cal[0] != cal[2] else "")
               if cal else "")
            + _est_note(ctx, job["workstream"], job["est_minutes"]) + _kind_note(ctx, job["command"], job["slots"], job["est_minutes"], job["where"])}


def job_summary(job, now):
    return {k: job.get(k) for k in ("id", "status", "workstream", "label", "priority", "slots", "est_minutes", "est_sched", "where",
                                    "split", "self_heavy", "placed_where", "parts", "exit", "reason", "parent", "part",
                                    "temp_storage", "memory_max_gb", "cancel_requested", "host_id", "host", "defer",
                                    "deferred_phase", "deferred_registered", "artifact_missing")} | {
        "age_min": round((now - job.get("t_submitted", now)) / 60, 1),
        "elapsed_min": round((now - job["t_started"]) / 60, 1) if job.get("t_started") and job.get("status") == "running" else None,
    } | {k: True for k in ("sandbox", "network") if job.get(k) is True}  # shown only when asked for


def _mins(later, earlier):
    """Minutes from `earlier` to `later`, 1 decimal; None when either time is missing."""
    if not _is_num(later) or not _is_num(earlier):
        return None
    return round((later - earlier) / 60, 1)


def status_row(job, now, cap=None):
    """job_summary with what a status line needs to read true: the wait before launch, the run and the time since the end
    of an ended job, the cores and RAM a running host job uses (capacity job_cpu), and the exit of a failed job that stored
    no reason. Display only: nothing stored, and nothing in the tick's rows, changes."""
    row = job_summary(job, now)
    status = job.get("status")
    launch = job.get("t_launch") or job.get("t_started")
    if job.get("handed_over"):
        # A fleet job registered on its host to wait there for room (fleet_queue._reserve): its launch is the
        # hand-over, and its wait goes on until its command starts. While it still waits it runs to now, and to
        # its end if it ended without starting.
        launch = job.get("t_started") or (now if status == "launching" else job.get("t_finished") or launch)
    run = _mins(job.get("t_finished"), job.get("t_started")) if status in TERMINAL else None
    use = None
    if status == "running" and job.get("placed_where") == "host" and cap:
        use = ((cap.get("host") or {}).get("job_cpu") or {}).get(job["id"])
    code = job.get("exit")
    if not row["reason"] and status == "failed" and isinstance(code, int) and not isinstance(code, bool):
        row["reason"] = f"exit {code}"
    row.update(
        wait_min=row["age_min"] if status == "queued" else _mins(launch, job.get("t_submitted")) if launch else None,
        elapsed_min=_mins(now, job.get("t_started")) if status in ("launching", "running") else None,
        run_min=run,
        ago_min=_mins(now, job.get("t_finished")) if status in TERMINAL else None,
        est_ratio=round(run / job["est_minutes"], 2) if run is not None and job.get("est_minutes") else None,
        cores_now=round(use["cores"], 1) if use else None,
        rss_gb_now=round(use["rss_gb"], 1) if use else None)
    if status in TERMINAL and job.get("notify", True) and not job.get("notified"):
        top = job.get("top") or ""
        owner = read_json(os.path.join(top, ".baton", "owner.json")) if top else None
        session = owner.get("session_id") if isinstance(owner, dict) else None
        if not notice_matches_worktree(job, top, owner_session=session):
            row["notice_hold_reason"] = "worktree identity unverified or changed; completion retained"
    return row


def cmd_list(ctx, req):
    now, cap = ctx.now(), load_capacity(ctx)
    jobs = all_jobs(ctx)
    st = req.get("status")
    if st:
        jobs = [j for j in jobs if j.get("status") == st]
    if req.get("root"):
        jobs = [j for j in jobs if same_path(j.get("top", ""), _top_of(req["root"]))]
    jobs.sort(key=order_key)
    return {"ok": True, "jobs": [status_row(j, now, cap) for j in jobs[-int(req.get("limit") or 100):]]}


def _top_of(root):
    """The worktree's top directory, spelled as `git rev-parse --show-toplevel` prints it, without starting git: the
    nearest ancestor holding a `.git` entry (a directory, or the file a linked worktree or submodule has). Every
    session's 15 s tick asks this for each queue generation and host shard, and each git start is two processes on
    Windows (the Git\\cmd launcher, then git). A path that is not a directory inside a repository comes back
    unchanged, as it did when git failed."""
    top = _repository_top(root)
    return root if top is None else top


def _repository_top(root):
    """The top of the repository checkout holding the directory `root`, or None."""
    try:
        cur = os.path.realpath(root)
        if not os.path.isdir(cur):
            return None
        while True:
            if os.path.lexists(os.path.join(cur, ".git")):
                return cur.replace("\\", "/")
            parent = os.path.dirname(cur)
            if parent == cur:
                return None
            cur = parent
    except (OSError, ValueError, TypeError):
        return None


def attached_request(ctx, request):
    """The request with `root` replaced by the worktree its session attached to, when the session's own directory is
    outside any repository.

    A client reports the directory its session started in. A session started outside any checkout and linked to a
    worktree with `bt attach` therefore sent a root that is not a checkout: its submissions failed and the jobs of its
    worktree never matched it, so it received no completion notice. Baton records the link in attached.json beside the
    queue state; it is used only while that worktree's owner.json still names the same session."""
    root = request.get("root")
    session = request.get("watch_session") or request.get("session")
    if not isinstance(root, str) or not root or not isinstance(session, str) or not session:
        return request
    try:
        if _repository_top(root) is not None:
            return request
        book = read_json(os.path.join(os.path.dirname(queue_paths.config_state(ctx.state)), "attached.json"))
        entry = book.get(session) if isinstance(book, dict) else None
        target = entry.get("worktree") if isinstance(entry, dict) else None
        if not isinstance(target, str) or _repository_top(target) is None:
            return request
        owner = read_json(os.path.join(target, ".baton", "owner.json"))
        if not isinstance(owner, dict) or owner.get("session_id") != session:
            return request
        return {**request, "root": norm_path(target)}
    except (OSError, ValueError, TypeError, AttributeError):
        return request


def cmd_get(ctx, req):
    job = load_job(ctx, req.get("job", ""))
    if not job:
        raise CqError(f"no such job {req.get('job')!r}")
    delivered = {}
    if job.get("status") in TERMINAL:
        marked = mark_delivered(ctx, job["id"], req.get("seen_by"), "status")
        delivered = {"delivered": bool(marked or job.get("notified"))}   # true: no completion notice will follow
    if req.get("compact"):
        return {"ok": True, **delivered, "job": job_brief(job, ctx.now())}
    out = dict(job)
    if job.get("children"):
        out["child_jobs"] = [load_job(ctx, c) for c in job["children"]]
    return {"ok": True, "job": out}


def host_kill_script(job_id, grace=3):
    """Request a pidfd- and job-identity-checked stop at the pinned host."""
    if not isinstance(job_id, str) or not host_stop.JOB_ID.fullmatch(job_id):
        raise CqError('invalid host stop job identity')
    if type(grace) is not int or not 0 <= grace <= 10:
        raise CqError('invalid host stop grace')
    with open(os.path.join(os.path.dirname(__file__), 'host_stop.py'), 'rb') as f:
        encoded = base64.b64encode(zlib.compress(f.read())).decode('ascii')
    program = "import base64,zlib;exec(zlib.decompress(base64.b64decode(" + repr(encoded) + ")),{'__name__':'__main__'})"
    return (f'python3 -c {shlex.quote(program)} {shlex.quote(job_id)} {grace}'
            f' # CQ_PIDFD_STOP {job_id}\n')


def kill_running(ctx, job):
    """Request a safe stop; a successful request does not prove the job ended."""
    if job.get("placed_where") == "host":
        if job.get("temp_storage") == "ram":
            return ram_control(ctx, job, "stop") is not None
        rc, _, _ = job_ssh(ctx, job, host_kill_script(job["id"]), timeout=60)
        return rc == 0
    if job.get("placed_where") == "laptop":
        pid = read_pid(ctx.p("run", job["id"], "pid"))
        if pid:
            ctx.laptop.kill_tree(pid)
        return True
    return False


def read_pid(path):
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def cmd_cancel(ctx, req):
    job_id = req.get("job", "")
    require_request_identity(req, "cancellation")
    root, session = req["root"], req["session"]
    if not os.path.isdir(root):
        raise CqError("cancellation requires the current worktree root and owner session")
    top = _top_of(root)
    with owner_lock(top), ctx.qlock():
        job = load_job(ctx, job_id)
        if not job:
            raise CqError(f"no such job {job_id!r}")
        if not same_path(os.path.realpath(top), os.path.realpath(job.get("top") or "")):
            raise CqError("cancellation worktree does not match the job")
        identity = worktree_identity(top)
        if identity is None or job.get("worktree_identity") is None or identity != job["worktree_identity"]:
            raise CqError("cancellation worktree identity changed or is unavailable")
        require_worktree_owner(top, session)
        if job["status"] in TERMINAL:
            return {"ok": True, "job": job_id, "status": job["status"], "note": "already finished"}
        targets = [job]
        for c in job.get("children") or []:
            cj = load_job(ctx, c)
            if cj and cj["status"] not in TERMINAL:
                targets.append(cj)
        was_active = [j for j in targets if j["status"] in ("running", "launching")]
        uncertain_children = any(j.get("launch_uncertain") for j in was_active)
        for j in targets:
            if j.get("defer") and j["status"] in ("running", "launching"):
                j.update(cancel_requested=True, reason="cancellation pending host confirmation")
            elif j.get("temp_storage") == "ram" and j["status"] in ("running", "launching"):
                j.update(cancel_requested=True, reason="cancellation pending cgroup shutdown")
            elif j in was_active and j.get('placed_where') == 'host':
                j.update(cancel_requested=True, reason='cancellation pending host session shutdown')
            elif j in was_active and (j.get("launch_uncertain") or (j is job and uncertain_children)):
                j.update(cancel_requested=True, reason="cancellation pending original endpoint confirmation")
            else:
                j.update(status="cancelled", t_finished=ctx.now(), finished=iso(ctx.now()), reason="cancelled", notified=True)
                if j.get("defer"):
                    j.update(tail=[], tail_digest=stored_tail_digest([]), artifact_digests=[])
            save_job(ctx, j)
    for j in was_active:
        if j.get("children") is None and j.get("placed_where"):
            if j.get("defer"):
                deferred_control(ctx, j, "cancel")
            else:
                kill_running(ctx, j)
    if not job.get("parent") and not job.get("cancel_requested"):
        drop_ref(job.get("top"), job["id"])
    return {"ok": True, "job": job_id, "status": job["status"],
            "note": job.get("reason", "cancellation pending") if job.get("cancel_requested") else "cancelled"}


# --------------------------------------------------------------------------- leader

def cmd_leader(ctx, req):
    session, root = req.get("session") or "", req.get("root") or ""
    return {"ok": True, **leader_claim(ctx, session, root, bool(req.get("release")))}


def leader_claim(ctx, session, root, release=False):
    """Take leader.json when absent or its heartbeat is older than 45 s; renew while leader."""
    with FileLock(ctx.p("leader.lock"), 10.0):
        cur = read_json(ctx.p("leader.json"))
        now = ctx.now()
        if release:  # a session ending hands over at once instead of after the heartbeat goes stale
            if cur and cur.get("session") == session:
                cur["heartbeat"] = 0
                write_json(ctx.p("leader.json"), cur)
            return {"leader": False, "released": True}
        if cur is None or cur.get("session") == session or now - cur.get("heartbeat", 0) > LEADER_STALE:
            took = cur is not None and cur.get("session") != session
            since = cur.get("since", now) if cur and cur.get("session") == session else now
            write_json(ctx.p("leader.json"), {"session": session, "root": root, "heartbeat": now, "since": since})
            return {"leader": True, "took_over": took, "since": since}
        return {"leader": False, "leader_session": cur.get("session"), "age_s": round(now - cur.get("heartbeat", 0), 1)}


def leader_heartbeat(ctx, session):
    with contextlib.suppress(CqError):
        with FileLock(ctx.p("leader.lock"), 5.0):
            cur = read_json(ctx.p("leader.json"))
            if cur and cur.get("session") == session:
                cur["heartbeat"] = ctx.now()
                write_json(ctx.p("leader.json"), cur)


# --------------------------------------------------------------------------- host probe and listing

# The benchmark after the process pass is a fixed single-threaded loop: real and user+sys seconds. real - cpu is the time the loop
# was runnable but not running. On a guest with idle vCPUs that is the hypervisor running another guest on our vCPU (steal the
# host does not report); cpu_ms above its idle-host baseline is slowdown from a shared core (cache, SMT sibling, frequency).
SNAPSHOT_TEMPLATE = r'''mkdir -p "$HOME/q"
echo PROBE
N=@N@  # HOST_SLOTS of cq.py; jobs run heavy with HEAVY_SLOTS=@N@ (job.env), so the lock files are .heavy.0..@N@-1
echo "SLOTS $N"
i=0; while [ "$i" -lt "$N" ]; do flock -n "$HOME/q/.heavy.$i" true 2>/dev/null || echo "HELD $i"; i=$((i+1)); done
echo "LOAD $(cut -d' ' -f1-3 /proc/loadavg)"
echo "MEMAVAIL $(awk '/MemAvailable/{print $2}' /proc/meminfo)"
echo "MEMTOTAL $(awk '/MemTotal/{print $2}' /proc/meminfo)"
echo "CQMEM $(cat /sys/fs/cgroup/user.slice/user-1000.slice/memory.current 2>/dev/null) $(cat /sys/fs/cgroup/user.slice/user-1000.slice/memory.max 2>/dev/null)"
echo "DISKKB $(df --output=avail -k "$HOME" "$HOME/q" 2>/dev/null | tail -n +2 | tr -d ' ' | sort -n | head -1)"  # the job dirs are under ~/q, another drive on ember: the smaller free space
echo "NPROC $(nproc)"
echo "TICKHZ $(getconf CLK_TCK 2>/dev/null || echo 100)"
echo "CPUSTAT $(sed -n '1s/^cpu *//p' /proc/stat)"
echo "RUNQ $(awk '/^procs_running/{r=$2} /^procs_blocked/{b=$2} END{print r, b}' /proc/stat)"
for r in cpu memory io; do echo "PSI $r $(tr '\n' ' ' < /proc/pressure/$r 2>/dev/null)"; done
if [ "@PROF@" = 1 ]; then
  pstat() { awk -v tag="$1" 'FNR==1 { s=$0; c=s; sub(/^[0-9]+ \(/,"",c); sub(/\) [^)]*$/,"",c); gsub(/[^ -~]/, "?", c); r=s; sub(/^.*\) /,"",r); split(r,f," "); if (f[4] > 0) print tag, $1, f[4], f[12]+f[13], f[22], f[14]+f[15], c }' /proc/[0-9]*/stat 2>/dev/null; }
  pk=$(getconf PAGESIZE 2>/dev/null); echo "PAGEKB $(( ${pk:-4096} / 1024 ))"
  t0=$(date +%s%N); pstat PA; sleep 1; t1=$(date +%s%N); pstat PB
  echo "PROFMS $(( (${t1:-0} - ${t0:-0}) / 1000000 ))"
  b=$( LC_ALL=C; TIMEFORMAT='%R %U %S'; { time awk 'BEGIN{for(i=0;i<3000000;i++)s+=i*i}' >/dev/null; } 2>&1 ); echo "BENCH $b"
fi
echo JOBS
now=$(date +%s)
cd "$HOME/q" || exit 0
# One process listing for the liveness of every job dir (a job is alive while any process is in the session its pid names, which
# is what pgrep -s tests), shell builtins in the loop, and a pgrep only where the listing cannot be trusted: a pid that is not
# in it and has no exit file yet (a run.sh that started after the listing was taken, or one that died without an exit), or no
# listing at all (ps failed): a fork per file made the loop take 30 s with 800 dirs.
live=" $(ps -e -o sid= 2>/dev/null | sort -u | tr -d ' ' | tr '\n' ' ') "
nolive=0; [ "$live" = "  " ] && nolive=1
rv() { v=""; if [ -f "$d/$1" ]; then read -r v < "$d/$1" || :; fi; }
for d in cq-*; do
  [ -d "$d" ] || continue
  rv pid; pid=$v; alive=0
  if [ -n "$pid" ]; then
    case "$live" in *" $pid "*) alive=1 ;; esac
    if [ "$alive" = 0 ] && { [ "$nolive" = 1 ] || [ ! -f "$d/exit" ]; } && pgrep -s "$pid" >/dev/null 2>&1; then alive=1; fi
  fi
  rv exit; ex=$v  # after the liveness check: a run that ends in between shows alive=1 or its exit, never "dead without one"
  src=0; [ -d "$d/src" ] && src=1
  rv started; st=$v; rv launched; la=$v; rv finished; fin=$v; rv cpu; cp=$v
  echo "CQJOB $d exit=$ex started=$st launched=$la finished=$fin cpu=$cp pid=$pid alive=$alive src=$src now=$now"
done
echo END
'''


def snapshot_script(profile=False):
    """The probe script; profile=True keeps the per-process pass (1 s of wall time on the host), otherwise it is cut out."""
    # the slot count at call time: a long-lived bridge refreshes HOST_SLOTS per command
    script = SNAPSHOT_TEMPLATE.replace("@N@", str(HOST_SLOTS)).replace("@PROF@", "1" if profile else "0")
    return script if profile else re.sub(r'if \[ "0" = 1 \]; then\n.*?\nfi\n', "", script, flags=re.S)


SNAPSHOT_SCRIPT = snapshot_script(False)


def _int_at(words, i):
    return int(words[i]) if len(words) > i and words[i].isdigit() else None


def psi_avgs(words):
    """The some/full avg10 and avg60 of one PSI resource (keys some10, some60, full10, full60); a malformed one is left out."""
    vals, kind = {}, None
    for w in words:
        if w in ("some", "full"):
            kind = w
        elif kind and w.startswith(("avg10=", "avg60=")):
            with contextlib.suppress(ValueError):
                vals[kind + w[3:5]] = float(w[6:])
    return vals


def bench_ms(words):
    """{real_ms, cpu_ms} from the BENCH line's real, user and system seconds; None unless there are three finite, non-negative numbers."""
    try:
        r, u, s = (float(w) for w in words)
    except ValueError:
        return None
    if not all(math.isfinite(v) and v >= 0 for v in (r, u, s)):
        return None
    return {"real_ms": round(r * 1000), "cpu_ms": round((u + s) * 1000)}


def parse_snapshot(out):
    snap = {"held": 0, "slots": HOST_SLOTS, "load": "", "mem_avail_kb": 0, "mem_total_kb": 0, "disk_kb": 0, "nproc": 0, "jobs": {},
            "tickhz": 100, "cpu_jiffies": None, "runq": None, "psi": None, "prof": None, "bench": None}
    procs, pagekb, profms = {"PA": {}, "PB": {}}, 0, 0
    lines = [ln[:-1] if ln.endswith("\r") else ln for ln in out.split("\n")]  # only \n ends a line: a comm may hold \v, \f or \x85
    for line in lines:
        if line.startswith("HELD "):
            snap["held"] += 1
        elif line.startswith("SLOTS "):
            n = line.split()[1] if line.split()[1:] else ""
            snap["slots"] = int(n) if n.isdigit() and 1 <= int(n) <= 32 else HOST_SLOTS
        elif line.startswith("LOAD "):
            snap["load"] = line[5:].strip()
        elif line.startswith("MEMAVAIL "):
            snap["mem_avail_kb"] = int(line.split()[1] or 0) if line.split()[1:] else 0
        elif line.startswith("MEMTOTAL "):
            snap["mem_total_kb"] = int(line.split()[1] or 0) if line.split()[1:] else 0
        elif line.startswith("DISKKB "):
            snap["disk_kb"] = int(line.split()[1]) if line.split()[1:] else 0
        elif line.startswith("NPROC "):
            snap["nproc"] = int(line.split()[1]) if line.split()[1:] else 0
        elif line.startswith("TICKHZ "):
            snap["tickhz"] = _int_at(line.split(), 1) or snap["tickhz"]
        elif line.startswith("CPUSTAT "):
            words = line.split()[1:]
            snap["cpu_jiffies"] = [int(w) for w in words] if len(words) >= 8 and all(w.isdigit() for w in words) else None
        elif line.startswith("RUNQ "):
            words = line.split()[1:]
            snap["runq"] = [int(w) for w in words] if len(words) == 2 and all(w.isdigit() for w in words) else None
        elif line.startswith("PSI "):
            words = line.split()
            vals = psi_avgs(words[2:])
            if "some10" in vals and "some60" in vals:
                entry = {"some10": vals["some10"], "some60": vals["some60"]}
                if "full10" in vals and words[1] in ("memory", "io"):
                    entry["full10"] = vals["full10"]
                snap["psi"] = snap["psi"] or {}
                snap["psi"][words[1]] = entry
        elif line.startswith(("PA ", "PB ")):
            f = line.split(None, 6)  # the command name may hold spaces: it is the rest of the line
            if len(f) >= 6 and all(w.isdigit() for w in f[1:6]):
                procs[f[0]][int(f[1])] = (int(f[2]), int(f[3]), int(f[4]), int(f[5]), f[6] if len(f) == 7 else "")
        elif line.startswith("PAGEKB "):
            pagekb = _int_at(line.split(), 1) or 0
        elif line.startswith("PROFMS "):
            profms = _int_at(line.split(), 1) or 0
        elif line.startswith("BENCH "):
            snap["bench"] = bench_ms(line.split()[1:])
        elif line.startswith("CQJOB "):
            parts = line.split()
            kv = dict(p.split("=", 1) for p in parts[2:] if "=" in p)
            snap["jobs"][parts[1]] = {
                "exit": int(kv["exit"]) if kv.get("exit", "").lstrip("-").isdigit() else None,
                "started": int(kv["started"]) if kv.get("started", "").isdigit() else None,
                "launched": int(kv["launched"]) if kv.get("launched", "").isdigit() else None,
                "finished": int(kv["finished"]) if kv.get("finished", "").isdigit() else None,
                "cpu": int(kv["cpu"]) if kv.get("cpu", "").isdigit() else None,
                "pid": int(kv["pid"]) if kv.get("pid", "").isdigit() else None,
                "alive": kv.get("alive") == "1", "src": kv.get("src") == "1",
            }
        elif line.startswith("CQMEM "):
            words = line.split()
            if len(words) == 3 and all(w.isdigit() for w in words[1:]):
                snap["user_memory"] = {"current_gb": round(int(words[1]) / 2**30, 2), "max_gb": round(int(words[2]) / 2**30, 2)}
    if procs["PA"] and procs["PB"] and 0 < profms <= PROF_MS_MAX and pagekb > 0:  # a 1 s pass: an epoch-sized value is an empty date
        snap["prof"] = {"ms": profms, "pagekb": pagekb, "a": procs["PA"], "b": procs["PB"]}
    return snap


GO_TOOL_COMMS = {"compile", "link", "asm", "vet", "cgo", "cover", "buildid"}
GO_TOOL_OWN = {"compile", "link", "vet"}  # classes of their own (go-compile, go-link, go-vet): where build CPU goes


def cpu_rebooted(prev, cur):
    """True when the first 8 counters sum to less than the reference's: the host rebooted and its counters started again."""
    return sum(cur["j"][:8]) < sum(prev["j"][:8])


def cpu_usage(prev, cur):
    """The host CPU split between two probes, in percent of the jiffies of /proc/stat's cpu line (guest excluded); None when the
    interval is not 3-300 s, the host rebooted or nothing ticked. A counter that went back (iowait can) adds no ticks."""
    if not prev or not cur or len(prev["j"]) < 8 or len(cur["j"]) < 8 or cpu_rebooted(prev, cur):
        return None
    dt = cur["at"] - prev["at"]
    d = [max(0, b - a) for a, b in zip(prev["j"][:8], cur["j"][:8])]
    total = sum(d)
    if not 3 <= dt <= 300 or total <= 0:
        return None

    def pct(v):
        return round(100.0 * v / total, 1)

    return {"busy": round(100.0 * (total - d[3] - d[4]) / total, 1), "user": pct(d[0]), "nice": pct(d[1]), "sys": pct(d[2] + d[5] + d[6]),
            "iowait": pct(d[4]), "steal": pct(d[7]), "idle": pct(d[3]), "dt": round(dt, 1)}


def profile_usage(prof, tickhz, nproc=None):
    """Cores and RAM per process class and per session, from the two process passes PROFMS apart. A class counts only pids seen
    in both passes, with no fewer ticks in B and some RSS in B: a process that starts or exits inside the window is missed (the
    limit of a 1 s sample). A session's total is its members' ticks plus the ticks of the children they have already reaped
    (cticks), so a short-lived child is counted once its parent waits for it; a zombie not yet reaped still prints its ticks.
    The remaining gaps are a child reaped by a process outside the session, and the read order of the two passes: a parent
    read in A and a child reaped before B (or the reverse) lands a whole lifetime of cticks in one window. So a session's cores
    are capped at nproc, the host's vCPU count, when it is a positive int (None: no cap). A session seen only in A is dropped."""
    if not prof:
        return None
    secs = prof["ms"] / 1000.0
    classes, sessions = {}, {}
    for pid, (sid, ta, _, _, comm) in prof["a"].items():
        b = prof["b"].get(pid)
        if b is None or b[1] < ta or b[2] == 0:
            continue
        cores, rss_mb = (b[1] - ta) / tickhz / secs, b[2] * prof["pagekb"] / 1024
        name = ("go-test" if comm.endswith(".test") else f"go-{comm}" if comm in GO_TOOL_OWN
                else "go-tool" if comm in GO_TOOL_COMMS else comm)
        c = classes.setdefault(name, {"cores": 0.0, "rss_mb": 0.0, "n": 0})
        c["cores"] += cores
        c["rss_mb"] += rss_mb
        c["n"] += 1

    def session_ticks(procs):
        total = {}
        for sid, ticks, _, cticks, _ in procs.values():
            total[sid] = total.get(sid, 0) + ticks + cticks
        return total

    total_a, total_b = session_ticks(prof["a"]), session_ticks(prof["b"])
    for sid, _, rss, _, _ in prof["b"].values():
        s = sessions.setdefault(sid, {"cores": 0.0, "rss_mb": 0.0, "n": 0})
        s["rss_mb"] += rss * prof["pagekb"] / 1024
        s["n"] += 1
    cap = nproc if isinstance(nproc, int) and not isinstance(nproc, bool) and nproc > 0 else None
    for sid, s in sessions.items():
        s["cores"] = max(0, total_b[sid] - total_a.get(sid, 0)) / tickhz / secs
        if cap is not None:
            s["cores"] = min(s["cores"], cap)

    def fmt(c):
        return {"cores": round(c["cores"], 2), "rss_mb": int(c["rss_mb"]), "n": c["n"]}

    top = sorted((kv for kv in classes.items() if kv[1]["cores"] > 0), key=lambda kv: -kv[1]["cores"])[:8]
    return {"classes": [{"name": k, **fmt(c)} for k, c in top], "sessions": {str(k): fmt(c) for k, c in sessions.items()}}


def job_cpu(jobs, sessions):
    """{job id: cores and RAM} for the live jobs whose pid (the run.sh session leader) is a profiled session: the 16 busiest.
    A finished job's pid may be reused by an unrelated session, so only a job still running is matched."""
    found = []
    for jid, job in jobs.items():
        live = job.get("alive") and job.get("exit") is None
        s = sessions.get(str(job.get("pid"))) if live and job.get("pid") else None
        if s:
            found.append((s["cores"], jid, s))
    return {jid: {"cores": s["cores"], "rss_gb": round(s["rss_mb"] / 1024, 2)} for _, jid, s in sorted(found, reverse=True)[:16]}


def host_snapshot(ctx, profile=False, host=None, binding=None):
    binding = binding if binding is not None else (host_binding() if host in (None, HOST) else {})
    script = snapshot_script(profile)
    if binding.get("slots"):
        script = script.replace(f"N={HOST_SLOTS}  #", f"N={binding['slots']}  #", 1)
    rc, out, err = job_ssh(ctx, {"host": host or HOST, "host_config": binding}, script, timeout=60)
    if rc != 0 or "PROBE" not in out:
        return None, (err.strip() or f"ssh exit {rc}")[:200]
    return parse_snapshot(out), ""


def host_state(ctx):
    return read_json(ctx.p("host.json"), {}) or {}


def save_host_state(ctx, st):
    write_json(ctx.p("host.json"), st)


def take_snapshot(ctx, profile=False):
    """One ssh for the probe and the job listing; none while the host is backing off after a failure (S9). profile: the
    per-process pass too (1 s on the host), for the ticks that write a sample."""
    hs = host_state(ctx)
    if hs.get("address", HOST) == HOST and hs.get("next_probe", 0) > ctx.now():
        return None, hs.get("down_reason", "host down (backing off)"), False
    snap, err = host_snapshot(ctx, profile)
    return snap, err, True


def cmd_probe(ctx, req):
    snap, err, attempted = take_snapshot(ctx)
    return {"ok": True, **probe(ctx, snap, err, attempted)}


def _laptop_call(laptop, name, *args):
    """A laptop collector's result, or None when the object lacks it or the call raises."""
    fn = getattr(laptop, name, None)
    try:
        return fn(*args) if fn else None
    except Exception:
        return None


def _host_telemetry(hs, host, snap, now):
    """The CPU split, run queue, PSI, benchmark and per-process pass of a sampled probe, into host; the CPU references go to hs."""
    cpu_prev = hs.get("cpu_prev")  # the split is the change since this reference, kept in host.json
    if snap.get("cpu_jiffies"):
        cur = {"at": now, "j": snap["cpu_jiffies"]}
        usage = cpu_usage(cpu_prev, cur)
        # the reference stays while probes are close together, so the next split still spans a usable interval; it is replaced at
        # once after a reboot (the counters started again), so the stale test comes first: no reference, nothing to compare
        stale = not cpu_prev or len(cpu_prev["j"]) < 8 or not 0 <= now - cpu_prev["at"] <= CPU_PREV_MAX_AGE
        if usage or stale or cpu_rebooted(cpu_prev, cur):
            hs["cpu_prev"] = cur
        if usage:
            hs["cpu_last"] = dict(usage, at=now)
    last = hs.get("cpu_last")
    if last and 0 <= now - last["at"] < CPU_LAST_MAX_AGE:
        host["cpu"] = {k: v for k, v in last.items() if k != "at"}
    if snap.get("runq"):
        host["runq"] = snap["runq"]
    if snap.get("psi"):
        host["psi"] = snap["psi"]
    if snap.get("bench"):  # this probe's benchmark only: it is not kept in host.json
        host["bench"] = snap["bench"]
    if snap.get("prof"):
        use = profile_usage(snap["prof"], snap["tickhz"], snap["nproc"])
        hs["prof"] = {"at": now, "top": use["classes"], "job_cpu": job_cpu(snap["jobs"], use["sessions"])}
    prof = hs.get("prof")  # a profiled probe's classes and job cores, shown on the probes that follow it for a few minutes
    if prof and 0 <= now - prof["at"] < PROF_MAX_AGE:
        host.update(top=prof["top"], job_cpu=prof["job_cpu"], prof_age_s=round(now - prof["at"]))


def host_pause(hs):
    """Why no job is dispatched to the host: the host.conf.json pause ("host.conf.json"), else the disk pause ("disk"), else None."""
    return "host.conf.json" if HOST_PAUSED else ("disk" if hs.get("disk_paused") else None)


def probe(ctx, snap=None, err="", attempted=True):
    """Host and laptop capacity, written to capacity.json for the guard and the other sessions' status lines."""
    now = ctx.now()
    hs = host_state(ctx)
    if hs.get("address", HOST) != HOST:  # host.json is one machine's history: after a move to another address it starts again
        hs = {"address": HOST}
    hs.setdefault("address", HOST)
    pause = host_pause(hs)
    host = {"up": snap is not None, "total": HOST_SLOTS, "held": 0, "pending": 0, "free": 0, "usable": False,
            "paused": pause is not None, "pause": pause, "address": HOST}
    if snap is not None:
        hs.pop("down_since", None)
        hs.pop("next_probe", None)
        hs.pop("down_reason", None)
        disk_ok = snap["disk_kb"] >= HOST_DISK_MIN_KB
        if hs.get("disk_paused") and disk_ok:
            hs["disk_paused"] = False
            hs["disk_resumed"] = iso(now)
        pause = host_pause(hs)  # the disk pause may have just lifted
        host.update(total=snap.get("slots", HOST_SLOTS), held=snap["held"], load=snap["load"], mem_avail_gb=round(snap["mem_avail_kb"] / 2**20, 1),
                    disk_free_gb=round(snap["disk_kb"] / 2**20, 1), nproc=snap["nproc"],
                    mem_total_gb=round(snap["mem_total_kb"] / 2**20, 1), paused=pause is not None, pause=pause,
                    name=HOST_NAME, slot_vcpu=SLOT_VCPU, slot_ram_gb=SLOT_RAM_GB, ram_floor_gb=HOST_RAM_FLOOR_GB,
                    load_cap=LOAD_CAP or snap["nproc"])  # the planner text follows these (hooks/plan.ts systemText)
        try:
            _host_telemetry(hs, host, snap, now)
        except Exception:  # a malformed value in host.json heals itself: the probe still writes capacity.json and host.json
            for k in ("cpu_prev", "cpu_last", "prof"):
                hs.pop(k, None)
        pending = 0
        for j in all_jobs(ctx):
            if (j.get("placed_where") == "host" and j.get("host", HOST) == HOST
                    and j.get("status") in ("launching", "running") and not j.get("children")
                    and not (j.get("defer") and j.get("status") == "launching")):
                if not (snap["jobs"].get(j["id"]) or {}).get("started"):
                    pending += j.get("slots_eff", j.get("slots", 0))
        host["pending"] = pending
        if RAM_JOBS:
            # A detached RAM descendant can outlive both the session leader and the heavy lock.
            active_slots = sum(j.get("slots_eff", j.get("slots", 0)) for j in all_jobs(ctx)
                               if j.get("placed_where") == "host" and j.get("host", HOST) == HOST
                               and j.get("status") in ("launching", "running") and not j.get("children")
                               and not (j.get("defer") and j.get("status") == "launching"))
            pending = max(pending, active_slots - snap["held"])
            host["pending"] = pending
        host["committed"] = pending  # slots launched but not yet in the load/RAM sample; the caps count them
        host["free"] = max(0, host["total"] - snap["held"] - pending)
        host["usable"] = not host["paused"]
        if RAM_JOBS:
            host["memory_budget_gb"] = RAM_BUDGET_GB
            host["memory_reserved_gb"] = reserved_memory(ctx)
            host["user_memory"] = snap.get("user_memory")
        # consecutive samples with no heavy job on the host (no lock held, none launching): the caps-override condition
        hs["idle_samples"] = hs.get("idle_samples", 0) + 1 if snap["held"] == 0 and pending == 0 else 0
        host["idle_samples"] = hs["idle_samples"]
        # the idle RAM baseline: the largest MemAvailable per hour over the last MEM_HISTORY_HOURS hours
        hist, bucket = hs.get("mem_hist") or {}, int(now // 3600)
        hist[str(bucket)] = max(hist.get(str(bucket), 0), host["mem_avail_gb"])
        hist = {k: v for k, v in hist.items() if int(k) > bucket - MEM_HISTORY_HOURS}
        hs["mem_hist"] = hist
        host["mem_idle_gb"] = max(hist.values())
    else:
        hs["idle_samples"] = 0
        was_down = "down_since" in hs
        hs.setdefault("down_since", iso(now))
        if attempted:
            # the first failure after a good probe is retried on the next tick: one dropped connection is not an outage.
            # A failure while the host is already down backs off 60 s
            hs["next_probe"] = now + 60 if was_down else now
            hs["down_reason"] = err
        # the config fields follow the down host too, so the planner text never pairs the old name with the new slot count
        host.update(down_since=hs["down_since"], down_reason=hs.get("down_reason", err), name=HOST_NAME, slot_vcpu=SLOT_VCPU,
                    slot_ram_gb=SLOT_RAM_GB, ram_floor_gb=HOST_RAM_FLOOR_GB, load_cap=LOAD_CAP)  # load_cap None: plan.ts uses nproc
    save_host_state(ctx, hs)
    if laptop_executes():
        ram = ctx.laptop.free_ram_gb()
        heavy_jobs = [j for j in all_jobs(ctx) if j.get("placed_where") == "laptop" and j.get("status") in ("launching", "running")
                      and j.get("slots_eff", j.get("slots", 0)) > 0]
        laptop = {"heavy_busy": bool(ctx.laptop.heavy_lock_held() or heavy_jobs), "free_ram_gb": ram,
                  "ram_ok": ram >= LAPTOP_RAM_MIN_GB}
        mem = _laptop_call(ctx.laptop, "mem_stats")
        if isinstance(mem, dict):
            laptop.update({k: mem.get(k) for k in ("commit_gb", "commit_limit_gb", "np_gb", "paged_gb", "cache_gb")})
    else:  # this machine only controls: no RAM figure, no lock, no profile of a machine that runs nothing
        laptop = {"executor": False, "heavy_busy": False, "free_ram_gb": None, "ram_ok": False, "reason": LAPTOP_NOT_EXECUTOR}
    light = light_running(ctx)
    host["light_running"], laptop["light_running"] = light["host"], light["laptop"]
    cap = {"at": now, "host": host, "laptop": laptop}
    write_json(ctx.p("capacity.json"), cap)
    return {"capacity": cap}


def light_running(ctx, after=None):
    """Light (slots 0) jobs and parts launching or running per machine; `after`: only those launched after that time."""
    n = {"host": 0, "laptop": 0}
    for j in all_jobs(ctx):
        w = j.get("placed_where")
        if (w in n and j.get("status") in ("launching", "running") and not j.get("children")
                and j.get("slots_eff", j.get("slots", 0)) == 0 and (after is None or j.get("t_launch", 0) > after)):
            n[w] += 1
    return n


def load_capacity(ctx):
    cap = stamp_capacity(read_json(ctx.p("capacity.json")), ctx.now())
    if cap and cap.get("host"):
        address = cap["host"].get("address") or host_state(ctx).get("address", HOST)
        if address != HOST or HOST_PAUSED:
            cap["host"] = dict(cap["host"], usable=False, paused=True,
                               pause="host.conf.json" if HOST_PAUSED else "host changed; waiting for probe")
    return cap


# --------------------------------------------------------------------------- the guard (pure) and the fallback

def stamp_capacity(cap, now):
    """A copy of the capacity sample with host.stale set: a sample with no time, or older than SAMPLE_MAX_AGE, is no headroom."""
    if not cap or not isinstance(cap.get("host"), dict):
        return cap
    at = cap.get("at")
    stale = not isinstance(at, (int, float)) or isinstance(at, bool) or now - at > SAMPLE_MAX_AGE
    cap = dict(cap, host=dict(cap["host"], stale=stale))
    cap["host"]["sample_age_s"] = None if not isinstance(at, (int, float)) else round(now - at)
    return cap


def _host_sample(h):
    """(load1, load cap, mem_avail_gb) of a fresh, complete host sample, else None."""
    if h.get("stale"):
        return None
    try:
        load1 = float(str(h.get("load") or "").split()[0])
        nproc = float(h.get("nproc") or 0)
        mem = float(h["mem_avail_gb"])
    except (IndexError, KeyError, TypeError, ValueError):
        return None
    cap_load = LOAD_CAP or nproc
    return (load1, cap_load, mem) if cap_load > 0 else None


def host_cap_room(h):
    """Heavy slots the measured caps allow now: min(by load, by RAM) less the slots launched since the sample (h.committed).
    0 when the sample is stale or lacks load/RAM."""
    smp = _host_sample(h)
    if smp is None:
        return 0
    load1, cap_load, mem = smp
    committed = int(h.get("committed", 0) or 0)
    by_load = math.floor((cap_load - load1) / SLOT_VCPU + 1e-9)
    by_mem = math.floor((mem - HOST_RAM_FLOOR_GB) / SLOT_RAM_GB + 1e-9)
    return max(0, min(by_load, by_mem) - committed)


def host_light_ok(h):
    """A light (slots 0) job starts only below LOAD_CAP + LIGHT_LOAD_MARGIN on a fresh sample."""
    smp = _host_sample(h)
    return smp is not None and smp[0] < smp[1] + LIGHT_LOAD_MARGIN


def host_free(cap):
    """Slots a heavy job may take on the host now: the free slots, within the room the measured caps leave."""
    h = cap.get("host") or {}
    if not (h.get("up") and h.get("usable")):
        return 0
    return min(h.get("free", 0), host_cap_room(h))


def cap_clip_reason(h, slots, charged=0):
    """Why the measured caps leave no room for `slots` more heavy slots (`charged`: slots this plan already starts), or None."""
    if slots < 1:
        return None if host_light_ok(h) else _light_reason(h)
    smp = _host_sample(h)
    if smp is None:
        return _stale_reason(h)
    load1, cap_load, mem = smp
    n = slots + charged + int(h.get("committed", 0) or 0)
    if load1 + SLOT_VCPU * n > cap_load + 1e-9:
        return f"load cap: load {load1:g} + {SLOT_VCPU:g}*{n} slots > {cap_load:g}"
    if mem - SLOT_RAM_GB * n < HOST_RAM_FLOOR_GB - 1e-9:
        return f"RAM floor: {mem:g} GB available - {SLOT_RAM_GB:g}*{n} slots < {HOST_RAM_FLOOR_GB:g} GB"
    return None


def _stale_reason(h):
    if h.get("stale"):
        return f"capacity sample stale ({h.get('sample_age_s')} s old, max {SAMPLE_MAX_AGE:g}): no headroom"
    return "no load/RAM in the capacity sample: no headroom"


def _light_reason(h):
    smp = _host_sample(h)
    if smp is None:
        return _stale_reason(h)
    return f"light cap: load {smp[0]:g} >= {smp[1]:g} + {LIGHT_LOAD_MARGIN:g}"


def host_max_slots(h):
    """The most heavy slots one job can ever hold on the host: the caps on an idle host, whose RAM is the largest
    MemAvailable seen in the last MEM_HISTORY_HOURS hours (h.mem_idle_gb), or the current one when there is no history.
    None when unknown."""
    try:
        nproc = float(h.get("nproc") or 0)
    except (TypeError, ValueError):
        return None
    cap_load = LOAD_CAP or nproc
    if cap_load <= 0:
        return None
    n = math.floor(cap_load / SLOT_VCPU + 1e-9)
    try:
        idle = float(h.get("mem_idle_gb") or h.get("mem_avail_gb") or 0)
    except (TypeError, ValueError):
        idle = 0
    if idle > 0:
        n = min(n, math.floor((idle - HOST_RAM_FLOOR_GB) / SLOT_RAM_GB + 1e-9))
    return max(0, n)


def caps_override(job, cap):
    """True when `job`, the head, is blocked only by the measured caps and may start anyway: it fits the free slots, the
    sample is fresh, and no heavy job has been on the host for IDLE_OVERRIDE_SAMPLES consecutive samples (nothing launched
    since either), so the RAM floor's reason - no OOM beside other jobs - does not apply."""
    h = cap.get("host") or {}
    if not _CONF.get("caps_override", True):
        return False
    if not (h.get("up") and h.get("usable")) or h.get("stale") or job["where"] not in ("host", "any") or job["slots"] < 1:
        return False
    return (h.get("idle_samples", 0) >= IDLE_OVERRIDE_SAMPLES and h.get("held", 0) == 0 and not h.get("committed")
            and job["slots"] <= h.get("free", 0) and host_cap_room(h) < job["slots"])


def _laptop_off(cap):
    """True when the capacity sample says the laptop is not an executor on the machine that took it (a controller-only Mac)."""
    return (cap.get("laptop") or {}).get("executor") is False


def _light_room(cap):
    """How many more light (slots 0) jobs or parts each machine takes: LIGHT_CAP less those already launching or running.
    A laptop that is not an executor takes none."""
    room = {m: max(0, LIGHT_CAP - int((cap.get(m) or {}).get("light_running", 0) or 0)) for m in ("host", "laptop")}
    if _laptop_off(cap):
        room["laptop"] = 0
    return room


def _fits(job, cap, free_host, laptop_free, light=None):
    """Machines on which this job could start now, host first."""
    light = _light_room(cap) if light is None else light
    out = []
    h = cap.get("host") or {}
    if (job["where"] in ("host", "any") and h.get("up") and h.get("usable") and free_host >= job["slots"]
            and (job["slots"] > 0 or (light["host"] > 0 and host_light_ok(h)))):
        out.append("host")
    if job["where"] in ("laptop", "any") and job["slots"] <= 1 and not _laptop_off(cap):
        if (job["slots"] == 0 and light["laptop"] > 0) or (job["slots"] == 1 and laptop_free):
            out.append("laptop")
    return out


def _laptop_heavy_free(cap):
    lp = cap.get("laptop") or {}
    return (lp.get("executor") is not False and not lp.get("heavy_busy")
            and (lp.get("free_ram_gb") or 0) >= LAPTOP_RAM_MIN_GB)


def head_window(head, cap, running, now):
    """Minutes the blocked host head job J could still wait for its slots, shrunk by BACKFILL_SHADOW_FACTOR: the
    remaining estimate of the running job whose end first brings the free slots to J's slots (an overdue job has 0).
    None when J is not blocked by slots (the caps block it, or it fits), when `running` is not known, or when the running
    jobs cannot free enough slots."""
    if running is None:
        return None
    h = cap.get("host") or {}
    free = int(h.get("free", 0) or 0)
    if free >= head["slots"]:
        return None
    ends = []
    for j in running:
        slots = j.get("slots_eff", j.get("slots", 0))
        if j.get("placed_where") != "host" or j.get("children") or j.get("status") not in ("launching", "running") or slots < 1:
            continue
        t0 = j.get("t_started") if j.get("status") == "running" and j.get("t_started") else (j.get("t_launch") or now)
        est = min(v for v in (j.get("est_minutes", 0), sched_est(j) or 0) if _number(v))  # the smaller: a shorter window only backfills less
        k, _, n = str(j.get("part") or "").partition("/")
        if j.get("parent") and k.isdigit() and n.isdigit() and int(n) >= 2:
            est = est / int(n)  # a part inherits the whole job's estimate; taking 1/n of it only shrinks the window
        ends.append((max(0, est - (now - t0) / 60), slots))
    acc = free
    for remaining, slots in sorted(ends, key=lambda e: e[0]):
        acc += slots
        if acc >= head["slots"]:
            return remaining * BACKFILL_SHADOW_FACTOR
    return None


def can_backfill(job, r):
    """True when the host reservation r (of a blocked head job) lets `job` start beside it: a short heavy job that ends at
    least BACKFILL_MARGIN_MIN before the head job could start. Split and self-heavy jobs never backfill."""
    w = r.get("window_min")
    return (w is not None and job["slots"] >= 1 and not job.get("split") and not job.get("self_heavy")
            and job["est_minutes"] <= BACKFILL_MAX_MIN and job["est_minutes"] + BACKFILL_MARGIN_MIN <= w)


def reservations(queued, cap, running=None, now=None):
    """The reservation rule, per machine. J = the first queued job in start order (start_key: highest priority, then jobs
    queued longer than AGE_FIRST_MIN oldest first, then the shortest estimate) that could run its heavy (slots >= 1)
    work on the machine. Returns {"host": R|None, "laptop": R|None}, R = {"job": J, "blocked": bool,
    "place": where J would start now | None}. J is blocked when it fits no machine now: while it is, no other heavy job
    starts on the machine (the slots that free up accumulate for J), whatever its priority; light jobs are exempt and
    never heads; a job already running is never touched. A machine that is down, paused or whose pool J could never fit
    has no reservation. A blocked host J also gets "window_min" (head_window, from `running` at `now`): the bounded
    backfill window that can_backfill tests."""
    now = time.time() if now is None else now
    h = cap.get("host") or {}
    free, lap, light = host_free(cap), _laptop_heavy_free(cap), _light_room(cap)
    total = h.get("total")
    mx = host_max_slots(h)  # a job the caps could never admit, even on an idle host, holds nothing
    out = {"host": None, "laptop": None}
    for m in ("host", "laptop"):
        if m == "host":
            if not (h.get("up") and h.get("usable")):
                continue
            cands = [j for j in queued if j["slots"] >= 1 and j["where"] in ("host", "any") and (not total or j["slots"] <= total)
                     and (mx is None or j["slots"] <= mx)]
        elif _laptop_off(cap):
            continue  # no machine to hold: nothing is reserved on a laptop that runs nothing
        else:
            cands = [j for j in queued if j["slots"] == 1 and j["where"] in ("laptop", "any")]
        if not cands:
            continue
        head = min(cands, key=lambda j: start_key(j, now))
        places = _fits(head, cap, free, lap, light)
        if not places and m == "host" and caps_override(head, cap):
            out[m] = {"job": head, "blocked": False, "place": "host", "override": True}  # idle host: the caps give way
            continue
        out[m] = {"job": head, "blocked": not places, "place": places[0] if places else None}
        if m == "host" and not places:
            out[m]["window_min"] = head_window(head, cap, running, now)
    return out


def _clip(job, where, res):
    """Why `job` may not start on `where` under the reservation rule, or None."""
    r = res.get(where)
    if job["slots"] < 1 or not r or not r["blocked"] or r["job"]["id"] == job["id"]:
        return None
    if where == "host" and can_backfill(job, r):
        return None
    j = r["job"]
    return f"reserved for {j['id']} ({j['priority']}, {j['slots']} slots) waiting on the {where}"


def _open_places(job, cap, free, lap, light, res):
    """_fits, less the machines the reservation rule closes to this job."""
    return [m for m in _fits(job, cap, free, lap, light) if _clip(job, m, res) is None]


def split_parts(job, where, free, light_room):
    """The part count of a split job, set here and not by Haiku: every free slot (max_parts at most) on the host, up to
    SPLIT_LIGHT_PARTS for a light job (within the machine's light room), one on the laptop. 0 = it does not fit."""
    max_parts, slots = job["split"]["max_parts"], job["slots"]
    if slots == 0:
        return min(max_parts, SPLIT_LIGHT_PARTS, light_room)
    if where == "host":
        return min(max_parts, free // slots)
    return 1


def fallback_entries(queued, cap, running=None, now=None):
    """Start order (start_key), first fit into the free slots; parts = min(max_parts, free / slots). The reservation
    rule holds: while the head job J of a machine does not fit it, only J and light jobs start there, and a short job
    that backfills (can_backfill) beside a host J."""
    now = time.time() if now is None else now
    free = host_free(cap)
    lap = _laptop_heavy_free(cap)
    light = _light_room(cap)
    res = reservations(queued, cap, running, now)
    entries = []
    ov = (res.get("host") or {}).get("override") and res["host"]["job"]["id"]
    for job in sorted(queued, key=lambda j: start_key(j, now)):
        places = _open_places(job, cap, free, lap, light, res)
        if not places and job["id"] == ov:
            places = ["host"]
        if not places:
            continue
        where = places[0]
        e = {"job": job["id"], "where": where, "slots": job["slots"]}
        if job.get("split"):
            e["parts"] = max(1, split_parts(job, where, free, light[where]))
        n = e.get("parts", 1)
        if job["slots"] == 0:
            light[where] -= n
        elif where == "host":
            free = max(0, free - job["slots"] * n)
        else:
            lap = False
        entries.append(e)
    return entries


def guard(entries, queued, cap, now, mode="haiku", running=None):
    """Validate a plan against the hard limits. Returns {accepted, refused}. Haiku decides which jobs start and where;
    this drops, trims or reorders for starvation (a job queued over 60 min whose slots fit goes first) and sets the
    parts of split jobs itself, in a second pass after every other accepted entry has been charged. `running` (the
    host's launching and running jobs) gives the backfill windows; None: no backfill."""
    jobs = {j["id"]: j for j in queued}
    cap = stamp_capacity(cap, now)
    hcap = (cap or {}).get("host") or {}
    charged = 0  # host heavy slots this plan has accepted so far
    free = host_free(cap)
    lap_free = _laptop_heavy_free(cap)
    light = _light_room(cap)
    host_up = bool((cap.get("host") or {}).get("up") and (cap.get("host") or {}).get("usable"))
    accepted, refused, seen = [], [], set()
    res = reservations(queued, cap, running, now)

    if mode == "fallback":
        entries = fallback_entries(queued, cap, running, now)
    entries = [e for e in (entries or []) if isinstance(e, dict)]

    # reservation: a head job J that fits now is owed its slots (no other entry may take them), and when the plan
    # leaves it out yet starts something else it is added after the plan; a head job that does not fit holds its
    # machine: every other heavy entry there is clipped
    override = {r["job"]["id"] for r in res.values() if r and r.get("override")}  # a head the idle host lets past the caps
    owed, heads = {"host": 0, "laptop": 0}, set()
    named = {e.get("job") for e in entries}
    unnamed = []
    for r in res.values():
        if r and not r["blocked"] and r["job"]["id"] not in heads:
            j = r["job"]
            heads.add(j["id"])
            owed[r["place"]] += j["slots"]
            if j["id"] not in named:
                unnamed.append({"job": j["id"], "where": r["place"], "slots": j["slots"], "reserved": True})

    # starvation: the old go first (oldest first), then Haiku's order
    starved = []
    for job in sorted(queued, key=lambda j: j.get("t_submitted", 0)):
        if now - job.get("t_submitted", now) > STARVE_SECONDS and _open_places(job, cap, free, lap_free, light, res):
            starved.append(job)
    if starved:
        named = {e.get("job"): e for e in entries}
        head = []
        for job in starved:
            e = named.pop(job["id"], None)
            if e is None:
                where = _open_places(job, cap, free, lap_free, light, res)[0]
                e = {"job": job["id"], "where": where, "slots": job["slots"], "starved": True}
            head.append(e)
        entries = head + [e for e in entries if e.get("job") not in {h["job"] for h in head}]

    pending = []  # (accepted entry, job, Haiku's parts): split jobs whose parts the second pass sets
    entries.append(None)  # the end of the plan: where the unnamed head jobs are added
    for e in entries:
        if e is None:
            if accepted:
                entries.extend(unnamed)
            continue
        jid = e.get("job")
        job = jobs.get(jid)
        if not isinstance(jid, str) or job is None:
            refused.append({"job": jid, "reason": "unknown or not queued"})
            continue
        if jid in seen:
            refused.append({"job": jid, "reason": "duplicate"})
            continue
        seen.add(jid)
        where = e.get("where")
        if jid in heads and not (job.get("split") and job["slots"] > 0 and where == "host"):
            owed = {"host": 0, "laptop": 0}  # J is placed or refused here: nothing is owed after this entry
        if where not in ("host", "laptop"):
            refused.append({"job": jid, "reason": f"where {where!r} is not host or laptop"})
            continue
        if where == "laptop" and _laptop_off(cap):
            refused.append({"job": jid, "reason": f"the laptop is {LAPTOP_NOT_EXECUTOR}"})
            continue
        if job["where"] != "any" and job["where"] != where:
            refused.append({"job": jid, "reason": f"where {where} not allowed: the job says {job['where']}"})
            continue
        try:
            slots = e.get("slots")
            if slots is None:
                slots = job["slots"]  # a Haiku entry without slots means the job's own
            if isinstance(slots, bool) or int(slots) != slots:
                raise ValueError
            slots = int(slots)
            parts = e.get("parts")
            if parts is not None and (isinstance(parts, bool) or int(parts) != parts):
                raise ValueError
            parts = None if parts is None else int(parts)
        except (TypeError, ValueError):
            refused.append({"job": jid, "reason": "slots/parts are not integers"})
            continue
        if slots < job["slots"]:
            refused.append({"job": jid, "reason": f"slots {slots} below the job's {job['slots']}"})
            continue
        slots = job["slots"]  # clamped to the job's own: a light job stays light, a self-heavy job takes its own slots
        if where == "host" and not host_up:
            refused.append({"job": jid, "reason": "host is down or paused"})
            continue
        why = _clip(job, where, res)
        if why:
            refused.append({"job": jid, "reason": why})
            continue
        if where == "host" and slots == 0 and not host_light_ok(hcap):
            refused.append({"job": jid, "reason": _light_reason(hcap)})
            continue
        acc = {"job": jid, "where": where, "slots": slots}
        if e.get("starved"):
            acc["starved"] = True
        if e.get("reserved"):
            acc["reserved"] = True
        rh = res.get("host") or {}
        if where == "host" and rh.get("blocked") and rh["job"]["id"] != jid and can_backfill(job, rh):
            acc["backfill"] = True  # admitted beside the blocked head job by the window, not by the reservation rule
        if slots > 0 and jid not in heads:
            if where == "laptop" and owed["laptop"]:
                refused.append({"job": jid, "reason": "the laptop is reserved for the head job that fits now"})
                continue
            if where == "host" and owed["host"] and free - owed["host"] < slots <= free:
                refused.append({"job": jid, "reason": "its slots are reserved for the head job that fits now"})
                continue
        if job.get("split") and (where == "host" or slots == 0):
            acc["parts"] = 0
            accepted.append(acc)
            pending.append((acc, job, parts))
            continue
        if job.get("split"):
            acc["parts"] = 1  # a heavy split job on the laptop runs as one part
        if where == "host":
            if slots > free:
                if jid in override and slots <= hcap.get("free", 0) - charged:
                    acc["caps_override"] = CAPS_OVERRIDE_NOTE
                elif slots <= hcap.get("free", 0) - charged and cap_clip_reason(hcap, slots, charged):
                    refused.append({"job": jid, "reason": cap_clip_reason(hcap, slots, charged)})
                    continue
                else:
                    refused.append({"job": jid, "reason": f"needs {slots} slots, {free} free"})
                    continue
            free = max(0, free - slots)
            charged += slots
        else:
            slots = min(slots, 1)
            acc["slots"] = slots
            if slots > 0:
                if not lap_free:
                    refused.append({"job": jid, "reason": "laptop heavy lock busy or free RAM below 6 GB"})
                    continue
                lap_free = False
        if slots == 0:
            if light[where] < 1:
                refused.append({"job": jid, "reason": f"{LIGHT_CAP} light jobs already on the {where}"})
                continue
            light[where] -= 1
        accepted.append(acc)

    if not any(a["job"] in heads for a, _, _ in pending):
        owed = {"host": 0, "laptop": 0}  # a split head job that was refused leaves nothing owed
    dropped = set()
    for acc, job, hint in pending:  # the split jobs, in plan order, take what the others left
        where, slots, jid = acc["where"], acc["slots"], acc["job"]
        if jid in heads:
            owed = {"host": 0, "laptop": 0}  # the split head job takes its parts out of the whole of `free`
        if slots == 0:
            n = split_parts(job, where, free, light[where])
            if n < 1:
                refused.append({"job": jid, "reason": f"{LIGHT_CAP} light jobs already on the {where}"})
                dropped.add(jid)
                continue
            light[where] -= n
        else:
            avail = free - (owed["host"] if where == "host" else 0)  # less a split head job's first part, still owed
            n = split_parts(job, where, avail, 0)
            if hint is not None and 1 <= hint <= job["split"]["max_parts"] and hint * slots <= avail:
                n = max(n, hint)
            if n < 1 and jid in override and slots <= hcap.get("free", 0) - charged:
                n, acc["caps_override"] = 1, CAPS_OVERRIDE_NOTE  # the idle host's head job runs one part past the caps
                charged += slots
            if n < 1:
                refused.append({"job": jid, "reason": f"needs {slots} slots, {avail} free"})
                dropped.add(jid)
                continue
            free -= n * slots
        acc["parts"] = n
    accepted = [a for a in accepted if a["job"] not in dropped]
    if _number(hcap.get("memory_budget_gb")):
        remaining = max(0, hcap["memory_budget_gb"] - hcap.get("memory_reserved_gb", 0))
        bounded = []
        for entry in accepted:
            if entry["where"] != "host":
                bounded.append(entry)
                continue
            job = jobs[entry["job"]]
            cost = memory_reservation(job)
            parts = entry.get("parts", 1)
            if cost * parts > remaining:
                if job.get("split") and job.get("temp_storage") != "ram":
                    parts = min(parts, int(remaining // cost))
                else:
                    parts = 0
                if parts < 1:
                    refused.append({"job": entry["job"], "reason": "mixed disk/RAM memory reservation budget exhausted"})
                    continue
                entry["parts"] = parts
            remaining -= cost * parts
            bounded.append(entry)
        accepted = bounded
    if mode == "fallback":  # the fallback names only what fits: log the jobs the measured caps (not the slots) kept back
        done = {a["job"] for a in accepted} | {r["job"] for r in refused}
        raw = hcap.get("free", 0) if hcap.get("up") and hcap.get("usable") else 0
        for job in sorted(queued, key=lambda j: start_key(j, now)):
            if len(refused) >= 5 + len(done & {r["job"] for r in refused}):
                break
            if job["id"] in done or job["where"] not in ("host", "any"):
                continue
            n = job["slots"]
            if hcap.get("up") and hcap.get("usable") and (n == 0 or n <= raw) and cap_clip_reason(hcap, n):
                refused.append({"job": job["id"], "reason": cap_clip_reason(hcap, n)})
    return {"accepted": accepted, "refused": refused}


def cmd_guard(ctx, req):
    cap = req.get("capacity") or load_capacity(ctx) or {}
    queued = req.get("queued") if req.get("queued") is not None else [j for j in queued_jobs(ctx) if not j.get("defer")]
    running = req.get("running") if req.get("running") is not None else running_jobs(ctx)
    res = guard(req.get("entries"), queued, cap, req.get("now", ctx.now()), req.get("mode", "haiku"), running)
    return {"ok": True, **res}


def cmd_apply(ctx, req):
    """The leader's decision: Haiku's plan (or none) -> guard -> fallback when needed -> dispatch -> log. planner "code":
    no plan at all, the guard's fallback plans the round (mode "code")."""
    cap = load_capacity(ctx) or {}
    now = ctx.now()
    cap = adjust_for_launches(ctx, cap)
    queued = [j for j in queued_jobs(ctx) if not j.get("defer")]
    running = running_jobs(ctx)
    plan = req.get("plan")
    entries = plan.get("start") if isinstance(plan, dict) and isinstance(plan.get("start"), list) else None
    mode, why = "haiku", (plan or {}).get("why", "") if isinstance(plan, dict) else ""
    note = ""
    if req.get("planner") == "code":
        mode, why = "code", ""
        res = guard(None, queued, cap, now, "fallback", running)
    elif entries is None:
        mode, note = "fallback", req.get("reason") or "no usable plan"
        res = guard(None, queued, cap, now, "fallback", running)
    else:
        res = guard(entries, queued, cap, now, "haiku", running)
        if entries and not res["accepted"]:
            mode, note = "fallback", "every entry refused"
            refused_by_plan = res["refused"]
            res = guard(None, queued, cap, now, "fallback", running)
            res["refused"] = refused_by_plan + res["refused"]
        elif not entries:
            rsv = reservations(queued, cap, running, now)
            waiting = [j for j in queued if _open_places(j, cap, host_free(cap), _laptop_heavy_free(cap), _light_room(cap), rsv)]
            if waiting and now - min(j.get("t_submitted", now) for j in waiting) > 120:
                mode, note = "fallback", "idle override: Haiku started nothing while a fitting job waited over 2 min"
                res = guard(None, queued, cap, now, "fallback", running)
    accepted = res["accepted"]
    started = []
    for a in accepted:
        if a.get("caps_override"):
            h = cap.get("host") or {}
            decision(ctx, {"kind": "caps_override", "note": a["caps_override"], "job": a["job"], "slots": a["slots"],
                           "load": h.get("load"), "mem_avail_gb": h.get("mem_avail_gb"), "idle_samples": h.get("idle_samples")})
        r = dispatch(ctx, a)
        started.append({**a, "dispatched": r.get("ok", False), **({"reason": r.get("error")} if not r.get("ok") else {})})
    # a model call that starts nothing still logs its usage; a code round that starts nothing does not (it runs every tick, and
    # its refusals are the guard's own, repeated each tick until the job fits)
    if accepted or (res["refused"] and mode != "code") or mode == "fallback" or req.get("usage"):
        decision(ctx, {"kind": "plan", "mode": mode, "why": why[:200], "note": note, "accepted": started,
                       "refused": res["refused"], "queued": len(queued), "free_host": host_free(cap),
                       **({"usage": req["usage"]} if req.get("usage") else {})})
    if ctx.sync is False and started:
        spawn_workers(ctx, [s for s in started if s["dispatched"]])
    elif started:
        run_workers(ctx, [s for s in started if s["dispatched"]])
    return {"ok": True, "mode": mode, "accepted": started, "refused": res["refused"]}


def adjust_for_launches(ctx, cap):
    """Slots of host jobs set launching after the probe are not in the probe's held count (nor in its load and RAM, which
    the caps count as committed). Also stamps the sample's age for the caps."""
    if not cap:
        return cap
    cap = stamp_capacity(cap, ctx.now())
    if RAM_JOBS and cap.get("host"):
        cap["host"]["memory_budget_gb"] = RAM_BUDGET_GB
        cap["host"]["memory_reserved_gb"] = reserved_memory(ctx)
    at = cap.get("at", 0)
    extra = 0
    for j in all_jobs(ctx):
        if j.get("placed_where") == "host" and j.get("status") == "launching" and j.get("t_launch", 0) > at and not j.get("children") and not j.get("defer"):
            extra += j.get("slots_eff", 0)
    light = light_running(ctx, after=at)  # light jobs dispatched after the probe count against the per-machine cap too
    if extra or any(light.values()):
        cap = json.loads(json.dumps(cap))
        if extra:
            cap["host"]["free"] = max(0, cap["host"]["free"] - extra)
            cap["host"]["committed"] = int(cap["host"].get("committed", 0) or 0) + extra
        for m, n in light.items():
            if n and isinstance(cap.get(m), dict):
                cap[m]["light_running"] = int(cap[m].get("light_running", 0) or 0) + n
    return cap


# --------------------------------------------------------------------------- dispatch (mark first, then a worker)

def substitute_part(command, k, n):
    return command.replace("{part}", f"{k}/{n}")


def dispatch(ctx, a):
    """Mark the job launching under the queue lock (a second dispatcher sees it taken). Split jobs get child jobs."""
    if getattr(ctx, 'generation', 1) == 2:
        return {"ok": False, "error": "generation-two ordinary dispatch is disabled; reconcile existing launches or submit a contained fleet job"}
    with ctx.qlock():
        job = load_job(ctx, a["job"])
        if not job or job["status"] != "queued":
            return {"ok": False, "error": f"{a['job']} is not queued"}
        if a.get("where") == "host" and HOST_PAUSED:
            return {"ok": False, "error": "host paused (host.conf.json)"}
        if a.get("where") == "laptop" and not laptop_executes():
            return {"ok": False, "error": f"the laptop is {LAPTOP_NOT_EXECUTOR}: {a['job']} stays queued"}
        if a.get("where") == "host" and job.get("host_config") and not binding_matches_current(job):
            return {"ok": False, "error": "previous launch is bound to another host/configuration; reconcile it before retrying"}
        if RAM_JOBS and a.get("where") == "host" and not job.get("defer"):
            active = [j for j in all_jobs(ctx) if j.get("status") in ("launching", "running")
                      and j.get("placed_where") == "host" and j.get("host", HOST) == HOST and not j.get("children")
                      and not (j.get("defer") and j.get("status") == "launching")]
            reserved = reserved_memory(ctx)
            if reserved + memory_reservation(job) * a.get("parts", 1) > RAM_BUDGET_GB:
                return {"ok": False, "error": "mixed disk/RAM memory reservation budget exhausted"}
            slots = sum(j.get("slots_eff", j.get("slots", 0)) for j in active)
            if slots + job["slots"] * a.get("parts", 1) > HOST_SLOTS:
                return {"ok": False, "error": "host slots reserved until workload shutdown"}
        if job.get("temp_storage") == "ram":
            if not RAM_JOBS or a.get("where") != "host":
                return {"ok": False, "error": "RAM host capability unavailable; no disk fallback"}
        now = ctx.now()
        slots = a.get("slots", job["slots"])
        parts = a.get("parts", 1) if job.get("split") else 1
        job.update(placed_where=a["where"], slots_eff=slots, t_launch=now, launch=iso(now), attempts=job.get("attempts", 0) + 1)
        if a["where"] == "host":
            if not job.get("host_config"):
                job["host"] = HOST
                job["host_config"] = host_binding()
            if job.get("defer"):
                job["deferred_phase"] = "staging"
                job["deferred_load_cap"] = LOAD_CAP or a.get("load_cap") or ((load_capacity(ctx) or {}).get("host") or {}).get("nproc")
        runnable = [job["id"]]
        if job.get("split"):
            job["parts"] = parts
            if parts > 1:
                job["children"] = [f"{job['id']}.p{k}" for k in range(1, parts + 1)]
                job["status"] = "running"
                job["t_started"] = now
                job["started"] = iso(now)
                runnable = []
                for k, cid in enumerate(job["children"], 1):
                    child = {kk: vv for kk, vv in job.items() if kk not in ("children", "split", "parts")}
                    child.update(id=cid, parent=job["id"], part=f"{k}/{parts}", notify=False, status="launching",
                                 command=substitute_part(job["command"], k, parts), split=None, label=f"{job['label']} {k}/{parts}",
                                 notified=True, attempts=1)
                    save_job(ctx, child)
                    runnable.append(cid)
            else:
                job["command"] = substitute_part(job["command"], 1, 1)
                job["status"] = "launching"
        else:
            job["status"] = "launching"
        save_job(ctx, job)
    return {"ok": True, "launch": runnable}


def cmd_dispatch(ctx, req):
    r = dispatch(ctx, req)
    if r.get("ok"):
        (run_workers if ctx.sync else spawn_workers)(ctx, [req])
    return r


def runnable_ids(ctx, a):
    job = load_job(ctx, a["job"])
    if not job:
        return []
    return job["children"] if job.get("children") else [job["id"]]


def spawn_workers(ctx, items):
    for a in items:  # one worker per accepted job, so launches of different jobs overlap
        ids = runnable_ids(ctx, a)
        if not ids:
            continue
        if ctx.spawn:
            ctx.spawn(ids)
            continue
        os.makedirs(ctx.p("launch"), exist_ok=True)
        env = dict(os.environ, CQ_STATE=ctx.state)
        with open(ctx.p("launch", ids[0] + ".log"), "wb") as log:
            spawn_detached([sys.executable, os.path.abspath(__file__), "_launch", *ids], env=env, stdout=log)


def run_workers(ctx, items):
    for a in items:
        ids = runnable_ids(ctx, a)
        for jid in ids:
            launch(ctx, jid, ids)


# --------------------------------------------------------------------------- launching: host

LAUNCHER = r'''set -eu
ID=@ID@
Q="$HOME/q"
D="$Q/$ID"
mkdir -p "$Q"
if ! mkdir "$D" 2>/dev/null; then echo "CQ_EXISTS $ID"; exit 17; fi
trap 'rc=$?; if [ "$rc" -ne 0 ]; then rm -rf "$D"; fi' EXIT
TOP=$(git rev-parse --show-toplevel)
mkdir -p "$D/tmp"
@TMP_BLOCK@cp -a "$TOP" "$D/src"
printf '%s' @CMD_B64@ | base64 -d > "$D/cmd.sh"
{
  printf 'export PATH=%q\n' "$PATH"
  printf 'export GOTOOLCHAIN=%q\n' "${GOTOOLCHAIN:-local}"
  printf 'export GOCACHE=%q\n' "${GOCACHE:-$HOME/.cache/go-build}"
  printf 'export GOMODCACHE=%q\n' "${GOMODCACHE:-$HOME/.cache/go-mod}"
@GOFLAGS@@TMP_ENV@  printf 'export CQ_JOB=%q\n' "$ID"
  printf 'export CQ_SHA=%q\nexport CQ_BASE=%q\nexport CQ_SRC=%q\n' @SHA@ @BASE@ "$D/src"
  printf 'export HEAVY_SLOTS=%q\n' @HEAVY_SLOTS@
  # the host's frozen interpreter (pyfrozen's), which the document parser helper's tests need: a host fact, not a job's
  if [ -x "$HOME/venvs/app/bin/python" ]; then printf 'export CQ_DOCPARSE_PYTHON=%q\n' "$HOME/venvs/app/bin/python"; fi
  printf 'SLOTS=%q\nSELFHEAVY=%q\nTIMEOUT_S=%q\nSUBDIR=%q\n' @SLOTS@ @SELFHEAVY@ @TIMEOUT_S@ @SUBDIR@
} > "$D/job.env"
cat > "$D/run.sh" <<'RUNEOF'
set -u
D=$(cd "$(dirname "$0")" && pwd)
export D
. "$D/job.env"
export SLOTS SELFHEAVY TIMEOUT_S SUBDIR
RT=$(cat "$D/ramtmp" 2>/dev/null || true)  # the tmp_root dir, read before the job runs: the job cannot redirect its removal
echo $$ > "$D/pid.tmp" && mv "$D/pid.tmp" "$D/pid"
ps -o pgid= -p $$ 2>/dev/null | tr -d ' ' > "$D/pgid.tmp"; mv "$D/pgid.tmp" "$D/pgid"
date +%s > "$D/launched"
cd "$D/src/$SUBDIR" || { echo "cq: no such subdir $SUBDIR" >&2; echo 2 > "$D/exit.tmp" && mv "$D/exit.tmp" "$D/exit"; exit 2; }
inner='date +%s > "$D/started"; exec timeout -k 30 "$TIMEOUT_S" bash "$D/cmd.sh"'
if [ "$SLOTS" -gt 0 ] && [ "$SELFHEAVY" != 1 ]; then
  heavy -n "$SLOTS" bash -c "$inner"; rc=$?
else
  bash -c "$inner"; rc=$?
fi
# CPU seconds (user+sys) of the finished process tree: line 2 of bash `times` is the children's, as NmN.NNNs
times > "$D/cpu.raw" && awk 'NR==2{for(i=1;i<=NF;i++){split($i,a,"m");t+=a[1]*60+a[2]} printf "%.0f\n",t}' "$D/cpu.raw" > "$D/cpu.tmp" 2>/dev/null && mv "$D/cpu.tmp" "$D/cpu" || true
rm -f "$D/cpu.raw"  # `times` is read from a file: in a pipeline bash forks it and the children's time reads 0
if [ -n "$RT" ]; then rm -rf "$RT"; fi  # RAM back as the job ends; cleanup_host takes a killed job's
date +%s > "$D/finished"
echo $rc > "$D/exit.tmp" && mv "$D/exit.tmp" "$D/exit"
RUNEOF
setsid nohup bash "$D/run.sh" > "$D/run.log" 2>&1 < /dev/null 9>&- &
trap - EXIT  # a lost acknowledgement must retain the idempotency directory
echo "CQ_LAUNCHED $ID"
'''


# host.conf.json tmp_root, a tmpfs: the job's temp files in RAM (the host's drives pay ~10 ms per fsync), under a path
# short enough for the unix sockets tests bind in it; $D/tmp when the root cannot be made or is not writable
TMP_BLOCK = r'''T="$D/tmp"; TR=@TMP_ROOT@
if mkdir -p "$TR" 2>/dev/null && [ -O "$TR" ] && R=$(mktemp -d "$TR/XXXXXXXX" 2>/dev/null); then
  T=$R
  trap 'rc=$?; if [ "$rc" -ne 0 ]; then rm -rf "$D" "$T"; fi' EXIT
  printf '%s' "$T" > "$D/ramtmp"
else
  : > "$D/tmp_fallback"  # temp files on the disk: a visible sign for the probe or a reader of the job dir
fi
'''
# -count=1: with the directory out of the cache keys, a test binary is the same in every job copy, and go test would
# replay another job's cached PASS (it rechecks only the env and module files the test itself reads). GOROOT: a
# -trimpath binary has no default, so runtime.GOROOT() would be "" for tests that run the toolchain through it.
GOFLAGS_TRIMPATH = r'''  printf 'export GOFLAGS=%q\n' "-trimpath -count=1${GOFLAGS:+ $GOFLAGS}"
  G=$(command -v go >/dev/null 2>&1 && go env GOROOT 2>/dev/null || true)
  if [ -n "$G" ]; then printf 'export GOROOT=%q\n' "$G"; fi
'''
TMP_ENV_DISK = r'''  printf 'export TMPDIR=%q\n' "$D/tmp"
'''
TMP_ENV_RAM = r'''  printf 'export TMPDIR=%q\n' "$T"
  if [ "$T" != "$D/tmp" ]; then printf 'export GOTMPDIR=%q\nexport CQ_RAM_TMP=%q\n' "$T" "$T"; fi  # Go 1.27's t.TempDir reads GOTMPDIR
'''


def job_sandbox(job):
    """(boxed, network) for the job as its launcher is made: the one decision make_launcher and the staging calls share.
    The host setting binds late (fleet placement), so this is the authoritative check of the job's sandbox request.
    A binding is frozen when the job is first dispatched, so a setting made stricter since then must still apply:
    where the binding is for the host whose settings are loaded now, the stricter of the two decides."""
    binding = job.get("host_config") or {}
    mode = binding.get("sandbox", "off") if binding else None
    if not binding or binding.get("machine_id") == _CONF.get("machine_id"):
        mode = stricter_sandbox(mode, _CONF.get("sandbox", "off"))
    return sandbox_plan(job, {"sandbox": mode})


def make_launcher(job):
    binding = job.get("host_config") or {}
    tmp_root = binding.get("tmp_root", TMP_ROOT) if job.get("temp_storage") != "ram" else ""
    template = LAUNCHER
    if job.get("defer"):
        tmp_root = ""  # deferred jobs use disk scratch even when the host has a tmpfs default
        if binding.get("_policy_error"):
            raise CqError(binding["_policy_error"])
        try:
            effective_policy = queue_policy.policy_fields(binding, binding.get("slots", HOST_SLOTS))
        except ValueError as exc:
            raise CqError(str(exc)) from exc
        custom_policy = not queue_policy.policy_is_default(effective_policy)
        descriptor = {"version": 1, "id": job["id"], "mode": job["defer"], "slots": job["slots"],
                      "memory_max_gb": job["memory_max_gb"], "est_minutes": job["est_minutes"],
                      "timeout_minutes": job["timeout_minutes"], "pool_slots": binding.get("slots", HOST_SLOTS),
                      "load_cap": job.get("deferred_load_cap") or LOAD_CAP,
                      "ram_floor_gb": binding.get("ram_floor_gb", HOST_RAM_FLOOR_GB), "source_sha": job["sha"],
                      "timezone": "Europe/London", "window_start": 0, "window_end": 8}
        if custom_policy:
            descriptor.update(effective_policy)
            descriptor["resource_policy_version"] = queue_policy.RESOURCE_POLICY_VERSION
        if job.get("fleet"):
            descriptor.update(fleet=True, priority=job.get("priority", "lane"))
            # the expectation only for a host that admits on it (its helper has the fleet memory pool in effect and the
            # host is not switched off, as the binding was made from its probe), and only when it is below the cap
            expected = job.get("memory_expected_gb")
            if (binding.get("expected_memory_admission") is True and _number(expected)
                    and 0 < expected < job["memory_max_gb"]):
                descriptor["memory_expected_gb"] = expected
            # the CPU expectation likewise: only for a host set to charge it, and only when it is below the declared slots
            cores = job.get("cpu_expected")
            if binding.get("expected_cpu_admission") is True and _number(cores) and 0 < cores < job["slots"]:
                descriptor["cpu_expected"] = cores
            # when an immediate job was submitted: the host's queue ages a waiting job from then and not from its
            # registration, so one handed over after a long wait keeps the place its age gives it (deferred.py
            # queue_order). A helper from before the field ignores it.
            if job["defer"] == "immediate" and _number(job.get("t_submitted")) and job["t_submitted"] >= 1:
                descriptor["submitted"] = int(job["t_submitted"])
        data = base64.b64encode(json.dumps(descriptor, separators=(",", ":")).encode()).decode()
        policy_check = 'python3 "$HOME/.local/share/baton/compute-deferred/deferred.py" check >/dev/null'
        if custom_policy:
            check_code = ('import json,sys; p=json.load(sys.stdin); '
                          'sys.exit(0 if p.get("ok") is True and p.get("resource_policy_version") == 2 '
                          'else "host helper does not support configurable scheduling policy")')
            policy_check = ('CQ_POLICY_CHECK=$(python3 "$HOME/.local/share/baton/compute-deferred/deferred.py" check)\n'
                            'printf "%s" "$CQ_POLICY_CHECK" | python3 -c ' + shlex.quote(check_code))
        template = template.replace('mkdir -p "$Q"', policy_check + '\nmkdir -p "$Q"', 1)
        template = template.replace('mkdir -p "$D/tmp"', 'mkdir -p "$D/tmp"\nprintf deferred > "$D/temp_storage"', 1)
        template = template.replace('. "$D/job.env"', '. "$D/job.env"\nSELFHEAVY=1', 1)
        template = template.replace('setsid nohup bash "$D/run.sh" > "$D/run.log" 2>&1 < /dev/null 9>&- &\ntrap - EXIT  # a lost acknowledgement must retain the idempotency directory\necho "CQ_LAUNCHED $ID"',
            f'printf %s {shlex.quote(data)} | base64 -d > "$D/deferred.json"\n'
            'trap - EXIT  # retain staged data even when registration acknowledgement is lost\n'
            'RESULT=$(python3 "$HOME/.local/share/baton/compute-deferred/deferred.py" register "$ID")\n'
            'printf "%s\\n" "$RESULT"\n'
            "printf '%s' \"$RESULT\" | python3 -c 'import json,sys; s=json.load(sys.stdin); sys.exit(0 if s.get(\"phase\") == \"queued\" and s.get(\"registered\") else 1)'\n"
            'echo "CQ_REGISTERED $ID"')
        if job.get("fleet"):
            template = template.replace('echo "CQ_REGISTERED $ID"',
                'systemctl --user start --no-block baton-deferred.service >/dev/null 2>&1 || true\n'
                'echo "CQ_REGISTERED $ID"')
    if job.get("temp_storage") == "ram":
        template = template.replace('mkdir -p "$Q"', '"$HOME/.local/bin/cq-job" check >/dev/null\nmkdir -p "$Q"', 1)
        template = template.replace("trap 'rc=$?; if [ \"$rc\" -ne 0 ]; then rm -rf \"$D\"; fi' EXIT",
                                    ': # RAM launch errors retain disk evidence and host reservations')
        template = template.replace('. "$D/job.env"', '. "$D/job.env"\nexport TMPDIR="${CQ_RAM_TMP:?RAM service required}"\nexport GOTMPDIR="$TMPDIR"', 1)
        template = template.replace('setsid nohup bash "$D/run.sh" > "$D/run.log" 2>&1 < /dev/null 9>&- &',
                                    '"$HOME/.local/bin/cq-job" start "$ID" --memory-bytes ' +
                                    str(int(job["memory_max_gb"] * 2**30)) + ' --timeout-seconds @TIMEOUT_S@')
        template = template.replace('mkdir -p "$D/tmp"', 'printf ram > "$D/temp_storage"')
    boxed, network = job_sandbox(job)
    if boxed:
        # the job runs through the helper, which builds its sandbox or refuses (the job never runs outside it). timeout
        # stays inside, so TERM reaches the job; started is written outside, the job directory is read-only inside.
        # --rw: the job's scratch directory, which the sandbox's /tmp does not hold: the tmp_root dir of a disk job
        # (RT, read from ramtmp before the job runs) or the directory the RAM service made for a RAM job.
        check = f'[ -f {JOBBOX} ] && [ -x {JOBBOX} ] || {{ echo "CQ_NO_JOBBOX $ID"; echo {shlex.quote(JOBBOX_MISSING)} >&2; exit 79; }}'
        template = template.replace('mkdir -p "$Q"', check + '\nmkdir -p "$Q"', 1)
        # GOFLAGS: the bundled staging helper (remote_stage.py) gives a boxed job a self-contained src, but a host that
        # stages through the external REMOTERUN helper (no portable_runner) still gives it a shared clone whose
        # alternates point into the real home, absent in the sandbox: no git history there, and go build of a main
        # package would fail to get VCS status. -buildvcs=false is kept for those jobs.
        template = template.replace("inner='date +%s", 'export CQ_BOX_RW="${RT:-${CQ_RAM_TMP:-}}"\n'
                                    'export GOFLAGS="${GOFLAGS:+$GOFLAGS }-buildvcs=false"\ninner=\'date +%s', 1)
        template = template.replace('exec timeout -k 30',
                                    f'exec {JOBBOX}{" --net" if network else ""} --dir "$D" ${{CQ_BOX_RW:+--rw "$CQ_BOX_RW"}} -- timeout -k 30', 1)
        if (check not in template or template.count(f"exec {JOBBOX}") != 1 or template.count("export CQ_BOX_RW=") != 1
                or template.count("-buildvcs=false") != 1):
            raise CqError("sandbox launcher template mismatch")  # not a SandboxError: a changed template must be loud
    return (template.replace("@ID@", shlex.quote(job["id"]))
            .replace("@CMD_B64@", shlex.quote(base64.b64encode(job["command"].encode("utf-8")).decode()))
            .replace("@TMP_BLOCK@", TMP_BLOCK.replace("@TMP_ROOT@", shlex.quote(tmp_root)) if tmp_root else "")
            .replace("@TMP_ENV@", TMP_ENV_RAM if tmp_root else TMP_ENV_DISK)
            .replace("@GOFLAGS@", GOFLAGS_TRIMPATH if binding.get("go_trimpath", GO_TRIMPATH) else "")
            .replace("@HEAVY_SLOTS@", str(binding.get("slots", HOST_SLOTS)))
            .replace("@SLOTS@", str(int(job.get("slots_eff", job["slots"]))))
            .replace("@SHA@", shlex.quote(job["sha"]))
            .replace("@BASE@", shlex.quote(job.get("base") or job["sha"]))
            .replace("@SELFHEAVY@", "1" if job.get("self_heavy") else "0")
            .replace("@TIMEOUT_S@", str(int(job["timeout_minutes"]) * 60))
            .replace("@SUBDIR@", shlex.quote(job["subdir"])))


def dispatch_lock_path(ctx, top):
    return ctx.p("locks", "dispatch-" + re.sub(r"[^A-Za-z0-9_-]", "_", norm_path(top)) + ".lock")


def launch_age(job, now):
    """Seconds since the launch began or its worker last said it is alive (t_touch), whichever is later."""
    return now - max(job.get("t_launch", now), job.get("t_touch", 0))


def touch_launch(ctx, ids):
    """The launch worker is alive: refresh t_touch of the jobs it still has to launch (the reaper's LAUNCH_STALE and the
    120 s no-pid checks read it). t_launch stays the dispatch time, which the probe's slot accounting compares."""
    with ctx.qlock():
        for jid in ids:
            j = load_job(ctx, jid)
            if j and j["status"] == "launching":
                j["t_touch"] = ctx.now()
                save_job(ctx, j)


def launch(ctx, job_id, family=None):
    """Launch one job; `family`: every job of this worker (a split job's parts), kept fresh while it waits its turn."""
    job = load_job(ctx, job_id)
    if not job or job["status"] != "launching":
        return
    try:
        touch_launch(ctx, family or [job_id])
        if job["placed_where"] == "host":
            launch_host(ctx, job, family or [job_id])
        elif not laptop_executes():
            raise CqError(f"the laptop is {LAPTOP_NOT_EXECUTOR}: a laptop job is never started here")
        else:
            launch_laptop(ctx, job)
    except LockTimeout as e:  # busy, not broken: a requeue (a part cannot be requeued: it fails with this reason)
        requeue_or_fail(ctx, job_id, f"launch lock busy: {e}", transient=True)
    except Exception as e:  # a worker must never leave a job 'launching'
        if job.get("defer"):
            _launch_result(ctx, job_id, "launching", launch_uncertain=True, deferred_phase="registration_unconfirmed",
                           reason=f"staging outcome unconfirmed: {type(e).__name__}: {e}")
        else:
            requeue_or_fail(ctx, job_id, f"launch error: {type(e).__name__}: {e}", transient=False)


def _launch_result(ctx, job_id, status, **fields):
    """Set the outcome of a launch under the queue lock; a cancel that raced the launch wins."""
    with ctx.qlock():
        job = load_job(ctx, job_id)
        if not job:
            return None
        if job["status"] != "launching":  # cancelled meanwhile, or already reaped as finished
            return job
        now = ctx.now()
        job.update(status=status, **fields)
        if status == "running":
            t0 = fields.get("t_started") or now
            job.update(t_started=t0, started=iso(t0))
            job.pop("launch_uncertain", None)
        save_job(ctx, job)
        return job


def requeue_or_fail(ctx, job_id, reason, transient):
    with ctx.qlock():
        job = load_job(ctx, job_id)
        if not job or job["status"] != "launching":
            return
        if job.get('cancel_requested') or job.get('host_timeout_requested'):
            job['reason'] = 'stop pending original host confirmation'
            save_job(ctx, job)
            return
        if job.get("launch_uncertain"):
            job["reason"] = "remote launch remains unconfirmed at its original endpoint: " + reason
            save_job(ctx, job)
            return
        if job.get("temp_storage") == "ram":
            # SSH and worker failure can leave a service running. Stop/tombstone and observe it before settlement.
            job.update(launch_error=reason, reason=reason)
            save_job(ctx, job)
            return
        if transient and not job.get("parent") and job.get("attempts", 1) < MAX_ATTEMPTS:
            job.update(status="queued", reason=reason, placed_where=None)
            save_job(ctx, job)
            return
    finish(ctx, job_id, "failed", exit_code=None, reason=reason, tail=[])


def launch_host(ctx, job, family=None):
    if not os.path.isdir(job["top"]):
        raise CqError(f"worktree {job['top']} is gone")
    with FileLock(dispatch_lock_path(ctx, job["top"]), 540.0):  # one push at a time per lane ref on the host
        if getattr(ctx, "refresh_config", False):
            with CONFIG_LOCK:
                refresh_host_config(ctx.state, ctx.host_id)
        touch_launch(ctx, family or [job["id"]])  # the wait may have taken minutes; the push below takes up to 9 more
        current = load_job(ctx, job["id"])
        if (not current or current["status"] != "launching" or current.get("cancel_requested")
                or current.get('host_timeout_requested')):
            return
        if HOST_PAUSED or not binding_matches_current(job):
            requeue_or_fail(ctx, job["id"], "host paused or changed before launch", transient=True)
            return
        try:
            script = make_launcher(job)
            boxed = job_sandbox(job)[0]  # the decision make_launcher just made, for the staging helper
        except SandboxError as exc:  # the host's setting changed since submit, or fleet placement bound another host
            requeue_or_fail(ctx, job["id"], str(exc), transient=False)
            return
        b64 = base64.b64encode(script.encode("utf-8")).decode()
        argv = ["--rev", job["sha"], "--subdir", job["subdir"], "--", "bash", "-c", f"echo {b64} | base64 -d | bash"]
        if ctx.run_remoterun is real_run_remoterun:
            rc, out, err = real_run_remoterun(argv, job["top"], host=job.get("host", HOST), binding=job.get("host_config"),
                                              sandboxed=boxed)
        else:
            rc, out, err = ctx.run_remoterun(argv, job["top"])
    if f"CQ_NO_JOBBOX {job['id']}" in out:
        # the launcher stopped before it made anything on the host: a fixed fact of the host, not a transient fault. A
        # disk or deferred job fails with the reason; a RAM job takes its usual settlement (requeue_or_fail).
        requeue_or_fail(ctx, job["id"], JOBBOX_MISSING, transient=False)
        return
    if job.get("defer"):
        if f"CQ_REGISTERED {job['id']}" in out:
            _launch_result(ctx, job["id"], "launching", deferred_phase="queued", deferred_registered=True, reason="registered on host",
                           host_dir=f"~/q/{job['id']}", **staging_note(rc, err))
        else:
            # Any interruption after remote staging can leave an accepted registration.
            # Reconcile the ID at the original endpoint before considering another push.
            _launch_result(ctx, job["id"], "launching", launch_uncertain=True,
                           deferred_phase="registration_unconfirmed", t_touch=ctx.now(),
                           reason=f"registration unconfirmed (exit {rc}); reconciling host: {(out + err)[-300:]}")
        return
    if f"CQ_LAUNCHED {job['id']}" in out or rc == 17 or f"CQ_EXISTS {job['id']}" in out:
        res = _launch_result(ctx, job["id"], "running", host_dir=f"~/q/{job['id']}", log_path=f"{job.get('host', HOST)}:~/q/{job['id']}/run.log",
                             **staging_note(rc, err))
        if res and (res["status"] == "cancelled" or res.get("cancel_requested")):
            kill_running(ctx, res)
        return
    tail = "\n".join((out + "\n" + err).strip().splitlines()[-6:])
    if rc == 75 or "host disk low" in err:
        hs = host_state(ctx)
        hs["disk_paused"] = True
        hs["disk_alert"] = {"at": iso(ctx.now()), "job": job["id"], "msg": "host disk low (lanerun exit 75): host dispatch paused until a probe shows the disk ok", "acked": False}
        save_host_state(ctx, hs)
        if job.get("temp_storage") == "ram":
            requeue_or_fail(ctx, job["id"], "host disk low (lanerun exit 75); host dispatch paused", transient=False)
        else:
            finish(ctx, job["id"], "failed", exit_code=75, reason="host disk low (lanerun exit 75); host dispatch paused", tail=tail.splitlines())
        return
    down = rc == 255 or "push failed" in err or "Connection" in err or "timed out" in err or "Could not resolve" in err
    if down:
        hs = host_state(ctx)
        # a dropped connection (ssh could not reach the host) is re-probed on the next tick, whose success lets dispatch resume;
        # any other launch failure (a failing push, a slow one timing out) keeps the 60 s pause, so a push problem on a host that
        # still probes fine does not burn every queued job's attempts within a minute
        blip = any(s in err for s in SSH_TRANSIENT)
        hs.update(down_since=hs.get("down_since") or iso(ctx.now()), next_probe=ctx.now() + (0 if blip else 60),
                  down_reason=tail[-200:])
        save_host_state(ctx, hs)
    # RAM services have a stop/tombstone protocol: retain that recovery path
    # until the helper proves every descendant gone. Disk launches lack it.
    if job.get("temp_storage") != "ram" and rc == 255 and not any(s in err for s in SSH_TRANSIENT + ("Permission denied", "Host key verification failed", "REMOTE HOST IDENTIFICATION HAS CHANGED")):
        with ctx.qlock():
            current = load_job(ctx, job["id"])
            if current and current.get("status") == "launching":
                current.update(launch_uncertain=True, t_touch=ctx.now(),
                               reason="remote launch unconfirmed; reconciling its original endpoint: " + tail[-200:],
                               host_dir=f"~/q/{job['id']}", log_path=f"{job.get('host', HOST)}:~/q/{job['id']}/run.log")
                save_job(ctx, current)
        return
    requeue_or_fail(ctx, job["id"], f"launch failed (remoterun exit {rc}): {tail[-300:]}", transient=True)


# --------------------------------------------------------------------------- launching: laptop

def bash_exe():
    return os.environ.get("CQ_BASH") or (GIT_BASH if os.path.isfile(GIT_BASH) else (shutil.which("bash") or "bash"))


def export_tree(top, sha, dest):
    """git archive <sha> | extract into dest (never a git worktree)."""
    os.makedirs(dest, exist_ok=True)
    skipped = []
    with subprocess.Popen(["git", "-C", top, "archive", "--format=tar", sha], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          creationflags=CREATE_NO_WINDOW if os.name == "nt" else 0) as p:
        with tarfile.open(fileobj=p.stdout, mode="r|") as tf:
            for m in tf:
                if m.issym() or m.islnk():
                    skipped.append(m.name)
                    continue
                try:
                    tf.extract(m, dest, filter="data")
                except (OSError, tarfile.TarError):
                    skipped.append(m.name)  # a name Windows refuses, a path over the limit
        err = p.stderr.read().decode("utf-8", "replace")
    if p.returncode != 0:
        raise CqError(f"git archive failed: {err[:200]}")
    return skipped


def checkout_tree(top, sha, dest):
    """A real git checkout of the submitting repo at `sha` in dest, with its tags, as the host's src is: a shared clone with
    no checkout (objects borrowed from `top`, so the snapshot commit is there; never a linked worktree), HEAD detached on
    sha, files written with no newline conversion. When Windows refuses a name the checkout fails: the tree is then
    filled by export_tree (which skips what it cannot write) and HEAD stays on sha. Returns the skipped names."""
    if os.path.isdir(dest):
        rmtree_force(dest)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    git(os.path.dirname(dest), "clone", "-q", "--shared", "--no-checkout", "-c", "core.autocrlf=false", top, dest)
    git(dest, "update-ref", "--no-deref", "HEAD", sha)
    rc, _ = git(dest, "reset", "-q", "--hard", check=False)
    if rc == 0:
        return []
    skipped = export_tree(top, sha, dest)
    git(dest, "reset", "-q", "--mixed", check=False)
    return skipped or ["(git checkout failed; filled by export)"]


def launch_laptop(ctx, job):
    run_dir = ctx.p("run", job["id"])
    src = os.path.join(run_dir, "src")
    skipped = checkout_tree(job["top"], job["sha"], src)
    with open(os.path.join(run_dir, "run.log"), "wb") as f:
        if skipped:
            f.write(f"[cq] export skipped {len(skipped)} entries (links, names Windows refuses): {', '.join(skipped[:10])}\n".encode())
    with open(os.path.join(run_dir, "cmd.sh"), "wb") as f:
        f.write(job["command"].replace("\r\n", "\n").encode("utf-8"))
    sub = os.path.join(src, *job["subdir"].split("/"))
    if not os.path.isdir(sub):
        raise CqError(f"no such subdir {job['subdir']} in {job['sha'][:12]}")
    env = dict(os.environ, CQ_STATE=ctx.state)
    touch_launch(ctx, [job["id"]])  # the export may have taken minutes
    with ctx.qlock():  # a cancel or a reap that came meanwhile wins: no process is started for a job that is not launching
        j = load_job(ctx, job["id"])
        if not j or j["status"] not in ("launching", "running"):
            return
        if not ctx.sync:
            with open(os.path.join(run_dir, "run.log"), "ab") as log:
                spawn_detached([sys.executable, os.path.abspath(__file__), "_laprun", job["id"]], env=env, stdout=log)
        if j["status"] == "launching":
            now = ctx.now()
            j.update(status="running", log_path=os.path.join(run_dir, "run.log"), t_started=now, started=iso(now))
            save_job(ctx, j)
    if ctx.sync:
        laprun(ctx, job["id"])


def laprun(ctx, job_id):
    """Detached wrapper: run the command (through heavyrun.py when slots > 0), write pid/started/exit atomically."""
    job = load_job(ctx, job_id)
    run_dir = ctx.p("run", job_id)
    src = os.path.join(run_dir, "src")
    atomic_write(os.path.join(run_dir, "pid"), str(os.getpid()).encode())
    inner = [bash_exe(), os.path.join(run_dir, "cmd.sh")]
    needs_heavy = job.get("slots_eff", job["slots"]) > 0
    argv = [sys.executable, HEAVYRUN, "--", *inner] if needs_heavy else inner
    atomic_write(os.path.join(run_dir, "started"), str(int(time.time())).encode())
    # the job's src is its own git checkout; the ceiling keeps git from ever climbing out of the run dir into the baton repo
    env = dict(os.environ, CQ_JOB=job_id, CQ_SHA=job["sha"], CQ_BASE=job.get("base") or job["sha"], CQ_SRC=src,
               GIT_CEILING_DIRECTORIES=run_dir.replace("\\", "/"))
    flags = (CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP) if os.name == "nt" else 0
    with open(os.path.join(run_dir, "run.log"), "ab") as log:
        if needs_heavy and (not HEAVYRUN or not os.path.isfile(HEAVYRUN)):
            log.write(b"cq: legacy laptop jobs require CQ_HEAVYRUN or heavyrun in runtime.local.json; configure the existing helper and its shared heavy.lock.\n")
            atomic_write(os.path.join(run_dir, "finished"), str(int(time.time())).encode())
            atomic_write(os.path.join(run_dir, "exit"), b"1")
            return 1
        p = subprocess.Popen(argv, cwd=os.path.join(src, *job["subdir"].split("/")), stdin=subprocess.DEVNULL, stdout=log,
                             stderr=subprocess.STDOUT, env=env, creationflags=flags)
        try:
            rc = p.wait(timeout=job["timeout_minutes"] * 60)
        except subprocess.TimeoutExpired:
            ctx.laptop.kill_tree(p.pid)
            p.wait()
            rc = 124
    atomic_write(os.path.join(run_dir, "finished"), str(int(time.time())).encode())
    atomic_write(os.path.join(run_dir, "exit"), str(rc).encode())
    return rc


# --------------------------------------------------------------------------- finishing jobs and reaping

def memory_reservation(job):
    """One charge per leaf: RAM declaration, or estimated disk memory for its slots (one for a light job)."""
    return (job["memory_max_gb"] if job.get("temp_storage") == "ram" else
            max(1, job.get("slots_eff", job.get("slots", 1))) * (job.get("host_config") or {}).get("slot_ram_gb", SLOT_RAM_GB))


def reserved_memory(ctx):
    """Finished RAM scratch still consumes its full declaration until acknowledged cleanup."""
    return sum(memory_reservation(j) for j in all_jobs(ctx)
               if j.get("placed_where") == "host" and j.get("host", HOST) == HOST and not j.get("children")
               and ((j.get("status") in ("launching", "running") and not (j.get("defer") and j.get("status") == "launching"))
                    or (j.get("temp_storage") == "ram" and j.get("status") in TERMINAL and not j.get("ram_cleaned"))))


def job_ssh(ctx, job, script, timeout=60):
    if ctx.ssh is real_ssh:
        binding = job.get("host_config")
        if binding is None:
            binding = host_binding() if job.get("host", HOST) == HOST else {}
        return real_ssh(script, timeout=timeout, host=job.get("host", HOST), binding=binding)
    return ctx.ssh(script, timeout=timeout)


def deferred_control(ctx, job, operation):
    """Query the pinned endpoint; a failed query is never a registration or cancellation receipt."""
    jid = job["id"]
    if not re.fullmatch(r"cq-[A-Za-z0-9_-]+", jid):
        raise CqError("unsafe deferred job ID")
    helper = '"$HOME/.local/share/baton/compute-deferred/deferred.py"'
    script = (f'if [ ! -f {helper} ]; then echo CQ_DEFERRED_HELPER_MISSING; exit 42; fi\n'
              f'python3 {helper} {operation} {shlex.quote(jid)}\n')
    rc, out, err = job_ssh(ctx, job, script, timeout=60)
    if rc != 0:
        return {"error": "deferred host helper is not installed" if "CQ_DEFERRED_HELPER_MISSING" in out
                else (err or out or f"helper exit {rc}")[-300:]}
    try:
        result = json.loads(out.strip().splitlines()[-1])
        if result.get("version") != 1 or result.get("id") != jid or result.get("phase") not in (
                "absent", "queued", "starting", "running", "done", "failed", "cancelled"):
            raise ValueError("invalid deferred status")
        return result
    except (ValueError, IndexError, AttributeError):
        return {"error": "invalid deferred helper response"}


def reap_deferred(ctx, job, ent, now, ended, allow_registration=True, state=None):
    with FileLock(ctx.p("locks", "deferred-result-" + job["id"] + ".lock"), 60):
        current = load_job(ctx, job["id"])
        if current and current.get("status") not in TERMINAL:
            return _reap_deferred(ctx, current, ent, now, ended, allow_registration, state)


def _reap_deferred(ctx, job, ent, now, ended, allow_registration=True, state=None):
    state = state or deferred_control(ctx, job, "cancel" if job.get("cancel_requested") else "status")
    if state.get("error"):
        with ctx.qlock():
            current = load_job(ctx, job["id"])
            if current and current.get("status") not in TERMINAL:
                current["reason"] = "deferred host confirmation pending: " + state["error"]
                save_job(ctx, current)
        return
    phase = state["phase"]
    if phase == "absent":
        # A lost acknowledgement can leave a complete descriptor behind. Inspect it
        # before registering; never replay the source push over an existing directory.
        if allow_registration and job.get("launch_uncertain") and not job.get("cancel_requested"):
            jid = shlex.quote(job["id"])
            rc, out, _ = job_ssh(ctx, job, f'd="$HOME/q/"{jid}; if [ -f "$d/deferred.json" ]; then echo complete; elif [ -e "$d" ]; then echo partial; else echo absent; fi\n')
            if rc == 0 and out.strip() == "complete":
                state = deferred_control(ctx, job, "register")
                phase = state.get("phase", "absent")
            elif rc == 0 and out.strip() == "absent":
                with ctx.qlock():
                    current = load_job(ctx, job["id"])
                    if current and current.get("status") == "launching" and current.get("launch_uncertain"):
                        current.update(status="queued", deferred_phase="staging_pending", reason="host has no staged copy; will retry",
                                       next_stage=now + 60, placed_where=None)
                        current.pop("launch_uncertain", None)
                        save_job(ctx, current)
                return
        if phase == "absent":
            with ctx.qlock():
                current = load_job(ctx, job["id"])
                if current and current.get("status") not in TERMINAL:
                    current["reason"] = "registration unconfirmed; staged directory is incomplete or absent"
                    save_job(ctx, current)
            return
    with ctx.qlock():
        current = load_job(ctx, job["id"])
        if not current or current.get("status") in TERMINAL:
            return
        current.update(deferred_phase=phase, deferred_registered=bool(current.get("deferred_registered") or
                       phase != "cancelled" or state.get("registered")), reason=state.get("reason") or "")
        current.pop("launch_uncertain", None)
        if phase == "running" and current["status"] == "launching":
            current.update(status="running", t_started=(ent or {}).get("started") or now,
                           started=iso((ent or {}).get("started") or now))
        save_job(ctx, current)
    if phase == "cancelled" or (phase == "failed" and (ent or {}).get("exit") is None):
        digests = []
        fetched = fetch_results(ctx, job, capture=digests) if job.get("gets") else ([], [])
        tail, paths = fetched or ([], (load_job(ctx, job['id']) or {}).get('out_paths', []))
        missing = [{"path": rel, "reason": _artifact_failure_reason(
                    tail, 'artifact fetch unavailable, missing, unsafe, changed or exceeds quota')}
                   for rel in (job.get("gets") or []) if ctx.p("out", job["id"], *rel.split("/")) not in paths]
        finish(ctx, job["id"], phase, None,
               reason=state.get("reason") or ("cancelled after host confirmation" if phase == "cancelled" else "deferred host failed"),
               tail=tail, out_paths=paths, artifact_missing=missing, artifact_digests=digests)
        ended.append(job["id"])
    elif phase in ("done", "failed") and ent and ent.get("exit") is not None:
        reap_host(ctx, current, ent, now, ended)


DEFERRED_RUNTIME_FILES = ("queue_policy.py", "queue_generations.py", "artifact_transfer.py", "host_stop.py", "queue_paths.py", "cq.py", "multi_host.py", "remote_stage.py", "quiet_helper.py",
                          "fleet_queue.py", "fleet_telemetry.py", "windows_pool.py", "windows_broker.py")


def deferred_runtime(ctx):
    """Pin the small launch stack in queue state, independent of a disposable worktree."""
    source = os.path.dirname(os.path.abspath(__file__))
    files = {}
    digest = hashlib.sha256()
    for name in DEFERRED_RUNTIME_FILES:
        with open(os.path.join(source, name), "rb") as f:
            data = f.read()
        files[name] = data
        digest.update(name.encode("ascii") + b"\0" + len(data).to_bytes(8, "big") + data)
    version = digest.hexdigest()
    directory = ctx.p("deferred-runtime", version)
    for name, data in files.items():
        path = os.path.join(directory, name)
        try:
            with open(path, "rb") as f:
                same = f.read() == data
        except FileNotFoundError:
            same = False
        if not same:
            atomic_write(path, data)
    return os.path.join(directory, "cq.py"), version


def _controller_start_identity():
    """Windows process creation FILETIME; None means PID reuse cannot be ruled out."""
    if os.name != "nt":
        return None
    try:
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
        created, exited, kernel_time, user_time = (wintypes.FILETIME() for _ in range(4))
        if not kernel.GetProcessTimes(kernel.GetCurrentProcess(), ctypes.byref(created), ctypes.byref(exited),
                                      ctypes.byref(kernel_time), ctypes.byref(user_time)):
            return None
        return (created.dwHighDateTime << 32) | created.dwLowDateTime
    except (OSError, ValueError):
        return None


def ensure_deferred_controller(ctx, force=False):
    """Start at most one detached controller per shard, independently of legacy poller leases."""
    if (os.environ.get("CQ_DEFERRED_AUTOSTART") == "0" or ctx.sync or ctx.spawn is not None
            or not any(j.get("status") not in TERMINAL for j in all_deferred_jobs(ctx))):
        return
    if not force:
        try:
            with FileLock(ctx.p("deferred-controller.lock"), 0):
                pass
        except LockTimeout:
            return
    os.makedirs(ctx.p("launch"), exist_ok=True)
    runtime, _ = deferred_runtime(ctx)
    with open(ctx.p("launch", "deferred-controller.log"), "ab") as log:
        spawn_detached([sys.executable, runtime, "_deferred_loop", "--state", ctx.state], env=dict(os.environ, CQ_STATE=ctx.state), stdout=log)


def deferred_pass(ctx):
    """One bounded pass; stage without CPU reservations and reconcile only pinned host IDs."""
    jobs = [j for j in all_deferred_jobs(ctx) if j.get("status") not in TERMINAL]
    if not jobs:
        return False
    snap, _ = host_snapshot(ctx)
    now = ctx.now()
    can_stage = snap is not None and not HOST_PAUSED and snap.get("disk_kb", 0) >= HOST_DISK_MIN_KB
    staging = []
    for job in jobs:
        if job.get("status") == "queued":
            if can_stage and now >= job.get("next_stage", 0):
                entry = {"job": job["id"], "where": "host", "slots": job["slots"], "load_cap": snap.get("nproc")}
                if dispatch(ctx, entry).get("ok"):
                    staging.append(entry)
        elif job.get("status") in ("launching", "running"):
            binding = job.get("host_config") or {}
            if job.get("host", HOST) == HOST and binding_matches_current(job):
                remote = snap
            else:
                remote, _ = host_snapshot(ctx, host=job.get("host"), binding=binding)
            if remote is not None:
                reap_deferred(ctx, job, remote["jobs"].get(job["id"]), now, [])
    if staging:
        (run_workers if ctx.sync else spawn_workers)(ctx, staging)
    return True


def deferred_loop(ctx):
    try:
        # A second starter waits through the first starter's empty/exit boundary,
        # so a concurrent submission cannot lose its wake-up.
        with FileLock(ctx.p("deferred-controller.lock"), 45):
            receipt = {"version": 1, "pid": os.getpid(), "process_start_filetime": _controller_start_identity(),
                       "started": iso(ctx.now()), "runtime": norm_path(__file__),
                       "runtime_digest": os.path.basename(os.path.dirname(os.path.abspath(__file__))), "status": "running"}
            write_json(ctx.p("deferred-controller.json"), receipt)
            try:
                while True:
                    try:
                        refresh_host_config(ctx.state, ctx.host_id)
                        if not deferred_pass(ctx):
                            return
                    except Exception as exc:
                        print(f"deferred controller: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
                    time.sleep(30)
            finally:
                receipt.update(status="stopped", stopped=iso(ctx.now()))
                write_json(ctx.p("deferred-controller.json"), receipt)
    except LockTimeout:
        return


def cmd_ensure_deferred(ctx, req):
    ensure_deferred_controller(ctx)
    return {"ok": True}


def ram_control(ctx, job, operation):
    """A failed host query never means empty. RAM jobs require the versioned helper."""
    jid = job["id"]
    if not re.fullmatch(r"cq-[A-Za-z0-9_-]+(?:\.p[1-8])?", jid):
        raise CqError("unsafe RAM job ID")
    rc, out, _ = job_ssh(ctx, job, f'"$HOME/.local/bin/cq-job" {operation} {shlex.quote(jid)}\n', timeout=60)
    try:
        result = json.loads(out.strip().splitlines()[-1]) if rc == 0 else None
    except (ValueError, IndexError):
        return None
    return result if isinstance(result, dict) and result.get("version") == 1 and not result.get("error") else None


def reap_ram(ctx, job, ent, now, ended):
    """Hold slots and snapshot until systemd proves every descendant gone."""
    status = ram_control(ctx, job, "status")
    if status is None:
        return
    interrupted = status.get("needs_stop")
    abort = job.get("cancel_requested") or job.get("launch_error") or interrupted
    if status.get("absent") and not abort and launch_age(job, now) <= LAUNCH_STALE:
        return
    if abort or status.get("absent"):
        status = ram_control(ctx, job, "stop")
        if status is None:
            return
    if status.get("empty") is not True:
        return
    digests = []
    result = fetch_results(ctx, job, capture=digests)
    if result is None:
        return
    tail, paths = result
    code = status.get("exit", 255)
    reason = job.get("launch_error", "") or ("RAM controller interrupted during submission" if interrupted else "")
    state = "done" if code == 0 else "failed"
    if job.get("cancel_requested"):
        state, reason = "cancelled", "cancelled after cgroup shutdown"
    elif status.get("result") == "timeout" or (code == 124 and not reason):
        state, reason = "timeout", f"timeout after {job['timeout_minutes']} min"
    elif status.get("result") == "oom-kill":
        reason = "RAM job memory limit (processes plus tmpfs) exceeded"
    elif status.get("result") == "reboot":
        reason = "host reboot ended RAM job"
    elif status.get("result") == "unit-lost":
        reason = "systemd lost RAM unit metadata"
    elif status.get("absent") or status.get("cancelled"):
        state, code, reason = "failed", 255, reason or "RAM launcher lost before service start"
    with ctx.qlock():
        current = load_job(ctx, job["id"])
        if not current or current["status"] in TERMINAL:
            return
        if current.get("cancel_requested"):
            state, reason = "cancelled", "cancelled after cgroup shutdown"
        current["ram_status"] = status
        save_job(ctx, current)
    missing = [{'path': rel, 'reason': _artifact_failure_reason(
                tail, 'artifact missing, unsafe, changed or exceeds quota')}
               for rel in (job.get('gets') or []) if ctx.p('out', job['id'], *rel.split('/')) not in paths]
    finish(ctx, job["id"], state, code, reason=reason, tail=tail, out_paths=paths,
           started=(ent or {}).get("started"), finished=status.get("finished"),
           artifact_missing=missing, artifact_digests=digests)
    ended.append(job["id"])


def history_kind(command):
    words = [w for w in command.split() if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", w)]
    return " ".join(w.split("/")[-1] for w in words[:2])


def stored_tail_digest(lines):
    data = json.dumps(lines, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return {"sha256": hashlib.sha256(data).hexdigest(), "source": "stored_tail_utf8_json_v1"}


def finish(ctx, job_id, status, exit_code, reason="", tail=None, out_paths=None, started=None, finished=None, cpu_s=None, artifact_missing=None, artifact_digests=None, host_empty_confirmed=False):
    """Record a job's end under the queue lock: result fields, history, the protecting ref, parent roll-up."""
    parent_id = None
    with ctx.qlock():
        job = load_job(ctx, job_id)
        if not job or job["status"] in TERMINAL:
            return job
        if job.get("temp_storage") == "ram" and job.get("cancel_requested"):
            status, reason = "cancelled", "cancelled after cgroup shutdown"
        elif job.get('placed_where') == 'host' and not job.get('defer') and job.get('cancel_requested'):
            status, reason = 'cancelled', 'cancelled after host session shutdown'
        if host_empty_confirmed:
            job['host_empty_confirmed'] = True
        now = ctx.now()
        t0 = started or job.get("t_started") or job.get("t_launch") or now
        t1 = finished or now
        mins = round(max(0.0, t1 - t0) / 60, 1)
        paths = list(dict.fromkeys((job.get('out_paths') or []) + (out_paths or [])))
        job.update(status=status, exit=exit_code, reason=reason or job.get("reason", ""), t_finished=t1, finished=iso(t1),
                   minutes=mins, tail=cut_lines(tail or []), out_paths=paths)
        if job.get("defer"):
            job["tail_digest"] = stored_tail_digest(job["tail"])
        if artifact_digests is not None:
            by_path = {d['path']: d for d in (job.get('artifact_digests') or []) + artifact_digests}
            job['artifact_digests'] = list(by_path.values())
        if artifact_missing is not None:
            job["artifact_missing"] = artifact_missing
        cpu = {}
        if cpu_s is not None:
            cpu = {"cpu_s": int(cpu_s), "cores": round(cpu_s / max(1.0, t1 - t0), 2)}
        job.update(cpu)
        save_job(ctx, job)
        if not job.get("children"):
            row = {"kind": history_kind(job["command"]), "cmd": cmd_key(job["command"]), "workstream": job["workstream"],
                   "est": job["est_minutes"], "actual": mins,
                   "slots": job.get("slots_eff", job["slots"]), "where": job.get("placed_where"), "exit": exit_code, "status": status,
                   "t": iso(now), **cpu}
            if job.get("parent"):  # a split part: est_hint leaves its row out; ordinary rows carry no such key
                row["parent"] = job["parent"]
            append_line(ctx.p("history.jsonl"), row)
        parent_id = job.get("parent")
    if not parent_id and not job.get("artifact_missing") and not job.get('artifact_pending'):
        drop_ref(job.get("top"), job["id"])
    elif parent_id:
        roll_up_parent(ctx, parent_id)
    return job


def roll_up_parent(ctx, parent_id):
    with ctx.qlock():
        parent = load_job(ctx, parent_id)
        if not parent or parent["status"] in TERMINAL:
            return
        kids = [load_job(ctx, c) for c in parent.get("children", [])]
        if any(k is None or k["status"] not in TERMINAL for k in kids):
            return
        codes = [k["exit"] if isinstance(k.get("exit"), int) else 1 for k in kids]
        if all(k["status"] == "done" for k in kids):
            status = "done"
        elif any(k["status"] == "failed" for k in kids):
            status = "failed"
        elif any(k["status"] == "timeout" for k in kids):
            status = "timeout"
        else:
            status = "cancelled"
        per = max(3, TAIL_LINES // max(1, len(kids)))
        tail = []
        for k in sorted(kids, key=lambda x: x.get("part", "")):
            tail.append(f"-- part {k.get('part')} (exit {k.get('exit')}) --")
            tail.extend(cut_lines(k.get("tail") or [], per - 1))
        now = ctx.now()
        parent.update(status=status, exit=max(codes), t_finished=now, finished=iso(now),
                      minutes=round((now - parent.get("t_started", now)) / 60, 1), tail=cut_lines(tail),
                      out_paths=sorted({o for k in kids for o in (k.get("out_paths") or [])}),
                      parts_info=[{"id": k["id"], "part": k.get("part"), "status": k["status"], "exit": k.get("exit"),
                                   "minutes": k.get("minutes"), "log": k.get("log_path")} for k in kids])
        save_job(ctx, parent)
    drop_ref(parent.get("top"), parent["id"])


def _artifact_callbacks(ctx, jid):
    """Durably journal publication and merge it into the current job record."""
    def update(action, entry):
        with ctx.qlock():
            current = load_job(ctx, jid)
            if not current:
                raise OSError('artifact job disappeared')
            pending = current.get('artifact_pending')
            same = pending and pending.get('path') == entry.get('path') and pending.get('temp') == entry.get('temp')
            if action == 'record':
                if pending and not same:
                    raise OSError('another artifact requires recovery')
                current['artifact_pending'] = entry
            elif not same:
                raise OSError('artifact journal changed')
            elif action == 'done':
                digest = {'path': entry['path'], 'sha256': entry['sha256'], 'size_bytes': entry['size_bytes']}
                existing = [d for d in current.get('artifact_digests', []) if d.get('path') == entry['path']]
                if existing and existing != [digest]:
                    raise OSError('artifact metadata conflicts with journal')
                if not existing:
                    current.setdefault('artifact_digests', []).append(digest)
                path = ctx.p('out', jid, *entry['path'].split('/'))
                if path not in current.setdefault('out_paths', []):
                    current['out_paths'].append(path)
                current['artifact_missing'] = [m for m in current.get('artifact_missing', [])
                                               if m.get('path') != entry['path']]
                current.pop('artifact_pending', None)
            else:
                current.pop('artifact_pending', None)
            target = job_file(ctx, jid, bool(current.get('defer')))
            atomic_write(target, (json.dumps(current, indent=1, sort_keys=True) + '\n').encode('utf-8'), durable=True)
    return (lambda entry: update('record', entry), lambda entry: update('done', entry),
            lambda entry: update('clear', entry))


def capture_local_job(ctx, job, source):
    """Capture local files once, reconciling any interrupted publication first."""
    jid = job['id']
    with FileLock(ctx.p('locks', 'artifact-result-' + jid + '.lock'), 60):
        current = load_job(ctx, jid)
        if not current:
            raise OSError('artifact job disappeared')
        record, done, clear = _artifact_callbacks(ctx, jid)
        if current.get('artifact_pending'):
            if not artifact_transfer.recover_pending(ctx.state, jid, current['artifact_pending'], done, clear):
                raise OSError('artifact journal conflicts with published file')
            current = load_job(ctx, jid)
        policy = (artifact_transfer.validate_policy(current['artifact_policy']) if 'artifact_policy' in current else
                  {'max_file_bytes': artifact_transfer.LEGACY_FILE, 'total_bytes': artifact_transfer.DEFAULT_TOTAL})
        digests = current.get('artifact_digests') or []
        used = sum(d['size_bytes'] for d in digests)
        remaining = max(0, policy['total_bytes'] - used)
        captured = {d['path'] for d in digests}
        wanted = [(i, rel) for i, rel in enumerate(current.get('gets') or []) if rel not in captured]
        sink = artifact_transfer.CaptureSink(ctx.state, jid, record, done, clear)
        missing = artifact_transfer.capture_local(source, wanted, policy['max_file_bytes'], remaining, sink)
        current = load_job(ctx, jid)
        return (current.get('out_paths') or [], current.get('artifact_digests') or [],
                [{'path': rel, 'reason': 'artifact missing, unsafe, changed or exceeds quota'} for rel in missing])


class ArtifactCommandTooLong(Exception):
    pass


ARTIFACT_COMMAND_ERROR = ('artifact transport command exceeds the Windows 30000 UTF-16 unit limit; '
                          'source retained for manual recovery; use fewer or shorter paths in future requests')


def _artifact_failure_reason(tail, default):
    return ARTIFACT_COMMAND_ERROR if any(line == '[cq] ' + ARTIFACT_COMMAND_ERROR for line in tail) else default


def fetch_script(job, only_missing=None):
    """Compatibility script for older direct callers; queue fetch uses streaming."""
    return fleet_fetch_script(job, only_missing)


def fleet_fetch_script(job, only_missing=None):
    """Bounded regular artifacts confined to this job's captured source root."""
    wanted = [[i, rel] for i, rel in enumerate(job.get("gets") or [])
              if only_missing is None or rel in only_missing]
    config = base64.b64encode(json.dumps([job["id"], wanted, TAIL_LINES, LINE_CUT]).encode()).decode()
    program = '''import base64,contextlib,json,os,stat
from pathlib import Path,PurePosixPath
jid,wanted,tail_lines,line_cut=json.loads(base64.b64decode(CONFIG))
d=Path.home()/'q'/jid
print('==TAIL==')
try:
 fd=os.open(d/'run.log',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
 with os.fdopen(fd,'rb') as f:
  s=os.fstat(f.fileno())
  if not stat.S_ISREG(s.st_mode):raise OSError('invalid log')
  f.seek(max(0,s.st_size-131072))
  lines=f.read(131072).decode('utf-8','replace').splitlines()[-tail_lines:]
 for line in lines:print('> '+line[:line_cut])
except OSError:pass
root=d/'src'
for i,rel in wanted:
 try:
  path=PurePosixPath(rel)
  if path.is_absolute() or not path.parts or '..' in path.parts:raise OSError('invalid path')
  with contextlib.ExitStack() as stack:
   parent=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW);stack.callback(os.close,parent)
   for part in path.parts[:-1]:
    parent=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent);stack.callback(os.close,parent)
   fd=os.open(path.parts[-1],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
   with os.fdopen(fd,'rb') as f:
    s=os.fstat(f.fileno())
    if not stat.S_ISREG(s.st_mode) or s.st_size>8388608:raise OSError('invalid artifact')
    data=f.read(8388609)
    if len(data)>8388608:raise OSError('artifact grew')
  print('==GET %s=='%i);print(base64.b64encode(data).decode())
 except (OSError,ValueError):print('==MISSING %s=='%i)
print('==END==')
'''.replace('CONFIG', repr(config))
    code = base64.b64encode(program.encode()).decode()
    return "python3 -c " + shlex.quote("import base64;exec(base64.b64decode(" + repr(code) + "))") + "\n"


def parse_fetch(out):
    tail, gets, section, cur = [], {}, None, None
    for line in out.splitlines():
        if line.startswith("==") and line.endswith("=="):
            tag = line.strip("=")
            if tag == "TAIL":
                section = "tail"
            elif tag.startswith("GET "):
                section, cur = "get", int(tag.split()[1])
                gets[cur] = ""
            elif tag.startswith("MISSING "):
                section = None
                gets[int(tag.split()[1])] = None
            else:
                section = None
            continue
        if section == "tail":
            tail.append(line)
        elif section == "get" and cur is not None:
            gets[cur] += line.strip()
    return tail, gets


def fetch_results(ctx, job, capture=None, only_missing=None):
    """Capture a stopped host job via bounded binary streaming and a journal."""
    jid = job['id']
    with FileLock(ctx.p('locks', 'artifact-result-' + jid + '.lock'), 60):
        current = load_job(ctx, jid)
        if not current:
            return None
        record, done, clear = _artifact_callbacks(ctx, jid)
        if current.get('artifact_pending'):
            if not artifact_transfer.recover_pending(ctx.state, jid, current['artifact_pending'], done, clear):
                return None
            current = load_job(ctx, jid)
        policy = (artifact_transfer.validate_policy(current['artifact_policy']) if 'artifact_policy' in current else
                  {'max_file_bytes': artifact_transfer.LEGACY_FILE, 'total_bytes': artifact_transfer.DEFAULT_TOTAL})
        digests = current.get('artifact_digests') or []
        captured = {d['path'] for d in digests}
        used = sum(d['size_bytes'] for d in digests)
        # Older jobs recorded out_paths without digests. Count those files
        # against the aggregate limit without replacing them or claiming they
        # were verified against a remote sender. Unverifiable paths fail closed.
        recorded = {os.path.normcase(os.path.abspath(path)) for path in current.get('out_paths') or []}
        declared = {os.path.normcase(os.path.abspath(ctx.p('out', jid, *rel.split('/')))): rel
                    for rel in current.get('gets') or []}
        if recorded - declared.keys():
            return None
        opener = artifact_transfer.open_source_windows if os.name == 'nt' else artifact_transfer.open_source_posix
        for path in recorded:
            rel = declared[path]
            if rel in captured:
                continue
            try:
                source, before = opener(ctx.p('out', jid), rel)
                with source:
                    remaining = policy['total_bytes'] - used
                    if remaining < 0 or before.st_size > remaining:
                        return None
                    size, _ = artifact_transfer.hash_stream(source, remaining)
                    if artifact_transfer.source_identity(before) != artifact_transfer.source_identity(os.fstat(source.fileno())):
                        return None
                used += size
                captured.add(rel)
            except (OSError, ValueError):
                return None
        remaining = max(0, policy['total_bytes'] - used)
        wanted = [(i, rel) for i, rel in enumerate(current.get('gets') or [])
                  if rel not in captured and (only_missing is None or rel in only_missing)]
        sink = artifact_transfer.CaptureSink(ctx.state, jid, record, done, clear)
        try:
            tail = _stream_host_artifacts(ctx, current, wanted, policy['max_file_bytes'], remaining, sink)
        except ArtifactCommandTooLong:
            tail = ['[cq] ' + ARTIFACT_COMMAND_ERROR]
        if tail is None:
            return None
        latest = load_job(ctx, jid)
        if capture is not None:
            capture.extend(d for d in latest.get('artifact_digests') or [] if d['path'] not in captured)
        return tail, latest.get('out_paths') or []


class _DeadlinePipeReader:
    """Bound pipe reads even when an SSH helper retains an inherited handle."""
    def __init__(self, process, timeout):
        self.process = process
        self.deadline = time.monotonic() + timeout
        self.seen = 0
        self.errors = bytearray()
        self.buffer = bytearray()
        self.stderr_open = True
        self._peek = None
        try:
            self.out_fd = process.stdout.fileno()
            self.err_fd = process.stderr.fileno()
        except (AttributeError, OSError, ValueError):
            # An explicitly injected backend can supply synchronous streams.
            self.out_fd = self.err_fd = None
            self.errors.extend(process.stderr.read(8192))

    def _available_windows(self, fd):
        import ctypes
        import msvcrt
        from ctypes import wintypes
        if self._peek is None:
            api = ctypes.WinDLL('kernel32', use_last_error=True)
            self._peek = api.PeekNamedPipe
            self._peek.argtypes = [wintypes.HANDLE, wintypes.LPVOID, wintypes.DWORD,
                                   wintypes.LPVOID, ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID]
            self._peek.restype = wintypes.BOOL
        count = wintypes.DWORD()
        if not self._peek(msvcrt.get_osfhandle(fd), None, 0, None, ctypes.byref(count), None):
            error = ctypes.get_last_error()
            if error in (109, 232, 233):
                return -1
            raise ctypes.WinError(error)
        return count.value

    def _next(self, size):
        if self.out_fd is None:
            data = self.process.stdout.read(size)
            self.seen += len(data)
            return data
        while True:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('artifact stream deadline exceeded')
            if os.name == 'nt':
                out_avail = self._available_windows(self.out_fd)
                err_avail = self._available_windows(self.err_fd) if self.stderr_open else -1
                if err_avail > 0:
                    chunk = os.read(self.err_fd, min(err_avail, 4096))
                    self.errors.extend(chunk[:max(0, 8192 - len(self.errors))])
                elif err_avail < 0:
                    self.stderr_open = False
                if out_avail > 0:
                    data = os.read(self.out_fd, min(out_avail, size, 65536))
                    self.seen += len(data)
                    return data
                if out_avail < 0 or self.process.poll() is not None:
                    return b''
                if err_avail > 0:
                    continue
                time.sleep(min(0.02, remaining))
            else:
                fds = [self.out_fd] + ([self.err_fd] if self.stderr_open else [])
                ready, _, _ = select.select(fds, [], [], min(0.05, remaining))
                if self.err_fd in ready:
                    chunk = os.read(self.err_fd, 4096)
                    if chunk:
                        self.errors.extend(chunk[:max(0, 8192 - len(self.errors))])
                    else:
                        self.stderr_open = False
                if self.out_fd in ready:
                    data = os.read(self.out_fd, min(size, 65536))
                    self.seen += len(data)
                    return data
                if self.process.poll() is not None:
                    return b''

    def read(self, size):
        if self.buffer:
            data = bytes(self.buffer[:size])
            del self.buffer[:size]
            return data
        return self._next(size)

    def readline(self, size):
        while len(self.buffer) < size:
            index = self.buffer.find(b'\n')
            if index >= 0:
                return self.read(index + 1)
            data = self._next(min(65536, size - len(self.buffer)))
            if not data:
                break
            self.buffer.extend(data)
        index = self.buffer.find(b'\n')
        return self.read(index + 1 if index >= 0 else min(size, len(self.buffer))) if self.buffer else b''


def _stream_host_artifacts(ctx, job, wanted, maximum, remaining, sink):
    """Run a bounded framed reader over SSH; never buffer the response."""
    with open(os.path.join(os.path.dirname(__file__), 'artifact_transfer.py'), 'rb') as f:
        module = base64.b64encode(zlib.compress(f.read())).decode('ascii')
    config = base64.b64encode(json.dumps([job['id'], wanted, maximum, remaining], separators=(',', ':')).encode()).decode()
    program = '''import base64,json,os,stat,sys,zlib
exec(zlib.decompress(base64.b64decode(MODULE)))
jid,wanted,maximum,remaining=json.loads(base64.b64decode(CONFIG))
d=os.path.join(os.path.expanduser('~'),'q',jid)
tail=[]
try:
 fd=os.open(os.path.join(d,'run.log'),os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
 with os.fdopen(fd,'rb') as f:
  st=os.fstat(f.fileno())
  if not stat.S_ISREG(st.st_mode):raise OSError('invalid log')
  f.seek(max(0,st.st_size-131072));lines=f.read(131072).decode('utf-8','replace').splitlines()[-40:]
  tail=[s[:300] for s in lines]
except OSError:pass
sys.stdout.buffer.write(json.dumps({'tail':tail},ensure_ascii=True,separators=(',',':')).encode()+b'\\n')
send(os.path.join(d,'src'),wanted,maximum,remaining,sys.stdout.buffer)
'''.replace('MODULE', repr(module)).replace('CONFIG', repr(config))
    script = 'python3 -c ' + shlex.quote(program)
    binding = job.get('host_config') or {}
    if binding.get('machine_id'):
        script = ('[ "$(cat /etc/machine-id)" = ' + shlex.quote(binding['machine_id']) +
                  ' ] || exit 78\n' + script)
    timeout = min(3600, max(120, 120 + remaining // artifact_transfer.MIB))
    def run_one(host):
        process = reader = None
        result = None
        code = None
        try:
            argv = [SSH, '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'ConnectTimeout=10',
                    '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3', host,
                    'bash -c ' + shlex.quote(script)]
            if os.name == 'nt' and len(subprocess.list2cmdline(argv).encode('utf-16le')) // 2 + 1 > 30000:
                raise ArtifactCommandTooLong(ARTIFACT_COMMAND_ERROR)
            if ctx.artifact_stream is not None:
                process = ctx.artifact_stream(job, script, timeout)
            else:
                if ctx.ssh is not real_ssh or not _address(host):
                    return None, None, 0, ''
                kwargs = {'stdin': subprocess.DEVNULL, 'stdout': subprocess.PIPE, 'stderr': subprocess.PIPE}
                if os.name == 'nt':
                    kwargs['creationflags'] = CREATE_NO_WINDOW
                process = subprocess.Popen(argv, **kwargs)
            reader = _DeadlinePipeReader(process, timeout)
            header = artifact_transfer._header(reader, 160000)
            if set(header) != {'tail'} or not isinstance(header['tail'], list) or any(
                    not isinstance(line, str) or len(line) > 300 for line in header['tail']) or len(header['tail']) > 40:
                raise ValueError('invalid artifact log frame')
            artifact_transfer.receive(reader, wanted, maximum, remaining, sink)
            if not reader.read(1):
                left = max(0.0, reader.deadline - time.monotonic())
                try:
                    code = process.wait(timeout=left)
                except TypeError:  # synchronous injected backend
                    code = process.wait()
                if code == 0:
                    result = header['tail']
        except (OSError, ValueError, subprocess.SubprocessError):
            pass
        finally:
            if process is not None:
                if process.poll() is None:
                    process.kill()
                try:
                    code = process.wait(timeout=2)
                except TypeError:
                    code = process.wait()
                except subprocess.TimeoutExpired:
                    code = None
                process.stdout.close()
                process.stderr.close()
        return (result, code, reader.seen if reader is not None else 0,
                reader.errors.decode('utf-8', 'replace') if reader is not None else '')

    host = job.get('host', HOST)
    result, code, seen, err = run_one(host)
    fallback = binding.get('fallback_address')
    if (result is None and ctx.artifact_stream is None and code == 255 and not seen and fallback and fallback != host
            and any(marker in err for marker in SSH_TRANSIENT)):
        return run_one(fallback)[0]
    return result


def reap(ctx, snap):
    """Settle every job whose host or laptop run has ended; enforce timeouts; clean finished copies."""
    now = ctx.now()
    ended = []
    jobs = all_jobs(ctx)
    current_binding = host_binding()
    primary = (HOST, current_binding.get("machine_id", ""))
    snapshots = {primary: snap}
    bindings = {primary: current_binding}
    for job in jobs:
        if job.get("children") or job.get("status") not in ("launching", "running"):
            continue
        if job.get("placed_where") == "laptop":
            reap_laptop(ctx, job, now, ended)
        elif job.get("placed_where") == "host":
            address = job.get("host", HOST)
            binding = job.get("host_config") or (current_binding if address == HOST else {})
            identity = (address, binding.get("machine_id", ""))
            bindings.setdefault(identity, binding)
            if identity not in snapshots:
                snapshots[identity] = host_snapshot(ctx, host=address, binding=binding)[0]
            remote_snap = snapshots[identity]
            if job.get("defer") and remote_snap is not None:
                reap_deferred(ctx, job, remote_snap["jobs"].get(job["id"]), now, ended)
            elif job.get("temp_storage") == "ram":
                reap_ram(ctx, job, (remote_snap or {}).get("jobs", {}).get(job["id"]), now, ended)
            elif remote_snap is not None:
                reap_host(ctx, job, remote_snap["jobs"].get(job["id"]), now, ended)
    # parents whose children all ended (e.g. children finished while no parent roll-up ran)
    for job in all_jobs(ctx):
        if job.get("children") and job["status"] == "running":
            roll_up_parent(ctx, job["id"])
    cleanup_ram(ctx)
    for identity, remote_snap in snapshots.items():
        if remote_snap is not None:
            cleanup_host(ctx, remote_snap, now, host=identity[0], binding=bindings[identity], calls=1)
    return ended


def reap_host(ctx, job, ent, now, ended):
    jid = job["id"]
    if ent is None:
        if job.get('cancel_requested') or job.get('host_timeout_requested'):
            return  # no host receipt: do not infer that the session is empty
        if job["status"] == "launching":
            if launch_age(job, now) > LAUNCH_STALE and not job.get("launch_uncertain"):
                requeue_or_fail(ctx, jid, "launcher lost (no host dir after 10 min)", transient=True)
        else:
            missing = job.get("missing_since")
            if missing is None:
                with ctx.qlock():
                    j = load_job(ctx, jid)
                    if j and j["status"] == "running":
                        j["missing_since"] = now
                        save_job(ctx, j)
            elif now - missing > VANISH_SECONDS:
                finish(ctx, jid, "failed", None, reason="host job directory vanished")
                ended.append(jid)
        return
    if ent["exit"] is not None:
        if ent.get('alive') or (not job.get('defer') and not ent.get('pid')):
            if not job.get('defer'):
                started = ent.get('started') or ent.get('launched') or job.get('t_started') or job.get('t_launch')
                expired = started is not None and now - started > job['timeout_minutes'] * 60 + 60
                if job.get('cancel_requested') or expired:
                    with ctx.qlock():
                        current = load_job(ctx, jid)
                        if not current or current.get('status') in TERMINAL:
                            return
                        if expired and not current.get('cancel_requested'):
                            current['host_timeout_requested'] = True
                            save_job(ctx, current)
                    kill_running(ctx, current)
            return  # wait for a confirmed empty original host session
        digests = [] if job.get("defer") else None
        res = fetch_results(ctx, job, capture=digests) if digests is not None else fetch_results(ctx, job)
        if res is None:
            return  # host hiccup: try again next tick
        tail, paths = res
        missing = [{"path": rel, "reason": _artifact_failure_reason(
                    tail, 'artifact missing, unsafe, changed or exceeds quota')}
                   for rel in (job.get("gets") or [])
                   if ctx.p("out", job["id"], *rel.split("/")) not in paths]
        t0 = ent["started"] or ent["launched"]
        elapsed_ok = bool(t0 and ent["finished"] and (ent["finished"] - t0) >= job["timeout_minutes"] * 60 - 30)
        code = ent["exit"]
        status = ("cancelled" if job.get("cancel_requested") and not job.get("defer") else
                  "timeout" if job.get('host_timeout_requested') else
                  "done" if code == 0 else ("timeout" if code in (124, 137) and elapsed_ok else "failed"))
        if job.get("defer") and job.get("deferred_phase") == "failed":
            status = "failed"
        reason = {"timeout": f"timeout after {job['timeout_minutes']} min"}.get(status, "")
        if code == 75:
            reason = "host disk low (lanerun exit 75)"
        finish(ctx, jid, status, code, reason=reason, tail=tail, out_paths=paths, started=ent["started"] or None,
               finished=now if job.get('host_timeout_requested') else ent["finished"] or None,
               cpu_s=ent.get("cpu"), artifact_missing=missing, artifact_digests=digests)
        ended.append(jid)
        return
    if ent["alive"]:
        if job.get("cancel_requested"):
            kill_running(ctx, job)
            return
        if job.get("status") == "launching":
            _launch_result(ctx, jid, "running", t_started=ent["started"] or ent["launched"] or now,
                           reason="", host_dir=f"~/q/{jid}", log_path=f"{job.get('host', HOST)}:~/q/{jid}/run.log")
        if ent["started"] and now - ent["started"] > job["timeout_minutes"] * 60 + 60:  # the host `timeout` should have fired
            with ctx.qlock():
                current = load_job(ctx, jid)
                if not current or current.get('status') in TERMINAL:
                    return
                current['host_timeout_requested'] = True
                current['reason'] = 'timeout stop pending host session shutdown'
                save_job(ctx, current)
            kill_running(ctx, current)
        return
    # no exit file and not alive: died without writing one, or run.sh has not started yet
    if not job.get('defer') and (job.get('cancel_requested') or job.get('host_timeout_requested')):
        if not ent.get('pid'):
            return  # an absent session receipt cannot prove shutdown
        digests = []
        res = fetch_results(ctx, job, capture=digests)
        if res is None:
            return
        tail, paths = res
        missing = [{'path': rel, 'reason': _artifact_failure_reason(
                    tail, 'artifact missing, unsafe, changed or exceeds quota')}
                   for rel in (job.get('gets') or [])
                   if ctx.p('out', jid, *rel.split('/')) not in paths]
        status = 'cancelled' if job.get('cancel_requested') else 'timeout'
        reason = 'cancelled' if status == 'cancelled' else f"killed after {job['timeout_minutes']} min"
        finish(ctx, jid, status, None, reason=reason, tail=tail, out_paths=paths,
               started=ent.get('started'), finished=now, artifact_missing=missing, artifact_digests=digests,
               host_empty_confirmed=True)
        ended.append(jid)
        return
    if ent["launched"] is None:
        launching = job["status"] == "launching"  # its launcher may still be pushing: the 10 min of LAUNCH_STALE, not 2
        if launching and job.get("launch_uncertain"):
            return
        if launch_age(job, now) <= (LAUNCH_STALE if launching else 120):
            return
        if launching:
            requeue_or_fail(ctx, jid, "launcher lost (the host dir never started run.sh in 10 min)", transient=True)
            return
    finish(ctx, jid, "cancelled" if job.get("cancel_requested") else "failed", None,
           reason="cancelled" if job.get("cancel_requested") else "run.sh died without an exit file")
    ended.append(jid)


def reap_laptop(ctx, job, now, ended):
    run_dir = ctx.p("run", job["id"])
    pid = read_pid(os.path.join(run_dir, "pid"))
    alive = ctx.laptop.pid_alive(pid) if pid else False  # before the exit file: a wrapper that ends in between has written it
    code = read_pid(os.path.join(run_dir, "exit"))
    if code is None and os.path.exists(os.path.join(run_dir, "exit")):
        return
    if code is not None:
        if alive or (job.get('gets') and not pid):
            return  # wait for the producing wrapper to exit before capture
        started = read_pid(os.path.join(run_dir, "started"))
        finished = read_pid(os.path.join(run_dir, "finished"))
        status = "done" if code == 0 else ("timeout" if code == 124 else "failed")
        try:
            paths, digests, missing = capture_local_job(ctx, job, os.path.join(run_dir, 'src'))
        except (OSError, ValueError):
            return  # reconcile any durable pending publication on the next tick
        tail = [l for l in read_lines(os.path.join(run_dir, "run.log"), TAIL_LINES)]
        finish(ctx, job["id"], status, code, reason=f"timeout after {job['timeout_minutes']} min" if status == "timeout" else "",
               tail=tail, out_paths=paths, started=started, finished=finished,
               artifact_missing=missing, artifact_digests=digests)
        if not missing:
            with contextlib.suppress(OSError):
                rmtree_force(os.path.join(run_dir, "src"))
        ended.append(job["id"])
        return
    started = read_pid(os.path.join(run_dir, "started"))
    if alive and started and now - started > job["timeout_minutes"] * 60 + 60:
        ctx.laptop.kill_tree(pid)
        finish(ctx, job["id"], "timeout", None, reason=f"killed after {job['timeout_minutes']} min", started=started)
        ended.append(job["id"])
    elif not alive and (pid or launch_age(job, now) > (LAUNCH_STALE if job["status"] == "launching" else 120)):
        finish(ctx, job["id"], "failed", None, reason="laptop wrapper died without an exit file",
               tail=read_lines(os.path.join(run_dir, "run.log"), TAIL_LINES))
        ended.append(job["id"])


def cleanup_ram(ctx):
    """Acknowledge final RAM results even when the currently selected host's probe is down."""
    for job in all_jobs(ctx):
        if (job.get("temp_storage") != "ram" or job.get("status") not in TERMINAL or job.get("ram_cleaned")
                or job.get('artifact_missing') or job.get('artifact_pending')):
            continue
        if job.get("placed_where") != "host":
            continue  # a cancelled queued job never acquired remote resources
        status = ram_control(ctx, job, "cleanup")
        if status is not None and status.get("empty") is True:
            with ctx.qlock():
                current = load_job(ctx, job["id"])
                if current:
                    current["ram_cleaned"] = True
                    save_job(ctx, current)


def ramtmp_rm(name, tmp_root=None):
    """Shell removing a killed job's tmp_root dir (run.sh removes it when the job ends). The job could have rewritten
    the file, so only <tmp_root>/<8 letters or digits> (mktemp's form) is removed; nothing without a tmp_root."""
    tmp_root = TMP_ROOT if tmp_root is None else tmp_root
    if not tmp_root:
        return ""
    alnum = "[A-Za-z0-9]" * 8
    return (f'p=$(cat "$HOME/q/{name}/ramtmp" 2>/dev/null) && case "$p" in '
            f'{shlex.quote(tmp_root)}/{alnum}) rm -rf "$p";; esac; ')


CLEANUP_SCRIPT_CHARS = 12000  # of one cleanup command: ssh takes the script as an argument, Windows allows a command
#                               line 32,767 characters and its quoting doubles some of them
ARTIFACT_KEEP_S = 7 * 86400  # a finished job with an artifact still missing keeps its source copy on the host this long
CLEANUP_CALLS = 4  # cleanup commands per call of cleanup_host at most; what is left waits for the next one. The leader's
#                    tick sends one (as it always did): it waits for each, and its lease is 45 s


def script_chunks(lines, limit):
    """lines in order, grouped so that each group joined by newlines stays within limit (a longer line goes alone)."""
    chunk, size = [], 0
    for line in lines:
        if chunk and size + len(line) + 1 > limit:
            yield chunk
            chunk, size = [], 0
        chunk.append(line)
        size += len(line) + 1
    if chunk:
        yield chunk


def cleanup_host(ctx, snap, now, host=None, binding=None, calls=CLEANUP_CALLS, report=None):
    """Clean only known completed jobs; unknown remote directories remain owned elsewhere. The work goes to the host
    in commands of bounded length, at most `calls` of them: one command for all of it stopped working for good once
    its length passed what a command line holds (about 110 finished jobs), and every later attempt was longer. Stops
    and source copies come before week-old directories, which hold little. Returns the number of commands sent;
    report, a dict, gets `left`: the commands not sent or not taken, so 0 means the host has the whole list."""
    script, old = [], []
    for name, ent in snap["jobs"].items():
        try:
            job = load_job(ctx, name)
        except CqError:
            continue  # one record this store cannot read does not stop the cleanup of the others
        if not isinstance(job, dict):
            continue  # another generation/controller may retain its only artifact copy
        machine = (binding or {}).get("machine_id")
        if (machine and (job.get("host_config") or {}).get("machine_id") != machine
                and any(isinstance(r, dict) and r.get("machine_id") == machine and not r.get("removed")
                        and r.get("address") in (None, host or HOST) for r in job.get("recalled_from") or [])):
            # a fleet job recalled from this machine before it started (fleet_queue._requeue_recalled): it runs, or
            # waits, elsewhere, so the directory it left here is of no use, and no rule below would ever take it (the
            # record is pinned to another host). Never while the record is on this machine, and not once the entry
            # is marked removed: from then on the job may be staged here again under the same ID.
            if not ent["alive"]:
                script.append(f'rm -rf "$HOME/q/{name}"')
            continue
        if job and job.get("host", HOST) != (host or HOST):
            continue
        if (job and (job.get("host_config") or {}).get("machine_id")
                and (job.get("host_config") or {}).get("machine_id") != (binding or {}).get("machine_id")):
            continue
        if job and job.get("temp_storage") == "ram":
            if job.get("ram_cleaned") and ent["src"]:
                script.append(f'rm -rf "$HOME/q/{name}/src"')
            continue
        if job.get('artifact_pending'):
            continue  # a fetch is under way
        if job.get("artifact_missing"):
            # the remote source is the only remaining copy of unfetched artifacts: kept ARTIFACT_KEEP_S from the job's
            # end for a refresh to fetch them, then cleaned like any other (with no limit, 82 fleet jobs in two days
            # kept a source copy each, most of them failed runs that never wrote the file)
            ended = job.get("t_finished") or ent["finished"]
            if not isinstance(ended, (int, float)) or isinstance(ended, bool) or now - ended <= ARTIFACT_KEEP_S:
                continue
        if job.get("status") not in TERMINAL:
            continue
        if ent["alive"]:
            # any terminal status: nothing legitimate runs in a finished job's session, and a lingering child can hold a
            # .heavy.i flock for days
            script.append(host_kill_script(name, grace=2))
        elif ent["src"] and not ent["alive"]:
            tmp_root = (job.get("host_config") or {}).get("tmp_root")
            script.append(ramtmp_rm(name, tmp_root) + f'rm -rf "$HOME/q/{name}/src" "$HOME/q/{name}/tmp"')
        elif ent["finished"] and now - ent["finished"] > 7 * 86400:
            tmp_root = (job.get("host_config") or {}).get("tmp_root")
            old.append(ramtmp_rm(name, tmp_root) + f'rm -rf "$HOME/q/{name}"')
    chunks = list(script_chunks(script + old, CLEANUP_SCRIPT_CHARS))
    sent = taken = 0
    for chunk in chunks[:max(0, calls)]:
        sent += 1
        rc = job_ssh(ctx, {"host": host or HOST, "host_config": binding or {}}, "\n".join(chunk) + "\n", timeout=120)[0]
        if rc != 0:
            break  # the host did not take it (or the removal outlasted the wait): the next call lists what is left
        taken += 1
    if report is not None:
        report["left"] = len(chunks) - taken
    return sent


def cmd_reap(ctx, req):
    snap, err = host_snapshot(ctx)
    ended = reap(ctx, snap)
    return {"ok": True, "host_up": snap is not None, "ended": ended}


# --------------------------------------------------------------------------- tick, watch, status

def history_medians(ctx, jobs):
    wanted = {(history_kind(j["command"]), j["workstream"]) for j in jobs}
    rows = {}
    for line in read_lines(ctx.p("history.jsonl"), 500):
        try:
            h = json.loads(line)
        except json.JSONDecodeError:
            continue
        key = (h.get("kind"), h.get("workstream"))
        if key in wanted and h.get("status") == "done":
            rows.setdefault(key, []).append(h.get("actual", 0))
    return [{"kind": k, "workstream": w, "median_min": round(statistics.median(v), 1), "n": len(v)}
            for (k, w), v in sorted(rows.items())][:12]


NOTICE_TEXT_SUFFIXES = (".txt", ".tsv", ".csv", ".md", ".log", ".json")
NOTICE_TEXT_FILE_BYTES = 4096   # a larger artifact is named in the notice, not shown
NOTICE_TEXT_LINES = 30          # across all the artifacts of one job
NOTICE_TEXT_LINE = 200
_NOTICE_CONTROL = re.compile("[\x00-\x1f\x7f-\x9f\u2028\u2029\u202a-\u202e\u2066-\u2069]")   # C0, C1, line/paragraph separators, bidi controls


def artifact_text(job):
    """The small text artifacts a finished job delivered, as [{"path", "lines", "cut"}]: the notice then carries the
    result itself (a summary, a results table) and nobody opens the files in a shell to learn it. Summaries first;
    bounded per file and in all; control characters made spaces, as in the log tail. Never raises."""
    shown, room = [], NOTICE_TEXT_LINES
    paths = [p for p in job.get("out_paths") or [] if isinstance(p, str) and p.lower().endswith(NOTICE_TEXT_SUFFIXES)]
    digests = [d for d in job.get("artifact_digests") or [] if isinstance(d, dict) and isinstance(d.get("path"), str)]
    paths.sort(key=lambda p: not os.path.basename(p).lower().startswith("summary"))   # stable: summaries, then the rest
    for path in paths:
        if room <= 0:
            break
        try:
            if os.path.islink(path) or not os.path.isfile(path) or os.path.getsize(path) > NOTICE_TEXT_FILE_BYTES:
                continue
            with open(path, "rb") as fh:
                raw = fh.read(NOTICE_TEXT_FILE_BYTES + 1)
        except OSError:
            continue
        if len(raw) > NOTICE_TEXT_FILE_BYTES or b"\x00" in raw:
            continue
        # The notice calls the fetched files verified: what is quoted must be those bytes, not a later edit.
        local = path.replace("\\", "/")
        recorded = next((d for d in digests if local.endswith("/" + d["path"].replace("\\", "/").lstrip("/"))), None)
        if recorded is not None and recorded.get("sha256") != hashlib.sha256(raw).hexdigest():
            continue
        lines = [_NOTICE_CONTROL.sub(" ", ln)[:NOTICE_TEXT_LINE].rstrip()
                 for ln in raw.decode("utf-8", errors="replace").splitlines()]
        while lines and not lines[-1]:
            lines.pop()
        if lines:
            shown.append({"path": path, "lines": lines[:room], "cut": len(lines) > room})
            room -= len(lines[:room])
    return shown


def notice_data(job):
    data = {k: job.get(k) for k in ("id", "label", "status", "exit", "minutes", "placed_where", "slots_eff", "slots", "log_path",
                                    "out_paths", "tail", "parts_info", "reason", "workstream", "finished", "host_id", "host", "artifact_missing", "artifact_digests", "tail_digest")}
    if job.get("status") in TERMINAL:
        text = artifact_text(job)
        if text:
            data["artifact_text"] = text
    return data


def result_view(job):
    """A finished job as a tool answer: the notice's facts without the digests (their count says they were checked)
    and without empty fields. `status` with full: true still returns the stored record."""
    data = notice_data(job)
    digests = data.pop("artifact_digests", None) or []
    data.pop("tail_digest", None)
    if digests:
        data["artifacts_verified"] = len(digests)
    data["staging_stderr"] = job.get("staging_stderr")
    return {k: v for k, v in data.items() if v not in (None, "", [], {})}


def job_brief(job, now):
    """One job as a tool answer: the result when it has ended, else where it stands. No host configuration, keys or
    policy blocks: those are in the stored record (`full: true`)."""
    if job.get("status") in TERMINAL:
        return result_view(job)
    row = job_summary(job, now) | {k: job.get(k) for k in ("command", "sha", "base", "source", "submitted", "gets",
                                                           "timeout_minutes", "attempts", "children", "candidates",
                                                           "placement_reasons", "notice_hold_reason", "staging_stderr")}
    return {k: v for k, v in row.items() if v is not None and v is not False and v not in ("", [], {})}


def mark_delivered(ctx, job_id, seen_by, via):
    """The result of `job_id` has just reached its owner another way (`via`: a wait or status answer): mark it
    notified, so the completion notice does not repeat it. Only for the session that would get the notice: the
    worktree's current owner, for a job of that worktree. True when this call made the mark. Never raises, and
    never waits on the queue lock for a job that needs no mark: the answer it rides on must not fail for it."""
    try:
        return _mark_delivered(ctx, job_id, seen_by, via)
    except (CqError, OSError):
        return False


def _mark_delivered(ctx, job_id, seen_by, via):
    if not isinstance(seen_by, dict):
        return False
    seen = load_job(ctx, job_id)
    if not seen or seen.get("notified") or not seen.get("notify", True) or seen.get("status") not in TERMINAL:
        return False
    root, session = seen_by.get("root") or "", seen_by.get("session") or ""
    if not isinstance(root, str) or not isinstance(session, str) or not root or not session:
        return False
    top = _top_of(root) if os.path.isdir(root) else root
    vendor = seen_by.get("vendor") or "claude"
    if not notice_owner_matches(top, session, vendor):
        return False
    with ctx.qlock():
        job = load_job(ctx, job_id)
        if (not job or job.get("parent") or job.get("status") not in TERMINAL or job.get("notified")
                or not job.get("notify", True) or not notice_matches_worktree(job, top, worktree_identity(top), session)):
            return False
        job["notified"] = True
        job["notified_via"] = via
        save_job(ctx, job)
    return True


def parts_now(job, cap):
    """The parts a split job would get if it started alone now (0: nothing fits); shown to Haiku, set by the guard."""
    free, light = host_free(cap), _light_room(cap)
    places = _fits(job, cap, free, _laptop_heavy_free(cap), light)
    return split_parts(job, places[0], free, light[places[0]]) if places else 0


def sample_due(ctx, now):
    """True when no row was logged within SAMPLE_LOG_EVERY seconds (the samples.at sidecar), or the sidecar does not parse."""
    try:
        with open(ctx.p("samples.at"), encoding="utf-8") as fh:
            return abs(now - float(fh.read().strip())) >= SAMPLE_LOG_EVERY
    except (OSError, ValueError):
        return True


def log_sample(ctx, now, cap, queued, holding, can, host_res):
    """One row of samples.jsonl at most every SAMPLE_LOG_EVERY seconds: the capacity and the queue state, so that waits the
    plan log cannot show (free slots and queued jobs, with nothing started and no plan made) can be measured. Never raises."""
    try:
        path, at_path = ctx.p("samples.jsonl"), ctx.p("samples.at")
        if not sample_due(ctx, now):
            return
        try:
            with contextlib.suppress(OSError):
                if os.path.getsize(path) > SAMPLE_LOG_MAX_BYTES:
                    os.replace(path, path + ".1")
            h, lp = cap.get("host") or {}, cap.get("laptop") or {}
            try:
                load1 = float(str(h.get("load") or "").split()[0])
            except (IndexError, ValueError):
                load1 = None
            free = int(h.get("free") or 0)
            head = host_res["job"] if host_res else None
            cpu, psi = h.get("cpu") or {}, h.get("psi") or {}
            runq, top, job_cpu, bench = h.get("runq"), h.get("top"), h.get("job_cpu"), h.get("bench") or {}
            procs = _laptop_call(ctx.laptop, "top_procs", 6)  # the two laptop scans run only on a logged row
            pool = _laptop_call(ctx.laptop, "pool_top", 3)
            if host_res is None:
                head_state = None
            elif not host_res["blocked"]:
                head_state = "fits"
            elif free >= head["slots"]:
                head_state = "caps"  # the slots would fit; the measured caps do not
            else:
                head_state = "slots"
            append_line(path, {"t": iso(now), "at": now, "load1": load1, "mem_avail_gb": h.get("mem_avail_gb"),
                               "held": h.get("held"), "pending": h.get("pending"), "free": free, "room": host_free(cap),
                               "nproc": h.get("nproc"), "queued": len(queued),
                               "queued_heavy": sum(j["slots"] >= 1 for j in queued),
                               "waiting_fit": sum(1 for j in queued if j["slots"] >= 1 and j["where"] in ("host", "any")
                                                  and j["slots"] <= free),
                               "running": len(holding), "can_start": bool(can), "head": head["id"] if head else None,
                               "head_slots": head["slots"] if head else None, "head_state": head_state,
                               "laptop_free_ram_gb": lp.get("free_ram_gb"),
                               "laptop_commit_gb": lp.get("commit_gb"), "laptop_commit_limit_gb": lp.get("commit_limit_gb"),
                               "laptop_np_gb": lp.get("np_gb"), "laptop_paged_gb": lp.get("paged_gb"), "laptop_cache_gb": lp.get("cache_gb"),
                               "laptop_top": None if procs is None else [[g["name"], g["n"], g["ws_mb"], g["priv_mb"]] for g in procs["groups"]],
                               "laptop_unreadable": None if procs is None else procs["unreadable"],
                               "laptop_pool": None if pool is None else [[t["tag"], t["mb"], t["outstanding"]] for t in pool],
                               "cpu_busy": cpu.get("busy"), "cpu_user": cpu.get("user"), "cpu_nice": cpu.get("nice"),
                               "cpu_sys": cpu.get("sys"), "cpu_iowait": cpu.get("iowait"), "cpu_steal": cpu.get("steal"),
                               "cpu_idle": cpu.get("idle"), "runq": runq[0] if runq else None,
                               "psi_cpu10": (psi.get("cpu") or {}).get("some10"), "psi_cpu60": (psi.get("cpu") or {}).get("some60"),
                               "psi_mem10": (psi.get("memory") or {}).get("some10"), "psi_io10": (psi.get("io") or {}).get("some10"),
                               "top": [[c["name"], c["cores"]] for c in top[:8]] if top is not None else None,
                               "job_cores": {k: v["cores"] for k, v in job_cpu.items()} if job_cpu is not None else None,
                               "bench_real_ms": bench.get("real_ms"), "bench_cpu_ms": bench.get("cpu_ms")})
        finally:  # written even when the row fails: a failing tick must not profile again at every tick
            atomic_write(at_path, repr(float(now)).encode())
    except Exception:  # a logging failure must not break the tick
        pass


def cmd_tick(ctx, req):
    session, root = req.get("session") or "", req.get("root") or ""
    with contextlib.suppress(OSError, CqError):
        ensure_deferred_controller(ctx)
    now = ctx.now()
    lead = leader_claim(ctx, session, root)
    out = {"ok": True, "leader": lead["leader"], "took_over": lead.get("took_over", False), "now": now, "planner": PLANNER}
    ended = []
    if lead["leader"]:
        snap, err, attempted = take_snapshot(ctx, profile=sample_due(ctx, now))
        leader_heartbeat(ctx, session)
        ended = reap(ctx, snap)
        leader_heartbeat(ctx, session)
        out["capacity"] = probe(ctx, snap, err, attempted)["capacity"]
        leader_heartbeat(ctx, session)
        queued = [j for j in queued_jobs(ctx, now) if not j.get("defer")]
        running = [j for j in all_jobs(ctx) if j.get("status") == "running" and not j.get("children")]
        holding = running_jobs(ctx)
        cap = adjust_for_launches(ctx, out["capacity"])
        out["capacity"]["host"]["room"] = host_free(cap)  # slots a heavy job may take now, caps included (shown to Haiku)
        can = fallback_entries(queued, cap, holding, now)
        host = reservations(queued, cap, holding, now)["host"]
        log_sample(ctx, now, cap, queued, holding, can, host)
        rows = [job_summary(j, now) for j in queued]
        for j, row in zip(queued, rows):
            if j.get("split"):
                row["parts_now"] = parts_now(j, cap)
            if host and j["id"] != host["job"]["id"] and j["where"] in ("host", "any") and can_backfill(j, host):
                row["backfill_ok"] = True
        out.update(queued=rows, running=[job_summary(j, now) for j in running],
                   can_start=bool(can), medians=history_medians(ctx, queued + running), ended=ended)
        with contextlib.suppress(Exception):  # housekeeping: a failed sweep must not fail the tick
            archived = archive_jobs(ctx)
            if archived:
                out["archived"] = archived
    else:
        cap = load_capacity(ctx) or {}
        out["capacity"] = cap
        out["can_start"] = False
    alert = host_state(ctx).get("disk_alert")  # every session reads it: the lead's session may not be the leader
    out["alert"] = alert if alert and not alert.get("acked") else None
    out["watch"] = watch(ctx, root, now, req.get("watch_session") or session)
    return out


def notice_owner_matches(top, session, vendor="claude"):
    """A notice belongs to the currently claimed session, not merely its worktree."""
    if not top or not session:
        return False
    owner = read_json(os.path.join(top, ".baton", "owner.json"))
    return isinstance(owner, dict) and owner.get("vendor") == vendor and owner.get("session_id") == session


def attention_counts(jobs):
    """Finished jobs of one worktree that still wait for their notice and went wrong: failed or timed out, or done
    with missing artifacts. A delivered notice clears them; nothing here is a lifetime total."""
    failed = sum(1 for j in jobs if j.get("status") in ("failed", "timeout"))
    missing = sum(1 for j in jobs if j.get("status") not in ("failed", "timeout") and j.get("artifact_missing"))
    return {"failed": failed, "missing": missing}


def watch(ctx, root, now, session=""):
    """What one session sees: its own jobs' counts and the unnotified finished ones; the queue totals."""
    top = _top_of(root) if root and os.path.isdir(root) else root
    jobs = all_jobs(ctx)
    identity = worktree_identity(top) if top else None
    mine = [j for j in jobs if not j.get("parent") and identity is not None and notice_matches_worktree(j, top, identity, session)]
    # Legacy active jobs still use resources after a handoff. Include them in
    # activity counts, but never treat their completion as safe to deliver.
    uncertain = [j for j in jobs if not j.get("parent") and identity is not None and
                 j.get("worktree_identity") is None and j.get("session") != session and
                 same_path(j.get("top", ""), top) and j.get("status") in ("running", "launching", "queued")]
    pending = [j for j in mine if j.get("status") in TERMINAL and j.get("notify", True) and not j.get("notified")]
    done = [notice_data(j) for j in pending]
    # A predecessor's timer can finish a tick during any handoff, including
    # Claude-to-Claude. Never offer its result to a non-owner or unknown owner.
    if not notice_owner_matches(top, session):
        done, pending = [], []
    queued = [j for j in jobs if j.get("status") == "queued" and not j.get("parent")]
    return {"mine_running": sum(1 for j in mine + uncertain if j["status"] in ("running", "launching")),
            "mine_queued": sum(1 for j in mine + uncertain if j["status"] == "queued"),
            "mine_unverified": len(uncertain),
            "queued": len(queued), "to_notify": done, "top": top, "mine_attention": attention_counts(pending)}


def cmd_watch(ctx, req):
    return {"ok": True, "watch": watch(ctx, req.get("root", ""), ctx.now(), req.get("session") or ""), "capacity": load_capacity(ctx)}


def cmd_notice_authorize(ctx, req):
    """Recheck ownership and pending jobs immediately before a Claude prompt."""
    top, session = req.get("root") or "", req.get("session") or ""
    if not notice_owner_matches(top, session):
        return {"ok": False, "error": "notice owner changed"}
    wanted = req.get("ids") or []
    if not isinstance(wanted, list) or len(wanted) > 5 or any(
            not isinstance(jid, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", jid)
            for jid in wanted):
        return {"ok": False, "error": "invalid notice ids"}
    with ctx.qlock():
        if not notice_owner_matches(top, session):
            return {"ok": False, "error": "notice owner changed"}
        identity = worktree_identity(top)
        jobs = []
        for jid in wanted:
            j = load_job(ctx, jid)
            if (j and not j.get("parent") and j.get("status") in TERMINAL and j.get("notify", True)
                    and not j.get("notified") and identity is not None and notice_matches_worktree(j, top, identity, session)):
                jobs.append(notice_data(j))
        return {"ok": True, "jobs": jobs}


def _or_none(fn, *args):
    """fn(*args), or None when it raises: a status answer must not fail on one malformed field."""
    try:
        return fn(*args)
    except Exception:
        return None


def _paused_text(h):
    """The status words of a paused host, naming the config pause when the probe recorded it ("disk" otherwise)."""
    return "paused (host.conf.json)" if h.get("pause") == "host.conf.json" else "paused (disk)"


def _host_block(job, cap, res):
    """Why the host does not take the queued `job` now, by the planner's own checks (host_light_ok, cap_clip_reason, _clip);
    None: it does."""
    h = cap.get("host") or {}
    if not h.get("up"):
        return f"host down: {h.get('down_reason') or 'no sample'}"
    if not h.get("usable"):
        return f"host {_paused_text(h)}"
    if job["slots"] < 1:
        if not host_light_ok(h):
            return _light_reason(h)
        if _light_room(cap)["host"] < 1:
            return f"light jobs at the cap on the host ({LIGHT_CAP} running)"
    else:
        free = int(h.get("free") or 0)
        if free < job["slots"]:
            return f"needs {job['slots']} slots, {free} free"
        r = (res or {}).get("host") or {}
        if not (r.get("override") and r["job"]["id"] == job["id"]):  # the head job the caps give way to starts anyway
            why = cap_clip_reason(h, job["slots"])
            if why:
                return why
    return _clip(job, "host", res)


def _laptop_block(job, cap, res):
    """Why the laptop does not take the queued `job` now, by the planner's own rules; None: it does."""
    if _laptop_off(cap):
        return f"the laptop is {LAPTOP_NOT_EXECUTOR}; this job can only wait for a remote host (resubmit with where 'host' or 'fleet')"
    if job["slots"] > 1:  # _fits: the laptop runs light and 1-slot jobs only
        return "the laptop takes only light and 1-slot jobs"
    if job["slots"] < 1:
        if _light_room(cap)["laptop"] < 1:
            return f"light jobs at the cap on the laptop ({LIGHT_CAP} running)"
    elif not _laptop_heavy_free(cap):
        lp = cap.get("laptop") or {}
        if lp.get("heavy_busy"):
            return "laptop heavy lock busy"
        return f"laptop free RAM {lp.get('free_ram_gb', 0):g} GB < {LAPTOP_RAM_MIN_GB:g} GB"
    return _clip(job, "laptop", res)


def wait_reason(job, cap, res, now, started=None):
    """Why a queued job does not start now, or that it fits: the host's reason, and for where 'any' the laptop's too. An
    'any' job waits only when both machines refuse it. `res` is reservations() of the same pass, made at `now`. `started`: the
    job ids the planner starts in that pass (fallback_entries); a job it does not start waits for the jobs ahead of it, even
    when no helper names a block. None: the helpers alone decide."""
    where = job["where"]
    if where == "host":
        why = _host_block(job, cap, res)
    elif where == "laptop":
        why = _laptop_block(job, cap, res)
    elif _laptop_off(cap):  # 'any' on a controller that runs nothing itself waits for the host alone
        why = _host_block(job, cap, res)
    else:
        host, lap = _host_block(job, cap, res), _laptop_block(job, cap, res)
        why = f"{host} | laptop: {lap}" if host and lap else None
    if started is not None:
        if job["id"] in started:
            return "fits now: starts at the next planning tick"
        return why or "waits for the jobs ahead of it: their slots are taken first"
    return why or "fits now: starts at the next planning tick"


def _planned_starts(ctx, cap, queued, now):
    """The ids fallback_entries starts in one planning pass over `queued` with the running jobs of the host."""
    return {e.get("job") for e in fallback_entries(queued, cap, running_jobs(ctx), now)}


def _n(x):
    return f"{round(x, 1):g}"


def _host_line(h):
    if not h:
        return "no sample"
    if not h.get("up"):
        return f"host down: {h.get('down_reason') or 'no sample'}"
    parts = []
    if not h.get("usable"):
        parts.append(_paused_text(h))
    if h.get("held") is not None:
        parts.append(f"{h['held']}/{h['total']} slots held" if h.get("total") else f"{h['held']} slots held")
    if h.get("stale"):
        parts.append(f"sample {h.get('sample_age_s')} s old")
    try:
        load1 = float(str(h["load"]).split()[0])
        cap_load = LOAD_CAP or h.get("nproc")
        parts.append(f"load {_n(load1)}" + (f" (cap {_n(cap_load)})" if cap_load else ""))
    except (KeyError, IndexError, TypeError, ValueError):
        pass
    cpu = h.get("cpu")
    if isinstance(cpu, dict) and cpu.get("busy") is not None:
        parts.append(f"cpu {_n(cpu['busy'])}% busy (sys {_n(cpu['sys'])}, iowait {_n(cpu['iowait'])}, idle {_n(cpu['idle'])})")
    if h.get("runq"):
        parts.append(f"runq {h['runq'][0]}")
    psi = (h.get("psi") or {}).get("cpu") or {}
    if psi.get("some10") is not None:
        parts.append(f"psi cpu {_n(psi['some10'])}")
    if h.get("top"):
        text = "top: " + ", ".join(f"{t['name']} {_n(t['cores'])}" for t in h["top"][:3]) + " cores"
        age = h.get("prof_age_s")
        if _is_num(age) and age > PROF_SHOWN_AGE:
            text += f" (as of {round(age)}s ago)"
        parts.append(text)
    return ", ".join(parts) or "no sample"


def _laptop_line(lp):
    if lp.get("executor") is False:
        return LAPTOP_NOT_EXECUTOR
    parts = []
    if lp.get("free_ram_gb") is not None:
        parts.append(f"{_n(lp['free_ram_gb'])} GB free (heavy jobs need {LAPTOP_RAM_MIN_GB:g})")
    if lp.get("commit_gb") is not None and lp.get("commit_limit_gb") is not None:
        parts.append(f"commit {_n(lp['commit_gb'])}/{_n(lp['commit_limit_gb'])} GB")
    if lp.get("np_gb") is not None:
        parts.append(f"nonpaged {_n(lp['np_gb'])} GB")
    if lp.get("heavy_busy"):
        parts.append("heavy lock busy")
    return ", ".join(parts) or "no sample"


def capacity_summary(cap):
    """One line each for the host and the laptop, from the capacity sample (no ssh): only the parts it holds."""
    cap = cap or {}
    return {"host": _host_line(cap.get("host") or {}), "laptop": _laptop_line(cap.get("laptop") or {})}


def _plan_state(ctx, cap, queued, now):
    """The capacity as the tick plans with it, and the reservations of `queued` (None when they cannot be made)."""
    try:
        cap = adjust_for_launches(ctx, cap or {})  # no sample: the host is down, as the tick reads it
        return cap, reservations(queued, cap, running_jobs(ctx), now)
    except Exception:
        return cap, None


def refresh_deferred(ctx, job):
    """Refresh only this job on its original host, without staging or registering it."""
    if not job.get("defer"):
        raise CqError("refresh is available only for deferred jobs")
    if not job.get("host_config"):
        return {"ok": False, "cached": True, "error": "job has not yet been staged to a host"}
    state = deferred_control(ctx, job, "status")
    if state.get("error"):
        return {"ok": False, "cached": True, "error": state["error"]}
    jid = shlex.quote(job["id"])
    script = (f'd="$HOME/q/"{jid}\n'
              'if [ -d "$d" ]; then\n'
              'rv() { v=""; [ ! -f "$d/$1" ] || read -r v < "$d/$1" || :; };\n'
              'rv exit; ex=$v; rv started; st=$v; rv launched; la=$v; rv finished; fin=$v; rv cpu; cp=$v; rv pid; pid=$v;\n'
              f'echo "CQJOB {job["id"]} exit=$ex started=$st launched=$la finished=$fin cpu=$cp pid=$pid alive=0 src=1 now=0"; fi\n'
              'echo CQ_REFRESH_END\n')
    rc, out, err = job_ssh(ctx, job, script, timeout=60)
    if rc != 0 or "CQ_REFRESH_END" not in out:
        return {"ok": False, "cached": True, "error": (err or "host job data unavailable")[-300:]}
    ent = parse_snapshot(out)["jobs"].get(job["id"])
    if job.get("status") in TERMINAL:
        with FileLock(ctx.p("locks", "deferred-result-" + job["id"] + ".lock"), 60):
            current = load_job(ctx, job["id"])
            pending = {entry["path"] for entry in (current or {}).get("artifact_missing", [])}
            if pending or (current or {}).get('artifact_pending'):
                capture = []
                result = fetch_results(ctx, current, capture=capture, only_missing=pending)
                if result is None:
                    return {"ok": False, "cached": True, "error": "artifact fetch failed; host source retained"}
                tail, paths = result
                recovered = {entry["path"] for entry in capture}
                with ctx.qlock():
                    current = load_job(ctx, job["id"])
                    if current:
                        already_captured = {d['path'] for d in current.get('artifact_digests') or []}
                        missing = [entry for entry in current.get("artifact_missing", [])
                                   if entry["path"] not in recovered and entry['path'] not in already_captured]
                        if '[cq] ' + ARTIFACT_COMMAND_ERROR in tail:
                            missing = [dict(entry, reason=ARTIFACT_COMMAND_ERROR) for entry in missing]
                        by_path = {d['path']: d for d in (current.get('artifact_digests') or []) + capture}
                        current.update(out_paths=list(dict.fromkeys((current.get("out_paths") or []) + paths)),
                                       artifact_missing=missing, artifact_digests=list(by_path.values()))
                        save_job(ctx, current)
    else:
        reap_deferred(ctx, job, ent, ctx.now(), [], allow_registration=False, state=state)
    return {"ok": True, "cached": False, "phase": state["phase"]}


def refresh_host_artifacts(ctx, job):
    """Recover only artifacts from the original stopped ordinary host job."""
    if job.get('temp_storage') == 'ram':
        status = ram_control(ctx, job, 'status')
        if not status or status.get('empty') is not True:
            return {'ok': False, 'cached': True, 'error': 'RAM worker shutdown not confirmed'}
    else:
        jid = shlex.quote(job['id'])
        require_exit = not (job.get('host_empty_confirmed') is True and job.get('status') in ('cancelled', 'timeout'))
        script = (f'd="$HOME/q/"{jid}\n'
                  'p=""; [ -f "$d/pid" ] && read -r p < "$d/pid" || :\n'
                  'case "$p" in ""|*[!0-9]*) exit 2;; esac\n'
                  + ('[ -f "$d/exit" ] || exit 3\n' if require_exit else '') +
                  'pgrep -s "$p" >/dev/null 2>&1; rc=$?\n'
                  '[ "$rc" = 1 ] || exit 4\n'
                  'echo CQ_STOPPED\n')
        rc, out, _ = job_ssh(ctx, job, script, timeout=60)
        if rc != 0 or 'CQ_STOPPED' not in out:
            return {'ok': False, 'cached': True, 'error': 'host worker shutdown not confirmed'}
    current = load_job(ctx, job['id'])
    if not current:
        return {'ok': False, 'cached': True, 'error': 'job no longer exists'}
    pending = {item['path'] for item in current.get('artifact_missing', [])}
    result = fetch_results(ctx, current, only_missing=pending)
    if result is None:
        return {'ok': False, 'cached': True, 'error': 'artifact fetch failed; host source retained'}
    tail, _ = result
    with ctx.qlock():
        current = load_job(ctx, job['id'])
        missing = current.get('artifact_missing') or []
        if '[cq] ' + ARTIFACT_COMMAND_ERROR in tail:
            missing = [dict(item, reason=ARTIFACT_COMMAND_ERROR) if item['path'] in pending else item
                       for item in missing]
        current['artifact_missing'] = missing
        save_job(ctx, current)
    if not missing and not current.get('artifact_pending'):
        drop_ref(current.get('top'), current['id'])
    return {'ok': True, 'cached': False, 'remaining': len(missing)}


def cmd_status(ctx, req):
    now = ctx.now()
    if req.get("refresh"):
        if not req.get("job"):
            raise CqError("refresh requires a job ID")
        job = load_job(ctx, req["job"])
        if not job:
            raise CqError(f"no such job {req['job']!r}")
        if job.get('defer'):
            refresh = refresh_deferred(ctx, job)
        elif (job.get('placed_where') == 'laptop' and job.get('status') in TERMINAL
              and (job.get('artifact_missing') or job.get('artifact_pending'))):
            run = ctx.p('run', job['id'])
            pid = read_pid(os.path.join(run, 'pid'))
            if pid and ctx.laptop.pid_alive(pid):
                refresh = {'ok': False, 'cached': True, 'error': 'laptop wrapper has not exited'}
            else:
                try:
                    _, _, missing = capture_local_job(ctx, job, os.path.join(run, 'src'))
                    with ctx.qlock():
                        current = load_job(ctx, job['id'])
                        current['artifact_missing'] = missing
                        save_job(ctx, current)
                    if not missing:
                        with contextlib.suppress(OSError):
                            rmtree_force(os.path.join(run, 'src'))
                        drop_ref(current.get('top'), current['id'])
                    refresh = {'ok': True, 'cached': False, 'remaining': len(missing)}
                except (OSError, ValueError) as exc:
                    refresh = {'ok': False, 'cached': True, 'error': str(exc)}
        elif (job.get('placed_where') == 'host' and job.get('status') in TERMINAL
              and (job.get('artifact_missing') or job.get('artifact_pending'))):
            refresh = refresh_host_artifacts(ctx, job)
        else:
            raise CqError('refresh is available only for deferred jobs or missing laptop artifacts')
        return {**cmd_get(ctx, req), "refresh": refresh}
    if req.get("job"):
        return cmd_get(ctx, req)
    jobs = all_jobs(ctx)
    active = [j for j in jobs if j.get("status") in ("queued", "launching", "running") and not j.get("parent")]
    recent = sorted((j for j in jobs if j.get("status") in TERMINAL and not j.get("parent")), key=lambda j: j.get("t_finished", 0))[-5:]
    cap = load_capacity(ctx)
    waiting = [j for j in queued_jobs(ctx, now) if not j.get("defer")]
    plan_cap, res = _plan_state(ctx, cap, waiting, now)
    started = _or_none(_planned_starts, ctx, plan_cap, waiting, now) if res is not None else None  # once per status call
    position = {j["id"]: i for i, j in enumerate(waiting, 1)}
    queue = []
    for j in sorted(active, key=lambda j: start_key(j, now)):  # queued rows then come in `position` order
        row = status_row(j, now, cap)
        if j["status"] == "queued":
            row["position"] = position.get(j["id"])
            row["wait_reason"] = ("awaiting host staging" if j.get("defer") else
                                  _or_none(wait_reason, j, plan_cap, res, now, started) if res is not None else None)
        queue.append(row)
    return {"ok": True, "queue": queue, "recent": [status_row(j, now, cap) for j in recent], "capacity": cap,
            "summary": _or_none(capacity_summary, cap), "leader": read_json(ctx.p("leader.json")), "host": host_state(ctx)}


def cmd_wait(ctx, req):
    """Poll a job every 10 s until it ends or timeout_s (<= 540) passes; returns the job either way."""
    job_id = req.get("job", "")
    deadline = time.monotonic() + max(1, min(540, int(req.get("timeout_s") or 540)))
    interval = float(os.environ.get("CQ_WAIT_INTERVAL", "10"))
    while True:
        job = load_job(ctx, job_id)
        if not job:
            raise CqError(f"no such job {job_id!r}")
        if job["status"] in TERMINAL:
            # the caller now has the result: when it is the session the notice would go to, the notice is not sent
            marked = mark_delivered(ctx, job_id, req.get("seen_by"), "wait")
            return {"ok": True, "ended": True, "delivered": bool(marked or job.get("notified")),
                    "job": result_view(job) | {"status": job["status"]}}
        if time.monotonic() >= deadline:
            return {"ok": True, "ended": False, "job": job_summary(job, ctx.now())}
        time.sleep(min(interval, max(0.0, deadline - time.monotonic())))


def cmd_notified(ctx, req):
    ids = req.get("ids") or ([req["job"]] if req.get("job") else [])
    with ctx.qlock():
        if req.get("owner_session") and not notice_owner_matches(req.get("owner_root") or "", req["owner_session"],
                                                                   req.get("owner_vendor") or "claude"):
            return {"ok": False, "error": "notice owner changed"}
        if req.get("owner_session"):
            identity = worktree_identity(req.get("owner_root") or "")
            jobs = [load_job(ctx, i) for i in ids]
            if identity is None or any(not j or not notice_matches_worktree(j, req.get("owner_root") or "", identity, req.get("owner_session")) for j in jobs):
                return {"ok": False, "error": "notice worktree changed"}
        else:
            jobs = [load_job(ctx, i) for i in ids]
        for j in jobs:
            if (j and not j.get("notified") and
                    (not req.get("owner_session") or notice_matches_worktree(j, req.get("owner_root") or "", identity, req.get("owner_session")))):
                j["notified"] = True
                save_job(ctx, j)
    if req.get("ack_alert"):
        hs = host_state(ctx)
        if hs.get("disk_alert"):
            hs["disk_alert"]["acked"] = True
            save_host_state(ctx, hs)
    return {"ok": True, **({"acknowledged_ids": ids} if req.get("notice_protocol") == 2 else {})}


def cmd_note(ctx, req):
    decision(ctx, {"kind": req.get("kind", "note"), **{k: v for k, v in req.items() if k != "kind"}})
    return {"ok": True}


# --------------------------------------------------------------------------- main

COMMANDS = {"submit": cmd_submit, "list": cmd_list, "get": cmd_get, "cancel": cmd_cancel, "status": cmd_status,
            "ensure_deferred": cmd_ensure_deferred,
            "leader": cmd_leader, "probe": cmd_probe, "reap": cmd_reap, "tick": cmd_tick, "guard": cmd_guard,
            "apply": cmd_apply, "dispatch": cmd_dispatch, "notified": cmd_notified, "note": cmd_note, "watch": cmd_watch,
            "notice_authorize": cmd_notice_authorize,
            "wait": cmd_wait}


CONFIG_LOCK = threading.RLock()


def refresh_host_config(state, host_id=None):
    """Call under CONFIG_LOCK: globals stay consistent across MCP request and leader threads."""
    global _CONF, HOST, HOST_NAME, HOST_PAUSED, HOST_SLOTS, SLOT_VCPU, SLOT_RAM_GB
    global REMOTERUN, HEAVYRUN, HEAVY_LOCK
    if not host_id:
        REMOTERUN, HEAVYRUN, HEAVY_LOCK = queue_paths.runtime_paths(state)
    global LOAD_CAP, HOST_RAM_FLOOR_GB, PLANNER, RAM_JOBS, RAM_BUDGET_GB, TMP_ROOT, GO_TRIMPATH
    _CONF = host_conf(state)
    if host_id and (not _CONF.get("address") or _CONF.get("host_id") != host_id):
        raise CqError("queue shard needs a matching host_id and explicit address in host.conf.json")
    if host_id and _CONF.get("portable_runner") and not _CONF.get("machine_id"):
        raise CqError("portable queue host requires a pinned machine_id")
    if _CONF.get("fallback_address") and not _CONF.get("machine_id"):
        raise CqError("a fallback route requires a pinned machine_id")
    # Environment overrides belong to the legacy queue. Inheriting Ember's
    # LANE_HOST or caps into Hearth would silently execute on the wrong machine.
    env = {} if host_id else os.environ
    HOST = host_address(env, _CONF)
    HOST_NAME = _CONF.get("name", "ember")
    HOST_PAUSED = bool(_CONF.get("paused", False))
    HOST_SLOTS = _CONF.get("slots", 8)
    SLOT_VCPU = float(_CONF.get("slot_vcpu", 1.5))
    SLOT_RAM_GB = float(_CONF.get("slot_ram_gb", 2.5))
    LOAD_CAP = host_load_cap(env, _CONF)
    HOST_RAM_FLOOR_GB = host_ram_floor_gb(env, _CONF)
    PLANNER = host_planner(_CONF)
    RAM_JOBS = bool(_CONF.get("ram_jobs", False))
    RAM_BUDGET_GB = float(_CONF.get("memory_budget_gb", 42))
    TMP_ROOT = _CONF.get("tmp_root", "")
    GO_TRIMPATH = bool(_CONF.get("go_trimpath", False))


def host_binding():
    """Immutable execution settings copied into a job before any remote work."""
    return {**_CONF, "address": HOST, "slots": HOST_SLOTS, "slot_ram_gb": SLOT_RAM_GB,
            "tmp_root": TMP_ROOT, "go_trimpath": GO_TRIMPATH, "sandbox": _CONF.get("sandbox", "off")}


def binding_matches_current(job):
    binding = job.get("host_config") or {}
    return (job.get("host", HOST) == HOST
            and (not binding or (binding.get("machine_id") == _CONF.get("machine_id")
                                 and binding.get("slots", HOST_SLOTS) == HOST_SLOTS)))


def _execute_single(ctx, command, request):
    if command == "wait":
        return cmd_wait(ctx, request)
    with CONFIG_LOCK:
        refresh_host_config(ctx.state, ctx.host_id)
        ctx.refresh_config = True
        return COMMANDS[command](ctx, request)


def execute(ctx, command, request):
    import queue_generations
    return queue_generations.execute(sys.modules[__name__], ctx, command, attached_request(ctx, request), _execute_current)


def _execute_current(ctx, command, request):
    # Fleet jobs have one private primary record. Old controllers never see
    # unplaced jobs, and explicit host/laptop/any jobs keep their old meaning.
    import fleet_queue
    fleet_queue.bind(sys.modules[__name__])
    if command == 'status' and request.get('view') is not None:
        if request['view'] != 'fleet' or request.get('job') or request.get('refresh'):
            raise CqError('view=fleet is a cached overview; omit job and refresh')
        import fleet_telemetry
        return {'ok': True, 'fleet': fleet_telemetry.summarize(ctx.p('fleet'), ctx.now())}
    if not ctx.host_id and ((command == "submit" and request.get("where") == "fleet") or
            (command in ("get", "status", "wait", "cancel") and is_fleet_id(request.get("job")))):
        return fleet_queue.execute(ctx, command, request)
    def route():
        if os.path.isfile(ctx.p("hosts.json")):
            return multi_host.execute(sys.modules[__name__], ctx, command, request, _execute_single)
        if request.get("host_id") not in (None, "", "ember"):
            raise CqError("unknown host: configure hosts.json first")
        return _execute_single(ctx, command, request)
    if command == "wait":
        return route()  # polling must not stall configuration or the leader thread
    with CONFIG_LOCK:
        result = route()
    if not ctx.host_id:
        result = fleet_queue.augment(ctx, command, request, result)
    return result


def main(argv=None):
    ap = argparse.ArgumentParser(prog="cq.py")
    ap.add_argument("cmd")
    ap.add_argument("ids", nargs="*")
    ap.add_argument("--state")
    ap.add_argument("--sync", action="store_true", help="run launches inline instead of in a detached worker")
    args = ap.parse_args(argv)
    ctx = Ctx(state=args.state, sync=args.sync)
    refresh_host_config(ctx.state, ctx.host_id)
    ctx.refresh_config = True
    if args.cmd == "_launch":
        for jid in args.ids:
            launch(ctx, jid, args.ids)
        return 0
    if args.cmd == "_deferred_loop":
        deferred_loop(ctx)
        return 0
    if args.cmd == "_laprun":
        return 0 if laprun(ctx, args.ids[0]) is not None else 1
    fn = COMMANDS.get(args.cmd)
    if fn is None:
        print(json.dumps({"ok": False, "error": f"unknown command {args.cmd!r}"}))
        return 2
    raw = sys.stdin.buffer.read().decode("utf-8") if not sys.stdin.isatty() else ""
    try:
        req = json.loads(raw) if raw.strip() else {}
        res = execute(ctx, args.cmd, req)
    except CqError as e:
        res = {"ok": False, "error": str(e)}
    except Exception as e:  # never a traceback into the mod: a reason it can show
        res = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    sys.stdout.buffer.write((json.dumps(res) + "\n").encode("utf-8"))
    return 0 if res.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
