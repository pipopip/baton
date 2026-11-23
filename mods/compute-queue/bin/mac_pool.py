"""Contained macOS executor for fleet jobs. No admission decisions live here.

The macOS counterpart of windows_pool.py, with the same descriptor, claim, result and observe contracts (the claim
and descriptor helpers are windows_pool's own: they are file formats, not Windows calls). A launch is run as
`mac_pool.py <descriptor>`, or by mac_broker.LocalPool inside the controller.

macOS has neither cgroups nor Job Objects, so containment is a set of process-level measures. What each one is:

  tree           the job runs in its own session (its process group is its pid). The runner follows the tree as it
                 grows: a kqueue (EVFILT_PROC, NOTE_FORK | NOTE_EXIT) on every member says at once when one forks, and
                 the children of that member are listed (libproc) and registered the same way, with their start
                 times. A poll of every member's children every 100 ms backs the events up. The job starts held at a
                 pipe, so its first fork is seen. The members, as (pid, start time) pairs, are written beside the
                 result (tree.json, throttled), and observe() and the guard read that file and check each start time
                 before counting or killing a pid. A setsid() child, or one orphaned to launchd, stays a member.
                 The environment is not consulted (`ps` hides it for Apple's binaries); CQ_FLEET_JOB=<job name> is
                 exported for people to read only. What can still escape: a double fork whose intermediate process
                 exits before its fork event is handled, which leaves a grandchild no process lists as its child.
  end            the job is over when its main process has exited and the tracked tree is empty, as for a Job
                 Object. A process left running keeps it running until the job's timeout (exit 124, tree killed).
  kill           SIGTERM to every member, KILL_GRACE_S, then SIGKILL for at most KILL_PATIENCE_S more. A process that
                 cannot be signalled (EPERM) or will not die ends the job as failed, "shutdown unconfirmed: pids ...".
  priority       normal: the machine is the owner's, and a job may use all of it.
  memory         no reservation: admission gates on the kernel's memory pressure level (fleet_queue._fit). A watchdog
                 sums the physical footprint of the tree every MEM_POLL_S and, only while the pressure level is warn or
                 critical and the submitter declared a cap, kills the tree above that cap and records exit 137. At
                 normal pressure a job may exceed its cap (macOS compresses and swaps); that is written to the job log
                 once. A watchdog, not a kernel limit: a job can exceed its cap between two looks.
  CPU            admission only (cpu_slots against the pool). Nothing caps a running job's CPU time.
  sleep          `caffeinate -i -w <job>` keeps the Mac awake while the job runs. Closing the lid still sleeps it.
  orphans        a guard process (`mac_pool.py --guard`), started before the job and told its pid through a pipe,
                 kills the tree when the runner dies without finishing, as a Job Object's kill-on-close does. A
                 launch is refused when the guard cannot start.

Identities are process start times in microseconds since the epoch (libproc), the boot identity is the kernel's
boot session UUID. The claim and result keys keep their Windows names (`worker_start_filetime`): the controller
reads them as opaque identities.
"""
from __future__ import annotations

import contextlib
import ctypes
import json
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid

try:
    import fcntl
except ImportError:   # Windows: the module is imported there (the pool selector, the tests) but never run
    fcntl = None

from windows_pool import (_atomic, _claim, _descriptor_hash, _load_descriptor, claim_fingerprint, claim_path,  # noqa: F401
                          read_claim)

KILL_GRACE_S = 5.0      # SIGTERM to SIGKILL
KILL_PATIENCE_S = 5.0   # SIGKILL rounds, in all, before a process is given up on
MEM_POLL_S = 2.0        # the memory and full-table look
TRACK_POLL_S = 0.1      # the fallback poll of the members' children, and the longest wait for a fork event
TREE_WRITE_S = 0.25     # tree.json is rewritten at most this often
SAVE_S = 0.25           # result.json likewise
GUARD_READY_S = 10.0
TERMINAL_PHASES = ("done", "failed", "cancelled", "timeout")
MEMORY_KIND = "macos-footprint"
PS_MAX_AGE_S = 1.0      # a process-table sample is shared by every runner thread of this process for this long
TAG_ENV = "CQ_FLEET_JOB"   # exported for observability; no decision reads it
SWAP_RE = re.compile(r"used\s*=\s*([0-9.]+)([KMGT])", re.I)


def _setup():
    """windows_pool's hook for loading Win32 entry points; nothing to load here."""


# --------------------------------------------------------------------------------------------- libproc

class _BsdInfo(ctypes.Structure):  # struct proc_bsdinfo (sys/proc_info.h), 136 bytes
    _fields_ = [("flags", ctypes.c_uint32), ("status", ctypes.c_uint32), ("xstatus", ctypes.c_uint32),
                ("pid", ctypes.c_uint32), ("ppid", ctypes.c_uint32), ("uid", ctypes.c_uint32), ("gid", ctypes.c_uint32),
                ("ruid", ctypes.c_uint32), ("rgid", ctypes.c_uint32), ("svuid", ctypes.c_uint32),
                ("svgid", ctypes.c_uint32), ("rfu", ctypes.c_uint32), ("comm", ctypes.c_char * 16),
                ("name", ctypes.c_char * 32), ("nfiles", ctypes.c_uint32), ("pgid", ctypes.c_uint32),
                ("pjobc", ctypes.c_uint32), ("e_tdev", ctypes.c_uint32), ("e_tpgid", ctypes.c_uint32),
                ("nice", ctypes.c_int32), ("start_sec", ctypes.c_uint64), ("start_usec", ctypes.c_uint64)]


class _RusageV0(ctypes.Structure):  # struct rusage_info_v0
    _fields_ = [("uuid", ctypes.c_uint8 * 16)] + [(n, ctypes.c_uint64) for n in (
        "user_time", "system_time", "pkg_idle_wkups", "interrupt_wkups", "pageins", "wired_size", "resident_size",
        "phys_footprint", "proc_start_abstime", "proc_exit_abstime")]


_PROC_PIDTBSDINFO = 3
_PROC_ALL_PIDS = 1
_PROC_PPID_ONLY = 6
_SZOMB = 5
_libproc = None


def _lib():
    global _libproc
    if _libproc is None:
        lib = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        lib.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int]
        lib.proc_pidinfo.restype = ctypes.c_int
        lib.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
        lib.proc_pid_rusage.restype = ctypes.c_int
        lib.proc_listpids.argtypes = [ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_int]
        lib.proc_listpids.restype = ctypes.c_int
        _libproc = lib
    return _libproc


def _info(pid):
    """(ppid, pgid, status, start identity) of a process, or None when it does not exist or cannot be read."""
    if isinstance(pid, bool) or not isinstance(pid, int) or not 0 < pid <= 0x7FFFFFFF:
        return None
    try:
        info = _BsdInfo()
        if _lib().proc_pidinfo(pid, _PROC_PIDTBSDINFO, 0, ctypes.byref(info), ctypes.sizeof(info)) != ctypes.sizeof(info):
            return None
        start = info.start_sec * 1_000_000 + info.start_usec
        return (info.ppid, info.pgid, info.status, start) if start > 0 else None
    except (OSError, AttributeError):
        return None


def _bsd(pid):
    """(status, start identity) of a process, or None."""
    info = _info(pid)
    return (info[2], info[3]) if info else None


def process_identity(pid):
    """A process's start time in microseconds since the epoch, or None when it cannot be read."""
    info = _info(pid)
    return info[3] if info else None


def _list_pids(kind, info=0):
    """Pids from proc_listpids: all of them (kind 1) or the children of `info` (kind 6)."""
    size = 4096
    while True:
        buf = (ctypes.c_int * size)()
        got = _lib().proc_listpids(kind, info, buf, ctypes.sizeof(buf))
        if got < 0:
            return []
        count = got // ctypes.sizeof(ctypes.c_int)
        if count < size:
            return [buf[i] for i in range(count) if buf[i] > 0]
        size *= 4


def _children(pid):
    with contextlib.suppress(OSError, AttributeError):
        return _list_pids(_PROC_PPID_ONLY, pid)
    return []


def _worker_alive(pid, start):
    """Whether the process with this exact start identity is alive: True, False (gone, a zombie, or the PID reused)
    or None (cannot tell)."""
    if (isinstance(pid, bool) or not isinstance(pid, int) or not 0 < pid <= 0x7FFFFFFF or
            isinstance(start, bool) or not isinstance(start, int) or start <= 0):
        return None
    info = _info(pid)
    if info is None:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except OSError:
            return None
        # It exists and libproc will not read it: a zombie (the dead worker its parent has not reaped; its pid cannot
        # be reused while it stays) or a process of another user.
        try:
            done = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, timeout=5)
            return False if done.stdout.strip().startswith("Z") else None
        except (OSError, subprocess.SubprocessError):
            return None
    return info[3] == start and info[2] != _SZOMB


def _footprint(pid):
    """Physical footprint in bytes (what Activity Monitor calls Memory, compressed pages included), or None."""
    try:
        usage = _RusageV0()
        if _lib().proc_pid_rusage(pid, 0, ctypes.byref(usage)) != 0:
            return None
        return int(usage.phys_footprint)
    except (OSError, AttributeError):
        return None


# ------------------------------------------------------------------------------------- process table

class Proc:
    __slots__ = ("pid", "ppid", "pgid", "stat", "start", "rss")

    def __init__(self, pid, ppid, pgid, stat="S", start=None, rss=0):
        self.pid, self.ppid, self.pgid, self.stat, self.start, self.rss = pid, ppid, pgid, stat, start, rss


def parse_ps(text):
    """Rows of `ps -axww -o pid=,ppid=,pgid=,stat=,rss=` (no start times: the table of a platform without libproc)."""
    rows = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue
        try:
            rows.append(Proc(int(parts[0]), int(parts[1]), int(parts[2]), parts[3], None, int(parts[4])))
        except ValueError:
            continue
    return rows


def _table():
    if sys.platform == "darwin":
        rows = []
        for pid in _list_pids(_PROC_ALL_PIDS):
            info = _info(pid)
            if info:
                rows.append(Proc(pid, info[0], info[1], "Z" if info[2] == _SZOMB else "S", info[3]))
        if not rows:
            raise OSError("process table unreadable")
        return rows
    done = subprocess.run(["ps", "-axww", "-o", "pid=,ppid=,pgid=,stat=,rss="], capture_output=True, text=True,
                          timeout=20, env=dict(os.environ, LC_ALL="C", LANG="C"))
    rows = parse_ps(done.stdout)
    if done.returncode != 0 or not rows:
        raise OSError("ps failed")
    return rows


_table_lock = threading.Lock()
_table_cache = (0.0, [])


def snapshot(max_age=PS_MAX_AGE_S):
    """The process table, shared for max_age seconds between the threads of one process. Raises OSError when unreadable."""
    global _table_cache
    with _table_lock:
        taken, rows = _table_cache
        if rows and 0 < max_age and 0 <= time.monotonic() - taken < max_age:
            return rows
        rows = _table()
        _table_cache = (time.monotonic(), rows)
        return rows


def _pairs(known):
    return list(known.items()) if isinstance(known, dict) else list(known)


def members(rows, leader=None, known=()):
    """The pids of a job's tree in `rows`: the recorded members that are still the same process (`known`, (pid, start)
    pairs), the job's leader and its process group while the leader is the process that started the job (`leader`,
    (pid, start)), and the descendants of all of those. A pid whose start time is not the recorded one is another
    process; zombies and this process are not members."""
    live = {r.pid: r for r in rows if not r.stat.startswith("Z") and r.pid != os.getpid()}

    def same(pid, start):
        row = live.get(pid)
        return row is not None and (row.start is None or row.start == start)
    found = {pid for pid, start in _pairs(known) if same(pid, start)}
    if leader and same(*leader):
        found.add(leader[0])
        found.update(r.pid for r in live.values() if r.pgid == leader[0])
    children = {}
    for r in live.values():
        children.setdefault(r.ppid, []).append(r.pid)
    todo = list(found)
    while todo:
        for child in children.get(todo.pop(), ()):
            if child not in found:
                found.add(child)
                todo.append(child)
    return found


def tree_of(rows, root):
    """`root` and its descendants in `rows` (a tree walked by parent links only)."""
    live = {r.pid: r for r in rows if not r.stat.startswith("Z") and r.pid != os.getpid()}
    children = {}
    for r in live.values():
        children.setdefault(r.ppid, []).append(r.pid)
    found, todo = ({root} if root in live else set()), [root]
    while todo:
        for child in children.get(todo.pop(), ()):
            found.add(child)
            todo.append(child)
    return found


def _signal(pids, sig):
    """Send `sig`; returns the pids that refused it (EPERM)."""
    denied = set()
    for pid in pids:
        try:
            os.kill(pid, sig)
        except PermissionError:
            denied.add(pid)
        except OSError:
            pass
    return denied


def _killpg(pgid, pids, sig):
    """killpg(pgid) only while one of `pids` (verified members) is in that group: a pgid is a pid that may have been
    reused once its group was empty, and that group is not ours."""
    if not pgid or pgid == os.getpgrp():
        return
    for pid in pids:
        with contextlib.suppress(OSError):
            if os.getpgid(pid) == pgid:
                os.killpg(pgid, sig)
                return


def _kill_members(find, pgid=None, grace=None, patience=None):
    """SIGTERM, grace, SIGKILL for at most `patience` more seconds, until `find()` (a set of pids) is empty. Returns
    (the pids that took a signal, the pids still there at the end: a process that would not die or could not be
    signalled). `find` may raise; the caller decides what an unreadable table means."""
    grace = KILL_GRACE_S if grace is None else grace
    patience = KILL_PATIENCE_S if patience is None else patience
    denied = set()
    touched = set(find())
    if touched:
        _killpg(pgid, touched, signal.SIGTERM)
        denied |= _signal(touched, signal.SIGTERM)
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline and find() - denied:
            time.sleep(0.05)
    end = time.monotonic() + patience
    while True:
        left = find()
        if not left:
            return touched, set()
        touched |= left
        if time.monotonic() >= end:
            return touched, left
        if not left - denied:
            return touched, left   # nothing left that can be signalled
        _killpg(pgid, left, signal.SIGKILL)
        denied |= _signal(left - denied, signal.SIGKILL)
        time.sleep(0.05)


def kill_tree(pid, grace=None):
    """Kill `pid` and everything below it: SIGTERM, grace, SIGKILL. When `pid` leads its own group the group goes too."""
    def find():
        try:
            return tree_of(snapshot(0), pid)
        except (OSError, subprocess.SubprocessError):
            return set()
    try:
        snapshot(0)
    except (OSError, subprocess.SubprocessError):  # no process table: the old single-process kill
        _signal([pid], signal.SIGTERM)
        return {pid}
    try:
        leader = os.getpgid(pid) == pid
    except OSError:
        leader = False
    return _kill_members(find, pgid=pid if leader else None, grace=grace)[0]


# ----------------------------------------------------------------------------------- following a tree

class Tracker:
    """The members of one job's tree, (pid -> start time), followed by fork events and a poll of their children."""

    def __init__(self):
        self.kq = select.kqueue()
        self.known = {}
        self.changed = False

    def close(self):
        with contextlib.suppress(OSError):
            self.kq.close()

    def add(self, pid, start=None, recurse=True):
        start = start or process_identity(pid)
        if not start or self.known.get(pid) == start:
            return
        self.known[pid] = start
        self.changed = True
        with contextlib.suppress(OSError):   # ESRCH: it is already gone; refresh() drops it
            self.kq.control([select.kevent(pid, select.KQ_FILTER_PROC, select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_CLEAR,
                                           select.KQ_NOTE_FORK | select.KQ_NOTE_EXIT)], 0)
        if recurse:     # whatever it forked before it was registered
            self.scan(pid)

    def scan(self, pid):
        for child in _children(pid):
            self.add(child)

    def wait(self, timeout=TRACK_POLL_S):
        """Handle the fork events that arrive within `timeout`: at once, the forker's children are listed."""
        try:
            events = self.kq.control(None, 64, timeout)
        except OSError:
            time.sleep(timeout)
            return
        for event in events:
            if event.fflags & select.KQ_NOTE_FORK:
                self.scan(event.ident)

    def sweep(self):
        """The fallback poll: the children of every member."""
        for pid in list(self.known):
            self.scan(pid)

    def refresh(self):
        """Drop what is gone, a zombie, or another process with a reused pid."""
        for pid, start in list(self.known.items()):
            info = _info(pid)
            if info is None or info[3] != start or info[2] == _SZOMB:
                del self.known[pid]
                self.changed = True

    def live(self):
        return dict(self.known)


# ------------------------------------------------------------------------------------ host resources

def _vm_pages(text, key):
    match = re.search(r"^%s:\s+(\d+)\." % re.escape(key), text, re.M)
    return int(match.group(1)) if match else None


def parse_vm_stat(text):
    """Available bytes from `vm_stat`, for when kern.memorystatus_level cannot be read: free + speculative pages, and
    the inactive pages that are file-backed (inactive x file-backed / (file-backed + anonymous): anonymous pages are
    reclaimable only by compression or swap, purgeable ones are already counted in the others). The page size comes
    from the header line. None when the text is not vm_stat's."""
    size = re.search(r"page size of (\d+) bytes", text)
    if not size:
        return None
    free, spec, inactive = (_vm_pages(text, k) for k in ("Pages free", "Pages speculative", "Pages inactive"))
    file_backed, anonymous = _vm_pages(text, "File-backed pages"), _vm_pages(text, "Anonymous pages")
    if None in (free, spec, inactive, file_backed, anonymous):
        return None
    share = file_backed / (file_backed + anonymous) if file_backed + anonymous else 0.0
    return int((free + spec + inactive * share) * int(size.group(1)))


def parse_memorystatus(text, total_bytes):
    """Available bytes from kern.memorystatus_level (the kernel's percentage of memory available) x hw.memsize / 100;
    None when the value is not a percentage between 1 and 100."""
    try:
        level = int(str(text).strip())
    except ValueError:
        return None
    return int(total_bytes * level / 100) if 1 <= level <= 100 and total_bytes > 0 else None


def parse_pmset(text):
    """Whether the Mac runs on battery, from `pmset -g batt`: True unless its first line says AC Power, False when it
    does, None when the output is not pmset's (unknown, never taken for battery)."""
    first = text.strip().splitlines()[0] if text.strip() else ""
    if "Now drawing from" not in first:
        return None
    return "AC Power" not in first


def parse_swap(text):
    """Used swap in bytes from `sysctl -n vm.swapusage`, or None."""
    match = SWAP_RE.search(text or "")
    if not match:
        return None
    return float(match.group(1)) * 1024 ** ("KMGT".index(match.group(2).upper()) + 1)


def _out(argv, timeout=10):
    done = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, env=dict(os.environ, LC_ALL="C", LANG="C"))
    if done.returncode != 0:
        raise OSError(argv[0] + " failed")
    return done.stdout


def _sysctl(name):
    return _out(["/usr/sbin/sysctl", "-n", name]).strip()


def available_gb():
    """Memory a new job can count on, in GiB: the kernel's available percentage x memory size, else vm_stat's free,
    speculative and file-backed inactive pages; None when neither can be read. The same figure is the pool's probe's
    and Laptop.free_ram_gb's."""
    try:
        total = int(_sysctl("hw.memsize"))
        if total <= 0:
            return None
        free = None
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            free = parse_memorystatus(_sysctl("kern.memorystatus_level"), total)
        if free is None:
            free = parse_vm_stat(_out(["/usr/bin/vm_stat"]))
        return None if free is None else min(free, total) / 2 ** 30
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def on_battery():
    try:
        return parse_pmset(_out(["/usr/bin/pmset", "-g", "batt"]))
    except (OSError, subprocess.SubprocessError):
        return None


def boot_identity():
    """The kernel's boot session UUID, or None."""
    try:
        value = _sysctl("kern.bootsessionuuid")
        return str(uuid.UUID(value)) if uuid.UUID(value).int else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


PRESSURE_NAMES = {1: "normal", 2: "warn", 4: "critical"}   # kern.memorystatus_vm_pressure_level (DISPATCH_MEMORYPRESSURE_*)
PRESSURE_SYSCTL = "kern.memorystatus_vm_pressure_level"


def parse_pressure(text):
    """The kernel's memory pressure level from `sysctl -n kern.memorystatus_vm_pressure_level`: 1 normal, 2 warn,
    4 critical; None for anything else (unknown, never taken for pressure)."""
    try:
        level = int(str(text).strip())
    except ValueError:
        return None
    return level if level in PRESSURE_NAMES else None


def memory_pressure():
    """The kernel's current memory pressure level (1, 2 or 4), or None when it cannot be read."""
    try:
        value = ctypes.c_int(0)
        size = ctypes.c_size_t(ctypes.sizeof(value))
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.sysctlbyname(PRESSURE_SYSCTL.encode(), ctypes.byref(value), ctypes.byref(size), None, 0) == 0:
            return parse_pressure(value.value)
    except (OSError, AttributeError):
        pass
    try:
        return parse_pressure(_sysctl(PRESSURE_SYSCTL))
    except (OSError, subprocess.SubprocessError):
        return None


def probe():
    """Physical headroom in GiB; unknown means unsupported. macOS has no commit limit: commit_free_gb is the same
    available figure as available_gb, swap_used_gb says what has been paged out, on_battery whether the Mac is off AC
    power (None when unknown; information only), pressure the kernel's memory pressure level (1, 2 or 4; None when
    unknown), which is what laptop admission gates on."""
    result = dict(supported=False, logical_cpus=None, available_gb=None, commit_free_gb=None, total_gb=None,
                  capabilities=[], boot_identity=boot_identity(), on_battery=None, swap_used_gb=None,
                  pressure=None)
    try:
        n = os.cpu_count()
        total = int(_sysctl("hw.memsize"))
        free = available_gb()
        if not n or total <= 0 or free is None:
            return result
        result.update(supported=True, logical_cpus=n, available_gb=free, commit_free_gb=free, total_gb=total / 2 ** 30,
                      on_battery=on_battery(), pressure=memory_pressure())
        with contextlib.suppress(OSError, ValueError, subprocess.SubprocessError):
            swap = parse_swap(_sysctl("vm.swapusage"))
            result["swap_used_gb"] = None if swap is None else swap / 2 ** 30
        result["capabilities"] = ["macos"]
        if shutil.which("python3") or shutil.which("python"):
            result["capabilities"].append("python")
        result["capabilities"].extend(name for name in ("bash", "go", "node", "ffmpeg", "blender", "adb") if shutil.which(name))
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return result


# ------------------------------------------------------------------------------------------- locks

class SharedLock:
    """flock on the heavy-lock file, shared: the legacy laptop path and a cohort owner take it exclusively."""
    mode = fcntl.LOCK_SH if fcntl else None

    def __init__(self, path):
        self.fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        self.held = False

    def try_acquire(self):
        try:
            fcntl.flock(self.fd, self.mode | fcntl.LOCK_NB)
            self.held = True
        except BlockingIOError:
            self.held = False
        return self.held

    def close(self):
        if self.held:
            with contextlib.suppress(OSError):
                fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)


class ExclusiveLock(SharedLock):
    mode = fcntl.LOCK_EX if fcntl else None


def heavy_lock_held(path):
    """Whether anything holds the heavy lock (an exclusive try that fails), True when it cannot be told."""
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    except OSError:
        return True
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    except OSError:
        return True
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------------------------- jobs

def _job_name(d):
    return "cq-fleet-" + d["id"] + "-" + d["nonce"]


def tree_path(d):
    return os.path.join(os.path.dirname(os.path.abspath(d["result_path"])), "tree.json")


def _write_tree(d, leader, known):
    _atomic(tree_path(d), dict(version=1, id=d["id"], nonce=d["nonce"], leader=list(leader) if leader else None,
                               members=[[pid, start] for pid, start in sorted(_pairs(known))]))


def _pair(value):
    if (isinstance(value, (list, tuple)) and len(value) == 2 and all(type(x) is int and x > 0 for x in value)):
        return (value[0], value[1])
    return None


def read_tree(d):
    """(leader, [(pid, start)]) recorded beside the result, or (None, []) when absent or not this job's."""
    try:
        with open(tree_path(d), encoding="utf-8") as f:
            data = json.load(f)
        if data.get("version") != 1 or data.get("id") != d["id"] or data.get("nonce") != d["nonce"]:
            return None, []
        return _pair(data.get("leader")), [p for p in map(_pair, data.get("members") or []) if p]
    except (OSError, ValueError, AttributeError, TypeError):
        return None, []


def observe(descriptor_path, worker_pid=None, worker_start_filetime=None):
    """Read worker identity and the job's tree without terminating anything: the windows_pool.observe contract, with
    the recorded tree (tree.json, each start time checked, and the descendants of its members) in place of the named
    Job Object (job_exists: the tree has a live process)."""
    out = dict(worker_alive=None, job_exists=None, active_processes=None, empty_confirmed=False)
    try:
        d = _load_descriptor(descriptor_path)
        claim = read_claim(descriptor_path)
        if claim is None and os.path.lexists(claim_path(descriptor_path)):
            return out  # incomplete, invalid, or unreadable claim: never infer empty
        result = None
        try:
            with open(d["result_path"], encoding="utf-8") as f:
                result = json.load(f)
        except (OSError, ValueError):
            pass
        if not isinstance(result, dict):
            result = {}
        recorded = (result.get("worker_pid"), result.get("worker_start_filetime"))
        supplied = (worker_pid, worker_start_filetime)
        if result.get("job_name") not in (None, _job_name(d)):
            return out
        claimed = ((claim["worker_pid"], claim["worker_start_filetime"]) if claim else (None, None))
        identities = [pair for pair in (supplied, recorded, claimed) if pair != (None, None)]
        if identities and any(pair != identities[0] for pair in identities):
            return out
        identity = identities[0] if identities else (None, None)
        out["worker_alive"] = _worker_alive(*identity)
        leader, known = read_tree(d)
        leader = _pair([result.get("pid"), result.get("process_start_filetime")]) or leader
        found = members(snapshot(0), leader, known)
        out.update(job_exists=bool(found), active_processes=len(found))
        out["empty_confirmed"] = claim is not None and out["worker_alive"] is False and not found
    except (OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
        pass
    return out


def _spawn(d):
    """Start the job in its own session, tagged, at normal priority, held at a pipe: it runs its command (by exec, so the
    pid stays) when the returned fd gets a line, and exits when the fd is closed unwritten. The runner registers the
    pid with the tracker first, so the job's first fork is seen. Returns (Popen, go fd)."""
    env = dict(os.environ)
    env.update(d["env"])
    env[TAG_ENV] = _job_name(d)
    read_end, write_end = os.pipe()
    log = os.open(d["log_path"], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        script = 'read -r _ <&%d || exit 1; exec %d<&-; exec "$@"' % (read_end, read_end)
        proc = subprocess.Popen(["/bin/sh", "-c", script, "sh", *d["argv"]], cwd=d["cwd"], env=env,
                                stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
                                pass_fds=(read_end,))
    except BaseException:
        os.close(write_end)
        raise
    finally:
        os.close(read_end)
        os.close(log)
    return proc, write_end


def _spawn_sleep_guard(pid):
    """`caffeinate -i -w pid`: no idle sleep while the job lives. None when caffeinate is missing."""
    exe = shutil.which("caffeinate") or "/usr/bin/caffeinate"
    if not os.path.exists(exe):
        return None
    try:
        return subprocess.Popen([exe, "-i", "-w", str(pid)], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, start_new_session=True)
    except OSError:
        return None


def _start_guard(descriptor_path, runner_pid, runner_start):
    """Start the orphan guard and wait for it to say it is ready: the job is not started without it. It is told the
    job's pid on its stdin afterwards. Raises OSError when it does not start."""
    guard_proc = None
    try:
        guard_proc = subprocess.Popen([sys.executable, os.path.abspath(__file__), "--guard", os.path.abspath(descriptor_path),
                                       str(runner_pid), str(runner_start)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=subprocess.DEVNULL, start_new_session=True)
        if not select.select([guard_proc.stdout], [], [], GUARD_READY_S)[0] or guard_proc.stdout.readline().strip() != b"ready":
            raise OSError("the orphan guard did not start")
        return guard_proc
    except (OSError, ValueError) as exc:
        if guard_proc is not None:
            with contextlib.suppress(OSError):
                guard_proc.kill()
            with contextlib.suppress(Exception):
                guard_proc.wait(timeout=5)
        raise OSError("launch refused: " + str(exc)) from exc


def guard(descriptor_path, runner_pid, runner_start, interval=1.0, stdin=None):
    """Say ready, learn the job's pid and start time from stdin (end of input first: the job never started), then wait
    for the job's result to turn terminal (and leave) or for the runner to die (and kill the tree)."""
    stdin = sys.stdin.buffer if stdin is None else stdin
    d = _load_descriptor(descriptor_path)
    sys.stdout.write("ready\n")
    sys.stdout.flush()
    line = stdin.readline()
    if not line:
        return
    leader = _pair([int(x) for x in line.split()])
    if leader is None:
        return
    seen = time.monotonic()
    while True:
        try:
            with open(d["result_path"], encoding="utf-8") as f:
                if json.load(f).get("phase") in TERMINAL_PHASES:
                    return
        except (OSError, ValueError):
            pass
        if _worker_alive(runner_pid, runner_start) is False:
            def find():
                return members(snapshot(0), leader, read_tree(d)[1])
            for _ in range(20):   # an unreadable process table is retried, not taken for an empty tree
                try:
                    _kill_members(find, pgid=leader[0])
                    return
                except (OSError, subprocess.SubprocessError):
                    time.sleep(0.5)
            return
        if time.monotonic() - seen > 86400 * 2:
            return
        time.sleep(interval)


def _exit_code(returncode):
    return 128 - returncode if returncode < 0 else returncode


def run(descriptor_path, gate=None, on_claim=None, expected_hash=None):
    d = _load_descriptor(descriptor_path)
    if expected_hash is not None and _descriptor_hash(d) != expected_hash:
        raise ValueError("descriptor changed before claiming; launch refused")
    worker_pid = os.getpid()
    worker_start = process_identity(worker_pid)
    if worker_start is None:
        raise OSError("cannot establish worker process identity")
    if "boot_identity" in d:
        current_boot = boot_identity()
        if current_boot is None or uuid.UUID(current_boot) != uuid.UUID(d["boot_identity"]):
            raise OSError("boot identity unavailable or changed; launch refused")
    _claim(descriptor_path, d, worker_pid, worker_start)
    if on_claim is not None:
        on_claim(worker_pid, worker_start)
    if _descriptor_hash(_load_descriptor(descriptor_path)) != _descriptor_hash(d):
        raise ValueError("descriptor changed while claiming; launch refused")
    state = dict(phase="waiting", pid=None, process_start_filetime=None, job_name=None, started=None,
                 worker_pid=worker_pid, worker_start_filetime=worker_start,
                 finished=None, exit=None, active_processes=0, peak_private_bytes=0, reason="")

    def save():
        _atomic(d["result_path"], state)
    save()
    lock = gate if gate is not None else SharedLock(d["legacy_lock_path"])
    proc = sleeper = watcher = go = tracker = None
    leader = None

    def audit():
        """The tree by the full table now (the tracked members, the leader's group, their descendants), what the
        tracker did not have joining it, and the tree's footprint."""
        rows = snapshot(0)
        found = members(rows, leader, tracker.known if tracker else ())
        by_pid = {r.pid: r for r in rows}
        used = 0
        for pid in found:
            if tracker is not None:
                tracker.add(pid, by_pid[pid].start, recurse=False)
            size = _footprint(pid)
            used += size if size is not None else 0
        return found, used

    def shutdown():
        """Kill whatever is left of the tree; returns the pids that could not be got rid of (None: all gone)."""
        for attempt in range(5):
            try:
                _, stuck = _kill_members(lambda: audit()[0], pgid=leader[0] if leader else None)
                return stuck or None
            except (OSError, subprocess.SubprocessError):
                time.sleep(0.2)
        return {0}   # the process table could not be read

    def unconfirmed(stuck):
        return "shutdown unconfirmed: " + ("process table unreadable" if stuck == {0} else
                                          "pids " + ", ".join(str(p) for p in sorted(stuck)))
    try:
        while not lock.try_acquire():
            if os.path.exists(d["cancel_path"]):
                state.update(phase="cancelled", finished=time.time(), reason="cancelled waiting for legacy lock")
                save()
                return state
            state["reason"] = "legacy heavy lock busy"
            save()
            time.sleep(1)
        if os.path.exists(d["cancel_path"]):
            state.update(phase="cancelled", finished=time.time(), reason="cancelled before launch")
            save()
            return state
        launch_guard = gate.launch_guard() if gate is not None else contextlib.nullcontext(0)
        with launch_guard as reserved_gb:
            p = probe()
            if not p["supported"]:
                raise OSError("macOS resource probe unavailable")
            if d["cpu_slots"] > p["logical_cpus"]:
                raise ValueError("cpu_slots exceeds logical CPUs")
            # no memory headroom check here: the Mac reserves no RAM for a job (ram_floor_gb and the declared caps are
            # the Windows pool's); admission gated on memory pressure before the launch was requested
            state["job_name"] = _job_name(d)
            if os.path.exists(d["cancel_path"]):
                raise InterruptedError("cancelled before launch")
            watcher = _start_guard(descriptor_path, worker_pid, worker_start)
            tracker = Tracker()
            proc, go = _spawn(d)
        start = process_identity(proc.pid)
        if start is None:
            raise OSError("the job's process could not be identified")
        leader = (proc.pid, start)
        pgid = proc.pid
        tracker.add(proc.pid, start)
        _write_tree(d, leader, tracker.known)
        tracker.changed = False
        state.update(phase="running", pid=proc.pid, process_start_filetime=start, job_pgid=pgid, started=time.time(), reason="")
        save()
        sleeper = _spawn_sleep_guard(proc.pid)
        watcher.stdin.write(("%d %d\n" % leader).encode())
        watcher.stdin.flush()
        os.write(go, b"go\n")    # the command runs now
        os.close(go)
        go = None
        start_mono = last_save = last_tree = time.monotonic()
        outcome = code = None
        next_look = 0.0
        cap_bytes = d["memory_max_gb"] * 2 ** 30
        enforce = d.get("memory_cap_declared", True)    # a cap the submitter stated; one the queue derived is not enforced
        logged_over = False
        while True:
            tracker.wait(TRACK_POLL_S)
            tracker.sweep()
            tracker.refresh()
            now = time.monotonic()
            returned = proc.poll()
            state["active_processes"] = len(tracker.known)
            if outcome is None and now >= next_look:
                next_look = now + MEM_POLL_S
                try:
                    found, used = audit()
                    state.update(active_processes=len(found), peak_private_bytes=max(state["peak_private_bytes"], used))
                    if used > cap_bytes and enforce:
                        level = memory_pressure()
                        if level in (2, 4):
                            outcome, code = "failed", 137
                            state["reason"] = ("memory cap exceeded under memory pressure (%s): the job's processes use %.2f GB, "
                                               "the cap is %g GB" % (PRESSURE_NAMES[level], used / 2 ** 30, d["memory_max_gb"]))
                        elif not logged_over:
                            logged_over = True
                            with contextlib.suppress(OSError):
                                with open(d["log_path"], "a", encoding="utf-8") as log:
                                    log.write("cq: the job's processes use %.2f GB, over its declared cap of %g GB; the cap is "
                                              "enforced only under memory pressure (warn or critical), and it is normal now\n"
                                              % (used / 2 ** 30, d["memory_max_gb"]))
                except (OSError, subprocess.SubprocessError):
                    pass  # no look this time; the next one tries again
            if outcome is None and (os.path.exists(d["cancel_path"]) or now - start_mono >= d["timeout_seconds"]):
                if os.path.exists(d["cancel_path"]):
                    outcome, code = "cancelled", 1
                else:
                    outcome, code = "timeout", 124
                    state["reason"] = "timeout after %g seconds" % d["timeout_seconds"]
            ended = False
            if outcome is None and returned is not None and not tracker.known:
                try:
                    ended = not audit()[0]      # the tracker has nothing: the table agrees, or has more to follow
                except (OSError, subprocess.SubprocessError):
                    pass
            if outcome is not None or ended:
                # The job is over: its main process has returned and nothing of its tree is left (as for a Job
                # Object), or we end it. Never report a terminal state until the tree has no process.
                reason = state["reason"]
                if outcome is not None:
                    state["reason"] = "termination pending confirmation"
                    save()
                stuck = shutdown()
                if returned is None:
                    with contextlib.suppress(Exception):
                        returned = proc.wait(timeout=5)
                exit_code = code if code is not None else _exit_code(returned if returned is not None else 1)
                state.update(phase=outcome or ("done" if exit_code == 0 else "failed"), exit=exit_code,
                             finished=time.time(), active_processes=len(stuck or ()), reason=reason)
                if stuck:
                    state.update(phase="failed", reason=unconfirmed(stuck))
                with contextlib.suppress(OSError):
                    _write_tree(d, leader, {} if not stuck else tracker.known)
                save()
                return state
            if tracker.changed and now - last_tree >= TREE_WRITE_S:
                last_tree = now
                tracker.changed = False
                with contextlib.suppress(OSError):
                    _write_tree(d, leader, tracker.known)
            if now - last_save >= SAVE_S:
                last_save = now
                save()
    except BaseException as exc:
        if proc is not None:
            # Never report a terminal state until the tree really has no process.
            stuck = shutdown()
            with contextlib.suppress(Exception):
                proc.wait(timeout=5)
        else:
            stuck = None
        state.update(phase="cancelled" if isinstance(exc, InterruptedError) else "failed", finished=time.time(),
                     active_processes=len(stuck or ()), reason=unconfirmed(stuck) + ": " + str(exc) if stuck else str(exc))
        save()
        return state
    finally:
        if go is not None:
            with contextlib.suppress(OSError):
                os.close(go)
        if watcher is not None:
            with contextlib.suppress(Exception):
                watcher.stdin.close()
                watcher.stdout.close()
        for helper in (sleeper, watcher):
            if helper is not None and helper.poll() is None:
                with contextlib.suppress(OSError):
                    helper.terminate()
        for helper in (sleeper, watcher):
            if helper is not None:
                with contextlib.suppress(Exception):
                    helper.wait(timeout=5)
        if tracker is not None:
            tracker.close()
        lock.close()


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--guard":
        guard(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
        raise SystemExit(0)
    if len(sys.argv) != 2:
        raise SystemExit("usage: mac_pool.py DESCRIPTOR.json")
    result = run(sys.argv[1])
    raise SystemExit(0 if result["phase"] == "done" else 1)
