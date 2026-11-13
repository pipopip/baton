"""The scheduled nightly submit: ``bt night enroll|unenroll|run|schedule``.

``bt night submit`` needs a session to run it each evening. A worktree's owner can instead enrol the
worktree once; one scheduled task on the laptop then runs ``bt night run`` every evening, which does the
same plan-and-submit for each enrolled worktree in the name of the session that owns it at that moment,
so the results reach that session like any deferred job's. The task starts no model session and repairs
nothing; a worktree that cannot be submitted is recorded in the ledger and shown in the morning report.

The authority is the enrolment, made by the owning session for one directory: the run submits only for
that same directory (``_identity``), as whichever session ``.baton/owner.json`` names when it runs.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import getpass
import io
import json
import os
from pathlib import Path, PureWindowsPath
import re
import shutil
import subprocess
import sys
import tempfile
from xml.sax.saxutils import escape

from . import compute_pickup, config, gitutil, harvest, launchd, night, paths, procs

TASK = 'Baton night submit'
DEFAULT_AT = '21:30'   # local time: early enough that the laptop is usually still awake, late enough for the day's merges
FETCH_TIMEOUT = 120    # seconds for one remote; a hung transport must not hold the other worktrees
RUN_WAIT = 600         # seconds `bt night run` waits, once for all its jobs, for host registration (staging a large history takes minutes)
LOCK_WAIT = 300        # seconds the unattended run waits for the submit lock: a session's own submit can hold it for minutes
CLOCK = re.compile(r'([01]\d|2[0-3]):([0-5]\d)')


# ---------------------------------------------------------------- enrolment

def enrolled_path(state=None):
    return night.ledger_path(state).with_name('enrolled.json')


def read_enrolled(state=None):
    try:
        data = json.loads(enrolled_path(state).read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return []
    rows = data.get('worktrees') if isinstance(data, dict) else None
    return [r for r in rows or [] if isinstance(r, dict) and isinstance(r.get('root'), str)]


def write_enrolled(rows, state=None):
    harvest.atomic_write(enrolled_path(state), json.dumps({'version': 1, 'worktrees': rows}, indent=2) + '\n')


def _same(a, b):
    return os.path.normcase(os.path.realpath(str(a))) == os.path.normcase(os.path.realpath(str(b)))


def _identity(path):
    """The directory incarnation, as the queue engine's worktree_identity tells it: a worktree retired and created
    again at the same path, or a junction now pointing elsewhere, is not the directory that was enrolled."""
    try:
        st = os.stat(path)
    except OSError:
        return None
    birth = getattr(st, 'st_birthtime_ns', None)
    if birth is None and os.name == 'nt':
        birth = st.st_ctime_ns
    return {'device': st.st_dev, 'inode': st.st_ino, 'birth_ns': birth}


def _locked():
    lock = night._SubmitLock()
    lock.WAIT = LOCK_WAIT
    return lock


def cmd_enroll(args):
    """Only the owning session enrols a worktree: enrolment lets the task submit in that session's name every night.
    The manifest is planned first, so a manifest that cannot be submitted is refused here, not found at 21:30.
    Enrolling again keeps the worktree's place in the order, which is the order the night's budget is given out in."""
    try:
        root = paths.find_worktree_root(args.worktree or os.getcwd())
        if root is None:
            raise ValueError('Use this command in a worktree or specify --worktree')
        root = Path(os.path.abspath(root))
        session = compute_pickup._owner(root, args.session)
        manifest = night.load_manifest(night.find_manifest(root))
        now = night._now()
        when, _ = night.night_date(now)
        rows = night.read_ledger()
        items = night.plan(root, manifest, when.isoformat(), rows + night.derive_outcomes(rows, None, now))
        entry = {'root': str(root), 'repo': paths.repo_name(root), 'workstream': paths.ws_name(root),
                 'identity': _identity(root), 'enrolled_at': now, 'by': session}
        with night._SubmitLock():
            kept = [entry if _same(r['root'], root) else r for r in read_enrolled()]
            if entry not in kept:
                kept.append(entry)
            write_enrolled(kept)
    except (ValueError, OSError) as exc:  # includes ManifestError and a held lock
        print(f'bt night: {exc}', file=sys.stderr)
        return 2
    print(f"enrolled {root}: {len(items)} recipe variant(s) in {manifest['path']}; `bt night run` submits them each evening "
          f'({len(kept)} worktree(s) enrolled)')
    if not task_installed():
        print(f'note: the scheduled task "{TASK}" is not installed on this machine; `bt night schedule install` adds it')
    return 0


def cmd_unenroll(args):
    """Anyone on the machine may remove an enrolment (the worktree's session may be gone): it only ends a standing
    action."""
    target = args.worktree or os.getcwd()
    root = paths.find_worktree_root(target) if os.path.isdir(target) else None
    try:
        with night._SubmitLock():
            rows = read_enrolled()
            kept = [r for r in rows if not _same(r['root'], root or target)]
            if len(kept) != len(rows):
                write_enrolled(kept)
    except (ValueError, OSError) as exc:
        print(f'bt night: {exc}', file=sys.stderr)
        return 2
    if len(kept) == len(rows):
        print(f'not enrolled: {root or target}')
        return 1
    print(f'unenrolled {root or target}; {len(kept)} worktree(s) still enrolled')
    return 0


# ---------------------------------------------------------------- the scheduled run

def run_log_path(night_s, state=None):
    return night.ledger_path(state).with_name('runs') / f'{night_s}.log'


class _RunLog:
    """The night's run log, written line by line as the run goes: a run that hangs or is ended by the task's time
    limit has still said what it did."""

    def __init__(self, night_s):
        self.path, self.lines, self.broken = run_log_path(night_s), [], None

    def add(self, *lines):
        self.lines += lines
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, 'a', encoding='utf-8', errors='backslashreplace', newline='\n') as handle:
                handle.write(''.join(line + '\n' for line in lines))
        except OSError as exc:
            self.broken = self.broken or f'could not write the run log: {exc}'

    def text(self):
        return '\n'.join(self.lines + ([self.broken] if self.broken else [])) + '\n'


def _kill_tree(proc):
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        if procs.platform_name() == 'windows':   # git's transport and credential helpers are its children and outlive a plain kill
            subprocess.run(['taskkill', '/T', '/F', '/PID', str(proc.pid)], stdin=subprocess.DEVNULL,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20,
                           creationflags=config.CREATE_NO_WINDOW)
        else:   # quiet_git starts git as the leader of its own group, so its helpers go with it
            procs.kill_process_group(proc)
        proc.kill()
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        proc.wait(5)


def quiet_git(root, *args, timeout):
    """One git command that reaches the network, run with nobody there. True when it exited 0 in time.
    No pipes: with captured output a transport or credential helper that outlives git holds the call open long after
    the timeout. No prompt: none for the terminal, none from Git Credential Manager, and ssh in batch mode (appended
    to the ssh command git would use, so the client and its keys stay the configured ones). No automatic repacking of
    a repository live sessions are using. At the timeout the whole process tree is ended."""
    env = dict(os.environ, GIT_TERMINAL_PROMPT='0', GCM_INTERACTIVE='never')
    ssh = env.get('GIT_SSH_COMMAND') or gitutil.out(root, 'config', '--get', 'core.sshCommand')
    if ssh or not env.get('GIT_SSH'):
        env['GIT_SSH_COMMAND'] = (ssh or 'ssh') + ' -oBatchMode=yes'
    try:
        proc = subprocess.Popen(['git', '-C', str(root), '-c', 'credential.interactive=false', '-c', 'gc.auto=0',
                                 '-c', 'maintenance.auto=false', *args], env=env, stdin=subprocess.DEVNULL,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=config.CREATE_NO_WINDOW,
                                **procs.popen_group_kwargs())
    except OSError:
        return False
    try:
        return proc.wait(timeout) == 0
    except subprocess.TimeoutExpired:
        _kill_tree(proc)
        return False
    except KeyboardInterrupt:   # git no longer shares the terminal's group on POSIX, so Ctrl+C does not reach it
        _kill_tree(proc)
        raise


def fetch_remotes(root, manifest):
    """Refresh the remotes the enabled recipes' refs name, so a nightly ``origin/master`` is today's master and not
    the one this checkout last fetched. Returns notes for the remotes that could not be fetched: their refs are
    still submitted as they stand, which is what a session's own ``bt night submit`` does."""
    remotes = set(gitutil.out(root, 'remote').split())
    wanted = sorted({spec['ref'].split('/', 1)[0] for spec in manifest['recipes'] if spec['enabled'] and '/' in spec['ref']}
                    & remotes)
    return [f'fetch of {remote} failed; its refs are submitted as this checkout last saw them' for remote in wanted
            if not quiet_git(root, 'fetch', '--quiet', '--no-tags', remote, timeout=FETCH_TIMEOUT)]


def run_one(row, args, night_s):
    """Submit for one enrolled worktree, without waiting for host registration (cmd_run waits once for every
    worktree's jobs, after the submit lock is released: a session's own ``bt night submit`` gives up after a minute
    of waiting for that lock). Returns (ledger row of kind ``run`` with the submitted ``jobs``, the text printed).
    It never raises: whatever goes wrong here is this worktree's problem and the run goes on to the next."""
    result = {'kind': 'run', 'at': night._now(), 'night': night_s, 'root': row['root'], 'repo': row.get('repo'),
              'workstream': row.get('workstream'), 'code': 2, 'submitted': 0, 'skipped': 0, 'failed': 0, 'registered': 0,
              'problem': None, 'notes': [], 'jobs': []}
    out = io.StringIO()
    try:
        _run_one(row, args, night_s, result, out)
    except Exception as exc:
        result['problem'] = f'unexpected error, nothing more was done for this worktree: {type(exc).__name__}: {exc}'[:300]
    return result, out.getvalue()


def _run_one(row, args, night_s, result, out):
    again = 'its owner enrols it again with `bt night enroll`, or `bt night unenroll --worktree` removes it'
    if not os.path.isabs(row['root']):
        result['problem'] = 'the enrolled path is not absolute; ' + again
        return
    root = Path(row['root'])
    found = paths.find_worktree_root(root) if root.is_dir() else None
    if found is None or not _same(found, root):
        result['problem'] = 'the worktree no longer exists; `bt night unenroll --worktree` removes it'
        return
    if not isinstance(row.get('identity'), dict) or _identity(root) != row['identity']:
        result['problem'] = 'the directory at this path is not the one that was enrolled; ' + again
        return
    session = compute_pickup.read(root / '.baton/owner.json').get('session_id')
    if not isinstance(session, str) or not session:
        result['problem'] = 'no session owns this worktree (.baton/owner.json has no session); nothing was submitted'
        return
    try:
        result['notes'] = fetch_remotes(root, night.load_manifest(night.find_manifest(root)))
    except ValueError:
        pass  # the submit below reports the manifest error itself
    stats = {}
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        # the night this run was started for, for every worktree: a run that crosses 08:00 must not split
        result['code'] = night.cmd_submit(argparse.Namespace(
            worktree=str(root), manifest=None, night=night_s, session=session, defer=args.defer, retry_failed=False,
            wait_registered=None, lock_wait=LOCK_WAIT), stats)
    result.update({k: stats[k] for k in ('submitted', 'skipped', 'failed') if type(stats.get(k)) is int})
    result['jobs'] = [j for j in stats.get('jobs') or [] if isinstance(j, str)]
    if stats.get('error'):
        result['problem'] = str(stats['error'])[:300]
    elif result['failed']:
        result['problem'] = f"{result['failed']} submission(s) failed"


def wait_for_hosts(results, timeout):
    """One wait for every worktree's jobs. Sets each result's ``registered`` and, when a job is not host-registered
    by the end, its problem (kept when it already has one). Returns the lines to log. It never raises."""
    jobs = [job for result in results for job in result['jobs']]
    if not jobs:
        return []
    try:
        state = night.wait_registered(night._engine(), jobs, timeout)
    except Exception as exc:  # the jobs are submitted either way; only their registration is unknown
        state = {job: (False, f'status unavailable: {exc}'[:120]) for job in jobs}
    lines = []
    for result in results:
        result['registered'] = sum(1 for job in result['jobs'] if state[job][0])
        late = len(result['jobs']) - result['registered']
        if late and not result['problem']:
            result['problem'] = (f"{late} of {len(result['jobs'])} job(s) were not host-registered when the run ended; "
                                 'they do not start until the laptop is awake again')
        lines += [f"   {job}: {'host-registered' if state[job][0] else 'NOT host-registered (' + str(state[job][1]) + ')'}"
                  for job in result['jobs']]
    return lines


def _record(rows, log):
    try:
        with _locked():
            night.append_rows(rows)
    except (ValueError, OSError) as exc:  # the run still happened; only the report loses its lines
        log.add(f'   could not record this in the ledger: {exc}')


def cmd_run(args):
    try:
        when, note = night.night_date(night._now(), args.night)
    except ValueError as exc:
        print(f'bt night: {exc}', file=sys.stderr)
        return 2
    night_s = when.isoformat()
    rows = read_enrolled()
    log = _RunLog(night_s)
    log.add(f"{dt.datetime.now().astimezone().isoformat(timespec='seconds')} bt night run for night {night_s}: "
            f'{len(rows)} enrolled worktree(s)' + (f' ({note})' if note else ''))
    results = []
    for row in rows:
        result, text = run_one(row, args, night_s)
        results.append(result)
        log.add(f"== {result['root']}: " + (result['problem'] or f"{result['submitted']} submitted, {result['skipped']} skipped"),
                *('   note: ' + n for n in result['notes']), *('   ' + line for line in text.splitlines()))
        _record([result], log)   # now, not at the end: the wait below can outlast the task's time limit
    waiting = [dict(result) for result in results if result['jobs']]
    before = [result['problem'] for result in waiting]
    lines = wait_for_hosts(waiting, args.wait_registered)
    for result, old in zip(waiting, before):
        result['at'] = night._now()   # the later row of a worktree is the one the report and the status read
        if result['problem'] != old:
            lines.append(f"== {result['root']}: {result['problem']}")
    if waiting:
        log.add(*lines)
        _record(waiting, log)
    print(log.text(), end='')
    final = {result['root']: result for result in results + waiting}
    return 1 if any(result['problem'] for result in final.values()) else 0


# ---------------------------------------------------------------- the Windows task

def _windows():
    return os.name == 'nt'


def _schtasks(*args):
    """(returncode, output) of one schtasks call; (127, reason) when it cannot be started."""
    try:
        done = subprocess.run(['schtasks', *args], capture_output=True, text=True, errors='replace', timeout=60,
                              stdin=subprocess.DEVNULL, creationflags=config.CREATE_NO_WINDOW)
    except (OSError, subprocess.SubprocessError) as exc:
        return 127, str(exc)
    return done.returncode, (done.stdout or '') + (done.stderr or '')


def task_installed():
    return _windows() and _schtasks('/Query', '/TN', TASK)[0] == 0


def task_python():
    """The windowless interpreter beside this one when there is one: the task must not flash a console every evening."""
    exe = Path(shutil.which(str(config.PYTHON)) or config.PYTHON)
    quiet = exe.with_name('pythonw.exe')
    return str(quiet if quiet.is_file() else exe)


def task_xml(at, python, script, user, start_day):
    """Task Scheduler definition: daily at ``at`` local time, as the logged-on user without stored credentials, started
    late when the laptop was off or asleep at that time (StartWhenAvailable), never waking it, one instance at a time,
    also on battery (a laptop left unplugged in the evening would otherwise skip the night)."""
    return f'''<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Runs "bt night run": submits the night manifests of the worktrees enrolled with "bt night enroll" to the compute queue's overnight window. Remove with "bt night schedule remove".</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>{start_day}T{at}:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>
    </CalendarTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{escape(user)}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable>
    <WakeToRun>false</WakeToRun>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <ExecutionTimeLimit>PT2H</ExecutionTimeLimit>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{escape(python)}</Command>
      <Arguments>{escape(f'"{script}" night run')}</Arguments>
      <WorkingDirectory>{escape(str(PureWindowsPath(script).parent))}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
'''


def _user():
    domain = os.environ.get('USERDOMAIN')
    name = os.environ.get('USERNAME') or getpass.getuser()
    return f'{domain}\\{name}' if domain else name


def _macos():
    return config.MACOS and not _windows()


def cmd_schedule(args):
    kind = 'remove' if args.schedule_cmd == 'uninstall' else args.schedule_cmd
    if _windows():
        return _schedule_windows(args, kind)
    if _macos():
        return _schedule_macos(args, kind)
    print(f'bt night schedule: a scheduled task exists on Windows (Task Scheduler) and macOS (LaunchAgent), not on '
          f'{sys.platform}; run `bt night run` from cron there', file=sys.stderr)
    return 2


def _schedule_windows(args, kind):
    if kind == 'install':
        if not CLOCK.fullmatch(args.at):
            print(f'bt night schedule: --at must be HH:MM (24-hour, local time): {args.at}', file=sys.stderr)
            return 2
        python, script = task_python(), str(config.INSTALL_HOME / 'bt.py')
        if '%' in python + script:   # Task Scheduler expands %NAME% in the command and its arguments
            print(f'bt night schedule: a path with a percent sign cannot be scheduled: {python} {script}', file=sys.stderr)
            return 2
        replaced = task_installed()
        xml = task_xml(args.at, python, script, _user(), dt.date.today().isoformat())
        fd, name = tempfile.mkstemp(prefix='baton-night-task-', suffix='.xml')
        try:
            with os.fdopen(fd, 'w', encoding='utf-16', newline='\r\n') as handle:
                handle.write(xml)
            code, out = _schtasks('/Create', '/TN', TASK, '/XML', name, '/F')
        finally:
            with contextlib.suppress(OSError):
                os.unlink(name)
        if code:
            print(f'bt night schedule: schtasks could not create the task ({code}): {out.strip()[:400]}', file=sys.stderr)
            return 1
        print(f'{"replaced the existing" if replaced else "installed"} "{TASK}": `bt night run` daily at {args.at} local '
              f'time, or when the laptop is next awake after that; {len(read_enrolled())} worktree(s) enrolled')
        return 0
    if kind == 'remove':
        if not task_installed():
            print(f'"{TASK}" is not installed')
            return 0
        code, out = _schtasks('/Delete', '/TN', TASK, '/F')
        if code:
            print(f'bt night schedule: schtasks could not remove the task ({code}): {out.strip()[:400]}', file=sys.stderr)
            return 1
        print(f'removed "{TASK}"; enrolments are kept and `bt night run` still works by hand')
        return 0
    code, out = _schtasks('/Query', '/TN', TASK, '/V', '/FO', 'LIST')
    if code:
        print(f'"{TASK}" is not installed (`bt night schedule install`)')
    else:
        keep = ('Status:', 'Next Run Time:', 'Last Run Time:', 'Last Result:', 'Task To Run:', 'Start Time:')
        shown = [line.strip() for line in out.splitlines() if line.strip().startswith(keep)]
        print(f'"{TASK}" is installed')
        print('\n'.join('  ' + line for line in (shown or out.strip().splitlines())))
    _print_enrolled()
    return 0


def _print_enrolled():
    rows = read_enrolled()
    print(f'{len(rows)} worktree(s) enrolled' + (':' if rows else ' (`bt night enroll` in a worktree with a manifest)'))
    runs = [r for r in night.read_ledger() if r.get('kind') == 'run']
    for row in rows:
        last = max((r for r in runs if _same(r.get('root') or '', row['root'])), key=lambda r: r.get('at') or 0, default=None)
        seen = 'never run' if last is None else (f"last run for night {last.get('night')}: " + (
            last.get('problem') or f"{last.get('submitted')} submitted, {last.get('skipped')} skipped"))
        print(f"  {row['root']}  ({seen})")


# ---------------------------------------------------------------- the macOS LaunchAgent

def night_plist(at, python, script):
    """LaunchAgent definition: ``bt night run`` daily at ``at`` local time (``StartCalendarInterval``), in the
    background, logging to baton's state directory, with a PATH that finds git and ssh (see ``launchd.agent_path``)."""
    hour, minute = (int(part) for part in at.split(':'))
    return launchd.build_plist(launchd.NIGHT_LABEL, [python, script, 'night', 'run'], python,
                               StartCalendarInterval={'Hour': hour, 'Minute': minute})


def _schedule_macos(args, kind):
    label = launchd.NIGHT_LABEL
    if kind == 'install':
        if not CLOCK.fullmatch(args.at):
            print(f'bt night schedule: --at must be HH:MM (24-hour, local time): {args.at}', file=sys.stderr)
            return 2
        done = launchd.install(label, night_plist(args.at, launchd.python_exe(), str(config.INSTALL_HOME / 'bt.py')),
                              force=getattr(args, 'force', False))
        if done.error:
            print(f'bt night schedule: {done.error}', file=sys.stderr)
            return 1
        print(f'{"replaced the existing" if done.replaced else "installed"} LaunchAgent "{label}" ({done.path}): `bt night run` '
              f'daily at {args.at} local time. If the Mac is asleep at that time the run happens when it wakes; if it is '
              f'switched off at that time that night is skipped (launchd does not catch up after a shutdown); '
              f'{len(read_enrolled())} worktree(s) enrolled')
        return 0
    if kind == 'remove':
        removed, error = launchd.uninstall(label)
        if error:
            print(f'bt night schedule: {error}', file=sys.stderr)
            return 1
        print(f'removed LaunchAgent "{label}"; enrolments are kept and `bt night run` still works by hand' if removed
              else f'LaunchAgent "{label}" is not installed')
        return 0
    st = launchd.status(label)
    if not st.installed:
        print(f'LaunchAgent "{label}" is not installed (`bt night schedule install`)' + (' but is loaded' if st.loaded else ''))
    else:
        when = st.plist.get('StartCalendarInterval') or {}
        state = ('loaded' + (f', last exit code {st.last_exit}' if st.last_exit is not None else '') if st.loaded
                 else 'not loaded (`bt night schedule install` loads it)')
        print(f'LaunchAgent "{label}" is installed, {state}')
        print(f'  plist: {st.path}')
        print(f"  runs: {' '.join(st.plist.get('ProgramArguments') or [])}")
        if when:
            print(f"  daily at: {int(when.get('Hour', 0)):02d}:{int(when.get('Minute', 0)):02d} local time")
        print(f"  logs: {st.plist.get('StandardOutPath')}, {st.plist.get('StandardErrorPath')}")
    _print_enrolled()
    return 0


# ---------------------------------------------------------------- command line

def register(actions):
    enroll = actions.add_parser('enroll', help='Let the scheduled task submit this worktree\'s manifest every evening')
    enroll.add_argument('--worktree')
    enroll.add_argument('--session', help='Your own session identity (must own the worktree)')
    enroll.set_defaults(func=cmd_enroll)
    unenroll = actions.add_parser('unenroll', help='Stop the scheduled task submitting for a worktree')
    unenroll.add_argument('--worktree')
    unenroll.set_defaults(func=cmd_unenroll)
    run = actions.add_parser('run', help='Submit for every enrolled worktree (what the scheduled task runs)')
    run.add_argument('--night', metavar='YYYY-MM-DD', help='Override the computed night date')
    run.add_argument('--defer', choices=('overnight', 'spare'), default='overnight')
    run.add_argument('--wait-registered', type=int, default=RUN_WAIT, metavar='SECONDS',
                     help=f'Poll up to this long, once for all submitted jobs, for host registration (default {RUN_WAIT})')
    run.set_defaults(func=cmd_run)
    schedule = actions.add_parser('schedule', help='Install, remove or show the evening task that runs `bt night run`')
    kinds = schedule.add_subparsers(dest='schedule_cmd', required=True)
    install = kinds.add_parser('install', help='Create or replace the evening task (Windows scheduled task, macOS LaunchAgent)')
    install.add_argument('--at', default=DEFAULT_AT, metavar='HH:MM', help=f'Local start time (default {DEFAULT_AT})')
    install.add_argument('--force', action='store_true', help='macOS: replace the agent even while its run is in progress (that run is stopped)')
    kinds.add_parser('remove', aliases=['uninstall'], help='Delete the scheduled task; enrolments are kept')
    kinds.add_parser('status', help='Show the task, the enrolled worktrees and their last run')
    schedule.set_defaults(func=cmd_schedule)
