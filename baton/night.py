"""Standing overnight checks: ``bt night plan|submit|report``.

Submits through the existing compute-queue engine and reads existing records. It
schedules nothing itself and starts no daemon; the queue's deferred "overnight"
window decides when jobs run.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import html
import itertools
import json
import math
import os
from pathlib import Path
import re
import sys
import time
import tomllib
import unicodedata

from . import compute_pickup, config, gitutil, harvest, locking, paths

TERMINAL = compute_pickup.TERMINAL
NAME = re.compile(r'[a-z0-9][a-z0-9_-]{0,39}')
VAR_KEY = re.compile(r'[a-z][a-z0-9_]{0,19}')
VAR_VALUE = re.compile(r'[A-Za-z0-9_.-]{1,40}')
TOKEN = re.compile(r'\{([^{}]*)\}')
# Same two forms as the compute-queue allowlist: a flat saved script plus plain arguments.
COMMANDS = (re.compile(r'(?:bash|sh) \.baton/remote/[A-Za-z0-9_][A-Za-z0-9_.-]*\.sh(?: [A-Za-z0-9_./:=-]+)*'),
            re.compile(r'python3? \.baton/remote/[A-Za-z0-9_][A-Za-z0-9_.-]*\.py(?: [A-Za-z0-9_./:=-]+)*'))
REF = re.compile(r'[A-Za-z0-9_./~^@][A-Za-z0-9_./~^@-]{0,199}')
TOOL = re.compile(r'[a-zA-Z0-9_.+-]{1,64}')
ANSI = re.compile(r'\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b\[[0-?]*[ -/]*[@-~]|\x1b[@-Z\\-_]')
IMAGES = ('.png', '.jpg', '.jpeg', '.webp', '.gif')
DEFAULT_BUDGET = 32.0  # slot-hours per night: a quarter of one 16-slot host's window
MAX_VARIANTS, MAX_JOBS, MAX_GETS = 64, 256, 64
RECIPE_KEYS = {'name', 'command', 'subdir', 'ref', 'slots', 'memory_gb', 'memory_max_gb', 'est_minutes',
               'timeout_minutes', 'gets', 'requires', 'candidates', 'platform', 'rerun', 'toolchain', 'enabled', 'inputs',
               'why', 'matrix', 'artifact_max_mb', 'artifact_total_mb'}
LOCAL_ONLY = 'worktree'


class ManifestError(ValueError):
    """The manifest (or its interaction with the worktree) is invalid."""


# ---------------------------------------------------------------- manifest

def _fail(name, key, message):
    where = f'recipe {name!r} ' if name else ''
    raise ManifestError(f'{where}key {key}: {message}')


def _num(value, low, high):
    return (type(value) in (int, float) and math.isfinite(value) and low <= value <= high)


def _relative(name, key, value):
    if (not isinstance(value, str) or not value or '\\' in value or value.startswith('/')
            or re.match(r'[A-Za-z]:', value) or '..' in value.split('/') or any(ord(c) < 32 for c in value)):
        _fail(name, key, f'{value!r} must be a relative path with no "..", no backslash and no drive')
    return value


def _subst(text, values, name, key, strict=True):
    def repl(match):
        token = match.group(1)
        if token not in values:
            _fail(name, key, f'unknown token {match.group(0)}')
        return values[token]
    out = TOKEN.sub(repl, text)
    if strict and ('{' in out or '}' in out):
        _fail(name, key, 'unbalanced braces')
    return out


def check_command(name, command):
    if '..' in command or not any(p.fullmatch(command) for p in COMMANDS):
        _fail(name, 'command', f'{command!r} is not allowed: use "bash|sh .baton/remote/<script>.sh" or '
              '"python3 .baton/remote/<script>.py" (flat file, no subdirectory) followed by plain arguments '
              '(letters, digits and _ . / : = -); inline shell, environment assignments, pipes, quotes and '
              'wildcards belong inside the saved script')


def _recipe(raw, defaults, index):
    name = raw.get('name')
    if not isinstance(name, str) or not NAME.fullmatch(name):
        _fail(name if isinstance(name, str) else None, 'name', f'recipe #{index + 1}: must match [a-z0-9][a-z0-9_-]{{0,39}}')
    unknown = sorted(set(raw) - RECIPE_KEYS)
    if unknown:
        _fail(name, unknown[0], 'unknown key')
    spec = {**defaults, **raw}
    command = spec.get('command')
    if not isinstance(command, str) or not command:
        _fail(name, 'command', 'required')
    est = spec.get('est_minutes')
    if type(est) is not int or not 1 <= est <= 480:
        _fail(name, 'est_minutes', 'required, an integer in 1..480')
    slots = spec.get('slots', 1)
    if type(slots) is not int or not 1 <= slots <= 32:
        _fail(name, 'slots', 'must be an integer in 1..32')
    timeout = spec.get('timeout_minutes', min(480, max(30, 3 * est)))
    if type(timeout) is not int or not 1 <= timeout <= 480:
        _fail(name, 'timeout_minutes', 'must be an integer in 1..480')
    cap = spec.get('memory_max_gb')
    if not _num(cap, .25, 50):
        _fail(name, 'memory_max_gb', 'required, a number in 0.25..50 (GiB)')
    memory = spec.get('memory_gb')
    if memory is not None and (not _num(memory, .25, 50) or memory > cap):
        _fail(name, 'memory_gb', 'must be a number in 0.25..50 and at most memory_max_gb')
    platform = spec.get('platform', 'linux')
    if platform not in ('linux', 'windows', 'portable'):
        _fail(name, 'platform', 'must be linux, windows or portable')
    candidates = spec.get('candidates', [])
    if (not isinstance(candidates, list) or any(not isinstance(c, str) or not c for c in candidates)
            or (raw.get('candidates') is not None and not candidates)):
        _fail(name, 'candidates', 'must be a nonempty list of host IDs')
    if 'laptop' in candidates:
        _fail(name, 'candidates', 'deferred jobs cannot use the laptop')
    requires = spec.get('requires', [])
    if (not isinstance(requires, list) or len(requires) > 32
            or any(not isinstance(t, str) or not TOOL.fullmatch(t) for t in requires)):
        _fail(name, 'requires', 'must be a list of tool names')
    rerun = spec.get('rerun', 'on-change')
    if rerun not in ('on-change', 'always'):
        _fail(name, 'rerun', 'must be on-change or always')
    inputs = spec.get('inputs')
    if inputs is not None and (not isinstance(inputs, list) or len(inputs) > 32 or any(
            not isinstance(i, str) or not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,99}', i) for i in inputs)):
        _fail(name, 'inputs', 'must be a list of at most 32 flat file names under .baton/remote')
    toolchain = spec.get('toolchain', '')
    if not isinstance(toolchain, str) or len(toolchain) > 100:
        _fail(name, 'toolchain', 'must be a string of at most 100 characters')
    enabled = spec.get('enabled', True)
    if type(enabled) is not bool:
        _fail(name, 'enabled', 'must be true or false')
    why = spec.get('why', '')
    if not isinstance(why, str):
        _fail(name, 'why', 'must be a string')
    ref = spec.get('ref', 'HEAD')
    if not isinstance(ref, str) or not (ref == 'WORKTREE' or REF.fullmatch(ref)):
        _fail(name, 'ref', 'must be HEAD, WORKTREE or a ref/sha')
    artifacts = {}
    for key, high in (('artifact_max_mb', 1024), ('artifact_total_mb', 4096)):
        if spec.get(key) is not None:
            if type(spec[key]) is not int or not 1 <= spec[key] <= high:
                _fail(name, key, f'must be an integer in 1..{high}')
            artifacts[key] = spec[key]
    if len(artifacts) == 2 and artifacts['artifact_max_mb'] > artifacts['artifact_total_mb']:
        _fail(name, 'artifact_max_mb', 'must not exceed artifact_total_mb')
    gets = spec.get('gets', [])
    if not isinstance(gets, list) or len(gets) > MAX_GETS or any(not isinstance(g, str) for g in gets):
        _fail(name, 'gets', f'must be a list of at most {MAX_GETS} relative paths')
    subdir = _relative(name, 'subdir', spec.get('subdir', '.'))
    if subdir != '.':
        # the job starts in subdir, where `.baton/remote/<script>` does not exist; the script changes directory itself
        _fail(name, 'subdir', 'must be "." (carried scripts are addressed from the repository root)')
    matrix = raw.get('matrix', {})
    if not isinstance(matrix, dict):
        _fail(name, 'matrix', 'must be a table of lists')
    total = 1
    for key, values in matrix.items():
        if not VAR_KEY.fullmatch(key) or key in ('night', 'sha8'):
            _fail(name, 'matrix', f'key {key!r} must match [a-z][a-z0-9_]{{0,19}} and not be night or sha8')
        if (not isinstance(values, list) or not values or any(not isinstance(v, str) or not VAR_VALUE.fullmatch(v) for v in values)
                or len(set(values)) != len(values)):
            _fail(name, f'matrix.{key}', 'must be a nonempty list of distinct strings matching [A-Za-z0-9_.-]{1,40}')
        total *= len(values)
    if total > MAX_VARIANTS:
        _fail(name, 'matrix', f'{total} variants exceeds the limit of {MAX_VARIANTS}')
    keys = sorted(matrix)
    variants = [dict(zip(keys, combo)) for combo in itertools.product(*(matrix[k] for k in keys))]
    for variant in variants:
        probe = {**variant, 'night': '00000000', 'sha8': '00000000'}
        check_command(name, _subst(command, probe, name, 'command'))
        for g in gets:
            _relative(name, 'gets', _subst(g, probe, name, 'gets'))
    return dict(name=name, command=command, subdir=subdir, ref=ref, slots=slots, est_minutes=est, timeout_minutes=timeout,
                inputs=None if inputs is None else sorted(set(inputs)),
                memory_max_gb=float(cap), memory_gb=None if memory is None else float(memory), platform=platform,
                candidates=candidates, requires=requires, rerun=rerun, toolchain=toolchain, enabled=enabled, why=why,
                gets=gets, artifacts=artifacts, variants=variants)


def parse_manifest(text, source='night.toml'):
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise ManifestError(f'{source}: {exc}') from exc
    if data.get('version') != 1:
        raise ManifestError('key version: must be 1')
    unknown = sorted(set(data) - {'version', 'budget', 'defaults', 'recipe'})
    if unknown:
        raise ManifestError(f'key {unknown[0]}: unknown key')
    budget = (data.get('budget') or {}).get('slot_hours', DEFAULT_BUDGET)
    if not _num(budget, 0, 1e6):
        raise ManifestError('key budget.slot_hours: must be a non-negative number')
    defaults = data.get('defaults') or {}
    if not isinstance(defaults, dict) or {'name', 'matrix'} & set(defaults):
        raise ManifestError('key defaults: must be a table without name or matrix')
    raws = data.get('recipe') or []
    if not isinstance(raws, list) or not all(isinstance(r, dict) for r in raws):
        raise ManifestError('key recipe: must be an array of tables')
    recipes, seen = [], set()
    for index, raw in enumerate(raws):
        recipe = _recipe(raw, defaults, index)
        if recipe['name'] in seen:
            _fail(recipe['name'], 'name', 'duplicate recipe name')
        seen.add(recipe['name'])
        recipes.append(recipe)
    if sum(len(r['variants']) for r in recipes) > MAX_JOBS:
        raise ManifestError(f'more than {MAX_JOBS} jobs in one manifest')
    return {'budget': float(budget), 'recipes': recipes}


def find_manifest(root, override=None):
    if override:
        path = Path(override)
        if not path.is_file():
            raise ManifestError(f'manifest not found: {path}')
        return path
    for base in (root, paths.main_checkout(root)):
        path = Path(base) / '.baton/night.toml'
        if path.is_file():
            return path
    raise ManifestError(f'no manifest: expected {root / ".baton/night.toml"} or the main checkout\'s copy')


def load_manifest(path):
    path = Path(path)
    try:
        text = path.read_text(encoding='utf-8')
    except (OSError, UnicodeDecodeError) as exc:
        raise ManifestError(f'cannot read {path}: {exc}') from exc
    manifest = parse_manifest(text, str(path))
    manifest['path'] = str(path)
    return manifest


def display_name(name, variant):
    return name + ('[' + ','.join(f'{k}={variant[k]}' for k in sorted(variant)) + ']' if variant else '')


# ---------------------------------------------------------------- night date

def night_date(now, override=None):
    """(date, note): the night a submission at ``now`` (epoch seconds) belongs to, in Europe/London time."""
    if override:
        try:
            return dt.date.fromisoformat(override), None
        except ValueError as exc:
            raise ValueError(f'--night must be YYYY-MM-DD: {override}') from exc
    note = None
    try:
        from zoneinfo import ZoneInfo
        zone = ZoneInfo('Europe/London')
    except Exception:
        zone, note = dt.timezone.utc, 'Europe/London tz data unavailable; night date computed in UTC'
    local = dt.datetime.fromtimestamp(now, zone)
    day = local.date()
    return (day if local.hour < 8 else day + dt.timedelta(days=1)), note


# ---------------------------------------------------------------- ledger

def ledger_path(state=None):
    return Path(state or config.STATE_DIR) / 'night' / 'ledger.jsonl'


def read_ledger(state=None):
    rows = []
    try:
        handle = open(ledger_path(state), 'rb')
    except OSError:
        return rows
    with handle:
        for line in handle:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if isinstance(row, dict) and isinstance(row.get('kind'), str):
                rows.append(row)
    return rows


LOST_AFTER = 3 * 86400  # a submitted job whose queue record cannot be read for this long is recorded as lost


class _SubmitLock:
    """One submitter at a time: plan, submit and the ledger rows that record it must not interleave between sessions
    (two sessions planning from the same ledger would each submit the same recipe). An operating-system lock on a
    file that is never removed: it is held for as long as the submitter runs, however long that is, and the system
    releases it when the process ends, so there is no age to judge and no takeover to race."""
    WAIT = 60

    def __init__(self, state=None):
        self.path = ledger_path(state).with_name('submit.lock')
        self.lock = None

    def __enter__(self):
        try:
            lock = locking.exclusive(self.path, timeout=self.WAIT)
            lock.__enter__()
        except TimeoutError as exc:
            raise ValueError(f'another bt night submit holds {self.path}; retry when it has finished') from exc
        except OSError as exc:
            raise ValueError(f'cannot open the submit lock {self.path}: {exc}') from exc
        self.lock = lock
        return self

    def __exit__(self, *exc):
        if self.lock is not None:
            self.lock.__exit__(None, None, None)
            self.lock = None
        return False


def append_rows(rows, state=None):
    """Append one line per row. Appends from two processes are not atomic on Windows; submitters serialise with
    _SubmitLock, and a row that follows a partial line starts on a fresh line so one bad line never hides the next."""
    if not rows:
        return
    path = ledger_path(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, 'O_BINARY', 0)
    fd = os.open(path, flags, 0o644)
    try:
        for row in rows:
            data = (json.dumps(row, sort_keys=True, ensure_ascii=True, separators=(',', ':')) + '\n').encode('ascii')
            if os.fstat(fd).st_size and not _ends_with_newline(path):
                data = b'\n' + data  # a crashed writer left a partial line; keep this row on its own line
            os.write(fd, data)
    finally:
        os.close(fd)


def _ends_with_newline(path):
    try:
        with open(path, 'rb') as handle:
            handle.seek(-1, os.SEEK_END)
            return handle.read(1) == b'\n'
    except OSError:
        return True


def _host_for(job):
    return 'fleet' if job.startswith(('cq-fleet-', 'cq-v2--fleet-')) else 'ember'


def _record(job, state=None):
    return compute_pickup.job_record(Path(state or config.STATE_DIR) / 'compute', job, _host_for(job))


def _record_on_disk(job, state=None):
    try:
        return any(p.exists() for p in compute_pickup.job_record_paths(Path(state or config.STATE_DIR) / 'compute',
                                                                         job, _host_for(job)))
    except (OSError, ValueError, TypeError, AttributeError):
        return True  # cannot tell: do not declare the job lost


def input_failed(outcome):
    """True when the recipe itself failed on its input. A host or transport fault (no exit code, 75 host disk low,
    255 ssh) and a run that exited 0 but whose artifacts were not delivered say nothing about the input."""
    code = outcome.get('exit')
    return (outcome.get('status') in ('failed', 'done') and outcome.get('passed') is False
            and type(code) is int and code not in (0, 75, 255))


def _minutes(record):
    value = record.get('minutes')
    if type(value) in (int, float):
        return float(value)
    start, end = record.get('t_started'), record.get('t_finished')
    if type(start) in (int, float) and type(end) in (int, float) and end >= start:
        return round((end - start) / 60, 1)
    return None


def derive_outcomes(rows, state=None, now=None):
    """Outcome rows for submitted jobs that have none yet and whose queue record is terminal."""
    have = {r.get('job') for r in rows if r.get('kind') == 'outcome'}
    fresh = []
    for row in rows:
        job = row.get('job')
        if row.get('kind') != 'submitted' or not isinstance(job, str) or job in have:
            continue
        record = _record(job, state)
        age = (now or time.time()) - row['at'] if type(row.get('at')) in (int, float) else 0
        if not record and (age > 2 * LOST_AFTER or (age > LOST_AFTER and not _record_on_disk(job, state))):
            # pruned or migrated: without an outcome the recipe would stay "already queued" for good. A record
            # that exists but could not be read this time (mid-rewrite, a sharing violation) is read again later;
            # one that still cannot be read after twice the time (oversized, a link, two conflicting copies) is lost.
            have.add(job)
            fresh.append({'kind': 'outcome', 'at': now or time.time(), 'job': job, 'key': row.get('key'),
                          'recipe': row.get('recipe'), 'display': row.get('display'), 'repo': row.get('repo'),
                          'night': row.get('night'), 'status': 'lost', 'exit': None, 'minutes': None,
                          'artifact_missing': 0, 'passed': False})
            continue
        if record.get('status') not in TERMINAL:
            continue
        have.add(job)
        missing = record.get('artifact_missing')
        missing = len(missing) if isinstance(missing, list) else 0
        code = record.get('exit') if type(record.get('exit')) is int else None
        finished = record.get('t_finished')
        fresh.append({'kind': 'outcome', 'at': finished if type(finished) in (int, float) else (now or time.time()),
                      'job': job, 'key': row.get('key'), 'recipe': row.get('recipe'), 'display': row.get('display'),
                      'repo': row.get('repo'), 'night': row.get('night'), 'status': record['status'], 'exit': code,
                      'minutes': _minutes(record), 'artifact_missing': missing,
                      'passed': record['status'] == 'done' and code == 0 and not missing})
    return fresh


# ---------------------------------------------------------------- git and keys

def remote_digest(root):
    """Digest of every regular file under .baton/remote (relative path and content), as carried into a job."""
    base = Path(root) / '.baton/remote'
    if base.is_symlink():
        raise ManifestError('.baton/remote is a link; it is not carried')
    if not base.is_dir():
        return hashlib.sha256(b'').hexdigest()
    entries = []
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = sorted(d for d in dirnames if d != '__pycache__')
        for d in dirnames:
            if (Path(dirpath) / d).is_symlink():
                raise ManifestError(f'.baton/remote holds a link ({d}); only regular files are carried')
        for name in sorted(filenames):
            if name.endswith('.pyc'):
                continue
            full = Path(dirpath) / name
            if full.is_symlink() or not full.is_file():
                raise ManifestError(f'.baton/remote holds {name}, which is not a regular file')
            rel = full.relative_to(base).as_posix()
            entries.append([rel, hashlib.sha256(full.read_bytes()).hexdigest()])
    entries.sort()
    return hashlib.sha256(json.dumps(entries, separators=(',', ':')).encode()).hexdigest()


def inputs_digest(root, script, inputs):
    """Digest of a recipe's script and the carried files it declares as inputs."""
    base = Path(root) / '.baton/remote'
    entries = []
    for name in sorted({script, *inputs}):
        full = base / name
        if full.is_symlink() or not full.is_file():
            raise ManifestError(f'.baton/remote/{name} (a recipe script or input) is missing or not a regular file')
        entries.append([name, hashlib.sha256(full.read_bytes()).hexdigest()])
    return hashlib.sha256(json.dumps(entries, separators=(',', ':')).encode()).hexdigest()


def resolve_ref(root, ref):
    """(commit sha, tree sha) of ``ref``; (None, None) for WORKTREE."""
    if ref == 'WORKTREE':
        return None, None
    sha = gitutil.out(root, 'rev-parse', '--verify', '-q', ref + '^{commit}')
    tree = gitutil.out(root, 'rev-parse', '--verify', '-q', sha + '^{tree}') if sha else ''
    if not re.fullmatch(r'[0-9a-f]{40}', sha) or not tree:
        raise ManifestError(f'ref {ref!r} does not resolve to a commit in this repository')
    return sha, tree


def dedupe_key(common, spec, variant, command, tree, remote):
    doc = {'repo': common, 'recipe': spec['name'], 'variant': variant, 'command': command, 'subdir': spec['subdir'],
           'tree': tree, 'remote': remote, 'platform': spec['platform'], 'candidates': sorted(spec['candidates']),
           'requires': sorted(spec['requires']), 'toolchain': spec['toolchain']}
    return hashlib.sha256(json.dumps(doc, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()


# ---------------------------------------------------------------- plan

def plan(root, manifest, night, rows, budget=None, retry_failed=False):
    """One decision per variant. ``rows`` is the ledger plus any derived outcomes; nothing is written."""
    root = Path(root)
    night = str(night)
    budget = manifest['budget'] if budget is None else budget
    common_dir = paths.git_common_dir(root)
    common = os.path.normcase(os.path.realpath(str(common_dir or root)))
    remote = remote_digest(root)
    refs = {}
    repo, workstream = paths.repo_name(root), paths.ws_name(root)
    outcome_jobs = {r.get('job') for r in rows if r.get('kind') == 'outcome'}
    outcomes = [(i, r) for i, r in enumerate(rows) if r.get('kind') == 'outcome']
    pending = [r for r in rows if r.get('kind') == 'submitted' and r.get('job') not in outcome_jobs]
    # the budget is per night, not per invocation: what earlier submissions for this night still hold counts
    # (finished jobs included: the cap is on what a night requests, not on what is outstanding at this moment).
    # It counts every repository's submissions: the ledger is machine-wide and the cap protects the shared hosts.
    used = sum((r.get('slots') or 0) * (r.get('est_minutes') or 0) / 60 for r in rows
               if r.get('kind') == 'submitted' and r.get('night') == night
               and all(type(r.get(k)) in (int, float) for k in ('slots', 'est_minutes')))
    items = []
    for spec in manifest['recipes']:
        for variant in spec['variants']:
            display = display_name(spec['name'], variant)
            item = {'recipe': spec['name'], 'variant': variant, 'display': display, 'ref': spec['ref'], 'repo': repo,
                    'workstream': workstream, 'root': str(root), 'slots': spec['slots'], 'est_minutes': spec['est_minutes'],
                    'hours': spec['slots'] * spec['est_minutes'] / 60, 'why': spec['why'], 'spec': spec,
                    'sha': None, 'key': None, 'decision': 'submit', 'reason': ''}
            items.append(item)
            if not spec['enabled']:
                item.update(decision='skip', reason='disabled')
                continue
            if spec['ref'] not in refs:
                refs[spec['ref']] = resolve_ref(root, spec['ref'])
            sha, tree = refs[spec['ref']]
            values = {**variant, 'night': '{night}', 'sha8': '{sha8}'}
            keyed = _subst(spec['command'], values, spec['name'], 'command', strict=False)
            stable = sha is not None
            script_name = spec['command'].split(' ')[1].rsplit('/', 1)[-1]
            # With declared inputs the key follows the recipe's own files; without, every carried file counts,
            # which is safe but re-runs the recipe whenever any session script in .baton/remote changes.
            carried = remote if spec['inputs'] is None else inputs_digest(root, script_name, spec['inputs'])
            item.update(sha=sha, tree=tree, key=dedupe_key(common, spec, variant, keyed, tree or LOCAL_ONLY, carried))
            shown = {**variant, 'night': night.replace('-', ''), 'sha8': sha[:8] if sha else LOCAL_ONLY}
            item['command'] = _subst(spec['command'], shown, spec['name'], 'command')
            item['gets'] = [_relative(spec['name'], 'gets', _subst(g, shown, spec['name'], 'gets')) for g in spec['gets']]
            check_command(spec['name'], item['command'])
            script = root / item['command'].split(' ')[1]
            if script.is_symlink() or not script.is_file():
                raise ManifestError(f"recipe {spec['name']!r} key command: script {item['command'].split(' ')[1]} "
                                    f'does not exist under {root}')
            # whichever night it was submitted for: a job still waiting or running must not gain a twin. An
            # uncommitted tree has no stable key, so it is matched by recipe and variant in this worktree.
            queued = next((r for r in pending if (r.get('key') == item['key'] if stable else
                           r.get('sha') is None and r.get('display') == display and r.get('root') == item['root'])), None)
            if queued:
                item.update(decision='skip', reason=f"already queued as {queued.get('job')}")
                continue
            if stable:
                newest = max((o for o in outcomes if o[1].get('key') == item['key']),
                             key=lambda o: (o[1].get('at') or 0, o[0]), default=None)
                if spec['rerun'] == 'on-change' and newest and newest[1].get('passed') is True:
                    item.update(decision='skip', reason=f"unchanged input passed on {newest[1].get('night')}")
                    continue
                # The same input failed: running it again every night only repeats the failure in every report.
                # A timeout, a cancellation, a lost job or a host fault says nothing about the input and is retried.
                if spec['rerun'] == 'on-change' and newest and not retry_failed and input_failed(newest[1]):
                    item.update(decision='skip', reason=f"unchanged input failed on {newest[1].get('night')} "
                                f"({newest[1].get('job')}); fix it, or submit with --retry-failed")
                    continue
            if used + item['hours'] > budget + 1e-9:
                item.update(decision='skip', reason=f"over the night budget ({used + item['hours']:g} of {budget:g} slot-hours, "
                            'all repositories)')
                continue
            used += item['hours']
    return items


def totals(items, budget):
    chosen = [i for i in items if i['decision'] == 'submit']
    return {'submit': len(chosen), 'skipped': len(items) - len(chosen), 'slot_hours': round(sum(i['hours'] for i in chosen), 2),
            'budget': budget}


def build_request(item, session, defer):
    spec = item['spec']
    ref = spec['ref']
    # the commit the plan resolved and keyed, never a name resolved again later: a commit made between plan and
    # snapshot would otherwise record a result for a tree that never ran
    source = 'worktree' if ref == 'WORKTREE' else item['sha']
    request = {'command': item['command'], 'root': str(item['root']).replace('\\', '/'), 'subdir': spec['subdir'],
               'where': 'fleet', 'platform': spec['platform'], 'requires': list(spec['requires']), 'defer': defer,
               'slots': spec['slots'], 'memory_max_gb': spec['memory_max_gb'], 'est_minutes': spec['est_minutes'],
               'timeout_minutes': spec['timeout_minutes'], 'label': f"night {item['display']}"[:60], 'session': session,
               'source': source, 'resource_key': 'night:' + spec['name']}
    if spec['candidates']:
        request['candidates'] = list(spec['candidates'])
    if item['gets']:
        request['gets'] = list(item['gets'])
    if spec['memory_gb'] is not None:
        request['memory_gb'] = spec['memory_gb']
    request.update(spec['artifacts'])
    return request


def _identity(item, night, kind, now):
    return {'kind': kind, 'at': now, 'night': night, 'repo': item['repo'], 'workstream': item['workstream'],
            'root': item['root'], 'recipe': item['recipe'], 'variant': item['variant'], 'display': item['display'],
            'key': item['key'], 'sha': item['sha'], 'slots': item['slots'], 'est_minutes': item['est_minutes']}


# ---------------------------------------------------------------- engine

class _Engine:
    def __init__(self, cq, ctx):
        self.cq, self.ctx = cq, ctx

    def submit(self, request):
        return self.cq.execute(self.ctx, 'submit', request)

    def status(self, job_id):
        return self.cq.execute(self.ctx, 'status', {'job': job_id})


def _engine():
    sys.path.insert(0, str(config.INSTALL_HOME / 'mods/compute-queue/bin'))
    import quiet_helper
    quiet_helper.install()
    import cq
    return _Engine(cq, cq.Ctx(state=str(config.STATE_DIR / 'compute')))


def _registered(engine, job):
    try:
        record = (engine.status(job) or {}).get('job') or {}
    except Exception:
        return False, 'status unavailable'
    return record.get('deferred_registered') is True, record.get('deferred_phase') or record.get('status') or 'unknown'


def wait_registered(engine, jobs, timeout, sleep=time.sleep, clock=time.monotonic, interval=5):
    """Query each job once, then poll every ``interval`` s until all are host-registered or ``timeout`` passes."""
    result = {job: (False, 'not queried') for job in jobs}
    deadline = clock() + timeout
    pending = list(jobs)
    while True:
        for job in list(pending):
            result[job] = _registered(engine, job)
            if result[job][0]:
                pending.remove(job)
        if not pending or clock() + interval > deadline:
            return result
        sleep(interval)


# ---------------------------------------------------------------- commands

def _now():
    return time.time()


def _setup(args):
    root = paths.find_worktree_root(args.worktree or os.getcwd())
    if root is None:
        raise ValueError('Use this command in a worktree or specify --worktree')
    manifest = load_manifest(find_manifest(root, args.manifest))
    now = _now()
    night, note = night_date(now, args.night)
    return root, manifest, now, night.isoformat(), note


def _item_view(item):
    return {k: v for k, v in item.items() if k not in ('spec', 'tree')}


def _print_plan(items, night, note, manifest, as_json):
    total = totals(items, manifest['budget'])
    if as_json:
        print(json.dumps({'night': night, 'note': note, 'manifest': manifest['path'], 'totals': total,
                          'items': [_item_view(i) for i in items]}, indent=2))
        return
    if note:
        print('note: ' + note)
    print(f'Night {night}, manifest {manifest["path"]}')
    for item in items:
        sha = 'WORKTREE' if item['ref'] == 'WORKTREE' else f"{item['ref']} -> {(item['sha'] or '')[:8]}"
        reason = f" ({item['reason']})" if item['reason'] else ''
        why = f" [{item['why']}]" if item['why'] else ''
        print(f"{item['display']}  {sha}  slots={item['slots']} est={item['est_minutes']}m "
              f"{item['hours']:.2f} slot-h  {item['decision']}{reason}{why}")
    print(f"to submit: {total['submit']} jobs, {total['slot_hours']:g} slot-hours of budget {total['budget']:g}; "
          f"skipped: {total['skipped']}")


def cmd_plan(args):
    try:
        root, manifest, now, night, note = _setup(args)
        rows = read_ledger()
        rows += derive_outcomes(rows, None, now)
        _print_plan(plan(root, manifest, night, rows), night, note, manifest, args.json)
    except ValueError as exc:  # includes ManifestError
        print(f'bt night: {exc}', file=sys.stderr)
        return 2
    return 0


def cmd_submit(args):
    try:
        with _SubmitLock():
            return _submit_locked(args)
    except ValueError as exc:  # the lock is held by another submitter
        print(f'bt night: {exc}', file=sys.stderr)
        return 2


def _submit_locked(args):
    try:
        root, manifest, now, night, note = _setup(args)
        session = compute_pickup._owner(root, args.session)
        rows = read_ledger()
        fresh = derive_outcomes(rows, None, now)
        append_rows(fresh)
        items = plan(root, manifest, night, rows + fresh, retry_failed=args.retry_failed)
    except (ValueError, OSError) as exc:  # includes ManifestError
        print(f'bt night: {exc}', file=sys.stderr)
        return 2
    if note:
        print('note: ' + note)
    engine = _engine() if any(i['decision'] == 'submit' for i in items) else None
    failed, jobs = 0, []
    for item in items:
        if item['decision'] == 'skip':
            with contextlib.suppress(OSError):  # a skip row only feeds the report
                append_rows([{**_identity(item, night, 'skipped', _now()), 'reason': item['reason']}])
            print(f"skip   {item['display']}: {item['reason']}")
            continue
        try:
            result = engine.submit(build_request(item, session, args.defer))
            job = result.get('job') if isinstance(result, dict) and result.get('ok') else None
            if not job:
                raise RuntimeError(f'engine returned {result!r}')
        except Exception as exc:
            failed += 1
            print(f"FAILED {item['display']}: {str(exc)[:300]}", file=sys.stderr)
            continue
        jobs.append(job)
        print(f"submit {item['display']}: {job}")
        try:
            append_rows([{**_identity(item, night, 'submitted', _now()), 'job': job, 'defer': args.defer}])
        except OSError as exc:
            # the job exists; without its row the next plan cannot see it and the report omits it
            failed += 1
            print(f"WARNING {item['display']}: job {job} was accepted but could not be recorded in the ledger ({exc}); "
                  'do not submit again tonight and review it with `bt compute`', file=sys.stderr)
    if jobs:
        state = wait_registered(engine, jobs, args.wait_registered, sleep=time.sleep, clock=time.monotonic)
        for job, (ok, phase) in state.items():
            print(f"{job}: {'host-registered' if ok else 'NOT yet registered (' + str(phase) + ')'}")
        done = [j for j, (ok, _) in state.items() if ok]
        waiting = [j for j in jobs if j not in done]
        print(f'{len(done)} of {len(jobs)} submitted jobs are host-registered (safe with the laptop asleep); '
              f'{len(waiting)} not yet' + (': ' + ', '.join(waiting) + '. Keep the laptop awake; do not resubmit, '
                                          'check `bt compute --refresh`' if waiting else ''))
    else:
        print('Nothing submitted.')
    return 1 if failed else 0


# ---------------------------------------------------------------- report

def _clean(text, limit=200):
    text = ANSI.sub('', str(text))
    text = ''.join(' ' if unicodedata.category(c).startswith('C') else c for c in text)
    return text.strip()[:limit]


def _tail(record):
    tail = record.get('tail')
    if isinstance(tail, str):
        tail = tail.splitlines()
    if not isinstance(tail, list):
        return []
    lines = [_clean(line) for line in tail if isinstance(line, str)]
    return [line for line in lines if line][-12:]


def _history(outcomes, repo, display, night):
    """Label a failure by what the same recipe variant did on earlier nights in this repository."""
    by_night = {}
    for index, row in enumerate(outcomes):
        when = str(row.get('night'))
        if row.get('repo') == repo and row.get('display') == display and when < night:
            old = by_night.get(when)
            if old is None or (row.get('at') or 0, index) >= (old[0].get('at') or 0, old[1]):
                by_night[when] = (row, index)
    nights = sorted(by_night, reverse=True)
    if not nights:
        return 'first run'
    if by_night[nights[0]][0].get('passed') is True:
        return f'new failure (passed on {nights[0]})'
    since = nights[0]
    for n in nights:
        if by_night[n][0].get('passed') is True:
            break
        since = n
    return f'still failing since {since}'


def build_report(night, rows, state=None):
    outcomes = [r for r in rows if r.get('kind') == 'outcome']
    by_job = {r.get('job'): r for r in outcomes}
    entries, submitted_names = [], set()
    for row in rows:
        if row.get('kind') != 'submitted' or row.get('night') != night:
            continue
        job = str(row.get('job'))
        record = _record(job, state)
        outcome = by_job.get(job)
        status = (outcome or {}).get('status') or record.get('status') or 'unknown'
        entry = {'display': _clean(row.get('display')), 'recipe': _clean(row.get('recipe')), 'variant': row.get('variant') or {},
                 'repo': _clean(row.get('repo')), 'workstream': _clean(row.get('workstream')), 'job': _clean(job),
                 'sha8': str(row.get('sha') or LOCAL_ONLY)[:8], 'slots': row.get('slots') or 1,
                 'est_minutes': row.get('est_minutes'), 'status': status, 'terminal': outcome is not None,
                 'exit': (outcome or {}).get('exit'), 'minutes': (outcome or {}).get('minutes'),
                 'passed': bool(outcome and outcome.get('passed')), 'phase': record.get('deferred_phase'),
                 'registered': record.get('deferred_registered') is True, 'reason': _clean(record.get('reason') or ''),
                 'tail': _tail(record) if outcome else [],
                 'missing': [_clean(m) for m in (record.get('artifact_missing') or []) if isinstance(m, (str, dict))][:20],
                 'out_paths': [str(p) for p in (record.get('out_paths') or []) if isinstance(p, str)]}
        if entry['terminal'] and not entry['passed']:
            entry['label'] = _history(outcomes, row.get('repo'), row.get('display'), night)
        submitted_names.add((row.get('repo'), row.get('display')))
        entries.append(entry)
    skipped, seen = [], set()
    for row in rows:
        if row.get('kind') == 'skipped' and row.get('night') == night:
            ident = (row.get('repo'), row.get('display'))
            if ident in submitted_names or (ident, row.get('reason')) in seen:
                continue
            seen.add((ident, row.get('reason')))
            skipped.append({'display': _clean(row.get('display')), 'repo': _clean(row.get('repo')), 'reason': _clean(row.get('reason'))})
    count = lambda *names: sum(1 for e in entries if e['terminal'] and e['status'] in names)
    used = sum(e['minutes'] * e['slots'] / 60 for e in entries if e['terminal'] and e['minutes'] is not None)
    requested = sum((e['est_minutes'] or 0) * e['slots'] / 60 for e in entries)
    summary = {'submitted': len(entries), 'passed': sum(1 for e in entries if e['passed']),
               'failed': count('failed', 'lost') + sum(1 for e in entries if e['terminal'] and e['status'] == 'done' and not e['passed']),
               'timeout': count('timeout'), 'cancelled': count('cancelled'),
               'not_finished': sum(1 for e in entries if not e['terminal']), 'slot_hours_used': round(used, 2),
               'slot_hours_requested': round(requested, 2),
               'skipped_unchanged': sum(1 for s in skipped if s['reason'].startswith('unchanged'))}
    return {'night': night, 'summary': summary, 'entries': entries, 'skipped': skipped}


def summary_line(model):
    s = model['summary']
    return (f"Night {model['night']}: {s['submitted']} jobs submitted, {s['passed']} passed, {s['failed']} failed, "
            f"{s['timeout']} timed out, {s['cancelled']} cancelled, {s['not_finished']} not finished; "
            f"{s['slot_hours_used']:g} slot-hours used of {s['slot_hours_requested']:g} requested; "
            f"{s['skipped_unchanged']} skipped as unchanged.")


def _timing(e):
    if e['minutes'] is None:
        return 'n/a'
    if not e['est_minutes']:
        return f"{e['minutes']:g} min"
    return f"{e['minutes']:g} min vs {e['est_minutes']} est ({e['minutes'] / e['est_minutes']:.2f}x)"


def _attention(model):
    return [e for e in model['entries'] if e['terminal'] and not e['passed']]


CLOSING = ('This report does not mark anything reviewed; the owning session inspects the results and runs '
           '`bt compute --reviewed <job>`.')


def render_markdown(model):
    out = [f"# Night report {model['night']}", '', summary_line(model), '', '## Needs attention', '']
    attention = _attention(model)
    for e in attention:
        out += [f"### {e['display']} ({e['repo']}/{e['workstream']})",
                f"- job `{e['job']}`, sha `{e['sha8']}`, status {e['status']}, exit {e['exit']}, "
                f"{'n/a' if e['minutes'] is None else format(e['minutes'], 'g')} min, {e['label']}"]
        if e['reason']:
            out.append(f"- reason: {e['reason']}")
        if e['missing']:
            out.append('- missing artifacts: ' + ', '.join(e['missing']))
        if e['tail']:
            out += ['', '```text'] + [line.replace('```', "'''") for line in e['tail']] + ['```']
        out.append('')
    if not attention:
        out += ['None.', '']
    out += ['## Passed', '']
    passed = [e for e in model['entries'] if e['passed']]
    out += [f"- {e['display']}: {_timing(e)}, job `{e['job']}`" for e in passed] or ['None.']
    out += ['', '## Not finished', '']
    waiting = [e for e in model['entries'] if not e['terminal']]
    out += [f"- {e['display']}: {e['status']}, phase {e['phase'] or 'unknown'}, "
            f"{'host-registered' if e['registered'] else 'not host-registered'}, job `{e['job']}`" for e in waiting] or ['None.']
    out += ['', '## Skipped', '']
    out += [f"- {s['display']}: {s['reason']}" for s in model['skipped']] or ['None.']
    out += ['', '## Artifacts', '']
    groups = {}
    for e in model['entries']:
        groups.setdefault(e['recipe'], []).extend(e['out_paths'])
    for recipe, files in groups.items():
        if files:
            out += [f'### {recipe}'] + [f'- {_clean(f, 400)}' for f in files]
    if not any(groups.values()):
        out.append('None.')
    return '\n'.join(out + ['', CLOSING, ''])


def render_html(model):
    esc = lambda value: html.escape(str(value), quote=True)
    body = [f"<h1>Night report {esc(model['night'])}</h1>", f'<p class="sum">{esc(summary_line(model))}</p>']
    attention = _attention(model)
    body.append('<h2>Needs attention</h2>')
    for e in attention:
        body.append(f"<h3>{esc(e['display'])} <small>{esc(e['repo'])}/{esc(e['workstream'])}</small></h3><ul>"
                    f"<li>job {esc(e['job'])}, sha {esc(e['sha8'])}, status {esc(e['status'])}, exit {esc(e['exit'])}, {esc(e['label'])}</li>"
                    + (f"<li>reason: {esc(e['reason'])}</li>" if e['reason'] else '')
                    + (f"<li>missing artifacts: {esc(', '.join(e['missing']))}</li>" if e['missing'] else '') + '</ul>'
                    + (f"<pre>{esc(chr(10).join(e['tail']))}</pre>" if e['tail'] else ''))
    if not attention:
        body.append('<p>None.</p>')
    for title, rows in (('Passed', [f"{e['display']}: {_timing(e)}, job {e['job']}" for e in model['entries'] if e['passed']]),
                        ('Not finished', [f"{e['display']}: {e['status']}, {e['phase'] or 'unknown'}, "
                                          f"{'host-registered' if e['registered'] else 'not host-registered'}"
                                          for e in model['entries'] if not e['terminal']]),
                        ('Skipped', [f"{s['display']}: {s['reason']}" for s in model['skipped']])):
        body.append(f'<h2>{title}</h2>' + ('<ul>' + ''.join(f'<li>{esc(r)}</li>' for r in rows) + '</ul>' if rows else '<p>None.</p>'))
    body.append('<h2>Images</h2>')
    galleries = {}
    for e in model['entries']:
        for path in e['out_paths']:
            if path.lower().endswith(IMAGES):
                try:
                    uri = Path(path).as_uri()
                except ValueError:
                    continue
                caption = ', '.join(f'{k}={v}' for k, v in sorted(e['variant'].items())) or e['display']
                galleries.setdefault(e['recipe'], []).append((uri, f"{caption} ({e['job']})"))
    for recipe, images in galleries.items():
        body.append(f'<h3>{esc(recipe)}</h3><div class="grid">' + ''.join(
            f'<figure><img src="{esc(uri)}" alt="{esc(cap)}" loading="lazy"><figcaption>{esc(cap)}</figcaption></figure>'
            for uri, cap in images) + '</div>')
    if not galleries:
        body.append('<p>None.</p>')
    body.append(f'<p class="note">{esc(CLOSING)}</p>')
    style = ('body{font:15px/1.5 system-ui,sans-serif;margin:2rem auto;max-width:72rem;padding:0 1rem;color:#1b1b1b;background:#fff}'
             'pre{background:#f3f3f3;padding:.6rem;overflow:auto}small{color:#666;font-weight:400}'
             '.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(16rem,1fr));gap:1rem}'
             'figure{margin:0}img{max-width:100%;border:1px solid #ccc}figcaption{font-size:.85em;color:#555}.note{color:#555}'
             '@media(prefers-color-scheme:dark){body{background:#161616;color:#e6e6e6}pre{background:#242424}'
             'img{border-color:#444}figcaption,small,.note{color:#aaa}}')
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'; img-src file:; style-src \'unsafe-inline\'">'
            f'<title>Night report {esc(model["night"])}</title><style>{style}</style></head><body>'
            + '\n'.join(body) + '</body></html>\n')


def cmd_report(args):
    state = Path(args.state) if args.state else config.STATE_DIR
    rows = read_ledger(state)
    fresh = derive_outcomes(rows, state, _now())
    append_rows(fresh, state)
    rows += fresh
    if args.night:
        try:
            dt.date.fromisoformat(args.night)  # it becomes a file name: nothing but a date
        except ValueError:
            print(f'bt night: --night must be YYYY-MM-DD: {args.night}', file=sys.stderr)
            return 2
    night = args.night or max((r['night'] for r in rows if r.get('kind') == 'submitted' and isinstance(r.get('night'), str)
                               and re.fullmatch(r'\d{4}-\d{2}-\d{2}', r['night'])), default=None)
    if night is None:
        print('No night submissions recorded.')
        return 0
    model = build_report(night, rows, state)
    out = Path(args.out) if args.out else state / 'night' / 'reports'
    written = [out / f'{night}.md']
    text = render_markdown(model)
    harvest.atomic_write(written[0], text)
    if args.html:
        written.append(out / f'{night}.html')
        harvest.atomic_write(written[1], render_html(model))
    model['files'] = [str(p) for p in written]
    if args.json:
        print(json.dumps(model, indent=2))
    else:
        print(text)
        for path in written:
            print(f'wrote {path}')
    return 0


def register(sub):
    parser = sub.add_parser('night', help='Standing overnight checks: plan, submit and morning report')
    actions = parser.add_subparsers(dest='night_cmd', required=True)

    def common(p):
        p.add_argument('--worktree')
        p.add_argument('--manifest', help='Manifest path (default: .baton/night.toml in the worktree, then the main checkout)')
        p.add_argument('--night', metavar='YYYY-MM-DD', help='Override the computed night date')

    plan_p = actions.add_parser('plan', help='Show what would be submitted; writes nothing')
    common(plan_p)
    plan_p.add_argument('--json', action='store_true')
    plan_p.set_defaults(func=cmd_plan)
    submit_p = actions.add_parser('submit', help='Submit the night\'s changed recipes through the deferred queue')
    common(submit_p)
    submit_p.add_argument('--session', help='Your own session identity (must own the worktree)')
    submit_p.add_argument('--defer', choices=('overnight', 'spare'), default='overnight')
    submit_p.add_argument('--retry-failed', action='store_true',
                          help='Also submit recipes whose unchanged input failed last time')
    submit_p.add_argument('--wait-registered', type=int, default=60, metavar='SECONDS',
                          help='Poll up to this long for host registration (default 60)')
    submit_p.set_defaults(func=cmd_submit)
    report_p = actions.add_parser('report', help='Morning overview of a night across worktrees')
    report_p.add_argument('--night', metavar='YYYY-MM-DD')
    report_p.add_argument('--state', help='Baton state directory (default: the installed one)')
    report_p.add_argument('--out', help='Report directory (default: <state>/night/reports)')
    report_p.add_argument('--html', action='store_true')
    report_p.add_argument('--json', action='store_true')
    report_p.set_defaults(func=cmd_report)
