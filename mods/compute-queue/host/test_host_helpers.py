import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('bootstrap', HERE / 'bootstrap.py')
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)


@unittest.skipUnless(os.name == 'posix', 'POSIX ownership and flock required')
class HostTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.home = self.root / 'home'
        self.home.mkdir()
        self.shm = self.root / 'shm'
        self.env = {**os.environ, 'HOME': str(self.home), 'HEAVY_SLOTS': '2'}

    def test_bootstrap_create_only(self):
        missing = bootstrap.install(self.home, self.shm, HERE, check=True)
        self.assertTrue(missing)
        self.assertFalse((self.home / 'bin').exists())
        bootstrap.install(self.home, self.shm, HERE)
        self.assertEqual(bootstrap.install(self.home, self.shm, HERE, check=True), [])
        self.assertEqual((self.home / 'bin/heavy').stat().st_mode & 0o777, 0o700)
        bootstrap.install(self.home, self.shm, HERE)
        (self.home / 'bin/heavy').write_text('not ours')
        with self.assertRaises(ValueError):
            bootstrap.install(self.home, self.shm, HERE)
        self.assertEqual((self.home / 'bin/heavy').read_text(), 'not ours')

    def test_bootstrap_rejects_symlink_before_writing(self):
        (self.home / 'q').symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ValueError):
            bootstrap.install(self.home, self.shm, HERE)
        self.assertFalse((self.home / 'bin').exists())

    def test_heavy_rejects_bad_arguments(self):
        (self.home / 'q').mkdir()
        for args in [('-n', '0', '--', 'true'), ('-n', '3', '--', 'true'),
                     ('-n', 'abc', '--', 'true'), ('-n', '1'), ('--',)]:
            result = subprocess.run(['bash', str(HERE / 'heavy'), *args], env=self.env, capture_output=True)
            self.assertEqual(result.returncode, 2, (args, result.stderr))

    def test_heavy_waits_until_all_requested_slots_free(self):
        (self.home / 'q').mkdir()
        ready, release, second = (self.root / n for n in ('ready', 'release', 'second'))
        code = 'import pathlib,sys,time; pathlib.Path(sys.argv[1]).touch();\nwhile not pathlib.Path(sys.argv[2]).exists(): time.sleep(.02)'
        first = subprocess.Popen(['bash', str(HERE / 'heavy'), '-n', '1', '--', sys.executable, '-c', code, str(ready), str(release)], env=self.env)
        self.addCleanup(lambda: first.kill() if first.poll() is None else None)
        deadline = time.monotonic() + 3
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertTrue(ready.exists())
        other = subprocess.Popen(['bash', str(HERE / 'heavy'), '-n', '2', '--', sys.executable, '-c', 'import pathlib,sys; pathlib.Path(sys.argv[1]).touch()', str(second)], env=self.env)
        self.addCleanup(lambda: other.kill() if other.poll() is None else None)
        time.sleep(.3)
        self.assertFalse(second.exists())
        release.touch()
        self.assertEqual(first.wait(timeout=3), 0)
        self.assertEqual(other.wait(timeout=3), 0)
        self.assertTrue(second.exists())

    def test_lanerun_disk_and_boundary(self):
        stage = self.home / '.local/share/baton/staging/repo/ref'
        stage.mkdir(parents=True)
        fake = self.root / 'fake'
        fake.mkdir()
        df = fake / 'df'
        df.write_text('#!/bin/sh\nprintf "Filesystem 1024-blocks Used Available Capacity Mounted\\nfs 100 0 100 0%% /\\n"\n')
        df.chmod(0o700)
        env = {**self.env, 'PATH': str(fake) + ':' + self.env['PATH']}
        result = subprocess.run(['bash', str(HERE / 'lanerun'), str(stage), '--', 'true'], env=env, capture_output=True)
        self.assertEqual(result.returncode, 75, result.stderr)
        result = subprocess.run(['bash', str(HERE / 'lanerun'), str(self.home), '--', 'true'], env=env, capture_output=True)
        self.assertEqual(result.returncode, 2, result.stderr)


def posix(path):
    """A path as the bash that runs jobbox spells it (the identity on a POSIX host)."""
    return str(path)


BASH = shutil.which('bash')
# the system directories jobbox binds, in its order
SYSTEM_DIRS = ('/usr', '/etc', '/opt', '/bin', '/sbin', '/lib', '/lib32', '/lib64', '/libx32')


@unittest.skipUnless(os.name == 'posix' and BASH and shutil.which('realpath'), 'POSIX bash and realpath required')
class JobboxPrintTests(unittest.TestCase):
    """jobbox --print builds the bubblewrap argument list and runs nothing: how the sandbox is put together is tested
    here without bwrap or root. Whether the kernel enforces it is the host's own proof, not these tests'."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.home = self.root / 'home'
        self.job = self.home / 'q' / 'cq-1'
        for sub in ('src', 'tmp'):
            (self.job / sub).mkdir(parents=True)
        self.conf = self.root / 'jobbox.conf'
        self.conf.write_text('', newline='\n')
        self.work = self.job / 'src'
        self.run_path = '/usr/local/bin:/usr/bin:/bin'

    def p(self, path):
        return posix(path)

    def box(self, *args, conf='', home=None, env=None, cwd=None, command=('true',), job=None):
        """The result of `jobbox --print` with a configuration file holding `conf` (None: the file is absent). The
        environment is only what the test names, so what the helper passes on is exactly what it chose to."""
        if conf is not None:
            self.conf.write_text(conf, newline='\n')
        environ = {k: v for k, v in os.environ.items() if k.upper() in ('SYSTEMROOT', 'COMSPEC')}
        environ.update({'SHELL': '/bin/sh', 'TERM': 'dumb',  # named below: some bash builds export them on their own
                        'HOME': self.p(self.home if home is None else home), 'PATH': self.run_path,
                        'JOBBOX_CONF': self.p(self.conf if conf is not None else self.root / 'absent.conf'), **(env or {})})
        argv = [BASH, self.p(HERE / 'jobbox'), '--print', *args]
        if job is not False:
            argv += ['--dir', self.p(self.job if job is None else job)]
        # no terminal on any stream: --new-session is added only for one
        return subprocess.run(argv + ['--', *command], env=environ, capture_output=True, text=True, stdin=subprocess.DEVNULL,
                              cwd=str(cwd or self.work))

    def lines(self, *args, **kwargs):
        result = self.box(*args, **kwargs)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.split('\n')[:-1]

    def pairs(self, lines, flag):
        """The value lists that follow each occurrence of flag, in order."""
        width = {'--bind': 2, '--ro-bind': 2, '--dev-bind': 2, '--tmpfs': 1, '--ro-bind-try': 2, '--symlink': 2, '--setenv': 2}[flag]
        return [tuple(lines[i + 1:i + 1 + width]) for i, x in enumerate(lines) if x == flag]

    def setenv(self, lines):
        """The environment the job gets, as {name: value}, and the order the names come in."""
        pairs = self.pairs(lines, '--setenv')
        return dict(pairs), [name for name, _ in pairs]

    def refused(self, *args, code=2, message='', **kwargs):
        result = self.box(*args, **kwargs)
        self.assertEqual(result.returncode, code, (args, result.stdout, result.stderr))
        self.assertIn('jobbox: ', result.stderr)
        self.assertIn(message, result.stderr)
        self.assertEqual(result.stdout, '')  # nothing printed for a sandbox that cannot be built

    def system_binds(self):
        """The allowlist section jobbox prints on this host: a symlink or a read-only bind per system directory present."""
        out = []
        for d in SYSTEM_DIRS:
            if os.path.islink(d):
                out += ['--symlink', os.readlink(d), d]
            elif os.path.isdir(d):
                out += ['--ro-bind', d, d]
        return out

    # -- argument construction ---------------------------------------------------------------------------------------

    def test_base_flags_and_their_order(self):
        home, job = self.p(self.home), self.p(self.job)
        lines = self.lines()
        self.assertEqual(lines, [
            '--unshare-user', '--disable-userns', '--unshare-pid', '--unshare-ipc', '--unshare-uts', '--unshare-cgroup-try',
            '--die-with-parent', '--cap-drop', 'ALL', '--clearenv',
            '--unshare-net',
            *self.system_binds(),
            '--ro-bind', '/sys', '/sys', '--dev', '/dev', '--proc', '/proc', '--tmpfs', '/tmp', '--tmpfs', '/var/tmp', '--tmpfs', '/run',
            '--bind', job + '/home', home,
            '--ro-bind', job, job,
            '--bind', job + '/src', job + '/src',
            '--bind', job + '/tmp', job + '/tmp',
            '--setenv', 'SHELL', '/bin/sh', '--setenv', 'TERM', 'dumb',
            '--setenv', 'PATH', self.run_path, '--setenv', 'HOME', home, '--setenv', 'CQ_SANDBOX', '1', '--setenv', 'CQ_NETWORK', '0',
            '--chdir', self.p(self.work), '--', 'true'])

    def test_the_root_filesystem_is_an_allowlist(self):
        lines = self.lines()
        self.assertNotIn(('/', '/'), self.pairs(lines, '--ro-bind'))
        self.assertNotIn(('/', '/'), self.pairs(lines, '--bind'))
        bound = [x[0] for x in self.pairs(lines, '--ro-bind')] + [x[1] for x in self.pairs(lines, '--symlink')]
        self.assertIn('/usr', bound)
        self.assertIn('/etc', bound)
        for absent in ('/srv', '/mnt', '/media', '/root', '/var', '/home'):
            self.assertNotIn(absent, bound)
        self.assertEqual(self.pairs(lines, '--tmpfs'), [('/tmp',), ('/var/tmp',), ('/run',)])  # no tmpfs over the home directory
        self.assertEqual(lines[lines.index('--proc') + 1], '/proc')
        self.assertIn('--clearenv', lines)

    def test_the_home_is_the_jobs_own_directory_and_print_makes_nothing(self):
        home, job = self.p(self.home), self.p(self.job)
        before = sorted(p.name for p in self.job.iterdir())
        lines = self.lines()
        self.assertIn((job + '/home', home), self.pairs(lines, '--bind'))
        self.assertEqual(sorted(p.name for p in self.job.iterdir()), before)
        self.assertFalse((self.job / 'home').exists())
        (self.job / 'home').mkdir()  # an existing one is used as it is
        self.assertIn((job + '/home', home), self.pairs(self.lines(), '--bind'))
        self.assertEqual(list((self.job / 'home').iterdir()), [])

    def test_the_job_keeps_the_path_it_was_given_and_the_command_follows_the_separator(self):
        lines = self.lines(env={'PATH': '/opt/x/bin:/usr/bin'}, command=('timeout', '-k', '30', '60', 'bash', 'a b/cmd.sh'))
        self.assertEqual(self.setenv(lines)[0]['PATH'], '/opt/x/bin:/usr/bin')
        self.assertEqual(lines[lines.index('--') + 1:], ['timeout', '-k', '30', '60', 'bash', 'a b/cmd.sh'])

    def test_network_off_and_on(self):
        off = self.lines()
        self.assertIn('--unshare-net', off)
        self.assertNotIn('--ro-bind-try', off)
        self.assertEqual(self.setenv(off)[0]['CQ_NETWORK'], '0')
        on = self.lines('--net')
        self.assertNotIn('--unshare-net', on)
        self.assertEqual(self.pairs(on, '--ro-bind-try'), [('/run/systemd/resolve', '/run/systemd/resolve')])
        self.assertEqual(self.setenv(on)[0]['CQ_NETWORK'], '1')
        # the resolver's directory is bound after /run is emptied, and before the job's home
        self.assertLess(on.index('/run'), on.index('--ro-bind-try'))
        self.assertLess(on.index('--ro-bind-try'), on.index(self.p(self.home)))

    def test_configuration_entries_come_sorted_parents_first_between_the_home_and_the_job_directory(self):
        for name in ('bin', '.cache', '.cache/go-mod', 'zeta'):
            (self.home / name).mkdir(parents=True, exist_ok=True)
        conf = '\n'.join(['ro ~/zeta', 'ro ~/.cache/go-mod', 'dev /dev/null', 'ro ~/bin', 'rw ~/.cache', ''])
        lines = self.lines(conf=conf)
        home, job = self.p(self.home), self.p(self.job)
        entries = [('--dev-bind', '/dev/null'), ('--ro-bind', home + '/bin'), ('--bind', home + '/.cache'),
                   ('--ro-bind', home + '/.cache/go-mod'), ('--ro-bind', home + '/zeta')]
        entries.sort(key=lambda e: e[1])
        home_bind = next(i for i, x in enumerate(lines) if x == '--bind' and lines[i + 1] == job + '/home')
        job_bind = next(i for i, x in enumerate(lines) if x == '--ro-bind' and lines[i + 1] == job)
        order = [next(k for k, x in enumerate(lines) if x == flag and lines[k + 1] == path) for flag, path in entries]
        self.assertEqual(order, sorted(order))  # parents first: ~/.cache before ~/.cache/go-mod
        self.assertTrue(all(home_bind < i < job_bind for i in order))
        # then the job directory read-only, then what is writable
        self.assertEqual(lines[job_bind:job_bind + 9], ['--ro-bind', job, job, '--bind', job + '/src', job + '/src', '--bind', job + '/tmp', job + '/tmp'])

    def test_an_rw_parent_with_an_ro_child_leaves_the_child_read_only_and_after_the_parent(self):
        (self.home / '.cache' / 'go-mod').mkdir(parents=True)
        lines = self.lines(conf='ro ~/.cache/go-mod\nrw ~/.cache\n')
        parent = next(i for i, x in enumerate(lines) if x == '--bind' and lines[i + 1] == self.p(self.home / '.cache'))
        child = next(i for i, x in enumerate(lines) if x == '--ro-bind' and lines[i + 1] == self.p(self.home / '.cache/go-mod'))
        self.assertLess(parent, child)

    def test_only_existing_writable_subdirectories_are_bound(self):
        (self.job / 'tmp').rmdir()
        lines = self.lines()
        self.assertEqual([x for x in self.pairs(lines, '--bind') if x[0].startswith(self.p(self.job) + '/s')],
                         [(self.p(self.job) + '/src',) * 2])
        self.assertNotIn(self.p(self.job) + '/tmp', lines)

    def test_rw_adds_a_writable_directory_for_this_job_after_src_and_tmp(self):
        scratch = self.root / 'scratch dir'
        scratch.mkdir()
        inside = self.job / 'extra'
        inside.mkdir()
        lines = self.lines('--rw', self.p(scratch), '--rw', self.p(inside))
        job = self.p(self.job)
        self.assertEqual(self.pairs(lines, '--bind')[-4:], [(job + '/src',) * 2, (job + '/tmp',) * 2, (self.p(scratch),) * 2, (self.p(inside),) * 2])
        self.assertLess(lines.index(job), lines.index(self.p(scratch)))  # after the read-only job directory

    # -- environment -------------------------------------------------------------------------------------------------

    def test_the_environment_is_named_variables_only(self):
        (self.job / 'job.env').write_text("export PATH='/job/bin'\nexport MYTOOL_HOME=/opt/mytool\nexport GOCACHE=/c\n"
                                          "SLOTS=2\nexport NOT_SET_HERE=1\n  export INDENTED=1\n", newline='\n')
        env = {'SECRET_TOKEN': 'x', 'AWS_SECRET_ACCESS_KEY': 'y', 'SSH_AUTH_SOCK': '/run/s', 'DBUS_SESSION_BUS_ADDRESS': 'unix:z',
               'XDG_RUNTIME_DIR': '/run/user/1', 'BASH_ENV': '/etc/x', 'ENV': '/etc/y', 'LD_LIBRARY_PATH': '/evil',
               'USER': 'q', 'LOGNAME': 'q', 'SHELL': '/bin/bash', 'LANG': 'C.UTF-8', 'LANGUAGE': 'en', 'TZ': 'UTC', 'TERM': 'dumb',
               'TMPDIR': '/t', 'D': '/d', 'SLOTS': '2', 'SELFHEAVY': '0', 'TIMEOUT_S': '60', 'SUBDIR': '.', 'HEAVY_SLOTS': '8',
               'LC_ALL': 'C', 'LC_TIME': 'C', 'CQ_JOB': 'cq-1', 'CQ_SHA': 'abc', 'CQ_SANDBOX': 'evil', 'CQ_NETWORK': 'evil',
               'MYTOOL_HOME': '/opt/mytool', 'GOCACHE': '/c', 'JAVA_HOME': '/j', 'ANDROID_SDK_ROOT': '/a', 'ANDROID_X': 'b',
               'ANDROIDX': 'not a prefix match', 'GOFLAGS': '-count=1', 'GOPRIVATE': 'dropped unless job.env or the configuration names it'}
        lines = self.lines(env=env, conf='env JAVA_HOME\nenv ANDROID_*\nenv   NOT_SET_EITHER\n')
        values, order = self.setenv(lines)
        self.assertIn('--clearenv', lines)
        for gone in ('SECRET_TOKEN', 'AWS_SECRET_ACCESS_KEY', 'SSH_AUTH_SOCK', 'DBUS_SESSION_BUS_ADDRESS', 'XDG_RUNTIME_DIR',
                     'BASH_ENV', 'ENV', 'LD_LIBRARY_PATH', 'ANDROIDX', 'GOPRIVATE', 'JOBBOX_CONF', 'NOT_SET_HERE', 'NOT_SET_EITHER', 'INDENTED'):
            self.assertNotIn(gone, values)
        self.assertNotIn('x', [v for _, v in self.pairs(lines, '--setenv')])
        for kept in ('USER', 'LOGNAME', 'SHELL', 'LANG', 'LANGUAGE', 'TZ', 'TERM', 'TMPDIR', 'D', 'SLOTS', 'SELFHEAVY', 'TIMEOUT_S', 'SUBDIR',
                     'HEAVY_SLOTS', 'LC_ALL', 'LC_TIME', 'CQ_JOB', 'CQ_SHA', 'GOFLAGS'):  # GOFLAGS: run.sh sets it for a boxed job
            self.assertEqual(values[kept], env[kept], kept)
        self.assertEqual(values['MYTOOL_HOME'], '/opt/mytool')  # a name job.env exports
        self.assertEqual(values['GOCACHE'], '/c')
        self.assertEqual(values['JAVA_HOME'], '/j')  # `env NAME`
        self.assertEqual((values['ANDROID_SDK_ROOT'], values['ANDROID_X']), ('/a', 'b'))  # `env PREFIX*`
        # the helper's own variables win over anything the caller had: PATH is the one the job was given
        self.assertEqual((values['HOME'], values['CQ_SANDBOX'], values['CQ_NETWORK']), (self.p(self.home), '1', '0'))
        self.assertEqual(values['PATH'], self.run_path)
        self.assertEqual(order[-4:], ['PATH', 'HOME', 'CQ_SANDBOX', 'CQ_NETWORK'])
        self.assertEqual(order[:-4], sorted(order[:-4]))  # a fixed order
        self.assertEqual(len(order), len(set(order)))

    def test_without_job_env_or_configuration_only_the_fixed_names_pass(self):
        values, order = self.setenv(self.lines(env={'GOPRIVATE': 'x', 'MYTOOL_HOME': '/m', 'LANG': 'C'}))
        self.assertEqual(sorted(values), ['CQ_NETWORK', 'CQ_SANDBOX', 'HOME', 'LANG', 'PATH', 'SHELL', 'TERM'])

    def test_a_bad_env_line_is_refused(self):
        for text in ('env 1bad\n', 'env a-b\n', 'env *\n', 'env A*B\n', 'env\n', 'env A B\n', 'env A**\n'):
            with self.subTest(text=text):
                self.refused(code=78, message='', conf=text)

    # -- --rw validation ---------------------------------------------------------------------------------------------

    def test_rw_refusals(self):
        scratch = self.root / 'scratch'
        scratch.mkdir()
        link = self.root / 'link'
        link.symlink_to(scratch, target_is_directory=True)
        through = self.root / 'through'
        through.symlink_to(self.root, target_is_directory=True)
        plain_file = self.root / 'file'
        plain_file.write_text('')
        elsewhere = self.home / 'other'
        elsewhere.mkdir()
        outside = 'inside the home directory but not inside the job directory'
        for value, message in (
                ('relative/dir', 'not an absolute directory'),
                (self.p(self.root / 'missing'), 'not an absolute directory'),
                (self.p(plain_file), 'not an absolute directory'),
                (self.p(link), 'not an absolute directory'),
                (self.p(through / 'scratch'), '--rw must be a resolved path'),
                (self.p(self.root), 'contains the home directory'),
                (self.p(self.home), 'contains the home directory'),
                (self.p(self.job), outside),  # the job directory itself would make its read-only part writable
                (self.p(elsewhere), outside),
                (self.p(self.home / 'q'), outside)):
            with self.subTest(value=value):
                self.refused('--rw', value, message=message)
        # the root directory: not owned by this user, or (as root) refused as the root directory
        self.refused('--rw', '/', message='--rw is')

    # -- job directory validation ------------------------------------------------------------------------------------

    def test_job_directory_refusals(self):
        real = self.root / 'real'
        real.mkdir()
        link = self.home / 'q' / 'cq-link'
        link.symlink_to(real, target_is_directory=True)
        nested = self.job / 'src' / 'deeper'
        nested.mkdir()
        stray = self.home / 'stray'
        stray.mkdir()
        file_job = self.home / 'q' / 'cq-file'
        file_job.write_text('')
        for job, message in ((self.root / 'missing', 'job directory is missing, a link or not owned by this user'),
                             (link, 'job directory is missing, a link or not owned by this user'),
                             (file_job, 'job directory is missing, a link or not owned by this user'),
                             (nested, 'not directly under the queue directory'),
                             (stray, 'not directly under the queue directory'),
                             (self.home / 'q', 'not directly under the queue directory'),
                             (real, 'not directly under the queue directory')):
            with self.subTest(job=str(job)):
                self.refused(message=message, job=job)
        self.refused(message='missing --dir', job=False)

    def test_a_linked_job_subdirectory_is_refused(self):
        for sub in ('tmp', 'home'):
            with self.subTest(sub=sub):
                path = self.job / sub
                if path.is_symlink():
                    path.unlink()
                elif path.exists():
                    path.rmdir()
                path.symlink_to(self.root, target_is_directory=True)
                self.refused(message=f'unsafe job subdirectory: {sub}')
                path.unlink()
                path.mkdir()

    def test_home_and_queue_directory_refusals(self):
        self.refused(message='HOME is not a directory', home=self.root / 'nohome')
        self.refused(message='HOME is not a directory', env={'HOME': 'relative'})
        bare = self.root / 'bare'
        bare.mkdir()
        self.refused(message='no queue directory', home=bare)

    def test_argument_refusals(self):
        self.refused('--bogus', message='unknown argument: --bogus')
        result = subprocess.run([BASH, self.p(HERE / 'jobbox'), '--print', '--dir', self.p(self.job), '--rw'], env={'HOME': self.p(self.home), 'PATH': self.run_path},
                                capture_output=True, text=True, stdin=subprocess.DEVNULL)
        self.assertEqual(result.returncode, 2)
        self.assertIn('jobbox: missing value for --rw', result.stderr)
        self.refused(message='missing command', command=())

    # -- configuration -----------------------------------------------------------------------------------------------

    def test_configuration_kinds_home_expansion_comments_and_missing_paths(self):
        for name in ('bin', 'go-mod', 'go-build'):
            (self.home / name).mkdir()
        (self.home / 'spaced').mkdir()
        conf = '\n'.join([
            '# comment', '', '   ',
            'ro ~/bin   # trailing comment',
            'ro ~/go-mod',
            '  rw    ~/go-build',
            'ro ~/not-installed',
            'rw ~/not-created',
            'dev /dev/null',
            f'dev {self.p(self.root / "no-device")}',  # missing: skipped
            ''])
        lines = self.lines(conf=conf)
        home = self.p(self.home)
        ro = [x[0] for x in self.pairs(lines, '--ro-bind') if x[0] not in SYSTEM_DIRS + ('/sys', self.p(self.job))]
        self.assertEqual(ro, [home + '/bin', home + '/go-mod'])
        self.assertEqual([x[0] for x in self.pairs(lines, '--bind') if not x[0].startswith(self.p(self.job))], [home + '/go-build'])
        self.assertEqual(self.pairs(lines, '--dev-bind'), [('/dev/null', '/dev/null')])
        self.assertNotIn('not-installed', '\n'.join(lines))
        self.assertNotIn('not-created', '\n'.join(lines))
        self.assertNotIn('no-device', '\n'.join(lines))

    def test_configuration_can_not_make_the_job_directory_writable(self):
        job = self.p(self.job)
        for text in (f'rw {job}\n', f'rw {self.p(self.home / "q")}\n', f'ro {job}/src\n'):
            with self.subTest(text=text):
                self.refused(code=78, message='the path lies in the queue directory', conf=text)

    def test_an_absent_configuration_is_an_empty_one(self):
        self.assertEqual(self.lines(conf=None), self.lines(conf=''))

    def test_hide_is_gone(self):
        self.refused(code=78, message='unknown kind: hide', conf=f'hide {self.p(self.root)}\n')

    def test_a_line_with_a_control_character_is_refused(self):
        for text in ('ro ~/bin\r\n', 'ro\t~/bin\n', 'ro ~/bin\x01\n', '# comment\x07\nro ~/bin\n'):
            with self.subTest(text=text):
                self.refused(code=78, message='a line holds a control character', conf=text)

    def test_paths_that_are_the_home_contain_it_or_lie_in_the_queue_are_refused(self):
        home = self.p(self.home)
        for kind in ('ro', 'rw'):
            for value, message in ((home, 'the path is the home directory or contains it'),
                                   (self.p(self.root), 'the path is the home directory or contains it'),
                                   (self.p(self.root.parent), 'the path is the home directory or contains it'),
                                   (home + '/q', 'the path lies in the queue directory'),
                                   (home + '/q/cq-1', 'the path lies in the queue directory'),
                                   (home + '/q/cq-1/src', 'the path lies in the queue directory'),
                                   (home + '/q/anything', 'the path lies in the queue directory'),
                                   ('~/q', 'the path lies in the queue directory'),
                                   ('~/q/cq-1', 'the path lies in the queue directory')):
                with self.subTest(kind=kind, value=value):
                    self.refused(code=78, message=message, conf=f'{kind} {value}\n')
        # a path beside the queue directory, under home, is fine
        (self.home / 'qq').mkdir()
        self.assertIn((home + '/qq',) * 2, self.pairs(self.lines(conf='ro ~/qq\n'), '--ro-bind'))

    def test_a_configuration_path_that_leads_home_or_into_the_queue_through_a_link_is_refused(self):
        outside = self.root / 'outside'
        outside.mkdir()
        (outside / 'to-home').symlink_to(self.home, target_is_directory=True)
        (outside / 'to-queue').symlink_to(self.home / 'q', target_is_directory=True)
        (outside / 'to-job').symlink_to(self.job, target_is_directory=True)
        (self.home / 'tools').mkdir()
        (outside / 'to-tools').symlink_to(self.home / 'tools', target_is_directory=True)
        for kind in ('ro', 'rw'):
            for name, message in (('to-home', 'leads to the home directory or contains it'),
                                  ('to-queue', 'leads into the queue directory'),
                                  ('to-job', 'leads into the queue directory')):
                with self.subTest(kind=kind, name=name):
                    self.refused(code=78, message=message, conf=f'{kind} {self.p(outside)}/{name}\n')
        # one path, one line: `ro X` beside `rw X` would otherwise be settled by the sort, not by the author
        (self.home / 'cache').mkdir()
        self.refused(code=78, message='the path is named twice', conf='ro ~/cache\nrw ~/cache\n')
        self.refused(code=78, message='the path is named twice', conf=f'rw ~/cache\nro {self.p(self.home)}/cache\n')
        # a link to something else under home is an ordinary entry
        self.assertIn((self.p(outside) + '/to-tools',) * 2,
                      self.pairs(self.lines(conf=f'ro {self.p(outside)}/to-tools\n'), '--ro-bind'))

    def test_dev_must_be_a_device_node(self):
        plain = self.root / 'plain'
        plain.write_text('')
        adir = self.root / 'adir'
        adir.mkdir()
        for target in (plain, adir):
            with self.subTest(target=str(target)):
                self.refused(code=78, message='not a device node', conf=f'dev {self.p(target)}\n')
        self.assertEqual(self.pairs(self.lines(conf='dev /dev/null\n'), '--dev-bind'), [('/dev/null', '/dev/null')])

    def test_bad_configuration_lines_are_refused_with_78(self):
        for text, message in (('bogus ~/x\n', 'unknown kind: bogus'),
                              ('ro\n', "expected 'KIND VALUE'"),
                              ('ro ~/a ~/b\n', "expected 'KIND VALUE'"),
                              ('ro relative/path\n', 'not a plain absolute path'),
                              ('ro /\n', 'not a plain absolute path'),
                              ('ro ~\n', 'not a plain absolute path'),
                              ('ro /a//b\n', 'not a plain absolute path'),
                              ('ro /a/../b\n', 'not a plain absolute path'),
                              ('ro /a/./b\n', 'not a plain absolute path'),
                              ('ro /a/..\n', 'not a plain absolute path'),
                              ('ro /a/\n', 'not a plain absolute path'),
                              ('rw ~/ok\nro ~/a b\n', "expected 'KIND VALUE'"),
                              ('ro ~/../x\n', 'not a plain absolute path')):
            with self.subTest(text=text):
                self.refused(code=78, message=message, conf=text)

    def test_a_configuration_that_is_not_a_regular_file_is_refused_with_78(self):
        target = self.root / 'real.conf'
        target.write_text('')
        self.conf.unlink()
        self.conf.symlink_to(target)
        result = subprocess.run([BASH, self.p(HERE / 'jobbox'), '--print', '--dir', self.p(self.job), '--', 'true'],
                                env={'HOME': self.p(self.home), 'PATH': self.run_path, 'JOBBOX_CONF': self.p(self.conf)},
                                capture_output=True, text=True, stdin=subprocess.DEVNULL, cwd=str(self.work))
        self.assertEqual(result.returncode, 78, result.stderr)
        self.assertIn('is not a regular file', result.stderr)


if __name__ == '__main__':
    unittest.main()
