"""Tests for the queue's isolated Windows helper launcher; no network access."""
import ast
import os
import py_compile
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import quiet_helper
import cq


class QuietHelperTests(unittest.TestCase):
    def test_flags_preserve_explicit_console_modes_and_other_flags(self):
        quiet = quiet_helper.CREATE_NO_WINDOW
        self.assertEqual(quiet_helper.quiet_flags(0x400), quiet | 0x400)
        for mode in (quiet_helper.CREATE_NEW_CONSOLE, quiet_helper.DETACHED_PROCESS):
            self.assertEqual(quiet_helper.quiet_flags(mode | 0x400), mode | 0x400)

    def test_helper_arguments_streams_cwd_and_exit_match_direct_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            helper = Path(directory) / 'helper.py'
            helper.write_text('''import os, subprocess, sys
print(repr((sys.argv, sys.path[0], os.getcwd())), flush=True)
r = subprocess.run([sys.executable, "-c", "import sys; b=sys.stdin.buffer.read(); sys.stdout.buffer.write(b); sys.stderr.buffer.write(b'child-error')"], input=sys.stdin.buffer.read(), capture_output=True)
sys.stdout.buffer.write(r.stdout)
sys.stderr.buffer.write(r.stderr)
sys.exit(23)
''', encoding='utf-8')
            kw = dict(cwd=directory, input=b'input\0bytes', capture_output=True)
            if os.name == 'nt':
                kw['creationflags'] = quiet_helper.CREATE_NO_WINDOW
            args = ['space here', '', 'ü']
            direct = subprocess.run([sys.executable, str(helper), *args], **kw)
            wrapped = subprocess.run([sys.executable, quiet_helper.__file__, str(helper), *args], **kw)
            self.assertEqual((wrapped.returncode, wrapped.stdout, wrapped.stderr), (direct.returncode, direct.stdout, direct.stderr))
            self.assertEqual(wrapped.returncode, 23)
            self.assertTrue(wrapped.stdout.endswith(b'input\0bytes'))
            self.assertEqual(wrapped.stderr, b'child-error')

    def test_relative_helper_keeps_file_and_argv_semantics_after_chdir(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'resource.txt').write_text('resource-data', encoding='utf-8')
            (root / 'helper.py').write_text("import os, pathlib, sys\nos.chdir('..')\nprint(repr((__file__, sys.argv[0], pathlib.Path(__file__).with_name('resource.txt').read_text())))\n", encoding='utf-8')
            kw = dict(cwd=directory, capture_output=True, text=True)
            if os.name == 'nt':
                kw['creationflags'] = quiet_helper.CREATE_NO_WINDOW
            direct = subprocess.run([sys.executable, 'helper.py'], **kw)
            wrapped = subprocess.run([sys.executable, quiet_helper.__file__, 'helper.py'], **kw)
            self.assertEqual((wrapped.returncode, wrapped.stdout, wrapped.stderr), (direct.returncode, direct.stdout, direct.stderr))
            self.assertEqual(wrapped.returncode, 0)
            self.assertEqual(ast.literal_eval(wrapped.stdout), (str(root / 'helper.py'), 'helper.py', 'resource-data'))

    def test_relative_bytecode_helper_keeps_file_and_argv_semantics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'helper.py'
            source.write_text("import sys; print(repr((__file__, sys.argv[0])))\n", encoding='utf-8')
            py_compile.compile(str(source), cfile=str(root / 'helper.pyc'), doraise=True)
            kw = dict(cwd=directory, capture_output=True, text=True)
            if os.name == 'nt':
                kw['creationflags'] = quiet_helper.CREATE_NO_WINDOW
            direct = subprocess.run([sys.executable, 'helper.pyc'], **kw)
            wrapped = subprocess.run([sys.executable, quiet_helper.__file__, 'helper.pyc'], **kw)
            self.assertEqual((wrapped.returncode, wrapped.stdout, wrapped.stderr), (direct.returncode, direct.stdout, direct.stderr))
            self.assertEqual(ast.literal_eval(wrapped.stdout), (str(root / 'helper.pyc'), 'helper.pyc'))

    def test_missing_helper_retains_exit_code_two(self):
        with tempfile.TemporaryDirectory() as directory:
            kw = dict(cwd=directory, capture_output=True, text=True)
            if os.name == 'nt':
                kw['creationflags'] = quiet_helper.CREATE_NO_WINDOW
            wrapped = subprocess.run([sys.executable, quiet_helper.__file__, 'missing.py'], **kw)
            self.assertEqual(wrapped.returncode, 2)
            self.assertIn('No such file or directory', wrapped.stderr)
            self.assertNotIn('Traceback', wrapped.stderr)

    def test_queue_invokes_external_helper_with_host_cwd_arguments_and_output(self):
        with tempfile.TemporaryDirectory() as directory:
            helper = Path(directory) / 'remote.py'
            helper.write_text("import os, sys\nprint(repr((sys.argv[1:], os.getcwd(), os.environ['LANE_HOST'])))\nprint('remote-error', file=sys.stderr)\nsys.exit(7)\n", encoding='utf-8')
            with mock.patch.object(cq, 'REMOTERUN', str(helper)):
                rc, stdout, stderr = cq.real_run_remoterun(['--rev', 'abc', '--', 'echo', 'hi'], directory, host='host.example')
            self.assertEqual(rc, 7)
            self.assertEqual(ast.literal_eval(stdout), (['--rev', 'abc', '--', 'echo', 'hi'], directory, 'host.example'))
            self.assertEqual(stderr.strip(), 'remote-error')

    @unittest.skipUnless(os.name == 'nt', 'Windows creation flags')
    def test_real_process_flags_include_hidden_and_preserve_explicit_detached(self):
        original = subprocess.Popen.__init__
        try:
            quiet_helper.install()
            with mock.patch.object(subprocess._winapi, 'CreateProcess', wraps=subprocess._winapi.CreateProcess) as create:
                for flags in (0, quiet_helper.DETACHED_PROCESS, 0x400):
                    with subprocess.Popen([sys.executable, '-c', 'pass'], creationflags=flags) as proc:
                        self.assertEqual(proc.wait(), 0)
                actual = [call.args[5] for call in create.call_args_list]
                self.assertTrue(actual[0] & quiet_helper.CREATE_NO_WINDOW)
                self.assertTrue(actual[1] & quiet_helper.DETACHED_PROCESS)
                self.assertFalse(actual[1] & quiet_helper.CREATE_NO_WINDOW)
                self.assertTrue(actual[2] & quiet_helper.CREATE_NO_WINDOW)
                self.assertTrue(actual[2] & 0x400)
        finally:
            subprocess.Popen.__init__ = original


if __name__ == '__main__':
    unittest.main()
