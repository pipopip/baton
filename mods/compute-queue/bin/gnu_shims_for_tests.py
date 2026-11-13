"""GNU tools that the Linux build hosts have and macOS does not, for tests that run a host's shell script on this machine.

The queue's host-side scripts (the job launcher run.sh, remote_stage's staging script) only ever run on the Linux build
hosts, where `timeout` and `realpath -e` are GNU's. Some tests run those scripts locally with a stand-in for ssh; on a Mac
that script finds no `timeout` and a BSD `realpath` without `-e`. The fake host should behave like the real one, so `install()`
puts stand-ins first on PATH when the machine lacks the GNU form. It does nothing elsewhere or when the tools are there.
Used by tests only; the product never imports this.
"""
import atexit
import os
import shutil
import subprocess
import sys
import tempfile

_TIMEOUT = r'''import os, signal, subprocess, sys

def seconds(text):
    scale = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return float(text[:-1]) * scale[text[-1]] if text and text[-1] in scale else float(text)

args, kill_after, sig = sys.argv[1:], 0.0, signal.SIGTERM
while args and args[0].startswith("-") and args[0] != "--":
    option = args.pop(0)
    if option in ("-k", "--kill-after"):
        kill_after = seconds(args.pop(0))
    elif option.startswith("--kill-after="):
        kill_after = seconds(option.split("=", 1)[1])
    elif option in ("-s", "--signal"):
        name = args.pop(0)
        sig = signal.Signals(int(name)) if name.isdigit() else signal.Signals["SIG" + name.removeprefix("SIG")]
    elif option in ("--foreground", "--preserve-status"):
        pass
if args and args[0] == "--":
    args.pop(0)
duration, command = seconds(args[0]), args[1:]
child = subprocess.Popen(command, preexec_fn=os.setpgrp)
try:
    code = child.wait(timeout=duration or None)
except subprocess.TimeoutExpired:
    os.killpg(child.pid, sig)
    try:
        child.wait(timeout=kill_after if kill_after else None)
        code = 124
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait()
        code = 137
sys.exit(128 - code if code < 0 else code)
'''

_REALPATH = r'''import os, sys

args = sys.argv[1:]
must_exist = False
paths = []
for arg in args:
    if arg in ("-e", "--canonicalize-existing"):
        must_exist = True
    elif arg == "--":
        continue
    else:
        paths.append(arg)
status = 0
for path in paths:
    if must_exist and not os.path.exists(path):
        sys.stderr.write("realpath: %s: No such file or directory\n" % path)
        status = 1
    else:
        print(os.path.realpath(path))
sys.exit(status)
'''


def _lacks(command):
    try:
        return subprocess.run(command, capture_output=True, timeout=20).returncode != 0
    except (OSError, subprocess.SubprocessError):
        return True


def install():
    if sys.platform != "darwin":
        return None
    wanted = {}
    if shutil.which("timeout") is None:
        wanted["timeout"] = _TIMEOUT
    if _lacks(["realpath", "-e", "--", os.getcwd()]):
        wanted["realpath"] = _REALPATH
    if not wanted:
        return None
    directory = tempfile.mkdtemp(prefix="gnu-shims-")
    atexit.register(shutil.rmtree, directory, True)
    for name, code in wanted.items():
        script = os.path.join(directory, name + ".py")
        with open(script, "w", newline="\n") as handle:
            handle.write(code)
        launcher = os.path.join(directory, name)
        with open(launcher, "w", newline="\n") as handle:
            handle.write('#!/bin/sh\nexec "%s" "%s" "$@"\n' % (sys.executable, script))
        os.chmod(launcher, 0o755)
    os.environ["PATH"] = directory + os.pathsep + os.environ.get("PATH", "")
    return directory
