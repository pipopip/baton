"""Run a Python helper with ordinary Windows subprocesses kept console-free."""
import importlib.machinery
import inspect
import os
import runpy
import subprocess
import sys
import types

# Values are part of the Windows process-creation API; using numeric constants
# also makes the decision function testable on other platforms.
CREATE_NEW_CONSOLE = 0x00000010
DETACHED_PROCESS = 0x00000008
CREATE_NO_WINDOW = 0x08000000


def quiet_flags(flags):
    """Preserve explicit independent console choices and all existing flags."""
    if flags & (CREATE_NEW_CONSOLE | DETACHED_PROCESS):
        return flags
    return flags | CREATE_NO_WINDOW


def install():
    if os.name != "nt":
        return
    original = subprocess.Popen.__init__
    signature = inspect.signature(original)

    def quiet_init(*args, **kwargs):
        bound = signature.bind(*args, **kwargs)
        bound.arguments["creationflags"] = quiet_flags(
            bound.arguments.get("creationflags", 0)
        )
        return original(*bound.args, **bound.kwargs)

    subprocess.Popen.__init__ = quiet_init


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: python quiet_launcher.py HELPER.py [ARG ...]")
    helper = sys.argv[1]
    sys.argv = sys.argv[1:]
    absolute_helper = os.path.abspath(helper)
    sys.path[0] = os.path.dirname(absolute_helper)
    install()
    # Direct Python execution makes __file__ absolute while preserving the
    # spelling of argv[0]. run_path(relative) leaves __file__ relative; using
    # an absolute path with run_path instead changes argv[0]. Execute script
    # files in a fresh __main__ to preserve both properties.
    if os.path.isfile(absolute_helper):
        if absolute_helper.lower().endswith(".pyc"):
            loader = importlib.machinery.SourcelessFileLoader("__main__", absolute_helper)
            code = loader.get_code("__main__")
        else:
            loader = importlib.machinery.SourceFileLoader("__main__", absolute_helper)
            with open(absolute_helper, "rb") as source:
                code = compile(source.read(), absolute_helper, "exec")
        module = types.ModuleType("__main__")
        module.__file__ = absolute_helper
        module.__loader__ = loader
        module.__package__ = None
        module.__spec__ = None
        module.__cached__ = None
        sys.modules["__main__"] = module
        exec(code, module.__dict__)
    elif not os.path.exists(absolute_helper):
        print(f"{sys.executable}: can't open file {absolute_helper!r}: [Errno 2] No such file or directory", file=sys.stderr)
        raise SystemExit(2)
    else:
        runpy.run_path(helper, run_name="__main__")


if __name__ == "__main__":
    main()

