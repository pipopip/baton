#!/usr/bin/env python3
"""Create the minimal user-owned queue directories and helper scripts."""
import argparse
import os
from pathlib import Path
import stat
import sys


def inspect_dir(path):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError(f"unsafe or unowned directory: {path}")
    return True


def inspect_file(path, content):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError(f"unsafe or unowned helper: {path}")
    if path.read_bytes() != content:
        raise ValueError(f"existing helper differs: {path}")
    if stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError(f"existing helper mode is not 700: {path}")
    return True


def install(home, shm, source, check=False):
    home, shm, source = Path(home), Path(shm), Path(source)
    # Check every existing component before making any changes.
    directories = [home, home / "bin", home / ".local", home / ".local/share",
                   home / ".local/share/baton", home / "q", home / "tmp",
                   home / "tmp/cache", shm]
    contents = {name: (source / name).read_bytes() for name in ("heavy", "lanerun")}
    for directory in directories:
        inspect_dir(directory)
    for name, content in contents.items():
        inspect_file(home / "bin" / name, content)
    missing = [str(p) for p in directories if not inspect_dir(p)]
    missing += [str(home / "bin" / n) for n, data in contents.items()
                if not inspect_file(home / "bin" / n, data)]
    if check:
        return missing
    for directory in directories:
        if not inspect_dir(directory):
            try:
                directory.mkdir(mode=0o700)
                os.chmod(directory, 0o700, follow_symlinks=False)
            except FileExistsError:
                pass
            if not inspect_dir(directory):
                raise ValueError(f"directory creation failed: {directory}")
    for name, content in contents.items():
        target = home / "bin" / name
        if inspect_file(target, content):
            continue
        try:
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o700)
        except FileExistsError:
            if inspect_file(target, content):
                continue
            raise
        with os.fdopen(fd, "wb") as output:
            output.write(content)
        # umask can remove requested mode bits; this file was created by us.
        os.chmod(target, 0o700, follow_symlinks=False)
    return missing


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        missing = install(Path.home(), Path("/dev/shm/cq"), Path(__file__).resolve().parent, args.check)
        if args.check and missing:
            print("Missing:\n" + "\n".join(missing))
            return 1
        print("Queue prerequisites " + ("ready" if args.check or not missing else "created"))
        return 0
    except (OSError, ValueError) as exc:
        print(f"bootstrap: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
