"""The ``mac-start.sh`` that ``bt migrate export`` writes into the bundle (ASCII, LF, POSIX sh; run as ``sh mac-start.sh``).

It finds a Python 3.11 or later (Baton needs ``tomllib``; the python3 of the Xcode command line tools is older), checks for git,
points ``BATON_STATE`` at a fresh temporary directory so that nothing is written into the bundle, and runs the import of the copy
of Baton in the bundle's ``tool/``. With no arguments it runs the import with ``--dry-run`` and prints the command for the real run.
It never installs anything: when a tool is missing it says what to install (Homebrew's install page, ``brew install python@3.12 git``).
"""
SCRIPT = r"""#!/bin/sh
# mac-start.sh - the start of the migration on the Mac. Written by `bt migrate export`.
#
#   sh mac-start.sh                    dry run: verifies the bundle and shows what an import would do; writes nothing
#   sh mac-start.sh --root ~/Code      the real import (any other `bt migrate import` options may follow)
#
# It runs the copy of Baton that sits in tool/ next to this file (the one that made this bundle), with a Python 3.11 or later.
# It installs nothing and writes nothing into this directory.

here=$(cd "$(dirname "$0")" && pwd) || { echo "mac-start.sh: cannot find the directory it is in" >&2; exit 1; }

# The python3 of a Mac without the command line tools is a stub that opens an install dialog when it is run: never run it to
# probe its version before xcode-select says the tools are installed. (BATON_APPLE_PYTHON only lets a test name another path.)
apple_python=${BATON_APPLE_PYTHON:-/usr/bin/python3}

# The names and paths tried, in order. (BATON_MAC_START_CANDIDATES replaces the list, only so that a test on a Mac that really has
# Homebrew's Python can make the script see nothing but its fake interpreters; it is never set otherwise.)
candidates=${BATON_MAC_START_CANDIDATES:-python3.13 python3.12 python3.11 /opt/homebrew/bin/python3 /usr/local/bin/python3 python3}

find_python() {
    for candidate in $candidates; do
        found=$(command -v "$candidate" 2>/dev/null) || continue
        if [ "$found" = "$apple_python" ] && command -v xcode-select >/dev/null 2>&1 && ! xcode-select -p >/dev/null 2>&1; then
            continue
        fi
        if "$found" -c 'import sys; sys.exit(0 if sys.version_info[:2] >= (3, 11) else 1)' >/dev/null 2>&1; then
            printf '%s\n' "$found"
            return 0
        fi
    done
    return 1
}

install_help() {
    echo "Install Homebrew first if you do not have it: https://brew.sh" >&2
    echo "Then run:   brew install python@3.12 git" >&2
    echo "and start this script again. Nothing was installed." >&2
}

py=$(find_python) || {
    echo "mac-start.sh: Baton needs Python 3.11 or later and none was found (the python3 that comes with the Xcode command line tools is older)." >&2
    install_help
    exit 1
}

git --version >/dev/null 2>&1 || {
    echo "mac-start.sh: git was not found." >&2
    install_help
    exit 1
}

if [ ! -f "$here/tool/bt.py" ]; then
    echo "mac-start.sh: $here/tool/bt.py is missing; this is not a complete bundle." >&2
    exit 1
fi

state=$(mktemp -d "${TMPDIR:-/tmp}/baton-migrate.XXXXXX") || { echo "mac-start.sh: cannot make a temporary directory" >&2; exit 1; }
trap 'rm -rf "$state"' EXIT
trap 'exit 130' INT TERM
BATON_STATE=$state
export BATON_STATE

# $1 as one word for a POSIX shell: in single quotes, each ' written as '\''
shquote() {
    rest=$1
    quoted=
    while :; do
        case $rest in
            *\'*)
                quoted="$quoted${rest%%\'*}'\\''"
                rest=${rest#*\'}
                ;;
            *)
                quoted="$quoted$rest"
                break
                ;;
        esac
    done
    printf "'%s'" "$quoted"
}

if [ "$#" -eq 0 ]; then
    "$py" "$here/tool/bt.py" migrate import --from "$here" --dry-run
    status=$?
    if [ "$status" -eq 0 ]; then
        echo
        echo "That was a dry run: nothing was written. When it looks right, run the import for real:"
        echo "  sh $(shquote "$here/mac-start.sh") --root ~/Code"
    fi
    exit "$status"
fi

"$py" "$here/tool/bt.py" migrate import --from "$here" "$@"
exit $?
"""
