"""Skill directories as text for this machine: the skills Baton installs name absolute paths under the owner's Windows
code root (``C:/Work/baton/mods/...``). On a machine whose code root is elsewhere the installers copy the source tree
to a private temporary directory first, with that root rewritten (``baton.migrate.rewrite.Rewriter``, every spelling it
handles), and install the rendered copy; the installed text and its hashes then describe what this machine holds, so a
later run finds the copy unchanged instead of modified. Where the root is ``C:/Work`` nothing is copied.
"""
from __future__ import annotations

import contextlib
import shutil
import tempfile
from pathlib import Path

from . import config
from .migrate.rewrite import Rewriter

SOURCE_ROOT = "C:/Work"   # the code root the tracked skill texts are written against


def _slashes(path: object) -> str:
    return str(path).replace("\\", "/").rstrip("/")


def is_source_root(code_root: object) -> bool:
    return _slashes(code_root).lower() == SOURCE_ROOT.lower()


def render_tree(source: Path, dest: Path, code_root: object) -> int:
    """Copy ``source`` to ``dest`` with ``C:/Work`` replaced by ``code_root`` in every text file; returns the number of
    replacements. Symbolic links are copied as links (the installers refuse them), binary and non-UTF-8 files as they are."""
    shutil.copytree(source, dest, symlinks=True)
    rewriter = Rewriter([(SOURCE_ROOT, _slashes(code_root))])
    total = 0
    for path in sorted(dest.rglob("*")):
        if path.is_symlink() or not path.is_file():
            continue
        data = path.read_bytes()
        new, count, _ = rewriter.rewrite_bytes(data, path.name)
        if count and new != data:
            path.write_bytes(new)
            total += count
    return total


@contextlib.contextmanager
def rendered(source: Path, code_root: object = None):
    """The directory to install from: ``source`` itself where the code root (``code_root``, else this machine's
    ``config.CODE_ROOT``) is ``C:/Work``, else a rendered copy in a temporary directory that is removed on exit,
    also when the body raises."""
    source = Path(source)
    root = config.CODE_ROOT if code_root is None else code_root
    if is_source_root(root) or not source.is_dir():
        yield source
        return
    tmp = Path(tempfile.mkdtemp(prefix="baton-skills-render-"))
    try:
        dest = tmp / source.name
        render_tree(source, dest, root)
        yield dest
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
