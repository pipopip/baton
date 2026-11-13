"""Path rewriting for text files carried by ``bt migrate``.

``Rewriter`` maps source paths (a Windows code root, a home directory, any extra directory) to their new place in every
spelling they take in configuration files and documents: ``C:/Work``, ``C:\\Work``, ``D:\\\\Code`` (inside JSON or TOML
strings), any drive-letter case, Git Bash ``/d/Code`` and ``//d/Code``, WSL ``/mnt/d/Code``, ``file:///C:/Work``, and for
a home directory ``%USERPROFILE%`` and ``$env:USERPROFILE``. A match must start and end at a path boundary, so
``C:/Workx``, ``C:/Work2`` and ``xC:/Work`` are left alone. After the prefix, the rest of the path token (up to
whitespace, a quote, ``)``, ``]``, ``>``, ``,``, ``;`` or the end of the line) has each run of backslashes turned into one ``/``;
a run before the closing ``"`` of a JSON or TOML string is read by escape pairs (``\\\\"`` ends the string after a trailing
separator, ``\\"`` is an escaped quote and stays), so JSON and TOML text stays valid. A ``.json`` file whose rewritten text
would not parse is carried unchanged and listed as needing a person.
"""
from __future__ import annotations

import json
import re

JSON_BROKEN = "rewriting the paths would have broken this JSON file, so it was left as it was"
MAX_TEXT_BYTES = 2 * 1024 * 1024

_SEP = r"(?:\\{4}|\\\\|\\|/)"
_END = r"(?![\w-]|\.[\w-])"
_REST = r"(?P<rest>(?:\\+|[^\s\"'`)\]>,;\\])*)"
_BACKSLASHES = re.compile(r"\\+")


def _convert_rest(rest: str, following: str) -> str:
    """The tail of a path token with each run of backslashes turned into one ``/``. A run at the very end of the token that is
    followed by a double quote is read as JSON or TOML text does: an odd run ends in an escaped quote (that last backslash stays,
    so the string stays valid), an even run is a trailing separator and becomes ``/`` before the closing quote. Before an
    apostrophe (a TOML literal string, PowerShell) every run is a separator."""
    def run(m):
        n = len(m.group(0))
        if m.end() == len(rest) and following[:1] == '"' and n % 2 == 1:
            return ("/" if n > 1 else "") + "\\"
        return "/"
    return _BACKSLASHES.sub(run, rest)


def _segments(path: str) -> tuple[str | None, list[str]]:
    """(drive letter or None, path segments) of a source path in any spelling."""
    text = path.replace("\\", "/")
    m = re.match(r"^([A-Za-z]):(?:/|$)", text)
    if m:
        return m.group(1), [s for s in text[2:].split("/") if s]
    return None, [s for s in text.split("/") if s]


def _seg_pat(segs: list[str], sep: str) -> str:
    return "".join(sep + "(?i:" + re.escape(s) + ")" for s in segs)


class Rewriter:
    """Rewrite source-machine paths to their new spelling. ``pairs`` is a list of (source path, new path)."""

    def __init__(self, pairs: list[tuple[str, str]], source_home: str | None = None, new_home: str | None = None):
        self.rules: list[tuple[re.Pattern, str, bool]] = []
        parsed = []
        for src, dst in pairs:
            drive, segs = _segments(src)
            if segs:
                parsed.append((len(segs), drive, segs, dst.replace("\\", "/").rstrip("/") or "/"))
        parsed.sort(key=lambda t: -t[0])
        for _, drive, segs, dst in parsed:
            if drive is not None:
                lo, up = drive.lower(), drive.upper()
                win = (r"(?P<file>file:///)?(?<![A-Za-z0-9_])[" + lo + up + r"]:" + _seg_pat(segs, _SEP) + _END + _REST)
                self.rules.append((re.compile(win), dst, True))
                bash = (r"(?<![\w.\\/-])(?<!\b[A-Za-z]:)(?P<lead>//|/(?:mnt/)?)[" + lo + up + r"]" + _seg_pat(segs, "/") + _END + _REST)
                self.rules.append((re.compile(bash), dst, True))
            else:
                posix = r"(?<![\w.\\/-])" + _seg_pat(segs, "/") + _END + r"(?P<rest>)"
                self.rules.append((re.compile(posix), dst, False))
        if source_home and new_home:
            home = new_home.replace("\\", "/").rstrip("/") or "/"
            self.rules.append((re.compile(r"(?i:%USERPROFILE%)" + _REST), home, True))
            self.rules.append((re.compile(r"(?i:\$env:USERPROFILE)(?![\w])" + _REST), home, True))

    def rewrite(self, text: str) -> tuple[str, int]:
        total = 0
        for pattern, dst, windows in self.rules:
            def repl(m, dst=dst, windows=windows):
                nonlocal total
                rest = m.group("rest")
                if windows:
                    rest = _convert_rest(rest, m.string[m.end():m.end() + 1])
                if "file" in pattern.groupindex and m.group("file"):
                    new = ("file://" + dst if dst.startswith("/") else "file:///" + dst) + rest
                else:
                    new = dst + rest
                if new != m.group(0):
                    total += 1
                return new
            text = pattern.sub(repl, text)
        return text, total

    def rewrite_bytes(self, data: bytes, name: str = "") -> tuple[bytes, int, str]:
        """(new bytes, replacements, reason). ``reason`` is '' when the data was rewritable text, else why it was left alone.
        A ``.json`` file whose rewritten text no longer parses (while the original did) is kept as it was."""
        if len(data) >= MAX_TEXT_BYTES:
            return data, 0, "larger than 2 MB"
        if b"\0" in data:
            return data, 0, "binary (NUL byte)"
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            return data, 0, "not UTF-8"
        new, count = self.rewrite(text)
        if count and name.lower().endswith(".json"):
            try:
                json.loads(text.lstrip("﻿"))
            except ValueError:
                pass
            else:
                try:
                    json.loads(new.lstrip("﻿"))
                except ValueError:
                    return data, 0, JSON_BROKEN
        return (new.encode("utf-8") if count else data), count, ""


NEEDS_PERSON = re.compile(
    r"(?<![A-Za-z0-9_])[A-Za-z]:[\\/]|\.exe\b|\.cmd\b|\.ps1\b|\bpwsh\b|\bpowershell\b|\bcmd(?:\.exe)?\s*/c\b|%APPDATA%|\$env:",
    re.IGNORECASE)
_CREDENTIAL = re.compile(r"(?i)token|secret|password|api[_-]?key|authorization|bearer")


def needs_person(text: str, limit: int = 20) -> list[tuple[int, str]]:
    """Lines that still look Windows-specific (after rewriting); a line that looks like a credential is not echoed."""
    out = []
    for n, line in enumerate(text.splitlines(), 1):
        if NEEDS_PERSON.search(line):
            shown = "<line withheld: looks like a credential>" if _CREDENTIAL.search(line) else line.strip()[:160]
            out.append((n, shown))
            if len(out) >= limit:
                break
    return out
