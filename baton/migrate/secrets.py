"""Secrets in ``bt migrate``: what is a secret by name, by content, and how configuration files are redacted.

Rules, in the order a file meets them on its way into a bundle:

1. A name that is a credential file (``.env``, ``.npmrc``, ``id_rsa``, ``*.pfx``, ``token.json`` ...) is never carried; it is
   listed for copying by hand. A ``.pem`` is decided by its content (a certificate bundle is public), the example/sample/
   template twin of a secret name is not a secret by name, and source code called ``credentials.py`` is not either.
2. ``settings*.json``, ``.mcp.json``, ``.claude.json`` and Codex ``config.toml`` are parsed and carried with every value under
   ``env``, ``headers``, ``http_headers`` and every key that looks like a token, secret, password or key replaced by
   ``REDACTED`` (key names are kept), and URL userinfo and query strings removed. A file that does not parse is not carried.
3. Any other text file up to 2 MiB that holds a high-confidence credential pattern (a private key block, a GitHub, Anthropic,
   OpenAI, Slack or AWS key, a ``"refresh_token"`` or ``"private_key"`` field) is not carried either.

Nothing here prints a value: a held file is reported by name and reason only.
"""
from __future__ import annotations

import fnmatch
import json
import re
import tomllib

REDACTED = "REDACTED"
MAX_CONTENT_BYTES = 2 * 1024 * 1024      # the size the rewriter reads (rewrite.MAX_TEXT_BYTES)

SECRET_NAME_PATTERNS = (".env", ".env.*", "*.env", ".envrc", ".dev.vars", ".dev.vars.*", ".npmrc", ".netrc", "_netrc", ".pypirc",
                        "*.key", "*.p12", "*.pfx", "*.jks", "*.keystore", "*.ppk", "*.dpapi",
                        "id_rsa*", "id_ed25519*", "id_ecdsa*", "id_dsa*",
                        "*credentials*", "auth.json", "google-services.json", "local.properties",
                        ".secrets", "secrets.*", "token.json", "client_secret*.json", "service-account*.json", "*.tfvars", "*.tfvars.json")
TEMPLATE_SUFFIXES = (".example", ".sample", ".template", ".dist")
SOURCE_SUFFIXES = (".py", ".pyi", ".js", ".mjs", ".cjs", ".ts", ".tsx", ".jsx", ".go", ".rs", ".java", ".kt", ".swift", ".rb", ".php",
                   ".cs", ".c", ".h", ".cpp", ".md", ".rst")

STRUCTURED_JSON = ("settings*.json", ".mcp.json", ".claude.json")

SECRET_CONTENT = (
    ("a private key block", re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----")),
    ("a GitHub token", re.compile(r"\bgh[pos]_\w{30,}")),
    ("a GitHub token", re.compile(r"github_pat_\w{6,}")),
    ("an Anthropic key", re.compile(r"sk-ant-[\w-]{8,}")),
    ("an API key (sk-)", re.compile(r"\bsk-[A-Za-z0-9_-]{30,}")),
    ("a Slack token", re.compile(r"xox[abpr]-[\w-]{4,}")),
    ("an AWS access key id", re.compile(r"\bAKIA[0-9A-Z]{16}")),
    ("a Bearer token", re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]{16,}")),
    ("a Basic authorization value", re.compile(r"(?i)\bbasic\s+[A-Za-z0-9+/=]{16,}")),
    ("a Stripe live key", re.compile(r"\b[rs]k_live_\w{16,}")),
    ("a Hugging Face token", re.compile(r"\bhf_[A-Za-z0-9]{30,}")),
    ("a Google API key", re.compile(r"\bAIza[\w-]{35}")),
    ("a Slack webhook URL", re.compile(r"https://hooks\.slack\.com/services/")),
    ("a password on a curl command line", re.compile(r"\bcurl\b[^\n]*?\s(?:-u|--user)[\s=]+['\"]?[^\s:'\"]+:[^\s'\"]{3,}")),
    ("a refresh_token field", re.compile(r'"refresh_token"')),
    ("a private_key field", re.compile(r'"private_key"')),
)
# What an uncommitted change may not contain (a tracked diff, a stash): the rules above without the two JSON field names, which
# are ordinary code in a diff, plus a literal assigned to a secret-named key.
PATCH_RULES = tuple(r for r in SECRET_CONTENT if not r[0].endswith(" field")) + (
    ("a literal assigned to a secret-named key", re.compile(
        r"""(?i)["']?[\w.-]*(?:passw(?:or)?d|secret|api[_-]?key|access[_-]?key|private[_-]?key|token|credential)[\w.-]*["']?\s*[:=]\s*["'][^"'\s]{6,}["']""")),
)

_SECRETISH = re.compile(r"(?i)token(?!s(?:$|[^a-z]))|secret|passw|passphrase|api[_-]?key|access[_-]?key|private[_-]?key|credential|authoriz|bearer|"
                        r"cookie|session|(?:^|[^a-z])(?:key|auth|sig|signature)(?:$|[^a-z])")
_URL = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_USERINFO = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*://)([^/@\s?#]*)@")
_BEARER = re.compile(r"(?i)bearer\s")
_ENV_KEYS = frozenset({"env", "headers", "http_headers", "env_http_headers"})


def scan_patch(text: str) -> list[tuple[str, str]]:
    """(file, rule name) for each rule that an added line of a patch matches, once per file and rule. Never returns a value."""
    out: list[tuple[str, str]] = []
    cur = "(unknown file)"
    for line in text.splitlines():
        if line.startswith("diff --git "):
            m = re.match(r"diff --git a/(.*) b/(.*)$", line)
            cur = m.group(2) if m else line[11:]
        elif line.startswith("+") and not line.startswith("+++"):
            for what, rx in PATCH_RULES:
                if rx.search(line) and (cur, what) not in out:
                    out.append((cur, what))
    return out


def is_secret_name(name: str) -> bool:
    low = name.lower()
    if low.endswith(TEMPLATE_SUFFIXES) or low == "credentials.py":
        return False
    if not any(fnmatch.fnmatchcase(low, p) for p in SECRET_NAME_PATTERNS):
        return False
    # source code and notes that merely have "credentials" in the name
    if ("credentials" in low or low.startswith("secrets.")) and low.endswith(SOURCE_SUFFIXES) and not fnmatch.fnmatchcase(low, ".git-credentials"):
        return False
    return True


def is_structured(name: str, rel: str = "") -> str:
    """'json', 'toml', 'gitconfig' or '' for a file whose values are redacted in place instead of the file being held."""
    low = name.lower()
    if any(fnmatch.fnmatchcase(low, p) for p in STRUCTURED_JSON):
        return "json"
    parts = rel.lower().split("/")
    if low == "config.toml" and (any(p.startswith(".codex") for p in parts) or parts[:2] == ["user", "codex"]):
        return "toml"
    if rel == "gitconfig" or low == ".gitconfig":
        return "gitconfig"
    return ""


# ------------------------------------------------------------------ redaction of values

def redact_url(url: str) -> str:
    """The URL without credentials: userinfo of an http(s)/ftp URL (a token can be the user name) or the password of any
    other scheme becomes ``REDACTED``; a query string becomes ``?REDACTED``. A URL without either comes back unchanged."""
    m = _USERINFO.match(url)
    if m:
        scheme, info = m.group(1), m.group(2)
        if scheme.lower().startswith(("http", "ftp")) or ":" in info:
            keep = "" if scheme.lower().startswith(("http", "ftp")) else info.split(":", 1)[0] + ":"
            url = scheme + keep + REDACTED + "@" + url[m.end():]
    q = url.find("?")
    if q >= 0 and _URL.match(url):
        base, _, rest = url[:q], "?", url[q + 1:]
        frag = ""
        if "#" in rest:
            rest, _, frag = rest.partition("#")
            frag = "#" + frag
        params = []
        for part in rest.split("&"):
            name, eq, value = part.partition("=")
            secret = eq and value and (_SECRETISH.search(name) or content_reason(value) or content_reason(part))
            params.append(f"{name}={REDACTED}" if secret else part)
        url = base + "?" + "&".join(params) + frag
    return url


def redact_userinfo_text(text: str) -> str:
    """Every ``scheme://userinfo@`` inside free text (a git config, a command line)."""
    def sub(m):
        info = m.group(2)
        if m.group(1).lower().startswith(("http", "ftp")):
            return m.group(1) + REDACTED + "@"
        return m.group(1) + (info.split(":", 1)[0] + ":" + REDACTED if ":" in info else info) + "@"
    return re.sub(r"((?<![A-Za-z0-9+-])[A-Za-z][A-Za-z0-9+-]*://)([^/@\s?#\"']+)@", sub, text)


_INLINE_ASSIGN = re.compile(r"(?<![\w-])([A-Za-z_][A-Za-z0-9_]*)=(\"[^\"]*\"|'[^']*'|[^\s'\")]+)")
_INLINE_BEARER = re.compile(r"(?i)\b(bearer)(\s+)(?!REDACTED\b)[^\s'\"]+")
_INLINE_BASIC = re.compile(r"(?i)\b(basic)(\s+)[A-Za-z0-9+/=]{16,}")


def redact_inline(text: str) -> str:
    """Secrets inside one string, the rest of it kept: ``NAME=VALUE`` with a secret-looking NAME (a permission rule such as
    ``Bash(API_TOKEN=abc ./deploy.sh:*)``), and the token after ``Bearer`` or ``Basic``."""
    text = _INLINE_BEARER.sub(lambda m: m.group(1) + m.group(2) + REDACTED, text)
    text = _INLINE_BASIC.sub(lambda m: m.group(1) + m.group(2) + REDACTED, text)
    return _INLINE_ASSIGN.sub(lambda m: m.group(1) + "=" + REDACTED if _SECRETISH.search(m.group(1)) else m.group(0), text)


def _flag_kind(flag: str) -> str:
    low = flag.lower()
    if flag == "-u" or low == "--user":
        return "user"
    if flag in ("-e", "--env", "--env-var"):
        return "env"
    if flag == "-H" or low == "--header":
        return "header"
    return "secret" if flag.startswith("-") and _SECRETISH.search(flag) else ""


def _flag_value(kind: str, value: str) -> str:
    if _BEARER.search(value):
        return REDACTED
    if kind == "secret":
        return REDACTED
    if kind == "user":                                # curl -u user:password
        name, colon, rest = value.partition(":")
        return f"{name}:{REDACTED}" if colon and len(rest) >= 3 else value
    if kind == "env":
        name, eq, _ = value.partition("=")
        return name + "=" + REDACTED if eq and _SECRETISH.search(name) else (redact_url(value) if _URL.match(value) else value)
    name, colon, _ = value.partition(":")            # a header
    return f"{name}: {REDACTED}" if colon and _SECRETISH.search(name) else value


def redact_args(args: list) -> list:
    """An argument list with secret values removed: the value after ``--token``, ``--api-key``, ``--key`` and any option whose
    name looks secret (also ``--token=VALUE``), ``-e NAME=VALUE`` / ``--env`` when NAME looks secret, ``--header`` / ``-H``
    values naming a secret header, any argument holding ``Bearer ``, ``NAME=VALUE`` arguments with a secret NAME, and URLs."""
    out: list[str] = []
    pending = ""
    for raw in args:
        a = str(raw)
        if pending:
            out.append(_flag_value(pending, a))
            pending = ""
        elif _BEARER.search(a):
            out.append(REDACTED)
        elif a.startswith("-"):
            flag, eq, val = a.partition("=")
            kind = _flag_kind(flag)
            if not kind:
                out.append(a)
            elif eq:
                out.append(flag + "=" + _flag_value(kind, val))
            else:
                out.append(a)
                pending = kind
        elif _URL.match(a):
            out.append(redact_url(a))
        elif re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", a) and _SECRETISH.search(a.split("=", 1)[0]):
            out.append(a.split("=", 1)[0] + "=" + REDACTED)
        else:
            out.append(a)
    return out


def redact_command_text(cmd: str) -> str:
    """A shell command line with secret-looking option values removed; a line with nothing to remove is returned as it was."""
    cmd = redact_inline(cmd)
    parts = re.findall(r"\"[^\"]*\"|'[^']*'|\S+", cmd)
    new = redact_args(parts)
    return cmd if new == parts else " ".join(new)


def _all_leaves(obj):
    """Every string below ``obj`` replaced; numbers, booleans and dates are not secrets and stay what they are."""
    if isinstance(obj, dict):
        return {k: _all_leaves(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_all_leaves(v) for v in obj]
    return REDACTED if isinstance(obj, str) else obj


_VAR_NAME_KEY = re.compile(r"(?i)(?:_env_var|env_key|_env)$")      # the value names an environment variable: not a secret


def _env_like(obj):
    """The values of an env or headers table: one is redacted only when its key looks secret or the value matches a content rule,
    so LOG_LEVEL, PORT and CLAUDE_CODE_MAX_OUTPUT_TOKENS survive."""
    if isinstance(obj, dict):
        return {k: _env_like(v) if isinstance(v, (dict, list)) else
                (REDACTED if _SECRETISH.search(str(k)) or (isinstance(v, str) and (content_reason(v) or content_reason(f"{k}={v}"))) else v)
                for k, v in obj.items()}
    if isinstance(obj, list):
        return [_env_like(v) for v in obj]
    return obj


def redact_doc(obj, key: str = ""):
    """A copy of a parsed JSON/TOML document: values under env/headers/http_headers and under secret-looking keys become
    ``REDACTED`` (names kept), ``args`` lists go through ``redact_args``, URLs lose userinfo and query, and any string with
    ``Bearer `` is replaced."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "env_http_headers":                         # names of variables, not values
                out[k] = v
            elif k in _ENV_KEYS:
                out[k] = _env_like(v)
            elif isinstance(v, str) and _VAR_NAME_KEY.search(str(k)):
                out[k] = v
            elif _SECRETISH.search(str(k)) and not isinstance(v, bool):
                out[k] = _all_leaves(v)
            elif k == "command" and isinstance(v, str):
                out[k] = redact_command_text(v)
            else:
                out[k] = redact_doc(v, str(k))
        return out
    if isinstance(obj, list):
        if key == "args" and all(isinstance(x, (str, int, float)) for x in obj):
            return redact_args(obj)
        return [redact_doc(v, key) for v in obj]
    if isinstance(obj, str):
        return redact_url(obj) if _URL.match(obj) else redact_inline(obj)
    return obj


def redacted_keys(data: bytes, name: str = "") -> list[str]:
    """Where ``REDACTED`` stands in a carried file: dotted key paths for JSON and TOML, ``key`` of the line otherwise."""
    text = data.decode("utf-8", errors="replace")
    if REDACTED not in text:
        return []
    found: list[str] = []

    def walk(obj, path):
        if isinstance(obj, dict):
            for k, v in obj.items():
                walk(v, path + [str(k)])
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                walk(v, path + [str(i)])
        elif isinstance(obj, str) and REDACTED in obj:
            found.append(".".join(path))

    doc = None
    try:
        if name.lower().endswith(".json"):
            doc = json.loads(text.lstrip("\ufeff"))
        elif name.lower().endswith(".toml"):
            doc = tomllib.loads(text)
    except ValueError:
        doc = None
    if doc is not None:
        walk(doc, [])
    else:
        found = [m.group(1) for m in re.finditer(r"([A-Za-z_][\w.-]*)\s*[:=]\s*[\"']?[^\n]*REDACTED", text)] or ["(see the file)"]
    return list(dict.fromkeys(found))


# ------------------------------------------------------------------ git configuration

_PLAIN_HELPER = re.compile(r"^(?:osxkeychain|manager(?:-core)?|store|cache|wincred|libsecret|oauth|gcm|netrc)(?:\s+--[\w-]+(?:[= ][^\s=]+)?)*$")


def gitconfig_value(key: str, value: str) -> str:
    """The value of one git configuration entry as it may be shown or carried: ``REDACTED`` for a secret-named key, for
    ``http.*.extraheader`` and for a credential helper that embeds anything (a plain helper name or a program path may stay),
    URL userinfo and secret ``NAME=VALUE`` words removed from the rest."""
    k = key.lower()
    if k.endswith("extraheader") or (k.startswith("credential.") and k.endswith(".helper") or k == "credential.helper"):
        plain = _PLAIN_HELPER.match(value.strip()) or re.fullmatch(r"[\w./\\:~-]+", value.strip())
        return value if plain and not k.endswith("extraheader") else REDACTED
    if k.startswith("credential.") and not _SECRETISH.search(k.replace("credential.", "", 1)):
        return redact_inline(redact_userinfo_text(value))
    if _SECRETISH.search(k) or k.endswith("smtppass"):
        return REDACTED
    return redact_inline(redact_userinfo_text(value))


def redact_gitconfig_text(text: str) -> str:
    """A git configuration file with every value that ``gitconfig_value`` redacts replaced, comments and layout kept."""
    out, section, skipping = [], "", False
    for line in text.split("\n"):
        if skipping:                                   # the continuation of a redacted value
            skipping = line.endswith("\\")
            continue
        m = re.match(r'^\s*\[([^\s\]"]+)(?:\s+"((?:[^"\\]|\\.)*)")?\]', line)
        if m:
            section = m.group(1) + ("." + m.group(2) if m.group(2) else "")
            out.append(redact_userinfo_text(line))
            continue
        m = re.match(r"^(\s*)([A-Za-z][\w-]*)(\s*=\s*)(.*)$", line)
        if m:
            value = m.group(4)
            new = gitconfig_value(f"{section}.{m.group(2)}", value.rstrip("\\"))
            if new != value.rstrip("\\"):
                out.append(f"{m.group(1)}{m.group(2)}{m.group(3)}{new}")
                skipping = value.endswith("\\")
                continue
        out.append(redact_userinfo_text(line))
    return "\n".join(out)


# ------------------------------------------------------------------ TOML writer (the standard library only reads)

_BARE = re.compile(r"^[A-Za-z0-9_-]+$")


def _toml_str(s: str) -> str:
    """A basic string: backslash, quote and every control character (U+0000-U+001F, U+007F) escaped."""
    out = ['"']
    for ch in s:
        o = ord(ch)
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif o < 0x20 or o == 0x7F:
            out.append("\\u%04X" % o)
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def _toml_key(k: str) -> str:
    return k if _BARE.match(k) else _toml_str(k)


def _toml_val(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return _toml_str(v)
    if isinstance(v, list):
        return "[" + ", ".join(_toml_val(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{ " + ", ".join(f"{_toml_key(k)} = {_toml_val(x)}" for k, x in v.items()) + " }"
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return _toml_str(str(v))


def same_toml(a, b) -> bool:
    """Whether two parsed TOML values are the same, types included (1 is not 1.0 is not True; NaN equals NaN; the order of a
    table's keys does not count)."""
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return set(a) == set(b) and all(same_toml(a[k], b[k]) for k in a)          # a table has no order
    if isinstance(a, list):
        return len(a) == len(b) and all(same_toml(x, y) for x, y in zip(a, b))
    if isinstance(a, float):
        return a == b or (a != a and b != b)
    return a == b


def toml_dumps(doc: dict) -> str:
    """A small TOML writer for what a Codex ``config.toml`` holds (scalars, arrays, nested tables, arrays of tables)."""
    lines: list[str] = []

    def table(prefix: list[str], d: dict) -> None:
        scalars = {k: v for k, v in d.items() if not isinstance(v, dict) and not (isinstance(v, list) and v and all(isinstance(x, dict) for x in v))}
        sub = {k: v for k, v in d.items() if k not in scalars}
        if prefix and (scalars or not sub):
            lines.append("[" + ".".join(_toml_key(p) for p in prefix) + "]")
        for k, v in scalars.items():
            lines.append(f"{_toml_key(k)} = {_toml_val(v)}")
        if scalars or (prefix and not sub):
            lines.append("")
        for k, v in sub.items():
            if isinstance(v, dict):
                table(prefix + [k], v)
            else:
                for item in v:
                    lines.append("[[" + ".".join(_toml_key(p) for p in prefix + [k]) + "]]")
                    for kk, vv in item.items():
                        lines.append(f"{_toml_key(kk)} = {_toml_val(vv)}")
                    lines.append("")

    table([], doc)
    return "\n".join(lines).rstrip("\n") + "\n"


# ------------------------------------------------------------------ files

# Every rule above needs one of these words (lower case) to match: a file with none of them is cleared by a few substring tests
# in C instead of a dozen regular expressions over every byte, which was most of the CPU time of the audit.
_LITERALS = ("-----begin", "ghp_", "gho_", "ghs_", "github_pat_", "sk-", "xox", "akia", "bearer", "basic", "_live_", "hf_", "aiza",
             "hooks.slack.com", "curl", "refresh_token", "private_key")

# Formats whose bytes are compressed or media: no pattern can be read out of them, and opening 25 thousand of them costs minutes
# where every open is virus-scanned.
OPAQUE_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".mp3", ".mp4", ".mov", ".webm", ".zip", ".gz", ".tgz", ".bz2",
                   ".xz", ".7z", ".jar", ".whl", ".woff", ".woff2", ".ttf", ".otf", ".class", ".pyc", ".mo")


def is_opaque(name: str) -> bool:
    return name.lower().endswith(OPAQUE_SUFFIXES)


def content_reason(text: str) -> str:
    low = text.lower()
    if not any(w in low for w in _LITERALS):
        return ""
    for what, rx in SECRET_CONTENT:
        if rx.search(text):
            return what
    return ""


def _decode(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def prepare(data: bytes, name: str, rel: str = "", *, content: bool = True) -> tuple[bytes | None, str]:
    """(bytes to carry, '') or (None, why the file is held back for copying by hand). ``content`` False skips every check
    that reads the file (a ``no_scan`` repository): a file that would need parsing is then held without being opened."""
    kind = is_structured(name, rel)
    low = name.lower()
    if kind in ("json", "toml") and not content:
        return None, "its values would have to be redacted and the repository is not scanned (no_scan)"
    if kind == "json":
        try:
            doc = json.loads(_decode(data).lstrip("\ufeff"))
        except ValueError:
            return None, "does not parse as JSON, so its secret values cannot be removed"
        new = (json.dumps(redact_doc(doc), indent=2, ensure_ascii=False) + "\n").encode("utf-8")
        why = content_reason(_decode(new))
        return (None, f"holds {why} after redaction") if why else (new, "")
    if kind == "toml":
        try:
            doc = tomllib.loads(_decode(data))
        except ValueError:
            return None, "does not parse as TOML, so its secret values cannot be removed"
        redacted = redact_doc(doc)
        text = "# Redacted by bt migrate (secret values removed, comments not carried).\n" + toml_dumps(redacted)
        try:
            again = tomllib.loads(text)
        except ValueError as exc:
            return None, f"cannot be rewritten as TOML after redaction ({type(exc).__name__}); the original is not carried"
        if not same_toml(redacted, again):
            return None, "the rewritten TOML differs from the original in more than the redacted values; the original is not carried"
        why = content_reason(text)
        return (None, f"holds {why} after redaction") if why else (text.encode("utf-8"), "")
    if not content and low.endswith(".pem"):
        return None, "a .pem file and the repository is not scanned (no_scan): it cannot be told from a private key"
    if not content or len(data) > MAX_CONTENT_BYTES:
        return data, ""
    if kind == "gitconfig":
        text = redact_gitconfig_text(_decode(data))
        why = content_reason(text)
        return (None, f"holds {why}") if why else (text.encode("utf-8"), "")
    if low.endswith(".pem"):
        text = _decode(data)
        if b"\0" in data:
            return None, "a .pem file that is not text"
        why = content_reason(text)
        if why:
            return None, f"holds {why}"
        if "-----BEGIN " not in text:
            return None, "a .pem file with no recognisable block"
        return data, ""
    if b"\0" in data[:8192]:
        return data, ""
    why = content_reason(_decode(data))
    return (None, f"holds {why}") if why else (data, "")
