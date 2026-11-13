"""Windows dependence of tracked files, and the toolchains a repository needs (both from ``git ls-files``)."""
from __future__ import annotations

import os
import re
from pathlib import Path

from . import common as c

MAX_SCAN_BYTES = 1024 * 1024
WIN_SCRIPT_EXT = (".ps1", ".cmd", ".bat", ".vbs", ".ahk")
SKIP_SCAN_EXT = frozenset(".png .jpg .jpeg .gif .bmp .ico .webp .tif .tiff .pdf .zip .gz .tar .tgz .7z .rar .bz2 .xz .woff .woff2 .ttf "
                          ".otf .eot .mp3 .mp4 .mov .avi .mkv .wav .ogg .flac .webm .exe .dll .so .dylib .bin .obj .o .a .lib .class "
                          ".jar .pyc .pyd .db .sqlite .sqlite3 .pb .onnx .pt .pth .safetensors .gguf .npy .npz .parquet .blend .glb "
                          ".gltf .fbx .psd .map".split())
SKIP_SCAN_NAMES = frozenset({"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "uv.lock", "Cargo.lock", "go.sum", "poetry.lock",
                             "Pipfile.lock", "composer.lock", "bun.lockb", "bun.lock", "lake-manifest.json", "gradle.lockfile"})
SKIP_SCAN_DIRS = frozenset({"node_modules", "vendor", "third_party", "thirdparty", ".venv", "venv", "dist", "build",
                            "__pycache__", ".gradle", "Pods", "DerivedData"})
CATEGORIES = ("win_scripts", "win_paths", "powershell", "exe", "os_checks", "scheduling", "wsl", "ci")

_WINPATH = re.compile(r"(?<![A-Za-z0-9_])[A-Za-z]:[\\/]|(?<![\w.\\/:-])/[a-zA-Z]/(?:Users|Code|Windows|Program Files|ProgramData)\b")
_POWERSHELL = re.compile(r"(?i)\bpowershell(?:\.exe)?\b|\bpwsh(?:\.exe)?\b|\bcmd(?:\.exe)?\s+/[ck]\b|\bcmd\.exe\b")
_EXE = re.compile(r"(?i)\b[\w.-]+\.exe\b|\bScripts[\\/]python")
_OSCHECK = re.compile(r"\bos\.name\b|sys\.platform|\bwin32\b|\bmsvcrt\b|\bwinreg\b|ctypes\.windll|process\.platform|runtime\.GOOS|//go:build windows")
_SCHED = re.compile(r"(?i)\bschtasks\b|Task Scheduler|Startup folder|[\\/]Startup[\\/]|New-ScheduledTask")
_WSL = re.compile(r"(?i)\bwsl(?:\.exe)?(?=\s)")
_RUNS_ON = re.compile(r"^\s*runs-on:\s*(.+?)\s*$", re.MULTILINE)
_OS_LIST = re.compile(r"^\s*os:\s*\[(.+?)\]\s*$", re.MULTILINE)


def kind_of(rel: str) -> str:
    """code / test / doc / config by path heuristics."""
    low = rel.lower()
    parts = low.split("/")
    name = parts[-1]
    if any(p in ("test", "tests", "__tests__", "spec", "specs", "testing") for p in parts[:-1]) or name.startswith("test_") \
            or re.search(r"(_test|\.test|\.spec)\.[a-z]+$", name):
        return "test"
    if low.endswith((".md", ".rst", ".txt", ".adoc")) or "docs" in parts[:-1] or "doc" in parts[:-1]:
        return "doc"
    if low.endswith((".json", ".toml", ".yml", ".yaml", ".ini", ".cfg", ".conf", ".xml", ".plist", ".properties", ".gradle", ".kts")) \
            or name in ("dockerfile", "makefile") or ".github/" in low:
        return "config"
    return "code"


def skip_scan(rel: str) -> bool:
    parts = rel.split("/")
    name = parts[-1]
    if name in SKIP_SCAN_NAMES or any(p in SKIP_SCAN_DIRS for p in parts[:-1]):
        return True
    low = name.lower()
    if low.endswith((".min.js", ".min.css", ".bundle.js")) or low.endswith(".lock"):
        return True
    return os.path.splitext(low)[1] in SKIP_SCAN_EXT


class _Cat:
    def __init__(self):
        self.count = 0
        self.files = set()
        self.by_kind = {"code": 0, "test": 0, "doc": 0, "config": 0}
        self.examples: list[dict] = []

    def add(self, rel: str, line: int, kind: str, n: int = 1, extra: str = "") -> None:
        self.count += n
        self.files.add(rel)
        self.by_kind[kind] += n
        ex = {"loc": f"{rel}:{line}", "kind": kind}
        if extra:
            ex["what"] = extra
        if len(self.examples) < 5:
            self.examples.append(ex)
        elif kind != "doc":    # keep a real example in preference to a documentation mention
            for i, old in enumerate(self.examples):
                if old["kind"] == "doc":
                    self.examples[i] = ex
                    break

    def out(self) -> dict:
        return {"count": self.count, "files": len(self.files), "by_kind": self.by_kind, "examples": self.examples}


_root_cache: dict[str, re.Pattern] = {}


def _prefix_re(path: str) -> re.Pattern | None:
    if path in _root_cache:
        return _root_cache[path]
    text = path.replace("\\", "/")
    m = re.match(r"^([A-Za-z]):/*(.*)$", text)
    pat = None
    if m:
        segs = [s for s in m.group(2).split("/") if s]
        tail = "".join(r"(?:\\\\|\\|/)(?i:" + re.escape(s) + ")" for s in segs)
        btail = "".join("/(?i:" + re.escape(s) + ")" for s in segs)
        lo, up = m.group(1).lower(), m.group(1).upper()
        pat = re.compile(r"[" + lo + up + r"]:" + tail + r"(?![\w-])|/[" + lo + up + r"]" + btail + r"(?![\w-])")
    _root_cache[path] = pat
    return pat


def scan_tracked(root: Path, tracked: list[str], source_root: str, source_home: str) -> dict:
    """One pass over the tracked text files of a checkout; counts and up to five examples per category."""
    cats = {k: _Cat() for k in CATEGORIES}
    paths_split = {"root": 0, "home": 0, "other": 0}
    files_set = set(tracked)
    scripts = {"count": 0, "without_sibling": 0, "examples": []}
    ci: list[dict] = []
    root_re, home_re = _prefix_re(source_root), _prefix_re(source_home)
    scanned = skipped = 0
    for rel in tracked:
        low = rel.lower()
        kind = kind_of(rel)
        if low.endswith(WIN_SCRIPT_EXT):
            stem = rel[:rel.rfind(".")]
            sibling = any((stem + ext) in files_set for ext in (".sh", ".py"))
            scripts["count"] += 1
            if not sibling:
                scripts["without_sibling"] += 1
            if len(scripts["examples"]) < 5:
                scripts["examples"].append({"loc": rel, "kind": kind, "sibling": sibling})
        if skip_scan(rel):
            skipped += 1
            continue
        path = root / rel
        try:
            if os.path.islink(path) or os.stat(c.lp(path)).st_size >= MAX_SCAN_BYTES:
                skipped += 1
                continue
            with open(c.lp(path), "rb") as fh:
                data = fh.read()
        except OSError:
            skipped += 1
            continue
        if b"\0" in data[:8192]:
            skipped += 1
            continue
        text = data.decode("utf-8", errors="replace")
        scanned += 1
        if ".github/workflows/" in rel and low.endswith((".yml", ".yaml")):
            values = []
            for m in _RUNS_ON.finditer(text):
                v = m.group(1).strip().strip("\"'")
                values.append("(multi-line value)" if v[:1] in (">", "|") else v)
            for m in _OS_LIST.finditer(text):
                values.extend(v.strip().strip("\"'") for v in m.group(1).split(","))
            ci.append({"file": rel, "runs_on": sorted(set(values))})
            cats["ci"].add(rel, 1, "config", 1, ", ".join(sorted(set(values))))

        def line_of(pos):
            return text.count("\n", 0, pos) + 1

        for m in _WINPATH.finditer(text):
            which = "root" if root_re is not None and root_re.match(text, m.start()) else \
                    "home" if home_re is not None and home_re.match(text, m.start()) else "other"
            paths_split[which] += 1
            cats["win_paths"].add(rel, line_of(m.start()), kind, 1, which)
        for key, rx in (("powershell", _POWERSHELL), ("exe", _EXE), ("os_checks", _OSCHECK), ("scheduling", _SCHED), ("wsl", _WSL)):
            for m in rx.finditer(text):
                cats[key].add(rel, line_of(m.start()), kind)
    out = {k: v.out() for k, v in cats.items()}
    out["win_scripts"] = scripts
    out["win_paths"]["split"] = paths_split
    out["ci"]["workflows"] = ci
    out["scanned_files"] = scanned
    out["skipped_files"] = skipped
    return out


# ------------------------------------------------------------------ toolchains

_PY_REQ = re.compile(r"^requirements.*\.txt$")


def _read(root: Path, rel: str, limit: int = 20000) -> str:
    try:
        with open(c.lp(root / rel), "rb") as fh:
            return fh.read(limit).decode("utf-8", errors="replace")
    except OSError:
        return ""


def detect_toolchains(root: Path, tracked: list[str]) -> list[dict]:
    """Toolchains from manifest and lock files at depth <= 2 (tracked files only), each with the restore command it implies
    on a fresh machine. The commands are suggestions: ``bt migrate import`` never runs them."""
    by_dir: dict[str, set[str]] = {}
    for rel in tracked:
        parts = rel.split("/")
        if len(parts) > 3 or any(p in SKIP_SCAN_DIRS for p in parts[:-1]):
            continue
        by_dir.setdefault("/".join(parts[:-1]), set()).add(parts[-1])
    out: list[dict] = []

    def add(tool, d, evidence, restore, version="", needs=()):
        out.append({"tool": tool, "dir": d or ".", "evidence": evidence, "restore": restore, "version": version, "needs": list(needs),
                    "suggestion": True})

    def cd(d, cmd):
        return cmd if not d else f"cd {d} && {cmd}"

    for d, names in sorted(by_dir.items()):
        if "package.json" in names:
            if "pnpm-lock.yaml" in names:
                add("pnpm", d, ["package.json", "pnpm-lock.yaml"], cd(d, "pnpm install --frozen-lockfile"), needs=("node", "pnpm"))
            elif "yarn.lock" in names:
                add("yarn", d, ["package.json", "yarn.lock"], cd(d, "yarn install --frozen-lockfile"), needs=("node", "yarn"))
            elif names & {"bun.lockb", "bun.lock"}:
                add("bun", d, ["package.json", "bun.lock"], cd(d, "bun install --frozen-lockfile"), needs=("node", "bun"))
            elif "package-lock.json" in names:
                add("npm", d, ["package.json", "package-lock.json"], cd(d, "npm ci"), needs=("node",))
            else:
                add("npm", d, ["package.json"], cd(d, "npm install"), needs=("node",))
        py = sorted(n for n in names if n in ("pyproject.toml", "uv.lock", ".python-version", "Pipfile", "setup.py") or _PY_REQ.match(n))
        if py:
            version = _read(root, (d + "/" if d else "") + ".python-version").strip() if ".python-version" in names else ""
            if "uv.lock" in names:
                cmd = "uv sync"
            elif "pyproject.toml" in names:
                cmd = "uv sync   (or: python3 -m venv .venv && .venv/bin/pip install -e .)"
            elif any(_PY_REQ.match(n) for n in names):
                req = next(n for n in sorted(names) if _PY_REQ.match(n))
                cmd = f"uv venv && uv pip install -r {req}"
            else:
                cmd = "uv venv && uv pip install -e ."
            add("python", d, py, cd(d, cmd), version, needs=("python", "uv"))
        if "go.mod" in names:
            text = _read(root, (d + "/" if d else "") + "go.mod")
            m = re.search(r"^go\s+(\S+)", text, re.MULTILINE)
            add("go", d, ["go.mod"], cd(d, "go mod download"), m.group(1) if m else "", needs=("go",))
        lean = sorted(names & {"lakefile.lean", "lakefile.toml", "lean-toolchain", "lake-manifest.json"})
        if lean:
            ver = _read(root, (d + "/" if d else "") + "lean-toolchain").strip() if "lean-toolchain" in names else ""
            mathlib = "mathlib" in _read(root, (d + "/" if d else "") + "lake-manifest.json").lower() or \
                      "mathlib" in _read(root, (d + "/" if d else "") + "lakefile.lean").lower() or \
                      "mathlib" in _read(root, (d + "/" if d else "") + "lakefile.toml").lower()
            add("lean", d, lean, cd(d, "lake exe cache get" if mathlib else "lake build"), ver, needs=("elan",))
        if "Cargo.toml" in names:
            add("rust", d, ["Cargo.toml"], cd(d, "cargo fetch"), needs=("rustup",))
        if names & {"gradlew", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts"}:
            ver = ""
            props = (d + "/" if d else "") + "gradle/wrapper/gradle-wrapper.properties"
            if props in tracked:
                m = re.search(r"gradle-([\d.]+(?:-\w+)?)-(?:bin|all)\.zip", _read(root, props))
                ver = m.group(1) if m else ""
            ev = sorted(names & {"gradlew", "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts"})
            android = any(r.endswith("AndroidManifest.xml") and r.startswith((d + "/") if d else "") for r in tracked)
            needs = ["java"] + (["android"] if android else [])
            add("gradle", d, ev, cd(d, "./gradlew build" if "gradlew" in names else "gradle build"), ver, needs=needs)
        if "Package.swift" in names or any(n.endswith((".xcodeproj", ".xcworkspace")) for n in names):
            add("swift", d, ["Package.swift"] if "Package.swift" in names else ["Xcode project"],
                cd(d, "swift package resolve" if "Package.swift" in names else "xcodebuild -resolvePackageDependencies"), needs=("xcode",))
        dotnet = sorted(n for n in names if n.endswith((".csproj", ".sln", ".fsproj")))
        if dotnet:
            add("dotnet", d, dotnet, cd(d, "dotnet restore"), needs=("dotnet",))
        if names & {"Dockerfile", "docker-compose.yml", "docker-compose.yaml", "compose.yaml", "compose.yml"}:
            add("docker", d, sorted(names & {"Dockerfile", "docker-compose.yml", "docker-compose.yaml", "compose.yaml", "compose.yml"}),
                cd(d, "docker compose build"), needs=("docker",))
        if "Makefile" in names:
            add("make", d, ["Makefile"], cd(d, "make"), needs=())
        if names & {"justfile", "Justfile"}:
            add("just", d, ["justfile"], cd(d, "just"), needs=("just",))
    # Swift projects keep the project inside a directory: name it from the file list.
    for rel in tracked:
        if ".xcodeproj/" in rel and rel.count("/") <= 3 and not any(t["tool"] == "swift" for t in out):
            add("swift", rel.split(".xcodeproj/")[0].rsplit("/", 1)[0] if "/" in rel.split(".xcodeproj/")[0] else "", ["Xcode project"],
                "xcodebuild -resolvePackageDependencies", needs=("xcode",))
    return out
