// Command classification for the compute-queue guard (owner choice 2026-10-17: hard enforcement), for the Bash,
// PowerShell and Monitor tools. Pure: no `$`, no I/O. The guard denies what the lists name and nothing else; it is not
// a shell parser, it reads a command the way a person reads it (segments, the program of each, a `bash -c` string or a
// here-document one level down). A blacklist of spellings cannot be hard: a command written some other way passes.
// Real enforcement would sit on the host (lanerun/heavy refusing to start without CQ_JOB); that is not built.

export const RUN_HINT =
  "use mcp__compute-queue__run: the queue schedules every compute run (owner 2026-10-17); " +
  "e.g. run({command: '...', slots: 1, est_minutes: 10})"

export type Verdict = { deny: false } | { deny: true; reason: string }

const ALLOW: Verdict = { deny: false }
const deny = (what: string): Verdict => ({ deny: true, reason: `${what}: ${RUN_HINT}` })

// `dyn`: the segment is a command substitution that stands where a command goes (a backtick opening a command).
// `subst`: a command substitution follows within the segment's arguments (a backtick, or `$(`).
export type Segment = { sep: string; text: string; dyn?: boolean; subst?: boolean }

// Remove here-document bodies: their lines are text, not commands here. The bodies are kept, in order of appearance.
function stripHeredocs(cmd: string): { text: string; bodies: string[] } {
  const lines = cmd.split('\n')
  const out: string[] = []
  const bodies: string[] = []
  const pending: string[] = [] // delimiters opened on one line; their bodies follow in that order
  let end: string | null = null
  let body: string[] = []
  for (const line of lines) {
    if (end !== null) {
      if (line.trim() === end) {
        bodies.push(body.join('\n'))
        body = []
        end = pending.shift() ?? null
      } else {
        body.push(line)
      }
      continue
    }
    out.push(line)
    if (!line.includes('<<<')) for (const m of line.matchAll(HEREDOC_ALL)) pending.push(m[2] as string)
    end = pending.shift() ?? null
  }
  if (end !== null) bodies.push(body.join('\n')) // an unterminated here-document runs to the end
  return { text: out.join('\n'), bodies }
}

const HEREDOC_ALL = /<<-?\s*(['"]?)([A-Za-z_][\w.-]*)\1/g

// Split at ; & | newline ( ) ` outside quotes. `sep` is the separator that preceded the segment.
function splitText(text: string): Segment[] {
  const segs: Segment[] = []
  let cur = ''
  let sep = ''
  let quote: string | null = null
  let dynNext = false
  let substCur = false
  const push = (nextSep: string) => {
    if (cur.trim() !== '') {
      const seg: Segment = { sep, text: cur.trim() }
      if (dynNext) seg.dyn = true
      if (substCur) seg.subst = true
      segs.push(seg)
    }
    cur = ''
    dynNext = false
    substCur = false
    sep = nextSep
  }
  for (let i = 0; i < text.length; i++) {
    const c = text[i] as string
    if (quote !== null) {
      cur += c
      if (c === '\\' && quote === '"' && i + 1 < text.length) {
        cur += text[++i] as string
      } else if (c === quote) {
        quote = null
      }
      continue
    }
    if (c === "'" || c === '"') {
      quote = c
      cur += c
    } else if (c === '\\' && i + 1 < text.length && /[\s"'$`\\;&|()]/.test(text[i + 1] as string)) {
      cur += c + (text[++i] as string)
    } else if (c === '|' || c === '&' || c === ';') {
      const two = text.slice(i, i + 2)
      const op = two === '&&' || two === '||' ? two : c
      i += op.length - 1
      push(op)
    } else if (c === '`' && (text[i + 1] === '\n' || text[i + 1] === '\r')) {
      cur += ' ' // PowerShell: a backtick before the newline continues the line
      if (text[i + 1] === '\r') i++
      i++
    } else if (c === '`') {
      const atStart = cur.trim() === ''
      if (!atStart) substCur = true
      push('`')
      if (atStart) dynNext = true
    } else if (c === '(' && /\$$/.test(cur.trim())) {
      substCur = true // `$(`: the segment so far ends in the `$` of a command substitution
      push('(')
    } else if (c === '\n' || c === '(' || c === ')') {
      push(c === '\n' ? ';' : c)
    } else {
      cur += c
    }
  }
  push('')
  return segs
}

export function splitSegments(cmd: string): Segment[] {
  return splitText(stripHeredocs(cmd).text)
}

// The segments, each with the here-document bodies that feed it (in order of appearance).
export function segmentsOf(cmd: string): { seg: Segment; body?: string }[] {
  const { text, bodies } = stripHeredocs(cmd)
  let next = 0
  return splitText(text).map(seg => {
    if (seg.text.includes('<<<')) return { seg }
    const n = (seg.text.match(HEREDOC_ALL) ?? []).length
    if (n === 0) return { seg }
    const mine = bodies.slice(next, next + n)
    next += n
    return { seg, body: mine.join('\n') }
  })
}

export type Tok = { text: string; quoted: boolean }

// Whitespace tokens with quotes removed; a backslash before a quote or space escapes it.
export function tokenizeQ(seg: string): Tok[] {
  const toks: Tok[] = []
  let cur = ''
  let has = false
  let quoted = false
  let quote: string | null = null
  const flush = () => {
    toks.push({ text: cur, quoted })
    cur = ''
    has = false
    quoted = false
  }
  for (let i = 0; i < seg.length; i++) {
    const c = seg[i] as string
    if (quote !== null) {
      if (c === quote) quote = null
      else if (c === '\\' && quote === '"' && i + 1 < seg.length && /["\\$`]/.test(seg[i + 1] as string)) cur += seg[++i] as string
      else cur += c
      continue
    }
    if (c === "'" || c === '"') {
      if (!has && cur === '') quoted = true // the token starts with a quote: `'<tag>'` is an argument, `<<'EOF'` a redirection
      quote = c
      has = true
    } else if (c === '\\' && i + 1 < seg.length && /[\s"'$`\\]/.test(seg[i + 1] as string)) {
      cur += seg[++i] as string
      has = true
    } else if (/\s/.test(c)) {
      if (has || cur !== '') flush()
    } else {
      cur += c
      has = true
    }
  }
  if (has || cur !== '') flush()
  return toks
}

export function tokenize(seg: string): string[] {
  return tokenizeQ(seg).map(t => t.text)
}

type Redirects = { rest: string[]; fileIn: boolean; hereString?: string }

// The tokens without their redirections; whether stdin comes from a file, and a here-string's text. A quoted token is
// never a redirection (`grep '<tag>' f`).
function redirects(tokens: Tok[]): Redirects {
  const rest: string[] = []
  let fileIn = false
  let hereString: string | undefined
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i] as Tok
    let m: RegExpExecArray | null
    if (t.quoted) rest.push(t.text)
    else if ((m = /^\d*<<<(.*)$/.exec(t.text))) hereString = m[1] ? m[1] : (tokens[++i]?.text ?? '')
    else if ((m = /^\d*<<-?(.*)$/.exec(t.text))) {
      if (!m[1]) i++ // `<< EOF`: the delimiter is the next token
    } else if ((m = /^\d*<(.*)$/.exec(t.text))) {
      fileIn = true
      if (!m[1]) i++
    } else if ((m = /^(?:\d*|&)>>?(.*)$/.exec(t.text))) {
      if (!m[1]) i++
    } else rest.push(t.text)
  }
  return { rest, fileIn, hereString }
}

export const base = (t: string): string => {
  const name = t.replace(/\\/g, '/').split('/').pop() ?? t
  return name.replace(/\.exe$/i, '').toLowerCase()
}

const SKIP = new Set(['{', '}', '!', 'if', 'then', 'else', 'elif', 'do', 'while', 'until', 'time', 'command', 'builtin', 'exec', 'sudo', 'env', 'nice', 'ionice', 'timeout', 'stdbuf'])
const LOCAL_WRAPPERS = new Set(['nohup', 'setsid'])

type Head = { prog: string; args: string[]; raw: string }

// The program of a segment: skips NAME=value, wrappers (env, time, sudo, timeout 30, ...) and their flags.
function head(tokens: string[], keepWrappers: boolean): Head | null {
  let i = 0
  while (i < tokens.length) {
    const t = tokens[i] as string
    if (/^[A-Za-z_][A-Za-z0-9_]*=/.test(t)) {
      i++
      continue
    }
    const b = base(t)
    if (SKIP.has(b) || (!keepWrappers && LOCAL_WRAPPERS.has(b))) {
      i++
      while (i < tokens.length && (/^-/.test(tokens[i] as string) || /^\d+[smhd]?$/.test(tokens[i] as string))) i++
      continue
    }
    return { prog: b, raw: t, args: tokens.slice(i + 1) }
  }
  return null
}

// A program that is a variable or a substitution ($PY, ${PY}, $(...)), not a path that starts at $HOME.
const dynamicProgram = (h: Head, seg: Segment): boolean =>
  seg.dyn === true || h.raw === '$' || (/^\$[\w{(]/.test(h.raw) && !/^\$\{?(HOME|PWD)\}?\//.test(h.raw))

const SHELLS = new Set(['bash', 'sh', 'zsh', 'dash', 'ksh'])
const PY = /^(python|python3|python3\.\d+|py|pyfrozen|uv|uvx|pypy3?)$/
export const QUEUE_SCRIPTS = /(^|[\\/])(remoterun|heavyrun)\.py$/i
const GENERATOR = /(^|[\\/])(gen_[\w-]*|record[\w-]*|fuzz[\w-]*|mutate[\w-]*|[\w-]*oracle[\w-]*|split)\.py$/i
const PY_VALUE_FLAGS = new Set(['-W', '-X', '-Q', '--check-hash-based-pycs'])

// The script a `-c` flag (also combined: -lc, -ec) hands to a shell.
function dashC(args: string[]): string | null {
  for (let i = 0; i < args.length; i++) {
    const a = args[i] as string
    if (/^-[A-Za-z]*c[A-Za-z]*$/.test(a)) return args[i + 1] ?? ''
    if (!a.startsWith('-')) return null
  }
  return null
}

// What a python command line executes: `-c CODE`, `-m MODULE ARGS`, or the first non-option argument as the script.
type PyTarget = { kind: 'code' | 'module' | 'script' | 'none'; value: string; rest: string[] }

export function pythonTarget(args: string[]): PyTarget {
  for (let i = 0; i < args.length; i++) {
    const a = args[i] as string
    if (a === '-c' || /^-[A-Za-z]+c$/.test(a)) return { kind: 'code', value: args[i + 1] ?? '', rest: args.slice(i + 2) }
    if (a === '-m') return { kind: 'module', value: args[i + 1] ?? '', rest: args.slice(i + 2) }
    if (/^-m./.test(a)) return { kind: 'module', value: a.slice(2), rest: args.slice(i + 1) }
    if (PY_VALUE_FLAGS.has(a)) {
      i++
      continue
    }
    if (a === '-') return { kind: 'none', value: '', rest: args.slice(i + 1) }
    if (a.startsWith('-')) continue
    return { kind: 'script', value: a, rest: args.slice(i + 1) }
  }
  return { kind: 'none', value: '', rest: [] }
}

// ---- local commands --------------------------------------------------------------------------------------------

// A PowerShell assignment in front of a command (`$r = ssh H ...`, `[string]$r = ...`, `$null = ...`, `$env:X = ...`,
// `$r += ...`, `$r=ssh ...`): the tokens after it. null: the right side is a value (a quoted string, a number, a
// variable, nothing), not a command.
const PS_VAR = /^(?:\[[^\]]*\])?\$(?:[\w:]+|\{[^}]*\})$/
function unassign(toks: Tok[]): Tok[] | null {
  const t0 = toks[0]
  if (t0 === undefined || t0.quoted) return toks
  let rest: Tok[]
  let m: RegExpExecArray | null
  if (PS_VAR.test(t0.text) && toks[1] !== undefined && !toks[1].quoted && /^[-+*/%]?=$/.test(toks[1].text)) rest = toks.slice(2)
  else if ((m = /^((?:\[[^\]]*\])?\$(?:[\w:]+|\{[^}]*\}))[-+*/%]?=(.*)$/.exec(t0.text)) !== null) {
    rest = m[2] === '' ? toks.slice(1) : [{ text: m[2] as string, quoted: false }, ...toks.slice(1)]
  } else return toks
  const r0 = rest[0]
  if (r0 === undefined || r0.quoted || /^[\d$@]/.test(r0.text)) return null
  return rest
}

export function classifyBash(command: string, depth = 0): Verdict {
  if (depth > 4 || typeof command !== 'string') return ALLOW
  for (const { seg, body } of segmentsOf(command)) {
    const toks = unassign(tokenizeQ(seg.text))
    if (toks === null) continue
    const red = redirects(toks)
    const h = head(red.rest, false)
    if (h === null) continue
    const stdinBody = body ?? red.hereString
    if (h.prog === 'remoterun.py' || h.prog === 'heavyrun.py') {
      return deny(`${h.prog} runs compute outside the queue`)
    }
    if (h.raw.startsWith('$')) {
      // a variable as the program ($PY .baton/remote/remoterun.py): what it holds is unknown, what it is given is not
      const script = h.args.find(a => QUEUE_SCRIPTS.test(a))
      if (script !== undefined) return deny(`${base(script)} runs compute outside the queue`)
    }
    if (PY.test(h.prog)) {
      if (h.prog === 'uv' || h.prog === 'uvx') {
        const script = h.args.find(a => QUEUE_SCRIPTS.test(a))
        if (script !== undefined) return deny(`${base(script)} runs compute outside the queue`)
        continue
      }
      const t = pythonTarget(h.args)
      if (t.kind === 'script' && QUEUE_SCRIPTS.test(t.value)) return deny(`${base(t.value)} runs compute outside the queue`)
      if (t.kind === 'module' && /^runpy$/.test(t.value) && t.rest.some(a => QUEUE_SCRIPTS.test(a))) {
        return deny('python -m runpy driving remoterun.py/heavyrun.py runs compute outside the queue')
      }
      if (t.kind === 'code') {
        const code = t.value
        if (QUEUE_SCRIPTS.test(code) || (/remoterun\.py|heavyrun\.py/.test(code) && /subprocess|os\.system|popen|runpy|exec/.test(code))) {
          return deny('python code driving remoterun.py/heavyrun.py runs compute outside the queue')
        }
      }
      continue
    }
    if (SHELLS.has(h.prog)) {
      const inner = dashC(h.args)
      if (inner !== null) {
        const v = classifyBash(inner, depth + 1)
        if (v.deny) return v
      } else if (stdinBody !== undefined && h.args.every(a => a.startsWith('-'))) {
        const v = classifyBash(stdinBody, depth + 1) // `bash <<EOF ... EOF`: the body is the script
        if (v.deny) return v
      }
      continue
    }
    if (h.prog === 'eval') {
      const v = classifyBash(h.args.join(' '), depth + 1)
      if (v.deny) return v
      continue
    }
    if (PS_WRAPPERS.has(h.prog)) {
      if (/remoterun\.py|heavyrun\.py/.test(h.args.join(' '))) return deny('remoterun.py/heavyrun.py runs compute outside the queue')
      let a = h.args
      while (a.length > 0 && /^[-/]/.test(a[0] as string)) a = a.slice(1) // -NoProfile -Command, /c, -ScriptBlock
      const v = classifyBash(a.join(' '), depth + 1)
      if (v.deny) return v
      continue
    }
    if (h.prog === 'ssh') {
      const stdin: Stdin = stdinBody !== undefined ? { body: stdinBody } : red.fileIn || seg.sep === '|' ? { unknown: true } : undefined
      const v = classifySsh(h.args, stdin, seg.subst === true)
      if (v.deny) return v
    }
  }
  return ALLOW
}

const PS_WRAPPERS = new Set(['powershell', 'pwsh', 'cmd', 'start-process', 'invoke-expression', 'iex', 'invoke-command', 'icm', 'start-job', 'start-threadjob', 'sajb'])

// ---- ssh to the build host ---------------------------------------------------------------------------------------

const HOST_RE = /^(?:[\w.-]+@)?(?:\[2001:db8:2:1:[\da-f:]*\]|2001:db8:2:1:[\da-f:]*|198\.51\.100\.16|198\.51\.100\.200|198\.51\.100\.19|198\.51\.100\.18|198\.51\.100\.15|ember|lane-ex44|hestia|hestia-ts|hestia-wifi|hearth|hearth-ts|k12|k12-wifi|k12-ts)$/i
const SSH_ARG_FLAGS = new Set(['-b', '-c', '-D', '-E', '-e', '-F', '-I', '-i', '-J', '-L', '-l', '-m', '-O', '-o', '-p', '-Q', '-R', '-S', '-W', '-w'])

// What feeds a command's stdin: a script whose text is known (a here-document, a here-string), or something unknown (a
// pipe, a file).
export type Stdin = { body?: string; unknown?: boolean } | undefined

export function classifySsh(args: string[], stdin: Stdin, substitution = false): Verdict {
  let i = 0
  let host: string | undefined
  while (i < args.length) {
    const a = args[i] as string
    if (a === '--') {
      i++
      host = args[i]
      i++
      break
    }
    if (a.startsWith('-') && a.length > 1) {
      i += SSH_ARG_FLAGS.has(a) ? 2 : 1
      continue
    }
    host = a
    i++
    break
  }
  if (host === undefined) return ALLOW
  const isBuildHost = HOST_RE.test(host) || /^builder@/.test(host)
  if (!isBuildHost) return ALLOW
  if (args[i] === '--') i++ // `ssh H -- go test ./...`: the remote command follows the --
  const remote = args.slice(i).join(' ').trim()
  if (remote === '') {
    if (substitution) return deny('a remote command built by substitution on the build host runs compute outside the queue')
    if (stdin === undefined) return ALLOW
    // a login shell fed by stdin: a here-document is the remote script; a file or a pipe is unknown
    if (stdin.body !== undefined) return classifyRemote(stdin.body, 1, undefined)
    return deny('a script fed to a remote shell on the build host runs compute outside the queue')
  }
  return classifyRemote(remote, 0, stdin)
}

const REMOTE_WORKERS = new Set([
  'nohup', 'setsid', 'heavy', 'lanerun', 'pyfrozen', 'remoterun.py', 'heavyrun.py', 'mutate.py',
  'tmux', 'screen', 'at', 'batch', 'systemd-run',
])
const TMUX_READS = new Set(['ls', 'lsw', 'lsp', 'lsc', 'has-session', 'has', 'capture-pane', 'capturep', 'display-message', 'display', 'kill-session'])
// tmux subcommands that only read (`list-*`) or end one named session; anything else (new, send-keys, run-shell, ...) starts work
const tmuxRead = (sub: string): boolean => TMUX_READS.has(sub) || sub.startsWith('list-')
const LAKE_WORK = new Set(['build', 'exe', 'test', 'update', 'run'])
const GO_VALUE_FLAGS = new Set([
  '-run', '-bench', '-count', '-timeout', '-p', '-parallel', '-tags', '-o', '-cpu', '-coverprofile', '-covermode', '-coverpkg',
  '-ldflags', '-gcflags', '-asmflags', '-skip', '-fuzz', '-fuzztime', '-vet', '-mod', '-C', '-exec', '-benchtime', '-blockprofile',
  '-cpuprofile', '-memprofile', '-mutexprofile', '-trace', '-outputdir', '-shuffle', '-overlay', '-pkgdir', '-toolexec', '-modfile',
  '-buildvcs', '-compiler', '-installsuffix', '-asan', '-msan',
])

// go test/build/vet beyond a single package: a `...` pattern, or more than one package argument.
function classifyGo(args: string[]): Verdict {
  const sub = args[0]
  if (sub === 'run') return deny('go run on the build host (the lanetest campaigns) runs compute outside the queue')
  if (sub !== 'test' && sub !== 'build' && sub !== 'vet' && sub !== 'generate') return ALLOW
  const pkgs: string[] = []
  for (let i = 1; i < args.length; i++) {
    const a = args[i] as string
    if (a === '-args' || a === '--') break
    if (a.startsWith('-')) {
      if (!a.includes('=') && GO_VALUE_FLAGS.has(a)) i++
      continue
    }
    pkgs.push(a)
  }
  if (sub === 'generate') return deny('go generate on the build host runs compute outside the queue')
  if (pkgs.some(p => p.includes('...')) || pkgs.length > 1) {
    return deny(`go ${sub} beyond a single package on the build host runs compute outside the queue`)
  }
  return ALLOW
}

// make with no target (the default target) or a build/test/check/all one starts compute; `make clean`, `make -n`-style
// reads and named helper targets pass.
const MAKE_VALUE_FLAGS = new Set(['-C', '-f', '-I', '-o', '-W', '--directory', '--file'])
const MAKE_WORK = /^(test|check|build|all)([-_.:].*)?$/

function classifyMake(args: string[]): Verdict {
  const targets: string[] = []
  for (let i = 0; i < args.length; i++) {
    const a = args[i] as string
    if (a.startsWith('-')) {
      if (MAKE_VALUE_FLAGS.has(a)) i++
      else if (a === '-j' && /^\d+$/.test(args[i + 1] ?? '')) i++
      continue
    }
    if (/^[A-Za-z_][\w.]*=/.test(a)) continue // VAR=value
    targets.push(a)
  }
  if (targets.length === 0 || targets.some(t => MAKE_WORK.test(t))) {
    return deny('make on the build host (no target, or test/check/build/all) runs compute outside the queue')
  }
  return ALLOW
}

// Files a remote `. file` / `source file` may load: shell profiles and environment setups. Any other file is a script of
// unknown content: denied.
const READ_ONLY_SOURCE = /(^|\/)(\.profile|\.bashrc|\.bash_profile|\.bash_login|profile|environment|activate|\.cargo\/env|\.elan\/env)$/

// A script piped into a python that has no -c (`| python3 -c '...'` parses output and is a read).
function pipedPython(remote: string): boolean {
  for (const m of remote.matchAll(/\|\s*(?:sudo\s+)?(?:python[\d.]*|pypy3?|py)\b([^|;&\n]*)/g)) {
    if (pythonTarget(tokenize(m[1] ?? '')).kind === 'none') return true
  }
  return false
}

// What a remote command line starts. Reads (cat, tail, head, grep, ls, stat, wc, test, flock -n .. true, uptime, df, free,
// ps, sha256sum, find without -exec/-delete, lake env, ...) and everything else the lists do not name pass.
export function classifyRemote(remote: string, depth: number, stdin: Stdin): Verdict {
  if (depth > 4) return ALLOW
  if (/\|\s*(?:sudo\s+)?(?:ba|z|da|k)?sh\b/.test(remote) || pipedPython(remote)) {
    return deny('a script piped into a shell on the build host runs compute outside the queue')
  }
  let first = true
  for (const { seg, body } of segmentsOf(remote)) {
    const red = redirects(tokenizeQ(seg.text))
    const h = head(red.rest, true)
    const isFirst = first
    first = false
    if (h === null) continue
    if (dynamicProgram(h, seg)) return deny('a remote command built by substitution or from a variable on the build host runs compute outside the queue')
    const here = body ?? red.hereString
    const segStdin: Stdin = here !== undefined ? { body: here } : red.fileIn ? { unknown: true } : isFirst ? stdin : undefined
    if (h.prog === 'tmux' && tmuxRead(h.args.find(a => !a.startsWith('-')) ?? '')) continue
    if (h.prog === 'screen' && h.args.some(a => a === '-ls' || a === '-list')) continue
    if (h.prog === 'eval') {
      const v = classifyRemote(h.args.join(' '), depth + 1, undefined)
      if (v.deny) return v
      continue
    }
    if (REMOTE_WORKERS.has(h.prog)) return deny(`${h.prog} on the build host starts compute outside the queue`)
    if (SHELLS.has(h.prog)) {
      const inner = dashC(h.args)
      if (inner !== null) {
        const v = classifyRemote(inner, depth + 1, undefined)
        if (v.deny) return v
        continue
      }
      const operands = h.args.filter(a => !a.startsWith('-'))
      if (h.args.includes('-s') || operands.length === 0) {
        // a shell reading its script from stdin: a here-document is the script, anything else is unknown
        if (segStdin?.body !== undefined) {
          const v = classifyRemote(segStdin.body, depth + 1, undefined)
          if (v.deny) return v
          continue
        }
        return deny('a script read from stdin by a remote shell on the build host runs compute outside the queue')
      }
      if (/(^|\/)(run|launch[\w.-]*|cq[\w.-]*)\.sh$/.test(operands[0] as string)) {
        return deny(`${operands[0]} on the build host starts compute outside the queue`)
      }
      continue
    }
    if (PY.test(h.prog)) {
      const script = h.args.find(a => GENERATOR.test(a))
      if (script !== undefined || h.args.includes('--check')) {
        return deny(`${h.prog} ${script ?? '--check'} on the build host runs a generator outside the queue`)
      }
      if (segStdin !== undefined && pythonTarget(h.args).kind === 'none' && h.args.every(a => a === '-' || a.startsWith('-'))) {
        return deny(`${h.prog} reading its program from stdin on the build host runs compute outside the queue`)
      }
      continue
    }
    if (h.prog === 'lake') {
      const sub = h.args.find(a => !a.startsWith('-'))
      if (sub !== undefined && LAKE_WORK.has(sub)) return deny(`lake ${sub} on the build host runs compute outside the queue`)
      if (sub === 'env') {
        const v = classifyRemote(h.args.slice(h.args.indexOf('env') + 1).join(' '), depth + 1, undefined)
        if (v.deny) return v
      }
      continue
    }
    if (h.prog === 'go') {
      const v = classifyGo(h.args)
      if (v.deny) return v
    }
    if (h.prog === 'make') {
      const v = classifyMake(h.args)
      if (v.deny) return v
    }
    if (h.prog === '.' || h.prog === 'source') {
      const file = h.args[0] ?? ''
      if (!READ_ONLY_SOURCE.test(file.replace(/[\\]/g, '/'))) {
        return deny(`${h.prog} ${file} on the build host runs an unknown script outside the queue`)
      }
    }
    if (h.prog === 'xargs') {
      const rest = h.args.filter(a => !a.startsWith('-')).join(' ')
      const v = classifyRemote(rest, depth + 1, undefined)
      if (v.deny) return v
    }
    if (h.prog === 'find' && h.args.some(a => a === '-exec' || a === '-execdir' || a === '-ok')) {
      const idx = h.args.findIndex(a => a === '-exec' || a === '-execdir' || a === '-ok')
      const v = classifyRemote(h.args.slice(idx + 1).join(' '), depth + 1, undefined)
      if (v.deny) return v
    }
  }
  return ALLOW
}
