// The compute allowlist (owner decision 2026-10-17): the commands the run tool queues without asking anyone when the
// permission rules answer `ask`. Pure: no `$`, no I/O. A positive list: what is not named here is not allowed, and the
// refusal tells the owner which settings rule would allow it. The run tool never opens a dialog (see README).
//
// Allowed: segments joined by `&&`, `;` or a newline, and `|`; each head one of
//   cd <relative path>
//   go test|vet|build|run                          (not -exec / -toolexec)
//   lake build|exe|env|test                        (`lake env` runs only an allowed command or lean)
//   python|python3|pyfrozen|<path>/.venv/.../python <script>.py     (the script under testdata/, .baton/remote/,
//                                                   scripts/, or named gen_*, record*, mutate*, *oracle*)
//   heavy -n N <allowed>      timeout N <allowed>      bash -c '<allowed>'
//   bash|sh .baton/remote/<name>.sh [words]        (relative, name [A-Za-z0-9_.-]+, words [A-Za-z0-9_./:=-]+)
//   after a `|` also: tail, head, grep, wc (read-only filters)
// Refused anywhere: `$(`, a backtick, a here-document or `<`, a subshell, `&` (background), `||`, ssh, scp, curl, wget,
// rm, sudo, git push, and a redirection `>` whose target is not under `.` or $TMPDIR.

import { base, pythonTarget, QUEUE_SCRIPTS, segmentsOf, tokenizeQ } from './classify'
import type { Tok } from './classify'

export type Allowance = { ok: true } | { ok: false; why: string }

const NO = (why: string): Allowance => ({ ok: false, why })
const OK: Allowance = { ok: true }

const NEVER = /(?:^|[^\w./-])(ssh|scp|curl|wget|rm|sudo)(?![\w-])/
const GIT_PUSH = /(?:^|[^\w./-])git\s+push(?![\w-])/
const PIPE_FILTERS = new Set(['tail', 'head', 'grep', 'wc'])
const SCRIPT_DIRS = /(^|\/)(testdata|scripts)\/|(^|\/)\.baton\/remote\//
// A session's local script under .baton/remote/ (carried into the job's source by the queue), and its plain-word arguments.
const REMOTE_SCRIPT = /^\.baton\/remote\/[A-Za-z0-9_.-]+\.sh$/
const PLAIN_ARG = /^[A-Za-z0-9_./:=-]+$/
const SCRIPT_NAMES = /^(gen_[\w.-]*|record[\w.-]*|mutate[\w.-]*|[\w.-]*oracle[\w.-]*)\.py$/i

// A path inside the work tree: relative, no `..`, no variable.
function relPath(p: string): boolean {
  const q = p.replace(/\\/g, '/')
  return q !== '' && !/^[/~$-]/.test(q) && !/^[A-Za-z]:/.test(q) && !/\$/.test(q) && !q.split('/').includes('..')
}

const tmpTarget = (p: string): boolean => /^\$(\{TMPDIR\}|TMPDIR)\//.test(p) && !p.split('/').includes('..')

// The program a token names, only when it is the bare name or a known install path (a file in the work tree called
// `go` is not the go tool).
function program(t: string): string | null {
  const q = t.replace(/\\/g, '/')
  switch (q) {
    case 'go': case 'go.exe': return 'go'
    case 'lake': return 'lake'
    case 'heavy': return 'heavy'
    case 'timeout': return 'timeout'
    case 'cd': return 'cd'
    case 'bash': case 'sh': return 'bash'
    case 'python': case 'python3': case 'pyfrozen': return 'python'
    case 'lean': return 'lean'
  }
  if (/^(?:~|[A-Za-z]:)?\/(?:[^/]+\/)*bin\/go(?:\.exe)?$/.test(q)) return 'go'
  if (/^(?:~|[A-Za-z]:)?\/(?:[^/]+\/)*\.elan\/bin\/lake$/.test(q)) return 'lake'
  if (/^(?:~|[A-Za-z]:)?\/(?:[^/]+\/)*bin\/heavy$/.test(q)) return 'heavy'
  if (/(^|\/)\.venv\/(?:Scripts|bin)\/python(?:3)?(?:\.exe)?$/i.test(q)) return 'python'
  return null
}

// Walks the command outside quotes: refuses `&` (background), `||`, `(`, `)`, `<`; returns the targets of `>`.
function scan(text: string): { bad?: string; targets: string[] } {
  const targets: string[] = []
  let quote: string | null = null
  for (let i = 0; i < text.length; i++) {
    const c = text[i] as string
    if (quote !== null) {
      if (c === '\\' && quote === '"') i++
      else if (c === quote) quote = null
      continue
    }
    if (c === "'" || c === '"') quote = c
    else if (c === '\\') i++
    else if (c === '&') {
      if (text[i + 1] === '&') i++
      else return { bad: 'a background command (&)', targets }
    } else if (c === '|') {
      if (text[i + 1] === '|') return { bad: '|| (only &&, ; and | join commands)', targets }
    } else if (c === '(' || c === ')') return { bad: 'a subshell or a process substitution', targets }
    else if (c === '<') return { bad: 'an input redirection or a here-document', targets }
    else if (c === '>') {
      while (text[i + 1] === '>') i++
      let j = i + 1
      while (j < text.length && /\s/.test(text[j] as string)) j++
      let t = ''
      let q: string | null = null
      for (; j < text.length; j++) {
        const d = text[j] as string
        if (q !== null) {
          if (d === q) q = null
          else t += d
        } else if (d === "'" || d === '"') q = d
        else if (/[\s;&|<>()]/.test(d)) break
        else t += d
      }
      i = j - 1
      targets.push(t)
    }
  }
  return { targets }
}

// Drops the redirections from a segment's tokens (`> f`, `>f`, `2>f`, `>> f`).
function withoutRedirects(toks: Tok[]): Tok[] {
  const out: Tok[] = []
  for (let i = 0; i < toks.length; i++) {
    const t = toks[i] as Tok
    const m = t.quoted ? null : /^\d*>>?(.*)$/.exec(t.text)
    if (m === null) out.push(t)
    else if (m[1] === '') i++
  }
  return out
}

// null = the command at the head of `toks` is allowed.
function headOk(toks: Tok[], piped: boolean, depth: number): string | null {
  const t0 = toks[0]
  if (t0 === undefined) return null
  const args = toks.slice(1).map(t => t.text)
  const name = base(t0.text)
  if (piped && PIPE_FILTERS.has(name) && !t0.quoted && !/[/\\]/.test(t0.text)) return null
  const prog = t0.quoted ? null : program(t0.text)
  if (prog === null) return `${/[/\\]/.test(t0.text) ? name : t0.text} is not on the compute allowlist`
  switch (prog) {
    case 'cd':
      return args.length === 1 && relPath(args[0] as string) ? null : 'cd goes only to a relative path inside the work tree'
    case 'go': {
      if (!['test', 'vet', 'build', 'run'].includes(args[0] ?? '')) return 'only go test, vet, build and run are on the compute allowlist'
      if (args.some(a => /^-(exec|toolexec)(=|$)/.test(a))) return 'go -exec / -toolexec runs another program'
      return null
    }
    case 'lake': {
      const sub = args[0]
      if (sub === 'env') {
        const rest = toks.slice(2)
        if (rest.length === 0) return null
        if (!rest[0]?.quoted && program(rest[0]?.text ?? '') === 'lean') return null
        return headOk(rest, false, depth + 1)
      }
      return ['build', 'exe', 'test'].includes(sub ?? '') ? null : 'only lake build, exe, env and test are on the compute allowlist'
    }
    case 'python': {
      const t = pythonTarget(args)
      if (t.kind !== 'script') return 'python runs only a script file (not -c, -m or stdin)'
      const p = t.value.replace(/\\/g, '/').replace(/^\.\//, '')
      if (!relPath(p) || !p.endsWith('.py')) return 'the python script must be a relative .py path inside the work tree'
      if (QUEUE_SCRIPTS.test(p)) return 'remoterun.py and heavyrun.py are not queued jobs'
      if (!SCRIPT_DIRS.test(p) && !SCRIPT_NAMES.test(p.split('/').pop() as string)) {
        return 'the python script is not under testdata/, .baton/remote/ or scripts/, nor named gen_*, record*, mutate* or *oracle*'
      }
      return null
    }
    case 'heavy': {
      if (args[0] !== '-n' || !/^\d+$/.test(args[1] ?? '') || args.length < 3) return 'heavy is allowed as `heavy -n N <command>`'
      return headOk(toks.slice(3), false, depth + 1)
    }
    case 'timeout': {
      if (!/^\d+[smhd]?$/.test(args[0] ?? '') || args.length < 2) return 'timeout is allowed as `timeout N <command>`'
      return headOk(toks.slice(2), false, depth + 1)
    }
    case 'bash': {
      if (args[0] !== '-c' && args.length > 0) {
        const script = (args[0] as string).replace(/^\.\//, '')
        if (toks[1]?.quoted !== true && REMOTE_SCRIPT.test(script)) {
          return args.slice(1).every(a => PLAIN_ARG.test(a)) ? null : 'a script under .baton/remote/ takes only plain word arguments (letters, digits, _ . / : = -)'
        }
      }
      if (args.length !== 2 || args[0] !== '-c') return "bash is allowed only as `bash -c '<allowed command>'` or `bash .baton/remote/<name>.sh [plain words]`"
      const inner = computeAllowed(args[1] as string, depth + 1)
      return inner.ok ? null : inner.why
    }
    case 'lean':
      return 'lean is allowed only after lake env'
  }
  return null
}

export function computeAllowed(command: string, depth = 0): Allowance {
  if (depth > 3) return NO('the command is nested too deeply')
  if (typeof command !== 'string' || command.trim() === '') return NO('the command is empty')
  if (command.includes('$(')) return NO('it contains a command substitution ($( )')
  if (command.includes('`')) return NO('it contains a backtick substitution')
  if (/<</.test(command)) return NO('it contains a here-document or here-string')
  const never = NEVER.exec(command)
  if (never !== null) return NO(`it names ${never[1]}, which is never queued without a permission rule`)
  if (GIT_PUSH.test(command)) return NO('it contains git push, which is never queued without a permission rule')
  // `2>&1` and `>&2` duplicate a descriptor: no file, and its & is not a background operator
  const text = command.replace(/\d*>&(?:\d+|-)/g, ' ')
  const s = scan(text)
  if (s.bad !== undefined) return NO(`it contains ${s.bad}`)
  for (const t of s.targets) {
    if (!relPath(t) && !tmpTarget(t)) return NO(`it redirects output to ${t === '' ? 'a missing target' : t}, outside the work tree and $TMPDIR`)
  }
  let any = false
  for (const { seg } of segmentsOf(text)) {
    if (seg.dyn === true || seg.subst === true || !['', '&&', ';', '|'].includes(seg.sep)) return NO('it joins commands with something other than &&, ; or |')
    const why = headOk(withoutRedirects(tokenizeQ(seg.text)), seg.sep === '|', depth)
    if (why !== null) return NO(why)
    any = true
  }
  return any ? OK : NO('the command is empty')
}

// The settings rule that would let the person allow this command: Bash(<first words>:*).
export function suggestRule(command: string): string {
  const first = tokenizeQ(command.trim().split(/\r?\n|&&|;|\|/)[0] ?? '')
    .map(t => t.text)
    .filter(t => !/^[A-Za-z_]\w*=/.test(t))
  const words: string[] = []
  for (const w of first) {
    if (w.startsWith('-') || words.length === 2) break
    words.push(w)
  }
  return `Bash(${words.join(' ') || command.trim().split(/\s+/)[0] || '*'}:*)`
}
