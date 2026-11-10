import RUN_SCHEMA from '../bin/run_schema.mjs'
import type { EngineInterface, Register, Timer } from 'claude-code'

import { computeAllowed, suggestRule } from './allow'
import { classifyBash } from './classify'
import { alertText, fleetStatusLine, noticeText, statusLine, statusText } from './notice'
import type { Alert, NoticeJob } from './notice'
import { buildRequest, gate, parseReply, snapshotKey } from './plan'
import type { Capacity, LeaderMemory, Median, Plan, QueuedJob, RunningJob } from './plan'

const TICK_MS = 15_000
const LEAD_WORKSTREAM = 'lane_plan'

type Reply = Record<string, any> & { ok: boolean; error?: string }

type Tick = Reply & {
  leader: boolean
  took_over?: boolean
  planner?: string // host.conf.json's planner: 'code' (cq.py's deterministic path) or 'haiku'
  capacity?: Capacity
  queued?: QueuedJob[]
  running?: RunningJob[]
  medians?: Median[]
  can_start?: boolean
  alert?: Alert | null
  per_host?: Record<string, Tick>
  watch?: { mine_running: number; mine_queued: number; queued: number; to_notify: NoticeJob[]; top?: string; mine_attention?: { failed?: number; missing?: number } }
  fleet?: { line?: unknown }
}

// Per-process state. A hot reload runs register again and starts these over; the queue itself lives in cq.py's state dir.
// leaderId is the first session id of the process, kept across /clear (the conversation changes id, the process and its
// scheduling leadership do not); a job's own session id is read fresh at submit.
const S = {
  py: 'python',
  script: '',
  leaderId: '',
  root: '',
  timer: undefined as Timer | undefined,
  busy: false,
  delivered: new Set<string>(),
  pendingNoticeMarks: new Set<string>(),
  noticeBusySession: '',
  noticeCursor: 0,
  deliverySession: '',
  // What the last tick found finished and unannounced for this session, for the delivery inside a turn.
  ready: [] as NoticeJob[],
  readyAt: 0,
  readyTop: '',
  // The main loop is inside a turn (turn.start .. turn.complete); turnAt is its last sign of life.
  inTurn: false,
  turnAt: 0,
  alertSubmitting: false,
  alertsDelivered: new Set<string>(),
  mem: { lastCallAt: 0, lastKey: '', lastEmpty: false, lastPlan: null } as LeaderMemory,
}



// One cq.py call: JSON in on stdin, one JSON line out. Never throws; a failure is { ok: false, error }.
async function cq($: EngineInterface, cmd: string, payload: unknown = {}, timeoutMs = 60_000): Promise<Reply> {
  try {
    const r = await $.process.run([S.py, '-X', 'utf8', S.script, cmd], {
      stdin: JSON.stringify(payload),
      timeoutMs,
      env: { PYTHONIOENCODING: 'utf-8' },
    })
    const line = r.stdout.trim().split('\n').pop() ?? ''
    try {
      return JSON.parse(line) as Reply
    } catch {
      return { ok: false, error: (r.stderr || r.stdout || `cq.py exit ${r.exitCode}`).trim().slice(0, 400) }
    }
  } catch (e) {
    return { ok: false, error: `cq.py did not run: ${e instanceof Error ? e.message : String(e)}`.slice(0, 400) }
  }
}

const answer = (value: unknown) => ({ result: typeof value === 'string' ? value : JSON.stringify(value, null, 1) })

// The answer of status or wait for one job. When the queue says the result is now delivered (it marked the job for
// this caller, or the job was marked before), this process does not announce it again. An answer the queue did not
// mark (a subagent's call, a caller that is not the worktree's owner) leaves the notice due for the main loop.
function answered(r: Reply) {
  const job = r.job as { id?: unknown } | undefined
  if (r.ok && r.delivered === true && typeof job?.id === 'string') S.delivered.add(job.id)
  return answer(r)
}

// Who is asking, for the queue's delivered mark: the main loop of the session, at the worktree the tick resolved
// (an attached session's own directory is not its worktree). A subagent's call names nobody: the result it reads
// has not reached the session's main loop.
function seenBy(agentId: unknown, session: string) {
  return agentId === undefined ? { seen_by: { root: S.readyTop || S.root, session } } : {}
}

// Retry marking accepted notices without sending the same prompt a second time.
async function markNotices($: EngineInterface, session: string, top: string) {
  const ids = [...S.pendingNoticeMarks]
  if (ids.length === 0) return
  const r = await cq($, 'notified', { ids, owner_session: session, owner_root: top, notice_protocol: 2 })
  if (r.ok && S.deliverySession === session && Array.isArray(r.acknowledged_ids))
    for (const id of r.acknowledged_ids) S.pendingNoticeMarks.delete(id as string)
}

// A job's result reaches its session once, by whichever way gets there first, and every way records it in the same
// two places (S.delivered here, `notified` in the queue):
//   inside a turn   with the results of the next tool batch (classic.PostToolBatch): no wait for the turn to end,
//                   no polling, nothing queued behind the turn;
//   between turns   as a prompt, which wakes the idle session;
//   asked for       the answer of the wait or status tool for that job.
// A prompt submitted inside a turn would queue behind it and arrive after the same result came with a tool batch,
// so the prompt is kept for an idle session. A turn with no sign of life for STALE_TURN_MS counts as idle: a missed
// turn.complete must not hold results back for good.
const STALE_TURN_MS = 20 * 60_000
const READY_MAX_AGE_MS = 60_000

const turnActive = (now: number) => S.inTurn && now - S.turnAt < STALE_TURN_MS

// Up to five of the waiting jobs, taking turns so that five undeliverable ones cannot hide the rest.
function pick(jobs: NoticeJob[]): NoticeJob[] {
  const waiting = jobs.filter(j => !S.delivered.has(j.id))
  if (waiting.length === 0) return []
  const offset = S.noticeCursor % waiting.length
  const picked = [...waiting.slice(offset), ...waiting.slice(0, offset)].slice(0, 5)
  S.noticeCursor = (offset + picked.length) % waiting.length
  return picked
}

const batchText = (fresh: NoticeJob[]) =>
  fresh.length === 1 ? noticeText(fresh[0]!) :
    `[compute-queue] ${fresh.length} completed jobs:\n` +
    fresh.map(j => noticeText({ ...j, tail: (j.tail ?? []).slice(-8), artifact_text: (j.artifact_text ?? []).slice(0, 1) })).join('\n')

// The delivery inside a turn: the text to hand over with a tool batch, or null. No queue call here (the hook must
// answer at once): the jobs are those the last tick found for this session and owner, under a minute ago, and a
// session in the middle of a turn has not been handed over since. The mark follows; a failed mark is retried by the
// tick and S.delivered keeps this process from sending the result again meanwhile.
function midTurnNotice($: EngineInterface, session: string, now: number): string | null {
  if (S.deliverySession !== session || S.noticeBusySession === session || now - S.readyAt > READY_MAX_AGE_MS) return null
  const fresh = pick(S.ready)
  if (fresh.length === 0) return null
  // the text first: a job that cannot be rendered is not recorded as delivered, and the prompt path still has it
  const text = batchText(fresh) + '\n[compute-queue] This is the completion notice for the job(s) above; it is not sent again.'
  for (const j of fresh) { S.delivered.add(j.id); S.pendingNoticeMarks.add(j.id) }
  void markNotices($, session, S.readyTop).catch(() => undefined)
  return text
}

// One bounded prompt per batch; recheck the worktree's current owner just
// before submitting. An in-flight prompt cannot be recalled after acceptance.
function notify($: EngineInterface, jobs: NoticeJob[], session: string, top: string) {
  if (S.noticeBusySession === session) return
  const ids = pick(jobs).map(j => j.id)
  if (ids.length === 0) return
  S.noticeBusySession = session
  void (async () => {
    const checked = await cq($, 'notice_authorize', { root: top, session, ids, notice_protocol: 2 })
    if (!checked.ok || !Array.isArray(checked.jobs) || S.deliverySession !== session || (await $.session.id()) !== session) return
    // what a tool batch or a wait answered while the queue was asked is not sent again
    const fresh = (checked.jobs as NoticeJob[]).filter(j => !S.delivered.has(j.id))
    if (fresh.length === 0 || turnActive(await $.clock.now())) return
    const text = batchText(fresh)
    const r = await $.prompt.submit({ text })
    if (r.drop !== undefined || S.deliverySession !== session || (await $.session.id()) !== session) return
    for (const j of fresh) { S.delivered.add(j.id); S.pendingNoticeMarks.add(j.id) }
    await markNotices($, session, top)
  })().catch(() => undefined).finally(() => {
    if (S.noticeBusySession === session) S.noticeBusySession = ''
  })
}

function notifyAlert($: EngineInterface, alert: Alert) {
  const key = `${alert.host_id ?? 'ember'}:${alert.at}`
  if (S.alertSubmitting || S.alertsDelivered.has(key)) return
  S.alertSubmitting = true
  void $.prompt
    .submit({ text: alertText(alert) })
    .then(r => {
      if (r.drop !== undefined) return undefined
      S.alertsDelivered.add(key)
      return cq($, 'notified', { ids: [], ack_alert: true, notice_protocol: 2, ...(alert.host_id ? { host_id: alert.host_id } : {}) })
    })
    .catch(() => undefined)
    .finally(() => {
      S.alertSubmitting = false
    })
}

// The leader's decision. Planner 'code' (cq.py's default): the deterministic start-order path plans every round, so a
// tick with startable work sends apply with no plan and makes no model call. Planner 'haiku' (host.conf.json): Haiku
// plans, cq.py's guard enforces, and the fallback covers a reply that is unusable.
async function plan($: EngineInterface, t: Tick) {
  if (t.capacity === undefined || t.queued === undefined) return
  if (t.planner === 'code') {
    if (t.queued.length > 0 && t.can_start === true) await cq($, 'apply', { planner: 'code' }, 120_000)
    return
  }
  const input = { capacity: t.capacity, queued: t.queued, running: t.running ?? [], medians: t.medians ?? [] }
  const now = await $.clock.now()
  const g = gate(S.mem, input, t.can_start === true, now)
  if (g.call) {
    S.mem.lastCallAt = now
    const r = await $.model.complete(buildRequest(input))
    let parsed: Plan | null = null
    let reason = ''
    if (r.isAnswered) {
      parsed = parseReply(r.text)
      if (parsed === null) reason = 'Haiku reply was not usable JSON'
    } else {
      reason = `model call failed: ${r.reason}`
    }
    S.mem.lastKey = snapshotKey(input)
    S.mem.lastPlan = parsed
    S.mem.lastEmpty = parsed !== null && parsed.start.length === 0
    await cq($, 'apply', { plan: parsed, reason, usage: r.isAnswered ? r.usage : undefined }, 120_000)
  } else if (g.reuse !== undefined) {
    await cq($, 'apply', { plan: g.reuse, reason: 'unchanged snapshot, plan reused' }, 120_000)
  }
}

async function tick($: EngineInterface) {
  if (S.busy) return
  S.busy = true
  try {
    const currentSession = await $.session.id()
    if (S.deliverySession !== currentSession) {
      S.deliverySession = currentSession
      S.delivered.clear()
      S.noticeCursor = 0
      S.pendingNoticeMarks.clear()
      S.ready = []
    }
    const t = (await cq($, 'tick', { session: S.leaderId, watch_session: currentSession, root: S.root, notice_protocol: 2 }, 300_000)) as Tick
    if (!t.ok) {
      $.ui.status(`cq · ${String(t.error).slice(0, 160)}`)
      return
    }
    if (t.took_over) $.ui.toast('compute-queue: this session now schedules the queue')
    const top = t.watch?.top ?? S.root
    await markNotices($, currentSession, top)
    const tickedAt = await $.clock.now()
    S.ready = t.watch?.to_notify ?? []
    S.readyAt = tickedAt
    S.readyTop = top
    if (S.ready.length > 0 && !turnActive(tickedAt)) notify($, S.ready, currentSession, top)
    if (t.leader) await plan($, t)
    const w = t.watch
    if (t.alert && w?.top !== undefined && w.top.split('/').pop() === LEAD_WORKSTREAM) notifyAlert($, t.alert)
    if (w?.top !== undefined && w.top.split('/').pop() === LEAD_WORKSTREAM) {
      for (const [host_id, remote] of Object.entries(t.per_host ?? {})) {
        if (remote.alert) notifyAlert($, { ...remote.alert, host_id })
      }
    }
    // an unacknowledged disk alert pauses dispatch on its host: the one fleet-wide condition the snapshot cannot show
    const paused = [t.alert ? (t.alert.host_id ?? 'ember') : '', ...Object.entries(t.per_host ?? {}).map(([id, r]) => (r.alert ? id : ''))].filter(id => id !== '')
    const alerts = [...new Set(paused)].map(id => `${id} DISPATCH PAUSED`)
    $.ui.status(fleetStatusLine(t.fleet, w, alerts) ?? statusLine(t.capacity, w, await $.clock.now(), alerts))
  } finally {
    S.busy = false
  }
}

async function start($: EngineInterface) {
  S.py = (await $.env.get('CQ_PYTHON')) || 'python'
  S.script = `${$.plugin.root.replace(/\\/g, '/')}/bin/cq.py`
  S.leaderId = S.leaderId || (await $.session.id())
  S.root = await $.session.root()
  await $.tool.register({
    name: 'run',
    description:
      'Queue a compute run (go test/build/vet beyond one package, mutation runs, lake, generators, the integ verify, anything heavy). ' +
      'A scheduler places it on the build host (default) or this laptop, splits it when slots idle. When it ends you get one notice ' +
      'with the result (exit, the last log lines, small text artifacts in full): with your next tool results if you are working, ' +
      'as a prompt if you are idle. Keep working meanwhile; do not poll status or read the output files to learn the result. ' +
      'Returns { job, position, note } at once; do not run such commands any other way (remoterun.py, ssh builds and heavyrun.py are blocked).',
    inputSchema: RUN_SCHEMA,
    isDeferred: false,
  })
  await $.tool.register({
    name: 'status',
    description: 'Read cached queue or one job (where it stands, or its result once ended; a result read here is not sent again as a notice; full: true gives the whole stored record). view: fleet gives a compact cached slot/RAM/alert overview without host calls. refresh: true queries one bound deferred/fleet job and retries missing artifacts; captured outputs remain fixed.',
    inputSchema: { type: 'object', properties: { job: { type: 'string' }, refresh: { type: 'boolean' }, full: { type: 'boolean' }, view: { type: 'string', enum: ['fleet'] } } },
    isDeferred: false,
  })
  await $.tool.register({
    name: 'wait',
    description: 'Block until a job ends (up to timeout_s <= 540) and return its result; use it only when nothing else can be done before the result. A result returned here is not sent again as a notice. Otherwise keep working: the notice arrives by itself.',
    inputSchema: {
      type: 'object',
      properties: { job: { type: 'string' }, timeout_s: { type: 'integer', minimum: 1, maximum: 540 } },
      required: ['job'],
    },
    isDeferred: false,
  })
  await $.tool.register({
    name: 'cancel',
    description: 'Cancel a queued job (removed) or a running one (its process group is killed).',
    inputSchema: { type: 'object', properties: { job: { type: 'string' } }, required: ['job'] },
    isDeferred: false,
  })
  await $.command.register({ name: 'cq', description: 'Show the compute queue, or cancel a job', argumentHint: '[cancel <job>]' })
}

// The run tool answers for itself, so the engine's permission check never sees its `command`. Ask the engine now, as for
// a Bash call of that command: the rules and the mode only (no classifier, no hook of ours). Policy (owner 2026-10-17):
// allow queues; deny refuses with the rule's reason; ask queues only a command on the mod's strict compute allowlist
// (allow.ts) and otherwise refuses, naming the rule to add. No dialog anywhere: AskOptions has no timeout, so a dialog
// in an unattended session would hold the run call for good. null = submit; else the reason it is refused.
async function permit($: EngineInterface, command: string): Promise<string | null> {
  let verdict: Awaited<ReturnType<EngineInterface['tool']['check']>>
  try {
    verdict = await $.tool.check({ tool: 'Bash', input: { command } })
  } catch (err) {
    return `compute-queue: the permission check failed (${err instanceof Error ? err.message : String(err)}); nothing was queued`
  }
  if (verdict.decision === 'allow') return null
  if (verdict.decision === 'deny') return `compute-queue: not queued, the permission rules deny this command${verdict.reason ? `: ${verdict.reason}` : ''}`
  const a = computeAllowed(command)
  if (a.ok) return null
  return (
    `compute-queue: not queued: the permission rules ask about this command and it is not on the compute allowlist (${a.why}). ` +
    `No dialog is opened. To allow it, add the rule ${suggestRule(command)} to permissions.allow in a settings file, or run it yourself.`
  )
}

// Hard enforcement (owner choice 2026-10-17): compute runs go through the queue. One judge for the shell tools.
async function judge($: EngineInterface, command: string | undefined): Promise<{ deny: string } | null> {
  if (typeof command !== 'string') return null
  const v = classifyBash(command)
  if (!v.deny) return null
  if ((await $.env.get('COMPUTE_QUEUE_BYPASS')) === '1') {
    void cq($, 'note', { kind: 'bypass', session: S.leaderId, root: S.root, command: command.slice(0, 300) }, 10_000)
    return null
  }
  return { deny: v.reason }
}

// When the judge itself fails the guard still closes the two scripts it exists for.
const backstop = (command: string | undefined): { deny: string } | null =>
  typeof command === 'string' && /remoterun\.py|heavyrun\.py/.test(command)
    ? { deny: 'compute-queue: its guard failed; remoterun.py and heavyrun.py are blocked, use mcp__compute-queue__run' }
    : null

export const register: Register = on => {
  on('session.start', async ($, e, next) => {
    await start($)
    const started = await next(e)
    S.timer?.cancel()
    S.timer = undefined
    // a run with no person at the prompt (claude -p, e.g. bt close) neither watches for notices nor leads
    if (e.isInteractive !== false) {
      S.timer = $.clock.every(TICK_MS, () => {
        void tick($)
      })
      void tick($)
    }
    return started
  })

  on('session.end', async ($, e, next) => {
    // /clear and a resume end the conversation, not the process, and no session.start follows: the timer and the
    // leadership go on (leaderId is the process's, so leader.json still matches)
    if (e.reason === 'clear' || e.reason === 'resume') return next(e)
    const watching = S.timer !== undefined // a non-interactive run never led
    S.timer?.cancel()
    S.timer = undefined
    // the chain shares one short bound (1.5 s): the release gets what is left of it
    if (watching && S.leaderId !== '') {
      await cq($, 'leader', { session: S.leaderId, root: S.root, release: true }, Math.max(300, Math.min(10_000, next.budget.remainingMs - 200)))
    }
    return next(e)
  })

  on('tool.call', { tool: 'mcp__compute-queue__run' }, async ($, e) => {
    const { tool: _tool, tool_use_id: _id, ...args } = e as Record<string, unknown>
    const refused = await permit($, typeof args.command === 'string' ? args.command : '')
    if (refused !== null) return answer({ error: refused })
    const r = await cq($, 'submit', { ...args, session: await $.session.id(), root: S.root })
    return answer(r.ok ? { job: r.job, position: r.position, note: r.note, receipt_warning: r.receipt_warning } : { error: r.error })
  })

  // The main loop's turns, for the choice between the two notice paths. A subagent's turns and tool batches
  // (agentId / agent_id) are not this session's attention and neither start nor receive anything.
  on('turn.start', async ($, e, next) => {
    if (e.agentId === undefined) {
      S.inTurn = true
      S.turnAt = await $.clock.now()
    }
    return next(e)
  })

  on('turn.complete', async ($, e, next) => {
    if (e.agentId === undefined) S.inTurn = false
    return next(e)
  })

  on('classic.PostToolBatch', async ($, e, next) => {
    const r = await next(e)
    if (e.agent_id !== undefined) return r
    const now = await $.clock.now()
    S.inTurn = true
    S.turnAt = now
    const text = midTurnNotice($, e.session_id, now)
    return text === null ? r : { ...r, additionalContext: [...(r.additionalContext ?? []), text] }
  }).catch(($, e, next) => next(e))

  // A finished job the model has just been answered about is delivered: no notice repeats it. The queue marks it
  // (`seen_by`: only for the worktree's owner session); S.delivered covers the notice this process already holds.
  on('tool.call', { tool: 'mcp__compute-queue__status' }, async ($, e) => {
    const { job, refresh, view, full, agentId } = e as Record<string, unknown>
    const one = typeof job === 'string'
    const seen = seenBy(agentId, await $.session.id())
    return answered(await cq($, 'status', {
      ...(one ? { job, ...seen, ...(full === true ? {} : { compact: true }) } : {}),
      ...(refresh === true ? { refresh: true } : {}),
      ...(view === 'fleet' ? { view } : {}),
    }))
  })

  on('tool.call', { tool: 'mcp__compute-queue__wait' }, async ($, e) => {
    const a = e as Record<string, unknown>
    const timeoutS = Math.min(540, Math.max(1, Number(a.timeout_s ?? 540)))
    const seen = seenBy(a.agentId, await $.session.id())
    return answered(await cq($, 'wait', { job: a.job, timeout_s: timeoutS, ...seen }, (timeoutS + 30) * 1000))
  })

  on('tool.call', { tool: 'mcp__compute-queue__cancel' }, async ($, e) => {
    return answer(await cq($, 'cancel', { job: (e as Record<string, unknown>).job, root: S.root, session: await $.session.id() }))
  })

  on('command.run', { command: 'cq' }, async ($, e) => {
    const m = /^\s*cancel\s+(\S+)/.exec(e.args)
    if (m !== null) {
      const r = await cq($, 'cancel', { job: m[1], root: S.root, session: await $.session.id() })
      return { text: r.ok ? `${m[1]}: ${String(r.note)}` : `cancel failed: ${String(r.error)}` }
    }
    return { text: statusText(await cq($, 'status', {}) as Parameters<typeof statusText>[0]) }
  })

  // The same guard on every tool that runs a shell command: Bash, PowerShell (the primary shell on this machine) and
  // Monitor (its optional `command`).
  on('tool.call', { tool: 'Bash' }, async ($, e, next) => (await judge($, e.command)) ?? next(e)).catch(($, e, next) =>
    next.called ? next(e) : (backstop(e.command) ?? next(e)),
  )

  on('tool.call', { tool: 'PowerShell' }, async ($, e, next) => (await judge($, e.command)) ?? next(e)).catch(($, e, next) =>
    next.called ? next(e) : (backstop(e.command) ?? next(e)),
  )

  on('tool.call', { tool: 'Monitor' }, async ($, e, next) => (await judge($, e.command)) ?? next(e)).catch(($, e, next) =>
    next.called ? next(e) : (backstop(e.command) ?? next(e)),
  )
}
