// The Haiku planning step of the compute-queue leader: the prompt builder, the reply parser and the call gate.
// Pure. Haiku PLANS; cq.py's guard enforces the hard limits and the fallback runs when the reply is unusable.

export type Capacity = {
  at?: number
  host: {
    up: boolean
    total: number
    held: number
    pending?: number
    free: number
    usable: boolean
    paused: boolean
    pause?: string
    load?: string
    mem_avail_gb?: number
    room?: number
    disk_free_gb?: number
    down_since?: string
    name?: string
    nproc?: number
    mem_total_gb?: number
    slot_vcpu?: number
    slot_ram_gb?: number
    ram_floor_gb?: number
    load_cap?: number
  }
  laptop: { heavy_busy: boolean; free_ram_gb: number; ram_ok?: boolean }
  hosts?: Record<string, Capacity['host'] & { sample_at?: number }>
}

export type QueuedJob = {
  id: string
  workstream: string
  label: string
  priority: string
  slots: number
  est_minutes: number
  est_sched?: number | null
  age_min: number
  where: string
  split?: { max_parts: number } | null
  parts_now?: number
  self_heavy?: boolean
  backfill_ok?: boolean
}

export type RunningJob = {
  id: string
  workstream: string
  slots: number
  elapsed_min: number | null
  est_minutes: number
  placed_where?: string
}

export type Median = { kind: string; workstream: string; median_min: number; n: number }

export type PlanInput = {
  capacity: Capacity
  queued: QueuedJob[]
  running: RunningJob[]
  medians: Median[]
}

export type PlanEntry = { job: string; where: string; slots?: number; parts?: number }
export type Plan = { start: PlanEntry[]; why: string }

// The host facts the planner text follows: the live capacity sample (cq.py probe) and the host.conf.json settings.
export type HostFacts = {
  name?: string
  nproc?: number
  mem_total_gb?: number
  total?: number
  slot_vcpu?: number
  slot_ram_gb?: number
  ram_floor_gb?: number
  load_cap?: number
}

// A positive number, or the default when the field is missing or not one (ram_floor_gb may be 0).
const posOr = (v: unknown, dflt: number): number => (typeof v === 'number' && Number.isFinite(v) && v > 0 ? v : dflt)
const nonNegOr = (v: unknown, dflt: number): number => (typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : dflt)

// The planner's system text. With no argument it is the text for the host as the defaults describe it; each field of
// h that is present replaces its default, and a missing or invalid field keeps its own default.
export function systemText(h: HostFacts = {}): string {
  const name = typeof h.name === 'string' && h.name.trim() !== '' ? h.name : 'build-host'
  const nproc = posOr(h.nproc, 12)
  const memGb = Math.round(posOr(h.mem_total_gb, 22))
  const total = posOr(h.total, 8)
  const vcpu = posOr(h.slot_vcpu, 1.5)
  const ram = posOr(h.slot_ram_gb, 2.5)
  const floor = nonNegOr(h.ram_floor_gb, 4)
  const cap = posOr(h.load_cap, nproc)
  return [
    `You schedule compute runs for parallel coding sessions on two machines: the host ${name} (${nproc} vCPU, ${memGb} GB, ${total} heavy slots; one slot is about ${vcpu} vCPU / ${ram} GB) and this laptop (one heavy job at a time).`,
    'Choose which queued jobs to start now, on which machine, with how many slots, and how many parts for a splittable job.',
    'Objective, in order: the lead\'s verify first (priority verify), then fix checks (fixcheck), then lane runs (lane), background last; keep the host\'s slots busy, but never at the cost of the reservation rule below;',
    'RESERVATION RULE, per machine (host, laptop): the head job is the first queued job, in the order the queue list is printed (highest priority first; within a priority, jobs queued longer than 45 min first, oldest first; then the shortest ordering estimate first: sched= when shown, else est=), that can run on that machine with slots >= 1. If the head job does not fit the free slots now, start NO other job with slots >= 1 on that machine, whatever its priority or age (the slots that free up accumulate for the head job, which would otherwise wait forever behind a stream of small jobs); only light jobs (slots 0) and the head job itself may start there. This includes equal-priority jobs: no backfill, except a job marked backfill_ok. A running job is never stopped. If the head job fits, start it first. The scheduler\'s guard enforces this and drops any entry that breaks it.',
    'STARVATION: the guard itself starts a job queued for more than 60 min as soon as it fits and the reservation rule allows it, even if your plan leaves it out.',
    'BACKFILL EXCEPTION: a queued job marked backfill_ok may start now beside the blocked head job, within the free slots, if slots >= 1, not split or self_heavy, estimate at most 30 min, and it ends 2 min before the head job\'s window (half the remaining estimate of the running job that frees them); the guard checks it.',
    'the scheduler splits a splittable job into as many parts as the slots left allow (parts_now shows what it would use now), so you choose only whether and where it starts; among equal priority start short jobs before long ones;',
    'never start more than the free slots; use the laptop only for jobs whose where is laptop or any; the laptop runs one heavy job (slots >= 1) at a time and any number of light ones (slots 0).',
    'A job with slots 0 is light and costs no slot. Do not start a job that does not fit; leave it for a later round. Starting nothing is allowed only when every queued job is waiting for slots. If you start nothing while a fitting job has waited over 2 min, the scheduler starts it itself.',
    `HARD CAPS on the host, measured from the last capacity sample and enforced by the guard (an entry that breaks one is dropped): a job with slots >= 1 starts only if load1 + ${vcpu} x slots <= the load cap (${cap}) and available RAM - ${ram} GB x slots >= ${floor} GB, counting the slots you start in this plan and those launched since the sample; a light job (slots 0) only if load1 < ${cap} + 2. A stale sample (older than 2 min) is no headroom. "caps leave room for N slots" in the capacity text is the most slots you may start on the host now; the head job obeys the caps too, so when it does not fit them start no other job with slots >= 1 (reservation rule); the guard alone lets such a head start when no heavy job has run on the host for 2 samples.`,
    'Reply with one JSON object and nothing else:',
    '{"start":[{"job":"<id>","where":"host|laptop","slots":<int>,"parts":<int, optional>}],"why":"<= 120 chars"}',
    '"slots" is the job\'s own slots (a split job: slots per part); "parts" is optional and only a hint: the scheduler sets the part count to fit the slots left after the other jobs you start, so do not hold slots back for it.',
  ].join('\n')
}

export const SYSTEM_TEXT = systemText()

const round1 = (n: number | null | undefined): number | null => (typeof n === 'number' ? Math.round(n * 10) / 10 : null)

export function formatCapacity(c: Capacity): string {
  const h = c.host
  const host = !h.up
    ? `host: DOWN${h.down_since ? ` since ${h.down_since}` : ''} (no host jobs can start)`
    : h.paused
      ? `host: PAUSED (${h.pause === 'host.conf.json' ? 'host.conf.json' : 'disk low'}; no host jobs can start)`
      : `host: ${h.free} of ${h.total} heavy slots free (held ${h.held}${h.pending ? `, ${h.pending} reserved by launching jobs` : ''}); load ${h.load ?? '?'}; ` +
        `${h.mem_avail_gb ?? '?'} GB RAM available; ${h.disk_free_gb ?? '?'} GB disk free` +
        (h.room !== undefined ? `; caps leave room for ${h.room} slots` : '')
  const l = c.laptop
  const laptop = `laptop: heavy lock ${l.heavy_busy ? 'BUSY' : 'free'}; ${l.free_ram_gb} GB RAM free${l.ram_ok === false ? ' (below the 6 GB floor: no heavy job)' : ''}`
  return `${host}\n${laptop}`
}

export function formatQueued(q: QueuedJob[]): string {
  if (q.length === 0) return '(none)'
  return q
    .map(j => {
      const split = j.split ? ` split.max_parts=${j.split.max_parts}${j.parts_now !== undefined ? ` parts_now=${j.parts_now}` : ''}` : ''
      const sh = j.self_heavy ? ' self_heavy' : ''
      const bf = j.backfill_ok ? ' backfill_ok' : ''
      return `- ${j.id} ws=${j.workstream} "${j.label.replace(/\s+/g, ' ').replace(/"/g, "'").slice(0, 60)}" priority=${j.priority} slots=${j.slots}${sh} est=${j.est_minutes}min${j.est_sched != null && j.est_sched !== j.est_minutes ? ` sched=${j.est_sched}min` : ''} age=${round1(j.age_min)}min where=${j.where}${split}${bf}`
    })
    .join('\n')
}

export function formatRunning(r: RunningJob[]): string {
  if (r.length === 0) return '(none)'
  return r
    .map(j => `- ${j.id} ws=${j.workstream} slots=${j.slots} on ${j.placed_where ?? '?'} elapsed=${round1(j.elapsed_min) ?? '?'}min est=${j.est_minutes}min`)
    .join('\n')
}

export function formatMedians(m: Median[]): string {
  if (m.length === 0) return '(no history yet)'
  return m.map(x => `- ${x.kind} / ${x.workstream}: median ${x.median_min} min over ${x.n} runs`).join('\n')
}

export type ModelRequest = {
  model: 'haiku'
  system: { text: string; cache: true }[]
  prompt: string
  maxTokens: number
  timeoutMs: number
  effort: 'low'
}

export function buildRequest(input: PlanInput): ModelRequest {
  const prompt = [
    'CAPACITY',
    formatCapacity(input.capacity),
    '',
    'QUEUED JOBS (start order: highest priority first; within a priority, jobs queued over 45 min first, oldest first; then the shortest ordering estimate first: sched= when shown, else est=)',
    formatQueued(input.queued),
    '',
    'RUNNING JOBS',
    formatRunning(input.running),
    '',
    'HISTORY (actual minutes of finished runs)',
    formatMedians(input.medians),
    '',
    'Reply with the JSON object only.',
  ].join('\n')
  return { model: 'haiku', system: [{ text: systemText(input.capacity.host), cache: true }], prompt, maxTokens: 800, timeoutMs: 30000, effort: 'low' }
}

// The first balanced {...} in the text (strings and escapes respected).
function firstObject(text: string): string | null {
  const start = text.indexOf('{')
  if (start < 0) return null
  let depth = 0
  let inStr = false
  for (let i = start; i < text.length; i++) {
    const c = text[i] as string
    if (inStr) {
      if (c === '\\') i++
      else if (c === '"') inStr = false
    } else if (c === '"') {
      inStr = true
    } else if (c === '{') {
      depth++
    } else if (c === '}') {
      depth--
      if (depth === 0) return text.slice(start, i + 1)
    }
  }
  return null
}

// A usable plan, or null (prose, invalid JSON, wrong shape): the caller then runs the fallback.
export function parseReply(text: string): Plan | null {
  const cleaned = text.replace(/```(?:json)?/gi, '')
  const raw = firstObject(cleaned)
  if (raw === null) return null
  let obj: unknown
  try {
    obj = JSON.parse(raw)
  } catch {
    return null
  }
  if (typeof obj !== 'object' || obj === null) return null
  const rec = obj as Record<string, unknown>
  if (!Array.isArray(rec.start)) return null
  const start: PlanEntry[] = []
  for (const e of rec.start) {
    if (typeof e !== 'object' || e === null) continue
    const r = e as Record<string, unknown>
    if (typeof r.job !== 'string' || typeof r.where !== 'string') continue
    const entry: PlanEntry = { job: r.job, where: r.where }
    // no slots, or no number, means the job's own: left out, never NaN (JSON turns that into null)
    const slots = typeof r.slots === 'number' ? r.slots : r.slots === undefined || r.slots === null ? NaN : Number(r.slots)
    if (Number.isFinite(slots)) entry.slots = slots
    if (r.parts !== undefined && r.parts !== null) entry.parts = typeof r.parts === 'number' ? r.parts : Number(r.parts)
    start.push(entry)
  }
  const why = typeof rec.why === 'string' ? rec.why.slice(0, 200) : ''
  return { start, why }
}

// ---- the call gate (S14: no call when the queue is empty or nothing fits; at most one call per 20 s) -------------

export type LeaderMemory = {
  lastCallAt: number
  lastKey: string
  lastEmpty: boolean
  lastPlan: Plan | null
}

export const CALL_GAP_MS = 20_000
export const SAME_SNAPSHOT_MS = 120_000

export function snapshotKey(input: PlanInput): string {
  const h = input.capacity.host
  return JSON.stringify([
    input.queued.map(j => j.id),
    input.running.map(j => j.id),
    h.up && h.usable ? h.free : -1,
    input.capacity.laptop.heavy_busy,
  ])
}

export type Gate = { call: true } | { call: false; reason: 'empty' | 'nothing-fits' | 'debounce' | 'unchanged'; reuse?: Plan }

export function gate(mem: LeaderMemory, input: PlanInput, canStart: boolean, now: number): Gate {
  if (input.queued.length === 0) return { call: false, reason: 'empty' }
  if (!canStart) return { call: false, reason: 'nothing-fits' }
  if (now - mem.lastCallAt < CALL_GAP_MS) return { call: false, reason: 'debounce' }
  if (mem.lastEmpty && mem.lastPlan !== null && snapshotKey(input) === mem.lastKey && now - mem.lastCallAt < SAME_SNAPSHOT_MS) {
    return { call: false, reason: 'unchanged', reuse: mem.lastPlan }
  }
  return { call: true }
}
