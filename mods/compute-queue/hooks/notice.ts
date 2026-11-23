// Text the mod shows: the notice a session gets when its job ends, the disk alert for the lead, the status line.

import type { Capacity } from './plan'

export type NoticeJob = { id: string; status: string; exit?: unknown }

const PREFIX = '[compute-queue]'
const CONTROL = /[\x00-\x1f\x7f-\x9f\u2028\u2029\u202a-\u202e\u2066-\u2069]/g
// Queue-generated IDs, including pre-shard four-digit suffixes and generation two.
const JOB_ID = /^cq-(?:[0-9]{8}T[0-9]{6}-[0-9a-f]{4}|(?:v2--)?(?:[a-z][a-z0-9_]{0,31}-)?[0-9]{8}T[0-9]{6}-[0-9a-f]{16})$/
const TERMINAL = ['done', 'failed', 'timeout', 'cancelled']

const isJobId = (id: unknown): id is string => typeof id === 'string' && JOB_ID.test(id)

export function isNoticeJob(job: unknown): job is NoticeJob {
  if (typeof job !== 'object' || job === null) return false
  const { id, status } = job as Partial<NoticeJob>
  return isJobId(id) && typeof status === 'string' && TERMINAL.includes(status)
}

export function noticeText(job: NoticeJob): string {
  // Prompts and hook context get a summary. Job results stay in the status tool.
  const code = job.exit
  const exit = typeof code === 'number' && Number.isInteger(code) && code >= -(2 ** 31) && code < 2 ** 32 ? `, exit ${code}` : ''
  return `${PREFIX} Job ${job.id} ended (${job.status}${exit}). Use compute-queue status with this job ID for details.`
}

type JobRow = {
  id: string
  status?: string
  workstream?: string
  label?: string
  priority?: string
  slots?: number
  placed_where?: string | null
  host_id?: string | null
  age_min?: number
  elapsed_min?: number | null
}

export type StatusReply = {
  ok: boolean
  error?: string
  queue?: JobRow[]
  recent?: JobRow[]
  capacity?: Capacity | null
  fleet?: { line?: unknown }
  leader?: { session?: string; root?: string; heartbeat?: number } | null
}

// The /cq command's text.
export function statusText(r: StatusReply): string {
  if (!r.ok) return `compute-queue: ${r.error ?? 'no answer from cq.py'}`
  const row = (j: JobRow) =>
    `${j.id} [${j.status}] ${j.workstream ?? '?'} "${j.label ?? ''}" ${j.priority ?? ''} slots=${j.slots ?? 0}` +
    `${j.placed_where ? ` on ${j.placed_where === 'host' ? j.host_id ?? 'host' : j.placed_where}` : ''}${j.elapsed_min != null ? ` ${j.elapsed_min}min` : ` age ${j.age_min ?? 0}min`}`
  const q = r.queue ?? []
  const queued = q.filter(j => j.status === 'queued').length
  const watch = { mine_running: q.filter(j => j.status === 'running' || j.status === 'launching').length, mine_queued: queued, queued }
  const lines = [fleetStatusLine(r.fleet, watch) ?? statusLine(r.capacity, watch, Date.now())]
  if (q.length === 0) lines.push('queue empty')
  else lines.push(...q.map(row))
  if ((r.recent ?? []).length > 0) lines.push('recent:', ...(r.recent ?? []).map(row))
  if (r.leader?.root) lines.push(`leader: ${r.leader.root}`)
  return lines.join('\n')
}

export type Alert = { at: string; job?: string; msg: string; host_id?: string }

export function alertText(a: Alert): string {
  const host = typeof a.host_id === 'string' && /^[a-z][a-z0-9_]{0,31}$/.test(a.host_id) ? a.host_id : 'host'
  const job = isJobId(a.job) ? ` (job ${a.job})` : ''
  return `${PREFIX} ${host} dispatch paused because disk space is low${job}. Free disk in the build user's home on that host, then dispatch resumes on its own at the next probe.`
}

export type WatchCounts = {
  mine_running: number
  mine_queued: number
  queued: number
  // this worktree's finished jobs that still wait for their notice and failed or lack artifacts (not a lifetime total)
  mine_attention?: { failed?: number; missing?: number }
}

// What this session itself has to look at, and a pause of dispatch: the parts of the line somebody can act on.
// Uppercase words are the ones to act on (a host down, stale or paused, dispatch paused); the rest is information.
function mineText(w: WatchCounts | null | undefined, alerts: readonly string[] = []): string {
  const parts = [`mine ${w?.mine_running ?? 0}/${w?.mine_queued ?? 0}`]
  const need: string[] = []
  if (w?.mine_attention?.failed) need.push(`${w.mine_attention.failed} failed`)
  if (w?.mine_attention?.missing) need.push(`${w.mine_attention.missing} missing artifacts`)
  if (need.length > 0) parts.push(`needs you: ${need.join(', ')}`)
  for (const a of alerts) parts.push(a)
  return parts.join(' · ')
}

const STATUS_WIDTH = 100

// The line from the fleet snapshot (devices and live job counts, built by fleet_telemetry) with this session's part.
export function fleetStatusLine(fleet: { line?: unknown } | null | undefined, w: WatchCounts | null | undefined, alerts: readonly string[] = []): string | null {
  const line = typeof fleet?.line === 'string' ? fleet.line.replace(CONTROL, ' ').trim().slice(0, 400) : ''
  if (line === '') return null
  const full = `${line} · ${mineText(w, alerts)}`
  // over a status bar's width: the long words give way (the counts stay)
  return full.length > STATUS_WIDTH ? full.replace(' running', ' run').replace(/ worktrees?/, ' wt') : full
}

// The same wording for a queue that has no fleet snapshot (one host, one laptop).
export function statusLine(cap: Capacity | null | undefined, w: WatchCounts | null | undefined, now: number, alerts: readonly string[] = []): string {
  if (!cap || !cap.host) return 'cq · waiting for the leader'
  const device = (name: string, h: Capacity['host'], sampleAt: number | undefined) =>
    (sampleAt !== undefined ? now / 1000 - sampleAt > 90 : false) ? `${name} STALE` :
      !h.up ? `${name} DOWN` : h.paused ? `${name} PAUSED${h.pause === 'host.conf.json' ? ' (host.conf.json)' : ''}` : `${name} ${h.held}/${h.total}`
  const host = device(cap.host.name || 'ember', cap.host, cap.at)
  const extra = Object.entries(cap.hosts ?? {}).map(([id, remote]) => device(remote.name || id, remote, remote.sample_at))
  const lp = cap.laptop
  // a Mac's laptop runs fleet jobs: its slots, and memory pressure when it is not normal, say more than the heavy lock its pool holds
  const laptop = lp.executor === false ? 'off'
    : lp.legacy === false
      ? `${lp.slots_total !== undefined ? `${lp.slots_used ?? 0}/${lp.slots_total}` : 'fleet'}${lp.memory_pressure && lp.memory_pressure !== 'normal' ? ` ${lp.memory_pressure} pressure` : ''}`
      : lp.heavy_busy ? 'busy' : 'free'
  return [host, ...extra, `laptop ${laptop}`, `${w?.queued ?? 0} queued`, mineText(w, alerts)].join(' · ')
}
