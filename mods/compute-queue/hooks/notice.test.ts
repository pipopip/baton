import { describe, expect, test } from 'claude-code/testing'

import { alertText, fleetStatusLine, isNoticeJob, noticeText, statusLine, statusText } from './notice'
import type { Capacity } from './plan'

const id = 'cq-20261017T101500-ab12'
const done = { id, status: 'done', exit: 0 }

describe('the finish notice', () => {
  test('carries only the machine ID, terminal status, exit, and a lookup hint', () => {
    const payload = 'payload-sentinel'
    const job = {
      ...done, label: payload, reason: payload, host_id: payload, placed_where: payload, minutes: payload,
      log_path: payload, out_paths: [payload], artifact_missing: [payload], artifact_digests: [{ path: payload }],
      artifact_text: [{ path: payload, lines: [payload] }], tail: [payload], parts_info: [{ log: payload }],
    }
    expect(noticeText(job)).toBe(
      `[compute-queue] Job ${id} ended (done, exit 0). Use compute-queue status with this job ID for details.`,
    )
  })

  test('recognizes generated IDs for legacy, shard, fleet, and v2 fleet jobs', () => {
    for (const id of [
      done.id,
      'cq-20261017T101500-0123456789abcdef',
      'cq-hestia-20261017T101500-0123456789abcdef',
      'cq-fleet-20261017T101500-0123456789abcdef',
      'cq-v2--fleet-20261017T101500-0123456789abcdef',
      'cq-v2--hestia-20261017T101500-0123456789abcdef',
    ]) expect(isNoticeJob({ ...done, id })).toBe(true)
  })

  test('rejects any invalid ID or status and omits a malformed exit', () => {
    for (const id of ['cq-spoofed-id', done.id + '\n', done.id + '.p1', 1234]) {
      expect(isNoticeJob({ ...done, id })).toBe(false)
    }
    for (const status of ['done, spurious words', 'running', null]) {
      expect(isNoticeJob({ ...done, status })).toBe(false)
    }
    for (const exit of ['spurious words', true, 1.5, 2 ** 32, -(2 ** 31) - 1]) {
      expect(noticeText({ ...done, status: 'failed', exit })).toContain('ended (failed).')
    }
    expect(noticeText({ ...done, status: 'timeout', exit: 124 })).toContain('ended (timeout, exit 124).')
    expect(noticeText({ ...done, status: 'cancelled', exit: null })).toContain('ended (cancelled).')
  })
})

describe('the lead alert and the status line', () => {
  test('alert text names the job and the way out', () => {
    const alert = { at: '2026-10-17T10:00:00Z', job: id, msg: 'payload-sentinel', host_id: 'hestia' }
    expect(alertText(alert)).toBe(`[compute-queue] hestia dispatch paused because disk space is low (job ${id}). Free disk in the build user's home on that host, then dispatch resumes on its own at the next probe.`)
    expect(alertText({ ...alert, at: 'payload-sentinel', job: 'payload-sentinel', host_id: 'payload-sentinel' })).not.toContain('payload-sentinel')
  })

  const cap = (over: Partial<Capacity['host']> = {}, busy = false): Capacity => ({
    at: 1000,
    host: { up: true, total: 4, held: 2, free: 2, usable: true, paused: false, ...over },
    laptop: { heavy_busy: busy, free_ram_gb: 12 },
  })

  test('device used/total · laptop free|busy · n queued · mine running/queued', () => {
    const w = { mine_running: 2, mine_queued: 1, queued: 5 }
    expect(statusLine(cap(), w, 1_000_000)).toBe('ember 2/4 · laptop free · 5 queued · mine 2/1')
    expect(statusLine(cap({}, true), w, 1_000_000)).toContain('laptop busy')
    expect(statusLine(cap({ up: false }), w, 1_000_000)).toContain('ember DOWN')
    // a Mac's laptop: its fleet slots and the battery, never "busy" because its own pool holds the lock
    const mac = (laptop: Capacity['laptop']): Capacity => ({ ...cap(), laptop })
    expect(statusLine(mac({ heavy_busy: true, free_ram_gb: 30, legacy: false, slots_used: 2, slots_total: 12 }), w, 1_000_000)).toBe('ember 2/4 · laptop 2/12 · 5 queued · mine 2/1')
    expect(statusLine(mac({ heavy_busy: true, free_ram_gb: 30, legacy: false, slots_used: 0, slots_total: 12, on_battery: true }), w, 1_000_000)).toContain('laptop 0/12 ·')   // battery is not in the line
    expect(statusLine(mac({ heavy_busy: false, free_ram_gb: 3, legacy: false, slots_used: 1, slots_total: 18, memory_pressure: 'critical' }), w, 1_000_000)).toContain('laptop 1/18 critical pressure')
    expect(statusLine(mac({ heavy_busy: true, free_ram_gb: 30, legacy: false }), w, 1_000_000)).toContain('laptop fleet')
    expect(statusLine(cap({ paused: true }), w, 1_000_000)).toContain('ember PAUSED')
    expect(statusLine(cap({ paused: true, pause: 'host.conf.json' }), w, 1_000_000)).toContain('ember PAUSED (host.conf.json)')
    expect(statusLine(cap(), w, 1_000_000 + 200_000)).toContain('ember STALE') // the leader has not written for 90 s
    expect(statusLine(null, w, 1)).toBe('cq · waiting for the leader')
  })

  test("this session's finished jobs that need attention are named, and only until they are delivered", () => {
    const w = { mine_running: 0, mine_queued: 0, queued: 0, mine_attention: { failed: 1, missing: 2 } }
    expect(statusLine(cap(), w, 1_000_000)).toContain('mine 0/0 · needs you: 1 failed, 2 missing artifacts')
    expect(statusLine(cap(), { ...w, mine_attention: { failed: 0, missing: 0 } }, 1_000_000)).not.toContain('needs you')
    expect(statusLine(cap(), { ...w, mine_attention: undefined }, 1_000_000)).not.toContain('needs you')
  })

  test("the fleet line gets this session's part; a missing or empty line gives nothing to show", () => {
    const w = { mine_running: 1, mine_queued: 0, queued: 3 }
    const line = 'ember 10/14 · hestia 1/6 legacy busy · 5 running · 8 queued · 4 worktrees'
    const long = 'ember 10/14 · hestia 1/6 legacy busy · laptop 0/4 RAM floor · 5 running · 8 queued · 4 worktrees'
    expect(fleetStatusLine({ line }, w)).toBe(`${line} · mine 1/0`)
    expect(fleetStatusLine({ line }, null)).toBe(`${line} · mine 0/0`)
    expect(fleetStatusLine({ line }, { ...w, mine_attention: { failed: 1 } }, ['ember DISPATCH PAUSED'])).toBe(
      'ember 10/14 · hestia 1/6 legacy busy · 5 run · 8 queued · 4 wt · mine 1/0 · needs you: 1 failed · ember DISPATCH PAUSED')
    // past 100 characters the long words are shortened, nothing else is dropped
    expect(fleetStatusLine({ line: long }, { ...w, mine_running: 12, mine_queued: 10 })).toBe(
      'ember 10/14 · hestia 1/6 legacy busy · laptop 0/4 RAM floor · 5 run · 8 queued · 4 wt · mine 12/10')
    expect(fleetStatusLine(undefined, w)).toBeNull()
    expect(fleetStatusLine({ line: '' }, w)).toBeNull()
    expect(fleetStatusLine({ line: 42 }, w)).toBeNull()
    expect(fleetStatusLine({ line: 'a\nb' }, w)).toBe('a b · mine 1/0')
  })

  test('/cq text lists the queue and the leader', () => {
    const t = statusText({
      ok: true,
      capacity: cap(),
      queue: [{ id: 'cq-1', status: 'queued', workstream: 'lane_k0', label: 'go test', priority: 'lane', slots: 1, age_min: 2 }],
      recent: [],
      leader: { root: 'C:/Work/app/.claude/worktrees/lane_plan' },
    })
    expect(t).toContain('cq-1 [queued] lane_k0')
    expect(t).toContain('leader: C:/Work/app/.claude/worktrees/lane_plan')
    expect(statusText({ ok: false, error: 'boom' })).toContain('boom')
  })

  test('explicit remote capacity is visible without replacing the primary queue', () => {
    const capacity = cap()
    capacity.hosts = { hearth: { ...capacity.host, held: 3, total: 8, sample_at: 1000 } }
    expect(statusLine(capacity, null, 1_000_000)).toContain('ember 2/4 · hearth 3/8')
    expect(statusLine(capacity, null, 1_200_000)).toContain('hearth STALE')
  })
  test('a renamed host displays its name while retaining the queue identity', () => {
    const capacity = cap()
    capacity.hosts = { hearth: { ...capacity.host, name: 'hestia', held: 3, total: 8, sample_at: 1000 } }
    expect(statusLine(capacity, null, 1_000_000)).toContain('hestia 3/8')
    expect(statusLine(capacity, null, 1_200_000)).toContain('hestia STALE')
  })
})
