import { describe, expect, test } from 'claude-code/testing'

import { buildRequest, CALL_GAP_MS, formatQueued, gate, parseReply, snapshotKey, SYSTEM_TEXT, systemText } from './plan'
import type { Capacity, HostFacts, LeaderMemory, PlanInput, QueuedJob } from './plan'

const capacity = (free = 3): Capacity => ({
  host: { up: true, total: 4, held: 4 - free, free, usable: true, paused: false, load: '0.5 0.4 0.3', mem_avail_gb: 20.1, disk_free_gb: 380 },
  laptop: { heavy_busy: false, free_ram_gb: 14.2, ram_ok: true },
})

const input = (over: Partial<PlanInput> = {}): PlanInput => ({
  capacity: capacity(),
  queued: [
    { id: 'cq-1', workstream: 'lane_k0', label: 'go test pkg', priority: 'lane', slots: 1, est_minutes: 8, age_min: 3.2, where: 'host' },
    { id: 'cq-2', workstream: 'lane_plan', label: 'integ verify', priority: 'verify', slots: 3, est_minutes: 40, age_min: 1, where: 'host', self_heavy: true },
    { id: 'cq-3', workstream: 'lane_write', label: 'mutate', priority: 'background', slots: 1, est_minutes: 30, age_min: 12, where: 'any', split: { max_parts: 4 } },
  ],
  running: [{ id: 'cq-0', workstream: 'lane_auth', slots: 1, elapsed_min: 5.5, est_minutes: 10, placed_where: 'host' }],
  medians: [{ kind: 'go test', workstream: 'lane_k0', median_min: 7, n: 12 }],
  ...over,
})

describe('the prompt builder', () => {
  test('system text holds the objective and the output schema; user text the capacity, queue, running and history', () => {
    const req = buildRequest(input())
    expect(req.model).toBe('haiku')
    expect(req.effort).toBe('low')
    expect(req.timeoutMs).toBe(30000)
    expect(req.maxTokens).toBe(800)
    // the text follows the capacity's host (total 4 in this fixture, so not SYSTEM_TEXT)
    expect(req.system).toEqual([{ text: systemText(capacity().host), cache: true }])
    expect(req.system[0]?.text).toContain('4 heavy slots')
    // the queue header names the start order the list is printed in (not first-come-first-served)
    expect(req.prompt).toContain('QUEUED JOBS (start order:')
    expect(req.prompt).not.toContain('oldest first within priority')
    for (const word of ['verify', 'fixcheck', 'background', 'split', 'never start more than the free slots', 'laptop', '"start"', '"why"', '"parts"']) {
      expect(SYSTEM_TEXT).toContain(word)
    }
    expect(SYSTEM_TEXT).not.toContain('share fairly')
    expect(req.prompt).toContain('host: 3 of 4 heavy slots free')
    expect(req.prompt).toContain('laptop: heavy lock free; 14.2 GB RAM free')
    expect(req.prompt).toContain('- cq-1 ws=lane_k0 "go test pkg" priority=lane slots=1 est=8min age=3.2min where=host')
    expect(req.prompt).toContain('split.max_parts=4')
    expect(req.prompt).toContain('self_heavy')
    expect(req.prompt).toContain('- cq-0 ws=lane_auth slots=1 on host elapsed=5.5min est=10min')
    expect(req.prompt).toContain('go test / lane_k0: median 7 min over 12 runs')
  })

  test('a down or paused host and a busy laptop are stated', () => {
    const down = capacity()
    down.host.up = false
    down.host.down_since = '2026-10-17T10:00:00Z'
    expect(buildRequest(input({ capacity: down })).prompt).toContain('host: DOWN since 2026-10-17T10:00:00Z')
    const paused = capacity()
    paused.host.paused = true
    expect(buildRequest(input({ capacity: paused })).prompt).toContain('host: PAUSED')
    paused.host.pause = 'host.conf.json'
    expect(buildRequest(input({ capacity: paused })).prompt).toContain('host: PAUSED (host.conf.json;')
    const busy = capacity()
    busy.laptop = { heavy_busy: true, free_ram_gb: 4, ram_ok: false }
    const p = buildRequest(input({ capacity: busy })).prompt
    expect(p).toContain('heavy lock BUSY')
    expect(p).toContain('below the 6 GB floor')
  })

  test('empty queue and history render as (none)', () => {
    const p = buildRequest(input({ queued: [], running: [], medians: [] })).prompt
    expect(p).toContain('(none)')
    expect(p).toContain('(no history yet)')
  })

  test('labels cannot break the line format', () => {
    const p = buildRequest(input({ queued: [{ id: 'cq-9', workstream: 'w', label: 'a "quoted"\nlabel '.repeat(10), priority: 'lane', slots: 1, est_minutes: 1, age_min: 0, where: 'host' }] })).prompt
    expect(p.split('\n').filter(l => l.startsWith('- cq-9')).length).toBe(1)
  })
})

describe('S1 the reply parser', () => {
  test('a clean reply', () => {
    const r = parseReply('{"start":[{"job":"cq-1","where":"host","slots":1},{"job":"cq-3","where":"host","slots":1,"parts":3}],"why":"verify first"}')
    expect(r).toEqual({
      start: [
        { job: 'cq-1', where: 'host', slots: 1 },
        { job: 'cq-3', where: 'host', slots: 1, parts: 3 },
      ],
      why: 'verify first',
    })
  })

  test('fenced JSON and prose around it are tolerated', () => {
    const r = parseReply('Here is the plan:\n```json\n{"start":[{"job":"cq-1","where":"laptop","slots":0}],"why":"light"}\n```\nDone.')
    expect(r?.start[0]).toEqual({ job: 'cq-1', where: 'laptop', slots: 0 })
  })

  test('braces and quotes inside the why do not confuse the extraction', () => {
    const r = parseReply('{"start":[],"why":"a } b { \\"c\\""} trailing {')
    expect(r).toEqual({ start: [], why: 'a } b { "c"' })
  })

  test('prose, invalid JSON, the wrong shape: null, so the fallback runs', () => {
    expect(parseReply('I would start cq-1 on the host.')).toBeNull()
    expect(parseReply('')).toBeNull()
    expect(parseReply('{"start": [')).toBeNull()
    expect(parseReply('{"start": "cq-1"}')).toBeNull()
    expect(parseReply('{"jobs": []}')).toBeNull()
    expect(parseReply('[1,2]')).toBeNull()
    expect(parseReply('{start:[]}')).toBeNull()
  })

  test('malformed entries are dropped, numbers given as strings are read, why is cut to 200', () => {
    const r = parseReply(JSON.stringify({ start: [{ job: 'cq-1', where: 'host', slots: '2' }, { where: 'host' }, 5, null, { job: 'cq-2' }], why: 'x'.repeat(500) }))
    expect(r?.start).toEqual([{ job: 'cq-1', where: 'host', slots: 2 }])
    expect(r?.why.length).toBe(200)
  })
})

describe('S8 splitting and missing slots', () => {
  test('an entry without a usable slots has none (never NaN, which JSON turns into null)', () => {
    const r = parseReply('{"start":[{"job":"a","where":"host"},{"job":"b","where":"host","slots":null},{"job":"c","where":"host","slots":"x"},{"job":"d","where":"host","slots":0}],"why":""}')
    expect(r?.start).toEqual([
      { job: 'a', where: 'host' },
      { job: 'b', where: 'host' },
      { job: 'c', where: 'host' },
      { job: 'd', where: 'host', slots: 0 },
    ])
    expect(JSON.stringify(r?.start)).not.toContain('null')
  })

  test('the system text states the measured caps and the slot size; the capacity text shows the room', () => {
    expect(SYSTEM_TEXT).toContain('8 heavy slots')
    expect(SYSTEM_TEXT).toContain('1.5 vCPU / 2.5 GB')
    expect(SYSTEM_TEXT).toContain('HARD CAPS')
    expect(SYSTEM_TEXT).toContain('load1 + 1.5 x slots <= the load cap (12)')
    expect(SYSTEM_TEXT).toContain('load1 < 12 + 2')
    expect(SYSTEM_TEXT).not.toContain('nproc')
    const inp = input()
    inp.capacity.host.room = 2
    expect(buildRequest(inp).prompt).toContain('caps leave room for 2 slots')
  })

  test('the system text states the reservation rule that the guard enforces', () => {
    expect(SYSTEM_TEXT).toContain('RESERVATION RULE')
    expect(SYSTEM_TEXT).toContain('head job')
    expect(SYSTEM_TEXT).toContain('shortest ordering estimate first')
    expect(SYSTEM_TEXT).toContain('does not fit the free slots now, start NO other job with slots >= 1')
    expect(SYSTEM_TEXT).toContain('no backfill')
    expect(SYSTEM_TEXT).toContain('only light jobs (slots 0)')
  })

  test('the system text states the starvation rule, the backfill rule and the idle start the guard makes', () => {
    expect(SYSTEM_TEXT).toContain('STARVATION: the guard itself starts a job queued for more than 60 min as soon as it fits and the reservation rule allows it, even if your plan leaves it out.')
    expect(SYSTEM_TEXT).toContain('BACKFILL EXCEPTION: a queued job marked backfill_ok may start now beside the blocked head job')
    expect(SYSTEM_TEXT).toContain('if slots >= 1, not split or self_heavy, estimate at most 30 min, and it ends 2 min before the head job\'s window')
    expect(SYSTEM_TEXT).not.toContain("its estimate fits the head job's wait")
    expect(SYSTEM_TEXT).toContain('Starting nothing is allowed only when every queued job is waiting for slots. If you start nothing while a fitting job has waited over 2 min, the scheduler starts it itself.')
    expect(SYSTEM_TEXT).toContain('"why":"<= 120 chars"')
    expect(SYSTEM_TEXT).not.toContain('<= 200 chars')
  })

  test('a backfill_ok job is marked in its queue line, and the system text lets it start beside the blocked head job', () => {
    const lane: QueuedJob = { id: 'cq-1', workstream: 'lane_k0', label: 'go test pkg', priority: 'lane', slots: 1, est_minutes: 8, age_min: 3.2, where: 'host' }
    expect(formatQueued([{ ...lane, backfill_ok: true }])).toContain('where=host backfill_ok')
    expect(formatQueued([lane])).not.toContain('backfill_ok')
    expect(SYSTEM_TEXT).toContain('backfill_ok')
    expect(SYSTEM_TEXT).toContain('no backfill')
  })

  test('a calibrated job line shows sched= beside est=, and the order text names it', () => {
    const lane: QueuedJob = { id: 'cq-1', workstream: 'lane_k0', label: 'go test pkg', priority: 'lane', slots: 1, est_minutes: 40, age_min: 3.2, where: 'host' }
    expect(formatQueued([{ ...lane, est_sched: 20 }])).toContain('est=40min sched=20min age=')
    expect(formatQueued([{ ...lane, est_sched: 40 }])).not.toContain('sched=')
    expect(formatQueued([{ ...lane, est_sched: null }])).not.toContain('sched=')
    expect(SYSTEM_TEXT).toContain('shortest ordering estimate first: sched= when shown, else est=')
    expect(buildRequest(input()).prompt).toContain('shortest ordering estimate first: sched= when shown, else est=')
  })

  test('a split job line carries parts_now; the prompt says the scheduler sets the part count', () => {
    const q = input().queued.map(j => (j.split ? { ...j, parts_now: 3 } : j))
    const p = buildRequest(input({ queued: q })).prompt
    expect(p).toContain('split.max_parts=4 parts_now=3')
    expect(p.split('\n').filter(l => l.includes('parts_now')).length).toBe(1)
    expect(buildRequest(input()).prompt).not.toContain('parts_now')
    expect(SYSTEM_TEXT).toContain('parts_now')
    expect(SYSTEM_TEXT).toContain('the scheduler sets the part count')
    expect(SYSTEM_TEXT).not.toContain('parts x slots must fit')
  })
})

describe('the system text follows the live host', () => {
  const ex44: HostFacts = { name: 'lane-ex44', nproc: 20, mem_total_gb: 62.6, total: 13, slot_vcpu: 1.5, slot_ram_gb: 2.5, ram_floor_gb: 6, load_cap: 20 }

  test('with no facts it is SYSTEM_TEXT, the text the defaults describe', () => {
    expect(systemText()).toBe(SYSTEM_TEXT)
    expect(systemText({})).toBe(SYSTEM_TEXT)
    expect(SYSTEM_TEXT).toContain('the host build-host (12 vCPU, 22 GB, 8 heavy slots; one slot is about 1.5 vCPU / 2.5 GB)')
    expect(SYSTEM_TEXT).toContain('load1 + 1.5 x slots <= the load cap (12) and available RAM - 2.5 GB x slots >= 4 GB')
  })

  test('an EX44-like host states its own name, vCPU, RAM, slots and caps', () => {
    const t = systemText(ex44)
    expect(t).toContain('the host lane-ex44 (20 vCPU, 63 GB, 13 heavy slots; one slot is about 1.5 vCPU / 2.5 GB)')
    expect(t).toContain('load1 + 1.5 x slots <= the load cap (20) and available RAM - 2.5 GB x slots >= 6 GB')
    for (const w of ['20 vCPU', '63 GB', '13 heavy slots', 'the load cap (20)', '>= 6 GB', 'load1 < 20 + 2']) {
      expect(t).toContain(w)
    }
    expect(t).not.toContain('12 vCPU')
    expect(t).not.toContain('build-host')
  })

  test('the slot size follows slot_vcpu and slot_ram_gb; the caps show load_cap, or nproc when it is absent', () => {
    const t = systemText({ nproc: 20, slot_vcpu: 2, slot_ram_gb: 3.5 })
    expect(t).toContain('one slot is about 2 vCPU / 3.5 GB')
    expect(t).toContain('load1 + 2 x slots <= the load cap (20) and available RAM - 3.5 GB x slots >= 4 GB')
    expect(systemText({ nproc: 20, load_cap: 18 })).toContain('load1 + 1.5 x slots <= the load cap (18)')
    expect(systemText({ nproc: 20, load_cap: 18 })).toContain('load1 < 18 + 2')
  })

  test('a null load_cap, as a down host reports it, falls back to nproc on its own and keeps the other fields', () => {
    const t = systemText(JSON.parse('{"name":"lane-ex44","nproc":20,"total":13,"slot_vcpu":2,"slot_ram_gb":3,"ram_floor_gb":6,"load_cap":null}'))
    expect(t).toContain('the host lane-ex44 (20 vCPU, 22 GB, 13 heavy slots; one slot is about 2 vCPU / 3 GB)')
    expect(t).toContain('load1 + 2 x slots <= the load cap (20) and available RAM - 3 GB x slots >= 6 GB')
  })

  test('missing or invalid fields fall back to their own defaults', () => {
    const t = systemText({ nproc: 16, total: 0, slot_vcpu: NaN, ram_floor_gb: -1, name: '  ' })
    expect(t).toContain('the host build-host (16 vCPU, 22 GB, 8 heavy slots; one slot is about 1.5 vCPU / 2.5 GB)')
    expect(t).toContain('the load cap (16) and available RAM - 2.5 GB x slots >= 4 GB')
    expect(systemText({ mem_total_gb: 62.6 })).toContain('(12 vCPU, 63 GB, 8 heavy slots')
    expect(systemText({ ram_floor_gb: 0 })).toContain('available RAM - 2.5 GB x slots >= 0 GB')
  })

  test('buildRequest gives the model the text of the capacity host it is given', () => {
    const c = capacity()
    Object.assign(c.host, { name: 'lane-ex44', nproc: 20, mem_total_gb: 62.6, total: 13, slot_vcpu: 1.5, slot_ram_gb: 2.5, ram_floor_gb: 6, load_cap: 20 })
    const text = buildRequest(input({ capacity: c })).system[0]?.text ?? ''
    expect(text).toBe(systemText(c.host))
    expect(text).toContain('the host lane-ex44 (20 vCPU, 63 GB, 13 heavy slots')
    expect(text).toContain('the load cap (20) and available RAM - 2.5 GB x slots >= 6 GB')
    expect(text).not.toContain('12 vCPU')
    expect(buildRequest(input()).system[0]?.text).toBe(systemText(capacity().host))
    expect(buildRequest(input()).system[0]?.text).toContain('the host build-host (12 vCPU, 22 GB, 4 heavy slots;')
  })
})

describe('S14 the call gate', () => {
  const mem = (over: Partial<LeaderMemory> = {}): LeaderMemory => ({ lastCallAt: 0, lastKey: '', lastEmpty: false, lastPlan: null, ...over })

  test('no call when the queue is empty or nothing fits', () => {
    expect(gate(mem(), input({ queued: [] }), true, 1_000_000)).toEqual({ call: false, reason: 'empty' })
    expect(gate(mem(), input(), false, 1_000_000)).toEqual({ call: false, reason: 'nothing-fits' })
  })

  test('at most one call per 20 s', () => {
    const now = 1_000_000
    expect(gate(mem({ lastCallAt: now - 1000 }), input(), true, now)).toEqual({ call: false, reason: 'debounce' })
    expect(gate(mem({ lastCallAt: now - CALL_GAP_MS + 1 }), input(), true, now)).toEqual({ call: false, reason: 'debounce' })
    expect(gate(mem({ lastCallAt: now - CALL_GAP_MS }), input(), true, now)).toEqual({ call: true })
    expect(gate(mem(), input(), true, now)).toEqual({ call: true })
  })

  test('an unchanged snapshot after an empty plan reuses it for two minutes instead of calling', () => {
    const inp = input()
    const plan = { start: [], why: 'wait' }
    const m = mem({ lastCallAt: 1_000_000, lastKey: snapshotKey(inp), lastEmpty: true, lastPlan: plan })
    expect(gate(m, inp, true, 1_000_000 + 30_000)).toEqual({ call: false, reason: 'unchanged', reuse: plan })
    expect(gate(m, inp, true, 1_000_000 + 121_000)).toEqual({ call: true })
    const changed = input({ capacity: capacity(2) })
    expect(gate(m, changed, true, 1_000_000 + 30_000)).toEqual({ call: true })
  })
})
