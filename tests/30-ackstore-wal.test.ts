// Ackstore WAL: dedup, crash recovery, torn/corrupt tail, TTL, snapshot,
// confirmUpTo, compaction. Mirrors the Rust suite in
// `arbitro-client-tokio/src/ackstore/tests.rs` test-for-test.

import { describe, expect, it, afterEach } from 'vitest'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

import { MemoryStore, Wal, LOG_FILE } from '../src/ackstore'
import { crc32c } from '../src/ackstore/format'
import type { WalOptions } from '../src/ackstore'

const dirs: string[] = []
let counter = 0

function tmpDir(): string {
  const p = path.join(os.tmpdir(), `ackstore-ts-${process.pid}-${counter++}`)
  fs.mkdirSync(p, { recursive: true })
  dirs.push(p)
  return p
}

function open(dir: string, fsyncOn = false, extra: Partial<WalOptions> = {}): Wal {
  return Wal.open({ dir, fsync: fsyncOn, ...extra })
}

/** Injectable clock, so TTL is deterministic instead of wall-clock racy. */
function fakeClock(startMs: number) {
  let t = startMs
  return { now: () => t, add: (ms: number) => { t += ms } }
}

afterEach(() => {
  for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true })
})

describe('format', () => {
  it('crc32c matches the Castagnoli check vector', () => {
    const b = Buffer.from('123456789', 'ascii')
    expect(crc32c(b, 0, b.length)).toBe(0xe3069283)
  })
})

describe('basic correctness', () => {
  it('dedups repeated seqs', () => {
    const w = open(tmpDir())
    const s = w.slot('orders', 'worker')
    expect(s.checkRecord(1n)).toBe(true)
    expect(s.checkRecord(1n)).toBe(false)
    expect(s.checkRecord(2n)).toBe(true)
    w.close()
  })

  it('fresh() gates on bounds', () => {
    const w = open(tmpDir())
    const s = w.slot('s', 'c')
    expect(s.fresh(100n)).toBe(true)
    s.checkRecord(50n)
    expect(s.fresh(51n)).toBe(true)
    expect(s.fresh(50n)).toBe(false)
    w.close()
  })

  it('recreating a consumer under the SAME name keeps the dedup set', () => {
    const w = open(tmpDir())
    expect(w.slot('jobs', 'worker').checkRecord(100n)).toBe(true)
    w.sync()
    expect(w.slot('jobs', 'worker').checkRecord(100n)).toBe(false)
    w.close()
  })

  it('a DIFFERENT consumer name is a fresh workload', () => {
    const w = open(tmpDir())
    w.slot('jobs', 'worker').checkRecord(100n)
    expect(w.slot('jobs', 'worker-v2').checkRecord(100n)).toBe(true)
    w.close()
  })

  it('the same seq in different streams is independent', () => {
    const w = open(tmpDir())
    w.slot('orders', 'w').checkRecord(42n)
    expect(w.slot('payments', 'w').checkRecord(42n)).toBe(true)
    w.close()
  })

  // Regression (Rust audit finding 2): async handlers ack out of order, so 101
  // can be recorded before 100. A minSeq taken from the FIFO front would set
  // min=101 and make seen(100) short-circuit false without consulting the set.
  it('records made out of order are still seen', () => {
    const w = open(tmpDir())
    const s = w.slot('s', 'c')
    s.checkRecord(101n)
    s.checkRecord(100n)
    s.checkRecord(102n)
    expect(s.seen(100n)).toBe(true)
    expect(s.seen(101n)).toBe(true)
    expect(s.seen(102n)).toBe(true)
    expect(s.seen(99n)).toBe(false)
    w.close()

    const m = new MemoryStore(0).slot('s', 'c')
    m.checkRecord(101n)
    m.checkRecord(100n)
    expect(m.seen(100n)).toBe(true)
    expect(m.seen(99n)).toBe(false)
  })
})

describe('crash recovery', () => {
  it('recovers every synced record after a restart', () => {
    const dir = tmpDir()
    const w = open(dir, true)
    const s = w.slot('orders', 'worker')
    for (let seq = 1n; seq <= 100n; seq++) s.checkRecord(seq)
    w.sync()
    w.close()

    const w2 = open(dir, true)
    const s2 = w2.slot('orders', 'worker')
    for (let seq = 1n; seq <= 100n; seq++) {
      expect(s2.checkRecord(seq), `seq ${seq} should survive`).toBe(false)
    }
    expect(s2.checkRecord(101n)).toBe(true)
    w2.close()
  })

  it('confirm survives a restart', () => {
    const dir = tmpDir()
    const w = open(dir, true)
    const s = w.slot('s', 'c')
    s.checkRecord(1n); s.checkRecord(2n); s.checkRecord(3n)
    s.confirm(2n)
    w.sync()
    w.close()

    const w2 = open(dir)
    const s2 = w2.slot('s', 'c')
    expect(s2.checkRecord(1n)).toBe(false)
    expect(s2.checkRecord(3n)).toBe(false)
    expect(s2.checkRecord(2n), '2 was confirmed -> fresh again').toBe(true)
    w2.close()
  })

  it('survives a burst of interleaved writes across a restart', () => {
    const dir = tmpDir()
    const w = open(dir)
    const s = w.slot('s', 'c')
    for (let g = 0n; g < 8n; g++) {
      for (let i = 0n; i < 125n; i++) s.checkRecord(g * 125n + i)
    }
    w.sync()
    w.close()

    const w2 = open(dir)
    const s2 = w2.slot('s', 'c')
    let missing = 0
    for (let seq = 0n; seq < 1000n; seq++) if (s2.checkRecord(seq)) missing++
    expect(missing, 'no records lost across restart').toBe(0)
    w2.close()
  })
})

describe('TTL', () => {
  it('sweeps entries older than the TTL', async () => {
    const clk = fakeClock(1_000_000)
    const w = open(tmpDir(), false, { ttlMs: 1000, sweepIntervalMs: 20, now: clk.now })
    const s = w.slot('s', 'c')
    s.checkRecord(1n)
    clk.add(2000)
    for (let i = 0; i < 50 && w.metrics().expired < 1; i++) {
      await new Promise((r) => setTimeout(r, 20))
    }
    expect(w.metrics().expired, 'expected the sweeper to expire seq 1').toBeGreaterThanOrEqual(1)
    expect(s.checkRecord(1n), 'fresh after expiry').toBe(true)
    w.close()
  })

  it('does not resurrect expired entries on restart', () => {
    const dir = tmpDir()
    const clk1 = fakeClock(1_000_000)
    const w = open(dir, true, { now: clk1.now })
    w.slot('s', 'c').checkRecord(1n)
    w.sync()
    w.close()

    // Reopen far in the future with a TTL -> the old record must stay dead.
    const clk2 = fakeClock(1_000_000 + 10_000_000)
    const w2 = open(dir, false, { ttlMs: 1000, sweepIntervalMs: 60_000, now: clk2.now })
    expect(w2.slot('s', 'c').checkRecord(1n)).toBe(true)
    w2.close()
  })
})

describe('corruption', () => {
  it('truncates a torn tail and keeps writing', () => {
    const dir = tmpDir()
    const w = open(dir, true)
    const s = w.slot('s', 'c')
    s.checkRecord(1n); s.checkRecord(2n)
    w.sync()
    w.close()

    fs.appendFileSync(path.join(dir, LOG_FILE), Buffer.from([0xff, 0xff, 0x00]))

    const w2 = open(dir)
    const s2 = w2.slot('s', 'c')
    expect(s2.checkRecord(1n)).toBe(false)
    expect(s2.checkRecord(2n)).toBe(false)
    expect(s2.checkRecord(3n), 'writes after recovery work').toBe(true)
    w2.sync()
    w2.close()
  })

  it('truncates at a bad CRC, keeping everything before it', () => {
    const dir = tmpDir()
    const w = open(dir, true)
    const s = w.slot('s', 'c')
    s.checkRecord(1n); s.checkRecord(2n); s.checkRecord(3n)
    w.sync()
    w.close()

    const p = path.join(dir, LOG_FILE)
    const data = fs.readFileSync(p)
    data[data.length - 6] ^= 0xff
    fs.writeFileSync(p, data)

    const w2 = open(dir)
    expect(w2.slot('s', 'c').checkRecord(1n), 'pre-corruption record survives').toBe(false)
    w2.close()
  })
})

describe('snapshot', () => {
  it('restores from a snapshot plus the records after it', () => {
    const dir = tmpDir()
    const w = open(dir, true)
    const s = w.slot('s', 'c')
    for (let seq = 1n; seq <= 50n; seq++) s.checkRecord(seq)
    w.snapshot()
    for (let seq = 51n; seq <= 60n; seq++) s.checkRecord(seq)
    s.confirm(55n)
    w.sync()
    w.close()

    const w2 = open(dir)
    const s2 = w2.slot('s', 'c')
    for (let seq = 1n; seq <= 60n; seq++) {
      expect(s2.checkRecord(seq), `seq ${seq}`).toBe(seq === 55n)
    }
    w2.close()
  })
})

describe('confirmUpTo', () => {
  it('drops the whole range at once', () => {
    const w = open(tmpDir())
    const s = w.slot('s', 'c')
    for (let seq = 1n; seq <= 100n; seq++) s.checkRecord(seq)
    expect(s.confirmUpTo(50n)).toBe(50)
    for (let seq = 1n; seq <= 50n; seq++) expect(s.seen(seq), `${seq} dropped`).toBe(false)
    for (let seq = 51n; seq <= 100n; seq++) expect(s.seen(seq), `${seq} kept`).toBe(true)
    expect(s.info().live).toBe(50)
    w.close()
  })

  it('is a no-op (and writes no frame) when nothing is below the cursor', () => {
    const w = open(tmpDir())
    const s = w.slot('s', 'c')
    s.checkRecord(100n)
    const before = w.metrics().fileSize
    expect(s.confirmUpTo(50n)).toBe(0)
    expect(w.metrics().fileSize).toBe(before)
    w.close()
  })

  it('survives a restart', () => {
    const dir = tmpDir()
    const w = open(dir, true)
    const s = w.slot('s', 'c')
    for (let seq = 1n; seq <= 100n; seq++) s.checkRecord(seq)
    s.confirmUpTo(60n)
    w.sync()
    w.close()

    const w2 = open(dir)
    const s2 = w2.slot('s', 'c')
    for (let seq = 1n; seq <= 60n; seq++) {
      expect(s2.checkRecord(seq), `seq ${seq} fresh after confirm`).toBe(true)
    }
    w2.close()
  })
})

describe('compaction', () => {
  it('shrinks the file and keeps the live set', () => {
    const w = open(tmpDir(), true)
    const s = w.slot('s', 'c')
    for (let seq = 1n; seq <= 10_000n; seq++) s.checkRecord(seq)
    s.confirmUpTo(9990n)
    w.sync()
    const before = w.metrics().fileSize
    w.compact()
    const after = w.metrics().fileSize
    expect(after, `compaction shrinks: ${before} -> ${after}`).toBeLessThan(before)
    for (let seq = 9991n; seq <= 10_000n; seq++) expect(s.seen(seq), `${seq} kept`).toBe(true)
    expect(s.seen(5000n)).toBe(false)
    expect(s.checkRecord(20_000n), 'writes after compaction work').toBe(true)
    w.close()
  })

  it('survives a restart', () => {
    const dir = tmpDir()
    const w = open(dir, true)
    const s = w.slot('orders', 'worker')
    for (let seq = 1n; seq <= 500n; seq++) s.checkRecord(seq)
    s.confirmUpTo(490n)
    w.sync()
    w.compact()
    w.close()

    const w2 = open(dir)
    const s2 = w2.slot('orders', 'worker')
    for (let seq = 491n; seq <= 500n; seq++) {
      expect(s2.checkRecord(seq), `seq ${seq} survives compact+restart`).toBe(false)
    }
    expect(s2.checkRecord(100n), 'confirmed 100 is fresh again').toBe(true)
    w2.close()
  })

  // Regression (Rust audit finding 1): a fully-confirmed slot must keep its
  // Register through compaction, or a record written to it afterwards has no
  // slot_id -> name mapping at replay and is silently dropped.
  it('keeps the Register of an emptied slot, so later records replay', () => {
    const dir = tmpDir()
    const w = open(dir, true)
    const s = w.slot('jobs', 'worker')
    for (let seq = 1n; seq <= 100n; seq++) s.checkRecord(seq)
    s.confirmUpTo(100n)
    w.sync()
    w.compact()
    expect(s.checkRecord(200n)).toBe(true)
    w.sync()
    w.close()

    const w2 = open(dir)
    const s2 = w2.slot('jobs', 'worker')
    expect(s2.checkRecord(200n), 'post-compaction record survives restart').toBe(false)
    expect(s2.checkRecord(50n), 'confirmed 50 is fresh again').toBe(true)
    w2.close()
  })

  it('auto-compacts on sync once the file passes the threshold', () => {
    const w = open(tmpDir(), false, { compactAtBytes: 8 * 1024 })
    const s = w.slot('s', 'c')
    for (let round = 0n; round < 20n; round++) {
      const base = round * 1000n
      for (let i = 0n; i < 1000n; i++) s.checkRecord(base + i)
      s.confirmUpTo(base + 999n)
      w.sync()
    }
    expect(w.metrics().fileSize, 'auto-compaction bounds the file').toBeLessThanOrEqual(64 * 1024)
    w.close()
  })
})

describe('admin surface', () => {
  it('lists slots, reports info, and tombstones on delete', () => {
    const dir = tmpDir()
    const w = open(dir, true)
    w.slot('a', 'c1').checkRecord(5n)
    w.slot('b', 'c2').checkRecord(7n)
    expect(w.listSlots().map((s) => s.stream)).toEqual(['a', 'b'])
    expect(w.slotInfo('a', 'c1')?.live).toBe(1)
    expect(w.slotInfo('nope', 'nope')).toBeUndefined()

    w.deleteSlot('a', 'c1')
    expect(w.slotInfo('a', 'c1')).toBeUndefined()
    expect(w.metrics().tombstoned).toBe(1)
    w.sync()
    w.close()

    const w2 = open(dir)
    expect(w2.listSlots().map((s) => s.stream), 'tombstone replays').toEqual(['b'])
    // The tombstoned id must NOT be handed to a new (stream, consumer).
    expect(w2.slot('c', 'c3').info().slotId).toBeGreaterThan(1)
    w2.close()
  })

  it('memory backend implements the same contract', () => {
    const m = new MemoryStore(0)
    const s = m.slot('s', 'c')
    expect(s.checkRecord(1n)).toBe(true)
    expect(s.checkRecord(1n)).toBe(false)
    expect(s.confirmUpTo(1n)).toBe(1)
    expect(s.checkRecord(1n), 'fresh after confirmUpTo').toBe(true)
    m.sync(); m.restore(); m.close()
  })
})
