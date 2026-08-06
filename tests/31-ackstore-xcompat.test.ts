// Cross-client on-disk compatibility.
//
// The ackstore is a hand-rolled WAL instead of SQLite for exactly one reason:
// every arbitro client reads and writes the SAME bytes. `ackstore-rust.log` is
// a fixture produced by the RUST client
// (`cargo run --example ackstore_xcompat -- write <dir>`, then copied here).
// If this test ever fails, the TS format has drifted from the Rust one and the
// whole justification for the hand-rolled WAL is gone.
//
// The other direction (TS writes, Rust reads) is covered by
// `examples/ackstore-xcompat.ts`, whose output is byte-comparable with the
// Rust example's — see the header of either file for the two commands.

import { describe, expect, it, afterEach } from 'vitest'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

import { LOG_FILE, Wal } from '../src/ackstore'

const FIXTURE = path.join(__dirname, 'fixtures', 'ackstore-rust.log')

/** Exactly what the Rust reader prints for this fixture. */
const EXPECTED = [
  'empty|slot|',
  'orders|worker|3,5,7,9,10,11,12',
  'payments|w2|42',
]

const dirs: string[] = []
afterEach(() => {
  for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true })
})

function stage(logBytes: Buffer): string {
  const dir = path.join(os.tmpdir(), `ackstore-xc-${process.pid}-${dirs.length}`)
  fs.mkdirSync(dir, { recursive: true })
  dirs.push(dir)
  fs.writeFileSync(path.join(dir, LOG_FILE), logBytes)
  return dir
}

/** Same rendering as both xcompat examples: one sorted line per slot. */
function render(wal: Wal): string[] {
  const lines: string[] = []
  for (const info of wal.listSlots()) {
    const slot = wal.slot(info.stream, info.consumer)
    const seqs: string[] = []
    if (info.live > 0) {
      for (let seq = info.minSeq; seq <= info.maxSeq; seq++) {
        if (slot.seen(seq)) seqs.push(seq.toString())
      }
    }
    lines.push(`${info.stream}|${info.consumer}|${seqs.join(',')}`)
  }
  return lines.sort()
}

describe('cross-client WAL format', () => {
  it('replays a log written by the Rust client with an identical live set', () => {
    const dir = stage(fs.readFileSync(FIXTURE))
    const wal = Wal.open({ dir })
    expect(render(wal)).toEqual(EXPECTED)
    wal.close()
  })

  it('round-trips: TS rewrites the Rust log and still reads the same state', () => {
    const dir = stage(fs.readFileSync(FIXTURE))
    const wal = Wal.open({ dir, fsync: true })
    // Append TS-authored frames on top of Rust-authored ones, then compact so
    // the file is fully re-encoded by the TS writer.
    wal.slot('orders', 'worker').checkRecord(13n)
    wal.sync()
    wal.compact()
    wal.close()

    const reopened = Wal.open({ dir })
    expect(render(reopened)).toEqual([
      'empty|slot|',
      'orders|worker|3,5,7,9,10,11,12,13',
      'payments|w2|42',
    ])
    reopened.close()
  })

  it('the fixture really is Rust-authored: magic + framing are intact', () => {
    const data = fs.readFileSync(FIXTURE)
    expect(data.subarray(0, 8).toString('ascii')).toBe('ARBWAL01')
    // First frame after the magic is orders/worker's Register (op 1).
    expect(data.readUInt8(12)).toBe(1)
    expect(data.subarray(29, 41).toString('ascii')).toBe('ordersworker')
  })
})
