// Cross-client WAL compatibility harness — the TS half of
// `arbitro/crates/arbitro-client-tokio/examples/ackstore_xcompat.rs`.
//
// The ackstore is a hand-rolled WAL rather than SQLite precisely so that all
// clients share ONE on-disk format. This makes the claim executable: write a
// known log with one client, replay it with another, diff the output.
//
//   npx tsx examples/ackstore-xcompat.ts write <dir>
//   npx tsx examples/ackstore-xcompat.ts read  <dir>
//
// The fixture is identical to the Rust one:
//   orders/worker -> live { 3, 5, 7, 9, 10, 11, 12 }
//   payments/w2   -> live { 42 }
//   empty/slot    -> registered, no live seqs
//
// `read` prints one `stream|consumer|seq,seq,...` line per slot, sorted, so a
// byte-for-byte diff against the Rust output is the assertion. Round-trip:
//
//   cargo run --example ackstore_xcompat -- write /tmp/x && \
//     npx tsx examples/ackstore-xcompat.ts read /tmp/x
//   npx tsx examples/ackstore-xcompat.ts write /tmp/y && \
//     cargo run --example ackstore_xcompat -- read /tmp/y

import * as fs from 'node:fs'
import { Wal } from '../src/ackstore'

function open(dir: string): Wal {
  return Wal.open({ dir, fsync: true })
}

function writeFixture(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true })
  const wal = open(dir)

  const orders = wal.slot('orders', 'worker')
  for (let seq = 1n; seq <= 12n; seq++) orders.checkRecord(seq)
  // Out-of-order removals exercise both removal ops and the min/max gate.
  orders.confirmUpTo(2n)
  orders.confirm(4n)
  orders.confirm(6n)
  orders.confirm(8n)

  wal.slot('payments', 'w2').checkRecord(42n)

  // Registered but empty: its Register must survive so the id can never be
  // reused for a different (stream, consumer).
  wal.slot('empty', 'slot')

  wal.sync()
  wal.close()
  console.log(`wrote fixture to ${dir}`)
}

function readFixture(dir: string): void {
  const wal = open(dir)
  const lines: string[] = []
  for (const info of wal.listSlots()) {
    // SlotInfo carries bounds, not the set — probe the whole range through the
    // public seen() so this reads exactly like a real consumer.
    const slot = wal.slot(info.stream, info.consumer)
    const seqs: string[] = []
    if (info.live > 0) {
      for (let seq = info.minSeq; seq <= info.maxSeq; seq++) {
        if (slot.seen(seq)) seqs.push(seq.toString())
      }
    }
    lines.push(`${info.stream}|${info.consumer}|${seqs.join(',')}`)
  }
  lines.sort()
  for (const l of lines) console.log(l)
  wal.close()
}

const [mode, dir] = process.argv.slice(2)
if (!dir) throw new Error('usage: ackstore-xcompat <write|read> <dir>')
if (mode === 'write') writeFixture(dir)
else if (mode === 'read') readFixture(dir)
else throw new Error(`unknown mode ${mode}, expected write|read`)
