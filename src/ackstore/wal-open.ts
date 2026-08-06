// Open-time replay: rebuild the in-memory slot table from the log, truncate
// any torn/corrupt tail, and hand the writer a clean append point.
//
// Mirrors `Wal::replay` + `ensure_header` in `ackstore/wal.rs`. Robust against
// torn writes: `scanBytes` stops at the first frame it cannot fully validate
// and we cut the file there, so the next append starts on a record boundary.
// The TTL filter runs during the scan, so an expired Record is never
// resurrected by a restart.

import * as fs from 'node:fs'

import { ttlCutoff, type WalConfig } from './config'
import { FILE_MAGIC } from './format'
import { scanBytes } from './replay'
import { slotKey } from './store'
import { WalSlot } from './wal-slot'
import type { WalWriter } from './writer'

export interface OpenedLog {
  byName: Map<string, WalSlot>
  byId: Map<number, WalSlot>
  nextId: number
}

const ascending = (a: bigint, b: bigint): number => (a < b ? -1 : a > b ? 1 : 0)

export function openLog(cfg: WalConfig, writer: WalWriter): OpenedLog {
  fs.mkdirSync(cfg.dir, { recursive: true })
  const out: OpenedLog = { byName: new Map(), byId: new Map(), nextId: 0 }

  const data = fs.existsSync(writer.logPath) ? fs.readFileSync(writer.logPath) : Buffer.alloc(0)
  let size = 0

  if (data.length > 0) {
    const { slots, names, good } = scanBytes(data, ttlCutoff(cfg))
    // `nextId` must clear EVERY id seen in the log — including an orphaned
    // Record-without-Register id skipped below — so a fresh slot never reuses
    // an id and inherits leftover records.
    for (const id of [...slots.keys(), ...names.keys()]) {
      if (id >= out.nextId) out.nextId = id + 1
    }
    for (const [id, live] of [...slots.entries()].sort((a, b) => a[0] - b[0])) {
      const nm = names.get(id)
      if (!nm) continue // orphaned records: no Register, no durable identity
      const slot = new WalSlot(id, nm[0], nm[1], writer)
      slot.live.load(live, [...live.keys()].sort(ascending))
      out.byName.set(slotKey(nm[0], nm[1]), slot)
      out.byId.set(id, slot)
    }
    if (good < data.length) fs.truncateSync(writer.logPath, good)
    size = good
  }

  writer.openAt(size)
  if (size === 0) {
    writer.appendFrame(FILE_MAGIC)
    writer.flush()
  }
  return out
}
