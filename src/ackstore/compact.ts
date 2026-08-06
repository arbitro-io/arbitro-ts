// Log compaction — a WAL never deletes in place, so the file grows with every
// Record/Confirm/Expire/ConfirmUpTo until compaction reclaims the space.
//
// Mirrors `ackstore/compact.rs`: the OLD file (flushed to a stable point) is
// RE-SCANNED to derive the live sets rather than reading the in-memory slots,
// the compacted image is written to a temp file, fsync'd, and atomically
// renamed over the original. After compaction the on-disk representation is
// `magic + {Register, Snapshot}` per slot; in-memory state is untouched.

import * as fs from 'node:fs'
import * as path from 'node:path'

import { COMPACT_FILE, ttlCutoff, type WalConfig } from './config'
import { FILE_MAGIC, encodeRegister, encodeSnapshot } from './format'
import { scanBytes } from './replay'
import type { WalWriter } from './writer'

export function compactLog(writer: WalWriter, cfg: WalConfig): void {
  writer.flush()
  const data = fs.readFileSync(writer.logPath)
  const { slots, names } = scanBytes(data, ttlCutoff(cfg))

  const tmpPath = path.join(cfg.dir, COMPACT_FILE)
  const tmpFd = fs.openSync(tmpPath, 'w')
  let size = 0
  try {
    fs.writeSync(tmpFd, FILE_MAGIC)
    size += FILE_MAGIC.length
    const now = cfg.now()

    // Iterate every REGISTERED slot, not only those with live seqs. A
    // registered-but-empty slot (all seqs confirmed) is still live in memory
    // and can record again; dropping its Register here would orphan those
    // future records on the next replay (slot_id -> name lookup fails) and let
    // the id be reused by a different (stream, consumer). Always re-emit the
    // Register; emit a Snapshot only when there is live state to persist.
    for (const id of [...names.keys()].sort((a, b) => a - b)) {
      const [stream, consumer] = names.get(id)!
      const reg = encodeRegister(id, now, stream, consumer)
      fs.writeSync(tmpFd, reg)
      size += reg.length

      const live = slots.get(id)
      if (!live || live.size === 0) continue
      const seqs = [...live.keys()].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
      const snap = encodeSnapshot(id, now, seqs)
      fs.writeSync(tmpFd, snap)
      size += snap.length
    }
    fs.fsyncSync(tmpFd)
  } finally {
    fs.closeSync(tmpFd)
  }

  writer.replaceFile(() => fs.renameSync(tmpPath, writer.logPath), size)
}
