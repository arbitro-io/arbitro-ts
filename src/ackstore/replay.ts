// Log replay — scan a whole file image into per-slot live sets + names.
// Mirrors `scan_bytes` in `ackstore/wal.rs`, including its failure policy:
// the scan stops at the first frame that is torn, CRC-invalid, or
// structurally impossible, and reports the byte offset just past the last
// fully-valid frame so the caller can truncate the tail.
//
// Shared by open (replay) and compaction, exactly like the Rust version.

import {
  FILE_MAGIC, FRAME_CRC_SIZE, FRAME_LEN_SIZE, MAX_PAYLOAD, Op,
  REG_FIXED_PREFIX, SEQ_PAYLOAD_SIZE, SNAP_FIXED_PREFIX, TOMB_PAYLOAD_SIZE,
  crc32c,
} from './format'

/** Live-set accumulator for one slot during replay/compaction: seq -> tsMs. */
export type LiveMap = Map<bigint, number>

export interface ScanResult {
  slots: Map<number, LiveMap>
  names: Map<number, readonly [string, string]>
  /** Byte offset just past the last fully-valid frame (truncation point). */
  good: number
}

/**
 * Scan `data`. `ttlCutoffMs > 0` drops Record/Snapshot frames older than the
 * cutoff so a restart cannot resurrect entries the TTL would have swept.
 */
export function scanBytes(data: Buffer, ttlCutoffMs = 0): ScanResult {
  const slots = new Map<number, LiveMap>()
  const names = new Map<number, readonly [string, string]>()

  if (data.length < FILE_MAGIC.length || !data.subarray(0, FILE_MAGIC.length).equals(FILE_MAGIC)) {
    // No/invalid magic -> nothing recoverable; truncate to empty (the magic
    // is rewritten by the caller).
    return { slots, names, good: 0 }
  }

  const live = (id: number): LiveMap => {
    let m = slots.get(id)
    if (!m) {
      m = new Map()
      slots.set(id, m)
    }
    return m
  }

  let pos = FILE_MAGIC.length
  let good = pos

  // A structurally invalid frame stops the scan (mirrors Rust: a `None` from
  // decode_payload breaks replay and the tail is truncated there). `break scan`
  // — a bare `break` inside the switch would only leave the switch and let the
  // loop keep walking a log it has already proven untrustworthy.
  scan: while (pos + FRAME_LEN_SIZE <= data.length) {
    const plen = data.readUInt32LE(pos)
    if (plen === 0 || plen > MAX_PAYLOAD) break
    const pStart = pos + FRAME_LEN_SIZE
    const crcStart = pStart + plen
    const frameEnd = crcStart + FRAME_CRC_SIZE
    if (frameEnd > data.length) break // torn tail
    if (crc32c(data, pStart, crcStart) !== data.readUInt32LE(crcStart)) break // corrupt

    switch (data.readUInt8(pStart) as Op) {
      case Op.Record: {
        if (plen !== SEQ_PAYLOAD_SIZE) break scan
        const ts = Number(data.readBigUInt64LE(pStart + 5))
        if (ttlCutoffMs > 0 && ts < ttlCutoffMs) break // expired — do not resurrect
        live(data.readUInt32LE(pStart + 1)).set(data.readBigUInt64LE(pStart + 13), ts)
        break
      }
      case Op.Confirm:
      case Op.Expire: {
        if (plen !== SEQ_PAYLOAD_SIZE) break scan
        live(data.readUInt32LE(pStart + 1)).delete(data.readBigUInt64LE(pStart + 13))
        break
      }
      case Op.ConfirmUpTo: {
        if (plen !== SEQ_PAYLOAD_SIZE) break scan
        const m = live(data.readUInt32LE(pStart + 1))
        const cursor = data.readBigUInt64LE(pStart + 13)
        for (const seq of m.keys()) if (seq <= cursor) m.delete(seq)
        break
      }
      case Op.Register: {
        if (plen < REG_FIXED_PREFIX) break scan
        const id = data.readUInt32LE(pStart + 1)
        const sl = data.readUInt16LE(pStart + 13)
        const cl = data.readUInt16LE(pStart + 15)
        if (REG_FIXED_PREFIX + sl + cl !== plen) break scan
        live(id)
        names.set(id, [
          data.toString('utf8', pStart + 17, pStart + 17 + sl),
          data.toString('utf8', pStart + 17 + sl, pStart + 17 + sl + cl),
        ] as const)
        break
      }
      case Op.Tombstone: {
        if (plen !== TOMB_PAYLOAD_SIZE) break scan
        const id = data.readUInt32LE(pStart + 1)
        slots.delete(id)
        names.delete(id)
        break
      }
      case Op.Snapshot: {
        if (plen < SNAP_FIXED_PREFIX) break scan
        const id = data.readUInt32LE(pStart + 1)
        const ts = Number(data.readBigUInt64LE(pStart + 5))
        const count = data.readUInt32LE(pStart + 29)
        if (SNAP_FIXED_PREFIX + count * 8 !== plen) break scan
        const m = live(id)
        m.clear() // a snapshot REPLACES the slot's live set
        if (ttlCutoffMs > 0 && ts < ttlCutoffMs) break
        for (let i = 0; i < count; i++) m.set(data.readBigUInt64LE(pStart + 33 + i * 8), ts)
        break
      }
      default:
        break scan // unknown op -> stop (Rust: decode_payload returns None)
    }
    pos = frameEnd
    good = pos
  }
  return { slots, names, good }
}
