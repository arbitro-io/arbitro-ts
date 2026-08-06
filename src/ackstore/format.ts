// WAL wire format — byte-identical to the Rust client's
// `ackstore/format.rs` and the Go client's `ackstore/format.go`, so a log
// written by any arbitro client is readable by any other.
//
// File starts with an 8-byte magic, then length-framed records:
//
//   [len u32 LE][payload len bytes][crc32c u32 LE over payload]
//
// Length-framing (not fixed-size records) lets one reader loop handle every
// op and detect torn writes: if the tail can't supply a full frame, replay
// truncates there. crc32c (Castagnoli) guards against silent corruption; a
// bad CRC also truncates. `payload[0]` is the op byte.

/** File magic: "ARBWAL01". */
export const FILE_MAGIC = Buffer.from('ARBWAL01', 'ascii')

/** Op codes (payload[0]) — identical to Rust/Go. */
export const enum Op {
  Register    = 1, // (slot_id, ts, stream, consumer)
  Record      = 2, // (slot_id, ts, seq)  handler ran
  Confirm     = 3, // (slot_id, ts, seq)  broker acked, drop
  Expire      = 4, // (slot_id, ts, seq)  TTL swept, drop
  Tombstone   = 5, // (slot_id, ts)       forget the slot
  Snapshot    = 6, // (slot_id, ts, min, max, count, seqs[])
  ConfirmUpTo = 7, // (slot_id, ts, cursor) drop all <= cursor
}

export const FRAME_LEN_SIZE = 4
export const FRAME_CRC_SIZE = 4
export const FRAME_OVERHEAD = FRAME_LEN_SIZE + FRAME_CRC_SIZE

export const SEQ_PAYLOAD_SIZE = 1 + 4 + 8 + 8 // op+slot+ts+seq          = 21
export const TOMB_PAYLOAD_SIZE = 1 + 4 + 8 // op+slot+ts              = 13
export const REG_FIXED_PREFIX = 1 + 4 + 8 + 2 + 2 // op+slot+ts+slen+clen    = 17
export const SNAP_FIXED_PREFIX = 1 + 4 + 8 + 8 + 8 + 4 // op+slot+ts+min+max+count = 33

export const SEQ_FRAME_SIZE = FRAME_OVERHEAD + SEQ_PAYLOAD_SIZE // 29
export const TOMB_FRAME_SIZE = FRAME_OVERHEAD + TOMB_PAYLOAD_SIZE // 21

/** Payload ceiling used by replay to reject a garbage length prefix. */
export const MAX_PAYLOAD = 64 * 1024 * 1024

/** u16 ceiling on stream/consumer name bytes (the Register length fields). */
export const MAX_NAME_LEN = 65535

// ── crc32c (Castagnoli, reflected poly 0x82F63B78) ─────────────────────────
// Matches Go's crc32.MakeTable(crc32.Castagnoli) and the inline table in
// format.rs. Implemented here so the three clients agree byte for byte with
// zero dependencies.

const CRC32C_POLY = 0x82f63b78
const CRC_TABLE = new Uint32Array(256)
for (let i = 0; i < 256; i++) {
  let crc = i
  for (let j = 0; j < 8; j++) crc = crc & 1 ? (crc >>> 1) ^ CRC32C_POLY : crc >>> 1
  CRC_TABLE[i] = crc >>> 0
}

/** crc32c over `buf[start, end)`. */
export function crc32c(buf: Buffer, start: number, end: number): number {
  let crc = 0xffffffff
  for (let i = start; i < end; i++) crc = (crc >>> 8) ^ CRC_TABLE[(crc ^ buf[i]!) & 0xff]!
  return (crc ^ 0xffffffff) >>> 0
}

// ── encoders ───────────────────────────────────────────────────────────────

/**
 * Encode a Record/Confirm/Expire/ConfirmUpTo frame into `dst` at `off`
 * (`seq` carries the cursor for ConfirmUpTo). Returns bytes written.
 * Zero allocation — the caller owns the buffer.
 */
export function encodeSeqOp(
  dst: Buffer, off: number, op: Op, slotId: number, tsMs: number, seq: bigint,
): number {
  dst.writeUInt32LE(SEQ_PAYLOAD_SIZE, off)
  dst.writeUInt8(op, off + 4)
  dst.writeUInt32LE(slotId, off + 5)
  dst.writeBigUInt64LE(BigInt(tsMs), off + 9)
  dst.writeBigUInt64LE(seq, off + 17)
  dst.writeUInt32LE(crc32c(dst, off + 4, off + 4 + SEQ_PAYLOAD_SIZE), off + 25)
  return SEQ_FRAME_SIZE
}

/** Encode a Tombstone frame into `dst` at `off`. Returns bytes written. */
export function encodeTombstone(dst: Buffer, off: number, slotId: number, tsMs: number): number {
  dst.writeUInt32LE(TOMB_PAYLOAD_SIZE, off)
  dst.writeUInt8(Op.Tombstone, off + 4)
  dst.writeUInt32LE(slotId, off + 5)
  dst.writeBigUInt64LE(BigInt(tsMs), off + 9)
  dst.writeUInt32LE(crc32c(dst, off + 4, off + 4 + TOMB_PAYLOAD_SIZE), off + 17)
  return TOMB_FRAME_SIZE
}

/** Encode a Register frame (cold path — allocates its own buffer). */
export function encodeRegister(
  slotId: number, tsMs: number, stream: string, consumer: string,
): Buffer {
  const sb = Buffer.from(stream, 'utf8')
  const cb = Buffer.from(consumer, 'utf8')
  const plen = REG_FIXED_PREFIX + sb.length + cb.length
  const b = Buffer.allocUnsafe(FRAME_LEN_SIZE + plen + FRAME_CRC_SIZE)
  b.writeUInt32LE(plen, 0)
  b.writeUInt8(Op.Register, 4)
  b.writeUInt32LE(slotId, 5)
  b.writeBigUInt64LE(BigInt(tsMs), 9)
  b.writeUInt16LE(sb.length, 17)
  b.writeUInt16LE(cb.length, 19)
  sb.copy(b, 21)
  cb.copy(b, 21 + sb.length)
  b.writeUInt32LE(crc32c(b, 4, 4 + plen), 4 + plen)
  return b
}

/** Encode a Snapshot frame carrying one slot's whole live set (sorted). */
export function encodeSnapshot(slotId: number, tsMs: number, seqs: readonly bigint[]): Buffer {
  const plen = SNAP_FIXED_PREFIX + seqs.length * 8
  const b = Buffer.allocUnsafe(FRAME_LEN_SIZE + plen + FRAME_CRC_SIZE)
  b.writeUInt32LE(plen, 0)
  b.writeUInt8(Op.Snapshot, 4)
  b.writeUInt32LE(slotId, 5)
  b.writeBigUInt64LE(BigInt(tsMs), 9)
  b.writeBigUInt64LE(seqs[0] ?? 0n, 17)
  b.writeBigUInt64LE(seqs[seqs.length - 1] ?? 0n, 25)
  b.writeUInt32LE(seqs.length, 33)
  let off = 37
  for (const s of seqs) {
    b.writeBigUInt64LE(s, off)
    off += 8
  }
  b.writeUInt32LE(crc32c(b, 4, 4 + plen), 4 + plen)
  return b
}
