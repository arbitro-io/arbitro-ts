// Delivery lifecycle — Subscribe/Unsubscribe (cold path, JSON) +
// Ack/Nack/BatchAck/BatchNack (hot path, binary).

import { HEADER_SIZE, Action } from './constants'
import { frame } from './frame'

// ── Subscribe / Unsubscribe (cold path, JSON body) ──────────────────────
//
// Mirror of `arbitro_proto::v2::cold::Subscribe`:
//   { consumer_id: u32, subscription_id: u32, filters: Vec<Vec<u8>> }
//
// `subscription_id = 0` selects legacy "subscription_id == consumer_id"
// dispatch on the server. Empty `filters` (or single empty entry) =
// catch-all.

function packCold(action: Action, seq: bigint, body: unknown): Buffer {
  const utf8 = Buffer.from(JSON.stringify(body), 'utf8')
  const buf  = frame(action, seq, utf8.length)
  utf8.copy(buf, HEADER_SIZE)
  return buf
}

export function packSubscribe(
  seq: bigint, _connId: number, consumerId: number,
  filter: Buffer, _optionsFlags = 0,
): Buffer {
  const filters: number[][] =
    filter.length === 0 ? [] : [Array.from(filter)]
  return packCold(Action.Subscribe, seq, {
    consumer_id:     consumerId >>> 0,
    subscription_id: 0,
    filters,
  })
}

export function packUnsubscribe(seq: bigint, _connId: number, consumerId: number): Buffer {
  return packCold(Action.Unsubscribe, seq, { consumer_id: consumerId >>> 0 })
}

// ── Ack / Nack ──────────────────────────────────────────────────────────

// Body: consumer_id(4) + subject_hash(4) + seq(8) = 16B
export function packAck(
  seq: bigint, consumerId: number, subjectHash: number, ackSeq: bigint,
): Buffer {
  const buf = frame(Action.Ack, seq, 16)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(subjectHash, HEADER_SIZE + 4)
  buf.writeBigUInt64LE(ackSeq, HEADER_SIZE + 8)
  return buf
}

export function packNack(
  seq: bigint, consumerId: number, subjectHash: number, nackSeq: bigint,
): Buffer {
  const buf = frame(Action.Nack, seq, 16)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(subjectHash, HEADER_SIZE + 4)
  buf.writeBigUInt64LE(nackSeq, HEADER_SIZE + 8)
  return buf
}

// ── Batch Ack / Nack ────────────────────────────────────────────────────

// Body: consumer_id(4) + count(4) = 8B + entries[seq(8)+subject_hash(4)+_pad(4)] = 16B each
export function packBatchAck(
  seq: bigint, consumerId: number,
  entries: ReadonlyArray<{ seq: bigint; subjectHash: number }>,
): Buffer {
  const buf = frame(Action.BatchAck, seq, 8 + entries.length * 16)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(entries.length, HEADER_SIZE + 4)
  let off = HEADER_SIZE + 8
  for (const e of entries) {
    buf.writeBigUInt64LE(e.seq, off)
    buf.writeUInt32LE(e.subjectHash, off + 8)
    buf.writeUInt32LE(0, off + 12)
    off += 16
  }
  return buf
}

// entries[seq(8)+subject_hash(4)+delay_ms(4)] = 16B each
export function packBatchNack(
  seq: bigint, consumerId: number,
  entries: ReadonlyArray<{ seq: bigint; subjectHash: number; delayMs: number }>,
): Buffer {
  const buf = frame(Action.BatchNack, seq, 8 + entries.length * 16)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(entries.length, HEADER_SIZE + 4)
  let off = HEADER_SIZE + 8
  for (const e of entries) {
    buf.writeBigUInt64LE(e.seq, off)
    buf.writeUInt32LE(e.subjectHash, off + 8)
    buf.writeUInt32LE(e.delayMs, off + 12)
    off += 16
  }
  return buf
}
