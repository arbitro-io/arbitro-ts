// Delivery lifecycle — Subscribe, Unsubscribe, Ack, Nack, BatchAck, BatchNack.

import { HEADER_SIZE, Action } from './constants'
import { frame } from './frame'

// ── Subscribe / Unsubscribe ─────────────────────────────────────────────

// Body: conn_id(4) + consumer_id(4) + filter_len(2) + options_flags(2) = 12B + filter
export function packSubscribe(
  seq: bigint, connId: number, consumerId: number,
  filter: Buffer, optionsFlags = 0,
): Buffer {
  const buf = frame(Action.Subscribe, seq, 12 + filter.length)
  buf.writeUInt32LE(connId, HEADER_SIZE)
  buf.writeUInt32LE(consumerId, HEADER_SIZE + 4)
  buf.writeUInt16LE(filter.length, HEADER_SIZE + 8)
  buf.writeUInt16LE(optionsFlags, HEADER_SIZE + 10)
  filter.copy(buf, HEADER_SIZE + 12)
  return buf
}

// Same body shape, filter_len = 0
export function packUnsubscribe(seq: bigint, connId: number, consumerId: number): Buffer {
  const buf = frame(Action.Unsubscribe, seq, 12)
  buf.writeUInt32LE(connId, HEADER_SIZE)
  buf.writeUInt32LE(consumerId, HEADER_SIZE + 4)
  buf.writeUInt16LE(0, HEADER_SIZE + 8)
  buf.writeUInt16LE(0, HEADER_SIZE + 10)
  return buf
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
