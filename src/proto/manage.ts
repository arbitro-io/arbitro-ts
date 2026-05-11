// Stream + Consumer CRUD frames.

import { HEADER_SIZE, Action } from './constants'
import { frame } from './frame'

// ── Stream management ──────────────────────────────────────────────────

// CreateStream (0x0401) — 32B: name_len(2)+filter_len(2)+max_msgs(8)+max_bytes(8)+max_age(8)+replicas(1)+journal(1)+retention(1)+discard(1)
export function packCreateStream(
  seq: bigint, name: Buffer, filter: Buffer,
  maxMsgs: bigint, maxBytes: bigint, maxAgeSecs: bigint,
  replicas = 1, journalKind = 0, retention = 0, discard = 0,
): Buffer {
  const buf = frame(Action.CreateStream, seq, 32 + name.length + filter.length)
  let off = HEADER_SIZE
  buf.writeUInt16LE(name.length, off);   off += 2
  buf.writeUInt16LE(filter.length, off); off += 2
  buf.writeBigUInt64LE(maxMsgs, off);    off += 8
  buf.writeBigUInt64LE(maxBytes, off);   off += 8
  buf.writeBigUInt64LE(maxAgeSecs, off); off += 8
  buf[off++] = replicas
  buf[off++] = journalKind
  buf[off++] = retention
  buf[off++] = discard
  name.copy(buf, off);   off += name.length
  filter.copy(buf, off)
  return buf
}

// DeleteStream/GetStream/PurgeStream — 8B: name_len(2) + _pad(6)
function packNamedStream(action: Action, seq: bigint, name: Buffer): Buffer {
  const buf = frame(action, seq, 8 + name.length)
  buf.writeUInt16LE(name.length, HEADER_SIZE)
  buf.fill(0, HEADER_SIZE + 2, HEADER_SIZE + 8)
  name.copy(buf, HEADER_SIZE + 8)
  return buf
}

export const packDeleteStream = (s: bigint, n: Buffer) => packNamedStream(Action.DeleteStream, s, n)
export const packGetStream    = (s: bigint, n: Buffer) => packNamedStream(Action.GetStream, s, n)
export const packPurgeStream  = (s: bigint, n: Buffer) => packNamedStream(Action.PurgeStream, s, n)

// DrainSubject (0x0406) — 8B: name_len(2) + subj_len(2) + _pad(4)
export function packDrainSubject(seq: bigint, name: Buffer, subject: Buffer): Buffer {
  const buf = frame(Action.DrainSubject, seq, 8 + name.length + subject.length)
  buf.writeUInt16LE(name.length, HEADER_SIZE)
  buf.writeUInt16LE(subject.length, HEADER_SIZE + 2)
  buf.writeUInt32LE(0, HEADER_SIZE + 4)
  let off = HEADER_SIZE + 8
  name.copy(buf, off);    off += name.length
  subject.copy(buf, off)
  return buf
}

// ListStreams (0x0404) — 8B: offset(4) + limit(4)
export function packListStreams(seq: bigint, offset = 0, limit = 1000): Buffer {
  const buf = frame(Action.ListStreams, seq, 8)
  buf.writeUInt32LE(offset, HEADER_SIZE)
  buf.writeUInt32LE(limit, HEADER_SIZE + 4)
  return buf
}

// ── Consumer management ────────────────────────────────────────────────

/**
 * One per-subject inflight cap. `pattern` accepts NATS wildcards
 * (`*` = one token, `>` = remaining tokens). Effective only when the
 * consumer's `ackPolicy` is Explicit; server drops them otherwise.
 */
export interface WireSubjectLimit {
  pattern: Buffer
  limit:   number
}

export interface CreateConsumerOpts {
  streamId: number; name: Buffer; group: Buffer; filter: Buffer
  maxInflight?: number; ackPolicy?: number; deliverPolicy?: number
  deliverMode?: number; ackWaitMs?: number; startSeq?: bigint
  subjectLimits?: WireSubjectLimit[]
}

// CreateConsumer (0x0501) — 28B fixed + name + group + filter [+ subject_limits trailer]
//
// Optional trailer (only present when subjectLimits non-empty):
//   count u16 || N × (limit u32 + pattern_len u16 + pattern bytes)
//
// Layout mirrors arbitro_proto::wire::manager::CreateConsumerView so the
// command log can replay raw wire body bytes without translation.
export function packCreateConsumer(seq: bigint, opts: CreateConsumerOpts): Buffer {
  const { name, group, filter, streamId } = opts
  const limits = opts.subjectLimits ?? []

  // Pre-compute trailer length.
  let trailerLen = 0
  if (limits.length > 0) {
    trailerLen = 2 // count u16
    for (const l of limits) trailerLen += 6 + l.pattern.length // limit u32 + plen u16 + pattern
  }

  const buf = frame(
    Action.CreateConsumer, seq,
    28 + name.length + group.length + filter.length + trailerLen,
  )
  let off = HEADER_SIZE
  buf.writeUInt16LE(name.length, off);   off += 2
  buf.writeUInt16LE(filter.length, off); off += 2
  buf.writeUInt32LE(streamId, off);      off += 4
  buf.writeUInt16LE(Math.min(opts.maxInflight ?? 0, 0xFFFF), off); off += 2
  buf[off++] = opts.ackPolicy ?? 1
  buf[off++] = opts.deliverPolicy ?? 0
  buf[off++] = opts.deliverMode ?? 0
  buf[off++] = 0
  buf.writeUInt16LE(group.length, off);  off += 2
  buf.writeUInt32LE(opts.ackWaitMs ?? 0, off); off += 4
  buf.writeBigUInt64LE(opts.startSeq ?? 0n, off); off += 8
  name.copy(buf, off);   off += name.length
  group.copy(buf, off);  off += group.length
  filter.copy(buf, off); off += filter.length

  // Subject-limits trailer (optional).
  if (limits.length > 0) {
    buf.writeUInt16LE(limits.length, off); off += 2
    for (const l of limits) {
      buf.writeUInt32LE(l.limit, off);              off += 4
      buf.writeUInt16LE(l.pattern.length, off);     off += 2
      l.pattern.copy(buf, off);                     off += l.pattern.length
    }
  }
  return buf
}

// DeleteConsumer (0x0502) — 8B: consumer_id(4) + _pad(4)
export function packDeleteConsumer(seq: bigint, consumerId: number): Buffer {
  const buf = frame(Action.DeleteConsumer, seq, 8)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(0, HEADER_SIZE + 4)
  return buf
}

// ConsumerStats (0x0505) — 8B: consumer_id(4) + _pad(4)
// Reply is a standard RepOk whose 8-byte body carries the pending-ack
// count as a u64 (little-endian) in place of the usual ref_seq.
export function packConsumerStats(seq: bigint, consumerId: number): Buffer {
  const buf = frame(Action.ConsumerStats, seq, 8)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(0, HEADER_SIZE + 4)
  return buf
}

// GetConsumer (0x0503) — 8B: stream_id(4) + name_len(2) + _pad(2)
export function packGetConsumer(seq: bigint, streamId: number, name: Buffer): Buffer {
  const buf = frame(Action.GetConsumer, seq, 8 + name.length)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt16LE(name.length, HEADER_SIZE + 4)
  buf.writeUInt16LE(0, HEADER_SIZE + 6)
  name.copy(buf, HEADER_SIZE + 8)
  return buf
}

// ListConsumers (0x0504) — 16B: stream_id(4) + offset(4) + limit(4) + _pad(4)
export function packListConsumers(seq: bigint, streamId = 0, offset = 0, limit = 1000): Buffer {
  const buf = frame(Action.ListConsumers, seq, 16)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt32LE(offset, HEADER_SIZE + 4)
  buf.writeUInt32LE(limit, HEADER_SIZE + 8)
  buf.writeUInt32LE(0, HEADER_SIZE + 12)
  return buf
}
