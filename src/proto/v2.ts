// V2 frame builders — one allocation per frame, no concat on hot path.

import {
  HEADER_SIZE, HELLO_SIZE, MAGIC_V2, CURRENT_VERSION,
  OFF_ACTION, OFF_FLAGS, OFF_ENTRY_FLAGS, OFF_MSG_LEN, OFF_SEQ,
  Action, Flag, EntryFlag, Role, Cap,
} from './constants'

// ── Header helpers ──────────────────────────────────────────────────────

export function writeHeader(
  buf: Buffer, off: number,
  action: Action, flags: number, entryFlags: number,
  msgLen: number, seq: bigint,
): void {
  buf.writeUInt16LE(action,     off + OFF_ACTION)
  buf[off + OFF_FLAGS]       = flags
  buf[off + OFF_ENTRY_FLAGS] = entryFlags
  buf.writeUInt32LE(msgLen,     off + OFF_MSG_LEN)
  buf.writeBigUInt64LE(seq,     off + OFF_SEQ)
}

// ── Hello frame (8 bytes, no Header prefix) ─────────────────────────────

export function packHello(caps: number = Cap.Reply): Buffer {
  const buf = Buffer.allocUnsafe(HELLO_SIZE)
  buf.writeUInt32LE(MAGIC_V2, 0)
  buf[4] = CURRENT_VERSION
  buf[5] = Role.Client
  buf.writeUInt16LE(caps, 6)
  return buf
}

// ── Publish (0x0101) ────────────────────────────────────────────────────
// Body: stream_id(4) + subject_len(2) + _pad(2) = 8B + subject + payload

export function packPublish(
  seq: bigint, streamId: number,
  subject: Buffer, payload: Buffer,
  flags = 0, entryFlags = 0,
): Buffer {
  const bodyFixed = 8
  const msgLen = bodyFixed + subject.length + payload.length
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, Action.Publish, flags, entryFlags, msgLen, seq)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt16LE(subject.length, HEADER_SIZE + 4)
  buf.writeUInt16LE(0, HEADER_SIZE + 6)  // _pad
  subject.copy(buf, HEADER_SIZE + bodyFixed)
  payload.copy(buf, HEADER_SIZE + bodyFixed + subject.length)
  return buf
}

// ── PublishWithReply (0x0104) ────────────────────────────────────────────
// Body: stream_id(4) + subject_len(2) + reply_len(2) + _pad(4) = 12B

export function packPublishWithReply(
  seq: bigint, streamId: number,
  subject: Buffer, replyTo: Buffer, payload: Buffer,
  flags = 0, entryFlags = 0,
): Buffer {
  const bodyFixed = 12
  const msgLen = bodyFixed + subject.length + replyTo.length + payload.length
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, Action.PublishWithReply, flags, entryFlags, msgLen, seq)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt16LE(subject.length, HEADER_SIZE + 4)
  buf.writeUInt16LE(replyTo.length, HEADER_SIZE + 6)
  buf.writeUInt32LE(0, HEADER_SIZE + 8)  // _pad
  let off = HEADER_SIZE + bodyFixed
  subject.copy(buf, off); off += subject.length
  replyTo.copy(buf, off); off += replyTo.length
  payload.copy(buf, off)
  return buf
}

// ── PublishBatch (0x0103) ────────────────────────────────────────────────
// Body: stream_id(4) + count(4) = 8B
// Entries: [subject_len(2) + _pad(2) + payload_len(4) = 8B] + subject + payload

export function packPublishBatch(
  seq: bigint, streamId: number,
  entries: ReadonlyArray<{ subject: Buffer; payload: Buffer }>,
  flags = 0, entryFlags = 0,
): Buffer {
  let tailBytes = 0
  for (const e of entries) tailBytes += 8 + e.subject.length + e.payload.length
  const bodyFixed = 8
  const msgLen = bodyFixed + tailBytes
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, Action.PublishBatch, flags, entryFlags, msgLen, seq)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt32LE(entries.length, HEADER_SIZE + 4)
  let off = HEADER_SIZE + bodyFixed
  for (const e of entries) {
    buf.writeUInt16LE(e.subject.length, off)
    buf.writeUInt16LE(0, off + 2)  // _pad
    buf.writeUInt32LE(e.payload.length, off + 4)
    off += 8
    e.subject.copy(buf, off); off += e.subject.length
    e.payload.copy(buf, off); off += e.payload.length
  }
  return buf
}

// ── Subscribe (0x0301) ──────────────────────────────────────────────────
// Body: conn_id(4) + consumer_id(4) + filter_len(2) + options_flags(2) = 12B

export function packSubscribe(
  seq: bigint, connId: number, consumerId: number,
  filter: Buffer, optionsFlags = 0,
): Buffer {
  const bodyFixed = 12
  const msgLen = bodyFixed + filter.length
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, Action.Subscribe, 0, 0, msgLen, seq)
  buf.writeUInt32LE(connId, HEADER_SIZE)
  buf.writeUInt32LE(consumerId, HEADER_SIZE + 4)
  buf.writeUInt16LE(filter.length, HEADER_SIZE + 8)
  buf.writeUInt16LE(optionsFlags, HEADER_SIZE + 10)
  filter.copy(buf, HEADER_SIZE + bodyFixed)
  return buf
}

// ── Unsubscribe (0x0302) — same body shape as Subscribe ─────────────────

export function packUnsubscribe(
  seq: bigint, connId: number, consumerId: number,
): Buffer {
  const bodyFixed = 12
  const buf = Buffer.allocUnsafe(HEADER_SIZE + bodyFixed)
  writeHeader(buf, 0, Action.Unsubscribe, 0, 0, bodyFixed, seq)
  buf.writeUInt32LE(connId, HEADER_SIZE)
  buf.writeUInt32LE(consumerId, HEADER_SIZE + 4)
  buf.writeUInt16LE(0, HEADER_SIZE + 8)   // filter_len = 0
  buf.writeUInt16LE(0, HEADER_SIZE + 10)  // options_flags = 0
  return buf
}

// ── Ack (0x0201) ────────────────────────────────────────────────────────
// Body: consumer_id(4) + subject_hash(4) + ack_seq(8) = 16B

export function packAck(
  seq: bigint, consumerId: number, subjectHash: number, ackSeq: bigint,
): Buffer {
  const bodySize = 16
  const buf = Buffer.allocUnsafe(HEADER_SIZE + bodySize)
  writeHeader(buf, 0, Action.Ack, 0, 0, bodySize, seq)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(subjectHash, HEADER_SIZE + 4)
  buf.writeBigUInt64LE(ackSeq, HEADER_SIZE + 8)
  return buf
}

// ── Nack (0x0202) ───────────────────────────────────────────────────────
// Body: consumer_id(4) + subject_hash(4) + nack_seq(8) = 16B

export function packNack(
  seq: bigint, consumerId: number, subjectHash: number, nackSeq: bigint,
): Buffer {
  const bodySize = 16
  const buf = Buffer.allocUnsafe(HEADER_SIZE + bodySize)
  writeHeader(buf, 0, Action.Nack, 0, 0, bodySize, seq)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(subjectHash, HEADER_SIZE + 4)
  buf.writeBigUInt64LE(nackSeq, HEADER_SIZE + 8)
  return buf
}

// ── BatchAck (0x0206) ───────────────────────────────────────────────────
// Body: consumer_id(4) + count(4) = 8B
// Entries: [seq(8) + subject_hash(4) + _pad(4)] = 16B each

export function packBatchAck(
  seq: bigint, consumerId: number,
  entries: ReadonlyArray<{ seq: bigint; subjectHash: number }>,
): Buffer {
  const bodyFixed = 8
  const entrySize = 16
  const msgLen = bodyFixed + entries.length * entrySize
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, Action.BatchAck, 0, 0, msgLen, seq)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(entries.length, HEADER_SIZE + 4)
  let off = HEADER_SIZE + bodyFixed
  for (const e of entries) {
    buf.writeBigUInt64LE(e.seq, off)
    buf.writeUInt32LE(e.subjectHash, off + 8)
    buf.writeUInt32LE(0, off + 12)  // _pad
    off += entrySize
  }
  return buf
}

// ── BatchNack (0x020A) ──────────────────────────────────────────────────
// Body: consumer_id(4) + count(4) = 8B
// Entries: [seq(8) + subject_hash(4) + delay_ms(4)] = 16B each

export function packBatchNack(
  seq: bigint, consumerId: number,
  entries: ReadonlyArray<{ seq: bigint; subjectHash: number; delayMs: number }>,
): Buffer {
  const bodyFixed = 8
  const entrySize = 16
  const msgLen = bodyFixed + entries.length * entrySize
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, Action.BatchNack, 0, 0, msgLen, seq)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(entries.length, HEADER_SIZE + 4)
  let off = HEADER_SIZE + bodyFixed
  for (const e of entries) {
    buf.writeBigUInt64LE(e.seq, off)
    buf.writeUInt32LE(e.subjectHash, off + 8)
    buf.writeUInt32LE(e.delayMs, off + 12)
    off += entrySize
  }
  return buf
}

// ── CreateStream (0x0401) ───────────────────────────────────────────────
// Body(32B): name_len(2)+filter_len(2)+max_msgs(8)+max_bytes(8)+max_age_secs(8)+replicas(1)+journal_kind(1)+retention(1)+discard(1)

export function packCreateStream(
  seq: bigint, name: Buffer, filter: Buffer,
  maxMsgs: bigint, maxBytes: bigint, maxAgeSecs: bigint,
  replicas = 1, journalKind = 0, retention = 0, discard = 0,
): Buffer {
  const bodyFixed = 32
  const msgLen = bodyFixed + name.length + filter.length
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, Action.CreateStream, 0, 0, msgLen, seq)
  let off = HEADER_SIZE
  buf.writeUInt16LE(name.length, off); off += 2
  buf.writeUInt16LE(filter.length, off); off += 2
  buf.writeBigUInt64LE(maxMsgs, off); off += 8
  buf.writeBigUInt64LE(maxBytes, off); off += 8
  buf.writeBigUInt64LE(maxAgeSecs, off); off += 8
  buf[off++] = replicas
  buf[off++] = journalKind
  buf[off++] = retention
  buf[off++] = discard
  name.copy(buf, off); off += name.length
  filter.copy(buf, off)
  return buf
}

// ── DeleteStream / GetStream / PurgeStream (0x0402/0x0403/0x0405) ───────
// Body(8B): name_len(2) + _pad(6)

function packNamedFrame(action: Action, seq: bigint, name: Buffer): Buffer {
  const bodyFixed = 8
  const msgLen = bodyFixed + name.length
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, action, 0, 0, msgLen, seq)
  buf.writeUInt16LE(name.length, HEADER_SIZE)
  buf.fill(0, HEADER_SIZE + 2, HEADER_SIZE + bodyFixed)  // _pad
  name.copy(buf, HEADER_SIZE + bodyFixed)
  return buf
}

export function packDeleteStream(seq: bigint, name: Buffer): Buffer {
  return packNamedFrame(Action.DeleteStream, seq, name)
}

export function packGetStream(seq: bigint, name: Buffer): Buffer {
  return packNamedFrame(Action.GetStream, seq, name)
}

export function packPurgeStream(seq: bigint, name: Buffer): Buffer {
  return packNamedFrame(Action.PurgeStream, seq, name)
}

// ── DrainSubject (0x0406) ───────────────────────────────────────────────
// Body(8B): name_len(2) + subj_len(2) + _pad(4)

export function packDrainSubject(seq: bigint, name: Buffer, subject: Buffer): Buffer {
  const bodyFixed = 8
  const msgLen = bodyFixed + name.length + subject.length
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, Action.DrainSubject, 0, 0, msgLen, seq)
  buf.writeUInt16LE(name.length, HEADER_SIZE)
  buf.writeUInt16LE(subject.length, HEADER_SIZE + 2)
  buf.writeUInt32LE(0, HEADER_SIZE + 4)  // _pad
  let off = HEADER_SIZE + bodyFixed
  name.copy(buf, off); off += name.length
  subject.copy(buf, off)
  return buf
}

// ── ListStreams (0x0404) ────────────────────────────────────────────────
// Body(8B): offset(4) + limit(4)

export function packListStreams(seq: bigint, offset = 0, limit = 1000): Buffer {
  const bodySize = 8
  const buf = Buffer.allocUnsafe(HEADER_SIZE + bodySize)
  writeHeader(buf, 0, Action.ListStreams, 0, 0, bodySize, seq)
  buf.writeUInt32LE(offset, HEADER_SIZE)
  buf.writeUInt32LE(limit, HEADER_SIZE + 4)
  return buf
}

// ── CreateConsumer (0x0501) ─────────────────────────────────────────────
// Body(32B): name_len(2)+subj_len(2)+stream_id(4)+max_inflight(2)+ack_policy(1)+deliver_policy(1)
//            +deliver_mode(1)+_pad(1)+group_len(2)+ack_wait_ms(4)+start_seq(8)+max_subject_inflight(4)

export interface CreateConsumerOpts {
  streamId:      number
  name:          Buffer
  group:         Buffer
  filter:        Buffer
  maxInflight?:  number
  ackPolicy?:    number
  deliverPolicy?: number
  deliverMode?:  number
  ackWaitMs?:    number
  startSeq?:     bigint
  maxSubjectInflight?: number
}

export function packCreateConsumer(seq: bigint, opts: CreateConsumerOpts): Buffer {
  const bodyFixed = 32
  const { name, group, filter, streamId } = opts
  const msgLen = bodyFixed + name.length + group.length + filter.length
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, Action.CreateConsumer, 0, 0, msgLen, seq)
  let off = HEADER_SIZE
  buf.writeUInt16LE(name.length, off); off += 2
  buf.writeUInt16LE(filter.length, off); off += 2
  buf.writeUInt32LE(streamId, off); off += 4
  buf.writeUInt16LE(opts.maxInflight ?? 0, off); off += 2
  buf[off++] = opts.ackPolicy ?? 1       // 1 = Explicit
  buf[off++] = opts.deliverPolicy ?? 0   // 0 = All
  buf[off++] = opts.deliverMode ?? 0
  buf[off++] = 0                         // _pad
  buf.writeUInt16LE(group.length, off); off += 2
  buf.writeUInt32LE(opts.ackWaitMs ?? 0, off); off += 4
  buf.writeBigUInt64LE(opts.startSeq ?? 0n, off); off += 8
  buf.writeUInt32LE(opts.maxSubjectInflight ?? 0, off); off += 4
  name.copy(buf, off); off += name.length
  group.copy(buf, off); off += group.length
  filter.copy(buf, off)
  return buf
}

// ── DeleteConsumer (0x0502) ─────────────────────────────────────────────
// Body(8B): consumer_id(4) + _pad(4)

export function packDeleteConsumer(seq: bigint, consumerId: number): Buffer {
  const bodySize = 8
  const buf = Buffer.allocUnsafe(HEADER_SIZE + bodySize)
  writeHeader(buf, 0, Action.DeleteConsumer, 0, 0, bodySize, seq)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(0, HEADER_SIZE + 4)
  return buf
}

// ── GetConsumer (0x0503) ────────────────────────────────────────────────
// Body(8B): stream_id(4) + name_len(2) + _pad(2)

export function packGetConsumer(seq: bigint, streamId: number, name: Buffer): Buffer {
  const bodyFixed = 8
  const msgLen = bodyFixed + name.length
  const buf = Buffer.allocUnsafe(HEADER_SIZE + msgLen)
  writeHeader(buf, 0, Action.GetConsumer, 0, 0, msgLen, seq)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt16LE(name.length, HEADER_SIZE + 4)
  buf.writeUInt16LE(0, HEADER_SIZE + 6)
  name.copy(buf, HEADER_SIZE + bodyFixed)
  return buf
}

// ── ListConsumers (0x0504) ──────────────────────────────────────────────
// Body(16B): stream_id(4) + offset(4) + limit(4) + _pad(4)

export function packListConsumers(seq: bigint, streamId = 0, offset = 0, limit = 1000): Buffer {
  const bodySize = 16
  const buf = Buffer.allocUnsafe(HEADER_SIZE + bodySize)
  writeHeader(buf, 0, Action.ListConsumers, 0, 0, bodySize, seq)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt32LE(offset, HEADER_SIZE + 4)
  buf.writeUInt32LE(limit, HEADER_SIZE + 8)
  buf.writeUInt32LE(0, HEADER_SIZE + 12)
  return buf
}

// ── Ping (0x0601) / Disconnect (0x0605) ─────────────────────────────────

export function packPing(seq: bigint): Buffer {
  const buf = Buffer.allocUnsafe(HEADER_SIZE)
  writeHeader(buf, 0, Action.Ping, 0, 0, 0, seq)
  return buf
}

export function packDisconnect(seq: bigint): Buffer {
  const buf = Buffer.allocUnsafe(HEADER_SIZE)
  writeHeader(buf, 0, Action.Disconnect, 0, 0, 0, seq)
  return buf
}
