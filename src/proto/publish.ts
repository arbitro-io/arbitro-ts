// Publish family — Publish (0x0101), PublishWithReply (0x0104), PublishBatch (0x0103).

import { HEADER_SIZE, Action } from './constants'
import { frame } from './frame'

/**
 * One entry of a `publishBatch` call.
 *
 * `msgId` (optional) is an opaque byte string the broker uses for
 * per-stream deduplication when the target stream was created with
 * `idempotencyWindowMs > 0`. Leaving it empty/undefined disables
 * dedup for this entry (mixing dedup + non-dedup entries in a single
 * batch is allowed).
 */
export interface BatchPublishEntry {
  subject: string
  payload: Buffer
  msgId?:  Buffer
}

const EMPTY = Buffer.alloc(0)

// Body: stream_id(4) + subject_len(2) + msg_id_len(2) = 8B + subject + msg_id + payload
export function packPublish(
  seq: bigint, streamId: number,
  subject: Buffer, payload: Buffer,
  flags = 0, entryFlags = 0,
  msgId: Buffer = EMPTY,
): Buffer {
  const buf = frame(
    Action.Publish, seq,
    8 + subject.length + msgId.length + payload.length,
    flags, entryFlags,
  )
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt16LE(subject.length, HEADER_SIZE + 4)
  buf.writeUInt16LE(msgId.length, HEADER_SIZE + 6)
  let off = HEADER_SIZE + 8
  subject.copy(buf, off); off += subject.length
  msgId.copy(buf, off);   off += msgId.length
  payload.copy(buf, off)
  return buf
}

// Body: stream_id(4) + subject_len(2) + reply_len(2) + _pad(4) = 12B
export function packPublishWithReply(
  seq: bigint, streamId: number,
  subject: Buffer, replyTo: Buffer, payload: Buffer,
  flags = 0, entryFlags = 0,
): Buffer {
  const tail = subject.length + replyTo.length + payload.length
  const buf = frame(Action.PublishWithReply, seq, 12 + tail, flags, entryFlags)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt16LE(subject.length, HEADER_SIZE + 4)
  buf.writeUInt16LE(replyTo.length, HEADER_SIZE + 6)
  buf.writeUInt32LE(0, HEADER_SIZE + 8)
  let off = HEADER_SIZE + 12
  subject.copy(buf, off); off += subject.length
  replyTo.copy(buf, off); off += replyTo.length
  payload.copy(buf, off)
  return buf
}

// Body: stream_id(4) + count(4) = 8B
// + entries[subject_len(2)+msg_id_len(2)+payload_len(4) + subject + msg_id + payload]
export function packPublishBatch(
  seq: bigint, streamId: number,
  entries: ReadonlyArray<BatchPublishEntry>,
  flags = 0, entryFlags = 0,
): Buffer {
  let tail = 0
  for (const e of entries) {
    const midLen = e.msgId ? e.msgId.length : 0
    tail += 8 + e.subject.length + midLen + e.payload.length
  }
  const buf = frame(Action.PublishBatch, seq, 8 + tail, flags, entryFlags)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt32LE(entries.length, HEADER_SIZE + 4)
  let off = HEADER_SIZE + 8
  for (const e of entries) {
    const mid = e.msgId ?? EMPTY
    buf.writeUInt16LE(e.subject.length, off)
    buf.writeUInt16LE(mid.length,       off + 2)
    buf.writeUInt32LE(e.payload.length, off + 4)
    off += 8
    buf.write(e.subject, off); off += e.subject.length
    mid.copy(buf, off);        off += mid.length
    e.payload.copy(buf, off);  off += e.payload.length
  }
  return buf
}
