// Publish family — Publish (0x0101), PublishWithReply (0x0104), PublishBatch (0x0103).

import { HEADER_SIZE, Action } from './constants'
import { frame } from './frame'

export interface BatchPublishEntry {
  subject: string
  payload: Buffer
}

// Body: stream_id(4) + subject_len(2) + _pad(2) = 8B + subject + payload
export function packPublish(
  seq: bigint, streamId: number,
  subject: Buffer, payload: Buffer,
  flags = 0, entryFlags = 0,
): Buffer {
  const buf = frame(Action.Publish, seq, 8 + subject.length + payload.length, flags, entryFlags)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt16LE(subject.length, HEADER_SIZE + 4)
  buf.writeUInt16LE(0, HEADER_SIZE + 6)
  subject.copy(buf, HEADER_SIZE + 8)
  payload.copy(buf, HEADER_SIZE + 8 + subject.length)
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

// Body: stream_id(4) + count(4) = 8B + entries[subject_len(2)+_pad(2)+payload_len(4)+subject+payload]
export function packPublishBatch(
  seq: bigint, streamId: number,
  entries: ReadonlyArray<BatchPublishEntry>,
  flags = 0, entryFlags = 0,
): Buffer {
  let tail = 0
  for (const e of entries) tail += 8 + e.subject.length + e.payload.length
  const buf = frame(Action.PublishBatch, seq, 8 + tail, flags, entryFlags)
  buf.writeUInt32LE(streamId, HEADER_SIZE)
  buf.writeUInt32LE(entries.length, HEADER_SIZE + 4)
  let off = HEADER_SIZE + 8
  for (const e of entries) {
    buf.writeUInt16LE(e.subject.length, off)
    buf.writeUInt16LE(0, off + 2)
    buf.writeUInt32LE(e.payload.length, off + 4)
    off += 8
    buf.write(e.subject, off); off += e.subject.length
    e.payload.copy(buf, off); off += e.payload.length
  }
  return buf
}
