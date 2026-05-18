import { describe, it, expect } from 'vitest'
import {
  packCreateStream, packCreateConsumer, packDeleteStream, packDrainSubject,
  packPublish, packPublishBatch,
} from '../src/proto/v2'
import { Action, HEADER_SIZE, OFF_ACTION } from '../src/proto/constants'

// Cold-path management frames are JSON-encoded bodies after the 16B header.
// Mirror of `arbitro_proto::v2::cold` server-side. Hot path (publish/ack)
// stays binary.

/** Parse the JSON body of a cold-path frame. */
function bodyJson(frame: Buffer): any {
  return JSON.parse(frame.subarray(HEADER_SIZE).toString('utf8'))
}

describe('packCreateStream (cold/JSON)', () => {
  it('encodes name, filter, retention fields', () => {
    const frame = packCreateStream(
      1n, Buffer.from('orders'), Buffer.from('orders.>'),
      1000n, 500n, 3600n, 1, 1, 0, 0,
    )
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.CreateStream)
    const body = bodyJson(frame)
    expect(body.name).toEqual(Array.from(Buffer.from('orders')))
    expect(body.filter).toEqual(Array.from(Buffer.from('orders.>')))
    expect(body.max_msgs).toBe(1000)
    expect(body.max_bytes).toBe(500)
    expect(body.max_age_secs).toBe(3600)
    expect(body.replicas).toBe(1)
    expect(body.journal_kind).toBe(1)
    expect(body.idempotency_window_ms).toBe(0)
  })

  it('idempotency_window_ms is written when provided', () => {
    const frame = packCreateStream(
      1n, Buffer.from('dedup'), Buffer.from('>'),
      0n, 0n, 0n, 1, 0, 0, 0, /*idempotencyWindowMs*/ 60_000,
    )
    expect(bodyJson(frame).idempotency_window_ms).toBe(60_000)
  })
})

describe('packCreateConsumer (cold/JSON)', () => {
  it('encodes stream_id, name, group, subject, subject_limits', () => {
    const frame = packCreateConsumer(1n, {
      streamId: 7,
      name: Buffer.from('worker'),
      group: Buffer.from('grp'),
      filter: Buffer.from('orders.>'),
      maxInflight: 128,
      ackPolicy: 1,
      deliverPolicy: 2,
      deliverMode: 0,
      ackWaitMs: 30000,
      startSeq: 42n,
      subjectLimits: [{ pattern: Buffer.from('vip.>'), limit: 10 }],
    })
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.CreateConsumer)
    const body = bodyJson(frame)
    expect(body.stream_id).toBe(7)
    expect(body.name).toEqual(Array.from(Buffer.from('worker')))
    expect(body.group).toEqual(Array.from(Buffer.from('grp')))
    expect(body.subject).toEqual(Array.from(Buffer.from('orders.>')))
    expect(body.max_inflight).toBe(128)
    expect(body.ack_policy).toBe(1)
    expect(body.deliver_policy).toBe(2)
    expect(body.deliver_mode).toBe(0)
    expect(body.ack_wait_ms).toBe(30000)
    expect(body.start_seq).toBe(42)
    expect(body.subject_limits).toEqual([
      { pattern: Array.from(Buffer.from('vip.>')), limit: 10 },
    ])
  })
})

describe('packDeleteStream (cold/JSON)', () => {
  it('encodes name as serde Vec<u8>', () => {
    const frame = packDeleteStream(2n, Buffer.from('old-stream'))
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.DeleteStream)
    expect(bodyJson(frame)).toEqual({
      name: Array.from(Buffer.from('old-stream')),
    })
  })
})

describe('packDrainSubject (cold/JSON)', () => {
  it('encodes stream name + subject', () => {
    const frame = packDrainSubject(3n, Buffer.from('events'), Buffer.from('events.old'))
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.DrainSubject)
    expect(bodyJson(frame)).toEqual({
      name:    Array.from(Buffer.from('events')),
      subject: Array.from(Buffer.from('events.old')),
    })
  })
})

// ─────────────────────────── Hot path (unchanged) ─────────────────────

describe('packPublish — msg_id wire layout', () => {
  it('default (no msgId) writes msg_id_len = 0 and tail = subject||payload', () => {
    const frame = packPublish(
      1n, 0xDEADBEEF, Buffer.from('orders.new'), Buffer.from('payload'),
    )
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(0xDEADBEEF)
    expect(frame.readUInt16LE(HEADER_SIZE + 4)).toBe(10) // subject_len = 'orders.new'
    expect(frame.readUInt16LE(HEADER_SIZE + 6)).toBe(0)  // msg_id_len  = 0
    const tail = frame.subarray(HEADER_SIZE + 8)
    expect(tail.subarray(0, 10).toString()).toBe('orders.new')
    expect(tail.subarray(10).toString()).toBe('payload')
  })

  it('with msgId places it between subject and payload', () => {
    const frame = packPublish(
      2n, 0x1234, Buffer.from('k'), Buffer.from('data'),
      0, 0, Buffer.from('msg-id-x'),
    )
    expect(frame.readUInt16LE(HEADER_SIZE + 4)).toBe(1) // subject_len
    expect(frame.readUInt16LE(HEADER_SIZE + 6)).toBe(8) // msg_id_len = 'msg-id-x'
    const tail = frame.subarray(HEADER_SIZE + 8)
    expect(tail.subarray(0, 1).toString()).toBe('k')
    expect(tail.subarray(1, 9).toString()).toBe('msg-id-x')
    expect(tail.subarray(9, 13).toString()).toBe('data')
  })
})

describe('packPublishBatch — per-entry msg_id', () => {
  it('writes msg_id_len per entry and lays out msg_id between subject and payload', () => {
    const entries = [
      { subject: 'k.a', msgId: Buffer.from('id-1'), payload: Buffer.from('A') },
      { subject: 'k.b', payload: Buffer.from('BB') },
    ]
    const frame = packPublishBatch(1n, 7, entries)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.PublishBatch)
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(7)
    expect(frame.readUInt32LE(HEADER_SIZE + 4)).toBe(2) // count

    let off = HEADER_SIZE + 8
    expect(frame.readUInt16LE(off)).toBe(3)
    expect(frame.readUInt16LE(off + 2)).toBe(4)
    expect(frame.readUInt32LE(off + 4)).toBe(1)
    off += 8
    expect(frame.subarray(off, off + 3).toString()).toBe('k.a');  off += 3
    expect(frame.subarray(off, off + 4).toString()).toBe('id-1'); off += 4
    expect(frame.subarray(off, off + 1).toString()).toBe('A');    off += 1

    expect(frame.readUInt16LE(off)).toBe(3)
    expect(frame.readUInt16LE(off + 2)).toBe(0)
    expect(frame.readUInt32LE(off + 4)).toBe(2)
    off += 8
    expect(frame.subarray(off, off + 3).toString()).toBe('k.b'); off += 3
    expect(frame.subarray(off, off + 2).toString()).toBe('BB')
  })
})
