import { describe, it, expect } from 'vitest'
import {
  packCreateStream, packCreateConsumer, packDeleteStream, packDrainSubject,
  packPublish, packPublishBatch,
} from '../src/proto/v2'
import { Action, HEADER_SIZE, OFF_ACTION } from '../src/proto/constants'

// Tests for V2 binary frame serialization (replaced V1 msgpack serialization)

describe('packCreateStream', () => {
  it('encodes name and filter with correct lengths', () => {
    const frame = packCreateStream(
      1n, Buffer.from('orders'), Buffer.from('orders.>'),
      1000n, 500n, 3600n, 1, 1, 0, 0,
    )
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.CreateStream)
    expect(frame.readUInt16LE(HEADER_SIZE)).toBe(6)       // name_len = "orders"
    expect(frame.readUInt16LE(HEADER_SIZE + 2)).toBe(8)   // filter_len = "orders.>"
    expect(frame.readBigUInt64LE(HEADER_SIZE + 4)).toBe(1000n)   // max_msgs
    expect(frame.readBigUInt64LE(HEADER_SIZE + 12)).toBe(500n)   // max_bytes
    expect(frame.readBigUInt64LE(HEADER_SIZE + 20)).toBe(3600n)  // max_age_secs
    expect(frame[HEADER_SIZE + 28]).toBe(1)  // replicas
    expect(frame[HEADER_SIZE + 29]).toBe(1)  // journal_kind
    expect(frame.readUInt32LE(HEADER_SIZE + 32)).toBe(0) // idempotency_window_ms (default)
    // Tail: name + filter (fixed body is now 40B)
    const tail = frame.subarray(HEADER_SIZE + 40)
    expect(tail.subarray(0, 6).toString()).toBe('orders')
    expect(tail.subarray(6, 14).toString()).toBe('orders.>')
  })

  it('total frame size = HEADER_SIZE + 40 + name_len + filter_len', () => {
    const name = Buffer.from('x')
    const filter = Buffer.from('y')
    const frame = packCreateStream(1n, name, filter, 0n, 0n, 0n)
    expect(frame.length).toBe(HEADER_SIZE + 40 + 1 + 1)
  })

  it('idempotency_window_ms is written when provided', () => {
    const frame = packCreateStream(
      1n, Buffer.from('dedup'), Buffer.from('>'),
      0n, 0n, 0n, 1, 0, 0, 0,
      /*idempotencyWindowMs*/ 60_000,
    )
    expect(frame.readUInt32LE(HEADER_SIZE + 32)).toBe(60_000)
    expect(frame.readUInt32LE(HEADER_SIZE + 36)).toBe(0) // _pad
  })
})

describe('packCreateConsumer', () => {
  it('encodes stream_id, name, group, filter', () => {
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
    })
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.CreateConsumer)
    // Body fields
    expect(frame.readUInt16LE(HEADER_SIZE)).toBe(6)       // name_len
    expect(frame.readUInt16LE(HEADER_SIZE + 2)).toBe(8)   // subj_len (filter)
    expect(frame.readUInt32LE(HEADER_SIZE + 4)).toBe(7)   // stream_id
    expect(frame.readUInt16LE(HEADER_SIZE + 8)).toBe(128) // max_inflight
    expect(frame[HEADER_SIZE + 10]).toBe(1)               // ack_policy
    expect(frame[HEADER_SIZE + 11]).toBe(2)               // deliver_policy
    expect(frame.readUInt16LE(HEADER_SIZE + 14)).toBe(3)  // group_len
    expect(frame.readUInt32LE(HEADER_SIZE + 16)).toBe(30000) // ack_wait_ms
    expect(frame.readBigUInt64LE(HEADER_SIZE + 20)).toBe(42n) // start_seq
    // Tail: name + group + filter
    const tail = frame.subarray(HEADER_SIZE + 28)
    expect(tail.subarray(0, 6).toString()).toBe('worker')
    expect(tail.subarray(6, 9).toString()).toBe('grp')
    expect(tail.subarray(9, 17).toString()).toBe('orders.>')
  })
})

describe('packDeleteStream', () => {
  it('encodes name_len and name', () => {
    const frame = packDeleteStream(2n, Buffer.from('old-stream'))
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.DeleteStream)
    expect(frame.readUInt16LE(HEADER_SIZE)).toBe(10)  // name_len
    expect(frame.subarray(HEADER_SIZE + 8).toString()).toBe('old-stream')
  })
})

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
    // Mixing: one entry with msgId, one without.
    const entries = [
      { subject: 'k.a', msgId: Buffer.from('id-1'), payload: Buffer.from('A') },
      { subject: 'k.b', payload: Buffer.from('BB') },
    ]
    const frame = packPublishBatch(1n, 7, entries)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.PublishBatch)
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(7)
    expect(frame.readUInt32LE(HEADER_SIZE + 4)).toBe(2) // count

    // Entry 0
    let off = HEADER_SIZE + 8
    expect(frame.readUInt16LE(off)).toBe(3)          // subject_len = 'k.a'
    expect(frame.readUInt16LE(off + 2)).toBe(4)      // msg_id_len  = 'id-1'
    expect(frame.readUInt32LE(off + 4)).toBe(1)      // payload_len = 1
    off += 8
    expect(frame.subarray(off, off + 3).toString()).toBe('k.a');  off += 3
    expect(frame.subarray(off, off + 4).toString()).toBe('id-1'); off += 4
    expect(frame.subarray(off, off + 1).toString()).toBe('A');    off += 1

    // Entry 1 — no msg_id
    expect(frame.readUInt16LE(off)).toBe(3)          // subject_len = 'k.b'
    expect(frame.readUInt16LE(off + 2)).toBe(0)      // msg_id_len  = 0
    expect(frame.readUInt32LE(off + 4)).toBe(2)      // payload_len = 2
    off += 8
    expect(frame.subarray(off, off + 3).toString()).toBe('k.b'); off += 3
    expect(frame.subarray(off, off + 2).toString()).toBe('BB')
  })
})

describe('packDrainSubject', () => {
  it('encodes stream name + subject', () => {
    const frame = packDrainSubject(3n, Buffer.from('events'), Buffer.from('events.old'))
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.DrainSubject)
    expect(frame.readUInt16LE(HEADER_SIZE)).toBe(6)      // name_len
    expect(frame.readUInt16LE(HEADER_SIZE + 2)).toBe(10) // subj_len
    const tail = frame.subarray(HEADER_SIZE + 8)
    expect(tail.subarray(0, 6).toString()).toBe('events')
    expect(tail.subarray(6, 16).toString()).toBe('events.old')
  })
})
