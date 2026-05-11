import { describe, it, expect } from 'vitest'
import { packAck, packNack, packBatchNack, packSubscribe, packUnsubscribe } from '../src/proto/v2'
import { Action, HEADER_SIZE, OFF_ACTION, OFF_SEQ } from '../src/proto/constants'

// Unit tests for V2 frame building — no server needed.
// Verifies that ack/nack/subscribe frames are encoded correctly.

describe('V2 Ack/Nack frame encoding', () => {
  it('Ack frame has correct action and body layout', () => {
    const frame = packAck(1n, 42, 0xDEADBEEF, 999n)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.Ack)
    expect(frame.readBigUInt64LE(OFF_SEQ)).toBe(1n)
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(42)          // consumer_id
    expect(frame.readUInt32LE(HEADER_SIZE + 4)).toBe(0xDEADBEEF)  // subject_hash
    expect(frame.readBigUInt64LE(HEADER_SIZE + 8)).toBe(999n)     // ack_seq
    expect(frame.length).toBe(HEADER_SIZE + 16)
  })

  it('Nack frame with no delay', () => {
    const frame = packNack(5n, 88, 0xBEEF, 100n)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.Nack)
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(88)
    expect(frame.readBigUInt64LE(HEADER_SIZE + 8)).toBe(100n)
    expect(frame.length).toBe(HEADER_SIZE + 16)
  })

  it('BatchNack with delay_ms per entry', () => {
    const entries = [
      { seq: 200n, subjectHash: 0x11, delayMs: 5000 },
      { seq: 201n, subjectHash: 0x22, delayMs: 0 },
    ]
    const frame = packBatchNack(7n, 77, entries)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.BatchNack)
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(77)       // consumer_id
    expect(frame.readUInt32LE(HEADER_SIZE + 4)).toBe(2)    // count
    // First entry starts at HEADER_SIZE + 8
    expect(frame.readBigUInt64LE(HEADER_SIZE + 8)).toBe(200n)
    expect(frame.readUInt32LE(HEADER_SIZE + 16)).toBe(0x11)
    expect(frame.readUInt32LE(HEADER_SIZE + 20)).toBe(5000)
    // Second entry at HEADER_SIZE + 8 + 16
    expect(frame.readBigUInt64LE(HEADER_SIZE + 24)).toBe(201n)
    expect(frame.readUInt32LE(HEADER_SIZE + 36)).toBe(0)
  })
})

describe('V2 Subscribe/Unsubscribe frame encoding', () => {
  it('Subscribe frame carries conn_id, consumer_id, filter', () => {
    const filter = Buffer.from('orders.>')
    const frame = packSubscribe(10n, 100, 55, filter)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.Subscribe)
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(100)       // conn_id
    expect(frame.readUInt32LE(HEADER_SIZE + 4)).toBe(55)    // consumer_id
    expect(frame.readUInt16LE(HEADER_SIZE + 8)).toBe(filter.length)
    expect(frame.subarray(HEADER_SIZE + 12).toString()).toBe('orders.>')
  })

  it('Unsubscribe frame has zero filter', () => {
    const frame = packUnsubscribe(99n, 100, 55)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.Unsubscribe)
    expect(frame.readUInt32LE(HEADER_SIZE + 4)).toBe(55)    // consumer_id
    expect(frame.readUInt16LE(HEADER_SIZE + 8)).toBe(0)     // filter_len = 0
    expect(frame.length).toBe(HEADER_SIZE + 12)
  })
})

describe('V2 RepOk parsing', () => {
  it('ref_seq is at HEADER_SIZE as u64', () => {
    // Simulate a RepOk frame: Header(16) + ref_seq(8) = 24B
    const frame = Buffer.allocUnsafe(24)
    frame.writeUInt16LE(Action.RepOk, 0)
    frame[2] = 0; frame[3] = 0
    frame.writeUInt32LE(8, 4)  // msg_len = 8
    frame.writeBigUInt64LE(1n, 8)  // seq
    frame.writeBigUInt64LE(777n, HEADER_SIZE)  // ref_seq
    expect(frame.readBigUInt64LE(HEADER_SIZE)).toBe(777n)
  })
})
