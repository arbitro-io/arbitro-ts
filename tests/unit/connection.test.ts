import { describe, it, expect } from 'vitest'
import { pack } from '../../src/proto/codec'
import { Action, Flags, HEADER_SIZE, OFF_TIMESTAMP } from '../../src/proto/constants'

// Unit tests for connection frame building — no server needed.
// Verifies that ack/nack/nackDelay frames are encoded correctly.

describe('Connection frame encoding — sendAck / sendNack', () => {
  it('RepAck frame includes stream name as subject', () => {
    const frame = pack({
      action:    Action.RepAck,
      seq:       1n,
      timestamp: 42n,
      subject:   'my-stream',
      data:      Buffer.alloc(0),
    })
    expect(frame.readUInt16LE(6)).toBe(Action.RepAck)
    expect(frame.readBigUInt64LE(16)).toBe(1n)   // seq = subId
    expect(frame.readBigUInt64LE(24)).toBe(42n)  // timestamp = msgSeq
    // subject starts at offset 34 (after u16 len at 32)
    const subjLen = frame.readUInt16LE(32)
    expect(subjLen).toBe(9)
    expect(frame.subarray(34, 34 + subjLen).toString()).toBe('my-stream')
  })

  it('RepNack frame with empty data has no delay', () => {
    const frame = pack({
      action:    Action.RepNack,
      seq:       5n,
      timestamp: 100n,
      subject:   'stream-x',
      data:      Buffer.alloc(0),
    })
    expect(frame.readUInt16LE(6)).toBe(Action.RepNack)
    const subjLen = frame.readUInt16LE(32)
    const dataStart = 34 + subjLen
    expect(frame.length - dataStart).toBe(0)
  })

  it('RepNack frame with 4-byte data encodes delay_ms', () => {
    const data = Buffer.allocUnsafe(4)
    data.writeUInt32LE(5000, 0)
    const frame = pack({
      action:    Action.RepNack,
      seq:       7n,
      timestamp: 200n,
      subject:   'stream-y',
      data,
    })
    const subjLen  = frame.readUInt16LE(32)
    const dataOff  = 34 + subjLen
    const delayMs  = frame.readUInt32LE(dataOff)
    expect(delayMs).toBe(5000)
  })
})

describe('Connection frame encoding — subscribe / unsubscribe', () => {
  it('PubSubscribe frame has correct action and subject', () => {
    const frame = pack({
      action:  Action.PubSubscribe,
      flags:   Flags.None,
      seq:     10n,
      subject: 'orders',
      data:    Buffer.alloc(0),
    })
    expect(frame.readUInt16LE(6)).toBe(Action.PubSubscribe)
    const subjLen = frame.readUInt16LE(32)
    expect(frame.subarray(34, 34 + subjLen).toString()).toBe('orders')
  })

  it('PubUnsubscribe frame carries subId in seq field', () => {
    const frame = pack({
      action:  Action.PubUnsubscribe,
      seq:     99n,
      subject: 'events',
      data:    Buffer.alloc(0),
    })
    expect(frame.readBigUInt64LE(16)).toBe(99n)
  })
})

describe('Connection frame encoding — RepOk timestamp extraction', () => {
  it('subId is read from the timestamp field of RepOk', () => {
    const frame = pack({
      action:    Action.RepOk,
      seq:       0n,
      timestamp: 777n,
      subject:   Buffer.alloc(0),
      data:      Buffer.alloc(0),
    })
    const subId = frame.readBigUInt64LE(OFF_TIMESTAMP)
    expect(subId).toBe(777n)
  })
})
