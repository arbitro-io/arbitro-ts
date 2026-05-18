import { describe, it, expect } from 'vitest'
import {
  packPublish, packPublishBatch, packPublishWithReply,
  packAck, packNack, packBatchAck, packBatchNack,
  packSubscribe, packUnsubscribe, packHello, packPing, packDisconnect,
  packCreateStream, packDeleteStream, packGetStream, packPurgeStream,
  packDrainSubject, packListStreams,
  packCreateConsumer, packDeleteConsumer, packGetConsumer, packListConsumers,
} from '../src/proto/v2'
import {
  Action, HEADER_SIZE, MAGIC_V2, CURRENT_VERSION, HELLO_SIZE,
  OFF_ACTION, OFF_FLAGS, OFF_MSG_LEN, OFF_SEQ,
} from '../src/proto/constants'

describe('V2 Hello frame', () => {
  it('is 8 bytes with correct magic', () => {
    const h = packHello()
    expect(h.length).toBe(HELLO_SIZE)
    expect(h.readUInt32LE(0)).toBe(MAGIC_V2)
    expect(h[4]).toBe(CURRENT_VERSION)
    expect(h[5]).toBe(0)  // Role.Client
  })
})

describe('V2 Publish frame', () => {
  it('encodes action, stream_id, subject, payload', () => {
    const subj = Buffer.from('orders.eu')
    const data = Buffer.from('hello world')
    const frame = packPublish(42n, 0xCAFE, subj, data)

    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.Publish)
    expect(frame.readBigUInt64LE(OFF_SEQ)).toBe(42n)
    // Body: stream_id at HEADER_SIZE
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(0xCAFE)
    // subject_len at HEADER_SIZE + 4
    expect(frame.readUInt16LE(HEADER_SIZE + 4)).toBe(subj.length)
    // subject at HEADER_SIZE + 8
    expect(frame.subarray(HEADER_SIZE + 8, HEADER_SIZE + 8 + subj.length).toString()).toBe('orders.eu')
    // payload after subject
    expect(frame.subarray(HEADER_SIZE + 8 + subj.length).toString()).toBe('hello world')
  })

  it('frame size = 16 + 8 + subject + payload', () => {
    const subj = Buffer.from('x')
    const data = Buffer.from('y')
    const frame = packPublish(1n, 0, subj, data)
    expect(frame.length).toBe(HEADER_SIZE + 8 + 1 + 1)
  })

  it('msg_len in header matches body size', () => {
    const subj = Buffer.from('test')
    const data = Buffer.from('data')
    const frame = packPublish(1n, 0, subj, data)
    const msgLen = frame.readUInt32LE(OFF_MSG_LEN)
    expect(msgLen).toBe(8 + subj.length + data.length)
  })
})

describe('V2 PublishBatch frame', () => {
  it('encodes multiple entries', () => {
    const entries = [
      { subject: 'a.b',   payload: Buffer.from('P1') },
      { subject: 'c.d.e', payload: Buffer.from('P2P2') },
    ]
    const frame = packPublishBatch(7n, 0xBEEF, entries)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.PublishBatch)
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(0xBEEF)      // stream_id
    expect(frame.readUInt32LE(HEADER_SIZE + 4)).toBe(2)        // count
  })
})

function bodyJson(frame: Buffer): any {
  return JSON.parse(frame.subarray(HEADER_SIZE).toString('utf8'))
}

describe('V2 Subscribe frame (cold/JSON)', () => {
  it('encodes consumer_id + subscription_id + filters', () => {
    const filter = Buffer.from('orders.*.eu')
    const frame = packSubscribe(5n, 100, 42, filter)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.Subscribe)
    expect(bodyJson(frame)).toEqual({
      consumer_id:     42,
      subscription_id: 0,
      filters:         [Array.from(filter)],
    })
  })

  it('empty filter becomes empty filters array (catch-all)', () => {
    const frame = packSubscribe(5n, 100, 42, Buffer.alloc(0))
    expect(bodyJson(frame)).toEqual({
      consumer_id:     42,
      subscription_id: 0,
      filters:         [],
    })
  })
})

describe('V2 Ack/Nack frames', () => {
  it('Ack encodes consumer_id + subject_hash + ack_seq', () => {
    const frame = packAck(1n, 77, 0xDEAD, 999n)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.Ack)
    expect(frame.length).toBe(HEADER_SIZE + 16)
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(77)
    expect(frame.readUInt32LE(HEADER_SIZE + 4)).toBe(0xDEAD)
    expect(frame.readBigUInt64LE(HEADER_SIZE + 8)).toBe(999n)
  })

  it('Nack encodes consumer_id + subject_hash + nack_seq', () => {
    const frame = packNack(2n, 88, 0xBEEF, 500n)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.Nack)
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(88)
    expect(frame.readBigUInt64LE(HEADER_SIZE + 8)).toBe(500n)
  })

  it('BatchAck encodes entries', () => {
    const entries = [
      { seq: 100n, subjectHash: 0x11 },
      { seq: 101n, subjectHash: 0x22 },
    ]
    const frame = packBatchAck(3n, 77, entries)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.BatchAck)
    expect(frame.readUInt32LE(HEADER_SIZE)).toBe(77)
    expect(frame.readUInt32LE(HEADER_SIZE + 4)).toBe(2)
    // First entry at HEADER_SIZE + 8
    expect(frame.readBigUInt64LE(HEADER_SIZE + 8)).toBe(100n)
    expect(frame.readUInt32LE(HEADER_SIZE + 16)).toBe(0x11)
  })

  it('BatchNack encodes entries with delay_ms', () => {
    const entries = [
      { seq: 200n, subjectHash: 0x33, delayMs: 5000 },
    ]
    const frame = packBatchNack(4n, 88, entries)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.BatchNack)
    expect(frame.readUInt32LE(HEADER_SIZE + 8 + 12)).toBe(5000)
  })
})

describe('V2 Stream management frames (cold/JSON)', () => {
  it('CreateStream encodes name + filter + retention', () => {
    const frame = packCreateStream(1n, Buffer.from('events'), Buffer.from('events.>'), 1000n, 0n, 3600n)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.CreateStream)
    const body = bodyJson(frame)
    expect(body.name).toEqual(Array.from(Buffer.from('events')))
    expect(body.filter).toEqual(Array.from(Buffer.from('events.>')))
    expect(body.max_msgs).toBe(1000)
    expect(body.max_age_secs).toBe(3600)
  })

  it('DeleteStream encodes name', () => {
    const frame = packDeleteStream(2n, Buffer.from('old'))
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.DeleteStream)
    expect(bodyJson(frame)).toEqual({ name: Array.from(Buffer.from('old')) })
  })

  it('ListStreams encodes offset and limit', () => {
    const frame = packListStreams(3n, 10, 50)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.ListStreams)
    expect(bodyJson(frame)).toEqual({ offset: 10, limit: 50 })
  })
})

describe('V2 Consumer management frames (cold/JSON)', () => {
  it('CreateConsumer encodes all fields', () => {
    const frame = packCreateConsumer(1n, {
      streamId: 7,
      name: Buffer.from('worker'),
      group: Buffer.from('grp'),
      filter: Buffer.from('orders.>'),
      maxInflight: 128,
      ackPolicy: 1,
      deliverPolicy: 0,
      ackWaitMs: 30000,
      startSeq: 0n,
    })
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.CreateConsumer)
    const body = bodyJson(frame)
    expect(body.stream_id).toBe(7)
    expect(body.name).toEqual(Array.from(Buffer.from('worker')))
    expect(body.group).toEqual(Array.from(Buffer.from('grp')))
    expect(body.subject).toEqual(Array.from(Buffer.from('orders.>')))
    expect(body.max_inflight).toBe(128)
  })

  it('DeleteConsumer encodes consumer_id', () => {
    const frame = packDeleteConsumer(2n, 42)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.DeleteConsumer)
    expect(bodyJson(frame)).toEqual({ consumer_id: 42 })
  })

  it('GetConsumer encodes stream_id + name', () => {
    const frame = packGetConsumer(3n, 7, Buffer.from('worker'))
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.GetConsumer)
    expect(bodyJson(frame)).toEqual({
      stream_id: 7,
      name:      Array.from(Buffer.from('worker')),
    })
  })
})

describe('V2 System frames', () => {
  it('Ping is just a header (msg_len=0)', () => {
    const frame = packPing(5n)
    expect(frame.length).toBe(HEADER_SIZE)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.Ping)
    expect(frame.readUInt32LE(OFF_MSG_LEN)).toBe(0)
  })

  it('Disconnect is just a header', () => {
    const frame = packDisconnect(6n)
    expect(frame.length).toBe(HEADER_SIZE)
    expect(frame.readUInt16LE(OFF_ACTION)).toBe(Action.Disconnect)
  })
})
