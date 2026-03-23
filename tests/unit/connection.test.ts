import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { Connection } from '../../src/net/connection'
import { FrameView } from '../../src/proto/codec'
import { Action, HEADER_SIZE } from '../../src/proto/constants'
import { MockBroker } from '../helpers/mock-broker'

let broker: MockBroker
let port:   number

beforeEach(async () => {
  broker = new MockBroker()
  port   = await broker.start()
})

afterEach(async () => {
  await broker.stop()
})

describe('Connection', () => {
  it('connects successfully', async () => {
    const conn = await Connection.connect(`127.0.0.1:${port}`)
    await conn.close()
  })

  it('sendExpectReply resolves with seq from RepOk', async () => {
    const conn  = await Connection.connect(`127.0.0.1:${port}`)
    // PubSubscribe → broker replies RepOk with subId=1n
    const frame = buildSubscribeFrame(conn.nextSeq())
    const subId = await conn.sendExpectReply(frame)
    expect(subId).toBe(1n)
    await conn.close()
  })

  it('sendExpectReply rejects when RepError is received', async () => {
    // Override broker to send RepError instead
    broker.onFrame = (_action) => {
      broker.sendError('stream not found')
    }
    const conn   = await Connection.connect(`127.0.0.1:${port}`)
    // Any frame that waits for a reply — temporarily use subscribe (broker overrides response)
    const frame  = buildSubscribeFrame(conn.nextSeq())
    await expect(conn.sendExpectReply(frame)).rejects.toThrow('stream not found')
    await conn.close()
  })

  it('RepError message is correctly parsed (no subject prefix corruption)', async () => {
    broker.onFrame = () => { broker.sendError('consumer does not exist') }
    const conn  = await Connection.connect(`127.0.0.1:${port}`)
    const frame = buildSubscribeFrame(conn.nextSeq())
    const err   = await conn.sendExpectReply(frame).catch((e) => e)
    expect(err.message).toBe('consumer does not exist')
    await conn.close()
  })

  it('delivery frames are routed to the registered handler', async () => {
    const conn = await Connection.connect(`127.0.0.1:${port}`)
    // Subscribe to get a sub_id
    const subId = await conn.sendExpectReply(buildSubscribeFrame(conn.nextSeq()))

    const received: Buffer[] = []
    conn.registerRoute(subId, (frame) => received.push(frame))

    broker.deliver(subId, 'orders.new', Buffer.from('payload'))
    await waitMs(50)

    expect(received.length).toBe(1)
    const view = new FrameView(received[0]!)
    expect(view.data().toString()).toBe('payload')
    await conn.close()
  })

  it('sendAck sends a RepAck frame with correct seq and subId', async () => {
    const received: Buffer[] = []
    broker.onFrame = (_action, frame) => received.push(Buffer.from(frame))

    const conn = await Connection.connect(`127.0.0.1:${port}`)
    conn.sendAck(7n, 42n)  // subId=7, msgSeq=42
    await waitMs(50)
    await conn.close()

    // Find the RepAck frame
    const ackFrames = received.filter((f) => f.readUInt16LE(6) === Action.RepAck)
    expect(ackFrames.length).toBeGreaterThan(0)
    const ack = new FrameView(ackFrames[0]!)
    expect(ack.seq()).toBe(42n)       // msgSeq in sequence field
    expect(ack.timestamp()).toBe(7n)  // subId in timestamp field
  })
})

// ── Helpers ────────────────────────────────────────────────────────────────

import { pack } from '../../src/proto/codec'
import { Flags } from '../../src/proto/constants'

function buildSubscribeFrame(seq: bigint): Buffer {
  return pack({ action: Action.PubSubscribe, flags: Flags.None, seq, subject: 'test-group', data: Buffer.alloc(0) })
}

function waitMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
