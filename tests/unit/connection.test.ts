import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { JournalType } from '../../src'
import { Connection } from '../../src/net/connection'
import { pack } from '../../src/proto/codec'
import { Action, Flags } from '../../src/proto/constants'
import { createClient, waitUntil, BROKER_ADDR } from '../helpers/client'
import type { ArbitroClient } from '../../src'

let admin: ArbitroClient

beforeAll(async () => {
  admin = await createClient()
  // Pre-create groups used by connection-level tests.
  // PubSubscribe requires the consumer group to exist on the server.
  // conn-ack-test is created inside its own test with maxAckPending: 1 — not here.
  for (const name of ['conn-subscribe-test', 'conn-route-test']) {
    await admin.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await admin.createConsumer(name, { name, filter: `${name}.>` })
  }
})
afterAll(async () => { await admin.close() })

describe('Connection', () => {
  it('connects and closes cleanly', async () => {
    const conn = await Connection.connect(BROKER_ADDR)
    await conn.close()
  })

  it('sendExpectReply resolves with a valid subId from PubSubscribe', async () => {
    const conn  = await Connection.connect(BROKER_ADDR)
    const frame = buildSubscribeFrame(conn.nextSeq(), 'conn-subscribe-test')
    const subId = await conn.sendExpectReply(frame)
    expect(subId > 0n).toBe(true)
    await conn.close()
  })

  it('delivery frames are routed to the registered handler', async () => {
    const name = 'conn-route-test'

    const conn  = await Connection.connect(BROKER_ADDR)
    const subId = await conn.sendExpectReply(buildSubscribeFrame(conn.nextSeq(), name))

    const received: Buffer[] = []
    conn.registerRoute(subId, (frame) => received.push(frame))

    admin.publish(`${name}.e`, Buffer.from('payload'))
    await waitUntil(() => received.length >= 1)

    expect(received.length).toBe(1)
    await conn.close()
  })

  // it('sendAck unblocks delivery when maxAckPending is 1', async () => {
  //   const name = 'conn-ack-test'
  //   await admin.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
  //   await admin.createConsumer(name, { name, filter: `${name}.>`, maxAckPending: 1 })
  //
  //   const conn  = await Connection.connect(BROKER_ADDR)
  //   const subId = await conn.sendExpectReply(buildSubscribeFrame(conn.nextSeq(), name))
  //
  //   let lastSeq = 0n
  //   const received: string[] = []
  //   conn.registerRoute(subId, (frame) => {
  //     lastSeq = frame.readBigUInt64LE(16)
  //     const subjLen = frame.readUInt16LE(32)
  //     received.push(frame.subarray(34 + subjLen).toString())
  //   })
  //
  //   admin.publish(`${name}.e`, Buffer.from('first'))
  //   await waitUntil(() => received.length >= 1)
  //
  //   admin.publish(`${name}.e`, Buffer.from('second'))
  //   await new Promise((r) => setTimeout(r, 150))
  //   expect(received.length).toBe(1)
  //
  //   conn.sendAck(subId, lastSeq)
  //   await waitUntil(() => received.length >= 2)
  //
  //   await conn.close()
  //   expect(received).toEqual(['first', 'second'])
  // })
})

function buildSubscribeFrame(seq: bigint, group: string): Buffer {
  return pack({ action: Action.PubSubscribe, flags: Flags.None, seq, subject: group, data: Buffer.alloc(0) })
}
