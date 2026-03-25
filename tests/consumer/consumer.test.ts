import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType, schema } from '../../src'
import type { LazyMessage } from '../../src/topic'
import { cleanupNamedResources, createClient, uniqueName, waitUntil } from '../helpers/client'

let client: ArbitroClient
const created: string[] = []
const OrderCodec = schema({ id: 'number', status: 'string' })
type Order = ReturnType<typeof OrderCodec.decode>

beforeAll(async () => { client = await createClient() })
afterAll(async () => {
  await cleanupNamedResources(client, created)
  await client.close()
})

describe('Consumer delivery', () => {
  it('subscribe with typed codec delivers LazyMessage with correct field values', async () => {
    const name = uniqueName('c'); created.push(name)
    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    const consumer = await stream.consumer({ name }).create()
    const received: LazyMessage<Order>[] = []
    const sub = await consumer.subscribe(OrderCodec, (msg: LazyMessage<Order>) => received.push(msg))
    await client.publishAck(`${name}.new`, OrderCodec.encode({ id: 42, status: 'pending' }))
    await waitUntil(() => received.length >= 1)
    sub.close()

    expect(received[0]!.id).toBe(42)
    expect(received[0]!.status).toBe('pending')
  })

  it('subscribe without codec delivers raw Buffer unchanged', async () => {
    const name = uniqueName('c'); created.push(name)
    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    const consumer = await stream.consumer({ name }).create()
    const received: Buffer[] = []
    const sub = await consumer.subscribe((msg) => received.push(msg.data()))

    await client.publishAck(`${name}.e`, Buffer.from('raw-bytes'))
    await waitUntil(() => received.length >= 1)
    sub.close()

    expect(received[0]!.toString()).toBe('raw-bytes')
  })

  it('multiple messages are delivered in publish order', async () => {
    const name = uniqueName('c'); created.push(name)
    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    const consumer = await stream.consumer({ name }).create()
    const received: string[] = []
    const sub = await consumer.subscribe((msg) => received.push(msg.data().toString()))

    for (let i = 0; i < 5; i++) {
      await client.publishAck(`${name}.e`, Buffer.from(`msg-${i}`))
    }
    await waitUntil(() => received.length >= 5)
    sub.close()

    expect(received).toEqual(['msg-0', 'msg-1', 'msg-2', 'msg-3', 'msg-4'])
  })

  it('ack unblocks delivery when maxAckPending is 1', async () => {
    const name = uniqueName('c'); created.push(name)
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    const consumer = await client.createConsumer(name, {
      name,
      filter: `${name}.>`,
      maxAckPending: 1,
    })

    const received: string[] = []
    const sub = await consumer.subscribe((msg) => {
      received.push(msg.data().toString())
      msg.ack()
    })

    await client.publishAck(`${name}.e`, Buffer.from('first'))
    await client.publishAck(`${name}.e`, Buffer.from('second'))
    await waitUntil(() => received.length >= 2)
    sub.close()

    expect(received).toEqual(['first', 'second'])
  })

  it('fetch() returns messages in pull mode', async () => {
    const name = uniqueName('c'); created.push(name)
    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    const consumer = await stream.consumer({ name }).create()
    const sub = await consumer.subscribe()

    await client.publishAck(`${name}.e`, Buffer.from('pull-a'))
    await client.publishAck(`${name}.e`, Buffer.from('pull-b'))

    const msgs = await sub.fetch(2, 2_000)
    sub.close()

    expect(msgs.length).toBe(2)
    expect(msgs[0]!.data().toString()).toBe('pull-a')
    expect(msgs[1]!.data().toString()).toBe('pull-b')
  })
})
