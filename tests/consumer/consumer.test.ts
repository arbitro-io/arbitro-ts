import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType } from '../../src'
import { Codec } from '../../src/utils/codec'
import type { LazyMessage } from '../../src/topic'
import { startServer, waitUntil, type RealServer } from '../helpers/real-server'

let server: RealServer
let client: ArbitroClient
let counter = 0

function uid(): string { return `c${++counter}` }

interface Order { id: number; status: string }
const OrderCodec = new Codec<Order>({ id: 'number', status: 'string' })

beforeAll(async () => {
  server = await startServer()
  client = new ArbitroClient({ servers: [server.addr] })
  await client.connect()
})

afterAll(async () => {
  await client.close()
  await server.stop()
})

// TCP is ordered: createStream + createConsumer are processed before subscribe replies.
// No delay needed — await subscribe() is the natural synchronization point.
function setup(name: string): void {
  client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
  client.createConsumer(name, { name, filter: `${name}.>` })
}

describe('Consumer delivery', () => {
  it('subscribe with typed codec delivers LazyMessage with correct field values', async () => {
    const name = uid()
    setup(name)

    const received: LazyMessage<Order>[] = []
    const sub = await client.stream(name)
      .consumer({ name, filter: `${name}.>` })
      .subscribe(OrderCodec, (msg) => received.push(msg))

    await client.publishAck(`${name}.new`, OrderCodec.encode({ id: 42, status: 'pending' }))
    await waitUntil(() => received.length >= 1)
    sub.close()

    expect(received[0]!.id).toBe(42)
    expect(received[0]!.status).toBe('pending')
  })

  it('subscribe without codec delivers raw Buffer unchanged', async () => {
    const name = uid()
    setup(name)

    const received: Buffer[] = []
    const sub = await client.stream(name)
      .consumer({ name, filter: `${name}.>` })
      .subscribe((msg) => received.push(msg.data()))

    await client.publishAck(`${name}.e`, Buffer.from('raw-bytes'))
    await waitUntil(() => received.length >= 1)
    sub.close()

    expect(received[0]!.toString()).toBe('raw-bytes')
  })

  it('multiple messages are delivered in publish order', async () => {
    const name = uid()
    setup(name)

    const received: string[] = []
    const sub = await client.stream(name)
      .consumer()
      .subscribe((msg) => {
        console.log('Received:', msg.seq())
        received.push(msg.data().toString())
      })

    for (let i = 0; i < 5; i++) {
      await client.publishAck(`${name}.e`, Buffer.from(`msg-${i}`))
    }

    await waitUntil(() => received.length >= 5, 300)
    sub.close()

    expect(received).toEqual(['msg-0', 'msg-1', 'msg-2', 'msg-3', 'msg-4'])
  })

  it('ack unblocks delivery when maxAckPending is 1', async () => {
    const name = uid()
    client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    client.createConsumer(name, { name, filter: `${name}.>`, maxAckPending: 1 })

    const received: string[] = []
    const sub = await client.stream(name)
      .consumer({ name, filter: `${name}.>` })
      .subscribe((msg) => { received.push(msg.data().toString()); msg.ack() })

    await client.publishAck(`${name}.e`, Buffer.from('first'))
    await client.publishAck(`${name}.e`, Buffer.from('second'))
    await waitUntil(() => received.length >= 2)
    sub.close()

    expect(received).toEqual(['first', 'second'])
  })

  it('fetch() returns messages in pull mode', async () => {
    const name = uid()
    setup(name)

    const sub = await client.stream(name)
      .consumer({})
      .subscribe()
      

    await client.publishAck(`${name}.e`, Buffer.from('pull-a'))
    await client.publishAck(`${name}.e`, Buffer.from('pull-b'))

    const msgs = await sub.fetch(2, 2_000)
    sub.close()

    expect(msgs.length).toBe(2)
    expect(msgs[0]!.data().toString()).toBe('pull-a')
    expect(msgs[1]!.data().toString()).toBe('pull-b')
  })
})
