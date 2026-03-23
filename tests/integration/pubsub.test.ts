import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType } from '../../src'
import { startServer, waitUntil, type RealServer } from '../helpers/real-server'

let server: RealServer
let client: ArbitroClient
let counter = 0

function uid(): string { return `p${++counter}` }

function setup(c: ArbitroClient, name: string): void {
  c.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
  c.createConsumer(name, { name, filter: `${name}.>` })
}

beforeAll(async () => {
  server = await startServer()
  client = new ArbitroClient({ servers: [server.addr] })
  await client.connect()
})

afterAll(async () => {
  await client.close()
  await server.stop()
})

describe('publish/subscribe', () => {
  it('fire-and-forget publish reaches subscriber', async () => {
    const name = uid()
    setup(client, name)

    const received: string[] = []
    const sub = await client.subscribe(name, (msg) => received.push(msg.data().toString()))

    client.publish(`${name}.e`, Buffer.from('hello'))
    await waitUntil(() => received.length >= 1)
    sub.close()

    expect(received[0]).toBe('hello')
  })

  it('publishAck resolves after server confirms receipt', async () => {
    const name = uid()
    setup(client, name)
    await expect(client.publishAck(`${name}.e`, Buffer.from('acked'))).resolves.toBeUndefined()
  })

  it('publishBatch — all messages reach subscriber', async () => {
    const name = uid()
    setup(client, name)

    const received: string[] = []
    const sub = await client.subscribe(name, (msg) => received.push(msg.data().toString()))

    client.publishBatch([
      [`${name}.e`, Buffer.from('a')],
      [`${name}.e`, Buffer.from('b')],
      [`${name}.e`, Buffer.from('c')],
    ])
    await waitUntil(() => received.length >= 3)
    sub.close()

    expect(received).toEqual(['a', 'b', 'c'])
  })

  it('fanout consumer — two subscribers each receive every message', async () => {
    const name = uid()
    client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    client.createConsumer(name, { name, filter: `${name}.>`, fanout: true })

    const bucket1: string[] = []
    const bucket2: string[] = []
    const sub1 = await client.subscribe(name, (msg) => bucket1.push(msg.data().toString()))
    const sub2 = await client.subscribe(name, (msg) => bucket2.push(msg.data().toString()))

    await client.publishAck(`${name}.e`, Buffer.from('broadcast'))
    await waitUntil(() => bucket1.length >= 1 && bucket2.length >= 1)
    sub1.close()
    sub2.close()

    expect(bucket1[0]).toBe('broadcast')
    expect(bucket2[0]).toBe('broadcast')
  })
})
