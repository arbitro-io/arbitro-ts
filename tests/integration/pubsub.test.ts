import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType } from '../../src'
import { cleanupNamedResources, createClient, uniqueName, waitUntil } from '../helpers/client'

let client: ArbitroClient
const created: string[] = []
beforeAll(async () => { client = await createClient() })
afterAll(async () => {
  await cleanupNamedResources(client, created)
  await client.close()
})

describe('publish/subscribe', () => {
  it('fire-and-forget publish reaches subscriber', async () => {
    const name = uniqueName('p'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const sub = await client.subscribe(name, (msg) => received.push(msg.data().toString()))

    client.publish(name, `${name}.e`, Buffer.from('hello'))
    await waitUntil(() => received.length >= 1)
    sub.close()

    expect(received[0]).toBe('hello')
  })

  it('publishAck resolves after server confirms receipt', async () => {
    const name = uniqueName('p'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>` })
    await expect(
      client.publishAck(name, `${name}.e`, Buffer.from('acked')),
    ).resolves.toBeUndefined()
  })

  it('publishBatch — all messages reach subscriber', async () => {
    const name = uniqueName('p'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const sub = await client.subscribe(name, (msg) => received.push(msg.data().toString()))

    client.publishBatch(name, [
      [`${name}.e`, Buffer.from('a')],
      [`${name}.e`, Buffer.from('b')],
      [`${name}.e`, Buffer.from('c')],
    ])
    await waitUntil(() => received.length >= 3)
    sub.close()

    expect(received).toEqual(['a', 'b', 'c'])
  })

  // Broker overlap check blocks two subscribers with overlapping filters on the same stream,
  // even in fanout mode. Requires broker-side fix to skip overlap for same-group fanout.
  it.skip('fanout consumer — two subscribers each receive every message', async () => {
    const name = uniqueName('p'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>`, fanout: true })

    const bucket1: string[] = []
    const bucket2: string[] = []

    const sub1 = await client.subscribe(name, (msg) => bucket1.push(msg.data().toString()))
    const sub2 = await client.subscribe(name, (msg) => bucket2.push(msg.data().toString()))

    await client.publishAck(name, `${name}.e`, Buffer.from('broadcast'))
    await waitUntil(() => bucket1.length >= 1 && bucket2.length >= 1)
    sub1.close()
    sub2.close()

    expect(bucket1[0]).toBe('broadcast')
    expect(bucket2[0]).toBe('broadcast')
  })
})
