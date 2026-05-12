import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType } from '../src'
import { cleanupNamedResources, createClient, uniqueName, waitUntil } from './helpers/client'

let client: ArbitroClient
const created: string[] = []
beforeAll(async () => { client = await createClient() })
afterAll(async () => {
  await cleanupNamedResources(client, created)
  await client.close()
})

describe('ArbitroClient', () => {
  it('publish (NoAck) delivers to subscriber', async () => {
    const name = uniqueName('ct'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const sub = await client.subscribe(name, (msg) => received.push(msg.data().toString()))

    client.publish(name, `${name}.e`, Buffer.from('hello'))
    await waitUntil(() => received.length >= 1)
    sub.close()

    expect(received[0]).toBe('hello')
  })

  it('prefix is prepended to the wire subject', async () => {
    const name = uniqueName('app'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>` })

    const prefixed = await createClient({ prefix: name })
    await prefixed.resolveStream(name)
    const subjects: string[] = []
    const sub = await client.subscribe(name, (msg) => subjects.push(msg.subject().toString()))

    prefixed.publish(name, 'orders.new', Buffer.from('test'))
    await waitUntil(() => subjects.length >= 1)
    sub.close()
    await prefixed.close()

    expect(subjects[0]).toBe(`${name}.orders.new`)
  })

  it('publishBatch — all messages reach subscriber in order', async () => {
    const name = uniqueName('ct'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const sub = await client.subscribe(name, (msg) => received.push(msg.data().toString()))

    client.publishBatch(name, [
      { subject: `${name}.e`, payload: Buffer.from('a') },
      { subject: `${name}.e`, payload: Buffer.from('b') },
      { subject: `${name}.e`, payload: Buffer.from('c') },
    ])
    await waitUntil(() => received.length >= 3)
    sub.close()

    expect(received).toEqual(['a', 'b', 'c'])
  })

  it('stream info / exists / upsert work', async () => {
    const name = uniqueName('ct'); created.push(name)
    const cfg = { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory as const } }
    await client.createStream(name, cfg)

    await expect(client.streamExists(name)).resolves.toBe(true)
    await expect(client.getStreamInfo(name)).resolves.not.toBeNull()
    await expect(client.upsertStream(name, cfg)).resolves.toBeDefined()
  })

  it('consumer info / exists / upsert work', async () => {
    const name = uniqueName('ct'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    const cfg = { name, filter: `${name}.>` }
    await client.createConsumer(name, cfg)

    await expect(client.consumerExists(name, name)).resolves.toBe(true)
    await expect(client.getConsumerInfo(name, name)).resolves.toMatchObject({
      group: name,
      config: { name },
    })
    await expect(client.upsertConsumer(name, cfg)).resolves.toBeDefined()
  })

  it('deleteStream and deleteConsumer await server confirmation', async () => {
    const name = uniqueName('ct'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>` })

    // Server confirms deletion via RepOk (same as Rust client — no exists check after delete,
    // server keeps names() registry entry after shard removal).
    await client.deleteConsumer(name, name)
    await client.deleteStream(name)
    await expect(client.streamExists(name)).resolves.toBe(false)
  })
})
