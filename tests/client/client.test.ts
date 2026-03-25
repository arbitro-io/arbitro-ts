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

describe('ArbitroClient', () => {
  it('publish (NoAck) delivers to subscriber', async () => {
    const name = uniqueName('ct'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const sub = await client.subscribe(name, (msg) => received.push(msg.data().toString()))

    client.publish(`${name}.e`, Buffer.from('hello'))
    await waitUntil(() => received.length >= 1)
    sub.close()

    expect(received[0]).toBe('hello')
  })

  it('prefix is prepended to the wire subject', async () => {
    const name = uniqueName('app'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>` })

    const prefixed = await createClient({ prefix: name })
    const subjects: string[] = []
    const sub = await client.subscribe(name, (msg) => subjects.push(msg.subject().toString()))

    prefixed.publish('orders.new', Buffer.from('test'))
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

    client.publishBatch([
      [`${name}.e`, Buffer.from('a')],
      [`${name}.e`, Buffer.from('b')],
      [`${name}.e`, Buffer.from('c')],
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
    await expect(client.getStreamInfo(name)).resolves.toMatchObject({
      name,
      config: { subjectFilter: `${name}.>` },
    })
    await expect(client.upsertStream(name, cfg)).resolves.toBeDefined()
  })

  it('consumer info / exists / upsert work', async () => {
    const name = uniqueName('ct'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    const cfg = { name, filter: `${name}.>` }
    await client.createConsumer(name, cfg)

    await expect(client.consumerExists(name)).resolves.toBe(true)
    await expect(client.getConsumerInfo(name)).resolves.toMatchObject({
      group: name,
      stream: name,
      config: { name, filter: `${name}.>` },
    })
    await expect(client.upsertConsumer(name, cfg)).resolves.toBeDefined()
  })

  it('deleteStream and deleteConsumer await server confirmation', async () => {
    const name = uniqueName('ct'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await client.createConsumer(name, { name, filter: `${name}.>` })

    await client.deleteConsumer(name)
    await expect(client.consumerExists(name)).resolves.toBe(false)

    await client.deleteStream(name, { deleteData: false })
    await expect(client.streamExists(name)).resolves.toBe(false)
  })
})
