import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType } from '../../src'
import { createClient, waitUntil } from '../helpers/client'

let client: ArbitroClient
let counter = 0

beforeAll(async () => { client = await createClient() })
afterAll(async () => { await client.close() })

function uid(): string { return `ct${++counter}` }

describe('ArbitroClient', () => {
  it('publish (NoAck) delivers to subscriber', async () => {
    const name = uid()
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
    const name = 'app'
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
    const name = uid()
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
})
