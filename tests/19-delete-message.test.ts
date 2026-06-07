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

describe('deleteMessage', () => {
  it('client.deleteMessage — tombstones a published message', async () => {
    const name = uniqueName('del-msg'); created.push(name)

    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    // Publish 3 messages
    await stream.publish(`${name}.a`, Buffer.from('msg-1'))
    await stream.publish(`${name}.b`, Buffer.from('msg-2'))
    await stream.publish(`${name}.c`, Buffer.from('msg-3'))

    // Delete the second message (seq=2)
    const deleted = await client.deleteMessage(name, 2n)
    expect(deleted).toBe(true)

    // Idempotent — second call returns false
    const again = await client.deleteMessage(name, 2n)
    expect(again).toBe(false)

    // Non-existent seq returns false
    const missing = await client.deleteMessage(name, 999n)
    expect(missing).toBe(false)
  })

  it('stream.deleteMessage — convenience helper', async () => {
    const name = uniqueName('del-msg-stream'); created.push(name)

    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    await stream.publish(`${name}.x`, Buffer.from('payload'))
    const ok = await stream.deleteMessage(1n)
    expect(ok).toBe(true)
  })

  it('consumer.deleteMessage — convenience helper', async () => {
    const name = uniqueName('del-msg-cons'); created.push(name)

    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    await stream.publish(`${name}.z`, Buffer.from('data'))

    const consumer = stream.consumer({ name })
    const ok = await consumer.deleteMessage(1n)
    expect(ok).toBe(true)
  })

  it('tombstoned message is not delivered to consumer', async () => {
    const name = uniqueName('del-no-deliver'); created.push(name)

    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    // Publish 3 messages
    await stream.publish(`${name}.a`, Buffer.from('first'))
    await stream.publish(`${name}.b`, Buffer.from('second'))
    await stream.publish(`${name}.c`, Buffer.from('third'))

    // Delete message 2 BEFORE subscribing
    await client.deleteMessage(name, 2n)

    // Subscribe and collect delivered messages
    const received: Buffer[] = []
    const consumer = stream.consumer({ name, filter: `${name}.>` })
    await consumer.create()
    const sub = await consumer.subscribe((msg) => {
      received.push(Buffer.from(msg.data()))
      msg.ack()
    })

    // Wait for messages to arrive
    await waitUntil(() => received.length >= 2, 3000)
    // Give a bit more time to ensure no extra message arrives
    await new Promise(r => setTimeout(r, 200))

    // Should receive only msg 1 and 3 — msg 2 was tombstoned
    expect(received.length).toBe(2)
    expect(received[0].toString()).toBe('first')
    expect(received[1].toString()).toBe('third')

    await sub.close()
  })
})
