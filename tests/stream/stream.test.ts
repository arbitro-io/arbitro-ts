import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType } from '../../src'
import { Consumer } from '../../src/consumer'
import { Topic } from '../../src/topic'
import { schema } from '../../src/utils'
import { cleanupNamedResources, createClient, uniqueName } from '../helpers/client'

let client: ArbitroClient
const created: string[] = []

beforeAll(async () => { client = await createClient() })
afterAll(async () => {
  await cleanupNamedResources(client, created)
  await client.close()
})

describe('Stream', () => {
  it('stream() is pure construction — no network call', () => {
    const stream = client.stream('orders')
    expect(stream.name).toBe('orders')
  })

  it('stream.consumer() returns Consumer with defaults from stream context', async () => {
    const streamName = uniqueName('stream-consumer-defaults-test')
    const consumer = await client.stream(streamName).consumer({ name: 'workers' })
    expect(consumer).toBeInstanceOf(Consumer)
    expect(consumer.streamName).toBe(streamName)
    expect(consumer.config.name).toBe('workers')
    expect(consumer.config.filter).toBe(`${streamName}.>`)
  })

  it('stream.topic() returns Topic bound to subject and codec', () => {
    const codec = schema({ id: 'number' })
    const topic = client.stream('orders').topic('orders.new', codec)
    expect(topic).toBeInstanceOf(Topic)
  })

  it('stream.create() is accepted by server — resolves once confirmed', async () => {
    const streamName = uniqueName('stream-create-test'); created.push(streamName)
    const stream = await client.stream(streamName).create({
      subjectFilter: `${streamName}.>`,
      journal: { type: JournalType.Memory },
    })
    expect(stream.config?.subjectFilter).toBe(`${streamName}.>`)
  })
})
