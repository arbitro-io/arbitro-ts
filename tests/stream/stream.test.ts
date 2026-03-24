import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType } from '../../src'
import { Consumer } from '../../src/consumer'
import { Topic } from '../../src/topic'
import { schema } from '../../src/utils'
import { createClient } from '../helpers/client'

let client: ArbitroClient

beforeAll(async () => { client = await createClient() })
afterAll(async () => { await client.close() })

describe('Stream', () => {
  it('stream() is pure construction — no network call', () => {
    const stream = client.stream('orders')
    expect(stream.name).toBe('orders')
  })

  it('stream.consumer() returns Consumer with defaults from stream context', async () => {
    // Use a unique name to avoid conflicts with other test runs
    const consumer = await client.stream('stream-consumer-defaults-test').consumer({ name: 'workers' })
    expect(consumer).toBeInstanceOf(Consumer)
    expect(consumer.streamName).toBe('stream-consumer-defaults-test')
    expect(consumer.config.name).toBe('workers')
    expect(consumer.config.filter).toBe('stream-consumer-defaults-test.>')
  })

  it('stream.topic() returns Topic bound to subject and codec', () => {
    const codec = schema({ id: 'number' })
    const topic = client.stream('orders').topic('orders.new', codec)
    expect(topic).toBeInstanceOf(Topic)
  })

  it('stream.create() is accepted by server — resolves once confirmed', async () => {
    const stream = await client.stream('stream-create-test').create({
      subjectFilter: 'stream-create-test.>',
      journal: { type: JournalType.Memory },
    })
    expect(stream.config?.subjectFilter).toBe('stream-create-test.>')
  })
})
