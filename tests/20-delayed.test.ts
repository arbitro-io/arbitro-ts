import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient } from '../src'
import { cleanupNamedResources, createClient, uniqueName, waitUntil } from './helpers/client'

let client: ArbitroClient
const created: string[] = []
beforeAll(async () => { client = await createClient() })
afterAll(async () => {
  await cleanupNamedResources(client, created)
  await client.close()
})

describe('publishDelayed', () => {
  it('delayed message is delivered after the delay', async () => {
    const name = uniqueName('dly'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`})
    await client.createConsumer(name, { name, filter: `${name}.>` })

    const received: { data: string; elapsed: number }[] = []
    const start = Date.now()
    const sub = await client.subscribe(name, (msg) => {
      received.push({ data: msg.data().toString(), elapsed: Date.now() - start })
      msg.ack()
    })

    await client.publishDelayed(name, `${name}.later`, Buffer.from('delayed-payload'), 200)

    await waitUntil(() => received.length >= 1, 5_000)
    sub.close()

    expect(received[0].data).toBe('delayed-payload')
    expect(received[0].elapsed).toBeGreaterThanOrEqual(150)
  })

  it('multiple delayed messages with different delays are all accepted', async () => {
    const name = uniqueName('dly'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`})

    await expect(
      client.publishDelayed(name, `${name}.a`, Buffer.from('first'), 300),
    ).resolves.toBeUndefined()

    await expect(
      client.publishDelayed(name, `${name}.b`, Buffer.from('second'), 100),
    ).resolves.toBeUndefined()

    await expect(
      client.publishDelayed(name, `${name}.c`, Buffer.from('third'), 200),
    ).resolves.toBeUndefined()
  })

  it('delay=0 behaves like immediate publish', async () => {
    const name = uniqueName('dly'); created.push(name)
    await client.createStream(name, { subjectFilter: `${name}.>`})
    await client.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const sub = await client.subscribe(name, (msg) => {
      received.push(msg.data().toString())
      msg.ack()
    })

    await client.publishDelayed(name, `${name}.now`, Buffer.from('immediate'), 0)

    await waitUntil(() => received.length >= 1, 3_000)
    sub.close()

    expect(received[0]).toBe('immediate')
  })
})
