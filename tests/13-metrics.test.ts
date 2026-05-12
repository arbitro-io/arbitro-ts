import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { ArbitroClient, AckPolicy, JournalType } from '../src'
import { createClient, makeScope, uniqueName, waitUntil } from './helpers/client'

let client: ArbitroClient
const scope = makeScope(() => client)

beforeAll(async () => { client = await createClient() })
afterAll(async () => { await client.close() })
afterEach(async () => { await scope.cleanup() })

describe('client metrics', () => {
  it('counts publishes, deliveries, and tracks active subscriptions', async () => {
    const name = scope.track(uniqueName('met'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    await client.createConsumer(name, {
      name,
      filter: `${name}.>`,
      ackPolicy: AckPolicy.None,
    })

    const before = client.metrics()


    let delivered = 0
    const sub = await client.subscribe(name, () => { delivered++ })

    // Snapshot right after subscribe — gauge should show +1 active sub.
    const afterSub = client.metrics()
    expect(afterSub.activeSubscriptions).toBe(before.activeSubscriptions + 1)

    // Publish 5, expect counter to advance.
    for (let i = 0; i < 5; i++) {
      client.publish(name, `${name}.e`, Buffer.from(`m${i}`))
    }
    await waitUntil(() => delivered >= 5)

    const after = client.metrics()
    expect(after.publishesSent).toBe(before.publishesSent + 5)
    expect(after.deliveriesReceived).toBeGreaterThanOrEqual(before.deliveriesReceived + 5)

    sub.close()
    const afterClose = client.metrics()
    expect(afterClose.activeSubscriptions).toBe(before.activeSubscriptions)
  })

  it('publishBatch increments publishBatchEntries by entry count', async () => {
    const name = scope.track(uniqueName('met'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    const before = client.metrics()
    client.publishBatch(name, [
      { subject: `${name}.a`, payload: Buffer.from('1') },
      { subject: `${name}.b`, payload: Buffer.from('2') },
      { subject: `${name}.c`, payload: Buffer.from('3') },
    ])
    const after = client.metrics()
    expect(after.publishBatchEntries).toBe(before.publishBatchEntries + 3)
  })
})
