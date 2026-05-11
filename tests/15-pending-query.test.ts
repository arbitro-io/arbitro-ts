import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { ArbitroClient, AckPolicy, JournalType } from '../src'
import { createClient, makeScope, uniqueName, waitUntil } from './helpers/client'

// End-to-end query of broker-side `ack_pending` via the new
// `Action::ConsumerStats` wire frame. Saturates an Explicit-ack
// consumer with maxAckPending=N and verifies the broker reports
// exactly N as the live pending count.

let client: ArbitroClient
const scope = makeScope(() => client)

beforeAll(async () => { client = await createClient() })
afterAll(async () => { await client.close() })
afterEach(async () => { await scope.cleanup() })

describe('getPending — query broker-side ack_pending', () => {
  it('client.getPending(consumerId) reports baseline and saturation', { timeout: 10_000 }, async () => {
    const name = scope.track(uniqueName('pend'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    const consumer = await client.createConsumer(name, {
      name,
      filter: `${name}.>`,
      ackPolicy: AckPolicy.Explicit,
      maxAckPending: 10,
    })

    // Baseline: no pendings before anyone subscribes.
    expect(await client.getPending(consumer.consumerId!)).toBe(0)

    // Subscribe but never ack — let the broker saturate at 10.
    const received: { ack: () => void }[] = []
    const sub = await client.subscribe(name, (msg) => {
      received.push({ ack: () => msg.ack() })
    })

    for (let i = 0; i < 30; i++) {
      client.publish(name, `${name}.work.${i}`, Buffer.from(`m${i}`))
    }
    await waitUntil(() => received.length >= 10, 5_000)

    // Broker reports exactly 10 pending — equal to the maxAckPending cap.
    const pending = await client.getPending(consumer.consumerId!)
    expect(pending).toBe(10)

    // Also reachable via stream + name lookup.
    const pendingByName = await client.getPending(name, name)
    expect(pendingByName).toBe(10)

    received.forEach((r) => r.ack())
    sub.close()
  })

  it('consumer.getPendings() works on the Consumer wrapper', { timeout: 10_000 }, async () => {
    const name = scope.track(uniqueName('pend'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    const consumer = await client.createConsumer(name, {
      name,
      filter: `${name}.>`,
      ackPolicy: AckPolicy.Explicit,
      maxAckPending: 5,
    })

    // Before any subscription.
    expect(await consumer.getPendings()).toBe(0)

    const received: { ack: () => void }[] = []
    const sub = await client.subscribe(name, (msg) => {
      received.push({ ack: () => msg.ack() })
    })

    for (let i = 0; i < 20; i++) {
      client.publish(name, `${name}.work.${i}`, Buffer.from(`m${i}`))
    }
    await waitUntil(() => received.length >= 5, 5_000)

    // Saturated at 5.
    expect(await consumer.getPendings()).toBe(5)

    received.forEach((r) => r.ack())
    sub.close()
  })

  it('returns 0 for a fresh fire-and-forget consumer (no inflight tracking)', async () => {
    const name = scope.track(uniqueName('pend'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    const consumer = await client.createConsumer(name, {
      name,
      filter: `${name}.>`,
      ackPolicy: AckPolicy.None,
    })

    expect(await consumer.getPendings()).toBe(0)

    const received: unknown[] = []
    const sub = await client.subscribe(name, (msg) => { received.push(msg) })
    for (let i = 0; i < 10; i++) {
      client.publish(name, `${name}.x.${i}`, Buffer.from(`m${i}`))
    }
    await waitUntil(() => received.length >= 10)

    // Fire-and-forget: engine skips inflight tracking entirely → always 0.
    expect(await consumer.getPendings()).toBe(0)

    sub.close()
  })
})
