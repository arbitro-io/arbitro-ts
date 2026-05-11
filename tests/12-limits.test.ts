import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest'
import { ArbitroClient, AckPolicy, JournalType } from '../src'
import { createClient, makeScope, uniqueName, waitUntil } from './helpers/client'

// ── Per-test isolation ────────────────────────────────────────────────────
//
// Each test creates its own stream + consumer (under a unique name) and
// tears them down in `afterEach`. This prevents stale subscriptions /
// inflight queues from a previous test from racing with the current
// test's deliveries — the leading source of flakiness when a single
// shared broker is reused across the suite.

let client: ArbitroClient
const scope = makeScope(() => client)

beforeAll(async () => { client = await createClient() })
afterAll(async () => { await client.close() })
afterEach(async () => { await scope.cleanup() })

describe('ConsumerConfig limits', () => {
  it('maxAckPending = 0 means unlimited', async () => {
    const name = scope.track(uniqueName('lim'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    await client.createConsumer(name, {
      name,
      filter: `${name}.>`,
      ackPolicy: AckPolicy.Explicit,
      maxAckPending: 0,
    })

    const received: { ack: () => void }[] = []
    const sub = await client.subscribe(name, (msg) => {
      received.push({ ack: () => msg.ack() })
    })

    for (let i = 0; i < 20; i++) {
      client.publish(name, `${name}.e`, Buffer.from(`msg-${i}`))
    }
    await waitUntil(() => received.length >= 20)
    expect(received.length).toBe(20)

    received.forEach((r) => r.ack())
    sub.close()
  })

  // ── maxSubjectInflights — wire-exposed via CreateConsumer trailer ─────────
  //
  // Each entry caps in-flight (delivered, unacked) messages per subject
  // matching `pattern`. Only enforced with AckPolicy.Explicit; ignored
  // server-side for fire-and-forget consumers.

  it('maxSubjectInflights with wildcard patterns and uncapped subjects', async () => {
    const name = scope.track(uniqueName('lim'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    await client.createConsumer(name, {
      name,
      filter: `${name}.>`,
      ackPolicy: AckPolicy.Explicit,
      maxAckPending: 100,
      maxSubjectInflights: [
        { pattern: `${name}.premium.>`,  limit: 3 },
        { pattern: `${name}.freemium.>`, limit: 1 },
      ],
    })

    const received: { subject: string, ack: () => void }[] = []
    const sub = await client.subscribe(name, (msg) => {
      received.push({
        subject: Buffer.from(msg.subject()).toString(),
        ack: () => msg.ack(),
      })
    })

    // Burst: 3 other (uncapped), 3 freemium (cap 1), 5 premium (cap 3).
    for (let i = 0; i < 3; i++) client.publish(name, `${name}.other.x`, Buffer.from(`O${i}`))
    for (let i = 0; i < 3; i++) client.publish(name, `${name}.freemium.events`, Buffer.from(`F${i}`))
    for (let i = 0; i < 5; i++) client.publish(name, `${name}.premium.orders`, Buffer.from(`P${i}`))

    // Settle window — let the broker push everything it's allowed to.
    await new Promise((r) => setTimeout(r, 300))

    const premium = received.filter((m) => m.subject.includes('.premium.')).length
    const freemium = received.filter((m) => m.subject.includes('.freemium.')).length
    const other = received.filter((m) => m.subject.includes('.other.')).length

    expect(premium).toBe(3)    // capped at 3
    expect(freemium).toBe(1)   // capped at 1
    expect(other).toBe(3)      // no cap
    expect(received.length).toBe(7)

    received.forEach((r) => r.ack())
    sub.close()
  })

  it('maxSubjectInflights silently dropped for AckPolicy.None', async () => {
    const name = scope.track(uniqueName('lim'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    // Fire-and-forget consumers can't enforce per-subject inflight
    // (no ack-tracking). The server accepts the consumer and ignores
    // the limits — verifying no error and no enforcement.
    await expect(client.createConsumer(name, {
      name,
      filter: `${name}.>`,
      ackPolicy: AckPolicy.None,
      maxSubjectInflights: [{ pattern: `${name}.same`, limit: 2 }],
    })).resolves.toBeDefined()

    const received: unknown[] = []
    const sub = await client.subscribe(name, (msg) => {
      received.push(msg)
    })
    for (let i = 0; i < 10; i++) {
      client.publish(name, `${name}.same`, Buffer.from(`msg-${i}`))
    }
    await waitUntil(() => received.length >= 10)
    expect(received.length).toBe(10)
    sub.close()
  })
})
