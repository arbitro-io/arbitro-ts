import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest'
import { ArbitroClient, JournalType } from '../src'
import { cleanupNamedResources, createClient, makeScope, uniqueName, waitUntil } from './helpers/client'

// Mirrors the Rust e2e suite for queueSubscribe. These run against a real
// broker — compiling is not evidence that a queue distributes anything.

let admin: ArbitroClient
const created: string[] = []
const scope = makeScope(() => admin)

beforeAll(async () => { admin = await createClient() })
afterAll(async () => {
  await cleanupNamedResources(admin, created)
  await admin.close()
})
afterEach(async () => { await scope.cleanup() })

async function makeStream(name: string): Promise<void> {
  await admin.createStream(name, {
    subjectFilter: `${name}.>`,
    journal: { type: JournalType.Memory },
  })
}

describe('queueSubscribe', () => {
  // Same group from three connections: every message lands on exactly one
  // worker. A duplicate means queue dedup broke; a shortfall means loss.
  it('load-balances across workers, each message exactly once', async () => {
    const name = scope.track(uniqueName('qs-lb'))
    await makeStream(name)

    const TOTAL = 30
    const received: Buffer[] = []
    const workers = await Promise.all([createClient(), createClient(), createClient()])
    const subs = await Promise.all(workers.map(w =>
      w.queueSubscribe(name, {
        group:  'workers',
        filter: `${name}.>`,
        onMessage(msg) { received.push(Buffer.from(msg.data())); msg.ack() },
      }),
    ))

    for (let i = 0; i < TOTAL; i++) {
      await admin.publish(name, `${name}.job`, Buffer.from([i]))
    }

    await waitUntil(() => received.length >= TOTAL, 10_000)
    // Give any stray duplicate a chance to arrive before asserting.
    await new Promise(r => setTimeout(r, 300))

    expect(received.length).toBe(TOTAL)
    const distinct = new Set(received.map(b => b[0]))
    expect(distinct.size).toBe(TOTAL)

    subs.forEach(s => s.close())
    await Promise.all(workers.map(w => w.close()))
  })

  // One durable consumer per queue NAME, counted against the broker's own
  // listConsumers — not per subscription and not per connection.
  it('creates one consumer per queue name, not per subscription', async () => {
    const name = scope.track(uniqueName('qs-count'))
    await makeStream(name)

    const count = async (): Promise<number> => (await admin.listConsumers(name)).length
    expect(await count()).toBe(0)

    const workers = await Promise.all([createClient(), createClient(), createClient()])
    const subs = await Promise.all(workers.map(w =>
      w.queueSubscribe(name, { group: 'workers', onMessage(msg) { msg.ack() } }),
    ))
    expect(await count()).toBe(1)

    const auditor = await createClient()
    const auditSub = await auditor.queueSubscribe(name, {
      group: 'audit', onMessage(msg) { msg.ack() },
    })
    expect(await count()).toBe(2)

    const late = await createClient()
    const lateSub = await late.queueSubscribe(name, {
      group: 'workers', onMessage(msg) { msg.ack() },
    })
    expect(await count()).toBe(2)

    subs.forEach(s => s.close()); auditSub.close(); lateSub.close()
    await Promise.all([...workers, auditor, late].map(c => c.close()))
  })

  // A different group is an independent durable queue: each gets its own
  // full copy of the stream.
  it('treats a distinct group as an independent queue with its own copy', async () => {
    const name = scope.track(uniqueName('qs-indep'))
    await makeStream(name)

    const billing: number[] = []
    const audit: number[] = []
    const [cb, ca] = await Promise.all([createClient(), createClient()])
    const subB = await cb.queueSubscribe(name, {
      group: 'billing', onMessage(msg) { billing.push(msg.data()[0]!); msg.ack() },
    })
    const subA = await ca.queueSubscribe(name, {
      group: 'audit', onMessage(msg) { audit.push(msg.data()[0]!); msg.ack() },
    })

    const N = 5
    for (let i = 0; i < N; i++) {
      await admin.publish(name, `${name}.evt`, Buffer.from([i]))
    }

    await waitUntil(() => billing.length >= N && audit.length >= N, 10_000)
    expect(billing.length).toBe(N)
    expect(audit.length).toBe(N)

    subB.close(); subA.close()
    await Promise.all([cb.close(), ca.close()])
  })

  // Omitting group falls back to the stream name, so the minimal call is
  // just a handler.
  it('defaults the group to the stream name', async () => {
    const name = scope.track(uniqueName('qs-default'))
    await makeStream(name)

    const got: Buffer[] = []
    const worker = await createClient()
    const sub = await worker.queueSubscribe(name, {
      onMessage(msg) { got.push(Buffer.from(msg.data())); msg.ack() },
    })

    // The durable consumer must be reachable under the stream's own name —
    // that is what proves the fallback happened.
    expect(await admin.consumerExists(name, name)).toBe(true)

    await admin.publish(name, `${name}.job`, Buffer.from('x'))
    await waitUntil(() => got.length >= 1, 10_000)
    expect(got.length).toBe(1)

    sub.close()
    await worker.close()
  })

  // The stream context supplies itself and the queue identity, so the
  // common case is a handler and nothing else.
  it('joins from a Stream context with only a handler', async () => {
    const name = scope.track(uniqueName('qs-stream'))
    await makeStream(name)

    const got: Buffer[] = []
    const worker = await createClient()
    const stream = worker.stream(name)
    const sub = await stream.queueSubscribe({
      onMessage(msg) { got.push(Buffer.from(msg.data())); msg.ack() },
    })

    await admin.publish(name, `${name}.job`, Buffer.from('y'))
    await waitUntil(() => got.length >= 1, 10_000)
    expect(got[0]!.toString()).toBe('y')

    sub.close()
    await worker.close()
  })

  // A job the worker never acked must come back. This is the half of
  // at-least-once that separates a queue from fire-and-forget.
  it('redelivers a job the worker nacked', async () => {
    const name = scope.track(uniqueName('qs-redeliver'))
    await makeStream(name)

    const deliveries: string[] = []
    const worker = await createClient()
    const sub = await worker.queueSubscribe(name, {
      group: 'workers',
      onMessage(msg) {
        deliveries.push(msg.data().toString())
        // Refuse the first delivery, accept the redelivery.
        if (deliveries.length === 1) msg.nack()
        else msg.ack()
      },
    })

    await admin.publish(name, `${name}.job`, Buffer.from('job-1'))
    await waitUntil(() => deliveries.length >= 2, 10_000)

    expect(deliveries.length).toBeGreaterThanOrEqual(2)
    expect(deliveries[0]).toBe('job-1')
    expect(deliveries[1]).toBe('job-1')

    sub.close()
    await worker.close()
  })
})
