import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { ArbitroClient, AckPolicy, JournalType } from '../src'
import { createClient, makeScope, uniqueName, waitUntil } from './helpers/client'

// Drive the broker to a known saturation level: an Explicit-ack consumer
// that receives messages but never acks them. The broker's per-tick
// `ack_pending` gauge should rise to match the unacked count.
//
// We can't assert against the broker's `tracing` output from inside
// vitest (it goes to Docker stderr, not the test process). What we
// CAN assert is the client-observable saturation: deliveries stop at
// maxAckPending until acks land. The broker log shows the gauge —
// inspect with `docker logs arbitro-server`.

let client: ArbitroClient
const scope = makeScope(() => client)

beforeAll(async () => { client = await createClient() })
afterAll(async () => { await client.close() })
afterEach(async () => { await scope.cleanup() })

describe('state gauges (broker-side ack_pending)', () => {
  it('saturates at maxAckPending, holds steady without acks', { timeout: 15_000 }, async () => {
    const name = scope.track(uniqueName('state'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    // Cap inflight at 10. With no acks, ack_pending should pin at 10
    // and stay there — visible in the periodic metrics log.
    await client.createConsumer(name, {
      name,
      filter: `${name}.>`,
      ackPolicy: AckPolicy.Explicit,
      maxAckPending: 10,
    })

    const received: { ack: () => void }[] = []
    const sub = await client.subscribe(name, (msg) => {
      received.push({ ack: () => msg.ack() })
      // Intentionally NOT acking — let pending pile up against the cap.
    })

    // Publish 30 across distinct subjects (multi-subject avoids the
    // single-subject delivery flakiness we've seen elsewhere).
    for (let i = 0; i < 30; i++) {
      client.publish(name, `${name}.work.${i}`, Buffer.from(`m${i}`))
    }
    await waitUntil(() => received.length >= 10, 5_000)

    // Hold for ~3 seconds so the periodic metrics task (2s in CI) ticks
    // at least once with `ack_pending=10`. After this window passes,
    // the broker log will contain a `metrics ... ack_pending=10` line.
    await new Promise((r) => setTimeout(r, 3_000))
    expect(received.length).toBe(10) // capped — broker stopped delivering

    // Drain by acking — frees up the saturation gauge.
    received.forEach((r) => r.ack())
    sub.close()
  })
})
