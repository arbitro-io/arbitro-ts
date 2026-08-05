import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { ArbitroClient, Service } from '../src'
import { createClient, makeScope, uniqueName } from './helpers/client'

// Integration test — needs a live broker (see tests/helpers/client.ts).
//
// Service worker consumers are a QUEUE: N instances of the same service share
// the request load, each request handled exactly once. This used to be broken
// two ways at once, and both halves are asserted here:
//
//   1. `deliverMode: 0` (Fanout) made the broker force QueueId(0) and discard
//      the group, so the group could never round-robin.
//   2. Every instance created the worker consumer under the SAME name, so all
//      of them collapsed onto one broker consumer id. The broker allows one
//      subscription per consumer id, so the last instance to subscribe retired
//      the previous binding and took 100% of the traffic while its siblings
//      sat idle (measured before the fix: 0/0/30 across three instances).
//
// The reply consumer stays per-instance on purpose and is asserted separately:
// making replies a shared queue would deliver instance A's reply to instance B
// and hang every request.

let a: ArbitroClient
let b: ArbitroClient
let caller: ArbitroClient
const scope = makeScope(() => caller)

beforeAll(async () => {
  ;[a, b, caller] = await Promise.all([createClient(), createClient(), createClient()])
})

afterAll(async () => {
  await Promise.all([a.close(), b.close(), caller.close()])
})

afterEach(async () => { await scope.cleanup() })

/** Builds a service instance whose `work` handler records what it handled. */
async function worker(
  client: ArbitroClient,
  svcName: string,
  tag: string,
  seen: string[],
): Promise<Service> {
  const svc = await client.service(svcName).build()
  svc.handle('work', (req) => {
    const body = req.data().toString()
    seen.push(body)
    return Buffer.from(`${tag}:${body}`)
  })
  return svc
}

describe('service worker consumer (queue group)', () => {
  it('shares requests across two instances, each request handled exactly once', async () => {
    const svcName = uniqueName('svcq')
    scope.track(`_svc-${svcName}`)
    const callerName = uniqueName('svcc')
    scope.track(`_svc-${callerName}`)

    const seenA: string[] = []
    const seenB: string[] = []
    const [svcA, svcB] = await Promise.all([
      worker(a, svcName, 'A', seenA),
      worker(b, svcName, 'B', seenB),
    ])
    // Distinct instance ids are what keep the two worker consumers distinct.
    expect(svcA.instanceId).not.toBe(svcB.instanceId)

    const client = await caller.service(callerName).build()
    // Both worker subscriptions must be live before the first publish, or an
    // early request lands on whichever instance registered first and the
    // split looks lopsided for reasons that have nothing to do with the queue.
    await new Promise(r => setTimeout(r, 300))

    const TOTAL = 12
    const replies: string[] = []
    for (let i = 0; i < TOTAL; i++) {
      const rep = await client.request(svcName, 'work', Buffer.from(`m${i}`), 5_000)
      replies.push(rep.toString())
    }

    // Exactly-once: the two instances together saw every request, no dupes.
    const handled = [...seenA, ...seenB].sort()
    const expected = Array.from({ length: TOTAL }, (_, i) => `m${i}`).sort()
    expect(handled).toEqual(expected)

    // Shared: neither instance was starved. Asserted as "both did real work"
    // rather than an exact split — the drain rotates its match set, so the
    // balance is even in practice but not a contract worth pinning a test to.
    expect(seenA.length).toBeGreaterThan(0)
    expect(seenB.length).toBeGreaterThan(0)

    // Every reply came back to the caller, tagged by whichever instance ran it.
    expect(replies.length).toBe(TOTAL)
    for (let i = 0; i < TOTAL; i++) {
      expect(replies[i]).toMatch(new RegExp(`^[AB]:m${i}$`))
    }

    svcA.close(); svcB.close(); client.close()
  })

  it('keeps replies per-instance: two callers each get only their own', async () => {
    const svcName = uniqueName('svcq')
    scope.track(`_svc-${svcName}`)
    const c1Name = uniqueName('svcc')
    scope.track(`_svc-${c1Name}`)
    const c2Name = uniqueName('svcc')
    scope.track(`_svc-${c2Name}`)

    const seen: string[] = []
    const svc = await worker(a, svcName, 'S', seen)

    // Two caller instances in one process — if the reply consumer were a
    // shared queue (or if instance ids collided) one of these would hang.
    const c1 = await caller.service(c1Name).build()
    const c2 = await b.service(c2Name).build()
    await new Promise(r => setTimeout(r, 300))

    const [r1, r2] = await Promise.all([
      c1.request(svcName, 'work', Buffer.from('from-1'), 5_000),
      c2.request(svcName, 'work', Buffer.from('from-2'), 5_000),
    ])
    expect(r1.toString()).toBe('S:from-1')
    expect(r2.toString()).toBe('S:from-2')

    svc.close(); c1.close(); c2.close()
  })
})
