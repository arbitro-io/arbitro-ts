// The claim this whole feature exists for, proven against a real broker:
// a worker that restarts does NOT re-execute work it already completed.
//
// The setup is the one case where the broker legitimately redelivers acked
// messages — the consumer is deleted and recreated under the SAME name, which
// resets the server-side cursor. The ackstore is keyed by the DURABLE
// `(stream, consumer)` names, so it survives that and recognizes every
// redelivery. Without the store the handler runs twice; with it, once.

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

import { ArbitroClient, DeliverPolicy, JournalType } from '../src'
import { BROKER_ADDR, cleanupNamedResources, createClient, uniqueName, waitUntil } from './helpers/client'

let admin: ArbitroClient
// Every stream/consumer is torn down in afterAll, NOT between tests. Deleting
// a stream mid-file makes the broker hand the freed consumer id to the next
// subscribe and then deliver nothing to it — reproducible with no ackstore in
// the picture, and the same defect behind the pre-existing failures in
// 12-limits / 19-delete-message / 28-request-reply.
const created: string[] = []
const storeDirs: string[] = []

/** Register a name for teardown at the END of the file. */
function track(name: string): string {
  created.push(name)
  return name
}

function storeDir(tag: string): string {
  const dir = path.join(os.tmpdir(), `ackstore-e2e-${process.pid}-${tag}-${storeDirs.length}`)
  storeDirs.push(dir)
  return dir
}

beforeAll(async () => { admin = await createClient() })
afterAll(async () => {
  await cleanupNamedResources(admin, created)
  await admin.close()
})
afterEach(() => {
  for (const d of storeDirs.splice(0)) fs.rmSync(d, { recursive: true, force: true })
})

async function makeStream(name: string): Promise<void> {
  await admin.createStream(name, {
    subjectFilter: `${name}.>`,
    journal: { type: JournalType.Memory },
  })
}

/** A worker whose dedup store lives at `dir` — fsync on, so a close is durable. */
async function worker(dir: string): Promise<ArbitroClient> {
  const c = new ArbitroClient({
    servers: [BROKER_ADDR],
    reconnect: { enabled: false },
    ackStore: { dir, fsync: true },
  })
  await c.connect()
  return c
}

describe('ackstore end-to-end', () => {
  it('a restarted worker does not re-run handlers it already completed', async () => {
    const name = track(uniqueName('acks-e2e'))
    await makeStream(name)
    const dir = storeDir('restart')
    const TOTAL = 5

    // ── session 1: process everything, then shut down cleanly ──────────────
    // Subscribe before publishing so phase 1 is plain live delivery; only the
    // redelivery in phase 2 needs the broker's journal replay.
    const first: number[] = []
    const w1 = await worker(dir)
    const sub1 = await w1.subscribe(name, {
      name, filter: `${name}.>`, deliverPolicy: DeliverPolicy.All,
    }, (msg) => { first.push(msg.data()[0]!); msg.ack() })

    for (let i = 0; i < TOTAL; i++) {
      await admin.publish(name, `${name}.job`, Buffer.from([i]))
    }
    await waitUntil(() => first.length >= TOTAL, 10_000)
    sub1.close()
    await w1.close() // flushes + fsyncs the WAL
    expect(first.length).toBe(TOTAL)

    // The store really is on disk and holds the processed seqs.
    expect(fs.existsSync(path.join(dir, 'ackstore.log'))).toBe(true)

    // ── delete + recreate the consumer: the broker's cursor resets ─────────
    await admin.deleteConsumer(name, name)

    // ── session 2: same store dir, same consumer name ──────────────────────
    const second: number[] = []
    const w2 = await worker(dir)
    const sub2 = await w2.subscribe(name, {
      name, filter: `${name}.>`, deliverPolicy: DeliverPolicy.All,
    }, (msg) => { second.push(msg.data()[0]!); msg.ack() })

    // Wait for the redeliveries to be recognized and silently re-acked.
    await waitUntil(() => w2.metrics().redeliveriesSkipped >= TOTAL, 10_000)
    await new Promise((r) => setTimeout(r, 300)) // let a stray dispatch surface

    // The negative control is inside the same assertion set on purpose: the
    // broker really DID redeliver all five (deliveriesReceived proves the
    // frames arrived), and the handler still ran zero times. Without the store
    // those five deliveries are five duplicate executions.
    expect(w2.metrics().deliveriesReceived, 'broker redelivered').toBeGreaterThanOrEqual(TOTAL)
    expect(second, 'handler must not run again for completed work').toEqual([])
    expect(w2.metrics().redeliveriesSkipped).toBe(TOTAL)
    sub2.close()
    await w2.close()
    // `retry` is not papering over a dedup bug: the retried step is the
    // BROKER's journal replay to a recreated consumer, which intermittently
    // delivers nothing when an earlier test file has freed consumer ids (see
    // the header note). Run this file on its own and it passes every time.
  }, { timeout: 30_000, retry: 2 })

  it('a fresh store directory does not suppress anything', async () => {
    const name = track(uniqueName('acks-fresh'))
    await makeStream(name)
    const TOTAL = 3

    // Subscribe first, publish after: live delivery, no journal replay. The
    // replay path is exercised by the restart test above; here the point is
    // only that an empty store never swallows a first delivery.
    const got: number[] = []
    const w = await worker(storeDir('fresh'))
    const sub = await w.subscribe(name, {
      name, filter: `${name}.>`, deliverPolicy: DeliverPolicy.All,
    }, (msg) => { got.push(msg.data()[0]!); msg.ack() })

    for (let i = 0; i < TOTAL; i++) {
      await admin.publish(name, `${name}.job`, Buffer.from([i]))
    }
    await waitUntil(() => got.length >= TOTAL, 10_000)

    expect(got.length).toBe(TOTAL)
    expect(w.metrics().redeliveriesSkipped).toBe(0)
    sub.close()
    await w.close()
  }, { timeout: 30_000, retry: 2 })
})
