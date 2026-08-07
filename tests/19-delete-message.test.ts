import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType, Stream } from '../src'
import { cleanupNamedResources, createClient, uniqueName, waitUntil } from './helpers/client'

let client: ArbitroClient
const created: string[] = []

beforeAll(async () => { client = await createClient() })
afterAll(async () => {
  await cleanupNamedResources(client, created)
  await client.close()
})

// ── Why every test here reads the sequence off a delivery ──────────────────
//
// Sequence numbers are assigned by the SHARD store, which holds every stream
// mapped to that shard — not per stream. "The second message I published" is
// only seq 2 on a broker that has served nothing else, so hardcoding it makes
// these tests pass alone and delete a stranger's message under a full suite.
// Subscribing first and recording msg.seq() names the right entry regardless
// of what else the broker is holding.

/**
 * Publish `payloads` in order and return the broker seq assigned to each.
 *
 * The probe subscribes BEFORE publishing so it rides the live push path. A
 * consumer created after the write has to be served from the store instead,
 * and that catch-up read intermittently delivers nothing (see
 * examples/sub-close-repro.ts: 5/5 live vs 3/5 catch-up). Learning sequence
 * numbers is setup, not the thing under test, so it must not sit on the
 * flaky path.
 */
async function publishAndLearnSeqs(
  stream: Stream,
  name: string,
  payloads: string[],
): Promise<Map<string, bigint>> {
  const seqs = new Map<string, bigint>()
  const probe = stream.consumer({ name: `${name}-probe`, filter: `${name}.>` })
  await probe.create()
  const sub = await probe.subscribe((msg) => {
    seqs.set(Buffer.from(msg.data()).toString(), msg.seq())
    msg.ack()
  })

  for (const [i, p] of payloads.entries()) {
    await stream.publish(`${name}.${i}`, Buffer.from(p))
  }

  try {
    await waitUntil(() => seqs.size >= payloads.length, 3000)
  } finally {
    await sub.close()
  }
  return seqs
}

describe('deleteMessage', () => {
  it('client.deleteMessage — tombstones a published message', async () => {
    const name = uniqueName('del-msg'); created.push(name)

    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    const seqs = await publishAndLearnSeqs(stream, name, ['msg-1', 'msg-2', 'msg-3'])
    const victim = seqs.get('msg-2')!

    const deleted = await client.deleteMessage(name, victim)
    expect(deleted).toBe(true)

    // Idempotent — second call returns false
    const again = await client.deleteMessage(name, victim)
    expect(again).toBe(false)

    // Non-existent seq returns false
    const missing = await client.deleteMessage(name, 10_000_000n)
    expect(missing).toBe(false)
  })

  it('stream.deleteMessage — convenience helper', async () => {
    const name = uniqueName('del-msg-stream'); created.push(name)

    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    const seqs = await publishAndLearnSeqs(stream, name, ['payload'])
    const ok = await stream.deleteMessage(seqs.get('payload')!)
    expect(ok).toBe(true)
  })

  it('consumer.deleteMessage — convenience helper', async () => {
    const name = uniqueName('del-msg-cons'); created.push(name)

    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    const seqs = await publishAndLearnSeqs(stream, name, ['data'])

    const consumer = stream.consumer({ name })
    const ok = await consumer.deleteMessage(seqs.get('data')!)
    expect(ok).toBe(true)
  })

  it('tombstoned message is not delivered to consumer', async () => {
    const name = uniqueName('del-no-deliver'); created.push(name)

    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    const seqs = await publishAndLearnSeqs(stream, name, ['first', 'second', 'third'])
    const victim = seqs.get('second')!

    const deleted = await client.deleteMessage(name, victim)
    expect(deleted).toBe(true)

    // A consumer created AFTER the tombstone reads from the start — the path
    // that asks the store directly rather than replaying per-consumer state.
    const received: string[] = []
    const consumer = stream.consumer({ name, filter: `${name}.>` })
    await consumer.create()
    const sub = await consumer.subscribe((msg) => {
      expect(msg.seq()).not.toBe(victim)
      received.push(Buffer.from(msg.data()).toString())
      msg.ack()
    })

    await waitUntil(() => received.length >= 2, 3000)
    // The claim is that a third delivery never arrives — give it room to.
    await new Promise(r => setTimeout(r, 300))

    expect(received).toEqual(['first', 'third'])

    await sub.close()
  })
})
