import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest'

import { ArbitroClient, ArbitroError, ErrorCode, JournalType } from '../src'
import { createClient, makeScope, uniqueName } from './helpers/client'

// End-to-end coverage of per-stream broker dedup, exercised through
// the public TS API. Mirrors the Rust `idempotency_invariants.rs`
// suite — every assertion here pins a behaviour a downstream user can
// rely on.

describe('idempotency window', () => {
  let client: ArbitroClient
  const scope = makeScope(() => client)

  beforeAll(async () => { client = await createClient() })
  afterAll(async () => { await client.close() })
  beforeEach(scope.cleanup)
  afterEach(scope.cleanup)

  it('window=0 (default): duplicate msgId publishes both succeed', async () => {
    const name = scope.track(uniqueName('idem-off'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
      // idempotencyWindowMs omitted -> 0
    })

    await client.publish(name, `${name}.k`, Buffer.from('v1'), { msgId: 'same' })
    // Second publish with the same msgId must NOT be rejected when
    // the stream was created with no window.
    await expect(
      client.publish(name, `${name}.k`, Buffer.from('v1'), { msgId: 'same' }),
    ).resolves.toBeUndefined()
  })

  it('window>0: duplicate msgId rejected with IdempotencyDuplicate', async () => {
    const name = scope.track(uniqueName('idem-on'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
      idempotencyWindowMs: 60_000,
    })

    await client.publish(name, `${name}.k`, Buffer.from('first'), { msgId: 'order-1' })

    let caught: ArbitroError | undefined
    try {
      await client.publish(name, `${name}.k`, Buffer.from('second'), { msgId: 'order-1' })
    } catch (e) {
      caught = e as ArbitroError
    }
    expect(caught).toBeInstanceOf(ArbitroError)
    expect(caught?.wireCode).toBe(ErrorCode.IdempotencyDuplicate)
  })

  it('empty msgId is never deduped, even with window>0', async () => {
    const name = scope.track(uniqueName('idem-empty'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
      idempotencyWindowMs: 60_000,
    })
    // Three identical publishes with no msgId — all three must land.
    await client.publish(name, `${name}.k`, Buffer.from('v'))
    await client.publish(name, `${name}.k`, Buffer.from('v'))
    await expect(client.publish(name, `${name}.k`, Buffer.from('v'))).resolves.toBeUndefined()
  })

  it('batch with internal duplicate msgId is rejected atomically', async () => {
    const name = scope.track(uniqueName('idem-batch'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
      idempotencyWindowMs: 60_000,
    })

    let caught: ArbitroError | undefined
    try {
      await client.publishBatch(name, [
        { subject: `${name}.k`, payload: Buffer.from('a'), msgId: Buffer.from('twin') },
        { subject: `${name}.k`, payload: Buffer.from('b'), msgId: Buffer.from('twin') },
      ])
    } catch (e) {
      caught = e as ArbitroError
    }
    expect(caught?.wireCode).toBe(ErrorCode.IdempotencyDuplicate)

    // The twin id was NOT recorded (atomic rollback) — a follow-up
    // publish with the same id must succeed.
    await expect(
      client.publish(name, `${name}.k`, Buffer.from('retry'), { msgId: 'twin' }),
    ).resolves.toBeUndefined()
  })

  it('batch with mixed id + no-id entries: no-id entries never collide', async () => {
    const name = scope.track(uniqueName('idem-mixed'))
    await client.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
      idempotencyWindowMs: 60_000,
    })

    // First batch — three id-bearing entries + two no-id entries.
    await client.publishBatch(name, [
      { subject: `${name}.k`, payload: Buffer.from('a'), msgId: Buffer.from('m-1') },
      { subject: `${name}.k`, payload: Buffer.from('x') },
      { subject: `${name}.k`, payload: Buffer.from('b'), msgId: Buffer.from('m-2') },
      { subject: `${name}.k`, payload: Buffer.from('y') },
    ])

    // Second batch — same no-id entries (allowed) + new ids.
    await expect(client.publishBatch(name, [
      { subject: `${name}.k`, payload: Buffer.from('x-again') },
      { subject: `${name}.k`, payload: Buffer.from('c'), msgId: Buffer.from('m-3') },
      { subject: `${name}.k`, payload: Buffer.from('y-again') },
    ])).resolves.toBeTypeOf('bigint')

    // Third batch — reuses m-1, must be rejected.
    let caught: ArbitroError | undefined
    try {
      await client.publishBatch(name, [
        { subject: `${name}.k`, payload: Buffer.from('z') },
        { subject: `${name}.k`, payload: Buffer.from('replay'), msgId: Buffer.from('m-1') },
      ])
    } catch (e) {
      caught = e as ArbitroError
    }
    expect(caught?.wireCode).toBe(ErrorCode.IdempotencyDuplicate)
  })

  it('two streams: enabling dedup on one does not affect the other', async () => {
    const a = scope.track(uniqueName('idem-a'))
    const b = scope.track(uniqueName('idem-b'))
    await client.createStream(a, {
      subjectFilter: `${a}.>`,
      journal: { type: JournalType.Memory },
      idempotencyWindowMs: 60_000,
    })
    await client.createStream(b, {
      subjectFilter: `${b}.>`,
      journal: { type: JournalType.Memory },
    })

    await client.publish(a, `${a}.k`, Buffer.from('1'), { msgId: 'shared' })
    // Same msg id on the other stream — must NOT be rejected.
    await expect(
      client.publish(b, `${b}.k`, Buffer.from('1'), { msgId: 'shared' }),
    ).resolves.toBeUndefined()
    // Repeat on the plain stream is also fine.
    await expect(
      client.publish(b, `${b}.k`, Buffer.from('2'), { msgId: 'shared' }),
    ).resolves.toBeUndefined()

    // Repeat on the dedup stream must reject.
    let caught: ArbitroError | undefined
    try {
      await client.publish(a, `${a}.k`, Buffer.from('2'), { msgId: 'shared' })
    } catch (e) {
      caught = e as ArbitroError
    }
    expect(caught?.wireCode).toBe(ErrorCode.IdempotencyDuplicate)
  })
})
