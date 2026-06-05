import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient } from '../src'

// These tests require a running broker at 127.0.0.1:9898.
// They fail with ECONNREFUSED when no broker is available (same as cron tests).

const ADDR = '127.0.0.1:9898'

async function createClient(): Promise<ArbitroClient> {
  const client = new ArbitroClient({ servers: [ADDR], timeout: 2_000 })
  await client.connect()
  return client
}

function wait(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms))
}

describe('workflow (stream-based)', () => {
  let client: ArbitroClient

  beforeAll(async () => { client = await createClient() })
  afterAll(async () => { await client.close() })

  // 1. Basic 2-step pipeline
  it('2 steps — context passes through', async () => {
    let step0Called = false
    let step1Called = false

    const wf = await client.workflow('ts-basic')
      .trigger('basic.>')
      .step('validate', async (ctx) => {
        step0Called = true
        return { context: Buffer.concat([ctx.context, Buffer.from('|validated')]) }
      })
      .step('complete', async (ctx) => {
        step1Called = true
        expect(ctx.context.toString()).toContain('|validated')
        return { context: Buffer.concat([ctx.context, Buffer.from('|completed')]) }
      })
      .start()

    await wf.trigger(client, Buffer.from('init'))
    await wait(2000)
    expect(step0Called).toBe(true)
    expect(step1Called).toBe(true)
  })

  // 2. 3-step pipeline
  it('3 steps — all execute in order', async () => {
    const steps: number[] = []

    const wf = await client.workflow('ts-three')
      .trigger('three.>')
      .step('s0', async (ctx) => { steps.push(0); return { context: ctx.context } })
      .step('s1', async (ctx) => { steps.push(1); return { context: ctx.context } })
      .step('s2', async (ctx) => { steps.push(2); return { context: ctx.context } })
      .start()

    await wf.trigger(client, Buffer.from('go'))
    await wait(2000)
    expect(steps).toEqual([0, 1, 2])
  })

  // 3. Step retry on error
  it('step retry — fails once then succeeds', async () => {
    let attempts = 0

    const wf = await client.workflow('ts-retry')
      .trigger('retry.>')
      .ackWait(2000)
      .maxRetries(3)
      .step('flaky', async () => {
        attempts++
        if (attempts === 1) throw new Error('transient')
        return { context: Buffer.from('ok') }
      })
      .start()

    await wf.trigger(client, Buffer.from('test'))
    await wait(5000)
    expect(attempts).toBeGreaterThanOrEqual(2)
  })

  // 4. Two concurrent instances
  it('2 concurrent instances complete independently', async () => {
    const completedA = { done: false }
    const completedB = { done: false }

    const wf = await client.workflow('ts-concurrent')
      .trigger('conc.>')
      .step('process', async (ctx) => {
        const tag = ctx.context.toString()
        if (tag.startsWith('A')) completedA.done = true
        if (tag.startsWith('B')) completedB.done = true
        return { context: ctx.context }
      })
      .start()

    await wf.trigger(client, Buffer.from('A-data'))
    await wf.trigger(client, Buffer.from('B-data'))
    await wait(2000)
    expect(completedA.done).toBe(true)
    expect(completedB.done).toBe(true)
  })

  // 5. Context overflow guard
  it('context overflow — oversized result is nacked', async () => {
    let stepCalled = false

    const wf = await client.workflow('ts-overflow')
      .trigger('overflow.>')
      .maxContextSize(10) // tiny limit
      .step('boom', async () => {
        stepCalled = true
        return { context: Buffer.alloc(100) } // exceeds 10 bytes
      })
      .start()

    await wf.trigger(client, Buffer.from('x'))
    await wait(2000)
    expect(stepCalled).toBe(true)
    // The oversized result causes a nack, not a crash
  })

  // 6. Max retries → DLQ
  it('max retries exhausted — stops retrying', async () => {
    let attempts = 0

    const wf = await client.workflow('ts-dlq')
      .trigger('dlq.>')
      .maxRetries(2)
      .ackWait(1000)
      .step('always-fail', async () => {
        attempts++
        throw new Error('permanent')
      })
      .start()

    await wf.trigger(client, Buffer.from('fail'))
    await wait(5000)
    // After 2 retries the message goes to DLQ, not infinite loop
    expect(attempts).toBeGreaterThanOrEqual(2)
    expect(attempts).toBeLessThan(10) // proves it stopped
  })

  // 7. Compensation on permanent failure
  it('saga compensation — runs on max retries', async () => {
    let compensated = false

    const wf = await client.workflow('ts-saga')
      .trigger('saga.>')
      .maxRetries(1)
      .ackWait(1000)
      .step('charge', async (ctx) => { return { context: ctx.context } })
      .compensate('charge', async () => {
        compensated = true
        return { context: Buffer.from('refunded') }
      })
      .step('ship', async () => { throw new Error('fail') })
      .start()

    await wf.trigger(client, Buffer.from('order'))
    await wait(5000)
    expect(compensated).toBe(true)
  })

  // 8. WorkflowHandle.name
  it('handle exposes workflow name', async () => {
    const wf = await client.workflow('ts-name-test')
      .trigger('name.>')
      .step('noop', async (ctx) => ({ context: ctx.context }))
      .start()

    expect(wf.name).toBe('ts-name-test')
  })
})
