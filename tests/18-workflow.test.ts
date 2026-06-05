import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, AckPolicy } from '../src'

// These tests require a running broker at 127.0.0.1:9898.
// They are skipped in CI when no broker is available.

const ADDR = '127.0.0.1:9898'

async function createClient(): Promise<ArbitroClient> {
  const client = new ArbitroClient({ servers: [ADDR], timeout: 2_000 })
  await client.connect()
  return client
}

describe('workflow (stream-based)', () => {
  let client: ArbitroClient

  beforeAll(async () => { client = await createClient() })
  afterAll(async () => { await client.close() })

  it('WorkflowBuilder creates stream and processes 2 steps', async () => {
    let step0Called = false
    let step1Called = false

    const wf = await client.workflow('ts-test')
      .trigger('orders.created')
      .step('validate', async (ctx) => {
        step0Called = true
        return { context: Buffer.concat([ctx.context, Buffer.from('|validated')]) }
      })
      .step('complete', async (ctx) => {
        step1Called = true
        return { context: Buffer.concat([ctx.context, Buffer.from('|completed')]) }
      })
      .start()

    // Trigger an instance
    const instanceId = await wf.trigger(client, Buffer.from('init'))
    expect(instanceId).toBeGreaterThan(0)

    // Wait for steps to process
    await new Promise(r => setTimeout(r, 2000))

    expect(step0Called).toBe(true)
    expect(step1Called).toBe(true)
  })
})
