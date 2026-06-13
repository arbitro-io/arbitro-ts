import { ArbitroClient } from '../../src'

export const BROKER_ADDR = process.env.ARBITRO_ADDR ?? '127.0.0.1:9898'
let uniqueCounter = 0

/** Returns a connected ArbitroClient pointing at the always-running arbitro instance. */
export async function createClient(opts?: { prefix?: string }): Promise<ArbitroClient> {
  const client = new ArbitroClient({
    servers:   [BROKER_ADDR],
    prefix:    opts?.prefix,
    reconnect: { enabled: false },
  })
  await client.connect()
  return client
}

export function uniqueName(prefix = 'ts'): string {
  return `${prefix}-${process.pid}-${Date.now().toString(36)}-${++uniqueCounter}`
}

export async function cleanupNamedResources(client: ArbitroClient, names: string[]): Promise<void> {
  for (const name of [...new Set(names)].reverse()) {
    try { await client.deleteConsumer(name, name) } catch {}
    try { await client.deleteStream(name) } catch {}
  }
}

/**
 * Per-test scope cleanup helper. Returns a `track` function that registers
 * a stream/consumer name for teardown, and a `cleanup` function that
 * drops everything tracked since the last cleanup. Designed to be wired
 * to `beforeEach`/`afterEach` so each test starts with a clean broker
 * — accumulating state across tests is the leading cause of flakiness
 * (subscriptions from earlier tests can race with new deliveries).
 */
export function makeScope(getClient: () => ArbitroClient): {
  track: (name: string) => string
  cleanup: () => Promise<void>
} {
  let pending: string[] = []
  return {
    track: (name) => { pending.push(name); return name },
    cleanup: async () => {
      const names = pending
      pending = []
      await cleanupNamedResources(getClient(), names)
    },
  }
}

/** Polls `cond` every 20 ms until it returns true or `timeoutMs` elapses. */
export function waitUntil(cond: () => boolean, timeoutMs = 3_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs
    const poll = (): void => {
      if (cond()) return resolve()
      if (Date.now() > deadline) return reject(new Error('waitUntil timeout'))
      setTimeout(poll, 20)
    }
    poll()
  })
}
