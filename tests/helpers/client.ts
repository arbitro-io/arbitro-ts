import { ArbitroClient } from '../../src'

export const BROKER_ADDR = '127.0.0.1:9898'

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
