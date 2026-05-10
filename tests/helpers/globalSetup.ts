import { ArbitroClient } from '../../src'
import { BROKER_ADDR } from './client'

/** Delete every stream on the broker so tests start from a clean state. */
export async function setup(): Promise<void> {
  const client = new ArbitroClient({ servers: [BROKER_ADDR], reconnect: { enabled: false } })
  await client.connect()
  try {
    const streams = await client.listStreams()
    for (const s of streams) {
      try { await client.deleteStream(s.name) } catch {}
    }
  } finally {
    await client.close()
  }
}
