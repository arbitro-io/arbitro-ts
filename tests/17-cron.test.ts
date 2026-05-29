import { describe, it, expect } from 'vitest'
import { ArbitroClient } from '../src'

const ADDR = process.env.ARBITRO_ADDR ?? '127.0.0.1:9898'

async function connect(): Promise<ArbitroClient> {
  return new ArbitroClient({ servers: [ADDR] }).connect()
}

describe('cron', () => {
  it('basic fire — handler receives at least 1 fire in 3s', async () => {
    const client = await connect()
    let fires = 0

    const cron = await client.cron('ts-basic')
      .every('* * * * * *')
      .run(async () => { fires++ })

    await new Promise(r => setTimeout(r, 3500))
    await cron.stop()

    expect(fires).toBeGreaterThanOrEqual(1)
    await client.close()
  })

  it('queue semantics — 5 workers, only 1 fires per tick', { timeout: 15000 }, async () => {
    const clients: ArbitroClient[] = []
    let totalFires = 0

    for (let i = 0; i < 5; i++) {
      const c = await connect()
      await c.cron('ts-shared-job')
        .every('* * * * * *')
        .run(async () => { totalFires++ })
      clients.push(c)
    }

    await new Promise(r => setTimeout(r, 5000))

    // ~5 fires in 5 seconds, NOT 25 (that would be fanout)
    expect(totalFires).toBeGreaterThanOrEqual(2)
    expect(totalFires).toBeLessThanOrEqual(8)

    for (const c of clients) await c.close()
  })
})
