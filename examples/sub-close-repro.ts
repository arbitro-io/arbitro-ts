/**
 * Repro probe: does a subscribe → close → subscribe cycle on ONE client keep
 * delivering?
 *
 * 19-delete-message showed exactly one of its four tests passing regardless of
 * which ran first, and every test in it now subscribes. This isolates that
 * shape: N rounds, each creating its own stream + consumer, subscribing,
 * waiting for one delivery, then closing.
 *
 * Run: npx tsx examples/sub-close-repro.ts
 */
import { ArbitroClient, JournalType } from '../src'

const ADDR = process.env.ARBITRO_ADDR ?? '127.0.0.1:9898'
const ROUNDS = 10

function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

async function main(): Promise<void> {
  const client = new ArbitroClient({ servers: [ADDR], reconnect: { enabled: false } })
  await client.connect()

  const results: string[] = []

  for (let round = 0; round < ROUNDS; round++) {
    const name = `repro-${process.pid}-${Date.now().toString(36)}-${round}`

    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    // Alternate the two orderings. Publish-then-subscribe makes the broker
    // replay from the store for a consumer that did not exist at write time;
    // subscribe-then-publish is the live push path. If only the first loses
    // messages, the catch-up read is the suspect, not delivery itself.
    const publishFirst = round % 2 === 0
    if (publishFirst) await stream.publish(`${name}.a`, Buffer.from('hello'))

    const got: string[] = []
    const consumer = stream.consumer({ name, filter: `${name}.>` })
    await consumer.create()
    const sub = await consumer.subscribe((msg) => {
      got.push(Buffer.from(msg.data()).toString())
      msg.ack()
    })

    if (!publishFirst) await stream.publish(`${name}.a`, Buffer.from('hello'))

    const deadline = Date.now() + 3000
    while (got.length === 0 && Date.now() < deadline) await wait(25)

    const order = publishFirst ? 'publish-then-subscribe' : 'subscribe-then-publish'
    results.push(`${order} consumerId=${sub.id()} received=${got.length}`)
    console.log(`round ${round}: ${results[results.length - 1]}`)

    await sub.close()
    // CLEANUP=0 keeps the consumer alive, so the broker cannot recycle its ID
    // into the next round. That is the shape 19-delete-message actually has —
    // it only tears down in afterAll — so running both ways separates "IDs get
    // recycled" from "subscribe/close cycling is broken on its own".
    if (process.env.CLEANUP !== '0') {
      await client.deleteConsumer(name, name).catch(() => {})
      await client.deleteStream(name).catch(() => {})
    }
  }

  const delivered = results.filter((r) => r.endsWith('received=1')).length
  const pubFirst = results.filter((r) => r.startsWith('publish-then'))
  const subFirst = results.filter((r) => r.startsWith('subscribe-then'))
  console.log('')
  console.log(`publish-then-subscribe: ${pubFirst.filter(r=>r.endsWith('received=1')).length}/${pubFirst.length}`)
  console.log(`subscribe-then-publish: ${subFirst.filter(r=>r.endsWith('received=1')).length}/${subFirst.length}`)
  console.log('')
  console.log(`DELIVERED ${delivered}/${ROUNDS} rounds`)
  console.log(
    delivered === ROUNDS
      ? 'subscribe/close cycling is FINE — look elsewhere'
      : 'REPRODUCED — deliveries stop after the first subscribe/close cycle',
  )

  await client.close()
  process.exit(delivered === ROUNDS ? 0 : 1)
}

main().catch((e) => {
  console.error('repro failed to run:', e)
  process.exit(2)
})
