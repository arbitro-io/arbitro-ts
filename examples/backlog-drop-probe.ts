/**
 * Where does the backlog message go?
 *
 * The Rust client never loses it against the same broker, so the loss is on
 * this side. `Connection.onFrame` drops a Deliver whose consumer id is not in
 * `routes` and only logs a warning, which would look exactly like this. This
 * probe attaches a logger and counts those warnings per round.
 *
 * Run: npx tsx examples/backlog-drop-probe.ts
 */
import { ArbitroClient, JournalType } from '../src'

const ADDR = process.env.ARBITRO_ADDR ?? '127.0.0.1:9898'
const ROUNDS = 10

const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

/** Counts warnings the client emits, keyed by message text. */
const warnings: string[] = []
const logger = {
  trace: () => {},
  debug: () => {},
  info: () => {},
  warn: (a: unknown, b?: unknown) => {
    warnings.push(typeof b === 'string' ? b : String(a))
  },
  error: (a: unknown, b?: unknown) => {
    warnings.push(`ERROR ${typeof b === 'string' ? b : String(a)}`)
  },
  fatal: () => {},
}

async function main(): Promise<void> {
  const client = new ArbitroClient({
    servers: [ADDR],
    reconnect: { enabled: false },
    logger: logger as never,
  })
  await client.connect()

  let lost = 0
  let lostWithWarning = 0

  for (let round = 0; round < ROUNDS; round++) {
    const name = `drop-${process.pid}-${Date.now().toString(36)}-${round}`
    const stream = await client.stream(name).create({
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })

    // Only the publish-first ordering is interesting — the live push path
    // never loses anything.
    await stream.publish(`${name}.a`, Buffer.from('hello'))

    const before = warnings.length
    const got: string[] = []
    const consumer = stream.consumer({ name, filter: `${name}.>` })
    await consumer.create()
    const sub = await consumer.subscribe((msg) => {
      got.push(Buffer.from(msg.data()).toString())
      msg.ack()
    })

    const deadline = Date.now() + 3000
    while (got.length === 0 && Date.now() < deadline) await wait(25)

    const fresh = warnings.slice(before)
    const unknownConsumer = fresh.filter((w) => w.includes('unknown consumer')).length
    if (got.length === 0) {
      lost++
      if (unknownConsumer > 0) lostWithWarning++
    }
    console.log(
      `round ${round}: received=${got.length} ` +
        `unknown-consumer-drops=${unknownConsumer} ` +
        `otherWarnings=${JSON.stringify(fresh.filter((w) => !w.includes('unknown consumer')))}`,
    )

    await sub.close()
    await client.deleteConsumer(name, name).catch(() => {})
    await client.deleteStream(name).catch(() => {})
  }

  console.log('')
  console.log(`rounds that lost the message : ${lost}/${ROUNDS}`)
  console.log(`...of those, with a drop warn: ${lostWithWarning}/${lost}`)
  console.log(
    lost === 0
      ? 'nothing lost this run — re-run, it is intermittent'
      : lostWithWarning === lost
        ? 'CONFIRMED: the client received the message and threw it away'
        : 'NOT the drop path — the frame never reached the client',
  )

  await client.close()
}

main().catch((e) => {
  console.error('probe failed to run:', e)
  process.exit(2)
})
