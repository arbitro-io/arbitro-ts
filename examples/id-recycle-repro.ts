/**
 * Consumer-id recycling repro.
 *
 * Consumer ids are pool-recycled by design: they index dense `Vec<Option<T>>`
 * slots server-side, so they must stay small. The open question is whether
 * some per-consumer state survives the free and starves the id's next owner.
 *
 * The three cases below are chosen to separate a broker defect from a client
 * one. All three recycle the id the same way; they differ only in WHO holds
 * the second subscription:
 *
 *   A — same client object          → client and broker state both reused
 *   B — a second, fresh connection  → broker state reused, client state new
 *   C — no delete at all (control)  → neither reused; must always pass
 *
 * B failing means the broker starved it. B passing while A fails means the
 * stale state is in the client's subscription table, not in the broker.
 */
import { ArbitroClient, JournalType } from '../src'

const ADDR = process.env.ARBITRO_ADDR ?? '127.0.0.1:9898'
const N = 3

function connect(): Promise<ArbitroClient> {
  const c = new ArbitroClient({ servers: [ADDR], reconnect: { enabled: false } })
  return c.connect().then(() => c)
}

let seq = 0
const name = () => `idr-${process.pid}-${Date.now().toString(36)}-${++seq}`

/** Create stream+consumer, subscribe, publish N, return what arrived. */
async function round(
  client: ArbitroClient,
  label: string,
): Promise<{ id: number | undefined; got: number[]; stream: string }> {
  const s = name()
  const stream = await client.stream(s).create({
    subjectFilter: `${s}.>`,
    journal: { type: JournalType.Memory },
  })
  const consumer = await stream.consumer({ name: s }).create()

  const got: number[] = []
  const sub = await consumer.subscribe((m) => { got.push(Number(m.data().toString())) })

  for (let i = 0; i < N; i++) await client.publishAck(s, `${s}.k`, Buffer.from(String(i)))

  // Poll rather than sleep a fixed amount: a pass should be fast, and only a
  // genuine starvation should pay the full wait.
  const deadline = Date.now() + 3000
  while (got.length < N && Date.now() < deadline) await new Promise((r) => setTimeout(r, 25))
  sub.close()

  const id = consumer.consumerId
  console.log(`  [${label}] stream=${s} consumerId=${id} got=[${got}]`)
  return { id, got, stream: s }
}

async function main() {
  const a = await connect()
  let failures = 0

  const check = (label: string, got: number[]) => {
    const ok = got.length === N
    console.log(`  => ${label}: ${ok ? 'OK' : 'STARVED'} (${got.length}/${N})`)
    if (!ok) failures++
    return ok
  }

  console.log('\n--- CONTROL: two consumers, nothing deleted between them ---')
  await round(a, 'c1')
  check('control', (await round(a, 'c2')).got)

  // Without an actual id reuse these cases prove nothing, so say so out loud
  // rather than letting a vacuous pass read as evidence.
  const recycled = (before: number | undefined, after: number | undefined) => {
    const yes = before != null && before === after
    console.log(`  id ${before} -> ${after} : ${yes ? 'RECYCLED' : 'not recycled (case is vacuous)'}`)
    return yes
  }

  console.log('\n--- CASE A: delete consumer+stream, resubscribe on the SAME client ---')
  const r1 = await round(a, 'a1')
  await a.deleteConsumer(r1.stream, r1.stream)
  await a.deleteStream(r1.stream)
  console.log('  (deleted)')
  const a2 = await round(a, 'a2')
  const aRecycled = recycled(r1.id, a2.id)
  check('same-client', a2.got)

  console.log('\n--- CASE B: delete consumer+stream, resubscribe on a FRESH connection ---')
  const r2 = await round(a, 'b1')
  await a.deleteConsumer(r2.stream, r2.stream)
  await a.deleteStream(r2.stream)
  console.log('  (deleted)')
  const b = await connect()
  const b2 = await round(b, 'b2')
  const bRecycled = recycled(r2.id, b2.id)
  check('fresh-client', b2.got)
  await b.close()

  if (!aRecycled && !bRecycled) {
    console.log('\nWARNING: no id was ever reused — this run did not exercise recycling at all.')
  }

  await a.close()
  console.log(`\nRESULT: ${failures === 0 ? 'no starvation observed' : `${failures} starved`}`)
  process.exit(failures === 0 ? 0 : 1)
}

main().catch((e) => { console.error(e); process.exit(2) })
