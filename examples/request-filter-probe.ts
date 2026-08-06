/**
 * Why `client.request()` times out on an ordinary user stream.
 *
 * The reply mailbox subscribes to `_reply.<instanceId>.>` on the SAME stream
 * the request was sent to, and `msg.reply()` publishes there. A stream created
 * with `subjectFilter: "<name>.>"` does not admit that subject, so the reply is
 * never stored and the caller waits out its timeout.
 *
 * `service()` avoids this by owning a `_svc-<name>` stream, which is why the
 * service tests pass while `28-request-reply` does not.
 *
 * Run: two identical round trips, differing only in the stream's filter.
 */
import { ArbitroClient, JournalType } from '../src'

const ADDR = process.env.ARBITRO_ADDR ?? '127.0.0.1:9898'

function connect(): Promise<ArbitroClient> {
  const c = new ArbitroClient({ servers: [ADDR], reconnect: { enabled: false } })
  return c.connect().then(() => c)
}

let n = 0
const uniq = () => `rfp-${process.pid}-${Date.now().toString(36)}-${++n}`

async function trip(
  requester: ArbitroClient,
  responder: ArbitroClient,
  filter: (s: string) => string,
  label: string,
): Promise<boolean> {
  const s = uniq()
  await requester.createStream(s, {
    subjectFilter: filter(s),
    journal: { type: JournalType.Memory },
  })
  await requester.createConsumer(s, { name: s, filter: `${s}.>` })

  const sub = await responder.subscribe(s, (msg) => {
    // The reply can only be routed if the delivery carried a reply_to.
    const rt = msg.replyTo()
    console.log(`    responder saw: data=${JSON.stringify(msg.data().toString())} replyTo.len=${rt.length}` +
      (rt.length ? ` bytes=${rt.subarray(0, 5).toString('hex')} subj=${JSON.stringify(rt.subarray(5).toString())}` : ' (EMPTY)'))
    msg.reply(Buffer.from(msg.data().toString().toUpperCase()))
  })

  let ok = false
  try {
    const reply = await requester.request(s, `${s}.echo`, Buffer.from('hello'))
    ok = reply.toString() === 'HELLO'
    console.log(`  ${label}: reply=${JSON.stringify(reply.toString())} -> ${ok ? 'OK' : 'WRONG'}`)
  } catch (e) {
    console.log(`  ${label}: ${(e as Error).message}`)
  }
  sub.close()
  return ok
}

async function main() {
  const [requester, responder] = await Promise.all([connect(), connect()])

  console.log('\nstream filter "<name>.>"  (what 28-request-reply uses)')
  const narrow = await trip(requester, responder, (s) => `${s}.>`, 'narrow')

  console.log('\nstream filter ">"          (admits the _reply.* mailbox too)')
  const wide = await trip(requester, responder, () => '>', 'wide  ')

  console.log(
    `\nDIAGNOSIS: ${!narrow && wide
      ? 'confirmed — the stream subject filter is what blocks the reply'
      : `not confirmed (narrow=${narrow}, wide=${wide}) — cause is elsewhere`}`,
  )
  await Promise.all([requester.close(), responder.close()])
  process.exit(0)
}

main().catch((e) => { console.error(e); process.exit(2) })
