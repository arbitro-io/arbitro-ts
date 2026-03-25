// arbitro-ts bench — NATS bench style
//
// Usage:
//   tsx benches/throughput.ts [OPTIONS]
//
// Options:
//   --mode    pub|ack|pubsub|lat|credit|replay-noack|replay-ack|batch|pub-mt
//             |fire-and-forget|batch-publish|publish-and-deliver|fire-and-forget-mt
//             (default: pubsub)
//   --msgs    N                            (default: 100_000)
//   --size    N bytes                      (default: 128)
//   --addr    host:port                    (default: 127.0.0.1:9898)
//
// Examples:
//   tsx benches/throughput.ts --mode pub --msgs 1000000
//   tsx benches/throughput.ts --mode ack --msgs 10000
//   tsx benches/throughput.ts --mode pubsub --msgs 200000
//   tsx benches/throughput.ts --mode lat --msgs 5000
//   tsx benches/throughput.ts --mode credit --msgs 10000
//   tsx benches/throughput.ts --mode replay-noack --msgs 200000
//   tsx benches/throughput.ts --mode replay-ack --msgs 200000
//   tsx benches/throughput.ts --mode batch --msgs 100000
//   tsx benches/throughput.ts --mode pub-mt --msgs 100000

import { AckPolicy, ArbitroClient, DeliverPolicy, JournalType } from '../src'
import type { Subscription } from '../src'

// ── CLI ───────────────────────────────────────────────────────────────────

const argv  = process.argv.slice(2)
const str   = (flag: string, def: string) => { const i = argv.indexOf(flag); return i !== -1 && argv[i + 1] ? argv[i + 1]! : def }
const num   = (flag: string, def: number) => { const v = str(flag, ''); return v ? parseInt(v, 10) : def }

const MODE    = str('--mode', 'pubsub')
const CREDIT  = num('--credit', 0)   // 0 = no credit rule; >0 = max in-flight per pattern
const MSGS = num('--msgs', 100_000)
const SIZE = num('--size', 128)
const ADDR = str('--addr', '127.0.0.1:9898')
const MT_THREADS = num('--threads', 4)

// ── Helpers ───────────────────────────────────────────────────────────────

async function connect(): Promise<ArbitroClient> {
  for (let i = 0; i < 20; i++) {
    try { return await new ArbitroClient({ servers: [ADDR], timeout: 2_000 }).connect() }
    catch { await new Promise((r) => setTimeout(r, 150)) }
  }
  throw new Error(`could not connect to ${ADDR}`)
}

function fmtRate(msgs: number, secs: number): string {
  const mps  = msgs / secs
  const subjectLen = 'bench.event'.length
  const mbps = (msgs * (32 + 2 + subjectLen + SIZE)) / secs / 1_048_576
  return mps >= 1_000_000
    ? `${(mps / 1_000_000).toFixed(2)}M msg/s  ${mbps.toFixed(1)} MB/s`
    : `${(mps / 1_000).toFixed(1)}K msg/s  ${mbps.toFixed(1)} MB/s`
}

function header(mode: string): void {
  console.log('╔══════════════════════════════════════════════════════════════════╗')
  console.log('║         arbitro-ts bench  ─  NATS bench style                   ║')
  console.log('╚══════════════════════════════════════════════════════════════════╝')
  console.log()
  console.log(`  mode     : ${mode}`)
  console.log(`  msgs     : ${MSGS.toLocaleString()}`)
  console.log(`  size     : ${SIZE} bytes`)
  console.log(`  addr     : ${ADDR}`)
  console.log()
  console.log('  running…')
  console.log()
}

function uniqueBenchNames(prefix: string): { stream: string, subject: string, consumer: string } {
  const tag = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`
  return {
    stream:   tag,
    subject:  `${tag}.event`,
    consumer: `${tag}-workers`,
  }
}

async function cleanup(admin: ArbitroClient, consumer: string, stream: string): Promise<void> {
  try { await admin.deleteConsumer(consumer) } catch {}
  try { await admin.deleteStream(stream) } catch {}
  await admin.close()
}

// ── Mode: pub ─────────────────────────────────────────────────────────────

async function runPub(): Promise<void> {
  header('pub')
  const client = await connect()
  const { stream, subject, consumer } = uniqueBenchNames('bench-pub')
  await client.createStream(stream, { subjectFilter: `${stream}.>`, journal: { type: JournalType.Memory } })

  const payload = Buffer.alloc(SIZE, 0x42)
  const t0      = process.hrtime.bigint()

  for (let i = 0; i < MSGS; i++) client.publish(subject, payload)
  // Fence: publishAck uses SysKeepalive — server echoes RepOk only after
  // processing everything before it on this connection. Makes the timer honest.
  await client.publishAck(subject, Buffer.alloc(0))

  const elapsed = Number(process.hrtime.bigint() - t0) / 1e9
  console.log('  ┌── Pub results ───────────────────────────────────────────────┐')
  console.log(`  │  sent     : ${MSGS.toLocaleString()}  in ${elapsed.toFixed(2)}s`)
  console.log(`  │  rate     : ${fmtRate(MSGS, elapsed)}`)
  console.log('  └─────────────────────────────────────────────────────────────┘')

  await cleanup(client, consumer, stream)
}

// ── Mode: pubsub ──────────────────────────────────────────────────────────

async function runPubSub(): Promise<void> {
  header('pubsub')

  // Two separate connections: sub receives delivery, pub sends messages.
  // Mixing pub + delivery on one TCP connection creates backpressure — separate them.
  const admin = await connect()
  const subClient = await connect()
  const pubClient = await connect()
  const { stream, subject, consumer: consumerGroup } = uniqueBenchNames('bench-pubsub')

  await admin.createStream(stream, { subjectFilter: `${stream}.>`, journal: { type: JournalType.Memory } })
  await admin.createConsumer(stream, { name: consumerGroup, filter: `${stream}.>`, deliverPolicy: DeliverPolicy.New })
  const streamCtx = subClient.stream(stream)
  const consumer = streamCtx.consumer({ name: consumerGroup, filter: `${stream}.>`, deliverPolicy: DeliverPolicy.New })

  let received = 0
  let subStart = 0n
  let subEnd   = 0n

  const subscription: Subscription = await consumer.subscribe((msg) => {
    if (received === 0) subStart = process.hrtime.bigint()
    received++
    msg.ack()
    if (received >= MSGS) subEnd = process.hrtime.bigint()
  })

  const payload  = Buffer.alloc(SIZE, 0x42)
  const pubStart = process.hrtime.bigint()
  for (let i = 0; i < MSGS; i++) pubClient.publish(subject, payload)
  const pubElapsed = Number(process.hrtime.bigint() - pubStart) / 1e9

  // Wait for all messages (max 60s)
  const deadline = Date.now() + 60_000
  while (received < MSGS && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 50))
  }

  const subElapsed = Number(subEnd - subStart) / 1e9

  console.log('  ┌── PubSub results ────────────────────────────────────────────┐')
  console.log('  ├── Pub ──────────────────────────────────────────────────────')
  console.log(`  │  sent     : ${MSGS.toLocaleString()}  in ${pubElapsed.toFixed(2)}s`)
  console.log(`  │  rate     : ${fmtRate(MSGS, pubElapsed)}`)
  console.log('  ├── Sub ──────────────────────────────────────────────────────')
  console.log(`  │  received : ${received.toLocaleString()}  in ${subElapsed > 0 ? subElapsed.toFixed(2) : '—'}s`)
  if (subElapsed > 0) console.log(`  │  rate     : ${fmtRate(received, subElapsed)}`)
  if (received < MSGS) console.log(`  │  WARNING  : only ${received} / ${MSGS} received (timeout)`)
  console.log('  └─────────────────────────────────────────────────────────────┘')

  subscription.close()
  await Promise.all([subClient.close(), pubClient.close()])
  await cleanup(admin, consumerGroup, stream)
}

// ── Mode: ack ─────────────────────────────────────────────────────────────
// Measures server-confirmed publish throughput.
// Uses publishAck() as a fence every ACK_BATCH messages so all frames are
// durably journalled before the timer stops.

const ACK_BATCH = 256

async function runAck(): Promise<void> {
  header('ack')
  const client = await connect()
  const { stream, subject, consumer } = uniqueBenchNames('bench-ack')
  await client.createStream(stream, { subjectFilter: `${stream}.>`, journal: { type: JournalType.Memory } })

  const payload = Buffer.alloc(SIZE, 0x42)
  const t0      = process.hrtime.bigint()

  for (let i = 0; i < MSGS; i++) {
    client.publish(subject, payload)
    // Fence every ACK_BATCH messages — server echoes RepOk only after
    // processing everything queued before it on this connection.
    if ((i + 1) % ACK_BATCH === 0) await client.publishAck(subject, Buffer.alloc(0))
  }
  await client.publishAck(subject, Buffer.alloc(0))

  const elapsed = Number(process.hrtime.bigint() - t0) / 1e9
  console.log('  ┌── Ack results ───────────────────────────────────────────────┐')
  console.log(`  │  sent (confirmed) : ${MSGS.toLocaleString()}  in ${elapsed.toFixed(2)}s`)
  console.log(`  │  rate             : ${fmtRate(MSGS, elapsed)}`)
  console.log(`  │  batch            : every ${ACK_BATCH} publishes`)
  console.log('  └─────────────────────────────────────────────────────────────┘')

  await cleanup(client, consumer, stream)
}

// ── Mode: batch ───────────────────────────────────────────────────────────

async function runBatch(): Promise<void> {
  header('batch')
  const client = await connect()
  const { stream, subject, consumer } = uniqueBenchNames('bench-batch')
  await client.createStream(stream, { subjectFilter: `${stream}.>`, journal: { type: JournalType.Memory } })

  const payload = Buffer.alloc(SIZE, 0x42)
  const batchSizes = [8, 32, 128]

  console.log('  ┌── Batch results ─────────────────────────────────────────────┐')
  for (const batchSize of batchSizes) {
    const iterations = Math.max(1, Math.floor(MSGS / batchSize))
    const messages: [string, Buffer][] = Array.from({ length: batchSize }, () => [subject, payload])
    const t0 = process.hrtime.bigint()
    for (let i = 0; i < iterations; i++) client.publishBatch(messages)
    await client.publishAck(subject, Buffer.alloc(0))
    const sent = iterations * batchSize
    const elapsed = Number(process.hrtime.bigint() - t0) / 1e9
    console.log(`  │  batch=${batchSize.toString().padEnd(3)} sent=${sent.toString().padStart(7)}  rate=${fmtRate(sent, elapsed)}`)
  }
  console.log('  └─────────────────────────────────────────────────────────────┘')

  await cleanup(client, consumer, stream)
}

// ── Mode: pub-mt ──────────────────────────────────────────────────────────

async function runPubMt(): Promise<void> {
  header('pub-mt')
  const admin = await connect()
  const { stream, subject, consumer } = uniqueBenchNames('bench-pub-mt')
  await admin.createStream(stream, { subjectFilter: `${stream}.>`, journal: { type: JournalType.Memory } })

  const threadCounts = [1, 2, 4]
  const payload = Buffer.alloc(SIZE, 0x42)

  console.log('  ┌── Multi-thread publish results ─────────────────────────────┐')
  for (const n of threadCounts) {
    const clients = await Promise.all(Array.from({ length: n }, () => connect()))
    const perClient = Math.max(1, Math.floor(MSGS / n))
    const t0 = process.hrtime.bigint()
    await Promise.all(clients.map(async (client) => {
      for (let i = 0; i < perClient; i++) client.publish(subject, payload)
      await client.publishAck(subject, Buffer.alloc(0))
      await client.close()
    }))
    const sent = perClient * n
    const elapsed = Number(process.hrtime.bigint() - t0) / 1e9
    console.log(`  │  threads=${n} sent=${sent.toString().padStart(7)}  rate=${fmtRate(sent, elapsed)}`)
  }
  console.log('  └─────────────────────────────────────────────────────────────┘')

  await cleanup(admin, consumer, stream)
}

// ── Mode: credit ───────────────────────────────────────────────────────────
// Validates that credit_rules throttle per-subject delivery.
// Runs three scenarios back-to-back on the same broker:
//   1. no credit rule  — baseline delivery rate
//   2. credit=500      — large limit, minimal overhead
//   3. credit=5        — tight limit, visible back-pressure

async function runCreditScenario(
  label: string,
  maxCredit: number | null,
): Promise<void> {
  // Each scenario uses a unique tag for stream, consumer, AND subject to avoid
  // the broker's subject-routing cache being stale across scenarios.
  const tag      = `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`
  const stream   = `credit-${tag}`
  const consumer = `credit-workers-${tag}`
  // Unique subject per scenario — avoids routing cache collision between runs.
  // Pattern '*.msg' still matches '${tag}.msg' (single-token wildcard).
  const subject  = `${tag}.msg`

  const admin  = await connect()
  await admin.createStream(stream, { subjectFilter: subject, journal: { type: JournalType.Memory } })

  const consumerCfg = maxCredit !== null
    ? { name: consumer, filter: subject, maxAckPending: 20_000, deliverPolicy: DeliverPolicy.All,
        creditRules: [{ pattern: '*.msg', limit: maxCredit }] }
    : { name: consumer, filter: subject, maxAckPending: 20_000, deliverPolicy: DeliverPolicy.All }

  await admin.createConsumer(stream, consumerCfg)

  let received = 0
  const subClient = await connect()
  const sub = await subClient.subscribe(consumer, (msg) => { msg.ack(); received++ })

  const pubClient = await connect()
  const payload   = Buffer.alloc(SIZE, 0x42)
  const t0        = process.hrtime.bigint()

  for (let i = 0; i < MSGS; i++) pubClient.publish(subject, payload)

  const deadline = Date.now() + 30_000
  while (received < MSGS && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 50))
  }

  const elapsed    = Number(process.hrtime.bigint() - t0) / 1e9
  const creditStr  = maxCredit !== null ? `limit=${maxCredit}` : 'none    '
  console.log(`  credit_rule: ${creditStr.toString().padEnd(10)}  → ${fmtRate(received, elapsed)}  (${received}/${MSGS})`)

  sub.close()
  await Promise.all([subClient.close(), pubClient.close()])
  admin.deleteConsumer(consumer)
  admin.deleteStream(stream)
  await admin.close()
}

async function runCredit(): Promise<void> {
  header('credit')
  console.log('  ── Throughput vs credit_rule limit ─────────────────────────────')
  await runCreditScenario('no rule  ', null)
  await runCreditScenario('limit=500', 500)
  await runCreditScenario('limit=5  ', 5)
  console.log()
  console.log('  HOL isolation: run --mode pubsub to verify fast subject is')
  console.log('  unaffected when slow subject exhausts its credit slot.')
}

// ── Mode: lat ─────────────────────────────────────────────────────────────

async function runLat(): Promise<void> {
  header('lat')

  const admin = await connect()
  const subClient = await connect()
  const pubClient = await connect()
  const { stream, subject, consumer: consumerGroup } = uniqueBenchNames('bench-lat')

  await admin.createStream(stream, { subjectFilter: `${stream}.>`, journal: { type: JournalType.Memory } })
  await admin.createConsumer(stream, { name: consumerGroup, filter: `${stream}.>`, maxAckPending: 1, deliverPolicy: DeliverPolicy.New })
  const streamCtx = subClient.stream(stream)
  const consumer = streamCtx.consumer({ name: consumerGroup, filter: `${stream}.>`, maxAckPending: 1, deliverPolicy: DeliverPolicy.New })

  const samples: number[] = []
  let resolve: (() => void) | null = null

  const subscription: Subscription = await consumer.subscribe((msg) => {
    msg.ack()
    resolve?.()
  })

  const payload = Buffer.alloc(SIZE, 0x42)
  for (let i = 0; i < MSGS; i++) {
    const t0 = process.hrtime.bigint()
    await new Promise<void>((res) => { resolve = res; pubClient.publish(subject, payload) })
    samples.push(Number(process.hrtime.bigint() - t0) / 1_000)  // µs
  }

  samples.sort((a, b) => a - b)
  const p = (q: number) => samples[Math.floor(samples.length * q)]!.toFixed(1)

  console.log('  ┌── Latency (pub → deliver → ack) ─────────────────────────────┐')
  console.log(`  │  P50    ${p(0.50).padStart(9)} µs`)
  console.log(`  │  P90    ${p(0.90).padStart(9)} µs`)
  console.log(`  │  P99    ${p(0.99).padStart(9)} µs`)
  console.log(`  │  P99.9  ${p(0.999).padStart(9)} µs`)
  console.log(`  │  Max    ${p(0.9999).padStart(9)} µs`)
  console.log('  └─────────────────────────────────────────────────────────────┘')

  subscription.close()
  await Promise.all([subClient.close(), pubClient.close()])
  await cleanup(admin, consumerGroup, stream)
}

// ── Mode: replay-noack / replay-ack ───────────────────────────────────────
// Preload the stream first, then subscribe and measure pure replay/drain throughput.

async function runReplay(ackMode: boolean): Promise<void> {
  header(ackMode ? 'replay-ack' : 'replay-noack')

  const admin = await connect()
  const pubClient = await connect()
  const subClient = await connect()
  const { stream, subject, consumer } = uniqueBenchNames(ackMode ? 'bench-replay-ack' : 'bench-replay-noack')

  await admin.createStream(stream, { subjectFilter: `${stream}.>`, journal: { type: JournalType.Memory } })
  await admin.createConsumer(stream, {
    name: consumer,
    filter: `${stream}.>`,
    deliverPolicy: DeliverPolicy.All,
    ackPolicy: ackMode ? AckPolicy.Explicit : AckPolicy.None,
    maxAckPending: ackMode ? Math.max(50_000, Math.floor(MSGS / 5)) : undefined,
  })

  const payload = Buffer.alloc(SIZE, 0x42)
  const preloadStart = process.hrtime.bigint()
  for (let i = 0; i < MSGS; i++) pubClient.publish(subject, payload)
  await pubClient.publishAck(subject, Buffer.alloc(0))
  const preloadElapsed = Number(process.hrtime.bigint() - preloadStart) / 1e9

  let received = 0
  let subStart = 0n
  let subEnd   = 0n

  const subscription: Subscription = await subClient.subscribe(consumer, (msg) => {
    if (received >= MSGS) return
    if (received === 0) subStart = process.hrtime.bigint()
    received++
    if (ackMode) msg.ack()
    if (received >= MSGS) subEnd = process.hrtime.bigint()
  })

  const deadline = Date.now() + 60_000
  while (received < MSGS && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 50))
  }

  const replayElapsed = subEnd > 0n ? Number(subEnd - subStart) / 1e9 : 0

  console.log(`  ┌── Replay ${ackMode ? 'ACK' : 'NoAck'} results ──────────────────────────────────────┐`)
  console.log(`  │  preloaded : ${MSGS.toLocaleString()}  in ${preloadElapsed.toFixed(2)}s`)
  console.log(`  │  preload   : ${fmtRate(MSGS, preloadElapsed)}`)
  console.log(`  │  replayed  : ${received.toLocaleString()}  in ${replayElapsed > 0 ? replayElapsed.toFixed(2) : '—'}s`)
  if (replayElapsed > 0) console.log(`  │  replay    : ${fmtRate(received, replayElapsed)}`)
  if (received < MSGS) console.log(`  │  WARNING   : only ${received} / ${MSGS} received (timeout)`)
  console.log('  └─────────────────────────────────────────────────────────────┘')

  subscription.close()
  await Promise.all([subClient.close(), pubClient.close()])
  await cleanup(admin, consumer, stream)
}

// ── Main ──────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  switch (MODE) {
    case 'pub':    await runPub();    break
    case 'fire-and-forget': await runPub(); break
    case 'ack':    await runAck();    break
    case 'pubsub': await runPubSub(); break
    case 'publish-and-deliver': await runLat(); break
    case 'lat':    await runLat();    break
    case 'credit': await runCredit(); break
    case 'batch':
    case 'batch-publish':
      await runBatch(); break
    case 'pub-mt':
    case 'fire-and-forget-mt':
      await runPubMt(); break
    case 'replay-noack': await runReplay(false); break
    case 'replay-ack':   await runReplay(true); break
    default: console.error(`unknown mode: ${MODE}`); process.exit(1)
  }
}

main().catch((e) => { console.error(e); process.exit(1) })
