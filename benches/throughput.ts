// arbitro-ts bench — NATS bench style
//
// Usage:
//   tsx benches/throughput.ts [OPTIONS]
//
// Options:
//   --mode    pub|ack|pubsub|lat|credit|replay-noack|replay-ack|batch|pub-mt|perf
//             (default: pubsub)
//   --msgs    N                            (default: 100_000)
//   --size    N bytes                      (default: 128)
//   --addr    host:port                    (default: 127.0.0.1:9898)
//   --seconds N                            (default: 10)
//   --rate    N msg/s                      (default: 10_000)
//   --sample-ms N                          (default: 500)
//   --container NAME                       (default: arbitro-broker)

import { AckPolicy, ArbitroClient, DeliverPolicy, JournalType } from '../src'
import type { Subscription, Stream } from '../src'
import { execFile } from 'node:child_process'

// ── CLI ───────────────────────────────────────────────────────────────────

const argv  = process.argv.slice(2)
const str   = (flag: string, def: string) => { const i = argv.indexOf(flag); return i !== -1 && argv[i + 1] ? argv[i + 1]! : def }
const num   = (flag: string, def: number) => { const v = str(flag, ''); return v ? parseInt(v, 10) : def }

const MODE    = str('--mode', 'pubsub')
const CREDIT  = num('--credit', 0)
const MSGS = num('--msgs', 100_000)
const SIZE = num('--size', 128)
const ADDR = str('--addr', '127.0.0.1:9898')
const MT_THREADS = num('--threads', 4)
const SECONDS = num('--seconds', 10)
const RATE = num('--rate', 10_000)
const SAMPLE_MS = num('--sample-ms', 500)
const CONTAINER = str('--container', 'arbitro-broker')

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

async function createBenchStream(client: ArbitroClient, name: string): Promise<Stream> {
  return client.stream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } }).create()
}

async function cleanup(admin: ArbitroClient, consumer: string, stream: string): Promise<void> {
  try { await admin.deleteConsumer(stream, consumer) } catch {}
  try { await admin.deleteStream(stream) } catch {}
  await admin.close()
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function percentile(samples: number[], q: number): string {
  if (samples.length === 0) return '—'
  const idx = Math.min(samples.length - 1, Math.floor(samples.length * q))
  return samples[idx]!.toFixed(1)
}

function avg(samples: number[]): number {
  return samples.length === 0 ? 0 : samples.reduce((a, b) => a + b, 0) / samples.length
}

function parseMemToMiB(raw: string): number {
  const match = raw.trim().match(/^([0-9.]+)\s*([KMG]i?B|B)$/i)
  if (!match) return 0
  const value = Number(match[1])
  const unit = match[2].toUpperCase()
  switch (unit) {
    case 'B': return value / (1024 * 1024)
    case 'KIB': case 'KB': return value / 1024
    case 'MIB': case 'MB': return value
    case 'GIB': case 'GB': return value * 1024
    default: return 0
  }
}

type DockerSample = { cpuPct: number; memMiB: number }

function readDockerSample(container: string, onSample: (sample: DockerSample | null) => void): void {
  execFile(
    'docker',
    ['stats', '--no-stream', '--format', '{{.CPUPerc}}|{{.MemUsage}}', container],
    { encoding: 'utf8' },
    (err, stdout) => {
      if (err) return onSample(null)
      const out = stdout.trim()
      if (!out) return onSample(null)
      const [cpuRaw, memRaw] = out.split('|')
      if (!cpuRaw || !memRaw) return onSample(null)
      const memUsed = memRaw.split('/')[0]?.trim() ?? ''
      onSample({
        cpuPct: Number(cpuRaw.replace('%', '').trim()) || 0,
        memMiB: parseMemToMiB(memUsed),
      })
    },
  )
}

// ── Mode: pub ─────────────────────────────────────────────────────────────

async function runPub(): Promise<void> {
  header('pub')
  const client = await connect()
  const { stream, subject, consumer } = uniqueBenchNames('bench-pub')
  const s = await createBenchStream(client, stream)

  const payload = Buffer.alloc(SIZE, 0x42)
  const t0      = process.hrtime.bigint()

  for (let i = 0; i < MSGS; i++) s.publish(subject, payload)
  await s.publishAck(subject, Buffer.alloc(0))

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

  const admin = await connect()
  const subClient = await connect()
  const pubClient = await connect()
  const { stream, subject, consumer: consumerGroup } = uniqueBenchNames('bench-pubsub')

  await createBenchStream(admin, stream)
  const streamCtx = subClient.stream(stream)
  const consumer = streamCtx.consumer({ name: consumerGroup, filter: `${stream}.>`, deliverPolicy: DeliverPolicy.New, maxAckPending: Math.max(50_000, MSGS) })

  let received = 0
  let subStart = 0n
  let subEnd   = 0n

  const subscription: Subscription = await consumer.subscribe((msg) => {
    if (received === 0) subStart = process.hrtime.bigint()
    received++
    msg.ack()
    if (received >= MSGS) subEnd = process.hrtime.bigint()
  })

  await pubClient.resolveStream(stream)
  const pubStream = pubClient.stream(stream)
  const payload   = Buffer.alloc(SIZE, 0x42)
  const pubStart  = process.hrtime.bigint()
  for (let i = 0; i < MSGS; i++) pubStream.publish(subject, payload)
  const pubElapsed = Number(process.hrtime.bigint() - pubStart) / 1e9

  const deadline = Date.now() + 60_000
  while (received < MSGS && Date.now() < deadline) await sleep(50)

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

const ACK_BATCH = 256

async function runAck(): Promise<void> {
  header('ack')
  const client = await connect()
  const { stream, subject, consumer } = uniqueBenchNames('bench-ack')
  const s = await createBenchStream(client, stream)

  const payload = Buffer.alloc(SIZE, 0x42)
  const t0      = process.hrtime.bigint()

  for (let i = 0; i < MSGS; i++) {
    s.publish(subject, payload)
    if ((i + 1) % ACK_BATCH === 0) await s.publishAck(subject, Buffer.alloc(0))
  }
  await s.publishAck(subject, Buffer.alloc(0))

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
  const s = await createBenchStream(client, stream)

  const payload = Buffer.alloc(SIZE, 0x42)
  const batchSizes = [8, 32, 128]

  console.log('  ┌── Batch results ─────────────────────────────────────────────┐')
  for (const batchSize of batchSizes) {
    const iterations = Math.max(1, Math.floor(MSGS / batchSize))
    const messages: [string, Buffer][] = Array.from({ length: batchSize }, () => [subject, payload])
    const t0 = process.hrtime.bigint()
    for (let i = 0; i < iterations; i++) s.publishBatch(messages)
    await s.publishAck(subject, Buffer.alloc(0))
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
  await createBenchStream(admin, stream)

  const payload = Buffer.alloc(SIZE, 0x42)

  console.log('  ┌── Multi-thread publish results ─────────────────────────────┐')
  for (const n of [1, 2, 4]) {
    const clients = await Promise.all(Array.from({ length: n }, () => connect()))
    await Promise.all(clients.map((c) => c.resolveStream(stream)))
    const perClient = Math.max(1, Math.floor(MSGS / n))
    const t0 = process.hrtime.bigint()
    await Promise.all(clients.map(async (client) => {
      const s = client.stream(stream)
      for (let i = 0; i < perClient; i++) s.publish(subject, payload)
      await s.publishAck(subject, Buffer.alloc(0))
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

async function runCreditScenario(label: string, maxCredit: number | null): Promise<void> {
  const tag      = `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`
  const stream   = `credit-${tag}`
  const consumer = `credit-workers-${tag}`
  const subject  = `${tag}.msg`

  const admin = await connect()
  await admin.createStream(stream, { subjectFilter: subject, journal: { type: JournalType.Memory } })

  const consumerCfg = maxCredit !== null
    ? { name: consumer, filter: subject, maxAckPending: 20_000, deliverPolicy: DeliverPolicy.All,
        maxSubjectInflight: maxCredit }
    : { name: consumer, filter: subject, maxAckPending: 20_000, deliverPolicy: DeliverPolicy.All }

  let received = 0
  const subClient = await connect()
  const sub = await subClient.subscribe(stream, consumerCfg, (msg) => { msg.ack(); received++ })

  const pubClient = await connect()
  await pubClient.resolveStream(stream)
  const pubStream = pubClient.stream(stream)
  const payload   = Buffer.alloc(SIZE, 0x42)
  const t0        = process.hrtime.bigint()

  for (let i = 0; i < MSGS; i++) pubStream.publish(subject, payload)

  const deadline = Date.now() + 30_000
  while (received < MSGS && Date.now() < deadline) await sleep(50)

  const elapsed    = Number(process.hrtime.bigint() - t0) / 1e9
  const creditStr  = maxCredit !== null ? `limit=${maxCredit}` : 'none    '
  console.log(`  credit_rule: ${creditStr.toString().padEnd(10)}  → ${fmtRate(received, elapsed)}  (${received}/${MSGS})`)

  sub.close()
  await Promise.all([subClient.close(), pubClient.close()])
  await cleanup(admin, consumer, stream)
}

async function runCredit(): Promise<void> {
  header('credit')
  console.log('  ── Throughput vs credit_rule limit ─────────────────────────────')
  await runCreditScenario('no rule  ', null)
  await runCreditScenario('limit=500', 500)
  await runCreditScenario('limit=5  ', 5)
  console.log()
}

// ── Mode: lat ─────────────────────────────────────────────────────────────

async function runLat(): Promise<void> {
  header('lat')

  const admin = await connect()
  const subClient = await connect()
  const pubClient = await connect()
  const { stream, subject, consumer: consumerGroup } = uniqueBenchNames('bench-lat')

  await createBenchStream(admin, stream)
  const consumer = subClient.stream(stream).consumer({ name: consumerGroup, filter: `${stream}.>`, maxAckPending: 1, deliverPolicy: DeliverPolicy.New })

  const samples: number[] = []
  let resolve: (() => void) | null = null

  const subscription: Subscription = await consumer.subscribe((msg) => {
    msg.ack()
    resolve?.()
  })

  await pubClient.resolveStream(stream)
  const pubStream = pubClient.stream(stream)
  const payload   = Buffer.alloc(SIZE, 0x42)
  for (let i = 0; i < MSGS; i++) {
    const t0 = process.hrtime.bigint()
    await new Promise<void>((res) => { resolve = res; pubStream.publish(subject, payload) })
    samples.push(Number(process.hrtime.bigint() - t0) / 1_000)
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

async function runReplay(ackMode: boolean): Promise<void> {
  header(ackMode ? 'replay-ack' : 'replay-noack')

  const admin = await connect()
  const pubClient = await connect()
  const subClient = await connect()
  const { stream, subject, consumer } = uniqueBenchNames(ackMode ? 'bench-replay-ack' : 'bench-replay-noack')

  await createBenchStream(admin, stream)
  const replayCfg = {
    name: consumer,
    filter: `${stream}.>`,
    deliverPolicy: DeliverPolicy.All,
    ackPolicy: ackMode ? AckPolicy.Explicit : AckPolicy.None,
    maxAckPending: ackMode ? Math.max(50_000, Math.floor(MSGS / 5)) : undefined,
  }
  await pubClient.resolveStream(stream)
  const pubStream = pubClient.stream(stream)
  const payload = Buffer.alloc(SIZE, 0x42)
  const preloadStart = process.hrtime.bigint()
  for (let i = 0; i < MSGS; i++) pubStream.publish(subject, payload)
  await pubStream.publishAck(subject, Buffer.alloc(0))
  const preloadElapsed = Number(process.hrtime.bigint() - preloadStart) / 1e9

  let received = 0
  let subStart = 0n
  let subEnd   = 0n

  const subscription: Subscription = await subClient.subscribe(stream, replayCfg, (msg) => {
    if (received >= MSGS) return
    if (received === 0) subStart = process.hrtime.bigint()
    received++
    if (ackMode) msg.ack()
    if (received >= MSGS) subEnd = process.hrtime.bigint()
  })

  const deadline = Date.now() + 60_000
  while (received < MSGS && Date.now() < deadline) await sleep(50)

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

// ── Mode: perf / performance ───────────────────────────────────────────────

async function runPerf(): Promise<void> {
  header('perf')

  const admin = await connect()
  const subClient = await connect()
  const pubClient = await connect()
  const { stream, subject, consumer } = uniqueBenchNames('bench-perf')

  await createBenchStream(admin, stream)
  const perfCfg = {
    name: consumer,
    filter: `${stream}.>`,
    deliverPolicy: DeliverPolicy.New,
    maxAckPending: Math.max(50_000, RATE * Math.max(1, SECONDS)),
  }
  const latenciesUs: number[] = []
  const nodeCpuSamples: number[] = []
  const nodeRssSamplesMiB: number[] = []
  const nodeHeapSamplesMiB: number[] = []
  const brokerCpuSamples: number[] = []
  const brokerMemSamplesMiB: number[] = []
  let received = 0
  let firstRecv = 0n
  let lastRecv = 0n

  const subscription: Subscription = await subClient.subscribe(stream, perfCfg, (msg) => {
    const now = process.hrtime.bigint()
    if (received === 0) firstRecv = now
    lastRecv = now
    received++
    const data = msg.data()
    if (data.length >= 8 && latenciesUs.length < 200_000) {
      const sentNs = data.readBigUInt64LE(0)
      latenciesUs.push(Number(now - sentNs) / 1_000)
    }
    msg.ack()
  })

  await pubClient.resolveStream(stream)
  const pubStream = pubClient.stream(stream)
  const payload = Buffer.alloc(Math.max(SIZE, 8), 0x42)
  const start = process.hrtime.bigint()
  const stopAt = start + BigInt(SECONDS) * 1_000_000_000n
  let sent = 0
  let sampleTimer: ReturnType<typeof setInterval> | undefined
  let lastCpu = process.cpuUsage()
  let lastCpuAt = process.hrtime.bigint()
  let dockerSampleBusy = false

  sampleTimer = setInterval(() => {
    const now = process.hrtime.bigint()
    const cpu = process.cpuUsage()
    const wallUs = Number(now - lastCpuAt) / 1_000
    if (wallUs > 0) {
      const deltaCpuUs = (cpu.user - lastCpu.user) + (cpu.system - lastCpu.system)
      nodeCpuSamples.push((deltaCpuUs / wallUs) * 100)
    }
    lastCpu = cpu
    lastCpuAt = now

    const mem = process.memoryUsage()
    nodeRssSamplesMiB.push(mem.rss / 1024 / 1024)
    nodeHeapSamplesMiB.push(mem.heapUsed / 1024 / 1024)

    if (!dockerSampleBusy) {
      dockerSampleBusy = true
      readDockerSample(CONTAINER, (docker) => {
        if (docker) {
          brokerCpuSamples.push(docker.cpuPct)
          brokerMemSamplesMiB.push(docker.memMiB)
        }
        dockerSampleBusy = false
      })
    }
  }, SAMPLE_MS)

  while (process.hrtime.bigint() < stopAt) {
    const now = process.hrtime.bigint()
    const elapsedSec = Number(now - start) / 1e9
    const targetSent = Math.floor(elapsedSec * RATE)
    while (sent < targetSent) {
      payload.writeBigUInt64LE(process.hrtime.bigint(), 0)
      pubStream.publish(subject, payload)
      sent++
    }
    if (sent >= targetSent) await sleep(1)
  }

  await pubStream.publishAck(subject, Buffer.alloc(0))
  const sendElapsed = Number(process.hrtime.bigint() - start) / 1e9
  const graceDeadline = Date.now() + Math.max(5_000, Math.ceil((sent / Math.max(1, RATE)) * 1_000))
  while (received < sent && Date.now() < graceDeadline) await sleep(25)
  if (sampleTimer) clearInterval(sampleTimer)

  latenciesUs.sort((a, b) => a - b)
  const recvElapsed = firstRecv > 0n && lastRecv >= firstRecv
    ? Number(lastRecv - firstRecv) / 1e9
    : 0

  console.log('  ┌── Sustained performance ─────────────────────────────────────┐')
  console.log(`  │  target   : ${RATE.toLocaleString()} msg/s for ${SECONDS}s`)
  console.log(`  │  sent     : ${sent.toLocaleString()}  in ${sendElapsed.toFixed(2)}s`)
  console.log(`  │  send     : ${fmtRate(sent, sendElapsed)}`)
  console.log(`  │  received : ${received.toLocaleString()}${received < sent ? `  (lag ${sent - received})` : ''}`)
  if (recvElapsed > 0) console.log(`  │  consume  : ${fmtRate(received, recvElapsed)}`)
  if (nodeCpuSamples.length > 0) {
    console.log(`  │  node cpu : avg ${avg(nodeCpuSamples).toFixed(1)}%  max ${Math.max(...nodeCpuSamples).toFixed(1)}%`)
    console.log(`  │  node rss : avg ${avg(nodeRssSamplesMiB).toFixed(1)} MiB  max ${Math.max(...nodeRssSamplesMiB).toFixed(1)} MiB`)
    console.log(`  │  node heap: avg ${avg(nodeHeapSamplesMiB).toFixed(1)} MiB  max ${Math.max(...nodeHeapSamplesMiB).toFixed(1)} MiB`)
  }
  if (brokerCpuSamples.length > 0) {
    console.log(`  │  broker cpu: avg ${avg(brokerCpuSamples).toFixed(1)}%  max ${Math.max(...brokerCpuSamples).toFixed(1)}%`)
    console.log(`  │  broker mem: avg ${avg(brokerMemSamplesMiB).toFixed(1)} MiB  max ${Math.max(...brokerMemSamplesMiB).toFixed(1)} MiB`)
  }
  if (latenciesUs.length > 0) {
    console.log(`  │  lat P50  : ${percentile(latenciesUs, 0.50).padStart(9)} µs`)
    console.log(`  │  lat P90  : ${percentile(latenciesUs, 0.90).padStart(9)} µs`)
    console.log(`  │  lat P99  : ${percentile(latenciesUs, 0.99).padStart(9)} µs`)
    console.log(`  │  lat Max  : ${percentile(latenciesUs, 0.9999).padStart(9)} µs`)
  }
  console.log('  └─────────────────────────────────────────────────────────────┘')

  subscription.close()
  await Promise.all([subClient.close(), pubClient.close()])
  await cleanup(admin, consumer, stream)
}

// ── Main ──────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  switch (MODE) {
    case 'pub': case 'fire-and-forget':
      await runPub(); break
    case 'ack':    await runAck();    break
    case 'pubsub': case 'publish-and-deliver':
      await runPubSub(); break
    case 'lat':    await runLat();    break
    case 'credit': await runCredit(); break
    case 'batch': case 'batch-publish':
      await runBatch(); break
    case 'pub-mt': case 'fire-and-forget-mt':
      await runPubMt(); break
    case 'perf': case 'performance':
      await runPerf(); break
    case 'replay-noack': await runReplay(false); break
    case 'replay-ack':   await runReplay(true); break
    default: console.error(`unknown mode: ${MODE}`); process.exit(1)
  }
}

main().catch((e) => { console.error(e); process.exit(1) })
