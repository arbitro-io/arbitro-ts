// arbitro-ts bench — NATS bench style
//
// Usage:
//   tsx benches/throughput.ts [OPTIONS]
//
// Options:
//   --mode    pub|pubsub|lat        (default: pub)
//   --msgs    N                     (default: 1_000_000)
//   --size    N bytes               (default: 16)
//   --addr    host:port             (default: 127.0.0.1:9898)
//   --server  /path/to/arbitro      auto-start server binary
//
// Examples:
//   tsx benches/throughput.ts --mode pub
//   tsx benches/throughput.ts --mode pubsub --msgs 500000
//   tsx benches/throughput.ts --addr 127.0.0.1:9898 --mode pub

import { spawn, type ChildProcess } from 'child_process'
import { ArbitroClient } from '../src'
import type { Subscription } from '../src'

// ── CLI ───────────────────────────────────────────────────────────────────

const argv  = process.argv.slice(2)
const str   = (flag: string, def: string)  => { const i = argv.indexOf(flag); return i !== -1 && argv[i + 1] ? argv[i + 1]! : def }
const num   = (flag: string, def: number)  => { const v = str(flag, ''); return v ? parseInt(v, 10) : def }

const MODE   = str('--mode',   'pub')
const MSGS   = num('--msgs',   1_000_000)
const SIZE   = num('--size',   16)
const ADDR   = str('--addr',   '127.0.0.1:9898')
const SERVER = str('--server', '')

const STREAM   = 'bench'
const SUBJECT  = 'bench.msg'
const CONSUMER = 'bench-workers'

// ── Server lifecycle ──────────────────────────────────────────────────────

async function startServer(bin: string): Promise<ChildProcess> {
  const proc = spawn(bin, ['--bind', ADDR, '--journal', 'memory'], {
    stdio: 'ignore',
    detached: false,
  })
  proc.on('error', (e) => { console.error(`server error: ${e.message}`); process.exit(1) })
  await new Promise((r) => setTimeout(r, 300))   // wait for bind
  return proc
}

// ── Connect with retry ────────────────────────────────────────────────────

async function connect(): Promise<ArbitroClient> {
  for (let i = 0; i < 20; i++) {
    try {
      return await new ArbitroClient({ servers: [ADDR], timeout: 1_000 }).connect()
    } catch {
      await new Promise((r) => setTimeout(r, 100))
    }
  }
  throw new Error(`could not connect to ${ADDR}`)
}

// ── Formatting ────────────────────────────────────────────────────────────

function fmtRate(msgs: number, secs: number): string {
  const mps  = msgs / secs
  const mbps = (msgs * (32 + 2 + SUBJECT.length + SIZE)) / secs / 1_048_576
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

// ── Mode: pub ────────────────────────────────────────────────────────────

async function runPub(): Promise<void> {
  header('pub')
  const client = await connect()

  // Setup on same connection — server processes frames in FIFO order.
  client.stream(STREAM).create({ subjectFilter: `${STREAM}.*`, journal: { type: 'memory' as never } })

  const payload = Buffer.alloc(SIZE, 0x42)
  const t0      = process.hrtime.bigint()

  for (let i = 0; i < MSGS; i++) {
    client.publish(SUBJECT, payload)
  }

  const elapsed = Number(process.hrtime.bigint() - t0) / 1e9
  console.log('  ┌── Pub results ───────────────────────────────────────────────┐')
  console.log(`  │  sent       : ${MSGS.toLocaleString()}  in ${elapsed.toFixed(2)}s`)
  console.log(`  │  rate       : ${fmtRate(MSGS, elapsed)}`)
  console.log('  └─────────────────────────────────────────────────────────────┘')

  await client.close()
}

// ── Mode: pubsub ──────────────────────────────────────────────────────────

async function runPubSub(): Promise<void> {
  header('pubsub')
  const pub = await connect()
  const sub = await connect()

  // Setup on sub connection — PubCreateStream + PubCreateConsumer arrive before
  // PubSubscribe on the same TCP stream, guaranteeing FIFO ordering.
  sub.stream(STREAM).create({ subjectFilter: `${STREAM}.*`, journal: { type: 'memory' as never } })
  sub.stream(STREAM).consumer({ name: CONSUMER, filter: SUBJECT, maxAckPending: 20_000 }).create()

  let received  = 0
  let subStart  = 0n
  let subEnd    = 0n

  const subscription: Subscription = await sub
    .stream(STREAM)
    .consumer({ name: CONSUMER, filter: SUBJECT, maxAckPending: 20_000 })
    .subscribe((msg) => {
      if (received === 0) subStart = process.hrtime.bigint()
      received++
      msg.ack()
      if (received >= MSGS) subEnd = process.hrtime.bigint()
    })

  const payload  = Buffer.alloc(SIZE, 0x42)
  const pubStart = process.hrtime.bigint()

  for (let i = 0; i < MSGS; i++) {
    pub.publish(SUBJECT, payload)
  }

  const pubElapsed = Number(process.hrtime.bigint() - pubStart) / 1e9

  // Wait for all messages to be delivered (max 30s)
  const deadline = Date.now() + 30_000
  while (received < MSGS && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 50))
  }

  const subElapsed = Number(subEnd - subStart) / 1e9

  console.log('  ┌── PubSub results ────────────────────────────────────────────┐')
  console.log(`  ├── Pub ──────────────────────────────────────────────────────────`)
  console.log(`  │  sent       : ${MSGS.toLocaleString()}  in ${pubElapsed.toFixed(2)}s`)
  console.log(`  │  rate       : ${fmtRate(MSGS, pubElapsed)}`)
  console.log(`  ├── Sub ──────────────────────────────────────────────────────────`)
  console.log(`  │  received   : ${received.toLocaleString()}  in ${subElapsed > 0 ? subElapsed.toFixed(2) : '—'}s`)
  if (subElapsed > 0) console.log(`  │  rate       : ${fmtRate(received, subElapsed)}`)
  console.log('  └─────────────────────────────────────────────────────────────┘')

  subscription.close()
  await Promise.all([pub.close(), sub.close()])
}

// ── Mode: lat ─────────────────────────────────────────────────────────────

async function runLat(): Promise<void> {
  header('lat')
  const pub = await connect()
  const sub = await connect()

  // Setup on sub connection for guaranteed ordering before PubSubscribe.
  sub.stream(STREAM).create({ subjectFilter: `${STREAM}.*`, journal: { type: 'memory' as never } })
  sub.stream(STREAM).consumer({ name: CONSUMER, filter: SUBJECT, maxAckPending: 1 }).create()

  const samples: number[] = []
  let resolve: (() => void) | null = null

  const subscription: Subscription = await sub
    .stream(STREAM)
    .consumer({ name: CONSUMER, filter: SUBJECT, maxAckPending: 1 })
    .subscribe((msg) => {
      msg.ack()
      resolve?.()
    })

  const payload = Buffer.alloc(SIZE, 0x42)

  for (let i = 0; i < MSGS; i++) {
    const t0 = process.hrtime.bigint()
    await new Promise<void>((res) => {
      resolve = res
      pub.publish(SUBJECT, payload)
    })
    samples.push(Number(process.hrtime.bigint() - t0) / 1_000)  // µs
  }

  samples.sort((a, b) => a - b)
  const p = (q: number) => samples[Math.floor(samples.length * q)]!.toFixed(1)

  console.log('  ┌── Latency results ───────────────────────────────────────────┐')
  console.log(`  │  P50    ${p(0.50).padStart(9)} µs`)
  console.log(`  │  P90    ${p(0.90).padStart(9)} µs`)
  console.log(`  │  P99    ${p(0.99).padStart(9)} µs`)
  console.log(`  │  P99.9  ${p(0.999).padStart(9)} µs`)
  console.log(`  │  Max    ${p(0.9999).padStart(9)} µs`)
  console.log('  └─────────────────────────────────────────────────────────────┘')

  subscription.close()
  await Promise.all([pub.close(), sub.close()])
}

// ── Main ──────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  let proc: ChildProcess | undefined
  if (SERVER) proc = await startServer(SERVER)

  try {
    if      (MODE === 'pub')    await runPub()
    else if (MODE === 'pubsub') await runPubSub()
    else if (MODE === 'lat')    await runLat()
    else { console.error(`unknown mode: ${MODE}`); process.exit(1) }
  } finally {
    proc?.kill()
  }
}

main().catch((e) => { console.error(e); process.exit(1) })
