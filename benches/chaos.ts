import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { AckPolicy, ArbitroClient, DeliverPolicy, JournalType } from '../src'

const execFileAsync = promisify(execFile)

const argv = process.argv.slice(2)
const str = (flag: string, def: string) => { const i = argv.indexOf(flag); return i !== -1 && argv[i + 1] ? argv[i + 1]! : def }
const num = (flag: string, def: number) => { const v = str(flag, ''); return v ? parseInt(v, 10) : def }

const ADDR = str('--addr', '127.0.0.1:9898')
const CONTAINER = str('--container', 'arbitro-server')
const DURATION = num('--duration', 8)
const RATE = num('--rate', 50)
const CRASH_AT = num('--crash-at', Math.max(2, Math.floor(DURATION / 2)))
const DOWN_MS = num('--down-ms', 500)
const SIZE = num('--size', 128)

async function connect(): Promise<ArbitroClient> {
  for (let i = 0; i < 40; i++) {
    try {
      return await new ArbitroClient({
        servers: [ADDR],
        timeout: 2_000,
        reconnect: { enabled: true, maxAttempts: 100, intervalMs: 100, jitter: false },
      }).connect()
    } catch {
      await new Promise((r) => setTimeout(r, 150))
    }
  }
  throw new Error(`could not connect to ${ADDR}`)
}

function unique(prefix: string): { stream: string, subject: string, consumer: string } {
  const tag = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`
  return {
    stream: tag,
    subject: `${tag}.event`,
    consumer: `${tag}-workers`,
  }
}

async function docker(...args: string[]): Promise<void> {
  await execFileAsync('docker', args, { encoding: 'utf8' })
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitForBroker(): Promise<void> {
  for (let i = 0; i < 60; i++) {
    try {
      const c = await connect()
      await c.close()
      return
    } catch {
      await sleep(250)
    }
  }
  throw new Error('broker did not come back after restart')
}

async function cleanup(admin: ArbitroClient, consumer: string, stream: string): Promise<void> {
  try { await admin.deleteConsumer(consumer) } catch {}
  try { await admin.deleteStream(stream) } catch {}
  await admin.close()
}

async function main(): Promise<void> {
  console.log('╔══════════════════════════════════════════════════════════════════╗')
  console.log('║             arbitro-ts chaos bench                            ║')
  console.log('╚══════════════════════════════════════════════════════════════════╝')
  console.log()
  console.log(`  addr      : ${ADDR}`)
  console.log(`  container : ${CONTAINER}`)
  console.log(`  duration  : ${DURATION}s`)
  console.log(`  rate      : ${RATE} msg/s`)
  console.log(`  crash at  : ${CRASH_AT}s`)
  console.log(`  down ms   : ${DOWN_MS}`)
  console.log()

  const admin = await connect()
  const pub = await connect()
  const subClient = await connect()
  const { stream, subject, consumer } = unique('bench-chaos')

  await admin.createStream(stream, { subjectFilter: `${stream}.>`, journal: { type: JournalType.Tolerant } })
  await admin.createConsumer(stream, {
    name: consumer,
    filter: `${stream}.>`,
    ackPolicy: AckPolicy.Explicit,
    deliverPolicy: DeliverPolicy.All,
    maxAckPending: Math.max(5_000, RATE * DURATION),
  })

  const sentSeqs = new Set<number>()
  const receivedSeqs = new Set<number>()
  let deliveries = 0
  let crashStarted = false
  let crashed = false
  const start = Date.now()
  const stopAt = start + DURATION * 1000

  const sub = await subClient.subscribe(consumer, (msg) => {
    deliveries++
    const data = msg.data()
    if (data.length >= 4) receivedSeqs.add(data.readUInt32LE(0))
    msg.ack()
  })

  let seq = 0
  while (Date.now() < stopAt) {
    const elapsedMs = Date.now() - start
    const elapsedSec = elapsedMs / 1000

    if (!crashStarted && elapsedSec >= CRASH_AT) {
      crashStarted = true
      void (async () => {
        await docker('stop', '-t', '0', CONTAINER)
        await sleep(DOWN_MS)
        await docker('start', CONTAINER)
        await waitForBroker()
        const verify = await connect()
        const streamOk = await verify.streamExists(stream)
        const consumerOk = await verify.consumerExists(consumer)
        console.log(`  restart   : stream=${streamOk} consumer=${consumerOk}`)
        await verify.close()
        crashed = true
      })().catch((e) => {
        console.error(`  restart   : FAILED ${e instanceof Error ? e.message : String(e)}`)
      })
    }

    const target = Math.floor(elapsedSec * RATE)
    if (seq < target) {
      const payload = Buffer.alloc(Math.max(SIZE, 4), 0x42)
      payload.writeUInt32LE(seq, 0)
      try {
        await pub.publishAck(subject, payload)
        sentSeqs.add(seq)
        seq++
      } catch {
        await sleep(50)
      }
    } else {
      await sleep(1)
    }
  }
  const settleDeadline = Date.now() + 8_000
  while (receivedSeqs.size < sentSeqs.size && Date.now() < settleDeadline) {
    await sleep(25)
  }

  const sent = sentSeqs.size
  const uniqueDelivered = receivedSeqs.size
  const duplicateDeliveries = deliveries - uniqueDelivered
  const lost = sent - uniqueDelivered

  console.log('  ┌── Chaos results ─────────────────────────────────────────────┐')
  console.log(`  │  sent        : ${sent}`)
  console.log(`  │  delivered   : ${deliveries}`)
  console.log(`  │  unique      : ${uniqueDelivered}`)
  console.log(`  │  duplicates  : ${duplicateDeliveries}`)
  console.log(`  │  lost        : ${lost}`)
  console.log(`  │  restarted   : ${crashed}`)
  console.log(`  │  verdict     : ${lost === 0 && crashed ? 'PASS' : 'FAIL'}`)
  console.log('  └─────────────────────────────────────────────────────────────┘')

  sub.close()
  await Promise.all([pub.close(), subClient.close()])
  await cleanup(admin, consumer, stream)

  if (!(lost === 0 && crashed)) process.exit(1)
}

main().catch((e) => { console.error(e); process.exit(1) })
