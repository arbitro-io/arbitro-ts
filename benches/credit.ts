import { AckPolicy, ArbitroClient, DeliverPolicy, JournalType } from '../src'

const argv = process.argv.slice(2)
const str = (flag: string, def: string) => { const i = argv.indexOf(flag); return i !== -1 && argv[i + 1] ? argv[i + 1]! : def }
const num = (flag: string, def: number) => { const v = str(flag, ''); return v ? parseInt(v, 10) : def }

const MSGS = num('--msgs', 10_000)
const SIZE = num('--size', 128)
const ADDR = str('--addr', '127.0.0.1:9898')

async function connect(): Promise<ArbitroClient> {
  for (let i = 0; i < 20; i++) {
    try { return await new ArbitroClient({ servers: [ADDR], timeout: 2_000 }).connect() }
    catch { await new Promise((r) => setTimeout(r, 150)) }
  }
  throw new Error(`could not connect to ${ADDR}`)
}

function unique(prefix: string): { stream: string, subject: string, consumer: string } {
  const tag = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`
  return {
    stream: tag,
    subject: `${tag}.msg`,
    consumer: `${tag}-workers`,
  }
}

async function cleanup(admin: ArbitroClient, consumer: string, stream: string): Promise<void> {
  try { await admin.deleteConsumer(stream, consumer) } catch {}
  try { await admin.deleteStream(stream) } catch {}
  await admin.close()
}

function fmtRate(msgs: number, secs: number, size: number): string {
  const mps = msgs / secs
  const mbps = (msgs * (32 + 2 + 'bench.msg'.length + size)) / secs / 1_048_576
  return mps >= 1_000_000
    ? `${(mps / 1_000_000).toFixed(2)}M msg/s  ${mbps.toFixed(1)} MB/s`
    : `${(mps / 1_000).toFixed(1)}K msg/s  ${mbps.toFixed(1)} MB/s`
}

async function runScenario(maxCredit: number | null): Promise<void> {
  const { stream, subject, consumer } = unique(maxCredit === null ? 'credit-none' : `credit-${maxCredit}`)
  const admin = await connect()
  const subClient = await connect()
  const pubClient = await connect()

  await admin.createStream(stream, { subjectFilter: `${stream}.>`, journal: { type: JournalType.Memory } })

  const consumerCfg = maxCredit !== null
    ? {
        name: consumer,
        filter: subject,
        ackPolicy: AckPolicy.Explicit,
        deliverPolicy: DeliverPolicy.All,
        maxAckPending: 20_000,
        maxSubjectInflight: maxCredit,
      }
    : {
        name: consumer,
        filter: subject,
        ackPolicy: AckPolicy.Explicit,
        deliverPolicy: DeliverPolicy.All,
        maxAckPending: 20_000,
      }

  let received = 0
  const sub = await subClient.subscribe(stream, consumerCfg, (msg) => {
    received++
    msg.ack()
  })

  const payload = Buffer.alloc(SIZE, 0x42)
  const t0 = process.hrtime.bigint()
  for (let i = 0; i < MSGS; i++) pubClient.publish(stream, subject, payload)

  const deadline = Date.now() + 30_000
  while (received < MSGS && Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 25))
  }

  const elapsed = Number(process.hrtime.bigint() - t0) / 1e9
  const label = maxCredit === null ? 'none' : `limit=${maxCredit}`
  console.log(`  credit_rule: ${label.padEnd(10)}  -> ${fmtRate(received, elapsed, SIZE)}  (${received}/${MSGS})`)

  sub.close()
  await Promise.all([subClient.close(), pubClient.close()])
  await cleanup(admin, consumer, stream)
}

async function main(): Promise<void> {
  console.log('╔══════════════════════════════════════════════════════════════════╗')
  console.log('║            arbitro-ts credit bench                            ║')
  console.log('╚══════════════════════════════════════════════════════════════════╝')
  console.log()
  console.log(`  msgs     : ${MSGS.toLocaleString()}`)
  console.log(`  size     : ${SIZE} bytes`)
  console.log(`  addr     : ${ADDR}`)
  console.log()
  await runScenario(null)
  await runScenario(500)
  await runScenario(5)
}

main().catch((e) => { console.error(e); process.exit(1) })
