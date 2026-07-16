// Publish-path validation micro-bench.
//
// The `throughput.ts` bench spawns a fresh Node process per run, so at 1000
// msgs the V8 JIT never tiers up — measurements are dominated by cold-start /
// GC and bounce 3-4× run-to-run (the classic "bench artifact"). This bench
// instead WARMS UP the JIT inside one process, then measures steady-state for
// each publish path, so the numbers reflect the client code path, not warmup.
//
// Two regimes are measured:
//
//   PIPELINED (fire the frames, one final sync `publish` as the commit barrier —
//   TCP order makes the barrier's RepOk imply every prior frame reached the
//   broker). Throughput here is client-encode bound, not round-trip bound:
//     sync   — publishNoAckSync  (no Promise/await/microtask per msg; Rust/Go path)
//     ff     — publishNoAck       (fire-and-forget, async: Promise + await)
//     ack    — publish            (AckReq on the wire, but not awaited per msg)
//     batch  — publishBatch       (pipelined batches of --batchsize)
//
//   WAIT (await each op's broker ack before the next — round-trip bound, this is
//   the real `publishWait` / `publishBatchWait` confirmed-publish throughput):
//     wait        — await publish()        per message
//     batch-wait  — await publishBatch()   per batch of --batchsize
//
// Usage: tsx benches/publish-paths.ts [--addr host:port] [--n N] [--batchsize N] [--warmup N] [--rounds N]

import { ArbitroClient, JournalType, type BatchPublishEntry } from '../src';

const argv = process.argv.slice(2);
const arg = (f: string, d: string) => { const i = argv.indexOf(f); return i !== -1 && argv[i + 1] ? argv[i + 1]! : d; };
const num = (f: string, d: number) => { const v = arg(f, ''); return v ? parseInt(v, 10) : d; };

const ADDR = arg('--addr', '127.0.0.1:9899');
const N = num('--n', 1000);          // msgs per measured round (kept modest per bench-safety)
const BATCHSIZE = num('--batchsize', 128);
const WARMUP = num('--warmup', 5000); // msgs to tier up the JIT before timing
const ROUNDS = num('--rounds', 8);    // measured rounds; report median
const SIZE = num('--size', 128);

const payload = Buffer.alloc(SIZE, 0x42);
const nowNs = () => process.hrtime.bigint();
const ms = (a: bigint, b: bigint) => Number(b - a) / 1e6;
const median = (xs: number[]) => { const s = [...xs].sort((a, b) => a - b); return s[Math.floor(s.length / 2)]!; };
const rate = (n: number, msVal: number) => (msVal > 0 ? (n / msVal) * 1000 : 0);
const fmt = (r: number) => (r >= 1e6 ? `${(r / 1e6).toFixed(2)}M` : r >= 1e3 ? `${(r / 1e3).toFixed(0)}K` : `${r.toFixed(0)}`).padStart(7);

async function main(): Promise<void> {
  const client = await new ArbitroClient({ servers: [ADDR], timeout: 5000 }).connect();
  const stream = `bench-paths-${process.pid}`;
  const subject = `${stream}.e`;

  await client.deleteStream(stream).catch(() => {});
  await client.stream(stream, { subjectFilter: `${stream}.>`, journal: { type: JournalType.Memory } }).create();
  await client.resolveStream(stream); // cache sid for the sync path
  const s = client.stream(stream);

  const entry: BatchPublishEntry = { subject, payload };
  const batch: BatchPublishEntry[] = new Array(BATCHSIZE).fill(entry);
  const batchesPerRound = Math.floor(N / BATCHSIZE);
  const batchMsgs = batchesPerRound * BATCHSIZE;

  console.log(`publish-paths  ─  ${ADDR}  n=${N} batchsize=${BATCHSIZE} warmup=${WARMUP} rounds=${ROUNDS} size=${SIZE}B\n`);

  // ── ENQUEUE-only: raw client push (broker-independent) ────────────────────
  {
    for (let i = 0; i < WARMUP; i++) s.publishNoAckSync(subject, payload);
    const rs: number[] = [];
    for (let r = 0; r < ROUNDS; r++) {
      const t0 = nowNs();
      for (let i = 0; i < N; i++) s.publishNoAckSync(subject, payload);
      rs.push(rate(N, ms(t0, nowNs())));
      await new Promise((r) => setTimeout(r, 30)); // let the broker drain
    }
    console.log(`  ENQUEUE-only (sync, no barrier — raw client push):`);
    console.log(`    median ${fmt(median(rs))}  min ${fmt(Math.min(...rs))}  max ${fmt(Math.max(...rs))}  msg/s\n`);
  }

  // ── PIPELINED: fire frames, one final sync publish as commit barrier ──────
  const pipelined: Record<string, () => void> = {
    sync:  () => { s.publishNoAckSync(subject, payload); },
    ff:    () => { void s.publishNoAck(subject, payload).catch(() => {}); },
    ack:   () => { void s.publish(subject, payload).catch(() => {}); },
    batch: () => { void s.publishBatch(batch).catch(() => {}); },
  };
  async function runPipelined(fire: () => void, ops: number, msgs: number): Promise<number[]> {
    for (let i = 0; i < WARMUP; i++) s.publishNoAckSync(subject, payload);
    await s.publish(subject, payload);
    const rs: number[] = [];
    for (let r = 0; r < ROUNDS; r++) {
      const t0 = nowNs();
      for (let i = 0; i < ops; i++) fire();
      await s.publish(subject, payload); // barrier
      rs.push(rate(msgs, ms(t0, nowNs())));
    }
    return rs;
  }
  console.log(`  PIPELINED (commit barrier at end — client-encode bound):`);
  console.log(`  path   median      min      max   (msg/s)`);
  for (const name of ['sync', 'ff', 'ack']) {
    const rs = await runPipelined(pipelined[name]!, N, N);
    console.log(`  ${name.padEnd(6)} ${fmt(median(rs))}  ${fmt(Math.min(...rs))}  ${fmt(Math.max(...rs))}`);
  }
  {
    const rs = await runPipelined(pipelined.batch!, batchesPerRound, batchMsgs);
    console.log(`  ${`batch`.padEnd(6)} ${fmt(median(rs))}  ${fmt(Math.min(...rs))}  ${fmt(Math.max(...rs))}`);
  }

  // ── WAIT: await each op's ack before the next (round-trip bound) ──────────
  async function runWait(op: () => Promise<unknown>, ops: number, msgs: number): Promise<number[]> {
    for (let i = 0; i < 200; i++) await op(); // warm (round-trip bound, fewer iters)
    const rs: number[] = [];
    for (let r = 0; r < ROUNDS; r++) {
      const t0 = nowNs();
      for (let i = 0; i < ops; i++) await op();
      rs.push(rate(msgs, ms(t0, nowNs())));
    }
    return rs;
  }
  console.log(`\n  WAIT (await each ack — round-trip bound, real confirmed-publish):`);
  console.log(`  path        median      min      max   (msg/s)`);
  {
    const rs = await runWait(() => s.publish(subject, payload), N, N);
    console.log(`  ${`wait`.padEnd(11)} ${fmt(median(rs))}  ${fmt(Math.min(...rs))}  ${fmt(Math.max(...rs))}`);
  }
  {
    const rs = await runWait(() => s.publishBatch(batch), batchesPerRound, batchMsgs);
    console.log(`  ${`batch-wait`.padEnd(11)} ${fmt(median(rs))}  ${fmt(Math.min(...rs))}  ${fmt(Math.max(...rs))}`);
  }

  await client.deleteStream(stream).catch(() => {});
  await client.close();
}

main().catch((e) => { console.error('bench failed:', e); process.exit(1); });
