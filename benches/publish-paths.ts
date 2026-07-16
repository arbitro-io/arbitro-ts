// Publish-path validation micro-bench.
//
// The `throughput.ts` bench spawns a fresh Node process per run, so at 1000
// msgs the V8 JIT never tiers up — measurements are dominated by cold-start /
// GC and bounce 3-4× run-to-run (the classic "bench artifact"). This bench
// instead WARMS UP the JIT inside one process, then measures steady-state for
// each publish path, so the numbers reflect the client code path, not warmup.
//
// Paths compared (all fire the batch, then a final sync `publish` as the
// commit barrier — TCP order guarantees the barrier's RepOk implies every
// prior frame reached the broker):
//   sync   — publishNoAckSync  (no Promise/await/microtask per msg; Rust/Go path)
//   ff     — publishNoAck       (fire-and-forget, but async: Promise + await)
//   ack    — publish            (AckReq + RepOk round-trip per msg)
//
// Usage: tsx benches/publish-paths.ts [--addr host:port] [--batch N] [--warmup N] [--rounds N]

import { AckPolicy, ArbitroClient, DeliverPolicy, JournalType } from '../src';

const argv = process.argv.slice(2);
const arg = (f: string, d: string) => { const i = argv.indexOf(f); return i !== -1 && argv[i + 1] ? argv[i + 1]! : d; };
const num = (f: string, d: number) => { const v = arg(f, ''); return v ? parseInt(v, 10) : d; };

const ADDR = arg('--addr', '127.0.0.1:9899');
const BATCH = num('--batch', 1000);   // msgs per measured round (kept modest per bench-safety)
const WARMUP = num('--warmup', 5000);  // msgs to tier up the JIT before timing
const ROUNDS = num('--rounds', 8);     // measured rounds; report median
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

  const paths: Record<string, () => Promise<void> | void> = {
    sync: () => { s.publishNoAckSync(subject, payload); },
    ff:   () => { void s.publishNoAck(subject, payload).catch(() => {}); },
    ack:  () => { void s.publish(subject, payload).catch(() => {}); },
  };

  async function runPath(name: string, fire: () => Promise<void> | void): Promise<number[]> {
    // Warm up the JIT (not timed).
    for (let i = 0; i < WARMUP; i++) fire();
    await s.publish(subject, payload); // drain/barrier

    const rates: number[] = [];
    for (let r = 0; r < ROUNDS; r++) {
      const t0 = nowNs();
      for (let i = 0; i < BATCH - 1; i++) fire();
      await s.publish(subject, payload); // barrier: all prior reached broker
      rates.push(rate(BATCH, ms(t0, nowNs())));
    }
    return rates;
  }

  console.log(`publish-paths  ─  ${ADDR}  batch=${BATCH} warmup=${WARMUP} rounds=${ROUNDS} size=${SIZE}B\n`);

  // ── Client-side ENQUEUE-ONLY throughput ──────────────────────────────────
  // Apples-to-apples with Go's BenchmarkPublishFireAndForget: time ONLY the
  // publish CALLS (client encode + hand-off to the write buffer), no
  // end-to-end barrier. This is the client's raw push capability, independent
  // of broker ingest/fsync speed. Only meaningful for the synchronous
  // fire-and-forget path (no per-call Promise).
  {
    for (let i = 0; i < WARMUP; i++) s.publishNoAckSync(subject, payload); // warm JIT
    const encRates: number[] = [];
    for (let r = 0; r < ROUNDS; r++) {
      const t0 = nowNs();
      for (let i = 0; i < BATCH; i++) s.publishNoAckSync(subject, payload);
      encRates.push(rate(BATCH, ms(t0, nowNs())));
      // Let the broker drain between rounds so a slow (fsync) broker doesn't
      // accumulate a backlog that blows the final barrier's timeout.
      await new Promise((r) => setTimeout(r, 30));
    }
    console.log(`  ENQUEUE-only (sync, no barrier — raw client push, broker-independent):`);
    console.log(`    median ${fmt(median(encRates))}  min ${fmt(Math.min(...encRates))}  max ${fmt(Math.max(...encRates))}  msg/s\n`);
  }

  console.log(`  END-TO-END (with sync commit barrier — broker-bound):`);
  console.log(`  path   median      min      max   (msg/s)`);
  for (const [name, fire] of Object.entries(paths)) {
    const rs = await runPath(name, fire);
    console.log(`  ${name.padEnd(5)} ${fmt(median(rs))}  ${fmt(Math.min(...rs))}  ${fmt(Math.max(...rs))}`);
  }

  await client.deleteStream(stream).catch(() => {});
  await client.close();
}

main().catch((e) => { console.error('bench failed:', e); process.exit(1); });
