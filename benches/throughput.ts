// arbitro-ts throughput bench
//
// Four scenarios — two delivery shapes × two publish shapes:
//
//   pubsub-single   subscriber live + drain runs concurrently with publish,
//                   publishes are single fire-and-forget frames
//   pubsub-batch    same as above but publishes are batched
//   reply-single    publish everything first, then drain
//   reply-batch     publish everything first with batches, then drain
//
// Usage:
//   tsx benches/throughput.ts [OPTIONS]
//
// Options:
//   --mode    all | pubsub-single | pubsub-batch | reply-single | reply-batch
//             | pubsub | reply
//             default: all
//   --msgs    N                           default: 10_000
//   --size    N bytes                     default: 128
//   --batch   N entries per batch frame   default: 128, hard-capped at 128
//   --addr    host:port                   default: 127.0.0.1:9898

import { AckPolicy, ArbitroClient, DeliverPolicy, JournalType } from '../src';
import type { BatchPublishEntry } from '../src/proto/publish';
import type { Stream } from '../src/stream/stream';

// ── Constants ─────────────────────────────────────────────────────────────

const MAX_BATCH_SIZE = 128;
const CONNECT_RETRIES = 20;
const CONNECT_RETRY_DELAY_MS = 150;
const CONNECT_TIMEOUT_MS = 2_000;
const CLEANUP_DELAY_MS = 50;
const RECEIVE_TIMEOUT_MS = 60_000;
const BYTES_PER_MB = 1024 * 1024;
const NS_PER_MS = 1_000_000;

const noop = (): void => { };

// ── CLI ───────────────────────────────────────────────────────────────────

const argv = process.argv.slice(2);

function argString(flag: string, fallback: string): string {
  const index = argv.indexOf(flag);
  return index !== -1 && argv[index + 1] ? argv[index + 1]! : fallback;
}

function argInt(flag: string, fallback: number): number {
  const raw = argString(flag, '');
  if (!raw) return fallback;

  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) ? value : fallback;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

const MODE = argString('--mode', 'all');
const MSGS = Math.max(0, argInt('--msgs', 10_000));
const SIZE = Math.max(0, argInt('--size', 128));
const BATCH = clamp(argInt('--batch', 128), 1, MAX_BATCH_SIZE);
const ADDR = argString('--addr', '127.0.0.1:9898');

// ── Time / formatting helpers ─────────────────────────────────────────────

function nowNs(): bigint {
  return process.hrtime.bigint();
}

function elapsedMs(start: bigint, end: bigint = nowNs()): number {
  return Number(end - start) / NS_PER_MS;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function fmtRate(msgs: number, secs: number): string {
  if (secs <= 0 || msgs <= 0) return '—';

  const mps = msgs / secs;
  const mbps = (mps * SIZE) / BYTES_PER_MB;

  const mpsText =
    mps >= 1e6
      ? `${(mps / 1e6).toFixed(2)}M msg/s`
      : mps >= 1e3
        ? `${(mps / 1e3).toFixed(1)}K msg/s`
        : `${mps.toFixed(0)} msg/s`;

  return `${mpsText.padEnd(13)} ${mbps.toFixed(1)} MB/s`;
}

function fmtCount(value: number): string {
  return value.toLocaleString();
}

function printBoxEnd(): void {
  console.log('└──────────────────────────────────────────────────────────────');
}

// ── Promise helpers ───────────────────────────────────────────────────────

function deferred(): {
  promise: Promise<void>;
  resolve: () => void;
} {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });

  return { promise, resolve };
}

async function waitWithTimeout(
  promise: Promise<void>,
  timeoutMs: number,
): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined;

  const timeout = new Promise<false>((resolve) => {
    timer = setTimeout(() => resolve(false), timeoutMs);
    timer.unref?.();
  });

  try {
    return await Promise.race([
      promise.then(() => true),
      timeout,
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function safe(op: () => Promise<unknown>): Promise<void> {
  try {
    await op();
  } catch {
    // Best-effort cleanup.
  }
}

// ── Client / lifecycle helpers ────────────────────────────────────────────

async function connect(): Promise<ArbitroClient> {
  for (let attempt = 1; attempt <= CONNECT_RETRIES; attempt++) {
    try {
      return await new ArbitroClient({
        servers: [ADDR],
        timeout: CONNECT_TIMEOUT_MS,
      }).connect();
    } catch {
      if (attempt < CONNECT_RETRIES) {
        await sleep(CONNECT_RETRY_DELAY_MS);
      }
    }
  }

  throw new Error(`could not connect to ${ADDR}`);
}

async function withStream(
  client: ArbitroClient,
  stream: string,
  consumerName: string,
  body: () => Promise<void>,
): Promise<void> {
  await safe(() => client.deleteConsumer(stream, consumerName));
  await safe(() => client.deleteStream(stream));

  await client.stream(stream, {
    subjectFilter: `${stream}.>`,
    journal: { type: JournalType.Memory },
  }).create();

  try {
    await body();
  } finally {
    await sleep(CLEANUP_DELAY_MS);
    await safe(() => client.deleteConsumer(stream, consumerName));
    await safe(() => client.deleteStream(stream));
  }
}

async function closeClient(client: ArbitroClient): Promise<void> {
  await sleep(CLEANUP_DELAY_MS);
  await safe(() => client.close());
}

// ── Publish helpers ───────────────────────────────────────────────────────

function makeBatch(size: number, entry: BatchPublishEntry): BatchPublishEntry[] {
  const batch = new Array<BatchPublishEntry>(size);

  for (let i = 0; i < size; i++) {
    batch[i] = entry;
  }

  return batch;
}

/**
 * Single-frame publishes.
 *
 * The middle publishes are fire-and-forget.
 * The final publish is awaited as the commit barrier.
 */
async function publishLoopSingle(
  s: Stream,
  subject: string,
  payload: Buffer,
  count: number,
): Promise<void> {
  if (count <= 0) return;

  const last = count - 1;

  for (let i = 0; i < last; i++) {
    void s.publish(subject, payload).catch(noop);
  }

  await s.publish(subject, payload);
}

/**
 * Batched publishes.
 *
 * The middle batches are fire-and-forget.
 * The final batch is awaited as the commit barrier.
 *
 * Assumption:
 *   publishBatch() does not mutate the passed entries.
 */
async function publishLoopBatch(
  s: Stream,
  subject: string,
  payload: Buffer,
  count: number,
  requestedBatchSize: number,
): Promise<void> {
  if (count <= 0) return;

  if (requestedBatchSize <= 0) {
    throw new RangeError('batchSize must be greater than 0');
  }

  const batchSize = Math.min(requestedBatchSize, count);
  const entry: BatchPublishEntry = { subject, payload };

  const fullBatch = makeBatch(batchSize, entry);
  const fullBatches = Math.floor(count / batchSize);
  const tail = count - fullBatches * batchSize;

  const fireAndForgetFullBatches = tail === 0
    ? fullBatches - 1
    : fullBatches;

  for (let i = 0; i < fireAndForgetFullBatches; i++) {
    void s.publishBatch(fullBatch).catch(noop);
  }

  if (tail > 0) {
    await s.publishBatch(makeBatch(tail, entry));
  } else {
    await s.publishBatch(fullBatch);
  }
}

async function publishLoop(
  s: Stream,
  subject: string,
  payload: Buffer,
  count: number,
  batchPublish: boolean,
): Promise<void> {
  if (batchPublish) {
    await publishLoopBatch(s, subject, payload, count, BATCH);
  } else {
    await publishLoopSingle(s, subject, payload, count);
  }
}

// ── Scenario: pubsub / live drain ─────────────────────────────────────────

async function runPubSub(label: string, batchPublish: boolean): Promise<void> {
  console.log(
    `\n┌── ${label}  ${fmtCount(MSGS)} msgs × ${SIZE}B${batchPublish ? `  (batch=${BATCH})` : ''
    }`,
  );

  const client = await connect();
  const stream = `bench-pubsub-${batchPublish ? 'batch' : 'single'}`;
  const consumerName = `${stream}-workers`;
  const subject = `${stream}.event`;
  const payload = Buffer.alloc(SIZE, 0x42);

  try {
    await withStream(client, stream, consumerName, async () => {
      const consumer = client.stream(stream).consumer({
        name: consumerName,
        filter: `${stream}.>`,
        deliverPolicy: DeliverPolicy.New,
        ackPolicy: AckPolicy.None,
        maxAckPending: Math.max(50_000, MSGS),
      });

      let received = 0;
      let firstAt = 0n;
      let lastAt = 0n;

      const done = deferred();

      const sub = await consumer.subscribe((_msg) => {
        const t = nowNs();

        if (received === 0) {
          firstAt = t;
        }

        received++;

        if (received === MSGS) {
          lastAt = t;
          done.resolve();
        }
      });

      try {
        const s = client.stream(stream);

        const pubT0 = nowNs();
        await publishLoop(s, subject, payload, MSGS, batchPublish);
        const pubElapsedMs = elapsedMs(pubT0);

        const completed = await waitWithTimeout(done.promise, RECEIVE_TIMEOUT_MS);

        const drainMs = firstAt > 0n && lastAt > 0n
          ? elapsedMs(firstAt, lastAt)
          : 0;

        console.log(
          `│  pub    ${fmtCount(MSGS)} in ${pubElapsedMs.toFixed(2)}ms  →  ${fmtRate(MSGS, pubElapsedMs / 1000)
          }`,
        );

        console.log(
          `│  drain  ${fmtCount(received)} in ${drainMs.toFixed(2)}ms  →  ${fmtRate(received, drainMs / 1000)
          }`,
        );

        if (!completed || received < MSGS) {
          console.log(`│  WARN only ${received}/${MSGS} received (${RECEIVE_TIMEOUT_MS / 1000}s timeout)`);
        }

        printBoxEnd();
      } finally {
        try {
          sub.close();
        } catch {
          // Ignore close races.
        }
      }
    });
  } finally {
    await closeClient(client);
  }
}

// ── Scenario: reply / backlog drain ───────────────────────────────────────

async function runReply(label: string, batchPublish: boolean): Promise<void> {
  console.log(
    `\n┌── ${label}  ${fmtCount(MSGS)} msgs × ${SIZE}B${batchPublish ? `  (batch=${BATCH})` : ''
    }`,
  );

  const client = await connect();
  const stream = `bench-reply-${batchPublish ? 'batch' : 'single'}`;
  const consumerName = `${stream}-workers`;
  const subject = `${stream}.event`;
  const payload = Buffer.alloc(SIZE, 0x42);

  try {
    await withStream(client, stream, consumerName, async () => {
      const s = client.stream(stream);

      const pubT0 = nowNs();
      await publishLoop(s, subject, payload, MSGS, batchPublish);
      const pubElapsedMs = elapsedMs(pubT0);

      const consumer = client.stream(stream).consumer({
        name: consumerName,
        filter: `${stream}.>`,
        deliverPolicy: DeliverPolicy.All,
        ackPolicy: AckPolicy.Explicit,
        maxAckPending: Math.max(50_000, MSGS),
      });

      let received = 0;
      let firstAt = 0n;
      let lastAt = 0n;

      const done = deferred();
      const subT0 = nowNs();

      const sub = await consumer.subscribe((msg) => {
        const t = nowNs();

        if (received === 0) {
          firstAt = t;
        }

        received++;
        msg.ack();

        if (received === MSGS) {
          lastAt = t;
          done.resolve();
        }
      });

      try {
        const completed = await waitWithTimeout(done.promise, RECEIVE_TIMEOUT_MS);

        const subscribeLatencyMs = firstAt > 0n
          ? elapsedMs(subT0, firstAt)
          : 0;

        const drainMs = firstAt > 0n && lastAt > 0n
          ? elapsedMs(firstAt, lastAt)
          : 0;

        console.log(
          `│  pub             ${fmtCount(MSGS)} in ${pubElapsedMs.toFixed(2)}ms  →  ${fmtRate(MSGS, pubElapsedMs / 1000)
          }`,
        );

        console.log(`│  subscribe→1st  ${subscribeLatencyMs.toFixed(1)} ms`);

        console.log(
          `│  drain           ${fmtCount(received)} in ${drainMs.toFixed(2)}ms  →  ${fmtRate(received, drainMs / 1000)
          }`,
        );

        if (!completed || received < MSGS) {
          console.log(`│  WARN only ${received}/${MSGS} received (${RECEIVE_TIMEOUT_MS / 1000}s timeout)`);
        }

        printBoxEnd();
      } finally {
        try {
          sub.close();
        } catch {
          // Ignore close races.
        }
      }
    });
  } finally {
    await closeClient(client);
  }
}

// ── Main ──────────────────────────────────────────────────────────────────

function getRuns(): Array<() => Promise<void>> {
  switch (MODE) {
    case 'pubsub-single':
      return [() => runPubSub('pubsub-single', false)];

    case 'pubsub-batch':
      return [() => runPubSub('pubsub-batch', true)];

    case 'reply-single':
      return [() => runReply('reply-single', false)];

    case 'reply-batch':
      return [() => runReply('reply-batch', true)];

    case 'pubsub':
      return [
        () => runPubSub('pubsub-single', false),
        () => runPubSub('pubsub-batch', true),
      ];

    case 'reply':
      return [
        () => runReply('reply-single', false),
        () => runReply('reply-batch', true),
      ];

    case 'all':
      return [
        () => runPubSub('pubsub-single', false),
        () => runPubSub('pubsub-batch', true),
        () => runReply('reply-single', false),
        () => runReply('reply-batch', true),
      ];

    default:
      throw new Error(`invalid --mode "${MODE}"`);
  }
}

async function main(): Promise<void> {
  console.log(`arbitro-ts throughput bench  ─  ${ADDR}`);

  const runs = getRuns();

  for (const run of runs) {
    await run();
  }
}

main().catch((err) => {
  console.error('\nbench failed:', err);
  process.exitCode = 1;
});