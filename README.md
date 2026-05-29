# @arbitro/client

Official TypeScript client for the [Arbitro](https://github.com/arbitro-io/arbitro) stateful flow broker.

> Status: under active development. APIs, benchmarks, defaults, and reconnect behavior may still change.

`@arbitro/client` is built for the features that make Arbitro different from a plain pub/sub broker:

- durable streams and consumers
- exact `stream` / `consumer` introspection and idempotent `upsert`
- automatic reconnect + subscription reattach
- subject-level `maxSubjectInflights` — the strongest flow-control feature in the system
- live `ack_pending` queries per consumer
- client + broker metrics for observability

## Why Arbitro

The headline feature is **`maxSubjectInflights`** — per-subject in-flight caps with wildcard patterns inside a single consumer group, so one hot subject does not starve the rest of the workload.

That means you can run one worker pool and still say:

- `payments.critical`  → max `1`
- `payments.heavy.>`   → max `3`
- `payments.light.>`   → max `10`

without splitting your topology into many queues just to protect fairness.

## Requirements

- Node.js `>= 20`
- Arbitro broker reachable on `127.0.0.1:9898` or your own `--addr`

## Install

```bash
npm install @arbitro/client
```

## Run the broker locally (Docker)

The broker ships as a public Docker image (musl-static, ~3 MB, scratch base):

```bash
docker run --rm -p 9898:9898 ghcr.io/arbitro-io/arbitro-server:latest
```

Pin a major/minor tag for production:

- `ghcr.io/arbitro-io/arbitro-server:0.1.0` — immutable release tag
- `ghcr.io/arbitro-io/arbitro-server:0.1`   — auto-updates within `0.1.*`
- `ghcr.io/arbitro-io/arbitro-server:latest` — latest tagged release

## Quick start

```typescript
import { ArbitroClient } from '@arbitro/client'

const client = new ArbitroClient({ servers: ['127.0.0.1:9898'] })
await client.connect()

await client.createStream('orders', {
  subjectFilter: 'orders.>',
})

await client.createConsumer('orders', {
  name: 'workers',
  filter: 'orders.>',
})

const sub = await client.subscribe('workers', (msg) => {
  console.log(msg.data().toString())
  msg.ack()
})

// publish() returns Promise<void> — caller decides whether to await.
await client.publish('orders', 'orders.new', Buffer.from('hello'))
```

## Publish

`publish()` returns `Promise<void>` that resolves once the broker confirms receipt (`RepOk`). The TypeScript idiom is "everything async, the caller chooses to await" — the same call site supports both semantics:

```typescript
await client.publish('orders', 'orders.new', data)            // wait for broker ack
client.publish('orders', 'orders.new', data)                  // fire-and-forget
client.publish('orders', 'orders.new', data).catch(onError)   // async error path
```

The broker emits `RepOk` regardless of whether the caller awaits, so there's no wire-level savings from a "no-reply" variant. That's the property that lets TS expose a single, ergonomic API. Rust's lazy-future model has to pick `publish` (fire-and-forget) vs `publish_sync` (awaited) at the call site.

## Durable management

```typescript
await client.streamExists('orders')           // true
await client.getStreamInfo('orders')          // StreamInfo | null
await client.listStreams()                    // StreamInfo[]

await client.consumerExists('workers')        // true
await client.getConsumerInfo('workers')       // ConsumerInfo | null
await client.listConsumers()                  // ConsumerInfo[]
```

## Upsert / delete

`upsert*` is strict: it succeeds when the entity does not exist or already exists with an equivalent config. It does not silently mutate a conflicting durable entity.

```typescript
await client.upsertStream('orders', { subjectFilter: 'orders.>' })
await client.upsertConsumer('orders', { name: 'workers', filter: 'orders.>' })

await client.deleteConsumer('workers')
await client.deleteStream('orders')                        // default: delete metadata + data
await client.deleteStream('orders', { deleteData: false }) // preserve journal bytes
```

## Stream / consumer sugar

```typescript
const stream = client.stream('orders')
const consumer = stream.consumer({ name: 'workers', filter: 'orders.>' })

await consumer.create()

const sub = await consumer.subscribe((msg) => {
  msg.ack()
})
```

## Per-subject inflight limits

`maxSubjectInflights` caps the in-flight (delivered, unacked) count per subject pattern, with full wildcard support (`*`, `>`). Only enforced when `ackPolicy: Explicit`; silently dropped for fire-and-forget consumers (the engine doesn't track inflight without acks).

```typescript
import { AckPolicy, DeliverPolicy } from '@arbitro/client'

await client.createConsumer('orders', {
  name: 'workers',
  filter: 'orders.>',
  ackPolicy: AckPolicy.Explicit,
  deliverPolicy: DeliverPolicy.All,
  maxAckPending: 20_000,
  maxSubjectInflights: [
    { pattern: 'orders.critical',  limit: 1 },
    { pattern: 'orders.heavy.>',   limit: 3 },
    { pattern: 'orders.light.>',   limit: 10 },
  ],
})
```

## Query pending acks

Live count of messages delivered to a consumer but not yet acked (equivalent of NATS JetStream `num_ack_pending`). One broker round-trip; engine cost is O(1) per shard.

```typescript
// Via Consumer wrapper
const consumer = await client.stream('orders')
  .consumer({ name: 'workers' })
  .create()
await consumer.getPendings()                     // number

// Or directly via client (when you only have the id, or by name)
await client.getPending(consumerId)              // number
await client.getPending('orders', 'workers')     // number (resolves id by name)
```

## Client metrics

The client tracks atomic counters readable via `client.metrics()`. Use it as a saturation gauge for dashboards or alerts.

```typescript
const snap = client.metrics()
// {
//   publishesSent:        12048,
//   publishBatchEntries:  3210,
//   deliveriesReceived:   15258,
//   activeSubscriptions:  7,     // gauge
//   acksSent:             15101,
//   nacksSent:            12,
//   reconnects:           0,
//   pendingReplies:       0,
// }
```

## Typed lazy decode

```typescript
import { schema } from '@arbitro/client'

const OrderCodec = schema({ id: 'number', status: 'string' })

const sub = await client
  .stream('orders')
  .consumer({ name: 'workers', filter: 'orders.>' })
  .subscribe(OrderCodec, (msg) => {
    console.log(msg.id, msg.status)
    msg.ack()
  })
```

### Zod codec (optional)

If you already model your payloads with [zod](https://zod.dev), use `zodCodec` for free runtime validation on decode:

```typescript
import { ArbitroClient, zodCodec } from '@arbitro/client'
import { z } from 'zod'

const Order = z.object({ id: z.number(), status: z.string() })
const codec  = zodCodec(Order)

const sub = await client
  .stream('orders')
  .consumer({ name: 'workers', filter: 'orders.>' })
  .subscribe(codec, (msg) => {
    // msg is typed as `z.output<typeof Order>` — validated on decode
    msg.ack()
  })
```

`zod` is an optional peer dependency. `@arbitro/client` references zod only via `import type`, so users who never call `zodCodec` pay zero runtime cost and don't need zod installed.

## Reconnect behavior

The TS client reconnects transport automatically and reattaches active subscriptions after reconnect. That behavior lives in the client, not in the benchmarks. This matters for:

- Docker restarts
- broker failover tests
- chaos scenarios with durable consumers

## Benchmarks

Three primary bench families:

| File | npm script | What it measures |
|---|---|---|
| `benches/throughput.ts` | `npm run bench` | Publish + delivery throughput across modes (fire-and-forget, batch, sync, replay-ack, replay-noack). |
| `benches/limits.ts` | `npm run bench:limit` | Behaviour under `maxSubjectInflights` saturation. |
| `benches/chaos.ts` | `npm run bench:chaos` | Restart / reconnect / persistence under failure injection. |

Examples:

```bash
npx tsx benches/throughput.ts --mode all --msgs 1000
npx tsx benches/throughput.ts --mode pubsub-single --msgs 1000
npx tsx benches/throughput.ts --mode reply-batch --msgs 1000 --batch 128
npx tsx benches/limits.ts
npx tsx benches/chaos.ts --duration 8 --rate 50 --container arbitro-server
```

## Validation

```bash
npm run typecheck
npm test
npm run test:integration   # requires Docker
```

## Documentation

- `CONTRIBUTING.md` — dev setup, branch + commit conventions, PR review.
- `RELEASING.md` — SemVer policy and the npm publish flow.
- `.agent/rules/*.md` — internal coding rules (hot-path discipline, wire protocol, etc.).
- `CLAUDE.md` — index pointing at the rule files.

## License

MIT — see [LICENSE](./LICENSE).
