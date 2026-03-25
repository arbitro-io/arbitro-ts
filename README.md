# arbitro-ts

Official TypeScript client for the [Arbitro](../arbitro) stateful flow broker.

> Status: under active development. APIs, benchmarks, defaults, and reconnect behavior may still change.

`arbitro-ts` is built for the features that make Arbitro different from a plain pub/sub broker:
- durable streams and consumers
- exact `stream` / `consumer` introspection and idempotent `upsert`
- automatic reconnect + subscription reattach in the client
- subject-level `creditRules`, which are the strongest control feature in the system

## Why Arbitro

The best feature in Arbitro is `creditRules`.

They let you cap in-flight work per subject pattern inside a single consumer group, so one hot subject does not starve the rest of the workload.

That means you can run one worker pool and still say:
- `payments.critical` -> max `1`
- `payments.heavy` -> max `3`
- `payments.light` -> max `10`

without splitting your topology into many queues just to protect fairness.

## Requirements

- Node.js `>= 20`
- Arbitro broker reachable on `127.0.0.1:9898` or your own `--addr`

## Install

```bash
npm install arbitro-ts
```

## Quick start

```typescript
import { ArbitroClient } from 'arbitro-ts'

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

client.publish('orders.new', Buffer.from('hello'))
```

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
await client.deleteStream('orders')                    // default: delete metadata + data
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

## Credit rules

```typescript
import { AckPolicy, DeliverPolicy } from 'arbitro-ts'

await client.createConsumer('orders', {
  name: 'workers',
  filter: 'orders.>',
  ackPolicy: AckPolicy.Explicit,
  deliverPolicy: DeliverPolicy.All,
  maxAckPending: 20_000,
  creditRules: [
    { pattern: 'orders.critical', limit: 1 },
    { pattern: 'orders.heavy',    limit: 3 },
    { pattern: 'orders.light',    limit: 10 },
  ],
})
```

## Typed lazy decode

```typescript
import { schema } from 'arbitro-ts'

const OrderCodec = schema({ id: 'number', status: 'string' })

const sub = await client
  .stream('orders')
  .consumer({ name: 'workers', filter: 'orders.>' })
  .subscribe(OrderCodec, (msg) => {
    console.log(msg.id, msg.status)
    msg.ack()
  })
```

## Reconnect behavior

The TS client reconnects transport automatically and reattaches active subscriptions after reconnect. That behavior lives in the client, not in the benchmarks.

This matters for:
- Docker restarts
- broker failover tests
- chaos scenarios with durable consumers

## Benchmarks

`arbitro-ts` now includes three primary benchmark families plus focused scenarios:

- `throughput.ts`
  - `fire-and-forget`
  - `batch-publish`
  - `publish-and-deliver`
  - `fire-and-forget-mt`
  - `replay-ack`
  - `replay-noack`
  - `perf`
- `credit.ts`
  - throughput under `creditRules`
- `chaos.ts`
  - restart / reconnect / persistence validation

Examples:

```bash
npx tsx benches/throughput.ts --mode fire-and-forget --msgs 20000
npx tsx benches/throughput.ts --mode perf --seconds 10 --rate 20000 --container arbitro-server
npx tsx benches/credit.ts --msgs 10000
npx tsx benches/chaos.ts --duration 8 --rate 50 --container arbitro-server
```

## Validation

```bash
npm run typecheck
npm test
```
