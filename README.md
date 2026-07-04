# @arbitro/client

Official TypeScript client for the [Arbitro](https://github.com/arbitro-io/arbitro) message broker.

## Requirements

- Node.js `>= 20`
- Arbitro broker reachable on `127.0.0.1:9898`

## Install

```bash
npm install @arbitro/client
```

## Run the Broker (Docker)

```bash
docker run --rm -p 9898:9898 ghcr.io/arbitro-io/arbitro-server:latest
```

Pin a version tag for production:

- `ghcr.io/arbitro-io/arbitro-server:0.5.3` -- immutable release tag
- `ghcr.io/arbitro-io/arbitro-server:0.5`   -- auto-updates within `0.5.*`
- `ghcr.io/arbitro-io/arbitro-server:latest` -- latest tagged release

## Quick Start

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

await client.publish('orders', 'orders.new', Buffer.from('hello'))
```

## Publish

```typescript
await client.publish('orders', 'orders.new', data)            // wait for broker ack
client.publish('orders', 'orders.new', data)                  // fire-and-forget
client.publish('orders', 'orders.new', data).catch(onError)   // async error path

// With dedup (idempotency)
await client.publish('orders', 'orders.new', data, { msgId: 'order-abc-123' })
```

## Publish with Headers

Attach arbitrary key-value metadata to messages. Headers are persisted alongside the payload and stripped on delivery -- consumers always receive only the user payload.

```typescript
await client.publish('orders', 'orders.created', data, {
  headers: {
    'trace-id': 'abc-123',
    'source': 'checkout-svc',
  },
})

// Headers + dedup
await client.publish('orders', 'orders.created', data, {
  msgId: 'order-abc-123',
  headers: { priority: 'high', region: 'us-east-1' },
})

// Batch with headers
await client.publishBatch('orders', [
  { subject: 'orders.a', data: payloadA, headers: { priority: 'high' } },
  { subject: 'orders.b', data: payloadB, headers: { priority: 'low' } },
])
```

Headers use a zero-copy TLV wire format -- no serialization overhead. The broker persists them with the entry and strips them at delivery time.

## Delayed Publish

```typescript
await client.publishDelayed('orders', 'orders.reminder', payload, 5000) // 5s delay
```

## Durable Management

```typescript
await client.streamExists('orders')           // true
await client.getStreamInfo('orders')          // StreamInfo | null
await client.listStreams()                    // StreamInfo[]

await client.consumerExists('workers')        // true
await client.getConsumerInfo('workers')       // ConsumerInfo | null
await client.listConsumers()                  // ConsumerInfo[]
```

## Upsert / Delete

```typescript
await client.upsertStream('orders', { subjectFilter: 'orders.>' })
await client.upsertConsumer('orders', { name: 'workers', filter: 'orders.>' })

await client.deleteConsumer('workers')
await client.deleteStream('orders')
await client.deleteStream('orders', { deleteData: false })

await client.deleteMessage('orders', 42n)
await stream.deleteMessage(42n)
await consumer.deleteMessage(42n)
```

## Stream / Consumer Sugar

```typescript
const stream = client.stream('orders')
const consumer = stream.consumer({ name: 'workers', filter: 'orders.>' })

await consumer.create()

const sub = await consumer.subscribe((msg) => {
  msg.ack()
})
```

## Per-Subject Inflight Limits

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

## Query Pending Acks

```typescript
const consumer = await client.stream('orders')
  .consumer({ name: 'workers' })
  .create()
await consumer.getPendings()

await client.getPending(consumerId)
await client.getPending('orders', 'workers')
```

## Client Metrics

```typescript
const snap = client.metrics()
// {
//   publishesSent, publishBatchEntries, deliveriesReceived,
//   activeSubscriptions, acksSent, nacksSent, reconnects, pendingReplies
// }
```

## Typed Lazy Decode

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

### Zod Codec (optional)

```typescript
import { ArbitroClient, zodCodec } from '@arbitro/client'
import { z } from 'zod'

const Order = z.object({ id: z.number(), status: z.string() })
const codec  = zodCodec(Order)

const sub = await client
  .stream('orders')
  .consumer({ name: 'workers', filter: 'orders.>' })
  .subscribe(codec, (msg) => {
    msg.ack()
  })
```

## Cron Scheduling

Distributed cron jobs with queue semantics -- multiple workers, single delivery per fire.

```typescript
const cron = await client.cron("billing-monthly")
    .every("0 0 1 * *")
    .tz("America/New_York")
    .run(async (ctx) => {
        console.log(`fire #${ctx.fireCount} at ${ctx.fireTime}`);
        await processBilling();
    });

await cron.stop();
```

Crons re-register automatically on reconnect.

## Service (Request/Reply RPC)

Build named services with automatic stream/consumer creation, handler dispatch, and correlated request/reply.

```typescript
import { ArbitroClient, Service, ServiceBuilder } from '@arbitro/client'

const client = new ArbitroClient({ servers: ['127.0.0.1:9898'] })
await client.connect()

// Build a service — creates backing stream + consumer automatically
const svc = await client.service('calculator').setMaxInflight(1024).build()

// Register method handlers
svc.handle('add', (msg) => {
  const result = compute(msg.data())
  msg.reply(Buffer.from(`sum=${result}`))
  msg.ack()
})

svc.handle('multiply', (msg) => {
  msg.reply(Buffer.from(`product=${computeMul(msg.data())}`))
  msg.ack()
})

// Send a request to another service (or self)
const response = await svc.request('calculator', 'add', Buffer.from('2+3'), 5000)
console.log(response.toString()) // "sum=5"

// Fire-and-forget
await svc.send('audit', 'log', Buffer.from('event-data'))

// Cross-service RPC
const gateway = await client.service('gateway').build()
const resp = await gateway.request('calculator', 'multiply', Buffer.from('3*4'), 5000)
```

`msg.reply()` always works -- no need to check for reply_to presence.

## Workflow Orchestration

Client-side workflow pipelines over Arbitro streams. The broker has no workflow-specific code -- everything uses streams, consumer groups, and idempotent publish.

### WorkflowBuilder API

| Method | Signature | Description |
|--------|-----------|-------------|
| `trigger` | `(subject: string) => this` | Subject pattern that triggers new instances. |
| `triggerStream` | `(streamName: string) => this` | Auto-subscribe to an external stream for trigger. |
| `source` | `(streamName: string, subject: string) => this` | External stream as event source. |
| `step` | `(name: string, handler: StepHandler) => this` | Append a processing step. |
| `suspendStep` | `(name: string, timeoutMs: number, run: SuspendRunHandler, onResume: ResumeHandler) => this` | Step that can suspend and wait for external resume. |
| `onTimeout` | `(handler: TimeoutHandler) => this` | Timeout handler for the preceding suspend step. |
| `compensate` | `(name: string, handler: StepHandler) => this` | Rollback handler per step (saga pattern). |
| `maxRetries` | `(n: number) => this` | Attempts before DLQ (default: 3). |
| `maxContextSize` | `(bytes: number) => this` | Max context payload in bytes (default: 256 KB). |
| `ackWait` | `(ms: number) => this` | Ack timeout for failover (default: 30000). |
| `inflight` | `(n: number) => this` | Concurrent tasks per worker (default: 10). |
| `start` | `() => Promise<WorkflowHandle>` | Register streams, consumer, and start processing. |

### WorkflowHandle API

| Method | Signature | Description |
|--------|-----------|-------------|
| `trigger` | `(client, context: Buffer) => Promise<number>` | Trigger a new workflow instance. Returns the instance ID. |
| `triggerWithId` | `(client, id: string, context: Buffer) => Promise<void>` | Trigger with an explicit instance ID (dedup-safe). |
| `resume` | `(client, instanceId: string, payload: Buffer) => Promise<void>` | Resume a suspended workflow instance. |
| `cancel` | `(client, instanceId: string) => Promise<void>` | Cancel a running or suspended instance. |
| `name` | `string` (getter) | Workflow name. |

### Basic Example

```typescript
import { ArbitroClient, WorkflowBuilder } from '@arbitro/client'
import type { StepContext, StepResult } from '@arbitro/client'

const client = new ArbitroClient({ servers: ['127.0.0.1:9898'] })
await client.connect()

const wf = await new WorkflowBuilder(client, 'order-process')
  .trigger('orders.created')
  .step('validate', async (ctx: StepContext): Promise<StepResult> => {
    const validated = await validateOrder(ctx.context)
    return { context: validated }
  })
  .step('charge', async (ctx: StepContext): Promise<StepResult> => {
    const receipt = await chargePayment(ctx.context)
    return { context: receipt }
  })
  .step('ship', async (ctx: StepContext): Promise<StepResult> => {
    const tracking = await createShipment(ctx.context)
    return { context: tracking }
  })
  .ackWait(30_000)
  .inflight(10)
  .start()

const instanceId = await wf.trigger(client, Buffer.from('order-123-payload'))
```

### Suspend / Resume / Cancel

```typescript
import type { StepOutcome, ResumeContext, TimeoutContext } from '@arbitro/client'

const wf = await new WorkflowBuilder(client, 'payment-auth')
  .trigger('payments.initiated')
  .step('prepare', async (ctx) => {
    return { context: await preparePayment(ctx.context) }
  })
  .suspendStep('wait-auth', 30_000,
    async (ctx): Promise<StepOutcome> => {
      const state = await sendAuthLink(ctx.context)
      return { kind: 'suspend', state, timeoutMs: 30_000 }
    },
    async (resume: ResumeContext) => {
      return { context: await processPaymentResult(resume.state, resume.event) }
    }
  )
  .onTimeout(async (timeout: TimeoutContext) => {
    return { context: await cancelPaymentAuth(timeout.state) }
  })
  .step('finalize', async (ctx) => {
    return { context: await finalizePayment(ctx.context) }
  })
  .start()

// Trigger with explicit ID (dedup-safe)
await wf.triggerWithId(client, 'payment-abc-123', Buffer.from(payload))

// ... later, Stripe webhook confirms payment ...
await wf.resume(client, 'payment-abc-123', Buffer.from(stripeEvent))

// Or cancel a suspended instance
await wf.cancel(client, 'payment-abc-123')
```

### Source (External Stream Triggers)

```typescript
const wf = await new WorkflowBuilder(client, 'event-driven')
  .source('external-events', 'events.>')
  .step('process', async (ctx) => {
    return { context: await processEvent(ctx.context) }
  })
  .start()
```

## Replication

Replication is transparent to the client -- `replicas` is set at `createStream` time. The client publishes normally; the broker handles replication internally.

## License

MIT -- see [LICENSE](./LICENSE).
