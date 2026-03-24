# arbitro-ts

Official TypeScript client for the [Arbitro](../arbitro) stateful flow broker.

## Requirements

- Node.js ≥ 20
- Arbitro broker running on `127.0.0.1:9898`

## Install

```bash
npm install arbitro-ts
```

## Quick start

```typescript
import { ArbitroClient } from 'arbitro-ts'

const client = new ArbitroClient({ servers: ['127.0.0.1:9898'] })
await client.connect()

await client.createStream('orders', { subjectFilter: 'orders.>' })
await client.createConsumer('orders', { name: 'workers', filter: 'orders.>' })

const sub = await client.subscribe('workers', (msg) => {
  console.log(msg.data().toString())
  msg.ack()
})

client.publish('orders.new', Buffer.from('hello'))
```

## Stream / consumer sugar

```typescript
const sub = await client
  .stream('orders')
  .consumer({ name: 'workers', filter: 'orders.>' })
  .subscribe((msg) => { msg.ack() })
```

## Schema codec

```typescript
import { schema } from 'arbitro-ts'

const OrderCodec = schema({ id: 'number', status: 'string' })

const sub = await client
  .stream('orders')
  .consumer({ name: 'workers', filter: 'orders.>' })
  .subscribe(OrderCodec, (msg) => {
    console.log(msg.id, msg.status)  // typed, lazy-decoded
    msg.ack()
  })
```

## Tests

```bash
# start the broker first
./arbitro &
npm test
```
