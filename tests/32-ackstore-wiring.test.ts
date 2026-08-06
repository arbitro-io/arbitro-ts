// Ackstore <-> client wiring: the dedup gate on delivery, the record-on-ack
// write, and the on-connect purge driven by the broker's cursor.
//
// Uses a raw TCP mock server (same trick as 27-ack-batching) so the wire side
// is real bytes without needing a live broker.

import * as net from 'net'
import { describe, it, expect, afterEach } from 'vitest'

import { Connection } from '../src/net/connection'
import { Subscription } from '../src/subscription/subscription'
import { Message } from '../src/message/message'
import { Framer } from '../src/proto/framer'
import { HEADER_SIZE, Action } from '../src/proto/constants'
import { ACK_STATUS_OK } from '../src/proto/ackrel'
import { MemoryStore } from '../src/ackstore'
import { ClientMetrics } from '../src/client/metrics'

async function withMockServer<T>(
  fn: (o: { addr: string; frames: () => Buffer[]; push: (f: Buffer) => void }) => Promise<T>,
): Promise<T> {
  const received: Buffer[] = []
  const framer = new Framer()
  const sockets: net.Socket[] = []
  const server = net.createServer((sock) => {
    sockets.push(sock)
    let helloConsumed = false
    sock.on('data', (chunk) => {
      if (!helloConsumed) {
        helloConsumed = true
        chunk = chunk.subarray(8) // raw Hello handshake, not a framed message
        if (chunk.length === 0) return
      }
      framer.push(chunk, (f) => received.push(f))
    })
  })
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
  const port = (server.address() as net.AddressInfo).port
  try {
    return await fn({
      addr: `127.0.0.1:${port}`,
      frames: () => received,
      push: (f) => { for (const s of sockets) s.write(f) },
    })
  } finally {
    for (const s of sockets) s.destroy()
    await new Promise<void>((r) => server.close(() => r()))
  }
}

function waitFor(cond: () => boolean, timeoutMs = 2_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs
    const poll = (): void => {
      if (cond()) return resolve()
      if (Date.now() > deadline) return reject(new Error('waitFor timeout'))
      setTimeout(poll, 10)
    }
    poll()
  })
}

function makeDeliverFrame(seq: bigint, consumerId: number): Buffer {
  const subj = Buffer.from('x')
  const bodyLen = 12 + subj.length
  const buf = Buffer.alloc(HEADER_SIZE + bodyLen)
  buf.writeUInt16LE(Action.Deliver, 0)
  buf.writeUInt32LE(bodyLen, 4)
  buf.writeBigUInt64LE(seq, 8)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(0xbeef, HEADER_SIZE + 4)
  buf.writeUInt16LE(subj.length, HEADER_SIZE + 8)
  buf.writeUInt16LE(0, HEADER_SIZE + 10)
  subj.copy(buf, HEADER_SIZE + 12)
  return buf
}

function makeAckStateRep(consumerId: number, cursor: bigint, status: number): Buffer {
  const buf = Buffer.alloc(HEADER_SIZE + 40)
  buf.writeUInt16LE(Action.AckStateRep, 0)
  buf.writeUInt32LE(40, 4)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(0, HEADER_SIZE + 4) // generation
  buf.writeBigUInt64LE(cursor, HEADER_SIZE + 8)
  buf.writeBigUInt64LE(0n, HEADER_SIZE + 16) // lowSeq
  buf.writeBigUInt64LE(cursor, HEADER_SIZE + 24) // highSeq
  buf.writeUInt32LE(status, HEADER_SIZE + 32)
  return buf
}

const open: Connection[] = []
afterEach(async () => {
  for (const c of open.splice(0)) {
    try { await c.close() } catch { /* already closed */ }
  }
})

describe('delivery dedup gate', () => {
  it('runs the handler once and re-acks the redelivery without running it again', async () => {
    await withMockServer(async ({ addr, frames }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      open.push(conn)
      const metrics = new ClientMetrics()
      conn.setMetrics(metrics)

      const slot = new MemoryStore(0).slot('orders', 'worker')
      const sub = new Subscription(42, conn, 'orders', 5_000, slot)
      let runs = 0
      sub.onMessage((m) => { runs++; m.ack() })

      sub.deliver(makeDeliverFrame(7n, 42))
      expect(runs).toBe(1)
      expect(slot.seen(7n), 'ack() records the seq').toBe(true)

      sub.deliver(makeDeliverFrame(7n, 42)) // broker redelivers: ack was lost
      expect(runs, 'handler must NOT run twice').toBe(1)
      expect(metrics.redeliveriesSkipped).toBe(1)

      // The redelivery is still acked, otherwise it would loop forever. Both
      // acks fire in the same tick, so they collapse into one BatchAck of 2.
      await waitFor(() => frames().some((f) => f.readUInt16LE(0) === Action.BatchAck))
      const batch = frames().find((f) => f.readUInt16LE(0) === Action.BatchAck)!
      expect(batch.readUInt32LE(HEADER_SIZE), 'consumer id').toBe(42)
      expect(batch.readUInt32LE(HEADER_SIZE + 4), 'both seqs acked').toBe(2)
    })
  })

  it('without a slot, delivery behaves exactly as before (no dedup)', async () => {
    await withMockServer(async ({ addr }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      open.push(conn)
      const sub = new Subscription(42, conn, 'orders', 5_000)
      let runs = 0
      sub.onMessage((m) => { runs++; m.ack() })
      sub.deliver(makeDeliverFrame(7n, 42))
      sub.deliver(makeDeliverFrame(7n, 42))
      expect(runs).toBe(2)
    })
  })

  it('nack does not record — a rejected message must come back', async () => {
    await withMockServer(async ({ addr }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      open.push(conn)
      const slot = new MemoryStore(0).slot('orders', 'worker')
      const msg = new Message(
        makeDeliverFrame(9n, 1),
        (f) => conn.send(f), () => conn.nextSeq(),
        undefined, undefined, undefined, undefined,
        (seq) => slot.record(seq),
      )
      msg.nack()
      expect(slot.seen(9n)).toBe(false)
    })
  })
})

describe('on-connect purge', () => {
  it('requestAckState sends an AckStateReq for the consumer', async () => {
    await withMockServer(async ({ addr, frames }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      open.push(conn)
      conn.requestAckState(77)
      await waitFor(() => frames().some((f) => f.readUInt16LE(0) === Action.AckStateReq))
      const req = frames().find((f) => f.readUInt16LE(0) === Action.AckStateReq)!
      expect(req.readUInt32LE(HEADER_SIZE)).toBe(77)
    })
  })

  it('purges the store on an OK AckStateRep', async () => {
    await withMockServer(async ({ addr, push }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      open.push(conn)
      const slot = new MemoryStore(0).slot('orders', 'worker')
      for (let seq = 1n; seq <= 10n; seq++) slot.record(seq)
      conn.onAckConfirm = (_cid, cursor) => { slot.confirmUpTo(cursor) }

      push(makeAckStateRep(5, 6n, ACK_STATUS_OK))
      await waitFor(() => slot.info().live === 4)

      for (let seq = 1n; seq <= 6n; seq++) expect(slot.seen(seq), `${seq} purged`).toBe(false)
      for (let seq = 7n; seq <= 10n; seq++) expect(slot.seen(seq), `${seq} kept`).toBe(true)
    })
  })

  // A wrongly kept entry costs a little disk; a wrongly dropped one costs a
  // duplicate execution of real work. CONSUMER_UNKNOWN keeps everything.
  it('purges NOTHING on a non-OK status', async () => {
    await withMockServer(async ({ addr, push }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      open.push(conn)
      const slot = new MemoryStore(0).slot('orders', 'worker')
      for (let seq = 1n; seq <= 10n; seq++) slot.record(seq)
      conn.onAckConfirm = (_cid, cursor) => { slot.confirmUpTo(cursor) }

      let seen = false
      conn.onAckStateRep = () => { seen = true }
      push(makeAckStateRep(5, 6n, 1 /* CONSUMER_UNKNOWN */))
      await waitFor(() => seen)

      expect(slot.info().live, 'nothing dropped on a non-OK status').toBe(10)
    })
  })

  it('purges on AckBatchResp new_cursor', async () => {
    await withMockServer(async ({ addr, push }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      open.push(conn)
      const slot = new MemoryStore(0).slot('orders', 'worker')
      for (let seq = 1n; seq <= 10n; seq++) slot.record(seq)
      conn.onAckConfirm = (_cid, cursor) => { slot.confirmUpTo(cursor) }

      const buf = Buffer.alloc(HEADER_SIZE + 32)
      buf.writeUInt16LE(Action.AckBatchResp, 0)
      buf.writeUInt32LE(32, 4)
      buf.writeUInt32LE(5, HEADER_SIZE)
      buf.writeBigUInt64LE(8n, HEADER_SIZE + 4) // new_cursor
      push(buf)

      await waitFor(() => slot.info().live === 2)
      expect(slot.seen(9n)).toBe(true)
      expect(slot.seen(8n)).toBe(false)
    })
  })
})
