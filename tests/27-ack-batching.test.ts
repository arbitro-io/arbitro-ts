import * as net from 'net'
import { describe, it, expect, afterEach } from 'vitest'
import { Connection } from '../src/net/connection'
import { Message } from '../src/message/message'
import { Framer } from '../src/proto/framer'
import { HEADER_SIZE, Action } from '../src/proto/constants'

// Spins a raw TCP server (no arbitro protocol handshake needed —
// Connection.connect() resolves on TCP connect, before any reply) so we
// can inspect the exact bytes the client writes without depending on a
// live broker or Docker.
async function withMockServer<T>(
  fn: (opts: { addr: string; frames: () => Buffer[] }) => Promise<T>,
): Promise<T> {
  const received: Buffer[] = []
  const framer = new Framer()
  const sockets: net.Socket[] = []
  const server = net.createServer((sock) => {
    sockets.push(sock)
    let helloConsumed = false
    sock.on('data', (chunk) => {
      // First 8 bytes on the wire are the raw Hello handshake (no
      // standard header) — strip them before feeding the Framer, which
      // only understands the post-handshake header format.
      if (!helloConsumed) {
        helloConsumed = true
        chunk = chunk.subarray(8)
        if (chunk.length === 0) return
      }
      framer.push(chunk, (f) => received.push(f))
    })
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as net.AddressInfo).port
  try {
    return await fn({ addr: `127.0.0.1:${port}`, frames: () => received })
  } finally {
    // `server.close()`'s callback only fires once every accepted
    // connection has ended — the client's socket is normally still open
    // here (it's closed later by the test's own afterEach), so destroy
    // the server-side sockets first to avoid deadlocking the close.
    for (const s of sockets) s.destroy()
    await new Promise<void>((resolve) => server.close(() => resolve()))
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
  buf.writeUInt32LE(0xBEEF, HEADER_SIZE + 4)
  buf.writeUInt16LE(subj.length, HEADER_SIZE + 8)
  buf.writeUInt16LE(0, HEADER_SIZE + 10)
  subj.copy(buf, HEADER_SIZE + 12)
  return buf
}

const openConnections: Connection[] = []
afterEach(async () => {
  for (const c of openConnections.splice(0)) {
    try { await c.close() } catch { /* already closed */ }
  }
})

describe('ack batching — microtask accumulator', () => {
  it('5 acks fired synchronously in the same tick produce ONE BatchAck frame', async () => {
    await withMockServer(async ({ addr, frames }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      openConnections.push(conn)

      for (let i = 0; i < 5; i++) {
        const msg = new Message(
          makeDeliverFrame(BigInt(i + 1), 42),
          (f) => conn.send(f),
          () => conn.nextSeq(),
          () => conn.bumpAcksSent(),
          undefined,
          (cid, seq) => conn.ackRelay.record(cid, seq),
          (cid, subjectHash, seq) => conn.enqueueAck(cid, subjectHash, seq),
        )
        msg.ack()
      }

      // Only one BatchAck frame should ever land, after the microtask flush.
      await waitFor(() => frames().some((f) => f.readUInt16LE(0) === Action.BatchAck))

      const batchFrames = frames().filter((f) => f.readUInt16LE(0) === Action.BatchAck)
      const ackFrames = frames().filter((f) => f.readUInt16LE(0) === Action.Ack)
      expect(batchFrames.length).toBe(1)
      expect(ackFrames.length).toBe(0)

      const body = batchFrames[0]!.subarray(HEADER_SIZE)
      const consumerId = body.readUInt32LE(0)
      const count = body.readUInt32LE(4)
      expect(consumerId).toBe(42)
      expect(count).toBe(5)
    })
  })

  it('a single ack takes the inline single-frame fast path (no BatchAck)', async () => {
    await withMockServer(async ({ addr, frames }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      openConnections.push(conn)

      const msg = new Message(
        makeDeliverFrame(7n, 9),
        (f) => conn.send(f),
        () => conn.nextSeq(),
        () => conn.bumpAcksSent(),
        undefined,
        (cid, seq) => conn.ackRelay.record(cid, seq),
        (cid, subjectHash, seq) => conn.enqueueAck(cid, subjectHash, seq),
      )
      msg.ack()

      await waitFor(() => frames().some((f) => f.readUInt16LE(0) === Action.Ack))

      const ackFrames = frames().filter((f) => f.readUInt16LE(0) === Action.Ack)
      const batchFrames = frames().filter((f) => f.readUInt16LE(0) === Action.BatchAck)
      expect(ackFrames.length).toBe(1)
      expect(batchFrames.length).toBe(0)
      expect(ackFrames[0]!.readUInt32LE(HEADER_SIZE)).toBe(9) // consumer_id
    })
  })

  it('acks for two different consumers in the same tick produce TWO separate frames', async () => {
    await withMockServer(async ({ addr, frames }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      openConnections.push(conn)

      for (const cid of [1, 1, 2, 2, 2]) {
        const msg = new Message(
          makeDeliverFrame(1n, cid),
          (f) => conn.send(f),
          () => conn.nextSeq(),
          undefined, undefined,
          (c, seq) => conn.ackRelay.record(c, seq),
          (c, subjectHash, seq) => conn.enqueueAck(c, subjectHash, seq),
        )
        msg.ack()
      }

      await waitFor(() => frames().filter((f) => f.readUInt16LE(0) === Action.BatchAck).length >= 2)

      const batches = frames().filter((f) => f.readUInt16LE(0) === Action.BatchAck)
      expect(batches.length).toBe(2)
      const byConsumer = new Map(batches.map((f) => [f.subarray(HEADER_SIZE).readUInt32LE(0), f]))
      expect(byConsumer.get(1)!.subarray(HEADER_SIZE).readUInt32LE(4)).toBe(2) // count
      expect(byConsumer.get(2)!.subarray(HEADER_SIZE).readUInt32LE(4)).toBe(3) // count
    })
  })

  it('falls back to AckRelay when the batched send fails (socket destroyed before flush)', async () => {
    await withMockServer(async ({ addr }) => {
      const conn = await Connection.connect(addr, 2_000, undefined, { enabled: false })
      // Not pushed to openConnections — the socket is deliberately killed
      // below, so afterEach's conn.close() would hang waiting on a dead
      // socket's 'close' callback.

      // Force the accumulator to flush against a dead socket.
      for (let i = 0; i < 3; i++) {
        conn.enqueueAck(77, 0xAAAA, BigInt(i + 1))
      }
      // Kill the connection before the microtask flush runs — send() must
      // observe a non-writable/destroyed socket and report failure.
      ;(conn as unknown as { socket: net.Socket }).socket.destroy()

      await waitFor(() => conn.ackRelay.pendingSeqs(77).length === 3)
      expect(conn.ackRelay.pendingSeqs(77).sort()).toEqual([1n, 2n, 3n])
    })
  })
})
