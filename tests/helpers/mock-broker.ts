import * as net from 'net'
import { Framer } from '../../src/proto/framer'
import { FrameView, pack } from '../../src/proto/codec'
import { Action, Flags, HEADER_SIZE } from '../../src/proto/constants'

// Minimal in-process broker for tests.
// Handles PubSubscribe (assigns subId), PubPublish, PubCreateStream, PubCreateConsumer.
// Auto-routes PubPublish to all active subscriptions.
// Expose deliver() to push messages directly to a specific subscriber.
export class MockBroker {
  private readonly server: net.Server
  private sockets: net.Socket[] = []
  private subIdCounter  = 1n
  private lastSubId     = 0n
  private subscriptions = new Map<bigint, net.Socket>()

  // Called for every received frame — useful for assertions in tests.
  onFrame?: (action: number, frame: Buffer) => void

  constructor() {
    this.server = net.createServer((socket) => {
      const framer = new Framer()
      this.sockets.push(socket)
      socket.on('data', (chunk: Buffer) =>
        framer.push(chunk, (frame) => this.handleFrame(socket, frame))
      )
      socket.on('close', () => {
        this.sockets = this.sockets.filter((s) => s !== socket)
        for (const [sid, s] of this.subscriptions) {
          if (s === socket) this.subscriptions.delete(sid)
        }
      })
    })
  }

  private handleFrame(socket: net.Socket, frame: Buffer): void {
    const action = frame.readUInt16LE(6) as Action
    this.onFrame?.(action, frame)

    switch (action) {
      case Action.PubSubscribe: {
        const subId = this.subIdCounter++
        this.lastSubId = subId
        this.subscriptions.set(subId, socket)
        socket.write(repOk(subId))
        break
      }
      case Action.PubPublish: {
        if (!(frame[5]! & Flags.NoAck)) socket.write(repOk(0n))
        // Auto-route to all active subscribers.
        const view = new FrameView(frame)
        const subj = view.subject()
        const data = view.data()
        const seq  = view.seq()
        for (const [subId, subSocket] of this.subscriptions) {
          subSocket.write(pack({ action: Action.PubPublish, flags: Flags.None, seq, timestamp: subId, subject: subj, data }))
        }
        break
      }
      case Action.PubCreateStream:
      case Action.PubCreateConsumer:
      case Action.PubDeleteStream:
      case Action.PubDeleteConsumer:
        socket.write(repOk(0n))
        break
    }
  }

  // Send a delivery frame to all connected clients.
  // The client routes by sub_id (stored in the timestamp field).
  deliver(subId: bigint, subject: string, data: Buffer): void {
    const frame = pack({
      action:    Action.PubPublish,
      flags:     Flags.None,
      seq:       1n,
      timestamp: subId,
      subject,
      data,
    })
    for (const s of this.sockets) s.write(frame)
  }

  // Send a RepError to all connected clients.
  sendError(message: string): void {
    const frame = pack({
      action:  Action.RepError,
      seq:     0n,
      subject: Buffer.alloc(0),
      data:    Buffer.from(message),
    })
    for (const s of this.sockets) s.write(frame)
  }

  getLastSubId(): bigint { return this.lastSubId }

  async start(): Promise<number> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => {
        resolve((this.server.address() as net.AddressInfo).port)
      })
    })
  }

  async stop(): Promise<void> {
    for (const s of this.sockets) s.destroy()
    return new Promise((resolve) => this.server.close(() => resolve()))
  }
}

function repOk(seq: bigint): Buffer {
  return pack({ action: Action.RepOk, seq, subject: Buffer.alloc(0), data: Buffer.alloc(0) })
}
