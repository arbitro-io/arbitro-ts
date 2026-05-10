import * as net from 'net'
import * as tls from 'tls'
import { Unpackr } from 'msgpackr'
import { Framer } from '../proto/framer'
import { pack } from '../proto/codec'
import {
  Action, Flags,
  HEADER_SIZE, OFF_SEQUENCE, OFF_TIMESTAMP,
} from '../proto/constants'
import { ArbitroError, type BrokerError } from '../types/error'
import type { TlsConfig, ReconnectConfig } from '../types/config'
import { resolveLogger } from '../common/logger'
import type { Logger } from '../common/logger'

type DeliveryHandler = (frame: Buffer) => void
const unpackr = new Unpackr({ structuredClone: false, useRecords: false })

interface PendingMgmt {
  resolve: (frame: Buffer) => void
  reject:  (err: Error) => void
}

interface PendingRequest {
  resolve: (frame: Buffer) => void
  reject:  (err: Error) => void
}

interface ActiveSubscription {
  streamName: string
  configData: Buffer
  handler:    DeliveryHandler
  onRenew:    ((newSubId: bigint) => void) | undefined
}

function parseAddr(addr: string): { host: string; port: number } {
  const i = addr.lastIndexOf(':')
  return i === -1
    ? { host: addr, port: 9898 }
    : { host: addr.slice(0, i), port: parseInt(addr.slice(i + 1), 10) }
}

export class Connection {
  private seq             = 1n
  private framer          = new Framer()
  private routes          = new Map<bigint, DeliveryHandler>()
  private mgmtQ:          PendingMgmt[]                    = []
  private pendingRequests = new Map<bigint, PendingRequest>()
  private socket:         net.Socket
  private closing         = false
  private readonly log:   Logger
  private activeSubs      = new Map<bigint, ActiveSubscription>()

  private constructor(
    socket: net.Socket,
    private readonly reconnectAddr?: { host: string; port: number },
    private readonly reconnectCfg?:  ReconnectConfig,
    logger?: Logger,
  ) {
    this.log    = resolveLogger(logger)
    this.socket = socket
    this.attachSocket(socket)
  }

  private attachSocket(socket: net.Socket): void {
    socket.setNoDelay(true)
    socket.on('data',  (chunk: Buffer) => this.framer.push(chunk, (f) => this.onFrame(f)))
    socket.on('error', (err)           => this.drain(err))
    socket.on('close', ()              => this.handleClose())
  }

  static connect(
    addr:         string,
    timeoutMs   = 5_000,
    tlsCfg?:      TlsConfig,
    reconnectCfg?: ReconnectConfig,
    logger?:      Logger,
  ): Promise<Connection> {
    const parsed = parseAddr(addr)
    const { host, port } = parsed
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new ArbitroError('connect timeout', 'connect')),
        timeoutMs,
      )
      const done = (socket: net.Socket) => {
        clearTimeout(timer)
        const conn = new Connection(socket, parsed, reconnectCfg, logger)
        conn.log.info({ host, port }, 'arbitro connected')
        resolve(conn)
      }
      const fail = (e: Error) => { clearTimeout(timer); reject(e) }

      if (tlsCfg) {
        const s = tls.connect({
          host, port,
          ca:                 tlsCfg.ca,
          cert:               tlsCfg.cert,
          key:                tlsCfg.key,
          rejectUnauthorized: true,
        })
        s.once('secureConnect', () => done(s))
        s.once('error', fail)
      } else {
        const s = net.createConnection({ host, port })
        s.once('connect', () => done(s))
        s.once('error', fail)
      }
    })
  }

  nextSeq(): bigint { return this.seq++ }

  // ── Frame routing ─────────────────────────────────────────────────────────

  private onFrame(frame: Buffer): void {
    // switch compiles to a jump table in V8 — O(1) dispatch regardless of action.
    switch (frame.readUInt16LE(6) as Action) {
      case Action.RepOk: {
        this.mgmtQ.shift()?.resolve(frame)
        return
      }
      case Action.RepError: {
        const payload = frame.subarray(HEADER_SIZE)
        let broker: BrokerError | undefined
        try {
          broker = unpackr.unpack(payload) as BrokerError
        } catch {}
        const msg = broker?.message ?? (payload.toString('utf8') || 'server error')
        const err = new ArbitroError(msg, 'server', broker?.name, broker?.details)
        // RepError does NOT echo client_seq — use FIFO matching only.
        this.mgmtQ.shift()?.reject(err)
        return
      }
      case Action.RepReply: {
        // sequence was rewritten by the broker to publisher_seq for correlation.
        const seq = frame.readBigUInt64LE(OFF_SEQUENCE)
        const p   = this.pendingRequests.get(seq)
        if (p) { this.pendingRequests.delete(seq); p.resolve(frame) }
        return
      }
      case Action.RepMessage: {
        // sub_id is patched into the timestamp field by the server drain thread.
        const subId   = frame.readBigUInt64LE(OFF_TIMESTAMP)
        const handler = this.routes.get(subId)
        if (!handler) {
          this.log.warn({ subId: subId.toString() }, 'delivery frame for unknown subId')
          return
        }
        handler(frame)
        return
      }
      default: {
        // Unknown action — surface as error, never silent drop (invariant #3).
        const action = frame.readUInt16LE(6)
        this.log.error({ action: `0x${action.toString(16)}` }, 'unknown action received')
        this.drain(new ArbitroError(`unknown frame action 0x${action.toString(16)}`, 'protocol'))
      }
    }
  }

  // ── Subscriptions ─────────────────────────────────────────────────────────

  // Send PubSubscribe, register the delivery route, and track for reconnect.
  // subject = stream name, data = msgpack consumer config (broker protocol).
  async sendSubscribe(
    streamName: string,
    configData: Buffer,
    handler:    DeliveryHandler,
    onRenew?:   (newSubId: bigint) => void,
  ): Promise<bigint> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new ArbitroError('subscribe timeout: no RepOk from server', 'timeout')),
        5_000,
      )
      this.mgmtQ.push({
        resolve: (frame) => {
          clearTimeout(timer)
          const subId = frame.readBigUInt64LE(OFF_TIMESTAMP)
          // Install the delivery route before yielding back to the caller.
          // This closes the race where the server sends RepOk + first delivery
          // in the same TCP burst during replay.
          this.routes.set(subId, handler)
          this.activeSubs.set(subId, { streamName, configData, handler, onRenew })
          resolve(subId)
        },
        reject: (err) => {
          clearTimeout(timer)
          reject(err)
        },
      })
      this.socket.write(pack({
        action:  Action.PubSubscribe,
        flags:   Flags.None,
        seq:     this.nextSeq(),
        subject: streamName,
        data:    configData,
      }))
    })
  }

  // Remove the delivery route and cancel reconnect tracking.
  cancelSubscription(subId: bigint): void {
    this.routes.delete(subId)
    this.activeSubs.delete(subId)
  }

  // Re-establish all active subscriptions after a reconnect.
  private resubscribeAll(): void {
    const subs = [...this.activeSubs.values()]
    this.activeSubs.clear()
    this.routes.clear()
    for (const { streamName, configData, handler, onRenew } of subs) {
      this.sendSubscribe(streamName, configData, handler, onRenew)
        .then((newId) => { if (onRenew) onRenew(newId) })
        .catch(() => {})
    }
  }

  // ── Routes (internal use) ─────────────────────────────────────────────────

  registerRoute(subId: bigint, handler: DeliveryHandler): void {
    this.routes.set(subId, handler)
  }

  unregisterRoute(subId: bigint): void {
    this.routes.delete(subId)
  }

  // ── Write ─────────────────────────────────────────────────────────────────

  send(frame: Buffer): void {
    this.socket.write(frame)
  }

  // Send frame and wait for RepOk. Returns the server_seq (timestamp field).
  sendExpectReply(frame: Buffer, timeoutMs = 5_000): Promise<bigint> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new ArbitroError('subscribe timeout: no RepOk from server', 'timeout')),
        timeoutMs,
      )
      this.mgmtQ.push({
        resolve: (f) => { clearTimeout(timer); resolve(f.readBigUInt64LE(OFF_TIMESTAMP)) },
        reject:  (e) => { clearTimeout(timer); reject(e) },
      })
      this.socket.write(frame)
    })
  }

  // Send frame and wait for RepOk with data. Returns data bytes after header.
  sendExpectReplyData(frame: Buffer, timeoutMs = 5_000): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new ArbitroError('request timeout: no RepOk from server', 'timeout')),
        timeoutMs,
      )
      this.mgmtQ.push({
        resolve: (f) => { clearTimeout(timer); resolve(f.subarray(HEADER_SIZE)) },
        reject:  (e) => { clearTimeout(timer); reject(e) },
      })
      this.socket.write(frame)
    })
  }

  // Send a request and wait for RepReply. Resolves with the raw reply frame.
  sendRequest(seq: bigint, frame: Buffer, timeoutMs: number): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(seq)
        reject(new ArbitroError('request timeout: no reply received', 'timeout'))
      }, timeoutMs)
      this.pendingRequests.set(seq, {
        resolve: (f) => { clearTimeout(timer); resolve(f) },
        reject:  (e) => { clearTimeout(timer); reject(e) },
      })
      this.socket.write(frame)
    })
  }

  async requestMsgpack<T>(seq: bigint, frame: Buffer, timeoutMs: number): Promise<T> {
    const data = await this.sendExpectReplyData(frame, timeoutMs)
    return unpackr.unpack(data) as T
  }

  sendUnsubscribe(streamName: string, subId: bigint): void {
    this.socket.write(pack({
      action:    Action.PubUnsubscribe,
      seq:       subId,
      subject:   streamName,
      data:      Buffer.alloc(0),
    }))
  }

  sendAck(streamName: string, subId: bigint, msgSeq: bigint): void {
    this.socket.write(pack({
      action:    Action.RepAck,
      seq:       subId,
      timestamp: msgSeq,
      subject:   streamName,
      data:      Buffer.alloc(0),
    }))
  }

  sendNack(streamName: string, subId: bigint, msgSeq: bigint): void {
    this.socket.write(pack({
      action:    Action.RepNack,
      seq:       subId,
      timestamp: msgSeq,
      subject:   streamName,
      data:      Buffer.alloc(0),
    }))
  }

  sendNackDelay(streamName: string, subId: bigint, msgSeq: bigint, delayMs: number): void {
    const data = Buffer.allocUnsafe(4)
    data.writeUInt32LE(delayMs, 0)
    this.socket.write(pack({
      action:    Action.RepNack,
      seq:       subId,
      timestamp: msgSeq,
      subject:   streamName,
      data,
    }))
  }

  // Send a reply to a request-reply message. msgSeq must be the journal_seq from the received frame.
  sendReply(msgSeq: bigint, data: Buffer): void {
    this.socket.write(pack({
      action:  Action.RepReply,
      seq:     msgSeq,
      subject: Buffer.alloc(0),
      data,
    }))
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  close(): Promise<void> {
    this.closing = true
    return new Promise((resolve) => this.socket.end(resolve))
  }

  private drain(err: Error): void {
    const pending = this.mgmtQ.splice(0)
    for (const p of pending) p.reject(err)
    for (const [, p] of this.pendingRequests) p.reject(err)
    this.pendingRequests.clear()
  }

  private handleClose(): void {
    this.log.debug('arbitro connection closed')
    this.drain(new ArbitroError('connection closed', 'closed'))
    const cfg = this.reconnectCfg
    if (!this.closing && cfg && cfg.enabled !== false && this.reconnectAddr) this.tryReconnect(0)
  }

  private tryReconnect(attempt: number): void {
    const cfg = this.reconnectCfg
    if (!cfg) return
    const max    = cfg.maxAttempts ?? 10
    if (attempt >= max) {
      this.log.warn({ attempt }, 'reconnect exhausted — giving up')
      return
    }
    const base   = cfg.intervalMs ?? 500
    const jitter = cfg.jitter !== false ? Math.random() * 100 : 0
    const delay  = Math.min(base * 2 ** attempt, 30_000) + jitter
    this.log.debug({ attempt, delayMs: Math.round(delay) }, 'reconnecting')
    setTimeout(() => {
      const { host, port } = this.reconnectAddr!
      const socket = net.createConnection({ host, port })
      socket.once('connect', () => {
        this.log.info({ host, port, attempt }, 'arbitro reconnected')
        this.framer = new Framer()
        this.socket = socket
        this.attachSocket(socket)
        this.resubscribeAll()
      })
      socket.once('error', () => this.tryReconnect(attempt + 1))
    }, delay)
  }
}
