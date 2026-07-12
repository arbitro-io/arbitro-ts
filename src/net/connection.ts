import * as net from 'net'
import * as tls from 'tls'
import { Framer } from '../proto/framer'
import { packHello, packSubscribe, packUnsubscribe, packDisconnect, packPing } from '../proto/v2'
import {
  Action, HEADER_SIZE, OFF_ACTION, OFF_SEQ, OFF_MSG_LEN,
} from '../proto/constants'
import { ArbitroError } from '../types/error'
import type { TlsConfig, ReconnectConfig, KeepAliveConfig } from '../types/config'
import { resolveLogger } from '../common/logger'
import type { Logger } from '../common/logger'
import type { ClientMetrics } from '../client/metrics'
import { CronState } from '../cron/cron-state'
import { decodeCronFire, packCronAck, packCreateCron } from '../cron/cron-frame'

type DeliveryHandler = (frame: Buffer) => void

interface PendingMgmt {
  resolve: (frame: Buffer) => void
  reject: (err: Error) => void
}

interface ActiveSubscription {
  consumerId: number
  filter: Buffer
  handler: DeliveryHandler
  onRenew: ((newConsumerId: number) => void) | undefined
}

function parseAddr(addr: string): { host: string; port: number } {
  const i = addr.lastIndexOf(':')
  return i === -1
    ? { host: addr, port: 9898 }
    : { host: addr.slice(0, i), port: parseInt(addr.slice(i + 1), 10) }
}

export class Connection {
  private seq = 1n
  private connId = 0
  private framer = new Framer()
  private routes = new Map<number, DeliveryHandler>()
  private pending = new Map<bigint, PendingMgmt>()
  private socket: net.Socket
  private closing = false
  private readonly log: Logger
  private activeSubs = new Map<number, ActiveSubscription>()
  private metrics?: ClientMetrics
  private cronState?: CronState
  private heartbeatTimer?: ReturnType<typeof setInterval> | undefined
  private lastPongMs = 0

  private constructor(
    socket: net.Socket,
    private readonly reconnectAddr?: { host: string; port: number },
    private readonly reconnectCfg?: ReconnectConfig,
    logger?: Logger,
    private readonly tlsCfg?: TlsConfig,
    private readonly keepAliveCfg?: KeepAliveConfig,
  ) {
    this.log = resolveLogger(logger)
    this.socket = socket
    this.attachSocket(socket)
  }

  private attachSocket(socket: net.Socket): void {
    socket.setNoDelay(true)
    socket.on('data', (chunk: Buffer) => this.framer.push(chunk, (f) => this.onFrame(f)))
    socket.on('error', (err) => this.drain(err))
    socket.on('close', () => this.handleClose())
  }

  static connect(
    addr: string,
    timeoutMs = 5_000,
    tlsCfg?: TlsConfig,
    reconnectCfg?: ReconnectConfig,
    logger?: Logger,
    keepAliveCfg?: KeepAliveConfig,
  ): Promise<Connection> {
    const parsed = parseAddr(addr)
    const { host, port } = parsed
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new ArbitroError('connect timeout', 'connect')),
        timeoutMs,
      )
      Connection.dial(host, port, tlsCfg).then((socket) => {
        clearTimeout(timer)
        const conn = new Connection(socket, parsed, reconnectCfg, logger, tlsCfg, keepAliveCfg)
        socket.write(packHello())
        conn.startHeartbeat()
        conn.log.info({ host, port }, 'arbitro connected (v2)')
        resolve(conn)
      }).catch((e: Error) => { clearTimeout(timer); reject(e) })
    })
  }

  /**
   * Dial a raw socket — plain TCP or TLS depending on `tlsCfg`. Shared by
   * the initial `connect()` and `tryReconnect()` so a reconnect preserves
   * TLS instead of silently downgrading to plaintext.
   */
  private static dial(host: string, port: number, tlsCfg?: TlsConfig): Promise<net.Socket> {
    return new Promise((resolve, reject) => {
      if (tlsCfg) {
        const s = tls.connect({
          host, port,
          servername: tlsCfg.serverName ?? host,
          rejectUnauthorized: tlsCfg.rejectUnauthorized ?? true,
          ca: tlsCfg.ca,
          cert: tlsCfg.cert,
          key: tlsCfg.key,
        })
        s.once('secureConnect', () => resolve(s))
        s.once('error', reject)
      } else {
        const s = net.createConnection({ host, port })
        s.once('connect', () => resolve(s))
        s.once('error', reject)
      }
    })
  }

  nextSeq(): bigint { return this.seq++ }

  /**
   * Attach a metrics sink. Called by `ArbitroClient` after `connect()`.
   * The connection bumps `deliveriesReceived` on every Deliver/RepBatch
   * entry and `reconnects` on successful reconnections. Unset = no-op.
   */
  setMetrics(m: ClientMetrics): void { this.metrics = m }

  /** Attach cron state so the connection can dispatch CronFire frames. */
  setCronState(s: CronState): void { this.cronState = s }

  /** Live count of outstanding pending request-reply slots (gauge). */
  pendingCount(): number { return this.pending.size }

  /** Bump `acksSent` — called by `Message.ack()`. */
  bumpAcksSent(): void { if (this.metrics) this.metrics.acksSent++ }

  /** Bump `nacksSent` — called by `Message.nack()` / `nackDelay()`. */
  bumpNacksSent(): void { if (this.metrics) this.metrics.nacksSent++ }


  // ── Frame routing ─────────────────────────────────────────────────────────
  // Seq-based dispatch: match reply.header.seq → pending request. O(1).

  private resolvePending(frame: Buffer): void {
    const reqSeq = frame.readBigUInt64LE(OFF_SEQ)
    const p = this.pending.get(reqSeq)
    if (!p) return
    this.pending.delete(reqSeq)
    p.resolve(frame)
  }

  private rejectPending(frame: Buffer): void {
    const reqSeq = frame.readBigUInt64LE(OFF_SEQ)
    const p = this.pending.get(reqSeq)
    if (!p) return
    this.pending.delete(reqSeq)
    const errorCode = frame.length >= HEADER_SIZE + 10
      ? frame.readUInt16LE(HEADER_SIZE + 8)
      : 0
    p.reject(new ArbitroError(
      `server error (code=0x${errorCode.toString(16).padStart(4, '0')})`,
      'server', undefined, undefined, errorCode,
    ))
  }

  private onFrame(frame: Buffer): void {
    const action = frame.readUInt16LE(OFF_ACTION) as Action

    switch (action) {
      case Action.RepOk:
      case Action.ListStreams:
      case Action.ListConsumers: {
        this.resolvePending(frame)
        return
      }
      case Action.RepError: {
        this.rejectPending(frame)
        return
      }
      case Action.Deliver: {
        const consumerId = frame.readUInt32LE(HEADER_SIZE)
        const handler = this.routes.get(consumerId)
        if (!handler) {
          this.log.warn({ consumerId }, 'delivery for unknown consumer')
          return
        }
        if (this.metrics) this.metrics.deliveriesReceived++
        handler(frame)
        return
      }
      case Action.RepBatch:
      case Action.FanoutBatch: {
        this.handleBatchDeliver(frame)
        return
      }
      case Action.CronFire: {
        this.dispatchCronFire(frame)
        return
      }
      case Action.Pong: {
        this.lastPongMs = Date.now()
        return
      }
      default: {
        // Silently drop unknown actions (matches Rust client behavior)
        this.log.debug({ action: `0x${action.toString(16)}` }, 'unknown action, dropped')
      }
    }
  }

  private handleBatchDeliver(frame: Buffer): void {
    if (frame.length < HEADER_SIZE + 4) return
    const count = frame.readUInt16LE(HEADER_SIZE)
    let off = HEADER_SIZE + 4

    for (let i = 0; i < count; i++) {
      if (off + 24 > frame.length) break
      const consumerId = frame.readUInt32LE(off)
      const deliverSeq = frame.readBigUInt64LE(off + 4)
      const subjectLen = frame.readUInt16LE(off + 12)
      const replyLen = frame.readUInt16LE(off + 14)
      const dataLen = frame.readUInt32LE(off + 16)
      const subjectHash = frame.readUInt32LE(off + 20)
      off += 24

      const tailEnd = off + dataLen
      if (tailEnd > frame.length) break

      const handler = this.routes.get(consumerId)
      if (handler) {
        const payloadLen = dataLen - subjectLen - replyLen
        const bodyLen = 12 + subjectLen + replyLen + payloadLen
        const single = Buffer.allocUnsafe(HEADER_SIZE + bodyLen)
        single.writeUInt16LE(Action.Deliver, 0)
        single[2] = 0; single[3] = 0
        single.writeUInt32LE(bodyLen, 4)
        single.writeBigUInt64LE(deliverSeq, 8)
        single.writeUInt32LE(consumerId, HEADER_SIZE)
        single.writeUInt32LE(subjectHash, HEADER_SIZE + 4)
        single.writeUInt16LE(subjectLen, HEADER_SIZE + 8)
        single.writeUInt16LE(replyLen, HEADER_SIZE + 10)
        let dst = HEADER_SIZE + 12
        frame.copy(single, dst, off, off + subjectLen); dst += subjectLen
        frame.copy(single, dst, off + subjectLen, off + subjectLen + replyLen); dst += replyLen
        frame.copy(single, dst, off + subjectLen + replyLen, tailEnd)
        if (this.metrics) this.metrics.deliveriesReceived++
        handler(single)
      }
      off = tailEnd
    }
  }

  // ── Cron dispatch ──────────────────────────────────────────────────────────

  private dispatchCronFire(frame: Buffer): void {
    const body = frame.subarray(HEADER_SIZE)
    const view = decodeCronFire(body)
    if (!view) return

    const handler = this.cronState?.getHandler(view.name)
    const nameBuf = Buffer.from(view.name)

    if (!handler) {
      this.send(packCronAck(this.nextSeq(), nameBuf, false))
      return
    }

    // Execute async handler outside sync dispatch; send ack when done.
    handler({ name: view.name, fireTime: view.fireTimeMs, fireCount: view.fireCount })
      .then(() => this.send(packCronAck(this.nextSeq(), nameBuf, true)))
      .catch(() => this.send(packCronAck(this.nextSeq(), nameBuf, false)))
  }

  // ── Subscriptions ─────────────────────────────────────────────────────────

  async sendSubscribeV2(
    consumerId: number,
    filter: Buffer,
    handler: DeliveryHandler,
    onRenew?: (newConsumerId: number) => void,
  ): Promise<number> {
    return new Promise((resolve, reject) => {
      const seq = this.nextSeq()
      const timer = setTimeout(
        () => { this.pending.delete(seq); reject(new ArbitroError('subscribe timeout', 'timeout')) },
        5_000,
      )
      this.pending.set(seq, {
        resolve: (_frame) => {
          clearTimeout(timer)
          this.routes.set(consumerId, handler)
          this.activeSubs.set(consumerId, { consumerId, filter, handler, onRenew })
          resolve(consumerId)
        },
        reject: (err) => { clearTimeout(timer); reject(err) },
      })
      this.socket.write(packSubscribe(seq, this.connId, consumerId, filter))
    })
  }

  cancelSubscription(consumerId: number): void {
    this.routes.delete(consumerId)
    this.activeSubs.delete(consumerId)
    this.socket.write(packUnsubscribe(this.nextSeq(), this.connId, consumerId))
  }

  private resubscribeAll(): void {
    this.socket.write(packHello())
    const subs = [...this.activeSubs.values()]
    this.activeSubs.clear()
    this.routes.clear()
    for (const { consumerId, filter, handler, onRenew } of subs) {
      this.sendSubscribeV2(consumerId, filter, handler, onRenew)
        .then((id) => { if (onRenew) onRenew(id) })
        .catch(() => { })
    }
    this.replayCrons()
  }

  private replayCrons(): void {
    if (!this.cronState) return
    for (const { config } of this.cronState.allConfigs()) {
      const seq = this.nextSeq()
      this.socket.write(packCreateCron(seq, config))
    }
  }

  // ── Routes (internal use) ─────────────────────────────────────────────────

  registerRoute(consumerId: number, handler: DeliveryHandler): void {
    this.routes.set(consumerId, handler)
  }

  unregisterRoute(consumerId: number): void {
    this.routes.delete(consumerId)
  }

  // ── Write ─────────────────────────────────────────────────────────────────

  send(frame: Buffer): void {
    this.socket.write(frame)
  }

  /** Send frame and wait for RepOk. Returns the ref_seq from body. */
  sendExpectReply(frame: Buffer, timeoutMs = 5_000): Promise<bigint> {
    const seq = frame.readBigUInt64LE(OFF_SEQ)
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => { this.pending.delete(seq); reject(new ArbitroError('request timeout', 'timeout')) },
        timeoutMs,
      )
      this.pending.set(seq, {
        resolve: (f) => {
          clearTimeout(timer)
          resolve(f.readBigUInt64LE(HEADER_SIZE))
        },
        reject: (e) => { clearTimeout(timer); reject(e) },
      })
      this.socket.write(frame)
    })
  }

  /** Send frame and wait for full reply frame buffer. */
  sendExpectReplyRaw(frame: Buffer, timeoutMs = 5_000): Promise<Buffer> {
    const seq = frame.readBigUInt64LE(OFF_SEQ)
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => { this.pending.delete(seq); reject(new ArbitroError('request timeout', 'timeout')) },
        timeoutMs,
      )
      this.pending.set(seq, {
        resolve: (f) => { clearTimeout(timer); resolve(f) },
        reject: (e) => { clearTimeout(timer); reject(e) },
      })
      this.socket.write(frame)
    })
  }

  // ── Heartbeat / dead-connection watchdog ──────────────────────────────────
  // Mirrors arbitro-client-tokio/src/conn/heartbeat.rs: a header-only Ping
  // is sent every `intervalMs`; if no Pong lands within `timeoutMs` the
  // socket is destroyed, which routes through the existing close/reconnect
  // path (see `handleClose` / `tryReconnect`).

  private startHeartbeat(): void {
    this.stopHeartbeat()
    const intervalMs = this.keepAliveCfg?.intervalMs ?? 30_000
    const timeoutMs = this.keepAliveCfg?.timeoutMs ?? 60_000
    this.lastPongMs = Date.now()
    this.heartbeatTimer = setInterval(() => {
      if (Date.now() - this.lastPongMs > timeoutMs) {
        this.log.warn({ timeoutMs }, 'heartbeat timeout, dropping connection')
        this.socket.destroy()
        return
      }
      this.socket.write(packPing(this.nextSeq()))
    }, intervalMs)
    this.heartbeatTimer.unref()
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = undefined
    }
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  close(): Promise<void> {
    this.closing = true
    this.stopHeartbeat()
    this.socket.write(packDisconnect(this.nextSeq()))
    return new Promise((resolve) => this.socket.end(resolve))
  }

  private drain(err: Error): void {
    for (const p of this.pending.values()) p.reject(err)
    this.pending.clear()
  }

  private handleClose(): void {
    this.log.debug('arbitro connection closed')
    this.stopHeartbeat()
    this.drain(new ArbitroError('connection closed', 'closed'))
    const cfg = this.reconnectCfg
    if (!this.closing && cfg && cfg.enabled !== false && this.reconnectAddr) {
      this.tryReconnect(0)
    }
  }

  private tryReconnect(attempt: number): void {
    const cfg = this.reconnectCfg
    if (!cfg) return
    const max = cfg.maxAttempts ?? 10
    if (attempt >= max) {
      this.log.warn({ attempt }, 'reconnect exhausted')
      return
    }
    const base = cfg.intervalMs ?? 500
    const jitter = cfg.jitter !== false ? Math.random() * 100 : 0
    const delay = Math.min(base * 2 ** attempt, 30_000) + jitter
    this.log.debug({ attempt, delayMs: Math.round(delay) }, 'reconnecting')
    setTimeout(() => {
      const { host, port } = this.reconnectAddr!
      Connection.dial(host, port, this.tlsCfg).then((socket) => {
        this.log.info({ host, port, attempt }, 'arbitro reconnected')
        this.framer = new Framer()
        this.socket = socket
        this.attachSocket(socket)
        this.startHeartbeat()
        this.resubscribeAll()
        if (this.metrics) this.metrics.reconnects++
      }).catch(() => this.tryReconnect(attempt + 1))
    }, delay)
  }
}
