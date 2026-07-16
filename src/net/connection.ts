import * as net from 'net'
import * as tls from 'tls'
import { Framer } from '../proto/framer'
import {
  packHello, packSubscribe, packUnsubscribe, packDisconnect, packPing,
  packAck, packBatchAck,
} from '../proto/v2'
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
import {
  unpackAckStateRep, unpackAckBatchResp, packAckStateReq, packAckBatch,
  type AckStateRepBody, type AckBatchRespBody,
} from '../proto/ackrel'
import { AckRelay } from '../ackrel'

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
  private ackSweepTimer?: ReturnType<typeof setInterval> | undefined

  /** Ack-reliability hot tier — pending state, generation, reconciliation. */
  readonly ackRelay = new AckRelay()

  // ── Ack batching accumulator (Wave4b) ─────────────────────────────────────
  // Collects Message.ack() calls for one microtask tick, grouped by
  // consumer, then flushes as a single BatchAck frame per consumer
  // (inline single-frame fast path when a consumer only had one ack this
  // tick). Mirrors consume/mod.rs's dedicated batcher tasks, minus the
  // channel — a JS microtask boundary plays the same role as "drain up to
  // 64 commands, group by consumer".
  private ackAccumulator = new Map<number, Array<{ seq: bigint; subjectHash: number }>>()
  private ackFlushScheduled = false

  /** Wired to the ackRelay by default (see below); tests/callers may
   * override to observe raw AckStateRep/AckBatchResp frames instead. */
  onAckStateRep?: (body: AckStateRepBody) => void
  onAckBatchResp?: (body: AckBatchRespBody) => void

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
    this.onAckStateRep = (body) => this.ackRelay.applyAckStateRep(body.consumerId, body.cursor, body.lowSeq)
    this.onAckBatchResp = (body) => this.ackRelay.applyAckBatchResp(body.consumerId, body.newCursor, body.belowRetention)
    this.attachSocket(socket)
    this.startAckSweep()
  }

  private attachSocket(socket: net.Socket): void {
    socket.setNoDelay(true)
    socket.on('data', (chunk: Buffer) => {
      // A malformed/truncated frame (bad msg_len, short body) would otherwise
      // throw synchronously inside `framer.push` / `onFrame` — and a throw in a
      // 'data' listener becomes an `uncaughtException` that KILLS the process.
      // Contain it: log, surface via the socket 'error'/'close' path, and tear
      // the connection down so the reconnect state machine recovers cleanly.
      try {
        this.framer.push(chunk, (f) => this.onFrame(f))
      } catch (err) {
        this.log.error({ err }, 'protocol error decoding inbound frame, dropping connection')
        socket.destroy()
      }
    })
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
  setMetrics(m: ClientMetrics): void { this.metrics = m; this.ackRelay.setMetrics(m) }

  /** Attach cron state so the connection can dispatch CronFire frames. */
  setCronState(s: CronState): void { this.cronState = s }

  /** Live count of outstanding pending request-reply slots (gauge). */
  pendingCount(): number { return this.pending.size }

  /** Bump `acksSent` — called by `Message.ack()`. */
  bumpAcksSent(): void { if (this.metrics) this.metrics.acksSent++ }

  /** Bump `nacksSent` — called by `Message.nack()` / `nackDelay()`. */
  bumpNacksSent(): void { if (this.metrics) this.metrics.nacksSent++ }

  // ── Ack batching ─────────────────────────────────────────────────────────

  /** Queue one ack for `consumerId`/`seq`. Called by `Message.ack()`
   * instead of building+sending an `Ack` frame directly. Flushes on the
   * next microtask so every ack fired synchronously within the same tick
   * (e.g. a loop over a batch delivery) collapses into a single frame. */
  enqueueAck(consumerId: number, subjectHash: number, seq: bigint): void {
    let list = this.ackAccumulator.get(consumerId)
    if (!list) { list = []; this.ackAccumulator.set(consumerId, list) }
    list.push({ seq, subjectHash })
    if (!this.ackFlushScheduled) {
      this.ackFlushScheduled = true
      queueMicrotask(() => this.flushAckAccumulator())
    }
  }

  private flushAckAccumulator(): void {
    this.ackFlushScheduled = false
    const batches = this.ackAccumulator
    this.ackAccumulator = new Map()

    for (const [consumerId, entries] of batches) {
      const ok = entries.length === 1
        ? this.send(packAck(this.nextSeq(), consumerId, entries[0]!.subjectHash, entries[0]!.seq))
        : this.send(packBatchAck(this.nextSeq(), consumerId, entries))

      // Batched send failed (socket down / write threw) — defer every
      // seq in this batch to the AckRelay hot tier instead of losing
      // them; the sweep loop / reconnect replay resends them later.
      if (!ok) {
        for (const e of entries) this.ackRelay.record(consumerId, e.seq)
      }
    }
  }


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
      case Action.AckStateRep: {
        if (!this.onAckStateRep) {
          this.log.debug('AckStateRep received with no handler, dropped')
          return
        }
        this.onAckStateRep(unpackAckStateRep(frame.subarray(HEADER_SIZE)))
        return
      }
      case Action.AckBatchResp: {
        if (!this.onAckBatchResp) {
          this.log.debug('AckBatchResp received with no handler, dropped')
          return
        }
        this.onAckBatchResp(unpackAckBatchResp(frame.subarray(HEADER_SIZE)))
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

  /** Write-coalescing: cork the socket on the first write of an event-loop
   * tick and uncork on the next microtask. Every frame written within the
   * same tick (e.g. a loop firing N fire-and-forget publishes, or N acks)
   * is flushed by a single `_writev` — one vectored syscall instead of N,
   * without any `Buffer.concat`. Mirrors the single-writer batching of the
   * Go/Rust clients. `setNoDelay(true)` stays on, so the coalesced batch is
   * still sent immediately (no Nagle delay) once uncorked. */
  private corked = false
  private write(frame: Buffer): boolean {
    const sock = this.socket
    if (!sock.writable || sock.destroyed) return false
    if (!this.corked) {
      this.corked = true
      sock.cork()
      queueMicrotask(() => {
        this.corked = false
        if (!sock.destroyed) sock.uncork()
      })
    }
    try {
      sock.write(frame)
      return true
    } catch {
      return false
    }
  }

  /** Returns `true` if the frame was handed off to the socket, `false` if
   * the socket isn't connected/writable or the write threw. Callers on
   * the ack path (see `Message.ack`) use this to fall back to the
   * `AckRelay` hot tier instead of silently losing the ack. */
  send(frame: Buffer): boolean {
    return this.write(frame)
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
      this.write(frame)
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
      this.write(frame)
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

  // ── Ack-reliability sweep loop ─────────────────────────────────────────────
  // Mirrors consume/mod.rs's periodic re-flush: every 100ms, any consumer
  // with a non-empty AckRelay pending set gets its seqs resent as an
  // AckBatch. Confirmed entries are trimmed by `onAckBatchResp` (wired in
  // the constructor); anything still outstanding just gets resent next tick.

  private startAckSweep(): void {
    this.ackSweepTimer = setInterval(() => {
      for (const consumerId of this.ackRelay.consumerIds()) {
        const seqs = this.ackRelay.pendingSeqs(consumerId)
        if (seqs.length === 0) continue
        const generation = this.ackRelay.generationOf(consumerId)
        this.send(packAckBatch(this.nextSeq(), consumerId, generation, 0, seqs))
      }
    }, 100)
    this.ackSweepTimer.unref()
  }

  private stopAckSweep(): void {
    if (this.ackSweepTimer) {
      clearInterval(this.ackSweepTimer)
      this.ackSweepTimer = undefined
    }
  }

  /** On every successful reconnect: bump each tracked consumer's
   * generation and ask the broker for its authoritative ack cursor —
   * mirrors `send_ack_state_reqs` at `conn/session.rs:190`. */
  private replayAckState(): void {
    for (const consumerId of this.ackRelay.consumerIds()) {
      this.ackRelay.bumpGeneration(consumerId)
      const generation = this.ackRelay.generationOf(consumerId)
      this.send(packAckStateReq(this.nextSeq(), consumerId, generation))
    }
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  close(): Promise<void> {
    this.closing = true
    this.stopHeartbeat()
    this.stopAckSweep()
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
        this.replayAckState()
        if (this.metrics) this.metrics.reconnects++
      }).catch(() => this.tryReconnect(attempt + 1))
    }, delay)
  }
}
