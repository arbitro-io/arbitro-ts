import { Connection } from '../net/connection'
import { Subscription } from '../subscription/subscription'
import { ArbitroError } from '../types/error'
import { Stream } from '../stream/stream'
import { Consumer } from '../consumer/consumer'
import { ClientMetrics, type ClientMetricsSnapshot } from './metrics'
import { streamPublish, streamPublishAck, streamPublishBatch, streamRequest } from '../stream/publish'
import {
  packPublish, packCreateStream, packDeleteStream, packGetStream,
  packPurgeStream, packDrainSubject, packDeleteMessage, packListStreams,
  packCreateConsumer, packDeleteConsumer, packGetConsumer, packListConsumers,
  packConsumerStats,
  type CreateConsumerOpts,
} from '../proto/v2'
import { Flag, HEADER_SIZE } from '../proto/constants'
import type {
  ClientConfig, StreamConfig, ConsumerConfig, SubscribeOptions,
  DeleteStreamOpts, StreamInfo, ConsumerInfo,
} from '../types/config'
import { AckPolicy, DeliverPolicy, JournalType } from '../types/config'
import { Message } from '../message/message'
import { BatchPublishEntry } from '../proto/publish'
import { CronBuilder } from '../cron/cron-builder'
import { CronState } from '../cron/cron-state'

type MsgCallback = (msg: Message) => void

const DEFAULT_CONFIG: Required<Omit<ClientConfig, 'tls' | 'logger'>> = {
  servers: ['127.0.0.1:9898'],
  prefix: '',
  timeout: 5_000,
  reconnect: { enabled: true, maxAttempts: 10, intervalMs: 500, jitter: true },
}

export class ArbitroClient {
  private conn!: Connection
  private readonly cfg: typeof DEFAULT_CONFIG
  private readonly tls: ClientConfig['tls']
  private readonly logger: ClientConfig['logger']
  private readonly sidCache = new Map<string, number>()
  private readonly _metrics = new ClientMetrics()
  private readonly _cronState = new CronState()

  constructor(config: ClientConfig) {
    this.cfg = { ...DEFAULT_CONFIG, ...config }
    this.tls = config.tls
    this.logger = config.logger
  }

  async connect(): Promise<this> {
    const addr = this.cfg.servers[0]
    if (!addr) throw new ArbitroError('no servers configured', 'connect')
    this.conn = await Connection.connect(
      addr, this.cfg.timeout, this.tls, this.cfg.reconnect, this.logger,
    )
    this.conn.setMetrics(this._metrics)
    this.conn.setCronState(this._cronState)
    return this
  }

  /**
   * Point-in-time snapshot of client counters: publishes sent, deliveries
   * received, acks/nacks, active subscriptions, reconnects. Cheap — just
   * reads plain integer fields. Call on a timer to chart throughput.
   */
  metrics(): ClientMetricsSnapshot { return this._metrics.snapshot() }

  /** Internal connection accessor for Stream/Consumer publish methods. */
  _conn(): Connection { return this.conn }

  /** Default timeout from config. */
  get timeout(): number { return this.cfg.timeout }

  // ── Publish (direct to stream via V2 PubFrame) ────────────────────────────

  /** Apply prefix to subject if configured. */
  private prefixed(subject: string): string {
    return this.cfg.prefix ? `${this.cfg.prefix}.${subject}` : subject
  }

  /**
   * Publish a message. Returns a `Promise<void>` that resolves once the
   * broker confirms receipt (`RepOk`). The TS idiom is "everything async,
   * the caller chooses to await":
   *
   *   await client.publish(s, subj, data)               // wait for ack
   *   client.publish(s, subj, data)                     // fire-and-forget
   *   client.publish(s, subj, data).catch(handleError)  // async error path
   *
   * The broker always emits `RepOk` regardless of whether the caller
   * awaits — that's what enables the same call site to support both
   * semantics. A "no-reply" path doesn't save wire bytes, so it isn't
   * exposed.
   */
  async publish(
    streamName: string, subject: string, data: Buffer,
    opts?: import('../stream/publish').PublishOpts,
  ): Promise<void> {
    const sid = await this.resolveStreamId(streamName)
    await streamPublishAck(this.conn, sid, this.prefixed(subject), data, opts)
    this._metrics.publishesSent++
  }

  /**
   * @deprecated alias for {@link publish}. The default `publish` already
   * waits for `RepOk` and returns a Promise, so this method is identical.
   */
  publishAck(
    streamName: string, subject: string, data: Buffer,
    opts?: import('../stream/publish').PublishOpts,
  ): Promise<void> {
    return this.publish(streamName, subject, data, opts)
  }

  /** Batch publish — single V2 BatchPubFrame, ONE round-trip. Resolves
   * to `first_seq` (sequence of the first message in the batch; the N
   * messages occupy `[first_seq, first_seq + N - 1]`).
   *
   * Mirrors `publish`: the call always exchanges request/response with
   * the broker. The caller decides whether to actually wait:
   *
   *   await client.publishBatch(stream, msgs)   // barrier
   *   client.publishBatch(stream, msgs)         // fire-and-forget (promise dropped)
   *   client.publishBatch(stream, msgs)         //   (suppress unhandled rejection)
   *     .catch(() => {})
   *
   * Each entry may carry an optional `msgId` for broker-side dedup on
   * streams created with `idempotencyWindowMs > 0`. If any entry
   * collides with a previously-stored id (or another entry in the
   * SAME batch), the whole batch is rejected — `publishBatch` is
   * atomic.
   */
  publishBatch(streamName: string, messages: BatchPublishEntry[]): Promise<bigint> {
    const sid = this.cachedSid(streamName)
    const prefixedMsgs: BatchPublishEntry[] = this.cfg.prefix
      ? messages.map((m): BatchPublishEntry => {
        const entry: BatchPublishEntry = {
          subject: this.prefixed(m.subject),
          payload: m.payload,
        }
        if (m.msgId !== undefined) entry.msgId = m.msgId
        return entry
      })
      : messages
    this._metrics.publishBatchEntries += messages.length
    return streamPublishBatch(this.conn, sid, prefixedMsgs)
  }

  /** Request-reply. */
  async request(streamName: string, subject: string, data: Buffer, timeoutMs = this.cfg.timeout): Promise<Buffer> {
    const sid = await this.resolveStreamId(streamName)
    return streamRequest(this.conn, sid, subject, data, timeoutMs)
  }

  /**
   * Publish a message with a delivery delay. The broker parks the message
   * in its delayed journal and delivers it to consumers after `delayMs`
   * milliseconds. Returns a Promise that resolves once the broker confirms
   * receipt.
   */
  async publishDelayed(
    streamName: string, subject: string, data: Buffer, delayMs: number,
  ): Promise<void> {
    const sid = await this.resolveStreamId(streamName)
    const { packPublishDelayed } = await import('../proto/publish')
    const { Flag } = await import('../proto/constants')
    const subj = Buffer.from(this.prefixed(subject))
    await this.conn.sendExpectReply(
      packPublishDelayed(this.conn.nextSeq(), sid, subj, data, BigInt(delayMs), Flag.AckReq),
    )
    this._metrics.publishesSent++
  }

  // ── Subscribe ─────────────────────────────────────────────────────────────

  /** Subscribe with explicit consumer config. */
  subscribe(streamName: string, config: ConsumerConfig, callback?: MsgCallback, opts?: SubscribeOptions): Promise<Subscription>
  /** Subscribe by consumer name only (consumer must already exist). */
  subscribe(streamName: string, callback: MsgCallback): Promise<Subscription>

  async subscribe(
    streamName: string,
    configOrCb: ConsumerConfig | MsgCallback,
    callbackOrOpts?: MsgCallback | SubscribeOptions,
    opts?: SubscribeOptions,
  ): Promise<Subscription> {
    let config: ConsumerConfig
    let callback: MsgCallback | undefined
    let subOpts: SubscribeOptions | undefined

    if (typeof configOrCb === 'function') {
      config = { name: streamName, filter: '' }
      callback = configOrCb
      subOpts = undefined
    } else {
      config = configOrCb
      callback = typeof callbackOrOpts === 'function' ? callbackOrOpts : undefined
      subOpts = typeof callbackOrOpts === 'object' ? callbackOrOpts : opts
    }

    const consumerId = await this.ensureConsumer(streamName, config)
    const filter = Buffer.from(config.filter ?? '')
    const sub = new Subscription(consumerId, this.conn, streamName, subOpts?.fetchTimeoutMs ?? 5_000)
    const handler = (frame: Buffer) => sub.deliver(frame)

    await this.conn.sendSubscribeV2(consumerId, filter, handler)
    this._metrics.activeSubscriptions++
    // Best-effort gauge decrement when caller closes the subscription.
    const origClose = sub.close.bind(sub)
    sub.close = () => {
      if (this._metrics.activeSubscriptions > 0) this._metrics.activeSubscriptions--
      return origClose()
    }
    if (callback) sub.onMessage(callback)
    return sub
  }

  // ── Stream management ─────────────────────────────────────────────────────

  async createStream(name: string, config: StreamConfig): Promise<Stream> {
    const nameBuf = Buffer.from(name)
    const filterBuf = Buffer.from(config.subjectFilter ?? '')
    const maxMsgs = BigInt(config.maxMsgs ?? 0)
    const maxBytes = BigInt(config.maxBytes ?? 0)
    const maxAgeSecs = BigInt(config.maxAgeMs ? Math.ceil(config.maxAgeMs / 1000) : 0)
    const journalKind = journalTypeToU8(config.journal?.type)

    await this.conn.sendExpectReply(packCreateStream(
      this.conn.nextSeq(), nameBuf, filterBuf,
      maxMsgs, maxBytes, maxAgeSecs,
      1, journalKind, 0, 0,
      config.idempotencyWindowMs ?? 0,
    ))
    await this.resolveStreamId(name)
    return new Stream(this, name, config)
  }

  async upsertStream(name: string, config: StreamConfig): Promise<Stream> {
    try {
      return await this.createStream(name, config)
    } catch (e: any) {
      if (e?.message?.includes('code=')) {
        await this.resolveStreamId(name)
        return new Stream(this, name, config)
      }
      throw e
    }
  }

  async deleteStream(name: string, _opts?: DeleteStreamOpts): Promise<void> {
    await this.conn.sendExpectReply(
      packDeleteStream(this.conn.nextSeq(), Buffer.from(name)),
    )
    this.sidCache.delete(name)
  }

  async getStreamInfo(name: string): Promise<StreamInfo | null> {
    try {
      const refSeq = await this.conn.sendExpectReply(
        packGetStream(this.conn.nextSeq(), Buffer.from(name)),
      )
      this.sidCache.set(name, Number(refSeq & 0xFFFFFFFFn))
      return { name, config: { subjectFilter: '' }, lastSeq: Number(refSeq) }
    } catch {
      return null
    }
  }

  async listStreams(): Promise<StreamInfo[]> {
    const raw = await this.conn.sendExpectReplyRaw(
      packListStreams(this.conn.nextSeq()),
    )
    return parseListStreamsReply(raw)
  }

  async streamExists(name: string): Promise<boolean> {
    return (await this.getStreamInfo(name)) !== null
  }

  async purgeStream(name: string): Promise<number> {
    const refSeq = await this.conn.sendExpectReply(
      packPurgeStream(this.conn.nextSeq(), Buffer.from(name)),
    )
    return Number(refSeq)
  }

  async drainSubject(streamName: string, subject: string): Promise<number> {
    const refSeq = await this.conn.sendExpectReply(
      packDrainSubject(this.conn.nextSeq(), Buffer.from(streamName), Buffer.from(subject)),
    )
    return Number(refSeq)
  }

  /**
   * Tombstone a single message by sequence number.
   *
   * The broker marks the entry as deleted — it will never be delivered
   * to any consumer. Returns `true` if the message was found and
   * tombstoned, `false` if not found or already tombstoned.
   */
  async deleteMessage(streamName: string, seq: bigint): Promise<boolean> {
    const refSeq = await this.conn.sendExpectReply(
      packDeleteMessage(this.conn.nextSeq(), Buffer.from(streamName), seq),
    )
    return refSeq > 0n
  }

  // ── Consumer management ───────────────────────────────────────────────────

  async createConsumer(streamName: string, config: ConsumerConfig): Promise<Consumer> {
    const consumerId = await this.createConsumerRaw(streamName, config)
    return new Consumer(this, streamName, config, consumerId)
  }

  private async createConsumerRaw(streamName: string, config: ConsumerConfig): Promise<number> {
    const sid = await this.resolveStreamId(streamName)
    const name = Buffer.from(config.name ?? streamName)
    const group = Buffer.from(config.group ?? config.name ?? streamName)
    const filter = Buffer.from(config.filter ?? '')

    const ackPolicyByte = config.ackPolicy === AckPolicy.None ? 0 : 1
    const opts: CreateConsumerOpts = {
      streamId: sid,
      name,
      group,
      filter,
      maxInflight: config.maxAckPending ?? 0,
      ackPolicy: ackPolicyByte,
      deliverPolicy: deliverPolicyToU8(config.deliverPolicy),
      deliverMode: config.fanout ? 0 : 1,
      ackWaitMs: config.ackWaitMs ?? 0,
      startSeq: BigInt(config.startSeq ?? 0),
    }
    // Per-subject inflight is only enforced with Explicit ack — drop
    // the list silently for fire-and-forget consumers so they round-trip
    // cleanly through the server (which rejects the pairing).
    if (ackPolicyByte === 1 && config.maxSubjectInflights?.length) {
      opts.subjectLimits = config.maxSubjectInflights.map(l => ({
        pattern: Buffer.from(l.pattern),
        limit: l.limit >>> 0, // u32
      }))
    }

    const refSeq = await this.conn.sendExpectReply(
      packCreateConsumer(this.conn.nextSeq(), opts),
    )
    return Number(refSeq)
  }

  async upsertConsumer(streamName: string, config: ConsumerConfig): Promise<Consumer> {
    try {
      return await this.createConsumer(streamName, config)
    } catch (e: any) {
      // If already exists, get its ID
      const consumerId = await this.getConsumerId(streamName, config.name ?? streamName)
      if (consumerId !== null) return new Consumer(this, streamName, config, consumerId)
      throw e
    }
  }

  /** Delete consumer by server-assigned ID. */
  deleteConsumer(consumerId: number): Promise<void>
  /** Delete consumer by stream + name (lookup ID first). */
  deleteConsumer(streamName: string, name: string): Promise<void>

  async deleteConsumer(idOrStream: number | string, name?: string): Promise<void> {
    let consumerId: number
    if (typeof idOrStream === 'number') {
      consumerId = idOrStream
    } else {
      const id = await this.getConsumerId(idOrStream, name!)
      if (id === null) return  // already deleted
      consumerId = id
    }
    await this.conn.sendExpectReply(
      packDeleteConsumer(this.conn.nextSeq(), consumerId),
    )
  }

  async getConsumerId(streamName: string, name: string): Promise<number | null> {
    try {
      const sid = await this.resolveStreamId(streamName)
      const refSeq = await this.conn.sendExpectReply(
        packGetConsumer(this.conn.nextSeq(), sid, Buffer.from(name)),
      )
      return Number(refSeq)
    } catch {
      return null
    }
  }

  async consumerExists(streamName: string, name: string): Promise<boolean> {
    return (await this.getConsumerId(streamName, name)) !== null
  }

  /**
   * Live pending-ack count for one consumer — the number of messages the
   * consumer has been delivered but not yet acked. Equivalent of NATS
   * JetStream's `num_ack_pending`. Single broker round-trip; engine cost
   * is one O(1) Vec read per shard.
   */
  getPending(consumerId: number): Promise<number>
  getPending(streamName: string, name: string): Promise<number>
  async getPending(idOrStream: number | string, name?: string): Promise<number> {
    let consumerId: number
    if (typeof idOrStream === 'number') {
      consumerId = idOrStream
    } else {
      const id = await this.getConsumerId(idOrStream, name!)
      if (id === null) return 0
      consumerId = id
    }
    const refSeq = await this.conn.sendExpectReply(
      packConsumerStats(this.conn.nextSeq(), consumerId),
    )
    return Number(refSeq)
  }

  async getConsumerInfo(streamName: string, name: string): Promise<ConsumerInfo | null> {
    const id = await this.getConsumerId(streamName, name)
    if (id === null) return null
    return { group: name, stream: streamName, config: { name } }
  }

  async listConsumers(streamName?: string): Promise<ConsumerInfo[]> {
    const sid = streamName ? await this.resolveStreamId(streamName) : 0
    const raw = await this.conn.sendExpectReplyRaw(
      packListConsumers(this.conn.nextSeq(), sid),
    )
    return parseListConsumersReply(raw)
  }

  // ── Internal helpers ──────────────────────────────────────────────────────

  /** Resolve stream name → server wire_hash_32. Caches the result. */
  private async resolveStreamId(name: string): Promise<number> {
    const cached = this.sidCache.get(name)
    if (cached !== undefined) return cached
    const refSeq = await this.conn.sendExpectReply(
      packGetStream(this.conn.nextSeq(), Buffer.from(name)),
    )
    const sid = Number(refSeq & 0xFFFFFFFFn)
    this.sidCache.set(name, sid)
    return sid
  }

  /** Get cached stream_id or throw (for sync fire-and-forget paths). */
  private cachedSid(name: string): number {
    const sid = this.sidCache.get(name)
    if (sid === undefined) {
      throw new ArbitroError(
        `stream "${name}" not resolved — call createStream/getStreamInfo first`,
        'protocol',
      )
    }
    return sid
  }

  private async ensureConsumer(streamName: string, config: ConsumerConfig): Promise<number> {
    const name = config.name ?? streamName
    const existing = await this.getConsumerId(streamName, name)
    if (existing !== null) return existing
    return this.createConsumerRaw(streamName, config)
  }

  // ── Domain helpers ────────────────────────────────────────────────────────

  /** Pre-resolve stream_id from server (GetStream). Required before sync publish(). */
  async resolveStream(name: string): Promise<void> {
    await this.resolveStreamId(name)
  }

  stream(name: string, config?: StreamConfig): Stream {
    return new Stream(this, name, config)
  }

  // ── Cron ──────────────────────────────────────────────────────────────────

  /** Start building a cron job. Call `.every()` then `.run()` to register. */
  cron(name: string): CronBuilder {
    return new CronBuilder(this.conn, this._cronState, name)
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  async close(): Promise<void> {
    await this.conn.close()
  }
}

// ── Reply parsers ──────────────────────────────────────────────────────────

function parseListStreamsReply(frame: Buffer): StreamInfo[] {
  // Header(16) + count(4) + entries[wire_id(4) + name_len(2) + name]
  if (frame.length < HEADER_SIZE + 4) return []
  const count = frame.readUInt32LE(HEADER_SIZE)
  const results: StreamInfo[] = []
  let off = HEADER_SIZE + 4
  for (let i = 0; i < count; i++) {
    if (off + 6 > frame.length) break
    const wireId = frame.readUInt32LE(off)
    const nameLen = frame.readUInt16LE(off + 4)
    off += 6
    const name = frame.subarray(off, off + nameLen).toString()
    off += nameLen
    results.push({ name, config: { subjectFilter: '' }, lastSeq: wireId })
  }
  return results
}

function parseListConsumersReply(frame: Buffer): ConsumerInfo[] {
  // Header(16) + count(4) + entries[consumer_id(4) + stream_id(4) + queue_id(4) + paused(1)]
  if (frame.length < HEADER_SIZE + 4) return []
  const count = frame.readUInt32LE(HEADER_SIZE)
  const results: ConsumerInfo[] = []
  let off = HEADER_SIZE + 4
  for (let i = 0; i < count; i++) {
    if (off + 13 > frame.length) break
    const consumerId = frame.readUInt32LE(off)
    const _streamId = frame.readUInt32LE(off + 4)
    const _queueId = frame.readUInt32LE(off + 8)
    const _paused = frame[off + 12]
    off += 13
    results.push({
      group: consumerId.toString(),
      stream: '',
      config: { name: consumerId.toString() },
    })
  }
  return results
}

function deliverPolicyToU8(policy?: DeliverPolicy): number {
  switch (policy) {
    case DeliverPolicy.All: return 0
    case DeliverPolicy.New: return 1
    case DeliverPolicy.BySeq: return 2
    case DeliverPolicy.ByTime: return 3
    default: return 0
  }
}

function journalTypeToU8(type?: JournalType): number {
  switch (type) {
    case JournalType.Memory: return 0
    case JournalType.Tolerant: return 1
    case JournalType.Strict: return 2
    default: return 0
  }
}
