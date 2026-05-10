import { pack } from '../proto/codec'
import { Action, Flags, HEADER_SIZE } from '../proto/constants'
import { Connection } from '../net/connection'
import { Subscription } from '../subscription/subscription'
import { ArbitroError } from '../types/error'
import {
  ClientConfig, StreamConfig, ConsumerConfig, SubscribeOptions, DeleteStreamOpts, StreamInfo, ConsumerInfo,
  AckPolicy, DeliverPolicy, JournalType,
} from '../types/config'
import { Stream } from '../stream/stream'
import { Consumer } from '../consumer/consumer'
import { serializeStreamConfig, serializeConsumerConfig, serializeDeleteStreamOpts } from './serialize'
import { streamPublish, streamPublishAck, streamPublishBatch, streamRequest } from '../stream/publish'

type MsgCallback = (msg: import('../message/message').Message) => void

const DEFAULT_CONFIG: Required<Omit<ClientConfig, 'tls' | 'logger'>> = {
  servers:   ['127.0.0.1:9898'],
  prefix:    '',
  timeout:   5_000,
  reconnect: { enabled: true, maxAttempts: 10, intervalMs: 500, jitter: true },
}

export class ArbitroClient {
  private conn!: Connection
  private readonly cfg: typeof DEFAULT_CONFIG
  private readonly tls:    ClientConfig['tls']
  private readonly logger: ClientConfig['logger']

  constructor(config: ClientConfig) {
    this.cfg    = { ...DEFAULT_CONFIG, ...config }
    this.tls    = config.tls
    this.logger = config.logger
  }

  async connect(): Promise<this> {
    const addr = this.cfg.servers[0]
    if (!addr) throw new ArbitroError('no servers configured', 'connect')
    this.conn = await Connection.connect(
      addr, this.cfg.timeout, this.tls, this.cfg.reconnect, this.logger,
    )
    return this
  }

  /** Internal connection accessor for Stream/Consumer publish methods. */
  _conn(): Connection { return this.conn }

  /** Default timeout from config. */
  get timeout(): number { return this.cfg.timeout }

  // ── Publish (subject-routed via PubPublish) ────────────────────────────────

  /** Fire-and-forget publish. Subject is routed by the broker to the matching stream. */
  publish(subject: string, data: Buffer): void {
    this.conn.send(pack({
      action:  Action.PubPublish,
      flags:   Flags.NoAck,
      seq:     this.conn.nextSeq(),
      subject: this.applyPrefix(subject),
      data,
    }))
  }

  /** Publish and wait for server confirmation (RepOk). */
  async publishAck(subject: string, data: Buffer): Promise<void> {
    await this.conn.sendExpectReply(pack({
      action:  Action.PubPublish,
      seq:     this.conn.nextSeq(),
      subject: this.applyPrefix(subject),
      data,
    }))
  }

  /** Batch fire-and-forget to a specific stream — single write syscall. */
  publishBatch(streamName: string, messages: [subject: string, data: Buffer][]): void {
    streamPublishBatch(this.conn, streamName, messages)
  }

  /** Direct-to-stream publish (bypasses subject router). */
  publishToStream(streamName: string, subject: string, data: Buffer): void {
    streamPublish(this.conn, streamName, subject, data)
  }

  /** Direct-to-stream publish with server confirmation. */
  async publishToStreamAck(streamName: string, subject: string, data: Buffer): Promise<void> {
    await streamPublishAck(this.conn, streamName, subject, data)
  }

  /** Request-reply. Waits for subscriber reply or timeout. */
  async request(subject: string, data: Buffer, timeoutMs = this.cfg.timeout): Promise<Buffer> {
    return streamRequest(this.conn, '', subject, data, timeoutMs)
  }

  // ── Subscribe ─────────────────────────────────────────────────────────────

  async subscribe(stream: string, callback: MsgCallback): Promise<Subscription>
  async subscribe(stream: string, config: ConsumerConfig, callback?: MsgCallback, opts?: SubscribeOptions): Promise<Subscription>
  async subscribe(
    stream: string,
    configOrCallback: ConsumerConfig | MsgCallback,
    callback?: MsgCallback,
    opts?: SubscribeOptions,
  ): Promise<Subscription> {
    const config = typeof configOrCallback === 'function' ? { name: stream } : configOrCallback
    const cb     = typeof configOrCallback === 'function' ? configOrCallback : callback
    const sub        = new Subscription(0n, this.conn, stream, opts?.fetchTimeoutMs ?? 5_000)
    const handler    = (frame: Buffer) => sub.deliver(frame)
    const configData = serializeConsumerConfig(config)
    const subId      = await this.conn.sendSubscribe(stream, configData, handler, (id) => sub.updateSubId(id))
    sub.updateSubId(subId)
    if (cb) sub.onMessage(cb)
    return sub
  }

  // ── Stream management ─────────────────────────────────────────────────────

  /** Create a stream on the server and return the Stream context. Resolves once the server confirms. */
  async createStream(name: string, config: StreamConfig): Promise<Stream> {
    await this.conn.sendExpectReply(pack({
      action:  Action.PubCreateStream,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: name,
      data:    serializeStreamConfig(config),
    }))
    return new Stream(this, name, config)
  }

  async upsertStream(name: string, config: StreamConfig): Promise<Stream> {
    const existing = await this.getStreamInfo(name)
    if (!existing) return this.createStream(name, config)
    if (!sameStreamConfig(existing.config, config)) {
      throw new ArbitroError(
        `stream upsert conflict: '${name}' exists with different config`,
        'server',
        'StreamConflict',
      )
    }
    return new Stream(this, name, config)
  }

  async deleteStream(name: string, opts?: DeleteStreamOpts): Promise<void> {
    await this.conn.sendExpectReply(pack({
      action:  Action.PubDeleteStream,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: name,
      data:    serializeDeleteStreamOpts(opts),
    }))
  }

  async getStreamInfo(name: string): Promise<StreamInfo | null> {
    const seq = this.conn.nextSeq()
    try {
      const raw = await this.conn.requestMsgpack<any>(
        seq,
        pack({
          action:  Action.MgmtGetStream,
          flags:   Flags.None,
          seq,
          subject: name,
          data:    Buffer.alloc(0),
        }),
        this.cfg.timeout,
      )
      return raw ? normalizeStreamInfo(raw) : null
    } catch {
      return null
    }
  }

  async listStreams(): Promise<StreamInfo[]> {
    const seq = this.conn.nextSeq()
    const raw = await this.conn.requestMsgpack<any[]>(
      seq,
      pack({
        action:  Action.MgmtListStreams,
        flags:   Flags.None,
        seq,
        subject: '',
        data:    Buffer.alloc(0),
      }),
      this.cfg.timeout,
    )
    return raw.map(normalizeStreamInfo)
  }

  async streamExists(name: string): Promise<boolean> {
    return (await this.getStreamInfo(name)) !== null
  }

  // ── Consumer management ───────────────────────────────────────────────────

  /** Create a consumer on the server and return the Consumer context. Resolves once the server confirms. */
  async createConsumer(stream: string, config: ConsumerConfig): Promise<Consumer> {
    await this.conn.sendExpectReply(pack({
      action:  Action.PubCreateConsumer,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: stream,
      data:    serializeConsumerConfig(config),
    }))
    return new Consumer(this, stream, config)
  }

  async upsertConsumer(stream: string, config: ConsumerConfig): Promise<Consumer> {
    const group = config.name ?? stream
    const existing = await this.getConsumerInfo(stream, group)
    const requested = canonicalConsumerConfig(stream, config)
    if (!existing) return this.createConsumer(stream, config)
    if (!sameConsumerConfig(canonicalConsumerInfo(existing), requested)) {
      throw new ArbitroError(
        `consumer upsert conflict: '${group}' exists with different config`,
        'server',
        'ConsumerConflict',
      )
    }
    return new Consumer(this, stream, config)
  }

  async registerConsumer(stream: string, config: ConsumerConfig): Promise<void> {
    await this.conn.sendExpectReply(pack({
      action:  Action.PubCreateConsumer,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: stream,
      data:    serializeConsumerConfig(config),
    }))
  }

  async deleteConsumer(stream: string, group?: string): Promise<void> {
    const g = group ?? stream
    await this.conn.sendExpectReply(pack({
      action:  Action.PubDeleteConsumer,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: stream,
      data:    Buffer.from(g),
    }))
  }

  async getConsumerInfo(stream: string, group?: string): Promise<ConsumerInfo | null> {
    const g = group ?? stream
    const seq = this.conn.nextSeq()
    try {
      const raw = await this.conn.requestMsgpack<any>(
        seq,
        pack({
          action:  Action.MgmtGetConsumer,
          flags:   Flags.None,
          seq,
          subject: stream,
          data:    Buffer.from(g),
        }),
        this.cfg.timeout,
      )
      return raw ? normalizeConsumerInfo(g, raw) : null
    } catch {
      return null
    }
  }

  async listConsumers(stream?: string): Promise<ConsumerInfo[]> {
    const seq = this.conn.nextSeq()
    const raw = await this.conn.requestMsgpack<any[]>(
      seq,
      pack({
        action:  Action.MgmtListConsumers,
        flags:   Flags.None,
        seq,
        subject: stream ?? '',
        data:    Buffer.alloc(0),
      }),
      this.cfg.timeout,
    )
    return raw.map((info) => normalizeConsumerInfo(info.config?.group ?? info.group ?? '', info))
  }

  async consumerExists(stream: string, group?: string): Promise<boolean> {
    return (await this.getConsumerInfo(stream, group)) !== null
  }

  // ── Domain helpers ────────────────────────────────────────────────────────

  private applyPrefix(subject: string): string {
    return this.cfg.prefix ? `${this.cfg.prefix}.${subject}` : subject
  }

  stream(name: string, config?: StreamConfig): Stream {
    return new Stream(this, name, config)
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  async close(): Promise<void> {
    await this.conn.close()
  }
}

function sameStreamConfig(a: StreamConfig, b: StreamConfig): boolean {
  return stableStringify(normalizeStreamConfig(a)) === stableStringify(normalizeStreamConfig(b))
}

function canonicalConsumerConfig(stream: string, cfg: ConsumerConfig): ConsumerConfig {
  return { ...cfg, name: cfg.name ?? stream }
}

function canonicalConsumerInfo(info: ConsumerInfo): ConsumerConfig {
  return { ...info.config, name: info.group }
}

function sameConsumerConfig(a: ConsumerConfig, b: ConsumerConfig): boolean {
  return stableStringify(normalizeConsumerConfig(a)) === stableStringify(normalizeConsumerConfig(b))
}

function normalizeStreamInfo(raw: any): StreamInfo {
  const info: StreamInfo & { lastSeq?: number } = {
    name: raw.name,
    config: raw.config ? normalizeStreamConfig(raw.config) : { subjectFilter: '' },
    lastSeq: raw.last_seq ?? raw.lastSeq ?? 0,
  }
  return info
}

function normalizeStreamConfig(raw: any): StreamConfig {
  const journalType = raw.journal?.type ?? raw.journal_kind ?? raw.journal_type
  const config: StreamConfig = {
    subjectFilter: raw.subjectFilter ?? raw.filter ?? raw.subject_filter,
  }
  if (journalType) config.journal = { type: journalType as JournalType }
  const maxMsgs = raw.maxMsgs ?? raw.max_msgs
  const maxBytes = raw.maxBytes ?? raw.max_bytes
  if (maxMsgs) config.maxMsgs = maxMsgs
  if (maxBytes) config.maxBytes = maxBytes
  const maxAgeNs = raw.max_age_ns ?? raw.maxAgeNs
  const maxAgeMs = raw.maxAgeMs ?? (maxAgeNs ? Number(BigInt(maxAgeNs) / 1_000_000n) : undefined)
  if (maxAgeMs) config.maxAgeMs = maxAgeMs
  return config
}

function normalizeConsumerInfo(group: string, raw: any): ConsumerInfo {
  const cfg = raw.config ?? raw
  return {
    group: cfg.group ?? group,
    stream: raw.stream,
    config: normalizeConsumerConfig(cfg),
  }
}

function normalizeConsumerConfig(raw: any): ConsumerConfig {
  const config: ConsumerConfig = {}
  const name = raw.name ?? raw.group
  const fanout = raw.fanout ?? raw.deliver_mode === 'Fanout'
  const ackPolicy = raw.ackPolicy ?? (raw.no_ack ? AckPolicy.None : AckPolicy.Explicit)
  const deliverPolicy = raw.deliverPolicy ?? raw.deliver_policy
  const startSeq = raw.startSeq ?? raw.start_seq
  const startTime = raw.startTime ?? raw.start_time
  const maxAckPending = raw.maxAckPending ?? raw.max_ack_pending
  const ackWaitMs = raw.ackWaitMs ?? raw.ack_wait_ms
  const maxDeliver = raw.maxDeliver ?? raw.max_deliver
  const creditRules = raw.creditRules ?? raw.credit_rules

  if (name !== undefined) config.name = name
  if (raw.filter !== undefined && raw.filter !== '') config.filter = raw.filter
  if (fanout === true) config.fanout = true
  if (ackPolicy !== undefined && ackPolicy !== AckPolicy.Explicit) config.ackPolicy = ackPolicy
  if (deliverPolicy !== undefined && deliverPolicy !== DeliverPolicy.New) config.deliverPolicy = deliverPolicy as DeliverPolicy
  if (startSeq !== undefined) config.startSeq = startSeq
  if (startTime !== undefined) config.startTime = startTime
  if (maxAckPending !== undefined && maxAckPending !== 128) config.maxAckPending = maxAckPending
  if (ackWaitMs !== undefined) config.ackWaitMs = ackWaitMs
  if (maxDeliver !== undefined) config.maxDeliver = maxDeliver
  if (creditRules !== undefined && creditRules.length > 0) config.creditRules = creditRules
  return config
}

function stableStringify(value: unknown): string {
  return JSON.stringify(value, (_, v) => typeof v === 'bigint' ? v.toString() : v)
}
