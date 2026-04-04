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

  // ── Subscribe ─────────────────────────────────────────────────────────────

  async subscribe(group: string, callback?: MsgCallback, opts?: SubscribeOptions): Promise<Subscription> {
    const sub     = new Subscription(0n, this.conn, opts?.fetchTimeoutMs ?? 5_000)
    const handler = (frame: Buffer) => sub.deliver(frame)
    const subId   = await this.conn.sendSubscribe(group, handler, (id) => sub.updateSubId(id))
    sub.updateSubId(subId)
    if (callback) sub.onMessage(callback)
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
    const existing = await this.getConsumerInfo(group)
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

  async deleteConsumer(name: string): Promise<void> {
    await this.conn.sendExpectReply(pack({
      action:  Action.PubDeleteConsumer,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: name,
      data:    Buffer.alloc(0),
    }))
  }

  async getConsumerInfo(group: string): Promise<ConsumerInfo | null> {
    const seq = this.conn.nextSeq()
    const raw = await this.conn.requestMsgpack<any>(
      seq,
      pack({
        action:  Action.MgmtGetConsumer,
        flags:   Flags.None,
        seq,
        subject: group,
        data:    Buffer.alloc(0),
      }),
      this.cfg.timeout,
    )
    return raw ? normalizeConsumerInfo(group, raw) : null
  }

  async listConsumers(): Promise<ConsumerInfo[]> {
    const seq = this.conn.nextSeq()
    const raw = await this.conn.requestMsgpack<any[]>(
      seq,
      pack({
        action:  Action.MgmtListConsumers,
        flags:   Flags.None,
        seq,
        subject: '',
        data:    Buffer.alloc(0),
      }),
      this.cfg.timeout,
    )
    return raw.map((info) => normalizeConsumerInfo(info.config?.group ?? info.group ?? '', info))
  }

  async consumerExists(group: string): Promise<boolean> {
    return (await this.getConsumerInfo(group)) !== null
  }

  // ── Domain helpers ────────────────────────────────────────────────────────

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
  return {
    name: raw.name,
    config: normalizeStreamConfig(raw.config),
    lastSeq: raw.last_seq ?? raw.lastSeq ?? 0,
  } as StreamInfo & { lastSeq?: number }
}

function normalizeStreamConfig(raw: any): StreamConfig {
  const journalType = raw.journal?.type ?? raw.journal_type
  const config: StreamConfig = {
    subjectFilter: raw.subjectFilter ?? raw.subject_filter,
  }
  if (journalType) config.journal = { type: journalType as JournalType }
  const maxMsgs = raw.maxMsgs ?? raw.max_msgs
  const maxBytes = raw.maxBytes ?? raw.max_bytes
  if (maxMsgs !== undefined && maxMsgs !== null) config.maxMsgs = maxMsgs
  if (maxBytes !== undefined && maxBytes !== null) config.maxBytes = maxBytes
  const maxAgeMs = raw.maxAgeMs
    ?? (raw.max_age_ns !== undefined && raw.max_age_ns !== null
      ? Number(BigInt(raw.max_age_ns) / 1_000_000n)
      : undefined)
  if (maxAgeMs !== undefined) config.maxAgeMs = maxAgeMs
  return config
}

function normalizeConsumerInfo(group: string, raw: any): ConsumerInfo {
  return {
    group: raw.group ?? raw.config?.group ?? group,
    stream: raw.stream,
    config: normalizeConsumerConfig(raw.config),
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
