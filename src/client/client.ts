import { pack } from '../proto/codec'
import { Action, Flags, HEADER_SIZE } from '../proto/constants'
import { Connection } from '../net/connection'
import { Subscription } from '../subscription/subscription'
import { ArbitroError } from '../types/error'
import {
  ClientConfig, StreamConfig, ConsumerConfig,
  PublishOptions, SubscribeOptions,
} from '../types/config'
import { Stream } from '../stream/stream'
import { Topic } from '../topic/topic'
import type { Encoding } from '../utils/codec'
import { serializeStreamConfig, serializeConsumerConfig } from './serialize'

type MsgCallback = (msg: import('../message/message').Message) => void

const DEFAULT_CONFIG: Required<Omit<ClientConfig, 'tls' | 'logger'>> = {
  servers:   ['127.0.0.1:9898'],
  prefix:    '',
  timeout:   5_000,
  reconnect: { enabled: true, maxAttempts: 10, intervalMs: 500, jitter: true },
}

function toWireSubject(prefix: string, subject: string): string {
  return prefix ? `${prefix}.${subject}` : subject
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

  // ── Publish ───────────────────────────────────────────────────────────────

  // Fire-and-forget. Default is NoAck (maximum throughput).
  // Pass { noAck: false } to get a RepOk confirmation instead.
  publish(subject: string, data: Buffer, opts?: PublishOptions): void {
    const noAck = opts?.noAck ?? true
    this.conn.send(pack({
      action:  Action.PubPublish,
      flags:   noAck ? Flags.NoAck : Flags.None,
      seq:     this.conn.nextSeq(),
      subject: toWireSubject(this.cfg.prefix, subject),
      data,
    }))
  }

  // Publish and wait for a reply from a subscriber (request-reply pattern).
  // Sets FLAG_REPLY_TO. Blocks until the subscriber calls msg.reply() or timeout elapses.
  async request(subject: string, data: Buffer, timeoutMs = this.cfg.timeout): Promise<Buffer> {
    const seq = this.conn.nextSeq()
    const frame = pack({
      action:  Action.PubPublish,
      flags:   Flags.ReplyTo,
      seq,
      subject: toWireSubject(this.cfg.prefix, subject),
      data,
    })
    const reply = await this.conn.sendRequest(seq, frame, timeoutMs)
    return reply.subarray(HEADER_SIZE)
  }

  // Publish and wait for server to process it.
  // Uses a SysKeepalive fence: the server echoes it as RepOk only after
  // processing everything before it on the same TCP connection.
  async publishAck(subject: string, data: Buffer): Promise<void> {
    this.conn.send(pack({
      action:  Action.PubPublish,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: toWireSubject(this.cfg.prefix, subject),
      data,
    }))
    await this.conn.sendExpectReply(pack({
      action:  Action.SysKeepalive,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: '',
      data:    Buffer.alloc(0),
    }))
  }

  // Publish multiple messages as a single write syscall (all NoAck).
  publishBatch(messages: [subject: string, data: Buffer][]): void {
    const frames = messages.map(([subject, data]) =>
      pack({
        action:  Action.PubPublish,
        flags:   Flags.NoAck,
        seq:     this.conn.nextSeq(),
        subject: toWireSubject(this.cfg.prefix, subject),
        data,
      })
    )
    this.conn.send(Buffer.concat(frames))
  }

  // ── Subscribe ─────────────────────────────────────────────────────────────

  async subscribe(group: string, callback?: MsgCallback, opts?: SubscribeOptions): Promise<Subscription> {
    const subId = await this.conn.sendExpectReply(pack({
      action:  Action.PubSubscribe,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: group,
      data:    Buffer.alloc(0),
    }))
    const sub = new Subscription(subId, this.conn, opts?.fetchTimeoutMs ?? 5_000)
    if (callback) sub.onMessage(callback)
    return sub
  }

  // ── Stream management (fire-and-forget) ───────────────────────────────────

  createStream(name: string, config: StreamConfig): void {
    this.conn.send(pack({
      action:  Action.PubCreateStream,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: name,
      data:    serializeStreamConfig(config),
    }))
  }

  deleteStream(name: string): void {
    this.conn.send(pack({
      action:  Action.PubDeleteStream,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: name,
      data:    Buffer.alloc(0),
    }))
  }

  // ── Consumer management (fire-and-forget) ─────────────────────────────────

  createConsumer(stream: string, config: ConsumerConfig): void {
    this.conn.send(pack({
      action:  Action.PubCreateConsumer,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: stream,
      data:    serializeConsumerConfig(config),
    }))
  }

  deleteConsumer(name: string): void {
    this.conn.send(pack({
      action:  Action.PubDeleteConsumer,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: name,
      data:    Buffer.alloc(0),
    }))
  }

  // ── Domain helpers ────────────────────────────────────────────────────────

  stream(name: string): Stream {
    return new Stream(this, name)
  }

  topic<T extends Record<string, unknown>>(subject: string, codec: Encoding<T>): Topic<T> {
    return new Topic(this, subject, codec)
  }

  // Fence: waits until the server has processed all preceding commands on this connection.
  // Useful to synchronize between separate connections (admin creates → sync → sub subscribes).
  sync(): Promise<void> {
    return this.conn.sendExpectReply(pack({
      action:  Action.SysKeepalive,
      flags:   Flags.None,
      seq:     this.conn.nextSeq(),
      subject: '',
      data:    Buffer.alloc(0),
    })).then(() => undefined)
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  async close(): Promise<void> {
    await this.conn.close()
  }
}
