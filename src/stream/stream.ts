import type { ArbitroClient } from '../client/client'
import type { StreamConfig, ConsumerConfig, DeleteStreamOpts, StreamInfo } from '../types/config'
import type { Encoding } from '../utils/codec'
import { Consumer } from '../consumer/consumer'
import { Topic } from '../topic/topic'

// Stream — context object carrying name + config.
// No network calls at construction — only at .create(), .upsert(), and .delete().
export class Stream {
  private _config: StreamConfig | undefined

  constructor(
    private readonly client: ArbitroClient,
    readonly name: string,
    config?: StreamConfig,
  ) {
    this._config = config
  }

  get config(): StreamConfig | undefined { return this._config }

  async create(config?: StreamConfig): Promise<this> {
    const cfg = config ?? this._config
    if (!cfg) throw new Error('StreamConfig required — pass to create() or constructor')
    this._config = cfg
    await this.client.createStream(this.name, cfg)
    return this
  }

  async upsert(config?: StreamConfig): Promise<this> {
    const cfg = config ?? this._config
    if (!cfg) throw new Error('StreamConfig required — pass to upsert() or constructor')
    this._config = cfg
    await this.client.upsertStream(this.name, cfg)
    return this
  }

  async delete(opts?: DeleteStreamOpts): Promise<void> {
    await this.client.deleteStream(this.name, opts)
  }

  async exists(): Promise<boolean> {
    return this.client.streamExists(this.name)
  }

  async info(): Promise<StreamInfo | null> {
    return this.client.getStreamInfo(this.name)
  }

  // ── Publish ─────────────────────────────────────────────────────────────

  /**
   * Publish to this stream. Returns a `Promise<void>` that resolves on
   * broker `RepOk`. Await it to wait for confirmation, or ignore the
   * returned promise for fire-and-forget semantics.
   */
  publish(subject: string, data: Buffer): Promise<void> {
    return this.client.publish(this.name, subject, data)
  }

  /** @deprecated alias for {@link publish}. */
  publishAck(subject: string, data: Buffer): Promise<void> {
    return this.client.publish(this.name, subject, data)
  }

  publishBatch(messages: [subject: string, data: Buffer][]): void {
    this.client.publishBatch(this.name, messages)
  }

  request(subject: string, data: Buffer, timeoutMs?: number): Promise<Buffer> {
    return this.client.request(this.name, subject, data, timeoutMs)
  }

  // ── Context factories ───────────────────────────────────────────────────

  consumer(overrides?: Partial<ConsumerConfig>): Consumer {
    const config: ConsumerConfig = {
      ...overrides,
      name:   overrides?.name   ?? this.name,
      filter: overrides?.filter ?? `${this.name}.>`,
    }
    return new Consumer(this.client, this.name, config)
  }

  topic<T extends Record<string, unknown>>(subject: string, codec: Encoding<T>): Topic<T> {
    return new Topic(this, subject, codec)
  }
}
