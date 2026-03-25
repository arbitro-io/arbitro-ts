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

  /** The StreamConfig used to create this stream, if known. */
  get config(): StreamConfig | undefined { return this._config }

  async create(config: StreamConfig): Promise<this> {
    this._config = config
    await this.client.createStream(this.name, config)
    return this
  }

  async upsert(config: StreamConfig): Promise<this> {
    this._config = config
    await this.client.upsertStream(this.name, config)
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

  // Pure construction of a Consumer context with defaults derived from this stream.
  // name defaults to stream name, filter defaults to "${name}.>".
  // Call consumer.create() / consumer.upsert() for server-side registration.
  consumer(overrides?: Partial<ConsumerConfig>): Consumer {
    const config: ConsumerConfig = {
      ...overrides,
      name:   overrides?.name   ?? this.name,
      filter: overrides?.filter ?? `${this.name}.>`,
    }
    return new Consumer(this.client, this.name, config)
  }

  // Returns a Topic<T> bound to subject + codec — no network call.
  topic<T extends Record<string, unknown>>(subject: string, codec: Encoding<T>): Topic<T> {
    return new Topic(this.client, subject, codec)
  }
}
