import type { ArbitroClient } from '../client/client'
import type { StreamConfig, ConsumerConfig } from '../types/config'
import type { Encoding } from '../utils/codec'
import type { Consumer } from '../consumer/consumer'
import { Topic } from '../topic/topic'

// Stream — context object carrying name + config.
// No network calls at construction — only at .create(), .delete(), and .consumer().
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

  delete(): void {
    this.client.deleteStream(this.name)
  }

  // Creates the consumer on the server with defaults derived from this stream.
  // name defaults to stream name, filter defaults to "${name}.>".
  // Any field can be overridden.
  async consumer(overrides?: Partial<ConsumerConfig>): Promise<Consumer> {
    const config: ConsumerConfig = {
      ...overrides,
      name:   overrides?.name   ?? this.name,
      filter: overrides?.filter ?? `${this.name}.>`,
    }
    return this.client.createConsumer(this.name, config)
  }

  // Returns a Topic<T> bound to subject + codec — no network call.
  topic<T extends Record<string, unknown>>(subject: string, codec: Encoding<T>): Topic<T> {
    return new Topic(this.client, subject, codec)
  }
}
