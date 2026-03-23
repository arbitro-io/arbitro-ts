import type { ArbitroClient } from '../client/client'
import type { StreamConfig, ConsumerConfig } from '../types/config'
import type { Encoding } from '../utils/codec'
import { Consumer } from '../consumer/consumer'
import { Topic } from '../topic/topic'

// Stream — thin context wrapper that carries the stream name.
// No network calls at construction — only at .create() and .delete().
export class Stream {
  constructor(
    private readonly client: ArbitroClient,
    readonly name: string,
  ) {}

  create(config: StreamConfig): this {
    this.client.createStream(this.name, config)
    return this
  }

  delete(): void {
    this.client.deleteStream(this.name)
  }

  // Returns a Consumer pre-filled with this stream's name — no network call.
  // All config fields are optional: name defaults to stream name, filter defaults to "${name}.>".
  consumer(config?: Partial<ConsumerConfig>): Consumer {
    const resolved: ConsumerConfig = {
      ...config,
      name:   config?.name   ?? this.name,
      filter: config?.filter ?? `${this.name}.>`,
    }
    return new Consumer(this.client, this.name, resolved)
  }

  // Returns a Topic<T> bound to subject + codec — no network call.
  topic<T extends Record<string, unknown>>(subject: string, codec: Encoding<T>): Topic<T> {
    return new Topic(this.client, subject, codec)
  }
}
