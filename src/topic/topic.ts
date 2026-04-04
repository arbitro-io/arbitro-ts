import type { Stream } from '../stream/stream'
import type { Encoding } from '../utils/codec'
import type { Subscription } from '../subscription/subscription'
import { makeLazyMessage, type LazyMessage } from './lazy-message'

// Topic<T> — binds a subject + codec so publish/subscribe are always typed.
// Instantiate via stream.topic(subject, codec).
export class Topic<T extends Record<string, unknown>> {
  private readonly fields: string[]

  constructor(
    private readonly stream:  Stream,
    private readonly subject: string,
    private readonly codec:   Encoding<T>,
  ) {
    this.fields = (codec as { fields?: string[] }).fields ?? []
  }

  publish(value: T): void {
    this.stream.publish(this.subject, this.codec.encode(value))
  }

  async publishAck(value: T): Promise<void> {
    await this.stream.publishAck(this.subject, this.codec.encode(value))
  }

  publishBatch(values: T[]): void {
    this.stream.publishBatch(values.map((v) => [this.subject, this.codec.encode(v)]))
  }

  async subscribe(
    group: string,
    cb: (msg: LazyMessage<T>) => void,
  ): Promise<Subscription> {
    const consumer = this.stream.consumer({ name: group })
    return consumer.subscribe(this.codec, cb)
  }
}
