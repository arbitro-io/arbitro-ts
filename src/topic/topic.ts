import type { ArbitroClient } from '../client/client'
import type { Encoding } from '../utils/codec'
import type { Subscription } from '../subscription/subscription'
import { makeLazyMessage, type LazyMessage } from './lazy-message'

// Topic<T> — binds a subject + codec so publish/subscribe are always typed.
// Instantiate once per message type via client.topic() or stream.topic().
export class Topic<T extends Record<string, unknown>> {
  private readonly fields: string[]

  constructor(
    private readonly client:  ArbitroClient,
    private readonly subject: string,
    private readonly codec:   Encoding<T>,
  ) {
    // Codec<T> exposes .fields — other Encoding<T> implementations get empty array (full decode).
    this.fields = (codec as { fields?: string[] }).fields ?? []
  }

  publish(value: T): void {
    this.client.publish(this.subject, this.codec.encode(value))
  }

  async publishAck(value: T): Promise<void> {
    await this.client.publishAck(this.subject, this.codec.encode(value))
  }

  publishBatch(values: T[]): void {
    this.client.publishBatch(values.map((v) => [this.subject, this.codec.encode(v)]))
  }

  async subscribe(
    group: string,
    cb: (msg: LazyMessage<T>) => void,
  ): Promise<Subscription> {
    return this.client.subscribe(group, (raw) => {
      cb(makeLazyMessage(raw.data(), this.codec, this.fields, () => raw.ack(), () => raw.nack()))
    })
  }
}
