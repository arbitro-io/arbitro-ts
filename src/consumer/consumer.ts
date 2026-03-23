import type { ArbitroClient } from '../client/client'
import type { ConsumerConfig } from '../types/config'
import type { Subscription } from '../subscription/subscription'
import type { Message } from '../message/message'
import type { Encoding } from '../utils/codec'
import { makeLazyMessage, type LazyMessage } from '../topic/lazy-message'

type RawCallback = (msg: Message) => void

// Consumer — thin context wrapper that carries streamName + config.
// No network calls at construction — only at .create() and .subscribe().
export class Consumer {
  constructor(
    private readonly client:     ArbitroClient,
    readonly streamName: string,
    readonly config:     ConsumerConfig,
  ) {}

  get name(): string { return this.config.name ?? this.streamName }

  create(): this {
    this.client.createConsumer(this.streamName, this.config)
    return this
  }

  delete(): void {
    this.client.deleteConsumer(this.name)
  }

  // Raw subscribe — Message with manual ack/nack/decode.
  subscribe(cb?: RawCallback): Promise<Subscription>

  // Typed subscribe — LazyMessage<T> with schema getters.
  subscribe<T extends Record<string, unknown>>(
    codec: Encoding<T>,
    cb: (msg: LazyMessage<T>) => void,
  ): Promise<Subscription>

  subscribe<T extends Record<string, unknown>>(
    codecOrCb?: Encoding<T> | RawCallback,
    cb?: (msg: LazyMessage<T>) => void,
  ): Promise<Subscription> {
    if (!codecOrCb || typeof codecOrCb === 'function') {
      return this.client.subscribe(this.name, codecOrCb)
    }
    const fields = (codecOrCb as { fields?: string[] }).fields ?? []
    return this.client.subscribe(this.name, (raw) => {
      cb!(makeLazyMessage(raw.data(), codecOrCb, fields, () => raw.ack(), () => raw.nack()))
    })
  }
}
