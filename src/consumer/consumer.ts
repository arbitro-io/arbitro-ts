import type { ArbitroClient } from '../client/client'
import type { ConsumerConfig, SubscribeOptions } from '../types/config'
import type { Subscription } from '../subscription/subscription'
import type { Message } from '../message/message'
import type { Encoding } from '../utils/codec'
import { makeLazyMessage, type LazyMessage } from '../topic/lazy-message'
import { streamPublish } from '../stream/publish'

type RawCallback = (msg: Message) => void

// Consumer — thin context wrapper that carries streamName + config + consumerId.
// No network calls at construction — only at .create() and .subscribe().
export class Consumer {
  constructor(
    private readonly client:     ArbitroClient,
    readonly streamName: string,
    readonly config:     ConsumerConfig,
    private _consumerId?: number,
  ) {}

  get name(): string { return this.config.name ?? this.streamName }
  get consumerId(): number | undefined { return this._consumerId }

  publish(subject: string, data: Buffer): void {
    streamPublish(this.client._conn(), this.streamName, subject, data)
  }

  async create(): Promise<this> {
    const result = await this.client.createConsumer(this.streamName, this.config)
    this._consumerId = result.consumerId
    return this
  }

  async upsert(): Promise<this> {
    const result = await this.client.upsertConsumer(this.streamName, this.config)
    this._consumerId = result.consumerId
    return this
  }

  async delete(): Promise<void> {
    if (this._consumerId == null) throw new Error('consumer not created')
    await this.client.deleteConsumer(this._consumerId)
  }

  async exists(): Promise<boolean> {
    return this.client.consumerExists(this.streamName, this.name)
  }

  // Raw subscribe — Message with manual ack/nack/decode.
  subscribe(cb?: RawCallback, opts?: SubscribeOptions): Promise<Subscription>

  // Typed subscribe — LazyMessage<T> with schema getters.
  subscribe<T extends Record<string, unknown>>(
    codec: Encoding<T>,
    cb: (msg: LazyMessage<T>) => void,
    opts?: SubscribeOptions,
  ): Promise<Subscription>

  subscribe<T extends Record<string, unknown>>(
    codecOrCb?: Encoding<T> | RawCallback,
    cbOrOpts?: ((msg: LazyMessage<T>) => void) | SubscribeOptions,
    opts?: SubscribeOptions,
  ): Promise<Subscription> {
    if (!codecOrCb || typeof codecOrCb === 'function') {
      return this.client.subscribe(
        this.streamName, this.config,
        codecOrCb as RawCallback | undefined,
        cbOrOpts as SubscribeOptions | undefined,
      )
    }
    const codec  = codecOrCb
    const cb     = cbOrOpts as (msg: LazyMessage<T>) => void
    const fields = codec.fields ?? []
    return this.client.subscribe(this.streamName, this.config, (raw) => {
      cb(makeLazyMessage(
        raw.data(), codec, fields,
        () => raw.ack(), () => raw.nack(), (ms) => raw.nackDelay(ms),
      ))
    }, opts)
  }
}
