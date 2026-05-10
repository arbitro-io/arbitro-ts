import type { Encoding } from '../utils/codec'

// LazyMessage<T> = T fields as getters + meta methods.
// Decode happens only on first field access — result is cached.
// If msg.ack() is called without reading any field, zero deserialization occurs.
export type LazyMessage<T> = T & {
  readonly _raw: Buffer
  decode(): T
  ack():   void
  nack():  void
  nackDelay(delayMs: number): void
}

// Factory — uses Object.defineProperty for O(1) getter access (no Proxy overhead).
export function makeLazyMessage<T extends Record<string, unknown>>(
  raw:       Buffer,
  codec:     Encoding<T>,
  fields:    string[],
  onAck:     () => void,
  onNack:    () => void,
  onNackDelay?: (ms: number) => void,
): LazyMessage<T> {
  let cache: T | undefined
  const lazy = (): T => cache ??= codec.decode(raw)

  const msg: Record<string, unknown> = {
    _raw:      raw,
    decode:    lazy,
    ack:       onAck,
    nack:      onNack,
    nackDelay: onNackDelay ?? onNack,
  }

  for (const key of fields) {
    Object.defineProperty(msg, key, {
      get:        () => lazy()[key as keyof T],
      enumerable: true,
    })
  }

  return msg as LazyMessage<T>
}
