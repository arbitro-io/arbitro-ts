import { describe, it, expect } from 'vitest'
import { Codec, JsonCodec, StringCodec, TextEncoding } from '../src/utils/codec'
import type { Encoding } from '../src/utils/codec'

// ── Encoding<T> contract ───────────────────────────────────────────────────

function roundtrip<T>(codec: Encoding<T>, value: T): T {
  return codec.decode(codec.encode(value))
}

// ── StringCodec ────────────────────────────────────────────────────────────

describe('StringCodec', () => {
  it('extends TextEncoding', () => {
    expect(new StringCodec()).toBeInstanceOf(TextEncoding)
  })

  it('roundtrip utf-8', () => {
    const c = new StringCodec()
    expect(roundtrip(c, 'hello world')).toBe('hello world')
  })

  it('roundtrip empty string', () => {
    expect(roundtrip(new StringCodec(), '')).toBe('')
  })

  it('encodes to Buffer', () => {
    const buf = new StringCodec().encode('hi')
    expect(Buffer.isBuffer(buf)).toBe(true)
    expect(buf).toEqual(Buffer.from('hi', 'utf8'))
  })

  it('different encodings produce different buffers for non-ascii', () => {
    const utf8   = new StringCodec('utf8')
    const latin1 = new StringCodec('latin1')
    // 'café' is 5 bytes in latin1, 6 bytes in utf8 (é = 2 bytes)
    expect(utf8.encode('café').length).toBeGreaterThan(latin1.encode('café').length)
  })
})

// ── JsonCodec<T> ───────────────────────────────────────────────────────────

describe('JsonCodec', () => {
  it('does NOT extend TextEncoding', () => {
    expect(new JsonCodec()).not.toBeInstanceOf(TextEncoding)
  })

  it('roundtrip object', () => {
    const c = new JsonCodec<{ id: number; name: string }>()
    expect(roundtrip(c, { id: 1, name: 'test' })).toEqual({ id: 1, name: 'test' })
  })

  it('roundtrip array', () => {
    const c = new JsonCodec<number[]>()
    expect(roundtrip(c, [1, 2, 3])).toEqual([1, 2, 3])
  })

  it('roundtrip nested', () => {
    const c   = new JsonCodec<{ a: { b: number } }>()
    const val = { a: { b: 42 } }
    expect(roundtrip(c, val)).toEqual(val)
  })

  it('encodes to valid JSON Buffer', () => {
    const buf = new JsonCodec().encode({ x: 1 })
    expect(() => JSON.parse(buf.toString())).not.toThrow()
  })
})

// ── Codec<T> ───────────────────────────────────────────────────────────────

describe('Codec', () => {
  interface Order { id: number; status: string; amount: number }

  const OrderCodec = new Codec<Order>({ id: 'number', status: 'string', amount: 'number' })

  it('roundtrip known fields', () => {
    const val = { id: 1, status: 'pending', amount: 99.99 }
    expect(roundtrip(OrderCodec, val)).toEqual(val)
  })

  it('exposes fields array matching schema keys', () => {
    expect(OrderCodec.fields).toEqual(['id', 'status', 'amount'])
  })

  it('encode strips unknown fields', () => {
    const val = { id: 1, status: 'ok', amount: 5, extra: 'ignored' } as Order & { extra: string }
    const decoded = OrderCodec.decode(OrderCodec.encode(val))
    expect((decoded as Record<string, unknown>)['extra']).toBeUndefined()
  })

  it('roundtrip empty-string fields', () => {
    expect(roundtrip(OrderCodec, { id: 0, status: '', amount: 0 }))
      .toEqual({ id: 0, status: '', amount: 0 })
  })

  it('encoded buffer is smaller than JSON equivalent', () => {
    const val  = { id: 1, status: 'pending', amount: 99.99 }
    const mp   = OrderCodec.encode(val).length
    const json = Buffer.from(JSON.stringify(val)).length
    expect(mp).toBeLessThan(json)
  })
})
