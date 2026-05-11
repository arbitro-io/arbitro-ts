import { describe, it, expect, vi } from 'vitest'
import { makeLazyMessage } from '../src/topic/lazy-message'
import { Codec } from '../src/utils/codec'

interface Order { id: number; status: string }

const OrderCodec = new Codec<Order>({ id: 'number', status: 'string' })

function makeMsg(value: Order, onAck = () => {}, onNack = () => {}, onNackDelay?: (ms: number) => void) {
  const raw = OrderCodec.encode(value)
  return makeLazyMessage(raw, OrderCodec, OrderCodec.fields, onAck, onNack, onNackDelay)
}

describe('LazyMessage', () => {
  it('field access via getter without string', () => {
    const msg = makeMsg({ id: 42, status: 'pending' })
    expect(msg.id).toBe(42)
    expect(msg.status).toBe('pending')
  })

  it('decode is lazy — not called until field accessed', () => {
    const decodeSpy = vi.spyOn(OrderCodec, 'decode')
    const onAck = vi.fn()
    const msg = makeMsg({ id: 1, status: 'ok' }, onAck)

    // No field access yet — decode should not have been called
    expect(decodeSpy).not.toHaveBeenCalled()

    msg.ack()
    expect(onAck).toHaveBeenCalledOnce()
    expect(decodeSpy).not.toHaveBeenCalled()  // ack without read = zero decode

    decodeSpy.mockRestore()
  })

  it('decode is called exactly once across multiple field accesses', () => {
    const decodeSpy = vi.spyOn(OrderCodec, 'decode')
    const msg = makeMsg({ id: 1, status: 'ok' })

    const _ = msg.id      // first access — triggers decode
    const __ = msg.status // second access — uses cache

    expect(decodeSpy).toHaveBeenCalledOnce()
    decodeSpy.mockRestore()
  })

  it('decode() returns full object', () => {
    const msg = makeMsg({ id: 7, status: 'done' })
    expect(msg.decode()).toEqual({ id: 7, status: 'done' })
  })

  it('_raw exposes original buffer', () => {
    const value = { id: 1, status: 'x' }
    const raw   = OrderCodec.encode(value)
    const msg   = makeLazyMessage(raw, OrderCodec, OrderCodec.fields, () => {}, () => {})
    expect(msg._raw).toBe(raw)
  })

  it('ack calls onAck', () => {
    const onAck = vi.fn()
    makeMsg({ id: 1, status: 'x' }, onAck).ack()
    expect(onAck).toHaveBeenCalledOnce()
  })

  it('nack calls onNack', () => {
    const onNack = vi.fn()
    makeMsg({ id: 1, status: 'x' }, () => {}, onNack).nack()
    expect(onNack).toHaveBeenCalledOnce()
  })

  it('nackDelay calls onNackDelay with delay ms', () => {
    const onNackDelay = vi.fn()
    makeMsg({ id: 1, status: 'x' }, () => {}, () => {}, onNackDelay).nackDelay(5000)
    expect(onNackDelay).toHaveBeenCalledWith(5000)
  })

  it('nackDelay falls back to onNack when no delay callback', () => {
    const onNack = vi.fn()
    makeMsg({ id: 1, status: 'x' }, () => {}, onNack).nackDelay(100)
    expect(onNack).toHaveBeenCalledOnce()
  })
})
