import { describe, it, expect } from 'vitest'
import { Unpackr } from 'msgpackr'
import { serializeStreamConfig, serializeConsumerConfig, serializeDeleteStreamOpts } from '../../src/client/serialize'
import { DeliverPolicy, JournalType } from '../../src/types/config'

const unpackr = new Unpackr({ useRecords: false })
const unpack  = (buf: Buffer) => unpackr.unpack(buf) as Record<string, unknown>

describe('serializeStreamConfig', () => {
  it('encodes filter', () => {
    const buf  = serializeStreamConfig({ subjectFilter: 'orders.>' })
    const wire = unpack(buf)
    expect(wire['filter']).toBe('orders.>')
  })

  it('encodes journal_kind', () => {
    const buf  = serializeStreamConfig({ subjectFilter: 'x', journal: { type: JournalType.Tolerant } })
    const wire = unpack(buf)
    expect(wire['journal_kind']).toBe('Tolerant')
  })

  it('encodes flush config for Strict journal', () => {
    const buf  = serializeStreamConfig({
      subjectFilter: 'x',
      journal: { type: JournalType.Strict, flush: { intervalMs: 20, maxMessages: 256, maxBytes: 32768 } },
    })
    const wire = unpack(buf)
    expect(wire['flush_interval_ms']).toBe(20)
    expect(wire['flush_max_messages']).toBe(256)
    expect(wire['flush_max_bytes']).toBe(32768)
  })

  it('converts maxAgeMs to max_age_ns as bigint', () => {
    const buf  = serializeStreamConfig({ subjectFilter: 'x', maxAgeMs: 1000 })
    const wire = unpack(buf)
    expect(wire['max_age_ns']).toBe(1_000_000_000n)
  })

  it('omits optional fields when not set', () => {
    const wire = unpack(serializeStreamConfig({ subjectFilter: 'x' }))
    expect(wire['max_msgs']).toBeUndefined()
    expect(wire['max_bytes']).toBeUndefined()
    expect(wire['max_age_ns']).toBeUndefined()
    expect(wire['journal_kind']).toBeUndefined()
  })
})

describe('serializeConsumerConfig', () => {
  it('encodes group and filter', () => {
    const wire = unpack(serializeConsumerConfig({ name: 'workers', filter: 'orders.>' }))
    expect(wire['group']).toBe('workers')
    expect(wire['filter']).toBe('orders.>')
  })

  it('encodes fanout as deliver_mode Fanout', () => {
    const wire = unpack(serializeConsumerConfig({ name: 'broadcast', filter: 'events.>', fanout: true }))
    expect(wire['deliver_mode']).toBe('Fanout')
  })

  it('omits deliver_mode when fanout not set', () => {
    const wire = unpack(serializeConsumerConfig({ name: 'w', filter: 'x' }))
    expect(wire['deliver_mode']).toBeUndefined()
  })

  it('encodes deliver_policy', () => {
    const wire = unpack(serializeConsumerConfig({
      name: 'w', filter: 'x', deliverPolicy: DeliverPolicy.BySeq, startSeq: 42n,
    }))
    expect(wire['deliver_policy']).toBe('ByStartSeq')
    expect(wire['start_seq']).toBe(42n)
  })

  it('encodes max_ack_pending', () => {
    const wire = unpack(serializeConsumerConfig({ name: 'w', filter: 'x', maxAckPending: 500 }))
    expect(wire['max_ack_pending']).toBe(500)
  })

  it('encodes credit_rules', () => {
    const rules = [{ pattern: 'orders.us.>', limit: 100 }, { pattern: 'orders.eu.>', limit: 50 }]
    const wire  = unpack(serializeConsumerConfig({ name: 'w', filter: 'x', creditRules: rules }))
    expect(Array.isArray(wire['credit_rules'])).toBe(true)
    expect((wire['credit_rules'] as unknown[]).length).toBe(2)
  })

  it('omits optional fields when not set', () => {
    const wire = unpack(serializeConsumerConfig({ name: 'w', filter: 'x' }))
    expect(wire['deliver_policy']).toBeUndefined()
    expect(wire['max_ack_pending']).toBeUndefined()
    expect(wire['credit_rules']).toBeUndefined()
  })
})

describe('serializeDeleteStreamOpts', () => {
  it('encodes delete_data when set', () => {
    const wire = unpack(serializeDeleteStreamOpts({ deleteData: false }))
    expect(wire['delete_data']).toBe(false)
  })

  it('returns empty payload when opts are omitted', () => {
    expect(serializeDeleteStreamOpts()).toEqual(Buffer.alloc(0))
  })
})
