import { describe, it, expect } from 'vitest'
import { AckRelay, SeenCache } from '../src/ackrel'
import { Message } from '../src/message/message'
import { ClientMetrics } from '../src/client/metrics'
import { packAck } from '../src/proto/v2'
import { HEADER_SIZE, Action } from '../src/proto/constants'

// Builds a minimal Deliver-shaped frame carrying consumer_id/subject_hash/
// subject/reply_to/payload — enough for Message's getters to work.
function makeDeliverFrame(seq: bigint, consumerId: number, subject = 'x'): Buffer {
  const subjBuf = Buffer.from(subject)
  const bodyLen = 12 + subjBuf.length
  const buf = Buffer.alloc(HEADER_SIZE + bodyLen)
  buf.writeUInt16LE(Action.Deliver, 0)
  buf.writeUInt32LE(bodyLen, 4)
  buf.writeBigUInt64LE(seq, 8)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)      // consumer_id
  buf.writeUInt32LE(0xC0FFEE, HEADER_SIZE + 4)    // subject_hash
  buf.writeUInt16LE(subjBuf.length, HEADER_SIZE + 8)
  buf.writeUInt16LE(0, HEADER_SIZE + 10)          // reply_to_len
  subjBuf.copy(buf, HEADER_SIZE + 12)
  return buf
}

describe('AckRelay — record / pendingSeqs / generation', () => {
  it('record() adds to the per-consumer pending set', () => {
    const relay = new AckRelay()
    relay.record(7, 1n)
    relay.record(7, 2n)
    expect(relay.pendingSeqs(7).sort()).toEqual([1n, 2n])
    expect(relay.pendingSeqs(9)).toEqual([]) // untouched consumer
  })

  it('bumpGeneration increments without dropping pending seqs', () => {
    const relay = new AckRelay()
    relay.record(1, 5n)
    expect(relay.generationOf(1)).toBe(0n)
    relay.bumpGeneration(1)
    expect(relay.generationOf(1)).toBe(1n)
    expect(relay.pendingSeqs(1)).toEqual([5n]) // still pending, not dropped
  })

  it('stats() reports hotCount across consumers', () => {
    const relay = new AckRelay()
    relay.record(1, 1n)
    relay.record(1, 2n)
    relay.record(2, 3n)
    const stats = relay.stats()
    expect(stats.hotCount).toBe(3)
    expect(stats.oldestPendingMs).toBeGreaterThanOrEqual(0)
  })
})

describe('AckRelay — confirm / reconciliation trims pending', () => {
  it('confirm() removes an explicit seq list', () => {
    const relay = new AckRelay()
    relay.record(1, 1n)
    relay.record(1, 2n)
    relay.record(1, 3n)
    relay.confirm(1, [1n, 2n])
    expect(relay.pendingSeqs(1)).toEqual([3n])
  })

  it('applyAckBatchResp trims everything <= newCursor (simulated AckBatchResp)', () => {
    const metrics = new ClientMetrics()
    const relay = new AckRelay()
    relay.setMetrics(metrics)
    relay.record(42, 10n)
    relay.record(42, 11n)
    relay.record(42, 12n)

    // Simulated AckBatchResp: broker confirmed up to seq 11, none below retention.
    relay.applyAckBatchResp(42, 11n, 0)

    expect(relay.pendingSeqs(42)).toEqual([12n])
    expect(metrics.acksConfirmed).toBe(2)
    expect(metrics.acksExpired).toBe(0)
  })

  it('applyAckStateRep drops seqs below retention as expired and confirms <= cursor', () => {
    const metrics = new ClientMetrics()
    const relay = new AckRelay()
    relay.setMetrics(metrics)
    relay.record(5, 1n)  // below lowSeq -> expired
    relay.record(5, 8n)  // <= cursor -> confirmed
    relay.record(5, 20n) // still pending

    relay.applyAckStateRep(5, /* cursor */ 10n, /* lowSeq */ 5n)

    expect(relay.pendingSeqs(5)).toEqual([20n])
    expect(metrics.acksExpired).toBe(1)
    expect(metrics.acksConfirmed).toBe(1)
  })
})

describe('Message.ack() fallback to AckRelay on send failure', () => {
  it('records into AckRelay when the underlying send fails', () => {
    const relay = new AckRelay()
    const frame = makeDeliverFrame(123n, 7)
    let acked = false

    const msg = new Message(
      frame,
      () => false, // simulate: socket down / write failed
      () => 1n,
      () => { acked = true },
      undefined,
      (consumerId, seq) => relay.record(consumerId, seq),
    )

    msg.ack()

    expect(acked).toBe(false) // onAck must NOT fire on failure
    expect(relay.pendingSeqs(7)).toEqual([123n])
  })

  it('does not touch AckRelay when the send succeeds', () => {
    const relay = new AckRelay()
    const frame = makeDeliverFrame(9n, 3)
    let acked = false

    const msg = new Message(
      frame,
      (f) => { expect(f.readUInt16LE(0)).toBe(Action.Ack); return true },
      () => 1n,
      () => { acked = true },
      undefined,
      (consumerId, seq) => relay.record(consumerId, seq),
    )

    msg.ack()

    expect(acked).toBe(true)
    expect(relay.pendingSeqs(3)).toEqual([])
  })

  it('recovers across N failed attempts then a success (simulated retry loop)', () => {
    const relay = new AckRelay()
    const frame = makeDeliverFrame(55n, 4)
    let attempts = 0
    const FAIL_COUNT = 3

    const send = (): boolean => {
      attempts++
      return attempts > FAIL_COUNT
    }

    const msg = new Message(
      frame, send, () => 1n, undefined, undefined,
      (consumerId, seq) => relay.record(consumerId, seq),
    )

    for (let i = 0; i < FAIL_COUNT; i++) msg.ack()
    expect(relay.pendingSeqs(4)).toEqual([55n]) // recorded once, not duplicated

    msg.ack() // succeeds on the 4th attempt
    expect(attempts).toBe(FAIL_COUNT + 1)
    // AckRelay isn't auto-cleared by a successful direct ack (only by
    // confirm()/applyAck*Resp() reconciliation) — this documents that a
    // successful direct ack of an already-pending seq leaves it pending
    // until the broker's own confirmation arrives.
    expect(relay.pendingSeqs(4)).toEqual([55n])
  })
})

describe('AckRelay wired via packAck round-trip sanity', () => {
  it('packAck used by Message.ack() carries the expected consumer/seq', () => {
    const buf = packAck(1n, 7, 0xC0FFEE, 123n)
    expect(buf.readUInt32LE(HEADER_SIZE)).toBe(7)
    expect(buf.readBigUInt64LE(HEADER_SIZE + 8)).toBe(123n)
  })
})

describe('SeenCache — dedup + bounded FIFO eviction', () => {
  it('insertIfNew returns true once, false on repeat', () => {
    const seen = new SeenCache(10)
    expect(seen.insertIfNew(1, 100n)).toBe(true)
    expect(seen.insertIfNew(1, 100n)).toBe(false)
    expect(seen.insertIfNew(1, 101n)).toBe(true)
    expect(seen.insertIfNew(2, 100n)).toBe(true) // different consumer, same seq
  })

  it('evicts the oldest entry once capacity is exceeded', () => {
    const seen = new SeenCache(3)
    seen.insertIfNew(1, 1n)
    seen.insertIfNew(1, 2n)
    seen.insertIfNew(1, 3n)
    expect(seen.size()).toBe(3)

    seen.insertIfNew(1, 4n) // evicts (1,1n)
    expect(seen.size()).toBe(3)
    expect(seen.insertIfNew(1, 1n)).toBe(true)  // (1,1n) was evicted — new again
    expect(seen.insertIfNew(1, 4n)).toBe(false) // (1,4n) still tracked
  })
})
