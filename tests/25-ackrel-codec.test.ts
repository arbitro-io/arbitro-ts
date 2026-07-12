import { describe, it, expect } from 'vitest'
import { HEADER_SIZE, Action } from '../src/proto/constants'
import {
  packAckStateReq, packAckBatch,
  unpackAckStateRep, unpackAckBatchResp,
} from '../src/proto/ackrel'

// ── packAckStateReq ──────────────────────────────────────────────────────
//
// Body layout (8B, arbitro-proto/src/v2/ingress/ack_state.rs):
//   offset 0: consumer_id u32 LE
//   offset 4: generation  u32 LE

describe('packAckStateReq', () => {
  it('encodes header + body per fixture', () => {
    const buf = packAckStateReq(1n, 42, 7n)

    expect(buf.length).toBe(HEADER_SIZE + 8)
    // header
    expect(buf.readUInt16LE(0)).toBe(Action.AckStateReq) // action
    expect(buf.readUInt32LE(4)).toBe(8)                  // msg_len
    expect(buf.readBigUInt64LE(8)).toBe(1n)              // seq
    // body
    expect(buf.readUInt32LE(HEADER_SIZE)).toBe(42)       // consumer_id
    expect(buf.readUInt32LE(HEADER_SIZE + 4)).toBe(7)    // generation
  })

  it('round-trips against a hand-crafted fixture buffer', () => {
    // Fixture: seq=1, consumer_id=42, generation=7
    //   header: action=0x0A01 LE, flags=0, entry_flags=0, msg_len=8 LE, seq=1 LE
    //   body:   consumer_id=42 LE (u32), generation=7 LE (u32)
    const fixture = Buffer.concat([
      Buffer.from([0x01, 0x0A, 0x00, 0x00]),             // action=0x0A01, flags=0, entry_flags=0
      Buffer.from([0x08, 0x00, 0x00, 0x00]),             // msg_len=8
      Buffer.from([0x01, 0, 0, 0, 0, 0, 0, 0]),          // seq=1
      Buffer.from([0x2A, 0x00, 0x00, 0x00]),             // consumer_id=42
      Buffer.from([0x07, 0x00, 0x00, 0x00]),             // generation=7
    ])
    const actual = packAckStateReq(1n, 42, 7n)
    expect(actual).toEqual(fixture)
  })
})

// ── packAckBatch ─────────────────────────────────────────────────────────
//
// Body layout (16B fixed + N*8B, ingress/ack_state.rs):
//   offset 0:  consumer_id u32 LE
//   offset 4:  generation  u32 LE
//   offset 8:  flags       u32 LE
//   offset 12: seq_count   u32 LE
//   tail:      seqs[u64 LE] * seq_count

describe('packAckBatch', () => {
  it('round-trips consumer_id/generation/flags/seq_count/seqs', () => {
    const seqs = [1n, 2n, 100n]
    const buf = packAckBatch(7n, 77, 3n, 0, seqs)

    expect(buf.length).toBe(HEADER_SIZE + 16 + seqs.length * 8)
    expect(buf.readUInt16LE(0)).toBe(Action.AckBatch)
    expect(buf.readUInt32LE(4)).toBe(16 + seqs.length * 8) // msg_len
    expect(buf.readBigUInt64LE(8)).toBe(7n)                // seq

    expect(buf.readUInt32LE(HEADER_SIZE)).toBe(77)         // consumer_id
    expect(buf.readUInt32LE(HEADER_SIZE + 4)).toBe(3)      // generation
    expect(buf.readUInt32LE(HEADER_SIZE + 8)).toBe(0)      // flags
    expect(buf.readUInt32LE(HEADER_SIZE + 12)).toBe(3)     // seq_count

    let off = HEADER_SIZE + 16
    for (const s of seqs) {
      expect(buf.readBigUInt64LE(off)).toBe(s)
      off += 8
    }
  })

  it('propagates non-zero flags', () => {
    const buf = packAckBatch(1n, 1, 0n, 0xABCD, [1n])
    expect(buf.readUInt32LE(HEADER_SIZE + 8)).toBe(0xABCD)
  })
})

// ── unpackAckStateRep ─────────────────────────────────────────────────────
//
// Body layout (40B, arbitro-proto/src/v2/egress/ack_state.rs):
//   offset 0:  consumer_id u32 LE
//   offset 4:  generation  u32 LE
//   offset 8:  cursor      u64 LE
//   offset 16: low_seq     u64 LE
//   offset 24: high_seq    u64 LE
//   offset 32: status      u32 LE
//   offset 36: _pad        u32 LE (ignored)

describe('unpackAckStateRep', () => {
  it('parses a hand-crafted fixture body', () => {
    // consumer_id=42, generation=3, cursor=1000, low_seq=500, high_seq=2000, status=0
    const body = Buffer.alloc(40)
    body.writeUInt32LE(42, 0)
    body.writeUInt32LE(3, 4)
    body.writeBigUInt64LE(1000n, 8)
    body.writeBigUInt64LE(500n, 16)
    body.writeBigUInt64LE(2000n, 24)
    body.writeUInt32LE(0, 32)
    body.writeUInt32LE(0, 36) // _pad

    const parsed = unpackAckStateRep(body)
    expect(parsed).toEqual({
      consumerId: 42,
      generation: 3n,
      cursor:     1000n,
      lowSeq:     500n,
      highSeq:    2000n,
      status:     0,
    })
  })

  it('parses a non-zero status (e.g. generation mismatch)', () => {
    const body = Buffer.alloc(40)
    body.writeUInt32LE(9, 0)
    body.writeUInt32LE(1, 4)
    body.writeBigUInt64LE(0n, 8)
    body.writeBigUInt64LE(0n, 16)
    body.writeBigUInt64LE(0n, 24)
    body.writeUInt32LE(2, 32) // ACK_STATUS_GENERATION_MISMATCH
    body.writeUInt32LE(0, 36)

    const parsed = unpackAckStateRep(body)
    expect(parsed.status).toBe(2)
    expect(parsed.consumerId).toBe(9)
  })
})

// ── unpackAckBatchResp ────────────────────────────────────────────────────
//
// Body layout (32B, arbitro-proto/src/v2/egress/ack_state.rs):
//   offset 0:  consumer_id     u32 LE
//   offset 4:  new_cursor      u64 LE
//   offset 12: accepted        u32 LE
//   offset 16: ignored         u32 LE
//   offset 20: below_retention u32 LE
//   offset 24: still_pending   u32 LE
//   offset 28: status          u32 LE

describe('unpackAckBatchResp', () => {
  it('parses a hand-crafted fixture body', () => {
    // consumer_id=77, new_cursor=3000, accepted=10, ignored=2, below_retention=1,
    // still_pending=0, status=0
    const body = Buffer.alloc(32)
    body.writeUInt32LE(77, 0)
    body.writeBigUInt64LE(3000n, 4)
    body.writeUInt32LE(10, 12)
    body.writeUInt32LE(2, 16)
    body.writeUInt32LE(1, 20)
    body.writeUInt32LE(0, 24)
    body.writeUInt32LE(0, 28)

    const parsed = unpackAckBatchResp(body)
    expect(parsed).toEqual({
      consumerId:     77,
      newCursor:      3000n,
      accepted:       10,
      ignored:        2,
      belowRetention: 1,
      stillPending:   0,
      status:         0,
    })
  })
})
