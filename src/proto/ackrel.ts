// Ack-reliability wire codecs — AckStateReq/Rep (0x0A01/0x0A02) +
// AckBatch/AckBatchResp (0x0A03/0x0A04).
//
// Mirrors `arbitro-proto/src/v2/ingress/ack_state.rs` (request bodies) and
// `arbitro-proto/src/v2/egress/ack_state.rs` (reply bodies). All fields
// little-endian, no padding beyond what the Rust `#[repr(C)]` layouts show.

import { HEADER_SIZE, Action } from './constants'
import { frame } from './frame'

// ── AckStateReq (client→server, 8B body) ────────────────────────────────
// offset 0: consumer_id u32
// offset 4: generation  u32

export function packAckStateReq(seq: bigint, consumerId: number, generation: bigint): Buffer {
  const buf = frame(Action.AckStateReq, seq, 8)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(Number(generation), HEADER_SIZE + 4)
  return buf
}

// ── AckBatch (client→server, 16B fixed + N*8B seqs) ─────────────────────
// offset 0:  consumer_id u32
// offset 4:  generation  u32
// offset 8:  flags       u32
// offset 12: seq_count   u32
// tail:      seqs[u64] * seq_count

export function packAckBatch(
  seq: bigint, consumerId: number, generation: bigint, flags: number, seqs: readonly bigint[],
): Buffer {
  const buf = frame(Action.AckBatch, seq, 16 + seqs.length * 8)
  buf.writeUInt32LE(consumerId, HEADER_SIZE)
  buf.writeUInt32LE(Number(generation), HEADER_SIZE + 4)
  buf.writeUInt32LE(flags, HEADER_SIZE + 8)
  buf.writeUInt32LE(seqs.length, HEADER_SIZE + 12)
  let off = HEADER_SIZE + 16
  for (const s of seqs) {
    buf.writeBigUInt64LE(s, off)
    off += 8
  }
  return buf
}

// ── AckStateRep (server→client, 40B body) ────────────────────────────────
// offset 0:  consumer_id u32
// offset 4:  generation  u32
// offset 8:  cursor      u64
// offset 16: low_seq     u64
// offset 24: high_seq    u64
// offset 32: status      u32
// offset 36: _pad        u32

export interface AckStateRepBody {
  consumerId: number
  generation: bigint
  cursor:     bigint
  lowSeq:     bigint
  highSeq:    bigint
  status:     number
}

export function unpackAckStateRep(body: Buffer): AckStateRepBody {
  return {
    consumerId: body.readUInt32LE(0),
    generation: BigInt(body.readUInt32LE(4)),
    cursor:     body.readBigUInt64LE(8),
    lowSeq:     body.readBigUInt64LE(16),
    highSeq:    body.readBigUInt64LE(24),
    status:     body.readUInt32LE(32),
  }
}

// ── AckBatchResp (server→client, 32B body) ───────────────────────────────
// offset 0:  consumer_id     u32
// offset 4:  new_cursor      u64
// offset 12: accepted        u32
// offset 16: ignored         u32
// offset 20: below_retention u32
// offset 24: still_pending   u32
// offset 28: status          u32

export interface AckBatchRespBody {
  consumerId:     number
  newCursor:      bigint
  accepted:       number
  ignored:        number
  belowRetention: number
  stillPending:   number
  status:         number
}

export function unpackAckBatchResp(body: Buffer): AckBatchRespBody {
  return {
    consumerId:     body.readUInt32LE(0),
    newCursor:      body.readBigUInt64LE(4),
    accepted:       body.readUInt32LE(12),
    ignored:        body.readUInt32LE(16),
    belowRetention: body.readUInt32LE(20),
    stillPending:   body.readUInt32LE(24),
    status:         body.readUInt32LE(28),
  }
}
