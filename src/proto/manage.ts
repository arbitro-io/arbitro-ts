// Stream + Consumer management frames.
//
// Wire format: cold-path management frames now ride as
// `[Header 16B][serde_json(body)]`. The server's `v2::cold` module
// decodes the JSON body via `serde_json::from_slice`. This file mirrors
// the Rust `cold_body!` macro definitions in
// `crates/arbitro-proto/src/v2/cold/mod.rs`.
//
// Important: Rust's `Vec<u8>` is JSON-encoded by serde as an array of
// numbers (e.g. "orders" → `[111,114,100,101,114,115]`), NOT as a UTF-8
// string. Every byte-sequence field below uses `Array.from(buffer)` to
// match. Strings would require the Rust side to opt in to
// `#[serde(with = "serde_bytes")]`, which it does not.
//
// Hot-path frames (Publish, Ack, etc.) keep the zerocopy binary format
// from their dedicated modules.

import { HEADER_SIZE, Action } from './constants'
import { frame } from './frame'

/** Build a cold-path frame: `[Header][serde_json(body)]`. */
function packCold(action: Action, seq: bigint, body: unknown): Buffer {
  const utf8 = Buffer.from(JSON.stringify(body), 'utf8')
  const buf  = frame(action, seq, utf8.length)
  utf8.copy(buf, HEADER_SIZE)
  return buf
}

/** Encode a `Buffer` as the JSON array form serde uses for `Vec<u8>`. */
function bytesArr(b: Buffer): number[] { return Array.from(b) }

// ── Stream management ──────────────────────────────────────────────────

/**
 * Cold body for `CreateStream`. `idempotencyWindowMs = 0` disables
 * broker-side dedup (default); a non-zero value enables per-stream
 * `msgId` dedup over that window.
 */
export function packCreateStream(
  seq: bigint, name: Buffer, filter: Buffer,
  maxMsgs: bigint, maxBytes: bigint, maxAgeSecs: bigint,
  replicas = 1, journalKind = 0, retention = 0, discard = 0,
  idempotencyWindowMs = 0,
): Buffer {
  return packCold(Action.CreateStream, seq, {
    name:                  bytesArr(name),
    filter:                bytesArr(filter),
    max_msgs:              Number(maxMsgs),       // u64 — fits in JS number if < 2^53
    max_bytes:             Number(maxBytes),
    max_age_secs:          Number(maxAgeSecs),
    replicas, journal_kind: journalKind, retention, discard,
    idempotency_window_ms: idempotencyWindowMs >>> 0,
  })
}

export const packDeleteStream = (seq: bigint, name: Buffer): Buffer =>
  packCold(Action.DeleteStream, seq, { name: bytesArr(name) })

export const packGetStream = (seq: bigint, name: Buffer): Buffer =>
  packCold(Action.GetStream, seq, { name: bytesArr(name) })

export const packPurgeStream = (seq: bigint, name: Buffer): Buffer =>
  packCold(Action.PurgeStream, seq, { name: bytesArr(name) })

export const packDrainSubject = (seq: bigint, name: Buffer, subject: Buffer): Buffer =>
  packCold(Action.DrainSubject, seq, {
    name:    bytesArr(name),
    subject: bytesArr(subject),
  })

export const packListStreams = (seq: bigint, offset = 0, limit = 1000): Buffer =>
  packCold(Action.ListStreams, seq, { offset: offset >>> 0, limit: limit >>> 0 })

// ── Consumer management ────────────────────────────────────────────────

/** One per-subject inflight cap. Enforced only with `ackPolicy === Explicit`. */
export interface WireSubjectLimit {
  pattern: Buffer
  limit:   number
}

export interface CreateConsumerOpts {
  streamId: number; name: Buffer; group: Buffer; filter: Buffer
  maxInflight?: number; ackPolicy?: number; deliverPolicy?: number
  deliverMode?: number; ackWaitMs?: number; startSeq?: bigint
  subjectLimits?: WireSubjectLimit[]
}

export function packCreateConsumer(seq: bigint, opts: CreateConsumerOpts): Buffer {
  const limits = (opts.subjectLimits ?? []).map(l => ({
    pattern: bytesArr(l.pattern),
    limit:   l.limit >>> 0,
  }))
  return packCold(Action.CreateConsumer, seq, {
    stream_id:      opts.streamId >>> 0,
    name:           bytesArr(opts.name),
    group:          bytesArr(opts.group),
    subject:        bytesArr(opts.filter),
    max_inflight:   Math.min(opts.maxInflight ?? 0, 0xFFFF),
    ack_policy:     opts.ackPolicy     ?? 1,
    deliver_policy: opts.deliverPolicy ?? 0,
    deliver_mode:   opts.deliverMode   ?? 0,
    ack_wait_ms:    (opts.ackWaitMs ?? 0) >>> 0,
    start_seq:      Number(opts.startSeq ?? 0n),
    subject_limits: limits,
  })
}

export const packDeleteConsumer = (seq: bigint, consumerId: number): Buffer =>
  packCold(Action.DeleteConsumer, seq, { consumer_id: consumerId >>> 0 })

export const packGetConsumer = (seq: bigint, streamId: number, name: Buffer): Buffer =>
  packCold(Action.GetConsumer, seq, {
    stream_id: streamId >>> 0,
    name:      bytesArr(name),
  })

export const packListConsumers = (seq: bigint, streamId = 0, offset = 0, limit = 1000): Buffer =>
  packCold(Action.ListConsumers, seq, {
    stream_id: streamId >>> 0,
    offset:    offset >>> 0,
    limit:     limit >>> 0,
  })

export const packConsumerStats = (seq: bigint, consumerId: number): Buffer =>
  packCold(Action.ConsumerStats, seq, { consumer_id: consumerId >>> 0 })

/** M11: pause delivery to a consumer. */
export const packPauseConsumer = (seq: bigint, consumerId: number): Buffer =>
  packCold(Action.PauseConsumer, seq, { consumer_id: consumerId >>> 0 })

/** M11: resume delivery to a previously paused consumer. */
export const packResumeConsumer = (seq: bigint, consumerId: number): Buffer =>
  packCold(Action.ResumeConsumer, seq, { consumer_id: consumerId >>> 0 })
