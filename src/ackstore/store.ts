// Pluggable delivered-set store for the client's redelivery-dedup guarantee.
// Mirrors `arbitro-client-tokio/src/ackstore/store.rs` (and Go's
// `internal/ackstore/store.go`) one-for-one.
//
// On each incoming Deliver frame the client asks the store "have I already
// run the user handler for this message?" — a "yes" means silently re-ack, a
// "no" means run the handler.
//
// # Identity: (streamName, consumerName, seq)
//
// The dedup key is NOT the broker's numeric consumer id — that id is
// ephemeral and changes when a consumer is deleted and recreated. Because
// arbitro's `seq` is stream-scoped (the same message always has the same seq
// for every consumer of that stream), a durable key is
// `(streamName, consumerName, seq)`. A consumer recreated under the SAME name
// still recognizes already-processed messages; a DIFFERENT name is a fresh
// workload.
//
// # Hot-path design: resolve once, bounds-gate per message
//
// String keys are hashed once per subscription via `Store.slot`, returning a
// `SlotRef`. The delivery hot path calls `SlotRef.seen` — a `(minSeq, maxSeq)`
// bounds probe — and only falls back to the exact set check when the bounds
// gate is inconclusive.

/** Errors surfaced by store operations. */
export class AckStoreError extends Error {
  constructor(
    message: string,
    readonly kind: 'closed' | 'slot-overflow' | 'name-too-long' | 'unknown-slot' | 'io' | 'corrupt',
    options?: { cause?: unknown },
  ) {
    super(`ackstore: ${message}`, options)
    this.name = 'AckStoreError'
  }
}

/** Introspection view of one slot. */
export interface SlotInfo {
  slotId: number
  stream: string
  consumer: string
  /** Live seqs (recorded, not yet confirmed/expired). */
  live: number
  /** Smallest live seq (0n if empty). */
  minSeq: bigint
  /** Largest live seq (0n if empty). */
  maxSeq: bigint
  /** Timestamp (unix ms) of the oldest live seq (0 if empty). */
  oldestTsMs: number
  /** When this slot handle was created (unix ms). */
  registeredAtMs: number
}

/** Point-in-time snapshot of store counters. */
export interface StoreMetrics {
  slots: number
  liveEntries: number
  recorded: number
  confirmed: number
  expired: number
  registered: number
  tombstoned: number
  syncErrors: number
  fileSize: number
  lastSyncMs: number
  lastSnapshotMs: number
}

/**
 * A resolved handle to one `(stream, consumer)` pair. The client caches it
 * for the subscription's lifetime and uses it on the hot path.
 *
 * Recommended usage (record-at-ack):
 * ```ts
 * // on delivery:   if (!slot.seen(seq)) runHandler()
 * // on ack:        slot.record(seq)          // buffered write
 * // on broker-ack: slot.confirmUpTo(cursor)  // driven by AckBatchResp
 * ```
 */
export interface SlotRef {
  /** Has `seq` already been recorded? Bounds probe first, exact set second. */
  seen(seq: bigint): boolean

  /** Mark `seq` processed/acked and persist it. Idempotent. */
  record(seq: bigint): void

  /** Raw bounds gate: `true` = `seq` is outside the recorded range. */
  fresh(seq: bigint): boolean

  /** Atomic test-and-set: `true` when newly recorded, `false` on a duplicate. */
  checkRecord(seq: bigint): boolean

  /** Mark `seq` broker-acked: drop from the live set, append a Confirm. */
  confirm(seq: bigint): void

  /**
   * Drop every live seq `<= cursor` in one pass. Driven by the server's ack
   * cursor (`AckBatchResp.newCursor` / `AckStateRep.cursor`): the broker never
   * redelivers at or below it. Returns the count dropped.
   */
  confirmUpTo(cursor: bigint): number

  /** This slot's current metadata. */
  info(): SlotInfo
}

/** The pluggable delivered-set store. */
export interface Store {
  /** Resolve `(stream, consumer)` to a handle, registering it on first use. */
  slot(stream: string, consumer: string): SlotRef

  /** Flush buffered writes and (if configured) fsync. */
  sync(): void

  /** Rebuild in-memory state from persistent storage (no-op for memory). */
  restore(): void

  /** Flush and release resources. After close, mutating methods throw. */
  close(): void

  /** Snapshot of every known slot's metadata. */
  listSlots(): SlotInfo[]

  /** Metadata for one slot, `undefined` if unknown. */
  slotInfo(stream: string, consumer: string): SlotInfo | undefined

  /** Forget a slot entirely (all its seqs), persisting a tombstone. */
  deleteSlot(stream: string, consumer: string): void

  /** Cumulative counters and live gauges. */
  metrics(): StoreMetrics
}

/** Composite key for the (stream, consumer) symbol table. Length-prefixed so
 * ("ab", "c") and ("a", "bc") can never collide — the Rust/Go stores use a
 * NUL separator for the same reason. In-memory only; it never reaches the log. */
export function slotKey(stream: string, consumer: string): string {
  return stream.length + ":" + stream + consumer
}
