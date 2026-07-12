// Ack-reliability hot tier — in-memory `AckRelay` (per-consumer pending
// state, generation tracking, reconciliation against broker responses)
// mirroring `arbitro-client-tokio/src/ackrel/mod.rs` + `pending.rs` +
// `seen.rs`.
//
// Cold tier (SQLite persistence, `arbitro-client-tokio/src/ackrel/cold.rs`)
// is INTENTIONALLY OUT OF SCOPE for v1: users needing restart durability
// should design idempotent handlers. The hot tier survives reconnects
// (state lives in this in-memory `AckRelay`, not the socket) but not a
// process restart.

import { ConsumerPending } from './pending'
import { SeenCache } from './seen'
import type { ClientMetrics } from '../client/metrics'

export { ConsumerPending, SeenCache }

export interface AckRelayStats {
  /** Total pending-ack count across every tracked consumer. */
  hotCount: number
  /** Age (ms) of the single oldest pending entry across all consumers. */
  oldestPendingMs: number
}

export class AckRelay {
  private readonly consumers = new Map<number, ConsumerPending>()
  private metrics?: ClientMetrics

  setMetrics(m: ClientMetrics): void {
    this.metrics = m
  }

  private getOrCreate(consumerId: number): ConsumerPending {
    let c = this.consumers.get(consumerId)
    if (!c) {
      c = new ConsumerPending()
      this.consumers.set(consumerId, c)
    }
    return c
  }

  /** Record `seq` as pending for `consumerId` — call when the ack write
   * failed or the socket was down before flush. */
  record(consumerId: number, seq: bigint): void {
    this.getOrCreate(consumerId).record(seq)
    if (this.metrics) this.metrics.acksDeferred++
  }

  /** Bump generation on reconnect. Pending seqs survive — they're resent
   * (via the sweep loop / AckStateReq replay) tagged with the new generation. */
  bumpGeneration(consumerId: number): void {
    this.getOrCreate(consumerId).bumpGeneration()
  }

  generationOf(consumerId: number): bigint {
    return this.consumers.get(consumerId)?.generation ?? 0n
  }

  /** Current pending set for `consumerId` — feeds `packAckBatch` on the
   * sweep loop and the reconnect replay path. */
  pendingSeqs(consumerId: number): bigint[] {
    return this.consumers.get(consumerId)?.seqs() ?? []
  }

  /** Remove `seqs` from `consumerId`'s pending set (broker confirmed them). */
  confirm(consumerId: number, seqs: readonly bigint[]): void {
    if (seqs.length === 0) return
    const c = this.consumers.get(consumerId)
    if (!c) return
    const removed = c.remove(seqs)
    if (this.metrics && removed > 0) this.metrics.acksConfirmed += removed
  }

  /** Reconcile against an `AckStateRep`: seqs <= cursor are confirmed by
   * the broker; seqs < lowSeq fell below the broker's retention window
   * and will never be accepted — drop them as expired. */
  applyAckStateRep(consumerId: number, cursor: bigint, lowSeq: bigint): void {
    const c = this.consumers.get(consumerId)
    if (!c) return
    const expired = c.removeWhere((seq) => seq < lowSeq)
    const confirmed = c.removeWhere((seq) => seq <= cursor)
    if (this.metrics) {
      if (expired.length) this.metrics.acksExpired += expired.length
      if (confirmed.length) this.metrics.acksConfirmed += confirmed.length
    }
  }

  /** Reconcile against an `AckBatchResp`: seqs <= newCursor are confirmed;
   * `belowRetentionCount` entries reported by the broker are counted as
   * expired (the response doesn't carry their exact seqs). */
  applyAckBatchResp(consumerId: number, newCursor: bigint, belowRetentionCount: number): void {
    const c = this.consumers.get(consumerId)
    if (!c) return
    const confirmed = c.removeWhere((seq) => seq <= newCursor)
    if (this.metrics) {
      if (confirmed.length) this.metrics.acksConfirmed += confirmed.length
      if (belowRetentionCount > 0) this.metrics.acksExpired += belowRetentionCount
    }
  }

  /** Consumer ids currently tracked (for the sweep loop / reconnect replay). */
  consumerIds(): number[] {
    return Array.from(this.consumers.keys())
  }

  stats(): AckRelayStats {
    let hotCount = 0
    let oldestPendingMs = 0
    const now = Date.now()
    for (const c of this.consumers.values()) {
      hotCount += c.size()
      const oldest = c.oldestMs(now)
      if (oldest > oldestPendingMs) oldestPendingMs = oldest
    }
    return { hotCount, oldestPendingMs }
  }
}
