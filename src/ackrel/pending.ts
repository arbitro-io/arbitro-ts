// Per-consumer pending-ack tracking — mirrors
// `arbitro-client-tokio/src/ackrel/pending.rs`'s `ConsumerPending`.
//
// Tracks which delivery `seq`s this consumer has acked locally (client
// side) but the broker has not yet confirmed, plus the consumer's current
// "generation" (bumped on every reconnect so the broker can tell a stale
// pre-reconnect ack apart from a fresh one).

export class ConsumerPending {
  generation = 0n
  private readonly pending = new Map<bigint, number>() // seq -> inserted-at (ms)

  /** Record `seq` as pending (no-op if already tracked). */
  record(seq: bigint, nowMs: number = Date.now()): void {
    if (!this.pending.has(seq)) this.pending.set(seq, nowMs)
  }

  /** Bump generation on reconnect. Existing pending seqs are NOT dropped —
   * they still count as pending and get resent under the new generation. */
  bumpGeneration(): void {
    this.generation += 1n
  }

  /** Current pending seqs (unordered). */
  seqs(): bigint[] {
    return Array.from(this.pending.keys())
  }

  /** Remove an explicit list of seqs. Returns how many were actually removed. */
  remove(seqs: readonly bigint[]): number {
    let removed = 0
    for (const s of seqs) {
      if (this.pending.delete(s)) removed++
    }
    return removed
  }

  /** Remove every pending seq matching `pred`. Returns the removed seqs. */
  removeWhere(pred: (seq: bigint) => boolean): bigint[] {
    const hit: bigint[] = []
    for (const seq of this.pending.keys()) {
      if (pred(seq)) hit.push(seq)
    }
    for (const seq of hit) this.pending.delete(seq)
    return hit
  }

  size(): number {
    return this.pending.size
  }

  /** Age (ms) of the oldest still-pending entry, 0 if empty. */
  oldestMs(nowMs: number = Date.now()): number {
    let oldest = 0
    for (const insertedAt of this.pending.values()) {
      const age = nowMs - insertedAt
      if (age > oldest) oldest = age
    }
    return oldest
  }
}
