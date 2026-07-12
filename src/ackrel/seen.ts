// Bounded FIFO dedup cache — mirrors `arbitro-client-tokio/src/ackrel/seen.rs`.
//
// Consulted in the deliver dispatch path before invoking the user's
// message callback: if `(consumerId, seq)` was already seen (e.g. the
// broker redelivered a message the client had already processed but
// whose ack hadn't been confirmed yet), the caller re-acks without
// running the handler again — avoids duplicate side effects.
//
// Avoids composite string keys on the hot path: lookup is a nested
// `Map<number, Set<bigint>>`, eviction order is a flat array of the raw
// (consumerId, seq) pairs.

const DEFAULT_CAPACITY = 1_000_000

export class SeenCache {
  private readonly byConsumer = new Map<number, Set<bigint>>()
  private readonly order: Array<{ cid: number; seq: bigint }> = []

  constructor(private readonly capacity: number = DEFAULT_CAPACITY) {}

  /** Returns `true` if `(cid, seq)` is new (caller should process it),
   * `false` if already seen (caller should re-ack without dispatching). */
  insertIfNew(cid: number, seq: bigint): boolean {
    let set = this.byConsumer.get(cid)
    if (!set) {
      set = new Set<bigint>()
      this.byConsumer.set(cid, set)
    }
    if (set.has(seq)) return false

    set.add(seq)
    this.order.push({ cid, seq })
    if (this.order.length > this.capacity) this.evictOldest()
    return true
  }

  private evictOldest(): void {
    const oldest = this.order.shift()
    if (!oldest) return
    const set = this.byConsumer.get(oldest.cid)
    if (!set) return
    set.delete(oldest.seq)
    if (set.size === 0) this.byConsumer.delete(oldest.cid)
  }

  /** Current entry count — for tests / metrics. */
  size(): number {
    return this.order.length
  }
}
