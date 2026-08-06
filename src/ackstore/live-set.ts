// Per-slot live set + bounds gate, shared by the memory and WAL backends.
// Mirrors `SlotInner` + the `fresh`/`seen`/`compact_fifo`/`recompute_oldest`
// logic in `ackstore/wal.rs` and `ackstore/memory.rs`.

/** Cap that means "use the default" (matches Rust's `max_cap == 0`). */
export const DEFAULT_MAX_CAP = 1_000_000

export class LiveSet {
  /** seq -> recorded-at (unix ms). The authoritative delivered set. */
  readonly set = new Map<bigint, number>()
  /** Insertion order, for FIFO eviction once `cap` is exceeded. */
  private fifo: bigint[] = []
  /** Smallest live seq, 0n when empty. Bounds gate — see `fresh`. */
  minSeq = 0n
  /** Largest seq ever recorded, 0n when nothing was. Monotonic. */
  maxSeq = 0n
  /** Timestamp of the oldest live entry, 0 when empty. */
  oldestTs = 0
  /** Set by `deleteSlot`: the slot is gone, further records are ignored. */
  tombed = false

  constructor(private readonly cap: number = DEFAULT_MAX_CAP) {}

  get size(): number {
    return this.set.size
  }

  /**
   * Bounds gate: `true` = `seq` is outside the recorded `[min,max]` range and
   * therefore definitely not seen. No map lookup, which is the point — this
   * is what every first-delivery pays.
   */
  fresh(seq: bigint): boolean {
    if (this.maxSeq === 0n || seq > this.maxSeq) return true
    return seq < this.minSeq
  }

  seen(seq: bigint): boolean {
    if (this.fresh(seq)) return false
    return this.set.has(seq)
  }

  /** Insert `seq`. Returns `false` if it was already recorded. */
  insert(seq: bigint, nowMs: number): boolean {
    if (this.set.has(seq)) return false
    this.set.set(seq, nowMs)
    this.fifo.push(seq)
    if (this.oldestTs === 0) this.oldestTs = nowMs
    while (this.fifo.length > this.cap) {
      const old = this.fifo.shift()!
      this.set.delete(old)
    }
    if (seq > this.maxSeq) this.maxSeq = seq
    // `minSeq` must be the TRUE minimum live seq, not the FIFO front: async
    // handlers ack out of order (seq 101 before 100), so `fifo[0]` can exceed
    // the smallest live seq. A `minSeq` above the true minimum makes `fresh()`
    // short-circuit `seq < minSeq` for a seq that IS recorded — a silent dedup
    // miss. Only ever lower it here; removals recompute via `compactFifo`.
    if (this.minSeq === 0n || seq < this.minSeq) this.minSeq = seq
    return true
  }

  /** Remove one seq. Returns `true` if it was live. */
  remove(seq: bigint): boolean {
    const had = this.set.delete(seq)
    if (had && this.set.size === 0) this.oldestTs = 0
    return had
  }

  /** Drop every live seq `<= cursor`. Returns the count dropped. */
  removeUpTo(cursor: bigint): number {
    const before = this.set.size
    for (const seq of this.set.keys()) if (seq <= cursor) this.set.delete(seq)
    const removed = before - this.set.size
    if (removed > 0) {
      this.recomputeOldest()
      this.compactFifo()
    }
    return removed
  }

  /** Wipe the slot (tombstone / delete). */
  clear(): void {
    this.set.clear()
    this.fifo = []
    this.minSeq = 0n
    this.maxSeq = 0n
    this.oldestTs = 0
  }

  /** Restore replayed state: `seqs` MUST be ascending. */
  load(live: Map<bigint, number>, seqs: readonly bigint[]): void {
    for (const seq of seqs) this.set.set(seq, live.get(seq)!)
    this.fifo = [...seqs]
    this.minSeq = seqs.length ? seqs[0]! : 0n
    this.maxSeq = seqs.length ? seqs[seqs.length - 1]! : 0n
    this.recomputeOldest()
  }

  /** Smallest / largest CURRENTLY-live seq (for `SlotInfo`, not the gate). */
  liveBounds(): [bigint, bigint] {
    let min = 0n
    let max = 0n
    for (const seq of this.set.keys()) {
      if (min === 0n || seq < min) min = seq
      if (seq > max) max = seq
    }
    return [min, max]
  }

  /** Seqs whose timestamp is strictly older than `cutoffMs`. */
  expiredBefore(cutoffMs: number): bigint[] {
    const out: bigint[] = []
    for (const [seq, ts] of this.set) if (ts < cutoffMs) out.push(seq)
    return out
  }

  recomputeOldest(): void {
    let oldest = 0
    for (const ts of this.set.values()) if (oldest === 0 || ts < oldest) oldest = ts
    this.oldestTs = oldest
  }

  /** Drop dead entries from the FIFO and recompute the true `minSeq`.
   * `maxSeq` stays monotonic on purpose — see `fresh`. */
  compactFifo(): void {
    if (this.fifo.length === 0) return
    const kept: bigint[] = []
    let min = 0n
    for (const seq of this.fifo) {
      if (!this.set.has(seq)) continue
      kept.push(seq)
      if (min === 0n || seq < min) min = seq
    }
    this.fifo = kept
    this.minSeq = min
  }
}
