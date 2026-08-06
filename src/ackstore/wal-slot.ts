// One `(stream, consumer)` handle over the WAL. Mirrors `WalSlot` in
// `ackstore/wal.rs`: memory is the runtime source of truth, and every mutation
// appends its frame while the in-memory state is already updated, so disk
// order matches memory order.

import { LiveSet } from './live-set'
import { Op } from './format'
import type { SlotInfo, SlotRef } from './store'
import type { WalWriter } from './writer'

export class WalSlot implements SlotRef {
  readonly live: LiveSet
  readonly registeredAtMs: number

  constructor(
    readonly slotId: number,
    readonly stream: string,
    readonly consumer: string,
    private readonly writer: WalWriter,
  ) {
    this.live = new LiveSet(writer.cfg.maxCap || undefined)
    this.registeredAtMs = writer.cfg.now()
  }

  fresh(seq: bigint): boolean { return this.live.fresh(seq) }
  seen(seq: bigint): boolean { return this.live.seen(seq) }
  record(seq: bigint): void { this.checkRecord(seq) }

  checkRecord(seq: bigint): boolean {
    if (this.live.tombed) return true
    const now = this.writer.cfg.now()
    if (!this.live.insert(seq, now)) return false
    this.writer.appendSeqOp(Op.Record, this.slotId, now, seq)
    this.writer.counters.recorded++
    this.writer.counters.recsSinceSnap++
    return true
  }

  confirm(seq: bigint): void {
    this.live.remove(seq)
    this.writer.appendSeqOp(Op.Confirm, this.slotId, this.writer.cfg.now(), seq)
    this.writer.counters.confirmed++
  }

  confirmUpTo(cursor: bigint): number {
    const removed = this.live.removeUpTo(cursor)
    // No-op cursor: nothing live is <= cursor. Seqs are monotonic and the
    // broker cursor only advances, so no future record will be <= cursor
    // either — the frame would be dead weight. Skipping matters: this runs on
    // EVERY AckBatchResp, most of which remove nothing, and appending each
    // time would grow the log without bound.
    if (removed === 0) return 0
    this.writer.appendSeqOp(Op.ConfirmUpTo, this.slotId, this.writer.cfg.now(), cursor)
    this.writer.counters.confirmed += removed
    return removed
  }

  /** TTL sweep: drop `seqs` and append one Expire frame each. */
  expire(seqs: readonly bigint[]): void {
    if (seqs.length === 0) return
    for (const seq of seqs) {
      this.live.set.delete(seq)
      this.writer.appendSeqOp(Op.Expire, this.slotId, this.writer.cfg.now(), seq)
    }
    this.writer.counters.expired += seqs.length
    this.live.recomputeOldest()
    this.live.compactFifo()
  }

  info(): SlotInfo {
    const [minSeq, maxSeq] = this.live.liveBounds()
    return {
      slotId: this.slotId,
      stream: this.stream,
      consumer: this.consumer,
      live: this.live.size,
      minSeq,
      maxSeq,
      oldestTsMs: this.live.oldestTs,
      registeredAtMs: this.registeredAtMs,
    }
  }
}
