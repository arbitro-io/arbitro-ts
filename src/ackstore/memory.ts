// In-memory-only `Store` — no persistence, no recovery. The correct choice
// when restart-durability isn't required (the broker redelivers unacked
// messages on reconnect anyway) and the baseline the WAL is measured against.
// Same `(stream, consumer, seq)` identity + bounds-gate hot path.
//
// Mirrors `arbitro-client-tokio/src/ackstore/memory.rs`.

import { DEFAULT_MAX_CAP, LiveSet } from './live-set'
import { AckStoreError, slotKey } from './store'
import type { SlotInfo, SlotRef, Store, StoreMetrics } from './store'
import { MAX_NAME_LEN } from './format'

interface Counters {
  recorded: number
  confirmed: number
  registered: number
  tombstoned: number
}

class MemSlot implements SlotRef {
  readonly live: LiveSet
  readonly registeredAtMs = Date.now()

  constructor(
    readonly slotId: number,
    readonly stream: string,
    readonly consumer: string,
    cap: number,
    private readonly counters: Counters,
  ) {
    this.live = new LiveSet(cap)
  }

  fresh(seq: bigint): boolean { return this.live.fresh(seq) }
  seen(seq: bigint): boolean { return this.live.seen(seq) }
  record(seq: bigint): void { this.checkRecord(seq) }

  checkRecord(seq: bigint): boolean {
    if (this.live.tombed) return true
    if (!this.live.insert(seq, Date.now())) return false
    this.counters.recorded++
    return true
  }

  confirm(seq: bigint): void {
    if (this.live.remove(seq)) this.counters.confirmed++
  }

  confirmUpTo(cursor: bigint): number {
    const removed = this.live.removeUpTo(cursor)
    this.counters.confirmed += removed
    return removed
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

export class MemoryStore implements Store {
  private readonly byName = new Map<string, MemSlot>()
  private nextId = 0
  private readonly cap: number
  private readonly counters: Counters = {
    recorded: 0, confirmed: 0, registered: 0, tombstoned: 0,
  }

  /** `cap` bounds each slot's live set (FIFO eviction); `0` uses 1_000_000. */
  constructor(cap = 0) {
    this.cap = cap === 0 ? DEFAULT_MAX_CAP : cap
  }

  slot(stream: string, consumer: string): SlotRef {
    if (Buffer.byteLength(stream) > MAX_NAME_LEN || Buffer.byteLength(consumer) > MAX_NAME_LEN) {
      throw new AckStoreError('stream/consumer name too long', 'name-too-long')
    }
    const key = slotKey(stream, consumer)
    const found = this.byName.get(key)
    if (found) return found
    const s = new MemSlot(this.nextId++, stream, consumer, this.cap, this.counters)
    this.byName.set(key, s)
    this.counters.registered++
    return s
  }

  sync(): void { /* nothing buffered */ }
  restore(): void { /* nothing persisted */ }
  close(): void { /* nothing to release */ }

  listSlots(): SlotInfo[] {
    return [...this.byName.values()].map((s) => s.info()).sort((a, b) => a.slotId - b.slotId)
  }

  slotInfo(stream: string, consumer: string): SlotInfo | undefined {
    return this.byName.get(slotKey(stream, consumer))?.info()
  }

  deleteSlot(stream: string, consumer: string): void {
    const key = slotKey(stream, consumer)
    const s = this.byName.get(key)
    if (!s) throw new AckStoreError('unknown slot', 'unknown-slot')
    this.byName.delete(key)
    s.live.tombed = true
    s.live.clear()
    this.counters.tombstoned++
  }

  metrics(): StoreMetrics {
    let liveEntries = 0
    for (const s of this.byName.values()) liveEntries += s.live.size
    return {
      slots: this.byName.size,
      liveEntries,
      recorded: this.counters.recorded,
      confirmed: this.counters.confirmed,
      expired: 0,
      registered: this.counters.registered,
      tombstoned: this.counters.tombstoned,
      syncErrors: 0,
      fileSize: 0,
      lastSyncMs: 0,
      lastSnapshotMs: 0,
    }
  }
}
