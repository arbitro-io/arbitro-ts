// Persistent, append-only `Store` backed by a write-ahead log. The in-memory
// state (per-slot bounds + live set) is the runtime source of truth; the log
// is read only at open (see `wal-open.ts`). Records are length-framed and
// CRC-guarded (`format.ts`) in the SAME on-disk format as the Rust and Go
// clients — a log written by any of them is readable by the others.
//
// Durability: a record is durable once `sync()` returns with `fsync: true`.
// Between record and sync a crash loses that record and the message is
// reprocessed after restart — the at-least-once guarantee the broker already
// provides. Dedup is a best-effort optimization on top: never wrong (the exact
// set is always consulted), only occasionally missed after an unsynced crash.

import { compactLog } from './compact'
import { resolveWalConfig, type WalConfig, type WalOptions } from './config'
import { MAX_NAME_LEN, encodeRegister, encodeSnapshot } from './format'
import { AckStoreError, slotKey } from './store'
import type { SlotInfo, SlotRef, Store, StoreMetrics } from './store'
import { Sweeper } from './sweeper'
import { openLog } from './wal-open'
import { WalSlot } from './wal-slot'
import { WalWriter } from './writer'

const ascending = (a: bigint, b: bigint): number => (a < b ? -1 : a > b ? 1 : 0)

export class Wal implements Store {
  private readonly writer: WalWriter
  private readonly byName: Map<string, WalSlot>
  private readonly byId: Map<number, WalSlot>
  private nextId: number
  private readonly sweeper: Sweeper

  private constructor(readonly cfg: WalConfig) {
    this.writer = new WalWriter(cfg)
    const opened = openLog(cfg, this.writer)
    this.byName = opened.byName
    this.byId = opened.byId
    this.nextId = opened.nextId
    this.sweeper = new Sweeper(cfg, () => this.byName.values(), () => this.writer.closed)
  }

  /** Open (creating if needed) a WAL at `opts.dir` and replay it. */
  static open(opts: WalOptions): Wal {
    const wal = new Wal(resolveWalConfig(opts))
    wal.sweeper.start()
    return wal
  }

  // ── Store ───────────────────────────────────────────────────────────────

  slot(stream: string, consumer: string): SlotRef {
    if (Buffer.byteLength(stream) > MAX_NAME_LEN || Buffer.byteLength(consumer) > MAX_NAME_LEN) {
      throw new AckStoreError('stream/consumer name too long', 'name-too-long')
    }
    const key = slotKey(stream, consumer)
    const found = this.byName.get(key)
    if (found) return found

    const id = this.nextId++
    const slot = new WalSlot(id, stream, consumer, this.writer)
    this.byName.set(key, slot)
    this.byId.set(id, slot)
    this.writer.appendFrame(encodeRegister(id, this.cfg.now(), stream, consumer))
    this.writer.counters.registered++
    return slot
  }

  sync(): void {
    this.writer.sync()
    // Auto-compaction supersedes auto-snapshot when both would fire.
    const { compactAtBytes, snapshotEveryN } = this.cfg
    if (compactAtBytes > 0 && this.writer.size >= compactAtBytes) {
      this.compact()
      return
    }
    if (snapshotEveryN > 0 && this.writer.counters.recsSinceSnap >= snapshotEveryN) {
      this.snapshot()
    }
  }

  restore(): void { /* replay happened at open */ }

  close(): void {
    this.sweeper.stop()
    this.writer.close()
  }

  listSlots(): SlotInfo[] {
    return [...this.byName.values()].map((s) => s.info()).sort((a, b) => a.slotId - b.slotId)
  }

  slotInfo(stream: string, consumer: string): SlotInfo | undefined {
    return this.byName.get(slotKey(stream, consumer))?.info()
  }

  deleteSlot(stream: string, consumer: string): void {
    const key = slotKey(stream, consumer)
    const slot = this.byName.get(key)
    if (!slot) throw new AckStoreError('unknown slot', 'unknown-slot')
    this.byName.delete(key)
    this.byId.delete(slot.slotId)
    slot.live.tombed = true
    slot.live.clear()
    this.writer.appendTombstone(slot.slotId, this.cfg.now())
    this.writer.counters.tombstoned++
  }

  metrics(): StoreMetrics {
    let liveEntries = 0
    for (const s of this.byName.values()) liveEntries += s.live.size
    const c = this.writer.counters
    return {
      slots: this.byName.size,
      liveEntries,
      recorded: c.recorded,
      confirmed: c.confirmed,
      expired: c.expired,
      registered: c.registered,
      tombstoned: c.tombstoned,
      syncErrors: c.syncErrors,
      fileSize: this.writer.size,
      lastSyncMs: c.lastSyncMs,
      lastSnapshotMs: c.lastSnapshotMs,
    }
  }

  // ── snapshot / compaction ───────────────────────────────────────────────

  /** Write one Snapshot frame per non-empty slot. Speeds replay up and is
   * auto-triggered by `sync()` after `snapshotEveryN` records. */
  snapshot(): void {
    const now = this.cfg.now()
    for (const slot of this.byName.values()) {
      if (slot.live.size === 0) continue
      const seqs = [...slot.live.set.keys()].sort(ascending)
      this.writer.appendFrame(encodeSnapshot(slot.slotId, now, seqs))
    }
    this.writer.counters.recsSinceSnap = 0
    this.writer.counters.lastSnapshotMs = now
  }

  /** Rewrite the log so it contains only live state. */
  compact(): void {
    compactLog(this.writer, this.cfg)
  }

  /** Run one TTL sweep now instead of waiting for the timer (tests/admin). */
  sweepExpired(): void {
    this.sweeper.sweep()
  }
}
