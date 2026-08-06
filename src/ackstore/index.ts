// Client-side redelivery-dedup store.
//
// Two backends behind one `Store` interface: an in-memory store
// (`MemoryStore`) and a durable, restart-surviving write-ahead log (`Wal`).
// The WAL's on-disk format is byte-identical to the Rust and Go clients
// (`ackstore/format.rs`, `internal/ackstore/format.go`) — same "ARBWAL01"
// magic, same length-framed crc32c records, same op codes — so a log written
// by one client replays in any other.
//
// Design mirrors the Rust/Go reference implementations: key =
// `(streamName, consumerName, seq)`, bounds gate on the delivery hot path,
// server-cursor-driven cleanup, and log compaction.

export { MemoryStore } from './memory'
export { Wal } from './wal'
export { AckStoreError, slotKey } from './store'
export type { SlotInfo, SlotRef, Store, StoreMetrics } from './store'
export type { WalOptions, WalConfig } from './config'
export { LOG_FILE } from './config'

import { MemoryStore } from './memory'
import { Wal } from './wal'
import type { Store } from './store'
import type { WalOptions } from './config'

/** Client-facing knobs for `ClientConfig.ackStore`. */
export interface AckStoreConfig extends Partial<WalOptions> {
  /**
   * Directory for the persistent log. Omit for in-memory-only dedup (no disk
   * I/O, nothing survives a restart).
   */
  dir?: string
  /** Inject a ready-made store. Overrides every other field. */
  store?: Store
}

/** Build the store described by `cfg`. `dir` present => WAL, else memory. */
export function openAckStore(cfg: AckStoreConfig): Store {
  if (cfg.store) return cfg.store
  if (cfg.dir === undefined) return new MemoryStore(cfg.maxCap ?? 0)
  return Wal.open({ ...cfg, dir: cfg.dir })
}
