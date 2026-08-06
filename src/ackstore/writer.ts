// Buffered append-only writer + the counters shared by the WAL and its slots.
// The Node equivalent of Rust's `WalCore` { BufWriter<File>, size, counters }.
//
// Writes land in a 64 KiB user-space buffer and reach the OS on `flush()`;
// `sync(true)` additionally fsyncs. This is the durability lever the caller
// controls (see `WalOptions.fsync`) — buffered is the default because the
// measured gap is ~70x.

import * as fs from 'node:fs'
import * as path from 'node:path'

import { LOG_FILE, WRITE_BUF_SIZE, type WalConfig } from './config'
import {
  Op, SEQ_FRAME_SIZE, TOMB_FRAME_SIZE, encodeSeqOp, encodeTombstone,
} from './format'
import { AckStoreError } from './store'

export interface WalCounters {
  recorded: number
  confirmed: number
  expired: number
  registered: number
  tombstoned: number
  syncErrors: number
  lastSyncMs: number
  lastSnapshotMs: number
  recsSinceSnap: number
}

export class WalWriter {
  readonly logPath: string
  readonly counters: WalCounters = {
    recorded: 0, confirmed: 0, expired: 0, registered: 0, tombstoned: 0,
    syncErrors: 0, lastSyncMs: 0, lastSnapshotMs: 0, recsSinceSnap: 0,
  }

  private fd = -1
  private readonly pending = Buffer.allocUnsafe(WRITE_BUF_SIZE)
  private pendingLen = 0
  /** Bytes on disk + bytes still buffered. */
  size = 0
  closed = false

  constructor(readonly cfg: WalConfig) {
    this.logPath = path.join(cfg.dir, LOG_FILE)
  }

  /** Open the log for appends. `startSize` is the post-replay file length. */
  openAt(startSize: number): void {
    this.fd = fs.openSync(this.logPath, 'a')
    this.size = startSize
  }

  private guard(): void {
    if (this.closed) throw new AckStoreError('closed', 'closed')
  }

  /** Append a Record/Confirm/Expire/ConfirmUpTo frame — the hot path. */
  appendSeqOp(op: Op, slotId: number, tsMs: number, seq: bigint): void {
    this.guard()
    if (this.pendingLen + SEQ_FRAME_SIZE > this.pending.length) this.flush()
    this.pendingLen += encodeSeqOp(this.pending, this.pendingLen, op, slotId, tsMs, seq)
    this.size += SEQ_FRAME_SIZE
  }

  appendTombstone(slotId: number, tsMs: number): void {
    this.guard()
    if (this.pendingLen + TOMB_FRAME_SIZE > this.pending.length) this.flush()
    this.pendingLen += encodeTombstone(this.pending, this.pendingLen, slotId, tsMs)
    this.size += TOMB_FRAME_SIZE
  }

  /** Append a pre-encoded frame (Register / Snapshot — cold paths). */
  appendFrame(frame: Buffer): void {
    this.guard()
    if (this.pendingLen + frame.length > this.pending.length) this.flush()
    this.size += frame.length
    if (frame.length > this.pending.length) {
      fs.writeSync(this.fd, frame, 0, frame.length) // bigger than the buffer
      return
    }
    frame.copy(this.pending, this.pendingLen)
    this.pendingLen += frame.length
  }

  /** Push the user-space buffer to the OS. Does NOT fsync. */
  flush(): void {
    if (this.pendingLen === 0) return
    fs.writeSync(this.fd, this.pending, 0, this.pendingLen)
    this.pendingLen = 0
  }

  /** Flush and, when `fsync` is configured, force the data to stable storage. */
  sync(): void {
    this.guard()
    try {
      this.flush()
      if (this.cfg.fsync) fs.fsyncSync(this.fd)
    } catch (err) {
      this.counters.syncErrors++
      throw new AckStoreError('sync failed', 'io', { cause: err })
    }
    this.counters.lastSyncMs = this.cfg.now()
  }

  /** Swap the underlying file (compaction): close the fd, run `swap`, reopen.
   * Closing around the rename is required on Windows, where renaming over an
   * open file fails. */
  replaceFile(swap: () => void, newSize: number): void {
    this.guard()
    this.flush()
    fs.closeSync(this.fd)
    swap()
    this.fd = fs.openSync(this.logPath, 'a')
    this.size = newSize
    this.counters.recsSinceSnap = 0
  }

  close(): void {
    if (this.closed) return
    this.flush()
    if (this.cfg.fsync) fs.fsyncSync(this.fd)
    fs.closeSync(this.fd)
    this.fd = -1
    this.closed = true
  }
}
