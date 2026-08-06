// TTL sweep — drops dedup entries older than `ttlMs` and appends one Expire
// frame per seq. Mirrors `sweep_loop` in `ackstore/wal.rs`; a timer replaces
// the Rust background thread.
//
// The timer is `unref`'d: a dedup housekeeping tick must never be the reason a
// Node process stays alive.

import type { WalConfig } from './config'
import type { WalSlot } from './wal-slot'

export class Sweeper {
  private timer: ReturnType<typeof setInterval> | undefined

  constructor(
    private readonly cfg: WalConfig,
    private readonly slots: () => Iterable<WalSlot>,
    private readonly stopped: () => boolean,
  ) {}

  start(): void {
    if (this.cfg.ttlMs <= 0) return
    this.timer = setInterval(() => this.sweep(), this.cfg.sweepIntervalMs)
    this.timer.unref?.()
  }

  /** One sweep pass. Exposed so tests can drive it without waiting on a timer. */
  sweep(): void {
    if (this.stopped()) return
    const cutoff = this.cfg.now() - this.cfg.ttlMs
    for (const slot of this.slots()) {
      if (slot.live.size === 0) continue
      slot.expire(slot.live.expiredBefore(cutoff))
    }
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer)
    this.timer = undefined
  }
}
