// Shared registry of active cron handlers. Keyed by cron name.

import type { CreateCronBody } from './cron-frame'

export type CronHandler = (ctx: CronContext) => Promise<void>

/** Context passed to the cron handler on each fire. */
export interface CronContext {
  /** Cron job name. */
  readonly name: string
  /** UTC timestamp (ms since epoch) when the broker intended this fire. */
  readonly fireTime: bigint
  /** Monotonic fire counter (1-based). */
  readonly fireCount: bigint
}

interface CronEntry {
  handler: CronHandler
  config: CreateCronBody
}

export class CronState {
  private readonly handlers = new Map<string, CronEntry>()

  register(name: string, config: CreateCronBody, handler: CronHandler): void {
    this.handlers.set(name, { handler, config })
  }

  remove(name: string): void {
    this.handlers.delete(name)
  }

  getHandler(name: string): CronHandler | undefined {
    return this.handlers.get(name)?.handler
  }

  allConfigs(): ReadonlyArray<{ name: string; config: CreateCronBody }> {
    const out: Array<{ name: string; config: CreateCronBody }> = []
    for (const [name, entry] of this.handlers) {
      out.push({ name, config: entry.config })
    }
    return out
  }
}
