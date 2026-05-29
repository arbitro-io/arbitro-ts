// Fluent builder for cron job registration + handle for lifecycle.

import type { Connection } from '../net/connection'
import type { CronState, CronHandler, CronContext } from './cron-state'
import { packCreateCron, packDeleteCron, type CreateCronBody } from './cron-frame'

// ── CronBuilder ────────────────────────────────────────────────────────────

export class CronBuilder {
  private expr: string | undefined
  private timezone: string | undefined
  private timeoutMs = 30_000
  private allowOverlap = false

  constructor(
    private readonly conn: Connection,
    private readonly cronState: CronState,
    private readonly cronName: string,
  ) {}

  every(expression: string): this {
    this.expr = expression
    return this
  }

  tz(timezone: string): this {
    this.timezone = timezone
    return this
  }

  timeout(ms: number): this {
    this.timeoutMs = ms
    return this
  }

  overlap(allow: boolean): this {
    this.allowOverlap = allow
    return this
  }

  async run(handler: CronHandler): Promise<CronHandle> {
    if (!this.expr) throw new Error('cron expression required — call .every()')

    const body: CreateCronBody = {
      name: this.cronName,
      every: this.expr,
      tz: this.timezone,
      timeout_ms: this.timeoutMs,
      overlap: this.allowOverlap,
    }

    const seq = this.conn.nextSeq()
    await this.conn.sendExpectReply(packCreateCron(seq, body))
    this.cronState.register(this.cronName, body, handler)

    return new CronHandle(this.conn, this.cronState, this.cronName)
  }
}

// ── CronHandle ─────────────────────────────────────────────────────────────

export class CronHandle {
  constructor(
    private readonly conn: Connection,
    private readonly cronState: CronState,
    private readonly cronName: string,
  ) {}

  get name(): string { return this.cronName }

  async stop(): Promise<void> {
    const nameBuf = Buffer.from(this.cronName)
    const seq = this.conn.nextSeq()
    await this.conn.sendExpectReply(packDeleteCron(seq, nameBuf))
    this.cronState.remove(this.cronName)
  }
}
