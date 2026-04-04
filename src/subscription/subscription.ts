import { Message } from '../message/message'
import { OFF_SEQUENCE } from '../proto/constants'
import type { Connection } from '../net/connection'

type MsgCallback = (msg: Message) => void

interface PendingFetch {
  resolve: (msgs: Message[]) => void
  count:   number
  buf:     Message[]
  timer:   ReturnType<typeof setTimeout>
}

export class Subscription {
  private callback:    MsgCallback | undefined
  private fetchQueue:  PendingFetch[] = []
  private msgBuf:      Message[]      = []
  private closed      = false

  constructor(
    private subId: bigint,
    private readonly conn: Connection,
    private readonly streamName: string,
    private readonly fetchTimeoutMs: number,
  ) {}

  // Called by Connection.sendSubscribe after a reconnect assigns a new subId.
  updateSubId(newSubId: bigint): void {
    this.subId = newSubId
  }

  // Called by the delivery handler registered in Connection.sendSubscribe.
  deliver(frame: Buffer): void {
    if (this.closed) return

    const msgSeq = frame.readBigUInt64LE(OFF_SEQUENCE)
    const msg = new Message(
      frame,
      this.subId,
      () => this.conn.sendAck(this.streamName, this.subId, msgSeq),
      () => this.conn.sendNack(this.streamName, this.subId, msgSeq),
      (data) => this.conn.sendReply(msgSeq, data),
    )

    if (this.callback) {
      this.callback(msg)
      return
    }

    const pending = this.fetchQueue[0]
    if (pending) {
      pending.buf.push(msg)
      if (pending.buf.length >= pending.count) {
        clearTimeout(pending.timer)
        this.fetchQueue.shift()
        pending.resolve(pending.buf)
      }
      return
    }

    this.msgBuf.push(msg)
  }

  // Push mode — set a callback to receive messages as they arrive.
  onMessage(cb: MsgCallback): void {
    this.callback = cb
  }

  // Pull mode — fetch up to `count` messages, waiting up to `timeoutMs`.
  fetch(count: number, timeoutMs = this.fetchTimeoutMs): Promise<Message[]> {
    if (this.msgBuf.length >= count) {
      return Promise.resolve(this.msgBuf.splice(0, count))
    }
    return new Promise((resolve) => {
      const buf  = this.msgBuf.splice(0)
      const timer = setTimeout(() => {
        const idx = this.fetchQueue.findIndex((p) => p === pending)
        if (idx >= 0) this.fetchQueue.splice(idx, 1)
        resolve(buf)
      }, timeoutMs)
      const pending: PendingFetch = { resolve, count, buf, timer }
      this.fetchQueue.push(pending)
    })
  }

  close(): void {
    this.closed = true
    this.conn.cancelSubscription(this.subId)
    for (const p of this.fetchQueue) {
      clearTimeout(p.timer)
      p.resolve(p.buf)
    }
    this.fetchQueue = []
  }
}
