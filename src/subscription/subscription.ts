import { Message } from '../message/message'
import type { Connection } from '../net/connection'
import type { SlotRef } from '../ackstore'
import { HEADER_SIZE, OFF_SEQ } from '../proto/constants'

type MsgCallback = (msg: Message) => void

const OFF_CONSUMER_ID = HEADER_SIZE
const OFF_SUBJECT_HASH = HEADER_SIZE + 4

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
    private consumerId: number,
    private readonly conn: Connection,
    private readonly streamName: string,
    private readonly fetchTimeoutMs: number,
    /** Redelivery-dedup handle for this `(stream, consumer)`. Undefined when
     * no ackstore is configured — then delivery behaves exactly as before. */
    private readonly slot?: SlotRef,
  ) {}

  /** Consumer ID assigned by the server. */
  id(): number { return this.consumerId }

  // Called by the delivery handler.
  deliver(frame: Buffer): void {
    if (this.closed) return

    // Redelivery dedup: this seq was already recorded, so the handler ran and
    // the ack was lost (or raced a reconnect). Re-ack and drop it — running
    // the handler again is the duplicate execution the ackstore exists to
    // prevent. The gate is a bounds probe first, so a first delivery costs no
    // map lookup at all.
    if (this.slot && this.slot.seen(frame.readBigUInt64LE(OFF_SEQ))) {
      this.conn.enqueueAck(
        frame.readUInt32LE(OFF_CONSUMER_ID),
        frame.readUInt32LE(OFF_SUBJECT_HASH),
        frame.readBigUInt64LE(OFF_SEQ),
      )
      this.conn.bumpRedeliveriesSkipped()
      return
    }

    const msg = new Message(
      frame,
      (f) => this.conn.send(f),
      () => this.conn.nextSeq(),
      () => this.conn.bumpAcksSent(),
      () => this.conn.bumpNacksSent(),
      (consumerId, seq) => this.conn.ackRelay.record(consumerId, seq),
      (consumerId, subjectHash, seq) => this.conn.enqueueAck(consumerId, subjectHash, seq),
      this.slot ? (seq) => this.slot!.record(seq) : undefined,
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
    this.conn.cancelSubscription(this.consumerId)
    for (const p of this.fetchQueue) {
      clearTimeout(p.timer)
      p.resolve(p.buf)
    }
    this.fetchQueue = []
  }
}
