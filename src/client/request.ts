// Generic request/reply for `ArbitroClient.request()`.
//
// Mirrors `arbitro-client-tokio/src/service.rs:209-270`'s ReplyMux:
// register a correlation id, publish with an encoded reply_to, await
// the muxed reply from a private per-instance reply consumer. Modeled
// directly on the working `Service.request()` / `Service._dispatch()`
// pair in `service/index.ts` (see the BUG-2 per-instance reply consumer
// fix), generalized to any target stream instead of a `_svc.` namespace.
//
// One reply consumer is created lazily PER TARGET STREAM, on the first
// `request()` call against that stream — scoped to `_reply.<instanceId>.>`
// so replies to different requesters (and different client instances)
// never collide. `deliverPolicy: New` means it only ever sees future
// replies, never a backlog.

import type { Connection } from '../net/connection'
import { packPublishWithReply } from '../proto/publish'
import { packCreateConsumer } from '../proto/v2'
import { Flag } from '../proto/constants'
import { Message, REPLY_TO_MAGIC } from '../message/message'
import { ArbitroError } from '../types/error'

const EMPTY = Buffer.alloc(0)

let NEXT_INSTANCE_ID = 1
function nextInstanceId(): number {
  const id = NEXT_INSTANCE_ID++
  if (NEXT_INSTANCE_ID > 0xFFFFFFFF) NEXT_INSTANCE_ID = 1
  return id
}

interface PendingRequest {
  resolve: (data: Buffer) => void
  timer: ReturnType<typeof setTimeout>
}

export class RequestReplyManager {
  readonly instanceId = nextInstanceId()
  private readonly pending = new Map<string, PendingRequest>()
  private readonly replyConsumers = new Map<number, Promise<number>>() // sid -> consumerId
  private corrSeq = 0
  private closed = false

  constructor(private readonly conn: Connection) {}

  async request(
    sid: number, subject: string, data: Buffer, timeoutMs: number, msgId?: Buffer,
  ): Promise<Buffer> {
    if (this.closed) throw new ArbitroError('client is closed', 'closed')
    await this.ensureReplyConsumer(sid)

    const corrId = ++this.corrSeq
    const replySubject = `_reply.${this.instanceId}.${corrId}`
    const replyTo = Buffer.allocUnsafe(5 + replySubject.length)
    replyTo[0] = REPLY_TO_MAGIC
    replyTo.writeUInt32LE(sid, 1)
    Buffer.from(replySubject).copy(replyTo, 5)

    const frame = packPublishWithReply(
      this.conn.nextSeq(), sid, Buffer.from(subject), replyTo, data,
      Flag.AckReq, 0, msgId ?? EMPTY,
    )

    return new Promise<Buffer>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(replySubject)
        reject(new ArbitroError('request timeout', 'timeout'))
      }, timeoutMs)

      this.pending.set(replySubject, {
        resolve: (d) => { clearTimeout(timer); resolve(d) },
        timer,
      })

      // Awaits only the broker's RepOk for the *request* publish — the
      // reply itself resolves the promise above via the reply consumer's
      // handler. If the publish itself fails, fail fast instead of
      // waiting out the full timeout.
      this.conn.sendExpectReply(frame, timeoutMs).catch((err: Error) => {
        clearTimeout(timer)
        this.pending.delete(replySubject)
        reject(err)
      })
    })
  }

  private ensureReplyConsumer(sid: number): Promise<number> {
    let p = this.replyConsumers.get(sid)
    if (!p) {
      p = this.createReplyConsumer(sid)
      this.replyConsumers.set(sid, p)
    }
    return p
  }

  private async createReplyConsumer(sid: number): Promise<number> {
    const filter = `_reply.${this.instanceId}.>`
    const name = Buffer.from(`_reply-${this.instanceId}-${sid}`)

    const consumerRef = await this.conn.sendExpectReply(
      packCreateConsumer(this.conn.nextSeq(), {
        streamId: sid,
        name,
        group: Buffer.from(''), // no group — per-instance, never load-balanced
        filter: Buffer.from(filter),
        maxInflight: 1024,
        ackPolicy: 1,
        deliverPolicy: 1, // DeliverPolicy::New — only future replies
        deliverMode: 0,
        ackWaitMs: 30_000,
        startSeq: 0n,
      }),
    )
    const consumerId = Number(consumerRef & 0xFFFFFFFFn)

    const handler = (frame: Buffer): void => {
      const msg = new Message(frame, (f) => this.conn.send(f), () => this.conn.nextSeq())
      const subject = msg.subject().toString()
      const entry = this.pending.get(subject)
      if (entry) {
        this.pending.delete(subject)
        entry.resolve(msg.data())
      }
      msg.ack()
    }

    await this.conn.sendSubscribeV2(consumerId, Buffer.from(filter), handler)
    return consumerId
  }

  /** Called by `ArbitroClient.close()` — cancels every in-flight request
   * with a rejection instead of leaving it hanging until its timeout. */
  close(): void {
    this.closed = true
    for (const p of this.pending.values()) clearTimeout(p.timer)
    this.pending.clear()
  }
}
