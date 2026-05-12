import { packPublish, packPublishBatch, packPublishWithReply } from '../proto/v2'
import { Flag } from '../proto/constants'
import type { Connection } from '../net/connection'
import { BatchPublishEntry } from '../proto/publish'

const EMPTY = Buffer.alloc(0)

/**
 * Optional per-publish options.
 *
 * `msgId` is an opaque byte string the broker uses for dedup when the
 * target stream was created with `idempotencyWindowMs > 0`. Empty or
 * undefined means "no dedup for this publish" — safe to omit on
 * non-idempotent streams.
 */
export interface PublishOpts {
  msgId?: Buffer | string
}

function toMsgIdBuf(id: PublishOpts['msgId']): Buffer {
  if (id == null) return EMPTY
  return typeof id === 'string' ? Buffer.from(id) : id
}

/** Fire-and-forget publish. Caller provides pre-resolved stream_id. */
export function streamPublish(
  conn: Connection, sid: number, subject: string, data: Buffer,
  opts?: PublishOpts,
): void {
  const subj = Buffer.from(subject)
  conn.send(packPublish(conn.nextSeq(), sid, subj, data, 0, 0, toMsgIdBuf(opts?.msgId)))
}

/** Publish + wait for server RepOk confirmation. */
export async function streamPublishAck(
  conn: Connection, sid: number, subject: string, data: Buffer,
  opts?: PublishOpts,
): Promise<void> {
  const subj = Buffer.from(subject)
  await conn.sendExpectReply(
    packPublish(conn.nextSeq(), sid, subj, data, Flag.AckReq, 0, toMsgIdBuf(opts?.msgId)),
  )
}

/** Batch publish — single V2 BatchPubFrame, one write syscall, ONE
 *  RTT to the broker.
 *
 *  Returns a `Promise<bigint>` that resolves to `first_seq`: the
 *  sequence of the first message in the batch. The N messages occupy
 *  `[first_seq, first_seq + N - 1]` in the stream.
 *
 *  Like `publishAck`, the call always waits for the broker's RepOk at
 *  the promise level — the caller decides whether to actually wait by
 *  using `await` or letting the promise float:
 *
 *    await client.publishBatch(stream, msgs)   // barrier — broker has all N
 *    client.publishBatch(stream, msgs)         // fire-and-forget at caller
 *    client.publishBatch(stream, msgs)         //   (promise is dropped, rep ok ignored)
 *      .catch(() => {})                        // suppress unhandled rejection
 *
 *  This mirrors the design of `publish` / `publishAck`: the function
 *  always speaks the full request/response to the broker; await is the
 *  caller's contract decision, not the API's. */
export async function streamPublishBatch(
  conn: Connection, sid: number, messages: BatchPublishEntry[],
): Promise<bigint> {
  // `sendExpectReply` decodes the RepOk body (u64 LE) which the broker
  // fills with `first_seq` for PublishBatch.
  return await conn.sendExpectReply(
    packPublishBatch(conn.nextSeq(), sid, messages),
  )
}

/** Request-reply through a stream. Uses V2 PublishWithReply. */
export async function streamRequest(
  conn: Connection, sid: number, subject: string, data: Buffer, timeoutMs: number,
): Promise<Buffer> {
  const subj = Buffer.from(subject)
  const replyTo = Buffer.from(`_INBOX.${conn.nextSeq().toString(36)}`)
  const frame = packPublishWithReply(
    conn.nextSeq(), sid, subj, replyTo, data, Flag.AckReq,
  )
  await conn.sendExpectReply(frame, timeoutMs)
  // reply is ref_seq from RepOk — actual reply routing handled differently in V2.
  // For now, return empty buffer — request-reply semantics need server support.
  return Buffer.alloc(0)
}
