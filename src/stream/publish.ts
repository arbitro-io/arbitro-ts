import { packPublish, packPublishBatch, packPublishWithReply } from '../proto/v2'
import { EntryFlag, Flag } from '../proto/constants'
import type { Connection } from '../net/connection'
import { BatchPublishEntry } from '../proto/publish'
import { encodeExtendedPayload, extractMsgId, type HeaderMap } from '../proto/headers'

const EMPTY = Buffer.alloc(0)

// Mirrors PUBLISH_BATCH_MAX in arbitro-client-tokio/src/publish/mod.rs —
// the broker's wire limit for a single PublishBatch frame's entry count.
const PUBLISH_BATCH_MAX = 256

/**
 * Optional per-publish options.
 *
 * `msgId` is an opaque byte string the broker uses for dedup when the
 * target stream was created with `idempotencyWindowMs > 0`. Empty or
 * undefined means "no dedup for this publish" — safe to omit on
 * non-idempotent streams.
 *
 * `headers` attaches arbitrary key-value metadata. Headers are persisted
 * alongside the payload and stripped on delivery — consumers always
 * receive only the user payload.
 */
export interface PublishOpts {
  msgId?: Buffer | string
  headers?: HeaderMap
}

function toMsgIdBuf(id: PublishOpts['msgId']): Buffer {
  if (id == null) return EMPTY
  return typeof id === 'string' ? Buffer.from(id) : id
}

function buildPayloadAndFlags(
  data: Buffer, opts?: PublishOpts,
): { payload: Buffer; entryFlags: number; msgId: Buffer } {
  if (!opts?.headers || Object.keys(opts.headers).length === 0) {
    return { payload: data, entryFlags: 0, msgId: toMsgIdBuf(opts?.msgId) }
  }
  const headers = { ...opts.headers }
  let msgId = extractMsgId(headers)
  if (!msgId && opts.msgId) {
    const id = toMsgIdBuf(opts.msgId)
    if (id.length > 0) {
      headers['msg-id'] = id
      msgId = id
    }
  }
  return {
    payload: encodeExtendedPayload(data, headers),
    entryFlags: EntryFlag.HasHeaders,
    msgId: msgId ?? EMPTY,
  }
}

/** Fire-and-forget publish. Caller provides pre-resolved stream_id. */
export function streamPublish(
  conn: Connection, sid: number, subject: string, data: Buffer,
  opts?: PublishOpts,
): void {
  const subj = Buffer.from(subject)
  const { payload, entryFlags, msgId } = buildPayloadAndFlags(data, opts)
  conn.send(packPublish(conn.nextSeq(), sid, subj, payload, 0, entryFlags, msgId))
}

/** Publish + wait for server RepOk confirmation. */
export async function streamPublishAck(
  conn: Connection, sid: number, subject: string, data: Buffer,
  opts?: PublishOpts,
): Promise<void> {
  const subj = Buffer.from(subject)
  const { payload, entryFlags, msgId } = buildPayloadAndFlags(data, opts)
  await conn.sendExpectReply(
    packPublish(conn.nextSeq(), sid, subj, payload, Flag.AckReq, entryFlags, msgId),
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
 *  caller's contract decision, not the API's.
 *
 *  Entries beyond `PUBLISH_BATCH_MAX` (256) are split into multiple
 *  `PublishBatch` frames — mirrors `publish_batch_async`'s
 *  `entries.chunks(PUBLISH_BATCH_MAX)` in `publish/mod.rs`. Only the
 *  first chunk's `RepOk` is awaited (its `first_seq` is what the caller
 *  needs — the remaining N-1 messages occupy the following contiguous
 *  sequences); the rest are sent fire-and-forget so a single oversized
 *  batch still costs one logical round-trip from the caller's view. */
export async function streamPublishBatch(
  conn: Connection, sid: number, messages: BatchPublishEntry[],
): Promise<bigint> {
  if (messages.length <= PUBLISH_BATCH_MAX) {
    // `sendExpectReply` decodes the RepOk body (u64 LE) which the broker
    // fills with `first_seq` for PublishBatch.
    return await conn.sendExpectReply(
      packPublishBatch(conn.nextSeq(), sid, messages),
    )
  }

  const firstChunk = messages.slice(0, PUBLISH_BATCH_MAX)
  const firstSeq = await conn.sendExpectReply(
    packPublishBatch(conn.nextSeq(), sid, firstChunk),
  )
  for (let off = PUBLISH_BATCH_MAX; off < messages.length; off += PUBLISH_BATCH_MAX) {
    const chunk = messages.slice(off, off + PUBLISH_BATCH_MAX)
    conn.send(packPublishBatch(conn.nextSeq(), sid, chunk))
  }
  return firstSeq
}

/**
 * Publish with a reply-to subject, awaiting only the broker's `RepOk`
 * (no reply payload — actual request/reply correlation is handled by
 * `RequestReplyManager` in `client/request.ts`, used by
 * `ArbitroClient.request()`). `msgId` opts this publish into the target
 * stream's dedup window, same as `streamPublishAck`. Mirrors the Rust
 * client's `publish_with_reply` / `publish_with_reply_msg_id`.
 */
export async function streamPublishWithReply(
  conn: Connection, sid: number, subject: string, replyTo: string, data: Buffer,
  opts?: PublishOpts,
): Promise<void> {
  const subj = Buffer.from(subject)
  const reply = Buffer.from(replyTo)
  const { payload, entryFlags, msgId } = buildPayloadAndFlags(data, opts)
  await conn.sendExpectReply(
    packPublishWithReply(conn.nextSeq(), sid, subj, reply, payload, Flag.AckReq, entryFlags, msgId),
  )
}
