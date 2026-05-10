import { packPublish, packPublishBatch, packPublishWithReply } from '../proto/v2'
import { Flag } from '../proto/constants'
import { streamId } from '../proto/fnv1a'
import type { Connection } from '../net/connection'

/** Fire-and-forget publish to a known stream. Uses V2 PubFrame. */
export function streamPublish(
  conn: Connection, streamName: string, subject: string, data: Buffer,
): void {
  const sid  = streamId(streamName)
  const subj = Buffer.from(subject)
  conn.send(packPublish(conn.nextSeq(), sid, subj, data))
}

/** Publish + wait for server RepOk confirmation. */
export async function streamPublishAck(
  conn: Connection, streamName: string, subject: string, data: Buffer,
): Promise<void> {
  const sid  = streamId(streamName)
  const subj = Buffer.from(subject)
  await conn.sendExpectReply(
    packPublish(conn.nextSeq(), sid, subj, data, Flag.AckReq),
  )
}

/** Batch publish — single V2 BatchPubFrame, one write syscall. */
export function streamPublishBatch(
  conn: Connection, streamName: string, messages: [subject: string, data: Buffer][],
): void {
  const sid = streamId(streamName)
  const entries = messages.map(([subject, data]) => ({
    subject: Buffer.from(subject),
    payload: data,
  }))
  conn.send(packPublishBatch(conn.nextSeq(), sid, entries))
}

/** Request-reply through a stream. Uses V2 PublishWithReply. */
export async function streamRequest(
  conn: Connection, streamName: string, subject: string, data: Buffer, timeoutMs: number,
): Promise<Buffer> {
  const sid     = streamId(streamName)
  const subj    = Buffer.from(subject)
  const replyTo = Buffer.from(`_INBOX.${conn.nextSeq().toString(36)}`)
  const frame   = packPublishWithReply(
    conn.nextSeq(), sid, subj, replyTo, data, Flag.AckReq,
  )
  const reply = await conn.sendExpectReply(frame, timeoutMs)
  // reply is ref_seq from RepOk — actual reply routing handled differently in V2.
  // For now, return empty buffer — request-reply semantics need server support.
  return Buffer.alloc(0)
}
