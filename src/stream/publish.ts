import { pack } from '../proto/codec'
import { Action, Flags, HEADER_SIZE } from '../proto/constants'
import { streamId } from '../proto/fnv1a'
import type { Connection } from '../net/connection'

/** Fire-and-forget publish to a known stream. Uses PubPublishStream. */
export function streamPublish(
  conn: Connection, streamName: string, subject: string, data: Buffer,
): void {
  conn.send(pack({
    action:  Action.PubPublishStream,
    flags:   Flags.NoAck | Flags.ReplyTo,
    seq:     conn.nextSeq(),
    subject: streamName,
    replyTo: subject,
    data,
    crc32cOverride: streamId(streamName),
  }))
}

/** Publish + wait for server confirmation via SysKeepalive fence. */
export async function streamPublishAck(
  conn: Connection, streamName: string, subject: string, data: Buffer,
): Promise<void> {
  conn.send(pack({
    action:  Action.PubPublishStream,
    flags:   Flags.ReplyTo,
    seq:     conn.nextSeq(),
    subject: streamName,
    replyTo: subject,
    data,
    crc32cOverride: streamId(streamName),
  }))
  await conn.sendExpectReply(pack({
    action:  Action.SysKeepalive,
    flags:   Flags.None,
    seq:     conn.nextSeq(),
    subject: '',
    data:    Buffer.alloc(0),
  }))
}

/** Batch fire-and-forget — concatenated PubPublishStream frames, single write. */
export function streamPublishBatch(
  conn: Connection, streamName: string, messages: [subject: string, data: Buffer][],
): void {
  const sid = streamId(streamName)
  const frames = messages.map(([subject, data]) =>
    pack({
      action:  Action.PubPublishStream,
      flags:   Flags.NoAck | Flags.ReplyTo,
      seq:     conn.nextSeq(),
      subject: streamName,
      replyTo: subject,
      data,
      crc32cOverride: sid,
    }),
  )
  conn.send(Buffer.concat(frames))
}

/** Request-reply through a stream. Uses PubPublish (reply_to is used for the reply path). */
export async function streamRequest(
  conn: Connection, _streamName: string, subject: string, data: Buffer, timeoutMs: number,
): Promise<Buffer> {
  const seq = conn.nextSeq()
  const frame = pack({
    action:  Action.PubPublish,
    flags:   Flags.ReplyTo,
    seq,
    subject,
    data,
  })
  const reply = await conn.sendRequest(seq, frame, timeoutMs)
  return reply.subarray(HEADER_SIZE)
}
