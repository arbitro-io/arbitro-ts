import { HEADER_SIZE, OFF_SEQ } from '../proto/constants'
import { packAck, packNack, packBatchNack } from '../proto/v2'
import { packPublish } from '../proto/publish'
import { Flag } from '../proto/constants'

// V2 Deliver frame layout:
//   Header(16B) + DeliverBody(12B) + tail[subject + reply_to + payload]
//
// DeliverBody offsets (relative to HEADER_SIZE):
//   0:  consumer_id    u32
//   4:  subject_hash   u32
//   8:  subject_len    u16
//   10: reply_to_len   u16

const BODY_OFF        = HEADER_SIZE
const BODY_SIZE       = 12
const TAIL_OFF        = HEADER_SIZE + BODY_SIZE
const OFF_CONSUMER    = BODY_OFF
const OFF_SUBJ_HASH   = BODY_OFF + 4
const OFF_SUBJ_LEN    = BODY_OFF + 8
const OFF_REPLY_LEN   = BODY_OFF + 10

export const REPLY_TO_MAGIC = 0xFF

/** Returns `true` if the frame was handed off to the socket, `false` if
 * the write failed or the socket wasn't connected/writable. */
export type SendFn = (frame: Buffer) => boolean

export class Message {
  private readonly frame: Buffer
  private readonly send:  SendFn
  private readonly seqFn: () => bigint
  private readonly onAck: (() => void) | undefined
  private readonly onNack: (() => void) | undefined
  private readonly onAckSendFailure: ((consumerId: number, seq: bigint) => void) | undefined
  private _subjectLen: number | undefined
  private _replyToLen: number | undefined

  constructor(
    frame: Buffer, send: SendFn, seqFn: () => bigint,
    onAck?: () => void, onNack?: () => void,
    onAckSendFailure?: (consumerId: number, seq: bigint) => void,
  ) {
    this.frame = frame
    this.send  = send
    this.seqFn = seqFn
    this.onAck = onAck
    this.onNack = onNack
    this.onAckSendFailure = onAckSendFailure
  }

  /** Delivery sequence — used to ack/nack this message. */
  seq(): bigint {
    return this.frame.readBigUInt64LE(OFF_SEQ)
  }

  /** Consumer ID that received this delivery. */
  consumerId(): number {
    return this.frame.readUInt32LE(OFF_CONSUMER)
  }

  /** Subject hash — echoed back in ack for O(1) credit release. */
  subjectHash(): number {
    return this.frame.readUInt32LE(OFF_SUBJ_HASH)
  }

  private subjLen(): number {
    return this._subjectLen ??= this.frame.readUInt16LE(OFF_SUBJ_LEN)
  }

  private replyLen(): number {
    return this._replyToLen ??= this.frame.readUInt16LE(OFF_REPLY_LEN)
  }

  /** Zero-copy view of the subject bytes. */
  subject(): Buffer {
    return this.frame.subarray(TAIL_OFF, TAIL_OFF + this.subjLen())
  }

  /** Zero-copy view of the reply_to bytes (empty if none). */
  replyTo(): Buffer {
    const rLen = this.replyLen()
    if (rLen === 0) return Buffer.alloc(0)
    const start = TAIL_OFF + this.subjLen()
    return this.frame.subarray(start, start + rLen)
  }

  /** Zero-copy view of the payload bytes. */
  data(): Buffer {
    return this.frame.subarray(TAIL_OFF + this.subjLen() + this.replyLen())
  }

  /** Reply to the sender. Decodes reply_to as [0xFF][stream_id LE u32][subject]. */
  reply(payload: Buffer): void {
    const rt = this.replyTo()
    if (rt.length < 5 || rt[0] !== REPLY_TO_MAGIC) return
    const targetStreamId = rt.readUInt32LE(1)
    const replySubject = rt.subarray(5)
    this.send(packPublish(this.seqFn(), targetStreamId, replySubject, payload, Flag.AckReq, 0))
  }

  /** Acknowledge — fire-and-forget to broker. If the write fails (socket
   * down / not connected), the ack is handed to the `AckRelay` hot tier
   * via `onAckSendFailure` instead of being silently lost — it's resent
   * by the connection's sweep loop / reconnect replay once the socket
   * recovers. */
  ack(): void {
    const ok = this.send(packAck(
      this.seqFn(), this.consumerId(), this.subjectHash(), this.seq(),
    ))
    if (ok) {
      this.onAck?.()
    } else {
      this.onAckSendFailure?.(this.consumerId(), this.seq())
    }
  }

  /** Negative acknowledge — immediate requeue. */
  nack(): void {
    this.send(packNack(
      this.seqFn(), this.consumerId(), this.subjectHash(), this.seq(),
    ))
    this.onNack?.()
  }

  /** Negative acknowledge with redelivery delay (ms). */
  nackDelay(ms: number): void {
    // Single nack frame has no delay field — use BatchNack with 1 entry.
    this.send(packBatchNack(
      this.seqFn(), this.consumerId(),
      [{ seq: this.seq(), subjectHash: this.subjectHash(), delayMs: ms }],
    ))
    this.onNack?.()
  }
}
