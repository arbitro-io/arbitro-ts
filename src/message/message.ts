import { HEADER_SIZE, OFF_SEQ } from '../proto/constants'
import { packAck, packNack, packBatchNack } from '../proto/v2'

// V2 Deliver frame layout:
//   Header(16B) + DeliverBody(12B) + tail[subject + payload]
//
// DeliverBody offsets (relative to HEADER_SIZE):
//   0:  consumer_id   u32
//   4:  subject_hash  u32
//   8:  subject_len   u16
//   10: _pad          u16

const BODY_OFF       = HEADER_SIZE
const BODY_SIZE      = 12
const TAIL_OFF       = HEADER_SIZE + BODY_SIZE
const OFF_CONSUMER   = BODY_OFF
const OFF_SUBJ_HASH  = BODY_OFF + 4
const OFF_SUBJ_LEN   = BODY_OFF + 8

type SendFn = (frame: Buffer) => void

export class Message {
  private readonly frame: Buffer
  private readonly send:  SendFn
  private readonly seqFn: () => bigint
  private _subjectLen: number | undefined

  constructor(frame: Buffer, send: SendFn, seqFn: () => bigint) {
    this.frame = frame
    this.send  = send
    this.seqFn = seqFn
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

  /** Zero-copy view of the subject bytes. */
  subject(): Buffer {
    return this.frame.subarray(TAIL_OFF, TAIL_OFF + this.subjLen())
  }

  /** Zero-copy view of the payload bytes. */
  data(): Buffer {
    return this.frame.subarray(TAIL_OFF + this.subjLen())
  }

  /** Acknowledge — fire-and-forget to broker. */
  ack(): void {
    this.send(packAck(
      this.seqFn(), this.consumerId(), this.subjectHash(), this.seq(),
    ))
  }

  /** Negative acknowledge — immediate requeue. */
  nack(): void {
    this.send(packNack(
      this.seqFn(), this.consumerId(), this.subjectHash(), this.seq(),
    ))
  }

  /** Negative acknowledge with redelivery delay (ms). */
  nackDelay(ms: number): void {
    // Single nack frame has no delay field — use BatchNack with 1 entry.
    this.send(packBatchNack(
      this.seqFn(), this.consumerId(),
      [{ seq: this.seq(), subjectHash: this.subjectHash(), delayMs: ms }],
    ))
  }
}
