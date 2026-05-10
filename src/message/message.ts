import { OFF_FLAGS, OFF_SEQUENCE, OFF_CRC32C, OFF_LENGTH, HEADER_SIZE } from '../proto/constants'
import { Flags } from '../proto/constants'

// RepMessage layout:
//   crc32c  (offset 8,  u32) = topic_len
//   length  (offset 12, u32) = payload_len
//   flags   (offset 5,  u8)  = preserves REPLY_TO from original publish
//   sequence(offset 16, u64) = journal seq
//   timestamp(offset 24, u64) = sub_id
//   after 32-byte header: topic_bytes (topic_len) + payload_bytes (payload_len)
//
// When REPLY_TO flag is set, payload = [u16_le rto_len][reply_to bytes][user data]
// When REPLY_TO flag is not set, payload = [user data]

export class Message {
  private _topicLen: number | undefined
  private _payloadStart: number | undefined

  constructor(
    private readonly frame:  Buffer,
    readonly subId:          bigint,
    private readonly _ack:   () => void,
    private readonly _nack:  () => void,
    private readonly _nackDelayFn: (ms: number) => void,
    private readonly _reply?: (data: Buffer) => void,
  ) {}

  private topicLen(): number {
    return this._topicLen ??= this.frame.readUInt32LE(OFF_CRC32C)
  }

  /** Start offset of payload bytes (after header + topic) */
  private payloadOff(): number {
    return HEADER_SIZE + this.topicLen()
  }

  /** Start offset of user data within the frame (skips reply_to prefix if present) */
  private dataOff(): number {
    if (this._payloadStart !== undefined) return this._payloadStart
    const off = this.payloadOff()
    if (!this.hasReplyTo() || off + 2 > this.frame.length) {
      this._payloadStart = off
    } else {
      const rtoLen = this.frame.readUInt16LE(off)
      this._payloadStart = off + 2 + rtoLen
    }
    return this._payloadStart
  }

  subject(): Buffer {
    return this.frame.subarray(HEADER_SIZE, HEADER_SIZE + this.topicLen())
  }

  data(): Buffer {
    return this.frame.subarray(this.dataOff())
  }

  seq(): bigint {
    return this.frame.readBigUInt64LE(OFF_SEQUENCE)
  }

  hasReplyTo(): boolean { return (this.frame[OFF_FLAGS]! & Flags.ReplyTo) !== 0 }

  replyTo(): Buffer | undefined {
    if (!this.hasReplyTo()) return undefined
    const off = this.payloadOff()
    if (off + 2 > this.frame.length) return undefined
    const rtoLen = this.frame.readUInt16LE(off)
    if (off + 2 + rtoLen > this.frame.length) return undefined
    return this.frame.subarray(off + 2, off + 2 + rtoLen)
  }

  ack():  void { this._ack() }
  nack(): void { this._nack() }
  nackDelay(delayMs: number): void { this._nackDelayFn(delayMs) }

  reply(data: Buffer): void { this._reply?.(data) }
}
