import { OFF_FLAGS, OFF_SEQUENCE, OFF_CRC32C, HEADER_SIZE } from '../proto/constants'
import { Flags } from '../proto/constants'

// RepMessage layout:
//   crc32c  (offset 8,  u32) = topic_len
//   length  (offset 12, u32) = payload_len
//   sequence(offset 16, u64) = journal seq
//   timestamp(offset 24, u64) = sub_id
//   after 32-byte header: topic_bytes (topic_len) + payload_bytes (payload_len)

export class Message {
  private _topicLen: number | undefined

  constructor(
    private readonly frame:  Buffer,
    readonly subId:          bigint,
    private readonly _ack:   () => void,
    private readonly _nack:  () => void,
    private readonly _reply?: (data: Buffer) => void,
  ) {}

  private topicLen(): number {
    return this._topicLen ??= this.frame.readUInt32LE(OFF_CRC32C)
  }

  subject(): Buffer {
    return this.frame.subarray(HEADER_SIZE, HEADER_SIZE + this.topicLen())
  }

  data(): Buffer {
    return this.frame.subarray(HEADER_SIZE + this.topicLen())
  }

  seq(): bigint {
    return this.frame.readBigUInt64LE(OFF_SEQUENCE)
  }

  hasReplyTo(): boolean { return (this.frame[OFF_FLAGS]! & Flags.ReplyTo) !== 0 }

  ack():  void { this._ack() }
  nack(): void { this._nack() }

  reply(data: Buffer): void { this._reply?.(data) }
}
