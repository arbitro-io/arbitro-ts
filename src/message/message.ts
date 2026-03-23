import { OFF_FLAGS, OFF_SEQUENCE, OFF_SUBJ_LEN, OFF_SUBJ } from '../proto/constants'
import { Flags } from '../proto/constants'

// Zero-copy lazy view over a raw frame Buffer.
// subject() and data() return subarray views — no allocation.
export class Message {
  private _subjLen: number | undefined

  constructor(
    private readonly frame:  Buffer,
    readonly subId:          bigint,
    private readonly _ack:   () => void,
    private readonly _nack:  () => void,
    private readonly _reply?: (data: Buffer) => void,
  ) {}

  private subjLen(): number {
    return this._subjLen ??= this.frame.readUInt16LE(OFF_SUBJ_LEN)
  }

  // Zero-copy view into the frame buffer.
  subject(): Buffer {
    return this.frame.subarray(OFF_SUBJ, OFF_SUBJ + this.subjLen())
  }

  // Zero-copy view into the frame buffer.
  data(): Buffer {
    return this.frame.subarray(OFF_SUBJ + this.subjLen())
  }

  seq(): bigint {
    return this.frame.readBigUInt64LE(OFF_SEQUENCE)
  }

  /** True if the publisher is waiting for a reply (`FLAG_REPLY_TO`). */
  hasReplyTo(): boolean { return (this.frame[OFF_FLAGS]! & Flags.ReplyTo) !== 0 }

  ack():  void { this._ack() }
  nack(): void { this._nack() }

  /** Send a reply to the original publisher. Only valid when `hasReplyTo()` is true. */
  reply(data: Buffer): void { this._reply?.(data) }
}
