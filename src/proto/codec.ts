import { crc32c } from './crc32c'
import {
  MAGIC, VERSION, HEADER_SIZE,
  OFF_MAGIC, OFF_VERSION, OFF_FLAGS, OFF_ACTION,
  OFF_CRC32C, OFF_LENGTH, OFF_SEQUENCE, OFF_TIMESTAMP,
  OFF_SUBJ_LEN, OFF_SUBJ,
  Action, Flags,
} from './constants'

export interface PackOptions {
  action:    Action
  flags?:    Flags
  seq:       bigint
  timestamp?: bigint
  subject:   Buffer | string
  data:      Buffer
}

// Returns true if the payload for this action starts with a u16 subject-length prefix.
// Must stay in sync with Rust's Action::requires_subject().
export function requiresSubject(action: Action): boolean {
  return (
    action === Action.PubPublish ||
    action === Action.PubSubscribe ||
    action === Action.PubUnsubscribe ||
    action === Action.PubCreateStream ||
    action === Action.PubDeleteStream ||
    action === Action.PubPull ||
    action === Action.PubCreateConsumer ||
    action === Action.PubDeleteConsumer ||
    action === Action.SysStats ||
    action === Action.MgmtGetStream ||
    action === Action.MgmtGetConsumer
  )
}

// Pack a frame into a Buffer.
// Subject actions:     header(32) + subj_len(2) + subject + data
// Non-subject actions: header(32) + data
export function pack(opts: PackOptions): Buffer {
  const subj       = typeof opts.subject === 'string' ? Buffer.from(opts.subject) : opts.subject
  const subjLen    = subj.length
  const hasSubject = requiresSubject(opts.action)
  const payload    = (hasSubject ? 2 + subjLen : 0) + opts.data.length
  const frame       = Buffer.allocUnsafe(HEADER_SIZE + payload)

  frame.writeUInt32LE(MAGIC,            OFF_MAGIC)
  frame[OFF_VERSION] = VERSION
  frame[OFF_FLAGS]   = opts.flags ?? Flags.None
  frame.writeUInt16LE(opts.action,      OFF_ACTION)
  frame.writeUInt32LE(0,                OFF_CRC32C)   // zeroed for CRC computation
  frame.writeUInt32LE(payload,          OFF_LENGTH)
  frame.writeBigUInt64LE(opts.seq,      OFF_SEQUENCE)
  frame.writeBigUInt64LE(opts.timestamp ?? 0n, OFF_TIMESTAMP)

  if (hasSubject) {
    frame.writeUInt16LE(subjLen,        OFF_SUBJ_LEN)
    subj.copy(frame, OFF_SUBJ)
    opts.data.copy(frame, OFF_SUBJ + subjLen)
  } else {
    opts.data.copy(frame, HEADER_SIZE)
  }

  frame.writeUInt32LE(crc32c(frame), OFF_CRC32C)
  return frame
}

// FrameView — zero-copy lazy accessors over a raw frame Buffer.
// Handles both subject frames (Pub* actions) and non-subject frames (Rep*, Sys*).
export class FrameView {
  private _subjLen: number | undefined

  constructor(readonly buf: Buffer) {}

  action():    number { return this.buf.readUInt16LE(OFF_ACTION) }
  flags():     number { return this.buf[OFF_FLAGS]! }
  seq():       bigint { return this.buf.readBigUInt64LE(OFF_SEQUENCE) }
  timestamp(): bigint { return this.buf.readBigUInt64LE(OFF_TIMESTAMP) }
  length():    number { return this.buf.readUInt32LE(OFF_LENGTH) }

  // True if this frame's payload has a u16 subject-length prefix.
  hasSubject(): boolean {
    return requiresSubject(this.action() as Action)
  }

  private subjLen(): number {
    return this._subjLen ??= this.buf.readUInt16LE(OFF_SUBJ_LEN)
  }

  // Zero-copy view of the subject bytes.
  // Returns an empty slice for non-subject frames (Rep*, Sys*).
  subject(): Buffer {
    if (!this.hasSubject()) return this.buf.subarray(HEADER_SIZE, HEADER_SIZE)
    return this.buf.subarray(OFF_SUBJ, OFF_SUBJ + this.subjLen())
  }

  // Zero-copy view of the data bytes.
  // For non-subject frames, data starts immediately after the header.
  data(): Buffer {
    if (!this.hasSubject()) return this.buf.subarray(HEADER_SIZE)
    return this.buf.subarray(OFF_SUBJ + this.subjLen())
  }

  isValid(): boolean {
    return this.buf.readUInt32LE(OFF_MAGIC) === MAGIC && this.buf[OFF_VERSION] === VERSION
  }

  totalSize(): number {
    return HEADER_SIZE + this.length()
  }
}
