import { HEADER_SIZE, OFF_ACTION, OFF_MSG_LEN, Action } from './constants'
import { ArbitroError } from '../types/error'

type FrameCallback = (frame: Buffer) => void

// Envelope header (RepBatch/FanoutBatch): msg_len lives at offset 8, not 4.
const ENVELOPE_MSG_LEN_OFF = 8

// Upper bound on a single frame. A corrupt/hostile `msg_len` (up to 4 GiB in
// the u32 field) would otherwise drive `ensureCapacity` to attempt a multi-GB
// allocation (OOM/crash) or stall the connection buffering forever. Mirrors the
// broker's MAX_FRAME_SIZE ceiling; exceeding it is an unrecoverable protocol
// desync — surfaced so the caller tears the socket down and reconnects.
const MAX_FRAME_SIZE = 64 * 1024 * 1024

// Accumulates incoming TCP bytes and emits complete V2 frames.
// Server uses two header layouts:
//   Standard: action(2)+flags(1)+eflags(1)+msg_len(4)+seq(8)
//   Envelope: action(2)+flags(1)+eflags(1)+stream_id(4)+msg_len(4)+env_seq(4)
export class Framer {
  private buf = Buffer.allocUnsafe(65_536)
  private pos = 0

  push(chunk: Buffer, onFrame: FrameCallback): void {
    this.ensureCapacity(chunk.length)
    chunk.copy(this.buf, this.pos)
    this.pos += chunk.length

    let offset = 0
    while (offset < this.pos) {
      if (this.pos - offset < HEADER_SIZE) break
      const action = this.buf.readUInt16LE(offset + OFF_ACTION)
      const msgLen = action === Action.RepBatch || action === Action.FanoutBatch
        ? this.buf.readUInt32LE(offset + ENVELOPE_MSG_LEN_OFF)
        : this.buf.readUInt32LE(offset + OFF_MSG_LEN)
      const frameLen = HEADER_SIZE + msgLen
      if (frameLen > MAX_FRAME_SIZE) {
        throw new ArbitroError(
          `frame too large: ${frameLen} bytes (max ${MAX_FRAME_SIZE}) — protocol desync`,
          'protocol',
        )
      }
      if (this.pos - offset < frameLen) break
      onFrame(Buffer.from(this.buf.subarray(offset, offset + frameLen)))
      offset += frameLen
    }

    if (offset > 0 && offset < this.pos) {
      this.buf.copyWithin(0, offset, this.pos)
    }
    this.pos = offset < this.pos ? this.pos - offset : 0
  }

  private ensureCapacity(needed: number): void {
    if (this.pos + needed <= this.buf.length) return
    const next = Buffer.allocUnsafe(Math.max(this.buf.length * 2, this.pos + needed))
    this.buf.copy(next, 0, 0, this.pos)
    this.buf = next
  }
}
