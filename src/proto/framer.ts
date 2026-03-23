import { HEADER_SIZE, OFF_LENGTH } from './constants'

type FrameCallback = (frame: Buffer) => void

// Accumulates incoming TCP bytes and emits complete frames.
// A frame is: HEADER_SIZE bytes of header + header.length bytes of payload.
export class Framer {
  private buf = Buffer.allocUnsafe(65_536)
  private pos = 0

  push(chunk: Buffer, onFrame: FrameCallback): void {
    this.ensureCapacity(chunk.length)
    chunk.copy(this.buf, this.pos)
    this.pos += chunk.length

    let offset = 0
    while (offset < this.pos) {
      // Need at least a full header to read the payload length.
      if (this.pos - offset < HEADER_SIZE) break

      const payloadLen = this.buf.readUInt32LE(offset + OFF_LENGTH)
      const frameLen   = HEADER_SIZE + payloadLen

      if (this.pos - offset < frameLen) break

      // Copy frame out before compaction may overwrite it.
      onFrame(Buffer.from(this.buf.subarray(offset, offset + frameLen)))
      offset += frameLen
    }

    // Compact: move unconsumed bytes to front.
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
