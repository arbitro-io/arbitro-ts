import { HEADER_SIZE, OFF_ACTION, OFF_CRC32C, OFF_LENGTH, Action } from './constants'

type FrameCallback = (frame: Buffer) => void

// Accumulates incoming TCP bytes and emits complete frames.
// A frame is: HEADER_SIZE bytes of header + payload bytes.
// RepMessage/RepBatch use a different layout: payload = crc32c (topic_len) + length (data_len).
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

      const action     = this.buf.readUInt16LE(offset + OFF_ACTION)
      const lengthVal  = this.buf.readUInt32LE(offset + OFF_LENGTH)

      // RepMessage: crc32c = topic_len, length = data_len. Total payload = both.
      // RepBatch:   crc32c = entry_count, length = total payload bytes.
      let payloadLen: number
      if (action === Action.RepMessage) {
        const topicLen = this.buf.readUInt32LE(offset + OFF_CRC32C)
        payloadLen = topicLen + lengthVal
      } else {
        payloadLen = lengthVal
      }

      const frameLen = HEADER_SIZE + payloadLen
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
