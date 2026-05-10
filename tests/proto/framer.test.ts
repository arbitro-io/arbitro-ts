import { describe, it, expect } from 'vitest'
import { Framer } from '../../src/proto/framer'
import { packPublish } from '../../src/proto/v2'

function makeFrame(): Buffer {
  return packPublish(1n, 0xCAFE, Buffer.from('test'), Buffer.from('payload'))
}

describe('framer', () => {
  it('emits one frame when given complete bytes', () => {
    const framer = new Framer()
    const frames: Buffer[] = []
    framer.push(makeFrame(), (f) => frames.push(f))
    expect(frames.length).toBe(1)
  })

  it('emits two frames when given two back-to-back', () => {
    const framer = new Framer()
    const frames: Buffer[] = []
    const two = Buffer.concat([makeFrame(), makeFrame()])
    framer.push(two, (f) => frames.push(f))
    expect(frames.length).toBe(2)
  })

  it('emits frame split across two chunks', () => {
    const framer = new Framer()
    const frames: Buffer[] = []
    const full  = makeFrame()
    const half  = Math.floor(full.length / 2)
    framer.push(full.subarray(0, half),  (f) => frames.push(f))
    expect(frames.length).toBe(0)
    framer.push(full.subarray(half),     (f) => frames.push(f))
    expect(frames.length).toBe(1)
  })

  it('handles many single-byte pushes', () => {
    const framer = new Framer()
    const frames: Buffer[] = []
    const full = makeFrame()
    for (const byte of full) {
      framer.push(Buffer.from([byte]), (f) => frames.push(f))
    }
    expect(frames.length).toBe(1)
  })

  it('frame length = HEADER_SIZE + msg_len', () => {
    const frame = makeFrame()
    const msgLen = frame.readUInt32LE(4)  // OFF_MSG_LEN = 4
    expect(frame.length).toBe(16 + msgLen)
  })
})
