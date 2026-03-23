import { describe, it, expect } from 'vitest'
import { Framer } from '../../src/proto/framer'
import { pack } from '../../src/proto/codec'
import { Action } from '../../src/proto/constants'

function makeFrame(): Buffer {
  return pack({ action: Action.PubPublish, seq: 1n, subject: 'test', data: Buffer.from('payload') })
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
})
