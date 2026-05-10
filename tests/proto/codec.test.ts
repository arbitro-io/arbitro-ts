import { describe, it, expect } from 'vitest'
import { pack, FrameView, requiresSubject } from '../../src/proto/codec'
import { Action, Flags, HEADER_SIZE, MAGIC, VERSION } from '../../src/proto/constants'

describe('codec — subject frames (Pub* actions)', () => {
  it('pack produces correct magic and version', () => {
    const frame = pack({ action: Action.PubPublish, seq: 1n, subject: 'foo', data: Buffer.from('bar') })
    expect(new FrameView(frame).isValid()).toBe(true)
  })

  it('roundtrip preserves subject, data, seq, action', () => {
    const subj  = Buffer.from('orders.us.new')
    const data  = Buffer.from('{"id":1}')
    const frame = pack({ action: Action.PubPublish, seq: 42n, subject: subj, data })
    const view  = new FrameView(frame)
    expect(view.subject().toString()).toBe('orders.us.new')
    expect(view.data().toString()).toBe('{"id":1}')
    expect(view.seq()).toBe(42n)
    expect(view.action()).toBe(Action.PubPublish)
  })

  it('frame size is HEADER_SIZE + 2 + subject + data', () => {
    const subj  = Buffer.from('foo')
    const data  = Buffer.from('bar')
    const frame = pack({ action: Action.PubPublish, seq: 1n, subject: subj, data })
    expect(frame.length).toBe(HEADER_SIZE + 2 + subj.length + data.length)
  })

  it('flags are encoded correctly', () => {
    const frame = pack({ action: Action.PubPublish, flags: Flags.NoAck, seq: 1n, subject: 'x', data: Buffer.alloc(0) })
    expect(new FrameView(frame).flags() & Flags.NoAck).toBeTruthy()
  })

  it('subject() and data() are zero-copy views of the same buffer', () => {
    const frame = pack({ action: Action.PubPublish, seq: 1n, subject: 'hello', data: Buffer.from('world') })
    const view  = new FrameView(frame)
    expect(view.subject().buffer).toBe(frame.buffer)
    expect(view.data().buffer).toBe(frame.buffer)
  })

  it('hasSubject() is true for Pub* actions', () => {
    const frame = pack({ action: Action.PubPublish, seq: 1n, subject: 'x', data: Buffer.alloc(0) })
    expect(new FrameView(frame).hasSubject()).toBe(true)
  })
})

describe('codec — non-subject frames (Rep* / Sys* actions)', () => {
  it('RepAck: frame is HEADER_SIZE + 2 bytes (empty subject u16 prefix)', () => {
    const frame = pack({ action: Action.RepAck, seq: 1n, subject: Buffer.alloc(0), data: Buffer.alloc(0) })
    expect(frame.length).toBe(HEADER_SIZE + 2)
  })

  it('RepAck: subject() and data() return empty buffers without OOB', () => {
    const frame = pack({ action: Action.RepAck, seq: 1n, subject: Buffer.alloc(0), data: Buffer.alloc(0) })
    const view  = new FrameView(frame)
    expect(view.hasSubject()).toBe(true)
    expect(view.subject().length).toBe(0)
    expect(view.data().length).toBe(0)
  })

  it('RepOk: seq is preserved, no crash on subject()/data()', () => {
    const frame = pack({ action: Action.RepOk, seq: 99n, subject: Buffer.alloc(0), data: Buffer.alloc(0) })
    const view  = new FrameView(frame)
    expect(view.seq()).toBe(99n)
    expect(view.subject().length).toBe(0)
    expect(view.data().length).toBe(0)
  })

  it('RepError: data() returns the error message bytes', () => {
    const msg   = Buffer.from('consumer not found')
    const frame = pack({ action: Action.RepError, seq: 0n, subject: Buffer.alloc(0), data: msg })
    const view  = new FrameView(frame)
    expect(view.hasSubject()).toBe(false)
    expect(view.data().toString()).toBe('consumer not found')
  })

  it('RepError: frame size is HEADER_SIZE + message bytes', () => {
    const msg   = Buffer.from('oops')
    const frame = pack({ action: Action.RepError, seq: 0n, subject: Buffer.alloc(0), data: msg })
    expect(frame.length).toBe(HEADER_SIZE + msg.length)
  })
})

describe('requiresSubject', () => {
  it('returns true for all Pub* actions', () => {
    const pubActions = [
      Action.PubPublish, Action.PubSubscribe, Action.PubUnsubscribe,
      Action.PubCreateStream, Action.PubDeleteStream, Action.PubPull,
      Action.PubCreateConsumer, Action.PubDeleteConsumer,
    ]
    for (const a of pubActions) expect(requiresSubject(a)).toBe(true)
  })

  it('returns false for non-subject Rep* actions', () => {
    const repNoSubject = [Action.RepOk, Action.RepReply, Action.RepError]
    for (const a of repNoSubject) expect(requiresSubject(a)).toBe(false)
  })

  it('returns true for RepAck and RepNack (wire protocol includes stream_name)', () => {
    expect(requiresSubject(Action.RepAck)).toBe(true)
    expect(requiresSubject(Action.RepNack)).toBe(true)
  })

  it('returns false for SysConnect, SysKeepalive, SysDisconnect', () => {
    expect(requiresSubject(Action.SysConnect)).toBe(false)
    expect(requiresSubject(Action.SysKeepalive)).toBe(false)
    expect(requiresSubject(Action.SysDisconnect)).toBe(false)
  })
})
