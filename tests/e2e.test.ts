import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType } from '../src'
import { startServer, waitUntil, type RealServer } from './helpers/real-server'

// Three separate clients — one per role — same broker.
let server: RealServer
let admin:  ArbitroClient
let pub:    ArbitroClient
let sub:    ArbitroClient
let counter = 0

function uid(): string { return `e${++counter}` }

beforeAll(async () => {
  server = await startServer()
  const addr = server.addr
  admin = new ArbitroClient({ servers: [addr] })
  pub   = new ArbitroClient({ servers: [addr] })
  sub   = new ArbitroClient({ servers: [addr] })
  await Promise.all([admin.connect(), pub.connect(), sub.connect()])
})

afterAll(async () => {
  await Promise.all([admin.close(), pub.close(), sub.close()])
  await server.stop()
})

describe('end-to-end', () => {
  it('subscriber on one client receives messages published by a separate client', async () => {
    const name = uid()
    admin.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    admin.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const subscription = await sub.subscribe(name, (msg) => received.push(msg.data().toString()))

    pub.publish(`${name}.event`, Buffer.from('hello'))
    pub.publish(`${name}.event`, Buffer.from('world'))

    await waitUntil(() => received.length >= 2)
    subscription.close()

    expect(received).toEqual(['hello', 'world'])
  })

  it('publishAck resolves when broker confirms receipt', async () => {
    const name = uid()
    admin.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    // No subscriber needed — server sends RepOk once message is journaled.
    await expect(pub.publishAck(`${name}.e`, Buffer.from('data'))).resolves.toBeUndefined()
  })

  it('messages delivered in publish order across clients', async () => {
    const name = uid()
    admin.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    admin.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const subscription = await sub.subscribe(name, (msg) => received.push(msg.data().toString()))

    for (let i = 0; i < 5; i++) {
      await pub.publishAck(`${name}.e`, Buffer.from(`msg-${i}`))
    }
    await waitUntil(() => received.length >= 5)
    subscription.close()

    expect(received).toEqual(['msg-0', 'msg-1', 'msg-2', 'msg-3', 'msg-4'])
  })

  it('admin creates stream and consumer before subscriber and publisher connect', async () => {
    const name = uid()
    // Admin sets up infrastructure first
    admin.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    admin.createConsumer(name, { name, filter: `${name}.>` })
    // Fence: wait until server has processed admin's commands before sub subscribes.
    // admin and sub are different TCP connections — no implicit ordering guarantee.
    await admin.sync()

    const received: string[] = []
    const subscription = await sub.subscribe(name, (msg) => received.push(msg.data().toString()))

    await pub.publishAck(`${name}.log`, Buffer.from('event'))
    await waitUntil(() => received.length >= 1)
    subscription.close()

    expect(received[0]).toBe('event')
  })
})
