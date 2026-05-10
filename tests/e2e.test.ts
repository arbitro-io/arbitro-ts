import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { ArbitroClient, JournalType } from '../src'
import { cleanupNamedResources, createClient, uniqueName, waitUntil } from './helpers/client'

// Three separate clients — one per role — same broker.
let admin: ArbitroClient
let pub:   ArbitroClient
let sub:   ArbitroClient
const created: string[] = []
beforeAll(async () => {
  ;[admin, pub, sub] = await Promise.all([createClient(), createClient(), createClient()])
})

afterAll(async () => {
  await cleanupNamedResources(admin, created)
  await Promise.all([admin.close(), pub.close(), sub.close()])
})

describe('end-to-end', () => {
  it('subscriber on one client receives messages published by a separate client', async () => {
    const name = uniqueName('e'); created.push(name)
    await admin.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await admin.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const subscription = await sub.subscribe(name, (msg) => received.push(msg.data().toString()))

    pub.publish(name, `${name}.event`, Buffer.from('hello'))
    pub.publish(name, `${name}.event`, Buffer.from('world'))

    await waitUntil(() => received.length >= 2)
    subscription.close()

    expect(received).toEqual(['hello', 'world'])
  })

  it('publishAck resolves when broker confirms receipt', async () => {
    const name = uniqueName('e'); created.push(name)
    await admin.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    // No subscriber needed — server sends RepOk once message is journaled.
    await expect(pub.publishAck(name, `${name}.e`, Buffer.from('data'))).resolves.toBeUndefined()
  })

  it('messages delivered in publish order across clients', async () => {
    const name = uniqueName('e'); created.push(name)
    await admin.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await admin.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const subscription = await sub.subscribe(name, (msg) => received.push(msg.data().toString()))

    for (let i = 0; i < 5; i++) {
      await pub.publishAck(name, `${name}.e`, Buffer.from(`msg-${i}`))
    }
    await waitUntil(() => received.length >= 5)
    subscription.close()

    expect(received).toEqual(['msg-0', 'msg-1', 'msg-2', 'msg-3', 'msg-4'])
  })

  it('admin creates stream and consumer before subscriber and publisher connect', async () => {
    const name = uniqueName('e'); created.push(name)
    // createStream/createConsumer block until the server confirms — no sync() needed.
    await admin.createStream(name, { subjectFilter: `${name}.>`, journal: { type: JournalType.Memory } })
    await admin.createConsumer(name, { name, filter: `${name}.>` })

    const received: string[] = []
    const subscription = await sub.subscribe(name, (msg) => received.push(msg.data().toString()))

    await pub.publishAck(name, `${name}.log`, Buffer.from('event'))
    await waitUntil(() => received.length >= 1)
    subscription.close()

    expect(received[0]).toBe('event')
  })
})
