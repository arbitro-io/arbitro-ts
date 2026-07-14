import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { ArbitroClient, ArbitroError, JournalType } from '../src'
import { cleanupNamedResources, createClient, makeScope, uniqueName } from './helpers/client'

// Integration test — needs a live broker (see tests/helpers/client.ts).
// Verifies Wave4c: client.request() actually correlates a reply instead
// of the pre-Wave4 stub that always resolved with an empty buffer.

let requester: ArbitroClient
let responder: ArbitroClient
const scope = makeScope(() => requester)

beforeAll(async () => {
  ;[requester, responder] = await Promise.all([createClient(), createClient()])
})

afterAll(async () => {
  await Promise.all([requester.close(), responder.close()])
})

afterEach(async () => { await scope.cleanup() })

describe('client.request()', () => {
  it('resolves with the reply payload from a running service subscriber', async () => {
    const name = scope.track(uniqueName('req'))
    await requester.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    await requester.createConsumer(name, { name, filter: `${name}.>` })

    // Responder: subscribe, echo payload back uppercased via msg.reply().
    const sub = await responder.subscribe(name, (msg) => {
      const upper = msg.data().toString().toUpperCase()
      msg.reply(Buffer.from(upper))
    })

    const reply = await requester.request(name, `${name}.echo`, Buffer.from('hello'))
    expect(reply.toString()).toBe('HELLO')

    sub.close()
  })

  it('correlates multiple concurrent requests to their own replies', async () => {
    const name = scope.track(uniqueName('req'))
    await requester.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    await requester.createConsumer(name, { name, filter: `${name}.>` })

    const sub = await responder.subscribe(name, (msg) => {
      msg.reply(Buffer.from(`echo:${msg.data().toString()}`))
    })

    const replies = await Promise.all(
      Array.from({ length: 5 }, (_, i) => requester.request(name, `${name}.echo`, Buffer.from(`m${i}`))),
    )
    expect(replies.map((r) => r.toString()).sort()).toEqual(
      Array.from({ length: 5 }, (_, i) => `echo:m${i}`).sort(),
    )

    sub.close()
  })

  it('rejects with a typed ArbitroError("request_timeout"/timeout) when no one replies', async () => {
    const name = scope.track(uniqueName('req'))
    await requester.createStream(name, {
      subjectFilter: `${name}.>`,
      journal: { type: JournalType.Memory },
    })
    // No subscriber at all — nothing will ever call msg.reply().

    await expect(requester.request(name, `${name}.nobody`, Buffer.from('x'), 300))
      .rejects.toMatchObject({ code: 'timeout' })

    try {
      await requester.request(name, `${name}.nobody`, Buffer.from('x'), 300)
      expect.unreachable('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(ArbitroError)
      expect((e as ArbitroError).code).toBe('timeout')
    }
  })
})
