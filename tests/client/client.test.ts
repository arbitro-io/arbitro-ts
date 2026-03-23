import { describe, it, expect } from 'vitest'
import * as net from 'net'
import { ArbitroClient } from '../../src/client'
import { FrameView } from '../../src/proto/codec'
import { Action, Flags } from '../../src/proto/constants'

// Minimal echo server: accepts one connection, collects all received bytes.
function startEchoServer(): Promise<{
  port:     number
  received: () => Buffer
  close:    () => void
}> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = []

    const server = net.createServer((s) => {
      s.on('data', (d) => chunks.push(d))
    })

    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as net.AddressInfo).port
      resolve({
        port,
        received: () => Buffer.concat(chunks),
        close:    () => server.close(),
      })
    })
  })
}

describe('ArbitroClient', () => {
  it('publish sends a PubPublish frame with NoAck flag', async () => {
    const srv    = await startEchoServer()
    const client = new ArbitroClient({ servers: [`127.0.0.1:${srv.port}`] })
    await client.connect()

    client.publish('orders.new', Buffer.from('hello'))
    await new Promise((r) => setTimeout(r, 50))

    const view = new FrameView(srv.received())
    expect(view.action()).toBe(Action.PubPublish)
    expect(view.subject().toString()).toBe('orders.new')
    expect(view.flags() & Flags.NoAck).toBeTruthy()

    await client.close()
    srv.close()
  })

  it('prefix is prepended to subject', async () => {
    const srv    = await startEchoServer()
    const client = new ArbitroClient({ servers: [`127.0.0.1:${srv.port}`], prefix: 'myapp' })
    await client.connect()

    client.publish('orders.new', Buffer.alloc(0))
    await new Promise((r) => setTimeout(r, 50))

    const view = new FrameView(srv.received())
    expect(view.subject().toString()).toBe('myapp.orders.new')

    await client.close()
    srv.close()
  })

  it('publishBatch sends all frames concatenated', async () => {
    const srv    = await startEchoServer()
    const client = new ArbitroClient({ servers: [`127.0.0.1:${srv.port}`] })
    await client.connect()

    client.publishBatch([
      ['a.b', Buffer.from('1')],
      ['c.d', Buffer.from('2')],
      ['e.f', Buffer.from('3')],
    ])
    await new Promise((r) => setTimeout(r, 50))

    const received = srv.received()
    let offset = 0; let count = 0
    while (offset < received.length) {
      const view = new FrameView(received.subarray(offset))
      offset += view.totalSize()
      count++
    }
    expect(count).toBe(3)

    await client.close()
    srv.close()
  })
})
