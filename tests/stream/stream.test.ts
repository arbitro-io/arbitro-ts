import { describe, it, expect } from 'vitest'
import * as net from 'net'
import { ArbitroClient } from '../../src/client'
import { Consumer } from '../../src/consumer'
import { Topic } from '../../src/topic'
import { FrameView } from '../../src/proto/codec'
import { Action } from '../../src/proto/constants'
import { Codec } from '../../src/utils/codec'

function startServer(): Promise<{
  port:   number
  frames: () => FrameView[]
  close:  () => void
}> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = []

    const server = net.createServer((s) => {
      s.on('data', (d) => chunks.push(d))
    })

    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as net.AddressInfo).port

      const frames = (): FrameView[] => {
        const buf = Buffer.concat(chunks)
        const out: FrameView[] = []
        let off = 0
        while (off < buf.length) {
          const v = new FrameView(buf.subarray(off))
          out.push(v)
          off += v.totalSize()
        }
        return out
      }

      resolve({ port, frames, close: () => server.close() })
    })
  })
}

async function makeClient(port: number): Promise<ArbitroClient> {
  return new ArbitroClient({ servers: [`127.0.0.1:${port}`] }).connect()
}

describe('Stream', () => {
  it('stream() returns Stream without network call', async () => {
    const srv    = await startServer()
    const client = await makeClient(srv.port)
    const stream = client.stream('orders')

    await new Promise((r) => setTimeout(r, 30))
    expect(srv.frames().length).toBe(0) 

    await client.close()
    srv.close()
  })

  it('stream.create() sends PubCreateStream frame', async () => {
    const srv    = await startServer()
    const client = await makeClient(srv.port)

    client.stream('orders').create({ subjectFilter: 'orders.>' })
    await new Promise((r) => setTimeout(r, 30))

    const frames = srv.frames()
    expect(frames.length).toBe(1)
    expect(frames[0]!.action()).toBe(Action.PubCreateStream)
    expect(frames[0]!.subject().toString()).toBe('orders')

    await client.close()
    srv.close()
  })

  it('stream.consumer() returns Consumer with correct streamName', async () => {
    const srv    = await startServer()
    const client = await makeClient(srv.port)

    const consumer = client.stream('orders').consumer({ name: 'workers', filter: 'orders.>' })
    expect(consumer).toBeInstanceOf(Consumer)
    expect(consumer.streamName).toBe('orders')
    expect(consumer.config.name).toBe('workers')

    await client.close()
    srv.close()
  })

  it('stream.topic() returns Topic', async () => {
    const srv    = await startServer()
    const client = await makeClient(srv.port)
    const codec  = new Codec<{ id: number }>({ id: 'number' })

    const topic = client.stream('orders').topic('orders.new', codec)
    expect(topic).toBeInstanceOf(Topic)

    await client.close()
    srv.close()
  })
})
