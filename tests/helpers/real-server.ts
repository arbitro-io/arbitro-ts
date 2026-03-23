import { spawn } from 'child_process'
import * as net from 'net'

export interface RealServer {
  addr: string
  stop: () => Promise<void>
}

function findFreePort(): Promise<number> {
  return new Promise((resolve) => {
    const srv = net.createServer()
    srv.listen(0, () => {
      const port = (srv.address() as net.AddressInfo).port
      srv.close(() => resolve(port))
    })
  })
}

function waitForPort(host: string, port: number, timeoutMs = 20_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs
    const attempt = () => {
      const sock = net.createConnection({ host, port })
      sock.once('connect', () => { sock.destroy(); resolve() })
      sock.once('error', () => {
        sock.destroy()
        if (Date.now() > deadline) return reject(new Error(`${host}:${port} not ready after ${timeoutMs}ms`))
        setTimeout(attempt, 200)
      })
    }
    attempt()
  })
}

// Starts an arbitro Docker container on a free port.
// Requires the 'arbitro' image to be built: docker build -t arbitro ./arbitro
export async function startServer(): Promise<RealServer> {
  const port = await findFreePort()
  const name = `arbitro-test-${port}`

  spawn('docker', [
    'run', '--rm', '--name', name,
    '-p', `${port}:9898`,
    '-e', 'RUST_LOG=error',
    'arbitro',
  ], { stdio: 'ignore' })

  await waitForPort('127.0.0.1', port)

  return {
    addr: `127.0.0.1:${port}`,
    stop: () =>
      new Promise<void>((resolve) => {
        spawn('docker', ['stop', name], { stdio: 'ignore' }).on('close', () => resolve())
      }),
  }
}

export function waitUntil(cond: () => boolean, timeoutMs = 3_000): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs
    const poll = () => {
      if (cond()) return resolve()
      if (Date.now() > deadline) return reject(new Error('waitUntil timeout'))
      setTimeout(poll, 20)
    }
    poll()
  })
}
