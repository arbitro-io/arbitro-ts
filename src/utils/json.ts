export function encodeJson(value: unknown): Buffer {
  return Buffer.from(JSON.stringify(value), 'utf8')
}

export function decodeJson<T = unknown>(buf: Buffer): T {
  return JSON.parse(buf.toString('utf8')) as T
}
