export function encodeString(s: string): Buffer {
  return Buffer.from(s, 'utf8')
}

export function decodeString(buf: Buffer): string {
  return buf.toString('utf8')
}
