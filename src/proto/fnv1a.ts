// FNV-1a 32-bit hash — utility export. NOT used for server stream_id
// resolution (server uses foldhash). Kept for subject hashing and user code.

export function streamId(name: Buffer | string): number {
  const buf = typeof name === 'string' ? Buffer.from(name) : name
  let h = 0x811c_9dc5
  for (let i = 0; i < buf.length; i++) {
    h ^= buf[i]!
    h = Math.imul(h, 0x0100_0193) >>> 0
  }
  return h >>> 0
}
