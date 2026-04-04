// FNV-1a 32-bit hash — must match arbitro-broker's stream_id().
// Deterministic, ~1 ns, no deps. Client and broker compute the same hash.

export function streamId(name: Buffer | string): number {
  const buf = typeof name === 'string' ? Buffer.from(name) : name
  let h = 0x811c_9dc5
  for (let i = 0; i < buf.length; i++) {
    h ^= buf[i]!
    h = Math.imul(h, 0x0100_0193) >>> 0
  }
  return h >>> 0
}
