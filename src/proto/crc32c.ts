// CRC32c using the Castagnoli polynomial (0x82F63B78).
// Matches the crc32fast crate used in arbitro-proto.

const TABLE: Uint32Array = (() => {
  const t = new Uint32Array(256)
  for (let i = 0; i < 256; i++) {
    let c = i
    for (let j = 0; j < 8; j++) {
      c = (c & 1) ? (0x82F63B78 ^ (c >>> 1)) : (c >>> 1)
    }
    t[i] = c
  }
  return t
})()

export function crc32c(buf: Buffer, start = 0, end = buf.length): number {
  let crc = 0xFFFF_FFFF
  for (let i = start; i < end; i++) {
    crc = (crc >>> 8) ^ (TABLE[(crc ^ buf[i]!) & 0xFF]!)
  }
  return (crc ^ 0xFFFF_FFFF) >>> 0
}
