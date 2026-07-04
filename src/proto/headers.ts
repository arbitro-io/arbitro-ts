// TLV header encoding — matches arbitro-proto wire::msg_headers layout.
//
// Wire layout (ExtendedPayload):
//   [payload_len : u32 LE]
//   [user_payload : payload_len B]
//   [headers_len : u32 LE]       (= 6 + entries bytes)
//   [count       : u16 LE]
//   [entries...  : HeaderEntry × N]
//
// Each HeaderEntry:
//   [key_len : u8]
//   [val_len : u16 LE]
//   [data    : key_len + val_len B]

export const HDR_MSG_ID = 'msg-id'

export type HeaderMap = Record<string, string | Buffer>

/**
 * Encode user payload + headers into ExtendedPayload format.
 * Returns a Buffer ready to be sent as the PubFrame payload.
 */
export function encodeExtendedPayload(
  payload: Buffer,
  headers: HeaderMap,
): Buffer {
  const entries: Array<[Buffer, Buffer]> = []
  for (const [k, v] of Object.entries(headers)) {
    const keyBuf = Buffer.from(k)
    const valBuf = typeof v === 'string' ? Buffer.from(v) : v
    entries.push([keyBuf, valBuf])
  }

  let entriesLen = 0
  for (const [k, v] of entries) {
    entriesLen += 3 + k.length + v.length
  }

  const headersSection = 6 + entriesLen
  const total = 4 + payload.length + headersSection

  const buf = Buffer.allocUnsafe(total)
  let off = 0

  buf.writeUInt32LE(payload.length, off); off += 4
  payload.copy(buf, off); off += payload.length

  buf.writeUInt32LE(headersSection, off); off += 4
  buf.writeUInt16LE(entries.length, off); off += 2

  for (const [k, v] of entries) {
    buf.writeUInt8(k.length, off); off += 1
    buf.writeUInt16LE(v.length, off); off += 2
    k.copy(buf, off); off += k.length
    v.copy(buf, off); off += v.length
  }

  return buf
}

/**
 * Extract msg-id value from a headers map (for frame-level idempotency).
 */
export function extractMsgId(headers: HeaderMap): Buffer | undefined {
  const v = headers[HDR_MSG_ID]
  if (v == null) return undefined
  return typeof v === 'string' ? Buffer.from(v) : v
}
