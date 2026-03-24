import { Packr, Unpackr } from 'msgpackr'
import type { ZodObject, ZodRawShape, output } from 'zod'
import type { Encoding } from './codec'

// zod is an optional peer dep — only imported as a type here so the module
// can be tree-shaken. The runtime import happens inside zodCodec().

const packr   = new Packr({ structuredClone: false, useRecords: false })
const unpackr = new Unpackr({ structuredClone: false, useRecords: false })

/** Wraps a ZodObject schema as an Encoding<T>.
 *
 *  - encode: msgpack (no Zod overhead — bytes out)
 *  - decode: msgpack unpack → schema.parse() (validates on inbound data)
 *  - fields: Object.keys(schema.shape) — powers LazyMessage<T> getters
 *
 *  Validation cost on decode is intentional and acceptable on the management
 *  path. Do NOT use zodCodec on a hot publish loop.
 */
export function zodCodec<S extends ZodRawShape>(
  zodSchema: ZodObject<S>,
): Encoding<output<ZodObject<S>>> & { readonly fields: string[] } {
  const fields = Object.keys(zodSchema.shape)
  return {
    fields,
    encode(value): Buffer {
      return Buffer.from(packr.pack(value))
    },
    decode(buf): output<ZodObject<S>> {
      return zodSchema.parse(unpackr.unpack(buf))
    },
  }
}
