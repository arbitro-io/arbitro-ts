import { Packr, Unpackr } from 'msgpackr'

// ── Base interface ─────────────────────────────────────────────────────────

export interface Encoding<T> {
  encode(value: T): Buffer
  decode(buf: Buffer): T
}

// ── TextEncoding — abstract base for string-based encodings ───────────────

export abstract class TextEncoding implements Encoding<string> {
  abstract readonly encoding: BufferEncoding

  encode(value: string): Buffer { return Buffer.from(value, this.encoding) }
  decode(buf: Buffer):   string { return buf.toString(this.encoding) }
}

// ── StringCodec — UTF-8 by default ────────────────────────────────────────

export class StringCodec extends TextEncoding {
  readonly encoding: BufferEncoding
  constructor(encoding: BufferEncoding = 'utf8') {
    super()
    this.encoding = encoding
  }
}

// ── JsonCodec<T> — composes StringCodec, never extends it ─────────────────

export class JsonCodec<T> implements Encoding<T> {
  private readonly text: StringCodec
  constructor(encoding: BufferEncoding = 'utf8') {
    this.text = new StringCodec(encoding)
  }
  encode(value: T): Buffer { return this.text.encode(JSON.stringify(value)) }
  decode(buf: Buffer):   T { return JSON.parse(this.text.decode(buf)) as T }
}

// ── Codec<T> — schema-based msgpack, fastest ──────────────────────────────
// Fields are fixed at construction — no key discovery on hot path.

export type FieldType = 'string' | 'number' | 'boolean' | 'bigint' | 'buffer' | 'unknown'
export type Schema<T> = { [K in keyof Required<T>]: FieldType }

export class Codec<T extends Record<string, unknown>> implements Encoding<T> {
  readonly fields: (keyof T & string)[]
  private readonly packr:   Packr
  private readonly unpackr: Unpackr

  constructor(schema: Schema<T>) {
    this.fields  = Object.keys(schema) as (keyof T & string)[]
    this.packr   = new Packr({ structuredClone: false, useRecords: false })
    this.unpackr = new Unpackr({ structuredClone: false, useRecords: false })
  }

  // Encodes only known fields in definition order — no extras, no key enumeration.
  encode(value: T): Buffer {
    const obj: Record<string, unknown> = {}
    for (const k of this.fields) obj[k] = value[k]
    return Buffer.from(this.packr.pack(obj))
  }

  decode(buf: Buffer): T {
    return this.unpackr.unpack(buf) as T
  }
}
