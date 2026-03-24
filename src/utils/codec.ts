import { Packr, Unpackr } from 'msgpackr'

// ── Base interface ─────────────────────────────────────────────────────────

/** Core encode/decode contract. `fields` is required only for schema-based codecs
 *  that power LazyMessage<T> getters — other encodings may omit it. */
export interface Encoding<T> {
  encode(value: T): Buffer
  decode(buf: Buffer): T
  readonly fields?: string[]
}

// ── FieldType inference ────────────────────────────────────────────────────

/** Maps a FieldType string literal to its corresponding TypeScript type. */
export type FieldTypeMap = {
  string:  string
  number:  number
  boolean: boolean
  bigint:  bigint
  buffer:  Buffer
  unknown: unknown
}

/** Infers the TypeScript record type from a schema definition.
 *  Eliminates the need to define both an interface and a Schema<T>. */
export type InferSchema<S extends Record<string, FieldType>> = {
  [K in keyof S]: FieldTypeMap[S[K]]
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

// ── schema() factory ──────────────────────────────────────────────────────
// Creates a Codec<T> with T inferred from the schema definition.
// No need to define a separate interface.
//
// Before: new Codec<Order>({ id: 'number', status: 'string' })
//  After: schema({ id: 'number', status: 'string' })   ← type inferred

/** Creates a msgpack Codec with the TypeScript type inferred from the schema definition. */
export function schema<S extends Record<string, FieldType>>(def: S): Codec<InferSchema<S>> {
  return new Codec<InferSchema<S>>(def as Schema<InferSchema<S>>)
}
