---
description: Encoding hierarchy and LazyMessage contract — INVIOLABLE
---

# ENCODING

## Hierarchy

```
Encoding<T>                    ← interface — encode(T): Buffer, decode(Buffer): T
  ├── TextEncoding             ← abstract class, encoding: BufferEncoding
  │     └── StringCodec        ← concrete, encoding = 'utf8' by default
  ├── JsonCodec<T>             ← composes StringCodec (does NOT extend it)
  └── Codec<T>                 ← msgpack schema-based, fastest path
```

## Rules

1. **`JsonCodec<T>` composes `StringCodec`** — it does NOT extend `TextEncoding`. A JSON codec is a string codec with a schema; conceptually distinct from "the family of text encodings". Extending `TextEncoding` would imply JSON is a `BufferEncoding`, which it isn't.

2. **`Codec<T>` exposes `.fields: string[]`** when the schema is known up-front. `LazyMessage<T>` and `Topic<T>` use that list to materialise per-field getters at codec-construction time.

3. **Any `Encoding<T>` implementation works** in `Topic<T>` and `Consumer.subscribe()`. The client treats them polymorphically; there is no special case for `Codec<T>` in the runtime path (only in the constructor that wires getters).

## Picking a codec

| Codec | When |
|---|---|
| `StringCodec` (utf8) | Payload is plain text; no JSON parsing needed. |
| `JsonCodec<T>` | Compatibility with non-arbitro consumers that expect JSON, despite the cost. |
| `Codec<T>` (msgpack) | Default for high-throughput consumers. Roughly 10× faster than `JsonCodec`. |

## LazyMessage<T>

`LazyMessage<T>` = `T & { _raw, decode(), ack(), nack() }`.

### Contract

- Field access goes through `Object.defineProperty` getters — O(1), no `Proxy` overhead.
- `codec.decode()` is called **at most once**. Result cached in closure.
- Implementations MUST NOT call `decode()` eagerly. Lazy is the contract — calling it eagerly defeats the purpose and silently regresses ack-only flows (callers who never read fields pay zero deserialization cost).
- Field names come from `Codec<T>.fields`. If a codec has no `.fields`, full decode happens on first access — that is the expected fallback, NOT a bug to "fix" by calling decode eagerly.

### Why this matters

A typical workload acks 80%+ of messages without reading any field (e.g. dropping duplicates by header, routing-by-subject pipelines). Lazy decode means those messages never pay the msgpack cost. Eager decode would slow the path by 2-5×.

## Adding a new codec

1. Implement `Encoding<T>` interface (`encode(value: T): Buffer`, `decode(buf: Buffer): T`).
2. If the codec is schema-aware, expose `.fields: readonly string[]`.
3. Add a test in `tests/codec-*.test.ts` that exercises both encode and decode round-trips.
4. Update this file's "Picking a codec" table if the new codec deserves a primary slot.
