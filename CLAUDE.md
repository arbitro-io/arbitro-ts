# arbitro-ts

Official TypeScript client for the Arbitro message broker.
Inherits all rules from the root `CLAUDE.md` — every rule there applies here without exception.

---

## STACK

- **Runtime**: Node.js ≥ 20
- **Build**: tsup (esbuild) — CJS + ESM + `.d.ts`
- **Tests**: vitest
- **Serialization**: msgpackr for ConsumerConfig/StreamConfig + Codec<T>
- **TCP**: `net.Socket` with `setNoDelay(true)` — no Nagle algorithm
- **u64**: always `bigint` — never `number` for sequence numbers

---

## ARCHITECTURE

```
ArbitroClient
  └── stream(name)         → Stream      (context only, no network)
        └── consumer(cfg)  → Consumer    (context only, no network)
              └── subscribe() → Subscription
        └── topic(subj, codec) → Topic<T>

  └── topic(subj, codec)   → Topic<T>    (shortcut without stream)
```

**Rule: no network calls at construction.** Only `.create()`, `.delete()`, `.subscribe()` touch the wire.

---

## FRAME DISPATCH — MANDATORY PATTERN

The `onFrame` handler is on the hot path. It **must** use `switch`, never `if`-chains.

```typescript
// ✅ REQUIRED — V8 compiles switch on integer to jump table: O(1)
switch (action) {
  case Action.RepOk:      ...; return
  case Action.RepError:   ...; return
  case Action.RepReply:   ...; return
  case Action.PubPublish: ...; return
  default:
    // surface the error — never silent drop (root CLAUDE.md rule 3)
    this.drain(new ArbitroError(`unknown action 0x${action.toString(16)}`, 'protocol'))
}

// ❌ FORBIDDEN — sequential evaluation, O(N) comparisons per frame
if (action === Action.RepOk)      { ...; return }
if (action === Action.RepError)   { ...; return }
if (action === Action.RepReply)   { ...; return }
if (action === Action.PubPublish) { ... }
// delivery frame evaluated last — worst case on the most frequent action
```

---

## BYTES MOVEMENT IN TYPESCRIPT

- `Buffer.subarray(start, end)` = zero-copy view, same underlying ArrayBuffer
- `msg.subject()` and `msg.data()` on `Message` are always `subarray` — never copied
- `LazyMessage<T>` field getters call `codec.decode()` once on first access, then cached
- If a message is acked without reading any field → zero deserialization
- Never use `JSON.parse` / `JSON.stringify` on the hot path — use `Codec<T>` with msgpack
- Never use `Buffer.concat` on the deliver path — pre-allocate or use writev-style batching

---

## ENCODING HIERARCHY

```
Encoding<T>                    ← interface — encode(T): Buffer, decode(Buffer): T
  ├── TextEncoding             ← abstract class, encoding: BufferEncoding
  │     └── StringCodec        ← concrete, encoding = 'utf8' by default
  ├── JsonCodec<T>             ← composes StringCodec (never extends it)
  └── Codec<T>                 ← msgpack schema-based, fastest
```

**Rules:**
- `JsonCodec<T>` composes `StringCodec` — it does NOT extend `TextEncoding` (wrong abstraction)
- `Codec<T>` exposes `.fields: string[]` — used by `LazyMessage` and `Topic` to define getters
- Any `Encoding<T>` implementation works in `Topic<T>` and `Consumer.subscribe()`

---

## LAZY MESSAGE

`LazyMessage<T>` = `T & { _raw, decode(), ack(), nack() }`

- Fields are `Object.defineProperty` getters — O(1) access, no Proxy overhead
- `codec.decode()` is called at most once, result cached in closure
- Implementations must NOT call `decode()` eagerly — lazy is the contract
- Field names come from `Codec<T>.fields` — if codec has no `.fields`, full decode happens on first access

---

## WIRE CONTRACT

Header: 32 bytes, little-endian
```
magic(4) | version(1) | flags(1) | action(2) | crc32c(4) | length(4) | sequence(8) | timestamp(8)
```
Payload: `u16_le(subject_len) + subject_bytes + data_bytes`

**Immutable constants:**
- `MAGIC = 0xA1B2_C3D4`, `VERSION = 0x02`
- All action codes must match `arbitro-proto/src/action.rs`
- CRC32c covers full frame with crc field zeroed (Castagnoli 0x82F63B78)
- Subject on wire is always raw bytes — never UTF-8 validated on hot path

---

## RULES

1. **No magic strings** — use `Action` and `Flags` const enums, never raw hex literals
2. **No Proxy** — use `Object.defineProperty` for dynamic getters (LazyMessage)
3. **No network at construction** — Stream, Consumer, Topic are context objects
4. **One allocation per publish** — `pack()` allocates the frame buffer once; no concat after
5. **No JSON on hot path** — raw message bytes travel as `Buffer`, never deserialized by the broker
6. **Getters not methods for field access** — `msg.id` not `msg.get('id')`
7. **`bigint` for all u64** — sequence numbers, timestamps, subIds
8. **`Buffer.subarray` not `slice`** for zero-copy views — `slice` copies in some runtimes
9. **`switch` not `if`-chain** in `onFrame` — jump table dispatch, O(1)
10. **No debug output on hot path** — no `console.log`, no `process.stderr.write` in production code

---

## HOT PATH AUDIT — TYPESCRIPT

Before every PR touching `connection.ts`, `framer.ts`, `message.ts`, or `subscription.ts`:

- [ ] `onFrame` uses `switch`, not `if`-chain
- [ ] `switch` has a `default` case that surfaces an error (not silent drop)
- [ ] No `process.stderr.write` or `console.*` calls remaining
- [ ] No `Buffer.concat` in the deliver path
- [ ] `msg.subject()` and `msg.data()` return `subarray` views, never copies
- [ ] Unknown `subId` in delivery: error surfaced, not silently ignored

---

## FILE SIZE LIMITS

| Scope | Limit |
|---|---|
| File | 150 lines |
| Function | 30 lines |
| Class | 80 lines |

If a file exceeds the limit, extract into a submodule.

---

## PRE-MERGE CHECKLIST

- [ ] `npm run typecheck` passes
- [ ] `npm test` passes
- [ ] No `any` in public API types
- [ ] No `JSON.parse/stringify` in hot path (subscribe callback, publish loop)
- [ ] `LazyMessage` fields are getters, not eagerly decoded
- [ ] New codec implementations expose `.fields?: string[]` if schema-based
- [ ] Hot path audit above completed
