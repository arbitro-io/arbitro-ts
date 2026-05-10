# arbitro-ts

Official TypeScript client for the Arbitro message broker.
Inherits all rules from the root `CLAUDE.md` — every rule there applies here without exception.

---

## STACK

- **Runtime**: Node.js ≥ 20
- **Build**: tsup (esbuild) — CJS + ESM + `.d.ts`
- **Tests**: vitest
- **Serialization**: Binary V2 frames (zerocopy structs) + Codec<T> (msgpack) for user payloads
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

## WIRE CONTRACT — V2

**Handshake (client→server, first 8 bytes on connect):**
```
magic(4) "ARB2" (0x32425241 LE) | version(1)=2 | role(1)=0 (client) | caps(2)=0
```

**Frame Header: 16 bytes, little-endian**
```
action(2) | flags(1) | entry_flags(1) | msg_len(4) | seq(8)
```
Frame total = `HEADER_SIZE(16) + msg_len`

**Key constants (must match `arbitro-proto`):**
- `HEADER_SIZE = 16`, `MAGIC_V2 = 0x32425241`, `HELLO_SIZE = 8`
- `OFF_ACTION = 0`, `OFF_FLAGS = 2`, `OFF_ENTRY_FLAGS = 3`, `OFF_MSG_LEN = 4`, `OFF_SEQ = 8`
- `Flag.AckReq = 0x01` — publisher requests RepOk confirmation

**Action codes (u16 LE):**
| Code     | Action          | Direction |
|----------|-----------------|-----------|
| 0x0101   | Publish         | C→S       |
| 0x0103   | PublishBatch    | C→S       |
| 0x0104   | PublishWithReply| C→S       |
| 0x0201   | Ack             | C→S (fire-and-forget) |
| 0x0202   | Nack            | C→S (fire-and-forget) |
| 0x0206   | BatchAck        | C→S (fire-and-forget) |
| 0x020A   | BatchNack       | C→S (fire-and-forget) |
| 0x0301   | Subscribe       | C→S       |
| 0x0302   | Unsubscribe     | C→S       |
| 0x0401   | CreateStream    | C→S       |
| 0x0402   | DeleteStream    | C→S       |
| 0x0403   | GetStream       | C→S       |
| 0x0404   | ListStreams     | C→S       |
| 0x0405   | PurgeStream     | C→S       |
| 0x0406   | DrainSubject    | C→S       |
| 0x0501   | CreateConsumer  | C→S       |
| 0x0502   | DeleteConsumer  | C→S       |
| 0x0503   | GetConsumer     | C→S       |
| 0x0504   | ListConsumers   | C→S       |
| 0x0601   | Ping            | C→S       |
| 0x0602   | Pong            | S→C       |
| 0x0605   | Disconnect      | C→S       |
| 0x0701   | RepOk           | S→C       |
| 0x0702   | RepError        | S→C       |
| 0x0703   | Deliver         | S→C       |
| 0x0704   | RepBatch        | S→C       |

**Server replies:**
- `RepOk`:    Header(16) + ref_seq(8) = 24B total
- `RepError`: Header(16) + ref_seq(8) + error_code(2) + _pad(6) = 32B total
- `Deliver`:  Header(16) + consumer_id(4) + subject_hash(4) + subject_len(2) + _pad(2) + subject + payload

**stream_id** = server-returned `wire_hash_32` (foldhash). Cached client-side from CreateStream/GetStream RepOk responses. Never computed client-side.

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
