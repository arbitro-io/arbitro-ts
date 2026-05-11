---
description: Hot-path rules — INVIOLABLE. Read before touching connection / framer / subscription / publish or ack paths.
---

# PERFORMANCE — HOT PATH

The hot path in this client covers:

- `onFrame` (inbound frame dispatch)
- `pack()` / publish encoding
- `Subscription.recv` callback delivery
- Ack / Nack outbound encoding

Per-frame budget is **single-digit microseconds**. Every rule below is enforced because a small inefficiency in one of these paths shows up as throughput collapse under load.

## Frame dispatch — `switch`, NEVER `if`-chain

The `onFrame` handler in `connection.ts` MUST dispatch with `switch`.

```typescript
// ✅ REQUIRED — V8 compiles switch on integer to jump table: O(1)
switch (action) {
  case Action.RepOk:      ...; return
  case Action.RepError:   ...; return
  case Action.RepReply:   ...; return
  case Action.PubPublish: ...; return
  default:
    // surface — never silent drop
    this.drain(new ArbitroError(`unknown action 0x${action.toString(16)}`, 'protocol'))
}

// ❌ FORBIDDEN — sequential evaluation, O(N) per frame
if (action === Action.RepOk)      { ...; return }
if (action === Action.RepError)   { ...; return }
if (action === Action.PubPublish) { ... }
// delivery frame evaluated last — worst case on the most frequent action
```

The `default` arm MUST surface an error, never silently drop. Unknown action codes are a protocol break and need to bubble up.

## Bytes movement

- **`Buffer.subarray(start, end)`** — zero-copy view, same underlying `ArrayBuffer`. USE THIS.
- **`Buffer.slice(start, end)`** — deprecated alias of `subarray` in modern Node but **copies in some runtimes** (older Bun, browser polyfills). NEVER use.
- `msg.subject()` and `msg.data()` on `Message` MUST return `subarray` views, never copies.
- `LazyMessage<T>` field getters call `codec.decode()` **once** on first access, then cache the result. A message acked without reading any field → zero deserialization.
- NEVER `JSON.parse` / `JSON.stringify` on the hot path. Payload bytes travel as `Buffer`; user payload decoding goes through `Codec<T>` (msgpack).
- NEVER `Buffer.concat` on the deliver path. Pre-allocate or use writev-style batching.

## Allocation discipline

- **One allocation per publish.** `pack()` allocates the frame buffer once; no concat after.
- **No `Proxy`** for dynamic field access. Use `Object.defineProperty` getters — proxies cost ~10× the dispatch.
- **No `console.log` / `process.stderr.write`** in production hot-path code. They flush stderr synchronously and can block the event loop for milliseconds.
- **No `async/await` inside `onFrame`**. Frame dispatch must remain synchronous; if a handler needs to await, it enqueues into a per-subscription mailbox and returns.

## Hot path audit (PR checklist)

Before merging any PR that touches `connection.ts`, `framer.ts`, `message.ts`, or `subscription.ts`:

- [ ] `onFrame` uses `switch`, not `if`-chain
- [ ] `switch` has a `default` case that surfaces an error (not silent drop)
- [ ] No `process.stderr.write` or `console.*` calls remaining
- [ ] No `Buffer.concat` in the deliver path
- [ ] `msg.subject()` and `msg.data()` return `subarray` views, never copies
- [ ] Unknown `subId` in delivery: error surfaced, not silently ignored
- [ ] No `async`/`await` introduced inside the synchronous dispatch path

## Measurement

Throughput is measured by `benches/throughput.ts` against a local broker (Docker image `ghcr.io/arbitro-io/arbitro-server:latest`). A change is a regression if it drops msg/s by more than 5% on the same bench config.
