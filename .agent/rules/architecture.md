---
description: Client topology and the "no network at construction" rule — INVIOLABLE
---

# ARCHITECTURE

```
ArbitroClient
  └── stream(name)         → Stream      (context only, no network)
        └── consumer(cfg)  → Consumer    (context only, no network)
              └── subscribe() → Subscription   (touches the wire)
        └── topic(subj, codec) → Topic<T>      (context only, no network)

  └── topic(subj, codec)   → Topic<T>    (shortcut without stream — context only)
```

## Rule: no network calls at construction

`Stream`, `Consumer`, `Topic` are **context objects**. Their constructors and the factory methods that return them (`.stream()`, `.consumer()`, `.topic()`) MUST NOT touch the wire.

Only these methods touch the wire:

- `.create()` — create stream / consumer / subscription on the broker
- `.delete()` — delete the corresponding entity
- `.subscribe()` — open a delivery subscription
- `.publish()` — emit one or more frames
- `.ack()` / `.nack()` — feedback frames

## Why

1. Construction synchronous, no `await`. A caller can build a whole topology graph in one expression without awaiting.
2. Idempotency: re-instantiating a `Stream("orders")` twice is free; the broker only knows about it when `.create()` runs.
3. Testability: unit tests can build the object graph without a broker handshake or mock socket.
4. Reconnect: on socket drop, the in-memory object graph survives untouched; only the wire-touching methods need to retry.

## Composition rules

- `ArbitroClient` owns the socket and the framer.
- `Stream` holds a reference back to `ArbitroClient` for wire access.
- `Consumer` holds a reference back to `Stream`. Never to `ArbitroClient` directly.
- `Topic<T>` may be created from a `Stream` (scoped to that stream) or from `ArbitroClient` (shortcut with no stream context).
- `Subscription` is the one stateful object; it owns the registered handler and the consumer/subscription IDs returned by the broker.

## Adding a new primitive

If a new conceptual entity appears (e.g. `Workspace`, `ConsumerGroup`), it MUST follow the same pattern: context-only constructor, wire calls only in explicitly-named methods.
