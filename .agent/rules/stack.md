---
description: Runtime, build and serialization stack — INVIOLABLE
---

# STACK

- **Runtime**: Node.js ≥ 20
- **Build**: tsup (esbuild) — emits CJS + ESM + `.d.ts`
- **Tests**: vitest
- **Serialization**: Binary V2 frames (zerocopy structs over `Buffer`) + `Codec<T>` (msgpack via `msgpackr`) for user payloads
- **TCP**: `net.Socket` with `setNoDelay(true)` — Nagle disabled
- **u64**: always `bigint` — never `number` for sequence numbers, timestamps, or any wire `u64` field

## Why these choices

- **tsup over tsc-only**: ships ESM + CJS in one config, faster cold builds, no manual rollup.
- **vitest over jest**: faster, native ESM, same `describe`/`it` API.
- **msgpackr over JSON / protobuf**: ~10× faster encode/decode than JSON, schema-optional unlike protobuf, fits the lazy-decode pattern in `Codec<T>`.
- **Nagle off**: the broker treats sub-millisecond round-trips as table stakes; Nagle would batch tiny acks and add 40 ms of latency.

## Versioning floor

`engines.node` declared as `>=20`. Public APIs SHOULD NOT use features that postdate that floor without a polyfill. When bumping the floor, update `engines.node` and `peerDependencies."@types/node"` together.
