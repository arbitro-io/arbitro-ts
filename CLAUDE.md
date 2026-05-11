# arbitro-ts

Official TypeScript client for the Arbitro message broker.

## Rules
Read all `.agent/rules/*.md` before writing or modifying any code. Rules are INVIOLABLE.

## Stack
Read `.agent/rules/stack.md` for the Node.js / tsup / vitest / TCP defaults and the `bigint`-for-u64 contract.

## Architecture
Read `.agent/rules/architecture.md` before adding a new client primitive, a new network call, or before changing the `ArbitroClient → Stream → Consumer → Subscription` hierarchy. Defines the "no network at construction" rule.

## Performance (hot path)
Read `.agent/rules/performance.md` before touching `connection.ts`, `framer.ts`, `message.ts`, `subscription.ts`, or any publish/ack/deliver path. Defines `switch`-vs-`if` dispatch in `onFrame`, zero-copy `Buffer.subarray`, lazy-decode, allocation discipline, and the hot-path PR audit.

## Encoding
Read `.agent/rules/encoding.md` before adding codecs, changing `Codec<T>`, or modifying the `LazyMessage` contract.

## Wire protocol
Read `.agent/rules/wire-protocol.md` before changing the framer, handshake, action handling, or any byte offset. Source of truth for the V2 binary format and must stay in sync with `arbitro-proto` in the broker repo.

## Code style
Read `.agent/rules/code-style.md` for naming, the 10 numbered code rules (no magic strings, getters not methods, `bigint` for u64, `switch` not `if`, etc.), and the file / function / class size limits.

## Testing
Read `.agent/rules/testing.md` before running tests, modifying `vitest.config.ts`, or opening a PR. Includes the integration-test Docker setup and the pre-merge checklist.

---

## Contributing & releases

- `CONTRIBUTING.md` — PR workflow, dev setup, commit conventions.
- `RELEASING.md` — SemVer policy and the npm publish flow.
