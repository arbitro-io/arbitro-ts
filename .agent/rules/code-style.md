---
description: Code style, naming, and file/function/class size limits — INVIOLABLE
---

# CODE STYLE

## The 10 numbered rules

1. **No magic strings or magic numbers** — use the `Action` and `Flags` const enums, never raw hex literals like `0x0703`. New constants belong in `src/protocol/constants.ts` (or the closest equivalent module).

2. **No `Proxy`** for dynamic getters — use `Object.defineProperty` (see `LazyMessage`). Proxies cost ~10× compared to defineProperty getters on V8.

3. **No network at construction** — `Stream`, `Consumer`, `Topic` are context objects; constructors don't touch the wire. See `architecture.md` for the full rule.

4. **One allocation per publish** — `pack()` allocates the frame buffer once; no `Buffer.concat` after. See `performance.md`.

5. **No JSON on hot path** — raw message bytes travel as `Buffer`, never `JSON.parse`/`stringify` in publish, ack, or deliver loops. User payloads go through `Codec<T>`.

6. **Getters not methods for field access** — `msg.id`, not `msg.get('id')`. Drives autocompletion and matches the lazy-decode contract.

7. **`bigint` for all u64** — sequence numbers, timestamps, subscription IDs, anything declared as `u64` in the wire protocol. Never `number` (loses precision above 2^53).

8. **`Buffer.subarray` not `Buffer.slice`** for zero-copy views. `slice` is a deprecated alias and copies in some runtimes.

9. **`switch` not `if`-chain** in `onFrame` and any other action-code dispatch. V8 compiles `switch(integer)` to a jump table. See `performance.md`.

10. **No debug output on hot path** — no `console.log`, no `process.stderr.write` in production code. Tests can use them; production cannot.

## File / function / class size limits

| Scope | Limit | Action when exceeded |
|---|---|---|
| File | **150 lines** | Extract into a submodule. |
| Function | **30 lines** | Extract helpers. Don't comment-bloat to dodge the count. |
| Class | **80 lines** | Split responsibilities. If it's a god-class, this is the signal. |

These are not lint-enforced (yet) but reviewers will push back on PRs that go materially over without justification.

## Naming

- Classes: `PascalCase` (`ArbitroClient`, `Subscription`, `Codec`).
- Interfaces: `PascalCase` without `I` prefix (`Encoding<T>`, not `IEncoding<T>`).
- Type aliases: `PascalCase`.
- Functions and variables: `camelCase`.
- Constants exported from modules: `SCREAMING_SNAKE_CASE` for primitives, `PascalCase` for const enums (`Action.RepOk`).
- File names: `kebab-case.ts`. One primary export per file (re-exports from `src/index.ts`).

## TypeScript

- Strict mode on (`tsconfig.json` already enforces).
- `any` is forbidden in public API types. `unknown` + narrowing is fine.
- Prefer `readonly` arrays and `readonly` properties where applicable.
- No `enum` (non-const). Use `const enum` (zero-cost at runtime) or string union types.
