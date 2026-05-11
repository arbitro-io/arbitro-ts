---
description: Test commands, pre-merge checklist, integration setup — INVIOLABLE
---

# TESTING

## Test layers

| Layer | Command | Speed | Needs broker |
|---|---|---|---|
| Type check | `npm run typecheck` | seconds | no |
| Unit tests | `npm test` | seconds | no |
| Integration tests | `npm run test:integration` | tens of seconds | yes (Docker image `ghcr.io/arbitro-io/arbitro-server:latest`) |

## Running integration tests

The integration suite spawns the broker in Docker, runs the client against it, and tears down. The Docker image MUST be pulled before the first run:

```bash
docker pull ghcr.io/arbitro-io/arbitro-server:latest
npm run test:integration
```

To pin a specific broker version (e.g. when debugging a regression):

```bash
ARBITRO_IMAGE=ghcr.io/arbitro-io/arbitro-server:0.1.0 npm run test:integration
```

## Pre-merge checklist

Run all of these before opening a PR or pushing to a branch you intend to merge:

- [ ] `npm run typecheck` passes
- [ ] `npm test` passes
- [ ] `npm run test:integration` passes against `:latest`
- [ ] No `any` in public API types
- [ ] No `JSON.parse`/`JSON.stringify` introduced in any hot path (subscribe callback, publish loop)
- [ ] `LazyMessage` fields are still getters, not eagerly decoded
- [ ] New codec implementations expose `.fields?: readonly string[]` if schema-based
- [ ] Hot-path audit from `performance.md` completed if any of `connection.ts`, `framer.ts`, `message.ts`, `subscription.ts` was touched

## What NOT to do in tests

- **Don't** assert exact `bigint` values in framing tests using `toEqual(1n)` without explicit `BigInt` setup — `BigInt(1) !== 1` and `1n !== 1`. Use `toBe(1n)` and import `bigint` literals.
- **Don't** spin up the broker inside the test process. Always use the Docker image — keeps tests reproducible across machines and matches CI.
- **Don't** use `process.stderr.write` for debug. `console.log` is fine in tests; the hot-path ban only applies to production source.

## Adding a new test

1. Unit tests live in `tests/*.test.ts`, integration tests in `tests/integration/*.test.ts`.
2. Vitest config splits the two via `vitest.config.ts` vs `vitest.integration.config.ts`.
3. Name files after the module under test (`tests/codec.test.ts` for `src/codec.ts`).
4. Integration tests SHOULD assert observable client behaviour, not internal state. If you find yourself reaching into internals, the test belongs in unit-tests.

## Coverage

There is no hard coverage gate today. The intent is: any new code touching the protocol, framing, or wire types ships with a test that exercises a round-trip. Pure-style refactors don't need new tests if existing tests already cover the path.
