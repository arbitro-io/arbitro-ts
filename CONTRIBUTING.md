# Contributing to `@arbitro/client`

Thanks for considering a contribution. This package is the official TypeScript client for the [Arbitro message broker](https://github.com/arbitro-io/arbitro). Bug reports, feature requests, and pull requests are welcome.

## Code of conduct

Be respectful. Critique code, not people. Disagreements about technical direction are resolved by referencing the rules in `.agent/rules/`; if no rule applies, the discussion is open.

## Dev setup

```bash
git clone https://github.com/arbitro-io/arbitro-ts.git
cd arbitro-ts
npm install
```

You will need:

- **Node.js ≥ 20** — see `engines.node` in `package.json`.
- **Docker** — to pull the broker image for integration tests.

To pull the broker image once:

```bash
docker pull ghcr.io/arbitro-io/arbitro-server:latest
```

## Run the test suites

```bash
npm run typecheck         # type-only check, no emit
npm test                  # unit tests (vitest)
npm run test:integration  # spawns the broker in Docker
```

Pinning a specific broker version for the integration suite:

```bash
ARBITRO_IMAGE=ghcr.io/arbitro-io/arbitro-server:0.1.0 npm run test:integration
```

See `.agent/rules/testing.md` for the complete pre-merge checklist.

## Branch workflow

- `master` is the default branch. All changes land via pull request.
- Branch names: `feat/<topic>`, `fix/<topic>`, `chore/<topic>`, `docs/<topic>`, `refactor/<topic>`.
- Rebase against `master` before opening the PR. Linear history is preferred — squash on merge is fine.
- Keep PRs small and topical. One feature or fix per PR. Mixing in unrelated refactors makes review harder.

## Commit messages

We follow a lightweight [Conventional Commits](https://www.conventionalcommits.org/) convention:

```
<type>(<scope>?): <short summary>

<optional body — what changed and why, not how>

<optional footer — refs, breaking changes>
```

Types in use:

- `feat` — new user-facing functionality
- `fix` — bug fix
- `perf` — perf improvement without changing behaviour
- `refactor` — internal restructure, no functional change
- `docs` — documentation only
- `chore` — build, deps, CI, tooling
- `test` — adding or fixing tests
- `ci` — CI workflow changes

Example:

```
feat(subscription): expose paused state on Consumer

Adds `consumer.isPaused` getter backed by ConsumerStats (0x0505).
Useful for dashboards that want to surface backpressure without
polling the broker.
```

Breaking changes go in the footer:

```
BREAKING CHANGE: `subscribe()` now returns `Subscription` instead of `void`.
Callers must store the handle to call `.unsubscribe()`.
```

## Coding rules

All rules live under `.agent/rules/*.md`. Before opening a PR that touches code, re-read the rules for the area you are changing:

| Area | File |
|---|---|
| Hot path (`onFrame`, framer, publish, deliver) | `.agent/rules/performance.md` |
| Adding network calls / new primitives | `.agent/rules/architecture.md` |
| Codecs / `LazyMessage` | `.agent/rules/encoding.md` |
| Wire format / action codes | `.agent/rules/wire-protocol.md` |
| Style / naming / file size limits | `.agent/rules/code-style.md` |
| Tests | `.agent/rules/testing.md` |

The `CLAUDE.md` at the repo root is the index.

## Pull request review

- Open the PR against `master`.
- The CI pipeline runs typecheck, unit tests, and (when present) integration tests.
- A reviewer (maintainer) will request changes or approve. For now there is one maintainer; PRs from outside contributors require approval before CI can run on the PR for the first time (GitHub default for first-time contributors).
- After approval and green CI, the maintainer merges via squash or rebase to keep the master history linear.

## Reporting bugs

Open an issue at [github.com/arbitro-io/arbitro-ts/issues](https://github.com/arbitro-io/arbitro-ts/issues). Include:

- `@arbitro/client` version (from `package.json` of your project)
- Broker version (`ghcr.io/arbitro-io/arbitro-server:<tag>` you are running)
- Node version (`node --version`)
- Minimal reproduction (or describe the path: which methods called in which order)
- Observed vs expected behaviour
- Any stack trace

## Proposing a feature

Open an issue first with the motivation. Discussion in the issue is cheap; rewriting a rejected PR is expensive. Once the direction is agreed, open the PR.

## License

By contributing, you agree your contribution is licensed under the [MIT License](./LICENSE) and that you have the right to license it that way.
