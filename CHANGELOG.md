# Changelog

All notable changes to `@arbitro/client` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/) and the project uses SemVer with
the pre-1.0 interpretation described in `RELEASING.md`.

## [0.6.2] - 2026-07-18

Reliability and parity release. Brings the ack-reliability layer, request/reply,
and reconnect story to parity with the Rust reference client, and pairs with
`arbitro-server >= 0.6.2` (uses the `AckState` frames `0x0A01`–`0x0A04`).

### Added
- **`client.request()`** — correlated request/reply with timeout.
- **Ack reliability hot tier** — gated pending state, per-connection generation,
  a sweep for aged entries, and replay of unacked state on reconnect.
- **`AckState` wire frames** (`0x0A01`–`0x0A04`) codecs.
- **Heartbeat watchdog** for dead-connection detection.
- **`pauseConsumer` / `resumeConsumer`.**
- **`publishWait`** name-alias for the Rust `publish_wait` rename.
- Publish-path benchmarks: real `WAIT`, batch-wait, and pipelined-batch paths.

### Changed
- **Ack batching** now uses a microtask accumulator with a `BatchAck` fast path.
- **Reconnect** default is `maxAttempts = Infinity`; TLS connections reconnect.

### Fixed
- Correctness pass: `FanoutBatch` dispatch, upsert error surfacing,
  `DeliverPolicy` handling, `msgId` propagation, input validation, and metrics.

[0.6.2]: https://github.com/arbitro-io/arbitro-ts/compare/v0.6.1...v0.6.2
