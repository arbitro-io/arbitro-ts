# Changelog

All notable changes to `@arbitro/client` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/) and the project uses SemVer with
the pre-1.0 interpretation described in `RELEASING.md`.

## [0.7.0] - 2026-08-08

Carries a breaking API change, so this is a minor bump under the pre-1.0 policy
in `RELEASING.md`. Requires `arbitro-server >= 0.7.0` — `nackDelay` is only
accurate against a broker with the hierarchical timing wheel.

### Breaking
- **`client.request()` removed.** Reply is now confined to the service handler.
  A caller that wants a response uses `client.service()` and replies from
  inside the handler, which is what every sibling client does. The standalone
  request path allowed a reply to be routed to a sibling worker instance
  instead of the caller, and no amount of correlation on the client could fix
  that without the service's dedicated reply consumer.

### Added
- **`queueSubscribe()`** — a durable work queue in one call: creates the stream
  binding, the queue-group consumer and the subscription together, so the
  three-step setup that every worker repeated is no longer the caller's
  problem. Verified against a live broker, not just unit-mocked.
- **`deliveriesDropped` metric** — deliveries the broker sent for a consumer
  this client has no route for. A few are normal right after `close()`; a
  number that keeps climbing is message loss that was previously invisible.

### Fixed
- **A backlog delivery could be silently dropped.** Subscribing to a stream
  that already held messages lost roughly 1 delivery in 10 locally, and up to
  4 in 10 against a containerised broker. The broker serves the backlog the
  moment it processes Subscribe — frequently in the same TCP segment as its own
  RepOk — and the framer dispatches that synchronously while the caller's
  `await` continuation is still a queued microtask. The callback was installed
  *after* that round-trip, so `deliver` found none and parked the message in
  the pull-mode buffer, which push mode never reads again. No log, no metric,
  nothing. The callback is now attached before the round-trip and `onMessage`
  drains anything already parked. Measured: 50/50 and 30/30 rounds after,
  against 4/5 and 0-3/5 before.
- **Subscribe registered its route after the reply, not before.** Now mirrors
  step 1 of the Rust client's `subscribe_async`; a failed or timed-out
  subscribe removes what it registered.
- **Batch delivery for an unrouted consumer vanished silently.** It hit a bare
  `if (handler)` with no `else` — the single-delivery path at least warned.
  Both now warn and count.
- **Workflow retries never advanced the attempt counter.** A step below
  `maxRetries` nacked, so the broker redelivered the same bytes and `attempt`
  never moved. The retry now republishes with `attempt + 1` (the msg_id's last
  field carries it, so the bump clears the idempotency window) and acks only
  once that publish is queued. Port of the equivalent Rust fix.
- **`client.workflow()` restored** and workflow stream names aligned with Rust.
- **Reply-mailbox instance id is now unique across processes**, so two workers
  started in the same millisecond no longer share a mailbox.

### Changed
- Workflow worker consumers set `DeliverPolicy.All` explicitly instead of
  relying on the default, matching the Rust client.
- Tests no longer hardcode sequence numbers. Sequences are assigned by the
  shard store, which holds every stream mapped to that shard — "the second
  message I published" is only seq 2 on a broker that has served nothing else.
  Tests now read the real sequence off each delivery.
- Workflow tests wait on the condition instead of sleeping past the vitest
  deadline, which is what made three of them look broken when they were not.

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

[0.7.0]: https://github.com/arbitro-io/arbitro-ts/compare/v0.6.2...v0.7.0
[0.6.2]: https://github.com/arbitro-io/arbitro-ts/compare/v0.6.1...v0.6.2
