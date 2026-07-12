// Values must match Rust enum variant names (rmp_serde serializes as PascalCase).
// The broker's wire encoding only accepts All=0, New=1, ByStartSeq=2 — there
// is no `Last` or `ByStartTime` variant server-side, so those are not
// exposed here (see arbitro-client-tokio/src/consumer_builder.rs DeliverPolicy).
export enum DeliverPolicy {
  All   = 'All',
  New   = 'New',
  BySeq = 'ByStartSeq',
}

export enum JournalType {
  Memory   = 'Memory',
  Tolerant = 'Tolerant',
  Strict   = 'Strict',
}

export enum AckPolicy {
  Explicit = 'explicit',
  None     = 'none',
}

export interface FlushConfig {
  intervalMs?:  number   // default: 10
  maxMessages?: number   // default: 512
  maxBytes?:    number   // default: 65_536
}

export type JournalConfig =
  | { type: JournalType.Memory }
  | { type: JournalType.Tolerant }
  | { type: JournalType.Strict; flush?: FlushConfig }

export interface StreamConfig {
  subjectFilter:  string
  journal?:       JournalConfig
  maxMsgs?:       number
  maxBytes?:      number
  maxAgeMs?:      number
  /**
   * Per-stream broker-side dedup window in milliseconds.
   *
   * - `0` or undefined (default): no dedup. Every publish is stored;
   *   `msgId` is ignored.
   * - `>0`: any publish that carries a `msgId` matching one the
   *   broker has stored for THIS stream within the last
   *   `idempotencyWindowMs` is rejected with the `IdempotencyDuplicate`
   *   wire error. Useful for safe retries — the first publish wins.
   *
   * The broker clamps requested windows above 5 minutes (300 000 ms)
   * down to that ceiling, matching JetStream behaviour.
   */
  idempotencyWindowMs?: number
}

export interface DeleteStreamOpts {
  deleteData?: boolean
}

export interface StreamInfo {
  name: string
  /**
   * `config` is NOT parsed from a broker-side info body — the broker's
   * `GetStream`/`ListStreams` replies only carry the stream's `wire_id`
   * (and, for `ListStreams`, the name). There is no wire message that
   * returns the full `StreamConfig` back from the server today, so this
   * field is always a placeholder (`{ subjectFilter: '' }`). Do not rely
   * on it reflecting the stream's real configuration.
   */
  config: StreamConfig
  /**
   * Server-assigned wire id (`wire_hash_32`) for this stream. This is
   * NOT a sequence number — the field used to be misnamed `lastSeq`.
   */
  wireId: bigint
  /** @deprecated Misnamed alias for {@link wireId}, kept for source
   * compatibility. Prefer `wireId`. Will be removed in a future major. */
  lastSeq: number
}

/**
 * Per-subject inflight cap. Each entry caps the number of in-flight
 * (delivered, unacked) messages on subjects matching `pattern`. Patterns
 * may use NATS-style wildcards (`*`, `>`).
 *
 * Only enforced when the owning consumer's `ackPolicy` is `Explicit`;
 * silently dropped server-side for fire-and-forget consumers (because
 * fire-and-forget bindings skip inflight tracking entirely).
 */
export interface SubjectInflightLimit {
  pattern: string
  limit:   number
}

export interface ConsumerConfig {
  name?:                string   // defaults to stream name when created via stream.consumer()
  /** Shared consumer group name for round-robin delivery. Defaults to `name`. */
  group?:               string
  filter?:              string   // defaults to "${streamName}.>" when created via stream.consumer()
  fanout?:              boolean   // broadcast — every subscriber receives every message
  /** Consumer-side ACK policy. None = fire-and-forget delivery, Explicit = consumer must ACK. */
  ackPolicy?:           AckPolicy
  deliverPolicy?:       DeliverPolicy
  startSeq?:            bigint
  startTime?:           bigint
  maxAckPending?:       number
  ackWaitMs?:           number
  maxDeliver?:          number
  removeUnusedAfterMs?: number
  /**
   * Per-subject max inflight (list of pattern → limit pairs).
   * Only effective with `ackPolicy: Explicit`.
   */
  maxSubjectInflights?: SubjectInflightLimit[]
}

export interface ConsumerInfo {
  /**
   * `group`/`stream`/`config` are NOT parsed from a real info body — the
   * broker's `ListConsumers` reply only carries numeric ids
   * (`consumer_id`, `stream_id`, `queue_id`, `paused`), no names and no
   * `ConsumerConfig`. These three fields are placeholders derived from
   * `wireId` for source compatibility; do not treat them as the
   * consumer's actual group/stream name or config.
   */
  group: string
  stream: string
  config: ConsumerConfig
  /** Server-assigned consumer id (`wire_id`) from the `ListConsumers` reply. */
  wireId: bigint
  /** Server-assigned stream id this consumer is bound to. */
  streamWireId: bigint
  /** Whether delivery to this consumer is currently paused. */
  paused: boolean
}

export interface SubscribeOptions {
  fetchTimeoutMs?: number
}

export interface ReconnectConfig {
  enabled?:     boolean
  maxAttempts?: number
  intervalMs?:  number
  jitter?:      boolean
}

export interface TlsConfig {
  enabled?: boolean
  ca?:      Buffer | string
  cert?:    Buffer | string
  key?:     Buffer | string
}

export interface ClientConfig {
  servers:    string[]
  prefix?:    string
  timeout?:   number
  reconnect?: ReconnectConfig
  tls?:       TlsConfig
  // Pino-compatible logger. Default: silent.
  logger?:    import('../common/logger').Logger
}
