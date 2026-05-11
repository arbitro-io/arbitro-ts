// Values must match Rust enum variant names (rmp_serde serializes as PascalCase)
export enum DeliverPolicy {
  All    = 'All',
  Last   = 'Last',
  New    = 'New',
  BySeq  = 'ByStartSeq',
  ByTime = 'ByStartTime',
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
}

export interface DeleteStreamOpts {
  deleteData?: boolean
}

export interface StreamInfo {
  name: string
  config: StreamConfig
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
  group: string
  stream: string
  config: ConsumerConfig
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
