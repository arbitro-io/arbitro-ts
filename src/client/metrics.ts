// Client-side counters. Each field is an integer accumulator updated on
// the hot paths (publish, deliver, ack, nack). Reading is a cheap
// `snapshot()` — useful for periodic logging / Prometheus export.

export interface ClientMetricsSnapshot {
  /** Calls to `publish` / `publishAck` / `publishBatch` entries. */
  publishesSent:        number
  /** Total entries inside `publishBatch` calls (summed across batches). */
  publishBatchEntries:  number
  /** `Deliver` frames received from the broker (one per message). */
  deliveriesReceived:   number
  /** Currently-open subscriptions (gauge — inc at subscribe, dec at close). */
  activeSubscriptions:  number
  /** `Ack` frames sent to the broker. */
  acksSent:             number
  /** `Nack` frames sent to the broker. */
  nacksSent:            number
  /** Successful reconnections after a session drop. */
  reconnects:           number
  /** Outstanding pending request-reply slots (gauge). */
  pendingReplies:       number
}

/**
 * Live atomic-ish counters. JavaScript is single-threaded per worker so
 * plain `++` is sufficient — no `Atomics` needed.
 *
 * Held on `ArbitroClient` and shared with `Connection` so the demux loop
 * can bump `deliveriesReceived` directly. Public `snapshot()` returns a
 * frozen copy for logging or external scraping.
 */
export class ClientMetrics {
  publishesSent        = 0
  publishBatchEntries  = 0
  deliveriesReceived   = 0
  activeSubscriptions  = 0
  acksSent             = 0
  nacksSent            = 0
  reconnects           = 0
  pendingReplies       = 0

  snapshot(): ClientMetricsSnapshot {
    return {
      publishesSent:       this.publishesSent,
      publishBatchEntries: this.publishBatchEntries,
      deliveriesReceived:  this.deliveriesReceived,
      activeSubscriptions: this.activeSubscriptions,
      acksSent:            this.acksSent,
      nacksSent:           this.nacksSent,
      reconnects:          this.reconnects,
      pendingReplies:      this.pendingReplies,
    }
  }
}
