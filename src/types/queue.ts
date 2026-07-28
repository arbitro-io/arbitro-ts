import type { Message } from '../message/message'
import type { DeliverPolicy } from './config'
import type { SubjectInflightLimit } from './config'

/**
 * Settings for `queueSubscribe` — the one-call durable work queue.
 *
 * Everything except `onMessage` is optional and already carries a default,
 * so joining a queue is a single object with a single handler in it.
 *
 * `ackPolicy` is deliberately absent: a work queue is always explicit-ack.
 * Allowing fire-and-forget here would build a queue that silently drops
 * jobs when a worker dies, which is the one thing a queue exists to prevent.
 */
export interface QueueOptions {
  /**
   * Handler for each job. Required — a queue with no handler would take
   * delivery of work and drop it.
   *
   * Ack when the job is done; the broker redelivers anything left unacked.
   */
  onMessage: (msg: Message) => void

  /**
   * Queue identity. Workers sharing it share one round-robin queue; a
   * different value is a separate, independent durable queue.
   *
   * Defaults to the stream name. This single value becomes both the durable
   * consumer name and the queue group — they always move together, so two
   * workers can never disagree on the consumer config.
   */
  group?: string

  /**
   * Subject filter for this subscription. Optional: without it the queue
   * sees every subject in the stream.
   *
   * The filter lives on the subscription rather than the consumer, so
   * workers on one queue may each narrow to a different slice without ever
   * colliding.
   */
  filter?: string

  /**
   * How long the broker waits for an ack before handing the job back to the
   * queue. Omit for the broker default.
   *
   * Raise it for handlers that legitimately run long — otherwise a slow
   * success is indistinguishable from a crashed worker and the job gets
   * handed to someone else while the first worker is still on it.
   */
  ackWaitMs?: number

  /** Cap on delivered-but-unacked messages across the queue. The
   *  backpressure valve; omit for unlimited. */
  maxInflight?: number

  /** Where a brand-new queue starts reading. Ignored on later joins — by
   *  then the durable cursor already exists and the queue resumes from it. */
  deliverPolicy?: DeliverPolicy

  /** Starting sequence for `DeliverPolicy.ByStartSeq`. `bigint` because the
   *  wire field is a u64. */
  startSeq?: bigint

  /** Per-subject in-flight caps. Each distinct subject keeps its own count. */
  maxSubjectInflights?: SubjectInflightLimit[]
}
