import { Packr } from 'msgpackr'
import {
  AckPolicy, JournalType, type StreamConfig, type ConsumerConfig, type DeleteStreamOpts,
} from '../types/config'

const packr = new Packr({ structuredClone: false, useRecords: false })

export function serializeStreamConfig(cfg: StreamConfig): Buffer {
  const wire: Record<string, unknown> = { subject_filter: cfg.subjectFilter }
  if (cfg.journal) {
    wire['journal_type'] = cfg.journal.type
    if (cfg.journal.type === JournalType.Strict && cfg.journal.flush) {
      wire['flush_interval_ms']  = cfg.journal.flush.intervalMs
      wire['flush_max_messages'] = cfg.journal.flush.maxMessages
      wire['flush_max_bytes']    = cfg.journal.flush.maxBytes
    }
  }
  if (cfg.maxMsgs  !== undefined) wire['max_msgs']   = cfg.maxMsgs
  if (cfg.maxBytes !== undefined) wire['max_bytes']  = cfg.maxBytes
  if (cfg.maxAgeMs !== undefined) wire['max_age_ns'] = BigInt(cfg.maxAgeMs) * 1_000_000n
  return Buffer.from(packr.pack(wire))
}

export function serializeConsumerConfig(cfg: ConsumerConfig): Buffer {
  const wire: Record<string, unknown> = { group: cfg.name }
  if (cfg.filter) wire['filter'] = cfg.filter
  if (cfg.fanout)                              wire['deliver_mode']           = 'Fanout'
  if (cfg.ackPolicy === AckPolicy.None)        wire['no_ack']                 = true
  if (cfg.deliverPolicy       !== undefined)   wire['deliver_policy']         = cfg.deliverPolicy
  if (cfg.startSeq            !== undefined)   wire['start_seq']              = cfg.startSeq
  if (cfg.startTime           !== undefined)   wire['start_time']             = cfg.startTime
  if (cfg.maxAckPending       !== undefined)   wire['max_ack_pending']        = cfg.maxAckPending
  if (cfg.ackWaitMs           !== undefined)   wire['ack_wait_ms']            = cfg.ackWaitMs
  if (cfg.maxDeliver          !== undefined)   wire['max_deliver']            = cfg.maxDeliver
  if (cfg.removeUnusedAfterMs !== undefined)   wire['remove_unused_after_ms'] = cfg.removeUnusedAfterMs
  if (cfg.creditRules?.length)                 wire['credit_rules']           = cfg.creditRules
  return Buffer.from(packr.pack(wire))
}

export function serializeDeleteStreamOpts(opts?: DeleteStreamOpts): Buffer {
  if (!opts) return Buffer.alloc(0)
  const wire: Record<string, unknown> = {}
  if (opts.deleteData !== undefined) wire['delete_data'] = opts.deleteData
  return Buffer.from(packr.pack(wire))
}
