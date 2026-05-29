// Cron wire frames — cold path, JSON-encoded bodies for create/delete/list;
// fixed binary layout for CronFire (S→C) and CronAck (C→S).

import { HEADER_SIZE, Action } from '../proto/constants'
import { frame } from '../proto/frame'

// ── CreateCron body (JSON) ─────────────────────────────────────────────────

export interface CreateCronBody {
  readonly name: string
  readonly every: string
  readonly tz?: string | undefined
  readonly timeout_ms: number
  readonly overlap: boolean
}

export function packCreateCron(seq: bigint, body: CreateCronBody): Buffer {
  const json = Buffer.from(JSON.stringify(body), 'utf8')
  const buf = frame(Action.CreateCron, seq, json.length)
  json.copy(buf, HEADER_SIZE)
  return buf
}

// ── DeleteCron (body = name bytes) ─────────────────────────────────────────

export function packDeleteCron(seq: bigint, name: Buffer): Buffer {
  const buf = frame(Action.DeleteCron, seq, name.length)
  name.copy(buf, HEADER_SIZE)
  return buf
}

// ── ListCrons (no body) ────────────────────────────────────────────────────

export function packListCrons(seq: bigint): Buffer {
  return frame(Action.ListCrons, seq, 0)
}

// ── CronFire decode (S→C) ──────────────────────────────────────────────────
// Body: [2 name_len LE][8 fire_time_ms LE][8 fire_count LE][name bytes]

const CRON_FIRE_FIXED = 2 + 8 + 8 // 18 bytes before name

export interface CronFireView {
  readonly name: string
  readonly fireTimeMs: bigint
  readonly fireCount: bigint
}

export function decodeCronFire(body: Buffer): CronFireView | undefined {
  if (body.length < CRON_FIRE_FIXED) return undefined
  const nameLen = body.readUInt16LE(0)
  if (body.length < CRON_FIRE_FIXED + nameLen) return undefined
  const fireTimeMs = body.readBigUInt64LE(2)
  const fireCount = body.readBigUInt64LE(10)
  const name = body.subarray(18, 18 + nameLen).toString()
  return { name, fireTimeMs, fireCount }
}

// ── CronAck encode (C→S) ───────────────────────────────────────────────────
// Body: [2 name_len LE][1 status (0=ok, 1=error)][name bytes]

export function packCronAck(
  seq: bigint, name: Buffer, ok: boolean,
): Buffer {
  const bodyLen = 3 + name.length
  const buf = frame(Action.CronAck, seq, bodyLen)
  buf.writeUInt16LE(name.length, HEADER_SIZE)
  buf[HEADER_SIZE + 2] = ok ? 0 : 1
  name.copy(buf, HEADER_SIZE + 3)
  return buf
}
