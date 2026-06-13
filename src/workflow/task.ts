// Task payload encoding/decoding for workflow step messages.
// Format: [id_len:2 LE][instance_id:id_len][step_index:2 LE][attempt:1][context...]

/** Minimum task payload: 2 (id_len) + 0 (empty id) + 2 (step) + 1 (attempt). */
export const MIN_TASK_PAYLOAD = 5

/** Bit flag set on stepIndex to mark compensation tasks. */
export const COMPENSATION_BIT = 0x8000

export interface DecodedTask {
  readonly instanceId: string
  readonly stepIndex: number
  readonly attempt: number
  readonly context: Buffer
}

export function encodeTask(
  instanceId: string, stepIndex: number, attempt: number, context: Buffer,
): Buffer {
  const idBytes = Buffer.from(instanceId, 'utf8')
  const buf = Buffer.allocUnsafe(2 + idBytes.length + 2 + 1 + context.length)
  buf.writeUInt16LE(idBytes.length, 0)
  idBytes.copy(buf, 2)
  const off = 2 + idBytes.length
  buf.writeUInt16LE(stepIndex, off)
  buf[off + 2] = attempt
  context.copy(buf, off + 3)
  return buf
}

export function decodeTask(payload: Buffer): DecodedTask | undefined {
  if (payload.length < MIN_TASK_PAYLOAD) return undefined
  const idLen = payload.readUInt16LE(0)
  const header = 2 + idLen + 2 + 1
  if (payload.length < header) return undefined
  const instanceId = payload.subarray(2, 2 + idLen).toString('utf8')
  const off = 2 + idLen
  return {
    instanceId,
    stepIndex: payload.readUInt16LE(off),
    attempt: payload[off + 2]!,
    context: payload.subarray(header),
  }
}

// ── Park / Remove encoding (state stream) ──────────────────────────────
// Format: [step_index:2LE][state_len:4LE][state bytes][context bytes]

export interface DecodedPark {
  readonly stepIndex: number
  readonly state: Buffer
  readonly context: Buffer
}

export function encodePark(stepIndex: number, state: Buffer, context: Buffer): Buffer {
  const buf = Buffer.allocUnsafe(2 + 4 + state.length + context.length)
  buf.writeUInt16LE(stepIndex, 0)
  buf.writeUInt32LE(state.length, 2)
  state.copy(buf, 6)
  context.copy(buf, 6 + state.length)
  return buf
}

export function decodePark(payload: Buffer): DecodedPark | undefined {
  if (payload.length < 6) return undefined
  const stepIndex = payload.readUInt16LE(0)
  const stateLen = payload.readUInt32LE(2)
  if (payload.length < 6 + stateLen) return undefined
  const state = payload.subarray(6, 6 + stateLen)
  const context = payload.subarray(6 + stateLen)
  return { stepIndex, state, context }
}
