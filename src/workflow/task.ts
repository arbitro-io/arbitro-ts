// Task payload encoding/decoding for workflow step messages.
// Format: [instance_id:4 LE][step_index:2 LE][attempt:1][context...]

/** Header size: 4 (instanceId) + 2 (stepIndex) + 1 (attempt) = 7 bytes. */
export const TASK_HEADER = 7

/** Bit flag set on stepIndex to mark compensation tasks. */
export const COMPENSATION_BIT = 0x8000

export interface DecodedTask {
  readonly instanceId: number
  readonly stepIndex: number
  readonly attempt: number
  readonly context: Buffer
}

export function encodeTask(
  instanceId: number, stepIndex: number, attempt: number, context: Buffer,
): Buffer {
  const buf = Buffer.allocUnsafe(TASK_HEADER + context.length)
  buf.writeUInt32LE(instanceId, 0)
  buf.writeUInt16LE(stepIndex, 4)
  buf[6] = attempt
  context.copy(buf, TASK_HEADER)
  return buf
}

export function decodeTask(payload: Buffer): DecodedTask | undefined {
  if (payload.length < TASK_HEADER) return undefined
  return {
    instanceId: payload.readUInt32LE(0),
    stepIndex: payload.readUInt16LE(4),
    attempt: payload[6]!,
    context: payload.subarray(TASK_HEADER),
  }
}
