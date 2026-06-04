// Workflow wire frames — cold path, JSON body for CreateWorkflow;
// fixed binary layout for WorkflowStep (S→C), WorkflowResult (C→S),
// WorkflowError (S→C), CancelWorkflow, DeleteWorkflow, ListWorkflows.

import { HEADER_SIZE, Action } from '../proto/constants'
import { frame } from '../proto/frame'

// ── CreateWorkflow body (JSON) ────────────────────────────────────────────

export interface StepDef {
  readonly name: string
  /** Step timeout in ms (0 = none). */
  readonly timeout_ms?: number
  /** Max retries for this step (0 = none). */
  readonly max_retries?: number
}

export interface WorkflowConfig {
  /** Maximum concurrent instances (0 = unlimited). */
  readonly max_concurrent?: number
  /** JSON key for dedup (undefined = no dedup). */
  readonly dedup_key?: string
  /** Overall workflow timeout in ms (0 = none). */
  readonly timeout_ms?: number
}

export interface CreateWorkflowBody {
  readonly name: string
  readonly trigger: string
  readonly steps: readonly StepDef[]
  readonly config?: WorkflowConfig
}

export function packCreateWorkflow(seq: bigint, body: CreateWorkflowBody): Buffer {
  const json = Buffer.from(JSON.stringify(body), 'utf8')
  const buf = frame(Action.CreateWorkflow, seq, json.length)
  json.copy(buf, HEADER_SIZE)
  return buf
}

// ── DeleteWorkflow (body = name bytes) ────────────────────────────────────

export function packDeleteWorkflow(seq: bigint, name: Buffer): Buffer {
  const buf = frame(Action.DeleteWorkflow, seq, name.length)
  name.copy(buf, HEADER_SIZE)
  return buf
}

// ── ListWorkflows (no body) ───────────────────────────────────────────────

export function packListWorkflows(seq: bigint): Buffer {
  return frame(Action.ListWorkflows, seq, 0)
}

// ── CancelWorkflow (body = 4-byte instance_id) ───────────────────────────

export function packCancelWorkflow(seq: bigint, instanceId: number): Buffer {
  const buf = frame(Action.CancelWorkflow, seq, 4)
  buf.writeUInt32LE(instanceId, HEADER_SIZE)
  return buf
}

// ── ListInstances (body = name bytes) ─────────────────────────────────────

export function packListInstances(seq: bigint, name: Buffer): Buffer {
  const buf = frame(Action.ListInstances, seq, name.length)
  name.copy(buf, HEADER_SIZE)
  return buf
}

// ── WorkflowStep decode (S→C) ─────────────────────────────────────────────
// Body: [2 name_len LE][4 instance_id LE][2 step_index LE][name...][context...]

const STEP_FIXED = 2 + 4 + 2 // 8 bytes

export interface WorkflowStepView {
  readonly name: string
  readonly instanceId: number
  readonly stepIndex: number
  readonly context: Buffer
}

export function decodeWorkflowStep(body: Buffer): WorkflowStepView | undefined {
  if (body.length < STEP_FIXED) return undefined
  const nameLen = body.readUInt16LE(0)
  const instanceId = body.readUInt32LE(2)
  const stepIndex = body.readUInt16LE(6)
  if (body.length < STEP_FIXED + nameLen) return undefined
  const name = body.subarray(8, 8 + nameLen).toString()
  const context = body.subarray(8 + nameLen)
  return { name, instanceId, stepIndex, context }
}

// ── WorkflowResult encode (C→S) ───────────────────────────────────────────
// Body: [2 name_len LE][4 instance_id LE][1 status (0=ok, 1=error)][name...][context...]

const RESULT_FIXED = 2 + 4 + 1 // 7 bytes

export function packWorkflowResult(
  seq: bigint,
  name: Buffer,
  instanceId: number,
  ok: boolean,
  context: Buffer,
): Buffer {
  const bodyLen = RESULT_FIXED + name.length + context.length
  const buf = frame(Action.WorkflowResult, seq, bodyLen)
  let off = HEADER_SIZE
  buf.writeUInt16LE(name.length, off); off += 2
  buf.writeUInt32LE(instanceId, off); off += 4
  buf[off] = ok ? 0 : 1; off += 1
  name.copy(buf, off); off += name.length
  context.copy(buf, off)
  return buf
}

// ── WorkflowError decode (S→C) ────────────────────────────────────────────
// Body: [2 name_len LE][4 instance_id LE][name...][error_json...]

const ERROR_FIXED = 2 + 4 // 6 bytes

export interface WorkflowErrorView {
  readonly name: string
  readonly instanceId: number
  readonly errorJson: Buffer
}

export function decodeWorkflowError(body: Buffer): WorkflowErrorView | undefined {
  if (body.length < ERROR_FIXED) return undefined
  const nameLen = body.readUInt16LE(0)
  const instanceId = body.readUInt32LE(2)
  if (body.length < ERROR_FIXED + nameLen) return undefined
  const name = body.subarray(6, 6 + nameLen).toString()
  const errorJson = body.subarray(6 + nameLen)
  return { name, instanceId, errorJson }
}
