// Message processing loop for workflow steps, compensation, and DLQ.

import type { ArbitroClient } from '../client/client'
import type { Message } from '../message/message'
import { encodeTask, decodeTask, COMPENSATION_BIT } from './task'
import type { StepResult, StepKind, SuspendedEntry, ResumeContext, TimeoutContext } from './workflow'

interface StepDef {
  readonly kind: StepKind
  readonly compensation: ((ctx: import('./workflow').StepContext) => Promise<StepResult>) | undefined
}

export interface ProcessorConfig {
  readonly client: ArbitroClient
  readonly name: string
  readonly taskStreamName: string
  readonly dlqStreamName: string
  readonly steps: readonly StepDef[]
  readonly maxContextSize: number
  readonly maxRetries: number
  readonly suspended: Map<string, SuspendedEntry>
}

export async function processMessage(cfg: ProcessorConfig, msg: Message): Promise<void> {
  const subject = msg.subject().toString('utf8')
  const resumePrefix = `_wf.${cfg.name}.resume.`
  const timeoutPrefix = `_wf.${cfg.name}.timeout.`

  // ── Resume event ──
  if (subject.startsWith(resumePrefix)) {
    const instanceId = subject.slice(resumePrefix.length)
    const entry = cfg.suspended.get(instanceId)
    if (entry) {
      cfg.suspended.delete(instanceId)
      const step = cfg.steps[entry.stepIndex]
      if (step && step.kind.type === 'suspend') {
        const rctx: ResumeContext = {
          name: cfg.name, instanceId, stepIndex: entry.stepIndex,
          state: entry.state, event: msg.data(),
        }
        try {
          const result = await step.kind.onResume(rctx)
          await advance(cfg, msg, { instanceId, stepIndex: entry.stepIndex }, result)
        } catch { msg.nack() }
      } else { msg.ack() }
    } else { msg.ack() }
    return
  }

  // ── Timeout event ──
  if (subject.startsWith(timeoutPrefix)) {
    const instanceId = subject.slice(timeoutPrefix.length)
    const entry = cfg.suspended.get(instanceId)
    if (entry) {
      cfg.suspended.delete(instanceId)
      const step = cfg.steps[entry.stepIndex]
      if (step && step.kind.type === 'suspend' && step.kind.onTimeout) {
        const tctx: TimeoutContext = {
          name: cfg.name, instanceId, stepIndex: entry.stepIndex,
          state: entry.state,
        }
        try {
          const result = await step.kind.onTimeout(tctx)
          await advance(cfg, msg, { instanceId, stepIndex: entry.stepIndex }, result)
        } catch { msg.nack() }
      } else { msg.ack() }
    } else { msg.ack() }
    return
  }

  // ── Cancel event ──
  const cancelPrefix = `_wf.${cfg.name}.cancel.`
  if (subject.startsWith(cancelPrefix)) {
    const instanceId = subject.slice(cancelPrefix.length)
    cfg.suspended.delete(instanceId)
    msg.ack()
    return
  }

  // ── Normal task ──
  const task = decodeTask(msg.data())
  if (!task) { msg.ack(); return }
  if (task.context.length > cfg.maxContextSize) { msg.ack(); return }

  const isCompensation = (task.stepIndex & COMPENSATION_BIT) !== 0
  if (isCompensation) {
    await runCompensation(cfg, msg, task)
    return
  }
  if (task.stepIndex >= cfg.steps.length) { msg.ack(); return }
  await runStep(cfg, msg, task)
}

// ── Compensation ──────────────────────────────────────────────────────

async function runCompensation(
  cfg: ProcessorConfig, msg: Message,
  task: { instanceId: string; stepIndex: number; attempt: number; context: Buffer },
): Promise<void> {
  const idx = task.stepIndex & ~COMPENSATION_BIT
  const comp = cfg.steps[idx]?.compensation
  if (comp) {
    try {
      await comp({ name: cfg.name, instanceId: task.instanceId, stepIndex: idx, attempt: task.attempt, context: task.context })
    } catch { /* best-effort */ }
  }
  msg.ack()
}

// ── Step (Normal or Suspend) ─────────────────────────────────────────

async function runStep(
  cfg: ProcessorConfig, msg: Message,
  task: { instanceId: string; stepIndex: number; attempt: number; context: Buffer },
): Promise<void> {
  const step = cfg.steps[task.stepIndex]!
  const ctx = {
    name: cfg.name, instanceId: task.instanceId,
    stepIndex: task.stepIndex, attempt: task.attempt, context: task.context,
  }

  try {
    if (step.kind.type === 'normal') {
      const result = await step.kind.handler(ctx)
      if (result.context.length > cfg.maxContextSize) { msg.nack(); return }
      await advance(cfg, msg, task, result)
    } else {
      // Suspend step — run handler returns StepOutcome
      const outcome = await step.kind.run(ctx)
      if (outcome.kind === 'done') {
        if (outcome.result.context.length > cfg.maxContextSize) { msg.nack(); return }
        await advance(cfg, msg, task, outcome.result)
      } else {
        // Suspend: persist in registry and release worker
        cfg.suspended.set(task.instanceId, {
          stepIndex: task.stepIndex,
          state: outcome.state,
          context: task.context,
        })

        // Schedule timeout via local timer + fire-and-forget publish.
        // We avoid publishDelayed to match the Rust implementation
        // (in-memory registry = local timer is fine).
        const effectiveTimeout = outcome.timeoutMs > 0
          ? outcome.timeoutMs
          : step.kind.timeoutMs
        if (effectiveTimeout > 0) {
          const timeoutSubject = `_wf.${cfg.name}.timeout.${task.instanceId}`
          const timeoutMsgId = `wf:${task.instanceId}:timeout:${task.stepIndex}`
          setTimeout(() => {
            cfg.client.publish(
              cfg.taskStreamName, timeoutSubject,
              Buffer.alloc(0), { msgId: timeoutMsgId },
            ).catch(() => {})
          }, effectiveTimeout)
        }

        msg.ack()
      }
    }
  } catch (err) {
    await onFailure(cfg, msg, task, err)
  }
}

async function advance(
  cfg: ProcessorConfig, msg: Message,
  task: { instanceId: string; stepIndex: number }, result: StepResult,
): Promise<void> {
  const nextStep = task.stepIndex + 1
  if (nextStep < cfg.steps.length) {
    const msgId = `wf:${task.instanceId}:${nextStep}:0`
    const subject = `_wf.${cfg.name}.step.${nextStep}`
    const buf = encodeTask(task.instanceId, nextStep, 0, result.context)
    await cfg.client.publish(cfg.taskStreamName, subject, buf, { msgId })
  }
  msg.ack()
}

// ── Failure → DLQ + compensation chain ────────────────────────────────

async function onFailure(
  cfg: ProcessorConfig, msg: Message,
  task: { instanceId: string; stepIndex: number; attempt: number; context: Buffer },
  err: unknown,
): Promise<void> {
  if (task.attempt >= cfg.maxRetries) {
    await publishDlq(cfg, task, err)
    await publishCompensations(cfg, task)
    msg.ack()
  } else {
    msg.nack()
  }
}

async function publishDlq(
  cfg: ProcessorConfig,
  task: { instanceId: string; stepIndex: number; attempt: number; context: Buffer },
  err: unknown,
): Promise<void> {
  const dlqSubject = `_wf.${cfg.name}.dlq.${task.stepIndex}`
  const errBytes = Buffer.from(String(err))
  const idBytes = Buffer.from(task.instanceId, 'utf8')
  // DLQ format: [id_len:2 LE][instance_id:id_len][step_index:2 LE][attempt:1][err_len:4 LE][err][context]
  const buf = Buffer.allocUnsafe(2 + idBytes.length + 2 + 1 + 4 + errBytes.length + task.context.length)
  let off = 0
  buf.writeUInt16LE(idBytes.length, off); off += 2
  idBytes.copy(buf, off); off += idBytes.length
  buf.writeUInt16LE(task.stepIndex, off); off += 2
  buf[off] = task.attempt; off += 1
  buf.writeUInt32LE(errBytes.length, off); off += 4
  errBytes.copy(buf, off); off += errBytes.length
  task.context.copy(buf, off)
  const msgId = `wf:${task.instanceId}:dlq:${task.stepIndex}`
  await cfg.client.publish(cfg.dlqStreamName, dlqSubject, buf, { msgId }).catch(() => {})
}

async function publishCompensations(
  cfg: ProcessorConfig,
  task: { instanceId: string; stepIndex: number; context: Buffer },
): Promise<void> {
  for (let i = task.stepIndex - 1; i >= 0; i--) {
    const compStep = COMPENSATION_BIT | i
    const subject = `_wf.${cfg.name}.compensate.${i}`
    const buf = encodeTask(task.instanceId, compStep, 0, task.context)
    const msgId = `wf:${task.instanceId}:comp:${i}`
    await cfg.client.publish(cfg.taskStreamName, subject, buf, { msgId }).catch(() => {})
  }
}
