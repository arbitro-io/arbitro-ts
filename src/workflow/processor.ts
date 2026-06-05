// Message processing loop for workflow steps, compensation, and DLQ.

import type { ArbitroClient } from '../client/client'
import type { Message } from '../message/message'
import { encodeTask, decodeTask, COMPENSATION_BIT } from './task'
import type { StepHandler, StepResult } from './workflow'

interface StepDef {
  readonly handler: StepHandler
  readonly compensation: StepHandler | undefined
}

export interface ProcessorConfig {
  readonly client: ArbitroClient
  readonly name: string
  readonly taskStreamName: string
  readonly dlqStreamName: string
  readonly steps: readonly StepDef[]
  readonly maxContextSize: number
  readonly maxRetries: number
}

export async function processMessage(cfg: ProcessorConfig, msg: Message): Promise<void> {
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
  task: { instanceId: number; stepIndex: number; attempt: number; context: Buffer },
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

// ── Normal step ───────────────────────────────────────────────────────

async function runStep(
  cfg: ProcessorConfig, msg: Message,
  task: { instanceId: number; stepIndex: number; attempt: number; context: Buffer },
): Promise<void> {
  const handler = cfg.steps[task.stepIndex]!.handler
  try {
    const result = await handler({
      name: cfg.name, instanceId: task.instanceId,
      stepIndex: task.stepIndex, attempt: task.attempt, context: task.context,
    })
    if (result.context.length > cfg.maxContextSize) { msg.nack(); return }
    await advance(cfg, msg, task, result)
  } catch (err) {
    await onFailure(cfg, msg, task, err)
  }
}

async function advance(
  cfg: ProcessorConfig, msg: Message,
  task: { instanceId: number; stepIndex: number }, result: StepResult,
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
  task: { instanceId: number; stepIndex: number; attempt: number; context: Buffer },
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
  task: { instanceId: number; stepIndex: number; attempt: number; context: Buffer },
  err: unknown,
): Promise<void> {
  const dlqSubject = `_wf.${cfg.name}.dlq.${task.stepIndex}`
  const errBytes = Buffer.from(String(err))
  const buf = Buffer.allocUnsafe(7 + 4 + errBytes.length + task.context.length)
  buf.writeUInt32LE(task.instanceId, 0)
  buf.writeUInt16LE(task.stepIndex, 4)
  buf[6] = task.attempt
  buf.writeUInt32LE(errBytes.length, 7)
  errBytes.copy(buf, 11)
  task.context.copy(buf, 11 + errBytes.length)
  const msgId = `wf:${task.instanceId}:dlq:${task.stepIndex}`
  await cfg.client.publish(cfg.dlqStreamName, dlqSubject, buf, { msgId }).catch(() => {})
}

async function publishCompensations(
  cfg: ProcessorConfig,
  task: { instanceId: number; stepIndex: number; context: Buffer },
): Promise<void> {
  for (let i = task.stepIndex - 1; i >= 0; i--) {
    const compStep = COMPENSATION_BIT | i
    const subject = `_wf.${cfg.name}.compensate.${i}`
    const buf = encodeTask(task.instanceId, compStep, 0, task.context)
    const msgId = `wf:${task.instanceId}:comp:${i}`
    await cfg.client.publish(cfg.taskStreamName, subject, buf, { msgId }).catch(() => {})
  }
}
