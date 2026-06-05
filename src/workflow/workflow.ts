// Workflow client-side module — mirrors the Rust implementation.
// Creates internal _wf.{name}.tasks stream, consumer group with
// ack_wait_ms, publishes step tasks with idempotent msg_id.

import type { ArbitroClient } from '../client/client'
import type { Message } from '../message/message'
import { AckPolicy } from '../types/config'

// ── Types ─────────────────────────────────────────────────────────────────

export interface StepContext {
  readonly name: string
  readonly instanceId: number
  readonly stepIndex: number
  readonly attempt: number
  readonly context: Buffer
}

export interface StepResult {
  readonly context: Buffer
}

export type StepHandler = (ctx: StepContext) => Promise<StepResult>

interface StepDef {
  readonly name: string
  readonly handler: StepHandler
}

// ── Task payload encoding ─────────────────────────────────────────────────

const TASK_HEADER = 7

function encodeTask(instanceId: number, stepIndex: number, attempt: number, context: Buffer): Buffer {
  const buf = Buffer.allocUnsafe(TASK_HEADER + context.length)
  buf.writeUInt32LE(instanceId, 0)
  buf.writeUInt16LE(stepIndex, 4)
  buf[6] = attempt
  context.copy(buf, TASK_HEADER)
  return buf
}

function decodeTask(payload: Buffer): { instanceId: number; stepIndex: number; attempt: number; context: Buffer } | undefined {
  if (payload.length < TASK_HEADER) return undefined
  return {
    instanceId: payload.readUInt32LE(0),
    stepIndex: payload.readUInt16LE(4),
    attempt: payload[6]!,
    context: payload.subarray(TASK_HEADER),
  }
}

// ── Instance ID ───────────────────────────────────────────────────────────

let nextInstanceId = 1

// ── WorkflowBuilder ───────────────────────────────────────────────────────

export class WorkflowBuilder {
  private triggerSubject: string | undefined
  private readonly steps: StepDef[] = []
  private ackWaitMs = 30_000
  private maxInflight = 10

  constructor(
    private readonly client: ArbitroClient,
    private readonly workflowName: string,
  ) {}

  trigger(subject: string): this {
    this.triggerSubject = subject
    return this
  }

  step(name: string, handler: StepHandler): this {
    this.steps.push({ name, handler })
    return this
  }

  ackWait(ms: number): this {
    this.ackWaitMs = ms
    return this
  }

  inflight(n: number): this {
    this.maxInflight = n
    return this
  }

  async start(): Promise<WorkflowHandle> {
    if (!this.triggerSubject) throw new Error('trigger subject required — call .trigger()')
    if (this.steps.length === 0) throw new Error('at least one step required — call .step()')

    const name = this.workflowName
    const taskStreamName = `_wf.${name}.tasks`
    const taskSubject = `_wf.${name}.>`
    const consumerName = `_wf.${name}.workers`

    // Create internal task stream with idempotency.
    await this.client.createStream(taskStreamName, {
      subjectFilter: taskSubject,
      idempotencyWindowMs: 300_000,
    })

    // Subscribe with consumer group config + callback.
    const steps = this.steps
    const totalSteps = steps.length
    const client = this.client

    const sub = await this.client.subscribe(taskStreamName, {
      name: consumerName,
      filter: taskSubject,
      ackPolicy: AckPolicy.Explicit,
      ackWaitMs: this.ackWaitMs,
      maxAckPending: this.maxInflight,
    }, async (msg: Message) => {
      const task = decodeTask(msg.data())
      if (!task || task.stepIndex >= totalSteps) {
        msg.ack()
        return
      }

      const handler = steps[task.stepIndex]!.handler
      try {
        const result = await handler({
          name,
          instanceId: task.instanceId,
          stepIndex: task.stepIndex,
          attempt: task.attempt,
          context: task.context,
        })

        const nextStep = task.stepIndex + 1
        if (nextStep < totalSteps) {
          const msgId = `wf:${task.instanceId}:${nextStep}:0`
          const subject = `_wf.${name}.step.${nextStep}`
          const taskBuf = encodeTask(task.instanceId, nextStep, 0, result.context)
          await client.publish(taskStreamName, subject, taskBuf, { msgId })
        }
        msg.ack()
      } catch {
        msg.nack()
      }
    })

    return new WorkflowHandle(name, taskStreamName, sub)
  }
}

// ── WorkflowHandle ────────────────────────────────────────────────────────

export class WorkflowHandle {
  constructor(
    private readonly workflowName: string,
    private readonly taskStreamName: string,
    private readonly sub: unknown,
  ) {}

  get name(): string { return this.workflowName }

  async trigger(client: ArbitroClient, context: Buffer): Promise<number> {
    const instanceId = nextInstanceId++
    const msgId = `wf:${instanceId}:0:0`
    const subject = `_wf.${this.workflowName}.step.0`
    const task = encodeTask(instanceId, 0, 0, context)
    await client.publish(this.taskStreamName, subject, task, { msgId })
    return instanceId
  }
}
