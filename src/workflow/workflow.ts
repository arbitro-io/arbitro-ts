// Workflow client-side module — mirrors the Rust implementation.
// Creates internal _wf.{name}.tasks stream, consumer group with
// ack_wait_ms, publishes step tasks with idempotent msg_id.

import type { ArbitroClient } from '../client/client'

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
// Format: [instance_id:4 LE][step_index:2 LE][attempt:1][context...]

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
    attempt: payload[6],
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
    const streamResp = await this.client.createStream(taskStreamName, {
      subjects: taskSubject,
      idempotencyWindowMs: 300_000,
    })
    const taskStreamId = streamResp.id

    // Create consumer with ack_wait for failover.
    await this.client.createConsumer(taskStreamId, {
      name: consumerName,
      group: consumerName,
      filterSubject: taskSubject,
      maxInflight: this.maxInflight,
      ackPolicy: 'explicit',
      deliverMode: 'queue',
      ackWaitMs: this.ackWaitMs,
    })

    // Subscribe and process tasks.
    const steps = this.steps
    const totalSteps = steps.length
    const sub = await this.client.subscribe(taskStreamId, consumerName, taskSubject)

    const handle = new WorkflowHandle(name, taskStreamId)

    // Processing loop
    void (async () => {
      for await (const msg of sub) {
        const task = decodeTask(msg.payload)
        if (!task || task.stepIndex >= totalSteps) {
          msg.ack()
          continue
        }

        const handler = steps[task.stepIndex].handler
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
            await this.client.publishWithId(taskStreamId, subject, msgId, taskBuf)
          }
          msg.ack()
        } catch {
          msg.nack()
        }
      }
    })()

    return handle
  }
}

// ── WorkflowHandle ────────────────────────────────────────────────────────

export class WorkflowHandle {
  constructor(
    private readonly workflowName: string,
    private readonly taskStreamId: number,
  ) {}

  get name(): string { return this.workflowName }

  async trigger(client: ArbitroClient, context: Buffer): Promise<number> {
    const instanceId = nextInstanceId++
    const msgId = `wf:${instanceId}:0:0`
    const subject = `_wf.${this.workflowName}.step.0`
    const task = encodeTask(instanceId, 0, 0, context)
    await client.publishWithId(this.taskStreamId, subject, msgId, task)
    return instanceId
  }
}
