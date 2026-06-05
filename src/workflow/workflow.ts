// WorkflowBuilder — fluent builder for linear step pipelines.
// Mirrors the Rust WorkflowBuilder: trigger streams, saga compensation,
// max retries with DLQ, context size limits, unique consumer per worker.

import type { ArbitroClient } from '../client/client'
import type { Message } from '../message/message'
import { AckPolicy } from '../types/config'
import { encodeTask } from './task'
import { WorkflowHandle, allocInstanceId } from './handle'
import { processMessage, type ProcessorConfig } from './processor'

// ── Types ─────────────────────────────────────────────────────────────────

export interface StepContext {
  readonly name: string
  readonly instanceId: number
  readonly stepIndex: number
  readonly attempt: number
  readonly context: Buffer
}

export interface StepResult { readonly context: Buffer }

export type StepHandler = (ctx: StepContext) => Promise<StepResult>

export interface StepDef {
  readonly name: string
  readonly handler: StepHandler
  compensation: StepHandler | undefined
}

// ── Unique worker ID counter (per process) ────────────────────────────────

let nextWorkerUid = 1

// ── WorkflowBuilder ───────────────────────────────────────────────────────

export class WorkflowBuilder {
  private triggerSubject: string | undefined
  private triggerStreamName: string | undefined
  private readonly steps: StepDef[] = []
  private ackWaitMs = 30_000
  private maxInflightVal = 10
  private maxRetriesVal = 3
  private maxContextSizeVal = 256 * 1024

  constructor(
    private readonly client: ArbitroClient,
    private readonly workflowName: string,
  ) {}

  trigger(subject: string): this { this.triggerSubject = subject; return this }

  triggerStream(streamName: string): this { this.triggerStreamName = streamName; return this }

  step(name: string, handler: StepHandler): this {
    this.steps.push({ name, handler, compensation: undefined })
    return this
  }

  /** Compensation handler for the most recently added step. */
  compensate(_stepName: string, handler: StepHandler): this {
    const last = this.steps[this.steps.length - 1]
    if (last) last.compensation = handler
    return this
  }

  ackWait(ms: number): this { this.ackWaitMs = ms; return this }
  inflight(n: number): this { this.maxInflightVal = n; return this }
  maxRetries(n: number): this { this.maxRetriesVal = n; return this }
  maxContextSize(bytes: number): this { this.maxContextSizeVal = bytes; return this }

  async start(): Promise<WorkflowHandle> {
    if (!this.triggerSubject) throw new Error('trigger subject required')
    if (this.steps.length === 0) throw new Error('at least one step required')

    const name = this.workflowName
    const taskStream = `_wf.${name}.tasks`
    const taskSubject = `_wf.${name}.>`
    const dlqStream = `_wf.${name}.dlq`
    const dlqSubject = `_wf.${name}.dlq.>`

    await this.client.upsertStream(taskStream, { subjectFilter: taskSubject, idempotencyWindowMs: 300_000 })
    await this.client.upsertStream(dlqStream, { subjectFilter: dlqSubject })

    const cfg: ProcessorConfig = {
      client: this.client, name, taskStreamName: taskStream,
      dlqStreamName: dlqStream, steps: this.steps,
      maxContextSize: this.maxContextSizeVal, maxRetries: this.maxRetriesVal,
    }

    const sub = await this.subscribeWorker(cfg, taskStream, taskSubject)
    const triggerSub = await this.subscribeTrigger(taskStream, name)
    return new WorkflowHandle(name, taskStream, dlqStream, sub, triggerSub)
  }

  private async subscribeWorker(cfg: ProcessorConfig, taskStream: string, taskSubject: string) {
    const uid = nextWorkerUid++
    return this.client.subscribe(taskStream, {
      name: `_wf_${cfg.name}_w${uid}`,
      group: `_wf_${cfg.name}_workers`,
      filter: taskSubject,
      ackPolicy: AckPolicy.Explicit,
      ackWaitMs: this.ackWaitMs,
      maxAckPending: this.maxInflightVal,
    }, (msg: Message) => { void processMessage(cfg, msg) })
  }

  private async subscribeTrigger(taskStream: string, name: string) {
    if (!this.triggerSubject || !this.triggerStreamName) return undefined
    const subject = this.triggerSubject
    return this.client.subscribe(this.triggerStreamName, {
      name: `_wf_${name}_trigger`,
      filter: subject,
      ackPolicy: AckPolicy.Explicit,
      ackWaitMs: this.ackWaitMs,
      maxAckPending: 1,
    }, async (msg: Message) => {
      const id = allocInstanceId()
      const taskBuf = encodeTask(id, 0, 0, msg.data())
      await this.client.publish(taskStream, `_wf.${name}.step.0`, taskBuf, { msgId: `wf:${id}:0:0` })
      msg.ack()
    })
  }
}
