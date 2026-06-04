// Fluent builder for workflow registration + handle for lifecycle.

import type { Connection } from '../net/connection'
import type {
  WorkflowState,
  WorkflowStepHandler,
  WorkflowErrorHandler,
} from './workflow-state'
import {
  packCreateWorkflow,
  packDeleteWorkflow,
  packCancelWorkflow,
  type CreateWorkflowBody,
  type StepDef,
  type WorkflowConfig,
} from './workflow-frame'

// ── WorkflowBuilder ───────────────────────────────────────────────────────

export class WorkflowBuilder {
  private triggerSubject: string | undefined
  private steps: StepDef[] = []
  private errHandler: WorkflowErrorHandler | undefined
  private cfg: WorkflowConfig = {}

  constructor(
    private readonly conn: Connection,
    private readonly workflowState: WorkflowState,
    private readonly workflowName: string,
  ) {}

  /** Subject pattern that triggers new workflow instances. */
  trigger(subject: string): this {
    this.triggerSubject = subject
    return this
  }

  /** Append a step to the pipeline. */
  step(name: string, opts?: { timeoutMs?: number; maxRetries?: number }): this {
    this.steps.push({
      name,
      timeout_ms: opts?.timeoutMs ?? 0,
      max_retries: opts?.maxRetries ?? 0,
    })
    return this
  }

  /** Register an error handler called when the workflow instance fails. */
  onError(handler: WorkflowErrorHandler): this {
    this.errHandler = handler
    return this
  }

  /** Maximum concurrent instances (0 = unlimited). */
  maxConcurrent(n: number): this {
    this.cfg = { ...this.cfg, max_concurrent: n }
    return this
  }

  /** JSON key path for dedup (undefined = no dedup). */
  dedupKey(key: string): this {
    this.cfg = { ...this.cfg, dedup_key: key }
    return this
  }

  /** Overall workflow timeout in ms (0 = none). */
  timeout(ms: number): this {
    this.cfg = { ...this.cfg, timeout_ms: ms }
    return this
  }

  /** Send CreateWorkflow to broker, register step handler, return handle. */
  async start(handler: WorkflowStepHandler): Promise<WorkflowHandle> {
    if (!this.triggerSubject) throw new Error('trigger subject required — call .trigger()')
    if (this.steps.length === 0) throw new Error('at least one step required — call .step()')

    const body: CreateWorkflowBody = {
      name: this.workflowName,
      trigger: this.triggerSubject,
      steps: this.steps,
      config: this.cfg,
    }

    const seq = this.conn.nextSeq()
    await this.conn.sendExpectReply(packCreateWorkflow(seq, body))
    this.workflowState.register(this.workflowName, body, handler, this.errHandler)

    return new WorkflowHandle(this.conn, this.workflowState, this.workflowName)
  }
}

// ── WorkflowHandle ────────────────────────────────────────────────────────

export class WorkflowHandle {
  constructor(
    private readonly conn: Connection,
    private readonly workflowState: WorkflowState,
    private readonly workflowName: string,
  ) {}

  get name(): string { return this.workflowName }

  /** Delete the workflow definition from the broker. */
  async stop(): Promise<void> {
    const nameBuf = Buffer.from(this.workflowName)
    const seq = this.conn.nextSeq()
    await this.conn.sendExpectReply(packDeleteWorkflow(seq, nameBuf))
    this.workflowState.remove(this.workflowName)
  }

  /** Cancel a specific running instance by ID. */
  async cancel(instanceId: number): Promise<void> {
    const seq = this.conn.nextSeq()
    await this.conn.sendExpectReply(packCancelWorkflow(seq, instanceId))
  }
}
