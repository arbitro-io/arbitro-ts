// Shared registry of active workflow handlers. Keyed by workflow name.

import type { CreateWorkflowBody } from './workflow-frame'

/** Context passed to the step handler on each workflow step dispatch. */
export interface WorkflowStepContext {
  /** Workflow name. */
  readonly name: string
  /** Instance ID assigned by the broker. */
  readonly instanceId: number
  /** Zero-based step index within the workflow. */
  readonly stepIndex: number
  /** JSON context bag from the previous step (or trigger payload for step 0). */
  readonly context: Buffer
}

/** Context passed to the error handler when a workflow instance fails. */
export interface WorkflowErrorContext {
  /** Workflow name. */
  readonly name: string
  /** Instance ID. */
  readonly instanceId: number
  /** Raw error JSON from the broker. */
  readonly errorJson: Buffer
}

export type WorkflowStepHandler = (ctx: WorkflowStepContext) => Promise<Buffer>
export type WorkflowErrorHandler = (ctx: WorkflowErrorContext) => Promise<void>

interface WorkflowEntry {
  readonly config: CreateWorkflowBody
  readonly stepHandler: WorkflowStepHandler
  readonly errorHandler: WorkflowErrorHandler | undefined
}

export class WorkflowState {
  private readonly handlers = new Map<string, WorkflowEntry>()

  register(
    name: string,
    config: CreateWorkflowBody,
    stepHandler: WorkflowStepHandler,
    errorHandler?: WorkflowErrorHandler,
  ): void {
    this.handlers.set(name, { config, stepHandler, errorHandler })
  }

  remove(name: string): void {
    this.handlers.delete(name)
  }

  getStepHandler(name: string): WorkflowStepHandler | undefined {
    return this.handlers.get(name)?.stepHandler
  }

  getErrorHandler(name: string): WorkflowErrorHandler | undefined {
    return this.handlers.get(name)?.errorHandler
  }

  allConfigs(): ReadonlyArray<{ name: string; config: CreateWorkflowBody }> {
    const out: Array<{ name: string; config: CreateWorkflowBody }> = []
    for (const [name, entry] of this.handlers) {
      out.push({ name, config: entry.config })
    }
    return out
  }
}
