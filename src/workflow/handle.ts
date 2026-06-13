// WorkflowHandle — returned by WorkflowBuilder.start().

import type { ArbitroClient } from '../client/client'
import { encodeTask } from './task'

let nextInstanceId = 1

/** Generate a process-unique instance ID as a string. */
export function allocInstanceId(): string { return String(nextInstanceId++) }

export class WorkflowHandle {
  private resumeSeq = 0

  constructor(
    private readonly workflowName: string,
    private readonly taskStreamName: string,
    private readonly dlqStreamName: string,
    private readonly sub: unknown,
    private readonly triggerSub: unknown | undefined,
    private readonly sourceSubs: unknown[] = [],
    private readonly stateSub: unknown | undefined = undefined,
  ) {}

  get name(): string { return this.workflowName }
  get taskStream(): string { return this.taskStreamName }
  get dlqStream(): string { return this.dlqStreamName }

  /**
   * Trigger a new workflow instance with an explicit ID.
   *
   * The caller chooses the `instanceId` (e.g. a business key like
   * `"ord_123"`). The same ID can be used by external systems to
   * address this workflow instance.
   */
  async triggerWithId(client: ArbitroClient, instanceId: string, context: Buffer): Promise<void> {
    const msgId = `wf:${instanceId}:0:0`
    const subject = `_wf.${this.workflowName}.step.0`
    const task = encodeTask(instanceId, 0, 0, context)
    await client.publish(this.taskStreamName, subject, task, { msgId })
  }

  /**
   * Trigger a new workflow instance with an auto-generated ID.
   *
   * Returns the generated instance ID so the caller can track
   * or correlate the workflow instance.
   */
  async trigger(client: ArbitroClient, context: Buffer): Promise<string> {
    const instanceId = allocInstanceId()
    await this.triggerWithId(client, instanceId, context)
    return instanceId
  }

  /**
   * Resume a suspended workflow instance with an external event.
   *
   * Publishes a resume event to the task stream. The dispatch loop
   * picks it up, matches it against the in-memory suspended registry,
   * and invokes the `onResume` handler.
   */
  async resume(client: ArbitroClient, instanceId: string, event: Buffer): Promise<void> {
    const seq = this.resumeSeq++
    const msgId = `wf:${instanceId}:resume:${seq}`
    const subject = `_wf.${this.workflowName}.resume.${instanceId}`
    await client.publish(this.taskStreamName, subject, event, { msgId })
  }

  /**
   * Cancel a suspended workflow instance.
   *
   * Publishes a cancel event to the task stream. The dispatch loop
   * removes the instance from the in-memory suspended registry and
   * acks. If the instance is not suspended (already completed or
   * never existed), the cancel is a no-op.
   */
  async cancel(client: ArbitroClient, instanceId: string): Promise<void> {
    const msgId = `wf:${instanceId}:cancel`
    const subject = `_wf.${this.workflowName}.cancel.${instanceId}`
    await client.publish(this.taskStreamName, subject, Buffer.alloc(0), { msgId })
  }
}
