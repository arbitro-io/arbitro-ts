// WorkflowHandle — returned by WorkflowBuilder.start().

import type { ArbitroClient } from '../client/client'
import { encodeTask } from './task'

let nextInstanceId = 1

/** Generate a process-unique instance ID. */
export function allocInstanceId(): number { return nextInstanceId++ }

export class WorkflowHandle {
  constructor(
    private readonly workflowName: string,
    private readonly taskStreamName: string,
    private readonly dlqStreamName: string,
    private readonly sub: unknown,
    private readonly triggerSub: unknown | undefined,
  ) {}

  get name(): string { return this.workflowName }
  get taskStream(): string { return this.taskStreamName }
  get dlqStream(): string { return this.dlqStreamName }

  async trigger(client: ArbitroClient, context: Buffer): Promise<number> {
    const instanceId = allocInstanceId()
    const msgId = `wf:${instanceId}:0:0`
    const subject = `_wf.${this.workflowName}.step.0`
    const task = encodeTask(instanceId, 0, 0, context)
    await client.publish(this.taskStreamName, subject, task, { msgId })
    return instanceId
  }
}
