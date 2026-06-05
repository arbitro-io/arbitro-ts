// Workflow orchestration — client-side linear pipelines over streams.
//
// Uses existing streams + consumer groups + publish with msg_id.
// The broker has NO workflow-specific code.

export { WorkflowBuilder, type StepContext, type StepResult, type StepHandler } from './workflow'
export { WorkflowHandle } from './handle'
export { COMPENSATION_BIT, encodeTask, decodeTask, type DecodedTask } from './task'
