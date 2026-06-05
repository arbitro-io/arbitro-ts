// Workflow orchestration — client-side linear pipelines over streams.
//
// Uses existing streams + consumer groups + publish with msg_id.
// The broker has NO workflow-specific code.

export { WorkflowBuilder, WorkflowHandle, type StepContext, type StepResult, type StepHandler } from './workflow'
