/// <reference types="node" />

export { DeliverPolicy, JournalType, AckPolicy, ArbitroError, ErrorCode } from "./types";
export type { ErrorCodeValue } from "./types";
export type { PublishOpts } from "./stream/publish";
export type { BatchPublishEntry } from "./proto/publish";
export { encodeString, decodeString, encodeJson, decodeJson } from "./utils";
export { makeLazyMessage, type LazyMessage } from "./topic";
export { Subscription } from "./subscription";
export { ArbitroClient } from "./client";
export type { ClientMetricsSnapshot } from "./client/metrics";
export { streamId } from "./proto/fnv1a";
export { Consumer } from "./consumer";
export { Message } from "./message";
export { Stream } from "./stream";
export { Topic } from "./topic";
export { CronBuilder, CronHandle } from "./cron";
export type { CronContext, CronHandler } from "./cron";
export { Request, Service, ServiceBuilder } from "./service";
export type { ServiceHandler, ServiceConfig } from "./service";
export { WorkflowBuilder, WorkflowHandle, COMPENSATION_BIT } from "./workflow";
export type { StepContext, StepResult, StepHandler, DecodedTask } from "./workflow";

export type { Logger, LogFn } from "./common/logger";

export type {
  ClientConfig,
  StreamConfig,
  StreamInfo,
  ConsumerConfig,
  ConsumerInfo,
  JournalConfig,
  FlushConfig,
  DeleteStreamOpts,
  SubscribeOptions,
  ReconnectConfig,
  TlsConfig,
  SubjectInflightLimit,
} from "./types";

export {
  Codec,
  JsonCodec,
  StringCodec,
  schema,
  type Encoding,
  type Schema,
  type FieldType,
  type FieldTypeMap,
  type InferSchema,
} from "./utils";

// `zodCodec` lives next to the other codecs in the main export.
// Zod is referenced only via `import type` in `utils/zod.ts`, so this
// re-export adds zero runtime dependency on zod for users who never
// call zodCodec — TypeScript strips the type imports at compile time.
export { zodCodec } from "./utils/zod";
