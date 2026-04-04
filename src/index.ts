/// <reference types="node" />

export { DeliverPolicy, JournalType, AckPolicy, ArbitroError } from "./types";
export { encodeString, decodeString, encodeJson, decodeJson } from "./utils";
export { makeLazyMessage, type LazyMessage } from "./topic";
export { Subscription } from "./subscription";
export { ArbitroClient } from "./client";
export { streamId } from "./proto/fnv1a";
export { Consumer } from "./consumer";
export { Message } from "./message";
export { Stream } from "./stream";
export { Topic } from "./topic";

export type { Logger, LogFn } from "./common/logger";

export type {
  ClientConfig,
  StreamConfig,
  StreamInfo,
  ConsumerConfig,
  ConsumerInfo,
  JournalConfig,
  FlushConfig,
  CreditRule,
  DeleteStreamOpts,
  SubscribeOptions,
  ReconnectConfig,
  TlsConfig,
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
