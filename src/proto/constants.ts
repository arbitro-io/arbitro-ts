// V2 wire protocol constants — must match arbitro-proto/src/action.rs exactly.

// ── Hello handshake (only frame without a Header) ───────────────────────
export const MAGIC_V2       = 0x32425241  // "ARB2" as u32 LE
export const HELLO_SIZE     = 8
export const CURRENT_VERSION = 2

export const enum Role {
  Client = 0,
  Server = 1,
}

export const enum Cap {
  Headers          = 1 << 0,
  Reply            = 1 << 1,
  BatchHeaders     = 1 << 2,
  CompressedPayload = 1 << 3,
}

// ── Header (16 bytes, every frame after Hello) ──────────────────────────
export const HEADER_SIZE     = 16

// Byte offsets within the 16-byte header
export const OFF_ACTION      = 0   // u16 LE
export const OFF_FLAGS       = 2   // u8
export const OFF_ENTRY_FLAGS = 3   // u8
export const OFF_MSG_LEN     = 4   // u32 LE
export const OFF_SEQ         = 8   // u64 LE

// ── Transport flags (header.flags, offset 2) ────────────────────────────
export const enum Flag {
  None         = 0x00,
  AckReq       = 0x01,
  Dup          = 0x02,
  PriorityHigh = 0x04,
}

// ── Per-message flags (header.entry_flags, offset 3) ────────────────────
export const enum EntryFlag {
  None           = 0x00,
  Retain         = 0x01,
  Compressed     = 0x02,
  NoBackpressure = 0x04,
}

// ── Action codes (0xFFGG: FF=family, GG=variant) ────────────────────────
export const enum Action {
  // 0x00xx — Handshake / control
  Hello           = 0x0001,
  Auth            = 0x0002,

  // 0x01xx — Publish family
  Publish                 = 0x0101,
  PublishAccumulate       = 0x0102,
  PublishBatch            = 0x0103,
  PublishWithReply        = 0x0104,
  PublishWithHeaders      = 0x0105,
  PublishBatchWithHeaders = 0x0106,

  // 0x02xx — Delivery / Ack
  Deliver      = 0x0200,
  Ack          = 0x0201,
  Nack         = 0x0202,
  RepOk        = 0x0203,
  RepError     = 0x0204,
  RepBatch     = 0x0205,
  BatchAck     = 0x0206,
  FanoutBatch  = 0x0207,
  AckSync      = 0x0208,
  BatchAckSync = 0x0209,
  BatchNack    = 0x020A,

  // 0x03xx — Subscription
  Subscribe   = 0x0301,
  Unsubscribe = 0x0302,

  // 0x04xx — Stream management
  CreateStream = 0x0401,
  DeleteStream = 0x0402,
  GetStream    = 0x0403,
  ListStreams  = 0x0404,
  PurgeStream  = 0x0405,
  DrainSubject = 0x0406,

  // 0x05xx — Consumer management
  CreateConsumer = 0x0501,
  DeleteConsumer = 0x0502,
  GetConsumer    = 0x0503,
  ListConsumers  = 0x0504,
  ConsumerStats  = 0x0505,
  PauseConsumer  = 0x0506,
  ResumeConsumer = 0x0507,

  // 0x06xx — System
  Ping       = 0x0601,
  Pong       = 0x0602,
  Connect    = 0x0603,
  Connected  = 0x0604,
  Disconnect = 0x0605,

  // 0x07xx — Cron scheduling
  CreateCron = 0x0701,
  DeleteCron = 0x0702,
  ListCrons  = 0x0703,
  CronFire   = 0x0704,
  CronAck    = 0x0705,
}
