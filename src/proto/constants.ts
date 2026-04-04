export const MAGIC       = 0xA1B2_C3D4
export const VERSION     = 0x02
export const HEADER_SIZE = 32

// Header field byte offsets
export const OFF_MAGIC     = 0
export const OFF_VERSION   = 4
export const OFF_FLAGS     = 5
export const OFF_ACTION    = 6
export const OFF_CRC32C    = 8
export const OFF_LENGTH    = 12
export const OFF_SEQUENCE  = 16
export const OFF_TIMESTAMP = 24
export const OFF_SUBJ_LEN  = 32  // first 2 bytes of payload
export const OFF_SUBJ      = 34  // subject bytes start

export const enum Action {
  None            = 0x0000,

  SysConnect      = 0x0001,
  SysDisconnect   = 0x0002,
  SysKeepalive    = 0x0003,
  SysStats        = 0x0004,
  SysError        = 0x00FF,

  PubPublish        = 0x0101,
  PubSubscribe      = 0x0102,
  PubUnsubscribe    = 0x0103,
  PubCreateStream   = 0x0104,
  PubDeleteStream   = 0x0105,
  PubPull           = 0x0106,
  PubCreateConsumer = 0x0107,
  PubDeleteConsumer = 0x0108,
  PubPublishStream  = 0x0109,
  PubPublishBatch   = 0x010A,

  RepAck   = 0x0201,
  RepNack  = 0x0202,
  RepOk    = 0x0203,
  RepReply = 0x0204,
  RepError = 0x0205,

  MgmtRegisterPipeline   = 0x0301,
  MgmtUnregisterPipeline = 0x0302,
  MgmtListPipelines      = 0x0303,
  MgmtGetStream          = 0x0304,
  MgmtListStreams         = 0x0305,
  MgmtGetConsumer        = 0x0306,
  MgmtListConsumers      = 0x0307,
}

export const enum Flags {
  None       = 0x00,
  NoAck      = 0x01,
  Compressed = 0x02,
  ReplyTo    = 0x04,
  HasHeaders = 0x08,
}
