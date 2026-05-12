export class ArbitroError extends Error {
  constructor(
    message: string,
    readonly code: 'connect' | 'timeout' | 'protocol' | 'server' | 'closed',
    readonly brokerName?: string,
    readonly brokerDetails?: unknown,
    /**
     * Numeric wire error code from the broker's `RepError` frame. Only
     * populated when `code === 'server'`. Use the `ErrorCode` enum to
     * compare (e.g. `err.wireCode === ErrorCode.IdempotencyDuplicate`).
     */
    readonly wireCode?: number,
  ) {
    super(message)
    this.name = 'ArbitroError'
  }
}

/**
 * Wire-level error codes. Mirrors the `ErrorCode` enum in
 * `arbitro-proto`. New codes added to the broker MUST be appended here
 * with the matching numeric value.
 */
export const ErrorCode = {
  // 0x00xx — Protocol
  UnknownAction:        0x0001,
  BufferTooShort:       0x0002,
  InvalidLength:        0x0003,
  InvalidEntryCount:    0x0004,
  // 0x01xx — Auth
  AuthRequired:         0x0101,
  AuthFailed:           0x0102,
  // 0x02xx — Stream
  StreamNotFound:       0x0201,
  StreamAlreadyExists:  0x0202,
  StreamFull:           0x0203,
  StreamFilterOverlap:  0x0204,
  SubjectNotFound:      0x0205,
  /** Publish carried a `msgId` already seen for this stream within
   * `idempotencyWindowMs`. Original write stands; safe to treat as
   * a successful publish at the application level. */
  IdempotencyDuplicate: 0x0206,
  // 0x03xx — Consumer
  ConsumerNotFound:      0x0301,
  ConsumerAlreadyExists: 0x0302,
  ConsumerFilterOverlap: 0x0303,
  // 0x04xx — Delivery
  InvalidSequence:    0x0401,
  MaxInflightReached: 0x0402,
  AckTimeout:         0x0403,
  // 0x05xx — System
  ServerShuttingDown: 0x0501,
  InternalError:      0x0502,
} as const

export type ErrorCodeValue = (typeof ErrorCode)[keyof typeof ErrorCode]

export interface BrokerError {
  name: string
  message: string
  details?: unknown
}
