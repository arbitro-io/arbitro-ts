import type { ArbitroClient } from '../client'
import type { Connection } from '../net/connection'
import { Message, REPLY_TO_MAGIC } from '../message/message'
import { packPublish, packPublishWithReply } from '../proto/publish'
import { packCreateStream, packCreateConsumer, packGetStream, packSubscribe } from '../proto/v2'
import { Flag, HEADER_SIZE } from '../proto/constants'
import { ArbitroError } from '../types/error'

const SVC_PREFIX = '_svc.'
const METHOD_INFIX = '.m.'
const REPLY_INFIX = '._r.'

let NEXT_INSTANCE_ID = 1
function nextInstanceId(): number {
  const id = NEXT_INSTANCE_ID++
  if (NEXT_INSTANCE_ID > 0xFFFFFFFF) NEXT_INSTANCE_ID = 1
  return id
}

/**
 * Incoming service request. Read-only view over the delivered message —
 * intentionally does not expose ack/nack/reply. Ack, nack, and reply are
 * managed by the framework based on the handler's return value.
 */
export class Request {
  constructor(
    private readonly _subject: Buffer,
    private readonly _payload: Buffer,
    private readonly _hasReply: boolean,
    private readonly _seq: bigint,
    private readonly _consumerId: number,
  ) {}

  /** Full subject (e.g., `_svc.orders.charge`). */
  subject(): Buffer { return this._subject }

  /** Payload bytes. */
  data(): Buffer { return this._payload }

  /** `true` if the requester is waiting for a reply. */
  hasReply(): boolean { return this._hasReply }

  /** Delivery sequence assigned by the broker. */
  seq(): bigint { return this._seq }

  /** Consumer id that received this request. */
  consumerId(): number { return this._consumerId }

  /**
   * Method segment after the service prefix (e.g., `charge`).
   * Returns `undefined` if the subject is malformed.
   */
  method(serviceName: string): Buffer | undefined {
    // Subject: _svc.<serviceName>.m.<method>
    const prefixLen = SVC_PREFIX.length + serviceName.length + METHOD_INFIX.length
    if (this._subject.length <= prefixLen) return undefined
    return this._subject.subarray(prefixLen)
  }
}

/**
 * Handler for incoming service requests.
 *
 * Return value semantics (framework handles ack/nack/reply automatically):
 * - Return `Buffer` — framework replies to the requester (if a reply
 *   address is present) and acks the delivery.
 * - Return `void` / `undefined` — framework acks without replying.
 * - Throw / reject — framework nacks the delivery for redelivery.
 *
 * The framework guarantees exactly one ack or nack per invocation.
 */
export type ServiceHandler = (req: Request) => Buffer | void | Promise<Buffer | void>

export interface ServiceConfig {
  maxInflight?: number
}

export class Service {
  private readonly handlers = new Map<string, ServiceHandler>()
  private readonly streamCache = new Map<string, number>()
  private readonly pending = new Map<number, { resolve: (data: Buffer) => void; timer: ReturnType<typeof setTimeout> }>()
  private corrId = 0
  private closed = false
  readonly instanceId: number = nextInstanceId()

  constructor(
    private readonly name: string,
    private readonly streamId: number,
    private readonly conn: Connection,
    private readonly client: ArbitroClient,
  ) {
    this.streamCache.set(name, streamId)
  }

  handle(method: string, handler: ServiceHandler): void {
    const prefix = `${SVC_PREFIX}${this.name}${METHOD_INFIX}${method}`
    this.handlers.set(prefix, handler)
  }

  async request(target: string, method: string, payload: Buffer, timeoutMs = 5000): Promise<Buffer> {
    const targetStreamId = await this.resolveStream(target)
    const corrId = ++this.corrId
    const subject = Buffer.from(`${SVC_PREFIX}${target}${METHOD_INFIX}${method}`)
    const replySubject = `${SVC_PREFIX}${this.name}${REPLY_INFIX}${this.instanceId}.${corrId}`

    const replyTo = Buffer.allocUnsafe(5 + replySubject.length)
    replyTo[0] = REPLY_TO_MAGIC
    replyTo.writeUInt32LE(this.streamId, 1)
    Buffer.from(replySubject).copy(replyTo, 5)

    const frame = packPublishWithReply(
      this.conn.nextSeq(), targetStreamId, subject, replyTo, payload, Flag.AckReq,
    )

    return new Promise<Buffer>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(corrId)
        reject(new ArbitroError('request timeout', 'timeout'))
      }, timeoutMs)

      this.pending.set(corrId, { resolve: (data) => { clearTimeout(timer); resolve(data) }, timer })
      this.conn.sendExpectReply(frame, timeoutMs).catch((err) => {
        clearTimeout(timer)
        this.pending.delete(corrId)
        reject(err)
      })
    })
  }

  async send(target: string, method: string, payload: Buffer): Promise<void> {
    const targetStreamId = await this.resolveStream(target)
    const subject = Buffer.from(`${SVC_PREFIX}${target}${METHOD_INFIX}${method}`)
    this.conn.send(packPublish(this.conn.nextSeq(), targetStreamId, subject, payload, Flag.AckReq, 0))
  }

  close(): void {
    this.closed = true
    for (const [, { timer }] of this.pending) clearTimeout(timer)
    this.pending.clear()
  }

  /** @internal — called by the dispatch loop on each delivery. */
  _dispatch(msg: Message): void {
    if (this.closed) return
    const subject = msg.subject().toString()
    // Reply subject: `_svc.<name>._r.<instanceId>.<corrId>` — instance-scoped.
    const replyPrefix = `${SVC_PREFIX}${this.name}${REPLY_INFIX}${this.instanceId}.`

    if (subject.startsWith(replyPrefix)) {
      const corrStr = subject.slice(replyPrefix.length)
      const corrId = parseInt(corrStr, 10)
      if (!isNaN(corrId)) {
        const entry = this.pending.get(corrId)
        if (entry) {
          this.pending.delete(corrId)
          entry.resolve(msg.data())
        }
        msg.ack()
        return
      }
    }

    for (const [prefix, handler] of this.handlers) {
      if (subject.startsWith(prefix)) {
        const hasReply = msg.replyTo().length > 0
        const req = new Request(
          msg.subject(),
          msg.data(),
          hasReply,
          msg.seq(),
          msg.consumerId(),
        )
        Promise.resolve(handler(req)).then(
          (response) => {
            if (response && response.length > 0 && hasReply) {
              try { msg.reply(response) } catch { /* channel closed */ }
            }
            msg.ack()
          },
          () => {
            msg.nack()
          },
        )
        return
      }
    }

    msg.nack()
  }

  private async resolveStream(target: string): Promise<number> {
    const cached = this.streamCache.get(target)
    if (cached !== undefined) return cached

    const streamName = `_svc-${target}`
    const refSeq = await this.conn.sendExpectReply(
      packGetStream(this.conn.nextSeq(), Buffer.from(streamName)),
    )
    const sid = Number(refSeq & 0xFFFFFFFFn)
    this.streamCache.set(target, sid)
    return sid
  }
}

export class ServiceBuilder {
  private maxInflight = 1024

  constructor(
    private readonly client: ArbitroClient,
    private readonly conn: Connection,
    private readonly name: string,
  ) {}

  setMaxInflight(n: number): this {
    this.maxInflight = n
    return this
  }

  async build(): Promise<Service> {
    const streamName = `_svc-${this.name}`
    // Stream captures BOTH method and reply subjects.
    const streamFilter = `${SVC_PREFIX}${this.name}.>`

    const streamRef = await this.conn.sendExpectReply(
      packCreateStream(
        this.conn.nextSeq(),
        Buffer.from(streamName),
        Buffer.from(streamFilter),
        0n, 0n, 3600n, 1, 0, 0, 0, 0,
      ),
    )
    const streamId = Number(streamRef & 0xFFFFFFFFn)

    const svc = new Service(this.name, streamId, this.conn, this.client)

    // Worker consumer: queue-grouped, receives ONLY method calls (`.m.>`).
    const workerFilter = `${SVC_PREFIX}${this.name}${METHOD_INFIX}>`
    const workerName = `_svc-${this.name}-worker`
    const workerRef = await this.conn.sendExpectReply(
      packCreateConsumer(this.conn.nextSeq(), {
        streamId,
        name: Buffer.from(workerName),
        group: Buffer.from(workerName),
        filter: Buffer.from(workerFilter),
        maxInflight: this.maxInflight,
        ackPolicy: 1,
        deliverPolicy: 0,
        deliverMode: 0,
        ackWaitMs: 30_000,
        startSeq: 0n,
      }),
    )
    const workerConsumerId = Number(workerRef & 0xFFFFFFFFn)

    // Reply consumer: per-instance, NO group — replies MUST NOT be
    // load-balanced to sibling instances.
    const replyFilter = `${SVC_PREFIX}${this.name}${REPLY_INFIX}${svc.instanceId}.>`
    const replyName = `_svc-${this.name}-reply-${svc.instanceId}`
    const replyRef = await this.conn.sendExpectReply(
      packCreateConsumer(this.conn.nextSeq(), {
        streamId,
        name: Buffer.from(replyName),
        group: Buffer.from(''),
        filter: Buffer.from(replyFilter),
        maxInflight: this.maxInflight,
        ackPolicy: 1,
        deliverPolicy: 1, // DeliverPolicy::New — only future replies
        deliverMode: 0,
        ackWaitMs: 30_000,
        startSeq: 0n,
      }),
    )
    const replyConsumerId = Number(replyRef & 0xFFFFFFFFn)

    const handler = (frame: Buffer) => {
      const msg = new Message(
        frame, (f) => this.conn.send(f), () => this.conn.nextSeq(),
        undefined, undefined,
        (consumerId, seq) => this.conn.ackRelay.record(consumerId, seq),
        (consumerId, subjectHash, seq) => this.conn.enqueueAck(consumerId, subjectHash, seq),
      )
      svc._dispatch(msg)
    }

    await this.conn.sendSubscribeV2(workerConsumerId, Buffer.from(workerFilter), handler)
    await this.conn.sendSubscribeV2(replyConsumerId, Buffer.from(replyFilter), handler)
    return svc
  }
}
