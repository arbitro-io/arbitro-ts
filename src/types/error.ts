export class ArbitroError extends Error {
  constructor(
    message: string,
    readonly code: 'connect' | 'timeout' | 'protocol' | 'server' | 'closed',
    readonly brokerName?: string,
    readonly brokerDetails?: unknown,
  ) {
    super(message)
    this.name = 'ArbitroError'
  }
}

export interface BrokerError {
  name: string
  message: string
  details?: unknown
}
