export class ArbitroError extends Error {
  constructor(
    message: string,
    readonly code: 'connect' | 'timeout' | 'protocol' | 'server' | 'closed',
  ) {
    super(message)
    this.name = 'ArbitroError'
  }
}
