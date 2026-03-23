// Logger interface — compatible with pino's signature.
// Pass a pino instance directly: new ArbitroClient({ logger: pino() })
// Default (no logger provided) is fully silent — no output.

export type LogFn = {
  (msg: string): void
  (obj: object, msg: string): void
}

export interface Logger {
  trace: LogFn
  debug: LogFn
  info:  LogFn
  warn:  LogFn
  error: LogFn
  child(bindings: Record<string, unknown>): Logger
}

const noop = (() => {}) as LogFn

const NOOP: Logger = {
  trace: noop,
  debug: noop,
  info:  noop,
  warn:  noop,
  error: noop,
  child: () => NOOP,
}

export function resolveLogger(logger?: Logger): Logger {
  return logger ?? NOOP
}
