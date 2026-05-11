// Core frame allocator — single source for header layout.

import {
  HEADER_SIZE, HELLO_SIZE, MAGIC_V2, CURRENT_VERSION,
  OFF_ACTION, OFF_FLAGS, OFF_ENTRY_FLAGS, OFF_MSG_LEN, OFF_SEQ,
  Action, Role, Cap,
} from './constants'

/** Allocate HEADER_SIZE + bodyLen, write the 16-byte header, return buf. */
export function frame(
  action: Action, seq: bigint, bodyLen: number,
  flags = 0, entryFlags = 0,
): Buffer {
  const buf = Buffer.allocUnsafe(HEADER_SIZE + bodyLen)
  buf.writeUInt16LE(action, OFF_ACTION)
  buf[OFF_FLAGS]       = flags
  buf[OFF_ENTRY_FLAGS] = entryFlags
  buf.writeUInt32LE(bodyLen, OFF_MSG_LEN)
  buf.writeBigUInt64LE(seq, OFF_SEQ)
  return buf
}

/** Hello handshake — 8 bytes, no Header prefix. */
export function packHello(caps: number = Cap.Reply): Buffer {
  const buf = Buffer.allocUnsafe(HELLO_SIZE)
  buf.writeUInt32LE(MAGIC_V2, 0)
  buf[4] = CURRENT_VERSION
  buf[5] = Role.Client
  buf.writeUInt16LE(caps, 6)
  return buf
}

export function packPing(seq: bigint): Buffer {
  return frame(Action.Ping, seq, 0)
}

export function packDisconnect(seq: bigint): Buffer {
  return frame(Action.Disconnect, seq, 0)
}
