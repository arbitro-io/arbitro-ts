---
description: V2 binary wire protocol — INVIOLABLE. Must stay in sync with `arbitro-proto` in the broker repo.
---

# WIRE PROTOCOL — V2

This is the contract this client speaks with `arbitro-server`. Every constant here mirrors the broker's `arbitro-proto` crate. If they ever disagree, the broker is the source of truth.

## Handshake

Client sends 8 bytes on connect, before any frame:

```
magic(4) "ARB2" (0x32425241 LE) | version(1)=2 | role(1)=0 (client) | caps(2)=0
```

If the broker rejects the handshake it closes the socket; no `RepError` frame is emitted at this stage.

## Frame header — 16 bytes, little-endian

```
action(2) | flags(1) | entry_flags(1) | msg_len(4) | seq(8)
```

Frame total length = `HEADER_SIZE(16) + msg_len`.

## Key constants

| Name | Value |
|---|---|
| `HEADER_SIZE` | `16` |
| `MAGIC_V2` | `0x32425241` |
| `HELLO_SIZE` | `8` |
| `OFF_ACTION` | `0` |
| `OFF_FLAGS` | `2` |
| `OFF_ENTRY_FLAGS` | `3` |
| `OFF_MSG_LEN` | `4` |
| `OFF_SEQ` | `8` |
| `Flag.AckReq` | `0x01` — publisher requests `RepOk` confirmation |

## Action codes (u16 LE)

| Code | Action | Direction |
|------|--------|-----------|
| `0x0101` | Publish | C→S |
| `0x0103` | PublishBatch | C→S |
| `0x0104` | PublishWithReply | C→S |
| `0x0201` | Ack | C→S (fire-and-forget) |
| `0x0202` | Nack | C→S (fire-and-forget) |
| `0x0206` | BatchAck | C→S (fire-and-forget) |
| `0x020A` | BatchNack | C→S (fire-and-forget) |
| `0x0301` | Subscribe | C→S |
| `0x0302` | Unsubscribe | C→S |
| `0x0401` | CreateStream | C→S |
| `0x0402` | DeleteStream | C→S |
| `0x0403` | GetStream | C→S |
| `0x0404` | ListStreams | C→S |
| `0x0405` | PurgeStream | C→S |
| `0x0406` | DrainSubject | C→S |
| `0x0501` | CreateConsumer | C→S |
| `0x0502` | DeleteConsumer | C→S |
| `0x0503` | GetConsumer | C→S |
| `0x0504` | ListConsumers | C→S |
| `0x0505` | ConsumerStats | C→S |
| `0x0601` | Ping | C→S |
| `0x0602` | Pong | S→C |
| `0x0605` | Disconnect | C→S |
| `0x0701` | RepOk | S→C |
| `0x0702` | RepError | S→C |
| `0x0703` | Deliver | S→C |
| `0x0704` | RepBatch | S→C |

## Server replies — exact sizes

- **`RepOk`**: Header(16) + `ref_seq`(8) = **24 B total**
- **`RepError`**: Header(16) + `ref_seq`(8) + `error_code`(2) + `_pad`(6) = **32 B total**
- **`Deliver`**: Header(16) + `consumer_id`(4) + `subject_hash`(4) + `subject_len`(2) + `_pad`(2) + subject + payload

## `stream_id`

`stream_id` is the server-returned `wire_hash_32` (foldhash) of the stream name. The CLIENT MUST NOT compute it — cache the value from the `CreateStream` / `GetStream` `RepOk` body. Computing it client-side risks foldhash version drift between client and broker.

## Forbidden

- Hex literals for action codes anywhere in source. Use the `Action` const enum.
- Hand-rolled byte offsets. Use the `OFF_*` constants.
- Skipping the handshake on reconnect. Every new socket sends `HELLO` first.
- Sending `0x0701`–`0x0704` from the client side. Those are S→C only.
