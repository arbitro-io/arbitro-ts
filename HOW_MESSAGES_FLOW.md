# Cómo fluyen los mensajes en arbitro-ts

## FLUJO 1: `client.publish(subject, data)`

```
1. Llamada en el usuario
   client.publish("bench.msg", Buffer.from("hello"))

2. ArbitroClient.publish() [src/client/client.ts:82]
   → action = PubPublish (0x0101)
   → flags = NoAck (fire-and-forget, 0x01)
   → seq = nextSeq() (auto-increment bigint)
   → subject = "bench.msg" (with prefix if configured)
   → data = Buffer
   → Llama: this.conn.send(pack({...}))

3. pack() [src/proto/codec.ts:40]
   → requiresSubject(0x0101) = TRUE
   → payload = 2 + 9 + 5 = 16 bytes
   → Aloca: Buffer(32 + 16) = 48 bytes
   → Escribe header (32 bytes):
       [0xD4 0xC3 0xB2 0xA1] magic
       [0x02] version
       [0x01] flags (NoAck)
       [0x01 0x01] action
       [CRC computed after payload]
       [0x10 0x00 0x00 0x00] length=16
       [seq en bytes] sequence
       [0x00 0x00 ...] timestamp=0
   → Escribe payload (16 bytes):
       [0x09 0x00] subject length=9
       [b"bench.msg"] subject
       [b"hello"] data
   → Computa CRC32c(frame con crc=0)
   → Retorna Buffer de 48 bytes

4. Connection.send(frame) [src/net/connection.ts:83]
   → this.socket.write(frame)
   → Node.js TCP enqueue en outbound buffer
   → setNoDelay(true) asegura sin Nagle, envío inmediato

5. TCP stack → red → servidor Arbitro
```

**Total: 48 bytes en el wire, fire-and-forget (sin esperar respuesta)**

---

## FLUJO 2: `await client.publishAck(subject, data)`

```
1. Llamada en el usuario
   await client.publishAck("bench.msg", Buffer.from("hello"))

2. ArbitroClient.publishAck() [src/client/client.ts:93]
   → action = PubPublish (0x0101)
   → flags = None (0x00) ← NO ES NoAck
   → seq = nextSeq()
   → subject = "bench.msg"
   → data = Buffer
   → Llama: await this.conn.sendExpectReply(pack({...}))

3. pack() [como en FLUJO 1, pero flags=0x00]
   → Produce mismo frame de 48 bytes, pero sin flag NoAck

4. Connection.sendExpectReply() [src/net/connection.ts:89]
   → Crea Promise
   → Agrega callback a mgmtQ[]
   → socket.write(frame)
   → Retorna Promise que se resuelve cuando:
       a. onFrame() recibe RepOk (Action 0x0203)
       b. mgmtQ.shift() llama resolve(subId)
   → Si no hay respuesta en 5s → timeout error

5. Servidor procesa PubPublish sin NoAck
   → Journala el mensaje
   → Envia RepOk con subId en sequence field
   → Cliente receive RepOk → resolve Promise

6. Cliente retorna de await publishAck()
```

**Total: Request-reply round-trip, espera RepOk del servidor**

---

## FLUJO 3: `await client.subscribe(groupName, callback)`

```
1. Llamada en el usuario
   const sub = await client.subscribe("test-workers", (msg) => {
     console.log(msg.data())
     msg.ack()
   })

2. ArbitroClient.subscribe() [src/client/client.ts:118]
   → action = PubSubscribe (0x0102)
   → flags = None (0x00)
   → seq = nextSeq()
   → subject = "test-workers" ← nombre del consumer group
   → data = Buffer.alloc(0) ← vacío
   → Llama: await this.conn.sendExpectReply(pack({...}))

3. pack() [src/proto/codec.ts:40]
   → requiresSubject(0x0102) = TRUE
   → payload = 2 + 12 + 0 = 14 bytes
   → Frame = 32 + 14 = 46 bytes
   → Escribe:
       [header]
       [0x0C 0x00] subject length=12
       [b"test-workers"] subject
       [] data (vacío)

4. Connection.sendExpectReply()
   → Enqueue mgmt request
   → socket.write(46 bytes)
   → Espera RepOk del servidor

5. Servidor procesa PubSubscribe
   → Busca consumer con group="test-workers"
   → Asigna sub_id único (ej: 1000)
   → Envia RepOk:
       [header]
       [seq=1000] ← sub_id en el sequence field
       [timestamp=0]
       [] payload vacío

6. Cliente receive RepOk
   → readBigUInt64LE(OFF_SEQUENCE) = 1000
   → mgmtQ.shift().resolve(1000n)
   → subId = 1000n

7. Crea Subscription
   → new Subscription(1000n, conn, timeoutMs)
   → conn.registerRoute(1000n, (frame) => subscription.deliver(frame))

8. Retorna Subscription con callback registrado
   → Cuando servidor envie PubPublish delivery:
       [header]
       [timestamp=1000] ← sub_id aquí
       [subject del mensaje]
       [data del mensaje]
   → connection.onFrame() lee timestamp=1000
   → this.routes.get(1000n)?.(frame)
   → subscription.deliver(frame) llama callback(msg)
```

**Total: Subscribe es 1 round-trip (espera RepOk), luego push delivery async**

---

## FLUJO 4: `msg.ack()` — ACK frame

```
1. Usuario llamó msg.ack() desde el callback de subscribe

2. Message.ack() [implícito, connection.ts:104]
   → this.conn.sendAck(subId=1000n, msgSeq=42n)

3. Connection.sendAck() [src/net/connection.ts:104]
   → Crea frame:
       action = RepAck (0x0201)
       flags = None (0x00)
       seq = 42n ← mensaje sequence (para identificar cual ACK)
       timestamp = 1000n ← sub_id (para que servidor route el ACK)
       subject = Buffer.alloc(0)
       data = Buffer.alloc(0)
   → Llama: pack({...})

4. pack() [AQUÍ ESTÁ EL FIX]
   → requiresSubject(0x0201) = FALSE ← ¡Importante!
   → payload = (false ? 2 : 0) + 0 + 0 = 0 bytes ← SIN subject prefix
   → Frame = 32 + 0 = 32 bytes (SOLO header, sin payload)
   → Escribe header con length=0
   → NO escribe subject length prefix
   → CRC covers solo el header

5. Connection.send(frame) [no espera respuesta]
   → socket.write(32 bytes)
   → Fire-and-forget, no mgmt queue

6. Servidor recibe ACK
   → Decodifica frame con length=0
   → Sabe que NO hay payload
   → Extrae sub_id del timestamp=1000
   → Decrementa max_ack_pending para ese consumer
   → Si hay más mensajes pendientes de enviar → envia siguiente
```

**Total: ACK es 32 bytes, fire-and-forget, sin subject prefix**

---

## COMPARACIÓN: Rust vs TypeScript (DESPUÉS DEL FIX)

| Frame | Rust Action | Rust Payload | TypeScript Payload | ✓ Match |
|-------|-------------|--------------|-------------------|---------|
| PubPublish | requires_subject=TRUE | 2 + subj + data | 2 + subj + data | ✓ |
| PubSubscribe | requires_subject=TRUE | 2 + subj + data | 2 + subj + data | ✓ |
| PubCreateStream | requires_subject=TRUE | 2 + subj + data | 2 + subj + data | ✓ |
| RepAck | requires_subject=FALSE | data only (0 bytes) | data only (0 bytes) | ✓ |
| RepNack | requires_subject=FALSE | data only (0 bytes) | data only (0 bytes) | ✓ |
| RepOk | requires_subject=FALSE | data only (0 bytes) | data only (0 bytes) | ✓ |

---

## Resumen del Fix

**ANTES (INCORRECTO):**
```typescript
const payload = 2 + subjLen + opts.data.length  // SIEMPRE suma 2
frame.writeUInt16LE(subjLen, OFF_SUBJ_LEN)     // SIEMPRE escribe
```
→ ACK frames tenían `length=2` en lugar de `length=0` → servidor rechazaba

**DESPUÉS (CORRECTO):**
```typescript
const hasSubject = requiresSubject(opts.action)
const payload = (hasSubject ? 2 + subjLen : 0) + opts.data.length

if (hasSubject) {
  frame.writeUInt16LE(subjLen, OFF_SUBJ_LEN)
  subj.copy(frame, OFF_SUBJ)
  opts.data.copy(frame, OFF_SUBJ + subjLen)
} else {
  opts.data.copy(frame, HEADER_SIZE)
}
```
→ ACK frames ahora son 32 bytes con `length=0` ✓
