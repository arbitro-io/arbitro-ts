// Ackstore durability bench — measures the REAL store in `src/ackstore`.
//
// One question: what does each durability setting cost? The answer drives the
// default (`fsync: false`, buffered), so it has to stay re-runnable.
//
//   record(), fsync per op   — sync() after every record
//   record(), batched fsync  — sync() every 100 records
//   record(), no fsync       — buffered writes, flushed at the end
//   seen()                   — memory bounds gate + exact set
//   confirmUpTo()            — bulk purge of a broker-confirmed range
//   recovery                 — replay a realistic backlog at open
//   on-disk size             — before and after compaction
//
// All I/O is real (fs.writeSync / fs.fsyncSync); no mocks, no simulated I/O.
//
// Historical note — this bench replaced a WAL-vs-better-sqlite3 comparison.
// Measured on the author's machine at N=5000, the hand-rolled WAL tied SQLite
// on fsync-bound writes (1,650/s vs 1,576/s), tied batched (114,607/s vs
// 115,874/s), won 3x with no fsync (1.54M/s vs 511k/s), recovered faster
// (4.7ms vs 5.5ms) and used less than half the disk after compaction (8 B/entry
// vs 18 B/entry) — with zero native dependencies. The WAL won; the comparison
// is settled and the SQLite implementation is gone. What remains worth
// measuring is the durability lever, which is what this file does.
//
// Usage:
//   npx tsx benches/ackstore.ts [--smoke] [--n N] [--data DIR]

import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

import { Wal } from '../src/ackstore'

const argv = process.argv.slice(2)
const smoke = argv.includes('--smoke')

function argString(flag: string, fallback: string): string {
  const i = argv.indexOf(flag)
  return i !== -1 && argv[i + 1] ? argv[i + 1]! : fallback
}

const N = Number.parseInt(argString('--n', smoke ? '200' : '5000'), 10)
const BATCH = 100
const LOOKUPS = smoke ? 2_000 : 200_000
const RECOVERY_RECORDS = Math.floor(N * 1.5)
const RECOVERY_CURSOR = BigInt(Math.floor(N / 2))
const ROOT = argString('--data', path.join(os.tmpdir(), 'arbitro-ackstore-bench'))

const STREAM = 'orders'
const CONSUMER = 'worker-1'

function timeMs(fn: () => void): number {
  const t0 = process.hrtime.bigint()
  fn()
  return Number(process.hrtime.bigint() - t0) / 1e6
}

function freshDir(name: string): string {
  const dir = path.join(ROOT, name)
  fs.rmSync(dir, { recursive: true, force: true })
  fs.mkdirSync(dir, { recursive: true })
  return dir
}

function rate(n: number, ms: number): string {
  return `${Math.round((n / ms) * 1000).toLocaleString('en-US')}/s`
}

function row(label: string, value: string): void {
  console.log(`  ${label.padEnd(30)} ${value}`)
}

// ── record() under the three durability settings ──────────────────────────

function benchRecord(label: string, dirName: string, fsyncOn: boolean, every: number): void {
  const wal = Wal.open({ dir: freshDir(dirName), fsync: fsyncOn })
  const slot = wal.slot(STREAM, CONSUMER)
  const ms = timeMs(() => {
    for (let i = 1; i <= N; i++) {
      slot.record(BigInt(i))
      if (every > 0 && i % every === 0) wal.sync()
    }
    wal.sync()
  })
  row(label, `${rate(N, ms)}  (${ms.toFixed(1)} ms)`)
  wal.close()
}

// ── main ───────────────────────────────────────────────────────────────────

console.log(`\nackstore bench — N=${N}${smoke ? ' (smoke)' : ''}, data=${ROOT}\n`)

console.log('record()')
benchRecord('fsync per op', 'fsync-each', true, 1)
benchRecord(`fsync every ${BATCH}`, 'fsync-batch', true, BATCH)
benchRecord('no fsync (default)', 'no-fsync', false, 0)

console.log('\nseen() / confirmUpTo()')
{
  const wal = Wal.open({ dir: freshDir('lookup'), fsync: false })
  const slot = wal.slot(STREAM, CONSUMER)
  for (let i = 1; i <= N; i++) slot.record(BigInt(i))
  wal.sync()

  let hits = 0
  const lookupMs = timeMs(() => {
    for (let i = 0; i < LOOKUPS; i++) if (slot.seen(BigInt((i % N) + 1))) hits++
  })
  row('seen(), memory index', `${rate(LOOKUPS, lookupMs)}  (${hits} hits)`)

  const purgeMs = timeMs(() => slot.confirmUpTo(BigInt(Math.floor(N / 2))))
  row(`confirmUpTo(${Math.floor(N / 2)})`, `${purgeMs.toFixed(2)} ms`)
  wal.close()
}

console.log('\nrecovery + disk')
{
  const dir = freshDir('recovery')
  const wal = Wal.open({ dir, fsync: true })
  const slot = wal.slot(STREAM, CONSUMER)
  for (let i = 1; i <= RECOVERY_RECORDS; i++) slot.record(BigInt(i))
  slot.confirmUpTo(RECOVERY_CURSOR)
  wal.sync()
  const live = wal.metrics().liveEntries
  const beforeBytes = wal.metrics().fileSize
  wal.close()

  let restored = 0
  const openMs = timeMs(() => {
    const w2 = Wal.open({ dir })
    restored = w2.metrics().liveEntries
    w2.close()
  })
  row(`replay ${RECOVERY_RECORDS} records`, `${openMs.toFixed(2)} ms  (${restored} live restored)`)

  const w3 = Wal.open({ dir, fsync: true })
  w3.compact()
  const afterBytes = w3.metrics().fileSize
  w3.close()
  row('disk before compaction', `${beforeBytes.toLocaleString('en-US')} B`)
  row('disk after compaction', `${afterBytes.toLocaleString('en-US')} B  (${(afterBytes / live).toFixed(1)} B/entry)`)
}

console.log()
fs.rmSync(ROOT, { recursive: true, force: true })
