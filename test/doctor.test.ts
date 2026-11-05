import { afterAll, beforeAll, expect, setDefaultTimeout, test } from 'bun:test'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { type JetStreamManager, jetstreamManager } from '@nats-io/jetstream'
import { createUser } from '@nats-io/nkeys'
import { connect, type NatsConnection } from '@nats-io/transport-node'
import { bootstrapStreams } from './support/fleet'
import { startNats, type TestNats } from './support/nats'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

let nats: TestNats
let nc: NatsConnection
let jsm: JetStreamManager
let credsDir: string
let seedText: string

beforeAll(async () => {
  nats = await startNats({ jetstream: true })
  nc = await connect({ servers: nats.url })
  jsm = await jetstreamManager(nc)
  await bootstrapStreams(jsm)
  credsDir = await mkdtemp(join(tmpdir(), 'natter-doctor-'))
  seedText = new TextDecoder().decode(createUser().getSeed())
  await writeFile(join(credsDir, 'natter.nk'), `${seedText}\n`, { mode: 0o600 })
})

afterAll(async () => {
  await nc?.close()
  await nats?.cleanup()
  if (credsDir) await rm(credsDir, { recursive: true, force: true })
})

/** Runs the doctor against `url`; the bound fails a hang instead of waiting on it. */
const doctor = async (url: string, env: Record<string, string> = {}) => {
  const started = Date.now()
  const proc = Bun.spawn(['bun', 'src/doctor.ts'], {
    env: {
      PATH: process.env.PATH ?? '',
      NATTER_URL: url,
      NATTER_CREDS_DIR: credsDir,
      ...env,
    },
    stdout: 'pipe',
    stderr: 'pipe',
    timeout: 10_000,
  })
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ])
  return { out: stdout + stderr, code, ms: Date.now() - started }
}

test('every check passes on a bootstrapped server, and the seed is never printed', async () => {
  const { out, code } = await doctor(nats.url)

  expect(code).toBe(0)
  expect(out).not.toMatch(/^FAIL/m)
  expect(out).not.toContain(seedText)
  expect(out).not.toContain(seedText.slice(2))
})

test('a missing stream fails by name and exits non-zero', async () => {
  await jsm.streams.delete('NATTER_DEFAULT_SENT')
  try {
    const { out, code } = await doctor(nats.url)

    expect(code).toBe(1)
    expect(out).toMatch(/^FAIL stream NATTER_DEFAULT_SENT:/m)
    expect(out).not.toMatch(/^FAIL stream NATTER_DEFAULT_TASKS:/m)
  } finally {
    await bootstrapStreams(jsm)
  }
})

test('a drifted stream fails by name and exits non-zero', async () => {
  const { config } = await jsm.streams.info('NATTER_DEFAULT_RESULTS')
  await jsm.streams.update('NATTER_DEFAULT_RESULTS', {
    ...config,
    max_msgs_per_subject: 5,
  })
  try {
    const { out, code } = await doctor(nats.url)

    expect(code).toBe(1)
    expect(out).toMatch(
      /^FAIL stream NATTER_DEFAULT_RESULTS:.*max_msgs_per_subject/m,
    )
  } finally {
    await bootstrapStreams(jsm)
  }
})

test('a server that is down fails at tcp, skips the rest and exits promptly', async () => {
  const down = await startNats({ autostart: false })
  try {
    const { out, code, ms } = await doctor(down.url)

    expect(code).toBe(1)
    expect(out).toMatch(/^FAIL tcp:/m)
    expect(out).toMatch(/^SKIP auth:/m)
    expect(ms).toBeLessThan(8000)
  } finally {
    await down.cleanup()
  }
})

test('tls:// against a plain server fails the tls check', async () => {
  const { out, code } = await doctor(nats.url.replace('nats://', 'tls://'))

  expect(code).toBe(1)
  expect(out).toMatch(/^PASS tcp:/m)
  expect(out).toMatch(/^FAIL tls:/m)
  expect(out).toMatch(/^SKIP auth:/m)
})

test('a worker with no consumer yet fails that check only', async () => {
  const { out, code } = await doctor(nats.url, {
    NATTER_ROLE: 'worker',
    NATTER_NAME: 'doctor-test',
  })

  expect(code).toBe(1)
  expect(out).toMatch(/^FAIL consumer task-doctor-test:/m)
  expect(out.match(/^FAIL/gm)).toHaveLength(1)
})
