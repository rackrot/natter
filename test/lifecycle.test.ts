import { afterAll, beforeAll, expect, setDefaultTimeout, test } from 'bun:test'
import { mkdir, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startNats, type TestNats, until } from './support/nats'
import { type Session, startSession } from './support/session'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

const sessions: Session[] = []
const nats: TestNats[] = []
const worker = async (
  url: string,
  name: string,
  env: Record<string, string> = {},
) => {
  const s = await startSession({
    NATTER_ROLE: 'worker',
    NATTER_NAME: name,
    NATTER_URL: url,
    ...env,
  })
  sessions.push(s)
  return s
}
const overTls = (url: string) => url.replace('nats://', 'tls://')

afterAll(async () => {
  await Promise.all(sessions.map((s) => s.close()))
  await Promise.all(nats.map((n) => n.cleanup()))
  await rm(scratch, { recursive: true, force: true })
})

let up: TestNats
let scratch: string
beforeAll(async () => {
  up = await startNats()
  nats.push(up)
  scratch = await mkdtemp(join(tmpdir(), 'natter-lifecycle-'))
})

test('without NATTER_ROLE there is no NATS connection and no tools', async () => {
  const s = await startSession({ NATTER_URL: up.url, NATTER_NAME: 'plain' })
  sessions.push(s)
  expect((await s.client.listTools()).tools).toEqual([])
  await Bun.sleep(500)
  expect(await up.clientNames()).toEqual([])
  expect(s.errors).toEqual([])
})

test('handshake completes with NATS down, and the connection comes up once NATS starts', async () => {
  const down = await startNats({ autostart: false })
  nats.push(down)
  const s = await worker(down.url, 'late')

  await s.client.ping()
  expect(await down.clientNames().catch(() => [])).toEqual([])

  await down.start()
  await until(async () => (await down.clientNames()).includes('natter-late'))
  expect(s.errors).toEqual([])
})

test('closing stdin exits the process within 3s while NATS is connected', async () => {
  const s = await worker(up.url, 'orphan')
  await until(async () => (await up.clientNames()).includes('natter-orphan'))

  const started = Date.now()
  s.endStdin()
  await s.closed
  expect(Date.now() - started).toBeLessThan(3000)
  expect(await up.clientNames()).not.toContain('natter-orphan')
})

test('an unreadable seed keeps the session up with no broker client', async () => {
  const creds = join(scratch, 'creds')
  await mkdir(join(creds, 'natter.nk'), { recursive: true })
  const before = await up.totalConnections()

  const s = await worker(up.url, 'noread', { NATTER_CREDS_DIR: creds })
  await Bun.sleep(500)

  await s.client.ping()
  expect(s.errors).toEqual([])
  expect(await up.totalConnections()).toBe(before)
})

test('tls:// is refused by a plain server the client can otherwise reach', async () => {
  const plain = await startNats()
  nats.push(plain)
  const s = await worker(overTls(plain.url), 'needs-tls')

  // Two accepted dials: the client reached the server, gave up, and retried.
  await until(async () => (await plain.totalConnections()) >= 2, 8000)
  expect(await plain.clientNames()).not.toContain('natter-needs-tls')
  await s.client.ping()

  await worker(plain.url, 'plaintext-control')
  await until(async () =>
    (await plain.clientNames()).includes('natter-plaintext-control'),
  )
  expect(await plain.clientNames()).not.toContain('natter-needs-tls')
}, 15000)

test('tls:// connects to a TLS server only when its certificate is trusted', async () => {
  const certFile = join(scratch, 'cert.pem')
  const keyFile = join(scratch, 'key.pem')
  // biome-ignore format: flag and value pairs read better side by side
  const openssl = Bun.spawnSync(['openssl', 'req', '-x509', '-nodes',
    '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1',
    '-days', '1', '-subj', '/CN=natter-test',
    '-addext', 'subjectAltName=IP:127.0.0.1',
    '-keyout', keyFile, '-out', certFile,
  ])
  expect(openssl.exitCode).toBe(0)
  const secure = await startNats({ tls: { certFile, keyFile } })
  nats.push(secure)

  await worker(overTls(secure.url), 'untrusting')
  await worker(overTls(secure.url), 'trusting', { NATTER_CA_FILE: certFile })

  await until(async () =>
    (await secure.clientNames()).includes('natter-trusting'),
  )
  await until(async () => (await secure.totalConnections()) >= 3, 8000)
  expect(await secure.clientNames()).not.toContain('natter-untrusting')
}, 15000)

test('a gossiped server is never dialed', async () => {
  let dialed = false
  const gossiped = Bun.listen({
    hostname: '127.0.0.1',
    port: 0,
    socket: {
      open() {
        dialed = true
      },
      data() {},
    },
  })
  const info = {
    server_id: 'fake',
    version: '2.15.2',
    proto: 1,
    max_payload: 1048576,
    connect_urls: [`127.0.0.1:${gossiped.port}`],
  }
  let handshakes = 0
  const seeded = Bun.listen({
    hostname: '127.0.0.1',
    port: 0,
    socket: {
      open(socket) {
        socket.write(`INFO ${JSON.stringify(info)}\r\n`)
      },
      data(socket, data) {
        if (data.toString().includes('PING')) {
          handshakes++
          socket.write('PONG\r\n')
        }
      },
    },
  })

  try {
    await worker(`nats://127.0.0.1:${seeded.port}`, 'no-gossip')
    await until(async () => handshakes > 0)
    // Losing its server sends a client round its pool; the client's 2s
    // reconnect wait is what this outlasts.
    seeded.stop(true)
    await Bun.sleep(3000)
    expect(dialed).toBe(false)
  } finally {
    seeded.stop(true)
    gossiped.stop(true)
  }
}, 10000)
