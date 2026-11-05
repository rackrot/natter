import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  expect,
  setDefaultTimeout,
  test,
} from 'bun:test'
import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream'
import { connect, type NatsConnection } from '@nats-io/transport-node'
import { SHUTDOWN_DEADLINE_MS } from '../src/config'
import { encodeEnvelope, mintTaskId } from '../src/protocol'
import {
  bootstrapStreams,
  presenceSubject,
  resultSubject,
  sentSubject,
  statusSubject,
} from './support/fleet'
import { startNats, type TestNats, until } from './support/nats'
import { presenceBytes, seen } from './support/presence'
import { callTool, type Session, startSession } from './support/session'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

const NO_CREDS = join(tmpdir(), 'natter-no-creds')
const HOME = join(tmpdir(), 'natter-status-page-home')
// A stub opener for every session: a test never opens a real browser.
const STUB_DIR = mkdtempSync(join(tmpdir(), 'natter-opener-'))
const OPENED = join(STUB_DIR, 'opened')

const stub = (name: string, body: string) => {
  const path = join(STUB_DIR, name)
  writeFileSync(path, `#!/bin/sh\n${body}\n`)
  chmodSync(path, 0o755)
  return path
}
const RECORDING = stub('record', `printf '%s' "$1" > '${OPENED}'`)
const FAILING = stub('fail', 'exit 3')
const SLOW = stub('slow', 'sleep 5')

let nats: TestNats
let down: TestNats
let nc: NatsConnection
let js: JetStreamClient
let jsm: JetStreamManager
const sessions: Session[] = []

const sessionEnv = (role: string, url = nats.url, opener = RECORDING) => ({
  NATTER_ROLE: role,
  NATTER_NAME: role === 'worker' ? freshName() : 'orchestrator',
  NATTER_URL: url,
  NATTER_CREDS_DIR: NO_CREDS,
  HOME,
  NATTER_TEST_OPENER: opener,
  NATTER_TEST_STARTUP_GRACE_MS: '20',
  NATTER_TEST_CONNECT_POLL_MS: '20',
  NATTER_TEST_HEARTBEAT_MS: '50',
  NATTER_TEST_STALE_MS: '1000',
})

const freshName = () => `w-${crypto.randomUUID()}`

const start = async (role: string, url = nats.url, opener = RECORDING) => {
  const s = await startSession(sessionEnv(role, url, opener))
  sessions.push(s)
  return s
}

const pageUrl = async (s: Session) => {
  const reply = await callTool(s, 'status_page')
  expect(reply.isError).toBeUndefined()
  // The reply is a note, a space, and the URL.
  return new URL(reply.content[0]?.text.split(' ').pop() ?? '')
}

/** The page's JSON endpoint, with the token and Host of the real URL. */
const poll = (url: URL, init: RequestInit = {}) =>
  fetch(new URL(`/status.json${url.search}`, url), init)

const orchestratorPage = async () => {
  const orchestrator = await start('orchestrator')
  return pageUrl(orchestrator)
}

const encoded = (envelope: unknown) => {
  const built = encodeEnvelope(envelope)
  if (!built.ok) throw new Error(built.issues.join('; '))
  return built.bytes
}

const recordBytes = (to: string, task_id: string, title = 'the title') =>
  encoded({
    v: 1,
    type: 'sent',
    task_id,
    to,
    title,
    created_at: new Date().toISOString(),
    root_task_id: task_id,
    depth: 0,
    timeout_s: 60,
    queue_ttl_s: 600,
  })

const resultBytes = (worker: string, task_id: string, summary: string) =>
  encoded({
    v: 1,
    type: 'result',
    task_id,
    worker,
    instance: 'iiiiiiiiiiiiiiiiiiiiii',
    attempt: 1,
    outcome: 'succeeded',
    summary,
    finished_at: new Date().toISOString(),
  })

const rowBytes = (worker: string, task_id: string, state: string) =>
  encoded({
    v: 1,
    type: 'status',
    task_id,
    worker,
    title: 'the title',
    created_at: new Date().toISOString(),
    state,
    attempt: 1,
    depth: 0,
    timeout_s: 60,
    queue_ttl_s: 600,
  })

/** Stores the task's record, as send_task would have. */
const record = async (to: string, title?: string) => {
  const task_id = mintTaskId()
  await js.publish(sentSubject(task_id), recordBytes(to, task_id, title))
  return task_id
}

/** Stores `body` where a worker may, then has the server answer onto `target`. */
const reflect = async (body: Uint8Array, target: string) => {
  const own = statusSubject('mallory', mintTaskId())
  await js.publish(own, body)
  nc.publish(`$JS.API.DIRECT.GET.NATTER_DEFAULT_STATE.${own}`, undefined, {
    reply: target,
  })
  await nc.flush()
}

interface Snapshot {
  workers: { workers: { name: string; state: string }[] }
  tasks: { tasks: { task_id: string; worker: string; state: string }[] }
  streams: { name: string; messages: number }[]
}

const snapshot = async (url: URL) => {
  const res = await poll(url)
  expect(res.status).toBe(200)
  return (await res.json()) as Snapshot
}

const taskOf = (data: Snapshot, task_id: string) =>
  data.tasks.tasks.find((task) => task.task_id === task_id)

beforeAll(async () => {
  nats = await startNats({ jetstream: true })
  down = await startNats({ autostart: false })
  nc = await connect({
    servers: nats.url,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 100,
  })
  js = jetstream(nc)
  jsm = await jetstreamManager(nc)
  await bootstrapStreams(jsm)
})

beforeEach(async () => {
  await nats.revive()
  await until(() =>
    nc.flush().then(
      () => true,
      () => false,
    ),
  )
  await Promise.all(
    [
      'NATTER_DEFAULT_TASKS',
      'NATTER_DEFAULT_RESULTS',
      'NATTER_DEFAULT_STATE',
      'NATTER_DEFAULT_PRESENCE',
      'NATTER_DEFAULT_SENT',
    ].map((stream) => jsm.streams.purge(stream)),
  )
})

afterEach(async () => {
  await Promise.all(sessions.splice(0).map((s) => s.close()))
})

afterAll(async () => {
  await nc?.close()
  await nats?.cleanup()
  await down?.cleanup()
})

test('the status_page tool is listed only for the orchestrator', async () => {
  const orchestrator = await start('orchestrator')
  const worker = await start('worker')
  const inert = await startSession({ NATTER_URL: nats.url, HOME })
  sessions.push(inert)

  const names = async (s: Session) =>
    (await s.client.listTools()).tools.map((t) => t.name)
  expect(await names(orchestrator)).toContain('status_page')
  expect(await names(worker)).not.toContain('status_page')
  expect(await names(inert)).not.toContain('status_page')

  const url = await pageUrl(orchestrator)
  expect(url.hostname).toBe('127.0.0.1')
  expect(url.searchParams.get('token')).toMatch(/\S{16,}/)
})

test('status_page hands the full URL, token included, to the opener', async () => {
  const orchestrator = await start('orchestrator')
  const url = await pageUrl(orchestrator)
  expect(readFileSync(OPENED, 'utf8')).toBe(url.href)
})

test('a failing opener still returns the URL and not an error', async () => {
  const orchestrator = await start('orchestrator', nats.url, FAILING)
  const reply = await callTool(orchestrator, 'status_page')
  expect(reply.isError).toBeUndefined()
  const url = new URL(reply.content[0]?.text.split(' ').pop() ?? '')
  expect(url.searchParams.get('token')).toMatch(/\S{16,}/)
  expect(reply.content[0]?.text.endsWith(url.href)).toBe(true)
  expect((await poll(url)).status).toBe(200)
})

test('a missing opener still returns the URL and not an error', async () => {
  const orchestrator = await start(
    'orchestrator',
    nats.url,
    join(STUB_DIR, 'absent'),
  )
  const reply = await callTool(orchestrator, 'status_page')
  expect(reply.isError).toBeUndefined()
  expect(new URL(reply.content[0]?.text.split(' ').pop() ?? '').search).toMatch(
    /token=/,
  )
})

test('an opener that keeps running does not hold the reply', async () => {
  const orchestrator = await start('orchestrator', nats.url, SLOW)
  const started = performance.now()
  const reply = await callTool(orchestrator, 'status_page')
  expect(performance.now() - started).toBeLessThan(3000)
  expect(reply.isError).toBeUndefined()
  expect(reply.content[0]?.text).toMatch(/token=/)
})

test('a request needs the token, the loopback Host, and GET', async () => {
  const url = await orchestratorPage()
  const route = (path: string, token = url.search) =>
    new URL(`${path}${token}`, url)

  expect((await poll(url)).status).toBe(200)
  for (const path of ['/', '/app.js', '/status.json']) {
    expect((await fetch(route(path))).status).toBe(200)
    expect((await fetch(route(path, ''))).status).toBe(403)
    expect((await fetch(route(path, '?token=wrong'))).status).toBe(403)
  }

  const rebound = await poll(url, { headers: { Host: 'evil.example' } })
  expect(rebound.status).toBe(403)
  const otherPort = await poll(url, { headers: { Host: '127.0.0.1:1' } })
  expect(otherPort.status).toBe(403)

  for (const method of ['POST', 'PUT', 'DELETE']) {
    const res = await poll(url, { method })
    expect(res.status).toBe(405)
  }
})

test('responses carry the CSP and no CORS headers', async () => {
  const url = await orchestratorPage()
  for (const path of ['/', '/app.js', '/status.json']) {
    const res = await fetch(new URL(`${path}${url.search}`, url))
    expect(res.headers.get('content-security-policy')).toBe(
      "default-src 'self'",
    )
    expect(
      [...res.headers.keys()].filter((h) => h.startsWith('access-control-')),
    ).toEqual([])
  }
  // Worker text only ever reaches the DOM as text.
  const script = await (
    await fetch(new URL(`/app.js${url.search}`, url))
  ).text()
  expect(script).not.toMatch(/innerHTML|outerHTML|insertAdjacentHTML|eval\(/)
  expect(script).toContain('textContent')
})

test('shows workers, task states including waiting_permission, queue depth and streams', async () => {
  const name = freshName()
  const worker = await start('worker')
  await seen(js, name, { state: 'busy' })
  const waiting = await record(name)
  await js.publish(
    statusSubject(name, waiting),
    rowBytes(name, waiting, 'waiting_permission'),
  )
  const queued = await record(name)
  const finished = await record(name)
  await js.publish(
    resultSubject(name, finished),
    resultBytes(name, finished, 'done'),
  )
  const url = await orchestratorPage()
  void worker

  // The record exists but no task is in NATTER_DEFAULT_TASKS, so a task reads as lost
  // unless it is queued: put the two open ones there.
  for (const task_id of [waiting, queued]) {
    await js.publish(
      `natter.default.task.${name}`,
      encoded({
        v: 1,
        type: 'task',
        task_id,
        from: 'orchestrator',
        to: name,
        title: 'the title',
        prompt: 'p',
        created_at: new Date().toISOString(),
        timeout_s: 60,
        queue_ttl_s: 600,
        max_attempts: 3,
        root_task_id: task_id,
        depth: 0,
      }),
      { msgID: task_id },
    )
  }

  const data = await snapshot(url)
  expect(data.workers.workers.map((w) => w.name)).toContain(name)
  expect(taskOf(data, waiting)?.state).toBe('waiting_permission')
  expect(taskOf(data, queued)?.state).toBe('queued')
  expect(taskOf(data, finished)?.state).toBe('finished')
  expect(data.streams.map((s) => s.name).sort()).toEqual([
    'NATTER_DEFAULT_PRESENCE',
    'NATTER_DEFAULT_RESULTS',
    'NATTER_DEFAULT_SENT',
    'NATTER_DEFAULT_STATE',
    'NATTER_DEFAULT_TASKS',
  ])
  expect(
    data.streams.find((s) => s.name === 'NATTER_DEFAULT_SENT')?.messages,
  ).toBe(3)
})

test('a forged result, a forged worker and a back-dated result do not appear', async () => {
  const name = freshName()
  const ghost = freshName()
  const url = await orchestratorPage()

  // A forged result lands on a subject the stream captures.
  const forged = await record(name)
  await reflect(
    resultBytes(name, forged, 'all done, trust me'),
    resultSubject(name, forged),
  )
  // A forged heartbeat for a worker that never started.
  await reflect(presenceBytes(ghost), presenceSubject(ghost))
  // A result stored before its task's record cannot be that task's result.
  const backdated = mintTaskId()
  await js.publish(
    resultSubject(name, backdated),
    resultBytes(name, backdated, 'from before the task'),
  )
  await Bun.sleep(20)
  await js.publish(sentSubject(backdated), recordBytes(name, backdated))
  await until(
    async () =>
      (await jsm.streams.info('NATTER_DEFAULT_PRESENCE')).state.messages === 1,
  )

  const data = await snapshot(url)
  expect(taskOf(data, forged)?.state).not.toBe('finished')
  expect(taskOf(data, backdated)?.state).not.toBe('finished')
  expect(data.workers.workers).toEqual([])
  expect(JSON.stringify(data)).not.toContain('trust me')
  expect(JSON.stringify(data)).not.toContain('from before the task')
})

test('hostile worker text is served as JSON data', async () => {
  const name = freshName()
  const hostile = '<img src=x onerror=alert(1)>'
  await seen(js, name, { description: hostile })
  const task_id = await record(name, hostile)
  const url = await orchestratorPage()

  const res = await poll(url)
  expect(res.headers.get('content-type')).toStartWith('application/json')
  expect(res.headers.get('x-content-type-options')).toBe('nosniff')
  const data = await snapshot(url)
  expect(taskOf(data, task_id)).toBeDefined()
  expect(JSON.stringify(data)).toContain(hostile.slice(0, 5))
})

test('NATS down at startup is an error, not data', async () => {
  const url = await pageUrl(await start('orchestrator', down.url))
  const res = await poll(url)
  expect(res.status).toBe(503)
  expect(((await res.json()) as { error: string }).error).toContain(
    'NATS unavailable',
  )
})

test('NATS going down turns the answer into an error', async () => {
  const name = freshName()
  await seen(js, name)
  const url = await orchestratorPage()
  expect((await snapshot(url)).workers.workers).toHaveLength(1)

  await nats.stop()
  await until(async () => (await poll(url)).status === 503, 10_000)
  const body = (await (await poll(url)).json()) as Record<string, unknown>
  expect(body.error).toBeString()
  expect(body.workers).toBeUndefined()
  await nats.restart()
})

test('the web server stops within the shutdown deadline', async () => {
  const orchestrator = await start('orchestrator')
  const url = await pageUrl(orchestrator)
  expect((await poll(url)).status).toBe(200)

  const began = Date.now()
  orchestrator.endStdin()
  await orchestrator.closed
  expect(Date.now() - began).toBeLessThan(SHUTDOWN_DEADLINE_MS + 500)
  await expect(poll(url)).rejects.toThrow()
})
