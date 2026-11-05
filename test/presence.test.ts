import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  expect,
  setDefaultTimeout,
  test,
} from 'bun:test'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import {
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream'
import {
  connect,
  headers,
  type NatsConnection,
  nanos,
} from '@nats-io/transport-node'
import { OVERDUE_MARGIN_S, resolveConfig } from '../src/config'
import { startConnection } from '../src/connection'
import { NATS_UNAVAILABLE, UNKNOWN_WORKER } from '../src/orchestrator'
import {
  encodeEnvelope,
  mintTaskId,
  type Presence,
  type Sent,
  type Status,
  type Task,
} from '../src/protocol'
import { createWorker } from '../src/worker'
import {
  bootstrapStreams,
  presenceSubject,
  resultSubject,
  sentSubject,
  statusSubject,
  taskSubject,
} from './support/fleet'
import { startNats, type TestNats, until } from './support/nats'
import { presenceBytes, seen } from './support/presence'
import {
  callTool,
  type Session,
  startSession,
  type ToolReply,
} from './support/session'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

const HEARTBEAT_MS = 50
const STALE_MS = 1000
const SWEEP_MS = 100
const ACCEPT_TIMEOUT_MS = 200
// Enough sweeps for a second push of anything to have landed.
const SWEEPS_MS = 6 * SWEEP_MS
const NO_CREDS = join(tmpdir(), 'natter-no-creds')

let nats: TestNats
let nc: NatsConnection
let js: JetStreamClient
let jsm: JetStreamManager
const sessions: Session[] = []

const sessionEnv = (url: string) => ({
  NATTER_URL: url,
  NATTER_CREDS_DIR: NO_CREDS,
  NATTER_TEST_STARTUP_GRACE_MS: '20',
  NATTER_TEST_RESTART_MIN_MS: '100',
  NATTER_TEST_RESTART_MAX_MS: '300',
  NATTER_TEST_CONNECT_POLL_MS: '20',
  NATTER_TEST_HEARTBEAT_MS: String(HEARTBEAT_MS),
  NATTER_TEST_STALE_MS: String(STALE_MS),
  NATTER_TEST_ATTENTION_SWEEP_MS: String(SWEEP_MS),
  // Out of the way: test/limits.test.ts owns the push budgets.
  NATTER_TEST_ATTENTION_BUDGET: '100000',
  NATTER_TEST_PUSH_BUDGET: '100000',
})

const startWorker = async (
  name: string,
  env: Record<string, string> = {},
  url = nats.url,
) => {
  const s = await startSession({
    ...sessionEnv(url),
    NATTER_ROLE: 'worker',
    NATTER_NAME: name,
    ...env,
  })
  sessions.push(s)
  return s
}

const startOrchestrator = async (
  env: Record<string, string> = {},
  url = nats.url,
) => {
  const s = await startSession({
    ...sessionEnv(url),
    NATTER_ROLE: 'orchestrator',
    NATTER_NAME: 'orchestrator',
    ...env,
  })
  sessions.push(s)
  await until(async () => {
    const reply = await callTool(s, 'list_workers')
    return reply.content[0]?.text !== NATS_UNAVAILABLE
  })
  return s
}

const freshName = () => `w-${crypto.randomUUID()}`

const parsed = (reply: ToolReply) => {
  expect(reply.isError).toBeFalsy()
  return JSON.parse(reply.content[0]?.text ?? '')
}

interface WorkerRow {
  name: string
  repo: string
  host: string
  description: string
  state: Presence['state']
  task_id?: string
  stale: boolean
  age_s: number
}

const listing = async (s: Session) =>
  parsed(await callTool(s, 'list_workers')) as {
    workers: WorkerRow[]
    truncated: boolean
  }

const rowOf = async (s: Session, name: string) =>
  (await listing(s)).workers.find((w) => w.name === name)

const rowMatches = async (
  s: Session,
  name: string,
  match: (row: WorkerRow) => boolean,
  ms?: number,
) =>
  until(async () => {
    const row = await rowOf(s, name)
    return row !== undefined && match(row)
  }, ms)

const send = async (s: Session, to: string) =>
  parsed(
    await callTool(s, 'send_task', { to, title: 'the title', prompt: 'p' }),
  ) as { task_id: string; worker_state: string }

const signal = (s: Session, sig: NodeJS.Signals) => {
  const { pid } = s.client.transport as StdioClientTransport
  if (pid === null) throw new Error('session has no process')
  process.kill(pid, sig)
}

const sigkill = (s: Session) => signal(s, 'SIGKILL')

const attention = (s: Session) =>
  s.pushes.filter((push) => push.meta.kind === 'attention')

const staleFor = (s: Session, task_id: string) =>
  attention(s).filter(
    ({ meta }) => meta.task_id === task_id && meta.reason === 'stale',
  )

/** A task as the orchestrator would record it, with only `fields` set here. */
const writeRecord = async (
  to: string,
  fields: Partial<Sent> = {},
  conn = js,
) => {
  const task_id = fields.task_id ?? mintTaskId()
  const encoded = encodeEnvelope({
    v: 1,
    type: 'sent',
    task_id,
    to,
    title: 'the title',
    created_at: new Date().toISOString(),
    root_task_id: task_id,
    depth: 0,
    timeout_s: 60,
    queue_ttl_s: 60,
    ...fields,
  })
  if (!encoded.ok) throw new Error(encoded.issues.join('; '))
  await conn.publish(sentSubject(task_id), encoded.bytes)
  return task_id
}

/** Writes a status row naming worker `from`, by default on its own subject. */
const writeRow = async (
  from: string,
  task_id: string,
  state: Status['state'],
  subjectWorker = from,
  conn = js,
) => {
  const encoded = encodeEnvelope({
    v: 1,
    type: 'status',
    task_id,
    worker: from,
    title: 'the title',
    created_at: new Date().toISOString(),
    state,
    attempt: 1,
    depth: 0,
    timeout_s: 60,
    queue_ttl_s: 60,
  })
  if (!encoded.ok) throw new Error(encoded.issues.join('; '))
  await conn.publish(statusSubject(subjectWorker, task_id), encoded.bytes)
}

const writeResult = async (worker: string, task_id: string, conn = js) => {
  const encoded = encodeEnvelope({
    v: 1,
    type: 'result',
    task_id,
    worker,
    instance: 'iiiiiiiiiiiiiiiiiiiiii',
    attempt: 1,
    outcome: 'succeeded',
    summary: 'done',
    finished_at: new Date().toISOString(),
  })
  if (!encoded.ok) throw new Error(encoded.issues.join('; '))
  await conn.publish(resultSubject(worker, task_id), encoded.bytes)
}

const age = (seconds: number) =>
  new Date(Date.now() - seconds * 1000).toISOString()

/** The record of a task long past its deadline. */
const overdueFields = { created_at: age(2 * 3600) } as const

/** A private server, for tests that shrink a stream or need it never used. */
const onFreshServer = async (
  run: (
    server: TestNats,
    conn: JetStreamClient,
    manager: JetStreamManager,
  ) => Promise<void>,
) => {
  const server = await startNats({ jetstream: true })
  const conn = await connect({ servers: server.url })
  const manager = await jetstreamManager(conn)
  try {
    await bootstrapStreams(manager)
    await run(server, jetstream(conn), manager)
  } finally {
    await Promise.all(sessions.splice(0).map((s) => s.close()))
    await conn.close()
    await server.cleanup()
  }
}

beforeAll(async () => {
  nats = await startNats({ jetstream: true })
  nc = await connect({
    servers: nats.url,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 100,
  })
  js = jetstream(nc)
  jsm = await jetstreamManager(nc)
  await bootstrapStreams(jsm)
})

// A test that times out takes the shared server with it; the next one must not.
// The sweep reads every record on the account, so each test starts without any.
beforeEach(async () => {
  await nats.revive()
  await until(() =>
    nc.flush().then(
      () => true,
      () => false,
    ),
  )
  await jsm.consumers
    .delete('NATTER_DEFAULT_RESULTS', 'results')
    .catch(() => {})
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
})

// ------------------------------------------------------------- list_workers

test('a live worker is listed, with what it says about itself', async () => {
  const name = freshName()
  const project = await mkdtemp(join(tmpdir(), 'natter-repo-'))
  try {
    await mkdir(join(project, '.claude'))
    await writeFile(
      join(project, '.claude', 'natter.json'),
      JSON.stringify({ description: 'the worker of this repo' }),
    )
    const orchestrator = await startOrchestrator()
    await startWorker(name, { CLAUDE_PROJECT_DIR: project })

    await rowMatches(orchestrator, name, (row) => row.state === 'idle')
    const row = await rowOf(orchestrator, name)

    expect(row).toMatchObject({
      name,
      repo: project.split('/').at(-1),
      description: 'the worker of this repo',
      state: 'idle',
      stale: false,
    })
    expect(row?.host).not.toBe('')
    expect(row?.age_s).toBeGreaterThanOrEqual(0)
    expect(row?.task_id).toBeUndefined()
  } finally {
    await rm(project, { recursive: true, force: true })
  }
})

test('a killed worker goes stale and send_task reports it', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const worker = await startWorker(name)
  await rowMatches(orchestrator, name, (row) => !row.stale)

  sigkill(worker)

  await rowMatches(orchestrator, name, (row) => row.stale)
  expect((await rowOf(orchestrator, name))?.state).toBe('idle')
  expect((await send(orchestrator, name)).worker_state).toBe('stale')
})

test('a clean exit shows offline, and heartbeats stop with it', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const worker = await startWorker(name)
  await rowMatches(orchestrator, name, (row) => row.state === 'idle')
  const beats: Presence['state'][] = []
  nc.subscribe(presenceSubject(name), {
    callback: (_, msg) => {
      beats.push((msg.json() as Presence).state)
    },
  })

  worker.endStdin()
  await worker.closed
  await nc.flush()

  await rowMatches(orchestrator, name, (row) => row.state === 'offline')
  const atExit = beats.length
  await Bun.sleep(4 * HEARTBEAT_MS)
  expect(beats).toHaveLength(atExit)
  expect(beats.at(-1)).toBe('offline')
})

test('an offline worker still takes tasks, and send_task reports its state', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  await seen(js, name, { state: 'offline' })

  const { task_id, worker_state } = await send(orchestrator, name)

  expect(worker_state).toBe('offline')
  expect(task_id).toBeDefined()
  const subject = taskSubject(name)
  const info = await jsm.streams.info('NATTER_DEFAULT_TASKS', {
    subjects_filter: subject,
  })
  expect(info.state.subjects?.[subject]).toBe(1)
})

test('an offline record stays offline, however old it gets', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  await seen(js, name, { state: 'offline' })

  await rowMatches(orchestrator, name, (row) => row.stale)

  expect((await send(orchestrator, name)).worker_state).toBe('offline')
})

test('a busy worker that exits goes straight to offline', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const beats: Presence['state'][] = []
  const sub = nc.subscribe(presenceSubject(name), {
    callback: (_, msg) => {
      beats.push((msg.json() as Presence).state)
    },
  })
  await nc.flush()
  const worker = await startWorker(name, { NATTER_TEST_HEARTBEAT_MS: '60000' })
  await rowMatches(orchestrator, name, (row) => row.state === 'idle')
  await send(orchestrator, name)
  await rowMatches(orchestrator, name, (row) => row.state === 'busy')

  worker.endStdin()
  await worker.closed
  await rowMatches(orchestrator, name, (row) => row.state === 'offline')
  await nc.flush()
  sub.unsubscribe()

  expect(beats).toEqual(['idle', 'busy', 'offline'])
})

test('a busy worker shows its task until it finishes', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const worker = await startWorker(name)
  await rowMatches(orchestrator, name, (row) => row.state === 'idle')

  const { task_id, worker_state } = await send(orchestrator, name)
  expect(worker_state).toBe('idle')

  await rowMatches(orchestrator, name, (row) => row.state === 'busy')
  expect((await rowOf(orchestrator, name))?.task_id).toBe(task_id)
  expect((await send(orchestrator, name)).worker_state).toBe('busy')

  await callTool(worker, 'update_task', { task_id })
  await callTool(worker, 'finish_task', {
    task_id,
    outcome: 'succeeded',
    summary: 'done',
  })

  await rowMatches(orchestrator, name, (row) => row.task_id !== task_id)
})

test('an accept release shows stalled until a late tool call clears the latch', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const worker = await startWorker(name, {
    NATTER_TEST_ACCEPT_TIMEOUT_MS: String(ACCEPT_TIMEOUT_MS),
  })
  await rowMatches(orchestrator, name, (row) => row.state === 'idle')

  const { task_id } = await send(orchestrator, name)

  await rowMatches(orchestrator, name, (row) => row.state === 'stalled')
  expect((await rowOf(orchestrator, name))?.task_id).toBe(task_id)
  expect((await send(orchestrator, name)).worker_state).toBe('stalled')

  const late = await callTool(worker, 'update_task', { task_id })
  expect(late.isError).toBe(true)

  await rowMatches(orchestrator, name, (row) => row.state !== 'stalled')
})

test('every state change goes out at once, not at the next beat', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const beats: Presence['state'][] = []
  const sub = nc.subscribe(presenceSubject(name), {
    callback: (_, msg) => {
      beats.push((msg.json() as Presence).state)
    },
  })
  await nc.flush()
  const worker = await startWorker(name, {
    NATTER_TEST_HEARTBEAT_MS: '60000',
    NATTER_TEST_ACCEPT_TIMEOUT_MS: String(ACCEPT_TIMEOUT_MS),
  })
  await rowMatches(orchestrator, name, (row) => row.state === 'idle')

  const { task_id } = await send(orchestrator, name)
  await rowMatches(orchestrator, name, (row) => row.state === 'stalled')
  await callTool(worker, 'update_task', { task_id })
  await rowMatches(orchestrator, name, (row) => row.state === 'idle')
  await nc.flush()
  sub.unsubscribe()

  expect(beats).toEqual(['idle', 'busy', 'stalled', 'idle'])
})

test('above 2000 subjects list_workers still shows the newest 200', async () => {
  const orchestrator = await startOrchestrator()
  const names = Array.from({ length: 2100 }, () => freshName())
  await Promise.all(names.map((name) => seen(js, name)))
  const newest = freshName()
  await seen(js, newest)

  const { workers, truncated } = await listing(orchestrator)

  expect(truncated).toBe(true)
  expect(workers).toHaveLength(200)
  expect(workers[0]?.name).toBe(newest)
})

test('a read with nothing stored answers with no workers', async () => {
  const orchestrator = await startOrchestrator()
  expect(await listing(orchestrator)).toEqual({ workers: [], truncated: false })
})

// ------------------------------------------------- presence is worker-written

test('text from a worker reaches the tool sanitized', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  await seen(js, name, {
    description: 'plain\x1b[31m red‮ hidden',
    repo: 're\x07po',
    host: 'ho​st',
  })

  expect(await rowOf(orchestrator, name)).toMatchObject({
    description: 'plain red hidden',
    repo: 'repo',
    host: 'host',
  })
})

test('a presence naming another worker than its subject is dropped', async () => {
  const claimed = freshName()
  const writer = freshName()
  const orchestrator = await startOrchestrator()
  await js.publish(presenceSubject(writer), presenceBytes(claimed))

  const { workers } = await listing(orchestrator)
  expect(workers.map((w) => w.name)).not.toContain(claimed)
  expect(workers.map((w) => w.name)).not.toContain(writer)
  for (const to of [claimed, writer]) {
    const reply = await callTool(orchestrator, 'send_task', {
      to,
      title: 't',
      prompt: 'p',
    })
    expect(reply.isError).toBe(true)
  }
})

test('nothing a worker writes keeps it from going stale', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const forged = headers()
  const future = new Date(Date.now() + 3600_000).toISOString()
  forged.set('Nats-Time-Stamp', future)
  await js.publish(
    presenceSubject(name),
    presenceBytes(name, { heartbeat_s: 86400 }),
    { headers: forged },
  )

  await rowMatches(orchestrator, name, (row) => !row.stale)
  await rowMatches(orchestrator, name, (row) => row.stale)
})

// ---------------------------------------------------------------- send_task

test('send_task refuses a name nobody has seen, before any publish', async () => {
  const orchestrator = await startOrchestrator()

  const reply = await callTool(orchestrator, 'send_task', {
    to: freshName(),
    title: 't',
    prompt: 'p',
  })

  expect(reply.isError).toBe(true)
  expect((await jsm.streams.info('NATTER_DEFAULT_SENT')).state.messages).toBe(0)
  expect((await jsm.streams.info('NATTER_DEFAULT_TASKS')).state.messages).toBe(
    0,
  )
})

test('a worker with no presence left stays addressable through its records', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  await seen(js, name)
  await send(orchestrator, name)

  await jsm.streams.purge('NATTER_DEFAULT_PRESENCE')

  const { task_id, worker_state } = await send(orchestrator, name)
  expect(task_id).toBeDefined()
  expect(worker_state).toBe('no_presence')
})

test('a status flood leaves presence, the send gate and stale alone', async () => {
  await onFreshServer(async (server, conn, manager) => {
    const { config } = await manager.streams.info('NATTER_DEFAULT_STATE')
    await manager.streams.update('NATTER_DEFAULT_STATE', {
      ...config,
      max_msgs: 100,
    })
    const flood = (count: number) =>
      Promise.all(
        Array.from({ length: count }, () =>
          writeRow('flooder', mintTaskId(), 'accepted', undefined, conn),
        ),
      )
    const orchestrator = await startOrchestrator({}, server.url)
    const bystander = freshName()
    await seen(conn, bystander)

    await flood(300)

    expect(await rowOf(orchestrator, bystander)).toBeDefined()
    expect((await send(orchestrator, bystander)).worker_state).toBe('idle')

    const name = freshName()
    const worker = await startWorker(name, {}, server.url)
    await rowMatches(orchestrator, name, (row) => row.state === 'idle')
    const { task_id } = await send(orchestrator, name)
    await rowMatches(orchestrator, name, (row) => row.state === 'busy')
    await callTool(worker, 'update_task', { task_id })
    // Under the limit, so the worker's own row outlives this one.
    await flood(20)

    sigkill(worker)

    await until(async () => staleFor(orchestrator, task_id).length === 1)
  })
})

test('a never-used account answers, and send_task refuses properly', async () => {
  await onFreshServer(async (server) => {
    const orchestrator = await startOrchestrator({}, server.url)

    expect(parsed(await callTool(orchestrator, 'list_tasks'))).toEqual({
      tasks: [],
      truncated: false,
    })
    const reply = await callTool(orchestrator, 'send_task', {
      to: freshName(),
      title: 't',
      prompt: 'p',
    })
    expect(reply.isError).toBe(true)
    expect(reply.content[0]?.text).toBe(UNKNOWN_WORKER)
  })
})

test('the look through the records is bounded by the scan cap', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  await writeRecord(name)
  const other = freshName()
  await Promise.all(Array.from({ length: 2000 }, () => writeRecord(other)))

  const reply = await callTool(orchestrator, 'send_task', {
    to: name,
    title: 't',
    prompt: 'p',
  })

  expect(reply.isError).toBe(true)
})

// ------------------------------------------------------------ what stays quiet

test('orchestrator and inert sessions publish no presence', async () => {
  const published: string[] = []
  const sub = nc.subscribe('natter.default.presence.>', {
    callback: (_, msg) => {
      published.push(msg.subject)
    },
  })
  try {
    const orchestrator = await startOrchestrator()
    const inert = await startSession({ NATTER_URL: nats.url })
    sessions.push(inert)
    await Bun.sleep(4 * HEARTBEAT_MS)
    await Promise.all(sessions.splice(0).map((s) => s.close()))
    await orchestrator.closed
    await nc.flush()
  } finally {
    sub.unsubscribe()
  }
  expect(published).toEqual([])
})

test('heartbeats resume after a reconnect', async () => {
  const server = await startNats({ jetstream: true })
  const conn = await connect({ servers: server.url })
  const manager = await jetstreamManager(conn)
  const lastSeq = async () => {
    const msg = await manager.direct.getMessage('NATTER_DEFAULT_PRESENCE', {
      last_by_subj: presenceSubject(name),
    })
    return msg?.seq ?? 0
  }
  const name = freshName()
  try {
    await bootstrapStreams(manager)
    await startWorker(name, {}, server.url)
    await until(async () => (await lastSeq()) > 0)

    await server.restart()

    const after = await lastSeq().catch(() => 0)
    await until(async () => (await lastSeq().catch(() => 0)) > after, 10_000)
  } finally {
    await Promise.all(sessions.splice(0).map((s) => s.close()))
    await conn.close()
    await server.cleanup()
  }
})

test('a heartbeat that cannot be published does not touch task handling', async () => {
  const name = freshName()
  const full = {
    ...sessionEnv(nats.url),
    NATTER_ROLE: 'worker',
    NATTER_NAME: name,
  }
  const resolved = resolveConfig(full)
  if (!('config' in resolved)) throw new Error(resolved.inert)
  const log = () => {}
  const connection = startConnection(resolved.config, log)
  await until(async () => connection.nc !== undefined)
  const conn = connection.nc as NatsConnection
  const real = conn.publish.bind(conn)
  let failed = 0
  conn.publish = (subject, ...rest) => {
    if (subject === presenceSubject(name)) {
      failed++
      throw new Error('no presence today')
    }
    real(subject, ...rest)
  }
  const pushes: Record<string, string>[] = []
  const worker = createWorker({
    config: resolved.config,
    connection,
    push: async (_content, meta) => {
      pushes.push(meta)
    },
    log,
    env: full,
  })
  try {
    const task_id = mintTaskId()
    const task: Task = {
      v: 1,
      type: 'task',
      task_id,
      from: 'orchestrator',
      to: name,
      title: 'the title',
      prompt: 'the prompt',
      created_at: new Date().toISOString(),
      timeout_s: 60,
      queue_ttl_s: 600,
      max_attempts: 3,
      root_task_id: task_id,
      depth: 0,
    }
    await js.publish(taskSubject(name), JSON.stringify(task))
    worker.start()
    await until(async () => pushes.length === 1)

    const reply = await worker.callTool('finish_task', {
      task_id,
      outcome: 'succeeded',
      summary: 'done',
    })

    expect(reply.isError).toBeFalsy()
    expect(failed).toBeGreaterThan(0)
    const stored = await jsm.direct.getMessage('NATTER_DEFAULT_RESULTS', {
      last_by_subj: resultSubject(name, task_id),
    })
    expect(stored).not.toBeNull()
  } finally {
    await worker.stop()
    await connection.close()
  }
})

// ---------------------------------------------------------------- attention

test('a stale worker holding an open task raises one push', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const worker = await startWorker(name)
  await rowMatches(orchestrator, name, (row) => row.state === 'idle')
  const { task_id } = await send(orchestrator, name)
  await rowMatches(orchestrator, name, (row) => row.state === 'busy')
  await callTool(worker, 'update_task', { task_id })

  // While it beats, a task held for this long raises nothing.
  await Bun.sleep(STALE_MS + SWEEPS_MS)
  expect(attention(orchestrator)).toEqual([])
  sigkill(worker)
  await until(async () => attention(orchestrator).length >= 1)
  await Bun.sleep(SWEEPS_MS)

  const pushes = attention(orchestrator)
  expect(pushes).toHaveLength(1)
  expect(pushes[0]?.meta).toEqual({
    kind: 'attention',
    task_id,
    worker: name,
    reason: 'stale',
  })
  expect(pushes[0]?.content).toContain('the title')
})

test('a held task whose worker has no presence at all raises one push', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const task_id = await writeRecord(name)
  await writeRow(name, task_id, 'accepted')

  await until(async () => attention(orchestrator).length >= 1)
  await Bun.sleep(SWEEPS_MS)

  expect(attention(orchestrator).map((p) => p.meta.reason)).toEqual(['stale'])
})

test('a real accept release raises one push', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  await startWorker(name, {
    NATTER_TEST_ACCEPT_TIMEOUT_MS: String(ACCEPT_TIMEOUT_MS),
  })
  await rowMatches(orchestrator, name, (row) => row.state === 'idle')
  const { task_id } = await send(orchestrator, name)

  await until(async () => attention(orchestrator).length >= 1)
  await Bun.sleep(SWEEPS_MS)

  const pushes = attention(orchestrator)
  expect(pushes).toHaveLength(1)
  expect(pushes[0]?.meta).toMatchObject({ task_id, reason: 'released' })
})

test('an overdue task raises one push', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const task_id = await writeRecord(name, {
    ...overdueFields,
    title: 'plain\x1b[31m red',
  })

  await until(async () => attention(orchestrator).length >= 1)
  await Bun.sleep(SWEEPS_MS)

  const pushes = attention(orchestrator)
  expect(pushes).toHaveLength(1)
  expect(pushes[0]?.meta).toMatchObject({
    task_id,
    worker: name,
    reason: 'overdue',
  })
  expect(pushes[0]?.content).toContain('plain red')
})

test.each([
  ['the margin', '1', 60 + 60 + OVERDUE_MARGIN_S - 30],
  ['the accept timeout', '60000', 60 + 60 + OVERDUE_MARGIN_S + 30],
])('a task inside %s is not overdue yet', async (_what, accept, seconds) => {
  const orchestrator = await startOrchestrator({
    NATTER_TEST_ACCEPT_TIMEOUT_MS: accept,
  })
  await writeRecord(freshName(), { created_at: age(seconds) })

  await Bun.sleep(SWEEPS_MS)

  expect(attention(orchestrator)).toEqual([])
})

test('one task pushes once for each condition it meets', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const task_id = await writeRecord(name, overdueFields)
  await writeRow(name, task_id, 'released')

  await until(async () => attention(orchestrator).length >= 2)
  await Bun.sleep(SWEEPS_MS)

  const reasons = attention(orchestrator).map((p) => p.meta.reason)
  expect(reasons.sort()).toEqual(['overdue', 'released'])
})

test('a closed task raises nothing, whatever its row says', async () => {
  const name = freshName()
  const released = await writeRecord(name, overdueFields)
  await writeResult(name, released)
  await writeRow(name, released, 'released')
  const held = await writeRecord(name)
  await writeResult(name, held)
  await writeRow(name, held, 'accepted')
  const orchestrator = await startOrchestrator()

  await until(async () => orchestrator.pushes.length >= 2)
  await Bun.sleep(SWEEPS_MS)

  expect(attention(orchestrator)).toEqual([])
})

test('a finished row with no result closes nothing', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  await writeRow(name, await writeRecord(name, overdueFields), 'finished')

  await until(async () => attention(orchestrator).length >= 1)

  expect(attention(orchestrator)[0]?.meta.reason).toBe('overdue')
})

test('a row raises nothing unless the record sent that task to its writer', async () => {
  const name = freshName()
  const forger = freshName()
  const orchestrator = await startOrchestrator()
  const task_id = await writeRecord(name)
  await writeRow(forger, task_id, 'released')
  await writeRow(forger, mintTaskId(), 'released')
  await writeRow(forger, task_id, 'released', name)

  await Bun.sleep(SWEEPS_MS)

  expect(attention(orchestrator)).toEqual([])
})

test('a record past the scan cap raises nothing', async () => {
  const orchestrator = await startOrchestrator()
  await writeRecord(freshName(), overdueFields)
  const other = freshName()
  await Promise.all(Array.from({ length: 2000 }, () => writeRecord(other)))

  await Bun.sleep(SWEEPS_MS)

  expect(attention(orchestrator)).toEqual([])
})

test('a record older than the results stream keeps is aged_out and raises nothing', async () => {
  await onFreshServer(async (server, conn, manager) => {
    const maxAge = 1000
    const { config } = await manager.streams.info('NATTER_DEFAULT_RESULTS')
    await manager.streams.update('NATTER_DEFAULT_RESULTS', {
      ...config,
      max_age: nanos(maxAge),
      duplicate_window: nanos(maxAge),
    })
    const name = freshName()
    const task_id = await writeRecord(name, overdueFields, conn)
    await writeResult(name, task_id, conn)
    const orchestrator = await startOrchestrator(
      { NATTER_TEST_RESULTS_MAX_AGE_MS: String(maxAge) },
      server.url,
    )
    await until(async () => orchestrator.pushes.length === 1)

    await until(
      async () =>
        (await manager.direct.getMessage('NATTER_DEFAULT_RESULTS', {
          last_by_subj: resultSubject(name, task_id),
        })) === null,
    )
    await Bun.sleep(SWEEPS_MS)

    const tasks = async (open_only: boolean) =>
      parsed(await callTool(orchestrator, 'list_tasks', { open_only })).tasks
    expect((await tasks(false))[0]).toMatchObject({
      task_id,
      state: 'aged_out',
    })
    expect(await tasks(true)).toEqual([])
    expect(
      parsed(await callTool(orchestrator, 'get_result', { task_id })).state,
    ).toBe('aged_out')
    expect(attention(orchestrator)).toEqual([])
  })
})

test('age out goes by when the record was stored, not by what it says', async () => {
  const orchestrator = await startOrchestrator({
    NATTER_TEST_RESULTS_MAX_AGE_MS: '60000',
  })
  await writeRecord(freshName(), overdueFields)

  await until(async () => attention(orchestrator).length === 1)
})

test('one sweep pushes at most 20, and the rest follow once each', async () => {
  const name = freshName()
  const ids = await Promise.all(
    Array.from({ length: 25 }, () => writeRecord(name, overdueFields)),
  )
  const orchestrator = await startOrchestrator({
    NATTER_TEST_ATTENTION_SWEEP_MS: '500',
  })

  await until(async () => attention(orchestrator).length >= 20)
  await Bun.sleep(150)
  expect(attention(orchestrator)).toHaveLength(20)

  await until(async () => attention(orchestrator).length >= 25)
  await Bun.sleep(1100)
  const pushed = attention(orchestrator).map((p) => p.meta.task_id)
  expect(pushed.sort()).toEqual(ids.sort())
})

test('new attention cannot starve a task the cap keeps cutting off', async () => {
  const name = freshName()
  // Ids that sort after everything else, and arrivals that sort ever earlier,
  // so a sweep that always starts at the front never gets to it.
  const starved = 'z'.repeat(22)
  let lowest = 10 ** 9
  const arrivals = (count: number) =>
    Promise.all(
      Array.from({ length: count }, () =>
        writeRecord(name, {
          ...overdueFields,
          task_id: String(--lowest).padStart(22, '0'),
        }),
      ),
    )
  await writeRecord(name, { ...overdueFields, task_id: starved })
  await arrivals(45)
  const orchestrator = await startOrchestrator()

  await until(async () => {
    // Well past the cap, so without a rotating start the front never empties.
    await arrivals(60)
    await Bun.sleep(SWEEP_MS)
    return attention(orchestrator).some((p) => p.meta.task_id === starved)
  }, 8000)
})

test('a flood that evicts the row leaves a dead worker raising stale', async () => {
  await onFreshServer(async (server, conn, manager) => {
    const { config } = await manager.streams.info('NATTER_DEFAULT_STATE')
    await manager.streams.update('NATTER_DEFAULT_STATE', {
      ...config,
      max_msgs: 100,
    })
    const orchestrator = await startOrchestrator({}, server.url)
    const name = freshName()
    const worker = await startWorker(name, {}, server.url)
    await rowMatches(orchestrator, name, (row) => row.state === 'idle')
    const { task_id } = await send(orchestrator, name)
    await rowMatches(orchestrator, name, (row) => row.state === 'busy')

    await Promise.all(
      Array.from({ length: 300 }, () =>
        writeRow('flooder', mintTaskId(), 'accepted', undefined, conn),
      ),
    )
    const row = await manager.direct.getMessage('NATTER_DEFAULT_STATE', {
      last_by_subj: statusSubject(name, task_id),
    })
    expect(row).toBeNull()
    sigkill(worker)

    await until(async () => staleFor(orchestrator, task_id).length === 1)
  })
})

test('only the worker the record names can claim a task', async () => {
  const name = freshName()
  const other = freshName()
  const orchestrator = await startOrchestrator()
  const task_id = await writeRecord(name)
  await seen(js, name, { state: 'busy', task_id: mintTaskId() })
  await seen(js, other, { state: 'busy', task_id })

  await Bun.sleep(STALE_MS + SWEEPS_MS)

  expect(attention(orchestrator)).toEqual([])
})

test('a worker that dies before update_task still raises stale', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const worker = await startWorker(name)
  await rowMatches(orchestrator, name, (row) => row.state === 'idle')
  const { task_id } = await send(orchestrator, name)
  await rowMatches(orchestrator, name, (row) => row.state === 'busy')

  sigkill(worker)

  await until(async () => staleFor(orchestrator, task_id).length === 1)
})

test('only the dead worker of two raises stale', async () => {
  const dead = freshName()
  const live = freshName()
  const orchestrator = await startOrchestrator()
  const deadSession = await startWorker(dead)
  await startWorker(live)
  await rowMatches(orchestrator, dead, (row) => row.state === 'idle')
  await rowMatches(orchestrator, live, (row) => row.state === 'idle')
  const { task_id } = await send(orchestrator, dead)
  await send(orchestrator, live)
  await rowMatches(orchestrator, dead, (row) => row.state === 'busy')
  await rowMatches(orchestrator, live, (row) => row.state === 'busy')

  sigkill(deadSession)
  await until(async () => attention(orchestrator).length >= 1)
  await Bun.sleep(SWEEPS_MS)

  expect(attention(orchestrator).map((p) => p.meta.task_id)).toEqual([task_id])
})

test('a worker that comes back re-arms stale for its next death', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator()
  const worker = await startWorker(name)
  await rowMatches(orchestrator, name, (row) => row.state === 'idle')
  const { task_id } = await send(orchestrator, name)
  await rowMatches(orchestrator, name, (row) => row.state === 'busy')
  await callTool(worker, 'update_task', { task_id })

  signal(worker, 'SIGSTOP')
  try {
    await until(async () => staleFor(orchestrator, task_id).length === 1)
  } finally {
    signal(worker, 'SIGCONT')
  }
  await rowMatches(orchestrator, name, (row) => !row.stale)
  await Bun.sleep(SWEEPS_MS)
  expect(staleFor(orchestrator, task_id)).toHaveLength(1)

  sigkill(worker)

  await until(async () => staleFor(orchestrator, task_id).length === 2)
})

test('a worker flipping between stale and fresh raises at most 3 pushes', async () => {
  const name = freshName()
  const orchestrator = await startOrchestrator({ NATTER_TEST_STALE_MS: '300' })
  const task_id = await writeRecord(name)
  await writeRow(name, task_id, 'accepted')

  for (let flip = 0; flip < 5; flip++) {
    await seen(js, name)
    await Bun.sleep(2 * SWEEP_MS)
    await rowMatches(orchestrator, name, (row) => row.stale)
    await Bun.sleep(2 * SWEEP_MS)
  }

  expect(staleFor(orchestrator, task_id)).toHaveLength(3)
})
