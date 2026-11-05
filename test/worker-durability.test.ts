import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  expect,
  setDefaultTimeout,
  test,
} from 'bun:test'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import {
  AckPolicy,
  type JetStreamClient as JetStream,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream'
import {
  connect,
  type NatsConnection,
  nanos,
  nuid,
} from '@nats-io/transport-node'
import { resolveConfig, resolveTimers } from '../src/config'
import { startConnection } from '../src/connection'
import {
  encodeEnvelope,
  type Result,
  ResultSchema,
  type Status,
  type Task,
} from '../src/protocol'
import {
  ALREADY_CLOSED,
  ALREADY_RECORDED,
  createWorker,
  RELEASED,
  type ToolResult,
} from '../src/worker'
import {
  bootstrapStreams,
  resultSubject,
  statusSubject,
  taskSubject,
} from './support/fleet'
import { startNats, type TestNats, until } from './support/nats'
import { type Push, type Session, startSession } from './support/session'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

const ACK_WAIT_MS = 1000
const WORKING_MS = 100
const GRACE_MS = 20
const LONG_ACK_WAIT = { NATTER_TEST_ACK_WAIT_MS: '10000' }
// The error full streams answer with; unnamed in the client.
const STREAM_STORE_FAILED = 10077
const NO_CREDS = join(tmpdir(), 'natter-no-creds')

let nats: TestNats
let nc: NatsConnection
let jsm: JetStreamManager
const sessions: Session[] = []

const workerEnv = (
  name: string,
  url: string,
  env: Record<string, string> = {},
): Record<string, string> => ({
  NATTER_ROLE: 'worker',
  NATTER_NAME: name,
  NATTER_URL: url,
  NATTER_CREDS_DIR: NO_CREDS,
  NATTER_TEST_ACK_WAIT_MS: String(ACK_WAIT_MS),
  NATTER_TEST_WORKING_MS: String(WORKING_MS),
  NATTER_TEST_STARTUP_GRACE_MS: String(GRACE_MS),
  NATTER_TEST_STREAM_RETRY_MS: '100',
  NATTER_TEST_RESTART_MIN_MS: '100',
  NATTER_TEST_RESTART_MAX_MS: '300',
  ...env,
})

const startWorkerSession = async (
  name: string,
  env: Record<string, string> = {},
  url = nats.url,
) => {
  const s = await startSession(workerEnv(name, url, env))
  sessions.push(s)
  return s
}

const freshName = () => `w-${crypto.randomUUID()}`

const makeTask = (to: string, overrides: Partial<Task> = {}): Task => {
  const task_id = nuid.next()
  return {
    v: 1,
    type: 'task',
    task_id,
    from: 'orchestrator',
    to,
    title: 'the title',
    prompt: 'the prompt',
    created_at: new Date().toISOString(),
    timeout_s: 60,
    queue_ttl_s: 600,
    max_attempts: 3,
    root_task_id: task_id,
    depth: 0,
    ...overrides,
  }
}

const publishTask = async (task: Task, js = jetstream(nc)) => {
  await js.publish(taskSubject(task.to), JSON.stringify(task), {
    msgID: nuid.next(),
  })
  return task
}

const queued = async (worker: string, manager = jsm) => {
  const subject = taskSubject(worker)
  const info = await manager.streams.info('NATTER_DEFAULT_TASKS', {
    subjects_filter: subject,
  })
  return info.state.subjects?.[subject] ?? 0
}

const consumerOf = (worker: string, manager = jsm) =>
  manager.consumers.info('NATTER_DEFAULT_TASKS', `task-${worker}`)

const heldBy = (worker: string, manager = jsm) =>
  until(async () =>
    consumerOf(worker, manager).then(
      (info) => info.num_ack_pending === 1,
      () => false,
    ),
  )

const storedResult = async (worker: string, taskId: string, manager = jsm) => {
  const msg = await manager.direct.getMessage('NATTER_DEFAULT_RESULTS', {
    last_by_subj: resultSubject(worker, taskId),
  })
  return msg ? (msg.json() as Result) : undefined
}

const resultCount = async (worker: string, taskId: string) => {
  const subject = resultSubject(worker, taskId)
  const info = await jsm.streams.info('NATTER_DEFAULT_RESULTS', {
    subjects_filter: subject,
  })
  return info.state.subjects?.[subject] ?? 0
}

const statusOf = async (worker: string, taskId: string, manager = jsm) => {
  const msg = await manager.direct.getMessage('NATTER_DEFAULT_STATE', {
    last_by_subj: statusSubject(worker, taskId),
  })
  return msg ? (msg.json() as Status) : undefined
}

const statusIs = (worker: string, taskId: string, state: Status['state']) =>
  until(async () => (await statusOf(worker, taskId))?.state === state)

const pushCount = async <P>(s: { pushes: P[] }, count: number) => {
  await until(async () => s.pushes.length >= count)
  return s.pushes[count - 1]
}

const closePushes = (pushes: Pick<Push, 'meta'>[]) =>
  pushes.filter((p) => p.meta.kind === 'task_closed')

const callTool = async (
  s: Session,
  name: string,
  args: Record<string, unknown>,
) =>
  (await s.client.callTool({ name, arguments: args })) as unknown as ToolResult

const finishArgs = (task_id: string, args: Record<string, unknown> = {}) => ({
  task_id,
  outcome: 'succeeded',
  summary: 'done',
  ...args,
})

const finish = (
  s: Session,
  task_id: string,
  args: Record<string, unknown> = {},
) => callTool(s, 'finish_task', finishArgs(task_id, args))

const accept = (s: Session, task_id: string) =>
  callTool(s, 'update_task', { task_id })

const sigkill = (s: Session) => {
  const { pid } = s.client.transport as StdioClientTransport
  if (pid === null) throw new Error('session has no process')
  process.kill(pid, 'SIGKILL')
}

/** Records what the broker reports about terminated messages. */
const watchTerminated = async (worker: string, conn = nc) => {
  const terminated: unknown[] = []
  conn.subscribe(
    `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.NATTER_DEFAULT_TASKS.task-${worker}`,
    {
      callback: (_err, msg) => {
        terminated.push(msg.json())
      },
    },
  )
  await conn.flush()
  return terminated
}

/** Records status publishes, naks and terms, in the order the broker saw. */
const watchWire = async (worker: string, taskId: string) => {
  const events: string[] = []
  nc.subscribe(statusSubject(worker, taskId), {
    callback: (_err, msg) => {
      events.push((msg.json() as Status).state)
    },
  })
  nc.subscribe(`$JS.ACK.NATTER_DEFAULT_TASKS.task-${worker}.>`, {
    callback: (_err, msg) => {
      if (msg.string() === '-NAK') events.push('nak')
      if (msg.string().startsWith('+TERM')) events.push('term')
    },
  })
  await nc.flush()
  return events
}

/** Records every consumer create and pull request for this worker's durable. */
const watchPulls = async (worker: string) => {
  const pulls: string[] = []
  nc.subscribe('$JS.API.CONSUMER.>', {
    callback: (_err, { subject }) => {
      const pull =
        subject.includes('.MSG.NEXT.') || subject.includes('.CREATE.')
      if (pull && subject.includes(`task-${worker}`)) pulls.push(subject)
    },
  })
  await nc.flush()
  return pulls
}

/** Stores a result the way a session would, as if `instance` had finished. */
const recordResult = async (worker: string, task_id: string) => {
  const encoded = encodeEnvelope({
    v: 1,
    type: 'result',
    task_id,
    worker,
    instance: nuid.next(),
    attempt: 1,
    outcome: 'succeeded',
    summary: 'recorded first',
    finished_at: new Date().toISOString(),
  })
  if (!encoded.ok) throw new Error(encoded.issues.join('; '))
  await jetstream(nc).publish(resultSubject(worker, task_id), encoded.bytes, {
    expect: { lastSubjectSequence: 0 },
  })
}

/** Writes the ledger row for `task`, as an earlier session would. */
const writeStatus = async (
  task: Task,
  fields: Partial<Status>,
  js = jetstream(nc),
) => {
  const encoded = encodeEnvelope({
    v: 1,
    type: 'status',
    task_id: task.task_id,
    worker: task.to,
    title: task.title,
    created_at: task.created_at,
    depth: task.depth,
    timeout_s: task.timeout_s,
    queue_ttl_s: task.queue_ttl_s,
    ...fields,
  })
  if (!encoded.ok) throw new Error(encoded.issues.join('; '))
  await js.publish(statusSubject(task.to, task.task_id), encoded.bytes)
}

/** Delivers the queued task `times` times to nobody, as a dead standby would. */
const inflateDeliveries = async (name: string, times: number) => {
  await jsm.consumers.add('NATTER_DEFAULT_TASKS', {
    durable_name: `task-${name}`,
    filter_subject: taskSubject(name),
    ack_policy: AckPolicy.Explicit,
    ack_wait: nanos(50),
    max_ack_pending: 1,
  })
  const consumer = await jetstream(nc).consumers.get(
    'NATTER_DEFAULT_TASKS',
    `task-${name}`,
  )
  for (let i = 0; i < times; i++) {
    expect(await consumer.next({ expires: 2000 })).not.toBeNull()
  }
}

/** A private server, for tests that break it or a stream the others share. */
const onFreshServer = async (
  run: (
    server: TestNats,
    js: JetStream,
    manager: JetStreamManager,
    conn: NatsConnection,
  ) => Promise<void>,
) => {
  const server = await startNats({ jetstream: true })
  const conn = await connect({ servers: server.url })
  const manager = await jetstreamManager(conn)
  try {
    await bootstrapStreams(manager)
    await run(server, jetstream(conn), manager, conn)
  } finally {
    await Promise.all(sessions.splice(0).map((s) => s.close()))
    await conn.close()
    await server.cleanup()
  }
}

/** Leaves NATTER_DEFAULT_RESULTS with no room; returns how to give the room back. */
const fillResults = async (js: JetStream, manager: JetStreamManager) => {
  const { config } = await manager.streams.info('NATTER_DEFAULT_RESULTS')
  await manager.streams.update('NATTER_DEFAULT_RESULTS', {
    ...config,
    max_msgs: 1,
  })
  await js.publish(resultSubject('filler', nuid.next()), 'taking the slot')
  const refused = await js
    .publish(resultSubject('filler', nuid.next()), 'over')
    .catch((err: { code?: number }) => err.code)
  expect(refused).toBe(STREAM_STORE_FAILED)
  return () => manager.streams.update('NATTER_DEFAULT_RESULTS', config)
}

// In-process, because only here can a test sit between two steps of a tool call.
const inProcessWorker = async (
  name: string,
  env: Record<string, string> = {},
  pushed: () => Promise<void> = async () => {},
  url = nats.url,
) => {
  const fullEnv = workerEnv(name, url, env)
  const resolved = resolveConfig(fullEnv)
  if (!('config' in resolved)) throw new Error(resolved.inert)
  const log = () => {}
  const connection = startConnection(resolved.config, log)
  await until(async () => connection.nc !== undefined)
  const pushes: Pick<Push, 'content' | 'meta'>[] = []
  const worker = createWorker({
    config: resolved.config,
    connection,
    push: async (content, meta) => {
      pushes.push({ content, meta })
      await pushed()
    },
    log,
    env: fullEnv,
  })
  return {
    worker,
    connection,
    pushes,
    conn: connection.nc as NatsConnection,
  }
}

const RESULT_PUBLISH = 'natter.default.result.'

/**
 * Sits on the first request of `conn` to a subject starting with `prefix` until
 * `gate` settles: it is sent once resolved, and fails unsent if rejected.
 */
const gateFirst = (conn: NatsConnection, prefix: string) => {
  const real = conn.request.bind(conn)
  const gate = Promise.withResolvers<void>()
  let reached = 0
  conn.request = async (subject, ...rest) => {
    if (subject.startsWith(prefix) && reached++ === 0) await gate.promise
    return real(subject, ...rest)
  }
  return { gate, reached: () => reached }
}

/** Counts the result publishes `conn` attempts, refused ones included. */
const countResultPublishes = (conn: NatsConnection) => {
  const real = conn.request.bind(conn)
  let count = 0
  conn.request = async (subject, ...rest) => {
    if (subject.startsWith(RESULT_PUBLISH)) count++
    return real(subject, ...rest)
  }
  return () => count
}

/** Lets the first result publish of `conn` land, then loses its PubAck. */
const loseFirstPubAck = (conn: NatsConnection) => {
  const real = conn.request.bind(conn)
  let lost = false
  conn.request = async (subject, ...rest) => {
    const reply = await real(subject, ...rest)
    if (subject.startsWith(RESULT_PUBLISH) && !lost) {
      lost = true
      throw new Error('timed out waiting for the PubAck')
    }
    return reply
  }
}

beforeAll(async () => {
  nats = await startNats({ jetstream: true })
  nc = await connect({
    servers: nats.url,
    maxReconnectAttempts: -1,
    reconnectTimeWait: 100,
  })
  jsm = await jetstreamManager(nc)
  await bootstrapStreams(jsm)
})

// A test that times out takes the shared server with it; the next one must not.
beforeEach(async () => {
  await nats.revive()
  await until(() =>
    nc.flush().then(
      () => true,
      () => false,
    ),
  )
})

afterEach(async () => {
  await Promise.all(sessions.splice(0).map((s) => s.close()))
})

afterAll(async () => {
  await nc?.close()
  await nats?.cleanup()
})

test('a SIGKILL mid-task gives the next session attempt 2, after ack_wait', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await statusIs(name, task.task_id, 'delivered')

  const killed = Date.now()
  sigkill(s)
  await s.closed
  const next = await startWorkerSession(name)

  const push = await pushCount(next, 1)
  expect(push?.meta).toMatchObject({ task_id: task.task_id, attempt: '2' })
  // The last working() can land just before the kill, never much before it.
  expect((push?.at ?? 0) - killed).toBeGreaterThan(ACK_WAIT_MS - 300)
  expect((await finish(next, task.task_id)).isError).toBeUndefined()
  expect((await storedResult(name, task.task_id))?.attempt).toBe(2)
})

test('a clean stdin close redelivers at once, without waiting for ack_wait', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name, LONG_ACK_WAIT)
  await pushCount(s, 1)
  await statusIs(name, task.task_id, 'delivered')

  const closing = Date.now()
  s.endStdin()
  await s.closed
  expect(Date.now() - closing).toBeLessThan(3000)
  const next = await startWorkerSession(name, LONG_ACK_WAIT)

  expect((await pushCount(next, 1))?.meta).toMatchObject({
    task_id: task.task_id,
    attempt: '2',
  })
})

test('a clean exit with NATS down still leaves in time, and the task returns', async () => {
  await onFreshServer(async (server, js) => {
    const name = freshName()
    const task = await publishTask(makeTask(name), js)
    const s = await startWorkerSession(name, {}, server.url)
    await pushCount(s, 1)
    await server.stop()

    const closing = Date.now()
    s.endStdin()
    await s.closed
    expect(Date.now() - closing).toBeLessThan(3000)

    await server.start()
    const next = await startWorkerSession(name, {}, server.url)
    expect((await pushCount(next, 1))?.meta).toMatchObject({
      task_id: task.task_id,
      attempt: '2',
    })
  })
})

test('a clean exit with NATS hung still leaves in time, and the task returns', async () => {
  await onFreshServer(async (server, js) => {
    const name = freshName()
    const task = await publishTask(makeTask(name), js)
    const s = await startWorkerSession(name, {}, server.url)
    await pushCount(s, 1)
    server.signal('SIGSTOP')
    try {
      const closing = Date.now()
      s.endStdin()
      await s.closed
      expect(Date.now() - closing).toBeLessThan(3000)
    } finally {
      server.signal('SIGCONT')
    }

    const next = await startWorkerSession(name, {}, server.url)
    expect((await pushCount(next, 1))?.meta).toMatchObject({
      task_id: task.task_id,
      attempt: '2',
    })
  })
})

test('past max_attempts the task fails with attempts_exhausted, unpushed, and the queue moves on', async () => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  const task = await publishTask(makeTask(name, { max_attempts: 2 }))
  for (const attempt of ['1', '2']) {
    const s = await startWorkerSession(name)
    expect((await pushCount(s, 1))?.meta.attempt).toBe(attempt)
    await s.close()
  }
  const next = await publishTask(makeTask(name))

  const s = await startWorkerSession(name)

  expect((await pushCount(s, 1))?.meta.task_id).toBe(next.task_id)
  expect(terminated).toHaveLength(1)
  const stored = await storedResult(name, task.task_id)
  expect(ResultSchema.safeParse(stored).success).toBe(true)
  expect(stored).toMatchObject({
    outcome: 'failed',
    reason: 'attempts_exhausted',
  })
  await statusIs(name, task.task_id, 'finished')
  expect(s.pushes).toHaveLength(1)
  await finish(s, next.task_id)
  expect(await queued(name)).toBe(0)
})

test('max_attempts 1 allows no retry after a lost session', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { max_attempts: 1 }))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await statusIs(name, task.task_id, 'delivered')
  await s.close()

  const next = await startWorkerSession(name)

  await until(async () => (await queued(name)) === 0)
  expect((await storedResult(name, task.task_id))?.reason).toBe(
    'attempts_exhausted',
  )
  expect(next.pushes).toHaveLength(0)
})

test('deliveries that reached no push do not count against max_attempts', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { max_attempts: 2 }))
  await inflateDeliveries(name, 3)

  const s = await startWorkerSession(name)

  expect((await pushCount(s, 1))?.meta).toMatchObject({
    task_id: task.task_id,
    attempt: '1',
  })
  expect(await storedResult(name, task.task_id)).toBeUndefined()
})

test('a status that gives no attempt falls back to deliveryCount, which still bounds attempts', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  nc.publish(statusSubject(name, task.task_id), 'not a status')
  await until(async () => {
    const msg = await jsm.direct.getMessage('NATTER_DEFAULT_STATE', {
      last_by_subj: statusSubject(name, task.task_id),
    })
    return msg !== null
  })
  await inflateDeliveries(name, 3)

  const s = await startWorkerSession(name)

  await until(async () => (await queued(name)) === 0)
  expect((await storedResult(name, task.task_id))?.reason).toBe(
    'attempts_exhausted',
  )
  expect(s.pushes).toHaveLength(0)
})

test('a redelivery of a task with a stored result is acked, unpushed and unterminated', async () => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await statusIs(name, task.task_id, 'delivered')
  await recordResult(name, task.task_id)
  sigkill(s)
  await s.closed

  const next = await startWorkerSession(name)

  await until(async () => (await queued(name)) === 0)
  expect(next.pushes).toHaveLength(0)
  expect(terminated).toHaveLength(0)
  expect((await storedResult(name, task.task_id))?.summary).toBe(
    'recorded first',
  )
})

test('a broker restart mid-task means no second push, and finish_task still works', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  expect((await accept(s, task.task_id)).isError).toBeUndefined()

  await nats.restart()
  await until(async () => (await nats.clientNames()).includes(`natter-${name}`))
  await Bun.sleep(ACK_WAIT_MS * 1.5)

  expect(s.pushes).toHaveLength(1)
  await until(
    async () => (await finish(s, task.task_id)).isError === undefined,
    10_000,
  )
  expect(await storedResult(name, task.task_id)).toMatchObject({
    outcome: 'succeeded',
    attempt: 1,
  })
  await until(async () => (await queued(name)) === 0)

  // The consume loop came through it too.
  const next = await publishTask(makeTask(name))
  expect((await pushCount(s, 2))?.meta.task_id).toBe(next.task_id)
})

test('no update_task within the accept timeout: released before the nak, no more pulls, redelivered elsewhere at the same attempt', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const wire = await watchWire(name, task.task_id)
  const pulls = await watchPulls(name)
  const deaf = await startWorkerSession(name, {
    NATTER_TEST_ACCEPT_TIMEOUT_MS: '300',
  })
  await pushCount(deaf, 1)

  await statusIs(name, task.task_id, 'released')
  await until(async () => wire.includes('nak'))
  expect(wire.slice(wire.indexOf('delivered'))).toEqual([
    'delivered',
    'released',
    'nak',
  ])
  expect((await statusOf(name, task.task_id))?.attempt).toBe(1)

  // Neither the released task nor anything newer comes to this session, and
  // it does not even ask: longer than the restart backoff, with no pull sent.
  await publishTask(makeTask(name))
  await until(async () => (await consumerOf(name)).num_waiting === 0)
  await Bun.sleep(400)
  const asked = pulls.length
  await Bun.sleep(400)
  expect(pulls).toHaveLength(asked)
  expect(deaf.pushes).toHaveLength(1)
  expect((await consumerOf(name)).delivered.consumer_seq).toBe(1)

  const other = await startWorkerSession(name)
  expect((await pushCount(other, 1))?.meta).toMatchObject({
    task_id: task.task_id,
    attempt: '1',
  })
  await statusIs(name, task.task_id, 'delivered')
  const { delivered } = await consumerOf(name)

  // The task is gone from this session: leaving must not nak it from under
  // the one that has it now.
  deaf.endStdin()
  await deaf.closed
  await Bun.sleep(300)
  expect((await consumerOf(name)).delivered.consumer_seq).toBe(
    delivered.consumer_seq,
  )
  expect(other.pushes).toHaveLength(1)
  expect((await finish(other, task.task_id)).isError).toBeUndefined()
  expect((await storedResult(name, task.task_id))?.attempt).toBe(1)
})

test.each([['update_task'], ['finish_task']])(
  'a late %s gets isError, clears the latch and resumes pulling',
  async (tool) => {
    const name = freshName()
    const task = await publishTask(makeTask(name))
    const s = await startWorkerSession(name, {
      NATTER_TEST_ACCEPT_TIMEOUT_MS: '400',
    })
    await pushCount(s, 1)
    await statusIs(name, task.task_id, 'released')

    // A call about something else says nothing about the released task.
    expect((await accept(s, nuid.next())).isError).toBe(true)
    await Bun.sleep(300)
    expect(s.pushes).toHaveLength(1)

    const late = await callTool(s, tool, finishArgs(task.task_id))

    expect(late.isError).toBe(true)
    expect(late.content[0]?.text).toBe(RELEASED)
    expect(await storedResult(name, task.task_id)).toBeUndefined()
    expect((await pushCount(s, 2))?.meta).toMatchObject({
      task_id: task.task_id,
      attempt: '1',
    })
    expect((await finish(s, task.task_id)).isError).toBeUndefined()
    expect((await storedResult(name, task.task_id))?.attempt).toBe(1)
  },
)

test.each([
  ['update_task', accept],
  [
    'a rejected finish_task',
    (s: Session, id: string) => finish(s, id, { outcome: 'great' }),
  ],
])('%s in time keeps the task past the accept timeout', async (_kind, call) => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name, {
    NATTER_TEST_ACCEPT_TIMEOUT_MS: '400',
  })
  await pushCount(s, 1)

  await call(s, task.task_id)
  await Bun.sleep(700)

  expect((await statusOf(name, task.task_id))?.state).not.toBe('released')
  expect((await consumerOf(name)).delivered.consumer_seq).toBe(1)
  expect((await finish(s, task.task_id)).isError).toBeUndefined()
})

test('a run timeout records failed/timeout, terms, pushes task_closed, and later calls are already closed', async () => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  const task = await publishTask(makeTask(name, { timeout_s: 1 }))
  const wire = await watchWire(name, task.task_id)
  const next = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  const accepted = Date.now()
  await accept(s, task.task_id)

  await until(async () => closePushes(s.pushes).length === 1)

  expect(Date.now() - accepted).toBeGreaterThanOrEqual(900)
  expect(closePushes(s.pushes)[0]?.meta).toEqual({
    kind: 'task_closed',
    task_id: task.task_id,
    reason: 'timeout',
  })
  await until(async () => terminated.length === 1)
  const stored = await storedResult(name, task.task_id)
  expect(ResultSchema.safeParse(stored).success).toBe(true)
  expect(stored).toMatchObject({
    outcome: 'failed',
    reason: 'timeout',
    attempt: 1,
  })
  await statusIs(name, task.task_id, 'finished')
  expect(wire.slice(wire.indexOf('finished'))).toEqual(['finished', 'term'])

  const late = await finish(s, task.task_id, { summary: 'too late' })
  expect(late.isError).toBe(true)
  expect(late.content[0]?.text).toBe(ALREADY_CLOSED)
  expect((await accept(s, task.task_id)).content[0]?.text).toBe(ALREADY_CLOSED)
  expect((await storedResult(name, task.task_id))?.reason).toBe('timeout')

  // The next task is unaffected, and so is the late call's answer while it runs.
  await until(async () => s.pushes.some((p) => p.meta.task_id === next.task_id))
  expect((await finish(s, task.task_id)).content[0]?.text).toBe(ALREADY_CLOSED)
  expect((await finish(s, next.task_id)).isError).toBeUndefined()
  expect(await queued(name)).toBe(0)
})

test('the run timer counts from acceptance: an unaccepted task does not time out', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { timeout_s: 1 }))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)

  await Bun.sleep(1300)

  expect(await storedResult(name, task.task_id)).toBeUndefined()
  expect(await queued(name)).toBe(1)
  expect(closePushes(s.pushes)).toHaveLength(0)

  await accept(s, task.task_id)
  await Bun.sleep(500)
  expect(closePushes(s.pushes)).toHaveLength(0)
  await until(async () => closePushes(s.pushes).length === 1)
})

test.each([
  ['expired_in_queue', { queue_ttl_s: 1 }, undefined],
  ['attempts_exhausted', { max_attempts: 1 }, 1],
])(
  '%s is stored before term(): with no room nothing ends until it lands',
  async (reason, overrides, attempt) => {
    await onFreshServer(async (server, js, manager, conn) => {
      const name = freshName()
      const terminated = await watchTerminated(name, conn)
      const restore = await fillResults(js, manager)
      const task = await publishTask(makeTask(name, overrides), js)
      if (attempt) await writeStatus(task, { state: 'delivered', attempt }, js)
      else await Bun.sleep(1100)

      const s = await startWorkerSession(name, {}, server.url)
      await heldBy(name, manager)
      await Bun.sleep(ACK_WAIT_MS * 1.5)

      expect(terminated).toHaveLength(0)
      expect(await queued(name, manager)).toBe(1)
      expect((await consumerOf(name, manager)).num_redelivered).toBe(0)

      await restore()
      await until(async () => terminated.length === 1, 10_000)
      expect(await storedResult(name, task.task_id, manager)).toMatchObject({
        outcome: 'failed',
        reason,
      })
      expect(await queued(name, manager)).toBe(0)
      expect(s.pushes).toHaveLength(0)
    })
  },
)

test('a run timeout is stored before term(): with no room the task stays closed and held until it lands', async () => {
  await onFreshServer(async (server, js, manager, conn) => {
    const name = freshName()
    const terminated = await watchTerminated(name, conn)
    const restore = await fillResults(js, manager)
    const task = await publishTask(makeTask(name, { timeout_s: 1 }), js)
    const s = await startWorkerSession(name, {}, server.url)
    await pushCount(s, 1)
    await accept(s, task.task_id)

    await Bun.sleep(1000 + ACK_WAIT_MS * 1.5)

    expect(terminated).toHaveLength(0)
    expect(closePushes(s.pushes)).toHaveLength(0)
    expect(await queued(name, manager)).toBe(1)
    expect((await consumerOf(name, manager)).num_redelivered).toBe(0)
    expect((await finish(s, task.task_id)).content[0]?.text).toBe(
      ALREADY_CLOSED,
    )

    await restore()
    await until(async () => terminated.length === 1, 10_000)
    expect((await storedResult(name, task.task_id, manager))?.reason).toBe(
      'timeout',
    )
    await until(async () => closePushes(s.pushes).length === 1)
    expect(await queued(name, manager)).toBe(0)
  })
})

test('queue TTL expiry records expired_in_queue, unpushed', async () => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  const task = await publishTask(makeTask(name, { queue_ttl_s: 1 }))
  const wire = await watchWire(name, task.task_id)
  await Bun.sleep(1100)

  const s = await startWorkerSession(name)

  await until(async () => terminated.length === 1)
  await statusIs(name, task.task_id, 'finished')
  expect(wire).toEqual(['finished', 'term'])
  expect(await storedResult(name, task.task_id)).toMatchObject({
    outcome: 'failed',
    reason: 'expired_in_queue',
    attempt: 1,
  })
  expect(await queued(name)).toBe(0)
  expect(s.pushes).toHaveLength(0)
})

test('queue TTL is checked before attempts', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { queue_ttl_s: 1 }))
  await writeStatus(task, { state: 'delivered', attempt: 3 })
  await Bun.sleep(1100)

  await startWorkerSession(name)

  await until(async () => (await queued(name)) === 0)
  expect((await storedResult(name, task.task_id))?.reason).toBe(
    'expired_in_queue',
  )
})

test('a task inside its TTL is pushed', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { queue_ttl_s: 30 }))

  const s = await startWorkerSession(name)

  expect((await pushCount(s, 1))?.meta.task_id).toBe(task.task_id)
})

test('two sessions finishing one task leave one result, and the loser hears already recorded', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  // Too slow to hold the task, so the broker hands it on.
  const env = {
    NATTER_TEST_ACK_WAIT_MS: '200',
    NATTER_TEST_WORKING_MS: '60000',
  }
  const first = await startWorkerSession(name, env)
  await pushCount(first, 1)
  const second = await startWorkerSession(name, env)
  await pushCount(second, 1)

  const won = await finish(first, task.task_id, { summary: 'the winner' })
  const lost = await finish(second, task.task_id, { summary: 'the loser' })

  expect(won.isError).toBeUndefined()
  expect(lost.isError).toBe(true)
  expect(lost.content[0]?.text).toBe(ALREADY_RECORDED)
  expect(await resultCount(name, task.task_id)).toBe(1)
  expect((await storedResult(name, task.task_id))?.summary).toBe('the winner')
  expect(await queued(name)).toBe(0)
})

test('finish_task racing ahead of the run timeout keeps its result and hears the truth', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { timeout_s: 1 }))
  const { worker, connection, pushes, conn } = await inProcessWorker(name)
  const { gate, reached } = gateFirst(conn, RESULT_PUBLISH)
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    await worker.callTool('update_task', { task_id: task.task_id })

    const finishing = worker.callTool('finish_task', finishArgs(task.task_id))
    await until(async () => reached() === 1)
    await Bun.sleep(1300)
    gate.resolve()

    expect((await finishing).isError).toBeUndefined()
    expect(await resultCount(name, task.task_id)).toBe(1)
    expect(await storedResult(name, task.task_id)).toMatchObject({
      outcome: 'succeeded',
    })
    expect(await queued(name)).toBe(0)
    await Bun.sleep(300)
    expect(closePushes(pushes)).toHaveLength(0)
    expect(await resultCount(name, task.task_id)).toBe(1)
  } finally {
    gate.resolve()
    await worker.stop()
    await connection.close()
  }
})

test('the run timeout racing ahead of finish_task keeps its result and the call hears already closed', async () => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  const task = await publishTask(makeTask(name, { timeout_s: 1 }))
  const { worker, connection, pushes, conn } = await inProcessWorker(name)
  const { gate, reached } = gateFirst(conn, RESULT_PUBLISH)
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    await worker.callTool('update_task', { task_id: task.task_id })
    await until(async () => reached() === 1)

    const late = await worker.callTool(
      'finish_task',
      finishArgs(task.task_id, { summary: 'too late' }),
    )

    expect(late.isError).toBe(true)
    expect(late.content[0]?.text).toBe(ALREADY_CLOSED)
    expect(await storedResult(name, task.task_id)).toBeUndefined()

    gate.resolve()
    await until(async () => closePushes(pushes).length === 1)
    await until(async () => terminated.length === 1)
    expect(await resultCount(name, task.task_id)).toBe(1)
    expect((await storedResult(name, task.task_id))?.reason).toBe('timeout')
    expect(await queued(name)).toBe(0)
  } finally {
    gate.resolve()
    await worker.stop()
    await connection.close()
  }
})

test('a finish_task that fails as the run timeout passes does not disarm it', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { timeout_s: 1 }))
  const { worker, connection, pushes, conn } = await inProcessWorker(name)
  const { gate, reached } = gateFirst(conn, RESULT_PUBLISH)
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    await worker.callTool('update_task', { task_id: task.task_id })

    const finishing = worker.callTool('finish_task', finishArgs(task.task_id))
    await until(async () => reached() === 1)
    await Bun.sleep(1200)
    gate.reject(new Error('the broker went away'))

    const res = await finishing
    expect(res.isError).toBe(true)
    expect(res.content[0]?.text).not.toBe(ALREADY_CLOSED)

    await until(async () => closePushes(pushes).length === 1)
    expect((await storedResult(name, task.task_id))?.reason).toBe('timeout')
    expect(await queued(name)).toBe(0)
  } finally {
    gate.resolve()
    await worker.stop()
    await connection.close()
  }
})

test('a retry by the session that won, after a really lost PubAck, is a success', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const { worker, connection, pushes, conn } = await inProcessWorker(name)
  loseFirstPubAck(conn)
  try {
    worker.start()
    await pushCount({ pushes }, 1)

    const first = await worker.callTool('finish_task', finishArgs(task.task_id))
    expect(first.isError).toBe(true)
    expect(await queued(name)).toBe(1)

    const retry = await worker.callTool('finish_task', finishArgs(task.task_id))
    expect(retry.isError).toBeUndefined()
    expect(await resultCount(name, task.task_id)).toBe(1)
    expect(await queued(name)).toBe(0)
    await statusIs(name, task.task_id, 'finished')
  } finally {
    await worker.stop()
    await connection.close()
  }
})

/** Makes the broker forget the held task, which frees the one delivery slot. */
const forgetHeldTask = async (name: string) => {
  const { delivered } = await consumerOf(name)
  await jsm.streams.deleteMessage('NATTER_DEFAULT_TASKS', delivered.stream_seq)
}

test('the accept timer of a task finished elsewhere never fires on the task that replaced it', async () => {
  const name = freshName()
  const old = await publishTask(makeTask(name))
  const s = await startWorkerSession(name, {
    NATTER_TEST_ACCEPT_TIMEOUT_MS: '500',
  })
  await pushCount(s, 1)

  await forgetHeldTask(name)
  await recordResult(name, old.task_id)
  const task = await publishTask(makeTask(name))
  expect((await pushCount(s, 2))?.meta.task_id).toBe(task.task_id)
  await accept(s, task.task_id)
  await Bun.sleep(700)

  expect((await statusOf(name, old.task_id))?.state).toBe('delivered')
  expect((await statusOf(name, task.task_id))?.state).toBe('accepted')
  expect((await consumerOf(name)).delivered.consumer_seq).toBe(2)

  // Nothing latched either.
  const next = await publishTask(makeTask(name))
  expect((await finish(s, task.task_id)).isError).toBeUndefined()
  expect((await pushCount(s, 3))?.meta.task_id).toBe(next.task_id)
})

test('the run timer of a task finished elsewhere never fires on the task that replaced it', async () => {
  const name = freshName()
  const old = await publishTask(makeTask(name, { timeout_s: 1 }))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await accept(s, old.task_id)

  await forgetHeldTask(name)
  await recordResult(name, old.task_id)
  const task = await publishTask(makeTask(name))
  expect((await pushCount(s, 2))?.meta.task_id).toBe(task.task_id)
  await accept(s, task.task_id)
  await Bun.sleep(1300)

  expect(closePushes(s.pushes)).toHaveLength(0)
  expect((await finish(s, task.task_id)).isError).toBeUndefined()
  expect(await queued(name)).toBe(0)
})

test('the accept timer of a failed push never fires on the redelivery', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  let failing = true
  const { worker, connection, pushes } = await inProcessWorker(
    name,
    { NATTER_TEST_ACCEPT_TIMEOUT_MS: '400' },
    async () => {
      if (failing) {
        failing = false
        throw new Error('transport closed')
      }
    },
  )
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    await Bun.sleep(600)
    expect((await statusOf(name, task.task_id))?.state).toBe('delivered')

    await pushCount({ pushes }, 2)
    const done = await worker.callTool('finish_task', finishArgs(task.task_id))
    expect(done.isError).toBeUndefined()
  } finally {
    await worker.stop()
    await connection.close()
  }
})

test('stop() hands the task back, and its accept timer does not fire afterwards', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const wire = await watchWire(name, task.task_id)
  const { worker, connection, pushes } = await inProcessWorker(name, {
    NATTER_TEST_ACCEPT_TIMEOUT_MS: '300',
  })
  try {
    worker.start()
    await pushCount({ pushes }, 1)

    await worker.stop()
    await until(async () => wire.includes('nak'))
    await Bun.sleep(500)

    expect(wire.filter((event) => event === 'nak')).toHaveLength(1)
    expect((await statusOf(name, task.task_id))?.state).toBe('delivered')
    expect(pushes).toHaveLength(1)
  } finally {
    await connection.close()
  }
})

test('stop() keeps the run timer from firing afterwards', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { timeout_s: 1 }))
  const { worker, connection, pushes } = await inProcessWorker(name)
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    await worker.callTool('update_task', { task_id: task.task_id })

    await worker.stop()
    await Bun.sleep(1300)

    expect(await storedResult(name, task.task_id)).toBeUndefined()
    expect(closePushes(pushes)).toHaveLength(0)
    expect(await queued(name)).toBe(1)
  } finally {
    await connection.close()
  }
})

test('stop() during a finish_task does not nak the task under the result', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const wire = await watchWire(name, task.task_id)
  const { worker, connection, pushes, conn } = await inProcessWorker(name)
  const { gate, reached } = gateFirst(conn, RESULT_PUBLISH)
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    const finishing = worker.callTool('finish_task', finishArgs(task.task_id))
    await until(async () => reached() === 1)

    await worker.stop()
    gate.resolve()

    expect((await finishing).isError).toBeUndefined()
    expect(wire).not.toContain('nak')
    expect(await queued(name)).toBe(0)
  } finally {
    gate.resolve()
    await connection.close()
  }
})

test('stop() during the run timeout store does not nak the task under the result', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { timeout_s: 1 }))
  const wire = await watchWire(name, task.task_id)
  const { worker, connection, pushes, conn } = await inProcessWorker(name)
  const { gate, reached } = gateFirst(conn, RESULT_PUBLISH)
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    await worker.callTool('update_task', { task_id: task.task_id })
    await until(async () => reached() === 1)

    await worker.stop()
    gate.resolve()

    await until(async () => (await queued(name)) === 0)
    expect((await storedResult(name, task.task_id))?.reason).toBe('timeout')
    expect(wire).not.toContain('nak')
  } finally {
    gate.resolve()
    await connection.close()
  }
})

test('stop() naks at once, even with the consume loop stuck on the broker', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const wire = await watchWire(name, task.task_id)
  const { worker, connection, pushes, conn } = await inProcessWorker(name)
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    const { gate, reached } = gateFirst(
      conn,
      '$JS.API.DIRECT.GET.NATTER_DEFAULT_RESULTS',
    )
    await forgetHeldTask(name)
    await publishTask(makeTask(name))
    await until(async () => reached() === 1)

    const stopped = worker.stop()
    await until(async () => wire.includes('nak'))

    gate.resolve()
    await stopped
  } finally {
    await connection.close()
  }
})

test('a latch set while the consumer is being recreated still stops the pull', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const { worker, connection, pushes, conn } = await inProcessWorker(name, {
    NATTER_TEST_ACCEPT_TIMEOUT_MS: '1500',
  })
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    const { gate, reached } = gateFirst(conn, '$JS.API.CONSUMER.CREATE.')
    await jsm.consumers.delete('NATTER_DEFAULT_TASKS', `task-${name}`)
    await until(async () => reached() === 1)
    await statusIs(name, task.task_id, 'released')

    gate.resolve()
    await until(async () =>
      consumerOf(name).then(
        () => true,
        () => false,
      ),
    )
    await Bun.sleep(400)

    expect(pushes).toHaveLength(1)
    expect((await consumerOf(name)).delivered.consumer_seq).toBe(0)
  } finally {
    await worker.stop()
    await connection.close()
  }
})

test('the accept timeout defaults to the plan value and tests can shorten it', () => {
  expect(resolveTimers({}).acceptTimeout).toBe(600_000)
  expect(
    resolveTimers({ NATTER_TEST_ACCEPT_TIMEOUT_MS: '250' }).acceptTimeout,
  ).toBe(250)
})

test('a crash after acceptance still costs an attempt', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await accept(s, task.task_id)
  await statusIs(name, task.task_id, 'accepted')
  sigkill(s)
  await s.closed

  const next = await startWorkerSession(name)

  expect((await pushCount(next, 1))?.meta).toMatchObject({
    task_id: task.task_id,
    attempt: '2',
  })
})

test('deaf sessions spend no attempts: past max_attempts it stays attempt 1, until queue TTL expiry', async () => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  const task = await publishTask(
    makeTask(name, { max_attempts: 1, queue_ttl_s: 3 }),
  )
  const attempts: (string | undefined)[] = []

  while (true) {
    const s = await startWorkerSession(name, {
      NATTER_TEST_ACCEPT_TIMEOUT_MS: '300',
    })
    await until(
      async () => s.pushes.length > 0 || (await queued(name)) === 0,
      6000,
    )
    if (s.pushes.length === 0) break
    attempts.push(s.pushes[0]?.meta.attempt)
    await statusIs(name, task.task_id, 'released')
    await s.close()
  }

  expect(attempts.length).toBeGreaterThan(1)
  expect(new Set(attempts)).toEqual(new Set(['1']))
  expect(await storedResult(name, task.task_id)).toMatchObject({
    outcome: 'failed',
    reason: 'expired_in_queue',
    attempt: 1,
  })
  expect(terminated).toHaveLength(1)
})

test('a released status with no attempt falls back to deliveryCount', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { max_attempts: 4 }))
  await writeStatus(task, { state: 'released' })
  await inflateDeliveries(name, 3)

  const s = await startWorkerSession(name)

  expect((await pushCount(s, 1))?.meta.attempt).toBe('4')
})

test('stop() during a retrying run timeout store terms nothing, and the task returns as attempt 2', async () => {
  await onFreshServer(async (server, js, manager, conn) => {
    const name = freshName()
    const terminated = await watchTerminated(name, conn)
    const restore = await fillResults(js, manager)
    const task = await publishTask(makeTask(name, { timeout_s: 1 }), js)
    const {
      worker,
      connection,
      pushes,
      conn: own,
    } = await inProcessWorker(name, {}, undefined, server.url)
    const publishes = countResultPublishes(own)
    try {
      worker.start()
      await pushCount({ pushes }, 1)
      await worker.callTool('update_task', { task_id: task.task_id })
      await until(async () => publishes() >= 2)

      await worker.stop()
      await own.flush()
      await Bun.sleep(200)

      expect(terminated).toHaveLength(0)
      expect(await queued(name, manager)).toBe(1)

      await restore()
      const next = await startWorkerSession(name, {}, server.url)
      expect((await pushCount(next, 1))?.meta).toMatchObject({
        task_id: task.task_id,
        attempt: '2',
      })
    } finally {
      await worker.stop()
      await connection.close()
    }
  })
})

test.each([
  ['an expired task', { queue_ttl_s: 1 }, 1100],
  ['an invalid envelope', { v: 99 }, 0],
])(
  'stop() during the retrying store of %s terms nothing',
  async (_kind, overrides, wait) => {
    await onFreshServer(async (server, js, manager, conn) => {
      const name = freshName()
      const terminated = await watchTerminated(name, conn)
      await fillResults(js, manager)
      const task = { ...makeTask(name), ...overrides }
      await js.publish(taskSubject(name), JSON.stringify(task))
      await Bun.sleep(wait)
      const {
        worker,
        connection,
        conn: own,
      } = await inProcessWorker(name, {}, undefined, server.url)
      const publishes = countResultPublishes(own)
      try {
        worker.start()
        await until(async () => publishes() >= 2)

        await worker.stop()
        await own.flush()
        await Bun.sleep(200)

        expect(terminated).toHaveLength(0)
        expect(await queued(name, manager)).toBe(1)
      } finally {
        await connection.close()
      }
    })
  },
)

test('a run timeout that finds this session its own finish_task closes nothing, and the retry hears success', async () => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  const task = await publishTask(makeTask(name, { timeout_s: 1 }))
  const { worker, connection, pushes, conn } = await inProcessWorker(name)
  loseFirstPubAck(conn)
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    await worker.callTool('update_task', { task_id: task.task_id })

    const lost = await worker.callTool('finish_task', finishArgs(task.task_id))
    expect(lost.isError).toBe(true)
    await until(async () => (await queued(name)) === 0)

    const retry = await worker.callTool('finish_task', finishArgs(task.task_id))

    expect(retry.isError).toBeUndefined()
    await statusIs(name, task.task_id, 'finished')
    const stored = await storedResult(name, task.task_id)
    expect(stored).toMatchObject({ outcome: 'succeeded' })
    expect(stored).not.toHaveProperty('reason')
    expect(closePushes(pushes)).toHaveLength(0)
    expect(terminated).toHaveLength(0)
  } finally {
    await worker.stop()
    await connection.close()
  }
})

test('a run timeout that finds another session its result: the late call hears already recorded', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { timeout_s: 1 }))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await accept(s, task.task_id)
  await recordResult(name, task.task_id)

  await until(async () => closePushes(s.pushes).length === 1)

  const late = await finish(s, task.task_id)
  expect(late.isError).toBe(true)
  expect(late.content[0]?.text).toBe(ALREADY_RECORDED)
  expect((await storedResult(name, task.task_id))?.summary).toBe(
    'recorded first',
  )
  expect(await queued(name)).toBe(0)
})

test('a latch set during the lookup of a lost task leaves the next task unpushed, with no attempt spent', async () => {
  const name = freshName()
  const lost = await publishTask(makeTask(name))
  const { worker, connection, pushes, conn } = await inProcessWorker(name, {
    NATTER_TEST_ACCEPT_TIMEOUT_MS: '500',
  })
  try {
    worker.start()
    await pushCount({ pushes }, 1)
    const { gate, reached } = gateFirst(
      conn,
      '$JS.API.DIRECT.GET.NATTER_DEFAULT_RESULTS',
    )
    await forgetHeldTask(name)
    await recordResult(name, lost.task_id)
    const next = await publishTask(makeTask(name))
    await until(async () => reached() === 1)
    await statusIs(name, lost.task_id, 'released')

    gate.resolve()
    await Bun.sleep(300)

    expect(pushes).toHaveLength(1)
    expect(await statusOf(name, next.task_id)).toBeUndefined()

    await worker.callTool('update_task', { task_id: lost.task_id })
    expect((await pushCount({ pushes }, 2))?.meta).toMatchObject({
      task_id: next.task_id,
      attempt: '1',
    })
  } finally {
    await worker.stop()
    await connection.close()
  }
})

test('update_task calls do not push back the run timeout', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { timeout_s: 1 }))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)

  const accepted = Date.now()
  while (closePushes(s.pushes).length === 0 && Date.now() - accepted < 1800) {
    await accept(s, task.task_id)
    await Bun.sleep(200)
  }

  expect(closePushes(s.pushes)).toHaveLength(1)
})
