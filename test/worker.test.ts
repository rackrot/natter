import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  expect,
  setDefaultTimeout,
  spyOn,
  test,
} from 'bun:test'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
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
import * as protocol from '../src/protocol'
import {
  encodeEnvelope,
  type Result,
  type Status,
  type Task,
} from '../src/protocol'
import { ALREADY_RECORDED, createWorker } from '../src/worker'
import {
  bootstrapStreams,
  resultSubject,
  statusSubject,
  taskSubject,
} from './support/fleet'
import { startNats, type TestNats, until } from './support/nats'
import { type Session, startSession } from './support/session'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

// Per file: it does not carry over from a module another test file imported.

const ACK_WAIT_MS = 1000
const WORKING_MS = 100
const GRACE_MS = 20
// The error stored results and full streams answer with; unnamed in the client.
const STREAM_STORE_FAILED = 10077
const NO_CREDS = join(tmpdir(), 'natter-no-creds')

let nats: TestNats
let nc: NatsConnection
let jsm: JetStreamManager
const sessions: Session[] = []

/** The env a worker session runs with, so no test talks to a real identity. */
const workerEnv = (name: string, url: string, env = {}) => ({
  NATTER_ROLE: 'worker',
  NATTER_NAME: name,
  NATTER_URL: url,
  NATTER_CREDS_DIR: NO_CREDS,
  NATTER_TEST_ACK_WAIT_MS: String(ACK_WAIT_MS),
  NATTER_TEST_WORKING_MS: String(WORKING_MS),
  NATTER_TEST_STARTUP_GRACE_MS: String(GRACE_MS),
  NATTER_TEST_STREAM_RETRY_MS: '100',
  NATTER_TEST_RESTART_MIN_MS: '100',
  // Notes are coalesced per window: test/limits.test.ts owns that.
  NATTER_TEST_STATUS_WINDOW_MS: '20',
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

// Each test gets its own worker, so its queue and durable are its own.
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

const publishRaw = (worker: string, body: string | Uint8Array) =>
  jetstream(nc).publish(taskSubject(worker), body, { msgID: nuid.next() })

const publishTask = async (task: Task) => {
  await publishRaw(task.to, JSON.stringify(task))
  return task
}

const queued = async (worker: string) => {
  const subject = taskSubject(worker)
  const info = await jsm.streams.info('NATTER_DEFAULT_TASKS', {
    subjects_filter: subject,
  })
  return info.state.subjects?.[subject] ?? 0
}

const consumerOf = (worker: string) =>
  jsm.consumers.info('NATTER_DEFAULT_TASKS', `task-${worker}`)

const storedResult = async (worker: string, taskId: string) => {
  const msg = await jsm.direct.getMessage('NATTER_DEFAULT_RESULTS', {
    last_by_subj: resultSubject(worker, taskId),
  })
  return msg ? (msg.json() as Result) : undefined
}

const statusOf = async (worker: string, taskId: string, manager = jsm) => {
  const msg = await manager.direct.getMessage('NATTER_DEFAULT_STATE', {
    last_by_subj: statusSubject(worker, taskId),
  })
  return msg ? (msg.json() as Status) : undefined
}

const pushCount = async (s: Session, count: number) => {
  await until(async () => s.pushes.length >= count)
  return s.pushes[count - 1]
}

const statusIs = (worker: string, taskId: string, state: Status['state']) =>
  until(async () => (await statusOf(worker, taskId))?.state === state)

const callTool = async (
  s: Session,
  name: string,
  args: Record<string, unknown>,
) =>
  (await s.client.callTool({ name, arguments: args })) as {
    isError?: boolean
    content: { type: string; text: string }[]
  }

const finish = (
  s: Session,
  task_id: string,
  args: Record<string, unknown> = {},
) =>
  callTool(s, 'finish_task', {
    task_id,
    outcome: 'succeeded',
    summary: 'done',
    ...args,
  })

/** Records what the broker reports about terminated messages. */
const watchTerminated = async (worker: string) => {
  const terminated: unknown[] = []
  nc.subscribe(
    `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.NATTER_DEFAULT_TASKS.task-${worker}`,
    {
      callback: (_err, msg) => {
        terminated.push(msg.json())
      },
    },
  )
  await nc.flush()
  return terminated
}

/** Stores a result the way a session would, as if `instance` had finished. */
const recordResult = async (
  worker: string,
  task_id: string,
  instance: string,
  {
    js = jetstream(nc),
    ...overrides
  }: Partial<Result> & { js?: JetStream } = {},
) => {
  const encoded = encodeEnvelope({
    v: 1,
    type: 'result',
    task_id,
    worker,
    instance,
    attempt: 1,
    outcome: 'succeeded',
    summary: 'recorded first',
    finished_at: new Date().toISOString(),
    ...overrides,
  })
  if (!encoded.ok) throw new Error(encoded.issues.join('; '))
  return js.publish(resultSubject(worker, task_id), encoded.bytes, {
    expect: { lastSubjectSequence: 0 },
  })
}

/** Writes the ledger row for `task`, as an earlier session or S6 would. */
const writeStatus = async (task: Task, fields: Partial<Status>) => {
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
  nc.publish(statusSubject(task.to, task.task_id), encoded.bytes)
  await until(async () => (await statusOf(task.to, task.task_id)) !== undefined)
}

/** Delivers the queued task `times` times to nobody, as a dead standby would. */
const inflateDeliveries = async (
  name: string,
  times: number,
  js: JetStream = jetstream(nc),
) => {
  const manager = await js.jetstreamManager()
  await manager.consumers.add('NATTER_DEFAULT_TASKS', {
    durable_name: `task-${name}`,
    filter_subject: taskSubject(name),
    ack_policy: AckPolicy.Explicit,
    ack_wait: nanos(50),
    max_ack_pending: 1,
  })
  const consumer = await js.consumers.get(
    'NATTER_DEFAULT_TASKS',
    `task-${name}`,
  )
  for (let i = 0; i < times; i++) {
    expect(await consumer.next({ expires: 2000 })).not.toBeNull()
  }
}

/** A private server, for tests that break a stream the others share. */
const onFreshServer = async (
  run: (
    server: TestNats,
    js: JetStream,
    manager: JetStreamManager,
  ) => Promise<void>,
  { bootstrap = true } = {},
) => {
  const server = await startNats({ jetstream: true })
  const conn = await connect({ servers: server.url })
  const manager = await jetstreamManager(conn)
  try {
    if (bootstrap) await bootstrapStreams(manager)
    await run(server, jetstream(conn), manager)
  } finally {
    await Promise.all(sessions.splice(0).map((s) => s.close()))
    await conn.close()
    await server.cleanup()
  }
}

const setAllowDirect = async (
  manager: JetStreamManager,
  stream: string,
  allow_direct: boolean,
) => {
  const { config } = await manager.streams.info(stream)
  await manager.streams.update(stream, { ...config, allow_direct })
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

test('a queued task is pushed once, after the grace, and stays until finish_task', async () => {
  const name = freshName()
  const task = await publishTask(
    makeTask(name, { from: 'orchestrator', timeout_s: 90 }),
  )
  const grace = 400

  const s = await startWorkerSession(name, {
    NATTER_TEST_STARTUP_GRACE_MS: String(grace),
  })
  const initialized = Date.now()

  const push = await pushCount(s, 1)
  // The small slack is the gap between the server reading `initialized` and
  // the client returning from sending it.
  expect((push?.at ?? 0) - initialized).toBeGreaterThanOrEqual(grace - 50)
  expect(push?.meta).toEqual({
    kind: 'task',
    task_id: task.task_id,
    from: 'orchestrator',
    attempt: '1',
    timeout_s: '90',
  })
  expect(push?.content).toContain(task.title)
  expect(push?.content).toContain(task.prompt)

  await statusIs(name, task.task_id, 'delivered')
  expect(await queued(name)).toBe(1)
  expect((await consumerOf(name)).num_ack_pending).toBe(1)
  expect(s.pushes).toHaveLength(1)
  expect(s.errors).toEqual([])
})

test('the durable is one-at-a-time, explicit-ack, with no delivery limit', async () => {
  const name = freshName()
  await startWorkerSession(name)
  await until(() =>
    consumerOf(name).then(
      () => true,
      () => false,
    ),
  )

  const { config } = await consumerOf(name)
  expect(config).toMatchObject({
    durable_name: `task-${name}`,
    filter_subject: taskSubject(name),
    ack_policy: 'explicit',
    ack_wait: nanos(ACK_WAIT_MS),
    max_ack_pending: 1,
    max_deliver: -1,
  })
})

test('an escape sequence and invisible characters never reach the push', async () => {
  const name = freshName()
  const prompt =
    'keep\tthis\nvisible\x1b]0;osc-title\x07 \x1b[31mred\x1b[0m‮ rtl z​ero tag\u{e0041}s\x07'
  const task = await publishTask(
    makeTask(name, { title: 'ti\x1b[2Jtle⁦', prompt }),
  )

  const s = await startWorkerSession(name)
  const push = await pushCount(s, 1)

  expect(push?.content).toBe('title\n\nkeep\tthis\nvisible red rtl zero tags')
  expect(Object.values(push?.meta ?? {}).join('')).not.toContain('title')
  expect(push?.meta.task_id).toBe(task.task_id)
})

test('a held task is not redelivered past ack_wait', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)

  await Bun.sleep(ACK_WAIT_MS * 2.5)

  const info = await consumerOf(name)
  expect(info.num_redelivered).toBe(0)
  expect(info.num_ack_pending).toBe(1)
  expect(info.delivered.consumer_seq).toBe(1)
  expect(s.pushes).toHaveLength(1)
  expect(await queued(name)).toBe(1)
  expect((await finish(s, task.task_id)).isError).toBeUndefined()
})

test('a redelivery of the task in flight swaps the handle and is not pushed again', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  // Too slow to hold the task: the broker redelivers it to this same process.
  const s = await startWorkerSession(name, {
    NATTER_TEST_ACK_WAIT_MS: '200',
    NATTER_TEST_WORKING_MS: '60000',
  })
  await pushCount(s, 1)

  await until(async () => (await consumerOf(name)).num_redelivered > 0)
  expect(s.pushes).toHaveLength(1)

  expect((await finish(s, task.task_id)).isError).toBeUndefined()
  await until(async () => (await queued(name)) === 0)
  expect((await storedResult(name, task.task_id))?.attempt).toBe(1)
})

test('update_task writes status accepted and keeps the note', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await statusIs(name, task.task_id, 'delivered')

  const first = await callTool(s, 'update_task', { task_id: task.task_id })
  expect(first.isError).toBeUndefined()
  await statusIs(name, task.task_id, 'accepted')

  await callTool(s, 'update_task', { task_id: task.task_id, note: 'halfway' })
  await until(
    async () => (await statusOf(name, task.task_id))?.note === 'halfway',
  )
  expect(await statusOf(name, task.task_id)).toMatchObject({
    state: 'accepted',
    worker: name,
    attempt: 1,
    title: task.title,
  })
  expect(await queued(name)).toBe(1)
})

test('update_task rejects an oversize note and keeps the task', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)

  const res = await callTool(s, 'update_task', {
    task_id: task.task_id,
    note: 'x'.repeat(513),
  })

  expect(res.isError).toBe(true)
  expect(await queued(name)).toBe(1)
})

test('finish_task stores a valid result, empties the queue, and the next task arrives', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const next = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)

  const res = await finish(s, task.task_id, {
    outcome: 'blocked',
    summary: 'need a decision',
    data: { answer: 42 },
    artifacts: [{ kind: 'branch', value: 'feat/x' }],
  })

  expect(res.isError).toBeUndefined()
  // Not awaited: finish_task returns only after the broker confirmed the ack.
  expect(await queued(name)).toBe(1)
  const stored = await storedResult(name, task.task_id)
  expect(protocol.ResultSchema.safeParse(stored).success).toBe(true)
  expect(stored).toMatchObject({
    task_id: task.task_id,
    worker: name,
    attempt: 1,
    outcome: 'blocked',
    summary: 'need a decision',
    data: { answer: 42 },
    artifacts: [{ kind: 'branch', value: 'feat/x' }],
  })
  await statusIs(name, task.task_id, 'finished')
  const push = await pushCount(s, 2)
  expect(push?.meta.task_id).toBe(next.task_id)
  expect(await queued(name)).toBe(1)

  await finish(s, next.task_id)
  expect(await queued(name)).toBe(0)
  expect(s.pushes).toHaveLength(2)
  expect((await consumerOf(name)).num_redelivered).toBe(0)
})

test('a second task is not pushed while one is in flight', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  const second = await publishTask(makeTask(name))

  await Bun.sleep(ACK_WAIT_MS)

  expect(s.pushes).toHaveLength(1)
  expect(await queued(name)).toBe(2)
  expect(await consumerOf(name)).toMatchObject({
    num_ack_pending: 1,
    num_pending: 1,
  })

  await finish(s, task.task_id)
  expect((await pushCount(s, 2))?.meta.task_id).toBe(second.task_id)
})

/** Makes the broker forget the held task, which frees the one delivery slot. */
const forgetHeldTask = async (name: string) => {
  const { delivered } = await consumerOf(name)
  await jsm.streams.deleteMessage('NATTER_DEFAULT_TASKS', delivered.stream_seq)
  return delivered.stream_seq
}

test('a different task arriving while one is held waits, unpushed, and costs no attempt', async () => {
  const name = freshName()
  const held = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)

  const forgotten = await forgetHeldTask(name)
  const second = await publishTask(makeTask(name))
  await until(
    async () => (await consumerOf(name)).delivered.stream_seq > forgotten,
  )

  expect(s.pushes).toHaveLength(1)
  expect(await queued(name)).toBe(1)
  expect((await finish(s, held.task_id)).isError).toBeUndefined()
  expect(await pushCount(s, 2)).toMatchObject({
    meta: { task_id: second.task_id, attempt: '1' },
  })
})

test('a different task arriving after the held one finished elsewhere replaces it', async () => {
  const name = freshName()
  const held = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)

  await forgetHeldTask(name)
  await recordResult(name, held.task_id, nuid.next())
  const second = await publishTask(makeTask(name))

  expect((await pushCount(s, 2))?.meta.task_id).toBe(second.task_id)
  expect(
    (await callTool(s, 'update_task', { task_id: held.task_id })).isError,
  ).toBe(true)
  expect((await finish(s, second.task_id)).isError).toBeUndefined()
  expect(await queued(name)).toBe(0)
})

test('a bad envelope carrying the held task_id cannot pass for its result', async () => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  const held = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)

  await forgetHeldTask(name)
  await publishRaw(name, JSON.stringify({ ...held, timeout_s: -1 }))
  await until(async () => terminated.length === 1)

  expect(await storedResult(name, held.task_id)).toBeUndefined()
  const res = await finish(s, held.task_id, { summary: 'the real work' })
  expect(res.isError).toBeUndefined()
  const stored = await storedResult(name, held.task_id)
  expect(stored?.summary).toBe('the real work')
  expect(stored).not.toHaveProperty('reason')
})

test('a plugin-written result under this instance is not taken for this finish_task', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await statusIs(name, task.task_id, 'delivered')
  const instance = (await statusOf(name, task.task_id))?.instance as string
  await recordResult(name, task.task_id, instance, {
    outcome: 'failed',
    reason: 'timeout',
  })

  const res = await finish(s, task.task_id, { summary: 'the real work' })

  expect(res.isError).toBe(true)
  expect((await storedResult(name, task.task_id))?.reason).toBe('timeout')
  expect((await statusOf(name, task.task_id))?.state).toBe('delivered')
  await until(async () => (await queued(name)) === 0)
})

test.each([
  ['no status', undefined, '1'],
  ['a status from attempt 2', { state: 'delivered', attempt: 2 }, '3'],
  ['a delivered status with no attempt', { state: 'delivered' }, '4'],
] as const)(
  'with %s the attempt ignores deliveries that reached no push',
  async (_kind, status, attempt) => {
    const name = freshName()
    const task = await publishTask(makeTask(name, { max_attempts: 4 }))
    if (status) await writeStatus(task, status)
    await inflateDeliveries(name, 3)

    const s = await startWorkerSession(name)

    const push = await pushCount(s, 1)
    expect(push?.meta).toMatchObject({ task_id: task.task_id, attempt })
    await statusIs(name, task.task_id, 'delivered')
    expect((await statusOf(name, task.task_id))?.attempt).toBe(Number(attempt))
    await finish(s, task.task_id)
    expect((await storedResult(name, task.task_id))?.attempt).toBe(
      Number(attempt),
    )
  },
)

test('an unparsable status counts deliveries instead of starting over', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name, { max_attempts: 4 }))
  nc.publish(statusSubject(name, task.task_id), 'not a status')
  await until(async () => {
    const msg = await jsm.direct.getMessage('NATTER_DEFAULT_STATE', {
      last_by_subj: statusSubject(name, task.task_id),
    })
    return msg !== null
  })
  await inflateDeliveries(name, 3)

  const s = await startWorkerSession(name)

  expect((await pushCount(s, 1))?.meta.attempt).toBe('4')
})

test('each lost session spends one attempt', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))

  for (const attempt of ['1', '2', '3']) {
    const s = await startWorkerSession(name)
    expect((await pushCount(s, 1))?.meta).toMatchObject({
      task_id: task.task_id,
      attempt,
    })
    await s.close()
  }
})

test('with status capture broken nothing is pushed, however many sessions are lost', async () => {
  await onFreshServer(async (server, js, manager) => {
    const name = freshName()
    const task = makeTask(name, { max_attempts: 2 })
    await js.publish(taskSubject(name), JSON.stringify(task))
    const { config } = await manager.streams.info('NATTER_DEFAULT_STATE')
    await manager.streams.update('NATTER_DEFAULT_STATE', {
      ...config,
      subjects: ['natter.nothing.*'],
    })
    const delivered = async () =>
      manager.consumers.info('NATTER_DEFAULT_TASKS', `task-${name}`).then(
        (info) => info.delivered.consumer_seq,
        () => 0,
      )

    for (let lost = 1; lost <= 5; lost++) {
      const s = await startWorkerSession(name, {}, server.url)
      await until(async () => (await delivered()) >= lost)
      // A push would follow the delivery within a tick.
      await Bun.sleep(100)
      expect(s.pushes).toHaveLength(0)
      expect((await finish(s, task.task_id)).isError).toBe(true)
      await s.close()
    }
    expect(
      (await manager.streams.info('NATTER_DEFAULT_TASKS')).state.messages,
    ).toBe(1)

    await bootstrapStreams(manager)
    const s = await startWorkerSession(name, {}, server.url)
    expect((await pushCount(s, 1))?.meta).toMatchObject({
      task_id: task.task_id,
      attempt: '1',
    })
  })
}, 30_000)

test('an unreadable status falls back to the delivery count and still pushes', async () => {
  await onFreshServer(async (server, js, manager) => {
    const name = freshName()
    const task = makeTask(name, { max_attempts: 4 })
    await js.publish(taskSubject(name), JSON.stringify(task))
    await setAllowDirect(manager, 'NATTER_DEFAULT_STATE', false)
    await inflateDeliveries(name, 3, js)

    const s = await startWorkerSession(name, {}, server.url)

    expect((await pushCount(s, 1))?.meta).toMatchObject({
      task_id: task.task_id,
      attempt: '4',
    })
  })
})

test('a broken result lookup holds the task without redelivery until it works', async () => {
  await onFreshServer(async (server, js, manager) => {
    const name = freshName()
    const task = makeTask(name)
    await js.publish(taskSubject(name), JSON.stringify(task))
    await setAllowDirect(manager, 'NATTER_DEFAULT_RESULTS', false)

    const s = await startWorkerSession(name, {}, server.url)
    const consumer = () =>
      manager.consumers.info('NATTER_DEFAULT_TASKS', `task-${name}`)
    await until(async () =>
      consumer().then(
        (info) => info.num_ack_pending === 1,
        () => false,
      ),
    )
    await Bun.sleep(ACK_WAIT_MS * 2.5)

    expect((await consumer()).num_redelivered).toBe(0)
    expect(s.pushes).toHaveLength(0)
    expect((await finish(s, task.task_id)).isError).toBe(true)

    await setAllowDirect(manager, 'NATTER_DEFAULT_RESULTS', true)
    expect((await pushCount(s, 1))?.meta).toMatchObject({
      task_id: task.task_id,
      attempt: '1',
    })
  })
})

test('a rejection that cannot be stored is not terminated, and lands once it can', async () => {
  await onFreshServer(async (server, js, manager) => {
    const name = freshName()
    const conn = await connect({ servers: server.url })
    const terminated: unknown[] = []
    conn.subscribe(
      `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.NATTER_DEFAULT_TASKS.task-${name}`,
      {
        callback: () => {
          terminated.push(1)
        },
      },
    )
    await conn.flush()
    const restore = await fillResults(js, manager)
    const task = { ...makeTask(name), timeout_s: -1 }
    await js.publish(taskSubject(name), JSON.stringify(task))
    const messages = async () =>
      (await manager.streams.info('NATTER_DEFAULT_TASKS')).state.messages

    await startWorkerSession(name, {}, server.url)
    const consumer = () =>
      manager.consumers.info('NATTER_DEFAULT_TASKS', `task-${name}`)
    await until(async () =>
      consumer().then(
        (info) => info.num_ack_pending === 1,
        () => false,
      ),
    )
    await Bun.sleep(ACK_WAIT_MS * 1.5)

    expect(terminated).toHaveLength(0)
    expect(await messages()).toBe(1)
    expect((await consumer()).num_redelivered).toBe(0)

    await restore()
    await until(async () => terminated.length === 1, 20000)
    expect(await messages()).toBe(0)
    const stored = await manager.direct.getMessage('NATTER_DEFAULT_RESULTS', {
      last_by_subj: resultSubject(name, task.task_id),
    })
    expect(stored?.json()).toMatchObject({
      outcome: 'failed',
      reason: 'invalid_task',
      attempt: 1,
    })
    await conn.close()
  })
}, 30000)

test.each([
  ['not JSON', 'not json at all'],
  ['not an object', '[1, 2]'],
  ['missing fields', JSON.stringify({ v: 1, type: 'task' })],
])('an invalid envelope (%s) gets term() and no push', async (_kind, body) => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  await publishRaw(name, body)
  const valid = await publishTask(makeTask(name))

  const s = await startWorkerSession(name)

  const push = await pushCount(s, 1)
  expect(push?.meta.task_id).toBe(valid.task_id)
  await until(async () => terminated.length === 1)
  expect(await queued(name)).toBe(1)
})

test.each([
  ['a future version', { v: 2 }],
  ['a bad field', { timeout_s: -1 }],
])(
  'an envelope with %s and a usable task_id records failed/invalid_task',
  async (_kind, override) => {
    const name = freshName()
    const terminated = await watchTerminated(name)
    const task = { ...makeTask(name), ...override }
    await publishRaw(name, JSON.stringify(task))

    const s = await startWorkerSession(name)

    await until(async () => terminated.length === 1)
    const stored = await storedResult(name, task.task_id)
    expect(protocol.ResultSchema.safeParse(stored).success).toBe(true)
    expect(stored).toMatchObject({
      task_id: task.task_id,
      worker: name,
      outcome: 'failed',
      reason: 'invalid_task',
      attempt: 1,
    })
    expect(s.pushes).toHaveLength(0)
    expect(await queued(name)).toBe(0)
  },
)

test('an envelope addressed to someone else is terminated unanswered', async () => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  const forged = makeTask('someone-else')
  await publishRaw(name, JSON.stringify(forged))
  const valid = await publishTask(makeTask(name))

  const s = await startWorkerSession(name)

  const push = await pushCount(s, 1)
  expect(push?.meta.task_id).toBe(valid.task_id)
  await until(async () => terminated.length === 1)
  expect(await storedResult(name, forged.task_id)).toBeUndefined()
  expect(await storedResult('someone-else', forged.task_id)).toBeUndefined()
})

test('a broken envelope addressed to someone else cannot plant a result here', async () => {
  const name = freshName()
  const terminated = await watchTerminated(name)
  const forged = { ...makeTask('someone-else'), v: 2 }
  const subject = taskSubject(name)
  const parsed = protocol.parseMessage(
    subject,
    new TextEncoder().encode(JSON.stringify(forged)),
  )
  expect(parsed).toMatchObject({ ok: false, reason: 'unsupported_version' })
  expect((parsed as { task_id?: string }).task_id).toBeUndefined()

  await publishRaw(name, JSON.stringify(forged))
  await startWorkerSession(name)

  await until(async () => terminated.length === 1)
  expect(await storedResult(name, forged.task_id)).toBeUndefined()
})

test.each([
  ['oversize summary', { summary: 'x'.repeat(4097) }],
  ['oversize data', { data: { blob: 'x'.repeat(64 * 1024) } }],
  [
    'too many artifacts',
    {
      artifacts: Array.from({ length: 33 }, () => ({
        kind: 'commit',
        value: 'abcdef1',
      })),
    },
  ],
  [
    'a local path artifact',
    { artifacts: [{ kind: 'branch', value: '/etc/passwd' }] },
  ],
  ['an unknown outcome', { outcome: 'great' }],
  ['no summary', { summary: undefined }],
  ['non-object data', { data: 'text' }],
])(
  'finish_task with %s returns isError and keeps the task',
  async (_kind, args) => {
    const name = freshName()
    const task = await publishTask(makeTask(name))
    const s = await startWorkerSession(name)
    await pushCount(s, 1)

    const res = await finish(s, task.task_id, args)

    expect(res.isError).toBe(true)
    expect(await storedResult(name, task.task_id)).toBeUndefined()
    expect(await queued(name)).toBe(1)

    expect((await finish(s, task.task_id)).isError).toBeUndefined()
    await until(async () => (await queued(name)) === 0)
  },
)

test('Claude cannot set reason or the identity fields of a result', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await statusIs(name, task.task_id, 'delivered')
  const instance = (await statusOf(name, task.task_id))?.instance

  const res = await finish(s, task.task_id, {
    reason: 'timeout',
    worker: 'someone-else',
    instance: nuid.next(),
    attempt: 9,
  })

  expect(res.isError).toBeUndefined()
  const stored = await storedResult(name, task.task_id)
  expect(stored).not.toHaveProperty('reason')
  expect(stored).toMatchObject({ worker: name, instance, attempt: 1 })
})

test('finish_task after a lost PubAck (10071, same instance) counts as success', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await statusIs(name, task.task_id, 'delivered')
  const instance = (await statusOf(name, task.task_id))?.instance as string
  await recordResult(name, task.task_id, instance)

  const res = await finish(s, task.task_id, { summary: 'the retry' })

  expect(res.isError).toBeUndefined()
  expect((await storedResult(name, task.task_id))?.summary).toBe(
    'recorded first',
  )
  expect(await queued(name)).toBe(0)
  expect((await consumerOf(name)).num_redelivered).toBe(0)
  await statusIs(name, task.task_id, 'finished')
})

test('finish_task after another session won (10071, other instance) is already recorded', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const next = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  await statusIs(name, task.task_id, 'delivered')
  await recordResult(name, task.task_id, nuid.next())

  const res = await finish(s, task.task_id, { summary: 'the loser' })

  expect(res.isError).toBe(true)
  expect(res.content[0]?.text).toBe(ALREADY_RECORDED)
  expect((await storedResult(name, task.task_id))?.summary).toBe(
    'recorded first',
  )
  expect((await pushCount(s, 2))?.meta.task_id).toBe(next.task_id)
  expect(await queued(name)).toBe(1)
  // num_redelivered only counts what is pending now. The delivery sequence
  // shows the loser was acked outright: a redelivery would have taken seq 2.
  const { num_redelivered, delivered } = await consumerOf(name)
  expect(num_redelivered).toBe(0)
  expect(delivered.consumer_seq).toBe(2)
  expect((await statusOf(name, task.task_id))?.state).toBe('delivered')
  expect(
    (await callTool(s, 'update_task', { task_id: task.task_id })).isError,
  ).toBe(true)
})

test('finish_task on a full results stream returns isError, keeps the task, and succeeds once there is room', async () => {
  await onFreshServer(async (server, js, manager) => {
    const restore = await fillResults(js, manager)
    const name = freshName()
    const task = makeTask(name)
    await js.publish(taskSubject(name), JSON.stringify(task))
    const s = await startWorkerSession(name, {}, server.url)
    await pushCount(s, 1)

    const res = await finish(s, task.task_id)

    expect(res.isError).toBe(true)
    const consumer = await manager.consumers.info(
      'NATTER_DEFAULT_TASKS',
      `task-${name}`,
    )
    expect(consumer.num_ack_pending).toBe(1)
    expect(
      (await manager.streams.info('NATTER_DEFAULT_TASKS')).state.messages,
    ).toBe(1)

    await restore()
    expect((await finish(s, task.task_id)).isError).toBeUndefined()
    expect(
      (await manager.streams.info('NATTER_DEFAULT_TASKS')).state.messages,
    ).toBe(0)
  })
})

test('a failed lookup after 10071 returns isError and keeps the task', async () => {
  await onFreshServer(async (server, js, manager) => {
    const name = freshName()
    const task = makeTask(name)
    await js.publish(taskSubject(name), JSON.stringify(task))
    const s = await startWorkerSession(name, {}, server.url)
    await pushCount(s, 1)
    let instance: string | undefined
    await until(async () => {
      instance = (await statusOf(name, task.task_id, manager))?.instance
      return instance !== undefined
    })
    await recordResult(name, task.task_id, instance as string, { js })
    await setAllowDirect(manager, 'NATTER_DEFAULT_RESULTS', false)

    const res = await finish(s, task.task_id)

    expect(res.isError).toBe(true)
    expect(
      (await manager.streams.info('NATTER_DEFAULT_TASKS')).state.messages,
    ).toBe(1)
    expect(
      (await manager.consumers.info('NATTER_DEFAULT_TASKS', `task-${name}`))
        .num_ack_pending,
    ).toBe(1)

    await setAllowDirect(manager, 'NATTER_DEFAULT_RESULTS', true)
    await until(async () => (await finish(s, task.task_id)).isError !== true)
    expect(
      (await manager.streams.info('NATTER_DEFAULT_TASKS')).state.messages,
    ).toBe(0)
  })
})

test('a missing NATTER_DEFAULT_RESULTS is survived with the task held', async () => {
  await onFreshServer(async (server, js, manager) => {
    const name = freshName()
    const task = makeTask(name)
    await js.publish(taskSubject(name), JSON.stringify(task))
    const s = await startWorkerSession(name, {}, server.url)
    await pushCount(s, 1)
    await manager.streams.delete('NATTER_DEFAULT_RESULTS')

    expect((await finish(s, task.task_id)).isError).toBe(true)
    expect(
      (await manager.streams.info('NATTER_DEFAULT_TASKS')).state.messages,
    ).toBe(1)

    await bootstrapStreams(manager)
    await until(async () => (await finish(s, task.task_id)).isError !== true)
    expect(
      (await manager.streams.info('NATTER_DEFAULT_TASKS')).state.messages,
    ).toBe(0)
  })
})

test('tools refuse a task_id that is not in flight', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)
  const other = nuid.next()

  expect((await finish(s, other)).isError).toBe(true)
  expect((await callTool(s, 'update_task', { task_id: other })).isError).toBe(
    true,
  )
  expect(await storedResult(name, other)).toBeUndefined()
  expect(await queued(name)).toBe(1)

  await finish(s, task.task_id)
  await until(async () => (await queued(name)) === 0)
  expect((await finish(s, task.task_id)).isError).toBe(true)
})

test('a task whose result is already stored is acked without a push', async () => {
  const name = freshName()
  const done = makeTask(name)
  await recordResult(name, done.task_id, nuid.next())
  await publishTask(done)
  const next = await publishTask(makeTask(name))

  const s = await startWorkerSession(name)

  expect((await pushCount(s, 1))?.meta.task_id).toBe(next.task_id)
  expect(await queued(name)).toBe(1)
})

test('tools report NATS unavailable before connecting, and the task waits', async () => {
  const down = await startNats({ jetstream: true, autostart: false })
  try {
    const name = freshName()
    const s = await startWorkerSession(name, {}, down.url)
    const id = nuid.next()

    expect((await finish(s, id)).isError).toBe(true)
    expect((await callTool(s, 'update_task', { task_id: id })).isError).toBe(
      true,
    )

    await down.start()
    const conn = await connect({ servers: down.url })
    const manager = await jetstreamManager(conn)
    await bootstrapStreams(manager)
    const task = makeTask(name)
    await jetstream(conn).publish(taskSubject(name), JSON.stringify(task))

    expect((await pushCount(s, 1))?.meta.task_id).toBe(task.task_id)
    expect((await finish(s, task.task_id)).isError).toBeUndefined()
    await conn.close()
  } finally {
    await Promise.all(sessions.splice(0).map((s) => s.close()))
    await down.cleanup()
  }
}, 15000)

test('tools return isError when the transport is refused', async () => {
  const s = await startWorkerSession(freshName(), {}, 'nats://203.0.113.1:4222')

  expect((await finish(s, nuid.next())).isError).toBe(true)
  expect(
    (await callTool(s, 'update_task', { task_id: nuid.next() })).isError,
  ).toBe(true)
})

test('tools return isError when connecting failed, and stop() still returns', async () => {
  const resolved = resolveConfig(workerEnv(freshName(), nats.url))
  if (!('config' in resolved)) throw new Error(resolved.inert)
  const worker = createWorker({
    config: resolved.config,
    connection: {
      nc: undefined,
      unavailable: 'authorization violation',
      close: async () => {},
    },
    push: async () => {},
    log: () => {},
    env: workerEnv(freshName(), nats.url),
  })
  worker.start()

  const res = await worker.callTool('finish_task', { task_id: nuid.next() })

  expect(res.isError).toBe(true)
  await worker.stop()
})

test('a stream absent at start recovers after bootstrap, and is never created by the worker', async () => {
  const bare = await startNats({ jetstream: true })
  const conn = await connect({ servers: bare.url })
  const manager = await jetstreamManager(conn)
  try {
    const name = freshName()
    const s = await startWorkerSession(
      name,
      { NATTER_PROVISION: '1' },
      bare.url,
    )
    await until(async () =>
      (await bare.clientNames()).includes(`natter-${name}`),
    )
    expect((await finish(s, nuid.next())).isError).toBe(true)
    await Bun.sleep(300)
    expect(await manager.streams.names().next()).toEqual([])

    await bootstrapStreams(manager)
    const task = makeTask(name)
    await jetstream(conn).publish(taskSubject(name), JSON.stringify(task))

    expect((await pushCount(s, 1))?.meta.task_id).toBe(task.task_id)
  } finally {
    await Promise.all(sessions.splice(0).map((s) => s.close()))
    await conn.close()
    await bare.cleanup()
  }
})

test('a drifted durable is brought back to the wanted settings', async () => {
  const name = freshName()
  await jsm.consumers.add('NATTER_DEFAULT_TASKS', {
    durable_name: `task-${name}`,
    filter_subject: taskSubject(name),
    ack_policy: AckPolicy.Explicit,
    max_ack_pending: 5,
    max_deliver: 2,
  })

  await startWorkerSession(name)

  await until(async () => (await consumerOf(name)).config.max_ack_pending === 1)
  expect((await consumerOf(name)).config).toMatchObject({
    ack_wait: nanos(ACK_WAIT_MS),
    max_deliver: -1,
  })
})

test('a durable deleted under a running worker is recreated', async () => {
  const name = freshName()
  const s = await startWorkerSession(name)
  await until(() =>
    consumerOf(name).then(
      () => true,
      () => false,
    ),
  )

  await jsm.consumers.delete('NATTER_DEFAULT_TASKS', `task-${name}`)
  const task = await publishTask(makeTask(name))

  expect((await pushCount(s, 1))?.meta.task_id).toBe(task.task_id)
})

test('closing stdin with a task in flight exits within 3s and leaves the task queued', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)

  const started = Date.now()
  s.endStdin()
  await s.closed

  expect(Date.now() - started).toBeLessThan(3000)
  expect(await nats.clientNames()).not.toContain(`natter-${name}`)
  expect(await queued(name)).toBe(1)

  const next = await startWorkerSession(name)
  const push = await pushCount(next, 1)
  expect(push?.meta).toMatchObject({ task_id: task.task_id, attempt: '2' })
})

test('sessions outside the worker role have no worker tools', async () => {
  const orchestrator = await startSession({
    ...workerEnv('boss', nats.url),
    NATTER_ROLE: 'orchestrator',
  })
  const inert = await startSession({ NATTER_URL: nats.url })
  sessions.push(orchestrator, inert)
  const name = freshName()
  const worker = await startWorkerSession(name)

  expect(
    (await orchestrator.client.listTools()).tools.map((t) => t.name),
  ).not.toContain('finish_task')
  expect((await inert.client.listTools()).tools).toEqual([])
  await expect(finish(orchestrator, nuid.next())).rejects.toThrow()
  await expect(finish(inert, nuid.next())).rejects.toThrow()

  const { tools } = await worker.client.listTools()
  expect(tools.map((t) => t.name).sort()).toEqual([
    'finish_task',
    'update_task',
  ])
  expect(
    Object.keys(
      tools.find((t) => t.name === 'finish_task')?.inputSchema.properties ?? {},
    ).sort(),
  ).toEqual(['artifacts', 'data', 'outcome', 'summary', 'task_id'])
})

test('two finish_task calls at once store one result and one wins', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const s = await startWorkerSession(name)
  await pushCount(s, 1)

  const results = await Promise.all([
    finish(s, task.task_id, { summary: 'one' }),
    finish(s, task.task_id, { summary: 'two' }),
  ])

  expect(results.filter((r) => !r.isError)).toHaveLength(1)
  await until(async () => (await queued(name)) === 0)
  expect(s.pushes).toHaveLength(1)
})

test('timers default to the plan values, and only a positive override counts', () => {
  expect(resolveTimers({})).toMatchObject({
    ackWait: 120_000,
    working: 30_000,
    startupGrace: 5000,
    streamRetry: 10_000,
    restartMin: 1000,
    restartMax: 15_000,
  })
  const { ackWait, working } = resolveTimers({
    NATTER_TEST_ACK_WAIT_MS: '250',
    NATTER_TEST_WORKING_MS: 'soon',
  })
  expect(ackWait).toBe(250)
  expect(working).toBe(30_000)
})

// In-process, because a subprocess gives no way to make buildMeta fail: every
// field it checks already passed TaskSchema.
const inProcessWorker = (name: string, push = async () => {}) => {
  const resolved = resolveConfig(workerEnv(name, nats.url))
  if (!('config' in resolved)) throw new Error(resolved.inert)
  const log = () => {}
  const connection = startConnection(resolved.config, log)
  const pushes: { content: string; meta: Record<string, string> }[] = []
  const worker = createWorker({
    config: resolved.config,
    connection,
    push: async (content, meta) => {
      pushes.push({ content, meta })
      await push()
    },
    log,
    env: workerEnv(name, nats.url),
  })
  return { worker, connection, pushes }
}

test('a buildMeta throw leaves the task for redelivery at the same attempt, with the loop still running', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const spy = spyOn(protocol, 'buildMeta').mockImplementationOnce(() => {
    throw new protocol.WireError('invalid meta value: from')
  })
  const { worker, connection, pushes } = inProcessWorker(name)
  try {
    worker.start()
    await until(async () => spy.mock.calls.length === 1)
    expect(await queued(name)).toBe(1)

    await until(async () => pushes.length === 1)
    // No `delivered` status was written, so no attempt was spent.
    expect(pushes[0]?.meta).toMatchObject({
      task_id: task.task_id,
      attempt: '1',
    })
    expect((await consumerOf(name)).num_redelivered).toBe(1)
  } finally {
    spy.mockRestore()
    await worker.stop()
    await connection.close()
  }
})

test('a failed push leaves the task for redelivery, unheld', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  let fail = true
  const { worker, connection, pushes } = inProcessWorker(name, async () => {
    if (fail) {
      fail = false
      throw new Error('transport closed')
    }
  })
  try {
    worker.start()
    await until(async () => pushes.length === 1)
    const refused = await worker.callTool('finish_task', {
      task_id: task.task_id,
      outcome: 'succeeded',
      summary: 'done',
    })
    expect(refused.isError).toBe(true)

    await until(async () => pushes.length === 2)
    expect(pushes[1]?.meta.attempt).toBe('2')
  } finally {
    await worker.stop()
    await connection.close()
  }
})

test('stop() during a delivery means no push and nothing held', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const real = protocol.fleetScope
  let stopped: Promise<void> | undefined
  let worker: ReturnType<typeof createWorker> | undefined
  // The status read is the last await before the task is held. The worker
  // builds its scope once, so the spy goes in before it is created.
  const spy = spyOn(protocol, 'fleetScope').mockImplementationOnce((fleet) => {
    const scope = real(fleet)
    return {
      ...scope,
      statusSubject: (w, id) => {
        stopped ??= worker?.stop()
        return scope.statusSubject(w, id)
      },
    }
  })
  const made = inProcessWorker(name)
  worker = made.worker
  const { connection, pushes } = made
  try {
    worker.start()
    await until(async () => stopped !== undefined)
    await stopped

    expect(pushes).toHaveLength(0)
    expect(await statusOf(name, task.task_id)).toBeUndefined()
    expect(await queued(name)).toBe(1)
  } finally {
    spy.mockRestore()
    await connection.close()
  }
})

test('stop() ends the consume loop and the working() timer', async () => {
  const name = freshName()
  const task = await publishTask(makeTask(name))
  const { worker, connection, pushes } = inProcessWorker(name)
  worker.start()
  await until(async () => pushes.length === 1)

  await worker.stop()

  expect(await queued(name)).toBe(1)
  await until(async () => (await consumerOf(name)).num_waiting === 0)
  expect(
    (await worker.callTool('update_task', { task_id: task.task_id })).isError,
  ).toBe(true)
  // The connection stays open, so only a stopped working() lets this through.
  const next = await startWorkerSession(name)
  expect((await pushCount(next, 1))?.meta.attempt).toBe('2')
  expect(pushes).toHaveLength(1)
  await connection.close()
})
