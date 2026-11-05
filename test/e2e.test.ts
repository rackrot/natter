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
import {
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream'
import { connect, type NatsConnection } from '@nats-io/transport-node'
import { bootstrapStreams } from '../src/bootstrap'
import { NATS_UNAVAILABLE } from '../src/orchestrator'
import {
  encodeEnvelope,
  mintTaskId,
  resultSubject,
  taskSubject,
} from '../src/protocol'
import { startNats, type TestNats, until } from './support/nats'
import {
  callTool,
  type Session,
  startSession,
  type ToolReply,
} from './support/session'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

const NO_CREDS = join(tmpdir(), 'natter-no-creds')

let nats: TestNats
let nc: NatsConnection
let jsm: JetStreamManager
const sessions: Session[] = []

const start = async (role: string, name: string) => {
  const s = await startSession({
    NATTER_ROLE: role,
    NATTER_NAME: name,
    NATTER_URL: nats.url,
    NATTER_CREDS_DIR: NO_CREDS,
    NATTER_TEST_STARTUP_GRACE_MS: '20',
    NATTER_TEST_RESTART_MIN_MS: '100',
  })
  sessions.push(s)
  if (role === 'orchestrator') {
    await until(async () => {
      const reply = await callTool(s, 'list_tasks')
      return reply.content[0]?.text !== NATS_UNAVAILABLE
    })
  }
  return s
}

const pushed = async (s: Session, count: number) => {
  await until(async () => s.pushes.length >= count)
  return s.pushes[count - 1]
}

const answer = (reply: ToolReply) => {
  expect(reply.isError).toBeFalsy()
  return JSON.parse(reply.content[0]?.text ?? '')
}

const queued = async (worker: string) => {
  const subject = taskSubject(worker)
  const info = await jsm.streams.info('NATTER_TASKS', {
    subjects_filter: subject,
  })
  return info.state.subjects?.[subject] ?? 0
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

test('two server processes, one per role, complete a task end to end', async () => {
  const name = `w-${crypto.randomUUID()}`
  const orchestrator = await start('orchestrator', 'orchestrator')
  const worker = await start('worker', name)

  const { task_id } = answer(
    await callTool(orchestrator, 'send_task', {
      to: name,
      title: 'do the thing',
      prompt: 'the whole brief',
    }),
  )

  const task = await pushed(worker, 1)
  expect(task?.meta).toMatchObject({
    kind: 'task',
    task_id,
    from: 'orchestrator',
    attempt: '1',
  })
  expect(task?.content).toContain('the whole brief')

  const update = await callTool(worker, 'update_task', {
    task_id,
    note: 'on it',
  })
  expect(update.isError).toBeFalsy()
  await until(async () => {
    const row = answer(await callTool(orchestrator, 'get_result', { task_id }))
    return row.state === 'accepted'
  })
  const seen = answer(await callTool(orchestrator, 'get_result', { task_id }))
  expect(seen).toMatchObject({ state: 'accepted', note: 'on it', attempt: 1 })

  const data = { lines: 12 }
  const artifacts = [{ kind: 'branch', value: 'feat/the-thing' }]
  const finish = await callTool(worker, 'finish_task', {
    task_id,
    outcome: 'succeeded',
    summary: 'did the thing',
    data,
    artifacts,
  })
  expect(finish.isError).toBeFalsy()

  const result = await pushed(orchestrator, 1)
  expect(result?.content).toBe('did the thing')
  expect(result?.meta).toEqual({
    kind: 'result',
    task_id,
    worker: name,
    outcome: 'succeeded',
    data_bytes: String(JSON.stringify(data).length),
  })
  expect(
    answer(await callTool(orchestrator, 'get_result', { task_id })),
  ).toMatchObject({
    state: 'finished',
    summary: 'did the thing',
    data,
    artifacts,
  })
  const { tasks } = answer(await callTool(orchestrator, 'list_tasks'))
  expect(
    tasks.find((t: { task_id: string }) => t.task_id === task_id),
  ).toMatchObject({
    state: 'finished',
    outcome: 'succeeded',
  })
  expect(await queued(name)).toBe(0)
  expect(orchestrator.pushes).toHaveLength(1)
})

test('a forger holding the real id cannot hold off the honest result', async () => {
  const name = `w-${crypto.randomUUID()}`
  const forger = `w-${crypto.randomUUID()}`
  const orchestrator = await start('orchestrator', 'orchestrator')
  const worker = await start('worker', name)
  const { task_id } = answer(
    await callTool(orchestrator, 'send_task', {
      to: name,
      title: 'the real task',
      prompt: 'the brief',
    }),
  )
  await pushed(worker, 1)

  const forged = encodeEnvelope({
    v: 1,
    type: 'result',
    task_id,
    worker: forger,
    instance: mintTaskId(),
    attempt: 1,
    outcome: 'succeeded',
    summary: 'forged',
    finished_at: new Date().toISOString(),
  })
  if (!forged.ok) throw new Error(forged.issues.join('; '))
  // Under its own subjects, as its credential allows, claiming the msg ids an
  // honest store could use. ACLs reach subjects, not headers.
  const js = jetstream(nc)
  const claims = [
    [task_id, task_id],
    [mintTaskId(), resultSubject(name, task_id)],
  ] as const
  for (const [id, msgID] of claims) {
    const ack = await js.publish(resultSubject(forger, id), forged.bytes, {
      msgID,
    })
    expect(ack.duplicate).toBe(false)
  }

  const finish = await callTool(worker, 'finish_task', {
    task_id,
    outcome: 'succeeded',
    summary: 'the honest one',
  })

  expect(finish.isError).toBeFalsy()
  expect((await pushed(orchestrator, 1))?.content).toBe('the honest one')
  expect(orchestrator.pushes[0]?.meta.worker).toBe(name)
  expect(await queued(name)).toBe(0)
  const consumer = await jsm.consumers.info('NATTER_TASKS', `task-${name}`)
  expect(consumer.num_ack_pending).toBe(0)
  expect(
    answer(await callTool(orchestrator, 'get_result', { task_id })),
  ).toMatchObject({
    worker: name,
    state: 'finished',
    summary: 'the honest one',
  })
})

test('a task sent to a worker that is not running reaches it when it starts', async () => {
  const name = `w-${crypto.randomUUID()}`
  const orchestrator = await start('orchestrator', 'orchestrator')
  const { task_id } = answer(
    await callTool(orchestrator, 'send_task', {
      to: name,
      title: 'later',
      prompt: 'when you can',
    }),
  )
  expect(await queued(name)).toBe(1)

  const worker = await start('worker', name)

  expect((await pushed(worker, 1))?.meta).toMatchObject({
    task_id,
    attempt: '1',
  })
})
