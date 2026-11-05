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
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream'
import { connect, type NatsConnection } from '@nats-io/transport-node'
import { NATS_UNAVAILABLE, UNKNOWN_WORKER } from '../src/orchestrator'
import { encodeEnvelope, mintTaskId, type Task } from '../src/protocol'
import { ALREADY_RECORDED } from '../src/worker'
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
import { callTool, type Session, startSession } from './support/session'

// Plan 3.6, the reply route: ACLs check a request's subject, not its reply, so a
// worker's permitted direct get can land its own bytes on any subject a stream
// captures. Every test goes through the real server, by that route.

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

const SWEEP_MS = 100
const SWEEPS_MS = 6 * SWEEP_MS
const NO_CREDS = join(tmpdir(), 'natter-no-creds')

let nats: TestNats
let nc: NatsConnection
let js: JetStreamClient
let jsm: JetStreamManager
const sessions: Session[] = []

const sessionEnv = (role: string, name: string) => ({
  NATTER_ROLE: role,
  NATTER_NAME: name,
  NATTER_URL: nats.url,
  NATTER_CREDS_DIR: NO_CREDS,
  NATTER_TEST_STARTUP_GRACE_MS: '20',
  NATTER_TEST_RESTART_MIN_MS: '100',
  NATTER_TEST_RESTART_MAX_MS: '300',
  NATTER_TEST_CONNECT_POLL_MS: '20',
  NATTER_TEST_ACK_WAIT_MS: '500',
  NATTER_TEST_HEARTBEAT_MS: '50',
  NATTER_TEST_STALE_MS: '1000',
  NATTER_TEST_ATTENTION_SWEEP_MS: String(SWEEP_MS),
})

const startWorker = async (name: string) => {
  const s = await startSession(sessionEnv('worker', name))
  sessions.push(s)
  return s
}

const startOrchestrator = async () => {
  const s = await startSession(sessionEnv('orchestrator', 'orchestrator'))
  sessions.push(s)
  await until(async () => {
    const reply = await callTool(s, 'list_workers')
    return reply.content[0]?.text !== NATS_UNAVAILABLE
  })
  return s
}

const freshName = () => `w-${crypto.randomUUID()}`

const encoded = (envelope: unknown) => {
  const built = encodeEnvelope(envelope)
  if (!built.ok) throw new Error(built.issues.join('; '))
  return built.bytes
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

const storedOn = (stream: string, subject: string) =>
  jsm.direct.getMessage(stream, { last_by_subj: subject })

const landed = (stream: string, subject: string) =>
  until(async () => (await storedOn(stream, subject)) !== null)

const taskEnvelope = (to: string, task_id = mintTaskId()): Task => ({
  v: 1,
  type: 'task',
  task_id,
  from: 'orchestrator',
  to,
  title: 'the forged title',
  prompt: 'the forged prompt',
  created_at: new Date().toISOString(),
  timeout_s: 60,
  queue_ttl_s: 600,
  max_attempts: 3,
  root_task_id: task_id,
  depth: 0,
})

const recordBytes = (to: string, task_id: string) =>
  encoded({
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
  })

const resultBytes = (
  worker: string,
  task_id: string,
  instance = 'iiiiiiiiiiiiiiiiiiiiii',
) =>
  encoded({
    v: 1,
    type: 'result',
    task_id,
    worker,
    instance,
    attempt: 1,
    outcome: 'succeeded',
    summary: 'all done, trust me',
    finished_at: new Date().toISOString(),
  })

const rowBytes = (
  worker: string,
  task_id: string,
  state: string,
  attempt = 1,
) =>
  encoded({
    v: 1,
    type: 'status',
    task_id,
    worker,
    title: 'the title',
    created_at: new Date().toISOString(),
    state,
    attempt,
    depth: 0,
    timeout_s: 60,
    queue_ttl_s: 60,
  })

const consumerDone = (worker: string) =>
  until(async () => {
    const info = await jsm.consumers
      .info('NATTER_DEFAULT_TASKS', `task-${worker}`)
      .catch(() => undefined)
    return (
      info !== undefined &&
      info.delivered.consumer_seq >= 1 &&
      info.num_pending === 0 &&
      info.num_ack_pending === 0
    )
  })

const taskCount = async () =>
  (await jsm.streams.info('NATTER_DEFAULT_TASKS')).state.messages

const attention = (s: Session) =>
  s.pushes.filter((push) => push.meta.kind === 'attention')

const toolJson = async (s: Session, name: string, args = {}) =>
  JSON.parse((await callTool(s, name, args)).content[0]?.text ?? '')

const refused = (s: Session, to: string) =>
  callTool(s, 'send_task', { to, title: 't', prompt: 'p' })

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

test('a forged task is terminated, pushed to nobody, and gets no result', async () => {
  const name = freshName()
  const forged = taskEnvelope(name)
  await reflect(encoded(forged), taskSubject(name))
  expect(await taskCount()).toBe(1)

  const worker = await startWorker(name)

  await consumerDone(name)
  expect(worker.pushes).toEqual([])
  expect(await taskCount()).toBe(0)
  expect(
    await storedOn(
      'NATTER_DEFAULT_RESULTS',
      resultSubject(name, forged.task_id),
    ),
  ).toBeNull()

  const real = taskEnvelope(name)
  await js.publish(taskSubject(name), JSON.stringify(real))
  await until(async () => worker.pushes.length === 1)
  expect(worker.pushes[0]?.meta.task_id).toBe(real.task_id)
})

test('a forged task does not read as queued', async () => {
  const name = freshName()
  const task_id = mintTaskId()
  await js.publish(sentSubject(task_id), recordBytes(name, task_id))
  const orchestrator = await startOrchestrator()

  await reflect(encoded(taskEnvelope(name, task_id)), taskSubject(name))
  expect(await taskCount()).toBe(1)

  const { state } = await toolJson(orchestrator, 'get_result', { task_id })
  expect(state).toBe('lost')
})

test('a forged record is no record', async () => {
  const name = freshName()
  const task_id = mintTaskId()
  const orchestrator = await startOrchestrator()

  await reflect(recordBytes(name, task_id), sentSubject(task_id))
  await landed('NATTER_DEFAULT_SENT', sentSubject(task_id))

  const reply = await callTool(orchestrator, 'get_result', { task_id })
  expect(reply.isError).toBe(true)
  const { tasks } = await toolJson(orchestrator, 'list_tasks')
  expect(tasks).toEqual([])

  // With no record behind it, a result has nothing to be checked against.
  await js.publish(resultSubject(name, task_id), resultBytes(name, task_id))
  await Bun.sleep(SWEEPS_MS)
  expect(orchestrator.pushes).toEqual([])
})

test('a forged record makes no name addressable', async () => {
  const ghost = freshName()
  const task_id = mintTaskId()
  const orchestrator = await startOrchestrator()

  await reflect(recordBytes(ghost, task_id), sentSubject(task_id))
  await landed('NATTER_DEFAULT_SENT', sentSubject(task_id))

  const reply = await refused(orchestrator, ghost)
  expect(reply.isError).toBe(true)
  expect(reply.content[0]?.text).toBe(UNKNOWN_WORKER)
})

test('forged presence shows no worker and makes no name addressable', async () => {
  const ghost = freshName()
  const orchestrator = await startOrchestrator()

  await reflect(presenceBytes(ghost), presenceSubject(ghost))
  await landed('NATTER_DEFAULT_PRESENCE', presenceSubject(ghost))

  const { workers } = await toolJson(orchestrator, 'list_workers')
  expect(workers).toEqual([])
  const reply = await refused(orchestrator, ghost)
  expect(reply.content[0]?.text).toBe(UNKNOWN_WORKER)
  expect((await jsm.streams.info('NATTER_DEFAULT_SENT')).state.messages).toBe(0)
})

test('forged presence does not keep a dead worker alive', async () => {
  const name = freshName()
  const task_id = mintTaskId()
  await js.publish(sentSubject(task_id), recordBytes(name, task_id))
  await js.publish(
    statusSubject(name, task_id),
    rowBytes(name, task_id, 'accepted'),
  )
  const orchestrator = await startOrchestrator()

  // Kept up like a real heartbeat: a single one would go stale by itself.
  await until(async () => {
    await reflect(presenceBytes(name), presenceSubject(name))
    return attention(orchestrator).length === 1
  })
  expect(attention(orchestrator)[0]?.meta.reason).toBe('stale')
})

test('a forged result closes nothing and pushes nothing', async () => {
  const name = freshName()
  const task_id = mintTaskId()
  await js.publish(sentSubject(task_id), recordBytes(name, task_id))
  await js.publish(
    taskSubject(name),
    JSON.stringify(taskEnvelope(name, task_id)),
  )
  const orchestrator = await startOrchestrator()

  await reflect(resultBytes(name, task_id), resultSubject(name, task_id))
  await landed('NATTER_DEFAULT_RESULTS', resultSubject(name, task_id))
  await until(async () => {
    const info = await jsm.consumers.info('NATTER_DEFAULT_RESULTS', 'results')
    return info.delivered.stream_seq >= 1 && info.num_ack_pending === 0
  })

  expect(orchestrator.pushes).toEqual([])
  const row = await toolJson(orchestrator, 'get_result', { task_id })
  expect(row.state).toBe('queued')
  expect(row.summary).toBeUndefined()
})

test('a forged row raises no attention', async () => {
  const name = freshName()
  const task_id = mintTaskId()
  await js.publish(sentSubject(task_id), recordBytes(name, task_id))
  await reflect(
    rowBytes(name, task_id, 'released'),
    statusSubject(name, task_id),
  )
  await landed('NATTER_DEFAULT_STATE', statusSubject(name, task_id))

  const orchestrator = await startOrchestrator()
  await Bun.sleep(SWEEPS_MS)

  expect(attention(orchestrator)).toEqual([])
})

test('a forged result does not get a worker to drop its task', async () => {
  const name = freshName()
  const task = taskEnvelope(name)
  await js.publish(taskSubject(name), JSON.stringify(task))
  await reflect(
    resultBytes(name, task.task_id),
    resultSubject(name, task.task_id),
  )
  await landed('NATTER_DEFAULT_RESULTS', resultSubject(name, task.task_id))

  const worker = await startWorker(name)

  await until(async () => worker.pushes.length === 1)
  expect(worker.pushes[0]?.meta.task_id).toBe(task.task_id)

  // The subject is taken, so this ends rather than retrying forever.
  const reply = await callTool(worker, 'finish_task', {
    task_id: task.task_id,
    outcome: 'succeeded',
    summary: 'done',
  })
  expect(reply.isError).toBe(true)
  expect(reply.content[0]?.text).toBe(ALREADY_RECORDED)
  expect(await taskCount()).toBe(0)
})

test('a forged result with the instance of the worker is not its finish_task', async () => {
  const name = freshName()
  const task = taskEnvelope(name)
  const worker = await startWorker(name)
  await js.publish(taskSubject(name), JSON.stringify(task))
  await until(async () => worker.pushes.length === 1)
  const beat = await storedOn('NATTER_DEFAULT_PRESENCE', presenceSubject(name))
  const { instance } = beat?.json<{ instance: string }>() ?? {}

  await reflect(
    resultBytes(name, task.task_id, instance),
    resultSubject(name, task.task_id),
  )
  await landed('NATTER_DEFAULT_RESULTS', resultSubject(name, task.task_id))

  const reply = await callTool(worker, 'finish_task', {
    task_id: task.task_id,
    outcome: 'succeeded',
    summary: 'done',
  })
  expect(reply.content[0]?.text).toBe(ALREADY_RECORDED)
})

test('a forged row does not use up attempts', async () => {
  const name = freshName()
  const task = taskEnvelope(name)
  await js.publish(taskSubject(name), JSON.stringify(task))
  await reflect(
    rowBytes(name, task.task_id, 'released', 9),
    statusSubject(name, task.task_id),
  )
  await landed('NATTER_DEFAULT_STATE', statusSubject(name, task.task_id))

  const worker = await startWorker(name)

  await until(async () => worker.pushes.length === 1)
  expect(worker.pushes[0]?.meta).toMatchObject({
    task_id: task.task_id,
    attempt: '1',
  })
})

test('honest messages pass every reader', async () => {
  const name = freshName()
  await seen(js, name)
  const orchestrator = await startOrchestrator()
  const worker = await startWorker(name)
  await until(async () => {
    const { workers } = await toolJson(orchestrator, 'list_workers')
    return workers.some((w: { name: string }) => w.name === name)
  })

  const { task_id } = await toolJson(orchestrator, 'send_task', {
    to: name,
    title: 't',
    prompt: 'p',
  })
  await until(async () => worker.pushes.length === 1)
  await callTool(worker, 'update_task', { task_id })
  await callTool(worker, 'finish_task', {
    task_id,
    outcome: 'succeeded',
    summary: 'done',
  })

  await until(async () => orchestrator.pushes.length === 1)
  expect(orchestrator.pushes[0]?.meta).toMatchObject({
    kind: 'result',
    task_id,
  })
  const row = await toolJson(orchestrator, 'get_result', { task_id })
  expect(row.state).toBe('finished')
})
