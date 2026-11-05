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
import { connect, type NatsConnection, nuid } from '@nats-io/transport-node'
import {
  encodeEnvelope,
  type Result,
  type Sent,
  type Status,
  type Task,
} from '../src/protocol'
import {
  bootstrapStreams,
  resultSubject,
  sentSubject,
  statusSubject,
  taskSubject,
} from './support/fleet'
import { startNats, type TestNats, until } from './support/nats'
import { seen } from './support/presence'
import { callTool, type Session, startSession } from './support/session'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

const NO_CREDS = join(tmpdir(), 'natter-no-creds')

let nats: TestNats
let nc: NatsConnection
let jsm: JetStreamManager
const sessions: Session[] = []

const freshName = () => `w-${crypto.randomUUID()}`

const start = async (
  name: string,
  role: string,
  env: Record<string, string>,
) => {
  const s = await startSession({
    NATTER_ROLE: role,
    NATTER_NAME: name,
    NATTER_URL: nats.url,
    NATTER_CREDS_DIR: NO_CREDS,
    NATTER_TEST_STARTUP_GRACE_MS: '20',
    NATTER_TEST_STREAM_RETRY_MS: '100',
    NATTER_TEST_RESTART_MIN_MS: '100',
    NATTER_TEST_RESTART_MAX_MS: '300',
    ...env,
  })
  sessions.push(s)
  return s
}

const startOrchestrator = async (env: Record<string, string> = {}) => {
  const s = await start('orchestrator', 'orchestrator', env)
  await until(async () => {
    const reply = await callTool(s, 'list_tasks')
    return !reply.content[0]?.text.startsWith('NATS unavailable')
  })
  return s
}

const send = (s: Session, to: string, args: Record<string, unknown> = {}) =>
  callTool(s, 'send_task', {
    to,
    title: 'the title',
    prompt: 'the prompt',
    ...args,
  })

const sentId = (reply: { content: { text: string }[] }) =>
  (JSON.parse(reply.content[0]?.text ?? '') as { task_id: string }).task_id

/** Messages in a stream, all subjects. */
const stored = async (stream: string) =>
  (await jsm.streams.info(stream)).state.messages

const queued = async (worker: string) => {
  const subject = taskSubject(worker)
  const info = await jsm.streams.info('NATTER_DEFAULT_TASKS', {
    subjects_filter: subject,
  })
  return info.state.subjects?.[subject] ?? 0
}

const storeSent = async (worker: string, depth: number) => {
  const task_id = nuid.next()
  const encoded = encodeEnvelope({
    v: 1,
    type: 'sent',
    task_id,
    to: worker,
    title: 'the title',
    created_at: new Date().toISOString(),
    root_task_id: task_id,
    depth,
    timeout_s: 60,
    queue_ttl_s: 600,
  })
  if (!encoded.ok) throw new Error(encoded.issues.join('; '))
  await jetstream(nc).publish(sentSubject(task_id), encoded.bytes, {
    msgID: task_id,
  })
  return task_id
}

/** Stores a result the way a worker would, for a task the orchestrator sent. */
const storeResult = async (worker: string, task_id: string) => {
  const encoded = encodeEnvelope({
    v: 1,
    type: 'result',
    task_id,
    worker,
    instance: nuid.next(),
    attempt: 1,
    outcome: 'succeeded',
    summary: 'the summary',
    finished_at: new Date().toISOString(),
  } satisfies Result)
  if (!encoded.ok) throw new Error(encoded.issues.join('; '))
  await jetstream(nc).publish(resultSubject(worker, task_id), encoded.bytes, {
    expect: { lastSubjectSequence: 0 },
  })
}

const resultPushes = (s: Session) =>
  s.pushes.filter(({ meta }) => meta.kind === 'result')

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

test('a send over the per-worker rate is refused and publishes nothing, then the bucket refills', async () => {
  const worker = freshName()
  const other = freshName()
  await seen(jetstream(nc), worker)
  await seen(jetstream(nc), other)
  const s = await startOrchestrator({
    NATTER_TEST_SEND_PER_WORKER: '2',
    NATTER_TEST_RATE_WINDOW_MS: '1500',
  })

  expect((await send(s, worker)).isError).toBeFalsy()
  expect((await send(s, worker)).isError).toBeFalsy()
  const records = await stored('NATTER_DEFAULT_SENT')
  const tasks = await queued(worker)

  const refused = await send(s, worker)
  expect(refused.isError).toBe(true)
  expect(await stored('NATTER_DEFAULT_SENT')).toBe(records)
  expect(await queued(worker)).toBe(tasks)

  // The limit is per worker.
  expect((await send(s, other)).isError).toBeFalsy()

  await until(async () => !(await send(s, worker)).isError)
})

test('a send over the global rate is refused and publishes nothing', async () => {
  const [a, b, c] = [freshName(), freshName(), freshName()]
  for (const w of [a, b, c]) await seen(jetstream(nc), w)
  const s = await startOrchestrator({ NATTER_TEST_SEND_GLOBAL: '2' })

  expect((await send(s, a)).isError).toBeFalsy()
  expect((await send(s, b)).isError).toBeFalsy()
  const records = await stored('NATTER_DEFAULT_SENT')

  expect((await send(s, c)).isError).toBe(true)
  expect(await stored('NATTER_DEFAULT_SENT')).toBe(records)
  expect(await queued(c)).toBe(0)
})

test('a send the broker refuses gives its tokens back', async () => {
  const worker = freshName()
  await seen(jetstream(nc), worker)
  const s = await startOrchestrator({ NATTER_TEST_SEND_PER_WORKER: '1' })
  // A full queue for this worker: the stream keeps 16 per subject, discard new.
  const js = jetstream(nc)
  for (let i = 0; i < 16; i++) {
    const id = nuid.next()
    const task: Task = {
      v: 1,
      type: 'task',
      task_id: id,
      from: 'orchestrator',
      to: worker,
      title: 'the title',
      prompt: 'the prompt',
      created_at: new Date().toISOString(),
      timeout_s: 60,
      queue_ttl_s: 600,
      max_attempts: 3,
      root_task_id: id,
      depth: 0,
    }
    await js.publish(taskSubject(worker), JSON.stringify(task), { msgID: id })
  }

  const full = await send(s, worker)
  expect(full.isError).toBe(true)
  // Had the refused send kept its token, this would be the rate limit's error.
  expect(full.content[0]?.text).toBe((await send(s, worker)).content[0]?.text)
})

test('a send whose record publish fails without a broker answer keeps its tokens spent', async () => {
  const worker = freshName()
  await seen(jetstream(nc), worker)
  const s = await startOrchestrator({ NATTER_TEST_SEND_PER_WORKER: '1' })
  // No responders is not the broker's refusal: the record may have been stored.
  await jsm.streams.delete('NATTER_DEFAULT_SENT')
  try {
    expect((await send(s, worker)).isError).toBe(true)
  } finally {
    await bootstrapStreams(jsm)
  }

  const again = await send(s, worker)
  expect(again.isError).toBe(true)
  expect(await queued(worker)).toBe(0)
  expect(await stored('NATTER_DEFAULT_SENT')).toBe(0)
})

test('a follow-up past the depth limit is rejected and publishes nothing', async () => {
  const worker = freshName()
  await seen(jetstream(nc), worker)
  const s = await startOrchestrator()
  const five = await storeSent(worker, 5)
  const four = await storeSent(worker, 4)
  const records = await stored('NATTER_DEFAULT_SENT')

  const rejected = await send(s, worker, { parent_task_id: five })
  expect(rejected.isError).toBe(true)
  expect(await stored('NATTER_DEFAULT_SENT')).toBe(records)
  expect(await queued(worker)).toBe(0)

  const accepted = await send(s, worker, { parent_task_id: four })
  expect(accepted.isError).toBeFalsy()
  const record = await jsm.direct.getMessage('NATTER_DEFAULT_SENT', {
    last_by_subj: sentSubject(sentId(accepted)),
  })
  expect(record?.json<Sent>().depth).toBe(5)
})

test('results over the push budget are held unacked past ack_wait, then arrive once each', async () => {
  const worker = freshName()
  await seen(jetstream(nc), worker)
  const ids = [
    await storeSent(worker, 0),
    await storeSent(worker, 0),
    await storeSent(worker, 0),
  ]
  for (const id of ids) await storeResult(worker, id)

  // Refills one token a 2s, well past the 600ms ack_wait.
  const s = await startOrchestrator({
    NATTER_TEST_PUSH_BUDGET: '2',
    NATTER_TEST_RATE_WINDOW_MS: '4000',
    NATTER_TEST_ACK_WAIT_MS: '600',
    NATTER_TEST_WORKING_MS: '100',
  })

  await until(async () => resultPushes(s).length === 2)
  const consumer = await jsm.consumers.info('NATTER_DEFAULT_RESULTS', 'results')
  // The held one is still the consumer's: delivered, not acked.
  expect(consumer.num_ack_pending + consumer.num_pending).toBeGreaterThan(0)
  expect(resultPushes(s)).toHaveLength(2)

  await until(async () => resultPushes(s).length === 3)
  await until(async () => {
    const info = await jsm.consumers.info('NATTER_DEFAULT_RESULTS', 'results')
    return info.num_ack_pending === 0 && info.num_pending === 0
  })
  // working() kept the held one from coming back: three results, three deliveries.
  expect(
    (await jsm.consumers.info('NATTER_DEFAULT_RESULTS', 'results')).delivered
      .consumer_seq,
  ).toBe(3)
  expect(
    resultPushes(s)
      .map(({ meta }) => meta.task_id)
      .sort(),
  ).toEqual([...ids].sort())
})

const releasedTask = async (worker: string) => {
  const id = await storeSent(worker, 0)
  await jetstream(nc).publish(
    statusSubject(worker, id),
    statusBytes(worker, id, 'released'),
  )
  return id
}

const attentionPushes = (s: Session) =>
  s.pushes.filter(({ meta }) => meta.kind === 'attention')

test('attention pushes have a budget of their own and stay owed while it is empty', async () => {
  const worker = freshName()
  await seen(jetstream(nc), worker)
  await releasedTask(worker)
  await releasedTask(worker)

  // One token a 3s: the second push has to wait for the refill.
  const s = await startOrchestrator({
    NATTER_TEST_ATTENTION_BUDGET: '1',
    NATTER_TEST_RATE_WINDOW_MS: '3000',
    NATTER_TEST_ATTENTION_SWEEP_MS: '100',
  })

  await until(async () => attentionPushes(s).length === 2, 8000)
  const [first, second] = attentionPushes(s)
  expect((second?.at ?? 0) - (first?.at ?? 0)).toBeGreaterThanOrEqual(1500)
})

test('a backlog of held results does not delay attention', async () => {
  const worker = freshName()
  await seen(jetstream(nc), worker)
  for (let i = 0; i < 3; i++)
    await storeResult(worker, await storeSent(worker, 0))
  const open = await releasedTask(worker)

  // The results' bucket is empty for a minute after its one token.
  const s = await startOrchestrator({
    NATTER_TEST_PUSH_BUDGET: '1',
    NATTER_TEST_RATE_WINDOW_MS: '60000',
    NATTER_TEST_ATTENTION_SWEEP_MS: '100',
  })

  await until(async () => attentionPushes(s).length === 1)
  expect(attentionPushes(s)[0]?.meta.task_id).toBe(open)
  expect(resultPushes(s)).toHaveLength(1)
})

test('stopping mid-hold ends promptly and the held result reaches the next session', async () => {
  const worker = freshName()
  await seen(jetstream(nc), worker)
  const ids = [await storeSent(worker, 0), await storeSent(worker, 0)]
  for (const id of ids) await storeResult(worker, id)

  const first = await startOrchestrator({
    NATTER_TEST_PUSH_BUDGET: '1',
    NATTER_TEST_RATE_WINDOW_MS: '600000',
    NATTER_TEST_ACK_WAIT_MS: '500',
  })
  await until(async () => resultPushes(first).length === 1)

  const started = Date.now()
  first.endStdin()
  await first.closed
  expect(Date.now() - started).toBeLessThan(2500)
  await until(
    async () => !(await nats.clientNames()).includes('natter-orchestrator'),
  )

  // Unacked, so the broker hands it over once its ack_wait has run out.
  const next = await startOrchestrator({ NATTER_TEST_ACK_WAIT_MS: '500' })
  await until(async () => resultPushes(next).length >= 1)
  const pushed = [first, next].flatMap((x) => resultPushes(x))
  expect(new Set(pushed.map(({ meta }) => meta.task_id))).toEqual(new Set(ids))
})

test('a burst past max_ack_pending with a short ack_wait pushes each result once', async () => {
  const worker = freshName()
  await seen(jetstream(nc), worker)
  const ids: string[] = []
  for (let i = 0; i < 12; i++) {
    ids.push(await storeSent(worker, 0))
    await storeResult(worker, ids[i] as string)
  }

  const s = await startOrchestrator({
    NATTER_TEST_PUSH_BUDGET: '4',
    NATTER_TEST_RATE_WINDOW_MS: '2000',
    NATTER_TEST_ACK_WAIT_MS: '150',
    NATTER_TEST_WORKING_MS: '50',
  })

  await until(async () => resultPushes(s).length >= 12, 12_000)
  await until(async () => {
    const info = await jsm.consumers.info('NATTER_DEFAULT_RESULTS', 'results')
    return info.num_ack_pending === 0 && info.num_pending === 0
  })
  const got = resultPushes(s).map(({ meta }) => meta.task_id)
  expect(got).toHaveLength(12)
  expect(new Set(got)).toEqual(new Set(ids))
})

const statusBytes = (
  worker: string,
  task_id: string,
  state: Status['state'],
) => {
  const encoded = encodeEnvelope({
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
  if (!encoded.ok) throw new Error(encoded.issues.join('; '))
  return encoded.bytes
}

/** Status messages written for a worker's tasks, as they happen. */
const watchStatus = (worker: string) => {
  const seenStates: Status[] = []
  const sub = nc.subscribe(`natter.default.status.${worker}.*`)
  void (async () => {
    for await (const msg of sub) seenStates.push(msg.json<Status>())
  })()
  return { seenStates, stop: () => sub.unsubscribe() }
}

const startedTask = async (worker: string, env: Record<string, string>) => {
  const id = nuid.next()
  const task: Task = {
    v: 1,
    type: 'task',
    task_id: id,
    from: 'orchestrator',
    to: worker,
    title: 'the title',
    prompt: 'the prompt',
    created_at: new Date().toISOString(),
    timeout_s: 60,
    queue_ttl_s: 600,
    max_attempts: 3,
    root_task_id: id,
    depth: 0,
  }
  await jetstream(nc).publish(taskSubject(worker), JSON.stringify(task), {
    msgID: id,
  })
  const s = await start(worker, 'worker', env)
  await until(async () => s.pushes.some(({ meta }) => meta.kind === 'task'))
  return { s, id }
}

test('update_task bursts write at most one status per window: the first at once, the last note after', async () => {
  const worker = freshName()
  const watch = watchStatus(worker)
  const { s, id } = await startedTask(worker, {
    NATTER_TEST_STATUS_WINDOW_MS: '2500',
  })
  const accepted = () =>
    watch.seenStates.filter(({ state }) => state === 'accepted')

  const first = await callTool(s, 'update_task', { task_id: id, note: 'one' })
  expect(first.isError).toBeFalsy()
  // Well inside the window: a first write held back by it would miss this.
  await until(async () => accepted().length === 1, 1000)
  expect(accepted()[0]?.note).toBe('one')

  for (const note of ['two', 'three', 'four']) {
    expect(
      (await callTool(s, 'update_task', { task_id: id, note })).isError,
    ).toBeFalsy()
  }
  expect(accepted()).toHaveLength(1)

  await until(async () => accepted().length === 2)
  expect(accepted()[1]?.note).toBe('four')
  watch.stop()
})

test('a note still waiting is dropped when the task finishes, and a bad note is refused at once', async () => {
  const worker = freshName()
  const watch = watchStatus(worker)
  const { s, id } = await startedTask(worker, {
    NATTER_TEST_STATUS_WINDOW_MS: '400',
  })
  await callTool(s, 'update_task', { task_id: id })
  await until(async () => watch.seenStates.length === 2)

  expect(
    (await callTool(s, 'update_task', { task_id: id, note: 'x'.repeat(513) }))
      .isError,
  ).toBe(true)
  await callTool(s, 'update_task', { task_id: id, note: 'late' })
  const finished = await callTool(s, 'finish_task', {
    task_id: id,
    outcome: 'succeeded',
    summary: 'done',
  })
  expect(finished.isError).toBeFalsy()
  await until(async () => watch.seenStates.at(-1)?.state === 'finished')

  // Past the window: the dropped note must not appear after `finished`.
  const count = watch.seenStates.length
  await Bun.sleep(600)
  expect(watch.seenStates).toHaveLength(count)
  expect(watch.seenStates.at(-1)?.state).toBe('finished')
  watch.stop()
})

test('stopping the worker writes the note that was still waiting', async () => {
  const worker = freshName()
  const watch = watchStatus(worker)
  const { s, id } = await startedTask(worker, {
    NATTER_TEST_STATUS_WINDOW_MS: '600000',
  })
  await callTool(s, 'update_task', { task_id: id, note: 'one' })
  await callTool(s, 'update_task', { task_id: id, note: 'two' })
  await until(async () => watch.seenStates.some(({ note }) => note === 'one'))
  expect(watch.seenStates.some(({ note }) => note === 'two')).toBe(false)

  s.endStdin()
  await s.closed
  await until(async () => watch.seenStates.some(({ note }) => note === 'two'))
  watch.stop()
})
