import { afterAll, beforeAll, expect, setDefaultTimeout, test } from 'bun:test'
import { mkdtempSync, rmSync } from 'node:fs'
import { readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  type JetStreamClient,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream'
import { connect, type NatsConnection, nuid } from '@nats-io/transport-node'
import { bootstrapCommand } from '../src/config'
import type { StatusSnapshot } from '../src/orchestrator'
import { NATS_UNAVAILABLE } from '../src/orchestrator'
import { type FleetScope, fleetScope } from '../src/protocol'
import { startNats, type TestNats, until } from './support/nats'
import { presenceBytes } from './support/presence'
import {
  callTool,
  type Session,
  startSession,
  type ToolReply,
} from './support/session'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

const NO_CREDS = join(tmpdir(), 'natter-no-creds')
// Bun fixes os.homedir() at startup, so a held file only lands here through a
// subprocess's HOME.
const HOME = mkdtempSync(join(tmpdir(), 'natter-fleets-home-'))
const STALE_MS = 1000

interface Fleet {
  name: string
  scope: FleetScope
  orchestrator: Session
  api: Session
  /** Where `api` writes the task it holds. */
  heldPath: string
}

let nats: TestNats
let nc: NatsConnection
let js: JetStreamClient
let jsm: JetStreamManager
let a: Fleet
let b: Fleet
const extra: Session[] = []

const sessionEnv = (
  url: string,
  fleet: string,
  role: string,
  name: string,
) => ({
  NATTER_ROLE: role,
  NATTER_NAME: name,
  NATTER_FLEET: fleet,
  NATTER_URL: url,
  NATTER_CREDS_DIR: NO_CREDS,
  HOME,
  // `true` ignores its argument: a test never opens a real browser.
  NATTER_TEST_OPENER: 'true',
  NATTER_TEST_STARTUP_GRACE_MS: '20',
  NATTER_TEST_CONNECT_POLL_MS: '20',
  NATTER_TEST_HEARTBEAT_MS: '50',
  NATTER_TEST_STALE_MS: String(STALE_MS),
  NATTER_TEST_ATTENTION_SWEEP_MS: '50',
  NATTER_TEST_STREAM_RETRY_MS: '100',
  NATTER_TEST_RESTART_MIN_MS: '100',
})

const answer = (reply: ToolReply) => {
  expect(reply.isError).toBeFalsy()
  return JSON.parse(reply.content[0]?.text ?? '')
}

const workerNames = async (fleet: Fleet) => {
  const { workers } = answer(await callTool(fleet.orchestrator, 'list_workers'))
  return (workers as { name: string }[]).map((worker) => worker.name)
}

const workerState = async (fleet: Fleet, name: string) => {
  const { workers } = answer(await callTool(fleet.orchestrator, 'list_workers'))
  return (workers as { name: string; state: string }[]).find(
    (worker) => worker.name === name,
  )?.state
}

const taskIds = async (fleet: Fleet) => {
  const { tasks } = answer(await callTool(fleet.orchestrator, 'list_tasks'))
  return (tasks as { task_id: string }[]).map((task) => task.task_id)
}

const startFleet = async (name: string): Promise<Fleet> => {
  const session = `session-${name}`
  const orchestrator = await startSession({
    ...sessionEnv(nats.url, name, 'orchestrator', 'orchestrator'),
    NATTER_PROVISION: '1',
  })
  const api = await startSession({
    ...sessionEnv(nats.url, name, 'worker', 'api'),
    CLAUDE_CODE_SESSION_ID: session,
  })
  const fleet = {
    name,
    scope: fleetScope(name),
    orchestrator,
    api,
    heldPath: join(HOME, '.local', 'state', 'natter', 'held', session),
  }
  await until(async () => {
    const reply = await callTool(orchestrator, 'list_workers')
    return (
      !reply.isError &&
      JSON.parse(reply.content[0]?.text ?? '').workers.length === 1
    )
  })
  return fleet
}

const pushesOf = (s: Session, kind: string) =>
  s.pushes.filter((push) => push.meta.kind === kind)

const sendToApi = async (fleet: Fleet, title = 'the title') => {
  const { task_id } = answer(
    await callTool(fleet.orchestrator, 'send_task', {
      to: 'api',
      title,
      prompt: 'the prompt',
    }),
  )
  return task_id as string
}

const pushedTo = (s: Session, kind: string, taskId: string) =>
  until(async () =>
    pushesOf(s, kind).some((push) => push.meta.task_id === taskId),
  )

const finish = async (fleet: Fleet, task_id: string) => {
  const reply = await callTool(fleet.api, 'finish_task', {
    task_id,
    outcome: 'succeeded',
    summary: 'done',
  })
  expect(reply.isError).toBeFalsy()
}

/** A task taken all the way through `fleet`. */
const roundTrip = async (fleet: Fleet) => {
  const task_id = await sendToApi(fleet)
  await pushedTo(fleet.api, 'task', task_id)
  await callTool(fleet.api, 'update_task', { task_id })
  await finish(fleet, task_id)
  await pushedTo(fleet.orchestrator, 'result', task_id)
  return task_id
}

beforeAll(async () => {
  nats = await startNats({ jetstream: true })
  nc = await connect({ servers: nats.url })
  js = jetstream(nc)
  jsm = await jetstreamManager(nc)
  a = await startFleet('fleet-a')
  b = await startFleet('fleet-b')
})

afterAll(async () => {
  await Promise.all(
    [a, b].flatMap((fleet) =>
      fleet ? [fleet.orchestrator.close(), fleet.api.close()] : [],
    ),
  )
  await Promise.all(extra.map((s) => s.close()))
  await nc?.close()
  await nats?.cleanup()
  rmSync(HOME, { recursive: true, force: true })
})

test('each orchestrator created the five streams of its own fleet', async () => {
  const names = await jsm.streams.names().next()

  expect([...names].sort()).toEqual(
    [a, b].flatMap((fleet) => Object.values(fleet.scope.streams)).sort(),
  )
})

test('a task and its result stay in their fleet, with a worker of one name in both', async () => {
  const task_id = await sendToApi(a)

  await pushedTo(a.api, 'task', task_id)
  await callTool(a.api, 'update_task', { task_id })
  await until(async () => (await workerState(a, 'api')) === 'busy')
  expect(await workerState(b, 'api')).toBe('idle')
  expect(pushesOf(b.api, 'task').map((p) => p.meta.task_id)).not.toContain(
    task_id,
  )

  // Read before it ends, while a real worker holds this fleet's task.
  await until(() =>
    readFile(a.heldPath, 'utf8').then(
      () => true,
      () => false,
    ),
  )
  expect(JSON.parse(await readFile(a.heldPath, 'utf8')).fleet).toBe('fleet-a')

  await finish(a, task_id)
  await pushedTo(a.orchestrator, 'result', task_id)
  await until(async () => (await workerState(b, 'api')) === 'idle')

  expect(
    pushesOf(b.orchestrator, 'result').map((p) => p.meta.task_id),
  ).not.toContain(task_id)
  expect(await taskIds(b)).not.toContain(task_id)
  const unknown = await callTool(b.orchestrator, 'get_result', { task_id })
  expect(unknown.isError).toBe(true)
  expect(
    answer(await callTool(a.orchestrator, 'get_result', { task_id })),
  ).toMatchObject({
    state: 'finished',
    summary: 'done',
  })
})

test('a worker only one fleet has is absent from the other, and cannot be sent to from it', async () => {
  const only = `only-${nuid.next().toLowerCase()}`
  await js.publish(a.scope.presenceSubject(only), presenceBytes(only))

  expect(await workerNames(a)).toContain(only)
  expect(await workerNames(b)).not.toContain(only)
  const refused = await callTool(b.orchestrator, 'send_task', {
    to: only,
    title: 't',
    prompt: 'p',
  })
  expect(refused.isError).toBe(true)
})

/** A task held by a worker that then falls silent, so it goes stale. */
const ghostTask = async (fleet: Fleet) => {
  const name = `ghost-${nuid.next().toLowerCase()}`
  const subject = fleet.scope.presenceSubject(name)
  await js.publish(subject, presenceBytes(name))
  const { task_id } = answer(
    await callTool(fleet.orchestrator, 'send_task', {
      to: name,
      title: 't',
      prompt: 'p',
    }),
  )
  await js.publish(subject, presenceBytes(name, { state: 'busy', task_id }))
  return task_id as string
}

test('a stale worker raises attention only in its own fleet', async () => {
  const task_id = await ghostTask(a)
  await pushedTo(a.orchestrator, 'attention', task_id)
  expect(pushesOf(a.orchestrator, 'attention')[0]?.meta.reason).toBe('stale')

  // b's sweep works and has run since, so its silence about a's task means something.
  await pushedTo(b.orchestrator, 'attention', await ghostTask(b))

  expect(
    pushesOf(b.orchestrator, 'attention').map((push) => push.meta.task_id),
  ).not.toContain(task_id)
  expect(await taskIds(b)).not.toContain(task_id)
})

test('both task-api durables exist, on different streams with different filters', async () => {
  const [ours, theirs] = await Promise.all(
    [a, b].map((fleet) =>
      jsm.consumers.info(fleet.scope.streams.tasks, 'task-api'),
    ),
  )

  expect(ours?.stream_name).toBe(a.scope.streams.tasks)
  expect(theirs?.stream_name).toBe(b.scope.streams.tasks)
  expect(ours?.config.filter_subject).toBe(a.scope.taskSubject('api'))
  expect(theirs?.config.filter_subject).toBe(b.scope.taskSubject('api'))
})

test('a full results stream in one fleet leaves the other able to store', async () => {
  const stream = a.scope.streams.results
  const { config, state } = await jsm.streams.info(stream)
  await jsm.streams.update(stream, {
    ...config,
    max_msgs: state.messages + 1,
  })
  try {
    const filler = () => a.scope.resultSubject('filler', nuid.next())
    await js.publish(filler(), 'x')
    await expect(js.publish(filler(), 'x')).rejects.toThrow()

    await roundTrip(b)
  } finally {
    await jsm.streams.update(stream, config)
  }
})

test('the status page of a fleet shows that fleet', async () => {
  const task_id = await roundTrip(a)
  const theirs = await roundTrip(b)
  const reply = await callTool(a.orchestrator, 'status_page')
  const url = new URL(reply.content[0]?.text.split(' ').pop() ?? '')

  const res = await fetch(new URL(`/status.json${url.search}`, url))
  const data = (await res.json()) as StatusSnapshot

  expect(data.fleet).toBe('fleet-a')
  const shown = data.tasks.tasks.map((task) => task.task_id)
  expect(shown).toContain(task_id)
  expect(shown).not.toContain(theirs)
  expect(data.streams.map((stream) => stream.name).sort()).toEqual(
    Object.values(a.scope.streams).sort(),
  )
})

test('instructions name the session fleet', () => {
  for (const session of [a.orchestrator, a.api]) {
    expect(session.client.getInstructions()).toContain('fleet-a')
    expect(session.client.getInstructions()).not.toContain('fleet-b')
  }
})

test('a hint names the fleet, the default included', async () => {
  const bare = await startNats({ jetstream: true })
  try {
    const orchestrator = await startSession(
      sessionEnv(bare.url, 'x', 'orchestrator', 'orchestrator'),
    )
    const worker = await startSession(
      sessionEnv(bare.url, 'x', 'worker', 'api'),
    )
    extra.push(orchestrator, worker)
    const command = 'NATTER_FLEET=x mise run bootstrap'

    await until(async () => {
      const reply = await callTool(orchestrator, 'list_tasks')
      return reply.content[0]?.text !== NATS_UNAVAILABLE
    })
    const reply = await callTool(orchestrator, 'list_tasks')
    expect(reply.content[0]?.text).toContain(command)
    await until(async () => {
      const held = await callTool(worker, 'finish_task', {
        task_id: nuid.next(),
        outcome: 'succeeded',
        summary: 'done',
      })
      return held.content[0]?.text.includes(command) ?? false
    })

    expect(bootstrapCommand('default')).toBe(
      'NATTER_FLEET=default mise run bootstrap',
    )
  } finally {
    await Promise.all(extra.splice(0).map((s) => s.close()))
    await bare.cleanup()
  }
})
