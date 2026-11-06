import { afterAll, beforeAll, expect, setDefaultTimeout, test } from 'bun:test'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  DiscardPolicy,
  JetStreamApiCodes,
  JetStreamApiError,
  type JetStreamManager,
  jetstream,
  jetstreamManager,
  RetentionPolicy,
  StorageType,
} from '@nats-io/jetstream'
import {
  connect,
  errors,
  type NatsConnection,
  nanos,
  nuid,
} from '@nats-io/transport-node'
import {
  deleteStreams,
  diff,
  provisionStreams,
  streamsFor,
} from '../src/bootstrap'
import { MAX_TIMEOUT_S } from '../src/config'
import { fleetScope, MAX_MESSAGE_BYTES } from '../src/protocol'
import {
  bootstrapStreams,
  PRESENCE_STREAM,
  presenceSubject,
  RESULTS_STREAM,
  resultSubject,
  SENT_STREAM,
  STATE_STREAM,
  STREAMS,
  sentSubject,
  statusSubject,
  TASKS_STREAM,
  taskSubject,
} from './support/fleet'
import { startNats, type TestNats, until } from './support/nats'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

// Maximum messages per subject reached; the client has no name for this code.
const STREAM_STORE_FAILED = 10077
const MIB = 1024 * 1024

let nats: TestNats
let nc: NatsConnection
let jsm: JetStreamManager

const connectTo = async (url: string) => {
  const conn = await connect({ servers: url })
  return { conn, manager: await jetstreamManager(conn) }
}

const configOf = async (name: string) => (await jsm.streams.info(name)).config

/** The fields bootstrap owns, as stored by the server. */
const managed = async (want: (typeof STREAMS)[number]) => {
  const config = await configOf(want.name)
  return Object.fromEntries(
    Object.keys(want).map((k) => [
      k,
      config[k as keyof typeof config] ?? false,
    ]),
  )
}

/** The JetStream API error code a promise rejects with. */
const apiCode = async (p: Promise<unknown>) => {
  try {
    await p
  } catch (err) {
    if (err instanceof JetStreamApiError) return err.code
    throw err
  }
}

const messagesIn = async (name: string) =>
  (await jsm.streams.info(name)).state.messages

const onFreshServer = async (
  run: (
    server: TestNats,
    conn: NatsConnection,
    manager: JetStreamManager,
  ) => Promise<void>,
) => {
  const server = await startNats({ jetstream: true })
  const { conn, manager } = await connectTo(server.url)
  try {
    await run(server, conn, manager)
  } finally {
    await conn.close()
    await server.cleanup()
  }
}

/** Runs the real command, so its identity and connection policy are in play. */
const runBootstrap = async (url: string) => {
  const proc = Bun.spawn(['bun', 'src/bootstrap.ts'], {
    env: {
      PATH: process.env.PATH ?? '',
      NATTER_URL: url,
      NATTER_CREDS_DIR: join(tmpdir(), 'natter-no-creds'),
    },
    stdout: 'pipe',
    stderr: 'ignore',
    // A hang must not outlive its test; a kill also exits other than 1.
    timeout: 4000,
  })
  const [stdout, code] = await Promise.all([
    new Response(proc.stdout).text(),
    proc.exited,
  ])
  return { stdout, code }
}

const recordResult = async () => {
  const taskId = nuid.next()
  const subject = resultSubject('alpha', taskId)
  const { seq } = await jetstream(nc).publish(subject, 'first', {
    expect: { lastSubjectSequence: 0 },
  })
  return { subject, seq }
}

const storedResult = async (subject: string) => {
  const msg = await jsm.streams.getMessage('NATTER_DEFAULT_RESULTS', {
    last_by_subj: subject,
  })
  return { seq: msg?.seq, value: msg?.string() }
}

beforeAll(async () => {
  nats = await startNats({ jetstream: true })
  ;({ conn: nc, manager: jsm } = await connectTo(nats.url))
})

afterAll(async () => {
  await nc?.close()
  await nats?.cleanup()
})

test('creates the five streams with the plan settings', async () => {
  const outcomes = await bootstrapStreams(jsm)
  expect(outcomes.map((o) => [o.name, o.action])).toEqual([
    ['NATTER_DEFAULT_TASKS', 'created'],
    ['NATTER_DEFAULT_RESULTS', 'created'],
    ['NATTER_DEFAULT_STATE', 'created'],
    ['NATTER_DEFAULT_PRESENCE', 'created'],
    ['NATTER_DEFAULT_SENT', 'created'],
  ])

  const day = 24 * 60 * 60 * 1000
  const base = {
    storage: 'file',
    duplicate_window: nanos(2 * 60 * 1000),
    max_msg_size: MAX_MESSAGE_BYTES,
  }
  expect(await managed(TASKS_STREAM)).toEqual({
    ...base,
    name: 'NATTER_DEFAULT_TASKS',
    subjects: ['natter.default.task.*'],
    retention: 'workqueue',
    max_age: nanos(7 * day),
    max_msgs_per_subject: 16,
    discard: 'new',
    discard_new_per_subject: true,
    allow_direct: false,
    max_bytes: 256 * MIB,
  })
  expect(await managed(RESULTS_STREAM)).toEqual({
    ...base,
    name: 'NATTER_DEFAULT_RESULTS',
    subjects: ['natter.default.result.*.*'],
    retention: 'limits',
    max_age: nanos(14 * day),
    max_msgs_per_subject: 1,
    discard: 'new',
    discard_new_per_subject: true,
    allow_direct: true,
    max_bytes: 1024 * MIB,
  })
  expect(await managed(STATE_STREAM)).toEqual({
    ...base,
    name: 'NATTER_DEFAULT_STATE',
    subjects: ['natter.default.status.*.*'],
    retention: 'limits',
    max_age: nanos(14 * day),
    max_msgs_per_subject: 1,
    discard: 'old',
    discard_new_per_subject: false,
    allow_direct: true,
    max_bytes: 256 * MIB,
  })
  expect(await managed(PRESENCE_STREAM)).toEqual({
    ...base,
    name: 'NATTER_DEFAULT_PRESENCE',
    subjects: ['natter.default.presence.*'],
    retention: 'limits',
    max_age: nanos(14 * day),
    max_msgs_per_subject: 1,
    discard: 'old',
    discard_new_per_subject: false,
    allow_direct: true,
    max_msg_size: 4096,
    max_bytes: 64 * MIB,
  })
  expect(await managed(SENT_STREAM)).toEqual({
    ...base,
    name: 'NATTER_DEFAULT_SENT',
    subjects: ['natter.default.sent.*'],
    retention: 'limits',
    max_age: nanos(30 * day),
    max_msgs_per_subject: 1,
    discard: 'new',
    discard_new_per_subject: true,
    allow_direct: true,
    max_bytes: 256 * MIB,
  })
})

test('a record cannot be replaced, and a retry of it does not duplicate', async () => {
  const js = jetstream(nc)
  const taskId = nuid.next()
  const subject = sentSubject(taskId)
  const first = await js.publish(subject, 'first', { msgID: taskId })

  expect(
    (await js.publish(subject, 'first', { msgID: taskId })).duplicate,
  ).toBe(true)
  expect(await apiCode(js.publish(subject, 'second'))).toBe(STREAM_STORE_FAILED)
  const stored = await jsm.direct.getMessage('NATTER_DEFAULT_SENT', {
    last_by_subj: subject,
  })
  expect(stored?.seq).toBe(first.seq)
  expect(stored?.string()).toBe('first')
})

test('a record refuses a body over the message limit', async () => {
  const js = jetstream(nc)
  await expect(
    js.publish(sentSubject(nuid.next()), new Uint8Array(MAX_MESSAGE_BYTES + 1)),
  ).rejects.toThrow()
})

test('a task cannot outlive the results stream, so aged_out hides no open task', () => {
  const longest = TASKS_STREAM.max_age + nanos(MAX_TIMEOUT_S * 1000)
  expect(longest).toBeLessThan(RESULTS_STREAM.max_age)
})

test('a heartbeat over 4 KiB is rejected, one at the limit is stored', async () => {
  const js = jetstream(nc)
  await expect(
    js.publish(presenceSubject('big'), new Uint8Array(4097)),
  ).rejects.toThrow()
  const edge = new Uint8Array(4096 - 1024)
  expect((await js.publish(presenceSubject('big'), edge)).seq).toBeGreaterThan(
    0,
  )
})

test('a server with presence on NATTER_DEFAULT_STATE moves it to its own stream', async () => {
  await onFreshServer(async (_server, conn, manager) => {
    await manager.streams.add({
      ...STATE_STREAM,
      subjects: [...STATE_STREAM.subjects, 'natter.default.presence.*'],
    })

    const outcomes = await bootstrapStreams(manager)

    expect(
      outcomes.find((o) => o.name === 'NATTER_DEFAULT_STATE'),
    ).toMatchObject({
      action: 'updated',
      changed: ['subjects'],
    })
    expect(
      outcomes.find((o) => o.name === 'NATTER_DEFAULT_PRESENCE')?.action,
    ).toBe('created')
    const { seq } = await jetstream(conn).publish(
      presenceSubject('alpha'),
      'here',
    )
    const stored = await manager.direct.getMessage('NATTER_DEFAULT_PRESENCE', {
      last_by_subj: presenceSubject('alpha'),
    })
    expect(stored?.seq).toBe(seq)
  })
})

test('a run keeps the records of a NATTER_DEFAULT_SENT that has drifted', async () => {
  await onFreshServer(async (_server, conn, manager) => {
    await manager.streams.add({ ...SENT_STREAM, max_bytes: 512 * MIB })
    await jetstream(conn).publish(sentSubject(nuid.next()), 'kept')

    const outcomes = await bootstrapStreams(manager)

    expect(
      outcomes.find((o) => o.name === 'NATTER_DEFAULT_SENT'),
    ).toMatchObject({
      action: 'updated',
      changed: ['max_bytes'],
      messages: { before: 1, after: 1 },
    })
  })
})

test('a nats-server that cannot start says why', async () => {
  const missing = join(tmpdir(), 'natter-no-such-cert.pem')
  await expect(
    startNats({ tls: { certFile: missing, keyFile: missing } }),
  ).rejects.toThrow(missing)
})

test('the stream definitions cannot be changed at runtime', () => {
  for (const stream of STREAMS) {
    expect(Object.isFrozen(stream)).toBe(true)
    expect(Object.isFrozen(stream.subjects)).toBe(true)
  }
})

test('a second run changes nothing and keeps stored messages', async () => {
  const js = jetstream(nc)
  await js.publish(taskSubject('alpha'), 'queued', { msgID: nuid.next() })
  const before = await Promise.all(STREAMS.map((s) => configOf(s.name)))

  const outcomes = await bootstrapStreams(jsm)

  expect(outcomes.map((o) => o.action)).toEqual([
    'unchanged',
    'unchanged',
    'unchanged',
    'unchanged',
    'unchanged',
  ])
  expect(await Promise.all(STREAMS.map((s) => configOf(s.name)))).toEqual(
    before,
  )
  expect(await messagesIn('NATTER_DEFAULT_TASKS')).toBe(1)
})

test('a drifted stream is updated back without losing messages', async () => {
  const drifted = await configOf('NATTER_DEFAULT_TASKS')
  await jsm.streams.update('NATTER_DEFAULT_TASKS', {
    ...drifted,
    subjects: [...drifted.subjects, 'natter.drift.*'],
    max_age: nanos(60_000),
    max_msgs_per_subject: 2,
    max_msg_size: 1024,
    max_bytes: 512 * MIB,
    duplicate_window: nanos(30_000),
  })
  const resultsDrift = await configOf('NATTER_DEFAULT_RESULTS')
  await jsm.streams.update('NATTER_DEFAULT_RESULTS', {
    ...resultsDrift,
    discard_new_per_subject: false,
  })
  const stateDrift = await configOf('NATTER_DEFAULT_STATE')
  await jsm.streams.update('NATTER_DEFAULT_STATE', {
    ...stateDrift,
    allow_direct: false,
    discard: DiscardPolicy.New,
    discard_new_per_subject: true,
  })
  const held = await messagesIn('NATTER_DEFAULT_TASKS')

  const outcomes = await bootstrapStreams(jsm)

  expect(
    outcomes.map(({ name, action, changed, messages }) => [
      name,
      action,
      [...changed].sort(),
      messages?.before,
      messages?.after,
    ]),
  ).toEqual([
    [
      'NATTER_DEFAULT_TASKS',
      'updated',
      [
        'duplicate_window',
        'max_age',
        'max_bytes',
        'max_msg_size',
        'max_msgs_per_subject',
        'subjects',
      ],
      held,
      held,
    ],
    ['NATTER_DEFAULT_RESULTS', 'updated', ['discard_new_per_subject'], 0, 0],
    [
      'NATTER_DEFAULT_STATE',
      'updated',
      ['allow_direct', 'discard', 'discard_new_per_subject'],
      0,
      0,
    ],
    ['NATTER_DEFAULT_PRESENCE', 'unchanged', [], undefined, undefined],
    ['NATTER_DEFAULT_SENT', 'unchanged', [], undefined, undefined],
  ])
  for (const stream of STREAMS) {
    expect(await managed(stream)).toEqual({ ...stream })
  }
  expect(await messagesIn('NATTER_DEFAULT_TASKS')).toBe(held)
})

test('a tightened limit shows what it trimmed', async () => {
  await onFreshServer(async (server, conn, manager) => {
    await manager.streams.add({ ...STATE_STREAM, max_msgs_per_subject: 5 })
    const subject = statusSubject('alpha', nuid.next())
    for (let i = 0; i < 5; i++) await jetstream(conn).publish(subject, `p${i}`)

    const { code, stdout } = await runBootstrap(server.url)

    expect(code).toBe(0)
    const state = await manager.streams.info('NATTER_DEFAULT_STATE')
    expect(state.config.max_msgs_per_subject).toBe(1)
    expect(state.state.messages).toBe(1)
    const line = stdout
      .split('\n')
      .find((l) => l.includes('NATTER_DEFAULT_STATE'))
    expect(line).toMatch(/\b5\b.*\b1\b/)
  })
})

// The stuck stream comes last in plan order, so only the check that runs
// before any change can keep the first two untouched.
test.each([
  ['retention', { retention: RetentionPolicy.Workqueue }],
  ['storage', { storage: StorageType.Memory }],
])('a %s drift fails loudly and changes nothing', async (key, drift) => {
  await onFreshServer(async (_server, conn, manager) => {
    await manager.streams.add({ ...RESULTS_STREAM, max_msgs_per_subject: 5 })
    await manager.streams.add({ ...STATE_STREAM, ...drift })
    await jetstream(conn).publish(statusSubject('alpha', nuid.next()), 'here')

    await expect(bootstrapStreams(manager)).rejects.toThrow(
      `NATTER_DEFAULT_STATE.${key}`,
    )

    const state = await manager.streams.info('NATTER_DEFAULT_STATE')
    expect(state.config).toMatchObject(drift)
    expect(state.state.messages).toBe(1)
    const results = await manager.streams.info('NATTER_DEFAULT_RESULTS')
    expect(results.config.max_msgs_per_subject).toBe(5)
    expect(await apiCode(manager.streams.info('NATTER_DEFAULT_TASKS'))).toBe(
      JetStreamApiCodes.StreamNotFound,
    )
  })
})

test('the command creates the streams as the admin identity', async () => {
  await onFreshServer(async (server, _conn, manager) => {
    const { code } = await runBootstrap(server.url)

    expect(code).toBe(0)
    for (const stream of STREAMS) {
      expect((await manager.streams.info(stream.name)).config.name).toBe(
        stream.name,
      )
    }
  })
})

test('the command gives up at once when no server is listening', async () => {
  const down = await startNats({ autostart: false })
  try {
    expect((await runBootstrap(down.url)).code).toBe(1)
  } finally {
    await down.cleanup()
  }
})

test('tls:// against a plain server does not connect', async () => {
  await onFreshServer(async (server, _conn, manager) => {
    const dialsBefore = await server.totalConnections()

    const { code } = await runBootstrap(server.url.replace('nats://', 'tls://'))

    expect(code).toBe(1)
    expect(await server.totalConnections()).toBeGreaterThan(dialsBefore)
    expect(await apiCode(manager.streams.info('NATTER_DEFAULT_TASKS'))).toBe(
      JetStreamApiCodes.StreamNotFound,
    )
  })
})

test('the per-subject cap rejects the 17th queued task but not another worker', async () => {
  const js = jetstream(nc)
  const subject = taskSubject('capped')
  for (let i = 0; i < 16; i++) await js.publish(subject, `t${i}`)
  await expect(js.publish(subject, 't16')).rejects.toThrow()
  expect((await js.publish(taskSubject('other'), 'ok')).stream).toBe(
    'NATTER_DEFAULT_TASKS',
  )
})

test('Nats-Msg-Id dedupes a repeated publish', async () => {
  const js = jetstream(nc)
  const subject = taskSubject('dedupe')
  const msgID = nuid.next()
  const first = await js.publish(subject, 'x', { msgID })
  const second = await js.publish(subject, 'x', { msgID })
  expect(first.duplicate).toBe(false)
  expect(second.duplicate).toBe(true)
  expect(second.seq).toBe(first.seq)
})

test('a message over max_msg_size is rejected, one at the limit is stored', async () => {
  const js = jetstream(nc)
  const big = new Uint8Array(MAX_MESSAGE_BYTES + 1)
  await expect(js.publish(taskSubject('big'), big)).rejects.toThrow()
  await expect(
    js.publish(resultSubject('big', nuid.next()), big),
  ).rejects.toThrow()
  await expect(
    js.publish(statusSubject('big', nuid.next()), big),
  ).rejects.toThrow()
  const edge = new Uint8Array(MAX_MESSAGE_BYTES - 1024)
  expect((await js.publish(taskSubject('big'), edge)).seq).toBeGreaterThan(0)
})

test('the first result for a task wins', async () => {
  const { subject, seq } = await recordResult()

  const loser = jetstream(nc).publish(subject, 'second', {
    expect: { lastSubjectSequence: 0 },
  })

  expect(await apiCode(loser)).toBe(JetStreamApiCodes.StreamWrongLastSequence)
  expect(await storedResult(subject)).toEqual({ seq, value: 'first' })
})

test('a plain JetStream publish cannot replace a recorded result', async () => {
  const { subject, seq } = await recordResult()

  const forged = jetstream(nc).publish(subject, 'forged')

  expect(await apiCode(forged)).toBe(STREAM_STORE_FAILED)
  expect(await storedResult(subject)).toEqual({ seq, value: 'first' })
})

test('a core publish cannot replace a recorded result', async () => {
  const { subject, seq } = await recordResult()
  const control = resultSubject('alpha', nuid.next())

  nc.publish(subject, 'forged')
  // Sent after it on one connection: once stored, the forgery was processed.
  nc.publish(control, 'captured')
  await until(async () => (await storedResult(control)).value === 'captured')

  expect(await storedResult(subject)).toEqual({ seq, value: 'first' })
})

test('NATTER_FLEET=a-b creates five NATTER_A-B_* streams like the default fleet and leaves others alone', async () => {
  await onFreshServer(async (server, _conn, manager) => {
    const run = async (fleet: string) => {
      const proc = Bun.spawn(['bun', 'src/bootstrap.ts'], {
        env: {
          PATH: process.env.PATH ?? '',
          NATTER_URL: server.url,
          NATTER_CREDS_DIR: join(tmpdir(), 'natter-no-creds'),
          NATTER_FLEET: fleet,
        },
        stdout: 'ignore',
        stderr: 'ignore',
        timeout: 4000,
      })
      return proc.exited
    }
    const names = async () =>
      (await manager.streams.list().next()).map((s) => s.config.name).sort()
    const owned = (config: Record<string, unknown>) => {
      const { name, subjects, ...settings } = config
      return settings
    }

    expect(await run('default')).toBe(0)
    const defaults = await names()
    expect(defaults).toHaveLength(5)
    expect(await run('a-b')).toBe(0)

    expect(await names()).toEqual(
      [
        ...defaults,
        ...['TASKS', 'RESULTS', 'STATE', 'PRESENCE', 'SENT'].map(
          (s) => `NATTER_A-B_${s}`,
        ),
      ].sort(),
    )
    for (const suffix of ['TASKS', 'RESULTS', 'STATE', 'PRESENCE', 'SENT']) {
      const x = (await manager.streams.info(`NATTER_A-B_${suffix}`)).config
      const d = (await manager.streams.info(`NATTER_DEFAULT_${suffix}`)).config
      expect(x.subjects).toEqual(
        d.subjects.map((subject) => subject.replace('default', 'a-b')),
      )
      const keys = Object.keys(
        owned(streamsFor('a-b').find((w) => w.name === x.name) ?? {}),
      )
      for (const key of keys) {
        expect(x[key as keyof typeof x]).toEqual(d[key as keyof typeof d])
      }
    }
    expect(await run('Bad_Fleet')).not.toBe(0)
    expect(await names()).toHaveLength(10)
  })
})

// ------------------------------------------------------- provisioning, retiring

const FLEET_X = streamsFor('x')
const X_NAMES = FLEET_X.map((stream) => stream.name).sort()

const streamNames = async (manager: JetStreamManager) =>
  ((await manager.streams.names().next()) as string[]).sort()

test('provisioning creates all five once, to the definitions', async () => {
  await onFreshServer(async (_server, _conn, manager) => {
    expect(await provisionStreams(manager, FLEET_X)).toBe('created')

    expect(await streamNames(manager)).toEqual(X_NAMES)
    for (const want of FLEET_X) {
      expect(
        diff(want, (await manager.streams.info(want.name)).config),
      ).toEqual([])
    }
    expect(await provisionStreams(manager, FLEET_X)).toBe('complete')
  })
})

test('provisioning fills no partial set, whichever streams survive', async () => {
  await onFreshServer(async (_server, _conn, manager) => {
    for (const want of FLEET_X) {
      await bootstrapStreams(manager, FLEET_X)
      await manager.streams.delete(want.name)
      expect(await provisionStreams(manager, FLEET_X)).toBe('partial')
      expect(await streamNames(manager)).not.toContain(want.name)

      await deleteStreams(manager, FLEET_X)
      await manager.streams.add({ ...want })
      expect(await provisionStreams(manager, FLEET_X)).toBe('partial')
      expect(await streamNames(manager)).toEqual([want.name])
      await manager.streams.delete(want.name)
    }
  })
})

test('provisioning leaves a drifted set as it is', async () => {
  await onFreshServer(async (_server, _conn, manager) => {
    await bootstrapStreams(manager, FLEET_X)
    const name = 'NATTER_X_TASKS'
    const { config } = await manager.streams.info(name)
    await manager.streams.update(name, { ...config, max_msgs_per_subject: 5 })

    expect(await provisionStreams(manager, FLEET_X)).toBe('complete')

    expect((await manager.streams.info(name)).config.max_msgs_per_subject).toBe(
      5,
    )
  })
})

test('a lookup that fails is not read as a missing stream', async () => {
  const timeout = new errors.TimeoutError()
  const added: unknown[] = []
  const manager = {
    streams: {
      info: () => Promise.reject(timeout),
      add: async (config: unknown) => {
        added.push(config)
      },
    },
  } as unknown as JetStreamManager

  await expect(provisionStreams(manager, FLEET_X)).rejects.toBe(timeout)

  expect(added).toEqual([])
})

test('creators racing on a bare server all finish, with one set', async () => {
  await onFreshServer(async (_server, _conn, manager) => {
    await Promise.all(
      Array.from({ length: 4 }, () => provisionStreams(manager, FLEET_X)),
    )

    expect(await streamNames(manager)).toEqual(X_NAMES)
  })
})

/** A creator whose check ran before another creator: it sees no streams. */
const staleCheck = (manager: JetStreamManager) =>
  ({
    streams: {
      info: (name: string) => manager.streams.info(`GONE_${name}`),
      add: (config: Parameters<typeof manager.streams.add>[0]) =>
        manager.streams.add(config),
    },
  }) as unknown as JetStreamManager

test('a creator whose check went stale changes nothing the winner made', async () => {
  await onFreshServer(async (_server, conn, manager) => {
    await bootstrapStreams(manager, FLEET_X)
    const stored = fleetScope('x').taskSubject('w1')
    await jetstream(conn).publish(stored, 'queued')

    expect(await provisionStreams(staleCheck(manager), FLEET_X)).toBe('created')
    expect(await streamNames(manager)).toEqual(X_NAMES)
    expect((await manager.streams.info('NATTER_X_TASKS')).state.messages).toBe(
      1,
    )

    const name = 'NATTER_X_TASKS'
    const { config } = await manager.streams.info(name)
    await manager.streams.update(name, { ...config, max_msgs_per_subject: 5 })

    await expect(
      provisionStreams(staleCheck(manager), FLEET_X),
    ).rejects.toThrow(name)
    const after = await manager.streams.info(name)
    expect(after.config.max_msgs_per_subject).toBe(5)
    expect(after.state.messages).toBe(1)
  })
})

test('a create past the storage limit names the stream and keeps the ones made', async () => {
  const server = await startNats({ jetstream: true, maxFileStore: '512MiB' })
  const conn = await connect({ servers: server.url })
  try {
    const manager = await jetstreamManager(conn)
    const refused = await provisionStreams(manager, FLEET_X).catch(
      (err: Error) => err.message,
    )

    expect(refused).toContain('NATTER_X_RESULTS')
    expect(await streamNames(manager)).toEqual(['NATTER_X_TASKS'])
  } finally {
    await conn.close()
    await server.cleanup()
  }
})

const fleetDelete = (url: string, env: Record<string, string> = {}) =>
  Bun.spawn(['bun', 'src/bootstrap.ts', 'delete'], {
    env: {
      PATH: process.env.PATH ?? '',
      NATTER_URL: url,
      NATTER_CREDS_DIR: join(tmpdir(), 'natter-no-creds'),
      ...env,
    },
    stdout: 'ignore',
    stderr: 'ignore',
    timeout: 4000,
  }).exited

test('fleet-delete removes the five streams of one fleet, and a partial set too', async () => {
  await onFreshServer(async (server, _conn, manager) => {
    await bootstrapStreams(manager)
    await bootstrapStreams(manager, FLEET_X)
    await manager.streams.delete('NATTER_X_RESULTS')

    expect(await fleetDelete(server.url, { NATTER_FLEET: 'x' })).toBe(0)

    expect(await streamNames(manager)).toEqual(
      STREAMS.map((stream) => stream.name).sort(),
    )
    expect(await fleetDelete(server.url, { NATTER_FLEET: 'x' })).toBe(0)
  })
})

test.each([
  ['unset', {}],
  ['empty', { NATTER_FLEET: '' }],
  ['invalid', { NATTER_FLEET: 'Bad_Fleet' }],
])('fleet-delete with NATTER_FLEET %s deletes nothing', async (_, env) => {
  await onFreshServer(async (server, _conn, manager) => {
    await bootstrapStreams(manager)
    const before = await streamNames(manager)

    expect(await fleetDelete(server.url, env)).not.toBe(0)

    expect(await streamNames(manager)).toEqual(before)
  })
})
