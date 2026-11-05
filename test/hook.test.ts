import {
  afterAll,
  afterEach,
  beforeAll,
  expect,
  setDefaultTimeout,
  test,
} from 'bun:test'
import { existsSync } from 'node:fs'
import { chmod, mkdir, mkdtemp, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { jetstream, jetstreamManager } from '@nats-io/jetstream'
import { connect, type NatsConnection } from '@nats-io/transport-node'
import { bootstrapStreams } from '../src/bootstrap'
import type { HeldFile } from '../src/held'
import {
  encodeEnvelope,
  mintTaskId,
  parseMessage,
  sentSubject,
  statusSubject,
} from '../src/protocol'
import { startNats, type TestNats, until } from './support/nats'
import { seen } from './support/presence'
import { callTool, type Session, startSession } from './support/session'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

const HOOK_BOUND_MS = 2000
const SWEEP_MS = 100
const STALE_MS = 300
const NO_CREDS = join(tmpdir(), 'natter-no-creds')

let nats: TestNats
let down: TestNats
let nc: NatsConnection
let home: string
const sessions: Session[] = []

beforeAll(async () => {
  nats = await startNats({ jetstream: true })
  down = await startNats({ autostart: false })
  nc = await connect({ servers: nats.url })
  await bootstrapStreams(await jetstreamManager(nc))
  home = await mkdtemp(join(tmpdir(), 'natter-hook-'))
})

afterEach(async () => {
  await Promise.all(sessions.splice(0).map((s) => s.close()))
})

afterAll(async () => {
  await nc?.close()
  await nats?.cleanup()
  await down?.cleanup()
  if (home) {
    await chmod(heldDir(home), 0o700).catch(() => {})
    await rm(home, { recursive: true, force: true })
  }
})

const heldDir = (root: string) =>
  join(root, '.local', 'state', 'natter', 'held')

// What plugin.json passes the server; the hooks never see these.
const serverEnv = (url: string) => ({
  NATTER_URL: url,
  NATTER_CREDS_DIR: NO_CREDS,
})

const start = async (env: Record<string, string>, root = home) => {
  const s = await startSession({
    ...serverEnv(nats.url),
    HOME: root,
    NATTER_TEST_STARTUP_GRACE_MS: '20',
    NATTER_TEST_CONNECT_POLL_MS: '20',
    NATTER_TEST_HEARTBEAT_MS: '50',
    NATTER_TEST_ATTENTION_SWEEP_MS: String(SWEEP_MS),
    NATTER_TEST_ACCEPT_TIMEOUT_MS: '60000',
    ...env,
  })
  sessions.push(s)
  return s
}

const heldFiles = async (root = home) =>
  readdir(heldDir(root)).catch((): string[] => [])

const json = async (s: Session, tool: string, args = {}) =>
  JSON.parse((await callTool(s, tool, args)).content[0]?.text ?? '')

interface TaskRow {
  task_id: string
  worker: string
  state: string
}
const taskRows = async (s: Session) =>
  (await json(s, 'list_tasks')).tasks as TaskRow[]
const stateOf = async (s: Session, task_id: string) =>
  (await taskRows(s)).find((t) => t.task_id === task_id)?.state

const startOrchestrator = () =>
  start({ NATTER_ROLE: 'orchestrator', NATTER_NAME: 'orchestrator' })

/** A worker session holding a task, and the orchestrator that sent it. */
async function holdingWorker(root = home) {
  const name = `w-${crypto.randomUUID()}`
  const sessionId = crypto.randomUUID()
  const worker = await start(
    {
      NATTER_ROLE: 'worker',
      NATTER_NAME: name,
      CLAUDE_CODE_SESSION_ID: sessionId,
    },
    root,
  )
  const orchestrator = await startOrchestrator()
  let task_id = ''
  await until(async () => {
    const reply = await callTool(orchestrator, 'send_task', {
      to: name,
      title: 'the title',
      prompt: 'p',
    })
    if (reply.isError) return false
    task_id = JSON.parse(reply.content[0]?.text ?? '').task_id
    return true
  })
  return { name, sessionId, worker, orchestrator, task_id }
}

const attentionFor = (s: Session, task_id: string) =>
  s.pushes.filter(
    ({ meta }) => meta.kind === 'attention' && meta.task_id === task_id,
  )

/** Runs src/hook.ts as Claude Code would, and watches the status subject. */
async function runHook({
  name,
  mode = 'permission',
  env,
  stdin,
}: {
  name: string
  mode?: 'permission' | 'resumed'
  env: Record<string, string>
  stdin: unknown
}) {
  const writes: string[] = []
  const sub = nc.subscribe(`natter.status.${name}.*`, {
    callback: (_err, msg) => {
      const parsedMsg = parseMessage(msg.subject, msg.data)
      if (parsedMsg.ok && parsedMsg.envelope.type === 'status') {
        writes.push(parsedMsg.envelope.state)
      }
    },
  })
  await nc.flush()
  const started = performance.now()
  const proc = Bun.spawn(['bun', 'src/hook.ts', mode], {
    env: { PATH: process.env.PATH ?? '', HOME: home, ...env },
    stdin: new Blob([JSON.stringify(stdin)]),
    stdout: 'pipe',
    stderr: 'pipe',
  })
  const [code, out, err] = await Promise.all([
    proc.exited,
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ])
  const ms = performance.now() - started
  await Bun.sleep(200)
  sub.unsubscribe()
  return { code, printed: out + err, ms, writes }
}

// No NATTER_URL or creds dir: only the held file can say where NATS is.
const hookEnv = (name: string) => ({ NATTER_ROLE: 'worker', NATTER_NAME: name })

// Not writeHeld: Bun fixes os.homedir() at startup, so it would use the real home.
const plantHeld = async (sessionId: string, file: HeldFile) => {
  await mkdir(heldDir(home), { recursive: true })
  await writeFile(join(heldDir(home), sessionId), JSON.stringify(file))
}

const markerOf = (sessionId: string) =>
  join(heldDir(home), `${sessionId}.waiting`)

test('a held task gets one waiting_permission status, listed and pushed once', async () => {
  const { name, sessionId, orchestrator, task_id } = await holdingWorker()
  await until(async () => (await heldFiles()).includes(sessionId))

  const run = await runHook({
    name,
    env: hookEnv(name),
    stdin: { session_id: sessionId, message: 'secret tool input' },
  })

  expect(run).toMatchObject({
    code: 0,
    printed: '',
    writes: ['waiting_permission'],
  })
  expect(run.ms).toBeLessThan(HOOK_BOUND_MS)
  expect(existsSync(markerOf(sessionId))).toBe(true)

  await until(
    async () => (await stateOf(orchestrator, task_id)) === 'waiting_permission',
  )
  await until(async () => attentionFor(orchestrator, task_id).length >= 1)
  await Bun.sleep(6 * SWEEP_MS)
  expect(attentionFor(orchestrator, task_id)).toHaveLength(1)
  expect(attentionFor(orchestrator, task_id)[0]?.meta.reason).toBe(
    'waiting_permission',
  )
})

test('the resumed hook moves the row back to accepted and clears the marker', async () => {
  const { name, sessionId, orchestrator, task_id } = await holdingWorker()
  await until(async () => (await heldFiles()).includes(sessionId))
  await runHook({ name, env: hookEnv(name), stdin: { session_id: sessionId } })
  await until(
    async () => (await stateOf(orchestrator, task_id)) === 'waiting_permission',
  )

  const run = await runHook({
    name,
    mode: 'resumed',
    env: hookEnv(name),
    stdin: { session_id: sessionId },
  })

  expect(run).toMatchObject({ code: 0, printed: '', writes: ['accepted'] })
  expect(existsSync(markerOf(sessionId))).toBe(false)
  await until(async () => (await stateOf(orchestrator, task_id)) === 'accepted')
})

test('a second prompt on the same task alerts again once the first has cleared', async () => {
  const { name, sessionId, orchestrator, task_id } = await holdingWorker()
  const stdin = { session_id: sessionId }
  const prompt = () => runHook({ name, env: hookEnv(name), stdin })
  await until(async () => (await heldFiles()).includes(sessionId))

  await prompt()
  await until(async () => attentionFor(orchestrator, task_id).length === 1)

  await runHook({ name, mode: 'resumed', env: hookEnv(name), stdin })
  await until(async () => (await stateOf(orchestrator, task_id)) === 'accepted')
  await Bun.sleep(3 * SWEEP_MS)

  await prompt()
  await until(async () => attentionFor(orchestrator, task_id).length === 2)
  await Bun.sleep(6 * SWEEP_MS)
  expect(attentionFor(orchestrator, task_id)).toHaveLength(2)
})

test('no role, no held task, NATS down or bad input exits 0 in time and writes nothing', async () => {
  const { name, sessionId } = await holdingWorker()
  await until(async () => (await heldFiles()).includes(sessionId))
  const downSession = crypto.randomUUID()
  await plantHeld(downSession, {
    held: {
      task_id: mintTaskId(),
      title: 't',
      created_at: new Date().toISOString(),
      depth: 0,
      timeout_s: 60,
      queue_ttl_s: 60,
      instance: mintTaskId(),
      attempt: 1,
    },
    url: down.url,
    seed_path: join(NO_CREDS, 'natter.nk'),
  })

  const runs = await Promise.all([
    runHook({ name, env: {}, stdin: { session_id: sessionId } }),
    runHook({
      name,
      env: hookEnv(name),
      stdin: { session_id: crypto.randomUUID() },
    }),
    runHook({ name, env: hookEnv(name), stdin: { session_id: downSession } }),
    runHook({ name, env: hookEnv(name), stdin: 'not an object' }),
  ])

  for (const run of runs) {
    expect(run).toMatchObject({ code: 0, printed: '', writes: [] })
    expect(run.ms).toBeLessThan(HOOK_BOUND_MS)
  }
})

test('the held file is removed on finish and on stop', async () => {
  const { sessionId, worker, task_id } = await holdingWorker()
  await until(async () => (await heldFiles()).includes(sessionId))
  await callTool(worker, 'update_task', { task_id })
  await callTool(worker, 'finish_task', {
    task_id,
    outcome: 'succeeded',
    summary: 'done',
  })
  await until(async () => !(await heldFiles()).includes(sessionId))

  const second = await holdingWorker()
  await until(async () => (await heldFiles()).includes(second.sessionId))
  await second.worker.close()
  await until(async () => !(await heldFiles()).includes(second.sessionId))
})

test('a held file that cannot be written does not stop delivery', async () => {
  const root = await mkdtemp(join(tmpdir(), 'natter-ro-'))
  await mkdir(heldDir(root), { recursive: true })
  await chmod(heldDir(root), 0o500)
  try {
    const { worker } = await holdingWorker(root)
    await until(async () =>
      worker.pushes.some(({ meta }) => meta.kind === 'task'),
    )
    expect(await heldFiles(root)).toEqual([])
  } finally {
    await chmod(heldDir(root), 0o700)
    await rm(root, { recursive: true, force: true })
  }
})

test('a waiting_permission row with a stale busy worker raises stale, not waiting_permission', async () => {
  const name = `w-${crypto.randomUUID()}`
  const task_id = mintTaskId()
  const fields = {
    v: 1,
    task_id,
    title: 'the title',
    created_at: new Date().toISOString(),
    depth: 0,
    timeout_s: 60,
    queue_ttl_s: 60,
  }
  const sent = encodeEnvelope({
    ...fields,
    type: 'sent',
    to: name,
    root_task_id: task_id,
  })
  const row = encodeEnvelope({
    ...fields,
    type: 'status',
    worker: name,
    state: 'waiting_permission',
    attempt: 1,
  })
  if (!sent.ok || !row.ok) throw new Error('bad fixture')
  const js = jetstream(nc)
  await js.publish(sentSubject(task_id), sent.bytes)
  await js.publish(statusSubject(name, task_id), row.bytes)
  // The last beat of a worker that was killed while busy with this task.
  await seen(js, name, { state: 'busy', task_id })
  await Bun.sleep(2 * STALE_MS)
  const orchestrator = await start({
    NATTER_ROLE: 'orchestrator',
    NATTER_NAME: 'orchestrator',
    NATTER_TEST_STALE_MS: String(STALE_MS),
  })

  await until(async () => attentionFor(orchestrator, task_id).length >= 1)
  await Bun.sleep(6 * SWEEP_MS)

  expect(attentionFor(orchestrator, task_id).map((p) => p.meta.reason)).toEqual(
    ['stale'],
  )
})

// The commands are run through sh as Claude Code does, with a bun that only
// records that it ran.
test.each([
  ['Notification', { NATTER_ROLE: undefined }, false],
  ['Notification', { NATTER_ROLE: 'worker' }, true],
  ['PostToolUse', { NATTER_ROLE: undefined, marker: true }, false],
  ['PostToolUse', { NATTER_ROLE: 'worker', marker: false }, false],
  ['PostToolUse', { NATTER_ROLE: 'worker', marker: true }, true],
] as const)(
  'the %s command with %o starts bun: %p',
  async (event, setup, runs) => {
    const root = await mkdtemp(join(tmpdir(), 'natter-sh-'))
    try {
      const bin = join(root, 'bin')
      const ran = join(root, 'ran')
      await mkdir(bin)
      await writeFile(join(bin, 'bun'), `#!/bin/sh\ntouch ${ran}\n`, {
        mode: 0o755,
      })
      const sessionId = crypto.randomUUID()
      if ('marker' in setup && setup.marker) {
        await mkdir(heldDir(root), { recursive: true })
        await writeFile(join(heldDir(root), `${sessionId}.waiting`), '')
      }
      const hooks = await Bun.file('hooks/hooks.json').json()
      const command = hooks.hooks[event][0].hooks[0].command as string
      const env: Record<string, string> = {
        PATH: `${bin}:/usr/bin:/bin`,
        HOME: root,
        CLAUDE_PLUGIN_ROOT: root,
        CLAUDE_CODE_SESSION_ID: sessionId,
      }
      if (setup.NATTER_ROLE) env.NATTER_ROLE = setup.NATTER_ROLE
      const proc = Bun.spawn(['sh', '-c', command], { env })
      expect(await proc.exited).toBe(0)
      expect(existsSync(ran)).toBe(runs)
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  },
)

test('the hook trusts a private CA given only through the held file', async () => {
  const root = await mkdtemp(join(tmpdir(), 'natter-tls-'))
  const certFile = join(root, 'cert.pem')
  const keyFile = join(root, 'key.pem')
  // biome-ignore format: flag and value pairs read better side by side
  const openssl = Bun.spawnSync(['openssl', 'req', '-x509', '-nodes',
    '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1',
    '-days', '1', '-subj', '/CN=natter-test',
    '-addext', 'subjectAltName=IP:127.0.0.1',
    '-keyout', keyFile, '-out', certFile,
  ])
  expect(openssl.exitCode).toBe(0)
  const secure = await startNats({
    jetstream: true,
    tls: { certFile, keyFile },
  })
  const name = `w-${crypto.randomUUID()}`
  const sessionId = crypto.randomUUID()
  const task_id = mintTaskId()
  try {
    await plantHeld(sessionId, {
      held: {
        task_id,
        title: 't',
        created_at: new Date().toISOString(),
        depth: 0,
        timeout_s: 60,
        queue_ttl_s: 60,
        instance: mintTaskId(),
        attempt: 1,
      },
      url: secure.url.replace('nats://', 'tls://'),
      seed_path: join(NO_CREDS, 'natter.nk'),
      ca_file: certFile,
    })
    const watcher = await connect({
      servers: secure.url.replace('nats://', 'tls://'),
      tls: { caFile: certFile },
    })
    const writes: string[] = []
    watcher.subscribe(statusSubject(name, task_id), {
      callback: () => {
        writes.push('status')
      },
    })
    await watcher.flush()

    const proc = Bun.spawn(['bun', 'src/hook.ts', 'permission'], {
      env: { PATH: process.env.PATH ?? '', HOME: home, ...hookEnv(name) },
      stdin: new Blob([JSON.stringify({ session_id: sessionId })]),
    })
    expect(await proc.exited).toBe(0)
    await until(async () => writes.length > 0)
    await watcher.close()
  } finally {
    await secure.cleanup()
    await rm(root, { recursive: true, force: true })
  }
})
