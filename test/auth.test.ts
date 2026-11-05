import { afterAll, beforeAll, expect, setDefaultTimeout, test } from 'bun:test'
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createUser } from '@nats-io/nkeys'
import { type Config, resolveConfig } from '../src/config'
import { startConnection } from '../src/connection'
import { startNats, type TestNats, until } from './support/nats'
import { callTool, type Session, startSession } from './support/session'

// A Bun test timeout kills tracked subprocesses, including a shared nats-server.
setDefaultTimeout(15_000)

const TEMPLATE = join(import.meta.dir, '../deploy/nats-server.example.conf')
const decoder = new TextDecoder()

let dir: string
let nats: TestNats
let url: string
let ca: string
let creds: string
const sessions: Session[] = []

const run = async (cmd: string[]) => {
  const proc = Bun.spawn(cmd, { stdout: 'ignore', stderr: 'pipe' })
  if ((await proc.exited) !== 0)
    throw new Error(await Bun.readableStreamToText(proc.stderr))
}

/** A seed file in a fresh creds dir, with its public key. */
const writeSeed = async (name: string, suffix = '', mode = 0o600) => {
  const kp = createUser()
  const credsDir = join(dir, name)
  await mkdir(credsDir)
  await writeFile(
    join(credsDir, 'natter.nk'),
    decoder.decode(kp.getSeed()) + suffix,
    { mode },
  )
  await chmod(join(credsDir, 'natter.nk'), mode)
  return { credsDir, publicKey: kp.getPublicKey() }
}

const env = (credsDir: string, extra: Record<string, string> = {}) => ({
  NATTER_URL: url,
  NATTER_CREDS_DIR: credsDir,
  NATTER_CA_FILE: ca,
  ...extra,
})

/** Runs bootstrap as its own process; resolves to its exit code and stderr. */
const bootstrap = async (e: Record<string, string>) => {
  const proc = Bun.spawn(['bun', 'src/bootstrap.ts'], {
    env: { PATH: process.env.PATH ?? '', ...e },
    stdout: 'ignore',
    stderr: 'pipe',
  })
  const stderr = await Bun.readableStreamToText(proc.stderr)
  return { code: await proc.exited, stderr }
}

beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), 'natter-auth-'))
  ca = join(dir, 'ca.pem')
  const at = (f: string) => join(dir, f)
  const [caKey, srvKey, srvCsr, srvCert, ext] = [
    at('ca-key'),
    at('srv-key'),
    at('srv.csr'),
    at('srv.pem'),
    at('ext'),
  ] as [string, string, string, string, string]
  await writeFile(ext, 'subjectAltName=IP:127.0.0.1\n')
  await run([
    'openssl',
    'req',
    '-x509',
    '-newkey',
    'rsa:2048',
    '-nodes',
    '-keyout',
    caKey,
    '-out',
    ca,
    '-subj',
    '/CN=natter-test-ca',
    '-days',
    '1',
  ])
  await run([
    'openssl',
    'req',
    '-newkey',
    'rsa:2048',
    '-nodes',
    '-keyout',
    srvKey,
    '-out',
    srvCsr,
    '-subj',
    '/CN=127.0.0.1',
  ])
  await run([
    'openssl',
    'x509',
    '-req',
    '-in',
    srvCsr,
    '-CA',
    ca,
    '-CAkey',
    caKey,
    '-CAcreateserial',
    '-out',
    srvCert,
    '-days',
    '1',
    '-extfile',
    ext,
  ])

  const good = await writeSeed('good', '\n')
  creds = good.credsDir
  const conf = join(dir, 'nats-server.conf')
  await writeFile(
    conf,
    (await readFile(TEMPLATE, 'utf8'))
      .replace('CHANGE_ME_PUBLIC_NKEY', good.publicKey)
      .replace('CHANGE_ME/server-cert.pem', srvCert)
      .replace('CHANGE_ME/server-key.pem', srvKey)
      .replace('CHANGE_ME/natter-nats-data', join(dir, 'js')),
  )
  expect(await readFile(conf, 'utf8')).not.toContain('CHANGE_ME')
  nats = await startNats({ configFile: conf })
  url = nats.url.replace('nats://', 'tls://')
})

afterAll(async () => {
  await Promise.all(sessions.splice(0).map((s) => s.close()))
  await nats?.cleanup()
  await rm(dir, { recursive: true, force: true })
})

test('bootstrap, a worker and an orchestrator complete a task over tls with the shared nkey', async () => {
  // The seed file ends in a newline.
  const boot = await bootstrap(env(creds))
  expect(boot.stderr).toBe('')
  expect(boot.code).toBe(0)

  const name = `w-${crypto.randomUUID()}`
  const open = async (role: string, n: string) => {
    const s = await startSession(
      env(creds, {
        NATTER_ROLE: role,
        NATTER_NAME: n,
        NATTER_TEST_STARTUP_GRACE_MS: '20',
      }),
    )
    sessions.push(s)
    return s
  }
  const orchestrator = await open('orchestrator', 'orchestrator')
  const worker = await open('worker', name)

  // send_task refuses a worker until its first heartbeat is stored.
  let task_id = ''
  await until(async () => {
    const reply = await callTool(orchestrator, 'send_task', {
      to: name,
      title: 't',
      prompt: 'p',
    })
    if (!reply.isError)
      task_id = JSON.parse(reply.content[0]?.text ?? '').task_id
    return !reply.isError
  })
  await until(async () => worker.pushes.length >= 1)
  const finish = await callTool(worker, 'finish_task', {
    task_id,
    outcome: 'succeeded',
    summary: 'done',
  })
  expect(finish.isError).toBeFalsy()
  await until(async () => {
    const reply = await callTool(orchestrator, 'get_result', { task_id })
    return JSON.parse(reply.content[0]?.text ?? '{}').state === 'finished'
  })
})

test('a wrong key is refused', async () => {
  const wrong = await writeSeed('wrong')
  expect((await bootstrap(env(wrong.credsDir))).code).not.toBe(0)
})

test('an untrusted CA is refused', async () => {
  expect((await bootstrap(env(creds, { NATTER_CA_FILE: '' }))).code).not.toBe(0)
})

test('a seed readable by others logs one warning without the seed', async () => {
  const open = await writeSeed('open', '', 0o644)
  const seed = await readFile(join(open.credsDir, 'natter.nk'), 'utf8')
  const r = resolveConfig(
    env(open.credsDir, {
      NATTER_ROLE: 'worker',
      NATTER_NAME: `w-${crypto.randomUUID()}`,
    }),
  ) as { config: Config }
  const lines: string[] = []
  const conn = startConnection(r.config, (line) => lines.push(line))
  await conn.close()
  const warnings = lines.filter((l) =>
    l.includes('readable by group or others'),
  )
  expect(warnings).toHaveLength(1)
  expect(lines.join('\n')).not.toContain(seed)
})
