import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { type Config, resolveConfig, resolveTransport } from '../src/config'
import { startConnection } from '../src/connection'
import { startSession } from './support/session'

let root: string
const project = async (dir: string, natterJson?: unknown) => {
  const path = join(root, dir)
  await mkdir(join(path, '.claude'), { recursive: true })
  if (natterJson !== undefined) {
    await writeFile(
      join(path, '.claude', 'natter.json'),
      JSON.stringify(natterJson),
    )
  }
  return path
}
const nameOf = (r: ReturnType<typeof resolveConfig>) =>
  'config' in r ? r.config.name : undefined

beforeAll(async () => {
  root = await mkdtemp(join(tmpdir(), 'natter-config-'))
})
afterAll(() => rm(root, { recursive: true, force: true }))

describe('role', () => {
  test.each([undefined, '', 'boss', 'Worker'])('%p is inert', (role) => {
    const r = resolveConfig({ NATTER_ROLE: role, NATTER_NAME: 'a' })
    expect(r).toHaveProperty('inert')
  })

  test.each(['worker', 'orchestrator'] as const)('%s is active', (role) => {
    const r = resolveConfig({ NATTER_ROLE: role, NATTER_NAME: 'a' })
    expect(r).toHaveProperty('config.role', role)
  })
})

describe('name precedence', () => {
  test('env beats file beats project basename', async () => {
    const dir = await project('Repo', { name: 'from-file' })
    const base = { NATTER_ROLE: 'worker', CLAUDE_PROJECT_DIR: dir }
    expect(nameOf(resolveConfig({ ...base, NATTER_NAME: 'from-env' }))).toBe(
      'from-env',
    )
    expect(nameOf(resolveConfig(base))).toBe('from-file')

    const bare = await project('My Repo')
    expect(nameOf(resolveConfig({ ...base, CLAUDE_PROJECT_DIR: bare }))).toBe(
      'my-repo',
    )
  })

  test('never falls back to process.cwd()', () => {
    const r = resolveConfig({ NATTER_ROLE: 'worker' })
    expect(r).toHaveProperty('inert')
  })

  test.each(['', '-a', 'A', 'a b', 'a/b', '../x', 'x'.repeat(65)])(
    'invalid env name %p is inert',
    (name) => {
      const r = resolveConfig({ NATTER_ROLE: 'worker', NATTER_NAME: name })
      // '' means unset, which is inert here too for lack of a project dir.
      expect(r).toHaveProperty('inert')
    },
  )

  test('invalid file name is inert, not replaced by the basename', async () => {
    const dir = await project('okname', { name: '../evil' })
    const r = resolveConfig({ NATTER_ROLE: 'worker', CLAUDE_PROJECT_DIR: dir })
    expect(r).toHaveProperty('inert')
  })

  test('malformed or oversized natter.json is inert', async () => {
    const bad = await project('bad')
    await writeFile(join(bad, '.claude', 'natter.json'), '{nope')
    const big = await project('big', { description: 'x'.repeat(513) })
    for (const dir of [bad, big]) {
      const r = resolveConfig({
        NATTER_ROLE: 'worker',
        CLAUDE_PROJECT_DIR: dir,
      })
      expect(r).toHaveProperty('inert')
    }
  })

  test('description is read from the file', async () => {
    const dir = await project('described', { description: 'does things' })
    const r = resolveConfig({ NATTER_ROLE: 'worker', CLAUDE_PROJECT_DIR: dir })
    expect(r).toHaveProperty('config.description', 'does things')
  })
})

describe('connection settings', () => {
  const base = { NATTER_ROLE: 'worker', NATTER_NAME: 'w' }

  test('shell env overrides userConfig, which overrides defaults', () => {
    const get = (env: Record<string, string>) =>
      (resolveConfig({ ...base, ...env }) as { config: Config }).config
    expect(get({}).url).toStartWith('nats://127.0.0.1')
    expect(get({ NATTER_UC_URL: 'nats://uc:1' }).url).toBe('nats://uc:1')
    expect(
      get({ NATTER_UC_URL: 'nats://uc:1', NATTER_URL: 'nats://env:1' }).url,
    ).toBe('nats://env:1')
    expect(get({ NATTER_UC_URL: '' }).url).toStartWith('nats://127.0.0.1')
  })

  test('seed path is <creds_dir>/<name>.nk', () => {
    const r = resolveConfig({ ...base, NATTER_CREDS_DIR: '/creds' })
    expect(r).toHaveProperty('config.seedPath', '/creds/w.nk')
  })
})

describe('transport policy', () => {
  let creds: string
  beforeAll(async () => {
    creds = await project('creds')
    await writeFile(join(creds, 'w.nk'), 'SUAFAKE')
  })
  const transport = (url: string, extra: Record<string, string> = {}) => {
    const r = resolveConfig({
      NATTER_ROLE: 'worker',
      NATTER_NAME: 'w',
      NATTER_CREDS_DIR: creds,
      NATTER_URL: url,
      ...extra,
    }) as { config: Config }
    return resolveTransport(r.config)
  }
  const refusal = (url: string, extra?: Record<string, string>) => {
    const t = transport(url, extra)
    return 'refused' in t ? t.refused : undefined
  }

  test.each([
    'nats://127.0.0.1:4222',
    'nats://127.0.0.1',
    'nats://localhost:4222',
    'nats://[::1]:4222',
    'NATS://LocalHost:4222',
  ])('loopback %s is allowed in plaintext', (url) => {
    expect(refusal(url)).toBeUndefined()
  })

  test('non-loopback plaintext is refused, even with a seed', () => {
    expect(refusal('nats://10.0.0.5:4222')).toBeString()
  })

  test('non-loopback tls with a seed is allowed', () => {
    expect(refusal('tls://10.0.0.5:4222')).toBeUndefined()
  })

  test('non-loopback tls without a seed is refused', () => {
    expect(
      refusal('tls://10.0.0.5:4222', { NATTER_NAME: 'noseed' }),
    ).toBeString()
  })

  test('NATTER_ALLOW_PLAINTEXT=1 permits plaintext, but still needs a seed', () => {
    const allow = { NATTER_ALLOW_PLAINTEXT: '1' }
    expect(refusal('nats://10.0.0.5:4222', allow)).toBeUndefined()
    expect(
      refusal('nats://10.0.0.5:4222', { ...allow, NATTER_NAME: 'noseed' }),
    ).toBeString()
  })

  test('the client gets the checked url, with the scheme normalized', () => {
    expect(transport('TLS://10.0.0.5:4222')).toMatchObject({
      url: 'tls://10.0.0.5:4222',
      address: '10.0.0.5:4222',
      tls: true,
    })
    expect(transport('Nats://localhost')).toMatchObject({
      url: 'nats://localhost',
      tls: false,
    })
  })

  test('bracketed IPv6 stays valid and non-loopback IPv6 is held to policy', () => {
    expect(refusal('tls://[2001:db8::1]:4222')).toBeUndefined()
    expect(refusal('nats://[2001:db8::1]:4222')).toBeString()
    expect(refusal('nats://[::ffff:127.0.0.1]:4222')).toBeString()
  })

  // The client dials what follows the last `://`, not what URL parsing sees.
  // Allowing plaintext must not help: these never reach the host checks.
  test.each([
    'nats://127.0.0.1:4222/x://192.168.0.80:34222',
    'nats://127.0.0.1?x=://evil.com:4222',
    'nats://127.0.0.1#://evil.com',
    'nats://evil.com\\@127.0.0.1',
    'nats://user:hunter2@127.0.0.1:4222',
    'nats://hunter2@127.0.0.1:4222',
    'nats://127.0.0.1:4222,10.0.0.5:4222',
    'nats://127.0.0.1 :4222',
    ' nats://127.0.0.1:4222',
    'nats://127.0.0.1:4222\n',
    'nats://127.0.0.1\n://evil.com',
    'nats://nats://127.0.0.1:4222',
    'nats://127.0.0.1:4222/',
    'nats://127.0.0.1%2f@evil.com',
    'nats://127.0.0.1:99999',
    'nats://',
    'nats://:4222',
    'nats://[::1',
    '127.0.0.1:4222',
    'http://127.0.0.1:4222',
    'ws://127.0.0.1:4222',
  ])('%j is refused', (url) => {
    const allow = { NATTER_ALLOW_PLAINTEXT: '1' }
    expect(refusal(url)).toStartWith('invalid NATS url')
    expect(refusal(url, allow)).toStartWith('invalid NATS url')
  })

  test('a url with credentials is refused without logging them', () => {
    const r = resolveConfig({
      NATTER_ROLE: 'worker',
      NATTER_NAME: 'w',
      NATTER_CREDS_DIR: creds,
      NATTER_URL: 'nats://user:hunter2@127.0.0.1:4222',
    }) as { config: Config }
    const lines: string[] = []
    const conn = startConnection(r.config, (line) => lines.push(line))
    expect(lines).toHaveLength(1)
    expect(lines.join()).not.toContain('hunter2')
    expect(conn.unavailable).not.toContain('hunter2')
  })

  describe('unreadable seed', () => {
    const unusable = (name: string) => {
      const r = resolveConfig({
        NATTER_ROLE: 'worker',
        NATTER_NAME: name,
        NATTER_CREDS_DIR: creds,
        NATTER_URL: 'nats://127.0.0.1:4222',
      }) as { config: Config }
      const lines: string[] = []
      const conn = startConnection(r.config, (line) => lines.push(line))
      expect(conn.nc).toBeUndefined()
      expect(conn.unavailable).toContain(r.config.seedPath)
      expect(lines).toEqual([conn.unavailable as string])
    }

    test('a directory at <name>.nk is refused, even on loopback', async () => {
      await mkdir(join(creds, 'dir.nk'))
      unusable('dir')
    })

    test.skipIf(process.getuid?.() === 0)(
      'a file without read permission is refused, even on loopback',
      async () => {
        await writeFile(join(creds, 'locked.nk'), 'SUAFAKE')
        await chmod(join(creds, 'locked.nk'), 0o000)
        unusable('locked')
      },
    )
  })
})

describe('server process', () => {
  test('invalid name leaves the server up, inert, with zero tools', async () => {
    const s = await startSession({ NATTER_ROLE: 'worker', NATTER_NAME: '../x' })
    try {
      expect((await s.client.listTools()).tools).toEqual([])
      expect(s.errors).toEqual([])
    } finally {
      await s.close()
    }
  })

  test('missing CLAUDE_PROJECT_DIR with no NATTER_NAME is inert', async () => {
    const s = await startSession({ NATTER_ROLE: 'worker' })
    try {
      expect((await s.client.listTools()).tools).toEqual([])
    } finally {
      await s.close()
    }
  })
})
