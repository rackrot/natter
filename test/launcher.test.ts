import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { mkdir, readdir, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { FLEET_PATTERN, resolveConfig } from '../src/config'
import {
  dryCommands,
  isolatedPath,
  type Launch,
  makeSandbox,
  NATTER_BIN,
  parseEnvCommand,
  readHerdrCalls,
  readStubRun,
  runNatter,
  type Sandbox,
  splitLine,
  stubClaude,
  stubClaudeOrchestrate,
  stubHerdr,
} from './support/stubs'

const ROOT = join(import.meta.dir, '..')
const CHANNEL = [
  '--dangerously-load-development-channels',
  'plugin:nats-channel@natter',
]
const WORKER_SETTINGS = join(ROOT, 'config', 'worker.settings.json')
const ORCHESTRATOR_SETTINGS = join(ROOT, 'config', 'orchestrator.settings.json')

let sb: Sandbox
let repo: string
let spaced: string

beforeAll(async () => {
  sb = await makeSandbox()
  repo = join(sb.root, 'repo')
  spaced = join(sb.root, 'my repo')
  await Promise.all([mkdir(repo), mkdir(spaced)])
  await Promise.all([stubClaudeOrchestrate(sb), stubClaude(sb), stubHerdr(sb)])
})

afterAll(() => rm(sb.root, { recursive: true, force: true }))

/** The one `env ... claude-orchestrate ...` line a `--here` dry run prints. */
async function dryHere(
  args: string[],
  env: Record<string, string> = {},
): Promise<Launch> {
  const ran = await runNatter(sb, ['-n', ...args], { env })
  expect(ran.stderr).toBe('')
  expect(ran.code).toBe(0)
  const lines = dryCommands(ran.stdout)
  expect(lines).toHaveLength(1)
  return parseEnvCommand(lines[0] as string[])
}

const worker = (...args: string[]) => ['worker', ...args]
const orchestrator = (...args: string[]) => ['orchestrator', ...args]
/** Where an orchestrator of the fleet runs, under the sandbox's XDG_STATE_HOME. */
const stateDir = (fleet: string) =>
  join(sb.state, 'natter', 'orchestrator', fleet)

describe('dry run, in this terminal', () => {
  const expectedArgv = (dir: string, ...claudeArgs: string[]) => [
    'claude-orchestrate',
    '--no-herdr',
    dir,
    '--',
    ...CHANNEL,
    '--settings',
    WORKER_SETTINGS,
    ...claudeArgs,
  ]

  test('--here: env, argv and trailing claude args in order', async () => {
    const l = await dryHere(
      worker(
        '--fleet',
        'x',
        '--name',
        'api',
        '--here',
        spaced,
        '--',
        '--resume',
        '--model',
        'opus',
      ),
    )
    expect(l.set).toEqual({
      NATTER_ROLE: 'worker',
      NATTER_FLEET: 'x',
      NATTER_NAME: 'api',
    })
    expect(l.unset).toEqual(['CLAUDE_ORCHESTRATE_AGENT'])
    expect(l.argv).toEqual(expectedArgv(spaced, '--resume', '--model', 'opus'))
  })

  test('in a herdr pane the session stays in the pane, with no herdr call', async () => {
    const l = await dryHere(worker('--fleet', 'x', '--name', 'api', repo), {
      HERDR_ENV: '1',
    })
    expect(l.argv).toEqual(expectedArgv(repo))
  })

  test('without --name NATTER_NAME is cleared, not inherited', async () => {
    const l = await dryHere(worker('--fleet', 'x', '--here', repo), {
      NATTER_NAME: 'leaked',
    })
    expect(l.set).toEqual({ NATTER_ROLE: 'worker', NATTER_FLEET: 'x' })
    expect(l.unset).toContain('NATTER_NAME')
  })

  test('the directory defaults to the working directory', async () => {
    const ran = await runNatter(
      sb,
      ['-n', ...worker('--fleet', 'x', '--here')],
      {
        cwd: spaced,
      },
    )
    expect(
      parseEnvCommand(dryCommands(ran.stdout)[0] as string[]).argv[2],
    ).toBe(spaced)
  })

  test('the fleet may come from NATTER_FLEET, and --fleet wins', async () => {
    const fromEnv = await dryHere(worker('--here', repo), {
      NATTER_FLEET: 'envy',
    })
    expect(fromEnv.set.NATTER_FLEET).toBe('envy')
    const flag = await dryHere(worker('--fleet', 'flag', '--here', repo), {
      NATTER_FLEET: 'envy',
    })
    expect(flag.set.NATTER_FLEET).toBe('flag')
  })

  test('claude args after -- are not read as a directory', async () => {
    const l = await dryHere(worker('--fleet', 'x', '--here', '--', '--resume'))
    expect(l.argv[2]).toBe(sb.root)
    expect(l.argv.at(-1)).toBe('--resume')
  })

  test('an orchestrator runs the read-only agent in its per-fleet state directory', async () => {
    const l = await dryHere(
      orchestrator(
        '--fleet',
        'x',
        '--here',
        '--',
        '--resume',
        '--model',
        'opus',
      ),
    )
    expect(l.set).toEqual({
      NATTER_ROLE: 'orchestrator',
      NATTER_FLEET: 'x',
      NATTER_NAME: 'orchestrator',
      NATTER_PROVISION: '1',
    })
    expect(l.unset).toEqual(['CLAUDE_ORCHESTRATE_AGENT'])
    expect(l.argv).toEqual([
      'claude-orchestrate',
      '--no-herdr',
      '-a',
      'nats-channel:orchestrator',
      stateDir('x'),
      '--',
      ...CHANNEL,
      '--settings',
      ORCHESTRATOR_SETTINGS,
      '--resume',
      '--model',
      'opus',
    ])
  })

  test('an orchestrator in a herdr pane stays in the pane', async () => {
    const l = await dryHere(orchestrator('--fleet', 'x'), { HERDR_ENV: '1' })
    expect(l.argv[2]).toBe('-a')
    expect(l.argv[4]).toBe(stateDir('x'))
  })

  test('only an orchestrator provisions, and its directory ignores the working directory', async () => {
    const w = await dryHere(worker('--fleet', 'x', '--here', repo))
    expect(w.set).not.toHaveProperty('NATTER_PROVISION')
    const ran = await runNatter(
      sb,
      ['-n', ...orchestrator('--fleet', 'x', '--here')],
      {
        cwd: spaced,
      },
    )
    expect(
      parseEnvCommand(dryCommands(ran.stdout)[0] as string[]).argv[4],
    ).toBe(stateDir('x'))
  })

  test('the state directory follows XDG_STATE_HOME, then ~/.local/state', async () => {
    const xdg = await dryHere(orchestrator('--fleet', 'x', '--here'), {
      XDG_STATE_HOME: join(sb.root, 'elsewhere'),
    })
    expect(xdg.argv[4]).toBe(
      join(sb.root, 'elsewhere', 'natter', 'orchestrator', 'x'),
    )
    const ran = await runNatter(
      sb,
      ['-n', ...orchestrator('--fleet', 'x', '--here')],
      {
        env: { XDG_STATE_HOME: '' },
      },
    )
    expect(
      parseEnvCommand(dryCommands(ran.stdout)[0] as string[]).argv[4],
    ).toBe(join(sb.home, '.local', 'state', 'natter', 'orchestrator', 'x'))
  })

  test('two fleets get two directories, created on a real run', async () => {
    for (const fleet of ['one', 'two']) {
      expect(
        (await runNatter(sb, orchestrator('--fleet', fleet, '--here'))).code,
      ).toBe(0)
    }
    expect(await readdir(join(sb.state, 'natter', 'orchestrator'))).toEqual(
      expect.arrayContaining(['one', 'two']),
    )
  })

  test('an orchestrator takes no --name and no directory, and nothing is launched', async () => {
    await rm(sb.coLog, { force: true })
    for (const args of [
      orchestrator('--fleet', 'x', '--here', '--name', 'boss'),
      orchestrator('--fleet', 'x', '--here', repo),
    ]) {
      const ran = await runNatter(sb, ['-n', ...args])
      expect(ran.code).not.toBe(0)
      expect(ran.stdout).toBe('')
      expect((await runNatter(sb, args)).code).not.toBe(0)
    }
    expect(await Bun.file(sb.coLog).exists()).toBe(false)
  })
})

describe('dry run, new workspace', () => {
  const route = async (args: string[], env: Record<string, string> = {}) => {
    const ran = await runNatter(
      sb,
      ['-n', ...worker('--fleet', 'x', ...args)],
      {
        env,
      },
    )
    expect(ran.code).toBe(0)
    return dryCommands(ran.stdout)
  }
  const verbs = (lines: string[][]) =>
    lines.map((l) =>
      l[0] === 'herdr' ? l.slice(1, 3).join(' ') || 'attach' : l[0],
    )

  const focusOf = (lines: string[][]) =>
    (lines.find((l) => l[2] === 'create') as string[]).filter((a) =>
      a.endsWith('focus'),
    )
  const down = { HERDR_SERVER_DOWN: '1' }

  test('with the server running: a background workspace, no attach', async () => {
    const lines = await route([repo])
    expect(verbs(lines)).toEqual([
      'workspace create',
      'pane wait-output',
      'pane run',
      'pane wait-output',
    ])
    expect(focusOf(lines)).toEqual(['--no-focus'])
  })

  test('a server this run starts is followed: focus and attach', async () => {
    const lines = await route([repo], down)
    expect(verbs(lines)).toEqual([
      'server',
      'workspace create',
      'pane wait-output',
      'pane run',
      'pane wait-output',
      'attach',
    ])
    expect(focusOf(lines)).toEqual(['--focus'])
  })

  test('--attach focuses and attaches', async () => {
    const lines = await route(['--attach', repo])
    expect(verbs(lines).at(-1)).toBe('attach')
    expect(focusOf(lines)).toEqual(['--focus'])
  })

  test('-d never attaches, even for a server it starts', async () => {
    const lines = await route(['-d', repo], down)
    expect(verbs(lines).at(-1)).toBe('pane wait-output')
    expect(verbs(lines)).not.toContain('attach')
    expect(focusOf(lines)).toEqual(['--no-focus'])
  })

  test('-d with --attach is refused', async () => {
    await rm(sb.herdrLog, { force: true })
    const ran = await runNatter(
      sb,
      worker('--fleet', 'x', '-d', '--attach', repo),
      {},
    )
    expect(ran.code).not.toBe(0)
    expect(await readHerdrCalls(sb)).toEqual([])
  })

  test('-w inside herdr opens a background workspace and does not attach', async () => {
    const lines = await route(['-w', repo], { HERDR_ENV: '1' })
    expect(focusOf(lines)).toEqual(['--no-focus'])
    expect(verbs(lines)).toEqual([
      'workspace create',
      'pane wait-output',
      'pane run',
      'pane wait-output',
    ])
  })

  test('the pane re-enters natter with --here and no NATTER_ROLE', async () => {
    const lines = await route(['--name', 'api', spaced, '--', '--resume'])
    const run = lines.find((l) => l[2] === 'run') as string[]
    const typed = splitLine(run[4] as string)
    expect(typed).toEqual([
      NATTER_BIN,
      'worker',
      '--fleet',
      'x',
      '--name',
      'api',
      '--here',
      spaced,
      '--',
      '--resume',
    ])
    expect(lines.flat().join(' ')).not.toContain('NATTER_ROLE')
  })

  test('an orchestrator workspace starts in the state directory and re-enters with its claude args', async () => {
    const ran = await runNatter(
      sb,
      ['-n', ...orchestrator('--fleet', 'x', '-w', '--', '--resume', 'a b')],
      { env: { HERDR_ENV: '1' } },
    )
    expect(ran.code).toBe(0)
    const lines = dryCommands(ran.stdout)
    const create = lines.find((l) => l[2] === 'create') as string[]
    expect(
      create.slice(create.indexOf('--cwd'), create.indexOf('--cwd') + 4),
    ).toEqual(['--cwd', stateDir('x'), '--label', 'x:orchestrator'])
    const run = lines.find((l) => l[2] === 'run') as string[]
    expect(splitLine(run[4] as string)).toEqual([
      NATTER_BIN,
      'orchestrator',
      '--fleet',
      'x',
      '--here',
      '--',
      '--resume',
      'a b',
    ])
  })
})

describe('stub run equals the dry run', () => {
  // Built per test: the directories exist only after beforeAll.
  const cases = (): Record<
    string,
    { args: string[]; env?: Record<string, string> }
  > => ({
    'named, with claude args': {
      args: worker(
        '--fleet',
        'x',
        '--name',
        'api',
        '--here',
        spaced,
        '--',
        '--resume',
      ),
    },
    'unnamed, stale env': {
      args: worker('--fleet', 'x', '--here', repo),
      env: { NATTER_NAME: 'leaked', CLAUDE_ORCHESTRATE_AGENT: 'other' },
    },
    'in a herdr pane': {
      args: worker('--fleet', 'x', '--name', 'api', repo),
      env: { HERDR_ENV: '1', CLAUDE_ORCHESTRATE_AGENT: 'other' },
    },
    orchestrator: {
      args: orchestrator('--fleet', 'x', '--here', '--', '--resume'),
      env: { NATTER_NAME: 'leaked', CLAUDE_ORCHESTRATE_AGENT: 'other' },
    },
  })
  for (const label of Object.keys(cases())) {
    test(label, async () => {
      const c = cases()[label] as ReturnType<typeof cases>[string]
      const dry = await dryHere(c.args, c.env)
      const real = await runNatter(sb, c.args, {
        env: { NATTER_URL: 'nats://stub:4222', ...c.env },
      })
      expect(real.code).toBe(0)
      const seen = await readStubRun(sb)
      const [command, ...argv] = dry.argv
      expect(command).toBe('claude-orchestrate')
      expect(seen.argv).toEqual(argv)
      // Set where the dry run says, cleared where it says, and nothing else added.
      expect(seen.env).toEqual({ ...dry.set, NATTER_URL: 'nats://stub:4222' })
      for (const name of dry.unset) expect(seen.env).not.toHaveProperty(name)
    })
  }
})

describe('stub herdr', () => {
  const bareHerdr = async (args: string[], env: Record<string, string>) => {
    await rm(sb.herdrLog, { force: true })
    await rm(`${sb.herdrLog}.up`, { force: true })
    const ran = await runNatter(sb, args, { env })
    expect(ran.code).toBe(0)
    return readHerdrCalls(sb)
  }

  test('workspace create forwards set connection variables and no role', async () => {
    const calls = await bareHerdr(
      worker('--fleet', 'x', '--name', 'api', '-d', spaced),
      {
        NATTER_URL: 'nats://h:4222',
        NATTER_ROLE: 'worker',
        NATTER_NAME: 'api',
      },
    )
    const create = calls.find((c) => c[0] === 'workspace') as string[]
    expect(create).toEqual([
      'workspace',
      'create',
      '--cwd',
      spaced,
      '--label',
      'x:api',
      '--no-focus',
      '--env',
      'NATTER_URL=nats://h:4222',
    ])
    expect(calls.flat().join(' ')).not.toContain('NATTER_ROLE')
  })

  test('pane run is sent once, after the first output, to the created pane', async () => {
    const calls = await bareHerdr(
      worker('--fleet', 'x', '--name', 'api', spaced, '--', '--resume'),
      {},
    )
    expect(calls.map((c) => c.slice(0, 2).join(' '))).toEqual([
      'status server',
      'workspace create',
      'pane wait-output',
      'pane run',
      'pane wait-output',
    ])
    const run = calls[3] as string[]
    expect(run[2]).toBe('w9:p1')
    expect(splitLine(run[3] as string)).toEqual([
      NATTER_BIN,
      'worker',
      '--fleet',
      'x',
      '--name',
      'api',
      '--here',
      spaced,
      '--',
      '--resume',
    ])
    expect(calls[2]).toContain('w9:p1')
  })

  test('an orchestrator workspace is created in a directory that exists', async () => {
    const calls = await bareHerdr(orchestrator('--fleet', 'ws', '-d'), {})
    const create = calls.find((c) => c[0] === 'workspace') as string[]
    const cwd = create[create.indexOf('--cwd') + 1] as string
    expect(cwd).toBe(stateDir('ws'))
    expect((await readdir(cwd)).length).toBe(0)
    const sent = calls.flat().join(' ')
    expect(sent).not.toContain('NATTER_PROVISION')
    expect(sent).not.toContain('NATTER_ROLE')
  })

  const createFlags = (calls: string[][]) =>
    (calls.find((c) => c[0] === 'workspace') as string[]).filter((a) =>
      a.endsWith('focus'),
    )
  const attached = (calls: string[][]) => calls.some((c) => c.length === 0)

  test('a running server: no focus, no attach, one line naming the workspace', async () => {
    await rm(sb.herdrLog, { force: true })
    const ran = await runNatter(
      sb,
      worker('--fleet', 'x', '--name', 'api', repo),
    )
    expect(ran.code).toBe(0)
    expect(ran.stderr).toContain('x:api')
    const calls = await readHerdrCalls(sb)
    expect(createFlags(calls)).toEqual(['--no-focus'])
    expect(attached(calls)).toBe(false)
  })

  test('a server started by this run: focus and attach', async () => {
    const calls = await bareHerdr(worker('--fleet', 'x', repo), {
      HERDR_SERVER_DOWN: '1',
    })
    expect(createFlags(calls)).toEqual(['--focus'])
    expect(attached(calls)).toBe(true)
  })

  test('--attach: focus and attach', async () => {
    const calls = await bareHerdr(worker('--fleet', 'x', '--attach', repo), {})
    expect(createFlags(calls)).toEqual(['--focus'])
    expect(attached(calls)).toBe(true)
  })

  test('-d never attaches, even for a server it started', async () => {
    const calls = await bareHerdr(worker('--fleet', 'x', '-d', repo), {
      HERDR_SERVER_DOWN: '1',
    })
    expect(createFlags(calls)).toEqual(['--no-focus'])
    expect(attached(calls)).toBe(false)
  })

  test('-w inside herdr: no focus, no attach', async () => {
    const calls = await bareHerdr(worker('--fleet', 'x', '-w', repo), {
      HERDR_ENV: '1',
    })
    expect(createFlags(calls)).toEqual(['--no-focus'])
    expect(attached(calls)).toBe(false)
  })

  test('a pane that shows no output gets no command', async () => {
    await rm(sb.herdrLog, { force: true })
    const ran = await runNatter(sb, worker('--fleet', 'x', '-d', repo), {
      env: { HERDR_FAIL: '--regex' },
    })
    expect(ran.code).not.toBe(0)
    const calls = await readHerdrCalls(sb)
    expect(calls.filter((c) => c[1] === 'run')).toHaveLength(0)
  })

  test('a missing start line never resends', async () => {
    await rm(sb.herdrLog, { force: true })
    const ran = await runNatter(sb, worker('--fleet', 'x', '-d', repo), {
      env: { HERDR_FAIL: '--match' },
    })
    expect(ran.code).toBe(0)
    const calls = await readHerdrCalls(sb)
    expect(calls.filter((c) => c[1] === 'run')).toHaveLength(1)
  })
})

describe('stub herdr, round trip and failures', () => {
  const WORDS = ['--resume', '=ls', 'a b', "it's"]
  const typedLine = async (env: Record<string, string> = {}) => {
    await rm(sb.herdrLog, { force: true })
    const ran = await runNatter(
      sb,
      worker('--fleet', 'x', '--name', 'api', '-d', spaced, '--', ...WORDS),
      { env },
    )
    expect(ran.code).toBe(0)
    const run = (await readHerdrCalls(sb)).find((c) => c[1] === 'run')
    return run?.[3] as string
  }

  test('the line typed into the pane launches what a direct --here run does', async () => {
    const typed = splitLine(await typedLine())
    const direct = worker(
      '--fleet',
      'x',
      '--name',
      'api',
      '--here',
      spaced,
      '--',
      ...WORDS,
    )
    const fromPane = await runNatter(sb, typed.slice(1), {
      env: { HERDR_ENV: '1' },
    })
    expect(fromPane.code).toBe(0)
    const viaPane = await readStubRun(sb)
    expect((await runNatter(sb, direct)).code).toBe(0)
    expect(viaPane).toEqual(await readStubRun(sb))
    expect(viaPane.argv.slice(-WORDS.length)).toEqual(WORDS)
    expect(viaPane.argv).not.toContain('--here')
  })

  const zsh = Bun.which('zsh')
  ;(zsh ? test : test.skip)(
    'zsh reads the typed line as the same words',
    async () => {
      const line = await typedLine()
      const out = Bun.spawnSync([
        zsh as string,
        '-f',
        '-c',
        `printf '%s\\0' ${line}`,
      ])
      expect(out.exitCode).toBe(0)
      const words = out.stdout.toString().split('\0').slice(0, -1)
      expect(words).toEqual(splitLine(line))
      expect(words).toContain('=ls')
    },
  )

  test('the line typed for an orchestrator launches what a direct --here run does', async () => {
    await rm(sb.herdrLog, { force: true })
    const ran = await runNatter(
      sb,
      orchestrator('--fleet', 'x', '-d', '--', ...WORDS),
    )
    expect(ran.code).toBe(0)
    const run = (await readHerdrCalls(sb)).find((c) => c[1] === 'run')
    const typed = splitLine(run?.[3] as string)
    expect(
      (await runNatter(sb, typed.slice(1), { env: { HERDR_ENV: '1' } })).code,
    ).toBe(0)
    const viaPane = await readStubRun(sb)
    const direct = orchestrator('--fleet', 'x', '--here', '--', ...WORDS)
    expect((await runNatter(sb, direct)).code).toBe(0)
    expect(viaPane).toEqual(await readStubRun(sb))
    expect(viaPane.argv.slice(-WORDS.length)).toEqual(WORDS)
    expect(viaPane.argv).toContain('nats-channel:orchestrator')
    expect(viaPane.argv).not.toContain('--here')
  })

  const sentRuns = async () =>
    (await readHerdrCalls(sb)).filter((c) => c[1] === 'run')

  test('a workspace reply that is not JSON dies with no command sent', async () => {
    await rm(sb.herdrLog, { force: true })
    const ran = await runNatter(sb, worker('--fleet', 'x', '-d', repo), {
      env: { HERDR_CREATE_REPLY: 'not json' },
    })
    expect(ran.code).not.toBe(0)
    expect(await sentRuns()).toHaveLength(0)
  })

  test('a reply with a workspace and no pane dies with no command sent', async () => {
    await rm(sb.herdrLog, { force: true })
    const ran = await runNatter(sb, worker('--fleet', 'x', '-d', repo), {
      env: {
        HERDR_CREATE_REPLY: '{"result":{"workspace":{"workspace_id":"w7"}}}',
      },
    })
    expect(ran.code).not.toBe(0)
    expect(await sentRuns()).toHaveLength(0)
  })

  test('a failed pane run is not retried', async () => {
    await rm(sb.herdrLog, { force: true })
    const ran = await runNatter(sb, worker('--fleet', 'x', '-d', repo), {
      env: { HERDR_FAIL: 'pane run' },
    })
    expect(ran.code).not.toBe(0)
    expect(await sentRuns()).toHaveLength(1)
  })
})

describe('contract with the real claude-orchestrate', () => {
  const real = Bun.which('claude-orchestrate')
  const body = real ? test : test.skip

  /** Our command with the real script's own dry run in place of the stub. */
  async function realDry(env: Record<string, string>) {
    const dry = await dryHere(
      worker('--fleet', 'x', '--name', 'api', '--here', repo, '--', '--resume'),
      env,
    )
    const proc = Bun.spawn(
      [
        'env',
        ...dry.unset.flatMap((u) => ['-u', u]),
        ...Object.entries(dry.set).map(([k, v]) => `${k}=${v}`),
        real as string,
        '-n',
        ...dry.argv.slice(1),
      ],
      {
        env: {
          PATH: `${sb.bin}:${process.env.PATH}`,
          HOME: sb.home,
          XDG_STATE_HOME: sb.state,
          XDG_CONFIG_HOME: join(sb.home, '.config'),
          ...env,
        },
        stdout: 'pipe',
        stderr: 'pipe',
      },
    )
    const [out, code] = [
      await new Response(proc.stdout).text(),
      await proc.exited,
    ]
    expect(code).toBe(0)
    return parseEnvCommand(splitLine(out.trim().split('\n').at(-1) as string))
  }

  body(
    'a worker gets the orchestration prompt and ends at claude with the channel flag',
    async () => {
      const l = await realDry({})
      expect(l.set.CLAUDE_ORCHESTRATE).toBe('1')
      expect(l.argv[0]).toBe('claude')
      expect(l.argv).toContain('--append-system-prompt-file')
      expect(l.argv).not.toContain('--agent')
      const tail = l.argv.slice(l.argv.indexOf(CHANNEL[0] as string))
      expect(tail).toEqual([
        ...CHANNEL,
        '--settings',
        WORKER_SETTINGS,
        '--resume',
      ])
    },
  )

  body(
    'CLAUDE_ORCHESTRATE_AGENT in the environment does not change the route',
    async () => {
      const l = await realDry({ CLAUDE_ORCHESTRATE_AGENT: 'something' })
      expect(l.set.CLAUDE_ORCHESTRATE).toBe('1')
      expect(l.argv).toContain('--append-system-prompt-file')
      expect(l.argv).not.toContain('--agent')
    },
  )
})

describe('contract with the real claude-orchestrate, orchestrator', () => {
  const real = Bun.which('claude-orchestrate')
  ;(real ? test : test.skip)(
    'runs the agent with no orchestration prompt and CLAUDE_ORCHESTRATE cleared',
    async () => {
      await mkdir(stateDir('x'), { recursive: true })
      const dry = await dryHere(
        orchestrator('--fleet', 'x', '--here', '--', '--resume'),
      )
      const proc = Bun.spawn(
        [
          'env',
          ...dry.unset.flatMap((u) => ['-u', u]),
          ...Object.entries(dry.set).map(([k, v]) => `${k}=${v}`),
          real as string,
          '-n',
          ...dry.argv.slice(1),
        ],
        {
          env: {
            PATH: `${sb.bin}:${process.env.PATH}`,
            HOME: sb.home,
            XDG_STATE_HOME: sb.state,
            XDG_CONFIG_HOME: join(sb.home, '.config'),
            CLAUDE_ORCHESTRATE: '1',
          },
          stdout: 'pipe',
          stderr: 'pipe',
        },
      )
      const out = await new Response(proc.stdout).text()
      expect(await proc.exited).toBe(0)
      const l = parseEnvCommand(
        splitLine(out.trim().split('\n').at(-1) as string),
      )
      expect(l.set).toHaveProperty('CLAUDE_ORCHESTRATE', '')
      expect(l.argv[0]).toBe('claude')
      const at = l.argv.indexOf('--agent')
      expect(l.argv[at + 1]).toBe('nats-channel:orchestrator')
      expect(l.argv).not.toContain('--append-system-prompt-file')
      expect(l.argv.slice(l.argv.indexOf(CHANNEL[0] as string))).toEqual([
        ...CHANNEL,
        '--settings',
        ORCHESTRATOR_SETTINGS,
        '--resume',
      ])
    },
  )
})

describe('names agree with the session', () => {
  // `args` starts with the role, which the launcher takes first.
  const dry = async ([role, ...rest]: string[]) =>
    (await runNatter(sb, [role as string, '-n', '--here', ...rest])).code === 0

  const fleets: [string, boolean][] = [
    ['a', true],
    ['x1', true],
    ['a-b', true],
    ['0', true],
    ['default', true],
    ['a'.repeat(32), true],
    ['a'.repeat(33), false],
    ['A', false],
    ['a_b', false],
    ['-a', false],
    ['a b', false],
    ['a.b', false],
    ['a/b', false],
  ]
  const names: [string, boolean][] = [
    ['api', true],
    ['a_b', true],
    ['0x', true],
    ['a'.repeat(64), true],
    ['a'.repeat(65), false],
    ['A', false],
    ['-a', false],
    ['_a', false],
    ['a b', false],
    ['admin', false],
    ['orchestrator', false],
  ]

  test('the pattern text is the one in src/config.ts', async () => {
    const script = await Bun.file(NATTER_BIN).text()
    expect(script).toContain(`FLEET_PATTERN='${FLEET_PATTERN.source}'`)
  })

  test.each(fleets)('fleet %p is accepted: %p', async (fleet, ok) => {
    const session = resolveConfig({
      NATTER_ROLE: 'worker',
      NATTER_NAME: 'api',
      NATTER_FLEET: fleet,
      HOME: sb.home,
    })
    expect('config' in session).toBe(ok)
    expect(await dry(worker('--fleet', fleet, repo))).toBe(ok)
  })

  test.each(names)('worker name %p is accepted: %p', async (name, ok) => {
    const session = resolveConfig({
      NATTER_ROLE: 'worker',
      NATTER_NAME: name,
      NATTER_FLEET: 'x',
      HOME: sb.home,
    })
    expect('config' in session).toBe(ok)
    expect(await dry(worker('--fleet', 'x', '--name', name, repo))).toBe(ok)
  })
})

describe('errors', () => {
  const fails = async (
    args: string[],
    opts: Parameters<typeof runNatter>[2] = {},
  ) => {
    const ran = await runNatter(sb, args, opts)
    expect(ran.code).not.toBe(0)
    expect(ran.stdout).toBe('')
  }

  test('no fleet', () => fails(['-n', ...worker('--here', repo)]))
  test('an empty NATTER_FLEET counts as none', () =>
    fails(['-n', ...worker('--here', repo)], { env: { NATTER_FLEET: '' } }))
  test('a bad fleet', () => fails(['-n', ...worker('--fleet', 'Bad', repo)]))
  test('a missing directory', () =>
    fails(['-n', ...worker('--fleet', 'x', join(sb.root, 'nope'))]))
  test('an unknown role', () => fails(['-n', 'boss', '--fleet', 'x']))
  test('an unknown option', () =>
    fails(['-n', ...worker('--fleet', 'x', '--nope', repo)]))
  test('--here with -w', () =>
    fails(['-n', ...worker('--fleet', 'x', '--here', '-w', repo)]))

  test('claude-orchestrate missing', async () => {
    const path = await isolatedPath(sb)
    await fails(['-n', ...worker('--fleet', 'x', '--here', repo)], { path })
  })

  test('herdr missing, outside a pane, fails and sends nothing', async () => {
    const path = await isolatedPath(sb, 'claude-orchestrate')
    const ran = await runNatter(sb, ['-n', ...worker('--fleet', 'x', repo)], {
      path,
    })
    expect(ran.code).not.toBe(0)
    expect(ran.stdout).toBe('')
  })

  test('a dry run creates no directory', async () => {
    const home = join(sb.root, 'fresh-home')
    const state = join(sb.root, 'fresh-state')
    await Promise.all([mkdir(home), mkdir(state)])
    const env = { HOME: home, XDG_STATE_HOME: state }
    await runNatter(sb, ['-n', ...worker('--fleet', 'x', '--here', repo)], {
      env,
    })
    await runNatter(sb, ['-n', 'orchestrator', '--fleet', 'x'], { env })
    await runNatter(sb, ['-n', 'orchestrator', '--fleet', 'x', '--here'], {
      env,
    })
    await runNatter(sb, ['-n', ...worker('--fleet', 'x', repo)], { env })
    expect(await readdir(state)).toEqual([])
    expect(await readdir(home)).toEqual([])
  })
})
