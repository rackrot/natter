import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  symlink,
  writeFile,
} from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

export const NATTER_BIN = join(import.meta.dir, '..', '..', 'bin', 'natter')

/** Variables the stub `claude-orchestrate` reports when they are set. */
const REPORTED = [
  'NATTER_ROLE',
  'NATTER_FLEET',
  'NATTER_NAME',
  'NATTER_PROVISION',
  'NATTER_URL',
  'CLAUDE_ORCHESTRATE_AGENT',
]

export interface Sandbox {
  root: string
  home: string
  state: string
  /** Stub directory, put first on PATH. */
  bin: string
  /** Where the stub `claude-orchestrate` writes what it was run with. */
  coLog: string
  /** Where the stub `herdr` appends one line per call. */
  herdrLog: string
}

export async function makeSandbox(): Promise<Sandbox> {
  const root = await mkdtemp(join(tmpdir(), 'natter-launcher-'))
  const sb = {
    root,
    home: join(root, 'home'),
    state: join(root, 'state'),
    bin: join(root, 'bin'),
    coLog: join(root, 'co.log'),
    herdrLog: join(root, 'herdr.log'),
  }
  await Promise.all([mkdir(sb.home), mkdir(sb.state), mkdir(sb.bin)])
  return sb
}

export async function writeStub(sb: Sandbox, name: string, body: string) {
  const path = join(sb.bin, name)
  await writeFile(path, `#!/usr/bin/env bash\n${body}\n`)
  await chmod(path, 0o755)
}

/**
 * Records argv, then the reported variables that are set, as NUL-separated
 * items split by a `--env` marker. Nothing else about claude is run.
 */
export const stubClaudeOrchestrate = (sb: Sandbox) =>
  writeStub(
    sb,
    'claude-orchestrate',
    `{
  printf '%s\\0' "$@"
  printf '%s\\0' --env
  for v in ${REPORTED.join(' ')}; do
    [[ -z \${!v+x} ]] || printf '%s\\0' "$v=\${!v}"
  done
} > "${sb.coLog}"`,
  )

/**
 * A PATH holding only what `bin/natter` needs from the system, so neither the
 * real herdr nor the real claude-orchestrate is reachable. Stubs go in `bin`.
 */
export async function isolatedPath(sb: Sandbox, ...stubs: string[]) {
  const dir = join(sb.root, `isolated-${stubs.join('-') || 'none'}`)
  await mkdir(dir, { recursive: true })
  for (const tool of ['bash', 'env', 'dirname', 'readlink', 'basename']) {
    const real = Bun.which(tool)
    if (real) await symlink(real, join(dir, tool)).catch(() => {})
  }
  for (const stub of stubs) {
    await symlink(join(sb.bin, stub), join(dir, stub)).catch(() => {})
  }
  return dir
}

export const stubClaude = (sb: Sandbox) => writeStub(sb, 'claude', 'exit 0')

/**
 * Records each call as NUL-separated argv on one line, and replies with canned
 * JSON, or HERDR_CREATE_REPLY for `workspace create`. A call fails when
 * HERDR_FAIL is a regex that matches its arguments. `status server` says
 * running, or with HERDR_SERVER_DOWN set, stopped until `herdr server` is called.
 */
export const stubHerdr = (sb: Sandbox) =>
  writeStub(
    sb,
    'herdr',
    `{ if (($#)); then printf '%s\\0' "$@"; fi; printf '\\n'; } >> "${sb.herdrLog}"
[[ -z \${HERDR_FAIL:-} || ! "$*" =~ $HERDR_FAIL ]] || exit 1
reply='{"id":"x","result":{"type":"workspace_created","workspace":{"workspace_id":"w9"},"root_pane":{"pane_id":"w9:p1"}}}'
case "$1 $2" in
  "status server") [[ -z \${HERDR_SERVER_DOWN:-} || -e "${sb.herdrLog}.up" ]] && echo 'status: running' || echo 'status: stopped' ;;
  "server ") : > "${sb.herdrLog}.up" ;;
  "workspace create") echo "\${HERDR_CREATE_REPLY:-$reply}" ;;
esac`,
  )

export interface Ran {
  code: number
  stdout: string
  stderr: string
}

export interface RunOpts {
  env?: Record<string, string>
  cwd?: string
  /** Replaces the PATH, which otherwise is the stubs then the real one. */
  path?: string
}

/**
 * Runs `bin/natter` with exactly a temp HOME and XDG_STATE_HOME, so ambient
 * NATTER_*, CLAUDE_* and HERDR_* variables never reach it.
 */
export async function runNatter(
  sb: Sandbox,
  args: string[],
  opts: RunOpts = {},
): Promise<Ran> {
  // A leading -n reads as "dry run", but the launcher takes the role first.
  const argv =
    args[0] === '-n' ? [args[1] as string, '-n', ...args.slice(2)] : args
  const proc = Bun.spawn([NATTER_BIN, ...argv], {
    cwd: opts.cwd ?? sb.root,
    env: {
      PATH: opts.path ?? `${sb.bin}:${process.env.PATH ?? ''}`,
      HOME: sb.home,
      XDG_STATE_HOME: sb.state,
      XDG_CONFIG_HOME: join(sb.home, '.config'),
      ...opts.env,
    },
    stdout: 'pipe',
    stderr: 'pipe',
  })
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ])
  return { code, stdout, stderr }
}

/** Splits a `printf %q` line into argv the way a shell would. */
export function splitLine(line: string): string[] {
  const out = Bun.spawnSync(['bash', '-c', `printf '%s\\0' ${line}`])
  return out.stdout.toString().split('\0').slice(0, -1)
}

/** The command a dry run printed for each line, as argv. */
export const dryCommands = (stdout: string) =>
  stdout.trim().split('\n').map(splitLine)

export interface Launch {
  /** Variables set on the command, from `NAME=value`. */
  set: Record<string, string>
  /** Variables cleared with `-u`. */
  unset: string[]
  /** The command and its arguments after `env`. */
  argv: string[]
}

/** Reads `env [-u NAME]... [NAME=value]... command args...`. */
export function parseEnvCommand(words: string[]): Launch {
  if (words[0] !== 'env') throw new Error(`not an env command: ${words}`)
  const set: Record<string, string> = {}
  const unset: string[] = []
  let i = 1
  while (i < words.length) {
    const w = words[i] as string
    if (w === '-u') {
      unset.push(words[i + 1] as string)
      i += 2
    } else if (/^[A-Z_]+=/.test(w)) {
      set[w.slice(0, w.indexOf('='))] = w.slice(w.indexOf('=') + 1)
      i += 1
    } else break
  }
  return { set, unset, argv: words.slice(i) }
}

/** What the stub `claude-orchestrate` saw: its argv and the reported variables. */
export async function readStubRun(sb: Sandbox) {
  const items = (await readFile(sb.coLog, 'utf8')).split('\0').slice(0, -1)
  const marker = items.indexOf('--env')
  const env: Record<string, string> = {}
  for (const kv of items.slice(marker + 1)) {
    env[kv.slice(0, kv.indexOf('='))] = kv.slice(kv.indexOf('=') + 1)
  }
  return { argv: items.slice(0, marker), env }
}

/** Every call the stub `herdr` recorded, as argv. */
export async function readHerdrCalls(sb: Sandbox): Promise<string[][]> {
  const text = await readFile(sb.herdrLog, 'utf8').catch(() => '')
  return text
    .split('\n')
    .slice(0, -1)
    .map((l) => l.split('\0').slice(0, -1))
}
