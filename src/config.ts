import { existsSync, readFileSync, statSync } from 'node:fs'
import { homedir } from 'node:os'
import { basename, join } from 'node:path'

export const NAME_PATTERN = /^[a-z0-9][a-z0-9_-]{0,63}$/
export const MAX_DESCRIPTION_BYTES = 512
export const MAX_TIMEOUT_S = 86400
export const DEFAULT_NATS_URL = 'nats://127.0.0.1:4222'
export const DEFAULT_CREDS_DIR = '~/.config/natter'
export const DEFAULT_TIMEOUT_S = 3600
export const DEFAULT_QUEUE_TTL_S = 86400
export const DEFAULT_MAX_ATTEMPTS = 3
// Past created_at + queue_ttl_s + accept timeout + timeout_s (plan 3.5).
export const OVERDUE_MARGIN_S = 300
const DAY_MS = 24 * 60 * 60 * 1000
/** How long NATTER_RESULTS keeps a result, so what a record can still prove. */
export const RESULTS_MAX_AGE_MS = 14 * DAY_MS
const ADMIN_NAME = 'admin'
const SEED_FILE = 'natter.nk'
// Shutdown must finish well inside the 3s the plugin promises on stdin close.
export const SHUTDOWN_DEADLINE_MS = 2000
const ACK_WAIT_MS = 120_000
const WORKING_MS = 30_000
const ACCEPT_TIMEOUT_MS = 600_000
const STARTUP_GRACE_MS = 5000
const STREAM_RETRY_MS = 10_000
const CONNECT_POLL_MS = 100
const RESTART_MIN_MS = 1000
const RESTART_MAX_MS = 15_000
const HEARTBEAT_MS = 15_000
const STALE_MS = 45_000
const ATTENTION_SWEEP_MS = 30_000
// The window every per-minute limit of plan 3.7 is counted over.
const RATE_WINDOW_MS = 60_000
// The shortest gap between two `accepted` status writes for one task.
const STATUS_WINDOW_MS = 5000
// Plan 3.7 loop guards. Per process, in memory: a restart starts fresh.
const SEND_PER_WORKER = 6
const SEND_GLOBAL = 30
const PUSH_BUDGET = 30
// Attention pushes have a budget of their own, so a result backlog cannot
// hold them off.
const ATTENTION_PER_WINDOW = 10
const MAX_DEPTH = 5

export type Role = 'orchestrator' | 'worker'

export interface Config {
  role: Role
  name: string
  description: string
  url: string
  seedPath: string
  caFile: string | undefined
  allowPlaintext: boolean
}

export type Resolved = { config: Config } | { inert: string }

export type Env = Record<string, string | undefined>

// Empty counts as unset: unset `userConfig` values substitute as ''.
const get = (env: Env, key: string) => env[key] || undefined

// Reserved so a session cannot take the bootstrap identity's name or a
// worker the orchestrator's.
const isReserved = (role: Role, name: string) =>
  name === ADMIN_NAME || (role === 'worker' && name === 'orchestrator')

function sanitizeName(raw: string): string {
  return raw
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^[^a-z0-9]+/, '')
    .slice(0, 64)
}

function expandHome(path: string): string {
  return path === '~' || path.startsWith('~/')
    ? join(homedir(), path.slice(1))
    : path
}

function connectionSettings(env: Env) {
  const credsDir = expandHome(
    get(env, 'NATTER_CREDS_DIR') ??
      get(env, 'NATTER_UC_CREDS_DIR') ??
      DEFAULT_CREDS_DIR,
  )
  return {
    url:
      get(env, 'NATTER_URL') ?? get(env, 'NATTER_UC_URL') ?? DEFAULT_NATS_URL,
    // One shared seed for every session and bootstrap.
    seedPath: join(credsDir, SEED_FILE),
    caFile: get(env, 'NATTER_CA_FILE') ?? get(env, 'NATTER_UC_CA_FILE'),
    allowPlaintext: get(env, 'NATTER_ALLOW_PLAINTEXT') === '1',
  }
}

/**
 * Resolves identity and connection settings synchronously, because the MCP
 * `instructions` and capabilities are fixed when the Server is constructed.
 * Anything unusable yields `{ inert }` with the reason. The role comes from
 * the environment only, so a committed file cannot activate a plain session.
 */
export function resolveConfig(env: Env): Resolved {
  const role = get(env, 'NATTER_ROLE')
  if (role !== 'orchestrator' && role !== 'worker') {
    return { inert: 'NATTER_ROLE is not "orchestrator" or "worker"' }
  }

  // Never process.cwd(): the plugin runs with its own root as cwd.
  const projectDir = get(env, 'CLAUDE_PROJECT_DIR')
  let file: { name?: unknown; description?: unknown } = {}
  if (projectDir) {
    const path = join(projectDir, '.claude', 'natter.json')
    if (existsSync(path)) {
      try {
        file = JSON.parse(readFileSync(path, 'utf8'))
      } catch (err) {
        return { inert: `cannot read ${path}: ${(err as Error).message}` }
      }
      if (typeof file !== 'object' || file === null) {
        return { inert: `${path} must contain a JSON object` }
      }
    }
  }

  let name = get(env, 'NATTER_NAME')
  if (name === undefined && file.name !== undefined) {
    if (typeof file.name !== 'string') {
      return { inert: 'name in .claude/natter.json must be a string' }
    }
    name = file.name
  }
  if (name === undefined) {
    if (!projectDir) {
      return { inert: 'no NATTER_NAME and no CLAUDE_PROJECT_DIR to derive one' }
    }
    name = sanitizeName(basename(projectDir))
  }
  if (!NAME_PATTERN.test(name)) {
    return { inert: `name ${JSON.stringify(name)} must match ${NAME_PATTERN}` }
  }
  if (isReserved(role, name)) {
    return {
      inert: `name ${JSON.stringify(name)} is reserved, not for a ${role}`,
    }
  }

  const description = file.description ?? ''
  if (
    typeof description !== 'string' ||
    Buffer.byteLength(description) > MAX_DESCRIPTION_BYTES
  ) {
    return {
      inert: `description must be a string of at most ${MAX_DESCRIPTION_BYTES} bytes`,
    }
  }

  return {
    config: { role, name, description, ...connectionSettings(env) },
  }
}

/**
 * The identity bootstrap runs as, regardless of any session name in the
 * environment. It shares the seed of every session.
 */
export function adminConfig(env: Env): Config {
  return {
    role: 'orchestrator',
    name: ADMIN_NAME,
    description: '',
    ...connectionSettings(env),
  }
}

/** Plan 3.7 timers, in ms. Tests shorten any of them with `NATTER_TEST_*_MS`. */
export function resolveTimers(env: Env) {
  const ms = (key: string, fallback: number) => {
    const value = Number(env[`NATTER_TEST_${key}_MS`])
    return value > 0 ? value : fallback
  }
  return {
    ackWait: ms('ACK_WAIT', ACK_WAIT_MS),
    working: ms('WORKING', WORKING_MS),
    acceptTimeout: ms('ACCEPT_TIMEOUT', ACCEPT_TIMEOUT_MS),
    startupGrace: ms('STARTUP_GRACE', STARTUP_GRACE_MS),
    streamRetry: ms('STREAM_RETRY', STREAM_RETRY_MS),
    connectPoll: ms('CONNECT_POLL', CONNECT_POLL_MS),
    restartMin: ms('RESTART_MIN', RESTART_MIN_MS),
    restartMax: ms('RESTART_MAX', RESTART_MAX_MS),
    heartbeat: ms('HEARTBEAT', HEARTBEAT_MS),
    stale: ms('STALE', STALE_MS),
    attentionSweep: ms('ATTENTION_SWEEP', ATTENTION_SWEEP_MS),
    resultsMaxAge: ms('RESULTS_MAX_AGE', RESULTS_MAX_AGE_MS),
    rateWindow: ms('RATE_WINDOW', RATE_WINDOW_MS),
    statusWindow: ms('STATUS_WINDOW', STATUS_WINDOW_MS),
  }
}

/** Plan 3.7 loop guards, per `rateWindow`. Tests lower them with `NATTER_TEST_*`. */
export function resolveLimits(env: Env) {
  const count = (key: string, fallback: number) => {
    // Whole numbers of at least 1: a fraction rounds down, anything else is unset.
    const value = Math.floor(Number(env[`NATTER_TEST_${key}`]))
    return Number.isSafeInteger(value) && value >= 1 ? value : fallback
  }
  return {
    sendPerWorker: count('SEND_PER_WORKER', SEND_PER_WORKER),
    sendGlobal: count('SEND_GLOBAL', SEND_GLOBAL),
    pushBudget: count('PUSH_BUDGET', PUSH_BUDGET),
    attentionBudget: count('ATTENTION_BUDGET', ATTENTION_PER_WINDOW),
    maxDepth: count('MAX_DEPTH', MAX_DEPTH),
  }
}

/** The command that opens a URL. Tests point `NATTER_TEST_OPENER` at a stub. */
export function resolveOpener(env: Env, platform = process.platform): string {
  return (
    get(env, 'NATTER_TEST_OPENER') ??
    (platform === 'darwin' ? 'open' : 'xdg-open')
  )
}

export type Transport =
  | { refused: string }
  | {
      /** Normalized, and the exact string the policy checked. */
      url: string
      /** `host[:port]`: the only form of the URL that may be logged. */
      address: string
      tls: boolean
      seed: Buffer | undefined
      /** Set when the seed file is readable by group or others. Holds no seed content. */
      seedWarning: string | undefined
    }

// The client ignores the scheme and dials whatever follows the last `://`, so
// only a bare `scheme://host[:port]` is certain to reach the host checked here.
// No `u` flag: combined with `i` it would fold non-ASCII letters into [a-z].
const URL_PATTERN =
  /^(nats|tls):\/\/(\[[0-9a-f:.]+\]|[a-z0-9._-]+)(?::(\d{1,5}))?$/i
const MAX_PORT = 65535

const isLoopback = (host: string) =>
  host === 'localhost' || host === '::1' || /^127(\.\d{1,3}){3}$/.test(host)

/**
 * Applies the transport policy and returns what to connect with. The seed is
 * read here, once, so the one the policy saw is the one the client uses. Only
 * a missing seed counts as none: an unusable one must not mean unauthenticated.
 */
export function resolveTransport(config: Config): Transport {
  const [, rawScheme, rawHost, port] = URL_PATTERN.exec(config.url) ?? []
  if (!rawScheme || !rawHost || Number(port) > MAX_PORT) {
    // Echoes nothing from the URL, which may hold a password.
    return {
      refused:
        'invalid NATS url: expected nats://host[:port] or tls://host[:port]',
    }
  }
  const scheme = rawScheme.toLowerCase()
  const host = rawHost.toLowerCase()
  const address = port ? `${host}:${port}` : host

  let seed: Buffer | undefined
  let seedWarning: string | undefined
  try {
    // Surrounding whitespace is not part of the seed, and an empty file is no seed.
    const text = readFileSync(config.seedPath).toString().trim()
    seed = text ? Buffer.from(text) : undefined
    if (statSync(config.seedPath).mode & 0o077) {
      seedWarning = `seed ${config.seedPath} is readable by group or others; chmod 600 it`
    }
  } catch (err) {
    const { code } = err as NodeJS.ErrnoException
    if (code !== 'ENOENT') {
      return { refused: `cannot read seed ${config.seedPath}: ${code}` }
    }
  }

  const tls = scheme === 'tls'
  if (!isLoopback(host.replace(/^\[|\]$/g, ''))) {
    if (!seed) {
      return {
        refused: `non-loopback NATS host ${address} requires a seed at ${config.seedPath}`,
      }
    }
    if (!tls && !config.allowPlaintext) {
      return {
        refused: `non-loopback NATS host ${address} requires a tls:// url (or NATTER_ALLOW_PLAINTEXT=1)`,
      }
    }
  }
  return { url: `${scheme}://${address}`, address, tls, seed, seedWarning }
}
