import { existsSync, statSync } from 'node:fs'
import { createConnection } from 'node:net'
import { type JetStreamManager, jetstreamManager } from '@nats-io/jetstream'
import { connect, type NatsConnection } from '@nats-io/transport-node'
import { z } from 'zod'
import { diff, streamsFor } from './bootstrap'
import {
  adminConfig,
  bootstrapCommand,
  type Config,
  type Env,
  resolveConfig,
  resolveTransport,
} from './config'
import { connectOptions } from './connection'
import { fleetScope, RESULTS_DURABLE, workerDurable } from './protocol'

// Runs as a command and as the `doctor` tool of both roles. It connects once
// and never retries, which a session's silent retry cannot do, so each failure
// names its cause. It never prints seed content.

const REACH_TIMEOUT_MS = 3000
const CONNECT_TIMEOUT_MS = 5000
const DEFAULT_PORT = 4222
// Above the checks' own bounds, so it only stops a step that has none.
const TOOL_TIMEOUT_MS = 20_000
// The client's wording for a failed TLS handshake or certificate check.
const TLS_FAILURE =
  /tls|ssl|certificate|self[- ]signed|unable to verify|handshake|secure connection/i

type Status = 'pass' | 'fail' | 'skip'
interface Check {
  name: string
  status: Status
  detail: string
}

const reason = (err: unknown) =>
  err instanceof Error ? err.message : String(err)

/** Opens and closes a TCP connection, so an unreachable host is told from a TLS or auth failure. */
function reach(host: string, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ host, port, timeout: REACH_TIMEOUT_MS })
    socket.once('connect', () => {
      socket.destroy()
      resolve()
    })
    socket.once('timeout', () => {
      socket.destroy()
      reject(
        new Error(`no answer from ${host}:${port} in ${REACH_TIMEOUT_MS}ms`),
      )
    })
    socket.once('error', (err: NodeJS.ErrnoException) => {
      reject(
        new Error(`cannot reach ${host}:${port}: ${err.code ?? err.message}`),
      )
    })
  })
}

export async function run(env: Env): Promise<Check[]> {
  const checks: Check[] = []
  let blocked = false
  /** Runs `fn` unless an earlier check failed; its return value is the detail of a pass. */
  const check = async (name: string, fn: () => Promise<string> | string) => {
    if (blocked) {
      checks.push({ name, status: 'skip', detail: 'an earlier check failed' })
      return
    }
    try {
      checks.push({ name, status: 'pass', detail: await fn() })
    } catch (err) {
      blocked = true
      checks.push({ name, status: 'fail', detail: reason(err) })
    }
  }

  // No role is fine for the connection checks; a role that cannot resolve is not.
  const resolved = resolveConfig(env)
  let config: Config
  let consumer: { stream: string; durable: string } | undefined
  if ('config' in resolved) {
    config = resolved.config
    const { streams } = fleetScope(config.fleet)
    consumer =
      config.role === 'orchestrator'
        ? { stream: streams.results, durable: RESULTS_DURABLE }
        : { stream: streams.tasks, durable: workerDurable(config.name) }
    checks.push({
      name: 'config',
      status: 'pass',
      detail: `${config.role} ${config.name} in fleet ${config.fleet}`,
    })
  } else {
    try {
      config = adminConfig(env)
    } catch (err) {
      // An invalid fleet: nothing below can name a stream.
      return [{ name: 'config', status: 'fail', detail: reason(err) }]
    }
    const noRole = !env.NATTER_ROLE
    checks.push({
      name: 'config',
      status: noRole ? 'pass' : 'fail',
      detail: noRole
        ? `no NATTER_ROLE; connection checks only, fleet ${config.fleet}`
        : resolved.inert,
    })
    blocked = !noRole
  }

  const transport = resolveTransport(config)
  await check('transport policy', () => {
    if ('refused' in transport) throw new Error(transport.refused)
    return transport.address
  })
  if ('refused' in transport) {
    // The seed is read by the policy check, so it fails there with the reason.
    for (const name of ['seed', 'tcp', 'tls', 'auth', 'jetstream', 'streams']) {
      await check(name, () => '')
    }
    return checks
  }
  // A loose mode is reported but does not stop the connection checks.
  const wasBlocked = blocked
  await check('seed', () => {
    if (!transport.seed) return `none at ${config.seedPath}`
    const mode = (statSync(config.seedPath).mode & 0o777).toString(8)
    if (transport.seedWarning) {
      throw new Error(`mode ${mode}; chmod 600 ${config.seedPath}`)
    }
    return `readable, mode ${mode}`
  })

  blocked = wasBlocked
  const { hostname, port } = new URL(transport.url)
  await check('tcp', async () => {
    await reach(hostname.replace(/^\[|\]$/g, ''), Number(port) || DEFAULT_PORT)
    return transport.address
  })

  // One attempt serves the TLS and auth checks.
  let nc: NatsConnection | undefined
  let failure: unknown
  if (!blocked) {
    try {
      nc = await connect({
        ...connectOptions(config, transport),
        waitOnFirstConnect: false,
        maxReconnectAttempts: 0,
        timeout: CONNECT_TIMEOUT_MS,
      })
    } catch (err) {
      failure = err
    }
  }
  try {
    await check('tls', () => {
      if (!transport.tls) return 'not used (nats://)'
      if (config.caFile && !existsSync(config.caFile)) {
        throw new Error(`CA file ${config.caFile} does not exist`)
      }
      if (failure && TLS_FAILURE.test(reason(failure))) throw failure
      return config.caFile ? `verified against ${config.caFile}` : 'verified'
    })
    await check('auth', () => {
      if (failure) throw failure
      return transport.seed ? 'nkey accepted' : 'no seed; server needs no auth'
    })
    let jsm: JetStreamManager | undefined
    await check('jetstream', async () => {
      jsm = await jetstreamManager(nc as NatsConnection)
      const { streams } = await jsm.getAccountInfo()
      return `${streams} streams in the account`
    })
    const reconcile = bootstrapCommand(config.fleet)
    for (const want of streamsFor(config.fleet)) {
      await check(`stream ${want.name}`, async () => {
        const info = await (jsm as JetStreamManager).streams
          .info(want.name)
          .catch(() => {
            throw new Error(`missing; run ${reconcile}`)
          })
        const drift = diff(want, info.config)
        if (drift.length) {
          throw new Error(
            `differs from bootstrap in ${drift.join(', ')}; run ${reconcile}`,
          )
        }
        return 'matches bootstrap'
      })
    }
    if (consumer) {
      const { stream, durable } = consumer
      await check(`consumer ${durable}`, async () => {
        await (jsm as JetStreamManager).consumers
          .info(stream, durable)
          .catch(() => {
            throw new Error(
              `missing on ${stream}; the session creates it when it first connects`,
            )
          })
        return `on ${stream}`
      })
    }
  } finally {
    await nc?.close()
  }
  return checks
}

const formatChecks = (checks: Check[]) =>
  checks
    .map(
      ({ name, status, detail }) =>
        `${status.toUpperCase().padEnd(4)} ${name}: ${detail}`,
    )
    .join('\n')

export const DOCTOR_TOOL = {
  name: 'doctor',
  description:
    "Checks NATS connection, auth, streams and consumer on a fresh connection with this session's settings, and returns one PASS, FAIL or SKIP line per check. A failed check is in the text, not an error: report each FAIL with its reason. Opens one extra connection and closes it. Call it yourself, not through a subagent.",
  inputSchema: z.toJSONSchema(z.object({})),
}

/** The tool's reply: the CLI's lines, bounded so a hung server cannot hold it. */
export async function doctorReply(env: Env) {
  let timer: ReturnType<typeof setTimeout> | undefined
  const timedOut = new Promise<Check[]>((resolve) => {
    timer = setTimeout(
      () =>
        resolve([
          {
            name: 'doctor',
            status: 'fail',
            detail: `no result in ${TOOL_TIMEOUT_MS}ms`,
          },
        ]),
      TOOL_TIMEOUT_MS,
    )
  })
  try {
    // A bad address throws from `run`: it is a failed check, not a tool error.
    const checks = await Promise.race([
      run(env).catch((err): Check[] => [
        {
          name: 'doctor',
          status: 'fail',
          detail: err instanceof Error ? err.message : String(err),
        },
      ]),
      timedOut,
    ])
    return { content: [{ type: 'text' as const, text: formatChecks(checks) }] }
  } finally {
    clearTimeout(timer)
  }
}

if (import.meta.main) {
  const checks = await run(process.env)
  console.log(formatChecks(checks))
  process.exit(checks.some((c) => c.status === 'fail') ? 1 : 0)
}
