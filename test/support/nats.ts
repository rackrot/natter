import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

export interface TestNats {
  /** `nats://127.0.0.1:<port>`; stable across restart(). */
  url: string
  /** Names of the clients currently connected, from the monitoring port. */
  clientNames(): Promise<string[]>
  /** Connections accepted since the server started, even if they then failed. */
  totalConnections(): Promise<number>
  start(): Promise<void>
  stop(): Promise<void>
  restart(): Promise<void>
  /** Signals only the server process this helper started. */
  signal(sig: NodeJS.Signals): void
  /** Brings a server killed under a test back, on the same port and store. */
  revive(): Promise<void>
  /** Stops the server and removes the store directory. */
  cleanup(): Promise<void>
}

const openProbe = () =>
  Bun.listen({ hostname: '127.0.0.1', port: 0, socket: { data() {} } })

/** Two free ports. Both probes stay open until both are read, so they differ. */
function freePorts(): [number, number] {
  const probes = [openProbe(), openProbe()] as const
  const ports: [number, number] = [probes[0].port, probes[1].port]
  for (const probe of probes) probe.stop(true)
  return ports
}

// Below Bun's 5s default test timeout, so a slow condition fails its own test.
// A timeout would also kill every subprocess Bun tracks, shared servers included.
export const until = async (check: () => Promise<boolean>, ms = 4000) => {
  const deadline = Date.now() + ms
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error('condition not met in time')
    await Bun.sleep(50)
  }
}

/**
 * Starts a loopback nats-server. `restart()` reuses the port and store dir.
 * With `autostart: false` the port is reserved but nothing listens until
 * `start()`, which models NATS being down at session startup.
 */
export async function startNats(
  opts: {
    jetstream?: boolean
    autostart?: boolean
    tls?: { certFile: string; keyFile: string }
    /** A nats-server config file; the flags below still set address, ports and store. */
    configFile?: string
    /** A size such as `512MiB`, for JetStream with a storage limit. */
    maxFileStore?: string
  } = {},
): Promise<TestNats> {
  const [port, monitorPort] = freePorts()
  const storeDir = await mkdtemp(join(tmpdir(), 'natter-nats-'))
  const stderrPath = join(storeDir, 'stderr.log')
  let proc: Bun.Subprocess | undefined
  // Bun's timeout kill reaches this at once, while a killed nats-server keeps
  // answering, /healthz included, for tens of ms: no probe of it can tell.
  let canary: Bun.Subprocess | undefined

  const args = [
    '-a',
    '127.0.0.1',
    '-p',
    String(port),
    '-m',
    String(monitorPort),
  ]
  // A config file sets its own store and JetStream.
  if (opts.configFile) args.push('-c', opts.configFile)
  else args.push('-sd', storeDir)
  if (opts.maxFileStore) {
    const conf = join(storeDir, 'limits.conf')
    await writeFile(
      conf,
      `jetstream { max_file_store: ${opts.maxFileStore} }\n`,
    )
    args.push('-c', conf)
  }
  if (opts.jetstream && !opts.configFile) args.push('-js')
  if (opts.tls) {
    args.push('--tls', '--tlscert', opts.tls.certFile)
    args.push('--tlskey', opts.tls.keyFile)
  }

  const startFailure = async (what: string) =>
    new Error(`nats-server ${what}: ${await Bun.file(stderrPath).text()}`)
  const ready = async (server: Bun.Subprocess) => {
    for (let attempt = 0; ; attempt++) {
      try {
        const res = await fetch(`http://127.0.0.1:${monitorPort}/healthz`)
        if (res.ok) return
      } catch {}
      if (attempt >= 100) throw await startFailure('did not become ready')
      const code = await Promise.race([server.exited, Bun.sleep(50)])
      if (code !== undefined) throw await startFailure(`exited with ${code}`)
    }
  }
  const start = async () => {
    const server = Bun.spawn(['nats-server', ...args], {
      stdout: 'ignore',
      stderr: Bun.file(stderrPath),
    })
    proc = server
    canary = Bun.spawn(['sleep', '86400'], {
      stdout: 'ignore',
      stderr: 'ignore',
    })
    await ready(server)
  }
  const stop = async () => {
    canary?.kill()
    canary = undefined
    proc?.kill()
    await proc?.exited
    proc = undefined
  }
  const cleanup = async () => {
    await stop()
    await rm(storeDir, { recursive: true, force: true })
  }

  if (opts.autostart !== false) {
    try {
      await start()
    } catch (err) {
      await cleanup()
      throw err
    }
  }
  return {
    url: `nats://127.0.0.1:${port}`,
    async clientNames() {
      const res = await fetch(`http://127.0.0.1:${monitorPort}/connz`)
      const body = (await res.json()) as { connections: { name?: string }[] }
      return body.connections.map((c) => c.name ?? '')
    },
    async totalConnections() {
      const res = await fetch(`http://127.0.0.1:${monitorPort}/varz`)
      const body = (await res.json()) as { total_connections: number }
      return body.total_connections
    },
    start,
    stop,
    signal: (sig) => {
      proc?.kill(sig)
    },
    restart: async () => {
      await stop()
      await start()
    },
    revive: async () => {
      if (!canary) return
      // Gives a kill that has already landed time to be reported.
      await Promise.race([canary.exited, Bun.sleep(5)])
      if (canary.exitCode !== null || canary.signalCode !== null) {
        await stop()
        await start()
      }
    },
    cleanup,
  }
}
