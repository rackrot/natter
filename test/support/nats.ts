import { mkdtemp, rm } from 'node:fs/promises'
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
  /** Stops the server and removes the store directory. */
  cleanup(): Promise<void>
}

async function freePort(): Promise<number> {
  const probe = Bun.listen({
    hostname: '127.0.0.1',
    port: 0,
    socket: { data() {} },
  })
  const { port } = probe
  probe.stop(true)
  return port
}

export const until = async (check: () => Promise<boolean>, ms = 5000) => {
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
  } = {},
): Promise<TestNats> {
  const port = await freePort()
  const monitorPort = await freePort()
  const storeDir = await mkdtemp(join(tmpdir(), 'natter-nats-'))
  let proc: Bun.Subprocess | undefined

  const args = [
    '-a',
    '127.0.0.1',
    '-p',
    String(port),
    '-m',
    String(monitorPort),
    '-sd',
    storeDir,
  ]
  if (opts.jetstream) args.push('-js')
  if (opts.tls) {
    args.push('--tls', '--tlscert', opts.tls.certFile)
    args.push('--tlskey', opts.tls.keyFile)
  }

  const ready = async () => {
    for (let attempt = 0; ; attempt++) {
      try {
        const res = await fetch(`http://127.0.0.1:${monitorPort}/healthz`)
        if (res.ok) return
      } catch {}
      if (attempt >= 100) throw new Error('nats-server did not become ready')
      await Bun.sleep(50)
    }
  }
  const start = async () => {
    proc = Bun.spawn(['nats-server', ...args], {
      stdout: 'ignore',
      stderr: 'ignore',
    })
    await ready()
  }
  const stop = async () => {
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
    restart: async () => {
      await stop()
      await start()
    },
    cleanup,
  }
}
