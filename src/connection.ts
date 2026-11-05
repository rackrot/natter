import {
  connect,
  type NatsConnection,
  type NodeConnectionOptions,
  nkeyAuthenticator,
} from '@nats-io/transport-node'
import { type Config, resolveTransport, type Transport } from './config'

export interface Connection {
  /** Set once connected. */
  nc: NatsConnection | undefined
  /** Why there is no connection, when connecting was refused or failed. */
  unavailable: string | undefined
  close(): Promise<void>
}

/** The connection policy for a session; a one-shot command overrides the waiting. */
export function connectOptions(
  config: Config,
  { url, tls, seed }: Exclude<Transport, { refused: string }>,
): NodeConnectionOptions {
  return {
    servers: url,
    name: `natter-${config.name}`,
    inboxPrefix: `_INBOX.${config.name}`,
    waitOnFirstConnect: true,
    maxReconnectAttempts: -1,
    // shortcut: a gossiped server would skip the transport policy, so this
    // client stays on the configured server; revisit if clustering is wanted.
    ignoreClusterUpdates: true,
    ...(seed && { authenticator: nkeyAuthenticator(seed) }),
    // Without `tls` the client treats tls:// as nats:// on a plain server.
    ...(tls && { tls: config.caFile ? { caFile: config.caFile } : {} }),
  }
}

/**
 * Connects in the background so the MCP handshake never waits on NATS.
 * Failures are logged and recorded, never thrown: the session stays up.
 */
export function startConnection(
  config: Config,
  log: (line: string) => void,
): Connection {
  const conn: Connection = {
    nc: undefined,
    unavailable: undefined,
    async close() {
      await conn.nc?.drain()
    },
  }

  const transport = resolveTransport(config)
  if ('refused' in transport) {
    conn.unavailable = transport.refused
    log(transport.refused)
    return conn
  }
  if (transport.seedWarning) log(transport.seedWarning)
  connect(connectOptions(config, transport)).then(
    (nc) => {
      conn.nc = nc
      log(`connected to ${transport.address}`)
    },
    (err: unknown) => {
      conn.unavailable = err instanceof Error ? err.message : String(err)
      log(`NATS connect failed: ${conn.unavailable}`)
    },
  )
  return conn
}
