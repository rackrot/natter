import {
  connect,
  type NatsConnection,
  nkeyAuthenticator,
} from '@nats-io/transport-node'
import { type Config, resolveTransport } from './config'

export interface Connection {
  /** Set once connected. */
  nc: NatsConnection | undefined
  /** Why there is no connection, when connecting was refused or failed. */
  unavailable: string | undefined
  close(): Promise<void>
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
  const { url, address, tls, seed } = transport

  connect({
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
  }).then(
    (nc) => {
      conn.nc = nc
      log(`connected to ${address}`)
    },
    (err: unknown) => {
      conn.unavailable = err instanceof Error ? err.message : String(err)
      log(`NATS connect failed: ${conn.unavailable}`)
    },
  )
  return conn
}
