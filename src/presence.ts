import { hostname } from 'node:os'
import { basename } from 'node:path'
import type { StoredMsg } from '@nats-io/jetstream'
import manifest from '../.claude-plugin/plugin.json'
import type { Config, Env } from './config'
import {
  encodeEnvelope,
  PROTOCOL_VERSION,
  type Presence,
  WireError,
} from './protocol'

// Presence, plan 3.5: what a worker says about itself, and how the orchestrator
// reads it. All of it is worker-written, so it is shown and never relied on;
// only the stream's own timestamp says how old it is.

export type PresenceStatus = Pick<Presence, 'state' | 'task_id'>

/** What send_task reports when a record, not presence, vouches for a worker. */
export const NO_PRESENCE = 'no_presence'

/** A function from a worker's current state to its heartbeat envelope. */
export function presenceEncoder(
  config: Config,
  env: Env,
  instance: string,
  heartbeatMs: number,
) {
  const identity = {
    v: PROTOCOL_VERSION,
    type: 'presence',
    worker: config.name,
    instance,
    host: hostname(),
    // The basename: machines differ, and a local path means nothing elsewhere.
    repo: basename(env.CLAUDE_PROJECT_DIR ?? ''),
    description: config.description,
    plugin_version: manifest.version,
    heartbeat_s: Math.ceil(heartbeatMs / 1000),
  }
  return (status: PresenceStatus): Uint8Array => {
    const encoded = encodeEnvelope({ ...identity, ...status })
    if (!encoded.ok) throw new WireError(encoded.issues.join('; '))
    return encoded.bytes
  }
}

const ageMs = (msg: StoredMsg) => Date.now() - msg.time.getTime()

/** Stream time, on a threshold of the reader's: nothing a worker wrote counts. */
export const isStale = (msg: StoredMsg, staleMs: number) => ageMs(msg) > staleMs

/** A worker's line in list_workers. */
export function workerRow(presence: Presence, msg: StoredMsg, staleMs: number) {
  const { worker, repo, host, description, state, task_id } = presence
  return {
    name: worker,
    repo,
    host,
    description,
    state,
    task_id,
    stale: isStale(msg, staleMs),
    // Orchestrator and broker clocks can differ by a few seconds.
    age_s: Math.max(0, Math.round(ageMs(msg) / 1000)),
  }
}

/** One word for send_task. A clean exit stays `offline` however old it gets. */
export const workerState = (
  presence: Presence,
  msg: StoredMsg,
  staleMs: number,
) =>
  presence.state !== 'offline' && isStale(msg, staleMs)
    ? 'stale'
    : presence.state
