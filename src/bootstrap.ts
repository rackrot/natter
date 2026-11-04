import {
  DiscardPolicy,
  JetStreamApiCodes,
  JetStreamApiError,
  type JetStreamManager,
  jetstreamManager,
  RetentionPolicy,
  StorageType,
  type StreamConfig,
} from '@nats-io/jetstream'
import { connect, nanos } from '@nats-io/transport-node'
import { adminConfig, resolveTransport } from './config'
import { connectOptions } from './connection'
import {
  MAX_MESSAGE_BYTES,
  PRESENCE_SUBJECTS,
  RESULT_SUBJECTS,
  STATUS_SUBJECTS,
  TASK_SUBJECTS,
} from './protocol'

// Admin-only: creates or reconciles the three streams (plan 3.3). Workers
// never create streams. Standalone command, not imported by src/server.ts.

const DAY_MS = 24 * 60 * 60 * 1000
const MIB = 1024 * 1024
const TASKS_MAX_BYTES = 256 * MIB
const RESULTS_MAX_BYTES = 1024 * MIB
const STATE_MAX_BYTES = 256 * MIB

/** The settings this command owns; everything else is left at server defaults. */
export type ManagedStream = Pick<
  StreamConfig,
  | 'name'
  | 'subjects'
  | 'retention'
  | 'storage'
  | 'max_age'
  | 'max_msgs_per_subject'
  | 'discard'
  | 'discard_new_per_subject'
  | 'duplicate_window'
  | 'allow_direct'
  | 'max_msg_size'
  | 'max_bytes'
>

export const TASKS_STREAM: ManagedStream = {
  name: 'NATTER_TASKS',
  subjects: [TASK_SUBJECTS],
  retention: RetentionPolicy.Workqueue,
  storage: StorageType.File,
  max_age: nanos(7 * DAY_MS),
  max_msgs_per_subject: 16,
  discard: DiscardPolicy.New,
  discard_new_per_subject: true,
  duplicate_window: nanos(2 * 60 * 1000),
  allow_direct: false,
  max_msg_size: MAX_MESSAGE_BYTES,
  max_bytes: TASKS_MAX_BYTES,
}

// First-result-wins is enforced here: a recorded result refuses every later
// message on its subject until max_age, however it is published, because ACLs
// cannot require the lastSubjectSequence: 0 header. A full stream rejects new
// results rather than evicting recorded ones.
export const RESULTS_STREAM: ManagedStream = {
  name: 'NATTER_RESULTS',
  subjects: [RESULT_SUBJECTS],
  retention: RetentionPolicy.Limits,
  storage: StorageType.File,
  max_age: nanos(14 * DAY_MS),
  max_msgs_per_subject: 1,
  discard: DiscardPolicy.New,
  discard_new_per_subject: true,
  duplicate_window: nanos(2 * 60 * 1000),
  allow_direct: true,
  max_msg_size: MAX_MESSAGE_BYTES,
  max_bytes: RESULTS_MAX_BYTES,
}

// Status and presence are core publishes the stream captures; the newest
// replaces the previous one.
export const STATE_STREAM: ManagedStream = {
  name: 'NATTER_STATE',
  subjects: [STATUS_SUBJECTS, PRESENCE_SUBJECTS],
  retention: RetentionPolicy.Limits,
  storage: StorageType.File,
  max_age: nanos(14 * DAY_MS),
  max_msgs_per_subject: 1,
  discard: DiscardPolicy.Old,
  discard_new_per_subject: false,
  duplicate_window: nanos(2 * 60 * 1000),
  allow_direct: true,
  max_msg_size: MAX_MESSAGE_BYTES,
  max_bytes: STATE_MAX_BYTES,
}

export const STREAMS = Object.freeze([
  TASKS_STREAM,
  RESULTS_STREAM,
  STATE_STREAM,
])
for (const stream of STREAMS) {
  Object.freeze(stream.subjects)
  Object.freeze(stream)
}

/**
 * Settings never changed in place. The server refuses storage and any move
 * to or from workqueue; other retention changes would alter what is kept.
 */
const IMMUTABLE = ['retention', 'storage'] as const

export interface StreamOutcome {
  name: string
  action: 'created' | 'unchanged' | 'updated'
  /** Settings that differed, for `updated`. */
  changed: string[]
  /** Stored messages around an `updated`: a drop is data a tighter limit trimmed. */
  messages?: { before: number; after: number }
}

const same = (a: unknown, b: unknown) =>
  JSON.stringify(Array.isArray(a) ? [...a].sort() : a) ===
  JSON.stringify(Array.isArray(b) ? [...b].sort() : b)

// The server omits booleans that are false.
const diff = (want: ManagedStream, have: StreamConfig) =>
  (Object.keys(want) as (keyof ManagedStream)[]).filter(
    (key) => !same(want[key], have[key] ?? false),
  )

/**
 * Creates each missing stream and updates drifted ones in place. A drift in a
 * setting that cannot be updated throws before anything is changed, so data
 * is never deleted to fix it.
 */
export async function bootstrapStreams(
  jsm: JetStreamManager,
  wanted: readonly ManagedStream[] = STREAMS,
): Promise<StreamOutcome[]> {
  const found = await Promise.all(
    wanted.map(async (want) => {
      try {
        return await jsm.streams.info(want.name)
      } catch (err) {
        if (
          err instanceof JetStreamApiError &&
          err.code === JetStreamApiCodes.StreamNotFound
        ) {
          return undefined
        }
        throw err
      }
    }),
  )

  const plan = wanted.map((want, i) => {
    const have = found[i]
    return { want, have, changed: have ? diff(want, have.config) : [] }
  })
  const stuck = plan.flatMap(({ want, changed }) =>
    changed
      .filter((key) => (IMMUTABLE as readonly string[]).includes(key))
      .map((key) => `${want.name}.${key}`),
  )
  if (stuck.length > 0) {
    throw new Error(
      `cannot update ${stuck.join(', ')} in place; nothing was changed. ` +
        'Drain the stream and delete it by hand if the data is expendable, then rerun.',
    )
  }

  const outcomes: StreamOutcome[] = []
  for (const { want, have, changed } of plan) {
    if (!have) {
      // A copy: the client writes keys onto the config `add` is given.
      await withStreamName(want.name, () => jsm.streams.add({ ...want }))
      outcomes.push({ name: want.name, action: 'created', changed: [] })
    } else if (changed.length > 0) {
      const { state } = await withStreamName(want.name, () =>
        jsm.streams.update(want.name, want),
      )
      outcomes.push({
        name: want.name,
        action: 'updated',
        changed,
        messages: { before: have.state.messages, after: state.messages },
      })
    } else {
      outcomes.push({ name: want.name, action: 'unchanged', changed: [] })
    }
  }
  return outcomes
}

/** Server errors such as insufficient storage do not say which stream failed. */
async function withStreamName<T>(
  name: string,
  apply: () => Promise<T>,
): Promise<T> {
  try {
    return await apply()
  } catch (err) {
    throw new Error(`${name}: ${err instanceof Error ? err.message : err}`)
  }
}

async function main(): Promise<void> {
  const config = adminConfig(process.env)
  const transport = resolveTransport(config)
  if ('refused' in transport) throw new Error(transport.refused)

  const nc = await connect({
    ...connectOptions(config, transport),
    // A one-shot command fails now instead of waiting for a server.
    waitOnFirstConnect: false,
    maxReconnectAttempts: 0,
  })
  try {
    const outcomes = await bootstrapStreams(await jetstreamManager(nc))
    console.log(`streams on ${transport.address}:`)
    for (const { name, action, changed, messages } of outcomes) {
      const settings = changed.length ? ` (${changed.join(', ')})` : ''
      const stored = messages
        ? `, messages ${messages.before} -> ${messages.after}`
        : ''
      console.log(`  ${name}: ${action}${settings}${stored}`)
    }
  } finally {
    await nc.drain()
  }
}

if (import.meta.main) {
  main().catch((err: unknown) => {
    console.error(
      `bootstrap failed: ${err instanceof Error ? err.message : err}`,
    )
    process.exit(1)
  })
}
