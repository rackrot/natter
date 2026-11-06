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
import { adminConfig, RESULTS_MAX_AGE_MS, resolveTransport } from './config'
import { connectOptions } from './connection'
import {
  type FleetScope,
  fleetScope,
  MAX_MESSAGE_BYTES,
  PRESENCE_MAX_BYTES,
} from './protocol'

// The stream definitions, and the admin command that creates or reconciles a
// fleet's five streams or deletes them (plan 3.3). Sessions only reach this
// file for `provisionStreams` and the definitions. Workers never create streams.

const DAY_MS = 24 * 60 * 60 * 1000
const MIB = 1024 * 1024
const TASKS_MAX_BYTES = 256 * MIB
const RESULTS_MAX_BYTES = 1024 * MIB
const STATE_MAX_BYTES = 256 * MIB
const SENT_MAX_BYTES = 256 * MIB
const PRESENCE_MAX_MSG_BYTES = 4 * 1024

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

const tasksStream = (scope: FleetScope): ManagedStream => ({
  name: scope.streams.tasks,
  subjects: [scope.subjects.task],
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
})

// First-result-wins is enforced here: a recorded result refuses every later
// message on its subject until max_age, however it is published, because ACLs
// cannot require the lastSubjectSequence: 0 header. A full stream rejects new
// results rather than evicting recorded ones.
const resultsStream = (scope: FleetScope): ManagedStream => ({
  name: scope.streams.results,
  subjects: [scope.subjects.result],
  retention: RetentionPolicy.Limits,
  storage: StorageType.File,
  max_age: nanos(RESULTS_MAX_AGE_MS),
  max_msgs_per_subject: 1,
  discard: DiscardPolicy.New,
  discard_new_per_subject: true,
  duplicate_window: nanos(2 * 60 * 1000),
  allow_direct: true,
  max_msg_size: MAX_MESSAGE_BYTES,
  max_bytes: RESULTS_MAX_BYTES,
})

// Status is a core publish the stream captures, `delivered` a JetStream one;
// the newest replaces the previous one.
const stateStream = (scope: FleetScope): ManagedStream => ({
  name: scope.streams.state,
  subjects: [scope.subjects.status],
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
})

// Heartbeats, apart from status rows so a flood of those cannot evict one. A
// worker's ACL allows publishing only its own subject and each keeps one
// message, so no worker can push out another's with a publish. The reply route
// of plan 3.6 is the open exception. Comes after the state stream in `streamsFor`: a server
// with presence still on that stream has to give the subject up first.
const presenceStream = (scope: FleetScope): ManagedStream => ({
  name: scope.streams.presence,
  subjects: [scope.subjects.presence],
  retention: RetentionPolicy.Limits,
  storage: StorageType.File,
  max_age: nanos(14 * DAY_MS),
  max_msgs_per_subject: 1,
  discard: DiscardPolicy.Old,
  discard_new_per_subject: false,
  duplicate_window: nanos(2 * 60 * 1000),
  allow_direct: true,
  max_msg_size: PRESENCE_MAX_MSG_BYTES,
  max_bytes: PRESENCE_MAX_BYTES,
})

// The orchestrator's record of each send, what every result is checked against
// (plan 3.6). Only the orchestrator can publish on it, so nothing is evicted to
// make room, and it outlives the results stream. Never drop it: every result still
// in the durable would then be terminated for having no record.
const sentStream = (scope: FleetScope): ManagedStream => ({
  name: scope.streams.sent,
  subjects: [scope.subjects.sent],
  retention: RetentionPolicy.Limits,
  storage: StorageType.File,
  max_age: nanos(30 * DAY_MS),
  max_msgs_per_subject: 1,
  discard: DiscardPolicy.New,
  discard_new_per_subject: true,
  duplicate_window: nanos(2 * 60 * 1000),
  allow_direct: true,
  max_msg_size: MAX_MESSAGE_BYTES,
  max_bytes: SENT_MAX_BYTES,
})

/** The five streams of one fleet, in the order a server needs them. */
export const streamsFor = (fleet: string): readonly ManagedStream[] => {
  const scope = fleetScope(fleet)
  return Object.freeze(
    [tasksStream, resultsStream, stateStream, presenceStream, sentStream].map(
      (build) => {
        const stream = build(scope)
        Object.freeze(stream.subjects)
        return Object.freeze(stream)
      },
    ),
  )
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

const isStreamNotFound = (err: unknown) =>
  err instanceof JetStreamApiError &&
  err.code === JetStreamApiCodes.StreamNotFound

/** Each stream as it is now, undefined only if the server says it does not exist. */
const lookup = (jsm: JetStreamManager, wanted: readonly ManagedStream[]) =>
  Promise.all(
    wanted.map((want) =>
      jsm.streams.info(want.name).catch((err: unknown) => {
        if (isStreamNotFound(err)) return undefined
        throw err
      }),
    ),
  )

// A copy: the client writes keys onto the config `add` is given.
const create = (jsm: JetStreamManager, want: ManagedStream) =>
  withStreamName(want.name, () => jsm.streams.add({ ...want }))

const same = (a: unknown, b: unknown) =>
  JSON.stringify(Array.isArray(a) ? [...a].sort() : a) ===
  JSON.stringify(Array.isArray(b) ? [...b].sort() : b)

/** The settings of `want` that `have` does not match. The server omits booleans that are false. */
export const diff = (want: ManagedStream, have: StreamConfig) =>
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
  wanted: readonly ManagedStream[],
): Promise<StreamOutcome[]> {
  const found = await lookup(jsm, wanted)

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
      await create(jsm, want)
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

/**
 * All a session may do to streams (plan 3.2): create the set when none of it
 * exists. A partial set is left alone, because a lost SENT recreated beside live
 * results gets every stored result terminated, and nothing is ever updated.
 *
 * A failure partway throws and deletes nothing: what it made is a partial set,
 * for bootstrap to finish. Two creators can race: `add` with identical settings
 * returns the existing stream untouched, and with other settings fails 10058.
 */
export async function provisionStreams(
  jsm: JetStreamManager,
  wanted: readonly ManagedStream[],
): Promise<'created' | 'complete' | 'partial'> {
  const present = (await lookup(jsm, wanted)).filter(Boolean).length
  if (present === wanted.length) return 'complete'
  if (present > 0) return 'partial'
  for (const want of wanted) await create(jsm, want)
  return 'created'
}

/**
 * Deletes streams and everything in them. In order, so SENT outlives RESULTS if
 * this stops partway, and a stream already gone lets a rerun finish the rest.
 */
export async function deleteStreams(
  jsm: JetStreamManager,
  wanted: readonly ManagedStream[],
): Promise<{ name: string; deleted: boolean }[]> {
  const outcomes = []
  for (const { name } of wanted) {
    const deleted = await withStreamName(name, () =>
      jsm.streams.delete(name).catch((err: unknown) => {
        if (isStreamNotFound(err)) return false
        throw err
      }),
    )
    outcomes.push({ name, deleted })
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

async function main(deleting: boolean): Promise<void> {
  // The raw variable: unset resolves to `default`, whose streams must not go
  // because one was forgotten.
  if (deleting && !process.env.NATTER_FLEET) {
    throw new Error('NATTER_FLEET must name the fleet to delete')
  }
  const config = adminConfig(process.env)
  const transport = resolveTransport(config)
  if ('refused' in transport) throw new Error(transport.refused)
  if (transport.seedWarning) console.error(transport.seedWarning)

  const nc = await connect({
    ...connectOptions(config, transport),
    // A one-shot command fails now instead of waiting for a server.
    waitOnFirstConnect: false,
    maxReconnectAttempts: 0,
  })
  try {
    const jsm = await jetstreamManager(nc)
    const wanted = streamsFor(config.fleet)
    console.log(`streams on ${transport.address}:`)
    if (deleting) {
      for (const { name, deleted } of await deleteStreams(jsm, wanted)) {
        console.log(`  ${name}: ${deleted ? 'deleted' : 'already gone'}`)
      }
      return
    }
    const outcomes = await bootstrapStreams(jsm, wanted)
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
  const deleting = process.argv[2] === 'delete'
  main(deleting).catch((err: unknown) => {
    console.error(
      `${deleting ? 'fleet-delete' : 'bootstrap'} failed: ${err instanceof Error ? err.message : err}`,
    )
    process.exit(1)
  })
}
