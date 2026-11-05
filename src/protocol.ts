import type { MsgHdrs } from '@nats-io/transport-node'
import { z } from 'zod'
import {
  FLEET_PATTERN,
  MAX_DESCRIPTION_BYTES,
  MAX_TIMEOUT_S,
  NAME_PATTERN,
} from './config'

// Pure wire protocol: subjects, envelopes, and what may reach Claude.
// Everything arriving from NATS is untrusted; see plan section 3.2 and 3.6.

export const PROTOCOL_VERSION = 1
export const MAX_MESSAGE_BYTES = 128 * 1024
export const PRESENCE_MAX_BYTES = 64 * 1024 * 1024
/** The orchestrator's durable on its fleet's results stream. */
export const RESULTS_DURABLE = 'results'
/** A worker's durable on its fleet's tasks stream. */
export const workerDurable = (name: string) => `task-${name}`
const MAX_JSON_DEPTH = 32
const MAX_ARTIFACTS = 32

/** A nuid: 22 base62 characters. Also a safe subject token. */
export const NUID_PATTERN = /^[0-9A-Za-z]{22}$/

const BASE62 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
// The largest multiple of 62 a byte can hold; rejecting above it keeps the
// characters uniform.
const BASE62_BYTES = 248
const TASK_ID_LENGTH = 22

/**
 * A nuid-shaped id from the system CSPRNG. A nuid's prefix is shared and its
 * tail counts up, so a few of them give away the next. Hardening only: the
 * record, not the secrecy of the id, is what makes a result trusted.
 */
export function mintTaskId(): string {
  let id = ''
  while (id.length < TASK_ID_LENGTH) {
    for (const byte of crypto.getRandomValues(new Uint8Array(32))) {
      if (byte < BASE62_BYTES && id.length < TASK_ID_LENGTH) {
        id += BASE62[byte % BASE62.length]
      }
    }
  }
  return id
}

export class WireError extends Error {}

/** The `Nats-Stream` values a direct get adds to every message it answers with. */
export const DIRECT_READ = 1

/**
 * True for a stored message that was the server's own answer to a direct get.
 * ACLs check a request's subject, not its reply, so a worker can aim a permitted
 * direct get of its own row at a subject it may not write, and a stream captures
 * the answer (plan 3.6). Such a message keeps the `Nats-Stream` the server set,
 * which no honest publish carries. Partial: it stops chosen content, not a fill.
 */
export const isReflected = (
  headers: MsgHdrs | undefined,
  addedByRead: 0 | typeof DIRECT_READ = 0,
) => (headers?.values('Nats-Stream').length ?? 0) > addedByRead

// ---------------------------------------------------------------- subjects

const nameToken = (value: string, what: string): string => {
  if (!NAME_PATTERN.test(value)) throw new WireError(`invalid ${what}`)
  return value
}
const nuidToken = (value: string, what: string): string => {
  if (!NUID_PATTERN.test(value)) throw new WireError(`invalid ${what}`)
  return value
}

const fleetToken = (value: string): string => {
  if (!FLEET_PATTERN.test(value)) throw new WireError('invalid fleet')
  return value
}

/**
 * Every stream name and subject of one fleet, built once per session. A fleet
 * is five streams and a subject prefix (plan 3.1), so a session that only
 * names its own scope cannot reach another fleet's records.
 */
export function fleetScope(fleet: string) {
  const prefix = `natter.${fleetToken(fleet)}`
  const stream = (suffix: string) => `NATTER_${fleet.toUpperCase()}_${suffix}`
  const streams = {
    tasks: stream('TASKS'),
    results: stream('RESULTS'),
    state: stream('STATE'),
    sent: stream('SENT'),
    presence: stream('PRESENCE'),
  }
  return {
    fleet,
    streams,
    /** Wildcard forms of the five subjects, for stream capture and ACLs. */
    subjects: {
      task: `${prefix}.task.*`,
      result: `${prefix}.result.*.*`,
      status: `${prefix}.status.*.*`,
      presence: `${prefix}.presence.*`,
      sent: `${prefix}.sent.*`,
    },
    taskSubject: (worker: string) =>
      `${prefix}.task.${nameToken(worker, 'worker')}`,
    presenceSubject: (worker: string) =>
      `${prefix}.presence.${nameToken(worker, 'worker')}`,
    resultSubject: (worker: string, taskId: string) =>
      `${prefix}.result.${nameToken(worker, 'worker')}.${nuidToken(taskId, 'task_id')}`,
    statusSubject: (worker: string, taskId: string) =>
      `${prefix}.status.${nameToken(worker, 'worker')}.${nuidToken(taskId, 'task_id')}`,
    sentSubject: (taskId: string) =>
      `${prefix}.sent.${nuidToken(taskId, 'task_id')}`,
  }
}
export type FleetScope = ReturnType<typeof fleetScope>

export type SubjectInfo =
  | { kind: 'task' | 'presence'; worker: string }
  | { kind: 'result' | 'status'; worker: string; taskId: string }
  | { kind: 'sent'; taskId: string }

/**
 * Returns undefined for any subject that is not exactly one of the five, in
 * a fleet whose name is valid. The fleet itself is not returned: the stream a
 * message came from already fixes it.
 */
export function parseSubject(subject: string): SubjectInfo | undefined {
  const [root, fleet, kind, worker, taskId, ...rest] = subject.split('.')
  if (root !== 'natter' || rest.length > 0) return undefined
  if (fleet === undefined || !FLEET_PATTERN.test(fleet)) return undefined
  if (kind === 'sent') {
    // The second token is the id here: a record belongs to no worker's subject.
    return worker !== undefined &&
      NUID_PATTERN.test(worker) &&
      taskId === undefined
      ? { kind, taskId: worker }
      : undefined
  }
  if (worker === undefined || !NAME_PATTERN.test(worker)) return undefined
  if (kind === 'task' || kind === 'presence') {
    return taskId === undefined ? { kind, worker } : undefined
  }
  if (kind === 'result' || kind === 'status') {
    return taskId !== undefined && NUID_PATTERN.test(taskId)
      ? { kind, worker, taskId }
      : undefined
  }
  return undefined
}

// ----------------------------------------------------------------- schemas

const OUTCOMES = ['succeeded', 'failed', 'blocked'] as const
const REASONS = [
  'timeout',
  'attempts_exhausted',
  'expired_in_queue',
  'invalid_task',
] as const
/** Why a task needs a look (plan 3.5). In `meta` only, never on the wire. */
export const ATTENTION_REASONS = [
  'stale',
  'released',
  'overdue',
  'waiting_permission',
  'session_error',
] as const
export type AttentionReason = (typeof ATTENTION_REASONS)[number]

const utf8Bytes = (s: string) => Buffer.byteLength(s, 'utf8')
const bounded = (max: number, min = 0) =>
  z
    .string()
    .refine((s) => utf8Bytes(s) <= max, `at most ${max} UTF-8 bytes`)
    .refine((s) => utf8Bytes(s) >= min, `at least ${min} UTF-8 bytes`)

const name = z.string().regex(NAME_PATTERN)
const nuid = z.string().regex(NUID_PATTERN)
// z.iso.datetime() accepts unbounded fractional digits, so cap the length.
const timestamp = z.iso.datetime().max(35)
const count = z.int().nonnegative()
const positive = z.int().positive()

// Unserializable data (cycles, BigInt) counts as oversize rather than throwing.
const jsonBytes = (value: unknown) => {
  try {
    return utf8Bytes(JSON.stringify(value))
  } catch {
    return Number.POSITIVE_INFINITY
  }
}

const jsonObject = z
  .record(z.string(), z.unknown())
  .refine((o) => jsonBytes(o) <= 64 * 1024, 'at most 64 KiB')

// Whitespace, controls and invisible characters (bidi, zero-width, tags) hide
// or reorder text for a human reader; none belong in a reference.
const HIDDEN_CHARS =
  // biome-ignore lint/suspicious/noControlCharactersInRegex: matching them is the point
  /[\s\x00-\x1f\x7f-\x9f\u200b-\u200d\u2028\u2029\u202a-\u202e\u2060\u2066-\u2069\ufeff\u{e0000}-\u{e007f}]/u

const HEX_SHA = /^[0-9a-f]{7,64}$/i
// Conservative git refname subset; its letter and digit classes exclude
// whitespace, controls and invisible characters. Relative paths such as `src/foo` look like
// branch names and cannot be told apart; only absolute, home-relative, dotted
// and drive-letter paths are excluded here.
const BRANCH = /^[\p{L}\p{N}][\p{L}\p{N}._/#+@-]*$/u

function isHttpUrl(value: string): boolean {
  if (HIDDEN_CHARS.test(value)) return false
  try {
    const url = new URL(value)
    return (
      (url.protocol === 'https:' || url.protocol === 'http:') &&
      url.hostname !== '' &&
      url.username === '' &&
      url.password === ''
    )
  } catch {
    return false
  }
}

function isBranch(value: string): boolean {
  return (
    BRANCH.test(value) &&
    !value.includes('..') &&
    !value.includes('//') &&
    !value.includes('@{') &&
    !value.endsWith('/') &&
    !value.endsWith('.') &&
    !value.endsWith('.lock')
  )
}

const artifactValidators = {
  branch: { ok: isBranch, expected: 'a git branch name' },
  commit: { ok: (v: string) => HEX_SHA.test(v), expected: 'a hex commit id' },
  pr: { ok: isHttpUrl, expected: 'an http(s) URL' },
  url: { ok: isHttpUrl, expected: 'an http(s) URL' },
} as const

const artifact = z
  .object({
    kind: z.enum(['branch', 'commit', 'pr', 'url']),
    value: bounded(512, 1),
  })
  .superRefine((a, ctx) => {
    const rule = artifactValidators[a.kind]
    if (!rule.ok(a.value)) {
      ctx.addIssue({
        code: 'custom',
        message: `${a.kind} value must be ${rule.expected}`,
        path: ['value'],
      })
    }
  })

export const TaskSchema = z.object({
  v: z.literal(PROTOCOL_VERSION),
  type: z.literal('task'),
  task_id: nuid,
  from: name,
  to: name,
  title: bounded(120, 1),
  prompt: bounded(32 * 1024, 1),
  created_at: timestamp,
  timeout_s: positive.max(MAX_TIMEOUT_S),
  queue_ttl_s: positive,
  max_attempts: positive,
  root_task_id: nuid,
  parent_task_id: nuid.optional(),
  depth: count,
})

// The orchestrator's own record of a send. Only its credential can publish on
// this subject, so it is what every result is checked against (plan 3.6). A
// reflected message can still land here, so readers refuse those.
export const SentSchema = TaskSchema.pick({
  v: true,
  task_id: true,
  to: true,
  title: true,
  created_at: true,
  root_task_id: true,
  parent_task_id: true,
  depth: true,
  timeout_s: true,
  queue_ttl_s: true,
}).extend({ type: z.literal('sent') })

export const ResultSchema = z.object({
  v: z.literal(PROTOCOL_VERSION),
  type: z.literal('result'),
  task_id: nuid,
  worker: name,
  instance: nuid,
  attempt: positive,
  outcome: z.enum(OUTCOMES),
  reason: z.enum(REASONS).optional(),
  summary: bounded(4 * 1024, 1),
  data: jsonObject.optional(),
  artifacts: z.array(artifact).max(MAX_ARTIFACTS).optional(),
  finished_at: timestamp,
})

export const StatusSchema = z.object({
  v: z.literal(PROTOCOL_VERSION),
  type: z.literal('status'),
  task_id: nuid,
  worker: name,
  title: bounded(120, 1),
  created_at: timestamp,
  state: z.enum([
    'delivered',
    'accepted',
    'waiting_permission',
    'session_error',
    'stalled',
    'released',
    'finished',
  ]),
  instance: nuid.optional(),
  attempt: positive.optional(),
  note: bounded(512).optional(),
  depth: count,
  timeout_s: positive.max(MAX_TIMEOUT_S),
  queue_ttl_s: positive,
})

// host, repo and plugin_version are unbounded in the plan; capped here.
export const PresenceSchema = z.object({
  v: z.literal(PROTOCOL_VERSION),
  type: z.literal('presence'),
  worker: name,
  instance: nuid,
  host: bounded(255),
  repo: bounded(255),
  description: bounded(MAX_DESCRIPTION_BYTES),
  plugin_version: bounded(64),
  state: z.enum(['idle', 'busy', 'stalled', 'offline']),
  task_id: nuid.optional(),
  heartbeat_s: positive,
})

export type Task = z.infer<typeof TaskSchema>
export type Sent = z.infer<typeof SentSchema>
export type Result = z.infer<typeof ResultSchema>
export type Status = z.infer<typeof StatusSchema>

/** What a status row needs from the task it describes; also the hook's file. */
export const HeldSchema = StatusSchema.pick({
  task_id: true,
  title: true,
  created_at: true,
  depth: true,
  timeout_s: true,
  queue_ttl_s: true,
}).extend({ instance: nuid, attempt: positive })
export type Held = z.infer<typeof HeldSchema>
export type Presence = z.infer<typeof PresenceSchema>
export type Envelope = Task | Sent | Result | Status | Presence
export type EnvelopeType = Envelope['type']

const schemas = {
  task: TaskSchema,
  sent: SentSchema,
  result: ResultSchema,
  status: StatusSchema,
  presence: PresenceSchema,
} as const

// ---------------------------------------------------------- encode / parse

/** True for `__proto__` keys or nesting deeper than the limit. */
function isUnsafeJson(value: unknown, depth = 0): boolean {
  if (typeof value !== 'object' || value === null) return false
  if (depth > MAX_JSON_DEPTH) return true
  if (Array.isArray(value)) return value.some((v) => isUnsafeJson(v, depth + 1))
  return Object.keys(value).some(
    (k) =>
      k === '__proto__' ||
      isUnsafeJson((value as Record<string, unknown>)[k], depth + 1),
  )
}

const describe = (error: z.ZodError) =>
  error.issues.map((i) => `${i.path.join('.') || '(root)'}: ${i.message}`)

export type Encoded =
  | { ok: true; bytes: Uint8Array; envelope: Envelope }
  | { ok: false; issues: string[] }

/**
 * Validates an outbound envelope and serializes it. Failure `issues` name
 * fields and limits, never values, so a tool can tell Claude what to shorten.
 */
export function encodeEnvelope(input: unknown): Encoded {
  const type = (input as { type?: unknown } | null)?.type
  if (typeof type !== 'string' || !Object.hasOwn(schemas, type)) {
    return { ok: false, issues: ['type: unknown envelope type'] }
  }
  const parsed = schemas[type as EnvelopeType].safeParse(input)
  if (!parsed.success) return { ok: false, issues: describe(parsed.error) }
  if (isUnsafeJson(parsed.data)) {
    return { ok: false, issues: ['(root): unsafe key or nesting too deep'] }
  }
  const bytes = new TextEncoder().encode(JSON.stringify(parsed.data))
  if (bytes.length > MAX_MESSAGE_BYTES) {
    return { ok: false, issues: [`(root): at most ${MAX_MESSAGE_BYTES} bytes`] }
  }
  return { ok: true, bytes, envelope: parsed.data }
}

/** A status row for a held task; throws WireError when it does not encode. */
export function encodeStatus(
  worker: string,
  held: Held,
  state: Status['state'],
  note?: unknown,
): Uint8Array {
  const encoded = encodeEnvelope({
    ...held,
    v: PROTOCOL_VERSION,
    type: 'status',
    worker,
    state,
    note,
  })
  if (!encoded.ok) throw new WireError(encoded.issues.join('; '))
  return encoded.bytes
}

export type FailureReason =
  | 'bad_subject'
  | 'too_large'
  | 'not_utf8'
  | 'not_json'
  | 'not_object'
  | 'unsafe_content'
  | 'unsupported_version'
  | 'invalid'
  | 'subject_mismatch'

export type ParsedMessage =
  | { ok: true; subject: SubjectInfo; envelope: Envelope }
  | {
      ok: false
      reason: FailureReason
      /**
       * Set only when the body is an object whose `task_id` is a valid nuid,
       * and, for a task, whose `to` does not name another worker.
       */
      task_id?: string
      issues?: string[]
    }

const fail = (
  reason: FailureReason,
  extra: { task_id?: string; issues?: string[] } = {},
): ParsedMessage => ({ ok: false, reason, ...extra })

function mismatches(subject: SubjectInfo, e: Envelope): boolean {
  if (e.type === 'task')
    return subject.kind !== 'task' || e.to !== subject.worker
  if (e.type === 'sent')
    return subject.kind !== 'sent' || e.task_id !== subject.taskId
  if (subject.kind === 'sent') return true
  if (e.type !== subject.kind || e.worker !== subject.worker) return true
  return 'taskId' in subject && 'task_id' in e && e.task_id !== subject.taskId
}

/**
 * Parses and validates a received message. Never throws: every malformed
 * input becomes a typed failure. A receiver drops any message that fails.
 */
export function parseMessage(subject: string, data: Uint8Array): ParsedMessage {
  const info = parseSubject(subject)
  if (!info) return fail('bad_subject')
  if (data.length > MAX_MESSAGE_BYTES) return fail('too_large')

  let text: string
  try {
    text = new TextDecoder('utf-8', { fatal: true }).decode(data)
  } catch {
    return fail('not_utf8')
  }

  let body: unknown
  try {
    body = JSON.parse(text)
  } catch {
    return fail('not_json')
  }
  if (typeof body !== 'object' || body === null || Array.isArray(body)) {
    return fail('not_object')
  }
  if (isUnsafeJson(body)) return fail('unsafe_content')

  const raw = body as Record<string, unknown>
  // A recovered id gets a failure recorded under this subject's worker, so an
  // envelope addressed to someone else must not offer one.
  const misaddressed =
    info.kind === 'task' && raw.to !== undefined && raw.to !== info.worker
  const task_id =
    typeof raw.task_id === 'string' &&
    NUID_PATTERN.test(raw.task_id) &&
    !misaddressed
      ? raw.task_id
      : undefined

  if (typeof raw.v !== 'number' || !Number.isInteger(raw.v)) {
    return fail('invalid', { task_id, issues: ['v: expected an integer'] })
  }
  if (raw.v !== PROTOCOL_VERSION)
    return fail('unsupported_version', { task_id })

  const parsed = schemas[info.kind].safeParse(raw)
  if (!parsed.success) {
    return fail('invalid', { task_id, issues: describe(parsed.error) })
  }
  if (mismatches(info, parsed.data)) return fail('subject_mismatch')
  return { ok: true, subject: info, envelope: parsed.data }
}

// -------------------------------------------------------- content and meta

// OSC, DCS, SOS, PM and APC strings run to BEL or ST; CSI has its own form;
// anything else after ESC is a two-byte sequence.
const ESC_SEQUENCE =
  // biome-ignore lint/suspicious/noControlCharactersInRegex: matching them is the point
  /\x1b(?:[\]PX^_][^\x07\x1b]*(?:\x07|\x1b\\)?|\[[0-?]*[ -/]*[@-~]?|[ -/]*[0-~]?)/g
// C0 except \t and \n, DEL, and C1 (0x9b is a one-byte CSI on some terminals).
// biome-ignore lint/suspicious/noControlCharactersInRegex: matching them is the point
const CONTROLS = /[\x00-\x08\x0b-\x1f\x7f-\x9f]/g

// Invisible characters that hide injected text from a human reader: bidi
// controls, zero-width characters (U+200D too, so ZWJ emoji sequences split
// into their parts), line and paragraph separators, BOM, and tag characters.
const INVISIBLE =
  /[\u200b-\u200d\u2028\u2029\u202a-\u202e\u2060\u2066-\u2069\ufeff\u{e0000}-\u{e007f}]/gu

/**
 * Strips escape sequences, control characters and invisible characters;
 * keeps `\n`, `\t` and ordinary non-ASCII text.
 */
export function sanitizeContent(text: string): string {
  return text
    .replace(ESC_SEQUENCE, '')
    .replace(CONTROLS, '')
    .replace(INVISIBLE, '')
}

/**
 * `sanitizeContent` for every string in a parsed JSON value, keys included.
 * Keys that become equal collapse, last one wins. `parseMessage` bounds the
 * depth, and `fromEntries` keeps a key that sanitizes to `__proto__` inert.
 */
export function sanitizeJson(value: unknown): unknown {
  if (typeof value === 'string') return sanitizeContent(value)
  if (Array.isArray(value)) return value.map(sanitizeJson)
  if (typeof value !== 'object' || value === null) return value
  return Object.fromEntries(
    Object.entries(value).map(([k, v]) => [
      sanitizeContent(k),
      sanitizeJson(v),
    ]),
  )
}

const META_KINDS = ['task', 'result', 'task_closed', 'attention'] as const

const metaValidators = {
  kind: z.enum(META_KINDS),
  task_id: nuid,
  from: name,
  worker: name,
  attempt: positive,
  timeout_s: positive.max(MAX_TIMEOUT_S),
  outcome: z.enum(OUTCOMES),
  reason: z.enum([...REASONS, ...ATTENTION_REASONS]),
  data_bytes: count,
}

export type MetaInput = {
  [K in keyof typeof metaValidators]?: z.input<(typeof metaValidators)[K]>
}

/**
 * Builds channel `meta`: only the identifier keys of plan section 3.6, each
 * validated, rendered as strings. Throws on an unknown key or a bad value,
 * so free text cannot reach `meta`; callers pass already-validated fields.
 */
export function buildMeta(input: MetaInput): Record<string, string> {
  const meta: Record<string, string> = {}
  for (const key of Object.keys(input)) {
    if (!Object.hasOwn(metaValidators, key)) {
      throw new WireError(`meta key not allowed: ${key}`)
    }
    const value = (input as Record<string, unknown>)[key]
    if (value === undefined) continue
    const checked =
      metaValidators[key as keyof typeof metaValidators].safeParse(value)
    if (!checked.success) throw new WireError(`invalid meta value: ${key}`)
    meta[key] = String(checked.data)
  }
  return meta
}
