import { z } from 'zod'
import { MAX_DESCRIPTION_BYTES, MAX_TIMEOUT_S, NAME_PATTERN } from './config'

// Pure wire protocol: subjects, envelopes, and what may reach Claude.
// Everything arriving from NATS is untrusted; see plan section 3.2 and 3.6.

export const PROTOCOL_VERSION = 1
export const MAX_MESSAGE_BYTES = 128 * 1024
const MAX_JSON_DEPTH = 32
const MAX_ARTIFACTS = 32

/** A nuid: 22 base62 characters. Also a safe subject token. */
export const NUID_PATTERN = /^[0-9A-Za-z]{22}$/

export class WireError extends Error {}

// ---------------------------------------------------------------- subjects

const nameToken = (value: string, what: string): string => {
  if (!NAME_PATTERN.test(value)) throw new WireError(`invalid ${what}`)
  return value
}
const nuidToken = (value: string, what: string): string => {
  if (!NUID_PATTERN.test(value)) throw new WireError(`invalid ${what}`)
  return value
}

export const taskSubject = (worker: string) =>
  `natter.task.${nameToken(worker, 'worker')}`
export const presenceSubject = (worker: string) =>
  `natter.presence.${nameToken(worker, 'worker')}`
export const resultSubject = (worker: string, taskId: string) =>
  `natter.result.${nameToken(worker, 'worker')}.${nuidToken(taskId, 'task_id')}`
export const statusSubject = (worker: string, taskId: string) =>
  `natter.status.${nameToken(worker, 'worker')}.${nuidToken(taskId, 'task_id')}`

export type SubjectInfo =
  | { kind: 'task' | 'presence'; worker: string }
  | { kind: 'result' | 'status'; worker: string; taskId: string }

/** Returns undefined for any subject that is not exactly one of the four. */
export function parseSubject(subject: string): SubjectInfo | undefined {
  const [root, kind, worker, taskId, ...rest] = subject.split('.')
  if (root !== 'natter' || rest.length > 0) return undefined
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
    'queued',
    'delivered',
    'accepted',
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
export type Result = z.infer<typeof ResultSchema>
export type Status = z.infer<typeof StatusSchema>
export type Presence = z.infer<typeof PresenceSchema>
export type Envelope = Task | Result | Status | Presence
export type EnvelopeType = Envelope['type']

const schemas = {
  task: TaskSchema,
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
      /** Set only when the body is an object whose `task_id` is a valid nuid. */
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
  const task_id =
    typeof raw.task_id === 'string' && NUID_PATTERN.test(raw.task_id)
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

const META_KINDS = ['task', 'result', 'task_closed', 'attention'] as const

const metaValidators = {
  kind: z.enum(META_KINDS),
  task_id: nuid,
  from: name,
  worker: name,
  attempt: positive,
  timeout_s: positive.max(MAX_TIMEOUT_S),
  outcome: z.enum(OUTCOMES),
  reason: z.enum(REASONS),
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
