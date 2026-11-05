import { describe, expect, test } from 'bun:test'
import {
  buildMeta,
  type Envelope,
  encodeEnvelope,
  type FailureReason,
  mintTaskId,
  NUID_PATTERN,
  type ParsedMessage,
  type Presence,
  parseMessage,
  parseSubject,
  presenceSubject,
  type Result,
  resultSubject,
  type Sent,
  type Status,
  sanitizeContent,
  sentSubject,
  statusSubject,
  type Task,
  taskSubject,
} from '../src/protocol'

const ID = '0123456789abcdefghijAB' // 22 base62 characters
const ID2 = 'ZZZZZZZZZZZZZZZZZZZZZZ'
const NOW = '2026-01-02T03:04:05.000Z'
// biome-ignore lint/suspicious/noControlCharactersInRegex: asserting their absence
const CONTROL_CHARS = /[\x00-\x08\x0b-\x1f\x7f-\x9f]/
const enc = (v: unknown) => new TextEncoder().encode(JSON.stringify(v))

const task: Task = {
  v: 1,
  type: 'task',
  task_id: ID,
  from: 'orch',
  to: 'w1',
  title: 'title',
  prompt: 'do it',
  created_at: NOW,
  timeout_s: 3600,
  queue_ttl_s: 86400,
  max_attempts: 3,
  root_task_id: ID,
  depth: 0,
}
const result: Result = {
  v: 1,
  type: 'result',
  task_id: ID,
  worker: 'w1',
  instance: ID2,
  attempt: 1,
  outcome: 'succeeded',
  summary: 'done',
  data: { n: 1, nested: { a: [1, 2] } },
  artifacts: [
    { kind: 'branch', value: 'feat/x' },
    { kind: 'commit', value: 'abc1234' },
    { kind: 'pr', value: 'https://example.com/o/r/pull/1' },
    { kind: 'url', value: 'https://example.com/x' },
  ],
  finished_at: NOW,
}
const status: Status = {
  v: 1,
  type: 'status',
  task_id: ID,
  worker: 'w1',
  title: 'title',
  created_at: NOW,
  state: 'accepted',
  instance: ID2,
  attempt: 1,
  note: 'working',
  depth: 1,
  timeout_s: 60,
  queue_ttl_s: 60,
}
const sent: Sent = {
  v: 1,
  type: 'sent',
  task_id: ID,
  to: 'w1',
  title: 'title',
  created_at: NOW,
  root_task_id: ID2,
  parent_task_id: ID2,
  depth: 1,
  timeout_s: 60,
  queue_ttl_s: 60,
}
const presence: Presence = {
  v: 1,
  type: 'presence',
  worker: 'w1',
  instance: ID2,
  host: 'box',
  repo: 'natter',
  description: 'a worker',
  plugin_version: '0.0.1',
  state: 'idle',
  heartbeat_s: 30,
}

const subjectFor = (e: Envelope) => {
  switch (e.type) {
    case 'task':
      return taskSubject(e.to)
    case 'sent':
      return sentSubject(e.task_id)
    case 'result':
      return resultSubject(e.worker, e.task_id)
    case 'status':
      return statusSubject(e.worker, e.task_id)
    case 'presence':
      return presenceSubject(e.worker)
  }
}
const failure = (m: ParsedMessage) => {
  if (m.ok) throw new Error('expected failure')
  return m
}
const reasonOf = (subject: string, data: Uint8Array): FailureReason =>
  failure(parseMessage(subject, data)).reason

describe('task ids', () => {
  test('have the form of a nuid and do not count up', () => {
    const ids = Array.from({ length: 2000 }, mintTaskId)
    for (const id of ids) expect(id).toMatch(NUID_PATTERN)
    expect(new Set(ids).size).toBe(ids.length)
    // Consecutive nuids share their first twelve characters.
    expect(new Set(ids.map((id) => id.slice(0, 12))).size).toBe(ids.length)
    // The whole alphabet is in use, so no position is narrowed.
    expect(new Set(ids.map((id) => id[0])).size).toBe(62)
  })
})

describe('subjects', () => {
  test.each(['', 'A', 'a.b', 'a*', 'a>', '-a', 'a b', 'a'.repeat(65), 'a\n'])(
    'builders reject worker %p',
    (bad) => {
      expect(() => taskSubject(bad)).toThrow()
      expect(() => presenceSubject(bad)).toThrow()
      expect(() => resultSubject(bad, ID)).toThrow()
      expect(() => statusSubject(bad, ID)).toThrow()
    },
  )

  test.each(['', 'short', `${ID}x`, 'a.b', '*', '>', `${ID.slice(1)}.`])(
    'builders reject task_id %p',
    (bad) => {
      expect(() => resultSubject('w1', bad)).toThrow()
      expect(() => statusSubject('w1', bad)).toThrow()
      expect(() => sentSubject(bad)).toThrow()
    },
  )

  test('builders and parseSubject agree', () => {
    expect(parseSubject(taskSubject('w1'))).toEqual({
      kind: 'task',
      worker: 'w1',
    })
    expect(parseSubject(resultSubject('w1', ID))).toEqual({
      kind: 'result',
      worker: 'w1',
      taskId: ID,
    })
    expect(parseSubject(sentSubject(ID))).toEqual({ kind: 'sent', taskId: ID })
  })

  test.each([
    'natter.task',
    'natter.task.w1.extra',
    `natter.result.w1`,
    `natter.presence.w1.${ID}`,
    `natter.other.w1`,
    `other.task.w1`,
    'natter.task.W1',
    'natter.task.>',
    `natter.result.*.${ID}`,
    'natter.sent',
    'natter.sent.w1',
    'natter.sent.*',
    `natter.sent.${ID}.extra`,
    `natter.sent.w1.${ID}`,
  ])('parseSubject rejects %p', (s) => {
    expect(parseSubject(s)).toBeUndefined()
  })
})

describe('envelopes', () => {
  test.each([task, sent, result, status, presence])(
    'round-trips $type',
    (envelope) => {
      const encoded = encodeEnvelope(envelope)
      if (!encoded.ok) throw new Error(encoded.issues.join())
      const parsed = parseMessage(subjectFor(envelope), encoded.bytes)
      expect(parsed).toMatchObject({ ok: true, envelope })
    },
  )

  test('optional fields may be absent', () => {
    const { data, artifacts, ...bare } = result
    const parsed = parseMessage(
      resultSubject('w1', ID),
      enc({ ...bare, reason: 'timeout' }),
    )
    expect(parsed.ok && parsed.envelope).toEqual({ ...bare, reason: 'timeout' })
  })

  test('unknown keys are tolerated and dropped', () => {
    const parsed = parseMessage(
      taskSubject('w1'),
      enc({ ...task, future: { a: 1 } }),
    )
    expect(parsed).toMatchObject({ ok: true, envelope: task })
    expect(parsed.ok && 'future' in parsed.envelope).toBe(false)
  })

  test('encode rejects unknown type and invalid fields without echoing values', () => {
    expect(encodeEnvelope({ type: 'nope' }).ok).toBe(false)
    expect(encodeEnvelope(null).ok).toBe(false)
    const bad = encodeEnvelope({ ...task, title: '' })
    expect(bad.ok).toBe(false)
    const secret = encodeEnvelope({ ...task, to: 'SECRET VALUE' })
    expect(!secret.ok && secret.issues.join()).not.toContain('SECRET')
  })

  test('encode strips unknown keys', () => {
    const encoded = encodeEnvelope({ ...task, extra: 1 })
    expect(
      encoded.ok && JSON.parse(new TextDecoder().decode(encoded.bytes)),
    ).toEqual(task)
  })
})

describe('size limits in UTF-8 bytes', () => {
  // U+20AC is 1 UTF-16 unit but 3 bytes.
  const euro = (bytes: number) => '€'.repeat(bytes / 3)
  const ok = (e: unknown) => encodeEnvelope(e).ok

  test.each([
    ['title', (s: string) => ({ ...task, title: s }), 120],
    ['prompt', (s: string) => ({ ...task, prompt: s }), 32 * 1024 - 1],
    ['summary', (s: string) => ({ ...result, summary: s }), 4 * 1024 - 1],
    ['note', (s: string) => ({ ...status, note: s }), 512 - 2],
  ] as const)('%s counts bytes', (_name, make, limit) => {
    const n = Math.floor(limit / 3) * 3
    expect(ok(make(euro(n)))).toBe(true)
    // string length is far under the limit; byte length is over it
    const over = euro(n + 3)
    expect(over.length).toBeLessThanOrEqual(limit)
    expect(ok(make(over))).toBe(false)
    expect(
      parseMessage(subjectFor(make('x') as Envelope), enc(make(over))).ok,
    ).toBe(false)
  })

  test('exact boundaries for ASCII', () => {
    expect(ok({ ...task, title: 'a'.repeat(120) })).toBe(true)
    expect(ok({ ...task, title: 'a'.repeat(121) })).toBe(false)
    expect(ok({ ...result, summary: 'a'.repeat(4096) })).toBe(true)
    expect(ok({ ...result, summary: 'a'.repeat(4097) })).toBe(false)
    expect(ok({ ...status, note: 'a'.repeat(512) })).toBe(true)
    expect(ok({ ...status, note: 'a'.repeat(513) })).toBe(false)
  })

  test('data is limited by its JSON bytes', () => {
    // 21846 euros is 65538 bytes of JSON content, string length 21846
    expect(ok({ ...result, data: { s: '€'.repeat(21_000) } })).toBe(true)
    expect(ok({ ...result, data: { s: '€'.repeat(21_846) } })).toBe(false)
  })

  test('data must be an object', () => {
    for (const data of [[1], 'x', 1, null]) {
      expect(ok({ ...result, data })).toBe(false)
    }
  })

  test('artifact count and size', () => {
    const a = { kind: 'url', value: 'https://example.com/x' } as const
    expect(ok({ ...result, artifacts: Array(32).fill(a) })).toBe(true)
    expect(ok({ ...result, artifacts: Array(33).fill(a) })).toBe(false)
    const long = `https://example.com/${'€'.repeat(171)}` // > 512 bytes
    expect(long.length).toBeLessThan(512)
    expect(ok({ ...result, artifacts: [{ kind: 'url', value: long }] })).toBe(
      false,
    )
  })

  test('whole message over 128 KiB is rejected before decoding', () => {
    expect(reasonOf(taskSubject('w1'), new Uint8Array(128 * 1024 + 1))).toBe(
      'too_large',
    )
  })
})

describe('artifacts refer to branch, commit, PR or URL, never local paths', () => {
  const accepted = (kind: string, value: string) =>
    encodeEnvelope({ ...result, artifacts: [{ kind, value }] }).ok

  test.each([
    ['branch', 'main'],
    ['branch', 'feat/plugin-scaffold'],
    ['branch', 'release-1.2'],
    ['commit', 'abc1234'],
    ['commit', 'a'.repeat(40)],
    ['pr', 'https://github.com/o/r/pull/3'],
    ['url', 'http://example.com/a?b=c'],
  ])('accepts %s %p', (kind, value) => {
    expect(accepted(kind, value)).toBe(true)
  })

  test.each([
    ['branch', '/home/me/repo'],
    ['branch', '../repo'],
    ['branch', './repo'],
    ['branch', '~/repo'],
    ['branch', 'C:\\repo'],
    ['branch', 'a..b'],
    ['branch', 'a b'],
    ['branch', 'x/'],
    ['branch', 'x.lock'],
    ['branch', ''],
    ['commit', 'xyz'],
    ['commit', 'abc12'],
    ['commit', '/tmp/abcdef0'],
    ['pr', '#12'],
    ['pr', '/home/me/pr'],
    ['url', 'file:///etc/passwd'],
    ['url', '/etc/passwd'],
    ['url', 'ftp://example.com/x'],
    ['url', 'javascript:alert(1)'],
    ['url', 'https://user:pw@example.com/'],
    ['url', 'example.com/x'],
    ['other', 'https://example.com'],
  ])('rejects %s %p', (kind, value) => {
    expect(accepted(kind, value)).toBe(false)
  })
})

describe('versioning', () => {
  test.each([2, 0, -1, 1.5, 99])(
    'v=%p is rejected with task_id recoverable',
    (v) => {
      const failed = failure(
        parseMessage(
          taskSubject('w1'),
          enc({ ...task, v, title: 'new shape' }),
        ),
      )
      expect(failed.reason).toBe(v === 1.5 ? 'invalid' : 'unsupported_version')
      expect(failed.task_id).toBe(ID)
    },
  )

  test('future shape with unparseable fields still yields task_id', () => {
    const failed = failure(
      parseMessage(taskSubject('w1'), enc({ v: 2, task_id: ID, anything: [] })),
    )
    expect(failed).toMatchObject({ reason: 'unsupported_version', task_id: ID })
  })

  test('task_id is not recovered unless it is a nuid', () => {
    for (const task_id of ['a.b', 'x', 5, null, { a: 1 }, `${ID}>`]) {
      const failed = failure(
        parseMessage(taskSubject('w1'), enc({ v: 2, task_id })),
      )
      expect(failed.reason).toBe('unsupported_version')
      expect(failed.task_id).toBeUndefined()
    }
  })

  test('missing or non-integer v is invalid', () => {
    const { v, ...noV } = task
    expect(reasonOf(taskSubject('w1'), enc(noV))).toBe('invalid')
    expect(reasonOf(taskSubject('w1'), enc({ ...task, v: '1' }))).toBe(
      'invalid',
    )
  })
})

describe('subject binding', () => {
  test('task `to` must equal the subject worker', () => {
    expect(reasonOf(taskSubject('w2'), enc(task))).toBe('subject_mismatch')
    expect(reasonOf(sentSubject(ID2), enc(sent))).toBe('subject_mismatch')
    expect(reasonOf(sentSubject(ID), enc(result))).toBe('invalid')
  })

  test.each([
    ['result', result, resultSubject],
    ['status', status, statusSubject],
  ] as const)(
    '%s worker and task_id must equal the subject',
    (_t, e, build) => {
      expect(reasonOf(build('w2', ID), enc(e))).toBe('subject_mismatch')
      expect(reasonOf(build('w1', ID2), enc(e))).toBe('subject_mismatch')
    },
  )

  test('presence worker must equal the subject worker', () => {
    expect(reasonOf(presenceSubject('w2'), enc(presence))).toBe(
      'subject_mismatch',
    )
  })

  test('envelope type must fit the subject', () => {
    expect(reasonOf(taskSubject('w1'), enc(result))).toBe('invalid')
    expect(reasonOf(resultSubject('w1', ID), enc(status))).toBe('invalid')
  })

  test('a non-protocol subject is rejected', () => {
    expect(reasonOf('natter.task.w1.x', enc(task))).toBe('bad_subject')
  })
})

describe('malformed input never throws', () => {
  const bytes = (...b: number[]) => new Uint8Array(b)

  test.each([
    ['invalid utf-8', bytes(0x7b, 0xff, 0xfe, 0x7d), 'not_utf8'],
    ['truncated utf-8', bytes(0xe2, 0x82), 'not_utf8'],
    ['empty', bytes(), 'not_json'],
    ['garbage', new TextEncoder().encode('{nope'), 'not_json'],
    ['array', enc([task]), 'not_object'],
    ['null', enc(null), 'not_object'],
    ['string', enc('task'), 'not_object'],
    ['number', enc(5), 'not_object'],
    ['empty object', enc({}), 'invalid'],
  ] as const)('%s', (_name, data, reason) => {
    expect(reasonOf(taskSubject('w1'), data)).toBe(reason)
  })

  test('__proto__ keys are refused at any depth', () => {
    const text = (s: string) => new TextEncoder().encode(s)
    const top = `{"__proto__":{"polluted":1},${JSON.stringify(task).slice(1)}`
    // JSON.stringify drops a literal __proto__ key, so craft the text.
    const nested = JSON.stringify({
      ...result,
      data: { a: [{ k: 1 }] },
    }).replace('"k"', '"__proto__"')
    expect(reasonOf(taskSubject('w1'), text(top))).toBe('unsafe_content')
    expect(reasonOf(resultSubject('w1', ID), text(nested))).toBe(
      'unsafe_content',
    )
    expect(({} as Record<string, unknown>).polluted).toBeUndefined()
  })

  test('deep nesting is refused, whether the parser or the guard catches it', () => {
    const nest = (n: number) =>
      new TextEncoder().encode(`{"a":${'['.repeat(n)}${']'.repeat(n)}}`)
    expect(['not_json', 'unsafe_content']).toContain(
      reasonOf(taskSubject('w1'), nest(5000)),
    )
    expect(reasonOf(taskSubject('w1'), nest(100))).toBe('unsafe_content')
  })

  test('encode refuses nesting and cycles instead of throwing', () => {
    let deep: unknown = 1
    for (let i = 0; i < 100; i++) deep = { d: deep }
    expect(encodeEnvelope({ ...result, data: deep as object }).ok).toBe(false)
    const cyclic: Record<string, unknown> = {}
    cyclic.self = cyclic
    expect(() => encodeEnvelope({ ...result, data: cyclic })).not.toThrow()
    expect(encodeEnvelope({ ...result, data: cyclic }).ok).toBe(false)
  })
})

describe('sanitizeContent', () => {
  test('keeps newline, tab and printable text including non-ASCII', () => {
    expect(sanitizeContent('a\tb\nc €🙂')).toBe('a\tb\nc €🙂')
  })

  test('strips C0 controls, DEL and C1', () => {
    expect(
      sanitizeContent('a\x00b\x07c\x08d\re\x0bf\x0cg\x1fh\x7fi\x9bj'),
    ).toBe('abcdefghij')
  })

  test.each([
    ['CSI', 'a\x1b[31mred\x1b[0mb', 'aredb'],
    ['CSI with params', 'a\x1b[2;5Hb', 'ab'],
    ['OSC with BEL', 'a\x1b]0;title\x07b', 'ab'],
    ['OSC with ST', 'a\x1b]8;;http://x\x1b\\link\x1b]8;;\x1b\\b', 'alinkb'],
    ['DCS', 'a\x1bPdata\x1b\\b', 'ab'],
    ['two-byte', 'a\x1bcb', 'ab'],
    ['lone ESC', 'a\x1b', 'a'],
  ])('strips %s sequences', (_name, input, expected) => {
    expect(sanitizeContent(input)).toBe(expected)
  })

  test('output never contains ESC or forbidden controls', () => {
    const nasty = '\x1b[\x1b]\x1b\x1b[999\x00\x1b\x1b\\\x9d\x1bP'
    expect(sanitizeContent(nasty)).not.toMatch(CONTROL_CHARS)
  })
})

describe('buildMeta', () => {
  test('emits only listed keys, as strings', () => {
    expect(
      buildMeta({
        kind: 'result',
        task_id: ID,
        worker: 'w1',
        from: 'orch',
        attempt: 2,
        timeout_s: 60,
        outcome: 'failed',
        reason: 'timeout',
        data_bytes: 0,
      }),
    ).toEqual({
      kind: 'result',
      task_id: ID,
      worker: 'w1',
      from: 'orch',
      attempt: '2',
      timeout_s: '60',
      outcome: 'failed',
      reason: 'timeout',
      data_bytes: '0',
    })
  })

  test('omits undefined values', () => {
    expect(buildMeta({ kind: 'task', attempt: undefined })).toEqual({
      kind: 'task',
    })
  })

  test.each(['title', 'summary', 'prompt', 'path', 'note', 'cwd', '__proto__'])(
    'rejects free-text or unknown key %p',
    (key) => {
      const input = JSON.parse(`{"${key}":"x"}`)
      expect(() => buildMeta(input)).toThrow()
    },
  )

  test.each([
    { kind: 'file' },
    { task_id: '../../etc/passwd' },
    { task_id: 'a.b' },
    { worker: '/etc/passwd' },
    { worker: 'a\nb' },
    { from: 'Bad Name' },
    { outcome: 'ok' },
    { reason: 'because' },
    { attempt: 0 },
    { attempt: '1' },
    { timeout_s: -1 },
    { data_bytes: 1.5 },
    { data_bytes: '12' },
  ])('rejects invalid value %p', (input) => {
    expect(() => buildMeta(input as never)).toThrow()
  })
})

describe('fully populated envelopes', () => {
  const fullTask: Task = { ...task, parent_task_id: ID2, depth: 2 }
  const fullResult: Result = {
    ...result,
    reason: 'invalid_task',
    outcome: 'blocked',
  }
  const fullStatus: Status = { ...status, state: 'finished' }
  const fullPresence: Presence = { ...presence, task_id: ID, state: 'offline' }

  test.each([fullTask, fullResult, fullStatus, fullPresence])(
    'optional fields survive a round trip: $type',
    (envelope) => {
      const encoded = encodeEnvelope(envelope)
      if (!encoded.ok) throw new Error(encoded.issues.join())
      expect(JSON.parse(new TextDecoder().decode(encoded.bytes))).toEqual(
        envelope,
      )
      const parsed = parseMessage(subjectFor(envelope), encoded.bytes)
      expect(parsed.ok && parsed.envelope).toEqual(envelope)
    },
  )
})

describe('field validation', () => {
  const ok = (e: unknown) => encodeEnvelope(e).ok

  test('timeout_s maximum', () => {
    expect(ok({ ...task, timeout_s: 86400 })).toBe(true)
    expect(ok({ ...task, timeout_s: 86401 })).toBe(false)
    expect(ok({ ...status, timeout_s: 86400 })).toBe(true)
    expect(ok({ ...status, timeout_s: 86401 })).toBe(false)
  })

  test.each(['blocked', 'failed', 'succeeded'])('outcome %s', (outcome) => {
    expect(ok({ ...result, outcome })).toBe(true)
  })
  test('outcome and reason are closed sets', () => {
    expect(ok({ ...result, outcome: 'done' })).toBe(false)
    expect(ok({ ...result, reason: 'other' })).toBe(false)
  })

  test.each(['delivered', 'accepted', 'stalled', 'released', 'finished'])(
    'status state %s',
    (state) => {
      expect(ok({ ...status, state })).toBe(true)
    },
  )
  test('a record has the limits of the task it stands for', () => {
    expect(ok({ ...sent, title: 'x'.repeat(121) })).toBe(false)
    expect(ok({ ...sent, timeout_s: 86401 })).toBe(false)
    expect(ok({ ...sent, depth: -1 })).toBe(false)
    expect(ok({ ...sent, parent_task_id: 'nope' })).toBe(false)
    const { parent_task_id, ...root } = sent
    expect(ok(root)).toBe(true)
  })
  test('status and presence states are closed sets', () => {
    expect(ok({ ...status, state: 'done' })).toBe(false)
    expect(ok({ ...status, state: 'queued' })).toBe(false)
    expect(ok({ ...presence, state: 'finished' })).toBe(false)
  })

  test.each([
    '2026-01-02',
    'yesterday',
    '2026-01-02T03:04:05',
    '',
    'x'.repeat(40),
  ])('malformed timestamp %p', (bad) => {
    expect(ok({ ...task, created_at: bad })).toBe(false)
    expect(ok({ ...result, finished_at: bad })).toBe(false)
  })
  test('timestamp length is capped', () => {
    expect(ok({ ...task, created_at: '2026-01-02T03:04:05.123456789Z' })).toBe(
      true,
    )
    expect(
      ok({ ...task, created_at: `2026-01-02T03:04:05.${'1'.repeat(30)}Z` }),
    ).toBe(false)
  })

  test('empty summary, prompt and title are rejected', () => {
    expect(ok({ ...result, summary: '' })).toBe(false)
    expect(ok({ ...task, prompt: '' })).toBe(false)
    expect(ok({ ...status, title: '' })).toBe(false)
  })

  test.each([1.5, -1, '3', null, Number.NaN])('non-count number %p', (n) => {
    expect(ok({ ...task, depth: n })).toBe(false)
    expect(ok({ ...task, queue_ttl_s: n })).toBe(false)
    expect(ok({ ...task, max_attempts: n })).toBe(false)
    expect(ok({ ...presence, heartbeat_s: n })).toBe(false)
  })
  test('counts: zero depth ok, zero ttl/attempts/heartbeat not', () => {
    expect(ok({ ...task, depth: 0 })).toBe(true)
    expect(ok({ ...task, queue_ttl_s: 0 })).toBe(false)
    expect(ok({ ...task, max_attempts: 0 })).toBe(false)
    expect(ok({ ...presence, heartbeat_s: 0 })).toBe(false)
    expect(ok({ ...result, attempt: 0 })).toBe(false)
  })

  test('nuids must be exactly 22 base62 characters', () => {
    for (const bad of [ID.slice(1), `${ID}A`, `${ID.slice(1)}-`, '']) {
      expect(ok({ ...task, task_id: bad })).toBe(false)
      expect(ok({ ...task, root_task_id: bad })).toBe(false)
      expect(ok({ ...task, parent_task_id: bad })).toBe(false)
      expect(ok({ ...result, instance: bad })).toBe(false)
      expect(ok({ ...presence, task_id: bad })).toBe(false)
    }
  })

  test('presence text fields are limited in bytes', () => {
    const euro = (bytes: number) => '€'.repeat(bytes / 3)
    expect(ok({ ...presence, host: euro(255) })).toBe(true)
    expect(ok({ ...presence, host: euro(258) })).toBe(false)
    expect(ok({ ...presence, repo: euro(258) })).toBe(false)
    expect(ok({ ...presence, description: euro(510) })).toBe(true)
    expect(ok({ ...presence, description: euro(513) })).toBe(false)
    expect(ok({ ...presence, plugin_version: euro(63) })).toBe(true)
    expect(ok({ ...presence, plugin_version: euro(66) })).toBe(false)
  })

  test('a prompt within its field limit is rejected when JSON escaping overflows the total cap', () => {
    // Each control character escapes to 6 bytes, so 32 KiB of them serializes past 128 KiB.
    expect(
      encodeEnvelope({ ...task, prompt: '\u0001'.repeat(32 * 1024) }).ok,
    ).toBe(false)
  })

  test('the largest valid result stays under the total cap', () => {
    const encoded = encodeEnvelope({
      ...result,
      data: { s: 'a'.repeat(64 * 1024 - 8) },
      summary: 'a'.repeat(4096),
      artifacts: Array(32).fill({
        kind: 'url',
        value: `https://e.com/${'a'.repeat(490)}`,
      }),
    })
    expect(encoded.ok && encoded.bytes.length).toBeLessThan(128 * 1024)
  })
})

describe('hidden characters', () => {
  const accepted = (kind: string, value: string) =>
    encodeEnvelope({ ...result, artifacts: [{ kind, value }] }).ok
  const hidden = [
    '\x1b',
    '\n',
    '\t',
    ' ',
    ' ',
    '‮',
    '​',
    '‍',
    '⁠',
    '﻿',
    ' ',
    '⁦',
    '\u{e0041}',
  ]

  test.each(hidden)('url and pr reject %j anywhere', (ch) => {
    for (const kind of ['url', 'pr']) {
      expect(accepted(kind, `https://example.com/a${ch}b`)).toBe(false)
      expect(accepted(kind, `https://example.com/ab${ch}`)).toBe(false)
      expect(accepted(kind, `${ch}https://example.com/ab`)).toBe(false)
    }
  })
  test.each(hidden)('branch rejects %j', (ch) => {
    expect(accepted('branch', `feat${ch}x`)).toBe(false)
  })

  test('issue for a bad pr names the expected form, not the value', () => {
    const bad = encodeEnvelope({
      ...result,
      artifacts: [{ kind: 'pr', value: 'LEAKME' }],
    })
    const issues = !bad.ok ? bad.issues.join() : ''
    expect(issues).toContain('http(s) URL')
    expect(issues).not.toContain('LEAKME')
  })

  test.each(['feat/x#1', 'a+b', 'user@feature', 'fix/größe', 'ブランチ'])(
    'branch allows %p',
    (b) => {
      expect(accepted('branch', b)).toBe(true)
    },
  )
  test.each(['a@{1}', '#x', '+x', '@x', 'a:b', 'a?b', 'a*b'])(
    'branch still rejects %p',
    (b) => {
      expect(accepted('branch', b)).toBe(false)
    },
  )
})

describe('sanitizeContent invisible characters', () => {
  test.each([
    ['bidi override', 'a‮b‪c⁦d⁩e'],
    ['zero width', 'a​b‌c‍d⁠e﻿'],
    ['separators', 'a b c'],
    ['tag characters', 'a\u{e0001}\u{e0041}\u{e007f}b'],
  ])('strips %s', (_n, input) => {
    const out = sanitizeContent(input)
    expect(out).toMatch(/^[a-e]+$/)
  })

  test('keeps ordinary non-ASCII and single emoji', () => {
    const text = 'größe ブランチ café \u{1f642} ❤️  '
    expect(sanitizeContent(text)).toBe(text.replace('﻿', ''))
  })
})
