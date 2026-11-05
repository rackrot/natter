import { setTimeout as delay } from 'node:timers/promises'
import {
  AckPolicy,
  type ConsumerMessages,
  JetStreamApiCodes,
  JetStreamApiError,
  type JetStreamManager,
  type JsMsg,
  jetstream,
  jetstreamManager,
} from '@nats-io/jetstream'
import {
  type NatsConnection,
  nanos,
  nuid,
  RequestError,
} from '@nats-io/transport-node'
import { z } from 'zod'
import { type Config, type Env, resolveTimers } from './config'
import type { Connection } from './connection'
import {
  buildMeta,
  encodeEnvelope,
  type ParsedMessage,
  PROTOCOL_VERSION,
  parseMessage,
  RESULTS_STREAM_NAME,
  type Result,
  ResultSchema,
  resultSubject,
  STATE_STREAM_NAME,
  type Status,
  StatusSchema,
  sanitizeContent,
  statusSubject,
  TASKS_STREAM_NAME,
  type Task,
  taskSubject,
  WireError,
} from './protocol'

// The worker side of plan 3.3: pull one task at a time, hold it with
// working(), and ack it only once its result is durably stored.

// The client has no name for this code.
const CONSUMER_ALREADY_EXISTS = 10148

export const ALREADY_RECORDED =
  'already recorded: another session finished this task first, so this result was not stored'

export const ALREADY_CLOSED =
  'already closed: the run timeout ended this task, so this call stored nothing. Stop working on it'

export const RELEASED =
  'released: this was not accepted in time, so it will be redelivered and this was not stored'

export const WORKER_TOOLS = [
  {
    name: 'update_task',
    description:
      'Call right away when a task arrives, then with a short note at milestones. The first call marks the task accepted. The note is at most 512 bytes and is not pushed to the orchestrator.',
    inputSchema: z.toJSONSchema(
      StatusSchema.pick({ task_id: true, note: true }),
    ),
  },
  {
    name: 'finish_task',
    description:
      'Records the result of a task and takes the next one. Call exactly once, when done. summary is at most 4 KiB, data at most 64 KiB as JSON, artifacts at most 32 entries of 512 bytes. Artifacts are branches, commits, PRs or URLs, never local paths.',
    inputSchema: z.toJSONSchema(
      ResultSchema.pick({
        task_id: true,
        outcome: true,
        summary: true,
        data: true,
        artifacts: true,
      }),
    ),
  },
]

interface InFlight {
  task: Task
  attempt: number
  /** Swapped when the broker redelivers this task to this process. */
  msg: JsMsg
  working: ReturnType<typeof setInterval>
  /** The accept timer until the first tool call, then the run timer. */
  deadline: ReturnType<typeof setTimeout>
  accepted: boolean
  /** finish_task or the run timeout is writing the result. Never both. */
  finishing: boolean
}

/**
 * What holds a task's result subject after a store: this call's write, an
 * earlier write of this session (its finish_task, or a result the plugin
 * decided on), or anything else, which is taken for another session's.
 */
type Stored = 'written' | 'own_finish' | 'own_failure' | 'other'

export interface ToolResult {
  [key: string]: unknown
  content: { type: 'text'; text: string }[]
  isError?: boolean
}

const reply = (text: string): ToolResult => ({
  content: [{ type: 'text', text }],
})
const toolError = (text: string): ToolResult => ({
  ...reply(text),
  isError: true,
})
const NOT_IN_FLIGHT = toolError('that task_id is not in flight in this session')

const describeError = (err: unknown) =>
  err instanceof Error ? err.message : String(err)
const isApiError = (err: unknown, code: number) =>
  err instanceof JetStreamApiError && err.code === code
// A missing stream has no responder; a publish reports it as JetStream being off.
const isNoResponders = (err: unknown) => {
  const request =
    err instanceof Error && err.cause instanceof RequestError ? err.cause : err
  return request instanceof RequestError && request.isNoResponders()
}

export interface Worker {
  /** Call once MCP is `initialized`; pulling begins after the startup grace. */
  start(): void
  /** Stops the consume loop and the timers, and hands back a task in flight. */
  stop(): Promise<void>
  callTool(
    name: string,
    args: Record<string, unknown> | undefined,
  ): Promise<ToolResult>
}

export function createWorker({
  config,
  connection,
  push,
  log,
  env,
}: {
  config: Config
  connection: Connection
  /** Delivers one channel event. Rejecting leaves the task for redelivery. */
  push(content: string, meta: Record<string, string>): Promise<void>
  log(line: string): void
  env: Env
}): Worker {
  const timers = resolveTimers(env)
  const instance = nuid.next()
  const durable = `task-${config.name}`
  const stopping = new AbortController()
  let loop: Promise<void> | undefined
  let messages: ConsumerMessages | undefined
  let inFlight: InFlight | undefined
  // Set by the accept timeout, and keeps this session from pulling until a tool
  // call for that task clears it. S7 reports this as presence `stalled`.
  let released: string | undefined
  // The answer to late tool calls for tasks the run timeout ended.
  const closed = new Map<unknown, ToolResult>()
  const missingStreams = new Set<string>()

  // Resolves early, not rejects, on stop(): every caller rechecks the signal.
  const sleep = (ms: number) =>
    delay(ms, undefined, { signal: stopping.signal }).catch(() => {})

  const bootstrapHint = () =>
    `${[...missingStreams].join(' and ')} missing: run mise run bootstrap`

  // Without this the broker redelivers at ack_wait, however long the wait.
  const keepWorking = (current: () => JsMsg) =>
    setInterval(() => {
      try {
        current().working()
      } catch (err) {
        log(`working() failed: ${describeError(err)}`)
      }
    }, timers.working)

  // Best effort: a lost nak only means the task returns after ack_wait.
  function handBack(msg: JsMsg): void {
    try {
      msg.nak()
    } catch (err) {
      log(`nak failed: ${describeError(err)}`)
    }
  }

  function hold(
    nc: NatsConnection,
    task: Task,
    msg: JsMsg,
    attempt: number,
  ): InFlight {
    const held: InFlight = {
      task,
      msg,
      attempt,
      accepted: false,
      finishing: false,
      working: keepWorking(() => held.msg),
      deadline: setTimeout(
        () => releaseUnaccepted(nc, held),
        timers.acceptTimeout,
      ),
    }
    inFlight = held
    return held
  }

  function release(held: InFlight): void {
    clearInterval(held.working)
    clearTimeout(held.deadline)
    if (inFlight === held) inFlight = undefined
  }

  /** Plan 3.3, no delivery ack: either the session is deaf or it is too busy. */
  function releaseUnaccepted(nc: NatsConnection, held: InFlight): void {
    released = held.task.task_id
    // Pulling stops first, or the nak hands the task straight back here.
    messages?.stop()
    release(held)
    log(`task ${held.task.task_id} not accepted in time, released`)
    try {
      publishStatus(nc, held, 'released')
    } catch (err) {
      log(`released status failed: ${describeError(err)}`)
    }
    handBack(held.msg)
  }

  function armRunTimer(nc: NatsConnection, held: InFlight, ms: number): void {
    held.deadline = setTimeout(() => {
      closeTimedOut(nc, held).catch((err: unknown) => {
        log(`run timeout failed: ${describeError(err)}`)
      })
    }, ms)
  }

  // Any tool call proves Claude has the task, even one rejected further on.
  function accept(nc: NatsConnection, held: InFlight): void {
    if (held.accepted) return
    held.accepted = true
    clearTimeout(held.deadline)
    armRunTimer(nc, held, held.task.timeout_s * 1000)
  }

  async function closeTimedOut(
    nc: NatsConnection,
    held: InFlight,
  ): Promise<void> {
    // finish_task got here first. Stored, it clears this timer; failed, the
    // task still needs its result, so look again.
    if (held.finishing) {
      armRunTimer(nc, held, timers.restartMin)
      return
    }
    const { task, attempt } = held
    held.finishing = true
    closed.set(task.task_id, toolError(ALREADY_CLOSED))
    const stored = await storeFailure(
      nc,
      held.msg,
      task.task_id,
      attempt,
      'timeout',
      `no finish_task within timeout_s (${task.timeout_s})`,
    )
    if (stored === undefined) return
    recordFinished(nc, held)
    // Its PubAck was lost, but the finish_task landed: nothing timed out.
    if (stored === 'own_finish') {
      closed.set(task.task_id, reply('finished'))
      held.msg.ack()
      release(held)
      return
    }
    if (stored === 'other') {
      closed.set(task.task_id, toolError(ALREADY_RECORDED))
    }
    held.msg.term('timeout')
    release(held)
    try {
      await push(
        sanitizeContent(`Timed out, stop working on: ${task.title}`),
        buildMeta({
          kind: 'task_closed',
          task_id: task.task_id,
          reason: 'timeout',
        }),
      )
    } catch (err) {
      log(`task_closed push failed: ${describeError(err)}`)
    }
  }

  function statusBytes(
    { task, attempt }: Pick<InFlight, 'task' | 'attempt'>,
    state: Status['state'],
    note?: unknown,
  ): Uint8Array {
    const encoded = encodeEnvelope({
      v: PROTOCOL_VERSION,
      type: 'status',
      task_id: task.task_id,
      worker: config.name,
      title: task.title,
      created_at: task.created_at,
      state,
      instance,
      attempt,
      note,
      depth: task.depth,
      timeout_s: task.timeout_s,
      queue_ttl_s: task.queue_ttl_s,
    })
    if (!encoded.ok) throw new WireError(encoded.issues.join('; '))
    return encoded.bytes
  }

  // Core publish: losing one of these is fine. `delivered` is not, see deliver().
  function publishStatus(
    nc: NatsConnection,
    held: Pick<InFlight, 'task' | 'attempt'>,
    state: Status['state'],
    note?: unknown,
  ): void {
    nc.publish(
      statusSubject(config.name, held.task.task_id),
      statusBytes(held, state, note),
    )
  }

  // Closes the ledger row of a result the plugin stored. Lost, the row is only
  // stale: readers treat a stored result as terminal whatever the row says.
  function recordFinished(
    nc: NatsConnection,
    held: Pick<InFlight, 'task' | 'attempt'>,
  ): void {
    try {
      publishStatus(nc, held, 'finished')
    } catch (err) {
      log(`finished status failed: ${describeError(err)}`)
    }
  }

  async function lastMessage(
    nc: NatsConnection,
    stream: string,
    subject: string,
  ) {
    const jsm = await jetstreamManager(nc, { checkAPI: false })
    return jsm.direct.getMessage(stream, { last_by_subj: subject })
  }

  /** Runs a call on `stream`, noting whether the stream is there. */
  async function onStream<T>(
    stream: string,
    call: () => Promise<T>,
  ): Promise<T> {
    try {
      const value = await call()
      missingStreams.delete(stream)
      return value
    } catch (err) {
      if (isNoResponders(err)) missingStreams.add(stream)
      else if (err instanceof JetStreamApiError) missingStreams.delete(stream)
      throw err
    }
  }

  const storedResult = (nc: NatsConnection, taskId: string) =>
    onStream(RESULTS_STREAM_NAME, () =>
      lastMessage(nc, RESULTS_STREAM_NAME, resultSubject(config.name, taskId)),
    )

  /**
   * First result wins, and this says whose it is. Anything else throws, and
   * the result may or may not be stored: a timed-out publish can still land.
   */
  async function storeResult(
    nc: NatsConnection,
    taskId: string,
    bytes: Uint8Array,
  ): Promise<Stored> {
    const subject = resultSubject(config.name, taskId)
    try {
      // No Nats-Msg-Id. Dedupe spans the stream and ACLs cannot reach headers, so
      // any worker could claim one and have this store answered as a duplicate.
      // The per-subject limit already makes a repeat safe.
      await onStream(RESULTS_STREAM_NAME, () =>
        jetstream(nc).publish(subject, bytes, {
          expect: { lastSubjectSequence: 0 },
        }),
      )
      return 'written'
    } catch (err) {
      if (!isApiError(err, JetStreamApiCodes.StreamWrongLastSequence)) throw err
    }
    // Also where this session's own retry lands after a lost PubAck. Only
    // `instance` says whose result it is, and a `reason` marks one the plugin
    // wrote, not this finish_task.
    const stored = await storedResult(nc, taskId)
    if (!stored) throw new Error('the stored result is gone, call again')
    const parsed = parseMessage(subject, stored.data)
    if (
      !parsed.ok ||
      parsed.envelope.type !== 'result' ||
      parsed.envelope.instance !== instance
    ) {
      return 'other'
    }
    return parsed.envelope.reason === undefined ? 'own_finish' : 'own_failure'
  }

  /**
   * Runs `call` until it succeeds, with backoff, keeping `msg` from being
   * redelivered meanwhile. Undefined means stop() came first.
   */
  async function retryHolding<T>(
    msg: JsMsg,
    call: () => Promise<T>,
  ): Promise<T | undefined> {
    const working = keepWorking(() => msg)
    let retryIn = timers.restartMin
    try {
      while (!stopping.signal.aborted) {
        try {
          return await call()
        } catch (err) {
          log(`retrying in ${retryIn}ms: ${describeError(err)}`)
          await sleep(retryIn)
          retryIn = Math.min(retryIn * 2, timers.restartMax)
        }
      }
      return undefined
    } finally {
      clearInterval(working)
    }
  }

  /**
   * Stores a result the plugin decided on, whoever else may be writing one.
   * Undefined means stop() came first: nothing is stored, so no term().
   */
  function storeFailure(
    nc: NatsConnection,
    msg: JsMsg,
    taskId: string,
    attempt: number,
    reason: NonNullable<Result['reason']>,
    summary: string,
  ): Promise<Stored | undefined> {
    const result = encodeEnvelope({
      v: PROTOCOL_VERSION,
      type: 'result',
      task_id: taskId,
      worker: config.name,
      instance,
      attempt,
      outcome: 'failed',
      reason,
      summary,
      finished_at: new Date().toISOString(),
    })
    if (!result.ok) throw new WireError(result.issues.join('; '))
    return retryHolding(msg, () => storeResult(nc, taskId, result.bytes))
  }

  async function rejectTask(
    nc: NatsConnection,
    msg: JsMsg,
    failure: Extract<ParsedMessage, { ok: false }>,
  ): Promise<void> {
    const { reason, task_id, issues = [] } = failure
    log(`rejected a task envelope: ${reason}`)
    // The held task's id must not get a result from here: it would pass for
    // this session's own when the real finish_task hits the stored subject.
    if (task_id && task_id !== inFlight?.task.task_id) {
      const stored = await storeFailure(
        nc,
        msg,
        task_id,
        // Never pushed, so this is the first attempt however often it arrived.
        1,
        'invalid_task',
        `rejected task envelope: ${[reason, ...issues].join('; ')}`,
      )
      if (stored === undefined) return
    }
    msg.term(reason)
  }

  /**
   * Attempts count pushes to Claude, not deliveries: the stored `delivered`
   * status written before each push is the record. Only no status means nothing
   * was pushed yet. A `released` push reached nobody, so it costs no attempt; a
   * lost `released` overcounts by one.
   * Anything else without a usable attempt falls back to `deliveryCount`, which
   * can overcount but keeps `max_attempts` a real bound.
   */
  async function nextAttempt(
    nc: NatsConnection,
    taskId: string,
    msg: JsMsg,
  ): Promise<number> {
    const subject = statusSubject(config.name, taskId)
    try {
      const stored = await lastMessage(nc, STATE_STREAM_NAME, subject)
      if (!stored) return 1
      const parsed = parseMessage(subject, stored.data)
      if (parsed.ok && parsed.envelope.type === 'status') {
        const { state, attempt } = parsed.envelope
        if (attempt !== undefined) {
          return state === 'released' ? attempt : attempt + 1
        }
      }
      log(`status of ${taskId} has no usable attempt`)
    } catch (err) {
      log(`status read failed: ${describeError(err)}`)
    }
    return msg.info.deliveryCount
  }

  async function deliver(
    nc: NatsConnection,
    task: Task,
    msg: JsMsg,
  ): Promise<void> {
    const attempt = await nextAttempt(nc, task.task_id, msg)
    // Unacked, not naked: this may be a stale handle on a task another session
    // holds. The latch can land here while the lookup of a lost task ran.
    if (stopping.signal.aborted || released !== undefined) return
    // Plan 3.3 steps 4 and 5. Either result carries the attempt it refused.
    const expired = Date.now() - msg.time.getTime() > task.queue_ttl_s * 1000
    if (expired || attempt > task.max_attempts) {
      const reason = expired ? 'expired_in_queue' : 'attempts_exhausted'
      const summary = expired
        ? `not started within queue_ttl_s (${task.queue_ttl_s})`
        : `not finished in max_attempts (${task.max_attempts})`
      const stored = await storeFailure(
        nc,
        msg,
        task.task_id,
        attempt,
        reason,
        summary,
      )
      if (stored === undefined) return
      recordFinished(nc, { task, attempt })
      msg.term(reason)
      return
    }
    const meta = buildMeta({
      kind: 'task',
      task_id: task.task_id,
      from: task.from,
      attempt,
      timeout_s: task.timeout_s,
    })
    const content = sanitizeContent(`${task.title}\n\n${task.prompt}`)
    // No push without a stored record, or lost sessions would repeat attempt 1
    // forever and `max_attempts` would never fire.
    const subject = statusSubject(config.name, task.task_id)
    const record = statusBytes({ task, attempt }, 'delivered')
    await retryHolding(msg, () =>
      onStream(STATE_STREAM_NAME, () => jetstream(nc).publish(subject, record)),
    )
    if (stopping.signal.aborted) return
    const held = hold(nc, task, msg, attempt)
    try {
      await push(content, meta)
    } catch (err) {
      release(held)
      throw err
    }
  }

  // Plan 3.3 steps 1 to 6. Whatever this throws leaves the message unacked.
  async function handleMessage(nc: NatsConnection, msg: JsMsg): Promise<void> {
    const parsed = parseMessage(msg.subject, msg.data)
    if (!parsed.ok) return rejectTask(nc, msg, parsed)
    // The consumer's filter makes this a task subject, which selects TaskSchema.
    const task = parsed.envelope as Task

    const held = inFlight
    if (held?.task.task_id === task.task_id) {
      held.msg = msg
      return
    }
    if (held) {
      // max_ack_pending forbids this unless the broker lost the held task.
      // Finished elsewhere, it frees this session; otherwise the new task is
      // left unacked and comes back after ack_wait.
      if (!(await storedResult(nc, held.task.task_id))) {
        log(`task ${task.task_id} arrived while another is in flight`)
        return
      }
      release(held)
    }
    // Closes the crash window between result publish and ack.
    const stored = await retryHolding(msg, () => storedResult(nc, task.task_id))
    if (stored === undefined) return
    if (stored) {
      msg.ack()
      return
    }
    await deliver(nc, task, msg)
  }

  async function ensureConsumer(jsm: JetStreamManager): Promise<void> {
    const settings = {
      filter_subject: taskSubject(config.name),
      ack_wait: nanos(timers.ackWait),
      // One task at a time, across every session sharing this name.
      max_ack_pending: 1,
      // Unlimited: a message that hits max_deliver strands silently (plan 3.3).
      max_deliver: -1,
    }
    try {
      await jsm.consumers.add(TASKS_STREAM_NAME, {
        ...settings,
        durable_name: durable,
        ack_policy: AckPolicy.Explicit,
      })
    } catch (err) {
      if (!isApiError(err, CONSUMER_ALREADY_EXISTS)) throw err
      await jsm.consumers.update(TASKS_STREAM_NAME, durable, settings)
    }
  }

  /** Runs until the iterator ends; `onMessage` marks progress for the backoff. */
  async function consume(
    nc: NatsConnection,
    onMessage: () => void,
  ): Promise<void> {
    const jsm = await jetstreamManager(nc)
    await ensureConsumer(jsm)
    missingStreams.delete(TASKS_STREAM_NAME)
    const consumer = await jsm
      .jetstream()
      .consumers.get(TASKS_STREAM_NAME, durable)
    messages = await consumer.consume({ abort_on_missing_resource: true })
    if (stopping.signal.aborted || released !== undefined) messages.stop()
    for await (const msg of messages) {
      onMessage()
      try {
        await handleMessage(nc, msg)
      } catch (err) {
        log(`task left for redelivery: ${describeError(err)}`)
      }
    }
  }

  async function run(): Promise<void> {
    // Claude Code registers its channel handler after connecting, and a push
    // before that is lost.
    await sleep(timers.startupGrace)
    let restartIn = timers.restartMin
    while (!stopping.signal.aborted) {
      if (released !== undefined) {
        await sleep(timers.connectPoll)
        continue
      }
      const { nc } = connection
      if (!nc) {
        if (connection.unavailable) return
        await sleep(timers.connectPoll)
        continue
      }
      try {
        await consume(nc, () => {
          restartIn = timers.restartMin
        })
      } catch (err) {
        if (isApiError(err, JetStreamApiCodes.StreamNotFound)) {
          missingStreams.add(TASKS_STREAM_NAME)
          log(bootstrapHint())
          await sleep(timers.streamRetry)
          continue
        }
        log(`consume failed: ${describeError(err)}`)
      }
      if (stopping.signal.aborted) return
      await sleep(restartIn)
      restartIn = Math.min(restartIn * 2, timers.restartMax)
    }
  }

  function updateTask(
    nc: NatsConnection,
    held: InFlight,
    args: Record<string, unknown> | undefined,
  ): ToolResult {
    publishStatus(nc, held, 'accepted', args?.note)
    return reply('recorded')
  }

  async function finishTask(
    nc: NatsConnection,
    held: InFlight,
    args: Record<string, unknown> | undefined,
  ): Promise<ToolResult> {
    if (held.finishing) return toolError('finish_task is already running')
    // Named fields only: `reason` and the identity fields are the plugin's.
    const encoded = encodeEnvelope({
      v: PROTOCOL_VERSION,
      type: 'result',
      task_id: held.task.task_id,
      worker: config.name,
      instance,
      attempt: held.attempt,
      outcome: args?.outcome,
      summary: args?.summary,
      data: args?.data,
      artifacts: args?.artifacts,
      finished_at: new Date().toISOString(),
    })
    if (!encoded.ok) return toolError(encoded.issues.join('; '))

    held.finishing = true
    let stored: boolean
    try {
      const found = await storeResult(nc, held.task.task_id, encoded.bytes)
      stored = found === 'written' || found === 'own_finish'
    } catch (err) {
      held.finishing = false
      const why = isNoResponders(err) ? bootstrapHint() : describeError(err)
      return toolError(`result may not be stored, task kept: ${why}`)
    }

    // A result is durable from here on, so nothing below can fail the call:
    // a lost `finished` is only a stale ledger row, and a lost ack is closed
    // by the result check when the task is redelivered.
    release(held)
    try {
      if (stored) {
        publishStatus(nc, held, 'finished')
        await held.msg.ackAck()
      } else {
        held.msg.ack()
      }
    } catch (err) {
      log(`task ${held.task.task_id} stored, ack failed: ${describeError(err)}`)
    }
    return stored ? reply('finished') : toolError(ALREADY_RECORDED)
  }

  return {
    start() {
      loop ??= run()
    },

    async stop() {
      stopping.abort()
      // Pulling stops first, or the nak hands the task straight back here.
      messages?.stop()
      // Not after the loop, which a stuck broker can hold past the deadline. No
      // delivery can start holding from here: it rechecks the signal first.
      const held = inFlight
      if (held) {
        release(held)
        // A result on its way settles the task, redelivered or not.
        if (!held.finishing) handBack(held.msg)
      }
      await loop
    },

    async callTool(name, args) {
      if (name !== 'update_task' && name !== 'finish_task') {
        return toolError('unknown tool')
      }
      const { nc, unavailable } = connection
      if (!nc) {
        return toolError(
          unavailable ? `NATS unavailable: ${unavailable}` : 'NATS unavailable',
        )
      }
      const settled = closed.get(args?.task_id)
      if (settled) return settled
      if (released !== undefined && released === args?.task_id) {
        released = undefined
        return toolError(RELEASED)
      }
      const held = inFlight
      if (!held || held.task.task_id !== args?.task_id) {
        return missingStreams.size > 0
          ? toolError(bootstrapHint())
          : NOT_IN_FLIGHT
      }
      accept(nc, held)
      try {
        return name === 'update_task'
          ? updateTask(nc, held, args)
          : await finishTask(nc, held, args)
      } catch (err) {
        return toolError(describeError(err))
      }
    },
  }
}
