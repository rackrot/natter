import { connect } from '@nats-io/transport-node'
import { resolveConfig, resolveTransport } from './config'
import { connectOptions } from './connection'
import { readHeld, setWaiting } from './held'
import { encodeStatus, statusSubject } from './protocol'

// Hooks of plan 3.4. `bun src/hook.ts permission` runs on a `permission_prompt`
// Notification and tells the orchestrator the held task waits on a human;
// `bun src/hook.ts resumed` runs on PostToolUse, once a marker says it did, and
// puts the task back to `accepted`. `bun src/hook.ts error` runs on a
// StopFailure and publishes `session_error` with the error type as its note.
// Short-lived and silent: never prints, never blocks, any failure exits 0. The
// prompt's message and tool input, and a failure's `error_details` and
// `last_assistant_message`, are not used, because they can hold secrets.

const BOUND_MS = 2000
const mode = process.argv[2]
const resumed = mode === 'resumed'
const state =
  mode === 'error'
    ? 'session_error'
    : resumed
      ? 'accepted'
      : 'waiting_permission'

// A short token, never free text: `error` comes off the session's stdin.
const ERROR_TYPE = /^[a-z][a-z0-9_]{0,31}$/
const errorType = (input: unknown) => {
  const type = (input as { error?: unknown } | null)?.error
  return typeof type === 'string' && ERROR_TYPE.test(type) ? type : 'unknown'
}

async function run(): Promise<void> {
  const resolved = resolveConfig(process.env)
  if (!('config' in resolved) || resolved.config.role !== 'worker') return
  const { config } = resolved

  const input: unknown = JSON.parse(await Bun.stdin.text())
  const sessionId = (input as { session_id?: unknown } | null)?.session_id
  const note = mode === 'error' ? errorType(input) : undefined
  try {
    const file = readHeld(sessionId)
    if (!file) return
    // The server's own settings: the hook's env does not carry plugin.json's.
    const transport = resolveTransport({
      ...config,
      url: file.url,
      seedPath: file.seed_path,
      caFile: file.ca_file,
    })
    if ('refused' in transport) return
    // One try, no waiting: a down server must not hold the session.
    const nc = await connect({
      ...connectOptions({ ...config, caFile: file.ca_file }, transport),
      waitOnFirstConnect: false,
      maxReconnectAttempts: 0,
      timeout: BOUND_MS - 500,
    })
    try {
      // Connecting took time: a release or a new task must not be overwritten.
      if (readHeld(sessionId)?.held.task_id !== file.held.task_id) return
      nc.publish(
        statusSubject(config.name, file.held.task_id),
        encodeStatus(config.name, file.held, state, note),
      )
      await nc.flush()
      // Also for an error: a revived session's next tool call clears the row.
      if (!resumed) setWaiting(sessionId, true)
    } finally {
      await nc.close()
    }
  } finally {
    if (resumed) setWaiting(sessionId, false)
  }
}

setTimeout(() => process.exit(0), BOUND_MS)
await run().catch(() => {})
process.exit(0)
