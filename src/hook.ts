import { connect } from '@nats-io/transport-node'
import { resolveConfig, resolveTransport } from './config'
import { connectOptions } from './connection'
import { readHeld, setWaiting } from './held'
import { encodeStatus, statusSubject } from './protocol'

// Hooks of plan 3.4. `bun src/hook.ts permission` runs on a `permission_prompt`
// Notification and tells the orchestrator the held task waits on a human;
// `bun src/hook.ts resumed` runs on PostToolUse, once a marker says it did, and
// puts the task back to `accepted`. Short-lived and silent: never prints, never
// blocks, any failure exits 0. The prompt's message and tool input are not
// used, because they can hold secrets.

const BOUND_MS = 2000
const waiting = process.argv[2] !== 'resumed'

async function run(): Promise<void> {
  const resolved = resolveConfig(process.env)
  if (!('config' in resolved) || resolved.config.role !== 'worker') return
  const { config } = resolved

  const input: unknown = JSON.parse(await Bun.stdin.text())
  const sessionId = (input as { session_id?: unknown } | null)?.session_id
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
        encodeStatus(
          config.name,
          file.held,
          waiting ? 'waiting_permission' : 'accepted',
        ),
      )
      await nc.flush()
      if (waiting) setWaiting(sessionId, true)
    } finally {
      await nc.close()
    }
  } finally {
    if (!waiting) setWaiting(sessionId, false)
  }
}

setTimeout(() => process.exit(0), BOUND_MS)
await run().catch(() => {})
process.exit(0)
