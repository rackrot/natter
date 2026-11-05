import {
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { z } from 'zod'
import { FLEET_PATTERN } from './config'
import { HeldSchema } from './protocol'

// The worker server writes the task it holds here and the permission hooks
// read it (plan 3.4). A per-user path both can compute: CLAUDE_PLUGIN_DATA
// reaches the server, but the hook's env was not confirmed to carry it.
// hooks/hooks.json computes the same marker path in shell.
const dir = () => join(homedir(), '.local', 'state', 'natter', 'held')

// The session id names a file, so anything but a plain token is refused.
const SESSION_PATTERN = /^[A-Za-z0-9_-]{1,64}$/

const pathFor = (sessionId: unknown) =>
  typeof sessionId === 'string' && SESSION_PATTERN.test(sessionId)
    ? join(dir(), sessionId)
    : undefined

// The server's NATS settings come from plugin.json's env, which the hooks do
// not get, and neither the fleet. Paths, the URL and the fleet only: never
// the seed itself.
export const HeldFileSchema = z.object({
  held: HeldSchema,
  fleet: z.string().regex(FLEET_PATTERN),
  url: z.string(),
  seed_path: z.string(),
  ca_file: z.string().optional(),
})
export type HeldFile = z.infer<typeof HeldFileSchema>

/** Throws on a failed write: callers decide that task handling goes on. */
export function writeHeld(sessionId: unknown, file: HeldFile): void {
  const path = pathFor(sessionId)
  if (!path) return
  mkdirSync(dir(), { recursive: true, mode: 0o700 })
  // Renamed into place, so a reader never sees half a file.
  writeFileSync(`${path}.tmp`, JSON.stringify(file), { mode: 0o600 })
  renameSync(`${path}.tmp`, path)
}

/** The held file and the waiting marker. */
export function clearHeld(sessionId: unknown): void {
  const path = pathFor(sessionId)
  if (!path) return
  rmSync(path, { force: true })
  rmSync(`${path}.waiting`, { force: true })
}

/** Undefined when nothing valid is held. */
export function readHeld(sessionId: unknown): HeldFile | undefined {
  const path = pathFor(sessionId)
  if (!path) return undefined
  try {
    const parsed = HeldFileSchema.safeParse(
      JSON.parse(readFileSync(path, 'utf8')),
    )
    return parsed.success ? parsed.data : undefined
  } catch {
    return undefined
  }
}

/** Set once `waiting_permission` is published; the PostToolUse hook clears it. */
export function setWaiting(sessionId: unknown, on: boolean): void {
  const path = pathFor(sessionId)
  if (!path) return
  if (on) writeFileSync(`${path}.waiting`, '', { mode: 0o600 })
  else rmSync(`${path}.waiting`, { force: true })
}
