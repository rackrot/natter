import { timingSafeEqual } from 'node:crypto'
import type { StatusSnapshot } from './orchestrator'
import { sanitizeJson } from './protocol'

// Plan 3.6, S13: a read-only page on a loopback port. It never publishes, and
// every byte of worker text in it is shown through textContent.

const HEADERS = {
  'Content-Security-Policy': "default-src 'self'",
  'Cache-Control': 'no-store',
  'Referrer-Policy': 'no-referrer',
  'X-Content-Type-Options': 'nosniff',
}

// The script is a request of its own, so its URL carries the token too.
const page = (token: string) => `<!doctype html>
<meta charset="utf-8">
<title>natter status</title>
<h1>natter</h1>
<p id="error" hidden></p>
<p id="note" hidden></p>
<h2>Workers</h2>
<table id="workers"></table>
<h2>Tasks</h2>
<table id="tasks"></table>
<h2>Streams</h2>
<table id="streams"></table>
<script src="/app.js?token=${token}"></script>
`

// The token rides in the page's query string; the script reads it from there.
const SCRIPT = `'use strict'
const token = new URLSearchParams(location.search).get('token') ?? ''
const POLL_MS = 3000

function fill(id, header, rows) {
  const table = document.getElementById(id)
  table.replaceChildren()
  for (const cells of [header, ...rows]) {
    const tr = table.insertRow()
    for (const cell of cells) tr.insertCell().textContent = String(cell ?? '')
  }
}

function show(data) {
  const note = document.getElementById('note')
  note.hidden = !(data.tasks.truncated || data.workers.truncated)
  note.textContent = 'Some older rows are left out, so the counts here are not complete.'
  const queued = new Map()
  for (const task of data.tasks.tasks) {
    if (task.state === 'queued') queued.set(task.worker, (queued.get(task.worker) ?? 0) + 1)
  }
  fill('workers', ['worker', 'state', 'stale', 'age_s', 'queued', 'task', 'repo', 'host', 'description'],
    data.workers.workers.map((w) => [w.name, w.state, w.stale, w.age_s, queued.get(w.name) ?? 0, w.task_id, w.repo, w.host, w.description]))
  fill('tasks', ['task', 'worker', 'title', 'state', 'attempt', 'outcome', 'note', 'created'],
    data.tasks.tasks.map((t) => [t.task_id, t.worker, t.title, t.state, t.attempt, t.outcome, t.note, t.created_at]))
  fill('streams', ['stream', 'messages', 'bytes'],
    data.streams.map((s) => [s.name, s.messages, s.bytes]))
}

// A slow answer must not land after a newer one and repaint old data.
let polling = false

async function poll() {
  if (polling) return
  polling = true
  const error = document.getElementById('error')
  try {
    const res = await fetch('/status.json?token=' + encodeURIComponent(token))
    const body = await res.json()
    if (!res.ok) throw new Error(body.error ?? res.statusText)
    error.hidden = true
    show(body)
  } catch (err) {
    // Never leave the last good data up as if it were current.
    for (const id of ['workers', 'tasks', 'streams']) document.getElementById(id).replaceChildren()
    error.textContent = 'Status unavailable: ' + (err instanceof Error ? err.message : String(err))
    error.hidden = false
    document.getElementById('note').hidden = true
  } finally {
    polling = false
  }
}

poll()
setInterval(poll, POLL_MS)
`

export interface StatusPage {
  /** The page's address, with this process's token. */
  url: string
  stop(force: boolean): Promise<void>
}

export function startStatusPage(
  read: () => Promise<StatusSnapshot>,
  log: (line: string) => void,
): StatusPage {
  const token = Buffer.from(
    crypto.getRandomValues(new Uint8Array(32)),
  ).toString('hex')
  const expected = Buffer.from(token)
  const respond = (body: string, status: number, type: string) =>
    new Response(body, {
      status,
      headers: { ...HEADERS, 'Content-Type': `${type}; charset=utf-8` },
    })

  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(req) {
      const url = new URL(req.url)
      if (req.method !== 'GET')
        return respond('method not allowed', 405, 'text/plain')
      // A DNS-rebound page arrives with its own name in Host.
      if (req.headers.get('host') !== `127.0.0.1:${server.port}`) {
        return respond('forbidden', 403, 'text/plain')
      }
      const given = Buffer.from(url.searchParams.get('token') ?? '')
      if (
        given.length !== expected.length ||
        !timingSafeEqual(given, expected)
      ) {
        return respond('forbidden', 403, 'text/plain')
      }
      if (url.pathname === '/') return respond(page(token), 200, 'text/html')
      if (url.pathname === '/app.js')
        return respond(SCRIPT, 200, 'text/javascript')
      if (url.pathname !== '/status.json')
        return respond('not found', 404, 'text/plain')
      try {
        return respond(
          JSON.stringify(sanitizeJson(await read())),
          200,
          'application/json',
        )
      } catch (err) {
        log(`status page read failed: ${err}`)
        const message = err instanceof Error ? err.message : String(err)
        return respond(
          JSON.stringify({ error: message }),
          503,
          'application/json',
        )
      }
    },
  })
  return {
    url: `http://127.0.0.1:${server.port}/?token=${token}`,
    stop: (force) => server.stop(force),
  }
}
