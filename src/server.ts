import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js'
import { resolveConfig, SHUTDOWN_DEADLINE_MS } from './config'
import { startConnection } from './connection'
import { instructionsFor } from './instructions'
import { createWorker, WORKER_TOOLS } from './worker'

// stdout belongs to MCP; everything else goes to stderr.
const log = (line: string) => process.stderr.write(`nats-channel: ${line}\n`)

const resolved = resolveConfig(process.env)
const config = 'config' in resolved ? resolved.config : undefined
log(
  'config' in resolved
    ? `role=${resolved.config.role} name=${resolved.config.name}`
    : `inactive: ${resolved.inert}`,
)

const mcp = new Server(
  { name: 'nats-channel', version: '0.0.1' },
  {
    capabilities: { tools: {}, experimental: { 'claude/channel': {} } },
    instructions: instructionsFor(config),
  },
)

// Connecting never blocks; the handlers below need these before MCP is up.
const connection = config ? startConnection(config, log) : undefined
const worker =
  config?.role === 'worker' && connection
    ? createWorker({
        config,
        connection,
        push: (content, meta) =>
          mcp.notification({
            method: 'notifications/claude/channel',
            params: { content, meta },
          }),
        log,
        env: process.env,
      })
    : undefined

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: worker ? WORKER_TOOLS : [],
}))
if (worker) {
  mcp.setRequestHandler(CallToolRequestSchema, ({ params }) =>
    worker.callTool(params.name, params.arguments),
  )
  mcp.oninitialized = () => worker.start()
}

await mcp.connect(new StdioServerTransport())

// When Claude Code closes the MCP connection, stdin gets EOF. A live NATS
// handle would keep this process alive as an orphan, so exit explicitly.
let shuttingDown = false
function shutdown(): void {
  if (shuttingDown) return
  shuttingDown = true
  log('shutting down')
  setTimeout(() => process.exit(0), SHUTDOWN_DEADLINE_MS)
  void Promise.resolve(worker?.stop())
    .then(() => connection?.close())
    .catch((err: unknown) => log(`drain failed: ${err}`))
    .finally(() => process.exit(0))
}
process.stdin.on('end', shutdown)
process.stdin.on('close', shutdown)
process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)
process.on('SIGHUP', shutdown)

// Backs up the stdin handlers, as the telegram plugin does.
setInterval(() => {
  if (process.stdin.destroyed || process.stdin.readableEnded) shutdown()
}, 5000).unref()

process.on('unhandledRejection', (err) => log(`unhandled rejection: ${err}`))
