import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import { resolveConfig, SHUTDOWN_DEADLINE_MS } from './config'
import { startConnection } from './connection'

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
    instructions: config
      ? `nats-channel ${config.role} role is a placeholder and does nothing yet.`
      : 'nats-channel is inactive in this session.',
  },
)

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: [] }))

await mcp.connect(new StdioServerTransport())

const connection = config ? startConnection(config, log) : undefined

// When Claude Code closes the MCP connection, stdin gets EOF. A live NATS
// handle would keep this process alive as an orphan, so exit explicitly.
let shuttingDown = false
function shutdown(): void {
  if (shuttingDown) return
  shuttingDown = true
  log('shutting down')
  setTimeout(() => process.exit(0), SHUTDOWN_DEADLINE_MS)
  void Promise.resolve(connection?.close())
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
