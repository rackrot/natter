import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js'

const mcp = new Server(
  { name: 'nats-channel', version: '0.0.1' },
  {
    capabilities: { tools: {}, experimental: { 'claude/channel': {} } },
    instructions:
      'NATS channel scaffold. It does not deliver any events or expose any tools yet.',
  },
)

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: [] }))

await mcp.connect(new StdioServerTransport())
