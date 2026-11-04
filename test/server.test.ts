import { afterAll, expect, test } from 'bun:test'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'

const transport = new StdioClientTransport({
  command: 'bun',
  args: ['src/server.ts'],
  stderr: 'inherit',
})
const client = new Client({ name: 'test', version: '0.0.0' })

afterAll(() => client.close())

test('server advertises the channel capability, without permission relay, and lists no tools', async () => {
  await client.connect(transport)

  const experimental = client.getServerCapabilities()?.experimental
  expect(experimental).toHaveProperty(['claude/channel'])
  expect(experimental).not.toHaveProperty(['claude/channel/permission'])
  expect((await client.listTools()).tools).toEqual([])
})
