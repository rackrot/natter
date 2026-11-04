import type { ChildProcess } from 'node:child_process'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'

export interface Session {
  client: Client
  /** Stdout lines or transport errors that were not valid MCP. */
  errors: Error[]
  /** Resolves when the server process has exited. */
  closed: Promise<void>
  /** Closes the server's stdin, as Claude Code does when it goes away. */
  endStdin(): void
  close(): Promise<void>
}

/**
 * Spawns `bun src/server.ts` and completes the MCP handshake. The server
 * gets exactly `env` plus a PATH, so ambient NATTER_* or CLAUDE_* variables
 * never leak into a test.
 */
export async function startSession(
  env: Record<string, string> = {},
): Promise<Session> {
  const transport = new StdioClientTransport({
    command: 'bun',
    args: ['src/server.ts'],
    env: { PATH: process.env.PATH ?? '', ...env },
    stderr: 'inherit',
  })
  const client = new Client({ name: 'test', version: '0.0.0' })
  const errors: Error[] = []
  client.onerror = (err) => errors.push(err)
  const closed = new Promise<void>((resolve) => {
    client.onclose = resolve
  })
  await client.connect(transport)
  return {
    client,
    errors,
    closed,
    // The SDK transport has no public stdin handle; reach the child directly.
    endStdin: () =>
      (
        transport as unknown as { _process?: ChildProcess }
      )._process?.stdin?.end(),
    close: () => client.close(),
  }
}
