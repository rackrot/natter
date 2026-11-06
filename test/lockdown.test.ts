import { describe, expect, test } from 'bun:test'
import { join } from 'node:path'
import { resolveConfig } from '../src/config'
import { ORCHESTRATOR_TOOLS } from '../src/orchestrator'
import { WORKER_TOOLS } from '../src/worker'

const ROOT = join(import.meta.dir, '..')
const PREFIX = 'mcp__plugin_nats-channel_nats-channel__'
const FORBIDDEN = ['Bash', 'Edit', 'Write', 'NotebookEdit', 'Agent']
const SEED = 'Read(~/.config/natter/**)'

const full = (tools: { name: string }[]) =>
  tools.map((tool) => `${PREFIX}${tool.name}`)

async function permissions(file: string) {
  const json = await Bun.file(join(ROOT, 'config', file)).json()
  return json.permissions as { allow: string[]; deny?: string[] }
}

async function agentFrontmatter() {
  const text = await Bun.file(join(ROOT, 'agents', 'orchestrator.md')).text()
  const match = /^---\n([\s\S]*?)\n---\n/.exec(text)
  expect(match).not.toBeNull()
  return Bun.YAML.parse(match?.[1] as string) as {
    name: string
    tools: string
  }
}

describe('the orchestrator agent', () => {
  test('is named orchestrator and its tools are an allowlist', async () => {
    const agent = await agentFrontmatter()
    expect(agent.name).toBe('orchestrator')
    const tools = agent.tools.split(',').map((tool) => tool.trim())
    for (const tool of full(ORCHESTRATOR_TOOLS)) expect(tools).toContain(tool)
    for (const tool of ['Read', 'Glob', 'Grep', 'WebSearch', 'WebFetch']) {
      expect(tools).toContain(tool)
    }
    for (const tool of [...FORBIDDEN, 'ToolSearch']) {
      expect(tools).not.toContain(tool)
    }
    // Nothing beyond the natter tools and the read-only ones.
    expect(tools).toHaveLength(ORCHESTRATOR_TOOLS.length + 5)
  })

  test('settings allow the natter tools and deny the writers and the seed', async () => {
    const { allow, deny } = await permissions('orchestrator.settings.json')
    expect([...allow].sort()).toEqual(full(ORCHESTRATOR_TOOLS).sort())
    for (const rule of [...FORBIDDEN, SEED]) expect(deny).toContain(rule)
  })

  test('the name the launcher sets is accepted for the orchestrator only', () => {
    const base = { NATTER_NAME: 'orchestrator', NATTER_FLEET: 'x', HOME: '/h' }
    expect(
      'config' in resolveConfig({ ...base, NATTER_ROLE: 'orchestrator' }),
    ).toBe(true)
    expect('config' in resolveConfig({ ...base, NATTER_ROLE: 'worker' })).toBe(
      false,
    )
  })
})

describe('the worker settings', () => {
  test('allow exactly the worker tools', async () => {
    const { allow } = await permissions('worker.settings.json')
    expect([...allow].sort()).toEqual(full(WORKER_TOOLS).sort())
  })
})
