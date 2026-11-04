# natter

Let your Claude Code sessions talk over NATS. Orchestrate repo-level agents with pub/sub, durable queues, and structured results.

## Status

Scaffold only. The `nats-channel` MCP server starts, declares the Claude Code channel capability and lists no tools. It does not subscribe to or publish on NATS yet, so no messaging works.

## Development

Dev tooling is managed by [mise](https://mise.jdx.dev). It pins Bun, Biome, `nats-server` and the `nats` CLI.

```sh
mise install     # install pinned tools
mise run setup   # bun install
```

| Task | What it does |
| --- | --- |
| `mise run setup` | Install dependencies |
| `mise run nats` | Run a local JetStream `nats-server` on 127.0.0.1, storing data in `.nats-data` |
| `mise run lint` | `biome check` and `tsc --noEmit` |
| `mise run fmt` | `biome format --write` |
| `mise run test` | `bun test` (spawns the server and a throwaway `nats-server`) |
| `mise run validate` | `claude plugin validate . --strict` |
| `mise run check` | lint, test and validate |

## Try it locally

```sh
claude plugin marketplace add /path/to/natter
claude plugin install nats-channel@natter
claude --dangerously-load-development-channels plugin:nats-channel@natter
```

Limits:

- Channels are a research preview.
- The plugin needs Bun on `PATH` at runtime. mise is only for development.
- Custom channels need the `--dangerously-load-development-channels` flag. It is interactive-only and ignored under `-p` or the Agent SDK, so headless workers cannot load this channel yet.
