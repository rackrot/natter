# natter

Let your Claude Code sessions talk over NATS. One orchestrator session sends tasks to repo-level worker sessions and gets structured results back. Tasks wait in a durable queue while a worker is offline, and results wait for the orchestrator.

natter is for personal use: one user, one shared seed, your own machines. It is not built for several users or for agents you do not control.

## How it works

The plugin `nats-channel` (marketplace `natter`) runs one MCP server per Claude Code session, as a Claude Code channel. The environment variable `NATTER_ROLE` picks what that session is. Without it the plugin does nothing.

- Orchestrator tools: `send_task`, `list_workers`, `list_tasks`, `get_result`, `status_page`. Results and alerts arrive as channel pushes.
- Worker tools: `update_task`, `finish_task`. A worker takes one task at a time from its queue.
- Everything a worker sends is untrusted data to the orchestrator. Results are checked against the orchestrator's own record of each send.

## Prerequisites

- Claude Code, interactive. Tested with 2.1.318.
- [Bun](https://bun.sh) on `PATH`. The plugin starts its server with it.
- A `nats-server` with JetStream. Tested with 2.15.2.
- [mise](https://mise.jdx.dev) for `mise run bootstrap`, `doctor` and `monitor`. It pins Bun, Biome, `nats-server` and the `nats` CLI.

## Install

From a checkout of this repo:

```sh
mise install
mise run setup
claude plugin marketplace add "$PWD"
claude plugin install nats-channel@natter
```

The plugin asks for three settings. They are per machine, not per repo:

| Setting | Default | Meaning |
| --- | --- | --- |
| NATS URL | `nats://127.0.0.1:4222` | Non-loopback hosts need `tls://` and a seed |
| Credentials directory | `~/.config/natter` | Holds `natter.nk`, the shared seed |
| CA file | empty | CA certificate for `tls://`. Empty uses the system store |

The environment overrides them: `NATTER_URL`, `NATTER_CREDS_DIR`, `NATTER_CA_FILE`. `mise run bootstrap`, `doctor` and `monitor` read only these variables, never the plugin settings.

## Bootstrap

Run it once per NATS server, from the checkout:

```sh
mise run bootstrap
```

It creates or reconciles five JetStream streams: `NATTER_TASKS`, `NATTER_RESULTS`, `NATTER_STATE`, `NATTER_PRESENCE` and `NATTER_SENT`. It is safe to rerun. Workers and the orchestrator never create streams; until it has run, tools report the streams as missing.

The streams reserve about 1.81 GiB of JetStream storage in total (256 MiB, 1 GiB, 256 MiB, 64 MiB and 256 MiB, in the order above).

**Upgrading a server that already has natter streams:** raise the server's JetStream storage first. Bootstrap updates `NATTER_STATE` to give up the presence subject before it creates `NATTER_PRESENCE`. If the create fails for lack of storage, no stream captures presence until you raise storage and rerun bootstrap.

## Launch

Both roles are interactive sessions started with the development channel flag.

Orchestrator, from an empty directory (see Orchestrator lockdown):

```sh
NATTER_ROLE=orchestrator claude --dangerously-load-development-channels plugin:nats-channel@natter
```

Worker, from the repo it works on:

```sh
NATTER_ROLE=worker NATTER_NAME=myrepo claude --dangerously-load-development-channels plugin:nats-channel@natter
```

A worker's name is the address tasks go to. It must match `^[a-z0-9][a-z0-9_-]{0,63}$`. `admin` is reserved, and a worker cannot be called `orchestrator`. The name comes from, in order, `NATTER_NAME`, `name` in `.claude/natter.json`, then the sanitized directory name of the project. A session with a bad name or no role stays inactive and says why in its server log.

### `.claude/natter.json`

Optional, in the repo a worker runs from:

```json
{ "name": "myrepo", "description": "What this repo is, shown to the orchestrator" }
```

`description` is at most 512 bytes. The file never sets the role, so a committed file cannot turn a plain session into a worker.

## Allow rules

Unattended workers need allow rules for the plugin tools, or every call waits on a prompt. Tool names are `mcp__plugin_nats-channel_nats-channel__<tool>`. In the worker repo's `.claude/settings.local.json`:

```json
{
  "permissions": {
    "allow": [
      "mcp__plugin_nats-channel_nats-channel__update_task",
      "mcp__plugin_nats-channel_nats-channel__finish_task"
    ]
  }
}
```

For the orchestrator:

```json
{
  "permissions": {
    "allow": [
      "mcp__plugin_nats-channel_nats-channel__send_task",
      "mcp__plugin_nats-channel_nats-channel__list_workers",
      "mcp__plugin_nats-channel_nats-channel__list_tasks",
      "mcp__plugin_nats-channel_nats-channel__get_result",
      "mcp__plugin_nats-channel_nats-channel__status_page"
    ]
  }
}
```

Do not run workers with `bypassPermissions`. Allow only what the repo's work needs.

## Orchestrator lockdown

The orchestrator is the high-value target. A compromised repo can write a worker result aimed at it, and from there at other workers. So run it from an empty directory and leave it only the plugin tools. In that directory's `.claude/settings.json`:

```json
{
  "permissions": {
    "deny": ["Bash", "Edit", "Write"]
  }
}
```

With Bash denied, `/nats-channel:status` cannot open the browser; ask the orchestrator to call `status_page` and open the printed URL yourself.

Its instructions also tell it to treat every worker field as data and to report, not obey, a result that asks for changed permissions, credentials or settings.

A worker's Bash runs as the same OS user that owns the seed, so a compromised worker can read it. With one shared seed that means it can act as any session; this is an accepted limit. A `Read` deny on the creds directory helps against the Read tool only, not against `cat` in Bash:

```json
{ "permissions": { "deny": ["Read(~/.config/natter/**)"] } }
```

## Auth and TLS

On loopback (`localhost`, `127.0.0.1`, `::1`) natter runs unauthenticated, which is what `mise run nats` serves for development.

A non-loopback URL needs both `tls://` and a seed, or the session refuses to connect and its tools say why. `NATTER_ALLOW_PLAINTEXT=1` lifts the `tls://` requirement only, for a trusted mesh such as WireGuard or Tailscale. The seed is still required.

Setup, with every session and bootstrap sharing one seed:

```sh
mkdir -p ~/.config/natter
nats auth nkey gen user --output ~/.config/natter/natter.nk
chmod 600 ~/.config/natter/natter.nk
nats auth nkey show ~/.config/natter/natter.nk
```

The last command prints the public key (starts with `U`). Copy `natter.nk` to the same path on every machine that runs a session. A seed readable by group or others logs a warning.

On the server, copy `deploy/nats-server.example.conf`, put that public key in `authorization`, and fill in the TLS certificate, key and JetStream store paths. Every `CHANGE_ME` must be replaced. It defines one nkey user with no per-subject permissions, which fits the personal-use scope. Point clients at `tls://host:4222` and set the CA file if the certificate is not in the system store.

## Permission stalls and `stale`

When a worker stops at a permission prompt, a hook marks its task `waiting_permission`. The orchestrator then gets a `kind="attention"` push with `reason="waiting_permission"`, within the 30 second sweep. It cannot approve the prompt, so it tells you which worker needs you. The status returns to `accepted` after the next tool call that runs.

`stale` means nothing has been heard from the worker for 45 seconds. Heartbeats come from the plugin's server process every 15 seconds, not from Claude. A busy or quiet Claude never goes stale; `stale` means the worker's server process stopped answering, for example the session ended or the machine is gone. It does not mean the worker is waiting on a command.

Other attention reasons: `released` (a task was not accepted in time and will be redelivered) and `overdue` (open far past its timeouts).

## Checking things

- `/nats-channel:doctor` runs the doctor in the session and reports PASS, FAIL or SKIP for the config, transport policy, seed, TCP, TLS, auth, JetStream, the five streams and the session's consumer. `mise run doctor` does the same from the checkout. It connects once with no retry, so it names the cause of a TLS or auth failure.
- `/nats-channel:status` (orchestrator only) opens a local read-only web page: workers and liveness, queue depth, open and recent tasks, stream usage. The URL holds a secret token and the port changes every run. Without the token every request gets 403.
- `mise run monitor` tails `natter.>` with the `nats` CLI. It is a development tool, and the payloads it shows are untrusted.

## Known limits

- Channels are a research preview. Custom channels need `--dangerously-load-development-channels`. The flag is interactive-only and ignored under `-p` or the Agent SDK, so there are no headless workers.
- Tested with Claude Code 2.1.318. The plugin relies on Claude Code escaping channel content and does not add its own tag escaping.
- `/nats-channel:doctor` does not see plugin settings. It runs in a shell and reads `NATTER_URL`, `NATTER_CREDS_DIR` and `NATTER_CA_FILE` only, so with non-default settings it checks the defaults unless you export those variables before starting Claude.
- A deny at a permission prompt fires no hook, so the task stays `waiting_permission` until the worker's next `update_task` or `finish_task`.
- The permission hooks need `CLAUDE_CODE_SESSION_ID` and `HOME`. Without them the alert is not raised.
- No real two-machine TLS run has been done. The TLS path is tested against the shipped server template on one machine.
- The status page has not been tried in a real browser.
- A second orchestrator session on the same account splits pushes between them. Run one.
- There is no cancel, no file transfer and no mid-task dialogue.

`docs/smoke.md` is a manual checklist for a two-session run.

## Development

Dev tooling is managed by mise.

| Task | What it does |
| --- | --- |
| `mise run setup` | Install dependencies |
| `mise run nats` | Run a local JetStream `nats-server` on 127.0.0.1, storing data in `.nats-data` |
| `mise run bootstrap` | Create or reconcile the `NATTER_*` streams |
| `mise run doctor` | Check config, transport, auth, JetStream, streams and consumer |
| `mise run monitor` | Tail `natter.>` |
| `mise run lint` | `biome check` and `tsc --noEmit` |
| `mise run fmt` | `biome format --write` |
| `mise run test` | `bun test` (spawns the server and a throwaway `nats-server`) |
| `mise run validate` | `claude plugin validate . --strict` |
| `mise run check` | lint, test and validate |
