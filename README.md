# natter

Let your Claude Code sessions talk over NATS. One orchestrator session sends tasks to repo-level worker sessions and gets structured results back. Tasks wait in a durable queue while a worker is offline, and results wait for the orchestrator.

natter is for personal use: one user, one shared seed, your own machines. It is not built for several users or for agents you do not control.

## How it works

The plugin `nats-channel` (marketplace `natter`) runs one MCP server per Claude Code session, as a Claude Code channel. The environment variable `NATTER_ROLE` picks what that session is. Without it the plugin does nothing. The `natter` command (see Launch) sets it for you.

- Orchestrator tools: `send_task`, `list_workers`, `list_tasks`, `get_result`, `status_page`, `doctor`. Results and alerts arrive as channel pushes.
- Worker tools: `update_task`, `finish_task`, `doctor`. A worker takes one task at a time from its queue.
- Everything a worker sends is untrusted data to the orchestrator. Results are checked against the orchestrator's own record of each send.

## Prerequisites

- Claude Code, interactive. Tested with 2.1.318.
- [Bun](https://bun.sh) on `PATH`. The plugin starts its server with it.
- A `nats-server` with JetStream. Tested with 2.15.2.
- [mise](https://mise.jdx.dev) for `mise run bootstrap`, `fleet-delete`, `doctor` and `monitor`. It pins Bun, Biome, ShellCheck, `nats-server` and the `nats` CLI.
- For the `natter` launcher: `claude-orchestrate` on `PATH`. A new herdr workspace also needs `herdr` and `jq`; `natter --here` and a launch from inside a herdr pane need neither.

## Install

From a checkout of this repo:

```sh
mise install
mise run setup
claude plugin marketplace add "$PWD"
claude plugin install nats-channel@natter
ln -s "$PWD/bin/natter" ~/.local/bin/natter
```

The last line puts the launcher on your `PATH` (make sure `~/.local/bin` is on it). The plugin's `bin/` directory is also on the Bash tool's `PATH` inside sessions while the plugin is enabled, so a session can run `natter` too.

The plugin asks for three settings. They are per machine, not per repo:

| Setting | Default | Meaning |
| --- | --- | --- |
| NATS URL | `nats://127.0.0.1:4222` | Non-loopback hosts need `tls://` and a seed |
| Credentials directory | `~/.config/natter` | Holds `natter.nk`, the shared seed |
| CA file | empty | CA certificate for `tls://`. Empty uses the system store |

The environment overrides them: `NATTER_URL`, `NATTER_CREDS_DIR`, `NATTER_CA_FILE`. `mise run bootstrap`, `doctor` and `monitor` read only these variables, never the plugin settings.

## Fleets

A fleet is one orchestrator and its workers. Several fleets can share one NATS server and account, and nothing crosses between them: each fleet has its own five JetStream streams and its own subject prefix, so a session only ever reads its own fleet's records. Two fleets can each have a worker with the same name.

- The fleet comes from `NATTER_FLEET`, or `default` when it is unset or empty. `.claude/natter.json` cannot set it.
- A fleet name matches `^[a-z0-9][a-z0-9-]{0,31}$`: lowercase letters, digits and hyphens, at most 32, starting with a letter or digit. A session with a bad name stays inactive and says why in its server log. `natter` refuses it up front.
- Streams are `NATTER_<FLEET>_TASKS`, `_RESULTS`, `_STATE`, `_PRESENCE` and `_SENT`, with the fleet name in capitals. Subjects are `natter.<fleet>.task.<worker>`, `natter.<fleet>.result.<worker>.<id>`, `natter.<fleet>.status.<worker>.<id>`, `natter.<fleet>.presence.<worker>` and `natter.<fleet>.sent.<id>`.
- Run one orchestrator per fleet. A second orchestrator in the same fleet splits pushes between them.

`natter` itself requires `--fleet` (or an exported `NATTER_FLEET`), so a forgotten flag does not land you in `default`. A plain launch with no `natter` and no variable uses `default`.

## Streams and bootstrap

Each fleet reserves about 1.81 GiB of JetStream storage in total: 256 MiB for `TASKS`, 1 GiB for `RESULTS`, 256 MiB for `STATE`, 64 MiB for `PRESENCE` and 256 MiB for `SENT`. That is the `max_bytes` of each stream, and it counts against the server's JetStream `max_file_store` when the stream is created. It is a reservation, not disk used: a fleet that holds little uses little disk. The shipped server template sets no limit. If creating a fleet fails with `insufficient storage resources available`, raise the limit in the server's `jetstream` block and restart the server:

```
jetstream {
  store_dir: "CHANGE_ME/natter-nats-data"
  max_file_store: 20GB
}
```

Delete the fleets you no longer use to free the reservation (below).

**Creation.** `natter orchestrator` creates its fleet's streams when none of the five exist. Workers never create streams, and an orchestrator never fills in a partial set or updates a stream: a partial set is logged with the bootstrap command to run. A worker started first retries and connects once the streams exist. Without `natter` (a plain launch), no session creates streams until `NATTER_PROVISION=1` is set for the orchestrator.

**Reconcile.** `mise run bootstrap` creates or reconciles the five streams of `NATTER_FLEET` (`default` when unset). It is safe to rerun and is the only thing that updates stream settings:

```sh
NATTER_FLEET=x mise run bootstrap
```

**Retire a fleet.** This deletes the fleet's five streams and everything in them, and refuses to run unless `NATTER_FLEET` is set:

```sh
NATTER_FLEET=x mise run fleet-delete
```

A mistyped `--fleet` on an orchestrator creates a fleet, so delete it with this.

### Upgrading from the single-fleet streams

Fleets are a clean break. The old streams `NATTER_TASKS`, `NATTER_RESULTS`, `NATTER_STATE`, `NATTER_PRESENCE` and `NATTER_SENT` are not read, migrated or reused. Once per server: finish or abandon open work, stop every session, then delete all five old streams:

```sh
for s in TASKS RESULTS STATE PRESENCE SENT; do
  nats --server "${NATTER_URL:-nats://127.0.0.1:4222}" stream rm -f "NATTER_$s"
done
```

Add `--nkey ~/.config/natter/natter.nk` (and `--tlsca FILE`) to the `nats` command when the server needs a seed or a private CA. Queued tasks and stored results in the old streams are lost. Deleting them also frees their 1.81 GiB.

## Launch

Both roles are interactive sessions, started with `natter`. It wraps `claude-orchestrate`, so workers get the orchestration prompt and shell setup, and it sets `NATTER_ROLE`, `NATTER_FLEET` and `NATTER_NAME`, the development channel flag and the role's `--settings` file (`config/worker.settings.json` or `config/orchestrator.settings.json`).

```sh
natter orchestrator --fleet F [OPTIONS] [-- CLAUDE_ARGS...]
natter worker --fleet F [--name N] [OPTIONS] [DIR] [-- CLAUDE_ARGS...]
```

| Option | Meaning |
| --- | --- |
| `--fleet F` | The fleet to join. Defaults to `NATTER_FLEET` |
| `--name N` | Worker name. Without it, `.claude/natter.json`, then the directory name decide |
| `DIR` | The repo a worker runs in. Defaults to the current directory. An orchestrator takes none |
| `-w` | Open a new herdr workspace even inside a herdr pane |
| `--here` | Run in this terminal, whatever herdr says |
| `-d` | Never attach to the new workspace |
| `--attach` | Focus the new workspace and attach this terminal to it. Not with `-d` |
| `-n` | Print the commands instead of running them |
| `-- CLAUDE_ARGS` | Everything after `--` goes to `claude`, for example `-- --resume` |

Put the options before `DIR`: `natter` stops reading options at `DIR`, so a later `--here` would go to `claude`. `-w` excludes `--here`, and `-d` excludes `--attach`.

Where the session runs:

- Inside a herdr pane (`HERDR_ENV=1`, including a pane opened by a herdr-plus project), or with `--here`: in that terminal.
- Anywhere else, or with `-w`: in a new herdr workspace labelled `fleet:name`. The workspace opens in the background and takes no focus from open herdr clients, and `natter` prints its label. It focuses the workspace and attaches this terminal only when this run started the herdr server, or with `--attach`. `--attach` inside herdr has no effect. If the herdr server is not running, `natter` starts it.
- The workspace's shell does not inherit yours. Only `NATTER_URL`, `NATTER_CREDS_DIR`, `NATTER_CA_FILE` and `NATTER_ALLOW_PLAINTEXT` are copied to it, and only when set.

The orchestrator always runs in `${XDG_STATE_HOME:-$HOME/.local/state}/natter/orchestrator/<fleet>`, an empty directory per fleet that `natter` creates (see Orchestrator lockdown).

Every launch stops at the development channel dialog until you press a key, and the first launch in a directory also stops at the trust dialog. Nothing answers them for you, and `-d` leaves the session waiting at the dialog.

A worker's name is the address tasks go to. It must match `^[a-z0-9][a-z0-9_-]{0,63}$`. `admin` is reserved, and a worker cannot be called `orchestrator`. The name comes from, in order, `--name` (`NATTER_NAME`), `name` in `.claude/natter.json`, then the sanitized directory name of the project. A session with a bad name or no role stays inactive and says why in its server log.

### herdr-plus project

A herdr-plus project file opens a whole fleet in one go. Each pane runs in a herdr pane, so `natter` runs in place. In `~/.config/herdr/plugins/config/cloudmanic.herdr-plus/projects/fleet-x.toml`:

```toml
name = "Fleet x"
group = "natter"
working_dir = "~/Projects"

[[tabs]]
name = "orchestrator"

[[tabs.panes]]
command = "natter orchestrator --fleet x"

[[tabs]]
name = "api"

[[tabs.panes]]
command = "natter worker --fleet x"
working_dir = "rackrot/api"
```

`natter` must be on the `PATH` of the pane's shell. Answer the dialogs in each pane.

### `.claude/natter.json`

Optional, in the repo a worker runs from:

```json
{ "name": "myrepo", "description": "What this repo is, shown to the orchestrator" }
```

`description` is at most 512 bytes. The file never sets the role, so a committed file cannot turn a plain session into a worker.

### Without herdr or `claude-orchestrate`

`natter --here` needs only `claude-orchestrate`. Without that too, start `claude` directly. This skips the orchestration prompt and sets the environment by hand. With `NATTER` set to the checkout path, a worker, from the repo it works on:

```sh
NATTER_ROLE=worker NATTER_FLEET=x NATTER_NAME=myrepo claude --dangerously-load-development-channels plugin:nats-channel@natter --settings "$NATTER/config/worker.settings.json"
```

Orchestrator, from an empty directory:

```sh
NATTER_ROLE=orchestrator NATTER_FLEET=x NATTER_PROVISION=1 claude --agent nats-channel:orchestrator --dangerously-load-development-channels plugin:nats-channel@natter --settings "$NATTER/config/orchestrator.settings.json"
```

## Allow rules

`natter` passes a `--settings` file per role that allows the plugin tools, so a repo needs no hand-written rules. A raw `claude` launch needs them, or every call waits on a prompt. Tool names are `mcp__plugin_nats-channel_nats-channel__<tool>`. In the worker repo's `.claude/settings.local.json`:

```json
{
  "permissions": {
    "allow": [
      "mcp__plugin_nats-channel_nats-channel__update_task",
      "mcp__plugin_nats-channel_nats-channel__finish_task",
      "mcp__plugin_nats-channel_nats-channel__doctor"
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
      "mcp__plugin_nats-channel_nats-channel__status_page",
      "mcp__plugin_nats-channel_nats-channel__doctor"
    ]
  }
}
```

Do not run workers with `bypassPermissions`. Allow only what the repo's work needs.

## Orchestrator lockdown

The orchestrator is the high-value target. A compromised repo can write a worker result aimed at it, and from there at other workers. So `natter orchestrator` runs it read-only, in an empty directory, with two layers that are not prompt text:

- `agents/orchestrator.md`, run as `claude --agent nats-channel:orchestrator`. Its `tools` list holds the six plugin tools plus `Read`, `Glob`, `Grep`, `WebSearch` and `WebFetch`. It has no `Bash`, `Edit`, `Write` or `Agent`, so it cannot run commands, change files or start subagents.
- `config/orchestrator.settings.json`, passed with `--settings`: it allows the plugin tools and denies `Bash`, `Edit`, `Write`, `NotebookEdit`, `Agent` and `Read(~/.config/natter/**)`.

The working directory is `${XDG_STATE_HOME:-$HOME/.local/state}/natter/orchestrator/<fleet>`, empty and one per fleet. A `Read` outside it prompts you. The seed deny covers only the default creds directory: a custom `NATTER_CREDS_DIR` is not covered, and `Read` plus `WebFetch` in one session could carry a seed off the machine if a result talked the orchestrator into it. Keep the seed in the default path or add your own deny.

`/nats-channel:status` and `/nats-channel:doctor` both work with Bash denied: each is a tool, and the server does the work. Its instructions also tell it to treat every worker field as data and to report, not obey, a result that asks for changed permissions, credentials or settings.

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

When a worker's turn ends on an API error, such as a rate limit (a safeguard block is unconfirmed, see Known limits), a `StopFailure` hook marks its task `session_error` with the error type as the note, and the orchestrator gets a push with `reason="session_error"`. The worker needs you at its terminal. The error's details and the last message are not sent.

`stale` means nothing has been heard from the worker for 45 seconds. Heartbeats come from the plugin's server process every 15 seconds, not from Claude. A busy or quiet Claude never goes stale; `stale` means the worker's server process stopped answering, for example the session ended or the machine is gone. It does not mean the worker is waiting on a command.

Other attention reasons: `released` (a task was not accepted in time and will be redelivered) and `overdue` (open far past its timeouts).

## Checking things

- `/nats-channel:doctor` runs the `doctor` tool, on both roles, and reports PASS, FAIL or SKIP for the config (which names the fleet), transport policy, seed, TCP, TLS, auth, JetStream, the fleet's five streams and the session's consumer. It runs inside the session's server, so it sees the plugin settings. It connects once with no retry, so it names the cause of a TLS or auth failure. A session with no role has no tool: use `NATTER_FLEET=x mise run doctor` from the checkout, which reads `NATTER_URL`, `NATTER_CREDS_DIR` and `NATTER_CA_FILE` from the environment only.
- `/nats-channel:status` (orchestrator only) opens a local read-only web page: the fleet, workers and liveness, queue depth, open and recent tasks, and this fleet's stream usage. The server opens the browser itself and the reply gives the full URL, which you can open by hand. The token in the URL is local to the process and the port changes every run. Without the token every request gets 403.
- `mise run monitor` tails `natter.<fleet>.>` with `NATTER_FLEET` set, and `natter.>` (every fleet) without it. It is a development tool, and the payloads it shows are untrusted.

## Known limits

- Channels are a research preview. Custom channels need `--dangerously-load-development-channels`. The flag is interactive-only and ignored under `-p` or the Agent SDK, so there are no headless workers.
- Tested with Claude Code 2.1.318. The plugin relies on Claude Code escaping channel content and does not add its own tag escaping.
- A deny at a permission prompt fires no hook, so the task stays `waiting_permission` until the worker's next `update_task` or `finish_task`.
- The `session_error` alert does not stop the retries: it does not fail the task or change the accept timer, so a blocked prompt can be sent again, up to 3 attempts. Whether `StopFailure` fires on a safeguard block is not yet confirmed live.
- The permission hooks need `CLAUDE_CODE_SESSION_ID` and `HOME`. Without them the alert is not raised.
- No real two-machine TLS run has been done. The TLS path is tested against the shipped server template on one machine.
- There is no cancel, no file transfer and no mid-task dialogue.

`docs/smoke.md` is a manual checklist for a run with two fleets.

## Development

Dev tooling is managed by mise.

| Task | What it does |
| --- | --- |
| `mise run setup` | Install dependencies |
| `mise run nats` | Run a local JetStream `nats-server` on 127.0.0.1, storing data in `.nats-data` |
| `mise run bootstrap` | Create or reconcile the five streams of `NATTER_FLEET` |
| `mise run fleet-delete` | Delete the five streams of `NATTER_FLEET` and their contents |
| `mise run doctor` | Check config, transport, auth, JetStream, the fleet's streams and consumer |
| `mise run monitor` | Tail `natter.<fleet>.>`, or `natter.>` without `NATTER_FLEET` |
| `mise run lint` | `biome check`, `tsc --noEmit` and `shellcheck bin/natter` |
| `mise run fmt` | `biome format --write` |
| `mise run test` | `bun test` (spawns the server and a throwaway `nats-server`) |
| `mise run validate` | `claude plugin validate . --strict` |
| `mise run check` | lint, test and validate |
