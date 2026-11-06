# Fleets, launcher and a read-only orchestrator agent

Status: plan, not implemented. Base: `main` at 1974d36. Follows `plans/nats-channel.md`, and replaces two of its decisions: 3.1 "the account is the fleet boundary" (a fleet is now a set of streams on one account) and the out-of-scope line on herdr integration.

## 1. Outcome and definition of done

One NATS account serves several orchestrators, each with dedicated workers, and nothing crosses between them by accident. Each session starts from one command that also fits a herdr-plus project file. The orchestrator cannot change anything but the task queue. Personal use, one user: there is no isolation against a hostile session, and none is added.

Done when all of these are observable:

1. Two fleets share one server, both with a worker named `api`. A task sent in fleet `a` reaches only `a`'s `api`, and its result reaches only `a`'s orchestrator.
2. `list_workers`, `list_tasks`, `get_result`, the attention sweep and the status page of one fleet show nothing of the other.
3. `natter worker --fleet x ~/repo` and `natter orchestrator --fleet x` each start a working session, from a plain terminal (new herdr workspace) and from inside a herdr pane (this pane), including a pane opened by a herdr-plus project.
4. A worker started that way has the claude-orchestrate settings: orchestration prompt, `CLAUDE_ORCHESTRATE=1`, bash with `agent.bashrc`.
5. The orchestrator runs as `nats-channel:orchestrator`. Bash, Edit, Write and Agent are absent from its tool list and denied by settings. Its natter tools work, and pushes reach it.
6. A hook-written status lands in the session's own fleet.
7. `mise run check` passes.

Out of scope: an `up` command or fleet config file, per-fleet credentials or ACLs, cross-fleet views, moving a task between fleets, more than one orchestrator per fleet, migrating stored tasks or results.

## 2. Evidence the plan rests on

Baseline at 1974d36: `mise run test` 603 pass, 0 fail, 226s; tree clean. Tools: Bun 1.4.4, `nats-server` 2.15.2, `@nats-io/*` 3.4.0, Claude Code 2.1.318, herdr 0.9.5, herdr-plus 0.1.24, orchestration plugin 0.3.7. Spikes ran in `/tmp` against a throwaway server. Binary facts are internal and may change.

| Fact | Source |
| --- | --- |
| Two streams can hold the same durable `task-w` with different filters, and a stream name may hold a hyphen (`NATTER_A-B_TASKS`) | spike |
| On one shared stream a second `add` of `task-w` with another filter fails 10148, and the `update` that `ensureConsumer` runs next swaps the filter. Two fleets with a worker of one name would take each other's queue | spike |
| A raw newest-2000 window on a shared `sent` stream held 3 of a quiet fleet's 5 records beside a busy fleet. `getBatch` with `next_by_subj` filters but walks forward only | spike |
| `max_bytes` reserves against `max_file_store` at create: the stream past the limit fails 10047 `insufficient storage resources available` | spike |
| `natter.default.task.*` does not overlap legacy `natter.result.*.*`. `natter.result.task.*` does, 10065: a fleet named `result` or `status` cannot be created while the legacy streams exist | spike |
| The plugin loads in place from this checkout. Every running server has `--cwd /home/buck/Projects/rackrot/natter`, though `installed_plugins.json` names a stale cache copy. A relative-path plugin in a local-path marketplace reads source at each session start, whatever its version | `ps`, `/proc/<pid>/cwd`; plugin loading docs |
| Files in a plugin's `bin/` are on the Bash tool's PATH while the plugin is enabled | plugin manifest docs |
| A main-thread agent's prompt replaces the default system prompt. `--agent` persists on resume | sub-agents docs; binary `Uyr` |
| Plugin agents ignore `hooks`, `mcpServers`, `permissionMode` and `initialPrompt` | sub-agents docs |
| `tools` takes MCP tools by full name, `mcp__plugin_<plugin>_<server>__<tool>`, or `mcp__<server>__*` | sub-agents and MCP docs |
| "If you omit `Agent` from the `tools` list entirely, the agent can't spawn any subagents", and this covers `--agent`. `Agent` in `permissions.deny` applies too | sub-agents docs |
| With ToolSearch unavailable, tool search turns off (`mcp_search_unavailable`), so listed MCP tools load inline | binary |
| MCP `instructions` reach the model as an `mcp_instructions_delta` attachment, apart from the system prompt | binary |
| The channel handler is registered per MCP client and enqueues into the session queue with no check on the agent | binary |
| `--settings` sits above user and project files, and permission lists merge across levels | settings docs |
| The dev flag shows a full-screen warning dialog that needs a keypress. An agent launched in a pane stops there until answered | channels docs; binary |
| Hooks already inherit the session env: the `NATTER_ROLE` guards in `hooks/hooks.json` work live | S12 of the prior plan |

**herdr-plus** is `cloudmanic/herdr-plus`, a herdr plugin (`~/.local/bin/herdr-plus`, config `~/.config/herdr/plugins/config/cloudmanic.herdr-plus/`). Its `projects/*.toml` files declare a workspace: tabs and up to 4 panes per tab, each with `working_dir` and a `command`. Opening a project types each command into a shell at the prompt and presses Enter (`runCommand` in `herdr.go`). It waits for a quiet screen, checks the echo and retries with ctrl+u, because a fresh pane can drop the first keystrokes. It has no env field. The user's projects run `command = "ct"`, a zsh alias for `claude-orchestrator`. So one plain command per session, with the fleet in its arguments and `HERDR_ENV=1` already set in the pane, is exactly what fits. A project file is the `up` this design leaves out.

**claude-orchestrate 0.3.7**, by route:

| Route | Prompt | `CLAUDE_ORCHESTRATE` | Shell | `NATTER_*` from the caller |
| --- | --- | --- | --- | --- |
| In a herdr pane, or no herdr: `exec env "${launch_env[@]}" claude ...` | appended unless `-a` | `1`, cleared with `-a` | bash, `agent.bashrc` | **Reach claude**, inherited |
| `-H` outside herdr, or `-t`: `herdr <kind> create --env ...`, then `herdr agent start` | same | same | same | **Lost**: only `launch_env` becomes `--env`, and a pane does not inherit the caller |

`-a AGENT` runs `--agent`, skips the prompt and sets `CLAUDE_ORCHESTRATE=`. `CLAUDE_ORCHESTRATE_AGENT` would silently take over a worker launch. Arguments after `DIR --` go to claude.

## 3. Decisions

### 3.1 Fleet scoping: one set of streams per fleet

A fleet is five streams and a subject prefix.

| Now | Fleet `x` |
| --- | --- |
| `NATTER_TASKS`, `NATTER_RESULTS`, `NATTER_STATE`, `NATTER_PRESENCE`, `NATTER_SENT` | `NATTER_X_TASKS`, `NATTER_X_RESULTS`, `NATTER_X_STATE`, `NATTER_X_PRESENCE`, `NATTER_X_SENT` |
| `natter.task.<w>` | `natter.x.task.<w>` |
| `natter.result.<w>.<id>` | `natter.x.result.<w>.<id>` |
| `natter.status.<w>.<id>` | `natter.x.status.<w>.<id>` |
| `natter.presence.<w>` | `natter.x.presence.<w>` |
| `natter.sent.<id>` | `natter.x.sent.<id>` |
| durable `task-<w>`, `results` | unchanged, because a durable belongs to its stream |

Streams need the subject prefix regardless, because two streams cannot capture one subject. Uppercasing is lossless, since a fleet name has no uppercase letters, and the name is injective because the suffix is fixed.

The deciding reason is structural. Every reader in `orchestrator.ts` already reads "the stream", so giving it a stream that holds one fleet makes each of them correct with no logic change: `recordWindows`, `queuedTaskIds`, `newestPresence`, `addressedState`, `sweep`, the 200 and 2000 caps and `truncated`. Nothing can cross, because a session never names another fleet's stream.

| | Per-fleet streams (chosen) | Prefix on shared streams (rejected) |
| --- | --- | --- |
| Crossover | Impossible by construction | Each reader must filter, and a miss is exactly this bug |
| Durables | Unchanged | Must embed the fleet, or `update` swaps the filter (spike) |
| Record walks | Unchanged | Diluted (spike). Needs a backward walk with `next_by_subj` and a new window cap, which changes the bounded-time rule of plan 3.6 and `truncated` |
| `discard: new` | One fleet's full stream stops that fleet | One fleet's full `RESULTS` or `SENT` refuses every fleet |
| Workqueue | Unchanged | Works, with non-overlapping filters |
| Presence | Unchanged | Forward walk needs a filter |
| Status page usage | Means this fleet | Means all fleets |
| Retiring a fleet | Delete five streams | Purge five subject filters |
| Storage | **1.81 GiB reserved per fleet** | Flat 1.81 GiB |
| Bootstrap | **Per fleet** (3.2) | Once |

The two costs are real and bounded. Reservation counts against `max_file_store` and is not disk used; the shipped server template sets no limit. 3.2 removes the bootstrap step for the normal path. Also rejected: one NATS account per fleet, which the user ruled out, and an orchestrator token in shared subjects, which keeps every shared-stream cost above.

### 3.2 Who creates a fleet's streams

An orchestrator started by `natter` creates its fleet's streams when **none of the five exist**. `natter orchestrator` sets `NATTER_PROVISION=1`. `mise run bootstrap` stays the reconcile tool, with `NATTER_FLEET=x`. Workers never create streams.

A partial set is never filled in by a session. Recreating a lost `NATTER_X_SENT` beside live results would get every stored result terminated as having no record (plan 3.6), so a partial set logs and reports the bootstrap hint, as today. A session never updates a stream either: that stays with bootstrap, so a session on an old checkout cannot revert a setting.

Worker first is fine: it retries every 10s as it does now, and its next heartbeat lands once the streams exist. A mistyped `--fleet` on an orchestrator costs a reservation until `NATTER_FLEET=typo mise run fleet-delete`.

Rejected:
- `natter` runs bootstrap itself: `mise run bootstrap` reads env only, never plugin settings, so with a URL kept in plugin settings the launcher would create streams on the default server. The session already has the right transport.
- `mise run bootstrap` only: a manual step before every new fleet, in a design meant to be one line in a project file.
- Every session creates: a mistyped worker would reserve 1.81 GiB, and it breaks "workers never create streams".
- Unconditional for every orchestrator: it would change plain launches and the missing-stream behaviour existing tests pin. `NATTER_PROVISION` keeps that behaviour for anything not started by `natter`.

### 3.3 Default, and the existing streams

`NATTER_FLEET` unset means fleet `default`, with streams `NATTER_DEFAULT_*` and subjects `natter.default.*`. There is one shape everywhere, so a plain launch and `mise run bootstrap` keep working. `natter` itself requires `--fleet` (or an exported `NATTER_FLEET`), because a forgotten flag quietly landing in `default` is the mistake a launcher should catch.

Clean break. The legacy streams are not read, migrated or reused. Upgrade, once, on each server: finish or abandon open work, stop all sessions, delete all five legacy streams together, then start sessions.

```sh
for s in TASKS RESULTS STATE PRESENCE SENT; do nats --server "$URL" stream rm -f "NATTER_$s"; done
```

Queued tasks and stored results in them are lost. Delete them first: that frees their 1.81 GiB, and until then a fleet named `result` or `status` fails 10065.

Rejected:
- Unset means the legacy names: two subject shapes in `parseSubject`, and a fleet named `task` or `result` becomes ambiguous.
- `NATTER_FLEET` required everywhere: a plain session goes inert and every test env needs the variable, for no gain over the launcher's check.
- Updating legacy `subjects` in place: it works (spike), but leaves records that fail to parse on every walk for 30 days, and the `NATTER_` names mean nothing.

### 3.4 Fleet names and where they resolve

`FLEET_PATTERN = /^[a-z0-9][a-z0-9-]{0,31}$/` in `src/config.ts`. No `_`, so a name reads cleanly in `NATTER_<FLEET>_TASKS`. No reserved words: the legacy overlap goes away with the upgrade step.

Resolved from `NATTER_FLEET` env only, then `default`. An invalid value makes a session inert with a reason, and makes `bootstrap` and `doctor` fail. `natter` checks first and exits non-zero, so a bad name never becomes a quiet inert session. `tests` hold the bash and TypeScript patterns together.

Rejected:
- `fleet` in `.claude/natter.json`: a committed file could move a worker into another fleet, which is the accidental crossover this work removes, and one repo may serve two fleets. The same reasoning kept `role` out of that file.
- Plugin `userConfig`: per machine, and one machine runs several fleets.

### 3.5 Protocol version

`v` stays 1. No envelope changes. `v` guards a reader against a body it cannot parse. Here old and new sessions cannot meet, because a new session has no read path to legacy streams. The subject token is the identity, as in plan 3.1, and a fleet token that fails the pattern fails `parseSubject`. `SubjectInfo` gains no field: no reader needs it, because the stream already fixes the fleet.

### 3.6 Nothing assumes a name is globally unique

| Place | Scope | Change |
| --- | --- | --- |
| durable `task-<name>` | its stream | none |
| `natter.<fleet>.presence.<name>`, status, result | its fleet | prefix only |
| `_INBOX.<name>` in `connection.ts` | no ACLs, and each inbox ends in a nuid, so a shared prefix cannot collide | none |
| connection name `natter-<name>` | monitoring text only; tests match it | none |
| `sendByWorker` buckets, `raised`, `pushed` | per process | none |
| held file | per session id | path none, see 3.8 |
| status page port | port 0 per process | none |
| herdr agent names | unique among live agents, but only `agent start` sets one and `natter` does not call it (3.9) | none |

### 3.7 Showing the fleet

| Surface | Change |
| --- | --- |
| `instructions` | Both roles name the fleet. The name matches its pattern, so it adds no markup |
| `doctor` | The `config` line names the fleet, and streams and consumer are the fleet's |
| Status page | `fleet` in the snapshot and shown in the header. Stream usage is this fleet's |
| `server.ts` log | `role=... name=... fleet=...` |
| Hints | `NATTER_FLEET=x mise run bootstrap` |
| `mise run monitor` | `natter.<fleet>.>` with `NATTER_FLEET` set, otherwise `natter.>`, all fleets. A tail is most useful wide |

Not added: `fleet` in tool replies, `meta` or presence. One session is in one fleet, and instructions already say which.

### 3.8 Hook path

`hooks/hooks.json` needs no change: its guards are per session. `src/held.ts` keeps its path. But `src/hook.ts` builds `statusSubject(config.name, id)`, and that subject now needs the fleet, so the hook does change. Add `fleet` to `HeldFileSchema`, written by the worker server, beside the url and seed path that are already there so the hook uses the server's own settings. A held file without it fails the schema and the hook exits quietly, which is the existing failure mode.

### 3.9 Launcher: wrap claude-orchestrate, one launch path

`bin/natter` is bash. It holds no copy of claude-orchestrate's prompt, shell or env handling.

```text
natter orchestrator --fleet F [-w] [--here] [-d | --attach] [-n] [-- CLAUDE_ARGS...]
natter worker --fleet F [--name N] [-w] [--here] [-d | --attach] [-n] [DIR] [-- CLAUDE_ARGS...]
```

| Where it runs | What happens |
| --- | --- |
| `--here`, or `HERDR_ENV=1` without `-w` (a pane, including a herdr-plus project) | Sets `NATTER_*` on an `exec` of `claude-orchestrate --no-herdr ... DIR -- <channel flag> --settings <file> "$@"`. The only path that launches claude |
| Otherwise, or `-w` | `herdr workspace create --cwd DIR --label fleet:name`, then `herdr pane run` of `natter ... --here ...` in the new pane. It uses `--no-focus` and does not attach, since focus is shared by every open herdr client; only when this run started the herdr server (no client can exist) or with `--attach` does it use `--focus` and attach. `-d` never attaches and excludes `--attach`; inside herdr it never attaches. It prints one stderr line naming the workspace label |

A new workspace re-enters `natter --here`, so both paths are one path, and role, name, fleet and the channel flag reach claude by construction. `--env` carries only connection overrides that are set (`NATTER_URL`, `NATTER_CREDS_DIR`, `NATTER_CA_FILE`, `NATTER_ALLOW_PLAINTEXT`). `NATTER_ROLE` deliberately does not go on the pane: that shell outlives claude, and a plain `claude` typed there later would be a deaf worker pulling tasks.

The launcher also: unsets `CLAUDE_ORCHESTRATE_AGENT` for a worker, and `NATTER_NAME` when no `--name` is given, so `.claude/natter.json` and the directory name still decide; sets `NATTER_NAME=orchestrator` for an orchestrator; passes `--no-herdr` so `CLAUDE_ORCHESTRATE_HERDR=1` cannot divert into a route that drops the env; and uses `$XDG_STATE_HOME/natter/orchestrator/<fleet>` as the orchestrator's empty, per-fleet directory.

The route choice for claude-orchestrate:

| Option | Verdict |
| --- | --- |
| Wrap, always through its in-pane route | **Chosen.** Inherited env reaches claude, the prompt and settings follow the installed plugin, and **it needs no change** |
| Add `-e KEY=VAL` passthrough to it and use `-H` | Rejected. Out of this repo (`Adminhead/claude-plugins`), and `NATTER_ROLE` on a pane has the deaf-worker problem above. Worth doing only if another tool needs it |
| Copy its logic, or call `herdr agent start` directly | Rejected. `agent start` runs `claude` itself, so the prompt strip and env would be copied and would drift from the settings the user wants reused |

The one copied piece is its 6 lines that start the herdr server. `natter` does not get a herdr agent name, because `agent start` cannot run a wrapper. herdr still detects the session: agents started through `ct` show in `herdr agent list` with no name.

`pane run` right after `workspace create` risks herdr-plus's dropped-keystroke race. So: `herdr pane wait-output --regex '\S'` first, then one `pane run`, then wait for a `natter: starting` line. On timeout print the exact command to paste, and do not retry blind, because a second send could land in Claude's prompt. S5 measures this before deciding anything more.

`config/worker.settings.json` allows `update_task` and `finish_task`, so a repo needs no hand-written `settings.local.json`.

### 3.10 Orchestrator agent

`agents/orchestrator.md`, run as `claude --agent nats-channel:orchestrator` through `claude-orchestrate -a`.

```yaml
name: orchestrator
description: Read-only main-session orchestrator for a natter fleet, started with `natter orchestrator`. Not for spawning as a subagent.
tools: <the five natter tools by full name>, Read, Glob, Grep, WebSearch, WebFetch
```

Enforcement has two layers, neither of them prompt text:

1. The `tools` allowlist. No `Agent` means no subagents.
2. `config/orchestrator.settings.json`, passed with `--settings`: `allow` for the natter tools, `deny` for `Bash`, `Edit`, `Write`, `NotebookEdit`, `Agent` and `Read(~/.config/natter/**)`. `permissionMode` in the agent file would be ignored, so permissions live here.

The last deny is new. Read and WebFetch together in one agent can carry the seed off the machine if a result ever talks the orchestrator into it. The user chose those tools and that stays; the deny closes the one secret on the default path, and a custom creds directory is not covered.

The agent body covers the role, the read-only stance and how to use the read and web tools. The protocol stays in `src/instructions.ts`, the single source, because it also serves a plain launch and arrives as an attachment. If the live check shows `instructions` do not reach the agent, the text moves to a shared place, not a second copy.

`ToolSearch` stays out of `tools`. A test keeps `ORCHESTRATOR_TOOLS`, the agent `tools` and the settings `allow` equal, so a new tool cannot go missing quietly.

Doctor: the Bash deny stops `/nats-channel:doctor`, and `status_page` already works without Bash. S8 makes doctor a tool on both roles. It runs inside the server, so it also sees plugin settings, which removes a known limit in the README. Without S8, `NATTER_FLEET=x mise run doctor` from a shell remains the route.

Rejected:
- `--tools` and `--append-system-prompt-file` instead of an agent: the user chose `agents/`. This stays the fallback if a live check fails (S6).
- `--disallowedTools` flags instead of a settings file: the same effect with no allow rules, and harder to test.
- A generated `.claude/settings.json` in the state directory: writes a file for what a flag does.

## 4. Reuse analysis

| Reviewed | Decision |
| --- | --- |
| `src/protocol.ts` subject builders, stream constants | Extend: one `fleetScope(fleet)` returning builders and names, built once per session |
| `src/bootstrap.ts` stream definitions, `bootstrapStreams` | Extend: `streamsFor(fleet)` keeps the settings in one place |
| `resolveConfig`, `adminConfig`, `isReserved` | Extend |
| `ensureConsumer`, consume loops, every reader | Reuse unchanged |
| `HeldFileSchema` url and seed pattern | Extend |
| `startStatusPage`, `readStatus` | Extend: one field |
| `claude-orchestrate` | Reuse as a wrapped command |
| herdr server start in `claude-orchestrate` | Copy 6 lines, named in a comment |
| herdr-plus `runCommand` echo probe | Not copied unless S5 shows the race |
| `test/support/session.ts`, `nats.ts` | Reuse. Add `test/support/fleet.ts` |
| Test runner | Reuse `bun test` to spawn `bin/natter`. No bats |

Approach: extend. Duplication risk: low. The one duplicated fact is the fleet and name patterns in bash and TypeScript, held together by a table test.

## 5. Slices

Each slice leaves `mise run check` green. Tests follow plan section 5: a real `nats-server`, assert stream state, published messages, tool results and pushes, never wording. Run with `mise run test`. Part A is S1 to S4, Part B is S5, Part C is S6, S7 and S8. S9 closes.

### S1. Fleet config

- Files: `src/config.ts`, `test/config.test.ts`.
- Depends on: nothing.
- Acceptance: `FLEET_PATTERN`, `DEFAULT_FLEET`; `Config.fleet` set by `resolveConfig` and `adminConfig`; unset gives `default`; empty counts as unset; invalid is inert with a reason, and throws from `adminConfig`; `.claude/natter.json` cannot set it.
- Cheapest check: `bun test test/config.test.ts`.
- Worker: mechanical.

### S2. Fleet scope in subjects, streams and every caller

- Files: `src/protocol.ts`, `src/bootstrap.ts`, `src/worker.ts`, `src/orchestrator.ts`, `src/doctor.ts`, `src/hook.ts`, `src/held.ts`, `src/presence.ts` if touched, `test/support/fleet.ts`, every test file.
- Depends on: S1.
- Design: one atomic slice, because the subject shape cannot change halfway. `fleetScope(fleet)` replaces the module constants and builders. `streamsFor(fleet)` replaces `STREAMS`. `parseSubject` takes `natter.<fleet>.<kind>...`. Tests: `test/support/fleet.ts` re-exports the `default` scope under today's names, so most test files change their import only, and a sed moves string literals to `natter.default.*` and `NATTER_DEFAULT_*`.
- Acceptance: the 603 tests pass with the default fleet and no change to what they assert; `git diff src/orchestrator.ts` shows names swapped and no reader logic changed; no `natter.task`-style literal or `NATTER_TASKS`-style constant remains in `src`; `parseSubject` rejects a bad fleet token; bootstrap with `NATTER_FLEET=x` creates five `NATTER_X_*` streams with the same settings as `default`, and leaves other fleets alone; `HeldFile` carries the fleet, and a hook publishes on that fleet's subject.
- Cheapest check: `mise run test`. This is the one slice that needs the whole suite, because it touches every file.
- Worker: mechanical, high volume.

### S3. Provisioning and retiring a fleet

- Files: `src/bootstrap.ts`, `src/orchestrator.ts`, `src/config.ts`, `mise.toml`, `test/bootstrap.test.ts`, `test/orchestrator.test.ts`.
- Depends on: S2.
- Acceptance: with `NATTER_PROVISION=1` and none of the five, an orchestrator creates the set, and a task round-trips with no bootstrap step; a worker started first recovers once the set exists; with any stream present nothing is created, and the existing hint stays; a recreated `NATTER_X_SENT` never happens, proved by deleting it with results stored and asserting no stream reappears; no stream is ever updated by a session; without the variable behaviour is unchanged; workers ignore the variable. `NATTER_FLEET=x mise run fleet-delete` deletes the five streams of `x` and refuses when `NATTER_FLEET` is unset.
- Cheapest check: `bun test test/bootstrap.test.ts`, then the provisioning cases in `test/orchestrator.test.ts`.
- Worker: design judgment.

### S4. Isolation and surfaces

- Files: `test/fleets.test.ts`, `src/instructions.ts`, `src/server.ts`, `src/status-page.ts`, `src/doctor.ts`, `mise.toml`, `test/status-page.test.ts`, `test/doctor.test.ts`.
- Depends on: S2. S3 for the provisioning case.
- Acceptance, two fleets on one server, both with worker `api`: a task sent in `a` is pushed only to `a`'s `api`, and `b`'s `api` keeps idling; the result is pushed only to `a`'s orchestrator; `b`'s `list_workers`, `list_tasks` and `get_result` show nothing of `a`; a stale worker in `a` raises attention only in `a`; `send_task` to a name that exists only in the other fleet is `isError`; the two `task-api` durables have different filters on different streams; a full `NATTER_A_RESULTS` leaves `b` able to store; the snapshot has `fleet`; `doctor` checks the fleet's streams. Add one line to worker `instructions`: call `update_task` and `finish_task` yourself, not through a subagent, because these workers now run the orchestration prompt.
- Cheapest check: `bun test test/fleets.test.ts`.
- Live check once: `NATTER_FLEET=x mise run monitor` tails one fleet, and unset tails both.
- Worker: design judgment.

### S5. Launcher

- Files: `bin/natter`, `config/worker.settings.json`, `test/launcher.test.ts`, `test/support/stubs.ts`, `mise.toml` (`shellcheck` tool and lint step).
- Depends on: nothing in the code. S2 for a real session.
- Channel flag: the launcher uses `--channels plugin:nats-channel@natter` with no fallback to the development flag. Live-confirmed with `/etc/claude-code/managed-settings.json` (`channelsEnabled`, `allowedChannelPlugins`), which README Install now documents.
- Live check first, before the workspace route is built: from a plain terminal script, 20 rounds of `workspace create`, the planned wait, `pane run`, reading the result. Clean: ship as designed. Any mangled command: add herdr-plus's echo check, with a retry guarded on `herdr pane process-info` showing the shell in the foreground, so a retry cannot reach Claude's prompt.
- Test approach. `--dry-run` goes through the same `run` function as the real path, as in claude-orchestrate, and prints each command with `printf %q`. `test/launcher.test.ts` spawns `bin/natter` with `Bun.spawn`, with a stub directory first on `PATH` and a temp `HOME` and `XDG_STATE_HOME`, and splits the line with `bash` to get argv. No test depends on the real `herdr` or `claude`.
  1. Dry-run table: role by route (in pane, plain, `--here`, `-w`, `-d`), asserting env and argv: `NATTER_ROLE`, `NATTER_FLEET`, `NATTER_NAME`, `-a nats-channel:orchestrator`, `--no-herdr`, DIR, the channel flag, `--settings`, trailing claude args in order.
  2. Stub run, not dry: stub `claude-orchestrate` writes its argv and env to a file, and the result must equal the dry-run line. This stops the dry-run from drifting.
  3. Stub `herdr` records calls and returns canned JSON: workspace create carries no `NATTER_ROLE`, forwards a set `NATTER_URL`, and `pane run` holds `--here` with the same fleet and name, quoted for a directory with a space.
  4. Contract, skipped when `claude-orchestrate` is absent: the real script's `-n` output for our arguments ends at `claude` with the channel flag, with `CLAUDE_ORCHESTRATE=1` and `--append-system-prompt-file` for a worker, and `--agent` and `CLAUDE_ORCHESTRATE=` for an orchestrator.
  5. Agreement table: a list of good and bad fleet and worker names gives the same verdict from `bin/natter -n` and `resolveConfig`.
  6. Errors: no fleet, a bad fleet, a missing DIR and a missing `claude-orchestrate` exit non-zero. `-n` creates no directory.
- Acceptance: the above; a worker launch with `CLAUDE_ORCHESTRATE_AGENT` set still takes the prompt route; a worker without `--name` has `NATTER_NAME` unset; `shellcheck bin/natter` is clean.
- Cheapest check: `bun test test/launcher.test.ts`, then `shellcheck bin/natter`.
- Live check once: a herdr-plus project with an orchestrator and a worker pane opens, both connect, and a task round-trips. A project file like this goes in the README:

  ```toml
  name = "Fleet x"
  group = "natter"
  working_dir = "~/Projects"

  [[tabs]]
  name = "orchestrator"
  command = "natter orchestrator --fleet x"

  [[tabs]]
  name = "api"
  working_dir = "rackrot/api"
  command = "natter worker --fleet x"
  ```

- Live check once: the orchestration prompt and worker `instructions` coexist. The worker calls `update_task` first, delegates to a subagent, and calls `finish_task` from the main thread. A permission prompt inside a subagent still raises `waiting_permission`.
- Worker: design judgment, infra.

### S6. Orchestrator agent

- Files: `agents/orchestrator.md`, `config/orchestrator.settings.json`, `bin/natter`, `test/lockdown.test.ts`, `test/launcher.test.ts`.
- Depends on: S5.
- Live check first, before anything else in this slice, with a draft agent file. The plugin loads in place, so a new session sees it. Run `claude --agent nats-channel:orchestrator --settings <file> --dangerously-load-development-channels plugin:nats-channel@natter` with `NATTER_ROLE=orchestrator` and confirm:
  1. The five natter tools are listed and a first `list_workers` works with no `ToolSearch`, including while the server is still in `bun install` at startup.
  2. A result push arrives while idle and starts a turn.
  3. `instructions` reached the model: `/context` lists them, or it can say what `kind="attention"` means.
  4. Bash, Edit, Write and Agent are absent, and asking for each fails.
  5. `/nats-channel:status` works.
  6. `--resume` keeps the agent, the tools and the channel.
- Triggers: if 1 or 2 fails, stop and use the fallback from 3.10 (`claude-orchestrate -p <prompt> -- --tools ...`), which keeps the settings deny, and tell the user. If only 3 fails, move the protocol text so both read it. If 4 fails on settings alone, add `--disallowedTools`.
- Acceptance: a test parses the frontmatter and settings: `tools` holds every `ORCHESTRATOR_TOOLS` name and no `Bash`, `Edit`, `Write`, `NotebookEdit` or `Agent`; `deny` holds those and the seed path; `allow` equals the natter tools; `config/worker.settings.json` `allow` equals `WORKER_TOOLS`. `mise run validate` passes with `agents/`.
- Cheapest check: `bun test test/lockdown.test.ts`, then `mise run validate`.
- Worker: design judgment.

### S7. Orchestrator through the launcher

- Files: `bin/natter`, `test/launcher.test.ts`.
- Depends on: S3, S5, S6.
- Acceptance: `natter orchestrator` sets `NATTER_PROVISION=1`; runs in `$XDG_STATE_HOME/natter/orchestrator/<fleet>`, created unless `-n`; two fleets get two directories; `--settings` names the orchestrator file; extra claude args survive the re-entry.
- Cheapest check: `bun test test/launcher.test.ts`.
- Live check once: `natter orchestrator --fleet new` on a fleet with no streams creates them; `natter worker --fleet new` in a second workspace connects; a task round-trips; the orchestrator is refused a Bash call.
- Worker: mechanical.

### S8. Doctor as a tool

- Files: `src/doctor.ts`, `src/orchestrator.ts`, `src/worker.ts`, `commands/doctor.md`, `config/*.settings.json`, `agents/orchestrator.md`, `test/doctor.test.ts`.
- Depends on: S6.
- Acceptance: `run(env)` is exported and reused; a `doctor` tool on both roles returns the same checks; it reports the URL from plugin settings when no env is set; it opens one extra connection and closes it; `commands/doctor.md` calls the tool and works with Bash denied; `mise run doctor` is unchanged.
- Cheapest check: `bun test test/doctor.test.ts`.
- Worker: mechanical.

### S9. Docs and the live run

- Files: `README.md`, `docs/smoke.md`, `.claude-plugin/plugin.json` (version), `src/config.ts` comments if stale.
- Depends on: all.
- Acceptance: README covers fleets and the name rule, 1.81 GiB per fleet and how to raise `max_file_store`, the upgrade step of 3.3, `natter` usage and the symlink (`ln -s "$PWD/bin/natter" ~/.local/bin/natter`), the herdr-plus example, `fleet-delete`, the dev-channel dialog on every launch, that `natter` is on the Bash tool PATH inside sessions, and "one orchestrator per fleet" replacing "on the same account". Orchestrator lockdown describes the agent and settings. `docs/smoke.md` uses the launcher and gains a two-fleet section with the same worker name in both.
- Cheapest check: run the smoke checklist once, and `mise run check`.
- Worker: docs.

Order: S1, S2, then S3 and S4; S5 can start after S1 because it touches no server code; S6 after S5; S7, S8, S9. Parts B and C do not depend on merging Part A first, except for their live checks.

## 6. Verification

| Condition or risk | Hypothesis | Cheapest reliable evidence | Rerun when | If it fails |
| --- | --- | --- | --- | --- |
| Crossover | Two fleets share nothing | S4 `test/fleets.test.ts` | Subjects, scope or stream names change | Treat as a product defect |
| Existing behaviour holds | A mechanical rename | S2, full suite, unchanged assertions | Any S2 change | Diagnose before widening |
| Provisioning destroys trust | Partial sets are never filled | S3 test with `SENT` deleted | Provisioning or `STREAMS` change | Treat as a product defect |
| Launcher drift | Dry-run equals the real path | S5 tests 2 and 4 | `bin/natter` or claude-orchestrate changes | Fix the launcher; do not patch the test |
| New-pane race | A wait before `pane run` is enough | S5 live loop | herdr update | Add the guarded retry |
| Agent lockdown | Tool list and deny both hold | S6 live check and `test/lockdown.test.ts` | Claude Code update, any new tool | Stop and use the fallback |
| Hook subject | Held file carries the fleet | S2 hook test | `held.ts` or `hook.ts` change | Diagnose before rerunning |

## 7. Material unknowns

| Unknown | Assumption | Least evidence | Trigger |
| --- | --- | --- | --- |
| `pane run` on a fresh pane | Waiting for first output is enough | S5 loop of 20 | Any mangled command: guarded retry |
| Main-thread agent plus plugin MCP plus dev channel | Works as static reading says | S6 list | 1 or 2 fails: fallback in 3.10 |
| herdr restoring a pane after a server restart | Without the env, the plugin goes inert. Nothing is lost, and rerunning `natter` recovers | Restart herdr once during S9 | If it restores a live deaf worker, document it |
| Bare `Agent` accepted in `permissions.deny` | Yes | S6 live check; `claude doctor` | Use `Agent(*)` or `--disallowedTools` |
| `max_file_store` on the real server | Room for the fleets wanted | `nats account info` | 10047: raise it |

## 8. Risks and rollback

- S2 is broad but mechanical. It is one commit, and reverting it restores everything before S3 lands.
- The checkout is the live plugin, for every session on this machine, and it loads at session start. A half-done branch reaches any new session. Only sessions with `NATTER_ROLE` act on it, and plain sessions stay inert. Develop in a worktree, and check out the branch here only for live checks.
- `bin/` in the plugin root puts `natter` on the Bash tool's PATH. A worker could start a session, but nothing new follows from it: it already has Bash. The user chose this path.
- The upgrade step loses whatever is in the legacy streams. Rollback after it: revert, `mise run bootstrap`, and accept the empty streams. Delete the per-fleet streams with `fleet-delete`.
- Each launch stops at the dev-channel dialog, and at the trust dialog the first time in a directory. `-d` leaves a session waiting there. No automation answers them.
- A second orchestrator in one fleet still splits pushes, as a second one on the account does today.

## 9. Baseline, limits, record

- Baseline: 603 pass, 0 fail at 1974d36. No known failures.
- Keep section 2 as the reference; do not re-derive it.
- Claims stop at the versions in section 2, single broker, one machine. S5 and S6 live results go in the PR.
- Stop when each item in section 1 has evidence.

## 10. Critique log

The outside critique did not finish: the reviewer call stalled and was stopped. The plan has had no second opinion. The S5 and S6 live checks are the guard on its least certain parts.

## 11. Decisions from the user

- Each fleet keeps the full 1.81 GiB reservation. A fleet past the server's limit fails to create, and the fix is raising `max_file_store`.
- Clean break on upgrade, as in 3.3.
- Doctor becomes a tool, so S8 stays.

No open questions remain.
