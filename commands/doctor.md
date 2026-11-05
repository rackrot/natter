---
description: Check this session's NATS connection, auth, streams and consumer
allowed-tools: Bash(bun *)
---

Run the natter doctor and report each check it prints as PASS, FAIL or SKIP, with the reason for every FAIL.

!`bun ${CLAUDE_PLUGIN_ROOT}/src/doctor.ts 2>&1`
