---
description: Check this session's NATS connection, auth, streams and consumer
allowed-tools: mcp__plugin_nats-channel_nats-channel__doctor
---

Call the doctor tool yourself, directly, not through a subagent. Report each check it returns as PASS, FAIL or SKIP, with the reason for every FAIL. A FAIL is a normal result, not a tool error. If there is no doctor tool, this session has no natter role: say so, and suggest `NATTER_FLEET=name mise run doctor` from a shell in the natter checkout.
