---
description: Open the natter status page in the browser
allowed-tools: mcp__plugin_nats-channel_nats-channel__status_page, Bash(xdg-open *), Bash(open *)
---

Call the status_page tool. It returns a local URL that holds a secret token. Open that URL with `xdg-open 'URL'` on Linux or `open 'URL'` on macOS, always in single quotes because the `?` is a glob in zsh, then tell your user the page is open. If the tool returns an error, report it.
