# Requirements

- Use one canonical public Watcher engine for local CLI use and application integrations.
- Keep the canonical authoring checkout separate from machine-local configuration and state.
- Let applications own their automation definitions and downstream actions. Use Watcher to judge streams and deliver findings.
- Offer 3 presets: eco, fast, and thorough. Default watches to eco.
- Use Luna for base stream judgments. Permit Sol for escalation, not for routine stream screening.
- Batch eco and thorough input every 300 seconds. Use medium effort for eco and high effort for thorough. Use Flex for both.
- When a single Flex request fails on the provider side, resend that request once on the standard tier.
- Batch fast input every 5 seconds. Use low effort and the provider's Fast inference tier.
- Own preset definitions in Watcher. Let applications read them through the CLI.
- Implement Watcher in Go as a standalone primitive that plugs into other systems, with an optional typed TypeScript client.
- Keep watch definitions in YAML files.
- Make adding a new preset a simple, ordinary operation.
- Let agents create watches themselves. Keep setup easy.
