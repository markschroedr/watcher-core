# Requirements

- Use one canonical public Watcher engine for local CLI use and application integrations.
- Keep the canonical authoring checkout separate from machine-local configuration and state.
- Let applications own their automation definitions and downstream actions. Use Watcher to judge streams and deliver findings.
- Offer 3 presets: eco, fast, and thorough. Default watches to eco.
- Use Luna for every built-in judge, confirmation, and escalation profile. Never select Sol automatically.
- Batch eco and thorough input every 300 seconds. Use medium effort for eco and high effort for thorough. Use Flex for both.
- Batch fast input every 5 seconds. Use low effort and the provider's Fast inference tier.
- Own preset definitions in Watcher. Let applications read them through the CLI.
