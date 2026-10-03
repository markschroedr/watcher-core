# Requirements

- Use one canonical public Watcher engine for local CLI use and application integrations.
- Keep the canonical authoring checkout separate from machine-local configuration and state.
- Let applications own their automation definitions and downstream actions. Use Watcher to judge streams and deliver findings.
- Offer 3 presets: eco, fast, and thorough. Default watches to eco.
- Use Luna for base stream judgments. Permit Sol for escalation, not for routine stream screening.
- Batch eco and thorough input every 300 seconds. Use medium effort for eco and high effort for thorough. Use Flex for both.
- Batch fast input every 5 seconds. Use low effort and the provider's Fast inference tier.
- Own preset definitions in Watcher. Let applications read them through the CLI.
- Implement Watcher in Go as a standalone CLI, YAML registry, and typed TypeScript process client.
- Keep criteria and actions inline in each watch. Let agents add, remove, apply, enable, and disable watches.
- Use one SQLite runtime database. Commit each judgment's cursor and findings atomically.
- Judge at least once after a crash. Deliver at least once. Let acknowledgement stop repeating notifications.
- Use Responses API function tools with no artificial output caps. Confirm command findings before commit.
- Expose committed findings through an exclusive sequence high-water query with watch and label filters.
- Use one cadence rule: non-empty buffer plus elapsed interval, quiet period, full window, source end, or wake.
- Keep registry generations caller-owned. Report the runner's loaded generation separately.
- Preserve terminal oneshot and budget states across restart. A changed resolved definition re-arms a watch.
- Keep cutover, deployment, private integration code, and machine-local runtime state outside this rewrite.
