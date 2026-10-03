# Watcher

Watch text. Say what to look for. Get a finding when it happens.

Watcher reads a file, a command, or stdin. A small judge checks the stream against
plain-language criteria. Silence is the normal result. Findings are data, not
instructions. Use the CLI directly or plug it into another system.

## Install

```sh
go install github.com/markschroedr/watcher-core/cmd/watcher@latest
export OPENAI_API_KEY="..."
```

Build locally with `go build -o watcher ./cmd/watcher`. Go 1.26 is required.

## Watch now

```sh
watcher watch --cmd 'journalctl -f -u api' \
  --criterion 'failed=The deployment fails; successful retries stay silent' \
  --preset fast --oneshot --notify

printf 'Deployment failed: rollout aborted.\n' |
  watcher judge --criterion 'A deployment failed' --preset fast --json
```

`watch` accepts one of `--file`, `--cmd`, or `--stdin`. Criteria are repeatable;
`id=text` gives a criterion a name. `--cmd` runs through `/bin/sh -c`.
`--every` and `--quiet` override cadence. `--from-start` includes at most one
window of existing file content. The default budget is $0.50. `watch` prints one
JSON line per committed finding and uses an in-memory database. It never repeats
notifications. `judge` is stateless and returns findings, usage, and cost.

## Keep it running

Definitions live in `~/.watcher/watches.yaml` and `watches.d/*.yaml` or `*.yml`
beside it. Each watch contains its own source, criteria, and actions. Relative
paths resolve next to the defining file. See `watches.example.yaml`.

```sh
mkdir -p ~/.watcher
cp watches.example.yaml ~/.watcher/watches.yaml
watcher check
watcher daemon
# Or install a user service:
watcher service install --env-file /path/to/trusted.env
```

The service uses launchd on macOS or systemd user services on Linux. It loads the
login-shell environment; `--env-file` additionally sources a trusted shell
KEY=VALUE file. `service status` and `service uninstall` manage the same service.
Use `--registry /path/to/watches.yaml` on any command for a separate registry and
runtime database. Installing a service changes the host's Watcher service.

The daemon polls definitions every second. It restarts only watches whose
resolved definitions changed. A broken watch does not stop other watches.
Failed, non-terminal watches restart after 30 seconds.

A file cursor tracks inode and byte position. Rotation and truncation start a new
segment. An optional `start_inode` plus `start_position` is a lower bound;
persisted progress can continue later but cannot resume before it.

A judge runs when there is buffered data and any of these holds: the interval
elapsed, the stream stayed quiet, the window filled, the source ended, or someone
requested a wake. No data means no model call.

## Presets

| Preset | Model | Effort | Tier | Interval |
| --- | --- | --- | --- | --- |
| `eco` (default) | GPT-6 Luna | medium | Flex | 300 s |
| `fast` | GPT-6 Luna | low | Fast (`priority`) | 5 s |
| `thorough` | GPT-6 Luna | high | Flex | 300 s |
| `sol` | GPT-6.1 Sol | medium | standard | escalation/confirmation only |

`watcher presets --json` prints the effective set. Add a preset with an ordinary
`presets:` entry in any registry file. Use all fields shown below; `base_url` is
optional. An entry with a built-in name replaces that built-in. Define each
override once across the registry.

```yaml
presets:
  slower:
    model: gpt-6-luna
    api_key_env: OPENAI_API_KEY
    reasoning_effort: medium
    service_tier: flex
    price_input_per_mtok: 0.05
    price_cached_input_per_mtok: 0.005
    price_output_per_mtok: 0.25
    cadence: {every_seconds: 600}
    window_max_chars: 65536
watches: []
```

Prices are USD per million tokens. Unpriced custom presets report an unknown
cost and cannot be used with a budget. Built-in Luna prices come from the prior
Watcher presets. Sol's standard short-context prices ($2 input, $0.10 cached,
$10 output) were verified on the [OpenAI pricing page](https://developers.openai.com/api/docs/pricing).
Only the Responses API is used, with `store=false` and encrypted reasoning replay.

## Integrate

Agents and apps use `add`, `remove`, `apply`, `enable`, `disable`, `status`,
`findings`, `alerts`, `ack`, `wake`, `judge`, and `presets`. Operators use `watch`,
`daemon`, `service`, and `check`. `catalog --json` keeps the two groups separate.

Every command accepts `--input-json`. Flags and catalog schemas come from the
same Go input structs. Nested flag values are JSON; string lists are repeatable.
`--json` writes only machine output to stdout. Diagnostics go to stderr.
Exit 1 means failure; exit 2 means conflict or invalid input.

```sh
watcher add --input-json '{"file":"app.yaml","watch":{
  "name":"build","generation":1,"preset":"fast",
  "source":{"type":"file","path":"../build.log"},
  "criteria":[{"id":"failed","text":"The build failed."}],
  "actions":[{"name":"record","kind":"record","description":"Record build failures."}],
  "labels":{"app":"builder"},"max_cost_usd":1
}}' --json

watcher findings --since 0 --watch build --label app=builder --json
watcher status --json
watcher wake build --json
watcher alerts --json
watcher ack --watch build --json
watcher remove build --json
watcher apply --file app.yaml --watches '[]' --json
```

`add` writes one watch to a named fragment, creating it if needed. `apply` replaces
that fragment's whole watch set while preserving its presets. `remove` edits the
watch's defining file. Writes use a lock, validate the merged registry, and replace
a file atomically. Duplicate names and unknown presets are errors.

Pull consumers keep their own high-water `seq`. `findings --since N` returns
committed rows with `seq > N`, in order. Labels are copied onto every finding.
No consumer cursor lives inside Watcher. `status` reports the generation actually
loaded by the runner, separately from the desired generation in YAML.

Push actions are `notify` and `command`; `record` needs no delivery. A command
uses fixed argv, receives the finding JSON on stdin, owns its process group, and
has a 60-second timeout. A watch with any command action must set `confirm` to a
preset. A rejected confirmation creates a `rejected` finding but does not fire a
oneshot. `escalate` optionally exposes a tool that asks a second preset to judge
before the finding commits.

A notification's `repeat_every_seconds` repeats delivery until acknowledgement.
Reminders keep the original timestamp. Acknowledgement stops future attempts;
a delivery already in progress cannot be recalled. Disabling a watch or removing
its repeating action cancels its reminders.

`integrations/client.ts` is a typed process client for agent/app commands. It
checks the catalog version once and accepts stdin and cancellation options.
`integrations/types.ts` is generated by `watcher catalog --typescript`.
Operator types are exported separately; `watch` streams `WatchResult` rows.

```ts
import { WatcherClient } from "./integrations/client";
const watcher = new WatcherClient({ registry: "/path/to/watches.yaml" });
const result = await watcher.call("judge", {
  criteria: ["A deployment failed"], preset: "fast"
}, { stdin: "Deployment failed: rollout aborted.\n" });
```

## Durability and budgets

One SQLite database, `watcher.db` beside the registry, holds progress, findings,
deliveries, judgments, wakes, and the daemon heartbeat. Runtime state never goes
in YAML. SQLite uses WAL, a busy timeout, and immediate write transactions.
A model call never holds a write transaction.

One judgment commits its cursor, conversation, spend, findings, and delivery rows
in one transaction. Judging is at least once; commit is at most once. A crash
before commit re-judges from the committed cursor. Delivery is at least once: a
crash after execution but before its receipt can repeat it. Command consumers
should deduplicate the finding's stable `id` when their effects require it.

The budget is a threshold, not a provider-side cap. Watcher checks it after every
screen, escalation, and confirmation call. One call can overshoot. Every call's
cost is logged independently; successful judgment spend commits with its findings.
A SIGKILL before commit can cost one additional re-judge call beyond the threshold.
Ordinary errors and graceful cancellation retain incurred spend.

Oneshot and budget terminal states survive restart. A changed resolved definition
resets conversation, spend, and terminal state; a changed source identity also
resets its cursor. Change the caller-owned generation to re-arm a completed watch.
This is a fresh database format, without migrations from older installations.
