<p align="center">
  <img src="docs/assets/watcher-hero.png" alt="Watcher detects a deployment failure in a terminal stream" width="100%">
</p>

<h1 align="center">Watcher</h1>

<p align="center">
  Watch text. Say what to look for. Get an alert when it happens.
</p>

Watcher does three things:

1. Read text from a file, command, or standard input.
2. Check for something you described in plain English.
3. Print a result, show an alert, or run a command when it happens.

Use it to watch logs, wait for a build, check prices, follow a sensor, or monitor
an inbox. The text and the condition change. Watcher stays the same.

Agents can use Watcher to watch things cheaply instead of checking again and
again. Watcher can also monitor agent runs, error streams, logs, and public
feeds.

## Install

```sh
uv tool install git+https://github.com/markschroedr/watcher-core.git
export OPENAI_API_KEY="..."
```

## Watch

```sh
watcher watch \
  --cmd "journalctl -f -u api" \
  --criterion "the deployment fails; successful retries stay silent" \
  --notify
```

Use `--file`, `--cmd`, or `--stdin`. Add `--oneshot` when the first finding
should end the watch.

## Presets

All built-in presets use GPT-6 Luna. None escalates to another model.

| Mode | Use case | Batch interval | Effort | Inference tier |
| --- | --- | --- | --- | --- |
| `eco` (default) | Background monitoring | 5 minutes | Medium | Flex |
| `fast` | Time-sensitive alerts | 5 seconds | Low | Fast (`priority`) |
| `thorough` | Difficult conditions | 5 minutes | High | Flex |

The interval controls when Watcher submits new data. Model latency comes after it.
No new data means no judgment call. A busy stream does not extend the interval.
Use `--every` to change the interval without changing the model or tier.

`watcher presets` prints the canonical definitions as JSON. Application integrations
read these definitions instead of maintaining their own presets.

## Keep it running

```sh
mkdir -p ~/.watcher
cp watches.example.yaml ~/.watcher/watches.yaml
watcher service install
watcher status
```

Watcher saves its place and starts again after a restart. Edit
`~/.watcher/watches.yaml` to add or change watches.

The daemon also loads `watches.d/*.yaml` and `watches.d/*.yml` beside that
registry. Applications can own separate fragments without editing your watches.
A `spool` action delivers findings as durable JSON files for an application to
consume. File start cursors and watch generations preserve activation boundaries.
The daemon publishes loaded generations in `status.json`.

Set `repeat_every_seconds` on a `notify` action to repeat an alert until you
acknowledge it. Use `watcher alerts` to inspect alerts and `watcher ack <id>`
to stop reminders. Repetition does not call the model again.

Applications own their automation definitions and downstream actions.
They use this same engine, not a separate integrated Watcher edition.
