<p align="center">
  <img src="docs/assets/watcher-hero.png" alt="Watcher detects a deployment failure in a terminal stream" width="100%">
</p>

<h1 align="center">Watcher</h1>

<p align="center">
  Watch any stream. Describe what matters. Stay quiet until it happens.
</p>

Watcher is one small building block: **a stream plus a condition in plain words
becomes an event.**

It reads a file, a command's output, or standard input. An LLM checks your
condition. When the condition is true, Watcher emits a finding, sends a
notification, or runs a verified action.

That one idea covers many jobs. The stream can be logs, prices, a sensor feed,
an inbox, or any text. The condition is a sentence, not a regex. So the same
tool can watch a deploy, flag a bad number, or wait for one specific event —
without new code each time.

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

## Keep it running

```sh
mkdir -p ~/.watcher
cp watches.example.yaml ~/.watcher/watches.yaml
watcher service install
watcher status
```

The daemon keeps offsets, context, findings, and budgets under `~/.watcher/`.
It follows registry changes automatically and survives restarts.

Watcher supports OpenAI-compatible models, JSON findings, desktop
notifications, escalation, and independently verified command actions.

