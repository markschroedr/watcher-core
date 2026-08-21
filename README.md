<p align="center">
  <img src="docs/assets/watcher-hero.png" alt="Watcher detects a deployment failure in a terminal stream" width="100%">
</p>

<h1 align="center">Watcher</h1>

<p align="center">
  Watch any stream. Describe what matters. Stay quiet until it happens.
</p>

Watcher reads a file, command, or standard input. An LLM judges your condition.
When it matches, Watcher emits a finding, sends a notification, or runs a
verified action.

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

