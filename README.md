# Watcher

Watcher is a general semantic monitoring primitive. It reads a file, command
output, or standard input. An LLM judges natural-language criteria and invokes
configured actions only when a criterion matches.

Use it for short agent-controlled waits, continuous background monitoring, or
durable watches that survive restarts.

## Requirements

- Python 3.12 or newer
- [uv](https://docs.astral.sh/uv/)
- An `OPENAI_API_KEY` environment variable for the default profiles

## Install

Clone the private repository on an authenticated machine:

```sh
gh repo clone markschroedr/watcher-core
cd watcher-core
uv tool install --editable .
```

Upgrade an editable installation after pulling changes:

```sh
git pull --ff-only
uv tool install --editable . --force
```

## Ad-hoc watches

Watch a file until a deployment fails:

```sh
watcher watch \
  --file /absolute/path/deploy.log \
  --criterion "the deployment fails; successful retries stay silent" \
  --oneshot \
  --name deploy-failure
```

Watch command output continuously and show desktop notifications:

```sh
watcher watch \
  --cmd "journalctl -f -u example.service" \
  --criterion "the service enters a failed state" \
  --mode balanced \
  --notify \
  --name example-service
```

Use `--stdin` instead of `--file` or `--cmd` to pipe an existing stream into
Watcher. Findings are JSON lines on standard output and are also stored below
`~/.watcher/<name>/`.

## Durable watches

Copy `watches.example.yaml` to the machine-local registry and edit its profiles,
criteria, actions, and watches:

```sh
mkdir -p ~/.watcher
cp watches.example.yaml ~/.watcher/watches.yaml
watcher check ~/.watcher/watches.yaml
watcher service install
watcher status
```

Manage individual watches without stopping the service:

```sh
watcher enable demo-log
watcher disable demo-log
```

The daemon follows registry changes automatically. It stores offsets,
conversation state, findings, decisions, and budget usage under `~/.watcher/`.
The macOS launchd service is verified. A systemd user-service implementation is
included for Linux but has not yet been verified end to end.

## Configuration model

A registry contains four parts:

- `profiles`: model, API, reasoning, tier, and pricing settings.
- `criteria`: precise natural-language conditions and explicit near-misses.
- `actions`: log, notify, print, escalate, or verified command execution.
- `watches`: one source, cadence, profile, criteria, actions, window, and budget.

Treat watched content as untrusted data. A command action requires an
independent confirmation profile before Watcher executes it.

## Commands

```text
watcher check       Validate a registry
watcher run         Run enabled watches from a config
watcher eval        Judge one input window
watcher watch       Start an ad-hoc watch
watcher estimate    Estimate cost from a sample
watcher daemon      Run the durable registry engine
watcher enable      Enable one registered watch
watcher disable     Disable one registered watch
watcher status      Show service and watch state
watcher service     Install, inspect, or uninstall the daemon service
```
