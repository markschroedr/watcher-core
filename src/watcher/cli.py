"""CLI: check a config, run the engine, or evaluate one window ad hoc."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

from watcher.config import (
    ActionSpec,
    Cadence,
    Config,
    ConfigError,
    Criterion,
    ModelProfile,
    SourceSpec,
    WatchSpec,
    load,
    registry_signature,
)
from watcher import service
from watcher.engine import Engine
from watcher.estimate import estimate
from watcher.judge import Judge, StreamTurn
from watcher.notifications import Alerts, remove_notification

DEFAULT_REGISTRY = Path.home() / ".watcher" / "watches.yaml"

# (input, cached input, output) USD per 1M tokens. Flex rates equal batch rates
# per the OpenAI pricing page; standard rates verified 2026-09-28.
_KNOWN_PRICES: dict[tuple[str, str | None], tuple[float, float, float]] = {
    ("gpt-6-luna", None): (0.10, 0.01, 0.50),
    ("gpt-6-luna", "flex"): (0.05, 0.005, 0.25),
    ("gpt-6-sol", None): (2.00, 0.20, 10.00),
    ("gpt-6-sol", "flex"): (1.00, 0.10, 5.00),
    ("gpt-6.1-sol", None): (2.00, 0.10, 10.00),
    ("gpt-6.1-sol", "flex"): (1.00, 0.05, 5.00),
    ("gpt-5.6-luna", None): (0.20, 0.02, 1.20),
    ("gpt-5.6-luna", "flex"): (0.10, 0.01, 0.60),
    ("gpt-5.6-terra", None): (2.00, 0.20, 12.00),
    ("gpt-5.6-terra", "flex"): (1.00, 0.10, 6.00),
    ("gpt-5.6-sol", None): (5.00, 0.50, 30.00),
    ("gpt-5.6-sol", "flex"): (2.50, 0.25, 15.00),
}

_REGISTRY_TEMPLATE = """\
# watcher registry — watches defined here run under the watcher daemon.
# Flip `enabled` (or use `watcher enable/disable <name>`); the daemon follows.

log_dir: .

profiles:
  relaxed:
    model: gpt-6-luna
    api: responses
    reasoning_effort: medium
    service_tier: flex
    price_input_per_mtok: 0.05
    price_cached_input_per_mtok: 0.005
    price_output_per_mtok: 0.25
  fast:
    model: gpt-6-luna
    api: responses
    reasoning_effort: low
    price_input_per_mtok: 0.10
    price_cached_input_per_mtok: 0.01
    price_output_per_mtok: 0.50
  smart:
    model: gpt-6.1-sol
    api: responses
    reasoning_effort: medium
    service_tier: flex
    price_input_per_mtok: 1.00
    price_cached_input_per_mtok: 0.05
    price_output_per_mtok: 5.00

criteria: []

actions:
  - name: record
    description: Record a finding so the user can review it later. Use for any matched criterion.
    kind: log
  - name: notify
    description: Show a desktop notification. Use only for findings that need attention now.
    kind: notify

watches: []

# Example watch:
#
# criteria:
#   - id: error-event
#     description: An exception or failure a developer would want to investigate.
#
# watches:
#   - name: my-log
#     enabled: true
#     source: {type: file, path: /path/to/some.log}
#     cadence: {preset: interval, every_seconds: 60}
#     profile: relaxed
#     criteria: [error-event]
#     actions: [record, notify]
#     max_cost_usd: 1.00
"""


def _ensure_registry(path: Path) -> None:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_REGISTRY_TEMPLATE, encoding="utf-8")
        print(f"created registry template at {path}", file=sys.stderr)


def _cmd_check(args: argparse.Namespace) -> int:
    config = load(args.config)
    enabled = [w for w in config.watches if w.enabled]
    print(f"config ok: {len(config.watches)} watches ({len(enabled)} enabled), "
          f"{len(config.criteria)} criteria, {len(config.actions)} actions, "
          f"{len(config.profiles)} profiles")
    for watch in config.watches:
        state = "on " if watch.enabled else "off"
        generation = f" generation={watch.generation}" if watch.generation is not None else ""
        print(f"  [{state}] {watch.name}: {watch.source.type} -> {watch.cadence.preset} "
          f"-> {watch.profile} -> {watch.actions}{generation}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    asyncio.run(Engine(load(args.config), args.config).run())
    return 0


# Pareto-placed presets for ad-hoc watches: each mode is a deliberate point on
# the cost / latency / judgment-quality front. Explicit flags override.
_MODES: dict[str, dict] = {
    "fast": {
        "cadence": ("realtime", 1.0),
        "min_gap": 2.0,
        "service_tier": None,
        "effort": "low",
        "window": 32768,
        "escalate": False,
    },
    "balanced": {
        "cadence": ("realtime", 2.0),
        "min_gap": 5.0,
        "service_tier": "flex",
        "effort": "medium",
        "window": 65536,
        "escalate": False,
    },
    "eco": {
        "cadence": ("interval", 60.0),
        "min_gap": 30.0,
        "service_tier": "flex",
        "effort": "medium",
        "window": 65536,
        "escalate": False,
    },
    "thorough": {
        "cadence": ("interval", 30.0),
        "min_gap": 15.0,
        "service_tier": "flex",
        "effort": "high",
        "window": 131072,
        "escalate": True,
    },
}


def _cmd_watch(args: argparse.Namespace) -> int:
    """Ad-hoc watch from flags — no YAML file, findings as JSON lines on stdout."""
    mode = _MODES[args.mode]
    # Unique default name: parallel unnamed watches must not share a log dir
    # or a prompt-cache key.
    name = args.name if args.name is not None else f"adhoc-{os.getpid()}"
    if args.file is not None:
        source = SourceSpec(type="file", path=args.file, from_start=args.from_start)
    elif args.cmd is not None:
        source = SourceSpec(type="command", command=["/bin/sh", "-lc", args.cmd])
    else:
        source = SourceSpec(type="stdin")

    criteria = []
    for i, description in enumerate(args.criterion, 1):
        cid, sep, rest = description.partition("=")
        if sep and rest and " " not in cid:
            criteria.append(Criterion(id=cid, description=rest))
        else:
            criteria.append(Criterion(id=f"c{i}", description=description))

    min_gap = args.min_gap if args.min_gap is not None else mode["min_gap"]
    if args.every is not None:
        cadence = Cadence(preset="interval", every_seconds=args.every, min_gap_seconds=min_gap)
    elif args.manual:
        cadence = Cadence(preset="manual")
    else:
        preset, pace = mode["cadence"]
        if preset == "interval":
            cadence = Cadence(preset="interval", every_seconds=pace, min_gap_seconds=min_gap)
        else:
            debounce = args.debounce if args.debounce is not None else pace
            cadence = Cadence(
                preset="realtime",
                debounce_seconds=debounce,
                max_wait_seconds=args.max_wait,
                min_gap_seconds=min_gap,
            )

    # One semantic reporting tool: print emits stdout JSON AND records durably,
    # so the model never has to choose between competing sinks.
    actions = [
        ActionSpec(
            name="report",
            description="Report a finding for a matched criterion.",
            kind="print",
        ),
    ]
    if args.notify or args.repeat_every is not None:
        actions.append(
            ActionSpec(
                name="notify",
                description="Show a desktop notification. Use only for findings that need attention now.",
                kind="notify",
                repeat_every_seconds=args.repeat_every,
            )
        )

    tier = mode["service_tier"]
    if args.price_in is not None and args.price_out is not None:
        cached = args.price_cached if args.price_cached is not None else args.price_in / 10
        prices = (args.price_in, cached, args.price_out)
    else:
        prices = _KNOWN_PRICES.get((args.model, tier))
        if prices is None:
            raise ConfigError(
                f"no known prices for model {args.model!r} at tier {tier or 'standard'}; "
                "pass --price-in and --price-out (USD per 1M tokens) so the budget is enforceable"
            )
    profiles = {
        "adhoc": ModelProfile(
            model=args.model,
            api="responses",
            reasoning_effort=args.effort if args.effort is not None else mode["effort"],
            service_tier=tier,
            price_input_per_mtok=prices[0],
            price_cached_input_per_mtok=prices[1],
            price_output_per_mtok=prices[2],
        )
    }
    if mode["escalate"]:
        profiles["smart"] = ModelProfile(
            model="gpt-6.1-sol",
            api="responses",
            reasoning_effort="medium",
            service_tier="flex",
            price_input_per_mtok=1.00,
            price_cached_input_per_mtok=0.05,
            price_output_per_mtok=5.00,
        )
        actions.append(
            ActionSpec(
                name="escalate",
                description=(
                    "Hand the situation to a smarter model. Use when a criterion may be met "
                    "but the situation is ambiguous, severe, or hard to judge from the stream alone."
                ),
                kind="escalate",
                profile="smart",
            )
        )

    config = Config(
        profiles=profiles,
        criteria=criteria,
        actions=actions,
        watches=[
            WatchSpec(
                name=name,
                source=source,
                cadence=cadence,
                profile="adhoc",
                criteria=[c.id for c in criteria],
                actions=[a.name for a in actions],
                window_max_chars=args.window if args.window is not None else mode["window"],
                oneshot=args.oneshot,
                max_cost_usd=args.max_cost,
            )
        ],
        log_dir=args.log_dir if args.log_dir is not None else Path.home() / ".watcher" / name,
    )
    asyncio.run(Engine(config).run())
    return 0


def _cmd_eval(args: argparse.Namespace) -> int:
    config = load(args.config)
    watch = next((w for w in config.watches if w.name == args.watch), None)
    if watch is None:
        print(f"unknown watch {args.watch!r}", file=sys.stderr)
        return 2
    window = args.input.read_text(encoding="utf-8") if args.input else sys.stdin.read()
    judge = Judge(config.profiles[watch.profile])
    criteria = [config.criterion(cid) for cid in watch.criteria]
    actions = [config.action(name) for name in watch.actions]
    result = asyncio.run(judge.evaluate(watch.name, criteria, actions, [StreamTurn(content=window)]))
    print(json.dumps(result.model_dump(), indent=2, ensure_ascii=False))
    return 0


def _cmd_estimate(args: argparse.Namespace) -> int:
    config = load(args.config)
    watch = next((w for w in config.watches if w.name == args.watch), None)
    if watch is None:
        print(f"unknown watch {args.watch!r}", file=sys.stderr)
        return 2
    sample = args.sample.read_text(encoding="utf-8") if args.sample else sys.stdin.read()
    result = asyncio.run(
        estimate(
            config,
            watch,
            sample,
            sample_seconds=args.sample_seconds,
            hours=args.hours,
            evals_per_hour_override=args.evals_per_hour,
        )
    )
    print(result.render(usd_per_eur=args.usd_per_eur))
    return 0


def _cmd_daemon(args: argparse.Namespace) -> int:
    _ensure_registry(args.registry)
    asyncio.run(Engine(load(args.registry), args.registry, daemon=True, persist=True).run())
    return 0


def _set_enabled(registry: Path, name: str, value: bool) -> int:
    from ruamel.yaml import YAML

    _ensure_registry(registry)
    yaml_rt = YAML()
    yaml_rt.preserve_quotes = True
    for candidate, _, _ in registry_signature(registry):
        path = Path(candidate)
        data = yaml_rt.load(path.read_text(encoding="utf-8"))
        for watch in data.get("watches") or []:
            if watch.get("name") == name:
                watch["enabled"] = value
                with path.open("w", encoding="utf-8") as handle:
                    yaml_rt.dump(data, handle)
                print(f"{name}: {'enabled' if value else 'disabled'} (daemon follows within ~2s)")
                return 0
    print(f"unknown watch {name!r} in {registry}", file=sys.stderr)
    return 2


def _cmd_enable(args: argparse.Namespace) -> int:
    return _set_enabled(args.registry, args.name, True)


def _cmd_disable(args: argparse.Namespace) -> int:
    return _set_enabled(args.registry, args.name, False)


def _cmd_status(args: argparse.Namespace) -> int:
    print(f"service: {service.status()}")
    if not args.registry.exists():
        print(f"registry: none ({args.registry})")
        return 0
    config = load(args.registry)
    runtime_watches = {}
    status_file = args.registry.resolve().parent / "status.json"
    if status_file.exists():
        try:
            runtime = json.loads(status_file.read_text(encoding="utf-8"))
            if runtime.get("alive") and time.time() - float(runtime.get("updated_at", 0)) <= 10:
                runtime_watches = runtime.get("watches") or {}
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            runtime_watches = {}
    print(f"registry: {args.registry} ({len(config.watches)} watches)")
    for watch in config.watches:
        state_file = config.log_dir / "state" / f"{watch.name}.json"
        spent = ""
        loaded_generation = (runtime_watches.get(watch.name) or {}).get("generation")
        if state_file.exists():
            try:
                state = json.loads(state_file.read_text(encoding="utf-8"))
                if loaded_generation is None:
                    loaded_generation = state.get("generation")
                spent_usd = state.get("spent_usd", 0.0)
                spent = f", spent ${spent_usd:.4f}"
                if watch.max_cost_usd is not None:
                    spent += f" of ${watch.max_cost_usd:.2f}"
                pending = len(state.get("pending_spool") or [])
                if pending:
                    spent += f", {pending} pending deliveries"
            except (OSError, json.JSONDecodeError):
                spent = ", state unreadable"
        flag = "on " if watch.enabled else "off"
        if watch.generation is None:
            generation = ""
        elif loaded_generation == watch.generation:
            generation = f", generation {watch.generation} loaded"
        elif loaded_generation is None:
            generation = f", generation {watch.generation} pending"
        else:
            generation = f", generation {watch.generation} pending (loaded {loaded_generation})"
        print(f"  [{flag}] {watch.name}: {watch.source.type} -> {watch.cadence.preset}{generation}{spent}")
    pending = Alerts(config.log_dir).pending()
    if pending:
        print(f"{len(pending)} unacknowledged alerts — watcher alerts to inspect; watcher ack <id> to stop")
    return 0


def _alert_store(args: argparse.Namespace) -> Alerts:
    return Alerts(args.log_dir if args.log_dir is not None else load(args.registry).log_dir)


def _cmd_alerts(args: argparse.Namespace) -> int:
    pending = _alert_store(args).pending()
    if args.json:
        print(json.dumps(pending, ensure_ascii=False, indent=2))
    elif not pending:
        print("No unacknowledged alerts.")
    else:
        for alert in pending:
            print(f"{alert['id']}  {alert['watch']}  every {alert['repeat_seconds']:g}s\n"
                  f"  {alert['summary']}\n  {alert['evidence']}")
            if alert["last_error"]:
                print(f"  delivery error: {alert['last_error']}")
    return 0


def _cmd_ack(args: argparse.Namespace) -> int:
    acknowledged = _alert_store(args).acknowledge(alert_id=args.id, watch=args.watch)
    for identity in acknowledged:
        try:
            asyncio.run(remove_notification(identity))
        except (OSError, RuntimeError, TimeoutError) as exc:
            print(f"acknowledged {identity}, but could not dismiss desktop notification: {exc}", file=sys.stderr)
    print(f"Acknowledged {len(acknowledged)} alert(s). Future reminders are stopped.")
    return 0


def _cmd_service(args: argparse.Namespace) -> int:
    if args.action == "install":
        _ensure_registry(args.registry)
        service.install(
            log=Path.home() / ".watcher" / "daemon.log",
            registry=args.registry.resolve(),
            env_file=None if args.env_file is None else args.env_file.resolve(),
        )
    elif args.action == "uninstall":
        service.uninstall()
    else:
        print(service.status())
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="watcher", description="Watch streams, judge with an LLM, act on triggers.")
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check", help="validate a config file")
    check.add_argument("config", type=Path)
    check.set_defaults(func=_cmd_check)

    run = sub.add_parser("run", help="run all enabled watches")
    run.add_argument("config", type=Path)
    run.set_defaults(func=_cmd_run)

    evaluate = sub.add_parser("eval", help="judge one window ad hoc (no actions loop, prints the result)")
    evaluate.add_argument("config", type=Path)
    evaluate.add_argument("--watch", required=True)
    evaluate.add_argument("--input", type=Path, help="file with window content; default stdin")
    evaluate.set_defaults(func=_cmd_eval)

    watch = sub.add_parser("watch", help="ad-hoc watch from flags: findings as JSON lines on stdout")
    src = watch.add_mutually_exclusive_group(required=True)
    src.add_argument("--file", type=Path, help="tail this file")
    src.add_argument("--cmd", help="run this shell command and watch its merged output")
    src.add_argument("--stdin", action="store_true", help="watch stdin")
    watch.add_argument("--criterion", action="append", required=True,
                       help="natural-language condition; repeatable; 'id=text' to name it")
    watch.add_argument("--name", help="watch name (default: adhoc-<pid>)")
    watch.add_argument("--mode", default="eco", choices=sorted(_MODES),
                       help="preset tradeoff: fast (standard tier, react asap), balanced (realtime, flex), "
                            "eco (default, 60s, flex), thorough (flex, high effort + escalation)")
    watch.add_argument("--every", type=float, help="interval cadence in seconds (overrides the mode's cadence)")
    watch.add_argument("--manual", action="store_true", help="manual cadence: evaluate on SIGUSR1 only")
    watch.add_argument("--debounce", type=float, help="realtime: quiet seconds before judging (default: mode)")
    watch.add_argument("--max-wait", type=float, default=30.0,
                       help="realtime: force an evaluation this long after the last one even without quiet (default 30)")
    watch.add_argument("--min-gap", type=float, help="cost governor: minimum seconds between evaluations (default: mode)")
    watch.add_argument("--window", type=int, help="screening window in chars (default: mode)")
    watch.add_argument("--oneshot", action="store_true", help="stop after the first firing evaluation")
    watch.add_argument("--max-cost", type=float, default=0.50,
                       help="budget threshold in USD; the watch stops after crossing it — "
                            "one in-flight call may overshoot (default 0.50)")
    watch.add_argument("--price-in", type=float, help="input USD per 1M tokens (required for unknown models)")
    watch.add_argument("--price-out", type=float, help="output USD per 1M tokens (required for unknown models)")
    watch.add_argument("--price-cached", type=float, help="cached-input USD per 1M tokens (default: --price-in / 10)")
    watch.add_argument("--notify", action="store_true", help="also expose a desktop-notification action")
    watch.add_argument("--repeat-every", type=float, metavar="SECONDS",
                       help="enable notifications that repeat until acknowledged; implies --notify")
    watch.add_argument("--from-start", action="store_true",
                       help="file source: include existing content (at most the last screening window)")
    watch.add_argument("--model", default="gpt-6-luna")
    watch.add_argument("--effort", choices=["minimal", "low", "medium", "high", "none"],
                       help="reasoning effort (default: mode)")
    watch.add_argument("--log-dir", type=Path, help="default: ~/.watcher/<name>")
    watch.set_defaults(func=_cmd_watch)

    est = sub.add_parser("estimate", help="estimate running cost from a stream sample (two live measurement calls)")
    est.add_argument("config", type=Path)
    est.add_argument("--watch", required=True)
    est.add_argument("--sample", type=Path, help="file with representative stream content; default stdin")
    est.add_argument("--sample-seconds", type=float, required=True, help="how many seconds of stream the sample spans")
    est.add_argument("--hours", type=float, default=1.0, help="extrapolation horizon (default 1)")
    est.add_argument("--evals-per-hour", type=float, help="override the evaluation rate (required for manual cadence)")
    est.add_argument("--usd-per-eur", type=float, default=1.16)
    est.set_defaults(func=_cmd_estimate)

    daemon = sub.add_parser("daemon", help="run the registry engine in the foreground (services wrap this)")
    daemon.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    daemon.set_defaults(func=_cmd_daemon)

    enable = sub.add_parser("enable", help="enable a registry watch (the daemon follows)")
    enable.add_argument("name")
    enable.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    enable.set_defaults(func=_cmd_enable)

    disable = sub.add_parser("disable", help="disable a registry watch (the daemon follows)")
    disable.add_argument("name")
    disable.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    disable.set_defaults(func=_cmd_disable)

    status = sub.add_parser("status", help="show service state and registry watches")
    status.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    status.set_defaults(func=_cmd_status)

    alerts = sub.add_parser("alerts", help="list unacknowledged repeating notifications")
    alerts.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    alerts.add_argument("--log-dir", type=Path, help="ad-hoc watch's log directory (instead of registry)")
    alerts.add_argument("--json", action="store_true", help="machine-readable pending alerts")
    alerts.set_defaults(func=_cmd_alerts)

    ack = sub.add_parser("ack", help="acknowledge notifications and stop their reminders")
    target = ack.add_mutually_exclusive_group(required=True)
    target.add_argument("id", nargs="?", help="alert ID from watcher alerts")
    target.add_argument("--watch", help="acknowledge all currently pending alerts for this watch")
    ack.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    ack.add_argument("--log-dir", type=Path, help="ad-hoc watch's log directory (instead of registry)")
    ack.set_defaults(func=_cmd_ack)

    svc = sub.add_parser("service", help="install/uninstall the background daemon (launchd/systemd)")
    svc.add_argument("action", choices=["install", "uninstall", "status"])
    svc.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    svc.add_argument("--env-file", type=Path, help="trusted KEY=VALUE file loaded only by the installed daemon")
    svc.set_defaults(func=_cmd_service)

    args = parser.parse_args()
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
