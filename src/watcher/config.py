"""Central YAML config: model profiles, criteria, actions, watches."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, model_validator


class ConfigError(Exception):
    pass


class ModelProfile(BaseModel):
    model: str
    api: Literal["responses", "chat"] = "responses"
    base_url: str | None = None
    api_key_env: str = "OPENAI_API_KEY"
    reasoning_effort: Literal["minimal", "low", "medium", "high", "none"] | None = None
    service_tier: Literal["auto", "default", "flex", "priority"] | None = None
    price_input_per_mtok: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    price_cached_input_per_mtok: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    price_output_per_mtok: float | None = Field(default=None, ge=0, allow_inf_nan=False)

    def api_key(self) -> str:
        key = os.environ.get(self.api_key_env, "")
        if not key:
            raise ConfigError(f"environment variable {self.api_key_env} is not set")
        return key

    def has_prices(self) -> bool:
        return self.price_input_per_mtok is not None and self.price_output_per_mtok is not None

    def cost_usd(self, input_tokens: int, cached_tokens: int, output_tokens: int) -> float:
        if not self.has_prices():
            return 0.0
        price_in = self.price_input_per_mtok
        price_cached = self.price_cached_input_per_mtok if self.price_cached_input_per_mtok is not None else price_in
        fresh = max(input_tokens - cached_tokens, 0)
        return (fresh * price_in + cached_tokens * price_cached + output_tokens * self.price_output_per_mtok) / 1e6


class Criterion(BaseModel):
    id: str
    description: str


class ActionSpec(BaseModel):
    # Names become API function-tool names; the pattern is OpenAI's constraint.
    name: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    description: str
    kind: Literal["log", "notify", "command", "escalate", "print", "spool"]
    command: list[str] | None = None
    directory: Path | None = None
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    profile: str | None = None  # escalate: re-judge the same conversation with this profile
    # command: an independent judge on this profile must confirm the finding
    # before the command runs. Reason: the trigger is model-controlled and the
    # stream is untrusted; a second skeptical judgment is the authorization gate.
    confirm_profile: str | None = None
    # Notification delivery repeats without re-judging until a human acknowledges it.
    repeat_every_seconds: float | None = Field(default=None, gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def _fields_match_kind(self) -> "ActionSpec":
        if self.repeat_every_seconds is not None and self.kind != "notify":
            raise ValueError(f"action {self.name!r}: only kind 'notify' can repeat until acknowledged")
        if self.kind == "command" and not self.command:
            raise ValueError(f"action {self.name!r}: kind 'command' requires a command argv list")
        if self.kind != "command" and self.command:
            raise ValueError(f"action {self.name!r}: only kind 'command' takes a command")
        if self.kind == "spool" and self.directory is None:
            raise ValueError(f"action {self.name!r}: kind 'spool' requires a directory")
        if self.kind != "spool" and self.directory is not None:
            raise ValueError(f"action {self.name!r}: only kind 'spool' takes a directory")
        if self.kind != "spool" and self.metadata:
            raise ValueError(f"action {self.name!r}: only kind 'spool' takes metadata")
        if self.kind == "escalate" and not self.profile:
            raise ValueError(f"action {self.name!r}: kind 'escalate' requires a target profile")
        if self.kind != "escalate" and self.profile:
            raise ValueError(f"action {self.name!r}: only kind 'escalate' takes a profile")
        if self.kind == "command" and not self.confirm_profile:
            raise ValueError(f"action {self.name!r}: kind 'command' requires a confirm_profile")
        if self.kind != "command" and self.confirm_profile:
            raise ValueError(f"action {self.name!r}: only kind 'command' takes a confirm_profile")
        return self


class Cadence(BaseModel):
    """When to wake the judge.

    The three presets are one loop with different wake conditions:
    realtime wakes when the stream goes quiet, interval on time/volume,
    manual only on SIGUSR1. Manual wake works in every preset.
    """

    preset: Literal["realtime", "interval", "manual"]
    debounce_seconds: float = Field(default=0.5, gt=0, allow_inf_nan=False)
    # Realtime anti-starvation bounds: a pause-less stream never goes quiet, so
    # the window is also forced shut on a deadline or a volume cap.
    max_wait_seconds: float = Field(default=30.0, gt=0, allow_inf_nan=False)
    max_buffer_chars: int = Field(default=65536, gt=0)
    every_seconds: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    every_bytes: int | None = Field(default=None, gt=0)
    # cost governor: no automatic evaluation sooner than this
    min_gap_seconds: float = Field(default=0.0, ge=0, allow_inf_nan=False)
    # Optional: touch the cached prefix when idle this long, so slow watches
    # keep paying the cached-input price. Worth it for large windows only.
    cache_keepalive_seconds: float | None = Field(default=None, gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def _interval_needs_a_bound(self) -> "Cadence":
        if self.preset == "interval" and self.every_seconds is None and self.every_bytes is None:
            raise ValueError("interval cadence requires every_seconds or every_bytes")
        return self


class SourceSpec(BaseModel):
    type: Literal["file", "stdin", "command"]
    path: Path | None = None
    from_start: bool = False
    start_inode: int | None = Field(default=None, gt=0)
    start_position: int | None = Field(default=None, ge=0)
    command: list[str] | None = None  # spawned; merged stdout+stderr is the stream

    @model_validator(mode="after")
    def _fields_match_type(self) -> "SourceSpec":
        if self.type == "file" and self.path is None:
            raise ValueError("source type 'file' requires a path")
        if self.type != "file" and self.path is not None:
            raise ValueError("only source type 'file' takes a path")
        if self.type != "file" and (self.start_inode is not None or self.start_position is not None):
            raise ValueError("only source type 'file' takes a start cursor")
        if (self.start_inode is None) != (self.start_position is None):
            raise ValueError("a file start cursor requires both start_inode and start_position")
        if self.type == "command" and not self.command:
            raise ValueError("source type 'command' requires a command argv list")
        if self.type != "command" and self.command:
            raise ValueError("only source type 'command' takes a command")
        return self


class WatchSpec(BaseModel):
    name: str = Field(min_length=1, pattern=r"^[^/\\\x00]+$")
    generation: int | None = Field(default=None, gt=0)
    enabled: bool = True
    source: SourceSpec
    cadence: Cadence
    profile: str
    criteria: list[str] = Field(min_length=1)
    actions: list[str] = Field(min_length=1)
    # Screening window: how much stream history (plus the judge's own past
    # findings) one watch conversation may hold before it is re-anchored.
    window_max_chars: int = Field(default=65536, gt=0)
    oneshot: bool = False  # stop this watch after its first firing evaluation
    # Budget threshold: the watch stops once spend has crossed this. The check
    # runs after each billable call, so one call may overshoot the threshold.
    max_cost_usd: float | None = Field(default=None, gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def _safe_name(self) -> "WatchSpec":
        if self.name in (".", ".."):
            raise ValueError("watch name must not be a path component")
        return self


class Config(BaseModel):
    profiles: dict[str, ModelProfile]
    criteria: list[Criterion]
    actions: list[ActionSpec]
    watches: list[WatchSpec]
    log_dir: Path = Path("runs")

    @model_validator(mode="after")
    def _refs_resolve(self) -> "Config":
        criterion_ids = [c.id for c in self.criteria]
        action_names = [a.name for a in self.actions]
        for label, names in (("criterion", criterion_ids), ("action", action_names)):
            dupes = {n for n in names if names.count(n) > 1}
            if dupes:
                raise ValueError(f"duplicate {label} ids: {sorted(dupes)}")
        watch_names = [w.name for w in self.watches]
        dupes = {n for n in watch_names if watch_names.count(n) > 1}
        if dupes:
            raise ValueError(f"duplicate watch names: {sorted(dupes)}")
        for action in self.actions:
            if action.kind == "escalate" and action.profile not in self.profiles:
                raise ValueError(f"action {action.name!r}: unknown escalation profile {action.profile!r}")
            if action.kind == "command" and action.confirm_profile not in self.profiles:
                raise ValueError(f"action {action.name!r}: unknown confirm profile {action.confirm_profile!r}")
        for watch in self.watches:
            if watch.profile not in self.profiles:
                raise ValueError(f"watch {watch.name!r}: unknown profile {watch.profile!r}")
            for cid in watch.criteria:
                if cid not in criterion_ids:
                    raise ValueError(f"watch {watch.name!r}: unknown criterion {cid!r}")
            for name in watch.actions:
                if name not in action_names:
                    raise ValueError(f"watch {watch.name!r}: unknown action {name!r}")
            if watch.max_cost_usd is not None:
                budget_profiles = [watch.profile] + [
                    ref
                    for a in self.actions
                    if a.name in watch.actions
                    for ref in (a.profile, a.confirm_profile)
                    if ref is not None
                ]
                for profile_name in budget_profiles:
                    if not self.profiles[profile_name].has_prices():
                        raise ValueError(
                            f"watch {watch.name!r}: max_cost_usd requires prices on profile {profile_name!r}"
                        )
        return self

    def criterion(self, cid: str) -> Criterion:
        return next(c for c in self.criteria if c.id == cid)

    def action(self, name: str) -> ActionSpec:
        return next(a for a in self.actions if a.name == name)


def fragment_directory(path: Path) -> Path:
    return path.parent / "watches.d"


def registry_signature(path: Path) -> tuple[tuple[str, int, int], ...]:
    paths = [path]
    directory = fragment_directory(path)
    if directory.is_dir():
        paths.extend(sorted(directory.glob("*.yaml")))
        paths.extend(sorted(directory.glob("*.yml")))
    signature = []
    for candidate in paths:
        try:
            stat = candidate.stat()
        except OSError:
            continue
        signature.append((str(candidate), stat.st_mtime_ns, stat.st_size))
    return tuple(signature)


def _read_mapping(path: Path) -> dict[str, Any]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"cannot read config {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"config {path} must be a YAML mapping")
    return data


def _resolve_paths(data: dict[str, Any], path: Path, *, allow_log_dir: bool) -> None:
    if "log_dir" in data and not allow_log_dir:
        raise ConfigError(f"fragment {path} must not set log_dir")
    for action in data.get("actions") or []:
        directory = action.get("directory") if isinstance(action, dict) else None
        if directory is not None and not Path(directory).is_absolute():
            action["directory"] = str((path.parent / directory).resolve())
    for watch in data.get("watches") or []:
        source = watch.get("source") if isinstance(watch, dict) else None
        source_path = source.get("path") if isinstance(source, dict) else None
        if source_path is not None and not Path(source_path).is_absolute():
            source["path"] = str((path.parent / source_path).resolve())


def load(path: Path) -> Config:
    data = _read_mapping(path)
    _resolve_paths(data, path, allow_log_dir=True)
    merged: dict[str, Any] = {
        "profiles": dict(data.get("profiles") or {}),
        "criteria": list(data.get("criteria") or []),
        "actions": list(data.get("actions") or []),
        "watches": list(data.get("watches") or []),
        "log_dir": data.get("log_dir", "runs"),
    }
    directory = fragment_directory(path)
    fragments = []
    if directory.is_dir():
        fragments = sorted({*directory.glob("*.yaml"), *directory.glob("*.yml")})
    for fragment in fragments:
        extra = _read_mapping(fragment)
        _resolve_paths(extra, fragment, allow_log_dir=False)
        for name, profile in (extra.get("profiles") or {}).items():
            if name in merged["profiles"]:
                raise ConfigError(f"fragment {fragment}: duplicate profile {name!r}")
            merged["profiles"][name] = profile
        for key in ("criteria", "actions", "watches"):
            merged[key].extend(extra.get(key) or [])

    config = Config.model_validate(merged)
    # A daemon may start with an arbitrary cwd; relative paths mean
    # "next to the config file".
    if not config.log_dir.is_absolute():
        config.log_dir = (path.parent / config.log_dir).resolve()
    return config
