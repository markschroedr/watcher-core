"""Cost estimation: measure real token usage on a sample window, extrapolate over cadence.

Two live judge calls: one on a near-empty window measures the static overhead
(instructions, criteria, tools), one on the sample measures data tokens. Cost
per hour then follows from the cadence:

    cost/h = data_tokens/h * price_in
           + evals/h * ((overhead + avg_window) * price_cached + output * price_out)

Each stream token is paid fresh exactly once; the conversation history
(on average half the screening window) and the static overhead are re-sent
per eval at the cached-input price. Approximation: prompt caching only applies
above a 1024-token prefix, so tiny windows cost slightly more than estimated.
"""

from __future__ import annotations

import math

from pydantic import BaseModel

from watcher.config import Config, ConfigError, WatchSpec
from watcher.judge import Judge, StreamTurn

_EMPTY_WINDOW = "(empty stream window)"


class Estimate(BaseModel):
    watch: str
    model: str
    service_tier: str | None
    # measured
    overhead_input_tokens: int
    sample_chars: int
    sample_input_tokens: int
    chars_per_token: float
    output_tokens_noop: int
    output_tokens_hit: int
    # derived
    evals_per_hour: float
    evals_per_hour_is_ceiling: bool
    data_chars_per_hour: float
    data_tokens_per_hour: float
    cost_per_hour_usd_noop: float  # warm cache
    cost_per_hour_usd_hit: float  # warm cache
    cost_per_hour_usd_cold_noop: float
    cost_per_hour_usd_cold_hit: float
    hours: float
    cost_usd_noop: float
    cost_usd_hit: float
    chars_per_usd_noop: float

    def render(self, usd_per_eur: float) -> str:
        lines = [
            f"watch {self.watch!r} — {self.model} ({self.service_tier or 'default'} tier)",
            f"  measured: {self.overhead_input_tokens} tok static overhead/eval, "
            f"{self.chars_per_token:.1f} chars/token, "
            f"output {self.output_tokens_noop} tok (noop) … {self.output_tokens_hit} tok (hit)",
            f"  cadence:  {self.evals_per_hour:.1f} evals/h"
            + (" (ceiling — realtime rate depends on burst structure)" if self.evals_per_hour_is_ceiling else "")
            + f", {self.data_chars_per_hour / 1000:.1f} kChars/h data",
            f"  per hour: warm cache ${self.cost_per_hour_usd_noop:.4f} (noop) … ${self.cost_per_hour_usd_hit:.4f} (hits), "
            f"cold cache ${self.cost_per_hour_usd_cold_noop:.4f} … ${self.cost_per_hour_usd_cold_hit:.4f}",
            f"  {self.hours:g} h:   ${self.cost_usd_noop:.4f} … ${self.cost_usd_hit:.4f} warm"
            f"  (≈ {self.cost_usd_noop / usd_per_eur:.4f} … {self.cost_usd_hit / usd_per_eur:.4f} €)",
            f"  volume:   ≈ {self.chars_per_usd_noop * usd_per_eur / 1e6:.0f} MB screenable per 1 €"
            f" at this cadence (warm, all-noop)",
            "  scope:    base judge calls only — keepalives, escalations, and confirm calls are not modeled",
        ]
        return "\n".join(lines)


def _evals_per_hour(
    watch: WatchSpec, data_chars_per_hour: float, override: float | None
) -> tuple[float, bool]:
    cadence = watch.cadence
    if override is not None:
        rate, ceiling = override, False
    elif cadence.preset == "interval":
        # Whichever bound trips first sets the cycle length.
        time_rate = 3600.0 / cadence.every_seconds if cadence.every_seconds is not None else 0.0
        byte_rate = data_chars_per_hour / cadence.every_bytes if cadence.every_bytes is not None else 0.0
        rate, ceiling = max(time_rate, byte_rate), False
    elif cadence.preset == "realtime":
        rate, ceiling = 3600.0 / cadence.debounce_seconds, True
    else:
        raise ConfigError("manual cadence has no inherent rate; pass --evals-per-hour")
    if cadence.min_gap_seconds > 0:
        rate = min(rate, 3600.0 / cadence.min_gap_seconds)
    return rate, ceiling


async def estimate(
    config: Config,
    watch: WatchSpec,
    sample: str,
    sample_seconds: float,
    hours: float,
    evals_per_hour_override: float | None = None,
) -> Estimate:
    for name, value in (("sample_seconds", sample_seconds), ("hours", hours),
                        ("evals_per_hour", evals_per_hour_override)):
        if value is not None and (not math.isfinite(value) or value <= 0):
            raise ConfigError(f"{name} must be finite and greater than zero")
    data_chars_per_hour = len(sample) / sample_seconds * 3600.0
    evals_per_hour, is_ceiling = _evals_per_hour(watch, data_chars_per_hour, evals_per_hour_override)
    profile = config.profiles[watch.profile]
    if profile.price_input_per_mtok is None or profile.price_output_per_mtok is None:
        raise ConfigError(
            f"profile {watch.profile!r} has no price_input_per_mtok/price_output_per_mtok; "
            "add the model's pricing (USD per 1M tokens at the configured tier) to the config"
        )
    if not sample.strip():
        raise ConfigError("sample is empty")

    judge = Judge(profile)
    criteria = [config.criterion(cid) for cid in watch.criteria]
    actions = [config.action(name) for name in watch.actions]
    baseline = await judge.evaluate(watch.name, criteria, actions, [StreamTurn(content=_EMPTY_WINDOW)])
    sampled = await judge.evaluate(watch.name, criteria, actions, [StreamTurn(content=sample)])

    sample_tokens = sampled.input_tokens - baseline.input_tokens
    if sample_tokens <= 0:
        raise ConfigError("sample too small to measure; provide a bigger sample")
    chars_per_token = len(sample) / sample_tokens

    data_tokens_per_hour = data_chars_per_hour / chars_per_token

    price_in = profile.price_input_per_mtok / 1e6
    price_cached = (
        profile.price_cached_input_per_mtok / 1e6
        if profile.price_cached_input_per_mtok is not None
        else price_in
    )
    price_out = profile.price_output_per_mtok / 1e6
    avg_window_tokens = watch.window_max_chars / chars_per_token / 2
    context_tokens = baseline.input_tokens + avg_window_tokens

    def rate(context_price: float, out_tokens: int) -> float:
        return data_tokens_per_hour * price_in + evals_per_hour * (
            context_tokens * context_price + out_tokens * price_out
        )

    # Warm assumes every eval hits the prompt cache; cold assumes none do
    # (slow cadences past the cache TTL, re-anchors, restarts). Reality is
    # between the two.
    warm_noop, warm_hit = rate(price_cached, baseline.output_tokens), rate(price_cached, sampled.output_tokens)
    cold_noop, cold_hit = rate(price_in, baseline.output_tokens), rate(price_in, sampled.output_tokens)
    chars_per_usd_noop = data_chars_per_hour / warm_noop if warm_noop else 0.0

    return Estimate(
        cost_per_hour_usd_cold_noop=cold_noop,
        cost_per_hour_usd_cold_hit=cold_hit,
        watch=watch.name,
        model=profile.model,
        service_tier=profile.service_tier,
        overhead_input_tokens=baseline.input_tokens,
        sample_chars=len(sample),
        sample_input_tokens=sample_tokens,
        chars_per_token=chars_per_token,
        output_tokens_noop=baseline.output_tokens,
        output_tokens_hit=sampled.output_tokens,
        evals_per_hour=evals_per_hour,
        evals_per_hour_is_ceiling=is_ceiling,
        data_chars_per_hour=data_chars_per_hour,
        data_tokens_per_hour=data_tokens_per_hour,
        cost_per_hour_usd_noop=warm_noop,
        cost_per_hour_usd_hit=warm_hit,
        hours=hours,
        cost_usd_noop=warm_noop * hours,
        cost_usd_hit=warm_hit * hours,
        chars_per_usd_noop=chars_per_usd_noop,
    )
