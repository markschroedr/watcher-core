"""Canonical built-in Watcher presets, shared by the CLI and applications."""

LUNA_MODEL = "gpt-6-luna"

# USD per million tokens: input, cached input, output.
# OpenAI short-context pricing, verified 2026-10-02.
LUNA_PRICES = {
    None: (0.10, 0.01, 0.50),
    "flex": (0.05, 0.005, 0.25),
    "priority": (0.20, 0.02, 1.00),
}


def _profile(effort: str, tier: str) -> dict:
    prices = LUNA_PRICES[tier]
    return {
        "model": LUNA_MODEL,
        "api": "responses",
        "reasoning_effort": effort,
        "service_tier": tier,
        "price_input_per_mtok": prices[0],
        "price_cached_input_per_mtok": prices[1],
        "price_output_per_mtok": prices[2],
    }


PRESETS = {
    "eco": {
        "profile": _profile("medium", "flex"),
        "cadence": {"preset": "interval", "every_seconds": 300},
        "window_max_chars": 65536,
    },
    "fast": {
        "profile": _profile("low", "priority"),
        "cadence": {"preset": "interval", "every_seconds": 5},
        "window_max_chars": 65536,
    },
    "thorough": {
        "profile": _profile("high", "flex"),
        "cadence": {"preset": "interval", "every_seconds": 300},
        "window_max_chars": 65536,
    },
}
