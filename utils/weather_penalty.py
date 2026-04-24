"""
Weather gate.

Maps a TAF condition category to one of four gate values:
  "trade"          — conditions acceptable, proceed normally
  "trade_cautious" — broken/overcast skies; tradeable but requires higher edge (BROKEN_SKY_MIN_EDGE)
  "skip"           — elevated uncertainty (marine fog), do not enter
  "hard_skip"      — dangerous/unpredictable conditions, never trade

Edge requirements are handled separately via MIN_EDGE / BROKEN_SKY_MIN_EDGE in config.py.

Usage:
    from utils.weather_penalty import compute_weather_gate
    gate = compute_weather_gate("broken")
    if gate == "hard_skip":
        # do not trade under any circumstances
    elif gate == "skip":
        # elevated uncertainty — pass
    elif gate == "trade_cautious":
        # proceed to edge check with BROKEN_SKY_MIN_EDGE instead of MIN_EDGE
    else:
        # gate == "trade" — proceed to normal edge check
"""

from __future__ import annotations


def compute_weather_gate(taf_condition: str) -> str:
    """
    Return the weather gate for a TAF condition category.

    Returns one of: "trade", "trade_cautious", "skip", "hard_skip"
    """
    condition = taf_condition.lower()
    if condition in ("hard_skip", "precip", "convective"):
        return "hard_skip"
    if condition == "marine_fog":
        return "skip"
    if condition == "broken":
        return "trade_cautious"
    return "trade"   # clear, scattered, or unknown


def describe_gate(taf_condition: str) -> str:
    """Human-readable one-liner for the gate result."""
    gate = compute_weather_gate(taf_condition)
    descriptions = {
        "clear":       "Clear skies — no restriction",
        "scattered":   "Scattered clouds — no restriction",
        "broken":      "Broken/overcast — cautious trade (higher edge required)",
        "marine_fog":  "Marine fog/stratus — skip (burn-off timing uncertain)",
        "convective":  "Thunderstorm in TAF — hard skip",
        "precip":      "Active precipitation — hard skip",
        "hard_skip":   "Extreme conditions — hard skip",
    }
    return descriptions.get(taf_condition.lower(), f"{taf_condition} → {gate}")
