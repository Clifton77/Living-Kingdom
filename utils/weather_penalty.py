"""
Binary weather gate.

Maps a TAF condition category to one of three gate values:
  "trade"     — conditions acceptable, proceed normally
  "skip"      — elevated uncertainty, do not enter
  "hard_skip" — dangerous/unpredictable conditions, never trade

Weather is a binary gate, not a threshold multiplier. Edge requirements
are handled separately via MIN_EDGE in config.py.

Usage:
    from utils.weather_penalty import compute_weather_gate
    gate = compute_weather_gate("convective")
    if gate == "hard_skip":
        # do not trade under any circumstances
    elif gate == "skip":
        # pass — elevated uncertainty
    else:
        # gate == "trade" — proceed to edge check
"""

from __future__ import annotations


def compute_weather_gate(taf_condition: str) -> str:
    """
    Return the weather gate for a TAF condition category.

    Returns one of: "trade", "skip", "hard_skip"
    """
    condition = taf_condition.lower()
    if condition in ("hard_skip", "precip", "convective"):
        return "hard_skip"
    if condition in ("broken", "marine_fog"):
        return "skip"
    return "trade"   # clear, scattered, or unknown


def describe_gate(taf_condition: str) -> str:
    """Human-readable one-liner for the gate result."""
    gate = compute_weather_gate(taf_condition)
    descriptions = {
        "clear":       "Clear skies — no restriction",
        "scattered":   "Scattered clouds — no restriction",
        "broken":      "Broken/overcast — skip (burn-off timing uncertain)",
        "marine_fog":  "Marine fog/stratus — skip (burn-off timing uncertain)",
        "convective":  "Thunderstorm in TAF — hard skip",
        "precip":      "Active precipitation — hard skip",
        "hard_skip":   "Extreme conditions — hard skip",
    }
    return descriptions.get(taf_condition.lower(), f"{taf_condition} → {gate}")
