"""
Hybrid edge threshold calculator.

Two independent stages:
  1. TAF penalty multiplier  — forward-looking (today's forecast conditions)
  2. bias_std gate           — backward-looking (historical cluster reliability)

Both must be satisfied. The effective threshold is the max of both checks.

Usage:
    from utils.weather_penalty import compute_effective_threshold
    result = compute_effective_threshold("convective", bias_std=3.1)
    if result is None:
        # hard skip — do not trade
    else:
        if edge >= result.threshold:
            # trade
"""

from __future__ import annotations
from dataclasses import dataclass

from config import (
    EDGE_THRESHOLD_BASE,
    WEATHER_PENALTY,
    STD_GATE_VALUE,
    STD_GATE_FLOOR,
)


@dataclass
class ThresholdResult:
    threshold: float        # effective edge threshold to beat
    base: float             # base threshold before penalty
    taf_penalty: float      # multiplier applied from TAF
    taf_condition: str      # condition category that drove the penalty
    std_gate_fired: bool    # whether the std gate raised the floor
    bias_std: float         # the bias_std value that was checked
    reason: str             # human-readable explanation for dashboard


def get_taf_penalty(condition: str) -> float | None:
    """
    Return the penalty multiplier for a TAF condition category.
    Returns None for hard_skip (trade should not be placed).
    """
    return WEATHER_PENALTY.get(condition.lower(), 1.0)


def compute_effective_threshold(
    taf_condition: str,
    bias_std: float,
    base: float = EDGE_THRESHOLD_BASE,
    std_gate: float = STD_GATE_VALUE,
    std_floor: float = STD_GATE_FLOOR,
) -> ThresholdResult | None:
    """
    Compute the effective edge threshold using the hybrid approach.

    Returns None if the trade should be hard-skipped (dangerous wx condition).
    Returns ThresholdResult otherwise — caller checks edge >= result.threshold.

    Parameters
    ----------
    taf_condition : str
        Condition category from taf_interpreter.py.
        One of: clear, scattered, broken, marine_fog, convective, precip, hard_skip
    bias_std : float
        bias_std from the bias table for this station/month/cluster/bin cell.
    base : float
        Base edge threshold (default EDGE_THRESHOLD_BASE = 0.12).
    std_gate : float
        If bias_std exceeds this value, the std gate fires (default 4.5°F).
    std_floor : float
        Minimum effective threshold when std gate fires (default 0.30).
    """
    # ── Stage 1: TAF penalty ──────────────────────────────────────────────
    penalty = get_taf_penalty(taf_condition)

    if penalty is None or taf_condition.lower() == "hard_skip":
        return None  # Hard skip — do not trade under any circumstances

    taf_threshold = base * penalty

    # ── Stage 2: bias_std gate ────────────────────────────────────────────
    std_gate_fired = bias_std > std_gate
    effective = max(taf_threshold, std_floor) if std_gate_fired else taf_threshold

    # ── Build explanation ─────────────────────────────────────────────────
    parts = [
        f"Base {base:.2f} × {taf_condition} penalty {penalty:.1f}× = {taf_threshold:.2f}"
    ]
    if std_gate_fired:
        parts.append(
            f"bias_std {bias_std:.1f}°F > gate {std_gate:.1f}°F "
            f"→ floor raised to {std_floor:.2f}"
        )
    reason = " | ".join(parts)

    return ThresholdResult(
        threshold=effective,
        base=base,
        taf_penalty=penalty,
        taf_condition=taf_condition,
        std_gate_fired=std_gate_fired,
        bias_std=bias_std,
        reason=reason,
    )


def describe_penalty(taf_condition: str) -> str:
    """Human-readable one-liner for the penalty category."""
    descriptions = {
        "clear":       "Clear skies — no penalty",
        "scattered":   "Scattered clouds — minor uncertainty (+20% threshold)",
        "broken":      "Broken/overcast — moderate uncertainty (+50% threshold)",
        "marine_fog":  "Marine fog/stratus — burn-off timing risk (+80% threshold)",
        "convective":  "Thunderstorm in TAF — high variance (+150% threshold)",
        "precip":      "Active precipitation — very high variance (+200% threshold)",
        "hard_skip":   "Hard skip — dangerous wx, no trade under any conditions",
    }
    return descriptions.get(taf_condition.lower(), f"Unknown condition: {taf_condition}")
