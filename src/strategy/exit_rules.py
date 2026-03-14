from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ExitInput:
    pnl_pct: float
    holding_hours: float
    thesis_strengthened: bool
    has_official_contradiction: bool
    hours_to_resolution: float
    reduce_risk_hours_threshold: float = 6.0


def evaluate_exit(payload: ExitInput) -> tuple[str, str]:
    if payload.has_official_contradiction:
        return "full_exit", "official_contradiction_detected"
    if payload.pnl_pct >= 0.20:
        return "full_exit", "take_profit_20pct"
    if payload.pnl_pct >= 0.12:
        return "half_exit", "take_profit_12pct"
    if payload.holding_hours >= 48 and not payload.thesis_strengthened:
        return "full_exit", "time_exit_without_thesis_strengthen"
    if payload.hours_to_resolution <= payload.reduce_risk_hours_threshold:
        return "reduce_risk", "near_resolution_reduce_exposure"
    return "hold", "no_exit_condition_met"
