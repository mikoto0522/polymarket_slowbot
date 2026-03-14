from __future__ import annotations

from dataclasses import dataclass


@dataclass
class EntryInput:
    directly_affects_resolution: bool
    source_quality: float
    confidence: float
    novelty: float
    spread: float
    recent_volatility_30m: float
    volatility_threshold_30m: float
    mispricing_gap: float
    hours_to_resolution: float


def should_enter(payload: EntryInput) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not payload.directly_affects_resolution:
        reasons.append("directly_affects_resolution=false")
    if payload.source_quality < 0.8:
        reasons.append("source_quality<0.8")
    if payload.confidence < 0.75:
        reasons.append("confidence<0.75")
    if payload.novelty < 0.7:
        reasons.append("novelty<0.7")
    if payload.spread > 0.06:
        reasons.append("spread>0.06")
    if payload.recent_volatility_30m > payload.volatility_threshold_30m:
        reasons.append("volatility_30m_above_threshold")
    if payload.mispricing_gap < 0.08:
        reasons.append("mispricing_gap<0.08")
    if payload.hours_to_resolution <= 6:
        reasons.append("hours_to_resolution<=6")
    return (len(reasons) == 0), reasons
