from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MarketQualityInput:
    spread: float
    top_book_depth: float
    hours_to_resolution: float
    title_clear: bool
    rules_clear: bool
    ai_confidence: float
    source_whitelisted: bool
    recent_jump_pct: float


def check_market_quality(payload: MarketQualityInput) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if payload.spread > 0.06:
        reasons.append("spread_too_wide")
    if payload.top_book_depth < 50:
        reasons.append("depth_too_thin")
    if payload.hours_to_resolution < 6:
        reasons.append("too_close_to_resolution")
    if not payload.title_clear or not payload.rules_clear:
        reasons.append("title_or_rules_unclear")
    if payload.ai_confidence < 0.7:
        reasons.append("ai_not_confident")
    if not payload.source_whitelisted:
        reasons.append("source_not_whitelisted")
    if payload.recent_jump_pct > 0.2:
        reasons.append("price_already_jumped")
    return (len(reasons) == 0), reasons
