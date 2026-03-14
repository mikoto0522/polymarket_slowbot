from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CapitalSnapshot:
    cash_available: float
    active_positions: int
    market_exposure: float
    order_size: float
    order_market_exposure_after: float
    daily_realized_pnl: float
    consecutive_losses: int


def check_capital_rules(snapshot: CapitalSnapshot) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if snapshot.order_size < 3.0:
        reasons.append("order_size_below_3u")
    if snapshot.order_size > 8.0:
        reasons.append("order_size_above_8u")
    if snapshot.active_positions >= 3:
        reasons.append("max_active_positions_reached")
    if snapshot.order_market_exposure_after > 10.0:
        reasons.append("single_market_exposure_above_10u")
    if snapshot.daily_realized_pnl <= -12.0:
        reasons.append("daily_loss_limit_hit")
    if snapshot.consecutive_losses >= 3:
        reasons.append("three_consecutive_losses_pause")
    if snapshot.cash_available - snapshot.order_size < 60.0:
        reasons.append("cash_reserve_below_60u")
    return (len(reasons) == 0), reasons
