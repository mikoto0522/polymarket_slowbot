from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BacktestStats:
    wins: int
    losses: int
    total_pnl: float
    max_drawdown: float
    avg_holding_hours: float

    @property
    def win_rate(self) -> float:
        total = self.wins + self.losses
        if total == 0:
            return 0.0
        return self.wins / total


def build_summary(stats: BacktestStats) -> dict[str, float]:
    return {
        "win_rate": round(stats.win_rate, 4),
        "total_pnl": round(stats.total_pnl, 4),
        "max_drawdown": round(stats.max_drawdown, 4),
        "avg_holding_hours": round(stats.avg_holding_hours, 2),
    }
