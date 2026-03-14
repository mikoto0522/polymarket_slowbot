from __future__ import annotations

from bisect import bisect_left
from datetime import timedelta
from typing import Any

from ..utils.db import Database
from ..utils.time import iso_utc, parse_to_utc


def _map_signal_direction(direction_suggestion: str) -> str | None:
    value = (direction_suggestion or "").lower().strip()
    if value == "positive":
        return "long_yes"
    if value == "negative":
        return "short_yes"
    return None


class PaperTradingEngine:
    def __init__(self, config: dict[str, Any]) -> None:
        paper_cfg = config.get("paper_trading", {})
        self.enabled = bool(paper_cfg.get("enabled", True))
        self.stake_per_signal = float(paper_cfg.get("stake_per_signal", 1.0))
        self.holding_minutes = int(paper_cfg.get("holding_minutes", 240))
        self.max_signals = int(paper_cfg.get("max_signals_per_day", 300))
        self.price_interval = str(paper_cfg.get("price_interval", "1h"))

    def simulate(self, db: Database) -> dict[str, float]:
        if not self.enabled:
            return {"enabled": 0.0}

        db.reset_today_paper_trades()
        signals = db.fetch_today_paper_signals(limit=self.max_signals)
        ordered = sorted(signals, key=lambda row: (row["ts_utc"], row["id"]))

        for signal in ordered:
            direction = _map_signal_direction(str(signal["direction_suggestion"]))
            common = {
                "signal_id": int(signal["id"]),
                "ts_utc": str(signal["ts_utc"]),
                "market_id": str(signal["market_id"]),
                "document_id": int(signal["document_id"]),
                "direction": str(signal["direction_suggestion"]),
                "created_at": iso_utc(),
            }
            if direction is None:
                db.insert_paper_trade(
                    {
                        **common,
                        "entry_ts_utc": None,
                        "entry_price": None,
                        "exit_ts_utc": None,
                        "exit_price": None,
                        "return_pct": None,
                        "pnl": None,
                        "status": "skipped",
                        "reason": "direction_not_actionable",
                    }
                )
                continue

            points = db.fetch_market_price_series(
                str(signal["market_id"]),
                limit=10_000,
                interval=self.price_interval,
            )
            if len(points) < 2:
                db.insert_paper_trade(
                    {
                        **common,
                        "entry_ts_utc": None,
                        "entry_price": None,
                        "exit_ts_utc": None,
                        "exit_price": None,
                        "return_pct": None,
                        "pnl": None,
                        "status": "skipped",
                        "reason": "insufficient_price_history",
                    }
                )
                continue

            parsed = []
            for row in points:
                dt = parse_to_utc(str(row["ts_utc"]))
                if dt is None:
                    continue
                parsed.append((dt, float(row["price"])))
            if len(parsed) < 2:
                db.insert_paper_trade(
                    {
                        **common,
                        "entry_ts_utc": None,
                        "entry_price": None,
                        "exit_ts_utc": None,
                        "exit_price": None,
                        "return_pct": None,
                        "pnl": None,
                        "status": "skipped",
                        "reason": "invalid_price_points",
                    }
                )
                continue

            ts_list = [item[0] for item in parsed]
            signal_time = parse_to_utc(str(signal["ts_utc"]))
            if signal_time is None:
                db.insert_paper_trade(
                    {
                        **common,
                        "entry_ts_utc": None,
                        "entry_price": None,
                        "exit_ts_utc": None,
                        "exit_price": None,
                        "return_pct": None,
                        "pnl": None,
                        "status": "skipped",
                        "reason": "invalid_signal_time",
                    }
                )
                continue

            used_latest_window_fallback = False
            if signal_time > ts_list[-1]:
                entry_idx = max(0, len(parsed) - 2)
                exit_idx = len(parsed) - 1
                used_latest_window_fallback = True
            else:
                entry_idx = bisect_left(ts_list, signal_time)
                if entry_idx >= len(parsed):
                    entry_idx = len(parsed) - 1
                target_exit = parsed[entry_idx][0] + timedelta(minutes=self.holding_minutes)
                exit_idx = bisect_left(ts_list, target_exit)
                if exit_idx >= len(parsed):
                    exit_idx = len(parsed) - 1

            entry_ts, entry_price = parsed[entry_idx]
            if entry_price <= 0:
                db.insert_paper_trade(
                    {
                        **common,
                        "entry_ts_utc": entry_ts.isoformat(),
                        "entry_price": entry_price,
                        "exit_ts_utc": None,
                        "exit_price": None,
                        "return_pct": None,
                        "pnl": None,
                        "status": "skipped",
                        "reason": "invalid_entry_price",
                    }
                )
                continue

            if exit_idx <= entry_idx:
                db.insert_paper_trade(
                    {
                        **common,
                        "entry_ts_utc": entry_ts.isoformat(),
                        "entry_price": entry_price,
                        "exit_ts_utc": None,
                        "exit_price": None,
                        "return_pct": None,
                        "pnl": None,
                        "status": "skipped",
                        "reason": "insufficient_future_prices",
                    }
                )
                continue

            exit_ts, exit_price = parsed[exit_idx]
            if direction == "long_yes":
                ret = (exit_price - entry_price) / entry_price
            else:
                ret = (entry_price - exit_price) / entry_price
            pnl = self.stake_per_signal * ret

            db.insert_paper_trade(
                {
                    **common,
                    "entry_ts_utc": entry_ts.isoformat(),
                    "entry_price": entry_price,
                    "exit_ts_utc": exit_ts.isoformat(),
                    "exit_price": exit_price,
                    "return_pct": ret,
                    "pnl": pnl,
                    "status": "closed",
                    "reason": (
                        f"{direction}_hold_{self.holding_minutes}m"
                        if not used_latest_window_fallback
                        else f"{direction}_latest_window_fallback"
                    ),
                }
            )

        summary = db.fetch_today_paper_trade_summary()
        summary["enabled"] = 1.0
        return summary
