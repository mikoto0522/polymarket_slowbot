from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from typing import Any

from ..strategy.exit_rules import ExitInput, evaluate_exit
from ..utils.db import Database
from ..utils.time import iso_utc, parse_to_utc, utc_now


def _map_signal_direction(direction_suggestion: str) -> str | None:
    value = (direction_suggestion or "").lower().strip()
    if value == "positive":
        return "long_yes"
    if value == "negative":
        return "short_yes"
    return None


def _opposite_signal(direction: str) -> str:
    return "negative" if direction == "long_yes" else "positive"


def _entry_price(snapshot: dict[str, Any], direction: str) -> float | None:
    last_price = snapshot.get("last_price")
    best_bid = snapshot.get("best_bid")
    best_ask = snapshot.get("best_ask")
    if direction == "long_yes":
        return float(best_ask or last_price or 0.0) or None
    return float(best_bid or last_price or 0.0) or None


def _exit_price(snapshot: dict[str, Any], direction: str) -> float | None:
    last_price = snapshot.get("last_price")
    best_bid = snapshot.get("best_bid")
    best_ask = snapshot.get("best_ask")
    if direction == "long_yes":
        return float(best_bid or last_price or 0.0) or None
    return float(best_ask or last_price or 0.0) or None


def _pnl_pct(direction: str, entry_price: float, current_price: float) -> float:
    if entry_price <= 0:
        return 0.0
    if direction == "long_yes":
        return (current_price - entry_price) / entry_price
    return (entry_price - current_price) / entry_price


@dataclass
class PaperTradingEngine:
    config: dict[str, Any]

    def __post_init__(self) -> None:
        paper_cfg = self.config.get("paper_trading", {})
        self.enabled = bool(paper_cfg.get("enabled", True))
        self.stake_per_signal = float(paper_cfg.get("stake_per_signal", 1.0))
        self.max_signals = int(paper_cfg.get("max_signals_per_day", 300))
        self.contradiction_confidence = float(paper_cfg.get("contradiction_confidence", 0.75))
        self.thesis_strengthen_confidence = float(
            paper_cfg.get("thesis_strengthen_confidence", 0.75)
        )
        self.entry_requires_worth_research = bool(
            paper_cfg.get("entry_requires_worth_research", False)
        )

    def _open_positions_from_signals(self, db: Database) -> dict[str, int]:
        opened = 0
        skipped = 0
        signals = db.fetch_entry_signals(
            limit=self.max_signals,
            requires_worth_research=self.entry_requires_worth_research,
        )
        for signal in signals:
            mapped = _map_signal_direction(str(signal["direction_suggestion"]))
            if mapped is None:
                skipped += 1
                continue
            signal_key = f"{signal['market_id']}:{signal['document_id']}:{mapped}"
            if db.has_position_for_signal_key(signal_key):
                skipped += 1
                continue
            if db.has_open_position_for_market_direction(str(signal["market_id"]), mapped):
                skipped += 1
                continue

            row = db.get_market_snapshot(str(signal["market_id"]))
            if row is None:
                skipped += 1
                continue
            snapshot = dict(row)
            price = _entry_price(snapshot, mapped)
            if price is None or price <= 0:
                skipped += 1
                continue

            db.insert_paper_position(
                {
                    "signal_key": signal_key,
                    "signal_id": int(signal["id"]),
                    "market_id": str(signal["market_id"]),
                    "document_id": int(signal["document_id"]),
                    "direction": mapped,
                    "entry_ts_utc": str(signal["ts_utc"]),
                    "entry_price": float(price),
                    "stake_total": self.stake_per_signal,
                    "stake_open": self.stake_per_signal,
                    "confidence": float(signal["confidence"]),
                    "status": "open",
                    "open_reason": str(signal["trigger_reason"]),
                    "created_at": iso_utc(),
                    "updated_at": iso_utc(),
                }
            )
            opened += 1
        return {"opened_positions": opened, "skipped_signals": skipped}

    def _has_contradiction(self, db: Database, *, market_id: str, direction: str, after_ts: str) -> bool:
        opposite = _opposite_signal(direction)
        for sig in db.fetch_market_signals_after(market_id, after_ts):
            if str(sig["direction_suggestion"]).lower().strip() == opposite and float(
                sig["confidence"] or 0.0
            ) >= self.contradiction_confidence:
                return True
        return False

    def _has_thesis_strengthened(
        self, db: Database, *, market_id: str, direction: str, after_ts: str
    ) -> bool:
        target = "positive" if direction == "long_yes" else "negative"
        for sig in db.fetch_market_signals_after(market_id, after_ts):
            if str(sig["direction_suggestion"]).lower().strip() == target and float(
                sig["confidence"] or 0.0
            ) >= self.thesis_strengthen_confidence:
                return True
        return False

    def _close_positions_by_rules(self, db: Database) -> dict[str, int]:
        now = utc_now()
        full_closed = 0
        partial_closed = 0
        held = 0
        for position in db.fetch_open_positions(limit=2000):
            market_id = str(position["market_id"])
            direction = str(position["direction"])
            snapshot_row = db.get_market_snapshot(market_id)
            if snapshot_row is None:
                held += 1
                continue
            snapshot = dict(snapshot_row)
            cur_price = _exit_price(snapshot, direction)
            if cur_price is None or cur_price <= 0:
                held += 1
                continue

            entry_price = float(position["entry_price"])
            pnl_pct = _pnl_pct(direction, entry_price, cur_price)
            entry_dt = parse_to_utc(str(position["entry_ts_utc"]))
            if entry_dt is None:
                held += 1
                continue
            holding_hours = max(0.0, (now - entry_dt).total_seconds() / 3600)

            end_dt = parse_to_utc(snapshot.get("end_date"))
            hours_to_resolution = 9999.0
            if end_dt is not None:
                hours_to_resolution = max(0.0, (end_dt - now).total_seconds() / 3600)

            has_contradiction = self._has_contradiction(
                db,
                market_id=market_id,
                direction=direction,
                after_ts=str(position["entry_ts_utc"]),
            )
            thesis_strengthened = self._has_thesis_strengthened(
                db,
                market_id=market_id,
                direction=direction,
                after_ts=str(position["entry_ts_utc"]),
            )

            action, reason = evaluate_exit(
                ExitInput(
                    pnl_pct=pnl_pct,
                    holding_hours=holding_hours,
                    thesis_strengthened=thesis_strengthened,
                    has_official_contradiction=has_contradiction,
                    hours_to_resolution=hours_to_resolution,
                )
            )
            if action == "hold":
                held += 1
                continue

            stake_open = float(position["stake_open"])
            if stake_open <= 0:
                held += 1
                continue
            close_fraction = 1.0 if action == "full_exit" else 0.5
            closed_stake = stake_open * close_fraction
            realized_pnl = closed_stake * pnl_pct
            new_stake_open = max(0.0, stake_open - closed_stake)
            new_status = "open" if new_stake_open > 1e-9 else "closed"
            close_ts = now.astimezone(timezone.utc).isoformat()

            db.update_paper_position_on_exit(
                position_id=int(position["id"]),
                close_price=float(cur_price),
                close_ts_utc=close_ts,
                closed_stake=closed_stake,
                realized_pnl_delta=realized_pnl,
                status=new_status,
                close_reason=reason,
            )

            db.insert_paper_trade(
                {
                    "position_id": int(position["id"]),
                    "signal_id": int(position["signal_id"] or 0),
                    "signal_key": str(position["signal_key"]),
                    "ts_utc": iso_utc(),
                    "market_id": market_id,
                    "document_id": int(position["document_id"]),
                    "direction": direction,
                    "entry_ts_utc": str(position["entry_ts_utc"]),
                    "entry_price": entry_price,
                    "exit_ts_utc": close_ts,
                    "exit_price": float(cur_price),
                    "closed_stake": closed_stake,
                    "return_pct": pnl_pct,
                    "pnl": realized_pnl,
                    "status": "closed" if new_status == "closed" else "partial_exit",
                    "reason": reason,
                    "created_at": iso_utc(),
                }
            )
            if new_status == "closed":
                full_closed += 1
            else:
                partial_closed += 1
        return {
            "full_closed_positions": full_closed,
            "partial_closed_positions": partial_closed,
            "held_positions": held,
        }

    def simulate(self, db: Database) -> dict[str, float]:
        if not self.enabled:
            return {"enabled": 0.0}

        open_stats = self._open_positions_from_signals(db)
        close_stats = self._close_positions_by_rules(db)
        summary = db.fetch_today_paper_trade_summary()
        open_summary = db.fetch_open_position_summary()
        summary.update(
            {
                "enabled": 1.0,
                "opened_positions": float(open_stats["opened_positions"]),
                "skipped_signals": float(open_stats["skipped_signals"]),
                "full_closed_positions": float(close_stats["full_closed_positions"]),
                "partial_closed_positions": float(close_stats["partial_closed_positions"]),
                "held_positions": float(close_stats["held_positions"]),
                "open_positions": open_summary["open_positions"],
                "open_stake": open_summary["open_stake"],
            }
        )
        return summary
