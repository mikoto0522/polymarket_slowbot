from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from statistics import pstdev
from typing import Any

from ..utils.db import Database
from ..utils.http import get_json
from ..utils.time import iso_utc, parse_to_utc, utc_now


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class PolymarketCollector:
    def __init__(self, config: dict[str, Any]) -> None:
        poly_cfg = config["polymarket"]
        self.config = config
        self.gamma_base_url = poly_cfg["gamma_base_url"].rstrip("/")
        self.clob_base_url = poly_cfg["clob_base_url"].rstrip("/")
        self.page_size = int(poly_cfg.get("page_size", 200))
        self.max_pages = int(poly_cfg.get("max_pages", 10))

    def _fetch_paginated(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        offset = 0
        for _ in range(self.max_pages):
            current = dict(params)
            current["limit"] = self.page_size
            current["offset"] = offset
            rows = get_json(f"{self.gamma_base_url}/{endpoint}", current)
            if not isinstance(rows, list) or not rows:
                break
            all_rows.extend(rows)
            if len(rows) < self.page_size:
                break
            offset += self.page_size
        return all_rows

    def fetch_events(self) -> list[dict[str, Any]]:
        status = self.config["polymarket"]["market_status"]
        rows = self._fetch_paginated(
            "events",
            {
                "active": str(status.get("active", True)).lower(),
                "closed": str(status.get("closed", False)).lower(),
                "archived": str(status.get("archived", False)).lower(),
            },
        )
        rows.sort(key=lambda item: str(item.get("id", "")))
        return rows

    def fetch_markets(self) -> list[dict[str, Any]]:
        status = self.config["polymarket"]["market_status"]
        rows = self._fetch_paginated(
            "markets",
            {
                "active": str(status.get("active", True)).lower(),
                "closed": str(status.get("closed", False)).lower(),
                "archived": str(status.get("archived", False)).lower(),
            },
        )
        rows.sort(key=lambda item: str(item.get("id", "")))
        return rows

    def fetch_tags(self) -> list[dict[str, Any]]:
        rows = self._fetch_paginated("tags", {})
        rows.sort(key=lambda item: str(item.get("id", "")))
        return rows

    def is_fee_allowed(self, market: dict[str, Any]) -> tuple[bool, str]:
        fee_cfg = self.config["fees"]
        market_id = str(market.get("id", ""))
        slug = str(market.get("slug", ""))
        fees_enabled = bool(market.get("feesEnabled", False))
        if not fees_enabled:
            return True, "allowed_fee_disabled"
        if fee_cfg.get("allow_fee_enabled_markets", False):
            return True, "allowed_by_config"
        if market_id in set(map(str, fee_cfg.get("whitelist_market_ids", []))):
            return True, "allowed_whitelist_market_id"
        if slug in set(map(str, fee_cfg.get("whitelist_slugs", []))):
            return True, "allowed_whitelist_slug"
        return False, "rejected_fee_enabled"

    def _normalize_market(self, market: dict[str, Any]) -> dict[str, Any]:
        event_id = None
        tags: list[str] = []
        events = market.get("events") or []
        if isinstance(events, list) and events:
            first = events[0]
            if isinstance(first, dict):
                event_id = first.get("id")
                for event in events:
                    for tag in event.get("tags", []) or []:
                        label = tag.get("slug") or tag.get("label")
                        if label and label not in tags:
                            tags.append(label)

        token_ids = []
        raw_token_ids = market.get("clobTokenIds")
        if isinstance(raw_token_ids, str):
            try:
                token_ids = json.loads(raw_token_ids)
            except json.JSONDecodeError:
                token_ids = []
        elif isinstance(raw_token_ids, list):
            token_ids = raw_token_ids

        best_bid = _to_float(market.get("bestBid"))
        best_ask = _to_float(market.get("bestAsk"))
        spread = _to_float(market.get("spread"))
        if spread is None and best_bid is not None and best_ask is not None:
            spread = max(0.0, best_ask - best_bid)

        allowed, fee_reason = self.is_fee_allowed(market)
        return {
            "market_id": str(market.get("id", "")),
            "event_id": str(event_id) if event_id is not None else None,
            "slug": market.get("slug"),
            "title": market.get("question"),
            "description": market.get("description"),
            "rules": market.get("resolutionSource"),
            "end_date": market.get("endDateIso") or market.get("endDate"),
            "resolution_date": market.get("endDateIso") or market.get("endDate"),
            "tags_json": json.dumps(tags, ensure_ascii=False),
            "active": 1 if market.get("active") else 0,
            "closed": 1 if market.get("closed") else 0,
            "archived": 1 if market.get("archived") else 0,
            "token_ids_json": json.dumps(token_ids, ensure_ascii=False),
            "liquidity": _to_float(market.get("liquidityClob") or market.get("liquidity")),
            "volume": _to_float(market.get("volumeClob") or market.get("volume")),
            "open_interest": _to_float(market.get("openInterest")),
            "last_price": _to_float(market.get("lastTradePrice")),
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "fee_status": fee_reason if allowed else "blocked_fee",
            "updated_at": iso_utc(),
        }

    @staticmethod
    def _normalize_event(event: dict[str, Any]) -> dict[str, Any]:
        tags = []
        for tag in event.get("tags", []) or []:
            label = tag.get("slug") or tag.get("label")
            if label:
                tags.append(label)
        return {
            "event_id": str(event.get("id", "")),
            "slug": event.get("slug"),
            "title": event.get("title"),
            "description": event.get("description"),
            "start_date": event.get("startDate"),
            "end_date": event.get("endDate"),
            "active": 1 if event.get("active") else 0,
            "closed": 1 if event.get("closed") else 0,
            "archived": 1 if event.get("archived") else 0,
            "liquidity": _to_float(event.get("liquidity")),
            "volume": _to_float(event.get("volume")),
            "open_interest": _to_float(event.get("openInterest")),
            "tags_json": json.dumps(tags, ensure_ascii=False),
            "updated_at": iso_utc(),
        }

    def sync_market_catalog(self, db: Database) -> dict[str, int]:
        events = self.fetch_events()
        tags = self.fetch_tags()
        markets = self.fetch_markets()

        for event in events:
            db.upsert_event(self._normalize_event(event))
        for tag in tags:
            db.upsert_tag(
                {
                    "tag_id": str(tag.get("id", "")),
                    "label": tag.get("label") or "",
                    "slug": tag.get("slug"),
                    "updated_at": iso_utc(),
                }
            )

        eligibility_rows: list[tuple[str, str, str]] = []
        for market in markets:
            normalized = self._normalize_market(market)
            db.upsert_market(normalized)
            fee_allowed, reason = self.is_fee_allowed(market)
            eligibility_rows.append(
                (
                    normalized["market_id"],
                    "eligible" if fee_allowed else "blocked",
                    reason,
                )
            )
        db.commit()
        db.replace_trade_eligibility(eligibility_rows, updated_at=iso_utc())

        return {
            "events": len(events),
            "tags": len(tags),
            "markets": len(markets),
            "eligible_markets": sum(1 for _, status, _ in eligibility_rows if status == "eligible"),
        }

    def fetch_price_history(
        self,
        token_id: str,
        interval: str,
        fidelity: int | None = None,
    ) -> list[tuple[str, float]]:
        params: dict[str, Any] = {"market": token_id, "interval": interval}
        if fidelity is not None:
            params["fidelity"] = fidelity
        payload = get_json(f"{self.clob_base_url}/prices-history", params=params)
        history = payload.get("history", []) if isinstance(payload, dict) else []
        points: list[tuple[str, float]] = []
        for row in history:
            ts = row.get("t")
            price = _to_float(row.get("p"))
            if ts is None or price is None:
                continue
            ts_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
            points.append((ts_utc, price))
        return points

    @staticmethod
    def _volatility(points: list[tuple[datetime, float]]) -> float:
        if len(points) < 2:
            return 0.0
        returns = []
        for idx in range(1, len(points)):
            prev = points[idx - 1][1]
            cur = points[idx][1]
            if prev <= 0:
                continue
            returns.append((cur - prev) / prev)
        if len(returns) < 2:
            return 0.0
        return float(pstdev(returns))

    @staticmethod
    def _max_drawdown(points: list[tuple[datetime, float]]) -> float:
        if not points:
            return 0.0
        peak = points[0][1]
        max_dd = 0.0
        for _, price in points:
            peak = max(peak, price)
            if peak > 0:
                max_dd = max(max_dd, (peak - price) / peak)
        return float(max_dd)

    def sync_one_market_price_history(self, db: Database) -> dict[str, Any] | None:
        markets = db.latest_active_markets(limit=1)
        if not markets:
            return None
        market = markets[0]
        token_ids = json.loads(market["token_ids_json"] or "[]")
        if not token_ids:
            return None
        token_id = str(token_ids[0])
        interval = self.config["polymarket"]["price_history_interval"]
        points = self.fetch_price_history(token_id=token_id, interval=interval)
        if not points:
            return None

        inserted = db.insert_price_points(
            market_id=str(market["market_id"]),
            token_id=token_id,
            interval=interval,
            points=points,
        )

        now = utc_now()
        parsed = [(parse_to_utc(ts), p) for ts, p in points]
        parsed = [(t, p) for t, p in parsed if t is not None]
        parsed.sort(key=lambda x: x[0])

        def in_last(hours: int) -> list[tuple[datetime, float]]:
            threshold = now.timestamp() - hours * 3600
            return [(t, p) for t, p in parsed if t.timestamp() >= threshold]

        one_h = in_last(1)
        six_h = in_last(6)
        day_h = in_last(24)

        price_change_24h = 0.0
        if len(day_h) >= 2 and day_h[0][1] > 0:
            price_change_24h = (day_h[-1][1] - day_h[0][1]) / day_h[0][1]

        end_dt = parse_to_utc(market["end_date"])
        hours_to_resolution = None
        if end_dt is not None:
            hours_to_resolution = max(0.0, (end_dt - now).total_seconds() / 3600)

        metrics = {
            "market_id": str(market["market_id"]),
            "token_id": token_id,
            "interval": interval,
            "volatility_1h": self._volatility(one_h),
            "volatility_6h": self._volatility(six_h),
            "volatility_24h": self._volatility(day_h),
            "price_change_24h": price_change_24h,
            "hours_to_resolution": hours_to_resolution,
            "recent_trade_density": (len(day_h) / 24.0) if day_h else 0.0,
            "max_drawdown_nh": self._max_drawdown(day_h),
            "updated_at": iso_utc(),
        }
        db.upsert_price_metrics(metrics)

        return {
            "market_id": str(market["market_id"]),
            "token_id": token_id,
            "price_points": len(points),
            "inserted_points": inserted,
        }

    def sync_price_history_for_market_ids(
        self,
        db: Database,
        market_ids: list[str],
        max_markets: int = 30,
        interval: str | None = None,
    ) -> dict[str, int]:
        processed = 0
        inserted_total = 0
        with_prices = 0
        use_interval = interval or self.config["polymarket"]["price_history_interval"]
        unique_ids = sorted({str(market_id) for market_id in market_ids if market_id})

        for market_id in unique_ids[:max_markets]:
            row = db.conn.execute(
                "SELECT market_id, token_ids_json FROM market_catalog WHERE market_id = ?",
                (market_id,),
            ).fetchone()
            if row is None:
                continue
            token_ids = json.loads(row["token_ids_json"] or "[]")
            if not token_ids:
                continue

            token_id = str(token_ids[0])
            points = self.fetch_price_history(token_id=token_id, interval=use_interval)
            processed += 1
            if not points:
                continue
            with_prices += 1
            inserted = db.insert_price_points(
                market_id=market_id,
                token_id=token_id,
                interval=use_interval,
                points=points,
            )
            inserted_total += inserted

        return {
            "requested_markets": min(len(unique_ids), max_markets),
            "processed_markets": processed,
            "markets_with_prices": with_prices,
            "inserted_points": inserted_total,
        }
