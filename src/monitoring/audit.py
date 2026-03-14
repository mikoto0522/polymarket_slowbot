from __future__ import annotations

import json
from pathlib import Path

from ..utils.db import Database
from ..utils.time import utc_now


def generate_audit_bundle(db: Database, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    today = utc_now().date().isoformat()
    stamp = utc_now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"audit_bundle_{stamp}.jsonl"

    rows = db.fetch_daily_audit_rows(report_date=today, limit=500)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            payload = {
                "report_date": today,
                "market_data": {
                    "market_id": row["market_id"],
                    "slug": row["market_slug"],
                    "title": row["market_title"],
                    "rules": row["market_rules"],
                    "tags_json": row["market_tags_json"],
                    "end_date": row["market_end_date"],
                    "active": row["market_active"],
                    "closed": row["market_closed"],
                    "archived": row["market_archived"],
                    "last_price": row["last_price"],
                    "best_bid": row["best_bid"],
                    "best_ask": row["best_ask"],
                    "spread": row["spread"],
                },
                "document_data": {
                    "document_id": row["document_id"],
                    "url": row["document_url"],
                    "title": row["document_title"],
                    "body": row["document_body"],
                    "publish_time": row["document_publish_time"],
                    "first_seen_time": row["document_first_seen_time"],
                    "source": row["source"],
                    "source_tier": row["source_tier"],
                    "source_classification": row["source_classification"],
                    "raw_text_hash": row["raw_text_hash"],
                },
                "ai_data": {
                    "analysis_json": row["analysis_json"],
                    "raw_ai_response": row["raw_ai_response"],
                    "analysis_model": row["analysis_model"],
                    "analyzed_at": row["analyzed_at"],
                },
                "rule_judgment": {
                    "worth_research": row["worth_research"],
                    "score": row["score"],
                    "link_reason": row["link_reason"],
                    "reason": row["reason"],
                    "acceptance_reason": row["acceptance_reason"],
                    "rejection_reason": row["rejection_reason"],
                    "rule_trace_json": row["rule_trace_json"],
                    "created_at": row["created_at"],
                },
            }
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return output_path
