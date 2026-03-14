from __future__ import annotations

import json
import re
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Any

from ..utils.db import Database
from ..utils.time import iso_utc, parse_to_utc


def _tokenize(text: str) -> set[str]:
    return {token for token in re.findall(r"[a-zA-Z0-9]{3,}", text.lower())}


def _overlap_score(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    denom = len(a | b)
    if denom == 0:
        return 0.0
    return inter / denom


def _format_link_reason(*, rules_score: float, ai_relevance: float, entities_used: int) -> str:
    return (
        f"keyword_overlap={rules_score:.4f}; ai_relevance={ai_relevance:.4f}; "
        f"entities_used={entities_used}"
    )


class CandidateSelector:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.strategy_cfg = config["strategy"]
        self.shadow_mode = bool(config["app"].get("shadow_mode", True))

    def run(self, db: Database) -> dict[str, int]:
        docs = db.fetch_recent_documents_with_analysis(limit=200)
        markets = db.latest_active_markets(limit=500)

        max_matches = int(self.strategy_cfg.get("max_market_matches_per_doc", 5))
        db.reset_trade_candidates()
        db.reset_today_paper_signals()

        link_count = 0
        candidate_count = 0
        signal_count = 0

        for doc in docs:
            doc_text = f"{doc['title']} {doc['body']}"
            doc_tokens = _tokenize(doc_text)
            doc_entities = json.loads(doc["entities_json"] or "[]")
            entities_text = " ".join(doc_entities)
            if entities_text:
                doc_tokens |= _tokenize(entities_text)

            scored: list[dict[str, Any]] = []
            for market in markets:
                market_text = f"{market['title']} {market['slug']} {market['tags_json']}"
                rules_score = _overlap_score(doc_tokens, _tokenize(market_text))
                ai_relevance = float(doc["market_relevance"]) * 0.6 + float(
                    doc["resolution_relevance"]
                ) * 0.4
                final_score = rules_score * 0.45 + ai_relevance * 0.55
                scored.append(
                    {
                        "market_id": str(market["market_id"]),
                        "document_id": int(doc["document_id"]),
                        "rules_score": rules_score,
                        "ai_relevance": ai_relevance,
                        "final_score": final_score,
                        "link_reason": _format_link_reason(
                            rules_score=rules_score,
                            ai_relevance=ai_relevance,
                            entities_used=len(doc_entities),
                        ),
                        "is_direct_impact": int(doc["directly_affects_resolution"]),
                        "created_at": iso_utc(),
                        "market": market,
                        "doc": doc,
                    }
                )

            # Stable ordering for reproducibility.
            scored.sort(
                key=lambda row: (
                    -row["final_score"],
                    -row["rules_score"],
                    row["market_id"],
                    row["document_id"],
                )
            )
            top_rows = scored[:max_matches]
            for row in top_rows:
                db.upsert_market_document_link(row)
                link_count += 1

                market = row["market"]
                doc_row = row["doc"]
                rejections: list[str] = []
                accepts: list[str] = []

                if market["fee_status"] == "blocked_fee":
                    rejections.append("fee_not_allowed")
                else:
                    accepts.append("fee_allowed")

                spread = float(market["spread"] or 0.0)
                if spread > float(self.strategy_cfg.get("max_spread", 0.06)):
                    rejections.append("spread_too_wide")
                else:
                    accepts.append("spread_ok")

                source_quality = float(doc_row["source_quality"])
                if source_quality < float(self.strategy_cfg.get("min_source_quality", 0.7)):
                    rejections.append("low_source_quality")
                else:
                    accepts.append("source_quality_ok")

                confidence = float(doc_row["confidence"])
                if confidence < float(self.strategy_cfg.get("min_confidence", 0.7)):
                    rejections.append("low_ai_confidence")
                else:
                    accepts.append("ai_confidence_ok")

                score = float(row["final_score"])
                if score < float(self.strategy_cfg.get("min_relevance", 0.6)):
                    rejections.append("low_market_message_match")
                else:
                    accepts.append("relevance_ok")

                if int(doc_row["directly_affects_resolution"]) != 1:
                    rejections.append("not_direct_resolution_impact")
                else:
                    accepts.append("direct_resolution_impact")

                worth_research = 1 if not rejections else 0
                decision_reason = "eligible" if worth_research else ", ".join(rejections)
                rule_trace = {
                    "acceptance_checks": accepts,
                    "rejection_checks": rejections,
                    "market_id": row["market_id"],
                    "document_id": row["document_id"],
                    "score": score,
                    "ai_direction": doc_row["direction"],
                    "ai_confidence": confidence,
                    "link_reason": row["link_reason"],
                }
                db.insert_trade_candidate(
                    {
                        "market_id": row["market_id"],
                        "document_id": row["document_id"],
                        "worth_research": worth_research,
                        "score": score,
                        "reason": decision_reason,
                        "acceptance_reason": ", ".join(accepts) if accepts else None,
                        "rejection_reason": ", ".join(rejections) if rejections else None,
                        "rule_trace_json": json.dumps(
                            rule_trace, ensure_ascii=False, sort_keys=True
                        ),
                        "created_at": iso_utc(),
                    }
                )
                candidate_count += 1

                if self.shadow_mode:
                    trigger_reason = ", ".join(accepts)
                    if rejections:
                        trigger_reason = f"{trigger_reason} | rejected: {', '.join(rejections)}"
                    signal_ts = iso_utc()
                    publish_time = doc_row["publish_time"]
                    if publish_time:
                        parsed_dt = None
                        try:
                            parsed_dt = parsedate_to_datetime(str(publish_time))
                        except Exception:
                            parsed_dt = parse_to_utc(str(publish_time))
                        if parsed_dt is not None:
                            if parsed_dt.tzinfo is None:
                                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                            signal_ts = parsed_dt.astimezone(timezone.utc).isoformat()
                    db.insert_paper_signal(
                        {
                            "ts_utc": signal_ts,
                            "market_id": row["market_id"],
                            "document_id": row["document_id"],
                            "direction_suggestion": str(doc_row["direction"]),
                            "trigger_reason": trigger_reason,
                            "confidence": confidence,
                            "rule_trace_json": json.dumps(
                                rule_trace, ensure_ascii=False, sort_keys=True
                            ),
                            "created_at": iso_utc(),
                        }
                    )
                    signal_count += 1

        return {
            "linked_pairs": link_count,
            "candidate_rows": candidate_count,
            "paper_signals": signal_count,
        }
