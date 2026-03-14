from __future__ import annotations

import json
import traceback
from pathlib import Path
from typing import Any, Callable, TypeVar

from . import __version__
from .ai.extractor import AIExtractor
from .backtest.paper_trading import PaperTradingEngine
from .collectors.polymarket_collector import PolymarketCollector
from .collectors.rss_collector import RSSCollector, SourceClassifier
from .monitoring.audit import generate_audit_bundle
from .monitoring.daily_report import generate_daily_report
from .strategy.candidate_selector import CandidateSelector
from .utils.config import load_config
from .utils.db import Database
from .utils.errors import SlowbotError
from .utils.logging import setup_logging
from .utils.time import iso_utc

T = TypeVar("T")


def _load_yaml_or_json(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        obj = yaml.safe_load(text)
        return obj if isinstance(obj, dict) else {}
    except ModuleNotFoundError:
        return json.loads(text)


def _run_stage(
    *,
    db: Database,
    stage: str,
    action: Callable[[], T],
    logger: Any,
) -> T:
    started = iso_utc()
    try:
        result = action()
        db.record_sync_run(
            stage=stage,
            status="success",
            details=result if isinstance(result, dict) else {"result": str(result)},
            error_text=None,
            started_at=started,
            finished_at=iso_utc(),
        )
        logger.info("%s done: %s", stage, result)
        return result
    except Exception as exc:
        db.record_sync_run(
            stage=stage,
            status="failed",
            details={},
            error_text=str(exc),
            started_at=started,
            finished_at=iso_utc(),
        )
        logger.error("%s failed: %s", stage, exc)
        raise


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    config = load_config(project_root)

    log_dir = project_root / config["storage"]["log_dir"]
    logger = setup_logging(log_dir=log_dir, level=config["app"].get("log_level", "INFO"))
    logger.info("Starting %s v%s", config["app"]["name"], __version__)
    logger.info("Shadow mode: %s", config["app"].get("shadow_mode", True))
    logger.info("AI deterministic mode: %s", config["ai"].get("deterministic_mode", True))

    db_path = project_root / config["storage"]["sqlite_path"]
    db = Database(db_path)
    db.init_schema()

    try:
        collector = PolymarketCollector(config)
        _run_stage(
            db=db,
            stage="market_sync",
            action=lambda: collector.sync_market_catalog(db),
            logger=logger,
        )

        _run_stage(
            db=db,
            stage="price_history_sync",
            action=lambda: collector.sync_one_market_price_history(db) or {"status": "no_data"},
            logger=logger,
        )

        whitelist_cfg = _load_yaml_or_json(project_root / "config" / "sources_whitelist.yaml")
        rss_collector = RSSCollector(
            rss_urls=config["external_sources"]["rss_urls"],
            classifier=SourceClassifier(whitelist_cfg),
        )
        _run_stage(
            db=db,
            stage="external_news_ingest",
            action=lambda: rss_collector.ingest(db),
            logger=logger,
        )

        extractor = AIExtractor(config)

        def _analyze_docs() -> dict[str, int]:
            ai_cfg = config.get("ai", {})
            unanalyzed = db.fetch_unanalyzed_documents(limit=int(ai_cfg.get("max_docs_per_run", 20)))
            analyzed_count = 0
            fallback_count = 0
            for doc in unanalyzed:
                payload = extractor.analyze(dict(doc))
                db.insert_document_analysis(int(doc["document_id"]), payload)
                analyzed_count += 1
                if str(payload.get("analysis_model", "")).startswith("rule_based_fallback"):
                    fallback_count += 1
            return {"analyzed": analyzed_count, "fallback_used": fallback_count}

        _run_stage(db=db, stage="ai_analysis", action=_analyze_docs, logger=logger)

        selector = CandidateSelector(config)
        _run_stage(
            db=db,
            stage="market_message_link_and_candidates",
            action=lambda: selector.run(db),
            logger=logger,
        )

        def _paper_price_sync() -> dict[str, int]:
            paper_cfg = config.get("paper_trading", {})
            market_ids = db.fetch_today_signal_market_ids(
                limit=int(paper_cfg.get("max_markets_price_sync", 30))
            )
            return collector.sync_price_history_for_market_ids(
                db,
                market_ids=market_ids,
                max_markets=int(paper_cfg.get("max_markets_price_sync", 30)),
                interval=str(paper_cfg.get("price_interval", "1h")),
            )

        _run_stage(db=db, stage="paper_price_sync", action=_paper_price_sync, logger=logger)

        paper_engine = PaperTradingEngine(config)
        _run_stage(
            db=db,
            stage="paper_trading",
            action=lambda: paper_engine.simulate(db),
            logger=logger,
        )

        report_dir = project_root / config["storage"]["report_dir"]
        report_path = _run_stage(
            db=db,
            stage="daily_report",
            action=lambda: generate_daily_report(db, report_dir),
            logger=logger,
        )

        audit_dir = project_root / config["storage"].get("audit_dir", "data/reports")
        audit_path = _run_stage(
            db=db,
            stage="audit_bundle",
            action=lambda: generate_audit_bundle(db, audit_dir),
            logger=logger,
        )

        logger.info("Daily report generated: %s", report_path)
        logger.info("Audit bundle generated: %s", audit_path)
        logger.info("Program completed successfully.")
        return 0
    except SlowbotError:
        logger.error("SlowbotError occurred:\n%s", traceback.format_exc())
        return 2
    except Exception:
        logger.error("Unhandled error occurred:\n%s", traceback.format_exc())
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
