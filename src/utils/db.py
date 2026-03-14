from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .time import iso_utc, utc_now


class Database:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA journal_mode = WAL;

            CREATE TABLE IF NOT EXISTS market_catalog (
                market_id TEXT PRIMARY KEY,
                event_id TEXT,
                slug TEXT,
                title TEXT,
                description TEXT,
                rules TEXT,
                end_date TEXT,
                resolution_date TEXT,
                tags_json TEXT,
                active INTEGER,
                closed INTEGER,
                archived INTEGER,
                token_ids_json TEXT,
                liquidity REAL,
                volume REAL,
                open_interest REAL,
                last_price REAL,
                best_bid REAL,
                best_ask REAL,
                spread REAL,
                fee_status TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS event_catalog (
                event_id TEXT PRIMARY KEY,
                slug TEXT,
                title TEXT,
                description TEXT,
                start_date TEXT,
                end_date TEXT,
                active INTEGER,
                closed INTEGER,
                archived INTEGER,
                liquidity REAL,
                volume REAL,
                open_interest REAL,
                tags_json TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS tag_catalog (
                tag_id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                slug TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS market_trade_eligibility (
                market_id TEXT PRIMARY KEY,
                fee_status TEXT NOT NULL,
                reason TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS market_price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                token_id TEXT NOT NULL,
                interval TEXT NOT NULL,
                ts_utc TEXT NOT NULL,
                price REAL NOT NULL,
                UNIQUE(market_id, token_id, interval, ts_utc)
            );

            CREATE TABLE IF NOT EXISTS market_price_metrics (
                market_id TEXT PRIMARY KEY,
                token_id TEXT NOT NULL,
                interval TEXT NOT NULL,
                volatility_1h REAL,
                volatility_6h REAL,
                volatility_24h REAL,
                price_change_24h REAL,
                hours_to_resolution REAL,
                recent_trade_density REAL,
                max_drawdown_nh REAL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS external_documents (
                document_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                author TEXT,
                publisher TEXT,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                publish_time TEXT,
                first_seen_time TEXT NOT NULL,
                tags_json TEXT,
                raw_text_hash TEXT NOT NULL UNIQUE,
                source_tier INTEGER NOT NULL,
                source_classification TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS document_ai_analysis (
                document_id INTEGER PRIMARY KEY,
                market_relevance REAL NOT NULL,
                resolution_relevance REAL NOT NULL,
                source_quality REAL NOT NULL,
                novelty REAL NOT NULL,
                direction TEXT NOT NULL,
                confidence REAL NOT NULL,
                event_type TEXT NOT NULL,
                directly_affects_resolution INTEGER NOT NULL,
                summary TEXT NOT NULL,
                why TEXT NOT NULL,
                entities_json TEXT NOT NULL,
                time_sensitivity TEXT NOT NULL,
                analysis_json TEXT NOT NULL,
                raw_ai_response TEXT NOT NULL,
                analysis_model TEXT NOT NULL,
                analyzed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS market_document_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                document_id INTEGER NOT NULL,
                rules_score REAL NOT NULL,
                ai_relevance REAL NOT NULL,
                final_score REAL NOT NULL,
                link_reason TEXT NOT NULL,
                is_direct_impact INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(market_id, document_id)
            );

            CREATE TABLE IF NOT EXISTS trade_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                document_id INTEGER NOT NULL,
                worth_research INTEGER NOT NULL,
                score REAL NOT NULL,
                reason TEXT NOT NULL,
                acceptance_reason TEXT,
                rejection_reason TEXT,
                rule_trace_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS paper_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                market_id TEXT NOT NULL,
                document_id INTEGER NOT NULL,
                direction_suggestion TEXT NOT NULL,
                trigger_reason TEXT NOT NULL,
                confidence REAL NOT NULL,
                worth_research INTEGER NOT NULL DEFAULT 0,
                score REAL NOT NULL DEFAULT 0.0,
                rule_trace_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id INTEGER,
                signal_id INTEGER,
                signal_key TEXT,
                ts_utc TEXT NOT NULL,
                market_id TEXT NOT NULL,
                document_id INTEGER NOT NULL,
                direction TEXT NOT NULL,
                entry_ts_utc TEXT,
                entry_price REAL,
                exit_ts_utc TEXT,
                exit_price REAL,
                closed_stake REAL,
                return_pct REAL,
                pnl REAL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS paper_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL UNIQUE,
                signal_id INTEGER,
                market_id TEXT NOT NULL,
                document_id INTEGER NOT NULL,
                direction TEXT NOT NULL,
                entry_ts_utc TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stake_total REAL NOT NULL,
                stake_open REAL NOT NULL,
                confidence REAL NOT NULL,
                status TEXT NOT NULL,
                close_ts_utc TEXT,
                close_price REAL,
                realized_pnl REAL NOT NULL DEFAULT 0.0,
                realized_return_pct REAL,
                open_reason TEXT NOT NULL,
                close_reason TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TEXT NOT NULL,
                stage TEXT NOT NULL,
                status TEXT NOT NULL,
                details_json TEXT,
                error_text TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL
            );
            """
        )
        self._run_migrations()
        self.conn.commit()

    def _run_migrations(self) -> None:
        self._ensure_column("document_ai_analysis", "summary", "TEXT")
        self._ensure_column("document_ai_analysis", "analysis_json", "TEXT")
        self._ensure_column("market_document_links", "link_reason", "TEXT")
        self._ensure_column("trade_candidates", "acceptance_reason", "TEXT")
        self._ensure_column("trade_candidates", "rejection_reason", "TEXT")
        self._ensure_column("trade_candidates", "rule_trace_json", "TEXT DEFAULT '{}'")
        self._ensure_column("paper_signals", "worth_research", "INTEGER DEFAULT 0")
        self._ensure_column("paper_signals", "score", "REAL DEFAULT 0.0")
        self._ensure_column("paper_trades", "position_id", "INTEGER")
        self._ensure_column("paper_trades", "signal_key", "TEXT")
        self._ensure_column("paper_trades", "closed_stake", "REAL")

    def _ensure_column(self, table: str, column: str, column_type: str) -> None:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {row["name"] for row in rows}
        if column in existing:
            return
        self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")

    def upsert_market(self, row: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO market_catalog (
                market_id, event_id, slug, title, description, rules, end_date,
                resolution_date, tags_json, active, closed, archived, token_ids_json,
                liquidity, volume, open_interest, last_price, best_bid, best_ask,
                spread, fee_status, updated_at
            ) VALUES (
                :market_id, :event_id, :slug, :title, :description, :rules, :end_date,
                :resolution_date, :tags_json, :active, :closed, :archived, :token_ids_json,
                :liquidity, :volume, :open_interest, :last_price, :best_bid, :best_ask,
                :spread, :fee_status, :updated_at
            )
            ON CONFLICT(market_id) DO UPDATE SET
                event_id = excluded.event_id,
                slug = excluded.slug,
                title = excluded.title,
                description = excluded.description,
                rules = excluded.rules,
                end_date = excluded.end_date,
                resolution_date = excluded.resolution_date,
                tags_json = excluded.tags_json,
                active = excluded.active,
                closed = excluded.closed,
                archived = excluded.archived,
                token_ids_json = excluded.token_ids_json,
                liquidity = excluded.liquidity,
                volume = excluded.volume,
                open_interest = excluded.open_interest,
                last_price = excluded.last_price,
                best_bid = excluded.best_bid,
                best_ask = excluded.best_ask,
                spread = excluded.spread,
                fee_status = excluded.fee_status,
                updated_at = excluded.updated_at
            """,
            row,
        )

    def upsert_event(self, row: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO event_catalog (
                event_id, slug, title, description, start_date, end_date, active,
                closed, archived, liquidity, volume, open_interest, tags_json, updated_at
            ) VALUES (
                :event_id, :slug, :title, :description, :start_date, :end_date, :active,
                :closed, :archived, :liquidity, :volume, :open_interest, :tags_json, :updated_at
            )
            ON CONFLICT(event_id) DO UPDATE SET
                slug = excluded.slug,
                title = excluded.title,
                description = excluded.description,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                active = excluded.active,
                closed = excluded.closed,
                archived = excluded.archived,
                liquidity = excluded.liquidity,
                volume = excluded.volume,
                open_interest = excluded.open_interest,
                tags_json = excluded.tags_json,
                updated_at = excluded.updated_at
            """,
            row,
        )

    def upsert_tag(self, row: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO tag_catalog(tag_id, label, slug, updated_at)
            VALUES (:tag_id, :label, :slug, :updated_at)
            ON CONFLICT(tag_id) DO UPDATE SET
                label = excluded.label,
                slug = excluded.slug,
                updated_at = excluded.updated_at
            """,
            row,
        )

    def replace_trade_eligibility(
        self, rows: list[tuple[str, str, str]], updated_at: str
    ) -> None:
        self.conn.execute("DELETE FROM market_trade_eligibility")
        self.conn.executemany(
            """
            INSERT INTO market_trade_eligibility(market_id, fee_status, reason, updated_at)
            VALUES(?, ?, ?, ?)
            """,
            [(market_id, status, reason, updated_at) for market_id, status, reason in rows],
        )
        self.conn.commit()

    def commit(self) -> None:
        self.conn.commit()

    def latest_active_markets(self, limit: int = 200) -> list[sqlite3.Row]:
        cursor = self.conn.execute(
            """
            SELECT *
            FROM market_catalog
            WHERE active = 1 AND closed = 0 AND archived = 0
            ORDER BY volume DESC
            LIMIT ?
            """,
            (limit,),
        )
        return list(cursor.fetchall())

    def insert_price_points(
        self,
        market_id: str,
        token_id: str,
        interval: str,
        points: list[tuple[str, float]],
    ) -> int:
        inserted = 0
        for ts_utc, price in points:
            cursor = self.conn.execute(
                """
                INSERT OR IGNORE INTO market_price_history(
                    market_id, token_id, interval, ts_utc, price
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (market_id, token_id, interval, ts_utc, price),
            )
            inserted += cursor.rowcount
        self.conn.commit()
        return inserted

    def upsert_price_metrics(self, row: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO market_price_metrics (
                market_id, token_id, interval, volatility_1h, volatility_6h,
                volatility_24h, price_change_24h, hours_to_resolution,
                recent_trade_density, max_drawdown_nh, updated_at
            ) VALUES (
                :market_id, :token_id, :interval, :volatility_1h, :volatility_6h,
                :volatility_24h, :price_change_24h, :hours_to_resolution,
                :recent_trade_density, :max_drawdown_nh, :updated_at
            )
            ON CONFLICT(market_id) DO UPDATE SET
                token_id = excluded.token_id,
                interval = excluded.interval,
                volatility_1h = excluded.volatility_1h,
                volatility_6h = excluded.volatility_6h,
                volatility_24h = excluded.volatility_24h,
                price_change_24h = excluded.price_change_24h,
                hours_to_resolution = excluded.hours_to_resolution,
                recent_trade_density = excluded.recent_trade_density,
                max_drawdown_nh = excluded.max_drawdown_nh,
                updated_at = excluded.updated_at
            """,
            row,
        )
        self.conn.commit()

    def insert_external_document(self, row: dict[str, Any]) -> bool:
        cursor = self.conn.execute(
            """
            INSERT OR IGNORE INTO external_documents (
                source, author, publisher, title, body, url, publish_time,
                first_seen_time, tags_json, raw_text_hash, source_tier, source_classification
            ) VALUES (
                :source, :author, :publisher, :title, :body, :url, :publish_time,
                :first_seen_time, :tags_json, :raw_text_hash, :source_tier, :source_classification
            )
            """,
            row,
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def fetch_unanalyzed_documents(self, limit: int = 100) -> list[sqlite3.Row]:
        cursor = self.conn.execute(
            """
            SELECT d.*
            FROM external_documents d
            LEFT JOIN document_ai_analysis a ON a.document_id = d.document_id
            WHERE a.document_id IS NULL
               OR a.summary IS NULL
               OR a.analysis_json IS NULL
            ORDER BY d.first_seen_time DESC
            LIMIT ?
            """,
            (limit,),
        )
        return list(cursor.fetchall())

    def insert_document_analysis(self, document_id: int, row: dict[str, Any]) -> None:
        payload = {
            "document_id": document_id,
            "entities_json": json.dumps(row["entities"], ensure_ascii=False),
            "directly_affects_resolution": 1 if row["directly_affects_resolution"] else 0,
            "market_relevance": row["market_relevance"],
            "resolution_relevance": row["resolution_relevance"],
            "source_quality": row["source_quality"],
            "novelty": row["novelty"],
            "direction": row["direction"],
            "confidence": row["confidence"],
            "event_type": row["event_type"],
            "summary": row["summary"],
            "why": row["why"],
            "time_sensitivity": row["time_sensitivity"],
            "analysis_json": row["analysis_json"],
            "raw_ai_response": row["raw_ai_response"],
            "analysis_model": row["analysis_model"],
            "analyzed_at": row.get("analyzed_at", iso_utc()),
        }
        self.conn.execute(
            """
            INSERT OR REPLACE INTO document_ai_analysis (
                document_id, market_relevance, resolution_relevance, source_quality, novelty,
                direction, confidence, event_type, directly_affects_resolution, summary, why,
                entities_json, time_sensitivity, analysis_json, raw_ai_response,
                analysis_model, analyzed_at
            ) VALUES (
                :document_id, :market_relevance, :resolution_relevance, :source_quality, :novelty,
                :direction, :confidence, :event_type, :directly_affects_resolution, :summary, :why,
                :entities_json, :time_sensitivity, :analysis_json, :raw_ai_response,
                :analysis_model, :analyzed_at
            )
            """,
            payload,
        )
        self.conn.commit()

    def fetch_recent_documents_with_analysis(self, limit: int = 200) -> list[sqlite3.Row]:
        cursor = self.conn.execute(
            """
            SELECT d.*, a.market_relevance, a.resolution_relevance, a.source_quality,
                   a.novelty, a.direction, a.confidence, a.event_type,
                   a.directly_affects_resolution, a.summary, a.why, a.entities_json,
                   a.time_sensitivity, a.analysis_json
            FROM external_documents d
            JOIN document_ai_analysis a ON a.document_id = d.document_id
            ORDER BY d.first_seen_time DESC
            LIMIT ?
            """,
            (limit,),
        )
        return list(cursor.fetchall())

    def upsert_market_document_link(self, row: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO market_document_links (
                market_id, document_id, rules_score, ai_relevance, final_score,
                link_reason, is_direct_impact, created_at
            ) VALUES (
                :market_id, :document_id, :rules_score, :ai_relevance, :final_score,
                :link_reason, :is_direct_impact, :created_at
            )
            ON CONFLICT(market_id, document_id) DO UPDATE SET
                rules_score = excluded.rules_score,
                ai_relevance = excluded.ai_relevance,
                final_score = excluded.final_score,
                link_reason = excluded.link_reason,
                is_direct_impact = excluded.is_direct_impact,
                created_at = excluded.created_at
            """,
            row,
        )
        self.conn.commit()

    def reset_trade_candidates(self) -> None:
        today = utc_now().date().isoformat()
        self.conn.execute(
            "DELETE FROM trade_candidates WHERE substr(created_at, 1, 10) = ?",
            (today,),
        )
        self.conn.commit()

    def insert_trade_candidate(self, row: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO trade_candidates(
                market_id, document_id, worth_research, score, reason,
                acceptance_reason, rejection_reason, rule_trace_json, created_at
            ) VALUES (
                :market_id, :document_id, :worth_research, :score, :reason,
                :acceptance_reason, :rejection_reason, :rule_trace_json, :created_at
            )
            """,
            row,
        )
        self.conn.commit()

    def fetch_today_trade_candidates(self, limit: int = 100) -> list[sqlite3.Row]:
        today = utc_now().date().isoformat()
        cursor = self.conn.execute(
            """
            SELECT *
            FROM trade_candidates
            WHERE substr(created_at, 1, 10) = ?
            ORDER BY score DESC
            LIMIT ?
            """,
            (today, limit),
        )
        return list(cursor.fetchall())

    def insert_paper_signal(self, row: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO paper_signals(
                ts_utc, market_id, document_id, direction_suggestion, trigger_reason,
                confidence, worth_research, score, rule_trace_json, created_at
            ) VALUES (
                :ts_utc, :market_id, :document_id, :direction_suggestion, :trigger_reason,
                :confidence, :worth_research, :score, :rule_trace_json, :created_at
            )
            """,
            row,
        )
        self.conn.commit()

    def reset_today_paper_signals(self) -> None:
        today = utc_now().date().isoformat()
        self.conn.execute(
            "DELETE FROM paper_signals WHERE substr(created_at, 1, 10) = ?",
            (today,),
        )
        self.conn.commit()

    def fetch_today_paper_signals(self, limit: int = 200) -> list[sqlite3.Row]:
        today = utc_now().date().isoformat()
        cursor = self.conn.execute(
            """
            SELECT *
            FROM paper_signals
            WHERE substr(created_at, 1, 10) = ?
            ORDER BY ts_utc DESC
            LIMIT ?
            """,
            (today, limit),
        )
        return list(cursor.fetchall())

    def fetch_entry_signals(
        self,
        limit: int = 300,
        requires_worth_research: bool = False,
    ) -> list[sqlite3.Row]:
        if requires_worth_research:
            cursor = self.conn.execute(
                """
                SELECT *
                FROM paper_signals
                WHERE worth_research = 1
                ORDER BY ts_utc ASC, score DESC, id ASC
                LIMIT ?
                """,
                (limit,),
            )
        else:
            cursor = self.conn.execute(
                """
                SELECT *
                FROM paper_signals
                ORDER BY ts_utc ASC, score DESC, id ASC
                LIMIT ?
                """,
                (limit,),
            )
        return list(cursor.fetchall())

    def fetch_market_signals_after(self, market_id: str, after_ts_utc: str) -> list[sqlite3.Row]:
        cursor = self.conn.execute(
            """
            SELECT *
            FROM paper_signals
            WHERE market_id = ? AND ts_utc > ?
            ORDER BY ts_utc ASC, id ASC
            """,
            (market_id, after_ts_utc),
        )
        return list(cursor.fetchall())

    def get_market_snapshot(self, market_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT market_id, end_date, last_price, best_bid, best_ask, spread
            FROM market_catalog
            WHERE market_id = ?
            """,
            (market_id,),
        ).fetchone()

    def has_open_position_for_market_direction(self, market_id: str, direction: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1
            FROM paper_positions
            WHERE market_id = ? AND direction = ? AND status = 'open'
            LIMIT 1
            """,
            (market_id, direction),
        ).fetchone()
        return row is not None

    def has_position_for_signal_key(self, signal_key: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM paper_positions WHERE signal_key = ? LIMIT 1",
            (signal_key,),
        ).fetchone()
        return row is not None

    def insert_paper_position(self, row: dict[str, Any]) -> int:
        cursor = self.conn.execute(
            """
            INSERT INTO paper_positions(
                signal_key, signal_id, market_id, document_id, direction, entry_ts_utc,
                entry_price, stake_total, stake_open, confidence, status,
                open_reason, created_at, updated_at
            ) VALUES (
                :signal_key, :signal_id, :market_id, :document_id, :direction, :entry_ts_utc,
                :entry_price, :stake_total, :stake_open, :confidence, :status,
                :open_reason, :created_at, :updated_at
            )
            """,
            row,
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def fetch_open_positions(self, limit: int = 1000) -> list[sqlite3.Row]:
        cursor = self.conn.execute(
            """
            SELECT *
            FROM paper_positions
            WHERE status = 'open'
            ORDER BY entry_ts_utc ASC, id ASC
            LIMIT ?
            """,
            (limit,),
        )
        return list(cursor.fetchall())

    def fetch_all_positions(self, limit: int = 1000) -> list[sqlite3.Row]:
        cursor = self.conn.execute(
            """
            SELECT *
            FROM paper_positions
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return list(cursor.fetchall())

    def update_paper_position_on_exit(
        self,
        *,
        position_id: int,
        close_price: float,
        close_ts_utc: str,
        closed_stake: float,
        realized_pnl_delta: float,
        status: str,
        close_reason: str,
    ) -> None:
        self.conn.execute(
            """
            UPDATE paper_positions
            SET stake_open = CASE
                    WHEN stake_open - ? < 0 THEN 0
                    ELSE stake_open - ?
                END,
                close_price = ?,
                close_ts_utc = ?,
                realized_pnl = COALESCE(realized_pnl, 0) + ?,
                status = ?,
                close_reason = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                closed_stake,
                closed_stake,
                close_price,
                close_ts_utc,
                realized_pnl_delta,
                status,
                close_reason,
                iso_utc(),
                position_id,
            ),
        )
        self.conn.commit()

    def fetch_today_signal_market_ids(self, limit: int = 300) -> list[str]:
        today = utc_now().date().isoformat()
        cursor = self.conn.execute(
            """
            SELECT DISTINCT market_id
            FROM paper_signals
            WHERE substr(created_at, 1, 10) = ?
            ORDER BY market_id ASC
            LIMIT ?
            """,
            (today, limit),
        )
        return [str(row["market_id"]) for row in cursor.fetchall()]

    def reset_today_paper_trades(self) -> None:
        today = utc_now().date().isoformat()
        self.conn.execute(
            "DELETE FROM paper_trades WHERE substr(created_at, 1, 10) = ?",
            (today,),
        )
        self.conn.commit()

    def insert_paper_trade(self, row: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO paper_trades(
                position_id, signal_id, signal_key, ts_utc, market_id, document_id, direction,
                entry_ts_utc, entry_price, exit_ts_utc, exit_price, closed_stake, return_pct, pnl,
                status, reason, created_at
            ) VALUES (
                :position_id, :signal_id, :signal_key, :ts_utc, :market_id, :document_id, :direction,
                :entry_ts_utc, :entry_price, :exit_ts_utc, :exit_price, :closed_stake, :return_pct, :pnl,
                :status, :reason, :created_at
            )
            """,
            row,
        )
        self.conn.commit()

    def fetch_market_price_series(
        self,
        market_id: str,
        limit: int = 5000,
        interval: str | None = None,
    ) -> list[sqlite3.Row]:
        if interval:
            cursor = self.conn.execute(
                """
                SELECT ts_utc, price
                FROM market_price_history
                WHERE market_id = ? AND interval = ?
                ORDER BY ts_utc ASC
                LIMIT ?
                """,
                (market_id, interval, limit),
            )
        else:
            cursor = self.conn.execute(
                """
                SELECT ts_utc, price
                FROM market_price_history
                WHERE market_id = ?
                ORDER BY ts_utc ASC
                LIMIT ?
                """,
                (market_id, limit),
            )
        return list(cursor.fetchall())

    def fetch_market_token_ids(self, market_id: str) -> list[str]:
        row = self.conn.execute(
            "SELECT token_ids_json FROM market_catalog WHERE market_id = ?",
            (market_id,),
        ).fetchone()
        if row is None:
            return []
        try:
            token_ids = json.loads(row["token_ids_json"] or "[]")
        except json.JSONDecodeError:
            token_ids = []
        return [str(token_id) for token_id in token_ids]

    def fetch_today_paper_trade_summary(self) -> dict[str, float]:
        today = utc_now().date().isoformat()
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) AS closed_rows,
                SUM(CASE WHEN status = 'partial_exit' THEN 1 ELSE 0 END) AS partial_rows,
                SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped_rows,
                SUM(CASE WHEN status = 'closed' AND pnl > 0 THEN 1 ELSE 0 END) AS win_rows,
                SUM(CASE WHEN status = 'closed' AND pnl = 0 THEN 1 ELSE 0 END) AS breakeven_rows,
                SUM(CASE WHEN status = 'closed' AND pnl < 0 THEN 1 ELSE 0 END) AS loss_rows,
                COALESCE(SUM(CASE WHEN status = 'closed' THEN pnl ELSE 0 END), 0.0) AS total_pnl,
                COALESCE(AVG(CASE WHEN status = 'closed' THEN return_pct END), 0.0) AS avg_return_pct
            FROM paper_trades
            WHERE substr(created_at, 1, 10) = ?
            """,
            (today,),
        ).fetchone()
        total_rows = int(row["total_rows"] or 0)
        closed_rows = int(row["closed_rows"] or 0)
        win_rows = int(row["win_rows"] or 0)
        win_rate = float(win_rows / closed_rows) if closed_rows > 0 else 0.0
        return {
            "total_rows": float(total_rows),
            "closed_rows": float(closed_rows),
            "partial_rows": float(int(row["partial_rows"] or 0)),
            "skipped_rows": float(int(row["skipped_rows"] or 0)),
            "win_rows": float(win_rows),
            "breakeven_rows": float(int(row["breakeven_rows"] or 0)),
            "loss_rows": float(int(row["loss_rows"] or 0)),
            "total_pnl": float(row["total_pnl"] or 0.0),
            "avg_return_pct": float(row["avg_return_pct"] or 0.0),
            "win_rate": win_rate,
        }

    def fetch_open_position_summary(self) -> dict[str, float]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS open_positions,
                COALESCE(SUM(stake_open), 0.0) AS open_stake
            FROM paper_positions
            WHERE status = 'open'
            """
        ).fetchone()
        return {
            "open_positions": float(int(row["open_positions"] or 0)),
            "open_stake": float(row["open_stake"] or 0.0),
        }

    def fetch_today_paper_trades(self, limit: int = 100) -> list[sqlite3.Row]:
        today = utc_now().date().isoformat()
        cursor = self.conn.execute(
            """
            SELECT *
            FROM paper_trades
            WHERE substr(created_at, 1, 10) = ?
            ORDER BY ts_utc DESC
            LIMIT ?
            """,
            (today, limit),
        )
        return list(cursor.fetchall())

    def record_sync_run(
        self,
        *,
        stage: str,
        status: str,
        details: dict[str, Any] | None,
        error_text: str | None,
        started_at: str,
        finished_at: str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO sync_runs(
                run_date, stage, status, details_json, error_text, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_now().date().isoformat(),
                stage,
                status,
                json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
                error_text,
                started_at,
                finished_at,
            ),
        )
        self.conn.commit()

    def fetch_daily_audit_rows(self, report_date: str, limit: int = 300) -> list[sqlite3.Row]:
        cursor = self.conn.execute(
            """
            SELECT c.market_id, c.document_id, c.worth_research, c.score, c.reason,
                   c.acceptance_reason, c.rejection_reason, c.rule_trace_json, c.created_at,
                   l.link_reason,
                   m.slug AS market_slug, m.title AS market_title, m.rules AS market_rules,
                   m.tags_json AS market_tags_json, m.end_date AS market_end_date,
                   m.active AS market_active, m.closed AS market_closed, m.archived AS market_archived,
                   m.last_price, m.best_bid, m.best_ask, m.spread,
                   d.url AS document_url, d.title AS document_title, d.body AS document_body,
                   d.publish_time AS document_publish_time, d.first_seen_time AS document_first_seen_time,
                   d.source, d.source_tier, d.source_classification, d.raw_text_hash,
                   a.analysis_json, a.raw_ai_response, a.analysis_model, a.analyzed_at
            FROM trade_candidates c
            JOIN market_document_links l
              ON l.market_id = c.market_id AND l.document_id = c.document_id
            JOIN market_catalog m ON m.market_id = c.market_id
            JOIN external_documents d ON d.document_id = c.document_id
            JOIN document_ai_analysis a ON a.document_id = c.document_id
            WHERE substr(c.created_at, 1, 10) = ?
            ORDER BY c.score DESC
            LIMIT ?
            """,
            (report_date, limit),
        )
        return list(cursor.fetchall())
