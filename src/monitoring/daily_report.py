from __future__ import annotations

from pathlib import Path

from ..utils.db import Database
from ..utils.time import utc_now


def generate_daily_report(db: Database, report_dir: Path) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    now = utc_now()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    file_path = report_dir / f"daily_report_{stamp}.md"

    rows = db.conn.execute(
        """
        SELECT c.market_id, c.document_id, c.worth_research, c.score, c.reason,
               c.acceptance_reason, c.rejection_reason, c.created_at,
               m.title AS market_title, m.slug AS market_slug, m.fee_status, m.spread,
               d.title AS doc_title, d.url AS doc_url, d.source_classification,
               a.confidence AS ai_confidence, a.market_relevance, a.resolution_relevance,
               a.summary AS ai_summary, a.why AS ai_why, a.direction AS ai_direction,
               l.link_reason
        FROM trade_candidates c
        JOIN market_catalog m ON m.market_id = c.market_id
        JOIN external_documents d ON d.document_id = c.document_id
        JOIN document_ai_analysis a ON a.document_id = c.document_id
        JOIN market_document_links l
          ON l.market_id = c.market_id AND l.document_id = c.document_id
        WHERE substr(c.created_at, 1, 10) = ?
        ORDER BY c.score DESC, c.market_id ASC, c.document_id ASC
        LIMIT 100
        """,
        (now.date().isoformat(),),
    ).fetchall()

    paper_summary = db.fetch_today_paper_trade_summary()
    paper_trades = db.fetch_today_paper_trades(limit=100)
    signals = db.fetch_today_paper_signals(limit=200)

    lines = [f"# Daily Review ({now.date().isoformat()} UTC)", "", "## Candidate Review List", ""]

    if not rows:
        lines.extend(
            [
                "- No candidate rows today.",
                "",
                "Conclusion: no opportunity today (or not enough quality evidence).",
            ]
        )
    else:
        for idx, row in enumerate(rows, start=1):
            worth = "yes" if row["worth_research"] else "no"
            lines.extend(
                [
                    f"### {idx}. {row['market_title']}",
                    f"- market_name: `{row['market_title']}`",
                    f"- market_ref: `{row['market_id']}` / `{row['market_slug']}`",
                    f"- linked_message: [{row['doc_title']}]({row['doc_url']})",
                    f"- link_reason: `{row['link_reason']}`",
                    f"- ai_summary: `{row['ai_summary']}`",
                    f"- ai_reasoning: `{row['ai_why']}`",
                    f"- direction_suggestion: `{row['ai_direction']}`",
                    f"- worth_research: `{worth}`",
                    f"- reason: `{row['acceptance_reason'] or row['reason']}`",
                    f"- rejection_reason: `{row['rejection_reason'] or 'none'}`",
                    f"- score: `{row['score']:.4f}`",
                    f"- source_classification: `{row['source_classification']}`",
                    f"- ai_confidence: `{row['ai_confidence']:.2f}`",
                    f"- market_relevance/resolution_relevance: "
                    f"`{row['market_relevance']:.2f}/{row['resolution_relevance']:.2f}`",
                    f"- fee_status/spread: `{row['fee_status']}` / `{row['spread']}`",
                    "",
                ]
            )

    lines.extend(["## Paper Trading PnL", ""])
    lines.extend(
        [
            f"- total_rows: `{int(paper_summary.get('total_rows', 0.0))}`",
            f"- closed_rows: `{int(paper_summary.get('closed_rows', 0.0))}`",
            f"- skipped_rows: `{int(paper_summary.get('skipped_rows', 0.0))}`",
            f"- wins/losses: `{int(paper_summary.get('win_rows', 0.0))}/"
            f"{int(paper_summary.get('loss_rows', 0.0))}`",
            f"- win_rate: `{paper_summary.get('win_rate', 0.0):.2%}`",
            f"- avg_return_pct: `{paper_summary.get('avg_return_pct', 0.0):.4%}`",
            f"- total_pnl (stake units): `{paper_summary.get('total_pnl', 0.0):.6f}`",
            "",
        ]
    )
    if paper_trades:
        lines.append("### Recent Paper Trades")
        lines.append("")
        for idx, trade in enumerate(paper_trades, start=1):
            lines.extend(
                [
                    f"#### trade-{idx}",
                    f"- market: `{trade['market_id']}`",
                    f"- direction: `{trade['direction']}`",
                    f"- status: `{trade['status']}`",
                    f"- reason: `{trade['reason']}`",
                    f"- entry: `{trade['entry_ts_utc']}` @ `{trade['entry_price']}`",
                    f"- exit: `{trade['exit_ts_utc']}` @ `{trade['exit_price']}`",
                    f"- return_pct: `{trade['return_pct']}`",
                    f"- pnl: `{trade['pnl']}`",
                    "",
                ]
            )

    lines.extend(["## Shadow Mode Signals", ""])
    if not signals:
        lines.append("- No paper signals today.")
    else:
        for idx, signal in enumerate(signals, start=1):
            lines.extend(
                [
                    f"### signal-{idx}",
                    f"- time: `{signal['ts_utc']}`",
                    f"- market: `{signal['market_id']}`",
                    f"- direction_suggestion: `{signal['direction_suggestion']}`",
                    f"- trigger_reason: `{signal['trigger_reason']}`",
                    f"- confidence: `{signal['confidence']:.2f}`",
                    "",
                ]
            )

    file_path.write_text("\n".join(lines), encoding="utf-8")
    return file_path
