# polymarket_slowbot

## Quick Start
1. Create `.env` from `.env.example`.
2. Run:

```bash
python -m src.main
```

## Server Deploy
- Linux deployment guide: [`DEPLOY_SERVER_CN.md`](DEPLOY_SERVER_CN.md)
- Helper scripts:
  - `scripts/server_setup.sh`
  - `scripts/server_run.sh`
  - `scripts/install_systemd.sh`

## Current Scope
- Daily market catalog sync (`events/markets/tags`).
- One-market price history sync.
- RSS external news ingest + dedup.
- AI JSON extraction with contract validation (includes summary and reasoning).
- Deterministic AI mode for reproducible reruns (`ai.deterministic_mode=true`).
- Market-message linking with explicit link reasons.
- Candidate selector + rejection reasons + daily markdown report.
- Shadow mode paper signals (time/market/direction/reason/confidence).
- Live-like paper trading:
  - signal-driven simulated open positions (`paper_positions`)
  - rule-driven simulated exits (`paper_trades`)
  - open positions + trade history + PnL in daily report
- Audit bundle output tracing report to raw market, raw message, AI JSON, and rule decision.
