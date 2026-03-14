# PROJECT_SCOPE

## 1) Candidate Market Catalog
- Sync active candidate markets once per day.
- Store core market fields:
  - title
  - rules
  - tags
  - resolution time
  - status
  - price summary
- Sync result must be traceable with update time and error logs.

## 2) External Message Ingestion
- Ingest at least one news/announcement source.
- New messages are auto-ingested, deduplicated, and stamped with first-seen time.
- Repeated fetches of the same message must not create duplicates.

## 3) AI Structured Analysis
- Every ingested message must produce a fixed JSON protocol.
- Output must pass JSON schema validation.
- Output must include at least:
  - relevance
  - trustworthiness
  - direct impact on resolution (yes/no)
  - summary
  - reasoning
- Original text and AI output must be traceable.

## 4) Market-Message Linking
- One message should link to only a small set of candidate markets.
- Every link must include a link reason.
- Perfect automation is not required in v1, but output must support human review.

## 5) Daily Research Report
- Generate a daily report:
  - candidate markets
  - linked messages
  - whether each is worth research
- Every candidate item must include:
  - market name
  - linked message
  - AI summary
  - worth research (yes/no)
  - reason
  - rejection reason (if not worth research)

## 6) Shadow Mode
- System outputs signals only and sends no live orders.
- All potential opportunities are recorded as paper signals.
- Every signal must include:
  - timestamp
  - market
  - suggested direction
  - trigger reason
  - confidence
- Signals must be replayable for post-mortem review.

## 7) Reproducibility
- With the same input dataset, repeated runs should produce near-identical outputs.
- Reproducibility is mandatory for AI/rules debugging.

## 8) Auditability
- Any daily report conclusion must be traceable to:
  - raw market data
  - raw message text
  - AI output JSON
  - final rule judgment
