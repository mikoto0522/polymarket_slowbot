from src.ai.contract import validate_ai_output


def test_contract_validation_passes() -> None:
    payload = {
        "market_relevance": 0.8,
        "resolution_relevance": 0.7,
        "source_quality": 0.9,
        "novelty": 0.75,
        "direction": "positive",
        "confidence": 0.82,
        "event_type": "announcement",
        "directly_affects_resolution": True,
        "summary": "Official source update confirms event direction.",
        "why": "Official source update",
        "entities": ["SEC", "ETF"],
        "time_sensitivity": "high",
    }
    validate_ai_output(payload)
