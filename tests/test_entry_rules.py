from src.strategy.entry_rules import EntryInput, should_enter


def test_should_enter_true() -> None:
    ok, reasons = should_enter(
        EntryInput(
            directly_affects_resolution=True,
            source_quality=0.9,
            confidence=0.8,
            novelty=0.8,
            spread=0.03,
            recent_volatility_30m=0.02,
            volatility_threshold_30m=0.05,
            mispricing_gap=0.1,
            hours_to_resolution=24,
        )
    )
    assert ok is True
    assert reasons == []
