from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..utils.errors import AIValidationError

REQUIRED_FIELDS = {
    "market_relevance",
    "resolution_relevance",
    "source_quality",
    "novelty",
    "direction",
    "confidence",
    "event_type",
    "directly_affects_resolution",
    "summary",
    "why",
    "entities",
    "time_sensitivity",
}

DIRECTION_VALUES = {"positive", "negative", "neutral", "unknown"}
EVENT_TYPE_VALUES = {
    "announcement",
    "rumor",
    "data_release",
    "legal",
    "personnel",
    "other",
}
TIME_SENSITIVITY_VALUES = {"low", "medium", "high"}


def load_contract_schema(contract_path: Path) -> dict[str, Any]:
    return json.loads(contract_path.read_text(encoding="utf-8"))


def validate_ai_output(payload: dict[str, Any]) -> None:
    missing = REQUIRED_FIELDS - set(payload.keys())
    extra = set(payload.keys()) - REQUIRED_FIELDS
    if missing:
        raise AIValidationError(f"Missing required keys: {sorted(missing)}")
    if extra:
        raise AIValidationError(f"Unexpected keys: {sorted(extra)}")

    score_fields = [
        "market_relevance",
        "resolution_relevance",
        "source_quality",
        "novelty",
        "confidence",
    ]
    for field in score_fields:
        value = payload[field]
        if not isinstance(value, (float, int)) or not (0.0 <= float(value) <= 1.0):
            raise AIValidationError(f"{field} must be number in [0.0, 1.0]")

    if payload["direction"] not in DIRECTION_VALUES:
        raise AIValidationError("Invalid direction value")
    if payload["event_type"] not in EVENT_TYPE_VALUES:
        raise AIValidationError("Invalid event_type value")
    if payload["time_sensitivity"] not in TIME_SENSITIVITY_VALUES:
        raise AIValidationError("Invalid time_sensitivity value")
    if not isinstance(payload["directly_affects_resolution"], bool):
        raise AIValidationError("directly_affects_resolution must be boolean")
    if not isinstance(payload["summary"], str) or not payload["summary"].strip():
        raise AIValidationError("summary must be a non-empty string")
    if not isinstance(payload["why"], str) or not payload["why"].strip():
        raise AIValidationError("why must be a non-empty string")
    if not isinstance(payload["entities"], list):
        raise AIValidationError("entities must be an array")
    if not all(isinstance(item, str) for item in payload["entities"]):
        raise AIValidationError("entities must be array of strings")
