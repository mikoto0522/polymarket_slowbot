from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass
class ReplayEvent:
    ts_utc: str
    kind: str
    payload: dict


def replay(events: Iterable[ReplayEvent]) -> list[ReplayEvent]:
    ordered = sorted(events, key=lambda e: e.ts_utc)
    return ordered
