from __future__ import annotations

from dataclasses import dataclass

VALID_TRANSITIONS = {
    "created": {"submitted", "cancelled", "rejected"},
    "submitted": {"resting", "partially_filled", "filled", "rejected", "expired"},
    "resting": {"partially_filled", "filled", "cancelled", "expired"},
    "partially_filled": {"filled", "cancelled", "expired"},
    "filled": set(),
    "cancelled": set(),
    "rejected": set(),
    "expired": set(),
}


@dataclass
class OrderStateMachine:
    state: str = "created"

    def transition(self, next_state: str) -> None:
        allowed = VALID_TRANSITIONS.get(self.state, set())
        if next_state not in allowed:
            raise ValueError(f"Invalid transition: {self.state} -> {next_state}")
        self.state = next_state
