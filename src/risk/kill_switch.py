from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class KillSwitchState:
    enabled: bool = False
    triggers: list[str] = field(default_factory=list)

    def evaluate(
        self,
        api_errors_in_a_row: int,
        time_sync_ok: bool,
        data_source_alive: bool,
        ai_invalid_rate: float,
        daily_pnl: float,
        order_state_consistent: bool,
        position_confirmed: bool,
    ) -> "KillSwitchState":
        self.triggers.clear()
        if api_errors_in_a_row >= 5:
            self.triggers.append("api_consecutive_errors")
        if not time_sync_ok:
            self.triggers.append("time_sync_abnormal")
        if not data_source_alive:
            self.triggers.append("data_source_down")
        if ai_invalid_rate > 0.2:
            self.triggers.append("ai_invalid_rate_high")
        if daily_pnl <= -12.0:
            self.triggers.append("daily_loss_limit")
        if not order_state_consistent:
            self.triggers.append("order_state_inconsistent")
        if not position_confirmed:
            self.triggers.append("position_not_confirmed")
        self.enabled = len(self.triggers) > 0
        return self

    def can_open_new_position(self) -> bool:
        return not self.enabled

    def can_close_position(self) -> bool:
        return True
