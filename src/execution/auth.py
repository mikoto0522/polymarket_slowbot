from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class AuthConfig:
    wallet_private_key: str
    api_key: str
    read_only: bool = True

    @classmethod
    def from_env(cls) -> "AuthConfig":
        return cls(
            wallet_private_key=os.getenv("POLY_PRIVATE_KEY", ""),
            api_key=os.getenv("POLY_API_KEY", ""),
            read_only=os.getenv("POLYMARKET_READ_ONLY", "true").lower() == "true",
        )

    def can_send_real_order(self) -> bool:
        return not self.read_only and bool(self.wallet_private_key and self.api_key)
