from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .order_state_machine import OrderStateMachine
from ..utils.time import iso_utc


@dataclass
class Order:
    order_id: str
    market_id: str
    side: str
    price: float
    size: float
    status: str
    created_at: str
    updated_at: str
    filled_size: float = 0.0


@dataclass
class OrderManager:
    orders: dict[str, Order] = field(default_factory=dict)

    def place_limit_order(self, order_id: str, market_id: str, side: str, price: float, size: float) -> Order:
        order = Order(
            order_id=order_id,
            market_id=market_id,
            side=side,
            price=price,
            size=size,
            status="created",
            created_at=iso_utc(),
            updated_at=iso_utc(),
        )
        sm = OrderStateMachine(state="created")
        sm.transition("submitted")
        sm.transition("resting")
        order.status = sm.state
        order.updated_at = iso_utc()
        self.orders[order_id] = order
        return order

    def cancel_order(self, order_id: str) -> Order:
        order = self.orders[order_id]
        sm = OrderStateMachine(state=order.status)
        sm.transition("cancelled")
        order.status = sm.state
        order.updated_at = iso_utc()
        return order

    def replace_order(self, order_id: str, new_price: float, new_size: float) -> Order:
        self.cancel_order(order_id)
        order = self.orders[order_id]
        new_order_id = f"{order_id}-r"
        return self.place_limit_order(
            order_id=new_order_id,
            market_id=order.market_id,
            side=order.side,
            price=new_price,
            size=new_size,
        )

    def expire_order(self, order_id: str) -> Order:
        order = self.orders[order_id]
        sm = OrderStateMachine(state=order.status)
        sm.transition("expired")
        order.status = sm.state
        order.updated_at = iso_utc()
        return order

    def mark_partial_fill(self, order_id: str, fill_size: float) -> Order:
        order = self.orders[order_id]
        sm = OrderStateMachine(state=order.status)
        sm.transition("partially_filled")
        order.status = sm.state
        order.filled_size += fill_size
        order.updated_at = iso_utc()
        if order.filled_size >= order.size:
            sm = OrderStateMachine(state=order.status)
            sm.transition("filled")
            order.status = sm.state
            order.updated_at = iso_utc()
        return order

    def get_order(self, order_id: str) -> dict[str, Any]:
        order = self.orders[order_id]
        return {
            "order_id": order.order_id,
            "market_id": order.market_id,
            "side": order.side,
            "price": order.price,
            "size": order.size,
            "filled_size": order.filled_size,
            "status": order.status,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
        }
