"""Domain model shared by every service (no Kafka dependency here)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

CITIES: tuple[str, ...] = (
    "New York", "London", "Paris", "Berlin", "Tokyo", "Warsaw", "Madrid",
    "Rome", "Vienna", "Prague", "Amsterdam", "Lisbon", "Dublin", "Oslo",
    "Stockholm", "Helsinki", "Minsk", "Vilnius", "Riga", "Tallinn",
    "Istanbul", "Dubai", "Singapore", "Sydney", "Toronto", "Chicago",
    "Los Angeles", "San Francisco", "Barcelona", "Budapest",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def mask_card(card_number: str) -> str:
    return f"**** {card_number[-4:]}"


@dataclass(frozen=True)
class Transaction:
    transaction_id: str      # primary key
    card_number: str
    cardholder_name: str
    timestamp: datetime      # event time, timezone-aware UTC
    city: str
    amount: float
    currency: str = "USD"

    def to_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "card_number": self.card_number,
            "cardholder_name": self.cardholder_name,
            "timestamp": self.timestamp.isoformat(timespec="milliseconds"),
            "city": self.city,
            "amount": round(self.amount, 2),
            "currency": self.currency,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Transaction":
        ts = datetime.fromisoformat(data["timestamp"])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        amount = float(data["amount"])
        if amount <= 0:
            raise ValueError(f"amount must be positive, got {amount}")
        return cls(
            transaction_id=str(data["transaction_id"]),
            card_number=str(data["card_number"]),
            cardholder_name=str(data["cardholder_name"]),
            timestamp=ts,
            city=str(data["city"]),
            amount=amount,
            currency=str(data.get("currency", "USD")),
        )
