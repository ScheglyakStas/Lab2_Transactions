"""Pure fraud-detection logic (no Kafka), so it can be unit-tested in isolation.

Rules (all evaluated on event time, per cardholder):

* CITY_HOPPING  - 3+ transactions within 2 minutes that are not all in the same
                  city (minimum number of distinct cities is configurable).
* RAPID_FIRE    - 3+ transactions within 30 seconds.
* LARGE_AMOUNT_NEW_CITY - amount >= 10x the cardholder's average AND the city
                  has never been seen for that cardholder before.

When any rule fires, the transaction is declined and the card is blocked. Every
later transaction on a blocked card is declined with reason CARD_BLOCKED.
Only approved transactions feed the sliding windows and the user profile.
"""

from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime

from .models import Transaction

APPROVED = "APPROVED"
DECLINED = "DECLINED"
REASON_FRAUD = "FRAUD_DETECTED"
REASON_CARD_BLOCKED = "CARD_BLOCKED"

RULE_CITY_HOPPING = "CITY_HOPPING"
RULE_RAPID_FIRE = "RAPID_FIRE"
RULE_LARGE_NEW_CITY = "LARGE_AMOUNT_NEW_CITY"


@dataclass(frozen=True)
class RuleConfig:
    city_hop_window_s: float = 120.0
    city_hop_min_tx: int = 3
    city_hop_min_cities: int = 2
    rapid_window_s: float = 30.0
    rapid_min_tx: int = 3
    large_multiplier: float = 10.0
    large_min_history: int = 5   # need some history before an "average" means anything

    @classmethod
    def from_env(cls) -> "RuleConfig":
        d = cls()
        return cls(
            city_hop_window_s=float(os.getenv("RULE_CITY_HOP_WINDOW_S", d.city_hop_window_s)),
            city_hop_min_tx=int(os.getenv("RULE_CITY_HOP_MIN_TX", d.city_hop_min_tx)),
            city_hop_min_cities=int(os.getenv("RULE_CITY_HOP_MIN_CITIES", d.city_hop_min_cities)),
            rapid_window_s=float(os.getenv("RULE_RAPID_WINDOW_S", d.rapid_window_s)),
            rapid_min_tx=int(os.getenv("RULE_RAPID_MIN_TX", d.rapid_min_tx)),
            large_multiplier=float(os.getenv("RULE_LARGE_MULTIPLIER", d.large_multiplier)),
            large_min_history=int(os.getenv("RULE_LARGE_MIN_HISTORY", d.large_min_history)),
        )


@dataclass
class UserProfile:
    tx_count: int = 0
    total_amount: float = 0.0
    cities: set[str] = field(default_factory=set)

    @property
    def average(self) -> float:
        return self.total_amount / self.tx_count if self.tx_count else 0.0

    def add(self, tx: Transaction) -> None:
        self.tx_count += 1
        self.total_amount += tx.amount
        self.cities.add(tx.city)


@dataclass(frozen=True)
class RuleHit:
    rule: str
    description: str
    related_transaction_ids: tuple[str, ...]


@dataclass(frozen=True)
class BlockInfo:
    card_number: str
    cardholder_name: str
    blocked_at: datetime
    transaction_id: str
    rules: tuple[str, ...]


@dataclass(frozen=True)
class Decision:
    status: str
    reason: str | None = None
    hits: tuple[RuleHit, ...] = ()
    card_blocked_now: bool = False

    @property
    def approved(self) -> bool:
        return self.status == APPROVED

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "reason": self.reason,
            "card_blocked_now": self.card_blocked_now,
            "rules": [
                {
                    "rule": h.rule,
                    "description": h.description,
                    "related_transaction_ids": list(h.related_transaction_ids),
                }
                for h in self.hits
            ],
        }


class FraudDetector:
    def __init__(self, config: RuleConfig | None = None) -> None:
        self.config = config or RuleConfig()
        self._window_s = max(self.config.city_hop_window_s, self.config.rapid_window_s)
        self._recent: dict[str, deque[Transaction]] = {}
        self._profiles: dict[str, UserProfile] = {}
        self._blocked: dict[str, BlockInfo] = {}

    # ----------------------------------------------------------------- queries
    def is_blocked(self, card_number: str) -> bool:
        return card_number in self._blocked

    def block_info(self, card_number: str) -> BlockInfo | None:
        return self._blocked.get(card_number)

    def profile(self, cardholder_name: str) -> UserProfile:
        return self._profiles.get(cardholder_name, UserProfile())

    @property
    def blocked_cards(self) -> dict[str, BlockInfo]:
        return dict(self._blocked)

    # -------------------------------------------------------------- main entry
    def evaluate(self, tx: Transaction) -> Decision:
        if tx.card_number in self._blocked:
            return Decision(status=DECLINED, reason=REASON_CARD_BLOCKED)

        person = tx.cardholder_name
        recent = self._recent.setdefault(person, deque())
        self._prune(recent, tx.timestamp)

        hits = [
            h for h in (
                self._check_city_hopping(recent, tx),
                self._check_rapid_fire(recent, tx),
                self._check_large_new_city(person, tx),
            ) if h is not None
        ]

        if hits:
            self._blocked[tx.card_number] = BlockInfo(
                card_number=tx.card_number,
                cardholder_name=person,
                blocked_at=tx.timestamp,
                transaction_id=tx.transaction_id,
                rules=tuple(h.rule for h in hits),
            )
            # The suspicious burst has been dealt with; start the windows afresh
            # so the replacement card is not punished for the old card's activity.
            recent.clear()
            return Decision(status=DECLINED, reason=REASON_FRAUD,
                            hits=tuple(hits), card_blocked_now=True)

        recent.append(tx)
        self._profiles.setdefault(person, UserProfile()).add(tx)
        return Decision(status=APPROVED)

    # ------------------------------------------------------------------ rules
    def _prune(self, recent: deque[Transaction], now: datetime) -> None:
        while recent and (now - recent[0].timestamp).total_seconds() > self._window_s:
            recent.popleft()

    @staticmethod
    def _within(recent: deque[Transaction], tx: Transaction, window_s: float) -> list[Transaction]:
        return [t for t in recent
                if 0 <= (tx.timestamp - t.timestamp).total_seconds() <= window_s] + [tx]

    def _check_city_hopping(self, recent: deque[Transaction], tx: Transaction) -> RuleHit | None:
        cfg = self.config
        txs = self._within(recent, tx, cfg.city_hop_window_s)
        cities = list(dict.fromkeys(t.city for t in txs))
        if len(txs) >= cfg.city_hop_min_tx and len(cities) >= cfg.city_hop_min_cities:
            return RuleHit(
                rule=RULE_CITY_HOPPING,
                description=(f"{len(txs)} tx in {len(cities)} cities within "
                             f"{cfg.city_hop_window_s:.0f}s ({', '.join(cities)})"),
                related_transaction_ids=tuple(t.transaction_id for t in txs),
            )
        return None

    def _check_rapid_fire(self, recent: deque[Transaction], tx: Transaction) -> RuleHit | None:
        cfg = self.config
        txs = self._within(recent, tx, cfg.rapid_window_s)
        if len(txs) >= cfg.rapid_min_tx:
            span = (tx.timestamp - txs[0].timestamp).total_seconds()
            return RuleHit(
                rule=RULE_RAPID_FIRE,
                description=f"{len(txs)} tx within {span:.1f}s",
                related_transaction_ids=tuple(t.transaction_id for t in txs),
            )
        return None

    def _check_large_new_city(self, person: str, tx: Transaction) -> RuleHit | None:
        cfg = self.config
        profile = self._profiles.get(person)
        if profile is None or profile.tx_count < cfg.large_min_history:
            return None
        avg = profile.average
        if tx.amount >= cfg.large_multiplier * avg and tx.city not in profile.cities:
            return RuleHit(
                rule=RULE_LARGE_NEW_CITY,
                description=(f"{tx.amount / avg:.1f}x avg {avg:.2f} in new city "
                             f"(known: {', '.join(sorted(profile.cities))})"),
                related_transaction_ids=(tx.transaction_id,),
            )
        return None
