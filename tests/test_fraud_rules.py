"""Unit tests for the fraud rules (no Kafka needed).

Run:  python -m unittest discover -s tests -v
"""

import sys
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bank_sim.fraud_rules import (  # noqa: E402
    REASON_CARD_BLOCKED, REASON_FRAUD, RULE_CITY_HOPPING, RULE_LARGE_NEW_CITY,
    RULE_RAPID_FIRE, FraudDetector,
)
from bank_sim.models import Transaction  # noqa: E402

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
CARD = "4000000000000002"


def tx(seconds: float, city: str = "Paris", amount: float = 50.0,
       name: str = "Alice", card: str = CARD) -> Transaction:
    return Transaction(str(uuid.uuid4()), card, name, T0 + timedelta(seconds=seconds), city, amount)


def rules(decision) -> set[str]:
    return {h.rule for h in decision.hits}


class FraudRuleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.d = FraudDetector()

    def test_normal_spacing_is_approved(self):
        for i, city in enumerate(["Paris", "Rome", "Paris", "Berlin", "Paris"]):
            self.assertTrue(self.d.evaluate(tx(i * 65, city)).approved)

    def test_city_hopping_within_two_minutes(self):
        self.assertTrue(self.d.evaluate(tx(0, "Paris")).approved)
        self.assertTrue(self.d.evaluate(tx(40, "Rome")).approved)
        decision = self.d.evaluate(tx(80, "Berlin"))
        self.assertEqual(decision.reason, REASON_FRAUD)
        self.assertEqual(rules(decision), {RULE_CITY_HOPPING})
        self.assertTrue(self.d.is_blocked(CARD))

    def test_city_hopping_outside_window_is_ok(self):
        self.d.evaluate(tx(0, "Paris"))
        self.d.evaluate(tx(60, "Rome"))
        self.assertTrue(self.d.evaluate(tx(121, "Berlin")).approved)

    def test_rapid_fire_same_city(self):
        self.d.evaluate(tx(0))
        self.d.evaluate(tx(10))
        decision = self.d.evaluate(tx(25))
        self.assertEqual(rules(decision), {RULE_RAPID_FIRE})

    def test_two_quick_transactions_are_fine(self):
        self.assertTrue(self.d.evaluate(tx(0)).approved)
        self.assertTrue(self.d.evaluate(tx(5)).approved)
        self.assertTrue(self.d.evaluate(tx(36)).approved)  # first one left the 30 s window

    def test_rapid_fire_in_different_cities_triggers_both(self):
        self.d.evaluate(tx(0, "Paris"))
        self.d.evaluate(tx(5, "Rome"))
        self.assertEqual(rules(self.d.evaluate(tx(10, "Oslo"))),
                         {RULE_RAPID_FIRE, RULE_CITY_HOPPING})

    def _history(self, n=5, amount=40.0, city="Paris"):
        for i in range(n):
            self.assertTrue(self.d.evaluate(tx(i * 100, city, amount)).approved)
        return n * 100

    def test_large_amount_in_new_city(self):
        t = self._history()
        decision = self.d.evaluate(tx(t, "Tokyo", 400.0))  # exactly 10x
        self.assertEqual(rules(decision), {RULE_LARGE_NEW_CITY})

    def test_large_amount_in_known_city_is_ok(self):
        t = self._history()
        self.assertTrue(self.d.evaluate(tx(t, "Paris", 1000.0)).approved)

    def test_normal_amount_in_new_city_is_ok(self):
        t = self._history()
        self.assertTrue(self.d.evaluate(tx(t, "Tokyo", 60.0)).approved)

    def test_large_rule_needs_history(self):
        t = self._history(n=4)
        self.assertTrue(self.d.evaluate(tx(t, "Tokyo", 5000.0)).approved)

    def test_blocked_card_declines_everything_but_new_card_works(self):
        self.d.evaluate(tx(0))
        self.d.evaluate(tx(1))
        self.d.evaluate(tx(2))
        self.assertEqual(self.d.evaluate(tx(500)).reason, REASON_CARD_BLOCKED)
        self.assertTrue(self.d.evaluate(tx(501, card="4111111111111111")).approved)

    def test_users_are_independent(self):
        self.d.evaluate(tx(0, name="Alice"))
        self.d.evaluate(tx(1, name="Bob", card="4111111111111111"))
        self.assertTrue(self.d.evaluate(tx(2, name="Carol", card="4222222222222222")).approved)


if __name__ == "__main__":
    unittest.main()
