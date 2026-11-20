"""Transaction generator.

Emits transactions at random (Poisson-process) moments into the `transactions`
topic. Most traffic is normal; with probability GEN_FRAUD_PROBABILITY an arrival
instead starts one of three fraud scenarios:

* city_hopping    - 3-4 transactions in different cities, 32-45 s apart
                    (so only the 2-minute / different-cities rule can catch it)
* rapid_fire      - 3-5 transactions in the same city, 2-8 s apart
* large_new_city  - one transaction 15-40x the user's average in a city the user
                    has never visited

Normal traffic also contains legitimate edge cases that must NOT be flagged:

* travel          - normal amount in a brand-new city
* legit_large     - a 10-20x purchase in the user's home city

Normal transactions of one user are kept at least GEN_MIN_GAP_S apart (default
65 s), so normal activity alone can never put 3 transactions in a 2-minute window.

The generator also listens to `cards.blocked` and, after GEN_REISSUE_DELAY_S,
issues the cardholder a new card (published to `cards.reissued`). Until then the
user keeps trying the old card, which the bank must keep declining.
"""

from __future__ import annotations

import heapq
import itertools
import os
import random
import time
import uuid
from dataclasses import dataclass, field

from faker import Faker

from .kafka_utils import (HDR_SCENARIO, HDR_SCENARIO_ID, Shutdown, Topics, decode_json,
                          make_consumer, make_producer, produce_json, setup_logging)
from .models import CITIES, Transaction, mask_card, utc_now

log = setup_logging("generator")

N_USERS = int(os.getenv("GEN_USERS", "150"))
MEAN_INTERVAL_S = float(os.getenv("GEN_MEAN_INTERVAL_S", "1.0"))
FRAUD_PROBABILITY = float(os.getenv("GEN_FRAUD_PROBABILITY", "0.05"))
MIN_GAP_S = float(os.getenv("GEN_MIN_GAP_S", "65"))
REISSUE_DELAY_S = float(os.getenv("GEN_REISSUE_DELAY_S", "20"))
LARGE_MIN_HISTORY = int(os.getenv("RULE_LARGE_MIN_HISTORY", "5"))
SEED = os.getenv("GEN_SEED")

FRAUD_SCENARIOS = ("city_hopping", "rapid_fire", "large_new_city")


def luhn_card_number(rng: random.Random, prefix: str = "4") -> str:
    digits = [int(c) for c in prefix] + [rng.randint(0, 9) for _ in range(15 - len(prefix))]
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return "".join(map(str, digits)) + str((10 - total % 10) % 10)


@dataclass
class User:
    name: str
    home_city: str
    frequent_cities: list[str]
    typical_amount: float
    card_number: str
    visited_cities: set[str] = field(default_factory=set)
    legit_count: int = 0
    legit_total: float = 0.0
    last_tx_at: float = float("-inf")
    last_city: str | None = None
    busy_until: float = 0.0
    reissue_at: float | None = None

    @property
    def average(self) -> float:
        return self.legit_total / self.legit_count if self.legit_count else self.typical_amount

    def available(self, now: float) -> bool:
        return now >= self.busy_until and now - self.last_tx_at >= MIN_GAP_S

    def normal_amount(self, rng: random.Random) -> float:
        return round(self.typical_amount * rng.uniform(0.3, 2.0), 2)


@dataclass(frozen=True)
class PlannedTx:
    user: User
    city: str
    amount: float
    scenario: str
    scenario_id: str
    # Fraud scenarios stay on the compromised card even if the real owner gets a new one.
    card_number: str | None = None


class TransactionGenerator:
    def __init__(self) -> None:
        self.rng = random.Random(SEED)
        self.faker = Faker()
        if SEED is not None:
            self.faker.seed_instance(int(SEED))
        self.users = self._create_users()
        self.by_name = {u.name: u for u in self.users}
        self.schedule: list[tuple[float, int, PlannedTx]] = []
        self._seq = itertools.count()
        self.producer = make_producer("transaction-generator")
        self.consumer = make_consumer("generator-card-service", [Topics.CARDS_BLOCKED],
                                      offset_reset="latest")

    # --------------------------------------------------------------- set-up
    def _create_users(self) -> list[User]:
        users, names = [], set()
        while len(users) < N_USERS:
            name = self.faker.name()
            if name in names:
                continue
            names.add(name)
            home = self.rng.choice(CITIES)
            others = self.rng.sample([c for c in CITIES if c != home], k=self.rng.randint(1, 2))
            users.append(User(
                name=name,
                home_city=home,
                frequent_cities=others,
                typical_amount=round(self.rng.lognormvariate(3.8, 0.6), 2),  # median ~45
                card_number=luhn_card_number(self.rng),
            ))
        return users

    # ------------------------------------------------------------ main loop
    def run(self) -> None:
        shutdown = Shutdown()
        next_arrival = time.time()
        log.info("Generator started: %d cardholders, mean interval %.2fs, fraud probability %.0f%%",
                 len(self.users), MEAN_INTERVAL_S, FRAUD_PROBABILITY * 100)
        try:
            while not shutdown.requested:
                now = time.time()
                while self.schedule and self.schedule[0][0] <= now:
                    _, _, planned = heapq.heappop(self.schedule)
                    self._emit(planned)
                if now >= next_arrival:
                    self._on_arrival(now)
                    next_arrival = now + self.rng.expovariate(1.0 / MEAN_INTERVAL_S)
                self._process_reissues(now)

                wake = min(next_arrival, self.schedule[0][0] if self.schedule else next_arrival)
                msg = self.consumer.poll(max(0.0, min(wake - time.time(), 0.5)))
                if msg is not None and not msg.error():
                    self._on_card_blocked(decode_json(msg))
                self.producer.poll(0)
        finally:
            log.info("Generator stopped")
            self.producer.flush(10)
            self.consumer.close()

    # ----------------------------------------------------------- arrivals
    def _on_arrival(self, now: float) -> None:
        if self.rng.random() < FRAUD_PROBABILITY and self._start_fraud(now):
            return
        self._normal(now)

    def _pick_available(self, now: float, predicate=lambda u: True) -> User | None:
        candidates = [u for u in self.users if u.available(now) and predicate(u)]
        return self.rng.choice(candidates) if candidates else None

    def _normal(self, now: float) -> None:
        user = self._pick_available(now)
        if user is None:
            return
        roll = self.rng.random()
        scenario, amount = "normal", user.normal_amount(self.rng)
        if roll < 0.04:
            new_cities = [c for c in CITIES if c not in user.visited_cities]
            if new_cities:
                scenario, city = "travel", self.rng.choice(new_cities)
            else:
                city = user.home_city
        elif roll < 0.06 and user.legit_count >= LARGE_MIN_HISTORY:
            scenario, city = "legit_large", user.home_city
            amount = round(user.average * self.rng.uniform(10, 20), 2)
        elif roll < 0.25:
            city = self.rng.choice(user.frequent_cities)
        else:
            city = user.home_city
        self._emit(PlannedTx(user, city, amount, scenario, uuid.uuid4().hex[:8]))

    def _start_fraud(self, now: float) -> bool:
        for scenario in self.rng.sample(FRAUD_SCENARIOS, k=len(FRAUD_SCENARIOS)):
            plan = getattr(self, f"_plan_{scenario}")(now)
            if plan:
                sid = uuid.uuid4().hex[:8]
                user = plan[0][1].user
                log.debug("FRAUD SCENARIO %-14s id=%s for %s (%s), %d tx planned",
                          scenario, sid, user.name, mask_card(user.card_number), len(plan))
                for due, p in plan:
                    planned = PlannedTx(p.user, p.city, p.amount, scenario, sid,
                                        card_number=user.card_number)
                    heapq.heappush(self.schedule, (due, next(self._seq), planned))
                user.busy_until = plan[-1][0] + MIN_GAP_S
                return True
        return False

    def _plan_city_hopping(self, now: float) -> list[tuple[float, PlannedTx]] | None:
        # Quiet for 2+ minutes first, so the scenario is judged on its own transactions.
        user = self._pick_available(now, lambda u: now - u.last_tx_at >= 125)
        if user is None:
            return None
        cities = self.rng.sample(CITIES, k=self.rng.randint(3, 4))
        plan, t = [], now
        for city in cities:
            plan.append((t, PlannedTx(user, city, user.normal_amount(self.rng), "", "")))
            t += self.rng.uniform(32, 45)
        return plan

    def _plan_rapid_fire(self, now: float) -> list[tuple[float, PlannedTx]] | None:
        user = self._pick_available(now)
        if user is None:
            return None
        city = user.last_city or user.home_city
        plan, t = [], now
        for _ in range(self.rng.randint(3, 5)):
            plan.append((t, PlannedTx(user, city, user.normal_amount(self.rng), "", "")))
            t += self.rng.uniform(2, 8)
        return plan

    def _plan_large_new_city(self, now: float) -> list[tuple[float, PlannedTx]] | None:
        user = self._pick_available(
            now, lambda u: u.legit_count >= LARGE_MIN_HISTORY + 1
            and len(u.visited_cities) < len(CITIES))
        if user is None:
            return None
        city = self.rng.choice([c for c in CITIES if c not in user.visited_cities])
        amount = round(user.average * self.rng.uniform(15, 40), 2)
        return [(now, PlannedTx(user, city, amount, "", ""))]

    # ------------------------------------------------------------- emitting
    def _emit(self, p: PlannedTx) -> None:
        user = p.user
        tx = Transaction(
            transaction_id=str(uuid.uuid4()),
            card_number=p.card_number or user.card_number,
            cardholder_name=user.name,
            timestamp=utc_now(),
            city=p.city,
            amount=p.amount,
        )
        produce_json(self.producer, Topics.TRANSACTIONS, key=user.name, value=tx.to_dict(),
                     headers={HDR_SCENARIO: p.scenario, HDR_SCENARIO_ID: p.scenario_id})

        user.last_tx_at = time.time()
        user.last_city = p.city
        user.visited_cities.add(p.city)
        if p.scenario not in FRAUD_SCENARIOS:
            user.legit_count += 1
            user.legit_total += p.amount
        # Per-transaction output belongs to the fraud detector; this is debug-only.
        log.debug("TX %s | %-22s | %s | %-14s | %9.2f %s | %s",
                  tx.transaction_id[:8], user.name, mask_card(tx.card_number), p.city,
                  p.amount, tx.currency, p.scenario)

    # ------------------------------------------------------- card reissue
    def _on_card_blocked(self, event: dict) -> None:
        user = self.by_name.get(event.get("cardholder_name", ""))
        if user is None or user.card_number != event.get("card_number") or user.reissue_at:
            return
        user.reissue_at = time.time() + REISSUE_DELAY_S
        log.debug("Card %s of %s was blocked; replacement in %.0fs",
                  mask_card(user.card_number), user.name, REISSUE_DELAY_S)

    def _process_reissues(self, now: float) -> None:
        for user in self.users:
            if user.reissue_at is not None and now >= user.reissue_at:
                old = user.card_number
                user.card_number = luhn_card_number(self.rng)
                user.reissue_at = None
                produce_json(self.producer, Topics.CARDS_REISSUED, key=user.name, value={
                    "cardholder_name": user.name,
                    "old_card_number": old,
                    "new_card_number": user.card_number,
                    "reissued_at": utc_now().isoformat(timespec="milliseconds"),
                })
                log.debug("Card reissued for %s: %s -> %s", user.name,
                         mask_card(old), mask_card(user.card_number))


def main() -> None:
    TransactionGenerator().run()


if __name__ == "__main__":
    main()
