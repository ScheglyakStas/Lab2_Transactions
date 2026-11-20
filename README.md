# Bank transaction & fraud-detection simulation (Kafka KRaft)

A Python simulation of card transactions flowing through a bank. It runs on
Apache Kafka in **KRaft mode** (no ZooKeeper), uses **Kafka UI** to show what is
happening, and starts with **Docker Compose**.

```
                         ┌──────────────────────┐   transactions.approved   ┌────────────────┐
┌───────────┐ transactions│                      │──────────────────────────▶│ bank-processor │──▶ transactions.completed
│ generator │────────────▶│    fraud-detector    │   transactions.declined   └────────────────┘
└───────────┘             │  (3 rules, in-memory │──▶ fraud.alerts                    ▲
      ▲                   │   per-user state)    │──▶ cards.blocked ───────────────────┤
      │                   └──────────────────────┘          │                         │
      └──────── reissues a new card after a delay ◀─────────┘──▶ cards.reissued ──────┘
```

## Quick start

```bash
docker compose up -d --build
```

* Kafka UI: <http://localhost:8080>. Open *Topics → fraud.alerts / cards.blocked / transactions.declined → Messages*.
* Live transaction feed: `docker compose logs -f fraud-detector`.
* Stop: `docker compose down`.

The fraud detector prints exactly **one line per transaction**: the transaction
and the bank's verdict. Fraud lines also give the rule that fired:

```
09:59:12 APPROVED | 71ed3b99 | Alexander Nguyen       | **** 0849 | Minsk         |     21.40 USD
09:59:13 FRAUD    | a090ffe4 | Christopher Johnson    | **** 4715 | Barcelona     |   1243.88 USD | LARGE_AMOUNT_NEW_CITY: 37.0x avg 33.60 in new city (known: New York, Toronto) -> card blocked
09:59:20 DECLINED | 5eaa135b | Christopher Johnson    | **** 4715 | Barcelona     |     88.10 USD | card is blocked
```

The generator and the bank processor print nothing per transaction. To see
their internal events (planned fraud scenarios, card reissues, settlements),
set `LOG_LEVEL=DEBUG` for that service.

## Services

| Service | What it does |
|---|---|
| `kafka` | Single-node `apache/kafka:3.9.0` in KRaft mode (broker + controller). Inside Docker it listens on `kafka:9092`; from the host, use `localhost:29092`. |
| `kafka-init` | Creates the 7 topics (3 partitions each), then exits. |
| `kafka-ui` | `provectuslabs/kafka-ui` on port 8080. |
| `generator` | Creates 150 cardholders and sends transactions at random times (exponential gaps). Mixes normal traffic with fraud scenarios. Reissues cards after they are blocked. |
| `fraud-detector` | Checks each transaction against the fraud rules. Approves it, or declines it and blocks the card. |
| `bank-processor` | Settles approved transactions and publishes them as `COMPLETED`. Logs card blocks and reissues. |

## Transaction format (`transactions` topic, key = cardholder name)

```json
{
  "transaction_id": "5f0b1c9e-…",       // primary key (UUID)
  "card_number": "4539…",               // Luhn-valid, used for blocking
  "cardholder_name": "Julia Espinoza",
  "timestamp": "2026-11-20T09:21:39.120+00:00",
  "city": "Warsaw",
  "amount": 17.87,
  "currency": "USD"
}
```

The key is the cardholder name, so all of one person's transactions go to the
same partition and are processed in order. The rules need that ordering.

## Fraud rules ([bank_sim/fraud_rules.py](bank_sim/fraud_rules.py))

| Rule | Fires when |
|---|---|
| `CITY_HOPPING` | The same person makes **3+ transactions within 2 minutes**, and they are **not all in the same city**. To require 3 *distinct* cities, set `RULE_CITY_HOP_MIN_CITIES=3`. |
| `RAPID_FIRE` | The same person makes **3+ transactions within 30 seconds**. |
| `LARGE_AMOUNT_NEW_CITY` | The amount is **≥ 10× the person's average** **and** the city is one where the person has never transacted. This rule only applies after at least 5 transactions of history. |

When any rule fires, the transaction is declined, a `fraud.alerts` record is
published, and the card is blocked (`cards.blocked`). From then on, every
transaction on that card is declined with reason `CARD_BLOCKED`. Only approved
transactions count towards the sliding windows, the average, and the set of
known cities.

## Generated traffic

| Label | Type | Pattern |
|---|---|---|
| `normal` | legit | Mostly the home city, sometimes 1–2 other regular cities. One user's transactions are always at least 65 s apart, so legit traffic alone can never put 3 transactions in 2 minutes. |
| `travel` | legit (edge case) | A normal amount in a new city. This must **not** be flagged. |
| `legit_large` | legit (edge case) | A purchase of 10–20× the average in the home city. This must **not** be flagged. |
| `city_hopping` | fraud | 3–4 transactions in different cities, 32–45 s apart. |
| `rapid_fire` | fraud | 3–5 transactions in one city, 2–8 s apart. |
| `large_new_city` | fraud | One transaction of 15–40× the average in a city the person has never visited. |

The label travels in the Kafka headers `sim-scenario` / `sim-scenario-id`, so you
can check it in Kafka UI. The detector never uses it to make decisions.

## Configuration

Set these as environment variables in [docker-compose.yml](docker-compose.yml):

* Generator: `GEN_USERS`, `GEN_MEAN_INTERVAL_S`, `GEN_FRAUD_PROBABILITY`,
  `GEN_MIN_GAP_S`, `GEN_REISSUE_DELAY_S`, `GEN_SEED`
* Detector: `RULE_CITY_HOP_WINDOW_S`, `RULE_CITY_HOP_MIN_TX`, `RULE_CITY_HOP_MIN_CITIES`,
  `RULE_RAPID_WINDOW_S`, `RULE_RAPID_MIN_TX`, `RULE_LARGE_MULTIPLIER`, `RULE_LARGE_MIN_HISTORY`

## Tests

The rule logic does not depend on Kafka:

```bash
python3 -m unittest discover -s tests -v
# or inside the image:
docker compose run --rm --no-deps fraud-detector python -m unittest discover -s tests -v
```

## Limitations

* The detector keeps its state in memory and runs as a single instance. After a
  restart it continues from its committed offset, but with empty history.
* The rules use event time (`timestamp`). The generator sends transactions in
  real time, so event time and processing time are almost the same.
