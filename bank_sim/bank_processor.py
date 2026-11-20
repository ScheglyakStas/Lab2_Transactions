"""Core-banking processor.

Settles every transaction that passed fraud screening: consumes
`transactions.approved`, debits the cardholder's account and publishes the
settled record to `transactions.completed`. It also follows `cards.blocked` and
`cards.reissued`.

Per-transaction and card events are logged at DEBUG only; the fraud detector
already prints one line per transaction (set LOG_LEVEL=DEBUG to see these).
"""

from __future__ import annotations

from collections import defaultdict

from .kafka_utils import (Shutdown, Topics, decode_headers, decode_json, make_consumer,
                          make_producer, produce_json, setup_logging)
from .models import mask_card, utc_now

log = setup_logging("bank-processor")


def main() -> None:
    producer = make_producer("bank-processor")
    consumer = make_consumer("bank-processor",
                             [Topics.APPROVED, Topics.CARDS_BLOCKED, Topics.CARDS_REISSUED])
    shutdown = Shutdown()
    spent: dict[str, float] = defaultdict(float)
    log.info("Bank processor started")

    try:
        while not shutdown.requested:
            msg = consumer.poll(1.0)
            producer.poll(0)
            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue

            data = decode_json(msg)
            topic = msg.topic()

            if topic == Topics.CARDS_BLOCKED:
                log.debug("Card %s of %s blocked (%s)", mask_card(data["card_number"]),
                          data["cardholder_name"], "+".join(data["reason"]))
                continue
            if topic == Topics.CARDS_REISSUED:
                log.debug("New card %s issued to %s", mask_card(data["new_card_number"]),
                          data["cardholder_name"])
                continue

            name = data["cardholder_name"]
            spent[name] += data["amount"]
            completed = {
                **{k: v for k, v in data.items() if k != "decision"},
                "status": "COMPLETED",
                "settled_at": utc_now().isoformat(timespec="milliseconds"),
                "cardholder_total_spent": round(spent[name], 2),
            }
            produce_json(producer, Topics.COMPLETED, name, completed, decode_headers(msg))
            log.debug("COMPLETED %s | %-22s | %-13s | %9.2f %s", data["transaction_id"][:8],
                      name, data["city"], data["amount"], data["currency"])
    finally:
        log.info("Bank processor stopped")
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()
