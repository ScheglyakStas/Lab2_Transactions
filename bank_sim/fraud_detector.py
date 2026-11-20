"""Fraud-detection service.

Consumes `transactions`, runs the rules from `fraud_rules`, and routes each
transaction to `transactions.approved` or `transactions.declined`. When fraud is
found it also publishes a `fraud.alerts` record and a `cards.blocked` event.

This is the only service that logs at INFO per transaction: exactly one line
with the transaction and the bank's verdict.

State (sliding windows, profiles, blocked cards) is kept in memory; this is a
single-instance service. Input is keyed by cardholder name, so all transactions
of one person arrive in order on the same partition.
"""

from __future__ import annotations

import uuid

from .fraud_rules import REASON_CARD_BLOCKED, Decision, FraudDetector, RuleConfig
from .kafka_utils import (Shutdown, Topics, decode_headers, decode_json, make_consumer,
                          make_producer, produce_json, setup_logging)
from .models import Transaction, mask_card, utc_now

log = setup_logging("fraud-detector")


def log_line(tx: Transaction, decision: Decision) -> None:
    if decision.approved:
        verdict, note = "APPROVED", ""
    elif decision.reason == REASON_CARD_BLOCKED:
        verdict, note = "DECLINED", " | card is blocked"
    else:
        verdict = "FRAUD   "
        note = " | " + "; ".join(f"{h.rule}: {h.description}" for h in decision.hits)
        note += " -> card blocked"
    log.info("%s | %s | %-22s | %s | %-13s | %9.2f %s%s",
             verdict, tx.transaction_id[:8], tx.cardholder_name, mask_card(tx.card_number),
             tx.city, tx.amount, tx.currency, note)


def main() -> None:
    detector = FraudDetector(RuleConfig.from_env())
    producer = make_producer("fraud-detector")
    consumer = make_consumer("fraud-detector", [Topics.TRANSACTIONS])
    shutdown = Shutdown()
    log.info("Fraud detector started")

    try:
        while not shutdown.requested:
            msg = consumer.poll(1.0)
            producer.poll(0)
            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue

            headers = decode_headers(msg)
            try:
                tx = Transaction.from_dict(decode_json(msg))
            except (ValueError, KeyError, TypeError) as exc:
                log.error("Skipping malformed message at offset %s: %s", msg.offset(), exc)
                continue

            decision = detector.evaluate(tx)
            log_line(tx, decision)
            key = tx.cardholder_name
            record = {
                **tx.to_dict(),
                "decision": decision.to_dict(),
                "processed_at": utc_now().isoformat(timespec="milliseconds"),
            }

            if decision.approved:
                produce_json(producer, Topics.APPROVED, key, record, headers)
                continue

            produce_json(producer, Topics.DECLINED, key, record, headers)
            if not decision.card_blocked_now:
                continue

            detected_at = utc_now().isoformat(timespec="milliseconds")
            produce_json(producer, Topics.FRAUD_ALERTS, key, {
                "alert_id": str(uuid.uuid4()),
                "transaction": tx.to_dict(),
                "rules": decision.to_dict()["rules"],
                "action": "CARD_BLOCKED",
                "detected_at": detected_at,
            }, headers)
            produce_json(producer, Topics.CARDS_BLOCKED, key, {
                "card_number": tx.card_number,
                "cardholder_name": tx.cardholder_name,
                "reason": [h.rule for h in decision.hits],
                "transaction_id": tx.transaction_id,
                "blocked_at": detected_at,
            })
    finally:
        log.info("Fraud detector stopped")
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()
