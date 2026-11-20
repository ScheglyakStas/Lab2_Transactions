"""Thin helpers around confluent-kafka shared by all services."""

from __future__ import annotations

import json
import logging
import os
import signal
from typing import Any, Iterable

from confluent_kafka import Consumer, KafkaError, Message, Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


class Topics:
    TRANSACTIONS = "transactions"                  # raw transactions from the generator
    APPROVED = "transactions.approved"             # passed fraud screening
    DECLINED = "transactions.declined"             # fraud or blocked card
    COMPLETED = "transactions.completed"           # settled by the bank processor
    FRAUD_ALERTS = "fraud.alerts"                  # one alert per detected fraud
    CARDS_BLOCKED = "cards.blocked"                # card-block events
    CARDS_REISSUED = "cards.reissued"              # replacement cards issued

    ALL = (TRANSACTIONS, APPROVED, DECLINED, COMPLETED,
           FRAUD_ALERTS, CARDS_BLOCKED, CARDS_REISSUED)


# Headers carrying the generator's ground truth (visible in Kafka UI). The
# detector forwards them but never uses them for decisions.
HDR_SCENARIO = "sim-scenario"
HDR_SCENARIO_ID = "sim-scenario-id"


class _Formatter(logging.Formatter):
    """`HH:MM:SS message`; the level is shown only when it is not INFO."""

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        if record.levelno != logging.INFO:
            time_part, _, rest = line.partition(" ")
            line = f"{time_part} {record.levelname}: {rest}"
        return line


def setup_logging(service: str) -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(_Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), handlers=[handler])
    return logging.getLogger(service)


def make_producer(client_id: str) -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
    })


def make_consumer(group_id: str, topics: Iterable[str], offset_reset: str = "earliest") -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": True,
    })
    consumer.subscribe(list(topics))
    return consumer


_log = logging.getLogger("kafka")


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        _log.error("Delivery to %s failed: %s", msg.topic(), err)


def produce_json(producer: Producer, topic: str, key: str, value: dict[str, Any],
                 headers: dict[str, str] | None = None) -> None:
    producer.produce(
        topic,
        key=key.encode(),
        value=json.dumps(value, ensure_ascii=False).encode(),
        headers=[(k, v.encode()) for k, v in (headers or {}).items()],
        on_delivery=_on_delivery,
    )
    producer.poll(0)


def decode_json(msg: Message) -> dict[str, Any]:
    return json.loads(msg.value().decode())


def decode_headers(msg: Message) -> dict[str, str]:
    return {k: (v.decode() if v is not None else "") for k, v in (msg.headers() or [])}


class Shutdown:
    """Flips `requested` on SIGINT/SIGTERM so main loops can exit cleanly."""

    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, *_: Any) -> None:
        self.requested = True
