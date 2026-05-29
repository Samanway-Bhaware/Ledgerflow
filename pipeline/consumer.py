"""Stream consumer: Kafka -> validate -> enrich -> raw.transactions.

Reads events off the topic, validates them against the Transaction model,
applies enrichment (FX + fraud flag), and lands them in Postgres in small
batches. Bad messages are logged and skipped rather than crashing the stream.

    python -m pipeline.consumer
"""

from __future__ import annotations

import signal
import time

from confluent_kafka import Consumer, KafkaError

from pipeline import db
from pipeline.config import settings
from pipeline.models import Transaction
from pipeline.transforms import enrich

BATCH_SIZE = 50
FLUSH_INTERVAL_S = 2.0

_running = True


def _stop(*_args) -> None:
    global _running
    _running = False


def _build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.kafka_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = _build_consumer()
    consumer.subscribe([settings.kafka_topic])
    conn = db.connect()

    batch = []
    last_flush = time.monotonic()
    total = 0
    rejected = 0
    print(f"[consumer] subscribed to {settings.kafka_topic}")

    try:
        while _running:
            msg = consumer.poll(0.5)
            if msg is not None and not msg.error():
                try:
                    txn = Transaction.model_validate_json(msg.value())
                    batch.append(enrich(txn))
                except Exception as exc:  # noqa: BLE001 - log and skip poison messages
                    rejected += 1
                    print(f"[consumer] rejected message: {exc}")
            elif msg is not None and msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[consumer] kafka error: {msg.error()}")

            due = (time.monotonic() - last_flush) >= FLUSH_INTERVAL_S
            if len(batch) >= BATCH_SIZE or (batch and due):
                written = db.insert_batch(conn, batch)
                consumer.commit(asynchronous=False)
                total += written
                print(f"[consumer] committed {written} (total {total}, rejected {rejected})")
                batch.clear()
                last_flush = time.monotonic()
    finally:
        if batch:
            db.insert_batch(conn, batch)
            consumer.commit(asynchronous=False)
        conn.close()
        consumer.close()
        print(f"[consumer] stopped. total={total} rejected={rejected}")


if __name__ == "__main__":
    main()
