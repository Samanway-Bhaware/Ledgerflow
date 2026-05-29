"""Synthetic transaction event producer.

Generates plausible card-transaction events and publishes them as JSON to the
Kafka topic. Stands in for a real card-authorisation feed. Run it as a module:

    python -m pipeline.producer
"""

from __future__ import annotations

import random
import signal
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from confluent_kafka import Producer

from pipeline.config import settings
from pipeline.fx import supported_currencies
from pipeline.models import Transaction, TxnStatus

MERCHANT_CATEGORIES = [
    "Groceries", "Restaurants", "Transport", "Travel", "Entertainment",
    "Utilities", "Shopping", "Health", "Cash", "Transfers",
]

COUNTRY_BY_CURRENCY = {
    "GBP": "GB", "USD": "US", "EUR": "DE", "INR": "IN",
    "PLN": "PL", "AED": "AE", "SGD": "SG", "JPY": "JP",
}

_running = True


def _stop(*_args) -> None:
    global _running
    _running = False


def _random_amount() -> Decimal:
    # Log-normal-ish spread: lots of small spends, a few large ones.
    base = random.lognormvariate(2.6, 1.1)
    return Decimal(f"{base:.2f}")


def make_transaction() -> Transaction:
    currency = random.choice(supported_currencies())
    category = random.choice(MERCHANT_CATEGORIES)
    status = random.choices(
        list(TxnStatus), weights=[0.88, 0.09, 0.03], k=1
    )[0]
    return Transaction(
        transaction_id=str(uuid.uuid4()),
        user_id=random.randint(1, settings.n_users),
        card_id=f"card_{random.randint(1, settings.n_users * 2)}",
        merchant_id=random.randint(1, settings.n_merchants),
        merchant_name=f"{category} Merchant {random.randint(1, settings.n_merchants)}",
        merchant_category=category,
        amount=_random_amount(),
        currency=currency,
        country=COUNTRY_BY_CURRENCY[currency],
        status=status,
        event_time=datetime.now(timezone.utc),
    )


def _serialise(txn: Transaction) -> bytes:
    return txn.model_dump_json().encode("utf-8")


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    interval = 1.0 / settings.events_per_second if settings.events_per_second else 0
    sent = 0
    print(f"[producer] -> {settings.kafka_topic} @ {settings.events_per_second}/s")

    while _running:
        txn = make_transaction()
        producer.produce(
            settings.kafka_topic,
            key=str(txn.user_id),
            value=_serialise(txn),
        )
        producer.poll(0)
        sent += 1
        if sent % 100 == 0:
            print(f"[producer] sent {sent} events")
        if interval:
            time.sleep(interval)

    producer.flush(10)
    print(f"[producer] stopped after {sent} events")


if __name__ == "__main__":
    main()
