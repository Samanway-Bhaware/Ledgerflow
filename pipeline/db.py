"""Thin Postgres access layer for the stream consumer.

Uses psycopg (v3). The consumer only ever appends to the raw landing table;
all modelling into dimensions and facts happens later in Airflow. Keeping the
write path append-only makes the stream side idempotent-friendly and simple.
"""

from __future__ import annotations

from collections.abc import Iterable

import psycopg

from pipeline.config import settings
from pipeline.models import EnrichedTransaction

INSERT_SQL = """
    INSERT INTO raw.transactions (
        transaction_id, user_id, card_id, merchant_id, merchant_name,
        merchant_category, amount, currency, amount_gbp, country, status,
        is_flagged, event_time, ingested_at
    ) VALUES (
        %(transaction_id)s, %(user_id)s, %(card_id)s, %(merchant_id)s,
        %(merchant_name)s, %(merchant_category)s, %(amount)s, %(currency)s,
        %(amount_gbp)s, %(country)s, %(status)s, %(is_flagged)s,
        %(event_time)s, %(ingested_at)s
    )
    ON CONFLICT (transaction_id) DO NOTHING
"""


def connect() -> psycopg.Connection:
    return psycopg.connect(settings.pg_dsn, autocommit=False)


def _to_row(txn: EnrichedTransaction) -> dict:
    row = txn.model_dump()
    row["status"] = txn.status.value
    return row


def insert_batch(conn: psycopg.Connection, batch: Iterable[EnrichedTransaction]) -> int:
    rows = [_to_row(t) for t in batch]
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
    conn.commit()
    return len(rows)
