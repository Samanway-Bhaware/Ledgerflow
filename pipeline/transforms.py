"""Pure transformation logic for the stream consumer.

Everything here is a pure function of its inputs: no Kafka, no Postgres, no
clock reads passed implicitly. That is deliberate — it makes the business rules
trivially unit-testable, which is the whole point of the TDD-first approach.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from pipeline import fx
from pipeline.models import EnrichedTransaction, Transaction, TxnStatus

# A transaction at or above this GBP value is flagged for review. Kept as a
# module constant so the rule is visible and testable rather than buried.
HIGH_VALUE_GBP_THRESHOLD = Decimal("5000.00")


def flag_suspicious(amount_gbp: Decimal, status: TxnStatus, currency: str) -> bool:
    """Lightweight rule-based fraud flag.

    Flags a transaction when any of the following hold:
      * the GBP value is at or above the high-value threshold, or
      * it is an authorised transaction in an unsupported currency
        (should never happen for a known good event, so treat as suspect).

    Real fraud detection would be a model; this demonstrates the rule slot in
    the pipeline and gives the warehouse a populated `is_flagged` column.
    """
    if amount_gbp >= HIGH_VALUE_GBP_THRESHOLD:
        return True
    if status is TxnStatus.AUTHORISED and currency not in fx.RATES_PER_GBP:
        return True
    return False


def enrich(txn: Transaction, now: datetime | None = None) -> EnrichedTransaction:
    """Convert a raw transaction into a warehouse-ready enriched record."""
    ingested_at = now or datetime.now(timezone.utc)
    amount_gbp = fx.to_gbp(txn.amount, txn.currency)
    return EnrichedTransaction(
        transaction_id=txn.transaction_id,
        user_id=txn.user_id,
        card_id=txn.card_id,
        merchant_id=txn.merchant_id,
        merchant_name=txn.merchant_name,
        merchant_category=txn.merchant_category,
        amount=txn.amount,
        currency=txn.currency,
        amount_gbp=amount_gbp,
        country=txn.country,
        status=txn.status,
        is_flagged=flag_suspicious(amount_gbp, txn.status, txn.currency),
        event_time=txn.event_time,
        ingested_at=ingested_at,
    )


def daily_spend_by_category(
    txns: list[EnrichedTransaction],
) -> dict[tuple[str, str], Decimal]:
    """Reference implementation of the warehouse daily aggregate.

    Returns a mapping of (date, merchant_category) -> total GBP spend, counting
    only authorised transactions. The Airflow DAG computes the same thing in
    SQL; this Python version exists so the aggregation logic can be unit-tested
    without standing up Postgres.
    """
    totals: dict[tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0.00"))
    for txn in txns:
        if txn.status is not TxnStatus.AUTHORISED:
            continue
        day = txn.event_time.astimezone(timezone.utc).date().isoformat()
        totals[(day, txn.merchant_category)] += txn.amount_gbp
    return dict(totals)
