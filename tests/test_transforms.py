from datetime import datetime, timezone
from decimal import Decimal

from pipeline.models import Transaction, TxnStatus
from pipeline.transforms import (
    HIGH_VALUE_GBP_THRESHOLD,
    daily_spend_by_category,
    enrich,
    flag_suspicious,
)


def _txn(**overrides) -> Transaction:
    payload = dict(
        transaction_id="txn-00000001",
        user_id=1,
        card_id="card_1",
        merchant_id=1,
        merchant_name="Groceries Merchant 1",
        merchant_category="Groceries",
        amount=Decimal("100.00"),
        currency="GBP",
        country="GB",
        status="AUTHORISED",
        event_time=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
    )
    payload.update(overrides)
    return Transaction(**payload)


def test_enrich_sets_gbp_amount_and_ingest_time():
    fixed_now = datetime(2026, 5, 1, 12, 0, 5, tzinfo=timezone.utc)
    out = enrich(_txn(amount=Decimal("12.70"), currency="USD"), now=fixed_now)
    assert out.amount_gbp == Decimal("10.00")
    assert out.ingested_at == fixed_now
    assert out.currency == "USD"


def test_low_value_is_not_flagged():
    assert flag_suspicious(Decimal("100.00"), TxnStatus.AUTHORISED, "GBP") is False


def test_high_value_is_flagged_at_threshold():
    assert flag_suspicious(HIGH_VALUE_GBP_THRESHOLD, TxnStatus.AUTHORISED, "GBP") is True


def test_enrich_flags_high_value_transaction():
    out = enrich(_txn(amount=Decimal("9000.00"), currency="GBP"))
    assert out.is_flagged is True


def test_daily_aggregate_sums_authorised_only():
    txns = [
        enrich(_txn(transaction_id="txn-aaaa", amount=Decimal("10.00"))),
        enrich(_txn(transaction_id="txn-bbbb", amount=Decimal("15.00"))),
        enrich(_txn(transaction_id="txn-cccc", amount=Decimal("99.00"), status="DECLINED")),
    ]
    result = daily_spend_by_category(txns)
    assert result[("2026-05-01", "Groceries")] == Decimal("25.00")


def test_daily_aggregate_groups_by_date_and_category():
    txns = [
        enrich(_txn(transaction_id="txn-aaaa", merchant_category="Travel", amount=Decimal("50.00"))),
        enrich(_txn(
            transaction_id="txn-bbbb",
            merchant_category="Travel",
            amount=Decimal("50.00"),
            event_time=datetime(2026, 5, 2, 9, 0, tzinfo=timezone.utc),
        )),
    ]
    result = daily_spend_by_category(txns)
    assert result[("2026-05-01", "Travel")] == Decimal("50.00")
    assert result[("2026-05-02", "Travel")] == Decimal("50.00")
