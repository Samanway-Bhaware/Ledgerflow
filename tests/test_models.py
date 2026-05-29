from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from pipeline.models import Transaction, TxnStatus


def _valid_payload(**overrides):
    payload = dict(
        transaction_id="txn-00000001",
        user_id=42,
        card_id="card_99",
        merchant_id=7,
        merchant_name="Groceries Merchant 7",
        merchant_category="Groceries",
        amount=Decimal("23.45"),
        currency="gbp",
        country="gb",
        status="AUTHORISED",
        event_time="2026-05-01T10:00:00+00:00",
    )
    payload.update(overrides)
    return payload


def test_valid_transaction_normalises_currency_and_country():
    txn = Transaction(**_valid_payload())
    assert txn.currency == "GBP"
    assert txn.country == "GB"
    assert txn.status is TxnStatus.AUTHORISED


def test_naive_datetime_is_coerced_to_utc():
    txn = Transaction(**_valid_payload(event_time=datetime(2026, 5, 1, 10, 0)))
    assert txn.event_time.tzinfo == timezone.utc


def test_negative_amount_rejected():
    with pytest.raises(ValidationError):
        Transaction(**_valid_payload(amount=Decimal("-1.00")))


def test_zero_amount_rejected():
    with pytest.raises(ValidationError):
        Transaction(**_valid_payload(amount=Decimal("0")))


def test_bad_currency_length_rejected():
    with pytest.raises(ValidationError):
        Transaction(**_valid_payload(currency="POUND"))


def test_unknown_status_rejected():
    with pytest.raises(ValidationError):
        Transaction(**_valid_payload(status="PENDING"))


def test_roundtrip_json():
    txn = Transaction(**_valid_payload())
    restored = Transaction.model_validate_json(txn.model_dump_json())
    assert restored == txn
