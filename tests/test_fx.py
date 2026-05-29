from decimal import Decimal

import pytest

from pipeline import fx


def test_gbp_is_identity():
    assert fx.to_gbp(Decimal("100.00"), "GBP") == Decimal("100.00")


def test_usd_converts_and_rounds_to_2dp():
    # 1 GBP = 1.27 USD  ->  12.70 USD = 10.00 GBP
    assert fx.to_gbp(Decimal("12.70"), "USD") == Decimal("10.00")


def test_currency_is_case_insensitive():
    assert fx.to_gbp(Decimal("12.70"), "usd") == fx.to_gbp(Decimal("12.70"), "USD")


def test_rounding_is_half_up():
    # 1 INR worth in GBP rounds to 2dp half-up
    result = fx.to_gbp(Decimal("1.00"), "INR")
    assert result == Decimal("0.01")


def test_unknown_currency_raises():
    with pytest.raises(fx.UnknownCurrencyError):
        fx.to_gbp(Decimal("10.00"), "ZZZ")


def test_supported_currencies_sorted_and_contains_base():
    cur = fx.supported_currencies()
    assert cur == sorted(cur)
    assert fx.BASE_CURRENCY in cur
