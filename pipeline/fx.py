"""FX conversion to the reporting base currency (GBP).

In production these rates would come from a rates service or a slowly-changing
dimension refreshed on a schedule. For a self-contained pipeline we use a static
table; the conversion logic and its tests are identical either way.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

BASE_CURRENCY = "GBP"

# Units of `currency` per 1 GBP. Example: 1 GBP = 1.27 USD.
RATES_PER_GBP: dict[str, Decimal] = {
    "GBP": Decimal("1.00"),
    "USD": Decimal("1.27"),
    "EUR": Decimal("1.17"),
    "INR": Decimal("106.50"),
    "PLN": Decimal("5.05"),
    "AED": Decimal("4.66"),
    "SGD": Decimal("1.71"),
    "JPY": Decimal("198.40"),
}


class UnknownCurrencyError(ValueError):
    """Raised when a currency has no configured FX rate."""


def to_gbp(amount: Decimal, currency: str) -> Decimal:
    """Convert `amount` in `currency` to GBP, rounded to 2 dp.

    >>> to_gbp(Decimal("12.70"), "USD")
    Decimal('10.00')
    """
    currency = currency.upper()
    rate = RATES_PER_GBP.get(currency)
    if rate is None:
        raise UnknownCurrencyError(f"no FX rate configured for {currency!r}")
    converted = Decimal(amount) / rate
    return converted.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def supported_currencies() -> list[str]:
    return sorted(RATES_PER_GBP)
