"""Domain models for the transaction pipeline.

These models are the contract between the producer, the Kafka topic, and the
stream consumer. Validation lives here so that a malformed event is rejected at
the edge of the system rather than corrupting the warehouse downstream.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class TxnStatus(str, Enum):
    AUTHORISED = "AUTHORISED"
    DECLINED = "DECLINED"
    REVERSED = "REVERSED"


class Transaction(BaseModel):
    """A raw card transaction event as it arrives on the Kafka topic.

    This is the producer-side contract. Anything that does not satisfy these
    constraints never makes it onto the topic.
    """

    transaction_id: str = Field(..., min_length=8)
    user_id: int = Field(..., gt=0)
    card_id: str = Field(..., min_length=4)
    merchant_id: int = Field(..., gt=0)
    merchant_name: str = Field(..., min_length=1)
    merchant_category: str = Field(..., min_length=1)
    amount: Decimal = Field(..., gt=0)
    currency: str = Field(..., min_length=3, max_length=3)
    country: str = Field(..., min_length=2, max_length=2)
    status: TxnStatus
    event_time: datetime

    @field_validator("currency")
    @classmethod
    def currency_upper(cls, v: str) -> str:
        if not v.isalpha():
            raise ValueError("currency must be alphabetic ISO-4217 code")
        return v.upper()

    @field_validator("country")
    @classmethod
    def country_upper(cls, v: str) -> str:
        if not v.isalpha():
            raise ValueError("country must be an ISO-3166 alpha-2 code")
        return v.upper()

    @field_validator("event_time")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)


class EnrichedTransaction(BaseModel):
    """A transaction after stream-side enrichment, ready to land in Postgres.

    Adds the base-currency amount, a fraud flag, and the ingestion timestamp.
    """

    transaction_id: str
    user_id: int
    card_id: str
    merchant_id: int
    merchant_name: str
    merchant_category: str
    amount: Decimal
    currency: str
    amount_gbp: Decimal
    country: str
    status: TxnStatus
    is_flagged: bool
    event_time: datetime
    ingested_at: datetime
