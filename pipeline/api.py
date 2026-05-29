"""REST API layer – exposes the analytics warehouse over HTTP.

All queries target analytics.* tables only.  Never touches raw.*.
Response models are defined here to keep pipeline/models.py as the
event contract only.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import date, timedelta
from decimal import Decimal
from typing import Annotated, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from pipeline.config import settings

logger = logging.getLogger(__name__)

_pool: ConnectionPool | None = None


def get_pool() -> ConnectionPool:
    if _pool is None:  # pragma: no cover
        raise RuntimeError("Connection pool not initialised – app not started via lifespan")
    return _pool


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    global _pool
    _pool = ConnectionPool(settings.pg_dsn, min_size=1, max_size=5, open=True)
    logger.info("DB connection pool opened")
    yield
    _pool.close()
    logger.info("DB connection pool closed")


app = FastAPI(title="Transaction Pipeline API", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str


class UserSpendResponse(BaseModel):
    user_id: int
    from_date: date
    to_date: date
    total_gbp: Decimal
    txn_count: int


class DailySpendRow(BaseModel):
    date_key: date
    merchant_category: str
    total_gbp: Decimal
    txn_count: int
    flagged_count: int


class MerchantTopRow(BaseModel):
    merchant_id: int
    merchant_name: str
    merchant_category: str
    total_gbp: Decimal
    txn_count: int


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
def health(pool: Annotated[ConnectionPool, Depends(get_pool)]):
    try:
        with pool.connection() as conn:
            conn.execute("SELECT 1")
    except Exception:
        logger.exception("Health-check DB ping failed")
        raise HTTPException(status_code=503, detail="database unreachable")
    return HealthResponse(status="ok")


@app.get("/users/{user_id}/spend", response_model=UserSpendResponse)
def user_spend(
    user_id: int,
    from_date: Optional[date] = Query(default=None),
    to_date: Optional[date] = Query(default=None),
    pool: ConnectionPool = Depends(get_pool),
):
    today = date.today()
    resolved_to = to_date if to_date is not None else today
    resolved_from = from_date if from_date is not None else today - timedelta(days=30)

    with pool.connection() as conn:
        row = conn.execute(
            "SELECT user_id FROM analytics.dim_user WHERE user_id = %s",
            (user_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="user not found")

        result = conn.execute(
            """
            SELECT
                COALESCE(SUM(amount_gbp), 0) AS total_gbp,
                COUNT(*) AS txn_count
            FROM analytics.fact_transaction
            WHERE user_id = %s
              AND status = 'AUTHORISED'
              AND date_key >= %s
              AND date_key <= %s
            """,
            (user_id, resolved_from, resolved_to),
        ).fetchone()

    total_gbp = result[0] if result else Decimal("0")
    txn_count = int(result[1]) if result else 0
    return UserSpendResponse(
        user_id=user_id,
        from_date=resolved_from,
        to_date=resolved_to,
        total_gbp=total_gbp,
        txn_count=txn_count,
    )


@app.get("/daily-spend", response_model=list[DailySpendRow])
def daily_spend(
    category: Optional[str] = Query(default=None),
    limit: Annotated[int, Query(ge=1, le=365)] = 30,
    pool: ConnectionPool = Depends(get_pool),
):
    if category is not None:
        sql = """
            SELECT date_key, merchant_category, total_gbp, txn_count, flagged_count
            FROM analytics.daily_spend_by_category
            WHERE merchant_category = %s
            ORDER BY date_key DESC
            LIMIT %s
        """
        params = (category, limit)
    else:
        sql = """
            SELECT date_key, merchant_category, total_gbp, txn_count, flagged_count
            FROM analytics.daily_spend_by_category
            ORDER BY date_key DESC
            LIMIT %s
        """
        params = (limit,)

    with pool.connection() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [
        DailySpendRow(
            date_key=r[0],
            merchant_category=r[1],
            total_gbp=r[2],
            txn_count=r[3],
            flagged_count=r[4],
        )
        for r in rows
    ]


@app.get("/merchants/top", response_model=list[MerchantTopRow])
def top_merchants(
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
    pool: ConnectionPool = Depends(get_pool),
):
    sql = """
        SELECT
            f.merchant_id,
            m.merchant_name,
            m.merchant_category,
            SUM(f.amount_gbp)  AS total_gbp,
            COUNT(*)           AS txn_count
        FROM analytics.fact_transaction f
        JOIN analytics.dim_merchant m ON f.merchant_id = m.merchant_id
        WHERE f.status = 'AUTHORISED'
        GROUP BY f.merchant_id, m.merchant_name, m.merchant_category
        ORDER BY total_gbp DESC
        LIMIT %s
    """
    with pool.connection() as conn:
        rows = conn.execute(sql, (limit,)).fetchall()

    return [
        MerchantTopRow(
            merchant_id=r[0],
            merchant_name=r[1],
            merchant_category=r[2],
            total_gbp=r[3],
            txn_count=r[4],
        )
        for r in rows
    ]
