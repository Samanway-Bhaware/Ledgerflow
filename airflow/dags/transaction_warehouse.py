"""Warehouse ETL DAG: raw.transactions -> analytics star schema.

Flow per run:
    data_quality_checks                (gate: fail the run on bad raw data)
        |--> upsert_dim_user        --\
        |--> upsert_dim_merchant    ----> load_fact_transaction --> build_daily_aggregates
                                                 (incremental by watermark)

Runs hourly. The fact load is watermarked on `ingested_at` so each run only
touches newly-landed rows, and every step is idempotent (safe to re-run /
backfill).
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "warehouse_pg"
SQL_DIR = "sql"


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=PG_CONN_ID)


def _read_sql(name: str) -> str:
    from pathlib import Path

    return (Path(__file__).parent / SQL_DIR / name).read_text()


@dag(
    dag_id="transaction_warehouse",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
    tags=["warehouse", "transactions"],
)
def transaction_warehouse():

    @task
    def data_quality_checks() -> None:
        """Gate the pipeline: raise if the raw landing zone looks wrong."""
        hook = _hook()
        checks = {
            "no_null_keys": (
                "SELECT count(*) FROM raw.transactions "
                "WHERE transaction_id IS NULL OR user_id IS NULL OR merchant_id IS NULL"
            ),
            "no_nonpositive_amounts": (
                "SELECT count(*) FROM raw.transactions WHERE amount <= 0"
            ),
            "valid_status": (
                "SELECT count(*) FROM raw.transactions "
                "WHERE status NOT IN ('AUTHORISED','DECLINED','REVERSED')"
            ),
        }
        failures = []
        for name, sql in checks.items():
            bad = hook.get_first(sql)[0]
            passed = bad == 0
            hook.run(
                "INSERT INTO analytics.dq_check_log (check_name, passed, detail) "
                "VALUES (%s, %s, %s)",
                parameters=(name, passed, f"{bad} offending rows"),
            )
            if not passed:
                failures.append(f"{name}: {bad} rows")
        if failures:
            raise ValueError("data quality checks failed -> " + "; ".join(failures))

    @task
    def upsert_dim_user() -> None:
        _hook().run(_read_sql("upsert_dim_user.sql"))

    @task
    def upsert_dim_merchant() -> None:
        _hook().run(_read_sql("upsert_dim_merchant.sql"))

    @task
    def load_fact_transaction() -> int:
        """Load rows ingested since the last watermark, then advance it."""
        hook = _hook()
        low = hook.get_first(
            "SELECT last_loaded FROM analytics.load_watermark "
            "WHERE table_name = 'fact_transaction'"
        )[0]
        high = hook.get_first("SELECT now()")[0]
        hook.run(
            _read_sql("load_fact_transaction.sql"),
            parameters={"low": low, "high": high},
        )
        loaded = hook.get_first(
            "SELECT count(*) FROM analytics.fact_transaction "
            "WHERE event_time > %s",
            parameters=(low,),
        )[0]
        hook.run(
            "UPDATE analytics.load_watermark SET last_loaded = %s "
            "WHERE table_name = 'fact_transaction'",
            parameters=(high,),
        )
        return int(loaded)

    @task
    def build_daily_aggregates() -> None:
        _hook().run(_read_sql("build_daily_aggregates.sql"))

    dq = data_quality_checks()
    dims = [upsert_dim_user(), upsert_dim_merchant()]
    fact = load_fact_transaction()
    aggregates = build_daily_aggregates()

    dq >> dims >> fact >> aggregates


transaction_warehouse()
