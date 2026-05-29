-- 02_analytics_schema.sql
-- Dimensional model (star schema) populated by the Airflow warehouse DAG.
--   dim_user, dim_merchant, dim_date  -> conformed dimensions
--   fact_transaction                  -> grain: one row per transaction
--   daily_spend_by_category           -> pre-aggregated reporting mart
--   dq_check_log                      -> data-quality audit trail

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_user (
    user_id     BIGINT PRIMARY KEY,
    first_seen  TIMESTAMPTZ NOT NULL,
    last_seen   TIMESTAMPTZ NOT NULL,
    txn_count   BIGINT      NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS analytics.dim_merchant (
    merchant_id        BIGINT PRIMARY KEY,
    merchant_name      TEXT NOT NULL,
    merchant_category  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key   DATE PRIMARY KEY,
    day        SMALLINT NOT NULL,
    month      SMALLINT NOT NULL,
    year       SMALLINT NOT NULL,
    weekday    SMALLINT NOT NULL,   -- 0 = Monday
    is_weekend BOOLEAN  NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.fact_transaction (
    transaction_id  TEXT PRIMARY KEY,
    date_key        DATE        NOT NULL REFERENCES analytics.dim_date(date_key),
    user_id         BIGINT      NOT NULL REFERENCES analytics.dim_user(user_id),
    merchant_id     BIGINT      NOT NULL REFERENCES analytics.dim_merchant(merchant_id),
    amount_gbp      NUMERIC(18,2) NOT NULL,
    currency        CHAR(3)     NOT NULL,
    status          TEXT        NOT NULL,
    is_flagged      BOOLEAN     NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_txn_date     ON analytics.fact_transaction (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_txn_user     ON analytics.fact_transaction (user_id);
CREATE INDEX IF NOT EXISTS idx_fact_txn_merchant ON analytics.fact_transaction (merchant_id);

CREATE TABLE IF NOT EXISTS analytics.daily_spend_by_category (
    date_key          DATE NOT NULL,
    merchant_category TEXT NOT NULL,
    total_gbp         NUMERIC(18,2) NOT NULL,
    txn_count         BIGINT        NOT NULL,
    flagged_count     BIGINT        NOT NULL,
    PRIMARY KEY (date_key, merchant_category)
);

-- Watermark so the fact load only processes newly-ingested rows each run.
CREATE TABLE IF NOT EXISTS analytics.load_watermark (
    table_name   TEXT PRIMARY KEY,
    last_loaded  TIMESTAMPTZ NOT NULL
);

INSERT INTO analytics.load_watermark (table_name, last_loaded)
VALUES ('fact_transaction', 'epoch')
ON CONFLICT (table_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS analytics.dq_check_log (
    id          BIGSERIAL PRIMARY KEY,
    run_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
    check_name  TEXT    NOT NULL,
    passed      BOOLEAN NOT NULL,
    detail      TEXT
);
