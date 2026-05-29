-- 01_raw_schema.sql
-- Raw landing zone. The stream consumer appends enriched events here verbatim.
-- This layer is intentionally denormalised and append-only: it is the system's
-- source of truth and replay buffer. Modelling happens downstream in `analytics`.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id     TEXT PRIMARY KEY,
    user_id            BIGINT       NOT NULL,
    card_id            TEXT         NOT NULL,
    merchant_id        BIGINT       NOT NULL,
    merchant_name      TEXT         NOT NULL,
    merchant_category  TEXT         NOT NULL,
    amount             NUMERIC(18,2) NOT NULL,
    currency           CHAR(3)      NOT NULL,
    amount_gbp         NUMERIC(18,2) NOT NULL,
    country            CHAR(2)      NOT NULL,
    status             TEXT         NOT NULL,
    is_flagged         BOOLEAN      NOT NULL DEFAULT FALSE,
    event_time         TIMESTAMPTZ  NOT NULL,
    ingested_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- The fact load reads incrementally by event_time, so index it.
CREATE INDEX IF NOT EXISTS idx_raw_txn_event_time
    ON raw.transactions (event_time);

CREATE INDEX IF NOT EXISTS idx_raw_txn_ingested_at
    ON raw.transactions (ingested_at);
