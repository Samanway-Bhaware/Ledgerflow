-- load_fact_transaction.sql
-- Incremental load: only rows ingested in (%(low)s, %(high)s] are processed,
-- so each DAG run does bounded work. Idempotent via ON CONFLICT — a re-run over
-- the same window is a no-op. Rows are joined to conformed dimensions.

INSERT INTO analytics.fact_transaction (
    transaction_id, date_key, user_id, merchant_id,
    amount_gbp, currency, status, is_flagged, event_time
)
SELECT
    r.transaction_id,
    (r.event_time AT TIME ZONE 'UTC')::date AS date_key,
    r.user_id,
    r.merchant_id,
    r.amount_gbp,
    r.currency,
    r.status,
    r.is_flagged,
    r.event_time
FROM raw.transactions r
WHERE r.ingested_at > %(low)s
  AND r.ingested_at <= %(high)s
ON CONFLICT (transaction_id) DO NOTHING;
