-- upsert_dim_user.sql
-- Rebuild/refresh the user dimension from everything in the raw landing zone.
-- first_seen never moves; last_seen and txn_count roll forward.

INSERT INTO analytics.dim_user (user_id, first_seen, last_seen, txn_count)
SELECT
    user_id,
    MIN(event_time) AS first_seen,
    MAX(event_time) AS last_seen,
    COUNT(*)        AS txn_count
FROM raw.transactions
GROUP BY user_id
ON CONFLICT (user_id) DO UPDATE SET
    last_seen = GREATEST(analytics.dim_user.last_seen, EXCLUDED.last_seen),
    first_seen = LEAST(analytics.dim_user.first_seen, EXCLUDED.first_seen),
    txn_count = EXCLUDED.txn_count;
