-- build_daily_aggregates.sql
-- Rebuild the reporting mart from the fact table. Authorised spend only for the
-- money totals; flagged_count tracks risk regardless of status. Full refresh is
-- fine at this volume; partition by date_key window if it ever grows large.

INSERT INTO analytics.daily_spend_by_category (
    date_key, merchant_category, total_gbp, txn_count, flagged_count
)
SELECT
    f.date_key,
    m.merchant_category,
    COALESCE(SUM(f.amount_gbp) FILTER (WHERE f.status = 'AUTHORISED'), 0) AS total_gbp,
    COUNT(*) FILTER (WHERE f.status = 'AUTHORISED')                       AS txn_count,
    COUNT(*) FILTER (WHERE f.is_flagged)                                  AS flagged_count
FROM analytics.fact_transaction f
JOIN analytics.dim_merchant m USING (merchant_id)
GROUP BY f.date_key, m.merchant_category
ON CONFLICT (date_key, merchant_category) DO UPDATE SET
    total_gbp     = EXCLUDED.total_gbp,
    txn_count     = EXCLUDED.txn_count,
    flagged_count = EXCLUDED.flagged_count;
