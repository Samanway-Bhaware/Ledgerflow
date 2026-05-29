-- upsert_dim_merchant.sql
-- Refresh merchant dimension. Latest observed name/category wins.

INSERT INTO analytics.dim_merchant (merchant_id, merchant_name, merchant_category)
SELECT DISTINCT ON (merchant_id)
    merchant_id, merchant_name, merchant_category
FROM raw.transactions
ORDER BY merchant_id, event_time DESC
ON CONFLICT (merchant_id) DO UPDATE SET
    merchant_name = EXCLUDED.merchant_name,
    merchant_category = EXCLUDED.merchant_category;
