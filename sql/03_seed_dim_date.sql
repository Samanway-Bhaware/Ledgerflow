-- 03_seed_dim_date.sql
-- Pre-populate the date dimension so fact loads never miss a date_key.
-- Covers 2024-01-01 through 2027-12-31.

INSERT INTO analytics.dim_date (date_key, day, month, year, weekday, is_weekend)
SELECT
    d::date                                        AS date_key,
    EXTRACT(DAY   FROM d)::smallint                AS day,
    EXTRACT(MONTH FROM d)::smallint                AS month,
    EXTRACT(YEAR  FROM d)::smallint                AS year,
    (EXTRACT(ISODOW FROM d)::smallint - 1)         AS weekday,   -- 0 = Monday
    EXTRACT(ISODOW FROM d) IN (6, 7)               AS is_weekend
FROM generate_series('2024-01-01'::date, '2027-12-31'::date, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;
