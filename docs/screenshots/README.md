# Screenshots

Placeholder directory for pipeline screenshots. Add images here and commit
them so the README badges render on GitHub.

## airflow-dag.png

Screenshot of the Airflow UI showing the `transaction_warehouse` DAG graph view
with all tasks green (data_quality_checks → upsert_dim_user / upsert_dim_merchant
→ load_fact_transaction → build_daily_aggregates). Capture after a successful
manual trigger with at least a few hundred raw events landed.

Suggested capture:
1. `make up` and wait for the consumer to land events
2. Open <http://localhost:8080>, log in, unpause `transaction_warehouse`, trigger a run
3. Wait for all tasks to succeed, then screenshot the graph view

## mart-query.png

Screenshot of a `psql` session (or pgAdmin) showing the output of:

```sql
SELECT date_key, merchant_category, total_gbp, txn_count, flagged_count
FROM analytics.daily_spend_by_category
ORDER BY date_key DESC, total_gbp DESC
LIMIT 10;
```

This confirms the mart is populated and the numbers look sensible.

Suggested capture:
1. Run `make psql` after a DAG run
2. Execute the query above
3. Screenshot the tabular output
