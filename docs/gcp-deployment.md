# Deploying on GCP

The local Docker Compose stack maps cleanly onto Google Cloud managed services.
The application code does not change — only configuration (bootstrap servers,
connection strings) moves into environment variables / secrets.

| Local component            | GCP managed service                          |
|----------------------------|----------------------------------------------|
| Kafka (Bitnami container)  | **Confluent Cloud** (managed Kafka) or **Pub/Sub** |
| PostgreSQL container       | **Cloud SQL for PostgreSQL**                 |
| Airflow (standalone)       | **Cloud Composer 2** (managed Airflow)       |
| producer / consumer apps   | **GKE Autopilot** Deployments, or **Cloud Run** |
| container images           | **Artifact Registry**                        |
| secrets (DB password etc.) | **Secret Manager**                           |

## Path of least change: keep Kafka

1. Provision **Confluent Cloud** and create the `transactions.raw` topic. Set
   `KAFKA_BOOTSTRAP` to the cluster endpoint and add the API key/secret to
   Secret Manager (the producer/consumer config reads them from env).
2. Provision **Cloud SQL (PostgreSQL 16)**. Run the `sql/` scripts once via the
   Cloud SQL Auth Proxy to create the schemas. Point `PG_HOST` at the instance
   (private IP + Auth Proxy sidecar on GKE).
3. Build the app image, push to **Artifact Registry**, and deploy the producer
   and consumer as **GKE** Deployments. The consumer scales horizontally: add
   more replicas and Kafka rebalances partitions across the consumer group
   (events are keyed by `user_id`, so per-user ordering is preserved).
4. Provision **Cloud Composer 2**, drop `airflow/dags/` into its DAGs bucket,
   and recreate the `warehouse_pg` connection pointing at Cloud SQL. The DAG is
   unchanged.

## Alternative: swap Kafka for Pub/Sub

If you prefer fully-native GCP, replace Kafka with **Pub/Sub**. Only the
transport layer in `producer.py` / `consumer.py` changes (publish to a topic /
pull from a subscription); the models, enrichment, validation, schema, and DAG
are identical. The `enable.auto.commit=false` + manual-commit pattern in the
consumer maps onto Pub/Sub explicit ack.

## Production hardening (next steps)

- **Schema Registry** (Avro/Protobuf) instead of raw JSON, for forward/backward
  compatible event evolution.
- **Dead-letter topic** for messages the consumer rejects, instead of log-and-drop.
- **Partitioning** `fact_transaction` by `date_key` once it grows large, and
  switching `daily_spend_by_category` to a windowed incremental refresh.
- **Observability**: export consumer lag and DQ metrics to Cloud Monitoring;
  alert on DAG failures and rising rejection rates.
- **CI/CD**: run `pytest` and a DAG-import check on every PR (Cloud Build /
  GitHub Actions), build and push images on merge.
