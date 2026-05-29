# Real-Time Transaction Analytics Pipeline

![CI](https://github.com/YOUR_USERNAME/transaction-pipeline/actions/workflows/ci.yml/badge.svg)

An end-to-end data engineering project that ingests a synthetic stream of
card-transaction events, lands them in a PostgreSQL warehouse, models them into
a star schema, and exposes the results through a REST API. Every layer is
containerised; one `make up` command starts the whole stack.

**Stack:** Python 3.12 · Kafka (KRaft) · PostgreSQL 16 · Apache Airflow 2.9 ·
FastAPI · Docker Compose · pytest · GitHub Actions CI

---

## Architecture

```
  ┌─────────────────────────────┐
  │  pipeline/producer.py        │
  │  20 events/s · 500 users    │──────────────────────────────┐
  │  8 currencies · log-normal  │                              │
  │  amount distribution        │                              ▼
  └─────────────────────────────┘               ┌─────────────────────────┐
                                                 │   Kafka topic            │
                                                 │   transactions.raw       │
                                                 │   partitioned by user_id │
                                                 └────────────┬────────────┘
                                                              │
                                                              ▼
                                                 ┌─────────────────────────────────┐
                                                 │  pipeline/consumer.py            │
                                                 │  • validate via Transaction      │
                                                 │    pydantic model                │
                                                 │  • enrich: FX → GBP,            │
                                                 │    fraud flag                    │
                                                 │  • batch-insert (50 rows / 2 s) │
                                                 │  • skip poison messages          │
                                                 └───────────────┬─────────────────┘
                                                                 │
                                                                 ▼
                              ┌──────────────────────────────────────────────┐
                              │  PostgreSQL  (warehouse db)                   │
                              │                                               │
                              │  raw.transactions  ─── append-only ───────── │
                              │  (source of truth, replay buffer)             │
                              └───────────────────────┬──────────────────────┘
                                                      │
                                      Airflow DAG runs @hourly
                                      max_active_runs = 1
                                                      │
                              ┌───────────────────────▼──────────────────────┐
                              │  data_quality_checks  (gate)                  │
                              │      │                                        │
                              │  ┌───┴────────────────────────┐              │
                              │  ▼                            ▼               │
                              │  upsert_dim_user    upsert_dim_merchant       │
                              │  └───────────────┬────────────┘              │
                              │                  ▼                            │
                              │       load_fact_transaction                   │
                              │       (watermarked, incremental)              │
                              │                  │                            │
                              │                  ▼                            │
                              │       build_daily_aggregates                  │
                              └───────────────────┬──────────────────────────┘
                                                  │
                              ┌───────────────────▼──────────────────────────┐
                              │  analytics schema                             │
                              │                                               │
                              │  dim_user · dim_merchant · dim_date          │
                              │  fact_transaction                             │
                              │  daily_spend_by_category  (mart)             │
                              │  dq_check_log · load_watermark               │
                              └───────────────────┬──────────────────────────┘
                                                  │
                                    pipeline/api.py (FastAPI)
                                    psycopg_pool · port 8000
                                                  │
                              ┌───────────────────▼──────────────────────────┐
                              │  REST API  http://localhost:8000               │
                              │                                               │
                              │  GET /health                                  │
                              │  GET /users/{user_id}/spend                   │
                              │  GET /daily-spend                             │
                              │  GET /merchants/top                           │
                              └───────────────────────────────────────────────┘
```

---

## What it demonstrates

| Area | Where it lives |
|---|---|
| **Well-designed REST APIs** | `pipeline/api.py` — FastAPI with pydantic response models, parameterised SQL, psycopg_pool connection pool, Query() validation, lifespan management |
| **Streaming data ingestion** | `pipeline/producer.py` + `pipeline/consumer.py` — Kafka producer/consumer with manual offset commit, batched idempotent writes |
| **Data modelling** | `sql/` — denormalised raw landing zone → conformed star schema (dims + fact + mart) |
| **Incremental ETL** | `airflow/dags/transaction_warehouse.py` — watermarked fact load; each run only touches newly-ingested rows |
| **Data quality** | Pydantic validation at ingest edge + Airflow DQ gate (null checks, amount positivity, status enum) writing to `dq_check_log` |
| **Pure-function TDD** | `pipeline/transforms.py` has no I/O; all business logic tested in `tests/` with zero infrastructure |
| **CI/CD** | `.github/workflows/ci.yml` — 4 parallel jobs: unit tests, linting, DAG import check, Docker build |
| **Cloud deployment** | `docs/gcp-deployment.md` — mapped path to Confluent Cloud / Cloud SQL / Cloud Composer / GKE |

---

## Quick start

Requires Docker and Docker Compose v2.

```bash
make up      # build images and start all 6 services
make logs    # tail the consumer (watch events land in real time)
make psql    # open a psql shell on the warehouse database
make test    # run the unit suite locally — no Docker needed
make clean   # stop everything and delete volumes (wipes all data)
```

After `make up`, services are reachable at:

| Service | URL |
|---|---|
| Airflow UI | <http://localhost:8080> |
| REST API | <http://localhost:8000> |
| PostgreSQL | `localhost:5432` (user `pipeline`, db `warehouse`) |
| Kafka | `localhost:9092` |

**Airflow login.** Username is `admin`. The password is generated on first boot
by `airflow standalone` and written to a file — retrieve it with:

```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
# or from the startup logs:
docker compose logs airflow | grep -i "Password for user 'admin'"
```

Unpause and trigger the `transaction_warehouse` DAG to model the events that
have already landed in `raw.transactions` into the star schema.

---

## First-run checklist

Run these after `make up` to confirm every layer is healthy:

```bash
# 1. Raw events are landing (row count should climb every few seconds)
docker compose exec postgres psql -U pipeline -d warehouse \
  -c "SELECT count(*), max(ingested_at) FROM raw.transactions;"

# 2. REST API health check
curl -s http://localhost:8000/health
# {"status":"ok"}

# 3. Trigger the Airflow DAG, then verify it ran clean
docker compose exec postgres psql -U pipeline -d warehouse \
  -c "SELECT run_ts, check_name, passed, detail
      FROM analytics.dq_check_log
      ORDER BY run_ts DESC LIMIT 5;"

# 4. Confirm the mart has rows
docker compose exec postgres psql -U pipeline -d warehouse \
  -c "SELECT date_key, merchant_category, total_gbp, txn_count
      FROM analytics.daily_spend_by_category
      ORDER BY date_key DESC, total_gbp DESC LIMIT 10;"
```

---

## Repository layout

```
pipeline/
  producer.py          synthetic event generator — publishes to Kafka
  consumer.py          stream consumer — validates, enriches, inserts
  models.py            pydantic event contract (Transaction, EnrichedTransaction)
  transforms.py        pure business logic — enrich(), flag_suspicious(),
                       daily_spend_by_category()
  fx.py                static FX rate table; to_gbp() raises UnknownCurrencyError
  db.py                thin psycopg v3 wrapper — append-only batch insert
  config.py            Settings dataclass; reads from env, defaults match
                       docker-compose service names
  api.py               FastAPI app — 4 endpoints, psycopg_pool, pydantic responses

sql/
  00_init_databases.sql    create the airflow metadata database
  01_raw_schema.sql        raw.transactions table + indexes
  02_analytics_schema.sql  star schema (dims, fact, mart, watermark, dq log)
  03_seed_dim_date.sql     populate dim_date for a multi-year range

airflow/
  Dockerfile             extends apache/airflow:2.9.3-python3.12;
                         adds apache-airflow-providers-postgres
  dags/
    transaction_warehouse.py   hourly ETL DAG (4 tasks)
    sql/
      upsert_dim_user.sql
      upsert_dim_merchant.sql
      load_fact_transaction.sql    incremental, watermarked
      build_daily_aggregates.sql   upsert into mart

tests/
  test_fx.py          6 tests — conversion, rounding, error handling
  test_models.py      7 tests — validation, coercion, JSON round-trip
  test_transforms.py  6 tests — enrichment, fraud flag, aggregation logic
  test_api.py        11 tests — all endpoints, 404, 422, date defaulting

docs/
  gcp-deployment.md   mapping from Docker Compose to GCP managed services
  screenshots/
    README.md         guidance on what screenshots to capture

.github/
  workflows/ci.yml    CI: test · lint · dag-import · docker-build (parallel)

docker-compose.yml    6 services: kafka, postgres, airflow, consumer, producer, api
Dockerfile            app image (python:3.12-slim); CMD overridden per service
Makefile              convenience targets: up, down, logs, test, psql, topics, clean
pyproject.toml        ruff (line-length 100, py312) + pytest config
requirements.txt      pydantic, confluent-kafka, psycopg[binary], psycopg_pool,
                      fastapi, uvicorn[standard]
requirements-dev.txt  above + pytest, httpx
```

---

## Stream layer

### Producer (`pipeline/producer.py`)

Generates synthetic card-transaction events at a configurable rate
(default 20/s) and publishes them as JSON to the Kafka topic
`transactions.raw`, keyed by `user_id` to preserve per-user ordering.

Event characteristics:
- **500 users**, **120 merchants**, **10 merchant categories**
  (Groceries, Restaurants, Transport, Travel, Entertainment, Utilities,
  Shopping, Health, Cash, Transfers)
- **8 currencies**: GBP, USD, EUR, INR, PLN, AED, SGD, JPY
- **Amount distribution**: log-normal (`μ=2.6, σ=1.1`) — realistic long tail
  of small spends with occasional large transactions
- **Status weights**: 88 % AUTHORISED · 9 % DECLINED · 3 % REVERSED

### Consumer (`pipeline/consumer.py`)

Polls the topic in a tight loop, processes events in batches of 50 (or
flushes every 2 s, whichever comes first), and uses **manual offset commit**
so that a crash before the database write does not acknowledge the messages.

Processing per event:
1. Deserialise JSON into a `Transaction` pydantic model — rejects malformed
   events with a log line rather than crashing the stream.
2. Call `enrich()` from `transforms.py` to compute `amount_gbp` and
   `is_flagged`.
3. Accumulate into a batch; flush to `raw.transactions` via `db.insert_batch`.

### Domain models (`pipeline/models.py`)

`Transaction` is the **producer-consumer contract** — it validates the inbound
event and rejects anything that doesn't conform:

| Field | Validation |
|---|---|
| `transaction_id` | `min_length=8` |
| `user_id`, `merchant_id` | `> 0` |
| `amount` | `> 0` |
| `currency` | 3-letter alphabetic ISO-4217, uppercased |
| `country` | 2-letter alphabetic ISO-3166, uppercased |
| `status` | enum: `AUTHORISED`, `DECLINED`, `REVERSED` |
| `event_time` | naive datetimes are coerced to UTC |

`EnrichedTransaction` extends the above with `amount_gbp`, `is_flagged`, and
`ingested_at`, and is what lands in the database.

### Enrichment logic (`pipeline/transforms.py`)

All functions are **pure** (no I/O, no clock reads injected implicitly), which
makes them trivially unit-testable without any infrastructure.

- **`enrich(txn, now=None)`** — calls `fx.to_gbp()` to compute `amount_gbp`,
  applies `flag_suspicious()`, attaches `ingested_at`.
- **`flag_suspicious(amount_gbp, status, currency)`** — returns `True` when
  GBP value ≥ £5 000 (`HIGH_VALUE_GBP_THRESHOLD`), or when a transaction is
  AUTHORISED in a currency with no configured FX rate.
- **`daily_spend_by_category(txns)`** — Python reference implementation of the
  SQL mart aggregate; exists so the business rule can be tested without
  Postgres.

### FX conversion (`pipeline/fx.py`)

Static rate table (units of currency per 1 GBP). `to_gbp()` divides by the
rate and rounds to 2 dp using `ROUND_HALF_UP`. Raises `UnknownCurrencyError`
for any unlisted currency.

| Currency | Rate (per GBP) |
|---|---|
| GBP | 1.00 |
| USD | 1.27 |
| EUR | 1.17 |
| INR | 106.50 |
| PLN | 5.05 |
| AED | 4.66 |
| SGD | 1.71 |
| JPY | 198.40 |

---

## Warehouse schema

SQL init scripts run in alphabetical order when the Postgres container first
starts (`docker-entrypoint-initdb.d`):

### `raw.transactions` (append-only landing zone)

| Column | Type | Notes |
|---|---|---|
| `transaction_id` | TEXT PK | dedup key |
| `user_id` | BIGINT | |
| `card_id` | TEXT | |
| `merchant_id` | BIGINT | |
| `merchant_name` | TEXT | |
| `merchant_category` | TEXT | |
| `amount` | NUMERIC(18,2) | original currency |
| `currency` | CHAR(3) | |
| `amount_gbp` | NUMERIC(18,2) | converted at ingest time |
| `country` | CHAR(2) | |
| `status` | TEXT | AUTHORISED / DECLINED / REVERSED |
| `is_flagged` | BOOLEAN | fraud-flag from consumer |
| `event_time` | TIMESTAMPTZ | when the transaction occurred |
| `ingested_at` | TIMESTAMPTZ | when the consumer wrote it (watermark column) |

Indexed on `event_time` and `ingested_at` for efficient incremental reads.

### Analytics star schema

**`analytics.fact_transaction`** — one row per transaction, FK to all three
dims. The fact load uses `ON CONFLICT (transaction_id) DO NOTHING`, making
re-runs idempotent.

**`analytics.dim_user`** — `user_id`, `first_seen`, `last_seen`, `txn_count`.
Upserted from `raw.transactions`; `first_seen` never regresses.

**`analytics.dim_merchant`** — `merchant_id`, `merchant_name`,
`merchant_category`. Upserted from raw.

**`analytics.dim_date`** — pre-seeded for a multi-year range; columns
`date_key`, `day`, `month`, `year`, `weekday`, `is_weekend`.

**`analytics.daily_spend_by_category`** — pre-aggregated mart:
`(date_key, merchant_category)` PK with `total_gbp` (AUTHORISED only),
`txn_count`, `flagged_count`.

**`analytics.load_watermark`** — single row per target table recording
`last_loaded` timestamp; drives the incremental fact load.

**`analytics.dq_check_log`** — audit trail of every data-quality check run
with `check_name`, `passed`, `detail`, and `run_ts`.

---

## Orchestration (Airflow DAG)

**DAG:** `transaction_warehouse` · schedule: `@hourly` · `max_active_runs=1` ·
retries: 2 with a 2-minute delay

```
data_quality_checks  ──►  upsert_dim_user    ──►  load_fact_transaction  ──►  build_daily_aggregates
                    └──►  upsert_dim_merchant ─┘
```

### Task detail

**`data_quality_checks`** (gate task)

Runs three SQL checks against `raw.transactions` and writes every result to
`dq_check_log`. Raises `ValueError` if any check fails, blocking the
downstream tasks.

| Check | Query |
|---|---|
| `no_null_keys` | count rows where `transaction_id`, `user_id`, or `merchant_id` is NULL |
| `no_nonpositive_amounts` | count rows where `amount <= 0` |
| `valid_status` | count rows where `status` is not in `AUTHORISED`, `DECLINED`, `REVERSED` |

**`upsert_dim_user` / `upsert_dim_merchant`**

Full rebuild of each dimension from raw data using `INSERT … ON CONFLICT DO
UPDATE`. `first_seen` is preserved using `LEAST`; `last_seen` uses `GREATEST`.

**`load_fact_transaction`** (watermarked)

Reads `analytics.load_watermark` for the `low` timestamp, takes `now()` as
`high`, inserts rows from `raw.transactions` where
`ingested_at ∈ (low, high]` into `analytics.fact_transaction` via
`ON CONFLICT DO NOTHING`, then advances the watermark to `high`.

**`build_daily_aggregates`**

Upserts the mart from `fact_transaction JOIN dim_merchant`. Counts
`total_gbp` and `txn_count` for AUTHORISED rows only;
`flagged_count` is status-independent.

The Postgres connection is resolved via the Airflow connection ID
`warehouse_pg` (injected as `AIRFLOW_CONN_WAREHOUSE_PG` in docker-compose).

---

## REST API (`pipeline/api.py`)

FastAPI app served by uvicorn on port 8000. All queries target `analytics.*`
only — the API never reads from `raw.*`.

A `psycopg_pool.ConnectionPool` (min 1, max 5 connections) is opened on app
startup and closed on shutdown via FastAPI's `lifespan` context manager.

### Endpoints

#### `GET /health`

Returns `{"status": "ok"}` (HTTP 200) when the database is reachable, or
HTTP 503 if the pool cannot execute a `SELECT 1`.

```bash
curl -s http://localhost:8000/health
{"status":"ok"}
```

#### `GET /users/{user_id}/spend`

Returns total authorised GBP spend and transaction count for a user within an
optional date window. Defaults to the last 30 days if dates are omitted.
Returns HTTP 404 if `user_id` is not present in `analytics.dim_user`.

Query parameters:

| Parameter | Type | Default | Notes |
|---|---|---|---|
| `from_date` | `YYYY-MM-DD` | today − 30 days | inclusive |
| `to_date` | `YYYY-MM-DD` | today | inclusive |

```bash
# Last 30 days (default window)
curl -s http://localhost:8000/users/42/spend
{"user_id":42,"from_date":"2026-04-29","to_date":"2026-05-29","total_gbp":"312.50","txn_count":7}

# Specific window
curl -s "http://localhost:8000/users/42/spend?from_date=2026-01-01&to_date=2026-01-31"
{"user_id":42,"from_date":"2026-01-01","to_date":"2026-01-31","total_gbp":"95.00","txn_count":2}

# Unknown user
curl -s http://localhost:8000/users/99999/spend
{"detail":"user not found"}   # HTTP 404

# Invalid date
curl -s "http://localhost:8000/users/42/spend?from_date=yesterday"
{"detail":[...]}              # HTTP 422
```

#### `GET /daily-spend`

Returns rows from `analytics.daily_spend_by_category`, ordered by
`date_key DESC`. Optional `category` filter; `limit` defaults to 30, max 365.

```bash
# Last 30 rows across all categories
curl -s http://localhost:8000/daily-spend
[{"date_key":"2026-05-29","merchant_category":"Groceries","total_gbp":"4210.00","txn_count":88,"flagged_count":1}, ...]

# Filter to Travel, last 7 days
curl -s "http://localhost:8000/daily-spend?category=Travel&limit=7"

# limit out of range
curl -s "http://localhost:8000/daily-spend?limit=400"
{"detail":[...]}              # HTTP 422
```

#### `GET /merchants/top`

Returns top merchants by total authorised GBP spend, joined to
`analytics.dim_merchant` for name and category. `limit` defaults to 10,
max 100.

```bash
curl -s http://localhost:8000/merchants/top
[{"merchant_id":17,"merchant_name":"Groceries Merchant 17","merchant_category":"Groceries","total_gbp":"12400.00","txn_count":248}, ...]

curl -s "http://localhost:8000/merchants/top?limit=5"
```

---

## Testing

```bash
make test          # installs dev deps then runs pytest -q
pytest -q          # directly if deps already installed
pytest tests/test_api.py -q   # single file
```

30 tests across 4 files, all infrastructure-free:

| File | Count | What it covers |
|---|---|---|
| `tests/test_fx.py` | 6 | GBP identity, USD conversion, case insensitivity, ROUND_HALF_UP, unknown currency error, sorted currency list |
| `tests/test_models.py` | 7 | Currency/country uppercasing, naive datetime → UTC coercion, negative/zero amount rejection, bad currency length, unknown status, JSON round-trip |
| `tests/test_transforms.py` | 6 | `enrich()` sets GBP amount and ingest time, low-value not flagged, high-value flagged at threshold, `daily_spend_by_category()` sums AUTHORISED only and groups by date + category |
| `tests/test_api.py` | 11 | `/health` ok and DB-error paths, `/users/.../spend` happy path / 404 / date-window defaulting / 422, `/daily-spend` happy path / category param forwarding / 422, `/merchants/top` happy path / 422 |

The API tests patch `psycopg_pool.ConnectionPool` during the app lifespan so
no Postgres is needed, and override the `get_pool` FastAPI dependency with a
`MagicMock` whose `execute().fetchone/fetchall` return canned rows.

---

## CI (GitHub Actions)

`.github/workflows/ci.yml` — triggers on push and pull request to `main`.
All four jobs run in parallel on `ubuntu-latest` / Python 3.12.

| Job | Steps |
|---|---|
| **test** | `pip install -r requirements-dev.txt` → `pytest -q` |
| **lint** | `pip install ruff` → `ruff check pipeline/ tests/` |
| **dag-import** | Install Airflow 2.9.3 + postgres provider → import `DagBag` and assert no `import_errors` |
| **docker-build** | `docker build -t tp-app:ci .` and `docker build -t tp-airflow:ci ./airflow` |

Replace `YOUR_USERNAME` in the badge URL at the top of this file with your
GitHub username/org once the repository is pushed.

---

## Configuration

All settings are read from environment variables at import time via
`pipeline/config.py`. Defaults match docker-compose service names so the stack
works out of the box.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `transactions.raw` | Topic name |
| `KAFKA_GROUP` | `warehouse-loader` | Consumer group ID |
| `PG_HOST` | `postgres` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DB` | `warehouse` | Database name |
| `PG_USER` | `pipeline` | Database user |
| `PG_PASSWORD` | `pipeline` | Database password |
| `EVENTS_PER_SECOND` | `20` | Producer throughput (0 = unlimited) |
| `N_USERS` | `500` | Number of synthetic users |
| `N_MERCHANTS` | `120` | Number of synthetic merchants |

Copy `.env.example` to `.env` and override any values before running locally
outside Docker.

---

## Docker services

| Service | Image / Build | Port | Depends on |
|---|---|---|---|
| `kafka` | `apache/kafka:3.7.0` | 9092 | — |
| `postgres` | `postgres:16` | 5432 | — |
| `airflow` | `./airflow/Dockerfile` | 8080 | postgres healthy |
| `consumer` | `./Dockerfile` | — | kafka healthy, postgres healthy |
| `producer` | `./Dockerfile` | — | kafka healthy |
| `api` | `./Dockerfile` | 8000 | postgres healthy |

The `api`, `consumer`, and `producer` services all build from the same root
`Dockerfile` (`python:3.12-slim`) and override `CMD` to run the appropriate
module. The Airflow image extends `apache/airflow:2.9.3-python3.12` and adds
the Postgres provider.

PostgreSQL data is stored in the named volume `pgdata`.

---

## Design decisions

**Append-only raw landing.** The consumer uses `ON CONFLICT (transaction_id)
DO NOTHING`, so replaying the topic produces exactly the same rows — the raw
zone is idempotent and can always be used to rebuild the warehouse from
scratch. No `UPDATE` or `DELETE` ever touches `raw.transactions`.

**Pure transforms.** `pipeline/transforms.py` contains only functions of their
inputs, with no Kafka, Postgres, or system-clock calls. This is what makes the
business rules unit-testable without any infrastructure — the same pattern
scales to much more complex enrichment logic.

**Watermarked incremental loads.** The fact load reads
`analytics.load_watermark` for its lower bound and `now()` for its upper bound
on every run. Each DAG execution does a bounded amount of work, re-runs over
the same window are no-ops (`ON CONFLICT DO NOTHING`), and the watermark only
advances after a successful insert. This is the standard pattern for cheap,
correct incremental ETL.

**Data-quality gate.** The Airflow DAG runs three checks against the raw zone
*before* touching any analytics table. If any check fails, the DAG stops and
nothing bad reaches the star schema. Every check result is written to
`dq_check_log` regardless of pass/fail, giving a full audit trail.

**Validation at the edge.** The pydantic `Transaction` model is the
producer-consumer contract. A malformed event is caught in the consumer before
any database write, logged, and skipped. Poison messages never crash the stream
or corrupt the warehouse.

**API reads analytics only.** `pipeline/api.py` queries `analytics.*`
exclusively. It never touches `raw.transactions`. This enforces the layered
architecture at the API boundary — consumers of the API see only curated,
quality-gated data.

---

## GCP deployment path

The Docker Compose topology maps directly to GCP managed services. Application
code does not change — only configuration moves to env vars / Secret Manager.

| Local | GCP |
|---|---|
| Kafka container | Confluent Cloud or Pub/Sub |
| PostgreSQL container | Cloud SQL for PostgreSQL 16 |
| Airflow standalone | Cloud Composer 2 |
| producer / consumer | GKE Autopilot Deployments or Cloud Run |
| container images | Artifact Registry |
| secrets | Secret Manager |

See [`docs/gcp-deployment.md`](docs/gcp-deployment.md) for step-by-step
instructions, the Pub/Sub swap-out path, and production-hardening notes
(Schema Registry, dead-letter topic, partitioning, observability).

---

## Verifying data end-to-end

```sql
-- How many raw events have landed and when was the last one?
SELECT count(*), max(ingested_at) FROM raw.transactions;

-- Which users have the most transactions?
SELECT user_id, txn_count, first_seen, last_seen
FROM analytics.dim_user
ORDER BY txn_count DESC LIMIT 10;

-- Fact table row count after a DAG run
SELECT count(*) FROM analytics.fact_transaction;

-- Top categories by GBP spend this week
SELECT merchant_category, SUM(total_gbp) AS weekly_gbp
FROM analytics.daily_spend_by_category
WHERE date_key >= current_date - 7
GROUP BY merchant_category
ORDER BY weekly_gbp DESC;

-- DQ audit trail
SELECT run_ts, check_name, passed, detail
FROM analytics.dq_check_log
ORDER BY run_ts DESC LIMIT 10;

-- Flagged high-value transactions
SELECT transaction_id, user_id, amount_gbp, currency, event_time
FROM raw.transactions
WHERE is_flagged = true
ORDER BY amount_gbp DESC LIMIT 10;
```

---

## Screenshots

![Airflow DAG](docs/screenshots/airflow-dag.png)
![Mart query](docs/screenshots/mart-query.png)

See [`docs/screenshots/README.md`](docs/screenshots/README.md) for capture
instructions.
