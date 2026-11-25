# E-Commerce Data Integration & Analytics Pipeline

DEPI Data Engineering capstone: an end-to-end pipeline on the
[Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(≈100k orders, 2016-09 → 2018-10). Data goes from CSV sources through a Bronze → Silver → Gold lake into a Postgres star schema,
orchestrated by Airflow (Astro) and processed with PySpark on a Spark cluster.

| Phase | Status |
|---|---|
| 0. Setup (repo, Astro, Docker, Spark cluster, DWH) | ✅ done |
| 1. Initial (full) load | ✅ done |
| 2. Incremental load (batch) | ⏳ next |
| 3. Streaming (Kafka + Spark Structured Streaming) | ⏳ |
| 4. dbt models, semantic layer, metrics dashboard | ⏳ |
| 5. Diagrams (data flow, system design, sequence) | ⏳ |
| 6. Azure (ADLS Gen2, Event Hubs, Databricks/Synapse) | ⏳ |

Team and ownership: [docs/TEAM.md](docs/TEAM.md) · Design decisions: [docs/decisions.md](docs/decisions.md) ·
Data profiling: [docs/data_profiling.md](docs/data_profiling.md)

---

## Architecture (initial load)

```
 CSV sources (include/data/raw)
        │  extract_to_bronze      Spark
        ▼
 lake/bronze   raw strings + ingestion metadata, partitioned by load_batch
        │  validate_bronze        Spark  → lake/quarantine + report, fails run if > 5% bad rows
        ▼
 lake/silver   typed, cleaned, de-duplicated
        │  build_and_load_gold    Spark  → lake/gold (parquet) + JDBC
        ▼
 Postgres DWH  schema dwh: 9 dimensions + fact_sales + fact_payments, indexes, views
        │  quality_checks         30 reconciliation / integrity checks
        ▼
 Analysts / dbt / dashboard
```

Containers (all started by `astro dev start`):

| Service | What | Host port |
|---|---|---|
| Airflow (Astro Runtime 3.3, Airflow 3.3, Python 3.12) | api-server, scheduler, dag-processor, triggerer, metadata DB | 8080 |
| spark-master / spark-worker | Spark 4.0.1 standalone cluster (Python 3.12, Java 21) | 8090 / 8091 |
| dwh-postgres | Postgres 16, database `ecommerce_dwh`, user/password `dwh`/`dwh` | 5433 |

Airflow submits the jobs with `SparkSubmitOperator` (client mode). The project's `include/` folder is mounted at the
same path (`/usr/local/airflow/include`) in Airflow and in the Spark containers, so the lake paths are identical everywhere.

## Data model

Star schema built from the team's conceptual / logical / physical design in [docs/schema](docs/schema):

* **fact_sales**: one row per order item. FKs to 8 date roles, customer, product, seller, order status, review score,
  review, customer and seller geolocation. Measures: `price`, `freight_value`, `allocated_payment_value`.
* **fact_payments**: one row per payment, so an order paid partly by card and partly by voucher keeps both rows.
  Measures: `payment_value`, `payment_installments`.
* **Dimensions**: date, customer, seller, product, geolocation, order status, payment type, score, order reviews.
  Every dimension has an *Unknown* member with key `-1`.

Analytical views: `vw_sales_daily`, `vw_sales_by_category_month`, `vw_delivery_performance`, `vw_payment_mix_month`,
`vw_sales_by_state`, `vw_seller_performance`.

## Run it locally

Prerequisites: Docker Desktop (WSL 2 on Windows, about 8 GB RAM for Docker) and the
[Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).

```bash
git clone <repo-url> && cd ecommerce-de-pipeline

# 1. put the 9 Olist CSV files in include/data/raw/  (see include/data/raw/README.md)
# 2. start everything
astro dev start
```

3. Open http://localhost:8080 and trigger **`ecommerce_initial_load`**. `end_date` defaults to `2017-12-31`.
4. Check the results:
   * Spark UI: http://localhost:8090
   * DWH: `psql -h localhost -p 5433 -U dwh -d ecommerce_dwh` (password `dwh`), e.g. `SELECT * FROM dwh.vw_sales_daily LIMIT 10;`
   * Reports: `include/data/lake/_reports/*.json` (row counts, validation, data-quality results)

Expected result for `end_date = 2017-12-31`: 45,430 orders → **51,234 rows in fact_sales**, **47,871 rows in fact_payments**,
and all 30 data-quality checks passing.

Try it without the full dataset: point the jobs at the sample files:
`RAW_DIR=include/data/sample` (see [docs/decisions.md](docs/decisions.md#running-spark-jobs-without-airflow)).

## Repository layout

```
dags/                      Airflow DAGs (ecommerce_initial_load)
include/spark_jobs/        PySpark jobs: bronze_ingest, validate_bronze, silver_transform, gold_build_load, common
include/sql/ddl/           DWH tables, keys, indexes (+ truncate script for full reloads)
include/sql/views/         Analytical views
include/dq/                Data-quality checks run after each load
include/data/raw/          Full dataset (git-ignored)
include/data/sample/       Small sample of every source file (committed)
include/data/lake/         Bronze / Silver / Gold / quarantine / reports (git-ignored, generated)
docker/spark/              Spark cluster image
docker-compose.override.yml  Spark + DWH Postgres, started with Airflow by Astro
airflow_settings.yaml      Local Airflow connections (spark_default, dwh_postgres)
tests/                     DAG integrity tests + unit tests
docs/                      Team, decisions, profiling, schema images, diagrams
```

## Development workflow

* `main` = what we present, protected. `dev` = integration branch. Work on `feature/<area>-<short-name>`.
* Every change goes through a Pull Request into `dev` with at least one review; `dev` → `main` at the end of each phase.
* One GitHub Issue per task, assigned to its owner (template in `.github/ISSUE_TEMPLATE`).
* CI (GitHub Actions) runs `ruff` and the unit tests on every PR. DAG tests: `astro dev pytest`.
