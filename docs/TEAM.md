# Team & ownership

Each member **owns** an area: they open the Issues for it, review every PR that touches it, and present it at the
demo. Owners can work on any area; ownership only says who is responsible.

| Member | Area | Initial load (done): what to own / review | Next phases |
|---|---|---|---|
| **Nancy Emad Farah** | Team lead · platform & orchestration | Repo, Astro project, `docker-compose.override.yml`, Spark cluster image, `dags/ecommerce_initial_load.py`, CI | Incremental DAG orchestration, Azure deployment, **system design diagram** |
| **Menna Mohamed Ahmed** | Data modeling & DWH | `include/sql/ddl`, `include/sql/views`, indexes, `dim_date`; update schema images with `fact_payments` | SCD design for incremental, **sequence diagrams** (with Mohab), Synapse/Azure SQL model |
| **Amr Abobakr** | Ingestion & data quality | `bronze_ingest.py`, `validate_bronze.py`, `include/dq/dwh_checks.py`, quarantine | Watermark-based incremental extract, DQ for incremental, **data flow diagram** |
| **Salma Nabil** | Transformations | `silver_transform.py`, `gold_build_load.py` (cleaning rules, keys, payment allocation) | Incremental MERGE / upserts, SCD implementation, Delta Lake on Azure |
| **Malak Adel** | Analytics engineering | Views as a starting point for metrics | **dbt** models + tests, **semantic layer / dbt metrics**, **dashboard** |
| **Mohab Ahmed** | Streaming | — | **Kafka** producer (replay 2018 orders/payments/reviews), **Spark Structured Streaming**, DLQ topic, **Event Hubs** on Azure, sequence diagram for streaming |

## How we work on GitHub

1. Pick or create an Issue (template "Task"), assign yourself, add the phase.
2. `git checkout dev && git pull` → `git checkout -b feature/<area>-<short-name>`
3. Small commits, then open a PR into `dev` using the PR template. Link the issue (`Closes #12`).
4. At least one review (the area owner) before merge. CI must be green.
5. At the end of a phase, Nancy opens a PR `dev` → `main` and we tag it (`v0.1-initial-load`, ...).
