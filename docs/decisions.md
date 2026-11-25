# Design decisions

Decisions taken by the team, newest last. Each one says what changed compared with the original schema or plan.

| # | Date | Decision | Why |
|---|---|---|---|
| 1 | 2026-11-25 | Project 1 (E-Commerce) on the Olist dataset | Team choice |
| 2 | 2026-11-25 | Airflow runs with the **Astro CLI** (Astro Runtime 3.3 = Airflow 3.3, Python 3.12) | Team choice; extra services go in `docker-compose.override.yml` |
| 3 | 2026-11-25 | **Spark standalone cluster** (1 master + 1 worker) instead of Spark inside Airflow | Streaming jobs need a long-running Spark; clearer system design |
| 4 | 2026-11-25 | Spark 4.0.1 + Python 3.12 + Java 21 in both the Airflow image and the Spark image | PySpark needs the same Python version on driver and workers |
| 5 | 2026-11-25 | Lake = shared folder `include/data/lake` (bronze / silver / gold / quarantine / _reports) | Works on every laptop without extra services. MinIO (S3) is planned for the Azure phase, so the lake moves to object storage before ADLS |
| 6 | 2026-11-25 | Initial load = orders purchased **on or before 2017-12-31**; 2018 is replayed as incremental + streaming | The dataset is a static snapshot, so new data has to be simulated |
| 7 | 2026-11-25 | **Payments: new `fact_payments` table** (one row per payment). `fact_sales` drops `payment_type_key` and `payment_installments` and keeps `allocated_payment_value` | An order paid 280 cash + 180 card must keep both payment types. 1,174 orders in the initial load use more than one type |
| 8 | 2026-11-25 | `allocated_payment_value` = the order's total payments split across its items by (price + freight) share; the rounding remainder goes to the last item | The allocated values add up exactly to what was paid. The data-quality check verifies this |
| 9 | 2026-11-25 | One review per order in the star: the **latest answered** review | `dim_order_reviews` is keyed on `order_id`, and some orders have more than one review |
| 10 | 2026-11-25 | `dim_customer` keyed on `customer_unique_id`; attributes from the person's most recent order | `customer_id` is generated for every order and is not a real customer |
| 11 | 2026-11-25 | SCD type for customer / product / seller decided in the incremental phase | Not needed for a single full load |
| 12 | 2026-11-25 | Postgres types: `TINYINT` → `SMALLINT`; `product_category_name_english` widened to `VARCHAR(60)` | Postgres has no TINYINT; untranslated categories keep their Portuguese name |
| 13 | 2026-11-25 | Unknown member (key `-1`) in every dimension | Missing dates (≈3k undelivered orders), 278 customer zips not in geolocation, orders without reviews |

## Cleaning rules (silver)

* Zip prefixes are 5-character strings, so leading zeros are kept.
* Cities: lower-case, trimmed, accents removed (`são paulo` → `sao paulo`). States: upper-case.
* Geolocation: 1,000,163 rows → **one row per zip prefix**. Exact duplicates are dropped, points outside Brazil's
  bounding box are dropped, lat/lng are averaged, and city/state take the most frequent value.
* Products: source typos fixed (`lenght` → `length`); English category from the translation table; missing translation →
  Portuguese name; null category → `unknown`.
* Review text: line breaks removed, title cut to 30 characters and message to 255 (the physical schema sizes).
* Malformed numbers or timestamps → NULL (`try_cast`), never a crashed job. Rows whose mandatory fields are NULL are dropped
  and also listed in `lake/quarantine` by the validation step.

## Validation gate (bronze)

`validate_bronze` fails the DAG run when a required column is missing, `orders`/`order_items` is empty, or more than 5% of
an entity's rows break a rule (null keys, non-numeric or negative amounts, bad timestamps, review score outside 1..5).
Bad rows are written to `lake/quarantine/<entity>/load_batch=<id>/` with the reason. We tested it: 12% corrupted prices
→ run fails; 50 corrupted prices → run passes and silver drops exactly those 50 rows.

## Running Spark jobs without Airflow

From `include/spark_jobs/` with PySpark 4.0.1 installed:

```bash
export PROJECT_ROOT=<repo path> SPARK_MASTER='local[4]'
# optional: RAW_DIR=<repo>/include/data/sample  LAKE_ROOT=/tmp/lake
python bronze_ingest.py   --batch-id initial_20171231 --end-date 2017-12-31
python validate_bronze.py --batch-id initial_20171231
python silver_transform.py --batch-id initial_20171231
python gold_build_load.py --batch-id initial_20171231 --sink parquet   # or --sink jdbc with SPARK_EXTRA_JARS=<postgres jdbc jar>
```
