"""
E-Commerce INITIAL (full) load
==============================

Extract -> Validate -> Transform -> Load -> Quality Check

    check_raw_files
          |
    create_dwh_tables            (idempotent DDL)
          |
    extract_to_bronze            Spark: CSV -> lake/bronze (orders up to `end_date`)
          |
    validate_bronze              Spark: rules + quarantine, fails the run on bad batches
          |
    transform_to_silver          Spark: clean / type / dedupe -> lake/silver
          |
    truncate_dwh                 full reload only
          |
    build_and_load_gold          Spark: star schema -> lake/gold + Postgres (JDBC)
          |
    create_views
          |
    quality_checks               reconcile DWH with the gold report

Run it manually from the Airflow UI ("Trigger DAG"). `end_date` defaults to 2017-12-31;
orders after that date are left for the incremental loads.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Param, chain, dag, task

# /usr/local/airflow inside the Astro containers (same path is mounted on the Spark workers)
PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
JOBS = f"{PROJECT_ROOT}/include/spark_jobs"
RAW_DIR = f"{PROJECT_ROOT}/include/data/raw"
REPORTS = f"{PROJECT_ROOT}/include/data/lake/_reports"
JDBC_JAR = "/opt/spark-jars/postgresql-42.7.4.jar"
DWH_CONN = "dwh_postgres"
SPARK_CONN = "spark_default"

BATCH_ID = "initial_{{ params.end_date | replace('-', '') }}"

SPARK_CONF = {
    "spark.pyspark.python": "python3",
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1536m",
    "spark.executor.cores": "2",
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.session.timeZone": "UTC",
    # Lake folders are written by the driver (Airflow) and the executors (Spark containers):
    # create them world-writable so either side can rename / clean them up.
    "spark.hadoop.fs.permissions.umask-mode": "000",
}

log = logging.getLogger(__name__)


def _on_failure(context) -> None:
    ti = context["ti"]
    log.error("Task %s failed (try %s) in run %s", ti.task_id, ti.try_number, context["run_id"])


def spark_job(task_id: str, script: str, args: list[str], **kwargs) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN,
        application=f"{JOBS}/{script}",
        py_files=f"{JOBS}/common.py",
        name=f"ecommerce_{task_id}",
        application_args=args,
        conf=SPARK_CONF,
        verbose=False,
        **kwargs,
    )


@dag(
    dag_id="ecommerce_initial_load",
    description="Full load of the Olist e-commerce data into the star schema",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[f"{PROJECT_ROOT}/include/sql"],
    params={
        "end_date": Param("2017-12-31", type="string", format="date",
                          description="Last purchase date included in the initial load"),
    },
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "on_failure_callback": _on_failure,
    },
    tags=["ecommerce", "initial-load", "spark"],
    doc_md=__doc__,
)
def ecommerce_initial_load():

    @task(retries=0)
    def check_raw_files() -> list[str]:
        """Fail fast with a clear message if the Olist CSV files are not in include/data/raw."""
        expected = [
            "olist_customers_dataset.csv", "olist_geolocation_dataset.csv",
            "olist_order_items_dataset.csv", "olist_order_payments_dataset.csv",
            "olist_order_reviews_dataset.csv", "olist_orders_dataset.csv",
            "olist_products_dataset.csv", "olist_sellers_dataset.csv",
            "product_category_name_translation.csv",
        ]
        missing = [f for f in expected if not os.path.exists(os.path.join(RAW_DIR, f))]
        if missing:
            raise FileNotFoundError(
                f"Missing files in include/data/raw: {missing}. "
                "Unzip the Olist dataset (archive.zip) into include/data/raw/."
            )
        log.info("All %s source files found", len(expected))
        return expected

    create_dwh_tables = SQLExecuteQueryOperator(
        task_id="create_dwh_tables", conn_id=DWH_CONN, sql="ddl/01_create_dwh.sql",
    )

    extract = spark_job("extract_to_bronze", "bronze_ingest.py",
                        ["--batch-id", BATCH_ID, "--end-date", "{{ params.end_date }}"])

    validate = spark_job("validate_bronze", "validate_bronze.py",
                         ["--batch-id", BATCH_ID, "--max-invalid-pct", "5"], retries=0)

    transform = spark_job("transform_to_silver", "silver_transform.py", ["--batch-id", BATCH_ID])

    truncate_dwh = SQLExecuteQueryOperator(
        task_id="truncate_dwh", conn_id=DWH_CONN, sql="ddl/00_truncate_dwh.sql",
    )

    load = spark_job(
        "build_and_load_gold", "gold_build_load.py",
        ["--batch-id", BATCH_ID, "--sink", "jdbc"],
        jars=JDBC_JAR,
        env_vars={
            "DWH_JDBC_URL": "jdbc:postgresql://{{ conn.dwh_postgres.host }}:"
                            "{{ conn.dwh_postgres.port }}/{{ conn.dwh_postgres.schema }}",
            "DWH_USER": "{{ conn.dwh_postgres.login }}",
            "DWH_PASSWORD": "{{ conn.dwh_postgres.password }}",
        },
    )

    create_views = SQLExecuteQueryOperator(
        task_id="create_views", conn_id=DWH_CONN, sql="views/02_create_views.sql",
    )

    @task(retries=0)
    def quality_checks(**context) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        from include.dq.dwh_checks import run_checks

        batch_id = "initial_" + context["params"]["end_date"].replace("-", "")
        with open(os.path.join(REPORTS, f"gold_{batch_id}.json"), encoding="utf-8") as fh:
            expected = json.load(fh)["expected"]

        hook = PostgresHook(postgres_conn_id=DWH_CONN)
        results = run_checks(lambda sql: hook.get_first(sql)[0], expected)

        for r in results:
            log.info("%s | %-70s | value=%s rule=%s", "PASS" if r["passed"] else "FAIL",
                     r["check"], r["value"], r["rule"])
        with open(os.path.join(REPORTS, f"dq_{batch_id}.json"), "w", encoding="utf-8") as fh:
            json.dump(results, fh, indent=2)

        failed = [r["check"] for r in results if not r["passed"]]
        if failed:
            raise ValueError(f"{len(failed)} data-quality checks failed: {failed}")
        return {"checks": len(results), "failed": 0}

    chain(check_raw_files(), create_dwh_tables, extract, validate, transform,
          truncate_dwh, load, create_views, quality_checks())


ecommerce_initial_load()
