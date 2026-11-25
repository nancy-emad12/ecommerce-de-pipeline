"""Shared helpers for the e-commerce Spark jobs.

Paths are configurable through environment variables so the same jobs run:
  * inside the Astro/Airflow containers (default paths below),
  * on a laptop with `SPARK_MASTER=local[*]`,
  * later on Azure (point LAKE_ROOT at abfss://... and RAW_DIR at the landing zone).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/usr/local/airflow")
RAW_DIR = os.getenv("RAW_DIR", f"{PROJECT_ROOT}/include/data/raw")
LAKE_ROOT = os.getenv("LAKE_ROOT", f"{PROJECT_ROOT}/include/data/lake")

BRONZE = f"{LAKE_ROOT}/bronze"
SILVER = f"{LAKE_ROOT}/silver"
GOLD = f"{LAKE_ROOT}/gold"
QUARANTINE = f"{LAKE_ROOT}/quarantine"
REPORTS = f"{LAKE_ROOT}/_reports"

# Data warehouse (Postgres) connection used by the gold job.
DWH_JDBC_URL = os.getenv("DWH_JDBC_URL", "jdbc:postgresql://dwh-postgres:5432/ecommerce_dwh")
DWH_USER = os.getenv("DWH_USER", "dwh")
DWH_PASSWORD = os.getenv("DWH_PASSWORD", "dwh")
DWH_SCHEMA = os.getenv("DWH_SCHEMA", "dwh")

UNKNOWN_KEY = -1

# Source entities: file name, primary key and the columns that must exist.
ENTITIES: dict[str, dict] = {
    "orders": {
        "file": "olist_orders_dataset.csv",
        "pk": ["order_id"],
        "required": [
            "order_id", "customer_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ],
    },
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "pk": ["order_id", "order_item_id"],
        "required": [
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value",
        ],
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "pk": ["order_id", "payment_sequential"],
        "required": [
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value",
        ],
    },
    "order_reviews": {
        "file": "olist_order_reviews_dataset.csv",
        "pk": ["review_id", "order_id"],
        "required": [
            "review_id", "order_id", "review_score", "review_comment_title",
            "review_comment_message", "review_creation_date", "review_answer_timestamp",
        ],
    },
    "customers": {
        "file": "olist_customers_dataset.csv",
        "pk": ["customer_id"],
        "required": [
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ],
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "pk": ["product_id"],
        "required": [
            "product_id", "product_category_name", "product_name_lenght",
            "product_description_lenght", "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ],
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "pk": ["seller_id"],
        "required": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "pk": [],  # no natural key in the source: many rows per zip prefix
        "required": [
            "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
            "geolocation_city", "geolocation_state",
        ],
    },
    "category_translation": {
        "file": "product_category_name_translation.csv",
        "pk": ["product_category_name"],
        "required": ["product_category_name", "product_category_name_english"],
    },
}

_ACCENTS_FROM = "áàâãäåéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÅÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
_ACCENTS_TO = "aaaaaaeeeeiiiiooooouuuucnAAAAAAEEEEIIIIOOOOOUUUUCN"


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger(name)


def get_spark(app_name: str) -> SparkSession:
    """Spark session. The master comes from spark-submit (Airflow) or SPARK_MASTER (local runs)."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
    )
    if os.getenv("SPARK_MASTER"):
        builder = builder.master(os.environ["SPARK_MASTER"])
    if os.getenv("SPARK_EXTRA_JARS"):
        builder = builder.config("spark.jars", os.environ["SPARK_EXTRA_JARS"])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def normalize_text(col: Column) -> Column:
    """lower-case, trim, strip Portuguese accents, collapse spaces."""
    cleaned = F.translate(F.lower(F.trim(col)), _ACCENTS_FROM, _ACCENTS_TO)
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    return F.when(cleaned == "", None).otherwise(cleaned)


def zip_prefix(col: Column) -> Column:
    """Zip prefixes are 5-char codes; keep leading zeros."""
    return F.lpad(F.regexp_replace(F.trim(col), r"\D", ""), 5, "0")


def to_ts(col: Column) -> Column:
    """Timestamp or NULL when malformed (Spark 4 runs in ANSI mode: plain casts would fail the job)."""
    return F.try_to_timestamp(F.trim(col))


def to_int(col: Column) -> Column:
    """Integer or NULL when malformed ('12', '12.0' -> 12, 'abc' -> NULL)."""
    return F.trim(col).try_cast("double").cast("int")


def to_decimal(col: Column, precision: str = "10,2") -> Column:
    return F.trim(col).try_cast(f"decimal({precision})")


def to_double(col: Column) -> Column:
    return F.trim(col).try_cast("double")


def date_key(col: Column) -> Column:
    """yyyymmdd integer key, -1 when the date is missing."""
    return F.coalesce(F.date_format(col, "yyyyMMdd").cast("int"), F.lit(UNKNOWN_KEY))


def null_if_blank(col: Column) -> Column:
    trimmed = F.trim(col)
    return F.when(trimmed == "", None).otherwise(trimmed)


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> int:
    df.write.mode(mode).parquet(path)
    return df.sparkSession.read.parquet(path).count()


def write_report(name: str, payload: dict) -> str:
    """Small JSON report next to the lake (local filesystem path)."""
    path = REPORTS.replace("file://", "")
    os.makedirs(path, exist_ok=True)
    payload = {"generated_at": datetime.now(UTC).isoformat(), **payload}
    full = os.path.join(path, f"{name}.json")
    with open(full, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, default=str)
    return full


def read_report(name: str) -> dict:
    with open(os.path.join(REPORTS.replace("file://", ""), f"{name}.json"), encoding="utf-8") as fh:
        return json.load(fh)
