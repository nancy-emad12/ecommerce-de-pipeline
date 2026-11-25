"""EXTRACT: source CSV files -> Bronze (raw parquet, every column kept as string).

The Olist dataset is a static snapshot (2016-09 .. 2018-10). To simulate a real
source we extract a time window of orders:
  * initial load      : every order purchased on or before --end-date
  * incremental loads : orders purchased after --start-date and on or before --end-date
Child records (items, payments, reviews, customers) follow their orders; products and
sellers follow the items. Reference data (geolocation, category translation) is loaded
in full.

Bronze path: lake/bronze/<entity>/load_batch=<batch_id>/
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import BRONZE, ENTITIES, RAW_DIR, get_logger, get_spark, write_report

log = get_logger("bronze_ingest")

# Reviews contain line breaks and quotes inside the text fields.
MULTILINE = {"order_reviews"}


def read_source(spark, entity: str) -> DataFrame:
    meta = ENTITIES[entity]
    path = f"{RAW_DIR}/{meta['file']}"
    reader = (
        spark.read.option("header", True)
        .option("inferSchema", False)  # bronze keeps raw strings, typing happens in silver
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", entity in MULTILINE)
    )
    df = reader.csv(path)
    # Strip a possible UTF-8 BOM from the first header.
    df = df.toDF(*[c.replace("﻿", "").strip() for c in df.columns])
    return df.withColumn("_source_file", F.lit(meta["file"]))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--end-date", required=True, help="inclusive, YYYY-MM-DD")
    parser.add_argument("--start-date", default=None, help="exclusive, YYYY-MM-DD (incremental only)")
    args = parser.parse_args()

    spark = get_spark(f"bronze_ingest_{args.batch_id}")
    end_exclusive = (date.fromisoformat(args.end_date) + timedelta(days=1)).isoformat()

    raw = {name: read_source(spark, name) for name in ENTITIES}

    purchase_ts = F.try_to_timestamp(F.trim(F.col("order_purchase_timestamp")))
    orders = raw["orders"].where(purchase_ts < F.lit(end_exclusive))
    if args.start_date:
        start_exclusive = (date.fromisoformat(args.start_date) + timedelta(days=1)).isoformat()
        orders = orders.where(purchase_ts >= F.lit(start_exclusive))
    orders = orders.cache()

    order_ids = orders.select("order_id").distinct()
    items = raw["order_items"].join(order_ids, "order_id", "left_semi").cache()

    extracted = {
        "orders": orders,
        "order_items": items,
        "order_payments": raw["order_payments"].join(order_ids, "order_id", "left_semi"),
        "order_reviews": raw["order_reviews"].join(order_ids, "order_id", "left_semi"),
        "customers": raw["customers"].join(
            orders.select("customer_id").distinct(), "customer_id", "left_semi"
        ),
        "products": raw["products"].join(
            items.select("product_id").distinct(), "product_id", "left_semi"
        ),
        "sellers": raw["sellers"].join(
            items.select("seller_id").distinct(), "seller_id", "left_semi"
        ),
        "geolocation": raw["geolocation"],
        "category_translation": raw["category_translation"],
    }

    counts = {}
    for entity, df in extracted.items():
        out = (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_batch_id", F.lit(args.batch_id))
            .withColumn("load_batch", F.lit(args.batch_id))
        )
        out.write.mode("overwrite").partitionBy("load_batch").parquet(f"{BRONZE}/{entity}")
        counts[entity] = spark.read.parquet(f"{BRONZE}/{entity}/load_batch={args.batch_id}").count()
        log.info("bronze.%s rows=%s", entity, counts[entity])

    report = write_report(
        f"bronze_{args.batch_id}",
        {"batch_id": args.batch_id, "start_date": args.start_date, "end_date": args.end_date,
         "row_counts": counts},
    )
    log.info("Bronze ingest finished, report: %s", report)
    spark.stop()


if __name__ == "__main__":
    main()
