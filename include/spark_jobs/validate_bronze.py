"""VALIDATE: checks on the Bronze batch before anything is transformed.

Hard failures (stop the pipeline):
  * a required column is missing
  * orders or order_items is empty
  * more than --max-invalid-pct of an entity's rows break a row rule

Row rules (bad rows are copied to lake/quarantine/<entity>/load_batch=<id>/ with a reason):
  * primary-key / mandatory columns are null or blank
  * numeric columns that do not parse, or are negative
  * timestamp columns that do not parse
  * review_score outside 1..5

Duplicates on the primary key are reported as warnings (they are removed in silver).
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from common import (
    BRONZE,
    ENTITIES,
    QUARANTINE,
    get_logger,
    get_spark,
    to_double,
    to_int,
    to_ts,
    write_report,
)

log = get_logger("validate_bronze")

NOT_NULL = {
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
    "order_items": ["order_id", "order_item_id", "product_id", "seller_id", "price"],
    "order_payments": ["order_id", "payment_sequential", "payment_type", "payment_value"],
    "order_reviews": ["review_id", "order_id", "review_score"],
    "customers": ["customer_id", "customer_unique_id", "customer_zip_code_prefix"],
    "products": ["product_id"],
    "sellers": ["seller_id", "seller_zip_code_prefix"],
    "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
    "category_translation": ["product_category_name", "product_category_name_english"],
}
NUMERIC = {
    "order_items": ["order_item_id", "price", "freight_value"],
    "order_payments": ["payment_sequential", "payment_installments", "payment_value"],
    "order_reviews": ["review_score"],
    "products": [
        "product_name_lenght", "product_description_lenght", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    ],
}
SIGNED_NUMERIC = {"geolocation": ["geolocation_lat", "geolocation_lng"]}
TIMESTAMPS = {
    "orders": [
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
    ],
    "order_items": ["shipping_limit_date"],
    "order_reviews": ["review_creation_date", "review_answer_timestamp"],
}
MUST_NOT_BE_EMPTY = {"orders", "order_items"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--max-invalid-pct", type=float, default=5.0)
    args = parser.parse_args()

    spark = get_spark(f"validate_bronze_{args.batch_id}")
    results, failures = {}, []

    for entity, meta in ENTITIES.items():
        df = spark.read.parquet(f"{BRONZE}/{entity}/load_batch={args.batch_id}")
        total = df.count()
        missing = [c for c in meta["required"] if c not in df.columns]
        if missing:
            failures.append(f"{entity}: missing columns {missing}")
            results[entity] = {"rows": total, "missing_columns": missing}
            continue
        if total == 0 and entity in MUST_NOT_BE_EMPTY:
            failures.append(f"{entity}: no rows in batch")

        reasons = []
        for c in NOT_NULL.get(entity, []):
            reasons.append(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), F.lit(f"{c} is null")))
        for c in NUMERIC.get(entity, []):
            reasons.append(F.when(
                F.col(c).isNotNull() & (to_double(F.col(c)).isNull() | (to_double(F.col(c)) < 0)),
                F.lit(f"{c} not a valid number"),
            ))
        for c in SIGNED_NUMERIC.get(entity, []):
            reasons.append(F.when(
                F.col(c).isNotNull() & to_double(F.col(c)).isNull(), F.lit(f"{c} not a number")
            ))
        for c in TIMESTAMPS.get(entity, []):
            reasons.append(F.when(
                F.col(c).isNotNull() & (F.trim(F.col(c)) != "") & to_ts(F.col(c)).isNull(),
                F.lit(f"{c} bad timestamp"),
            ))
        if entity == "order_reviews":
            reasons.append(F.when(
                ~F.coalesce(to_int(F.col("review_score")).between(1, 5), F.lit(False)),
                F.lit("review_score outside 1..5")
            ))

        checked = df.withColumn("_reject_reason", F.concat_ws("; ", *reasons)) if reasons else \
            df.withColumn("_reject_reason", F.lit(""))
        bad = checked.where(F.col("_reject_reason") != "")
        bad_count = bad.count()
        if bad_count:
            bad.write.mode("overwrite").parquet(f"{QUARANTINE}/{entity}/load_batch={args.batch_id}")

        dup_count = 0
        if meta["pk"]:
            dup_count = total - df.dropDuplicates(meta["pk"]).count()

        pct = round(100.0 * bad_count / total, 3) if total else 0.0
        results[entity] = {"rows": total, "invalid_rows": bad_count, "invalid_pct": pct,
                           "duplicate_pk_rows": dup_count}
        log.info("validate.%s rows=%s invalid=%s (%.3f%%) duplicate_pk=%s",
                 entity, total, bad_count, pct, dup_count)
        if pct > args.max_invalid_pct:
            failures.append(f"{entity}: {pct}% invalid rows > {args.max_invalid_pct}%")

    report = write_report(f"validate_{args.batch_id}",
                          {"batch_id": args.batch_id, "results": results, "failures": failures})
    spark.stop()
    if failures:
        log.error("Validation FAILED: %s (report %s)", failures, report)
        sys.exit(1)
    log.info("Validation passed, report: %s", report)


if __name__ == "__main__":
    main()
