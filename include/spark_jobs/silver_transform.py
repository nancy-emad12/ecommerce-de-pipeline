"""TRANSFORM: Bronze -> Silver (clean, typed, de-duplicated).

What happens per entity:
  * types: strings -> int / decimal / timestamp
  * text: trimmed, lower-case, accents removed for cities; states upper-case
  * zip prefixes kept as 5-character strings (leading zeros)
  * invalid rows (same rules as validate_bronze) are dropped
  * exact duplicates and primary-key duplicates removed (latest ingested row wins)
  * products: source typos fixed (lenght -> length), English category added
      - category missing in translation table -> Portuguese name is kept
      - category null -> 'unknown'
  * geolocation: 1M rows -> one row per zip prefix
      - points outside Brazil's bounding box are dropped
      - lat/lng = average of the remaining points, city/state = most frequent value

Silver path: lake/silver/<entity>/ (overwritten by the initial load)
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import (
    BRONZE,
    SILVER,
    get_logger,
    get_spark,
    normalize_text,
    null_if_blank,
    to_decimal,
    to_double,
    to_int,
    to_ts,
    write_parquet,
    write_report,
    zip_prefix,
)

log = get_logger("silver_transform")

# Brazil bounding box (generous) used to drop wrong coordinates.
LAT_MIN, LAT_MAX, LNG_MIN, LNG_MAX = -34.0, 5.5, -74.5, -28.5


def latest_per_key(df: DataFrame, keys: list[str], order_col: str = "_ingested_at") -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def clean_orders(df: DataFrame) -> DataFrame:
    out = df.select(
        null_if_blank(F.col("order_id")).alias("order_id"),
        null_if_blank(F.col("customer_id")).alias("customer_id"),
        F.lower(null_if_blank(F.col("order_status"))).alias("order_status"),
        to_ts(F.col("order_purchase_timestamp")).alias("order_purchase_ts"),
        to_ts(F.col("order_approved_at")).alias("order_approved_ts"),
        to_ts(F.col("order_delivered_carrier_date")).alias("order_delivered_carrier_ts"),
        to_ts(F.col("order_delivered_customer_date")).alias("order_delivered_customer_ts"),
        to_ts(F.col("order_estimated_delivery_date")).alias("order_estimated_delivery_ts"),
        "_ingested_at",
    ).dropna(subset=["order_id", "customer_id", "order_status", "order_purchase_ts"])
    return latest_per_key(out, ["order_id"])


def clean_order_items(df: DataFrame) -> DataFrame:
    out = df.select(
        null_if_blank(F.col("order_id")).alias("order_id"),
        to_int(F.col("order_item_id")).alias("order_item_id"),
        null_if_blank(F.col("product_id")).alias("product_id"),
        null_if_blank(F.col("seller_id")).alias("seller_id"),
        to_ts(F.col("shipping_limit_date")).alias("shipping_limit_ts"),
        to_decimal(F.col("price")).alias("price"),
        F.coalesce(to_decimal(F.col("freight_value")), F.lit(0).cast("decimal(10,2)"))
        .alias("freight_value"),
        "_ingested_at",
    ).dropna(subset=["order_id", "order_item_id", "product_id", "seller_id", "price"])
    out = out.where((F.col("price") >= 0) & (F.col("freight_value") >= 0))
    return latest_per_key(out, ["order_id", "order_item_id"])


def clean_order_payments(df: DataFrame) -> DataFrame:
    out = df.select(
        null_if_blank(F.col("order_id")).alias("order_id"),
        to_int(F.col("payment_sequential")).alias("payment_sequential"),
        F.lower(null_if_blank(F.col("payment_type"))).alias("payment_type"),
        F.coalesce(to_int(F.col("payment_installments")), F.lit(1)).alias("payment_installments"),
        to_decimal(F.col("payment_value")).alias("payment_value"),
        "_ingested_at",
    ).dropna(subset=["order_id", "payment_sequential", "payment_type", "payment_value"])
    out = out.where(F.col("payment_value") >= 0)
    return latest_per_key(out, ["order_id", "payment_sequential"])


def clean_order_reviews(df: DataFrame) -> DataFrame:
    out = df.select(
        null_if_blank(F.col("review_id")).alias("review_id"),
        null_if_blank(F.col("order_id")).alias("order_id"),
        to_int(F.col("review_score")).alias("review_score"),
        F.substring(null_if_blank(F.col("review_comment_title")), 1, 30).alias("review_comment_title"),
        F.substring(
            F.regexp_replace(null_if_blank(F.col("review_comment_message")), r"[\r\n]+", " "), 1, 255
        ).alias("review_comment_message"),
        to_ts(F.col("review_creation_date")).alias("review_creation_ts"),
        to_ts(F.col("review_answer_timestamp")).alias("review_answer_ts"),
        "_ingested_at",
    ).dropna(subset=["review_id", "order_id", "review_score"])
    out = out.where(F.col("review_score").between(1, 5))
    return latest_per_key(out, ["review_id", "order_id"])


def clean_customers(df: DataFrame) -> DataFrame:
    out = df.select(
        null_if_blank(F.col("customer_id")).alias("customer_id"),
        null_if_blank(F.col("customer_unique_id")).alias("customer_unique_id"),
        zip_prefix(F.col("customer_zip_code_prefix")).alias("customer_zip_code_prefix"),
        normalize_text(F.col("customer_city")).alias("customer_city"),
        F.upper(null_if_blank(F.col("customer_state"))).alias("customer_state"),
        "_ingested_at",
    ).dropna(subset=["customer_id", "customer_unique_id"])
    return latest_per_key(out, ["customer_id"])


def clean_sellers(df: DataFrame) -> DataFrame:
    out = df.select(
        null_if_blank(F.col("seller_id")).alias("seller_id"),
        zip_prefix(F.col("seller_zip_code_prefix")).alias("seller_zip_code_prefix"),
        normalize_text(F.col("seller_city")).alias("seller_city"),
        F.upper(null_if_blank(F.col("seller_state"))).alias("seller_state"),
        "_ingested_at",
    ).dropna(subset=["seller_id"])
    return latest_per_key(out, ["seller_id"])


def clean_products(df: DataFrame, translation: DataFrame) -> DataFrame:
    tr = translation.select(
        F.lower(null_if_blank(F.col("product_category_name"))).alias("product_category_name"),
        F.lower(null_if_blank(F.col("product_category_name_english"))).alias("category_en"),
    ).dropDuplicates(["product_category_name"])

    out = df.select(
        null_if_blank(F.col("product_id")).alias("product_id"),
        F.lower(null_if_blank(F.col("product_category_name"))).alias("product_category_name"),
        to_int(F.col("product_name_lenght")).alias("product_name_length"),
        to_int(F.col("product_description_lenght")).alias("product_description_length"),
        to_int(F.col("product_photos_qty")).alias("product_photos_qty"),
        to_int(F.col("product_weight_g")).alias("product_weight_g"),
        to_int(F.col("product_length_cm")).alias("product_length_cm"),
        to_int(F.col("product_height_cm")).alias("product_height_cm"),
        to_int(F.col("product_width_cm")).alias("product_width_cm"),
        "_ingested_at",
    ).dropna(subset=["product_id"])
    out = latest_per_key(out, ["product_id"])

    out = out.join(F.broadcast(tr), "product_category_name", "left")
    return out.select(
        "product_id",
        F.coalesce("product_category_name", F.lit("unknown")).alias("product_category_name"),
        F.coalesce("category_en", "product_category_name", F.lit("unknown"))
        .alias("product_category_name_english"),
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
        "_ingested_at",
    )


def clean_geolocation(df: DataFrame) -> DataFrame:
    pts = df.select(
        zip_prefix(F.col("geolocation_zip_code_prefix")).alias("zip"),
        to_double(F.col("geolocation_lat")).alias("lat"),
        to_double(F.col("geolocation_lng")).alias("lng"),
        normalize_text(F.col("geolocation_city")).alias("city"),
        F.upper(null_if_blank(F.col("geolocation_state"))).alias("state"),
    ).dropna(subset=["zip", "lat", "lng"]).dropDuplicates()
    pts = pts.where(F.col("lat").between(LAT_MIN, LAT_MAX) & F.col("lng").between(LNG_MIN, LNG_MAX))

    coords = pts.groupBy("zip").agg(
        F.round(F.avg("lat"), 6).alias("geolocation_lat"),
        F.round(F.avg("lng"), 6).alias("geolocation_lng"),
    )

    def most_frequent(col: str, alias: str) -> DataFrame:
        counts = pts.where(F.col(col).isNotNull()).groupBy("zip", col).count()
        w = Window.partitionBy("zip").orderBy(F.col("count").desc(), F.col(col))
        return (counts.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
                .select("zip", F.col(col).alias(alias)))

    return (
        coords.join(most_frequent("city", "geolocation_city"), "zip", "left")
        .join(most_frequent("state", "geolocation_state"), "zip", "left")
        .withColumnRenamed("zip", "geolocation_zip_code_prefix")
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    spark = get_spark(f"silver_transform_{args.batch_id}")

    def bronze(entity: str) -> DataFrame:
        return spark.read.parquet(f"{BRONZE}/{entity}/load_batch={args.batch_id}")

    outputs = {
        "orders": clean_orders(bronze("orders")),
        "order_items": clean_order_items(bronze("order_items")),
        "order_payments": clean_order_payments(bronze("order_payments")),
        "order_reviews": clean_order_reviews(bronze("order_reviews")),
        "customers": clean_customers(bronze("customers")),
        "sellers": clean_sellers(bronze("sellers")),
        "products": clean_products(bronze("products"), bronze("category_translation")),
        "geolocation": clean_geolocation(bronze("geolocation")),
    }

    counts = {}
    for entity, df in outputs.items():
        df = df.drop("_ingested_at") if "_ingested_at" in df.columns else df
        counts[entity] = write_parquet(df.withColumn("_batch_id", F.lit(args.batch_id)),
                                       f"{SILVER}/{entity}")
        log.info("silver.%s rows=%s", entity, counts[entity])

    report = write_report(f"silver_{args.batch_id}", {"batch_id": args.batch_id, "row_counts": counts})
    log.info("Silver transform finished, report: %s", report)
    spark.stop()


if __name__ == "__main__":
    main()
