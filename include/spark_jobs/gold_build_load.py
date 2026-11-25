"""LOAD: Silver -> Gold star schema -> Postgres DWH.

Builds every dimension and both facts from silver, writes them to lake/gold/<table>
(parquet) and, with --sink jdbc, appends them into the dwh schema in Postgres
(dimensions first, then facts, so foreign keys are satisfied).

Business rules (agreed with the team, see docs/decisions.md):
  * Fact_Sales grain = one order item. Fact_Payments grain = one payment row.
  * allocated_payment_value = order's total payments split across its items by
    (price + freight) share; rounding remainder goes to the last item so the
    allocated values add up exactly to what the customer paid.
  * One review per order in the star: the latest answered review.
  * Dim_Customer is keyed on customer_unique_id (the person); attributes come from
    the person's most recent order.
  * Missing references / dates point to the Unknown member (key -1).

Surrogate keys are generated here for the initial load (dense, ordered by the natural
key). Incremental loads will look up existing keys instead of regenerating them.
"""

from __future__ import annotations

import argparse
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import (
    DWH_JDBC_URL,
    DWH_PASSWORD,
    DWH_SCHEMA,
    DWH_USER,
    GOLD,
    SILVER,
    UNKNOWN_KEY,
    date_key,
    get_logger,
    get_spark,
    write_report,
)

log = get_logger("gold_build_load")

STATUS_ORDER = ["created", "approved", "invoiced", "processing", "shipped",
                "delivered", "canceled", "unavailable"]

# Load order matters for the foreign keys.
DIM_TABLES = ["dim_date", "dim_order_status", "dim_payment_type", "dim_score",
              "dim_geolocation", "dim_customer", "dim_seller", "dim_product",
              "dim_order_reviews"]
FACT_TABLES = ["fact_sales", "fact_payments"]


# ----------------------------------------------------------------- helpers
def with_surrogate_key(df: DataFrame, key: str, order_by: str) -> DataFrame:
    w = Window.orderBy(F.col(order_by))
    return df.withColumn(key, F.row_number().over(w).cast("int"))


def add_unknown(spark: SparkSession, df: DataFrame, values: dict) -> DataFrame:
    """Append the Unknown member row (columns not given are null)."""
    row = {c: values.get(c) for c in df.columns}
    unknown = spark.createDataFrame([row], schema=df.schema)
    return df.unionByName(unknown)


def lookup(df: DataFrame, dim: DataFrame, on_left: str, on_dim: str, key: str,
           alias: str | None = None) -> DataFrame:
    """Left join a surrogate key; missing -> Unknown (-1)."""
    alias = alias or key
    d = dim.select(F.col(on_dim).alias("_nk"), F.col(key).alias(alias))
    return (df.join(F.broadcast(d), df[on_left] == d["_nk"], "left").drop("_nk")
            .withColumn(alias, F.coalesce(F.col(alias), F.lit(UNKNOWN_KEY))))


# -------------------------------------------------------------- dimensions
def build_dim_date(spark: SparkSession, silver: dict[str, DataFrame]) -> DataFrame:
    dates = (
        silver["orders"].select(F.min("order_purchase_ts").alias("lo"),
                                F.max(F.greatest("order_estimated_delivery_ts",
                                                 "order_delivered_customer_ts")).alias("hi"))
        .union(silver["order_items"].select(F.min("shipping_limit_ts"), F.max("shipping_limit_ts")))
        .union(silver["order_reviews"].select(F.min("review_creation_ts"), F.max("review_answer_ts")))
        .agg(F.min("lo").alias("lo"), F.max("hi").alias("hi")).first()
    )
    start = f"{dates['lo'].year}-01-01"
    end = f"{dates['hi'].year + 1}-12-31"  # one spare year for future estimated dates
    cal = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS full_date"
    )
    cal = cal.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").cast("short").alias("year"),
        F.quarter("full_date").cast("short").alias("quarter"),
        F.month("full_date").cast("short").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").cast("short").alias("day"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.when(F.dayofweek("full_date").isin(1, 7), 1).otherwise(0).cast("short").alias("is_weekend"),
    )
    import datetime as dt
    return add_unknown(spark, cal, {
        "date_key": UNKNOWN_KEY, "full_date": dt.date(1900, 1, 1), "year": 1900, "quarter": 0,
        "month": 0, "month_name": "Unknown", "day": 0, "day_name": "Unknown", "is_weekend": 0,
    })


def build_small_dim(spark, values: list[str], key: str, name: str) -> DataFrame:
    rows = [(i + 1, v) for i, v in enumerate(values)] + [(UNKNOWN_KEY, "unknown")]
    schema = T.StructType([T.StructField(key, T.ShortType(), False),
                           T.StructField(name, T.StringType(), False)])
    return spark.createDataFrame(rows, schema)


def build_dim_order_status(spark, orders: DataFrame) -> DataFrame:
    found = [r[0] for r in orders.select("order_status").distinct().collect()]
    ordered = [s for s in STATUS_ORDER if s in found] + sorted(s for s in found if s not in STATUS_ORDER)
    return build_small_dim(spark, ordered, "order_status_key", "order_status")


def build_dim_payment_type(spark, payments: DataFrame) -> DataFrame:
    found = sorted(r[0] for r in payments.select("payment_type").distinct().collect())
    return build_small_dim(spark, found, "payment_type_key", "payment_type")


def build_dim_score(spark) -> DataFrame:
    rows = [(s, s) for s in range(1, 6)] + [(UNKNOWN_KEY, 0)]
    schema = T.StructType([T.StructField("score_key", T.ShortType(), False),
                           T.StructField("review_score", T.ShortType(), False)])
    return spark.createDataFrame(rows, schema)


def build_dim_geolocation(spark, geo: DataFrame) -> DataFrame:
    dim = with_surrogate_key(geo, "geolocation_key", "geolocation_zip_code_prefix").select(
        "geolocation_key", "geolocation_zip_code_prefix",
        F.col("geolocation_lat").cast("decimal(9,6)").alias("geolocation_lat"),
        F.col("geolocation_lng").cast("decimal(9,6)").alias("geolocation_lng"),
        "geolocation_city", "geolocation_state",
    )
    return add_unknown(spark, dim, {"geolocation_key": UNKNOWN_KEY,
                                    "geolocation_zip_code_prefix": "-----",
                                    "geolocation_city": "unknown"})


def build_dim_customer(spark, customers: DataFrame, orders: DataFrame) -> DataFrame:
    last_order = orders.groupBy("customer_id").agg(F.max("order_purchase_ts").alias("_last"))
    w = Window.partitionBy("customer_unique_id").orderBy(F.col("_last").desc_nulls_last(),
                                                         F.col("customer_id"))
    latest = (customers.join(last_order, "customer_id", "left")
              .withColumn("_rn", F.row_number().over(w)).where("_rn = 1"))
    dim = with_surrogate_key(latest, "customer_key", "customer_unique_id").select(
        "customer_key", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state",
    )
    return add_unknown(spark, dim, {"customer_key": UNKNOWN_KEY, "customer_unique_id": "UNKNOWN",
                                    "customer_city": "unknown"})


def build_dim_seller(spark, sellers: DataFrame) -> DataFrame:
    dim = with_surrogate_key(sellers, "seller_key", "seller_id").select(
        "seller_key", "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    return add_unknown(spark, dim, {"seller_key": UNKNOWN_KEY, "seller_id": "UNKNOWN",
                                    "seller_city": "unknown"})


def build_dim_product(spark, products: DataFrame) -> DataFrame:
    dim = with_surrogate_key(products, "product_key", "product_id").select(
        "product_key", "product_id", "product_category_name", "product_category_name_english",
        *[F.col(c).cast("short").alias(c) for c in (
            "product_name_length", "product_description_length", "product_photos_qty")],
        "product_weight_g",
        *[F.col(c).cast("short").alias(c) for c in (
            "product_length_cm", "product_height_cm", "product_width_cm")],
    )
    return add_unknown(spark, dim, {"product_key": UNKNOWN_KEY, "product_id": "UNKNOWN",
                                    "product_category_name": "unknown",
                                    "product_category_name_english": "unknown"})


def latest_review_per_order(reviews: DataFrame) -> DataFrame:
    w = Window.partitionBy("order_id").orderBy(
        F.col("review_answer_ts").desc_nulls_last(), F.col("review_creation_ts").desc_nulls_last(),
        F.col("review_id"))
    return reviews.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def build_dim_order_reviews(spark, latest_reviews: DataFrame) -> DataFrame:
    dim = with_surrogate_key(latest_reviews, "order_review_key", "order_id").select(
        "order_review_key", "review_id", "order_id", "review_comment_title", "review_comment_message")
    return add_unknown(spark, dim, {"order_review_key": UNKNOWN_KEY, "order_id": "UNKNOWN"})


# ------------------------------------------------------------------- facts
def build_fact_sales(silver, dims, latest_reviews, batch_id: str) -> DataFrame:
    items, orders, customers = silver["order_items"], silver["orders"], silver["customers"]
    payments = silver["order_payments"]

    order_paid = payments.groupBy("order_id").agg(F.sum("payment_value").alias("_order_paid"))

    df = (items.join(orders, "order_id", "inner")
          .join(customers.select("customer_id", "customer_unique_id", "customer_zip_code_prefix"),
                "customer_id", "left")
          .join(silver["sellers"].select("seller_id", "seller_zip_code_prefix"), "seller_id", "left")
          .join(latest_reviews.select("order_id", "review_score", "review_creation_ts",
                                      "review_answer_ts"), "order_id", "left")
          .join(order_paid, "order_id", "left"))

    # ---- allocate the order's payments across its items
    order_w = Window.partitionBy("order_id")
    weight = F.col("price") + F.col("freight_value")
    df = df.withColumn("_w", weight).withColumn("_w_total", F.sum("_w").over(order_w))
    df = df.withColumn(
        "_alloc",
        F.when(F.col("_order_paid").isNull(), F.lit(None))
        .when(F.col("_w_total") == 0, F.col("_order_paid") / F.count("*").over(order_w))
        .otherwise(F.col("_order_paid") * F.col("_w") / F.col("_w_total")),
    ).withColumn("_alloc", F.round("_alloc", 2).cast("decimal(12,2)"))
    df = df.withColumn("_residual", F.col("_order_paid") - F.sum("_alloc").over(order_w))
    df = df.withColumn("_is_last", F.col("order_item_id") == F.max("order_item_id").over(order_w))
    df = df.withColumn(
        "allocated_payment_value",
        F.when(F.col("_is_last"), F.col("_alloc") + F.col("_residual")).otherwise(F.col("_alloc"))
        .cast("decimal(10,2)"),
    )

    # ---- surrogate keys
    df = lookup(df, dims["dim_customer"], "customer_unique_id", "customer_unique_id", "customer_key")
    df = lookup(df, dims["dim_product"], "product_id", "product_id", "product_key")
    df = lookup(df, dims["dim_seller"], "seller_id", "seller_id", "seller_key")
    df = lookup(df, dims["dim_order_status"], "order_status", "order_status", "order_status_key")
    df = lookup(df, dims["dim_score"], "review_score", "review_score", "score_key")
    df = lookup(df, dims["dim_order_reviews"], "order_id", "order_id", "order_review_key")
    geo = dims["dim_geolocation"]
    df = lookup(df, geo, "customer_zip_code_prefix", "geolocation_zip_code_prefix",
                "geolocation_key", "customer_geolocation_key")
    df = lookup(df, geo, "seller_zip_code_prefix", "geolocation_zip_code_prefix",
                "geolocation_key", "seller_geolocation_key")

    return df.select(
        "order_id",
        F.col("order_item_id").cast("short").alias("order_item_id"),
        date_key(F.col("order_purchase_ts")).alias("purchase_date_key"),
        date_key(F.col("order_approved_ts")).alias("approved_date_key"),
        date_key(F.col("order_delivered_carrier_ts")).alias("delivered_carrier_date_key"),
        date_key(F.col("order_delivered_customer_ts")).alias("delivered_customer_date_key"),
        date_key(F.col("order_estimated_delivery_ts")).alias("estimated_delivery_date_key"),
        date_key(F.col("shipping_limit_ts")).alias("shipping_limit_date_key"),
        date_key(F.col("review_creation_ts")).alias("review_created_date_key"),
        date_key(F.col("review_answer_ts")).alias("review_answered_date_key"),
        "customer_key",
        F.col("order_status_key").cast("short").alias("order_status_key"),
        F.col("score_key").cast("short").alias("score_key"),
        "product_key", "seller_key", "seller_geolocation_key", "customer_geolocation_key",
        "order_review_key", "price", "freight_value", "allocated_payment_value",
        F.lit(batch_id).alias("etl_batch_id"),
    )


def build_fact_payments(silver, dims, batch_id: str) -> DataFrame:
    df = (silver["order_payments"]
          .join(silver["orders"].select("order_id", "customer_id", "order_status",
                                        "order_purchase_ts", "order_approved_ts"), "order_id", "inner")
          .join(silver["customers"].select("customer_id", "customer_unique_id",
                                           "customer_zip_code_prefix"), "customer_id", "left"))
    df = lookup(df, dims["dim_payment_type"], "payment_type", "payment_type", "payment_type_key")
    df = lookup(df, dims["dim_customer"], "customer_unique_id", "customer_unique_id", "customer_key")
    df = lookup(df, dims["dim_order_status"], "order_status", "order_status", "order_status_key")
    df = lookup(df, dims["dim_geolocation"], "customer_zip_code_prefix",
                "geolocation_zip_code_prefix", "geolocation_key", "customer_geolocation_key")
    return df.select(
        "order_id",
        F.col("payment_sequential").cast("short").alias("payment_sequential"),
        F.col("payment_type_key").cast("short").alias("payment_type_key"),
        "customer_key", "customer_geolocation_key",
        F.col("order_status_key").cast("short").alias("order_status_key"),
        date_key(F.col("order_purchase_ts")).alias("purchase_date_key"),
        date_key(F.col("order_approved_ts")).alias("approved_date_key"),
        F.col("payment_installments").cast("short").alias("payment_installments"),
        F.col("payment_value").cast("decimal(10,2)").alias("payment_value"),
        F.lit(batch_id).alias("etl_batch_id"),
    )


# -------------------------------------------------------------------- load
def write_jdbc(df: DataFrame, table: str) -> None:
    url = DWH_JDBC_URL + ("&" if "?" in DWH_JDBC_URL else "?") + "reWriteBatchedInserts=true"
    (df.write.format("jdbc")
     .option("url", url)
     .option("dbtable", f"{DWH_SCHEMA}.{table}")
     .option("user", DWH_USER)
     .option("password", DWH_PASSWORD)
     .option("driver", "org.postgresql.Driver")
     .option("batchsize", 10000)
     .option("stringtype", "unspecified")
     .mode("append")
     .save())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--sink", choices=["jdbc", "parquet"], default="jdbc",
                        help="parquet = only write lake/gold (useful without a database)")
    args = parser.parse_args()

    spark = get_spark(f"gold_build_load_{args.batch_id}")
    names = ["orders", "order_items", "order_payments", "order_reviews", "customers",
             "sellers", "products", "geolocation"]
    silver = {n: spark.read.parquet(f"{SILVER}/{n}").drop("_batch_id").cache() for n in names}

    latest_reviews = latest_review_per_order(silver["order_reviews"]).cache()
    dims = {
        "dim_date": build_dim_date(spark, silver),
        "dim_order_status": build_dim_order_status(spark, silver["orders"]),
        "dim_payment_type": build_dim_payment_type(spark, silver["order_payments"]),
        "dim_score": build_dim_score(spark),
        "dim_geolocation": build_dim_geolocation(spark, silver["geolocation"]),
        "dim_customer": build_dim_customer(spark, silver["customers"], silver["orders"]),
        "dim_seller": build_dim_seller(spark, silver["sellers"]),
        "dim_product": build_dim_product(spark, silver["products"]),
        "dim_order_reviews": build_dim_order_reviews(spark, latest_reviews),
    }
    # Materialise each dim once so the facts and the load reuse the same keys.
    for name in DIM_TABLES:
        dims[name].write.mode("overwrite").parquet(f"{GOLD}/{name}")
        dims[name] = spark.read.parquet(f"{GOLD}/{name}").cache()

    facts = {
        "fact_sales": build_fact_sales(silver, dims, latest_reviews, args.batch_id),
        "fact_payments": build_fact_payments(silver, dims, args.batch_id),
    }
    for name in FACT_TABLES:
        facts[name].write.mode("overwrite").parquet(f"{GOLD}/{name}")
        facts[name] = spark.read.parquet(f"{GOLD}/{name}")

    tables = {**dims, **facts}
    counts = {name: tables[name].count() for name in DIM_TABLES + FACT_TABLES}
    for name, n in counts.items():
        log.info("gold.%s rows=%s", name, n)

    if args.sink == "jdbc":
        for name in DIM_TABLES + FACT_TABLES:
            log.info("loading dwh.%s ...", name)
            write_jdbc(tables[name], name)
        log.info("DWH load finished")

    def total(df: DataFrame, col: str) -> str:
        value = df.agg(F.sum(col)).first()[0]
        return str(value if value is not None else Decimal("0"))

    expected = {
        "silver_order_items": silver["order_items"].join(silver["orders"], "order_id", "left_semi").count(),
        "silver_payments": silver["order_payments"].join(silver["orders"], "order_id", "left_semi").count(),
        "sum_price": total(facts["fact_sales"], "price"),
        "sum_allocated_payment_value": total(facts["fact_sales"], "allocated_payment_value"),
        "sum_payment_value": total(facts["fact_payments"], "payment_value"),
        "sum_payment_value_orders_with_items": total(
            facts["fact_payments"].join(facts["fact_sales"].select("order_id").distinct(),
                                        "order_id", "left_semi"), "payment_value"),
    }
    report = write_report(f"gold_{args.batch_id}", {"batch_id": args.batch_id, "sink": args.sink,
                                                    "row_counts": counts, "expected": expected})
    log.info("Gold finished, report: %s", report)
    spark.stop()


if __name__ == "__main__":
    main()
