"""QUALITY CHECK: data-quality rules run against the DWH after a load.

Every check is one SQL query returning a single number, plus a rule for that number.
`expected` comes from the gold job's report (lake/_reports/gold_<batch>.json), so the
DWH is reconciled against what Spark produced.

Used by the Airflow DAG (PostgresHook) and runnable on its own with any DB-API cursor.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

DIMENSIONS = {
    "dim_date": "date_key",
    "dim_customer": "customer_key",
    "dim_seller": "seller_key",
    "dim_product": "product_key",
    "dim_geolocation": "geolocation_key",
    "dim_order_status": "order_status_key",
    "dim_payment_type": "payment_type_key",
    "dim_score": "score_key",
    "dim_order_reviews": "order_review_key",
}


@dataclass
class Check:
    name: str
    sql: str
    rule: str  # human readable
    passes: Callable[[Decimal], bool]


def build_checks(expected: dict, schema: str = "dwh") -> list[Check]:
    s = schema
    checks: list[Check] = []

    for table, key in DIMENSIONS.items():
        checks.append(Check(f"{table}: has rows", f"SELECT COUNT(*) FROM {s}.{table}",
                            "> 1", lambda v: v > 1))
        checks.append(Check(f"{table}: unknown member (-1) exists",
                            f"SELECT COUNT(*) FROM {s}.{table} WHERE {key} = -1",
                            "= 1", lambda v: v == 1))

    items, pays = expected["silver_order_items"], expected["silver_payments"]
    checks += [
        Check("fact_sales: row count = silver order items",
              f"SELECT COUNT(*) FROM {s}.fact_sales", f"= {items}", lambda v: v == items),
        Check("fact_payments: row count = silver payments",
              f"SELECT COUNT(*) FROM {s}.fact_payments", f"= {pays}", lambda v: v == pays),
    ]

    sum_price = Decimal(expected["sum_price"])
    sum_paid = Decimal(expected["sum_payment_value_orders_with_items"])
    checks += [
        Check("fact_sales: SUM(price) matches gold",
              f"SELECT COALESCE(SUM(price), 0) FROM {s}.fact_sales",
              f"= {sum_price}", lambda v: abs(Decimal(v) - sum_price) <= Decimal("0.01")),
        Check("fact_sales: SUM(allocated_payment_value) = payments of those orders",
              f"SELECT COALESCE(SUM(allocated_payment_value), 0) FROM {s}.fact_sales",
              f"= {sum_paid}", lambda v: abs(Decimal(v) - sum_paid) <= Decimal("0.01")),
        Check("fact_sales: no negative amounts",
              f"SELECT COUNT(*) FROM {s}.fact_sales WHERE price < 0 OR freight_value < 0 "
              f"OR allocated_payment_value < 0", "= 0", lambda v: v == 0),
        Check("fact_payments: no negative amounts",
              f"SELECT COUNT(*) FROM {s}.fact_payments WHERE payment_value < 0", "= 0",
              lambda v: v == 0),
        Check("fact_sales: purchase date always known",
              f"SELECT COUNT(*) FROM {s}.fact_sales WHERE purchase_date_key = -1", "= 0",
              lambda v: v == 0),
        Check("fact_sales: product / seller / customer always known",
              f"SELECT COUNT(*) FROM {s}.fact_sales WHERE product_key = -1 OR seller_key = -1 "
              f"OR customer_key = -1", "= 0", lambda v: v == 0),
        Check("fact_payments: payment type always known",
              f"SELECT COUNT(*) FROM {s}.fact_payments WHERE payment_type_key = -1", "= 0",
              lambda v: v == 0),
        Check("fact_sales: % rows with unknown customer location",
              f"SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE customer_geolocation_key = -1) "
              f"/ NULLIF(COUNT(*), 0), 3) FROM {s}.fact_sales",
              "< 1%", lambda v: Decimal(v or 0) < 1),
        Check("fact_sales: delivered before purchase (impossible dates)",
              f"SELECT COUNT(*) FROM {s}.fact_sales WHERE delivered_customer_date_key <> -1 "
              f"AND delivered_customer_date_key < purchase_date_key", "= 0", lambda v: v == 0),
        Check("orders paid with more than one payment type are kept",
              f"SELECT COUNT(*) FROM (SELECT order_id FROM {s}.fact_payments GROUP BY order_id "
              f"HAVING COUNT(DISTINCT payment_type_key) > 1) x",
              ">= 0 (informational)", lambda v: v >= 0),
    ]
    return checks


def run_checks(fetch_one: Callable[[str], object], expected: dict, schema: str = "dwh") -> list[dict]:
    """fetch_one(sql) must return the first column of the first row."""
    results = []
    for check in build_checks(expected, schema):
        value = fetch_one(check.sql)
        value = Decimal(str(value)) if value is not None else Decimal("0")
        results.append({"check": check.name, "value": str(value), "rule": check.rule,
                        "passed": bool(check.passes(value))})
    return results
