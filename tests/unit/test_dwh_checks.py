"""Unit tests for the data-quality rules (no database needed)."""

from decimal import Decimal

from include.dq.dwh_checks import DIMENSIONS, build_checks, run_checks

EXPECTED = {
    "silver_order_items": 10,
    "silver_payments": 8,
    "sum_price": "100.00",
    "sum_payment_value_orders_with_items": "120.50",
}


def fake_db(answers: dict):
    def fetch_one(sql: str):
        for needle, value in answers.items():
            if needle in sql:
                return value
        return 0
    return fetch_one


def test_all_checks_pass_on_a_consistent_load():
    answers = {f"FROM dwh.{t} WHERE {k} = -1": 1 for t, k in DIMENSIONS.items()}
    answers.update({
        "SELECT COUNT(*) FROM dwh.dim_": 50,
        "SELECT COUNT(*) FROM dwh.fact_sales WHERE": 0,
        "SELECT COUNT(*) FROM dwh.fact_payments WHERE": 0,
        "SELECT COUNT(*) FROM dwh.fact_sales": 10,
        "SELECT COUNT(*) FROM dwh.fact_payments": 8,
        "SUM(price)": Decimal("100.00"),
        "SUM(allocated_payment_value)": Decimal("120.50"),
    })
    results = run_checks(fake_db(answers), EXPECTED)
    failed = [r["check"] for r in results if not r["passed"]]
    assert failed == []


def test_row_count_mismatch_is_caught():
    results = run_checks(fake_db({"SELECT COUNT(*) FROM dwh.fact_sales": 9}), EXPECTED)
    by_name = {r["check"]: r for r in results}
    assert by_name["fact_sales: row count = silver order items"]["passed"] is False


def test_every_dimension_has_an_unknown_member_check():
    names = [c.name for c in build_checks(EXPECTED)]
    assert sum("unknown member" in n for n in names) == 9
