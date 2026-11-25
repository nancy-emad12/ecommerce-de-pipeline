-- Initial (full) load only: empty every DWH table so the load can be re-run safely.
-- Incremental loads must NOT run this file.
TRUNCATE TABLE
    dwh.fact_sales,
    dwh.fact_payments,
    dwh.dim_order_reviews,
    dwh.dim_score,
    dwh.dim_payment_type,
    dwh.dim_order_status,
    dwh.dim_geolocation,
    dwh.dim_product,
    dwh.dim_seller,
    dwh.dim_customer,
    dwh.dim_date;
