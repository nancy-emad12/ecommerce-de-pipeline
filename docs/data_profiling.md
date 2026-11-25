# Data profiling: Olist dataset

Profiled on 2026-11-25 on the full dataset (`archive.zip`, 9 CSV files).

| File | Rows | Notes |
|---|---|---|
| olist_orders_dataset | 99,441 | 2016-09-04 → 2018-10-17. Nulls: approved_at 160, delivered_carrier 1,783, delivered_customer 2,965. 775 orders have no items |
| olist_order_items_dataset | 112,650 | up to 21 items per order; shipping_limit_date reaches 2020-04-09 |
| olist_order_payments_dataset | 103,886 | 2,961 orders have several payment rows; types credit_card, boleto, voucher, debit_card, not_defined (3); 1 order without payment |
| olist_order_reviews_dataset | 104,719 (99,224 reviews) | text fields contain line breaks (multi-line CSV); 814 duplicate review_id; some orders have >1 review |
| olist_customers_dataset | 99,441 | `customer_id` is per order; 96,096 real customers (`customer_unique_id`); 278 zip prefixes missing from geolocation |
| olist_products_dataset | 32,951 | 610 null categories; column typos `product_name_lenght`, `product_description_lenght` |
| olist_sellers_dataset | 3,095 | |
| olist_geolocation_dataset | 1,000,163 | only 19,015 zip prefixes, 261,831 exact duplicates, many coordinates per zip, a few points outside Brazil |
| product_category_name_translation | 71 | 2 categories missing: `pc_gamer`, `portateis_cozinha_e_preparadores_de_alimentos`; file starts with a UTF-8 BOM |

Order status: delivered 96,478 · shipped 1,107 · canceled 625 · unavailable 609 · invoiced 314 · processing 301 · created 5 · approved 2.

Orders per month are very low in 2016-09/10/12 and 2018-09/10 (end of the snapshot).

## Initial load result (end_date = 2017-12-31)

| Table | Rows |
|---|---|
| fact_sales | 51,234 |
| fact_payments | 47,871 |
| dim_customer | 44,035 (+ unknown) |
| dim_product | 17,452 |
| dim_seller | 1,812 |
| dim_geolocation | 19,012 |
| dim_order_reviews | 45,039 |
| dim_date | 2,193 (2016-01-01 → 2021-12-31 + unknown) |

Sum of `allocated_payment_value` = sum of payments of those orders = 7,201,192.15 (exact match).
0.267% of sales rows have an unknown customer location (zip missing from geolocation).
