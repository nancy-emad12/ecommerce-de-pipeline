-- =====================================================================
-- Analytical views on top of the star schema (common questions).
-- "Revenue" views exclude canceled / unavailable orders.
-- CREATE OR REPLACE -> safe to run after every load.
-- =====================================================================

-- Daily sales: orders, items, product revenue, freight, GMV
CREATE OR REPLACE VIEW dwh.vw_sales_daily AS
SELECT d.full_date,
       d.year, d.month, d.month_name, d.day_name, d.is_weekend,
       COUNT(DISTINCT f.order_id)                   AS orders,
       COUNT(*)                                     AS items,
       SUM(f.price)                                 AS product_revenue,
       SUM(f.freight_value)                         AS freight_revenue,
       SUM(f.price + f.freight_value)               AS gmv,
       ROUND(SUM(f.price + f.freight_value) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS avg_order_value
FROM dwh.fact_sales f
JOIN dwh.dim_date d          ON d.date_key = f.purchase_date_key
JOIN dwh.dim_order_status s  ON s.order_status_key = f.order_status_key
WHERE s.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.full_date, d.year, d.month, d.month_name, d.day_name, d.is_weekend;

-- Monthly revenue by product category
CREATE OR REPLACE VIEW dwh.vw_sales_by_category_month AS
SELECT d.year, d.month,
       p.product_category_name_english      AS category,
       COUNT(DISTINCT f.order_id)           AS orders,
       COUNT(*)                             AS items,
       SUM(f.price)                         AS product_revenue,
       ROUND(AVG(NULLIF(sc.review_score, 0)), 2) AS avg_review_score
FROM dwh.fact_sales f
JOIN dwh.dim_date d          ON d.date_key = f.purchase_date_key
JOIN dwh.dim_product p       ON p.product_key = f.product_key
JOIN dwh.dim_score sc        ON sc.score_key = f.score_key
JOIN dwh.dim_order_status s  ON s.order_status_key = f.order_status_key
WHERE s.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.year, d.month, p.product_category_name_english;

-- Delivery performance per order (delivered orders only)
CREATE OR REPLACE VIEW dwh.vw_delivery_performance AS
SELECT f.order_id,
       c.customer_state,
       dp.full_date                                   AS purchase_date,
       dd.full_date                                   AS delivered_date,
       de.full_date                                   AS estimated_date,
       (dd.full_date - dp.full_date)                  AS delivery_days,
       (dd.full_date - de.full_date)                  AS days_vs_estimate,
       CASE WHEN dd.full_date > de.full_date THEN 1 ELSE 0 END AS is_late,
       MAX(NULLIF(sc.review_score, 0))                AS review_score
FROM dwh.fact_sales f
JOIN dwh.dim_customer c ON c.customer_key = f.customer_key
JOIN dwh.dim_date dp    ON dp.date_key = f.purchase_date_key
JOIN dwh.dim_date dd    ON dd.date_key = f.delivered_customer_date_key
JOIN dwh.dim_date de    ON de.date_key = f.estimated_delivery_date_key
JOIN dwh.dim_score sc   ON sc.score_key = f.score_key
WHERE f.delivered_customer_date_key <> -1
  AND f.estimated_delivery_date_key <> -1
GROUP BY f.order_id, c.customer_state, dp.full_date, dd.full_date, de.full_date;

-- Payment mix: every payment type of every order is kept (fact_payments)
CREATE OR REPLACE VIEW dwh.vw_payment_mix_month AS
SELECT d.year, d.month,
       pt.payment_type,
       COUNT(*)                         AS payments,
       COUNT(DISTINCT fp.order_id)      AS orders,
       SUM(fp.payment_value)            AS payment_value,
       ROUND(AVG(fp.payment_installments), 2) AS avg_installments
FROM dwh.fact_payments fp
JOIN dwh.dim_date d          ON d.date_key = fp.purchase_date_key
JOIN dwh.dim_payment_type pt ON pt.payment_type_key = fp.payment_type_key
GROUP BY d.year, d.month, pt.payment_type;

-- Sales by customer state
CREATE OR REPLACE VIEW dwh.vw_sales_by_state AS
SELECT c.customer_state,
       COUNT(DISTINCT f.customer_key)   AS customers,
       COUNT(DISTINCT f.order_id)       AS orders,
       SUM(f.price + f.freight_value)   AS gmv,
       ROUND(AVG(f.freight_value), 2)   AS avg_freight
FROM dwh.fact_sales f
JOIN dwh.dim_customer c      ON c.customer_key = f.customer_key
JOIN dwh.dim_order_status s  ON s.order_status_key = f.order_status_key
WHERE s.order_status NOT IN ('canceled', 'unavailable')
GROUP BY c.customer_state;

-- Seller performance
CREATE OR REPLACE VIEW dwh.vw_seller_performance AS
SELECT se.seller_id, se.seller_city, se.seller_state,
       COUNT(DISTINCT f.order_id)               AS orders,
       SUM(f.price)                             AS product_revenue,
       ROUND(AVG(NULLIF(sc.review_score, 0)), 2) AS avg_review_score
FROM dwh.fact_sales f
JOIN dwh.dim_seller se ON se.seller_key = f.seller_key
JOIN dwh.dim_score sc  ON sc.score_key = f.score_key
GROUP BY se.seller_id, se.seller_city, se.seller_state;
