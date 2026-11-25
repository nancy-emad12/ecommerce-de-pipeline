-- =====================================================================
-- E-Commerce DWH (Postgres) - star schema, built from the team's
-- physical schema (docs/schema/physical schema.jpeg) with two changes:
--   1. TINYINT does not exist in Postgres -> SMALLINT
--   2. Payments: Fact_Sales keeps only allocated_payment_value;
--      every payment row (type, installments, value) lives in
--      fact_payments, so an order paid 280 cash + 180 card keeps both.
-- Key -1 in every dimension = "Unknown" member (missing / late data).
-- Idempotent: safe to run on every DAG run.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS dwh;

-- ---------------------------------------------------------------- dims
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key        INT          PRIMARY KEY,
    full_date       DATE         NOT NULL UNIQUE,
    year            SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL,
    month           SMALLINT     NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,
    day             SMALLINT     NOT NULL,
    day_name        VARCHAR(10)  NOT NULL,
    is_weekend      SMALLINT     NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_key              INT          PRIMARY KEY,
    customer_unique_id        CHAR(32)     NOT NULL UNIQUE,
    customer_zip_code_prefix  CHAR(5),
    customer_city             VARCHAR(50),
    customer_state            CHAR(2)
);

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_key              INT          PRIMARY KEY,
    seller_id               CHAR(32)     NOT NULL UNIQUE,
    seller_zip_code_prefix  CHAR(5),
    seller_city             VARCHAR(50),
    seller_state            CHAR(2)
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_key                    INT          PRIMARY KEY,
    product_id                     CHAR(32)     NOT NULL UNIQUE,
    product_category_name          VARCHAR(60),
    product_category_name_english  VARCHAR(60),
    product_name_length            SMALLINT,
    product_description_length     SMALLINT,
    product_photos_qty             SMALLINT,
    product_weight_g               INT,
    product_length_cm              SMALLINT,
    product_height_cm              SMALLINT,
    product_width_cm               SMALLINT
);

CREATE TABLE IF NOT EXISTS dwh.dim_geolocation (
    geolocation_key              INT            PRIMARY KEY,
    geolocation_zip_code_prefix  CHAR(5)        NOT NULL UNIQUE,
    geolocation_lat              DECIMAL(9,6),
    geolocation_lng              DECIMAL(9,6),
    geolocation_city             VARCHAR(50),
    geolocation_state            CHAR(2)
);

CREATE TABLE IF NOT EXISTS dwh.dim_order_status (
    order_status_key  SMALLINT     PRIMARY KEY,
    order_status      VARCHAR(20)  NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_payment_type (
    payment_type_key  SMALLINT     PRIMARY KEY,
    payment_type      VARCHAR(20)  NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_score (
    score_key     SMALLINT  PRIMARY KEY,
    review_score  SMALLINT  NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_order_reviews (
    order_review_key        INT           PRIMARY KEY,
    review_id               CHAR(32),
    order_id                CHAR(32)      NOT NULL UNIQUE,
    review_comment_title    VARCHAR(30),
    review_comment_message  VARCHAR(255)
);

-- --------------------------------------------------------------- facts
-- Grain: one row per order item.
CREATE TABLE IF NOT EXISTS dwh.fact_sales (
    order_id                   CHAR(32)       NOT NULL,
    order_item_id              SMALLINT       NOT NULL,
    purchase_date_key          INT            NOT NULL REFERENCES dwh.dim_date(date_key),
    approved_date_key          INT            NOT NULL REFERENCES dwh.dim_date(date_key),
    delivered_carrier_date_key INT            NOT NULL REFERENCES dwh.dim_date(date_key),
    delivered_customer_date_key INT           NOT NULL REFERENCES dwh.dim_date(date_key),
    estimated_delivery_date_key INT           NOT NULL REFERENCES dwh.dim_date(date_key),
    shipping_limit_date_key    INT            NOT NULL REFERENCES dwh.dim_date(date_key),
    review_created_date_key    INT            NOT NULL REFERENCES dwh.dim_date(date_key),
    review_answered_date_key   INT            NOT NULL REFERENCES dwh.dim_date(date_key),
    customer_key               INT            NOT NULL REFERENCES dwh.dim_customer(customer_key),
    order_status_key           SMALLINT       NOT NULL REFERENCES dwh.dim_order_status(order_status_key),
    score_key                  SMALLINT       NOT NULL REFERENCES dwh.dim_score(score_key),
    product_key                INT            NOT NULL REFERENCES dwh.dim_product(product_key),
    seller_key                 INT            NOT NULL REFERENCES dwh.dim_seller(seller_key),
    seller_geolocation_key     INT            NOT NULL REFERENCES dwh.dim_geolocation(geolocation_key),
    customer_geolocation_key   INT            NOT NULL REFERENCES dwh.dim_geolocation(geolocation_key),
    order_review_key           INT            NOT NULL REFERENCES dwh.dim_order_reviews(order_review_key),
    price                      DECIMAL(10,2)  NOT NULL,
    freight_value              DECIMAL(10,2)  NOT NULL,
    allocated_payment_value    DECIMAL(10,2),
    etl_batch_id               VARCHAR(40)    NOT NULL,
    etl_loaded_at              TIMESTAMP      NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, order_item_id)
);

-- Grain: one row per payment (an order can have several payment types).
CREATE TABLE IF NOT EXISTS dwh.fact_payments (
    order_id                  CHAR(32)       NOT NULL,
    payment_sequential        SMALLINT       NOT NULL,
    payment_type_key          SMALLINT       NOT NULL REFERENCES dwh.dim_payment_type(payment_type_key),
    customer_key              INT            NOT NULL REFERENCES dwh.dim_customer(customer_key),
    customer_geolocation_key  INT            NOT NULL REFERENCES dwh.dim_geolocation(geolocation_key),
    order_status_key          SMALLINT       NOT NULL REFERENCES dwh.dim_order_status(order_status_key),
    purchase_date_key         INT            NOT NULL REFERENCES dwh.dim_date(date_key),
    approved_date_key         INT            NOT NULL REFERENCES dwh.dim_date(date_key),
    payment_installments      SMALLINT       NOT NULL,
    payment_value             DECIMAL(10,2)  NOT NULL,
    etl_batch_id              VARCHAR(40)    NOT NULL,
    etl_loaded_at             TIMESTAMP      NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, payment_sequential)
);

-- ------------------------------------------------------------- indexes
-- Columns used most in filters / joins by the views and the dashboard.
CREATE INDEX IF NOT EXISTS ix_fact_sales_purchase_date  ON dwh.fact_sales (purchase_date_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_customer       ON dwh.fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_product        ON dwh.fact_sales (product_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_seller         ON dwh.fact_sales (seller_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_status         ON dwh.fact_sales (order_status_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_customer_geo   ON dwh.fact_sales (customer_geolocation_key);
CREATE INDEX IF NOT EXISTS ix_fact_payments_type        ON dwh.fact_payments (payment_type_key);
CREATE INDEX IF NOT EXISTS ix_fact_payments_purchase    ON dwh.fact_payments (purchase_date_key);
CREATE INDEX IF NOT EXISTS ix_dim_product_category      ON dwh.dim_product (product_category_name_english);
CREATE INDEX IF NOT EXISTS ix_dim_customer_state        ON dwh.dim_customer (customer_state);
