# Schema diagrams

`conceptual schema.jpeg`, `logical schema.jpeg` and `physical schema.jpeg` are the team's original design.

**Change since these images (decision #7):** payments moved to a new fact table.

```
fact_payments (grain = one payment)
  PK  order_id, payment_sequential
  FK  payment_type_key, customer_key, customer_geolocation_key, order_status_key,
      purchase_date_key, approved_date_key
      payment_installments, payment_value
fact_sales: payment_type_key and payment_installments removed (allocated_payment_value kept)
```

The source of truth for columns and types is `include/sql/ddl/01_create_dwh.sql`.
TODO (owner: Menna): update the three diagrams with `fact_payments`.
