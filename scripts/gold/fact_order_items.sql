CREATE OR REFRESH MATERIALIZED VIEW gold.fact_order_items
(
  CONSTRAINT fk_product         EXPECT (product_key IS NOT NULL)                             ON VIOLATION FAIL UPDATE,
  CONSTRAINT fk_order           EXPECT (order_key IS NOT NULL)                               ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_item_price   EXPECT (item_price IS NOT NULL AND item_price >= 0)          ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_item_total   EXPECT (item_total_value IS NOT NULL AND item_total_value >= 0),
  CONSTRAINT valid_order_date   EXPECT (order_date_key IS NOT NULL)                          ON VIOLATION FAIL UPDATE
)
CLUSTER BY (product_key, seller_key, order_date_key, customer_key)
COMMENT "This table provides order items transactions."
TBLPROPERTIES (
  'quality' = 'gold',
  'layer' = 'analytics',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.targetFileSize' = '268435456'
)
AS
WITH payment_totals AS (
  -- stable, ordered aggregation of payment types
  SELECT
    order_id,
    SUM(payment_value) AS total_payment_value,
    COUNT(DISTINCT payment_type) AS payment_type_count,
    MAX(payment_installments) AS max_installments,
    array_join(sort_array(collect_set(payment_type)), ', ') AS payment_types
  FROM LIVE.silver_order_payments
  GROUP BY order_id
),
review_summary AS (
  SELECT
    order_id,
    MAX(review_score) AS review_score,
    MAX(review_creation_date) AS review_creation_date,
    MAX(review_answer_timestamp) AS review_answer_timestamp
  FROM LIVE.silver_order_reviews
  GROUP BY order_id
)
SELECT
  -- Deterministic surrogate key (stable across incremental refreshes)
  sha2(concat(CAST(oi.order_id AS STRING), '|', CAST(oi.order_item_id AS STRING)), 256) AS order_item_key,

  -- Surrogate/FK references (assume dims enforce uniqueness on natural keys)
  do.order_key,
  dc.customer_key,
  dp.product_key,
  ds.seller_key,
  dd.date_key AS order_date_key,
  dd_delivered.date_key AS delivered_date_key,

  -- Natural keys (degenerate dims)
  oi.order_id AS order_natural_key,
  oi.order_item_id,

  -- Measures (null-safe arithmetic)
  oi.price AS item_price,
  oi.freight_value AS item_freight_value,
  (COALESCE(oi.price, 0) + COALESCE(oi.freight_value, 0)) AS item_total_value,

  -- Denormalized order-level aggregates
  pt.total_payment_value,
  pt.payment_type_count,
  pt.max_installments,
  pt.payment_types,

  -- Review metrics
  rs.review_score,
  CASE WHEN rs.review_score IS NOT NULL AND rs.review_score >= 4 THEN TRUE ELSE FALSE END AS is_positive_review,

  -- Delivery calculations (NULL-safe)
  CASE WHEN o.order_delivered_customer_date IS NOT NULL
       THEN datediff(date(o.order_delivered_customer_date), date(o.order_purchase_timestamp))
       ELSE NULL END AS actual_delivery_days,

  CASE WHEN o.order_estimated_delivery_date IS NOT NULL
       THEN datediff(date(o.order_estimated_delivery_date), date(o.order_purchase_timestamp))
       ELSE NULL END AS estimated_delivery_days,

  -- Timeline / audit
  oi.shipping_limit_date,
  o.order_purchase_timestamp,
  o.order_estimated_delivery_date,
  o.order_delivered_customer_date
  
FROM LIVE.silver_order_items oi
INNER JOIN LIVE.silver_orders o
  ON oi.order_id = o.order_id
INNER JOIN LIVE.silver_order_customers c
  ON o.customer_id = c.customer_id

-- Resolve surrogate keys from deterministic natural-key joins (dims must enforce uniqueness)
INNER JOIN gold.dim_orders do
  ON o.order_id = do.order_natural_key
INNER JOIN gold.dim_customers dc
  ON c.customer_id = dc.customer_natural_key
INNER JOIN gold.dim_products dp
  ON oi.product_id = dp.product_natural_key
INNER JOIN gold.dim_sellers ds
  ON oi.seller_id = ds.seller_natural_key

-- Date dimension joins (use DATE for deterministic match)
INNER JOIN gold.dim_date dd
  ON DATE(o.order_purchase_timestamp) = dd.date_actual
LEFT JOIN gold.dim_date dd_delivered
  ON DATE(o.order_delivered_customer_date) = dd_delivered.date_actual

-- Upstream aggregates
LEFT JOIN payment_totals pt
  ON oi.order_id = pt.order_id
LEFT JOIN review_summary rs
  ON oi.order_id = rs.order_id

-- Basic row-level filter to avoid obvious garbage
WHERE oi.order_id IS NOT NULL
;