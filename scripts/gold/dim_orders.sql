CREATE OR REFRESH MATERIALIZED VIEW gold.dim_orders

COMMENT "This table provides an orders dimension with calendar attributes for analytics."
TBLPROPERTIES (
  'quality' = 'gold',
  'layer' = 'analytics',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.targetFileSize' = '268435456'
)
AS
SELECT
    sha2(CAST(order_id AS STRING), 256) AS order_key, 
    order_id AS order_natural_key,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    -- Calculate delivery performance metrics
    CASE 
        WHEN order_delivered_customer_date IS NOT NULL 
        THEN DATE_DIFF(DATE(order_delivered_customer_date), DATE(order_estimated_delivery_date))
        ELSE NULL
    END AS delivery_days_vs_estimate,
    CASE 
        WHEN order_delivered_customer_date IS NOT NULL 
            AND DATE(order_delivered_customer_date) <= DATE(order_estimated_delivery_date)
        THEN TRUE
        WHEN order_delivered_customer_date IS NOT NULL
        THEN FALSE
        ELSE NULL
    END AS is_on_time_delivery
FROM LIVE.silver_orders
