CREATE OR REFRESH MATERIALIZED VIEW gold.dim_date 

COMMENT "This table provides a date dimension with calendar attributes for analytics."
TBLPROPERTIES (
  'quality' = 'gold',
  'layer' = 'analytics',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.targetFileSize' = '268435456'
)
AS
WITH date_range AS (
    SELECT 
        MIN(DATE(order_purchase_timestamp)) AS min_date,
        MAX(DATE(COALESCE(order_delivered_customer_date, order_estimated_delivery_date))) AS max_date
    FROM LIVE.silver_orders
) ,
 date_series AS (
    SELECT date_add((SELECT min_date FROM date_range), seq) AS date_day
    FROM (
        -- Generate sequence of numbers (adjust based on your SQL dialect)
        SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS seq
        FROM LIVE.silver_orders
        LIMIT 1000
    ) numbers
)
SELECT
    ROW_NUMBER() OVER (ORDER BY date_day) AS date_key,
    date_day AS date_actual,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    dayname(date_day) AS day_name,
    monthname(date_day) AS month_name,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_series ;



