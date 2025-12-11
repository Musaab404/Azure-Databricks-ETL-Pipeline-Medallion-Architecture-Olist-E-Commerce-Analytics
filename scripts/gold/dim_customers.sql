CREATE OR REFRESH MATERIALIZED VIEW gold.dim_customers

COMMENT "This table provides a customers dimension with calendar attributes for analytics."
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
    sha2(CAST(c.customer_id AS STRING), 256) AS customer_key, 
    c.customer_id AS customer_natural_key,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    c.customer_state_full,
    g.geolocation_lat,
    g.geolocation_lng
FROM LIVE.silver_order_customers c
LEFT JOIN (
    -- Get most relevant geolocation per zip code (handle duplicates)
    SELECT 
        zip_code_prefix,
        ANY_VALUE(geolocation_lat) AS geolocation_lat,
        ANY_VALUE(geolocation_lng) AS geolocation_lng
    FROM (
        SELECT 
            geolocation_zip_code_prefix AS zip_code_prefix,
            geolocation_lat,
            geolocation_lng,
            ROW_NUMBER() OVER (PARTITION BY geolocation_zip_code_prefix ORDER BY geolocation_lat) AS rn
        FROM olist_cat.silver.silver_geolocations
    ) ranked
    WHERE rn = 1
    GROUP BY zip_code_prefix
) g ON c.customer_zip_code_prefix = g.zip_code_prefix;

