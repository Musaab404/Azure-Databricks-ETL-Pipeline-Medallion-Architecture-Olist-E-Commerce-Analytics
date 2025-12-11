CREATE OR REFRESH MATERIALIZED VIEW gold.dim_sellers

COMMENT "This table provides a seller dimension with calendar attributes for analytics."
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
    sha2(CAST(s.seller_id AS STRING), 256) AS seller_key, 
    s.seller_id AS seller_natural_key,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    s.seller_state_full,
    g.geolocation_lat,
    g.geolocation_lng
FROM LIVE.silver_sellers s
LEFT JOIN (
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
) g ON s.seller_zip_code_prefix = g.zip_code_prefix