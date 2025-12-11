CREATE OR REFRESH MATERIALIZED VIEW gold.dim_products

COMMENT "This table provides a products dimension with calendar attributes for analytics."
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
    sha2(CAST(product_id AS STRING), 256) AS product_key, 
    product_id AS product_natural_key,
    product_category_name_english AS product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3,
    has_photos, 
    has_description
FROM LIVE.silver_products