# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze to Silver: Products Pipeline
# MAGIC 
# MAGIC This pipeline:
# MAGIC 1. Stages raw product data from bronze layer
# MAGIC 2. Enriches with English category translations
# MAGIC 3. Applies data type conversions and quality checks
# MAGIC 4. Produces clean, analytics-ready product dimension

# COMMAND ----------

# Configuration
BRONZE_PATH = "abfss://bronze@musaabstorage.dfs.core.windows.net/products/data"
DEFAULT_CATEGORY = "N/A"

# COMMAND ----------

@dlt.view(
    name="stage_products",
    comment="Raw streaming products data from bronze layer",
)
def stage_products():
    """
    Stage bronze product data for transformation.
    Source: Bronze container products table
    """
    return (dlt.readStream("bronze.bronze_products"))

# COMMAND ----------

@dlt.table(
    name="silver.silver_products",
    comment="Enriched products with English translations and validated metrics",
    table_properties={
        "quality": "silver",
    }
)
@dlt.expect_all({
    "valid_product_id": "product_id IS NOT NULL"
})
def silver_products():
    """
    Transform bronze products to silver with:
    - English category translations via broadcast join
    - Type casting for numeric fields
    - Null handling with defaults
    - Product dimension calculations
    
    Business Rules:
    - Product ID must exist
    - Dimensions and weight must be non-negative
    - Missing categories filled with 'N/A'
    """
    
    # Read streams
    products = dlt.read_stream("stage_products")
    category_translations = dlt.read("live.silver_product_category_trans")
    
    # Join with category translations (broadcast for performance)
    products_translated = products.join(
        F.broadcast(category_translations),
        on="product_category_name",
        how="left"
    )
    
    # Select and transform in single pass
    return (
        products_translated
        .select(
            F.col("product_id"),
            
            # Category with fallback
            F.coalesce(
                F.col("product_category_name_english"),
                F.lit(DEFAULT_CATEGORY)
            ).alias("product_category_name_english"),
            
            # Cast all numeric fields at once
            F.col("product_name_lenght").cast(IntegerType()).alias("product_name_length"),
            F.col("product_description_lenght").cast(IntegerType()).alias("product_description_length"),
            F.col("product_photos_qty").cast(IntegerType()).alias("product_photos_qty"),
            F.col("product_weight_g").cast(IntegerType()).alias("product_weight_g"),
            F.col("product_length_cm").cast(IntegerType()).alias("product_length_cm"),
            F.col("product_height_cm").cast(IntegerType()).alias("product_height_cm"),
            F.col("product_width_cm").cast(IntegerType()).alias("product_width_cm")
        )
        # Calculate derived metrics
        .withColumn("product_volume_cm3",
            F.col("product_length_cm") * 
            F.col("product_height_cm") * 
            F.col("product_width_cm")
        )
        .withColumn("has_photos", F.col("product_photos_qty") > 0)
        .withColumn("has_description", F.col("product_description_length") > 0)
        
        # Fill remaining nulls with 0 for numeric fields
        .fillna(0, subset=[
            "product_name_length",
            "product_description_length", 
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_volume_cm3"
        ])
        
        # Add metadata
        .withColumn("processed_timestamp", F.current_timestamp())
        
        # Remove duplicates
        .dropDuplicates(["product_id"])
    )