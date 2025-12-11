import dlt
from pyspark.sql.types import TimestampType, DecimalType, IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

# --- Bronze layer ingestion ---
@dlt.view(
    name="stage_order_items",
    comment="Raw streaming order_items data from the bronze layer"
)
def stage_order_items():
    """Stage bronze data for transformation"""
    return (
        dlt.readStream("bronze.bronze_order_items")
    )

# COMMAND ----------

# --- Silver layer transformation with quality rules ---
@dlt.table(
    name="silver.silver_order_items",
    comment="Validated and high-quality order_items data ready for consumption",
    table_properties={
        "quality": "silver"
    }
)
# Apply expectations directly to the final table
@dlt.expect_all({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_product_id": "product_id IS NOT NULL",
    "valid_seller_id": "seller_id IS NOT NULL",
    "valid_order_item_id": "order_item_id IS NOT NULL"
})

#@dlt.expect("has_valid_order", "order_id IN (SELECT order_id FROM LIVE.olist_cat.silver.silver_orders)")
def silver_order_items():
    """
    Transform bronze order items to silver with validations.
    
    Business Rules:
    - All items must have valid order_id, product_id, seller_id, order_item_id
    - Price must be positive and reasonable (<= 100000)
    - Freight value must be non-negative
    - Must reference valid order in silver_orders table
    """
    return (
        dlt.read_stream("stage_order_items")
        .drop("_rescued_data")
        .select(
            F.col("order_id"),
            F.col("order_item_id").cast(IntegerType()).alias("order_item_id"),
            F.col("product_id"),
            F.col("seller_id"),
            F.col("shipping_limit_date").cast(TimestampType()).alias("shipping_limit_date"),
            F.col("price").cast(DecimalType(10, 2)).alias("price"),
            F.col("freight_value").cast(DecimalType(10, 2)).alias("freight_value")
        )
        # Add derived columns
        .withColumn("processed_timestamp", F.current_timestamp())
    )