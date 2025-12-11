import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# --- Bronze layer ingestion ---
@dlt.view(
    name="stage_order_reviews",
    comment="Raw streaming order_reviews data from the bronze layer"
)
def stage_order_reviews():
    """Stage bronze data for transformation"""
    return (
        dlt.readStream("bronze.bronze_order_reviews")
    )

# COMMAND ----------

# --- Silver layer transformation with quality rules ---
@dlt.table(
    name="silver.silver_order_reviews",
    comment="Validated and high-quality order_reviews data ready for consumption",
    table_properties={"quality": "silver"}
)
@dlt.expect_all({
    "valid_order_id": "order_id IS NOT NULL" , 
    "valid_review_id": "review_id IS NOT NULL",
    "valid_review_score": "review_score BETWEEN 1 AND 5",
    "valid_review_creation_date": "review_creation_date IS NOT NULL",
    "valid_review_answer_timestamp": "review_answer_timestamp IS NOT NULL"
})

def silver_order_reviews():
    """
    Transform bronze order reviews to silver with validations and enrichment.
    Applies data quality rules, deduplication, and null imputation.
    """
    # Read source streams
    reviews_df = dlt.readStream("stage_order_reviews")
    orders_df = dlt.readStream("live.silver_orders")
    
    # Prepare orders lookup with required columns only
    orders_lookup = orders_df.select(
        "order_id",
        F.col("order_delivered_customer_date").cast("timestamp").alias("delivered_date"),
        F.col("order_estimated_delivery_date").cast("timestamp").alias("estimated_date")
    )
    
    # Join with orders and deduplicate in one operation
    df = (reviews_df
          .join(orders_lookup, on="order_id", how="inner")
          .dropDuplicates(["review_id"])
    )
    
    # Apply all transformations in a single select statement for better optimization
    df_transformed = df.select(
        "order_id",
        "review_id",
        F.col("review_score").cast("int").alias("review_score"),
        
        # Handle null comments with coalesce
        F.coalesce(F.col("review_comment_title"), F.lit("N/A")).alias("review_comment_title"),
        F.coalesce(F.col("review_comment_message"), F.lit("N/A")).alias("review_comment_message"),
        
        # Impute review_creation_date: use delivery date + 1 day if null
        F.coalesce(
            F.col("review_creation_date").cast("timestamp"),
            F.col("delivered_date") + F.expr("INTERVAL 1 DAY"),
            F.col("estimated_date") + F.expr("INTERVAL 1 DAY")
        ).alias("review_creation_date"),
        
        # Impute review_answer_timestamp: use creation date + 3 days if null
        F.coalesce(
            F.col("review_answer_timestamp").cast("timestamp"),
            F.col("review_creation_date").cast("timestamp") + F.expr("INTERVAL 3 DAY")
        ).alias("review_answer_timestamp")
    )
    df_transformed = df_transformed.withColumn("processed_timestamp", F.current_timestamp())
    
    return df_transformed















