import dlt
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F

# --- Bronze layer ingestion ---
@dlt.view(
    name="stage_orders",
    comment="Raw streaming orders data from the bronze layer"
)
def stage_orders():
    return (
        dlt.readStream("bronze.bronze_orders")
    )

# --- Silver quality table ---
@dlt.table(
    name="silver.silver_orders",
    comment="Validated and high-quality orders data ready for consumption",
    table_properties={"quality": "silver"}
)
# --- Data quality rules ---

@dlt.expect_all(   { "order_id_not_null": "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL"})
def silver_orders():
    return (dlt.read_stream("stage_orders").drop("_rescued_data")
          .withColumns({
              "order_purchase_timestamp": F.col("order_purchase_timestamp").cast(TimestampType()),
              "order_approved_at": F.col("order_approved_at").cast(TimestampType()),
              "order_delivered_carrier_date": F.col("order_delivered_carrier_date").cast(TimestampType()),
              "order_delivered_customer_date": F.col("order_delivered_customer_date").cast(TimestampType()),
              "order_estimated_delivery_date": F.col("order_estimated_delivery_date").cast(TimestampType())
          })
          .withColumn(
              "order_approved_at",
              F.when(
                  (F.col("order_status") == "delivered") & F.col("order_approved_at").isNull(),
                  F.col("order_purchase_timestamp") + F.expr("INTERVAL 18 MINUTES")
              ).otherwise(F.col("order_approved_at"))
          )
          .withColumn("processed_date", F.current_timestamp())
    )










