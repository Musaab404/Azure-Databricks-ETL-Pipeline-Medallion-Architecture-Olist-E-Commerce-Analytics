import dlt 
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DecimalType

# COMMAND ----------

# --- Bronze layer ingestion ---
@dlt.view(
    name="stage_order_payments",
    comment="Raw streaming order_payments data from the bronze layer"
)
def stage_order_payments():

    """Stage bronze data for transformation"""
    return (
        dlt.readStream("bronze.bronze_order_payments")
    )

# COMMAND ----------
@dlt.table(
    name="silver.silver_order_payments",
    comment="Transformed streaming order_payments data with quality rules",
    table_properties={"quality": "silver"}
    
)
@dlt.expect_all({ 
    "valid_order_id": "order_id IS NOT NULL" ,
    "valid_payment_sequential": "payment_sequential >= 1",
    "valid_payment_type": "payment_type IS NOT NULL",
    "valid_payment_installments": "payment_installments >= 0",
    "valid_payment_value": "payment_value >= 0",
})
def silver_order_payments():
    """
    Transform bronze order payments to silver layer with type casting and audit columns.
    
    Transformations:
    - Cast payment_sequential to integer
    - Cast payment_installments to integer  
    - Cast payment_value to decimal(10,2)
    - Add processing timestamp for audit
    
    Returns:
        Transformed streaming DataFrame
    """
    return (
        dlt.readStream("stage_order_payments")
        .drop("_rescued_data")
        .select(
            "order_id",
            "payment_type",
            F.col("payment_sequential").cast(IntegerType()).alias("payment_sequential"),
            F.col("payment_installments").cast(IntegerType()).alias("payment_installments"),
            F.col("payment_value").cast(DecimalType(10, 2)).alias("payment_value"),
            F.current_timestamp().alias("modified_timestamp")
        )
    )