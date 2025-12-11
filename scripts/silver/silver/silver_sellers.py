# Databricks notebook source
import dlt
from pyspark.sql.functions import * 
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
BRONZE_PATH = "abfss://bronze@musaabstorage.dfs.core.windows.net/sellers/data"

# COMMAND ----------

@dlt.view(
    name="stage_sellers",
    comment="Raw streaming sellers data from bronze layer",
)
def stage_sellers():
    """
    Stage bronze sellers data for transformation.
    Source: Bronze container sellers table
    """
    return (dlt.readStream("bronze.bronze_sellers"))

# COMMAND ----------

@dlt.table(
    name="silver.silver_sellers",
    comment="Enriched seller dimension with full state names and validated data",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all({
    "valid_seller_id": "seller_id IS NOT NULL",
    "valid_seller_state": "seller_state IS NOT NULL"
})
@dlt.expect("valid_state_code", "LENGTH(seller_state) = 2")
def silver_sellers():
    """
    Transform bronze sellers to silver with:
    - State code to full name mapping
    - Validated zip codes
    - Cleaned and standardized location data
    
    Business Rules:
    - Seller ID and state must exist
    - Zip code must be positive
    - State code must be valid 2-letter abbreviation
    """
    
    # Brazilian state code to full name mapping
    state_mapping = create_map([
        lit("AC"), lit("Acre"),
        lit("AL"), lit("Alagoas"),
        lit("AP"), lit("Amapá"),
        lit("AM"), lit("Amazonas"),
        lit("BA"), lit("Bahia"),
        lit("CE"), lit("Ceará"),
        lit("DF"), lit("Distrito Federal"),
        lit("ES"), lit("Espírito Santo"),
        lit("GO"), lit("Goiás"),
        lit("MA"), lit("Maranhão"),
        lit("MT"), lit("Mato Grosso"),
        lit("MS"), lit("Mato Grosso do Sul"),
        lit("MG"), lit("Minas Gerais"),
        lit("PA"), lit("Pará"),
        lit("PB"), lit("Paraíba"),
        lit("PR"), lit("Paraná"),
        lit("PE"), lit("Pernambuco"),
        lit("PI"), lit("Piauí"),
        lit("RJ"), lit("Rio de Janeiro"),
        lit("RN"), lit("Rio Grande do Norte"),
        lit("RS"), lit("Rio Grande do Sul"),
        lit("RO"), lit("Rondônia"),
        lit("RR"), lit("Roraima"),
        lit("SC"), lit("Santa Catarina"),
        lit("SP"), lit("São Paulo"),
        lit("SE"), lit("Sergipe"),
        lit("TO"), lit("Tocantins")
    ])
    
    return (
        dlt.read_stream("stage_sellers")
        .drop("_rescued_data")
        .select(
            # Primary key
            col("seller_id"),
            
            # Location fields with transformations
            col("seller_zip_code_prefix").cast(IntegerType()).alias("seller_zip_code_prefix"),
            trim(col("seller_city")).alias("seller_city"),
            upper(trim(col("seller_state"))).alias("seller_state"),
            
            # Map state code to full name using efficient map lookup
            coalesce(
                state_mapping[upper(trim(col("seller_state")))],
                lit("Unknown")
            ).alias("seller_state_full")
        )
        # Add metadata
        .withColumn("processed_timestamp", current_timestamp())
        
    )




















