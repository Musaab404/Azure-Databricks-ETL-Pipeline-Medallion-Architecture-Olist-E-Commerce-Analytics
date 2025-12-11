import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, MapType, StringType

# COMMAND ----------

@dlt.view(
    name="stage_order_customer",
    comment="Raw streaming order_customer data from the bronze layer"
)
def stage_order_customer():
    """Stage bronze data for transformation."""
    _ = dlt.read_stream("live.silver_orders")  # dependency only
    return (
        dlt.readStream("bronze.bronze_customers")
    )

# Create state mapping as a broadcast variable (one-time initialization)
STATE_MAPPING = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapá",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Pará",
    "PB": "Paraíba",
    "PR": "Paraná",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondônia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "São Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins"
}

@dlt.table(
    name="silver.silver_order_customers",
    comment="Validated and high-quality order_customers data ready for consumption",
    table_properties={
        "quality": "silver",
    }
)
@dlt.expect_all({
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_customer_unique_id": "customer_unique_id IS NOT NULL"
})
def silver_order_customers():
    """
    Transform bronze order_customers to silver layer.

    Performance optimizations:
    - Use map lookup instead of nested WHEN clauses
    - Minimize column operations
    - Enable Delta optimizations
    """
    df = dlt.read_stream("stage_order_customer")
    
    # Create a map column for efficient lookups (much faster than nested WHEN)
    state_map = F.create_map([F.lit(x) for pair in STATE_MAPPING.items() for x in pair])
    
    return (
        df.drop("_rescued_data")
          .withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast(IntegerType()))
          .withColumn("customer_state_full", 
                     F.coalesce(state_map[F.col("customer_state")], F.col("customer_state")))
          .withColumn("modified_timestamp", F.current_timestamp())
          .select(
              "customer_id",
              "customer_unique_id",
              "customer_zip_code_prefix",
              "customer_city",
              "customer_state",
              "customer_state_full",
              "modified_timestamp"
          )
    )