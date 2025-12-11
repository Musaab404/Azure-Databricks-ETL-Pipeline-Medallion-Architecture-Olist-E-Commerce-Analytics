import dlt

# Base configuration
STORAGE_ACCOUNT = "musaabstorage"
SOURCE_CONTAINER = "source"
BRONZE_SCHEMA = "bronze"  # Changed to schema name
DATASETS = ["order_items", "orders", "customers", "products", 
            "sellers", "product_cat_name_translation", "order_payments", "order_reviews", 
            "geolocations"]

# Helper function to generate paths
def get_paths(dataset_name):
    base_url = "dfs.core.windows.net"
    return {
        "source": f"abfss://{SOURCE_CONTAINER}@{STORAGE_ACCOUNT}.{base_url}/{dataset_name}",
        "schema": f"abfss://{SOURCE_CONTAINER}@{STORAGE_ACCOUNT}.{base_url}/{dataset_name}/_schemas"  # Schema location in source
    }

# Reusable function to read CSV with Auto Loader
def read_csv_autoloader(source_path, schema_path):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("header", "true")
            .option("delimiter", ",")
            .option("ignoreLeadingWhiteSpace", "true")
            .option("ignoreTrailingWhiteSpace", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(source_path)
    )

# Factory function
def create_bronze_table(dataset_name):
    paths = get_paths(dataset_name)
    
    @dlt.table(
        name=f"bronze.bronze_{dataset_name}",
        comment=f"Raw {dataset_name} data ingested from ADLS Gen2",
        table_properties={
            "quality": "bronze",
            "pipelines.autoOptimize.managed": "true"
        }
    )
    def bronze_table():
        return read_csv_autoloader(paths["source"], paths["schema"])
    
    return bronze_table

# Generate all tables
for dataset in DATASETS:
    create_bronze_table(dataset)