import dlt

@dlt.table(
    name="silver.silver_geolocations",
    comment="Validated and high-quality silver_geolocations data ready for consumption",
    table_properties={"quality": "silver"}
)
def silver_geolocations():
    return (
        dlt.readStream("bronze.bronze_geolocations").drop("_rescued_data")
    )
