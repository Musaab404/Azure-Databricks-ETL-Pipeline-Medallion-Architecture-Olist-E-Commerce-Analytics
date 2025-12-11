import dlt

@dlt.table(
    name="silver.silver_product_category_trans",
    comment="Validated and high-quality silver_product_category_trans data ready for consumption",
    table_properties={"quality": "silver"}
)
def silver_product_category_trans():
    return (
        dlt.readStream("bronze.bronze_product_cat_name_translation").drop("_rescued_data")
    )
