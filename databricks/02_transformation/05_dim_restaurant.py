"""
databricks/02_transformation/05_dim_restaurant.py
------------------------------------------------------
Reads raw bronze restaurant data, applies quality checks, 
and materializes the dim_restaurant table.
"""

import dlt
from pyspark.sql.functions import to_date

@dlt.table(
    name="dim_restaurant", 
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(
    {
        "valid_restaurant_id": "restaurant_id IS NOT NULL",
        "valid_opening_date": "opening_date IS NOT NULL",
    }
)
def dim_restaurant():
    return (
        dlt.read("zaferan_sofreh.bronze.restaurants_raw")
        .select(
            "restaurant_id",
            "name",
            "city",
            "country",
            "address",
            to_date("opening_date").alias("opening_date"),
            "phone",
        )
    )