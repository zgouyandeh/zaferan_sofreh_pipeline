"""
databricks/02_transformation/07_dim_menu_item.py
------------------------------------------------------
Reads raw bronze menu item data, applies quality checks, 
and materializes the dim_menu_item table.
"""

import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="dim_menu_item", 
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("restaurant_id_not_null", "restaurant_id IS NOT NULL")
@dlt.expect_or_drop("item_id_not_null", "item_id IS NOT NULL")
def dim_menu_item():
    return (
        dlt.read("zaferan_sofreh.bronze.menu_items_raw")
        .select(
            "restaurant_id",
            "item_id",
            "name",
            "category",
            col("price").cast("decimal(10,2)").alias("price"),
            "ingredients",
            "is_vegetarian",
            "spice_level",
        )
    )