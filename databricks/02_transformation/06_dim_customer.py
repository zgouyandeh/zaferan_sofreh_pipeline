"""
databricks/02_transformation/06_dim_customer.py
------------------------------------------------------
Reads raw bronze customer data, applies quality checks, 
and materializes the dim_customer table.
"""

import dlt
from pyspark.sql.functions import to_date

@dlt.table(
    name="dim_customer", 
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop(
    {
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_join_date": "join_date IS NOT NULL",
    }
)
def dim_customer():
    return (
        dlt.read("zaferan_sofreh.bronze.customers_raw")
        .select(
            "customer_id",
            "name",
            "phone",
            "city",
            to_date("join_date").alias("join_date"),
        )
    )