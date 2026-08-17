from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.table(name="dim_customer", table_properties={"quality":"silver"})
@dp.expect_all_or_drop(
    {
        "valid_customer_id":"customer_id IS NOT NULL",
        "valid_join_date":"join_date IS NOT NULL"
    }
)
def dim_customer():
    return (
        spark.readStream
        .table("zaferan_sofreh.bronze.customers_raw")
        .select(
            "customer_id",
            "name",
            "phone",
            "city",
            to_date("join_date").alias("join_date"),
        )
    )