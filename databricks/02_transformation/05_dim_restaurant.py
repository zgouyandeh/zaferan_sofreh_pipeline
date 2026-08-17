from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.table(name="dim_restaurant", table_properties={"quality":"silver"})
@dp.expect_all_or_drop(
    {
        "valid_restaurant_id":"restaurant_id IS NOT NULL",
        "valid_opening_date":"opening_date IS NOT NULL"
    }
)

def dim_restaurant():
    return (
        spark.readStream.table("zaferan_sofreh.bronze.restaurants_raw")
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