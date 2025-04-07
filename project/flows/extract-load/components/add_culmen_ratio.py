from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from ascend.resources import pyspark, ref, test


@pyspark(
  inputs=[ref("read_penguins")],
  tests=[
    test("not_null", column="Species")
  ],
)
def add_culmen_ratio(spark: SparkSession, read_penguins: DataFrame, context) -> DataFrame:
    # Add the Culmen_Ratio column safely
    penguins_with_ratio = read_penguins.withColumn(
        "Culmen_Ratio",
        F.when(
            (F.col("Culmen_Depth_mm").isNotNull()) &
            (F.col("Culmen_Length_mm").isNotNull()) &
            (F.col("Culmen_Depth_mm") != 0),
            F.col("Culmen_Length_mm") / F.col("Culmen_Depth_mm")
        ).otherwise(None)
    )

    return penguins_with_ratio
