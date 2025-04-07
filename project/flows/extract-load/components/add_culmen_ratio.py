import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from ascend.resources import ref, transform
from ascend.application.context import ComponentExecutionContext


@transform(inputs=[ref("read_penguins")])
def add_culmen_ratio(read_penguins, context: ComponentExecutionContext) -> DataFrame:
    # Convert ibis.Table to PySpark DataFrame
    spark = context.spark_session
    spark_df = context.as_spark_dataframe(read_penguins)

    # Add the Culmen_Ratio column
    penguins_with_ratio = spark_df.withColumn(
        "Culmen_Ratio",
        F.when(
            (F.col("Culmen_Depth_mm").isNotNull()) &
            (F.col("Culmen_Length_mm").isNotNull()) &
            (F.col("Culmen_Depth_mm") != 0),
            F.col("Culmen_Length_mm") / F.col("Culmen_Depth_mm")
        ).otherwise(None)
    )

    return penguins_with_ratio
