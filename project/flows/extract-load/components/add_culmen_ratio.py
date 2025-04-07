import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from ascend.resources import ref, transform
from ascend.application.context import ComponentExecutionContext


@transform(inputs=[ref("read_penguins")])
def add_culmen_ratio(read_penguins: DataFrame, context: ComponentExecutionContext) -> DataFrame:
    # Add a new column 'Culmen_Ratio' safely handling null and zero division
    penguins_with_ratio = read_penguins.withColumn(
        "Culmen_Ratio",
        F.when(
            (F.col("Culmen_Depth_mm").isNotNull()) &
            (F.col("Culmen_Length_mm").isNotNull()) &
            (F.col("Culmen_Depth_mm") != 0), &
            (F.col("Culmen_Length_mm") != 0),
            F.col("Culmen_Length_mm") / F.col("Culmen_Depth_mm")
        ).otherwise(None)
    )

    return penguins_with_ratio