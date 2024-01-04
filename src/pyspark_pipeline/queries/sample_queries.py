import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from pyspark_pipeline.queries import Query


class SampleJoinQuery(Query):
    """
    A sample query that performs a join
    on two input dataframe
    """

    def __init__(self, is_active_df: DataFrame, value_df: DataFrame, **kwargs):
        self.is_active_df = is_active_df
        self.value_df = value_df
        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        output_df = self.is_active_df.join(self.value_df, on=["id"]).where(
            F.col("active") == "true"
        )

        return output_df.select(self.schema.get_columns_list())


class SampleAggregationQuery(Query):
    """
    A sample query that performs an aggregation
    on an input dataframe
    """

    def __init__(self, sample_joined_df: DataFrame, **kwargs):
        self.sample_joined_df = sample_joined_df
        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        output_df = self.sample_joined_df.groupBy("id").agg(
            F.max("value").alias("max_value")
        )

        return output_df.select(self.schema.get_columns_list())
