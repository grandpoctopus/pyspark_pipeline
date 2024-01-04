from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from pyspark_pipeline.queries import Query


class SubsetByColumnValueQuery(Query):
    """
    Query to obtain a dataframe containing only column values in a provided
    dataframe of column names and values
    """

    def __init__(
        self,
        subsetting_df: DataFrame,
        input_df: DataFrame,
        cols: List[str],
        **kwargs,
    ):
        self.cols = cols
        self.subsetting_df = subsetting_df.select(cols).distinct()
        self.input_df = input_df
        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        output_df = self.input_df.join(self.subsetting_df, [*self.cols]).select(
            self.schema.get_columns_list()
        )

        # some dates are incorrectly interpreted by the CSVSerDe
        if "dt" in output_df.schema.names:
            output_df = output_df.where(
                (F.col("dt").isNotNull()) | (F.col("dt") != "")
            )

        return output_df.distinct()


class CopyTableQuery(Query):
    """
    Query to copy table without any changes
    """

    def __init__(self, input_df: DataFrame, **kwargs):
        self.input_df = input_df
        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        return self.input_df
