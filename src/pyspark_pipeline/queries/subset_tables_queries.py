from typing import List, Optional

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StructField, StructType

from pyspark_pipeline.queries import Query


class GetIdsOccurringInAllDataFrames(Query):
    """
    Query to obtain a dataframe wih a single column <id_col_name> that
    is all of the ids occuring in all DataFrames in dataframes
    """

    def __init__(
        self,
        dataframes: List[DataFrame],
        id_col_name: str,
        number_of_ids: int,
        exclusion_id_df: Optional[DataFrame] = None,
        **kwargs,
    ):
        self.dataframes = dataframes
        self.id_col_name = id_col_name
        self.number_of_ids = number_of_ids
        self.exclusion_id_df = exclusion_id_df
        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        df = self.dataframes.pop().select(self.id_col_name).distinct()

        for dataframe in self.dataframes:
            df = df.join(
                dataframe.select(self.id_col_name), [self.id_col_name]
            ).distinct()
            df.cache()

        if self.exclusion_id_df is not None:
            df = df.join(
                self.exclusion_id_df, on=[self.id_col_name], how="left_anti"
            ).distinct()

        return df.orderBy(self.id_col_name).limit(self.number_of_ids)


class IdsInLocalCsv(Query):
    """
    Query to obtain a dataframe from a local csv_file containing
    an id column
    """

    def __init__(self, id_csv_path: str, id_col_name: str, **kwargs):
        self.id_csv_path = id_csv_path
        self.id_col_name = id_col_name
        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        pandas_id_csv_df = pd.read_csv(
            self.id_csv_path, header=0, dtype={self.id_col_name: int}
        )[[self.id_col_name]]

        schema = StructType(
            [
                StructField(self.id_col_name, IntegerType(), True),
            ]
        )

        return self.spark.createDataFrame(pandas_id_csv_df, schema)


class IdsWithTableColumnString(Query):
    """
    Query to obtain a dataframe of ids from a particular table
    containing a specific string value

    args:
        df: A Spark DataFrame that will be used to create a list of ids
        column: the column name that will be used to search for a matching string
        pattern: the pattern that will be used to filter the dataframe
    """

    def __init__(
        self,
        df: DataFrame,
        column: str,
        id_col_name: str,
        pattern: str,
        **kwargs,
    ):
        self.df = df
        self.id_col_name = id_col_name
        self.column = column
        self.pattern = pattern
        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        return (
            self.df.where(F.col(self.column) == self.pattern)
            .select(self.id_col_name)
            .distinct()
        )


class SubsetById(Query):
    """
    Query to obtain a dataframe containing only ids in a provided
    dataframe of ids
    """

    def __init__(
        self, id_df: DataFrame, id_col_name: str, input_df: DataFrame, **kwargs
    ):
        self.id_df = id_df.distinct()
        self.id_col_name = id_col_name
        self.input_df = input_df
        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        output_df = self.input_df.join(
            F.broadcast(self.id_df), [self.id_col_name]
        ).select(self.schema.get_columns_list())

        return output_df
