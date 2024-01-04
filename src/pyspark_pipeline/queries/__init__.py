import logging
from abc import ABC
from datetime import datetime
from typing import List, Optional, cast

import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer
from pyspark.sql import Column, DataFrame, Row, SparkSession, Window
from pyspark.sql.types import StringType, StructField, StructType

from pyspark_pipeline.schemas import HiveSchema
from pyspark_pipeline.utilities.settings_utils import Settings
from pyspark_pipeline.utilities.table_writer import TableWriter


class Query(ABC):
    """
    Abstract class for all queries
    """

    def __init__(
        self,
        spark: SparkSession,
        settings: Settings,
        schema: HiveSchema,
        timestamps_as_strings: bool = False,
        do_log_filtered_values: bool = False,
        **kwargs,
    ):
        """
        args:
            spark: spark session for job
            settings: a Settings object containing hive
                database names and table suffixes
            schema: EtlSchema object with columns as
                attributes and datatypes as variables
            timestamps_as_strings: whether to convert timestamps to strings
                recommended for CSV and JSON output.
            do_log_filtered_values: option to record filtered
                values in log. NOTE this will greatly degrade performance
        """
        self.spark = spark
        self.settings = settings
        self.schema = schema
        self.timestamps_as_strings = timestamps_as_strings
        self.do_log_filtered_values = do_log_filtered_values

        if self.do_log_filtered_values is True:
            self.disable_snowflake_filter_pushdown()

        super().__init__()

    def get_empty_dataframe(self) -> DataFrame:
        field = [StructField("index", StringType(), True)]
        schema = StructType(field)
        sc = self.spark.sparkContext
        return self.spark.createDataFrame(sc.emptyRDD(), schema)

    def disable_snowflake_filter_pushdown(self):
        """
        snowflake pushdown obscures filtered ids
        because some filtering occurs in snowflake
        """
        sc = self.spark._sc
        try:
            snowflake = sc._jvm.net.snowflake.spark.snowflake
            snowflake.SnowflakeConnectorUtils.disablePushdownSession(
                sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate()
            )
        except TypeError:
            pass

    def replace_special_characters(self, col_name: str) -> Column:
        """
        Some data in snowflake contains null values for special
        characters. This function replaces special characters
        with a '?'
        """
        return F.regexp_replace(F.col(col_name), "[^a-zA-Z0-9 .,]+", "?")

    def get_filtered_values(
        self,
        input_df: DataFrame,
        output_df: DataFrame,
        col: str,
    ) -> List[Row]:
        """
        For each column in cols find the values that are filtered
        from input_df
        """
        return (
            input_df.join(output_df, col, how="left")
            .where(output_df[col].isNull())
            .select(col)
            .distinct()
            .collect()
        )

    def log_filtered_values(
        self,
        method_name: str,
        input_df: DataFrame,
        output_df: DataFrame,
        cols: List[str],
    ):
        """
        For each column in cols find the values that are filtered
        from input_df and records values in the log
        """
        logger = logging.getLogger()
        for col in cols:
            result = self.get_filtered_values(
                input_df,
                output_df,
                col,
            )

            logger.warning(f"{len(result)} {col} filtered by {method_name}")
            for row in result:
                message = f"{col} filtered by {method_name}: {row[col]}"
                logger.warning(message)

    def consolidate_dag(self, df: DataFrame) -> DataFrame:
        """
        Consolidates the physical plan for query so that
        it's string representation is shorter. Useful when
        error in org.codehaus.janino.CodeContext.flowAnalysis
        occur because many transformation occur in
        a single query.

        args:
            df: a spark DataFrame
        """
        return self.spark.createDataFrame(df.rdd, schema=df.schema)

    def enforce_schema_and_uniqueness(
        self, df: DataFrame, schema: HiveSchema
    ) -> DataFrame:
        """
        Prepares the table for output by only selecting the schema columns and
        calling distinct. Furthermore, it casts each column to the expected type,
        in case of missmatches.

        This does not cast to complex struct types.
        """
        df = df.select(schema.get_columns_list()).distinct()
        for col_name, data_type in schema.dict().items():
            # We do this check to make sure the schema type is a generic Hive Schema,
            # not a complex struct type.
            if type(data_type) == str:
                df = df.withColumn(col_name, F.col(col_name).cast(data_type))
        return df

    def get_most_recent_record(
        self, input_df: DataFrame, primary_key_cols: List, sort_cols: List
    ) -> DataFrame:
        """
        Obtain a dataframe with one row per primary_key cols
        the only row will be the most recent column
        based on the descending values in sort_cols
        """
        desc_sort_cols = [F.desc(col) for col in sort_cols]
        return (
            input_df.withColumn(
                "_row_number",
                F.row_number().over(
                    Window.partitionBy(*primary_key_cols).orderBy(
                        *desc_sort_cols
                    )
                ),
            )
            .where(F.col("_row_number") == 1)
            .drop("_row_number")
        )

    def get_bins_by_column(
        self,
        df: DataFrame,
        input_col: str,
        output_col: str,
        settings: Settings,
    ) -> DataFrame:
        """
        args:
            df: Spark DataFrame object.
            input_col: The input column name to send to the bucketizer.
            output_col: The output column name to generate from the bucketizer.
            settings: Settings object.
        Gets the bucket splits for creating a user defined output_col, from an
        input_col.
        """
        if (
            settings.bins_per_partition is not None
            and settings.output_partitions is not None
        ):
            max_bucket_number = (
                settings.output_partitions * settings.bins_per_partition
                + settings.bins_per_partition
            )
            splits = list(
                range(0, max_bucket_number, settings.bins_per_partition)
            )

            df = (
                Bucketizer(
                    splits=cast(List[float], splits),
                    inputCol=input_col,
                    outputCol=output_col,
                )
                .transform(df)
                .withColumn(output_col, F.col(output_col).cast("bigint"))
            )
        else:
            df = df.withColumn(output_col, F.lit(None).cast("bigint"))
        return df

    def run(self) -> DataFrame:
        pass

    def write(
        self,
        job_name: str,
        write_format: str,
        logger: Optional[logging.Logger],
        row_id_columns: Optional[List[str]] = None,
        hive_table_type: Optional[str] = None,
        do_repartition: bool = True,
        col_to_repartition_by: Optional[str] = None,
        partition_hdfs_by_col: Optional[str] = None,
        incremental_processing_type: Optional[str] = None,
        storage_timestamp_col: str = "storage_timestamp",
        write_mode: str = "overwrite",
        target_path: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> DataFrame:
        """
        Writes query result to an object store and if specified creates
        a managed or external table in Hive

        args:
            spark: a SparkSession object
            df: a pyspark dataframe
            schema: a HiveSchema object for the df to be written
            settings: a Settings object containing query and job settings
            job_name: used to generate Hive Table Name and hdfs path
            write_format: options are emptry string, hive, or hdfs
                if 'hive' then a managed table is created with parquet format
                if 'hdfs' then the files are written to hdfs and an external
                    table is created in hive from them
                if 'json' then the dataframe is written to hdfs as jsons
            logger: a logging.logger object
            row_id_columns: These columns function like a primary key. In cases of
                incremental updates the list of columns that will be used to uniquely
                identify a row and allow for updates of an existing row
            incremental_processing_type: string
            hive_table_type: either None, internal, or external.
            col_to_repartition_by: if specified then dataframe will be repartitioned
                by specified column and output partition number in settings
            partition_hdfs_by_col: if specified then dataframe will be repartitioned
                will partition the output hdfs file by the specified column
                and there will be a unique subdirectory for each unique value in
                the output object store directory
            write_mode: whether to overwrite or append a table
            target_path: the object store location of the output table, if not specified
                it will be determined by the settings
            table_name: if a hive_table_type is specified then the name that will be
                used for the table
        """

        writer = TableWriter(
            spark=self.spark,
            df=self.run(),
            schema=self.schema,
            settings=self.settings,
            job_name=job_name,
            write_format=write_format,
            logger=logger,
            row_id_columns=row_id_columns,
            hive_table_type=hive_table_type,
            do_repartition=do_repartition,
            col_to_repartition_by=col_to_repartition_by,
            partition_hdfs_by_col=partition_hdfs_by_col,
            incremental_processing_type=incremental_processing_type,
            storage_timestamp_col=storage_timestamp_col,
            write_mode=write_mode,
            target_path=target_path,
            table_name=table_name,
        )

        return writer.write()


class AuditQuery(Query):
    """
    Stores the date variables and settings used in a job rn
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        audit_schema = StructType(
            [
                StructField("run_id", StringType(), True),
                StructField("run_timestamp", StringType(), True),
                StructField("include_start_date", StringType(), True),
                StructField("include_end_date", StringType(), True),
                StructField("incremental_load_start_date", StringType(), True),
                StructField("incremental_load_end_date", StringType(), True),
                StructField("settings", StringType(), True),
            ]
        )

        audit_df = self.spark.createDataFrame(
            [
                (
                    str(self.settings.run_id),
                    str(datetime.now()),
                    str(self.settings.include_start_date),
                    str(self.settings.include_end_date),
                    str(self.settings.incremental_load_start_date),
                    str(self.settings.incremental_load_end_date),
                    str(self.settings.dict()),
                )
            ],
            audit_schema,
        )

        return audit_df.coalesce(1)


class DistinctValueQuery(Query):
    """
    Obtains a single df with distinct values
    for a list of columns (compound distinct)
    """

    def __init__(self, input_df: DataFrame, cols: List[str], **kwargs):
        self.input_df = input_df
        self.cols = cols

        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        return self.input_df.select(self.cols).distinct()


class CountKeysQuery(Query):
    """
    Obtains a single df with counts of different keys
    """

    def __init__(self, input_df: DataFrame, cols: List[str], **kwargs):
        self.input_df = input_df
        self.cols = cols

        super().__init__(**kwargs)

    def run(self) -> DataFrame:
        result_df = (
            self.get_empty_dataframe()
            .withColumn("key", F.lit(None))
            .withColumn("count", F.lit(None))
            .drop("index")
        )

        for col in self.cols:
            new_count_df = (
                self.input_df.select(col)
                .distinct()
                .agg(F.count(col).alias("count"))
                .withColumn("key", F.lit(col))
            ).select("key", "count")

            result_df = result_df.union(new_count_df)

        return result_df.where(F.col("key").isNotNull()).coalesce(1)
