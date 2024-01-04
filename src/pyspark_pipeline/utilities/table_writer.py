import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

from pyspark_pipeline.schemas import HiveSchema
from pyspark_pipeline.utilities.job_utils import read_hive_table
from pyspark_pipeline.utilities.query_utils import (
    format_ts_column_as_iso_string,
    manage_output_partitions,
)
from pyspark_pipeline.utilities.settings_utils import Settings, get_table_name

CSV_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss+0000"


class TableWriter:
    """
    Convenience class for writing a dataframe to an object
    store, creating managed or external tables and handling
    incremental updates.

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

    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame,
        schema: HiveSchema,
        settings: Settings,
        job_name: str,
        write_format: str,
        logger: Optional[logging.Logger],
        row_id_columns: Optional[List[str]] = None,
        hive_table_type: Optional[str] = None,
        do_repartition: bool = True,
        col_to_repartition_by: Optional[str] = None,
        incremental_processing_type: Optional[str] = None,
        storage_timestamp_col: str = "storage_timestamp",
        partition_hdfs_by_col: Optional[str] = None,
        write_mode: str = "overwrite",
        target_path: Optional[str] = None,
        table_name: Optional[str] = None,
    ):
        self._spark = spark
        self._df = df
        self._settings = settings
        self._schema = schema
        self._job_name = job_name
        self._write_format = write_format
        self._logger = logger
        if row_id_columns is not None:
            self._row_id_columns = row_id_columns
        else:
            self._row_id_columns = []
        self._hive_table_type = hive_table_type
        self._do_repartition = do_repartition
        self._col_to_repartition_by = col_to_repartition_by
        self._incremental_processing_type = incremental_processing_type
        self._storage_timestamp_col = storage_timestamp_col
        self._partition_hdfs_by_col = partition_hdfs_by_col
        self._write_mode = write_mode
        if target_path is None:
            self._target_path = settings.target_path + job_name
        else:
            self._target_path = target_path
        if table_name is None and hive_table_type is not None:
            self._table_name = get_table_name(job_name, settings)
        else:
            self._table_name = str(table_name)

        self.check_table_writer_args(
            self._write_format, self._write_mode, self._hive_table_type
        )

    def check_table_writer_args(
        self,
        write_format: Optional[str],
        write_mode: str,
        hive_table_type: Optional[str],
        **kwargs: Dict[Any, Any],
    ) -> None:
        """
        Validates arguments to TableWriter
        """
        type_options = (None, "internal", "external")
        if hive_table_type not in type_options:
            raise ValueError(
                f"write location must be either {type_options} not {hive_table_type}"
            )
        write_format_options = (
            None,
            "",
            "hive",
            "csv",
            "json",
            "parquet",
        )

        if write_format not in write_format_options:
            raise ValueError(
                f"write format must be either {write_format_options} not {write_format}"
            )

        write_mode_options = ("append", "overwrite")

        if write_mode not in write_mode_options:
            raise ValueError(
                f"write_mode must be either {write_mode_options} not {write_mode}"
            )

        internal_format_options = (
            "csv",
            "json",
            "parquet",
            "hive",
        )
        if (hive_table_type == "internal") and (
            write_format not in internal_format_options
        ):
            raise ValueError(
                f"You must specify either {internal_format_options} as write location "
                "if hive_table_type is not None"
            )
        external_format_options = (
            "csv",
            "json",
            "parquet",
        )
        if (hive_table_type == "external") and (
            write_format not in external_format_options
        ):
            raise ValueError(
                f"You must specify either {external_format_options} as write location "
                "if hive_table_type is not None"
            )

        if (
            self._incremental_processing_type is not None
            and not self._row_id_columns
        ):
            raise ValueError(
                "Incremental updates require a list of columns that will function"
                " as a primary key for the updated table."
            )

    def drop_table(self, table_name: str) -> DataFrame:
        """
        Drop a specified table from Hive

        args:
            hive_table: a string specifying the table to be dropped
        """
        return self._spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def create_table(
        self,
        table_name: str,
    ) -> DataFrame:
        """
        Creates a table from a specified EventsSchema object

        args:
            table_name: a string specifying the table to be dropped
        """
        schema_str = self._schema.get_schema_string()

        return self._spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name}"
            f"({schema_str})"
            "USING hive OPTIONS(fileFormat 'PARQUET')"
        )

    def create_hive_table_type(
        self,
        table_name: str,
        target_path: str,
        file_type: str = "csv",
        df_name: str = "DataFrame",
    ) -> DataFrame:
        """
        Creates an exteral table from a specified EventsSchema object
        and an target_path

        args:
            table_name: a string specifying the table to be dropped
            target_path: path hdfs file will be written to
            file_type: file type of underlying hdfs data
            df_name: the name of the spark dataframe being written
        """
        if self._logger:
            logging.info(
                "Writing %s_df as Hive External table: %s"
                % (df_name, table_name)
            )
        schema = self._schema.copy()

        file_type = file_type.lower()

        if self._partition_hdfs_by_col:
            col = self._partition_hdfs_by_col
            dtype = self._schema.dict()[col]
            setattr(schema, col, None)
            partitioned_by = f"PARTITIONED BY ({col} {dtype})"
        else:
            partitioned_by = ""

        schema_str = schema.get_schema_string()

        if file_type == "csv":
            result = self._spark.sql(
                f"CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} "
                f"({schema_str}) "
                "ROW FORMAT SERDE "
                "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' "
                "WITH SERDEPROPERTIES ("
                """"field.delim"="\t", """
                """ "line.delim"="\n", """
                """"quoteChar"= "\\"", """
                f""""timestamp.formats"= "{CSV_TIMESTAMP_FORMAT}") """
                "STORED AS TEXTFILE "
                f" {partitioned_by} "
                f"LOCATION '{target_path}' "
                "TBLPROPERTIES ('skip.header.line.count' = '1', "
                "'serialization.null.format'='')"
            )

        elif file_type == "json":
            result = self._spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_name} "
                f"({schema_str}) "
                f" USING json "
                f" {partitioned_by} "
                f"LOCATION '{target_path}' "
            )
        elif file_type == "parquet":
            result = self._spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_name} "
                f"({schema_str}) "
                f" USING parquet "
                f" {partitioned_by} "
                f"LOCATION '{target_path}' "
            )
        else:
            raise ValueError(
                "file_type must either be 'csv', 'json', or 'parquet'"
            )

        if self._partition_hdfs_by_col:
            self._spark.sql(f"MSCK REPAIR TABLE {table_name}")

        return result

    def rename_hive_table(self, old_name: str, new_name: str) -> DataFrame:
        """
        renames a hive table

        args:
            old_name: original hive table name
            new_name: new hive table name
        """
        return self._spark.sql(f"ALTER TABLE {old_name} RENAME TO {new_name}")

    def write_to_hive(
        self,
        df: DataFrame,
        table_name: str,
        df_name: str = "DataFrame",
    ) -> DataFrame:
        """
        Inserts rows from a DataFrame into a specified hive table

        NOTE: for complex columns (struct and array) the order of
        the columns in the query to create the DataFrame must be
        the same as the order of columns as they appear in
        the output of get_schema_string

        args:
            df: a Spark DataFrame
            hive_table_name: the hive table name (including database) of
                the targe table
            logger: an optional logger
            df_name: the name of the spark dataframe being written
        """
        if self._logger:
            logging.info(
                "Writing %s as Hive Internal table: %s" % (df_name, table_name)
            )
        do_overwrite = False

        if self._write_mode == "overwrite":
            do_overwrite = True
            self.drop_table(table_name)

        try:
            self.create_table(table_name)
        except AnalysisException:
            pass

        try:
            df.write.insertInto(table_name, overwrite=do_overwrite)
        except AnalysisException as exception:
            logging.warning(exception)
            logging.warning(
                "Table hdfs location is missing it may be in trash. "
                "Recreating the table with just the new data."
            )
            self.drop_table(table_name)
            self.create_table(table_name)
            df.write.insertInto(table_name, overwrite=do_overwrite)

        return df

    def write_to_hdfs(
        self,
        df: DataFrame,
        target_path: str,
        df_name: str = "DataFrame",
    ) -> DataFrame:
        """
        Writes a Spark DataFrame to hdfs as GZIPPED csv, json

        args:
            df: a Spark DataFrame
            target_path: the hdfs path where the csv will be written
            logger: an optional logger
            df_name: the name of the spark dataframe being written
        """
        file_type = self._write_format.lower()
        if self._logger:
            logging.info(
                "Writing %s to hdfs as %s at %s"
                % (df_name, file_type, target_path)
            )
        if file_type in ("csv", "json"):
            for col_name, data_type in self._schema.dict().items():
                if str(data_type).lower() == "timestamp":
                    if self._logger:
                        logging.info(
                            "Converting %s.%s to %s compatible format"
                            % (df_name, col_name, file_type)
                        )
                    df = format_ts_column_as_iso_string(df, col_name, file_type)
        writer = df.write.mode(self._write_mode)

        if self._partition_hdfs_by_col is not None:
            writer = writer.partitionBy(self._partition_hdfs_by_col)

        if file_type == "csv":
            writer.option("compression", "gzip").csv(
                target_path, sep="\t", emptyValue="", header=True
            )

        elif file_type == "json":
            writer.option("compression", "gzip").json(target_path)

        elif file_type == "parquet":
            writer.parquet(target_path)

        else:
            raise ValueError(
                "file_type must either be 'csv', 'parquet', or 'json'"
            )

        return df

    @staticmethod
    def load_table(
        spark: SparkSession,
        write_format: str,
        table_name: str,
        target_path: str,
        spark_schema: StructType,
    ) -> DataFrame:
        """
        Loads an existing table created with pyspark-pipeline. Uses hdfs path unless
        the method is hive

        args:
            spark: SparkSession object
            write_format: hdfs file format
            table_name: hive table <schema>.<table> only used
            target_path: hdfs path for table
            spark_schema: a spark DataFrame schema
        """

        if write_format == "csv":
            options = {
                "format": "csv",
                "header": True,
                "compression": "gzip",
                "sep": "\t",
                "emptyValue": "",
                "inferSchema": True,
                "timestampFormat": CSV_TIMESTAMP_FORMAT,
            }
        elif write_format == "json":
            options = {
                "format": "json",
                "compression": "gzip",
                "schema": spark_schema,
            }
        else:
            options = {
                "format": "parquet",
                "schema": spark_schema,
            }

        if write_format == "hive":
            old_df = read_hive_table(spark, table_name)
        else:
            old_df = spark.read.load(target_path, **options)  # type: ignore

        # without inferSchema partitioned CSV tables are
        # not correctly read into spark so the schema
        # cannot be used in the load method and the
        # column datatypes need to be validated
        for col in spark_schema:
            if old_df.schema[col.name] != col.dataType:
                old_df = old_df.withColumn(
                    col.name, F.col(col.name).cast(col.dataType)
                )

        return old_df

    def incremental_update(
        self,
        df: DataFrame,
        table_name: str,
        target_path: str,
        **kwargs: Dict[Any, Any],
    ) -> DataFrame:
        """
        Adds new rows to an existing hive table, drops rows that
        were updated (same values in join columns, but older storage_timestamps)

        args:
            df: a Spark DataFrame
            table_name: the hive location of the original table
            target_path: the hdfs path of the original table
        """
        if self._row_id_columns is None:
            self._row_id_columns = []
        if self._logger:
            logging.info("Performing upsert of table %s" % table_name)

        w = Window.partitionBy(*self._row_id_columns).orderBy(
            F.desc(self._storage_timestamp_col)
        )

        old_df = self.load_table(
            self._spark, self._write_format, table_name, target_path, df.schema
        )

        union_df = old_df.union(df.select(old_df.columns))
        return (
            union_df.withColumn("_row_number", F.row_number().over(w))
            .where(F.col("_row_number") == 1)
            .drop("_row_number")
        )

    def write_to_object_store_and_create_table(
        self,
        df: DataFrame,
        table_name: str,
        **kwargs: Dict[Any, Any],
    ) -> DataFrame:
        """
        Writes dataframe to hdfs in specified format and
        creates either an external or internal table in Hive

        args:
            df: a spark DataFrame
            table_name: name of table to be updated
        """
        df = manage_output_partitions(
            df,
            self._do_repartition,
            self._col_to_repartition_by,
            self._settings,
        )

        if self._hive_table_type == "internal":
            df = df.cache()

        self.write_to_hdfs(df, self._target_path, self._job_name)

        if self._hive_table_type == "external":
            self.drop_table(table_name)
            if self._write_format in ("csv", "json") and self._logger:
                self._logger.warning(
                    "Using External tables on dataframes with struct "
                    "columns unless using parquet because the current "
                    "version of Hue makes "
                    "querying the resulting tables impractical and it "
                    "is recommended to tables with struct columns as "
                    "hive internal tables or external table in parquet "
                    "format. "
                )
            if (
                self._settings.dict().get("output_partitions", 0) > 1000
                and self._logger
            ):
                self._logger.warning(
                    "Using External tables with more than 1000 "
                    "partitions is not recommended because the "
                    "discovery of the partitions can be very slow "
                )
            self.create_hive_table_type(
                table_name,
                self._target_path,
                self._write_format,
                self._job_name,
            )
        elif self._hive_table_type == "internal":
            self.write_to_hive(df, table_name, self._job_name)

        return df

    def update_open_table(
        self,
        table_name: str,
        df: DataFrame,
    ) -> DataFrame:
        """
        Allows for an update of a table spark is already reading from

        args:
                table_name: the name of a hive table
                df: a spark dataframe
        """
        self.write_to_hive(df, table_name + "_update")
        self.drop_table(table_name)
        self.rename_hive_table(table_name + "_update", table_name)

        return df

    def delete_object_store_path(
        self,
        path: str,
    ):
        """
        Uses the py4j jvm FileSystem object to delete an object store
        directory

        args:
            old_path: the path being renamed
            new_path: the new path name
        """
        if self._logger is not None:
            logging.info(f"deleting {path}")

        url_parts = urlparse(path, allow_fragments=False)
        if url_parts.scheme in ["s3a", "s3"]:
            raise ValueError("delete from S3 path not implemented")

        elif url_parts.scheme == "hdfs":
            sc = self._spark.sparkContext
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(  # type: ignore
                sc._jsc.hadoopConfiguration()  # type: ignore
            )
            fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)  # type: ignore

        elif url_parts.scheme == "file":
            raise ValueError("delete from file system not implemented")

        else:

            raise ValueError("url parts not defined")

    def rename_object_store_path(
        self,
        old_path: str,
        new_path: str,
    ):
        """
        Uses the py4j jvm FileSystem object to rename an object store
        directory

        args:
            spark: SparkSession,
            old_path: the path being renamed
            new_path: the new path name
        """
        if self._logger is not None:
            logging.info(f"renaming {old_path} to {new_path}")

        sc = self._spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(  # type: ignore
            sc._jsc.hadoopConfiguration()  # type: ignore
        )
        # if the directory exists it will be made as a sub dir,
        # but we intend an overwrite behavior
        fs.delete(sc._jvm.org.apache.hadoop.fs.Path(new_path), True)  # type: ignore

        fs.rename(
            sc._jvm.org.apache.hadoop.fs.Path(old_path),  # type: ignore
            sc._jvm.org.apache.hadoop.fs.Path(new_path),  # type: ignore
        )

    def update_open_object_store_path(
        self,
        df: DataFrame,
        target_path: str,
    ) -> None:
        """
        Performs update of a data frame at an open
        object store path

        args:
            df: spark dataframe to be written to object store path
            target_path: object store path
        """
        update_path = target_path + "_update"
        self.write_to_hdfs(df, update_path, self._job_name)

        if target_path.startswith("s3"):
            # ugly fix right here because otherwise the df doesn't get
            # extracted correctly
            if self._write_format == "csv":
                df = (
                    self._spark.read.option("delimiter", "\t")
                    .option("header", "true")
                    .option("timestampFormat", CSV_TIMESTAMP_FORMAT)
                    .csv(update_path)
                )
            else:
                df = self._spark.read.format(self._write_format).load(
                    update_path
                )
            self.write_to_hdfs(df, target_path, self._job_name)
        else:
            self.rename_object_store_path(update_path, target_path)

    def write_incremental_update(
        self,
        df: DataFrame,
        table_name: str,
        **kwargs: Dict[Any, Any],
    ) -> DataFrame:
        """
        Performs upsert of an existing internal or external table.

        args:
            df: input spark dataframe
            table_name: name of table to be updated
        """
        df = self.incremental_update(df, table_name, self._target_path)

        if self._write_format == "hive":
            self.update_open_table(table_name, df)
            return df

        df = manage_output_partitions(
            df,
            self._do_repartition,
            self._col_to_repartition_by,
            self._settings,
        )

        self.update_open_object_store_path(df, self._target_path)

        if self._hive_table_type is not None:
            self._spark.sql(f"REFRESH TABLE {table_name}")
        # spark has an issue with looking for specific file names
        # in the hdfs path. This redefines the dataframe as the files
        # in the new location.
        df = self._spark.read.format(self._write_format).load(self._target_path)

        return df

    def write(self) -> DataFrame:
        """
        Execute a query and write table to a specified location
        """
        start_time = datetime.now()

        if self._logger:
            logging.info(
                "Starting %s query at %s" % (self._job_name, start_time)
            )

        if self._incremental_processing_type == "update":
            df = self.write_incremental_update(self._df, self._table_name)

        elif self._write_format == "hive":
            df = self.write_to_hive(self._df, self._table_name, self._job_name)

        elif self._write_format in ("csv", "json", "parquet"):
            df = self.write_to_object_store_and_create_table(
                self._df, self._table_name
            )

        else:
            if self._logger:
                logging.info(
                    "No write location specified, %s_df will not be persisted"
                    % self._job_name
                )

        if self._logger:
            end_time = datetime.now()
            total_time = round((end_time - start_time).total_seconds() / 60, 3)
            logging.info("Finished %s query at %s" % (self._job_name, end_time))
            logging.info(
                "%s query run time (m) %s" % (self._job_name, total_time)
            )

        return df
