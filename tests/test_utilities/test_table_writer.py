from datetime import datetime, timezone
from random import randint
from typing import Generator, Tuple
from urllib.parse import urlparse

import boto3  # type: ignore
import pyspark.sql.functions as F
import pytest
from moto import mock_s3  # type: ignore
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException
from pyspark_test import assert_pyspark_df_equal  # type: ignore

from pyspark_pipeline.queries import Query
from pyspark_pipeline.schemas import HiveSchema
from pyspark_pipeline.utilities.logging_utils import get_logger
from pyspark_pipeline.utilities.settings_utils import Settings, get_table_name
from pyspark_pipeline.utilities.table_writer import (
    CSV_TIMESTAMP_FORMAT,
    TableWriter,
)

TEST_BUCKET = "spark-query-app-fake-s3"
TARGET_PATH = f"s3a://{TEST_BUCKET}/test-table-writer"
TEST_DB_NAME = "fake_pyspark_pipeline_database"
TEST_AWS_URL = "http://127.0.0.1:5000"


class IncrementalMockTableSchema(HiveSchema):
    """
    A dataclass for testing hive_utils
    """

    storage_timestamp = "timestamp"
    string_col = "string"
    id_col = "string"


class MockTableSchema(HiveSchema):
    """
    A dataclass for testing hive_utils
    """

    timestamp_col = "timestamp"
    string_col = "string"
    int_col = "bigint"
    varchar_col = "varchar(32)"


class MockQuery(Query):
    def run(self) -> DataFrame:
        schema = StructType(
            [
                StructField("timestamp_col", TimestampType(), True),
                StructField("string_col", StringType(), True),
                StructField("int_col", LongType(), True),
                StructField("varchar_col", StringType(), True),
            ]
        )
        df = self.spark.createDataFrame(
            [
                (
                    datetime(2000, 1, 1, tzinfo=timezone.utc),
                    "string",
                    1,
                    "varchar",
                ),
            ],
            schema,
        )

        return df


class MockIncrementalQuery(Query):
    def run(self) -> DataFrame:
        schema = StructType(
            [
                StructField("storage_timestamp", TimestampType(), True),
                StructField("string_col", StringType(), True),
                StructField("id_col", StringType(), True),
            ]
        )

        return self.spark.createDataFrame(
            [
                (datetime(1999, 1, 2), "thisrow", "1"),
                (datetime(1999, 1, 2), "thisrow", "3"),
            ],
            schema,
        )


@mock_s3
class TestUnit_table_writer:
    def setup_class():  # type: ignore
        conn = boto3.resource(
            "s3", region_name="us-east-1", endpoint_url=TEST_AWS_URL
        )
        conn.create_bucket(Bucket=TEST_BUCKET)

    @pytest.fixture()
    def expected_incremental_update_df(
        self, s3_spark: SparkSession
    ) -> DataFrame:
        schema = StructType(
            [
                StructField("storage_timestamp", TimestampType(), True),
                StructField("string_col", StringType(), True),
                StructField("id_col", StringType(), True),
            ]
        )
        return s3_spark.createDataFrame(
            [
                (datetime(1999, 1, 2), "thisrow", "1"),
                (datetime(1999, 1, 1), "thisrow", "2"),
                (datetime(1999, 1, 2), "thisrow", "3"),
            ],
            schema,
        )

    @pytest.fixture()
    def mock_df(
        self, s3_spark: SparkSession, settings_obj_aws: Settings
    ) -> DataFrame:
        df = MockQuery(
            s3_spark,
            settings_obj_aws,
            MockTableSchema(),
        ).run()

        return df

    @pytest.fixture(scope="function")
    def create_table(self, s3_spark: SparkSession) -> Generator:
        """
        create a test table and delete the table upon completion

        the table is of the form {TEST_DB_NAME}.table_{randint(0,10000)}
        """
        # create a random table name
        job_name = f"table_{randint(0,1000000)}"
        table_name = f"{TEST_DB_NAME}.{job_name}"

        schema = StructType(
            [
                StructField("storage_timestamp", TimestampType(), True),
                StructField("string_col", StringType(), True),
                StructField("id_col", StringType(), True),
            ]
        )

        initial_df = s3_spark.createDataFrame(
            [
                (datetime(1999, 1, 1), "notthisrow", "1"),
                (datetime(1999, 1, 1), "thisrow", "2"),
            ],
            schema,
        )
        schema_str = IncrementalMockTableSchema().get_schema_string()

        s3_spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name}"
            f"({schema_str})"
            "USING hive OPTIONS(fileFormat 'PARQUET')"
        )
        initial_df.write.insertInto(table_name, overwrite=True)

        yield job_name, table_name, initial_df

        # delete table when no longer being used
        # Checking if "is exists" because some tables are being renamed
        # inside the tests
        s3_spark.sql(f"DROP TABLE IF EXISTS `{table_name}`")

    @pytest.fixture()
    def expected_append_df(
        self,
        s3_spark: SparkSession,
        create_table: Tuple[str, str, DataFrame],
        settings_obj_aws: Settings,
    ):
        query = MockIncrementalQuery(
            s3_spark,
            settings_obj_aws,
            IncrementalMockTableSchema(),
        )

        _, _, df = create_table
        return query.run().union(df)

    @pytest.fixture()
    def table_writer(self, s3_spark, settings_obj_aws) -> TableWriter:
        query = MockQuery(
            spark=s3_spark,
            settings=settings_obj_aws,
            schema=MockTableSchema(),
            timestamps_as_strings=False,
        )
        return TableWriter(
            spark=s3_spark,
            df=query.run(),
            schema=MockTableSchema(),
            settings=settings_obj_aws,
            job_name="mock_job",
            write_format="csv",
            logger=get_logger("fake_app", datetime.now()),
            row_id_columns=["col"],
            hive_table_type="internal",
            col_to_repartition_by="string_col",
            partition_hdfs_by_col="string_col",
            incremental_processing_type=get_logger("fake_app", datetime.now()),
            storage_timestamp_col="storage_timestamp",
        )

    def test_create_table(
        self, s3_spark: SparkSession, table_writer: TableWriter
    ):
        table_name = f"{TEST_DB_NAME}.create_table"

        with pytest.raises(AnalysisException):
            s3_spark.table(table_name)

        # test create_table
        table_writer.create_table(table_name)

        mock_df = s3_spark.table(table_name)

        expected_schema = StructType(
            [
                StructField("timestamp_col", TimestampType(), True),
                StructField("string_col", StringType(), True),
                StructField("int_col", LongType(), True),
                StructField("varchar_col", StringType(), True),
            ]
        )

        assert str(mock_df.schema) == str(expected_schema)

    def test_drop_table(
        self, s3_spark: SparkSession, table_writer: TableWriter
    ):
        table_name = f"{TEST_DB_NAME}.drop_table"

        s3_spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} (test string)")

        s3_spark.table(table_name)

        table_writer.drop_table(table_name)

        with pytest.raises(AnalysisException):
            s3_spark.table(table_name)

    @pytest.mark.parametrize(
        "write_format",
        [
            "csv",
            "json",
        ],
    )
    def test_create_hive_table_type(
        self,
        s3_spark: SparkSession,
        mock_df: DataFrame,
        table_writer: TableWriter,
        write_format: str,
    ):
        tname = f"create_hive_table_{write_format}"
        table_name = f"{TEST_DB_NAME}.{tname}"
        s3_spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        target_path = f"{TARGET_PATH}/{tname}/"

        with pytest.raises(AnalysisException):
            s3_spark.table(table_name)

        schema = StructType(
            [
                StructField("timestamp_col", TimestampType(), True),
                StructField("int_col", LongType(), True),
                StructField("varchar_col", StringType(), True),
                StructField("string_col", StringType(), True),
            ]
        )

        if write_format == "csv":
            # datetime columns need to be cast as strings with iso formatting
            # to be correctly read in as timestamps
            csv_df = mock_df.withColumn(
                "timestamp_col",
                F.date_format(F.col("timestamp_col"), CSV_TIMESTAMP_FORMAT),
            )
            (
                csv_df.write.mode("overwrite")
                .option("header", "False")
                .option("compression", "gzip")
                .partitionBy("string_col")
                .csv(target_path, sep="\t", emptyValue="")
            )

        elif write_format == "json":
            json_df = mock_df.withColumn(
                "timestamp_col",
                F.date_format(F.col("timestamp_col"), "yyyy-MM-dd HH:mm:ss"),
            )

            (
                json_df.write.mode("overwrite")
                .option("compression", "gzip")
                .partitionBy("string_col")
                .json(target_path)
            )

        table_writer.create_hive_table_type(
            table_name, target_path, write_format
        )
        actual_df = s3_spark.table(table_name)

        assert str(actual_df.schema) == str(schema)
        assert_pyspark_df_equal(actual_df, mock_df)

        s3_spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_rename_hive_table(
        self,
        s3_spark: SparkSession,
        table_writer: TableWriter,
        create_table: Tuple,
    ):
        s3_spark.sql(f"use {TEST_DB_NAME}")
        final_table_name = f"{TEST_DB_NAME}.rename_hive_table"
        s3_spark.sql(f"DROP TABLE IF EXISTS {final_table_name}")

        _, initial_table_name, expected_df = create_table

        # Assert that the table doesn't exist
        with pytest.raises(AnalysisException):
            s3_spark.table(final_table_name)

        table_writer.rename_hive_table(initial_table_name, final_table_name)
        actual_df = s3_spark.table(final_table_name)

        assert_pyspark_df_equal(actual_df, expected_df)

        # Assert that the table doesn't exist
        with pytest.raises(AnalysisException):
            s3_spark.table(initial_table_name)

        s3_spark.sql(f"DROP TABLE {final_table_name}")

    def test_write_to_hive(
        self,
        s3_spark: SparkSession,
        mock_df: DataFrame,
        table_writer: TableWriter,
    ):
        tname = "write_to_hive"
        table_name = f"{TEST_DB_NAME}.{tname}"

        with pytest.raises(AnalysisException):
            s3_spark.table(table_name)

        table_writer.write_to_hive(mock_df, table_name)

        actual_df = s3_spark.table(table_name)
        assert_pyspark_df_equal(actual_df, mock_df)

        s3_spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    @pytest.mark.parametrize("write_format", ["csv", "json", "parquet"])
    def test_write_to_hdfs(
        self,
        s3_spark: SparkSession,
        mock_df: DataFrame,
        table_writer: TableWriter,
        write_format: str,
    ):
        path = f"{TARGET_PATH}/write_to_hdfs_{write_format}/"
        table_writer._write_format = write_format
        table_writer.write_to_hdfs(mock_df, path)

        schema = StructType(
            [
                StructField("timestamp_col", StringType(), True),
                StructField("int_col", LongType(), True),
                StructField("varchar_col", StringType(), True),
                StructField("string_col", StringType(), True),
            ]
        )

        if write_format == "csv":
            actual_df = (
                s3_spark.read.option("delimiter", "\t")
                .option("header", "true")
                .option("timestampFormat", CSV_TIMESTAMP_FORMAT)
                .csv(path, schema=schema)
            ).withColumn(
                "timestamp_col", F.col("timestamp_col").cast("timestamp")
            )
        elif write_format in ["json", "parquet"]:
            actual_df = TableWriter.load_table(
                s3_spark, write_format, "", path, mock_df.schema
            )

        assert_pyspark_df_equal(actual_df, mock_df)

    def test_write_to_hdfs_bad_format(
        self,
        s3_spark: SparkSession,
        mock_df: DataFrame,
        table_writer: TableWriter,
    ):
        write_format = "foo"
        path = f"{TARGET_PATH}/write_to_hdfs_bad_format/"
        table_writer._write_format = write_format
        with pytest.raises(ValueError):
            table_writer.write_to_hdfs(mock_df, path)

    def test_incremental_update(
        self,
        s3_spark,
        create_table: Tuple[str, str, DataFrame],
        settings_obj_aws: Settings,
        expected_incremental_update_df: DataFrame,
    ):
        _, table_name, _ = create_table

        query = MockIncrementalQuery(
            s3_spark,
            settings_obj_aws,
            IncrementalMockTableSchema(),
        )

        table_writer = TableWriter(
            spark=s3_spark,
            df=query.run(),
            schema=IncrementalMockTableSchema(),
            settings=settings_obj_aws,
            job_name="cavendish",
            write_format="hive",
            logger=get_logger("fake_app", datetime.now()),
            row_id_columns=["id_col"],
            hive_table_type=None,
            col_to_repartition_by=None,
            incremental_processing_type="update",
            storage_timestamp_col="storage_timestamp",
        )

        df = query.run()

        actual_df = table_writer.incremental_update(
            df,
            table_name,
            target_path="",
        )

        assert_pyspark_df_equal(
            actual_df.orderBy("id_col"),
            expected_incremental_update_df.orderBy("id_col"),
        )

    def test_update_open_table(
        self,
        s3_spark: SparkSession,
        create_table: Tuple[str, str, DataFrame],
        settings_obj_aws: Settings,
        table_writer: TableWriter,
    ):
        _, table_name, _ = create_table
        open_table = s3_spark.table(table_name)
        open_table.cache()

        query = MockIncrementalQuery(
            s3_spark, settings_obj_aws, IncrementalMockTableSchema()
        )

        expected_df = query.run()

        table_writer._schema = IncrementalMockTableSchema()

        table_writer.update_open_table(
            table_name,
            expected_df,
        )

        actual_df = s3_spark.table(table_name)
        assert_pyspark_df_equal(actual_df, expected_df)

    @pytest.mark.parametrize(
        "arg1, arg2, arg3",
        [
            ("hive", "overwrite", None),
            ("json", "overwrite", None),
            ("csv", "overwrite", None),
            ("parquet", "overwrite", None),
            (None, "append", None),
            ("", "append", None),
            ("json", "overwrite", "internal"),
            ("csv", "overwrite", "external"),
            ("parquet", "overwrite", "internal"),
        ],
    )
    def test_check_table_writer_args(
        self, table_writer: TableWriter, arg1, arg2, arg3
    ):
        # below should throw no errors
        table_writer.check_table_writer_args(arg1, arg2, arg3)

    @pytest.mark.parametrize(
        "arg1, arg2, arg3",
        [
            ("banana", "overwrite", "internal"),
            (None, "overwrite", "internal"),
            ("", "overwrite", "internal"),
            ("hive", "overwrite", "external"),
            ("", "", None),
            ("hive", "xxxx", None),
        ],
    )
    def test_check_table_writer_bad_args(
        self, table_writer: TableWriter, arg1, arg2, arg3
    ):
        with pytest.raises(ValueError):
            table_writer.check_table_writer_args(arg1, arg2, arg3)

    def test_write_to_object_store_and_create_table(
        self,
        s3_spark: SparkSession,
        settings_obj_aws: Settings,
        mock_df: DataFrame,
        table_writer: TableWriter,
    ):
        tname = "write_to_object_store_and_create_table"
        target_path = f"{TARGET_PATH}/{tname}"
        table_writer._target_path = target_path

        job_name = f"{tname}"
        table_name = get_table_name(job_name, settings_obj_aws)
        table_writer.drop_table(table_name)

        table_writer._hive_table_type = "external"
        table_writer._write_format = "parquet"
        table_writer.write_to_object_store_and_create_table(
            mock_df,
            table_name,
        )

        actual_df = s3_spark.table(table_name)

        assert_pyspark_df_equal(actual_df, mock_df)

        table_writer.drop_table(table_name)

        table_writer._hive_table_type = "internal"
        table_writer.write_to_object_store_and_create_table(
            mock_df,
            table_name,
        )

        actual_df = s3_spark.table(table_name)
        assert_pyspark_df_equal(actual_df, mock_df)

        table_writer.drop_table(table_name)

    def test_write_hive(
        self,
        s3_spark: SparkSession,
        settings_obj_aws: Settings,
        mock_df: DataFrame,
        table_writer: TableWriter,
    ):
        job_name = "write_hive"
        table_name = get_table_name(job_name, settings_obj_aws)
        table_writer.drop_table(table_name)
        table_writer._table_name = table_name
        table_writer._job_name = job_name
        table_writer._write_format = "hive"
        table_writer.write()

        actual_df = s3_spark.table(table_name)

        assert_pyspark_df_equal(actual_df, mock_df)

    @pytest.mark.parametrize("write_format", ["csv", "json", "parquet"])
    def test_write_write_format(
        self,
        s3_spark: SparkSession,
        settings_obj_aws: Settings,
        mock_df: DataFrame,
        table_writer: TableWriter,
        write_format: str,
    ):
        job_name = f"write_write_format_{write_format}"
        target_path = f"{TARGET_PATH}{job_name}"
        table_writer._settings.target_path = target_path

        job_name = f"write_hdfs_{write_format}"
        table_name = get_table_name(job_name, settings_obj_aws)
        table_writer._target_path = target_path
        table_writer._table_name = table_name
        table_writer.drop_table(table_name)

        table_writer._hive_table_type = "external"
        table_writer._job_name = job_name
        table_writer._write_format = write_format
        table_writer.write()

        if write_format == "csv":
            actual_df = s3_spark.table(table_name).where(
                (F.col("string_col") != "string_col")
                & (F.col("varchar_col") != "varchar_col")
            )
        elif write_format in ["json", "parquet"]:
            actual_df = TableWriter.load_table(
                s3_spark, write_format, "", target_path, mock_df.schema
            )

        assert_pyspark_df_equal(actual_df, mock_df)

    @pytest.mark.parametrize("write_format", ["csv", "json", "parquet"])
    def test_load_table_from_data_store(
        self,
        s3_spark: SparkSession,
        mock_df: DataFrame,
        table_writer: TableWriter,
        write_format: str,
    ):
        path = f"{TARGET_PATH}_load_table_from_data_store/"
        table_writer._write_format = write_format
        table_writer.write_to_hdfs(mock_df, path)

        actual_df = table_writer.load_table(
            s3_spark, write_format, "", path, mock_df.schema
        )
        assert_pyspark_df_equal(
            actual_df.orderBy("string_col"),
            mock_df.orderBy("string_col"),
        )

    def test_write_incremental_update_hive(
        self,
        s3_spark: SparkSession,
        settings_obj_aws: Settings,
        create_table: Tuple[str, str, DataFrame],
        expected_incremental_update_df: DataFrame,
        table_writer: TableWriter,
    ):
        job_name, table_name, _ = create_table

        query = MockIncrementalQuery(
            s3_spark,
            settings_obj_aws,
            IncrementalMockTableSchema(),
        )
        df = query.run()
        table_writer._job_name = job_name
        table_writer._schema = query.schema
        table_writer._write_format = "hive"
        table_writer._row_id_columns = ["id_col"]

        table_writer.write_incremental_update(df, table_name)

        actual_df = s3_spark.table(table_name)

        assert_pyspark_df_equal(
            actual_df.orderBy("id_col"),
            expected_incremental_update_df.orderBy("id_col"),
        )

        table_writer.drop_table(table_name)

    @pytest.mark.parametrize("write_format", ["csv", "json", "parquet"])
    def test_write_incremental_update_hdfs(
        self,
        s3_spark: SparkSession,
        settings_obj_aws: Settings,
        create_table: Tuple[str, str, DataFrame],
        expected_incremental_update_df: DataFrame,
        table_writer: TableWriter,
        write_format: str,
    ):
        schema = IncrementalMockTableSchema()
        query = MockIncrementalQuery(
            s3_spark,
            settings_obj_aws,
            schema,
        )

        job_name = f"incremental_update_hdfs_{write_format}"
        query_target_path = f"{TARGET_PATH}/{job_name}_query"
        query.settings.target_path = query_target_path

        job_name, table_name, initial_df = create_table
        initial_path = f"{TARGET_PATH}/{job_name}_initial"

        table_writer._job_name = job_name
        table_writer._schema = schema
        table_writer._row_id_columns = ["id_col"]
        table_writer._write_format = write_format
        table_writer._hive_table_type = None
        table_name = get_table_name(job_name, settings_obj_aws)
        table_writer._target_path = initial_path
        table_writer._table_name = table_name

        table_writer.write_to_hdfs(initial_df, initial_path, job_name)

        update_df = query.run()

        table_writer.write_incremental_update(update_df, table_name)

        actual_df = TableWriter.load_table(
            s3_spark, write_format, "", initial_path, update_df.schema
        )
        assert_pyspark_df_equal(
            actual_df.orderBy("id_col"),
            expected_incremental_update_df.orderBy("id_col"),
        )

    def test_append_write_mode(
        self,
        s3_spark: SparkSession,
        settings_obj_aws: Settings,
        create_table: Tuple[str, str, DataFrame],
        expected_append_df,
        table_writer: TableWriter,
    ):
        job_name, table_name, _ = create_table

        query = MockIncrementalQuery(
            s3_spark,
            settings_obj_aws,
            IncrementalMockTableSchema(),
        )
        table_writer._job_name = job_name
        table_writer._schema = query.schema
        df = query.run()

        table_writer._write_mode = "append"
        table_writer.write_to_hive(
            df,
            table_name,
        )

        actual_df = s3_spark.table(table_name)

        assert_pyspark_df_equal(
            actual_df.orderBy("storage_timestamp", "id_col"),
            expected_append_df.orderBy("storage_timestamp", "id_col"),
        )

    def test_write_append(
        self,
        s3_spark: SparkSession,
        settings_obj_aws: Settings,
        create_table: Tuple[str, str, DataFrame],
        expected_append_df,
        table_writer: TableWriter,
    ):
        job_name, table_name, _ = create_table
        query = MockIncrementalQuery(
            s3_spark,
            settings_obj_aws,
            IncrementalMockTableSchema(),
        )
        table_writer._df = query.run()
        table_writer._job_name = job_name
        table_writer._table_name = table_name
        table_writer._schema = query.schema
        table_writer._write_format = "hive"
        table_writer._write_mode = "append"

        table_writer.write()

        actual_df = s3_spark.table(table_name)

        assert_pyspark_df_equal(
            actual_df.orderBy("storage_timestamp", "id_col"),
            expected_append_df.orderBy("storage_timestamp", "id_col"),
        )

    @pytest.mark.skip(
        reason="depends on delete_object_store_path which is broken for S3",
    )
    def test_rename_object_store_path(
        self,
        s3_spark: SparkSession,
        mock_df: DataFrame,
        table_writer: TableWriter,
    ):
        tname = "rename_object_store_path"
        target_path = f"{TARGET_PATH}{tname}"

        table_writer._write_format = "csv"
        table_writer.write_to_hdfs(mock_df, target_path)

        schema = StructType(
            [
                StructField("timestamp_col", StringType(), True),
                StructField("string_col", StringType(), True),
                StructField("int_col", LongType(), True),
                StructField("varchar_col", StringType(), True),
            ]
        )

        actual_df = (
            s3_spark.read.option("delimiter", "\t")
            .option("header", "true")
            .option("timestampFormat", CSV_TIMESTAMP_FORMAT)
            .csv(target_path, schema=schema)
        ).withColumn("timestamp_col", F.col("timestamp_col").cast("timestamp"))

        assert_pyspark_df_equal(actual_df, mock_df)

        new_path = target_path + "_new"

        table_writer.rename_object_store_path(target_path, new_path)

        actual_df = (
            s3_spark.read.option("delimiter", "\t")
            .option("header", "true")
            .option("timestampFormat", CSV_TIMESTAMP_FORMAT)
            .csv(new_path, schema=schema)
        ).withColumn("timestamp_col", F.col("timestamp_col").cast("timestamp"))

        assert_pyspark_df_equal(actual_df, mock_df)

        # test that the old path has been removed
        with pytest.raises(AnalysisException):
            (
                s3_spark.read.option("delimiter", "\t")
                .option("header", "true")
                .csv(target_path, schema=schema)
            )

    @pytest.mark.skip(
        reason="delete_object_store_path doesn't support deleting S3 paths",
    )
    def test_delete_object_store_path(
        self,
        s3_spark: SparkSession,
        mock_df: DataFrame,
        table_writer: TableWriter,
    ):
        path = f"{TARGET_PATH}/test_delete_object_store_path"
        table_writer._write_format = "csv"

        table_writer.write_to_hdfs(mock_df, path)

        url_parts = urlparse(path, allow_fragments=False)
        bucket_name = url_parts.netloc
        s3_path = url_parts.path
        s3 = boto3.client(
            "s3", region_name="us-east-1", endpoint_url=TEST_AWS_URL
        )
        files_to_delete = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_path)
        for key in files_to_delete:
            key.delete()

        schema = StructType(
            [
                StructField("timestamp_col", TimestampType(), True),
                StructField("string_col", StringType(), True),
                StructField("int_col", LongType(), True),
                StructField("varchar_col", StringType(), True),
            ]
        )
        # test that the old path has been removed
        with pytest.raises(AnalysisException):
            (
                s3_spark.read.option("delimiter", "\t")
                .option("header", "true")
                .csv(path, schema=schema)
            )
