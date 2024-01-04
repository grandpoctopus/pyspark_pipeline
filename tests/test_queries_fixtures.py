from datetime import datetime

import pytest
from freezegun import freeze_time  # type: ignore
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="module")
def query_test_df(local_spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("fruit_type", StringType(), True),
            StructField("num_fruits", FloatType(), True),
            StructField("vitamin", ArrayType(StringType(), True), True),
        ]
    )

    return local_spark.createDataFrame(
        [("pineapple", 2.0, ["C"]), ("apple", 4.0, ["B12"])],
        schema,
    )


@pytest.fixture(scope="module")
def query_expected_test_df(local_spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("fruit_type", StringType(), True),
            StructField("num_fruits", IntegerType(), True),
            StructField("vitamin", ArrayType(StringType(), True), True),
        ]
    )

    return local_spark.createDataFrame(
        [("pineapple", 2, ["C"]), ("apple", 4, ["B12"])],
        schema,
    )


@pytest.fixture(scope="module")
@freeze_time("2021-01-01 0:00:00", tz_offset=0)
def expected_audit_df(settings_obj, local_spark) -> DataFrame:
    settings_obj.run_id = "starfruit"
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

    return local_spark.createDataFrame(
        [
            (
                "starfruit",
                str(datetime.now()),
                str(settings_obj.include_start_date),
                str(settings_obj.include_end_date),
                str(settings_obj.incremental_load_start_date),
                str(settings_obj.incremental_load_end_date),
                str(settings_obj.dict()),
            )
        ],
        audit_schema,
    )
