import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture
def is_active_df(local_spark: SparkSession) -> DataFrame:
    is_active_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("active", BooleanType(), True),
        ]
    )

    return local_spark.createDataFrame(
        [("1", True), ("2", False), ("3", True)],
        is_active_schema,
    )


@pytest.fixture
def value_df(local_spark: SparkSession) -> DataFrame:
    value_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )

    return local_spark.createDataFrame(
        [
            ("1", 1),
            ("1", 11),
            ("1", 111),
            ("2", 2),
            ("2", 22),
            ("2", 222),
            ("3", 3),
            ("3", 33),
            ("3", 333),
        ],
        value_schema,
    )


@pytest.fixture
def sample_output_df(local_spark: SparkSession) -> DataFrame:
    sample_output_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("max_value", IntegerType(), True),
        ]
    )

    return local_spark.createDataFrame(
        [("1", 111), ("3", 333)],
        sample_output_schema,
    )
