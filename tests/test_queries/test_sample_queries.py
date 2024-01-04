from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark_test import assert_pyspark_df_equal  # type: ignore

from pyspark_pipeline.queries import sample_queries
from pyspark_pipeline.schemas import sample_schemas
from pyspark_pipeline.utilities.settings_utils import Settings


def test_sample_join_query(
    local_spark: SparkSession,
    is_active_df: DataFrame,
    value_df: DataFrame,
    settings_obj: Settings,
):
    expected_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )

    expected_df = local_spark.createDataFrame(
        [("1", 1), ("1", 11), ("1", 111), ("3", 3), ("3", 33), ("3", 333)],
        expected_schema,
    )

    actual_df = sample_queries.SampleJoinQuery(
        is_active_df=is_active_df,
        value_df=value_df,
        schema=sample_schemas.SampleJoinSchema(),
        spark=local_spark,
        settings=settings_obj,
    ).run()

    assert_pyspark_df_equal(
        actual_df.orderBy("value"), expected_df.orderBy("value")
    )


def test_sample_aggregation_query(
    local_spark: SparkSession,
    settings_obj: Settings,
    sample_output_df: DataFrame,
):
    joined_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value", IntegerType(), True),
        ]
    )

    sample_joined_df = local_spark.createDataFrame(
        [("1", 1), ("1", 11), ("1", 111), ("3", 3), ("3", 33), ("3", 333)],
        joined_schema,
    )

    actual_df = sample_queries.SampleAggregationQuery(
        sample_joined_df=sample_joined_df,
        schema=sample_schemas.SampleAggregationSchema(),
        spark=local_spark,
        settings=settings_obj,
    ).run()

    assert_pyspark_df_equal(
        actual_df.orderBy("max_value"), sample_output_df.orderBy("max_value")
    )
