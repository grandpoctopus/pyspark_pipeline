from freezegun import freeze_time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark_test import assert_pyspark_df_equal  # type: ignore

from pyspark_pipeline.queries import (
    AuditQuery,
    CountKeysQuery,
    DistinctValueQuery,
    Query,
)
from pyspark_pipeline.schemas import AuditSchema, HiveSchema
from pyspark_pipeline.utilities.settings_utils import Settings


def test_query_class():
    query = Query(None, None, None)  # type: ignore

    assert query.run() is None


def test_get_filtered_values(local_spark):
    query = Query(None, None, None)  # type: ignore

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("mbr_key", StringType(), True),
        ]
    )

    input_df = local_spark.createDataFrame(
        [
            (1, "mbr_key_1"),
            (2, "mbr_key_2"),
            (3, "mbr_key_3"),
            (4, "mbr_key_4"),
        ],
        schema,
    )

    output_df = local_spark.createDataFrame(
        [
            (1, "mbr_key_1"),
            (3, "mbr_key_3"),
            (4, "mbr_key_4"),
        ],
        schema,
    )

    expected = (
        local_spark.createDataFrame(
            [
                (2, "mbr_key_2"),
            ],
            schema,
        )
        .select("id")
        .collect()
    )

    actual = query.get_filtered_values(input_df, output_df, "id")

    assert actual == expected


def test_log_filtered_values(local_spark, caplog):
    query = Query(None, None, None)  # type: ignore

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("mbr_key", StringType(), True),
        ]
    )

    input_df = local_spark.createDataFrame(
        [
            (1, "mbr_key_1"),
            (2, "mbr_key_2"),
            (3, "mbr_key_3"),
        ],
        schema,
    )

    output_df = local_spark.createDataFrame(
        [
            (1, "mbr_key_1"),
            (3, "mbr_key_3"),
        ],
        schema,
    )

    query.log_filtered_values(
        "test_func", input_df, output_df, ["id", "mbr_key"]
    )
    actual = caplog.text

    expected_lines = [
        "mbr_key filtered by test_func",
        "mbr_key_2",
        "id filtered by test_func",
    ]
    for line in expected_lines:
        assert line in actual


def test_query_class_prepare_data(
    query_test_df: DataFrame, query_expected_test_df: DataFrame
):
    query = Query(None, None, None)  # type: ignore

    class TestSchema(HiveSchema):
        fruit_type = "string"
        num_fruits = "int"
        vitamin = ["string"]

    df = query.enforce_schema_and_uniqueness(query_test_df, TestSchema())
    assert_pyspark_df_equal(df, query_expected_test_df)


@freeze_time("2021-01-01 0:00:00", tz_offset=0)
def test_audit_query(settings_obj, local_spark, expected_audit_df):
    settings_obj.run_id = "starfruit"
    actual_df = AuditQuery(
        spark=local_spark, settings=settings_obj, schema=AuditSchema()
    ).run()

    assert_pyspark_df_equal(actual_df, expected_audit_df)


def test_query_get_bin_col(local_spark: SparkSession, settings_obj: Settings):
    query = Query(
        spark=local_spark, settings=settings_obj, schema=HiveSchema()  # type: ignore
    )

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("id_group", LongType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, 1),
            (31, 31),
            (32, 32),
            (319349, 49),
        ],
        schema,
    )

    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("id_group", LongType(), True),
            StructField("id_group_bucket", LongType(), True),
        ]
    )

    expected = local_spark.createDataFrame(
        [
            (1, 1, 1),
            (31, 31, 31),
            (32, 32, 32),
            (319349, 49, 49),
        ],
        expected_schema,
    )
    actual = query.get_bins_by_column(
        df, "id_group", "id_group_bucket", settings_obj
    )

    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))

    expected_none = local_spark.createDataFrame(
        [
            (1, 1, None),
            (31, 31, None),
            (32, 32, None),
            (319349, 49, None),
        ],
        expected_schema,
    )

    settings_obj.bins_per_partition = None
    actual_no_bin = query.get_bins_by_column(
        df, "id_group", "id_group_bucket", settings_obj
    )
    assert_pyspark_df_equal(
        actual_no_bin.orderBy("id"), expected_none.orderBy("id")
    )


def test_count_keys_query(local_spark):

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("mbr_key", StringType(), True),
        ]
    )

    input_df = local_spark.createDataFrame(
        [
            (1, "mbr_key_1"),
            (2, "mbr_key_2"),
            (3, "mbr_key_3"),
            (1, "mbr_key_4"),
        ],
        schema,
    )

    expected_schema = StructType(
        [
            StructField("key", StringType(), True),
            StructField("count", LongType(), True),
        ]
    )

    expected_df = local_spark.createDataFrame(
        [
            ("id", 3),
            ("mbr_key", 4),
        ],
        expected_schema,
    )

    actual = CountKeysQuery(
        spark=local_spark,
        schema=None,
        settings=None,
        input_df=input_df,
        cols=["id", "mbr_key"],
    ).run()

    assert_pyspark_df_equal(actual.orderBy("key"), expected_df.orderBy("key"))


def test_distinct_value_query(local_spark):

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("mbr_key", StringType(), True),
        ]
    )

    input_df = local_spark.createDataFrame(
        [
            (1, "mbr_key_1"),
            (2, "mbr_key_2"),
            (3, "mbr_key_3"),
            (1, "mbr_key_4"),
            (1, "mbr_key_4"),
            (3, "mbr_key_3"),
        ],
        schema,
    )

    expected_df = local_spark.createDataFrame(
        [
            (1, "mbr_key_1"),
            (2, "mbr_key_2"),
            (3, "mbr_key_3"),
            (1, "mbr_key_4"),
        ],
        schema,
    )

    actual = DistinctValueQuery(
        spark=local_spark,
        schema=None,
        settings=None,
        input_df=input_df,
        cols=["id", "mbr_key"],
    ).run()

    assert_pyspark_df_equal(
        actual.orderBy("mbr_key"), expected_df.orderBy("mbr_key")
    )
