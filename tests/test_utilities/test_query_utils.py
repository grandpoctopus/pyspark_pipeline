from datetime import date, datetime, timedelta, timezone
from typing import Dict

import pandas as pd
import pytest
from numpy import NaN
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    NullType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark_test import assert_pyspark_df_equal  # type: ignore

from pyspark_pipeline.schemas import HiveSchema
from pyspark_pipeline.utilities.query_utils import (
    FLOAT_REGEX,
    NUMERIC_REGEX,
    add_provenance_column,
    aggregate_rows_by_collect_list,
    conditionally_repartition,
    format_ts_column_as_iso_string,
    get_group_by_cols,
    get_id_group_col,
    get_list_cols,
    get_most_common_value_for_column,
    get_newest_non_null_values,
    get_rows_stored_in_date_range,
    get_yearmonth_number,
    is_not_null_unk_or_na,
    manage_output_partitions,
    null_columns_for_missing_columns,
    pandas_string_cols_to_float,
    reformat_dataframe,
    rename_columns,
    use_database_udf,
    validate_code,
)
from pyspark_pipeline.utilities.settings_utils import Settings


@pytest.fixture
def repartition_df() -> DataFrame:
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .appName("pyspark_pipeline_pytest_local")
        .enableHiveSupport()
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ]
    )

    return spark.createDataFrame(
        [
            (1, "SomeValue1"),
            (2, "SomeValue2"),
            (11, "SomeValue1"),
            (12, "SomeValue2"),
            (21, "SomeValue1"),
            (22, "SomeValue2"),
        ],
        schema,
    )


@pytest.fixture()
def fake_schema() -> HiveSchema:
    class FakeSchema(HiveSchema):
        banana = "string"
        watermelon_list = "Array<str>"

    return FakeSchema()


def test_is_not_null_unk_or_na(local_spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [(1, None), (2, "UNK"), (3, "NA"), (4, "I should be here")], schema
    )

    expected = local_spark.createDataFrame([(4, "I should be here")], schema)

    actual = df.where(is_not_null_unk_or_na(F.col("value")))

    assert_pyspark_df_equal(actual, expected)


@pytest.mark.parametrize(
    "a_date, expected",
    [(date(2020, 1, 1), 202001), (date(2020, 11, 11), 202011)],
)
def test_get_yearmonth_number(a_date, expected):
    assert get_yearmonth_number(a_date) == expected


def test_rename_columns(local_spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, "value_1"),
        ],
        schema,
    )

    new_schema = StructType(
        [
            StructField("new_id", IntegerType(), True),
            StructField("new_value", StringType(), True),
        ]
    )

    expected = local_spark.createDataFrame(
        [
            (1, "value_1"),
        ],
        new_schema,
    )

    actual = rename_columns(
        df,
        name_map={
            "id": "new_id",
            "value": "new_value",
            "invalid_column": "invalid_columns",
        },
    )

    assert_pyspark_df_equal(actual, expected)


def test_null_columns_for_missing_columns(local_spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, "value_1"),
        ],
        schema,
    )

    new_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
            StructField("new_column", NullType(), True),
        ]
    )

    expected = local_spark.createDataFrame(
        [
            (1, "value_1", None),
        ],
        new_schema,
    )

    actual = null_columns_for_missing_columns(
        df, target_schema=["id", "value", "new_column"]
    )
    assert_pyspark_df_equal(actual, expected)


def test_reformat_dataframe(local_spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, "value_1"),
        ],
        schema,
    )

    new_schema = StructType(
        [
            StructField("new_id", IntegerType(), True),
            StructField("new_value", StringType(), True),
            StructField("new_column", NullType(), True),
        ]
    )

    expected = local_spark.createDataFrame(
        [
            (1, "value_1", None),
        ],
        new_schema,
    )

    actual = reformat_dataframe(
        df,
        name_map={
            "id": "new_id",
            "value": "new_value",
            "invalid_column": "invalid_columns",
        },
        target_schema=["new_id", "new_value", "new_column"],
    )
    assert_pyspark_df_equal(actual, expected)


def test_numeric_regex():
    actual = NUMERIC_REGEX.sub("", ">+-25 00,.00>?! ")
    assert actual == "2500.00"


@pytest.mark.parametrize(
    "input_str, expected",
    [
        ("2500", "2500"),
        ("2500.00", "2500.00"),
        ("0", "0"),
        ("xxx2500", None),
    ],
)
def test_float_regex(input_str: str, expected: str):
    actual = FLOAT_REGEX.match(input_str)
    if expected is None:
        assert actual == expected
    else:
        assert actual.groups()[0] == expected  # type: ignore


@pytest.mark.parametrize(
    "input_row, output_row",
    [
        (
            {
                "col1": "a123",
                "col2": ">+-2500,.00>?!",
                "col3": ">+-2500,.00>?!",
            },
            {"col1": "a123", "col2": 2500.00, "col3": ">+-2500,.00>?!"},
        ),
        (
            {
                "col1": "a123",
                "col2": "xxx2500.00xxxx",
                "col3": "xxx2500.00xxxx",
            },
            {"col1": "a123", "col2": 2500.00, "col3": "xxx2500.00xxxx"},
        ),
        (
            {"col1": "a123", "col2": "2500", "col3": "2500"},
            {"col1": "a123", "col2": 2500.00, "col3": "2500"},
        ),
        (
            {"col1": "a123", "col2": "", "col3": ""},
            {"col1": "a123", "col2": NaN, "col3": ""},
        ),
        (
            {"col1": "a123", "col2": "", "col3": "xxx00601xxx"},
            {"col1": "a123", "col2": NaN, "col3": "xxx00601xxx"},
        ),
        (
            {"col1": "a123", "col2": "  00601  ", "col3": "  00601  "},
            {"col1": "a123", "col2": 601.00, "col3": "  00601  "},
        ),
        (
            {"col1": "a123", "col2": "00601-03235", "col3": "00601-03235"},
            {"col1": "a123", "col2": 60103235.00, "col3": "00601-03235"},
        ),
    ],
)
def test_pandas_string_cols_to_float(input_row: Dict, output_row: Dict):
    df = pd.DataFrame([input_row])

    convert_cols = ["col2"]

    actual_df = pandas_string_cols_to_float(
        df, str_to_float_columns=convert_cols
    )

    expected_df = pd.DataFrame([output_row])

    assert actual_df.equals(expected_df)


def test_conditionally_repartition(repartition_df: DataFrame):
    assert repartition_df.rdd.getNumPartitions() != 10
    df = conditionally_repartition(repartition_df, 10)
    assert df.rdd.getNumPartitions() == 10


def test_manage_output_partitions(
    repartition_df: DataFrame, settings_obj: Settings
):
    settings_obj.output_partitions = 10
    assert repartition_df.rdd.getNumPartitions() != 10
    df = manage_output_partitions(
        df=repartition_df,
        do_repartition=True,
        col_to_repartition_by=None,
        settings=settings_obj,
    )
    assert df.rdd.getNumPartitions() == 10
    df = manage_output_partitions(
        df=repartition_df,
        do_repartition=True,
        col_to_repartition_by="id",
        settings=settings_obj,
    )
    assert df.rdd.getNumPartitions() == 10


def test_validate_code(local_spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("code", StringType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, "banana"),
            (2, "banana1"),
            (11, "banana 1"),
            (12, "apple"),
            (21, "banana banana"),
            (22, None),
            (23, ""),
        ],
        schema,
    )

    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("code", StringType(), True),
            StructField("valid_code", StringType(), True),
        ]
    )

    expected_nulls = local_spark.createDataFrame(
        [
            (1, "banana", "banana"),
            (2, "banana1", "banana"),
            (11, "banana 1", "banana"),
            (12, "apple", "kiwi"),
            (21, "banana banana", "banana"),
            (22, None, None),
            (23, "", "kiwi"),
        ],
        expected_schema,
    )

    expected_no_nulls = local_spark.createDataFrame(
        [
            (1, "banana", "banana"),
            (2, "banana1", "banana"),
            (11, "banana 1", "banana"),
            (12, "apple", "kiwi"),
            (21, "banana banana", "banana"),
            (22, None, "kiwi"),
            (23, "", "kiwi"),
        ],
        expected_schema,
    )

    actual_nulls = validate_code(
        df=df,
        code_column="code",
        regex="banana",
        output_column="valid_code",
        replacement_value="kiwi",
        replace_nulls=False,
    )

    actual_no_nulls = validate_code(
        df=df,
        code_column="code",
        regex="banana",
        output_column="valid_code",
        replacement_value="kiwi",
        replace_nulls=True,
    )

    assert_pyspark_df_equal(
        actual_nulls.orderBy("id"), expected_nulls.orderBy("id")
    )
    assert_pyspark_df_equal(
        actual_no_nulls.orderBy("id"), expected_no_nulls.orderBy("id")
    )


def test_add_provenance_column(local_spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("vend", StringType(), True),
            StructField("agg", StringType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, "banana", "apple"),
            (2, "orange", "pear"),
        ],
        schema,
    )

    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("vend", StringType(), True),
            StructField("agg", StringType(), True),
            StructField(
                "provenance",
                StructType(
                    [
                        StructField("database", StringType(), True),
                        StructField("table_name", StringType(), True),
                        StructField("vendor", StringType(), True),
                        StructField("aggregator", StringType(), True),
                    ]
                ),
            ),
        ]
    )

    expected = local_spark.createDataFrame(
        [
            (
                1,
                "banana",
                "apple",
                {
                    "database": "db",
                    "table_name": "fruit",
                    "vendor": "banana",
                    "aggregator": "apple",
                },
            ),
            (
                2,
                "orange",
                "pear",
                {
                    "database": "db",
                    "table_name": "fruit",
                    "vendor": "orange",
                    "aggregator": "pear",
                },
            ),
        ],
        expected_schema,
    )

    actual = add_provenance_column(
        df=df,
        database_name="db",
        table_name="fruit",
        vendor_column="vend",
        aggregator_column="agg",
    )

    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))


def test_get_group_by_cols(fake_schema):
    assert get_group_by_cols(fake_schema) == ["banana"]


def test_get_list_cols(fake_schema):
    assert get_list_cols(fake_schema) == ["watermelon"]


def test_aggregate_rows_by_collect_list(local_spark: SparkSession):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("code", StringType(), True),
            StructField("line_number", IntegerType(), True),
            StructField("amount", IntegerType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, "banana", 1, 11),
            (1, "banana", 2, 22),
            (2, "apple", 1, 11),
            (1, "banana banana", 3, 33),
            (1, "banana", 1, 11),
        ],
        schema,
    )

    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("code", StringType(), True),
            StructField(
                "line_number_list", ArrayType(IntegerType(), True), False
            ),
            StructField("amount_list", ArrayType(IntegerType(), True), False),
        ]
    )

    expected = local_spark.createDataFrame(
        [
            (1, "banana", [1, 2, 1], [11, 22, 11]),
            (1, "banana banana", [3], [33]),
            (2, "apple", [1], [11]),
        ],
        expected_schema,
    )

    actual = aggregate_rows_by_collect_list(
        df,
        group_by_cols=["id", "code"],
        columns_to_collect_into_list=["line_number", "amount"],
    )

    assert_pyspark_df_equal(
        actual.orderBy("id", "code"), expected.orderBy("id", "code")
    )


def test_get_most_common_value_for_column(local_spark: SparkSession):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("code", StringType(), True),
            StructField("line_number", IntegerType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, "banana", 1),
            (1, "banana", 2),
            (2, "apple", 2),
            (2, "mango", 1),
            (1, "banana banana", 3),
            (1, "pineapple", 0),
        ],
        schema,
    )

    expected = local_spark.createDataFrame(
        [
            (1, "banana", 1),
            (2, "mango", 1),
        ],
        schema,
    )

    actual = get_most_common_value_for_column(
        df,
        column_name="code",
        partition_cols=["id"],
        tie_breaker_col="line_number",
        tie_breaker_how="asc",
    )

    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))

    actual = get_most_common_value_for_column(
        df,
        column_name="code",
        partition_cols=["id"],
        tie_breaker_col=None,
    )

    expected = local_spark.createDataFrame(
        [
            (1, "banana", 1),
            (2, "apple", 2),
        ],
        schema,
    )

    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))


def test_get_newest_non_null_values(local_spark: SparkSession):
    oldest_timestamp_str = "1990-01-01T05:00:00+0000"
    older_timestamp_str = "1991-01-01T00:00:00-0500"
    newer_timestamp_str = "1992-01-01T00:00:00-0500"
    newest_timestamp_str = "1992-01-01T00:00:01-0500"

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("code", StringType(), True),
            StructField("line_number", IntegerType(), True),
            StructField("sor_col", StringType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, "banana", None, newest_timestamp_str),
            (1, "banana", 2, oldest_timestamp_str),
            (2, None, 2, newer_timestamp_str),
            (2, "mango", 1, oldest_timestamp_str),
            (1, "banana banana", 3, newer_timestamp_str),
            (1, "pineapple", 0, older_timestamp_str),
        ],
        schema,
    )

    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("code", StringType(), True),
            StructField("line_number", IntegerType(), True),
        ]
    )
    expected = local_spark.createDataFrame(
        [
            (1, "banana", 3),
            (2, "mango", 2),
        ],
        expected_schema,
    )

    actual = get_newest_non_null_values(
        df,
        "id",
        ["code", "line_number"],
        "sor_col",
    )

    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))


def test_format_ts_column_as_iso_string(
    local_spark: SparkSession, valid_timestamp: datetime
):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("ts", TimestampType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, valid_timestamp),
            (2, datetime(2021, 12, 28, 5, tzinfo=timezone.utc)),
        ],
        schema,
    )

    actual = format_ts_column_as_iso_string(df, "ts")

    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("ts", StringType(), True),
        ]
    )

    expected = local_spark.createDataFrame(
        [
            (1, "1990-01-01T01:00:00+0000"),
            (2, "2021-12-28T05:00:00+0000"),
        ],
        expected_schema,
    )

    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))

    actual = format_ts_column_as_iso_string(df, "ts", "json")

    expected = local_spark.createDataFrame(
        [
            (1, "1990-01-01 01:00:00"),
            (2, "2021-12-28 05:00:00"),
        ],
        expected_schema,
    )

    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))

    actual = format_ts_column_as_iso_string(df, "ts", "date")

    expected = local_spark.createDataFrame(
        [
            (1, "1990-01-01"),
            (2, "2021-12-28"),
        ],
        expected_schema,
    )

    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))

    actual = format_ts_column_as_iso_string(df, "ts", "fhir_datetime")

    expected = local_spark.createDataFrame(
        [
            (1, "1990-01-01T01:00:00+00:00"),
            (2, "2021-12-28T05:00:00+00:00"),
        ],
        expected_schema,
    )

    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))


def test_get_rows_stored_in_date_range(
    local_spark: SparkSession,
    valid_timestamp: datetime,
    invalid_timestamp_before: datetime,
    invalid_timestamp_after: datetime,
):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("ts", TimestampType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, valid_timestamp),
            (2, invalid_timestamp_after),
            (3, invalid_timestamp_before),
        ],
        schema,
    )

    actual = get_rows_stored_in_date_range(
        df,
        "ts",
        invalid_timestamp_before + timedelta(hours=1),
        invalid_timestamp_after - +timedelta(hours=1),
    )

    expected = df = local_spark.createDataFrame(
        [
            (1, valid_timestamp),
        ],
        schema,
    )

    assert_pyspark_df_equal(actual, expected)

    # the function just be inclusive between start and end

    actual = get_rows_stored_in_date_range(
        df,
        "ts",
        invalid_timestamp_before,
        invalid_timestamp_after,
    )

    assert_pyspark_df_equal(actual.orderBy("id"), df.orderBy("id"))


def test_get_id_group_col(local_spark: SparkSession, settings_obj: Settings):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1,),
            (31,),
            (32,),
            (100,),
            (319349,),
        ],
        schema,
    )
    expected_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("id_group", LongType(), True),
        ]
    )
    expected = local_spark.createDataFrame(
        [
            (1, 1),
            (31, 31),
            (32, 32),
            (100, 0),
            (319349, 49),
        ],
        expected_schema,
    )

    actual = get_id_group_col(df, "id", 100)
    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))


def test_use_database_udf(local_spark: SparkSession):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("code", StringType(), True),
            StructField("val", StringType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (
                1,
                "banana",
                "plum",
            ),
            (
                2,
                "banana",
                "pluot",
            ),
            (
                3,
                "banana",
                "cherry",
            ),
        ],
        schema,
    )

    expected = local_spark.createDataFrame(
        [
            (
                1,
                "banana",
                "PLUM",
            ),
            (
                2,
                "banana",
                "PLUOT",
            ),
            (
                3,
                "banana",
                "CHERRY",
            ),
        ],
        schema,
    )

    actual = use_database_udf(
        local_spark,
        df,
        "UPPER",
        "val",
        "val",
    )

    assert_pyspark_df_equal(actual.orderBy("id"), expected.orderBy("id"))
