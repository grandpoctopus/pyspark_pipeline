from pathlib import Path

from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark_test import assert_pyspark_df_equal

from pyspark_pipeline.queries import subset_tables_queries
from pyspark_pipeline.schemas import EtlSchema


def test_get_ids_occurring_in_all_dataframes(local_spark, settings_obj):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("guid", IntegerType(), False),
        ]
    )

    df_1 = local_spark.createDataFrame(
        [
            (1, 10),
            (2, 20),
            (3, 30),
        ],
        schema,
    )

    df_2 = local_spark.createDataFrame(
        [(2, 20), (3, 30), (4, 40)],
        schema,
    )

    expected_df = local_spark.createDataFrame(
        [(2, 20), (3, 30)],
        schema,
    ).drop("guid")

    actual_df = subset_tables_queries.GetIdsOccurringInAllDataFrames(
        spark=local_spark,
        id_col_name="id",
        settings=settings_obj,
        schema=schema,
        dataframes=[df_1, df_2],
        number_of_ids=3,
    ).run()

    assert_pyspark_df_equal(actual_df.orderBy("id"), expected_df.orderBy("id"))

    exclusion_id_df = local_spark.createDataFrame(
        [
            (1, 100),
            (3, 300),
        ],
        schema,
    ).drop("guid")

    actual_df = subset_tables_queries.GetIdsOccurringInAllDataFrames(
        spark=local_spark,
        id_col_name="id",
        settings=settings_obj,
        schema=schema,
        dataframes=[df_1, df_2],
        number_of_ids=3,
        exclusion_id_df=exclusion_id_df,
    ).run()

    expected_df = local_spark.createDataFrame(
        [(2, 20)],
        schema,
    ).drop("guid")

    assert_pyspark_df_equal(actual_df.orderBy("id"), expected_df.orderBy("id"))


def test_ids_in_local_csv(local_spark, settings_obj):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("guid", IntegerType(), False),
        ]
    )

    expected_df = local_spark.createDataFrame(
        [(1, 1)],
        schema,
    ).drop("guid")

    local_csv = Path(__file__).parent.parent / "data" / "ids.csv"

    actual_df = subset_tables_queries.IdsInLocalCsv(
        spark=local_spark,
        id_col_name="id",
        settings=settings_obj,
        schema=schema,
        id_csv_path=local_csv,
    ).run()

    assert_pyspark_df_equal(actual_df, expected_df)


def test_subset_by_id(local_spark, settings_obj):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("guid", IntegerType(), False),
        ]
    )

    df_1 = local_spark.createDataFrame(
        [
            (1, 10),
            (2, 20),
        ],
        schema,
    )

    df_2 = local_spark.createDataFrame(
        [(1, 10), (2, 20), (3, 30), (4, 40)],
        schema,
    )

    expected_df = local_spark.createDataFrame(
        [(1, 10), (2, 20)],
        schema,
    ).drop("guid")

    hive_schema = EtlSchema()

    actual_df = subset_tables_queries.SubsetById(
        spark=local_spark,
        id_col_name="id",
        settings=settings_obj,
        schema=hive_schema,
        id_df=df_1,
        input_df=df_2,
    ).run()

    assert_pyspark_df_equal(actual_df, expected_df)


def test_subset_by_column_table_string(local_spark, settings_obj):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("col", StringType(), True),
        ]
    )

    df = local_spark.createDataFrame(
        [
            (1, "strawberry"),
            (2, "_banana_bread"),
            (3, "banana"),
        ],
        schema,
    )

    actual = subset_tables_queries.IdsWithTableColumnString(
        spark=local_spark,
        id_col_name="id",
        settings=settings_obj,
        schema=EtlSchema(),
        df=df,
        column="col",
        pattern="banana",
    ).run()

    assert actual.collect()[0][0] == 3
