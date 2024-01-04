from pyspark.sql.types import IntegerType, StructField, StructType
from pyspark_test import assert_pyspark_df_equal

from pyspark_pipeline.queries import subset_source_tables_queries
from pyspark_pipeline.schemas import DataFrameSchemaFactory


def test_subset_by_column_value_query(local_spark, settings_obj):
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
    )

    hive_schema = DataFrameSchemaFactory("test_schema", df_2)()

    actual_df = subset_source_tables_queries.SubsetByColumnValueQuery(
        spark=local_spark,
        settings=settings_obj,
        schema=hive_schema,
        subsetting_df=df_1,
        input_df=df_2,
        cols=["id"],
    ).run()

    assert_pyspark_df_equal(actual_df.orderBy("id"), expected_df.orderBy("id"))


def test_copy_table_query(local_spark, settings_obj):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("guid", IntegerType(), False),
        ]
    )

    input_df = local_spark.createDataFrame(
        [
            (1, 10),
            (2, 20),
        ],
        schema,
    )

    hive_schema = DataFrameSchemaFactory("test_schema", input_df)()

    actual_df = subset_source_tables_queries.CopyTableQuery(
        spark=local_spark,
        settings=settings_obj,
        schema=hive_schema,
        input_df=input_df,
    ).run()

    assert_pyspark_df_equal(actual_df, input_df)
