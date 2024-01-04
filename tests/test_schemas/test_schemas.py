from pyspark.sql.types import (
    ArrayType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

from pyspark_pipeline.schemas import (
    DataFrameSchemaFactory,
    HiveSchema,
    SchemaFactory,
)


class MockSchema(HiveSchema):
    col_1 = "varchar(32)"
    col_2 = "bigint"


class NestedMockSchema(HiveSchema):
    col_1 = "varchar(32)"
    col_2: MockSchema = MockSchema()


class ArrayMockSchema(HiveSchema):
    col_1 = "varchar(32)"
    col_2 = [MockSchema()]


class ArrayNestedMockSchema(HiveSchema):
    col_1 = "varchar(32)"
    col_2 = [NestedMockSchema()]


def test_get_columns_list():
    schema = MockSchema()
    expected = ["col_1", "col_2"]

    assert schema.get_columns_list() == expected


def test_get_schema_string():
    schema = MockSchema()
    expected = "col_1 varchar(32), col_2 bigint"

    assert schema.get_schema_string() == expected

    schema = NestedMockSchema()
    expected = (
        "col_2 STRUCT<col_1:varchar(32), " "col_2:bigint>, col_1 varchar(32)"
    )

    assert schema.get_schema_string() == expected

    schema = ArrayMockSchema()
    expected = (
        "col_1 varchar(32), "
        "col_2 ARRAY<STRUCT<col_1:varchar(32), "
        "col_2:bigint>>"
    )
    assert schema.get_schema_string() == expected

    schema = ArrayNestedMockSchema()
    expected = (
        "col_1 varchar(32), "
        "col_2 ARRAY<STRUCT<col_2:STRUCT<col_1:varchar(32), col_2:bigint>, "
        "col_1:varchar(32)>>"
    )
    assert schema.get_schema_string() == expected


def test_schema_factory():
    class ExpectedSchema(HiveSchema):
        field_1 = "field_1_value"
        field_2 = "field_2_value"

    actual = SchemaFactory(
        "ExpectedSchema",
        {
            "field_1": "field_1_value",
            "field_2": "field_2_value",
        },
    )

    assert actual() == ExpectedSchema()


def test_dataframe_schema_factory(local_spark):

    spark_schema = StructType(
        [
            StructField("field_1", StringType(), True),
            StructField("field_2", FloatType(), True),
            StructField("field_3", ArrayType(StringType(), True), True),
        ]
    )

    df = local_spark.createDataFrame(
        [("pineapple", 2.0, ["C"]), ("apple", 4.0, ["B12"])],
        spark_schema,
    )

    class ExpectedSchema(HiveSchema):
        field_1 = "string"
        field_2 = "float"
        field_3 = "array<string>"

    actual = DataFrameSchemaFactory("ExpectedSchema", df)

    assert actual() == ExpectedSchema()
