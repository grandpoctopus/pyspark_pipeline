import os
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
from pyspark_test import assert_pyspark_df_equal  # type: ignore

from pyspark_pipeline.utilities.job_utils import (
    get_snowflake_creds,
    get_snowflake_options,
    get_source_table_dataframe,
    get_source_table_dataframes,
    get_subset_filter_str,
    parse_snowflake_location,
    subset_source_dataframe,
    subset_source_dataframes,
)
from pyspark_pipeline.utilities.settings_utils import (
    SnowFlakeSettings,
    SourceTable,
)


@pytest.fixture
def expected_df(local_spark):
    return local_spark.createDataFrame(
        pd.DataFrame([{"_c0": "id"}, {"_c0": "1"}])
    )


@pytest.fixture
def source_table():
    csv_path = f'file:///{Path(__file__).parent.parent / "data" / "ids.csv"}'
    return SourceTable(
        table_type="csv",
        location=csv_path,
    )


@pytest.fixture
def source_dataframes(local_spark):
    source_dataframes = {}

    source_dataframes["subset_population_df"] = local_spark.createDataFrame(
        pd.DataFrame(
            [
                {"id": "1", "mbr_key": "1A"},
                {"id": "2", "mbr_key": "2A"},
                {"id": "1", "mbr_key": "11A"},
                {"id": "3", "mbr_key": "3A"},
            ]
        )
    )

    source_dataframes["id_table_df"] = local_spark.createDataFrame(
        pd.DataFrame(
            [
                {"id": "1", "other_col": "other_col_1"},
                {"id": "22", "other_col": "other_col_22"},
                {"id": "11", "other_col": "other_col_11"},
                {"id": "33", "other_col": "other_col_33"},
            ]
        )
    )

    source_dataframes["mbr_key_table_df"] = local_spark.createDataFrame(
        pd.DataFrame(
            [
                {"other_col": "other_col_1", "mbr_key": "1A"},
                {"other_col": "other_col_22", "mbr_key": "22A"},
                {"other_col": "other_col_33", "mbr_key": "33A"},
            ]
        )
    )

    source_dataframes["both_join_df"] = local_spark.createDataFrame(
        pd.DataFrame(
            [
                {"id": "1", "mbr_key": "1A", "other_col": "other_col_1"},
                {"id": "1", "mbr_key": "2A", "other_col": "other_col_11"},
                {"id": "2", "mbr_key": "2B", "other_col": "other_col_22"},
                {"id": "1", "mbr_key": "11B", "other_col": "other_col_11b"},
                {"id": "3", "mbr_key": "3B", "other_col": "other_col_3"},
            ]
        )
    )

    source_dataframes["no_join_df"] = local_spark.createDataFrame(
        pd.DataFrame(
            [
                {"other_col": "other_col_1"},
                {"other_col": "other_col_22"},
                {"other_col": "other_col_11"},
                {"other_col": "other_col_33"},
            ]
        )
    )

    return source_dataframes


@pytest.fixture
def expected_source_dataframes(local_spark):
    source_dataframes = {}

    source_dataframes["subset_population_df"] = local_spark.createDataFrame(
        pd.DataFrame(
            [
                {"id": "1", "mbr_key": "1A"},
                {"id": "2", "mbr_key": "2A"},
                {"id": "1", "mbr_key": "11A"},
                {"id": "3", "mbr_key": "3A"},
            ]
        )
    )

    source_dataframes["id_table_df"] = local_spark.createDataFrame(
        pd.DataFrame(
            [
                {"id": "1", "other_col": "other_col_1"},
            ]
        )
    )

    source_dataframes["mbr_key_table_df"] = local_spark.createDataFrame(
        pd.DataFrame(
            [
                {"other_col": "other_col_1", "mbr_key": "1A"},
            ]
        )
    )

    source_dataframes["both_join_df"] = local_spark.createDataFrame(
        pd.DataFrame(
            [
                {"id": "1", "mbr_key": "1A", "other_col": "other_col_1"},
            ]
        )
    )

    source_dataframes["no_join_df"] = local_spark.createDataFrame(
        pd.DataFrame(
            [
                {"other_col": "other_col_1"},
                {"other_col": "other_col_22"},
                {"other_col": "other_col_11"},
                {"other_col": "other_col_33"},
            ]
        )
    )

    return source_dataframes


@pytest.fixture
def settings(settings_obj, source_table):
    sf_settings = SnowFlakeSettings(
        url="snowflake_url", warehouse="snowflake_warehouse"
    )
    settings_obj.source_tables = {"source_table": source_table}
    settings_obj.snowflake_settings = sf_settings
    return settings_obj


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_USER": "snowflake_user",
        "SNOWFLAKE_PASSWORD": "snowflake_password",
    },
)
def test_get_snowflake_creds():
    actual = get_snowflake_creds()

    assert actual["sf_user"] == "snowflake_user"
    assert actual["sf_password"] == "snowflake_password"

    for var in ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]:
        del os.environ[var]
        with pytest.raises(ValueError):
            get_snowflake_creds()
        os.environ[var] = "snowflake_stuff"


def test_parse_snowflake_location():
    source_table = SourceTable(
        table_type="snowflake",
        location="schema_name.database_name.table_name",
    )
    expected = {
        "schema": "schema_name",
        "database": "database_name",
        "table": "table_name",
    }
    actual = parse_snowflake_location(source_table)
    assert actual == expected


def test_get_snowflake_options(settings):
    sf_creds = {
        "sf_user": "snowflake_user",
        "sf_password": "snowflake_password",
    }

    table_location = {
        "schema": "schema_name",
        "database": "database_name",
        "table": "table_name",
    }

    expected = {
        "sfurl": "snowflake_url",
        "sfUser": "snowflake_user",
        "sfPassword": "snowflake_password",
        "sfDatabase": "database_name",
        "sfSchema": "schema_name",
        "sfWarehouse": "snowflake_warehouse",
        "ocspFailOpen": "false",
        "sfRole": None,
    }

    actual = get_snowflake_options(sf_creds, table_location, settings)
    assert actual == expected


def test_get_source_table_dataframe(
    local_spark, source_table, expected_df, settings
):
    actual = get_source_table_dataframe(
        spark=local_spark, source_table=source_table, settings=settings
    )

    assert_pyspark_df_equal(actual, expected_df)


def test_get_source_table_dataframes(local_spark, expected_df, settings):
    actual = get_source_table_dataframes(
        spark=local_spark,
        settings=settings,
    )

    expected = {"source_table": expected_df}

    assert actual.keys() == expected.keys()

    actual_df = actual["source_table"]

    assert_pyspark_df_equal(actual_df, expected_df)


def test_subset_source_dataframes(
    source_dataframes, expected_source_dataframes
):
    actual = subset_source_dataframes(source_dataframes)

    for k, v in actual.items():
        assert_pyspark_df_equal(v, expected_source_dataframes[k])


def test_subset_source_dataframe(source_dataframes, expected_source_dataframes):
    for k, v in source_dataframes.items():
        actual = subset_source_dataframe(
            source_dataframes["subset_population_df"], v
        )

        assert_pyspark_df_equal(
            actual.orderBy(*actual.columns),
            expected_source_dataframes[k].orderBy(*actual.columns),
        )


def test_get_subset_filter_str(source_dataframes):
    subset_population_df = source_dataframes["subset_population_df"]
    cols = subset_population_df.columns
    actual = get_subset_filter_str(subset_population_df, cols)
    expected = (
        "cast(id AS string) = '1' "
        "AND cast(mbr_key AS string) = '1A' "
        "OR cast(id AS string) = '2' "
        "AND cast(mbr_key AS string) = '2A' "
        "OR cast(id AS string) = '1' "
        "AND cast(mbr_key AS string) = '11A' "
        "OR cast(id AS string) = '3' "
        "AND cast(mbr_key AS string) = '3A'"
    )
    assert actual == expected

    # test with just one column
    actual = get_subset_filter_str(subset_population_df, ["id"])
    expected = (
        "cast(id AS string) = '1' "
        "OR cast(id AS string) = '2' "
        "OR cast(id AS string) = '1' "
        "OR cast(id AS string) = '3'"
    )
    assert actual == expected
