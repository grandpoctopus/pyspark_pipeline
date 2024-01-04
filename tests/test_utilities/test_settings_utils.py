from argparse import Namespace
from datetime import date, datetime
from pathlib import Path

import pytest
import yaml
from freezegun import freeze_time  # type: ignore
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType, TimestampType

from pyspark_pipeline.utilities.settings_utils import (
    Databases,
    Settings,
    SourceTable,
    SparkEnvironment,
    get_incremental_date_ranges,
    get_settings,
    get_table_name,
    parse_since_date_str,
    set_regression_test_databases,
    validate_date,
)


@pytest.fixture(scope="module")
def audit_df(local_spark) -> DataFrame:
    audit_schema = StructType(
        [
            StructField("run_timestamp", TimestampType(), True),
            StructField("incremental_load_start_date", TimestampType(), True),
            StructField("incremental_load_end_date", TimestampType(), True),
            StructField("include_start_date", TimestampType(), True),
        ]
    )

    return local_spark.createDataFrame(
        [
            (
                datetime(2021, 1, 1),
                datetime(1999, 1, 1),
                datetime(2020, 1, 1),
                datetime(1980, 1, 1),
            ),
            (
                datetime(2020, 1, 1),
                datetime(1998, 1, 1),
                datetime(2019, 1, 1),
                datetime(1979, 1, 1),
            ),
        ],
        audit_schema,
    )


@pytest.fixture(scope="function")
def settings_json():
    return {
        "include_start_date": "2017-01-01",
        "include_end_date": "2018-01-01",
        "suffix": "20160101_20210101",
        "table_tag": "tabletag",
        "hadoop_env": "hadoopenv",
        "release": "releasenum",
        "target_path": ("hdfs://nameservice{hadoop_env}/{table_tag}/{suffix}/"),
        "databases": {
            "cadl_src_db": "cadl",
            "rfdm_src_db": "rfdm",
            "edw_src_db": "edw_src",
            "target_db": "ext_wk",
            "ext_warehouse_db": "ext_wh",
            "cda_src_db": "cda",
        },
        "spark_configs": {"spark.config": "value"},
        "job_name": "fake_job",
        "source_tables": {
            "fake_table": {
                "table_type": "fake_type",
                "location": "fake_location",
            }
        },
        "sor_column_name": "sor_dtm",
        "run_id": "dragonfruit",
        "spark_environment": {
            "SPARK_HOME": "{pyspark_pipeline_home}/deps/spark-{pyspark_version}",
            "remote_spark_build_path": (
                "hdfs://nameservicehadoopenv2/user/ag82944/"
                "spark-{pyspark_version}.tgz"
            ),
            "spark_libs_path": (
                "hdfs://nameservicehadoopenv2/user/ag82944/"
                "spark-{pyspark_version}-libs.jar"
            ),
            "JAVA_HOME": "/usr/lib/jvm/jre-1.8.0-openjdk",
            "SPARK_CONF_DIR": "{pyspark_pipeline_home}/deps/spark-conf",
            "YARN_CONF_DIR": "/etc/hadoop/conf.cloudera.yarn",
            "HADOOP_CONF_DIR": "/etc/hadoop/conf:/etc/hive/conf",
            "PATH": ["/opt/cloudera/parcels/CDH/bin", "{JAVA_HOME}"],
        },
    }


@pytest.fixture(scope="function")
def expected_settings():
    expected = Settings(
        job_name="fake_job",
        include_start_date=date(2017, 1, 1),
        include_end_date=date(2018, 1, 1),
        suffix="20160101_20210101",
        table_tag="tabletag",
        hadoop_env="hadoopenv",
        release="releasenum",
        sor_column_name="sor_dtm",
        databases={
            "source_db": "src_db",
            "target_db": "ext_wk",
        },
        spark_configs={"spark.config": "value"},
        target_path="hdfs://nameservice{hadoop_env}/{table_tag}/{suffix}/",
        source_tables={
            "fake_table": SourceTable(
                table_type="fake_type",
                location="fake_location",
            )
        },
        spark_submit_mode="client",
        run_id="dragonfruit",
    )
    expected.target_path = (
        "hdfs://nameservicehadoopenv/tabletag/20160101_20210101/"
    )
    expected.databases = Databases(
        cadl_src_db="cadl",
        rfdm_src_db="rfdm",
        edw_src_db="edw_src",
        target_db="ext_wk",
        ext_warehouse_db="ext_wh",
        cda_src_db="cda",
    )
    expected.spark_environment = SparkEnvironment(
        SPARK_HOME="{pyspark_pipeline_home}/deps/spark-{pyspark_version}",
        remote_spark_build_path=(
            "hdfs://nameservicehadoopenv2/user/ag82944/"
            "spark-{pyspark_version}.tgz"
        ),
        spark_libs_path=(
            "hdfs://nameservicehadoopenv2/user/ag82944/"
            "spark-{pyspark_version}-libs.jar"
        ),
        JAVA_HOME="/usr/lib/jvm/jre-1.8.0-openjdk",
        SPARK_CONF_DIR="{pyspark_pipeline_home}/deps/spark-conf",
        YARN_CONF_DIR="/etc/hadoop/conf.cloudera.yarn",
        HADOOP_CONF_DIR="/etc/hadoop/conf:/etc/hive/conf",
        PATH=[
            "/opt/cloudera/parcels/CDH/bin",
            "/usr/lib/jvm/jre-1.8.0-openjdk",
        ],
    )

    return expected


@freeze_time("2021-01-01 0:00:00", tz_offset=0)
@pytest.mark.parametrize(
    "input_string,expected",
    [
        ("since 5 years", date(2016, 1, 3)),
        ("since 100 years", date(1921, 1, 26)),
        ("since 2 years", date(2019, 1, 2)),
        ("since 365 days", date(2020, 1, 2)),
        ("since 2 days", date(2020, 12, 30)),
        ("since 2 year", date(2019, 1, 2)),
        ("since 1 month", date(2020, 12, 2)),
        ("since 1 months", date(2020, 12, 2)),
        ("since 1 day", date(2020, 12, 31)),
        ("since 1 days", date(2020, 12, 31)),
    ],
)
def test_parse_since_date_str(input_string, expected):
    assert parse_since_date_str(input_string) == expected


@pytest.mark.parametrize(
    "input_string",
    [
        ("since 2.5 years"),
        ("1 years"),
        ("since 1 2 days"),
        ("since 2 days years"),
        ("since 2 days months"),
        ("since -1 days"),
    ],
)
def test_parse_since_date_str_exceptions(input_string):
    with pytest.raises(ValueError):
        parse_since_date_str(input_string)


def test_settings(settings_json, expected_settings):
    actual = Settings(**settings_json)
    assert actual == expected_settings


@freeze_time("2021-01-01 0:00:00", tz_offset=0)
def test_settings_with_since_date(settings_json, expected_settings):
    settings_json["include_start_date"] = "since 5 years"
    actual = Settings(**settings_json)
    expected_settings.include_start_date = date(2016, 1, 3)
    assert actual == expected_settings


@freeze_time("2021-01-01 0:00:00", tz_offset=0)
@pytest.mark.parametrize(
    "input,expected",
    [
        ("since 5 years", date(2016, 1, 3)),
        ("since 100 years", date(1921, 1, 26)),
        ("since 2 years", date(2019, 1, 2)),
        ("since 365 days", date(2020, 1, 2)),
        ("since 2 days", date(2020, 12, 30)),
        (date(2020, 12, 30), date(2020, 12, 30)),
        ("2021-12-01", date(2021, 12, 1)),
        ("2022/2/01", date(2022, 2, 1)),
    ],
)
def test_validate_date(input, expected):
    assert validate_date(input) == expected


def test_get_settings(settings_obj, settings_json, expected_settings):
    settings_obj.target_path = (
        "s3a://bucket_path/"
        "etl/pyspark-pipeline/{table_tag}/{suffix}/{job_name}"
    )

    args = Namespace(
        table_tag="new_tag",
        incremental_load_start_date="2020-01-01",
        incremental_load_end_date="2021-01-01",
        spark_submit_mode=None,
        run_id="dragonfruit",
        override_settings="",
    )
    actual = get_settings(args, settings_json)

    expected_settings.table_tag = "new_tag"
    expected_settings.target_path = expected_settings.target_path.replace(
        "tabletag", "new_tag"
    )

    assert actual == expected_settings


def test_get_settings_with_override(
    settings_obj, settings_json, expected_settings
):
    yaml_path = (
        Path(__file__).parent.parent / "data" / "test_override_settings.yaml"
    )
    # settings from yaml path will now be
    # the override settings
    args = Namespace(
        table_tag="",
        incremental_load_start_date="",
        incremental_load_end_date="",
        spark_submit_mode=None,
        run_id="dragonfruit",
        override_settings=str(yaml_path),
    )

    with yaml_path.open("r") as f:
        expected_json = yaml.safe_load(f)

    actual = get_settings(args, settings_json)
    expected = Settings(**expected_json)

    # run_id and spark_env not in expected_json
    # but they are in settings_json so they should be
    # in merged settings
    expected.spark_environment = settings_json["spark_environment"]
    expected.run_id = settings_json["run_id"]

    expected.source_tables = {
        **expected.source_tables,
        **settings_json["source_tables"],
    }
    expected.hive_output_table_type = None

    for k, v in expected.dict().items():
        assert actual.dict()[k] == v

    assert actual == expected

    args = Namespace(
        table_tag="new_tag",
        incremental_load_start_date="2020-01-01",
        incremental_load_end_date="2021-01-01",
        spark_submit_mode=None,
        run_id="dragonfruit",
        override_settings=str(yaml_path),
    )

    expected.table_tag = "new_tag"
    expected.target_path = (
        "s3a://bucket_path/"
        "etl/pyspark-pipeline/new_tag/20170101_88881231/base_reg_test/"
    )

    actual = get_settings(args, settings_json)

    assert actual == expected


def test_get_table_name(expected_settings):
    actual = get_table_name("test_job", expected_settings)
    expected = "ext_wk.test_job_tabletag_20160101_20210101"

    assert actual == expected


def test_get_incremental_date_ranges_incremental_true(
    expected_settings,
    settings_json,
    audit_df,
):

    args = Namespace(
        incremental_processing_type="update",
        incremental_load_start_date=None,
        incremental_load_end_date=None,
    )

    actual = get_incremental_date_ranges(
        args, Settings(**settings_json), audit_df, None
    )

    expected_settings.incremental_load_start_date = datetime(2020, 1, 1)
    expected_settings.include_start_date = datetime(1980, 1, 1)

    for key in expected_settings.dict():
        assert actual.dict()[key] == expected_settings.dict()[key]

    assert actual == expected_settings


def test_get_incremental_date_ranges_incremental_false(
    expected_settings, settings_json, audit_df
):
    args = Namespace(
        incremental_processing_type=None,
        incremental_load_start_date=str(date(1983, 1, 1)),
        incremental_load_end_date=str(date(1984, 1, 1)),
    )
    expected_settings.incremental_load_start_date = datetime(1983, 1, 1)
    expected_settings.incremental_load_end_date = datetime(1984, 1, 1)
    expected_settings.include_start_date = date(2017, 1, 1)

    actual = get_incremental_date_ranges(
        args, Settings(**settings_json), None, None
    )

    assert actual == expected_settings


def test_get_incremental_date_ranges_with_args(
    expected_settings, settings_json, audit_df
):
    args = Namespace(
        incremental_processing_type="update",
        incremental_load_start_date=str(date(1983, 1, 1)),
        incremental_load_end_date=str(date(1984, 1, 1)),
    )

    actual = get_incremental_date_ranges(
        args, Settings(**settings_json), audit_df, None
    )
    expected_settings.include_start_date = datetime(1980, 1, 1)
    expected_settings.incremental_load_start_date = datetime(1983, 1, 1)
    expected_settings.incremental_load_end_date = datetime(1984, 1, 1)

    assert actual == expected_settings


def test_set_regression_test_databases():
    databases = Databases(
        source_db="source_db",
        target_db="ext_wk",
    )

    expected = Databases(
        source_db="ext_wk",
        target_db="ext_wk",
    )

    actual = set_regression_test_databases(databases)

    assert actual == expected


def test_source_table_parsing():
    yaml_path = (
        Path(__file__).parent.parent / "data" / "test_override_settings.yaml"
    )
    with yaml_path.open("r") as f:
        actual = Settings(**yaml.safe_load(f)).source_tables["query_result_df"]

    expected = SourceTable(
        table_type="snowflake",
        location="EXT_WAREHOUSE_SCHEMA.EXT_WAREHOUSE_DB.query_df",
        query=(
            "SELECT FAKE.LOCATION.super_fake_udf(some_column) "
            "FROM EXT_WAREHOUSE_SCHEMA.EXT_WAREHOUSE_DB.query_df"
        ),
    )

    assert actual == expected
