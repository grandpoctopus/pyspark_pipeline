import os
import shutil
import signal
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml
from pyspark.sql import SparkSession

from pyspark_pipeline.utilities.settings_utils import Settings, SourceTable
from pyspark_pipeline.utilities.spark_utils import (
    is_hadoop_and_hive_environment,
    set_env_vars,
)

from .test_queries_fixtures import (  # NOQA
    expected_audit_df,
    query_expected_test_df,
    query_test_df,
)
from .test_sample_queries_fixtures import (  # NOQA
    is_active_df,
    sample_output_df,
    value_df,
)
from .test_subset_queries_fixtures import (  # NOQA
    id_df_from_hive,
    subset_id_table_name,
)

HIVE_JARS = Path(__file__).parent.parent / "jars" / "hive-hcatalog-core.jar"


@pytest.fixture(scope="module")
def valid_timestamp() -> datetime:
    return datetime(1990, 1, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def invalid_timestamp_before() -> datetime:
    return datetime(1969, 1, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def invalid_timestamp_after() -> datetime:
    return datetime(8888, 1, 1, 2, 0, 1, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def module_tmpdir():
    tmpdir = tempfile.mkdtemp()
    subdir = os.path.join(tmpdir, "sub")
    os.mkdir(subdir)
    path = os.path.join(subdir, "testCurrentTicketCount.txt")
    yield f"file://{path}"
    shutil.rmtree(tmpdir)


@pytest.fixture(scope="module")
def settings_obj(module_tmpdir) -> Settings:
    yaml_path = Path(__file__).parent / "data" / "test_override_settings.yaml"
    with yaml_path.open() as f:
        settings_obj = Settings(**yaml.safe_load(f))

    settings_obj.source_tables = {
        "fake_table": SourceTable(
            table_type="fake_type",
            location="fake_location",
        ),
    }
    settings_obj.job_name = "fake_job"
    settings_obj.include_start_date = datetime(1981, 1, 1, tzinfo=timezone.utc)
    settings_obj.include_end_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
    settings_obj.hive_output_table_type = None
    settings_obj.target_path = module_tmpdir
    return settings_obj


@pytest.fixture(scope="module")
def settings_obj_aws(module_tmpdir) -> Settings:
    yaml_path = Path(__file__).parent / "data" / "aws_test_settings.yaml"
    with yaml_path.open() as f:
        settings_obj = Settings(**yaml.safe_load(f))

    settings_obj.hive_output_table_type = None
    settings_obj.target_path = module_tmpdir
    return settings_obj


@pytest.fixture(scope="module")
def local_spark(settings_obj) -> SparkSession:
    if is_hadoop_and_hive_environment():
        set_env_vars(settings_obj.spark_environment.dict())
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    return (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.jars", str(HIVE_JARS))
        .appName("pyspark_pipeline_pytest_local")
        .enableHiveSupport()
        .getOrCreate()
    )


# NOTE there seems to be some issue with SparkSession configs
# bleeding over from local_spark to cluster_spark and vice versa
# when they are run on the same pytest worker. So if a config
# you did not intend to set in one or the other seems set
# then this bleed over is likely the cause


@pytest.fixture(scope="module")
def cluster_spark(settings_obj) -> SparkSession:
    set_env_vars(settings_obj.spark_environment.dict())
    os.environ["PYSPARK_DRIVER_PYTHON"] = "pyenv/bin/python"
    os.environ["PYSPARK_PYTHON"] = "pyenv/bin/python"
    zip_path = Path(__file__).parent.parent / "spark-query.zip"
    spark = (
        SparkSession.builder.master("yarn")
        .config("spark.yarn.dist.archives", f"{zip_path}#pyenv")
        .config("spark.shuffle.service.enabled", False)
        .config("spark.yarn.queue", "root.dtl_aixx_yarn")
        .config("spark.driver.port", "20000")
        .config("spark.driver.blockManager.port", "20000")
        .config("spark.port.maxRetries", "5000")
        .config("spark.dynamicAllocation.enabled", False)
        .config("spark.driver.memory", "20g")
        .config("spark.driver.cores", "1")
        .config("spark.executor.memory", "40g")
        .config("spark.executor.cores", "8")
        .config("spark.executor.memoryOverhead", "16g")
        .config("spark.locality.wait", "200ms")
        .config("spark.sql.read.partition", "1")
        .config("spark.sql.shuffle.partition", "1")
        .config("spark.default.parallelism", "1000")
        .config("spark.driver.maxResultSize", "5g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.hadoop.hive.exec.dynamic.partition", True)
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
        .config(
            "spark.yarn.archive",
            "hdfs://nameserviceds2/user/ag82944/spark-3.1.2-libs.jar",
        )
        .config("spark.jars", str(HIVE_JARS))
        .appName("pyspark_pipeline_pytest")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql(f"add jar {str(HIVE_JARS)}")

    return spark


@pytest.fixture(scope="module")
def handle_server():
    print(" Set Up Moto Server")
    process = subprocess.Popen(
        "moto_server s3",
        stdout=subprocess.PIPE,
        shell=True,
        preexec_fn=os.setsid,
    )

    yield
    print("Tear Down Moto Server")
    os.killpg(os.getpgid(process.pid), signal.SIGTERM)


@pytest.fixture(scope="module")
def s3_spark(handle_server):
    # Setup spark to use s3, and point it to the moto server.

    spark_session = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
        .config("spark.jars", str(HIVE_JARS))
        .appName("pyspark_pipeline_pytest_s3_local")
        .enableHiveSupport()
        .getOrCreate()
    )

    hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()  # NOQA
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", "mock")
    hadoop_conf.set("fs.s3a.secret.key", "mock")
    hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:5000")

    time.sleep(3)
    TEST_DB_NAME = "fake_pyspark_pipeline_database"
    spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {TEST_DB_NAME}")
    time.sleep(3)

    yield spark_session

    spark_session.sql(f"DROP DATABASE {TEST_DB_NAME} CASCADE")


@pytest.fixture(scope="module")
def valid_efctv_dt() -> datetime:
    return datetime(1990, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def invalid_efctv_dt() -> datetime:
    return datetime(8888, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def valid_trmntn_dt() -> datetime:
    return datetime(8888, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def invalid_trmntn_dt() -> datetime:
    return datetime(1969, 1, 1, tzinfo=timezone.utc)


def dt_str(input_datetime: datetime) -> str:
    return str(input_datetime)[:10].replace("-", "")
