from argparse import Namespace

import pyspark.sql.functions as F
import pytest
from freezegun import freeze_time  # type: ignore
from pyspark.sql.utils import AnalysisException
from pyspark_test import assert_pyspark_df_equal  # type: ignore

from pyspark_pipeline.jobs import Job
from pyspark_pipeline.queries import AuditQuery
from pyspark_pipeline.schemas import AuditSchema
from pyspark_pipeline.utilities.settings_utils import (
    Settings,
    SourceTable,
    get_table_name,
)
from pyspark_pipeline.utilities.spark_utils import (
    is_hadoop_and_hive_environment,
)

TARGET_PATH = "hdfs://nameservice/hdfsdata/test/"


@pytest.fixture
def mock_job(cluster_spark) -> Job:
    class Fake_Job(Job):
        def run(self):
            pass

    return Fake_Job(spark=cluster_spark, args=Namespace())


@pytest.mark.skipif(
    is_hadoop_and_hive_environment() is False,
    reason="not in hadooop/hive environment",
)
@freeze_time("2021-01-01 0:00:00", tz_offset=0)
def test_write_audit_hive(mock_job: Job, settings_obj: Settings, cluster_spark):
    table_name = get_table_name("test_write_audit", settings_obj)
    audit_spec = SourceTable(table_type="hive", location=table_name)
    settings_obj.run_id = "starfruit"
    settings_obj.source_tables["audit_df"] = audit_spec
    try:
        cluster_spark.sql(f"drop table {table_name}")
    except AnalysisException:
        pass

    audit_df = AuditQuery(
        spark=cluster_spark, settings=settings_obj, schema=AuditSchema()
    ).run()

    for col, dtype in AuditSchema().dict().items():
        audit_df = audit_df.withColumn(col, F.col(col).cast(dtype))

    mock_job.write_audit("test_write_audit", settings_obj)
    actual_df = cluster_spark.table(table_name)

    # check that write works if table doesn't exist
    assert_pyspark_df_equal(actual_df, audit_df)

    # check that the append works if table does exist
    mock_job.write_audit("test_write_audit", settings_obj)
    actual_updated_df = cluster_spark.table(table_name)

    assert_pyspark_df_equal(actual_updated_df, audit_df.union(audit_df))

    mock_job.spark.sql(f"drop table {table_name}")


@pytest.mark.skipif(
    is_hadoop_and_hive_environment() is False,
    reason="not in hadooop/hive environment",
)
@freeze_time("2021-01-01 0:00:00", tz_offset=0)
def test_write_audit_hdfs(mock_job: Job, settings_obj: Settings, cluster_spark):
    target_path = TARGET_PATH + "audit_test"

    def delete_path():
        sc = cluster_spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(  # type: ignore
            sc._jsc.hadoopConfiguration()  # type: ignore
        )
        fs.delete(sc._jvm.org.apache.hadoop.fs.Path(target_path), True)  # type: ignore

    delete_path()

    audit_spec = SourceTable(table_type="parquet", location=target_path)
    settings_obj.run_id = "starfruit"
    settings_obj.source_tables["audit_df"] = audit_spec

    audit_df = AuditQuery(
        spark=cluster_spark, settings=settings_obj, schema=AuditSchema()
    ).run()

    mock_job.write_audit("test_write_audit", settings_obj)
    actual_df = cluster_spark.read.parquet(target_path)

    # check that write works if table doesn't exist
    assert_pyspark_df_equal(actual_df, audit_df)

    # check that the append works if table does exist
    mock_job.write_audit("test_write_audit", settings_obj)
    actual_updated_df = cluster_spark.read.parquet(target_path)
    assert_pyspark_df_equal(actual_updated_df, audit_df.union(audit_df))

    delete_path()
