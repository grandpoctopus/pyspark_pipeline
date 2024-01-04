from argparse import Namespace

from pyspark.sql import DataFrame, SparkSession
from pyspark_test import assert_pyspark_df_equal  # type: ignore

from pyspark_pipeline.jobs.sample_job import SampleJob
from pyspark_pipeline.utilities.settings_utils import Settings, SourceTable


def test_sample_job(
    local_spark: SparkSession,
    is_active_df: DataFrame,
    value_df: DataFrame,
    sample_output_df: DataFrame,
    settings_obj: Settings,
    expected_audit_df: DataFrame,
    tmpdir,
):
    args = Namespace()

    settings_obj.source_tables["audit_df"] = SourceTable(
        table_type="parquet", location=f"file:///{str(tmpdir)}"
    )
    actual_df = SampleJob(
        spark=local_spark,
        settings=settings_obj,
        args=args,
        is_active_df=is_active_df,
        value_df=value_df,
        audit_df=expected_audit_df,
    ).run()["sample_output_df"]

    assert_pyspark_df_equal(
        actual_df.orderBy("max_value"), sample_output_df.orderBy("max_value")
    )
