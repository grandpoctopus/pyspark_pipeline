from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from pyspark_pipeline.jobs import Job
from pyspark_pipeline.queries import sample_queries
from pyspark_pipeline.schemas import sample_schemas
from pyspark_pipeline.utilities.settings_utils import Settings, get_table_name


class SampleJob(Job):
    def __init__(
        self,
        settings: Settings,
        is_active_df: DataFrame,
        value_df: DataFrame,
        audit_df: Optional[DataFrame] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.settings = settings
        if audit_df is None:
            try:
                audit_df = self.spark.table(
                    get_table_name("sample_job_audit", self.settings)
                )
            except AnalysisException:
                pass

        self.audit_df = audit_df
        self.is_active_df = is_active_df
        self.value_df = value_df

    def run(self):
        sample_joined_df = sample_queries.SampleJoinQuery(
            is_active_df=self.is_active_df,
            value_df=self.value_df,
            schema=sample_schemas.SampleJoinSchema(),
            spark=self.spark,
            settings=self.settings,
        ).run()

        sample_output_df = sample_queries.SampleAggregationQuery(
            sample_joined_df=sample_joined_df,
            schema=sample_schemas.SampleAggregationSchema(),
            spark=self.spark,
            settings=self.settings,
        ).write(
            "sample_output",
            "csv",
            logger=None,
        )

        self.write_audit("sample_job_audit", self.settings)

        return {"sample_output_df": sample_output_df}
