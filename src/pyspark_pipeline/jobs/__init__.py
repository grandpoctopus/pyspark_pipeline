from abc import ABC, abstractmethod
from argparse import Namespace
from logging import Logger
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession

from pyspark_pipeline.queries import (
    AuditQuery,
    CountKeysQuery,
    DistinctValueQuery,
)
from pyspark_pipeline.schemas import AuditSchema, CountKeysSchema, EtlSchema
from pyspark_pipeline.utilities.settings_utils import Settings


class Job(ABC):
    """
    Abstract class for excuting a series of spark-query queries
    """

    def __init__(
        self,
        spark: SparkSession,
        args: Namespace,
        logger: Logger = None,
        **kwargs,
    ):
        """
        args:
            spark: spark session for job
            args: an ArgumentParser NameSpace
            logger: A Logger
        """
        self.spark = spark
        self.args = args
        self.logger = logger
        super().__init__()

    def write_audit(
        self,
        job_name,
        settings: Settings,
    ):
        """
        Creates or updates the audit table for the job
        """
        hive_table_type = None
        table_name = None
        target_path = None
        audit_spec = settings.source_tables["audit_df"]
        if audit_spec.table_type == "hive":
            hive_table_type = "internal"
            table_name = audit_spec.location
        else:
            target_path = audit_spec.location

        AuditQuery(
            spark=self.spark,
            settings=settings,
            schema=AuditSchema(),
        ).write(
            job_name=job_name,
            write_format=audit_spec.table_type,
            logger=self.logger,
            hive_table_type=hive_table_type,
            write_mode="append",
            target_path=target_path,
            table_name=table_name,
            do_repartition=False,
        )

    def write_qa_counts(
        self,
        job_name,
        input_df: DataFrame,
        settings: Settings,
        cols: List,
    ):
        """
        Creates a table with counts for QA
        """

        return CountKeysQuery(
            spark=self.spark,
            settings=settings,
            schema=CountKeysSchema(),
            input_df=input_df,
            cols=cols,
        ).write(
            f"{job_name}_qa_{'&'.join(cols)}_count",
            "parquet",
            self.logger,
            hive_table_type=settings.hive_output_table_type,
            do_repartition=False,
        )

    def write_qa_distinct_ids(
        self,
        job_name,
        input_df: DataFrame,
        settings: Settings,
    ):
        """
        Creates a table with distinct values for QA
        """

        return DistinctValueQuery(
            spark=self.spark,
            settings=settings,
            schema=EtlSchema(),
            input_df=input_df,
            cols=["id"],
        ).write(
            f"{job_name}_qa_distinct_ids",
            "parquet",
            self.logger,
            hive_table_type=settings.hive_output_table_type,
        )

    @abstractmethod
    def run(self) -> Dict[str, DataFrame]:
        pass
