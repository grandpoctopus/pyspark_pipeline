from datetime import datetime
from typing import Generator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType, StructField, StructType


@pytest.fixture
def subset_id_table_name(scope="module"):
    db = "ds_dtlaixxph_gbd_r000_wk"
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M")
    table = f"{timestamp}_test_ids_subset_source"
    return f"{db}.{table}"


@pytest.fixture
def id_df_from_hive(
    local_spark: SparkSession,
    subset_id_table_name: str,
) -> Generator[DataFrame, None, None]:
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
        ]
    )

    df: DataFrame = local_spark.createDataFrame(
        [
            (1,),
            (123321,),
        ],
        schema,
    )
    df.write.saveAsTable(subset_id_table_name, mode="overwrite")
    yield df
    local_spark.sql(f"DROP TABLE IF EXISTS {subset_id_table_name}")
