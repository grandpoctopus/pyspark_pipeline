from pathlib import Path

import pytest

from pyspark_pipeline.print_bucket_name_for_spark_submit import (
    extract_bucket_name,
    get_target_path,
)

SETTINGS_PATH = Path(__file__).parent / "data" / "test_settings.yaml"


def test_get_target_path():
    actual = get_target_path(SETTINGS_PATH)
    expected = "hdfs://nameservicehadoopenv2//hdfs_path/targ"
    assert actual == expected


def test_extract_bucket_name():
    expected = "nectarines"
    actual = extract_bucket_name("s3a://nectarines/are/bald.peaches")
    assert actual == expected
    actual = extract_bucket_name("s3://nectarines/are/bald.peaches")
    assert actual == expected

    with pytest.raises(ValueError):
        extract_bucket_name("hdfs://nectarines/are/bald.peaches")
