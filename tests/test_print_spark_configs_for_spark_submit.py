from pathlib import Path
from typing import Dict

import pytest

from pyspark_pipeline.print_spark_configs_for_spark_submit import (
    get_spark_conf_string,
    get_spark_configs,
)

SETTINGS_PATH = Path(__file__).parent / "data" / "test_settings.yaml"


@pytest.fixture
def expected_spark_configs() -> Dict:
    return {
        "spark.master": "yarn",
        "spark.sql.shuffle.partitions": 1000,
        "spark.default.parallelism": 2000,
    }


def test_get_spark_configs(expected_spark_configs: Dict):
    actual = get_spark_configs(SETTINGS_PATH)
    assert actual == expected_spark_configs


def test_get_spark_conf_string(expected_spark_configs: Dict):
    actual = get_spark_conf_string(expected_spark_configs)

    expected = (
        "--conf spark.sql.shuffle.partitions=1000 "
        "--conf spark.default.parallelism=2000"
    )
    assert actual == expected
