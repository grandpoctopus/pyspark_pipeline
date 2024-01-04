from datetime import datetime
from logging import RootLogger
from pathlib import Path

import pytest

from pyspark_pipeline.utilities import logging_utils


def test_get_logger():
    start_time = datetime.now()
    logger = logging_utils.get_logger("fake_app", start_time)
    expected_path = (
        Path(__file__).parent.parent.parent
        / "logs"
        / "pyspark_logs"
        / f"fake_app_{start_time}.log"
    )
    assert expected_path.exists()
    assert type(logger) == RootLogger


def test_set_spark_log_level(local_spark):
    logging_utils.set_spark_log_level(local_spark, "info")

    logging_utils.set_spark_log_level(local_spark, "WARN")

    with pytest.raises(ValueError):
        logging_utils.set_spark_log_level(local_spark, "foo")
