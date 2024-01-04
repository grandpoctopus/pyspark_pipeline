import logging
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

BASE_PATH = Path.cwd()
LOG_DIRECTORY = BASE_PATH / "logs" / "pyspark_logs"


def get_logger(app_name: str, start_time: datetime):
    LOG_DIRECTORY.mkdir(exist_ok=True, parents=True)
    log_path = LOG_DIRECTORY / f"{app_name}_{start_time}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger()


def set_spark_log_level(spark: SparkSession, level: str):
    if level != "WARN":
        log_level = level.upper()
        if log_level not in ("WARN", "INFO", "DEBUG"):
            raise ValueError(
                "spark-log-level must be either 'WARN', 'INFO' or 'DEBUG'"
            )
        sc = spark.sparkContext
        sc.setLogLevel(log_level)
