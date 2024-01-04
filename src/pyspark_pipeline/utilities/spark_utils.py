import os
from pathlib import Path
from shutil import make_archive
from typing import Dict, Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark_pipeline.utilities.settings_utils import Settings
from pyspark_pipeline.utilities.spark_environment_utils import (
    get_or_create_spark_home,
)


def is_hadoop_and_hive_environment() -> bool:
    return bool(os.environ.get("HADOOP_HOME"))


def get_zipped_conda_env(
    env_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    rebuild_env: bool = True,
) -> Path:
    if env_path is None:
        env_path = Path(str(os.environ.get("CONDA_PREFIX")))

    if output_dir is None:
        output_dir = Path.cwd()

    if env_path is None:
        return None
    else:

        def make_archive_helper(archive_path: Path, dry_run=False) -> Path:
            return Path(
                make_archive(
                    str(archive_path),
                    format="zip",
                    root_dir=env_path,
                    dry_run=dry_run,
                )
            )

        output_archive_without_extension = output_dir / env_path.name
        output_archive = make_archive_helper(
            output_archive_without_extension, dry_run=True
        )

        if not output_archive.exists() or rebuild_env:
            print("Archiving conda environment. This may take a while.")
            output_archive = make_archive_helper(
                output_archive_without_extension
            )

        return output_archive


def set_env_vars(env_vars: Dict):
    for name, var in env_vars.items():
        if isinstance(var, list):
            append_var = ":".join(var)
            try:
                new_var = f"{append_var}:{os.environ[name]}"
            except KeyError:
                new_var = append_var
            if not os.environ.get(name, "").startswith(append_var):
                os.environ[name] = new_var
        else:
            assert isinstance(var, str)
            os.environ[name] = var


def get_spark_conf(spark_configs: Dict) -> SparkConf:
    """
    Use a dict of spark configs (which can be provide by a settings yaml)
    to make a SparkConf object that can be used with SparkSession.builder
    to make a spark session with the specified settings

    args:
        spark_configs: a dict of spark settings key = settings_name,
            value = settings_value
    """
    spark_conf = SparkConf()
    for config, setting in spark_configs.items():
        spark_conf.set(config, setting)
    return spark_conf


def register_sql_jars(spark: SparkSession, spark_configs: Dict):
    """
    Spark 3 has issues with not registering
    all jars unless they are "added" this
    function adds any included jars through
    a sql statement.

    args:
        spark_configs: a Dict of spark configs
    """
    for k, v in spark_configs.items():
        if k == "spark.jars":
            for jar in v.split(sep=","):
                spark.sql(f"add jar {jar}")


def get_spark_session(
    app_name: str,
    spark_conf: SparkConf,
    settings: Settings,
    rebuild_conda_env: bool = True,
    zipped_environment_path: str = None,
) -> SparkSession:
    """
    Uses configs from a sparkconf object to set create a spark session

    args:
        app_name: string specifying the app name
        spark_conf: a spark conf object create from the user provided
            settings
        settings: a settings object containing job settings
    """
    if settings.spark_submit_mode == "client":
        if zipped_environment_path is None:
            zipped_conda_env_path = get_zipped_conda_env(
                rebuild_env=rebuild_conda_env
            )
        else:
            zipped_conda_env_path = Path(zipped_environment_path)
        if settings.spark_environment is not None:
            spark_env_settings = settings.spark_environment.dict()
            spark_env_settings["SPARK_HOME"] = get_or_create_spark_home(
                spark_env_settings["SPARK_HOME"],
                spark_env_settings["remote_spark_build_path"],
            )
            set_env_vars(spark_env_settings)

        # the spark_conf setting tells spark to upload
        # the zipped virtual env
        # and aliases as "pyenv" the variables that are set
        # tell spark workers to use the virtual env provided
        # as its python
        spark_conf.set(
            "spark.yarn.dist.archives", f"{zipped_conda_env_path}#pyenv"
        )
        os.environ["PYSPARK_DRIVER_PYTHON"] = "pyenv/bin/python"
        os.environ["PYSPARK_PYTHON"] = "pyenv/bin/python"

    spark = (
        SparkSession.builder.appName(app_name)
        .config(conf=spark_conf)
        .enableHiveSupport()
        .getOrCreate()
    )

    register_sql_jars(spark, settings.spark_configs)

    return spark
