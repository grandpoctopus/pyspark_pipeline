import logging
from argparse import Namespace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, validator
from pyspark import __version__ as pyspark_version
from pyspark.sql import DataFrame

NAME_SERVICE = "nameservice{settings.hadoop_env}2"
MAX_PATH_LENGTH = 300


def today_datetime() -> datetime:
    """
    Returns a timestamp from the very beginning of today
    """
    today = date.today()
    return datetime(today.year, today.month, today.day, 0, 0, 0)


def parse_since_date_str(input_string: str) -> date:
    """
    Gets a date from a sring like
    'since 5 years' that will be interpreted as
    a date 5 years before today's date.
    """
    example = "since 5 years"
    input_words = [
        word for word in input_string.lower().split(sep=" ") if word != " "
    ]

    input_string = " ".join(input_words)

    if not input_string.startswith("since") or len(input_words) != 3:
        raise ValueError(f"'{input_string}' not in format '{example}'")

    valid_units = {"year", "years", "day", "days", "month", "months"}

    unit = input_words[2]

    if unit not in valid_units:
        raise ValueError(
            f"Date string, '{input_string}', "
            f"must contain exactly one of the units: {' '.join(valid_units)}"
        )

    quantity_error = (
        f"Date string, '{input_string}', must contain apositive integer."
    )

    try:
        quantity = int(input_words[1])
        if quantity < 1:
            raise ValueError(quantity_error)
    except ValueError:
        raise ValueError(quantity_error)

    if unit.startswith("month"):
        quantity *= 30
    elif unit.startswith("year"):
        quantity *= 365

    return datetime.today().date() - timedelta(days=quantity)


def validate_date(input_date: Union[date, str]) -> date:
    if isinstance(input_date, date):
        return input_date
    try:
        clean_date = input_date.replace("/", "-")[:10]
        return datetime.strptime(clean_date, "%Y-%m-%d").date()
    except ValueError:
        try:
            return parse_since_date_str(input_date)
        except ValueError:
            raise ValueError(f"Cannot parse {input_date}")


class Databases(BaseModel):
    """
    A dataclass for the databases
    """

    source_db: Optional[str] = None
    target_db: Optional[str] = None


class SnowFlakeSettings(BaseModel):
    """
    A dataclass for SnowFlake connection settings
    """

    url: str
    warehouse: str
    role: Optional[str] = None


class SourceTable(BaseModel):
    """
    A dataclass for source table information
    """

    table_type: str
    location: str
    query: Optional[str] = None
    options: Dict = {}


class SparkEnvironment(BaseModel):
    SPARK_HOME: str
    remote_spark_build_path: Optional[str]
    spark_libs_path: Optional[str]
    JAVA_HOME: Optional[str]
    SPARK_CONF_DIR: Optional[str]
    YARN_CONF_DIR: Optional[str]
    HADOOP_CONF_DIR: Optional[str]
    PATH: Optional[List[str]]

    @validator("SPARK_HOME", always=True)
    def validate_spark_environment(cls, SPARK_HOME: str, values: Dict):
        pyspark_pipeline_home = Path(__file__).parent.parent.parent.parent
        parsed_spark_home = SPARK_HOME.format(
            pyspark_pipeline_home=pyspark_pipeline_home,
            pyspark_version=pyspark_version,
        )
        return parsed_spark_home

    @validator("SPARK_CONF_DIR", always=True)
    def validate_spark_conf_dir(cls, SPARK_CONF_DIR):
        pyspark_pipeline_home = Path(__file__).parent.parent.parent.parent
        return SPARK_CONF_DIR.format(
            pyspark_pipeline_home=pyspark_pipeline_home
        )

    @validator("remote_spark_build_path", always=True)
    def validate_remote_spark_build_path(cls, remote_spark_build_path):
        return remote_spark_build_path.format(pyspark_version=pyspark_version)

    @validator("spark_libs_path", always=True)
    def validate_spark_libs_path(cls, spark_libs_path):
        return spark_libs_path.format(pyspark_version=pyspark_version)

    @validator("PATH", always=True)
    def validate_PATH(cls, PATH: List[str], values: Dict):
        return [x.format(JAVA_HOME=values.get("JAVA_HOME")) for x in PATH]


class Settings(BaseModel):
    """
    A dataclass for the job settings
    """

    spark_environment: Optional[SparkEnvironment]
    job_name: str
    include_start_date: date
    include_end_date: date
    suffix: Optional[str] = ""
    dataset_suffix: Optional[str] = ""
    table_tag: Optional[str] = ""
    hadoop_env: Optional[str] = ""
    release: Optional[str] = ""
    databases: Databases
    spark_configs: Dict
    target_path: str
    hive_output_table_type: Optional[str] = None
    incremental_processing_type: Optional[str] = None
    incremental_load_start_date: datetime = datetime(1970, 1, 1)
    incremental_load_end_date: datetime = today_datetime()
    spark_submit_mode: Optional[str] = "client"
    database_udfs: Optional[Dict] = None
    source_tables: Dict[str, SourceTable]
    sor_column_name: Optional[str] = ""
    output_partitions: Optional[int] = 1000
    bins_per_partition: Optional[int] = 1
    run_id: str = datetime.now().isoformat()
    snowflake_settings: Optional[SnowFlakeSettings]

    class Config:
        validate_assignment = True

    @validator("include_start_date", pre=True, always=True)
    def parse_include_start_date(
        cls, include_start_date: Union[date, str]
    ) -> date:
        return validate_date(include_start_date)

    @validator("include_end_date", pre=True, always=True)
    def parse_include_end_date(cls, include_end_date: Union[date, str]) -> date:
        return validate_date(include_end_date)

    @validator("incremental_load_start_date", pre=True, always=True)
    def parse_incremental_load_start_date(
        cls, incremental_load_start_date: Union[date, str]
    ) -> date:
        return validate_date(incremental_load_start_date)

    @validator("incremental_load_end_date", pre=True, always=True)
    def parse_incremental_load_end_date(
        cls, incremental_load_end_date: Union[date, str]
    ) -> date:
        return validate_date(incremental_load_end_date)

    @validator("spark_configs", always=True)
    def parse_spark_configs(cls, spark_configs: Dict):
        return {
            k: (
                v.format(pyspark_version=pyspark_version)
                if isinstance(v, str)
                else v
            )
            for k, v in spark_configs.items()
        }

    @validator("target_path", always=True)
    def parse_target_path(cls, target_path: str, values: Dict):
        return target_path.format(
            hadoop_env=values.get("hadoop_env"),
            table_tag=values.get("table_tag"),
            suffix=values.get("suffix"),
            job_name=values.get("job_name"),
        )

    @validator("source_tables", pre=False)
    def parse_source_tables(cls, source_tables: Dict, values: Dict):
        """
        Uses settings attributes to format source_table locations
        """
        databases = values.get("databases", Databases())
        for k, v in source_tables.items():
            location = v.location.format(
                source_db=databases.source_db,
                target_db=databases.target_db,
                table_tag=values.get("table_tag"),
                suffix=values.get("suffix"),
                dataset_suffix=values.get("dataset_suffix"),
                lob_short=values.get("lob_short"),
                job_name=values.get("job_name"),
                hadoop_env=values.get("hadoop_env"),
            )
            source_tables[k].location = location
            if v.query is not None:
                if values.get("database_udfs") is None:
                    database_udfs = {}
                else:
                    database_udfs = values.get("database_udfs", {})

                source_tables[k].query = v.query.format(
                    source_db=databases.source_db,
                    target_db=databases.target_db,
                    table_tag=values.get("table_tag"),
                    suffix=values.get("suffix"),
                    dataset_suffix=values.get("dataset_suffix"),
                    lob_short=values.get("lob_short"),
                    job_name=values.get("job_name"),
                    hadoop_env=values.get("hadoop_env"),
                    location=location,
                    **database_udfs,
                )
        return source_tables


def get_settings_json(settings_yaml: str) -> Dict:
    """
    Reads settings yaml as either yaml string or yaml path

    args:
        settings_yaml: either a settings yaml or a path to
            a settings yaml file
    """
    if (
        len(str(settings_yaml)) < MAX_PATH_LENGTH
        and Path(settings_yaml).is_file()
    ):
        with Path(settings_yaml).open() as f:
            settings_json = yaml.safe_load(f)
    else:
        try:
            settings_json = yaml.safe_load(settings_yaml)
        except TypeError:
            raise ValueError(
                f"Argument passed to `--settings-yaml`, '{settings_yaml}', "
                "is neither a valid spark-query settings yaml string "
                "nor a path to a valid spark-query settings yaml file"
            )
    return settings_json


def combine_settings_jsons(settings_json: Dict, override_settings_yaml: str):
    """
    Get a combined settings json by using keys
    specified in override_settings_yaml, unless
    they are specified in settings_json.

    Source tables are handled differently than other
    keys, all source tables from both settings
    are used. If conflicting keys are listed
    those in settings_json are used instead.

    args:
        settings: Settings object source of source_tables
        override_settings_path: path to yaml for generic
            settings
    """
    override_settings_json = get_settings_json(override_settings_yaml)
    settings_json["source_tables"] = {
        **override_settings_json["source_tables"],
        **settings_json["source_tables"],
    }

    for setting, value in override_settings_json.items():
        if setting != "source_tables":
            settings_json[setting] = value

    return settings_json


def get_settings(
    args: Namespace, settings_json: Dict, settings_class: Callable = Settings
) -> Settings:
    """
    Uses args and the settings yaml to get the user defined
    settings for the job

    args:
        args: the command line arguments captured by arg parser
    """
    if args.override_settings:
        settings_json = combine_settings_jsons(
            settings_json, args.override_settings
        )

    if args.table_tag:
        settings_json["table_tag"] = args.table_tag

    settings = settings_class(**settings_json)

    # if command line argument is passed for spark_submit_mode
    # overwrite  spark_submit_mode setting in settings
    # this results in not creating a zipped conda env
    # so that env can be passed into the spark submit-script
    if args.spark_submit_mode is not None:
        settings.spark_submit_mode = args.spark_submit_mode

    try:
        if args.incremental_processing_type:
            settings.incremental_processing_type = (
                args.incremental_processing_type
            )
    except AttributeError:
        pass

    if args.run_id:
        settings.run_id = args.run_id

    return settings


def get_table_name(job_name: str, settings: Settings) -> str:
    return (
        f"{settings.databases.target_db}.{job_name}_"
        f"{settings.table_tag}_{settings.suffix}"
    )


def get_incremental_date_ranges(
    args: Namespace,
    settings: Settings,
    audit_df: Optional[DataFrame],
    logger: Optional[logging.Logger] = None,
) -> Settings:
    """
    For incremental loads Finds the max storage_time_stamp from audit table.
    Uses original eligibillity start date from first full load to capture
    updates to events and uses the previous run's incremental_load_end_date
    as the current run's incremental_load_start_date.

    For full load, unless an argument is passed storage filtering is
    oped up for all available data (1970 - current day)

    args:
        args: arguments passed in by user
        settings: a settings object with user settings
        logger: a logging object
        audit_df: Spark DataFrame containg information about previous
            runs of the job.
    """
    if args.incremental_processing_type is not None and audit_df is not None:
        if logger:
            logger.info(
                "Running job with incremental processing: %s",
                args.incremental_processing_type,
            )

        previous_run_settings = audit_df.orderBy(
            "run_timestamp", ascending=False
        ).take(1)[0]
        settings.incremental_load_start_date = (
            previous_run_settings.incremental_load_end_date
        )
        settings.include_start_date = previous_run_settings.include_start_date
        settings.incremental_load_end_date = today_datetime()

    else:
        if logger:
            logger.info("Running job as Full load")

    if args.incremental_load_start_date:
        start_date = datetime.strptime(
            args.incremental_load_start_date, "%Y-%m-%d"
        )
        settings.incremental_load_start_date = start_date

    if args.incremental_load_end_date:
        end_date = datetime.strptime(args.incremental_load_end_date, "%Y-%m-%d")
        settings.incremental_load_end_date = end_date

    return settings


def set_regression_test_databases(databases: Databases):
    """
    Regression test jobs use the target_db for all tables,
    this sets all of the databases in the databases object to
    target_db

    args:
        databases: a data class object with all of the database names used in the script
    """
    target_db = databases.target_db
    databases_dict = databases.dict()
    for k, v in databases_dict.items():
        databases_dict[k] = target_db
    return Databases(**databases_dict)
