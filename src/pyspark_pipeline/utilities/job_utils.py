import os
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional, cast

import py4j  # type: ignore
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from pyspark_pipeline.utilities.logging_utils import (
    get_logger,
    set_spark_log_level,
)
from pyspark_pipeline.utilities.settings_utils import (
    Settings,
    SourceTable,
    get_settings,
    get_settings_json,
)
from pyspark_pipeline.utilities.spark_utils import (
    get_spark_conf,
    get_spark_session,
)

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


def get_snowflake_creds() -> Dict:
    """
    Extracts Snowflake login credentials from environment
    variables
    """
    env_vars = {
        "sf_user": "SNOWFLAKE_USER",
        "sf_password": "SNOWFLAKE_PASSWORD",
    }
    result = {}
    for k, v in env_vars.items():
        var = os.environ.get(v)
        if var is None:
            raise ValueError(
                f"No environment set for {v} so 'snowflake' table "
                "{table.table_location} cannot be accessed"
            )
        result[k] = var

    return result


def parse_snowflake_location(source_table: SourceTable) -> Dict:
    """
    Parses table location in the format
    snowflake_url.snowflake_warehouse.schema.database.table
    into url, warehouse, database and table
    """
    location = source_table.location
    try:
        schema, database, table = location.split(".")
    except ValueError:
        expected_format = "< schema >.< database >.< table >"
        raise ValueError(
            f"SnowFlake table locations must be specified as {expected_format}"
        )
    return {"schema": schema, "database": database, "table": table}


def get_snowflake_options(
    snowflake_creds: Dict, table_location: Dict, settings: Settings
) -> Dict:
    """
    Uses snowflake creds and table location to make snowflake connector
    config object

    args:
        snow_flake_creds: contains sf_user and sf_password
        table_location: contains schema, database and table name
        settings: pyspark-pipeline job settings
    """
    message = (
        "settings file does not contain 'snowflake_settings' so "
        f"{table_location['table']} cannot be loaded "
    )
    assert settings.snowflake_settings is not None, message

    return {
        "sfurl": settings.snowflake_settings.url,
        "sfUser": snowflake_creds["sf_user"],
        "sfRole": settings.snowflake_settings.role,
        "sfPassword": snowflake_creds["sf_password"],
        "sfDatabase": table_location["database"],
        "sfSchema": table_location["schema"],
        "sfWarehouse": settings.snowflake_settings.warehouse,
        "ocspFailOpen": "false",
    }


def get_snowflake_dataframe(
    spark: SparkSession,
    source_table: SourceTable,
    settings: Settings,
) -> DataFrame:
    """
    Uses snowflake creds and table location to read a snowflake
    table into a Spark DataFrame

    args:
        spark: SparkSession
        source_table: source table definition that contains
            table location string
    """
    sf_creds = get_snowflake_creds()
    table_location = parse_snowflake_location(source_table)

    sf_options = {
        **get_snowflake_options(sf_creds, table_location, settings),
        **source_table.options,
    }

    if source_table.query is None:
        read_format = ["dbtable", table_location["table"]]
    else:
        read_format = ["query", source_table.query]

    return (
        spark.read.format(SNOWFLAKE_SOURCE_NAME)
        .options(**sf_options)
        .option(*read_format)
        .load()
    )


def get_snowflake_query_result(
    spark: SparkSession,
    source_table: SourceTable,
    settings: Settings,
    query: str,
) -> DataFrame:
    """
    Uses snowflake creds and table location to read a snowflake
    table into a Spark DataFrame

    args:
        spark: SparkSession
        source_table: source table definition that contains
            table location string
        query: query executed to obtain dataframe
    """
    sf_creds = get_snowflake_creds()
    table_location = parse_snowflake_location(source_table)
    sf_options = get_snowflake_options(sf_creds, table_location, settings)

    # if a user specifies the full location in the query
    # snowflake will throw an error so the 'schema.database.'
    # needs to be removed if present
    query = query.replace(f"{table_location['schema']}.", "")
    query = query.replace(f"{table_location['database']}.", "")

    return (
        spark.read.format(SNOWFLAKE_SOURCE_NAME)
        .options(**sf_options)
        .option("query", query)
        .load()
    )


def read_hive_table(spark: SparkSession, table_location: str) -> DataFrame:
    """
    spark 3 upgrade broke the ability to read json format
    tables from Hive, but they can be read from hdfs location

    spark: a spark session
    table_location: location
    """

    def df_is_usable(input_df: DataFrame) -> bool:
        try:
            input_df.limit(1).count()
            return "_corrupt_record" not in input_df.columns
        except py4j.protocol.Py4JJavaError:
            return False

    df = spark.table(table_location)
    if df_is_usable(df):
        return df
    json_df = spark.read.schema(df.schema).json(df.inputFiles()[0])
    if df_is_usable(json_df):
        return json_df
    else:
        message = (
            f"{table_location} cannot be loaded as A Hive Table"
            "try using table's underlying object store path and "
            "specify its format in the source_tables settings"
        )
        raise ValueError(message)


def get_source_table_dataframe(
    spark: SparkSession,
    source_table: SourceTable,
    settings: Settings,
) -> DataFrame:
    """
    Returns a Spark Dataframe for the table definition
    corresponding to the table_name
    spark: a spark session
    source_table: a SourceTable object with table type and location
        information
    settings: Settings object containg pyspark-pipeline job settings
    """
    if source_table.table_type == "hive":
        return read_hive_table(spark, source_table.location)

    elif source_table.table_type == "snowflake":
        return get_snowflake_dataframe(spark, source_table, settings)

    else:
        return spark.read.load(
            source_table.location,
            format=source_table.table_type,
            **source_table.options,
        )


def get_subset_filter_str(
    subset_population_df: DataFrame,
    shared_columns: List,
) -> str:
    """
    Creates a filter analogous to an inner-join
    between the subset_population_df and some other dataframe
    but has better performance in joins between
    a spark dataframe and a datasource that
    has filter-pushdown like a dataframe from a
    SnowFlake table

        args:
            subset_population_df: dataframe used to subset some
                input dataframe
            shared_columns: common columns between
                an input dataframe and the subset_population_df
    """
    conditions_matrix = []
    for col in shared_columns:
        new_conditions = [
            f"cast({col} AS string) = '{x[0]}'"
            for x in subset_population_df.select(col).toLocalIterator()
        ]
        conditions_matrix.append(new_conditions)
    conditions = cast(Iterable[str], zip(*conditions_matrix))
    conditions = [" AND ".join(x) for x in conditions]
    return " OR ".join(conditions)


def subset_source_dataframe(
    subset_population_df: DataFrame,
    source_df: DataFrame,
) -> DataFrame:
    """
    Attempts to subset a source table dataframe by
    any overlapping columns in the subset_population_df and source
    dataframe

    args:
        subset_population_df: dataframe with column matching source_df
            on one or more column
        source_df: a source dataframe
    """
    subset_cols = set(x.lower() for x in subset_population_df.columns)
    source_df_cols = set(x.lower() for x in source_df.columns)

    join_cols = list(subset_cols & source_df_cols)

    if len(join_cols) > 0:
        if subset_population_df.select(join_cols).distinct().count() < 1000:
            filter_conditions = get_subset_filter_str(
                subset_population_df, join_cols
            )
            source_df = source_df.where(filter_conditions).distinct()
            return source_df
        else:
            return (
                source_df.join(subset_population_df, on=join_cols)
                .select(source_df.columns)
                .distinct()
            )
    else:
        return source_df


def subset_source_dataframes(source_dataframes: Dict) -> Dict:
    """
    Subsets a dict of dataframes by a subset dataframe

    args:
        source_dataframes: dictionary of source table dataframes
    """
    subset_population_df = source_dataframes["subset_population_df"]
    new_dataframes = {}
    for df_name, df in source_dataframes.items():
        if df_name not in ("subset_population_df", "audit_df"):
            new_dataframes[df_name] = subset_source_dataframe(
                subset_population_df, df
            )
        else:
            new_dataframes[df_name] = df

    return new_dataframes


def get_source_table_dataframes(
    spark: SparkSession,
    settings: Settings,
) -> Dict[str, Optional[DataFrame]]:
    """
    Use specs from source_tables to create Spark
    DataFrames
    args:
        settings: Settings object containg pyspark-pipeline job settings
    """
    source_dataframes: Dict[str, Optional[DataFrame]] = {}
    for df_name, source_table in settings.source_tables.items():
        # audit_df's are written at the end of the job so
        # they will not exist for the first run
        if df_name == "audit_df":
            try:
                message = "audit_df must be either json or parquet"
                assert source_table.table_type != "csv", message
                df: Optional[DataFrame] = get_source_table_dataframe(
                    spark=spark,
                    source_table=source_table,
                    settings=settings,
                )
            except AnalysisException:
                df = None
        else:
            df = get_source_table_dataframe(
                spark=spark,
                source_table=source_table,
                settings=settings,
            )

        source_dataframes[df_name] = df

    if "subset_population_df" in source_dataframes:
        source_dataframes = subset_source_dataframes(source_dataframes)

    return source_dataframes


def job_driver(
    app_name: str,
    arg_parser: Callable,
    job: Callable,
    settings_class: Callable = Settings,
) -> None:
    """
    A function for setting up the Spark environment
    and executing a Job object's run() method

        args:
            app_name: app_name that will be displayed in Spark UI
            arg_parser: the pyspark_pipeline arg_parse_utils ArgumentParser
                containing specific args for the job that will
                be executed
            job: the pyspark_pipeline.jobs.Job object which is a sequence of
                pyspark_pipeline queries that will be executed by the run()
                method
    """
    # setting up session
    start_time = datetime.now()
    logger = get_logger(app_name, start_time)
    logger.info("Start_time: %s " % start_time)

    args = arg_parser()
    settings_json = get_settings_json(args.settings_yaml)
    settings = get_settings(args, settings_json, settings_class)
    spark_conf = get_spark_conf(settings.spark_configs)
    spark = get_spark_session(
        app_name=app_name,
        spark_conf=spark_conf,
        settings=settings,
        rebuild_conda_env=args.rebuild_conda_env,
        zipped_environment_path=args.zipped_environment_path,
    )
    set_spark_log_level(spark, args.spark_log_level)

    # source_table keys must match the arguments for
    # the job being excecuted.
    source_table_dataframes = get_source_table_dataframes(
        spark=spark, settings=settings
    )

    # this is where the job is executed
    try:
        job(
            spark=spark,
            args=args,
            settings=settings,
            logger=logger,
            **source_table_dataframes,
        ).run()
    except TypeError as e:
        message = (
            "\n Make sure these tables are included in 'source_tables' "
            "in your settings file under the correct keys!"
        )
        raise TypeError(str(e) + message)

    # end of logs
    end_time = datetime.now()
    logger.info(
        "total run time (m): %s "
        % round(((end_time - start_time).total_seconds() / 60), 3)
    )
    spark.stop()
    logger.info("Success")
