import re
import string
import unicodedata
from datetime import date, datetime
from typing import Dict, List, Optional

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

from pyspark_pipeline import schemas
from pyspark_pipeline.utilities.settings_utils import Settings

NUMERIC_REGEX = re.compile(r"[^\d.]+")
FLOAT_REGEX = re.compile(r"(\d*\.\d+|\d+)")


def is_not_null_unk_or_na(column: Column):
    """checks whether a column value is Null, 'UNK' or 'NA'"""
    return column.isNotNull() & (~column.isin(["UNK", "NA"]))


def get_yearmonth_number(a_date: date) -> int:
    """
    Convert a date to a concatenated yearmonth integer
    example: 2020-01-02 -> 202001
    args:
        a_date: a datetime.date object
    """
    return int(a_date.strftime("%Y%m"))


def rename_columns(df: DataFrame, name_map: Dict) -> DataFrame:
    """
    Renames columns in a dataframe based on a dict
    of old -> new names (old_names = keys, new_names = values)

    args:
        df: a spark DataFrame
        name_map: a dict with column names in input df as keys and
            names in the output df as values
    """
    for old_name, new_name in name_map.items():
        try:
            df = df.withColumn(new_name, F.col(old_name)).drop(old_name)
        except AnalysisException:
            pass

    return df


def null_columns_for_missing_columns(
    df: DataFrame, target_schema: List[str]
) -> DataFrame:
    """
    adds columns of Null values for columns that
    are missing from the target_schema in df's schema

    args:
        df: a spark DataFrame
        target_schema: a list of columns that will be
            in the output dataframe
    """
    missing_columns = set(target_schema) - set(df.columns)

    for column in missing_columns:
        df = df.withColumn(column, F.lit(None))

    return df.select(target_schema)


def reformat_dataframe(
    df: DataFrame, name_map: Dict, target_schema: List[str]
) -> DataFrame:
    """
    Renames columns based on name_map, and adds
    null columns if any are missing from target_schema

     args:
        df: a spark DataFrame
        name_map: a dict with column names in input df as keys and
            names in the output df as values
        target_schema: a list of columns that will be
            in the output dataframe

    """

    df = rename_columns(df, name_map)

    df = null_columns_for_missing_columns(df, target_schema)

    return df.select(target_schema)


def clear_accents(df: DataFrame, column_name: str) -> DataFrame:
    """
    This function deletes accents in strings column dataFrames, it does
        not eliminate main characters, but only deletes special tildes.
    Note: this a modified version of
    https://gitlab.com/carelon/axel.bernal/icd-utils/-/blob/master/datastack/pyspark/src/shared/cdataframe.py#L125

        args:
            df: an input dataframe
            column_name: the column to clear accents from
    """

    # Receives  a string as an argument
    def remove_accents(input_str):
        # first, normalize strings:
        nfkd_str = unicodedata.normalize("NFKD", input_str)
        # Keep chars that has no other char combined (i.e. accents chars)
        with_out_accents = "".join(
            [ch for ch in nfkd_str if not unicodedata.combining(ch)]
        )
        return with_out_accents

    # User define function that does operation in cells
    function = F.udf(
        lambda cell: remove_accents(cell) if cell is not None else cell,
        StringType(),
    )

    return df.withColumn(column_name, function(column_name))


def remove_special_chars(df: DataFrame, column_name: str) -> DataFrame:
    """
    This function remove special chars in string columns, such as:
    .!"#$%&/()
       Args:
           df: an input dataframe
           column_name:   String or a list of columns names
    """

    # Receives a string as an argument
    def rm_spec_chars(input_str):
        # Remove all punctuation and control characters
        for punct in set(input_str) & set(string.punctuation):
            input_str = input_str.replace(punct, "")
        return input_str

    # User define function that does operation in cells
    function = F.udf(
        lambda cell: rm_spec_chars(cell) if cell is not None else cell,
        StringType(),
    )

    return df.withColumn(column_name, function(column_name))


def parse_date_cols(df: DataFrame, col_names: List[str]):
    for c in col_names:
        df = df.withColumn(c, F.to_date(F.col(c), "yyyyMMdd"))
    return df


def trim_leading_zeros(col: Column):
    return F.when(col.rlike("^0+$"), "0").otherwise(
        F.regexp_replace(col, "^0*", "")
    )


def pandas_string_cols_to_float(
    df: pd.DataFrame,
    str_to_float_columns: List,
) -> pd.DataFrame:
    """Removes chars from string columns that will make them unconvertable
    to numeric type and converts to float

    args:
        df: Pandas DataFrame
        str_to_float_columns: columns that will have nonumeric or '.' chars removed and
            converted to float
    """
    for col in str_to_float_columns:
        # remove chars that are not numeric or "."
        df[col] = df[col].str.replace(NUMERIC_REGEX, "", regex=True)
        # select chars that are numeric or "." if empty string return NaN
        df[col] = df[col].str.extract(FLOAT_REGEX, expand=False)
        df[col] = df[col].astype("float64")

    return df


def conditionally_repartition(df: DataFrame, num_partitions: int) -> DataFrame:
    """
    Checks whether a dataframe is already has the number of partitions
    set by settings.output_partitions. If it doesn not then it is
    repartitioned

    args:
        df: a spark DataFrame
        settings: a settings object
    """
    if df.rdd.getNumPartitions() == num_partitions:
        return df
    else:
        return df.repartition(num_partitions)


def get_num_output_partitions(settings: Settings) -> int:
    """
    Determines number of output partitions from
    settings.output_partitions if set, otherwise
    uses the number of shuffle partitions

    args:
        settings: the setting object
    """

    if settings.output_partitions is None:
        return settings.spark_configs.get("spark.sql.shuffle.partitions", 1000)
    else:
        return settings.output_partitions


def needs_repartition(
    df: DataFrame,
    col_to_repartition_by: Optional[str],
    num_output_partitions: int,
) -> bool:
    """
    Checks whether the dataframe is already has the
    number of output partitions specified in the settings
    if it does returns False, if not it returns True

    args:
        df: input spark dataframe
        col_to_repartition_by: option to have the column repartitioned
        num_output_partitions: determined from settings
    """
    return (
        col_to_repartition_by is not None
        or df.rdd.getNumPartitions() != num_output_partitions
    )


def repartition_by_column(
    df: DataFrame, col_name: str, num_output_partition: int
) -> DataFrame:
    """
    Repartitions a dataframe by a specified column into the specified
    number of output partitions

    args:
        df: DataFrame
        col_name: str
        num_output_partitions: specified by settings
    """
    return df.repartition(num_output_partition, col_name)


def repartition_randomly(
    df: DataFrame, num_output_partitions: int
) -> DataFrame:
    """
    Repartitions a dataframe randomly into the specified number of partitions

    args:
        df: DataFrame
        num_output_partitions: specified by settings
    """
    return df.repartition(num_output_partitions)


def manage_output_partitions(
    df: DataFrame,
    do_repartition: bool,
    col_to_repartition_by: Optional[str],
    settings: Settings,
) -> DataFrame:
    """
    Repartitions a dataframe based on user settings. If dataframe
    is already appropriately partitioned it will just return the
    existing dataframe.

    args:
        df: a spark DataFrame
        do_repartition: whether to repartition the data at all or leave it
            as is in the input data frame
        col_to_repartition_by: if specified then dataframe will be repartitioned
            by specified column and output partition number in settings
        settings: a settings object
    """
    if do_repartition is False:
        return df

    num_partitions = get_num_output_partitions(settings)

    if needs_repartition(df, col_to_repartition_by, num_partitions):
        if col_to_repartition_by is not None:
            return repartition_by_column(
                df, col_to_repartition_by, num_partitions
            )

        else:
            return repartition_randomly(df, num_partitions)
    else:
        return df


def str_cols_to_ts_cols(df: DataFrame, cols: List[str]):
    """
    Converts strings to timestamps.
    args:
        df: a Spark DataFrame
        cols: a list of columns to convert to timestamp columns
    """
    for col in cols:
        df = df.withColumn(
            col, F.to_timestamp(F.col(col), format="yyyy-MM-dd'T'HH:mm:ssZ")
        )

    return df


def validate_code(
    df: DataFrame,
    code_column: str,
    regex: str,
    output_column: str,
    replacement_value: Optional[str] = None,
    replace_nulls: bool = False,
) -> DataFrame:
    """
    Uses a regex pattern to validate that string in code_column is
    a valid code and creates a new column (output_column) with the
    valid code or the replacement value for an invalid code.
    The first matching group will be returned.

    args:
        df: the input dataframe must contain the column specified in code_column
        code_column: the name of the column containing the code to validate
        regex: the regex pattern used to validate the code column
        output_column: the name of the column for output
        replacement_value: An optional value to replace the value in the column with
        if the regex doesn't match.
        replace_nulls: A boolean for whether or not the replce originally null values
        with the new replacement_value.
    """
    return (
        df.withColumn("pre_output", F.regexp_extract(code_column, regex, 0))
        .withColumn(
            output_column,
            F.when(
                (F.col("pre_output") == "")
                | (F.col(code_column).isNull() & F.lit(replace_nulls)),
                F.lit(replacement_value),
            ).otherwise(F.col("pre_output")),
        )
        .drop("pre_output")
    )


def add_provenance_column(
    df: DataFrame,
    database_name: str,
    table_name: str,
    vendor_column: Optional[str],
    aggregator_column: Optional[str],
) -> DataFrame:
    """
    Builds a JSON column containing informatino about the provenance of an event.

    args:
        df: the input dataframe
        database_name: the name of the database that the event information is taken from
        table_name: the name of the table that the event information is taken from
        vendor_column: the name of the column containing information about the vendor
            (not available from all source tables)
        aggregator_column: the name of the column containing information about the
            aggregator of the information in the event for instance an HIE or Athena.
            (not available from all source tables)
    """
    df = df.withColumn("database", F.lit(database_name))
    df = df.withColumn("table_name", F.lit(table_name))

    if vendor_column is None:
        df = df.withColumn("vendor", F.lit(None).cast("string"))
    else:
        df = df.withColumn("vendor", F.col(vendor_column))

    if aggregator_column is None:
        df = df.withColumn("aggregator", F.lit(None).cast("string"))
    else:
        df = df.withColumn("aggregator", F.col(aggregator_column))

    return (
        df.withColumn(
            "provenance",
            F.struct(
                F.col("database"),
                F.col("table_name"),
                F.col("vendor"),
                F.col("aggregator"),
            ),
        )
        .drop(F.col("database"))
        .drop(F.col("table_name"))
        .drop(F.col("vendor"))
        .drop(F.col("aggregator"))
    )


def get_group_by_cols(
    schema: schemas.HiveSchema, columns_to_exclude: List[str] = []
):
    """
    returns a list of columns that do not have the "_list" suffix.
    These will be used to group by

    args:
        schema: a HiveSchema object
    """
    cols = schema.get_columns_list()
    return [
        col
        for col in cols
        if not col.endswith("_list") and col not in columns_to_exclude
    ]


def get_list_cols(
    schema: schemas.HiveSchema, columns_to_exclude: List[str] = []
):
    """
    returns a list of columns that have the "_list" suffix.
    These will be used to group by

    args:
        schema: a HiveSchema object
    """
    cols = schema.get_columns_list()
    return [
        col.replace("_list", "")
        for col in cols
        if col.endswith("_list") and col not in columns_to_exclude
    ]


def aggregate_rows_by_collect_list(
    df: DataFrame,
    group_by_cols: List[str],
    columns_to_collect_into_list: List[str],
    keep_nulls_by_filling_value: bool = False,
    null_fill_value: str = "UNK",
) -> DataFrame:
    """
    Groups by all columns except the column_to_collect_into_list and
    collects values in column_to_collect_into_list into an ordered list
    with the new name `<column_to_collect_into_list>s`

    args:
        DataFrame: a spark DataFrame
        group_by_cols: columns that will be used to group by
        column_to_collect_into_list: values of this column will be collected
            into a list in the output DataFrame
        keep_nulls_by_filling_value: collect_list will drop any nulls from lists
            this option allows users to keep nulls by replacing the value with
            the value specified in null_fill_value
        null_fill_value: the value used to replace nulls
    """
    if keep_nulls_by_filling_value is True:
        df = df.fillna(
            {col: null_fill_value for col in columns_to_collect_into_list}
        )

    collect_lists = (
        F.collect_list(col) for col in columns_to_collect_into_list
    )
    df = df.groupBy(group_by_cols).agg(*collect_lists)

    for col_name in columns_to_collect_into_list:
        df = df.withColumnRenamed(
            f"collect_list({col_name})", f"{col_name}_list"
        )

    return df


def get_most_common_value_for_column(
    df: DataFrame,
    column_name: str,
    partition_cols: Optional[List[str]] = None,
    tie_breaker_col: Optional[str] = None,
    tie_breaker_how: Optional[str] = "desc",
) -> DataFrame:
    """
    Returns a dataframe with the most common value for
    a specified column. If two values have the same occurence
    then the partition cols are used.

    args:
        df: a spark DataFrame
        column_name: the column that will be replaced with
            the most common value for that column per partition
            columns
        partition_cols: the partition columns that will be used
            in the window spec
        tie_breaker_cols: column that will be used
            as tie breakers in the event that two most common
            values have the same occurence
        tie_breaker_how: must be either None, desc or asc
    """
    tie_breaker_msg = "'tie_breaker_how' must be either None, 'desc' or 'asc'"
    assert tie_breaker_how in ("desc", "asc"), tie_breaker_msg

    if partition_cols is None:
        partition_cols = []

    df = df.withColumn(
        "_value_counts",
        F.count(column_name).over(
            Window.partitionBy(column_name, *partition_cols)
        ),
    )
    order_bys = [F.desc("_value_counts")]

    if tie_breaker_col is not None and tie_breaker_how == "asc":
        order_bys.append(F.asc(tie_breaker_col))
    elif tie_breaker_col is not None and tie_breaker_how == "desc":
        order_bys.append(F.desc(tie_breaker_col))

    window_spec = Window.partitionBy(*partition_cols).orderBy(*order_bys)
    df = (
        df.withColumn("_row_number", F.row_number().over(window_spec))
        .where(F.col("_row_number") == 1)
        .drop("_row_number", "_value_counts")
    )
    return df


def get_newest_non_null_values(
    df: DataFrame, id_col: str, column_list: List[str], sor_column_name: str
) -> DataFrame:
    """
    For each column in column_list gets a dataframe containing
    just the most recent info for each column

    args:
    df: a spark DataFrame,
    id_col: the the primary key column used for joining temp dfs
    column_list: a list of columns to get the most recent info for
    sor_column_name: the column that will be used to determine when values
        were updated
    """
    # create copy of sor_column_name in case it is also in column_list
    df = df.withColumn(f"_{sor_column_name}", F.col(sor_column_name))

    for col in column_list:
        df = df.withColumn(
            col,
            F.when(
                F.col(col).isNotNull(), F.struct(f"_{sor_column_name}", col)
            ).otherwise(F.lit(None)),
        )

    df = df.groupBy(id_col).agg(
        *[F.collect_list(F.col(col)).alias(col) for col in column_list]
    )

    for col in column_list:
        df = df.withColumn(col, F.array_max(col)[col])

    return df


def format_ts_column_as_iso_string(
    df: DataFrame, ts_col_name: str, output_format: str = "csv"
) -> DataFrame:
    """
    Converts a timestamp or date column into an ISO formatted string

    args:
        df: Spark DataFrame
        ts_column_name: name of the column that will be converted
    """
    if output_format == "date":
        date_format = "yyyy-MM-dd"
    elif output_format == "fhir_datetime":
        date_format = "yyyy-MM-dd'T'HH:mm:ss+00:00"
    elif output_format == "json":
        date_format = "yyyy-MM-dd HH:mm:ss"
    elif output_format == "csv":
        date_format = "yyyy-MM-dd'T'HH:mm:ssZ"
    else:
        raise ValueError(
            f"{output_format} is not an appropriate option for "
            "output_format argument"
        )

    return df.withColumn(
        ts_col_name, F.date_format(F.col(ts_col_name), date_format)
    )


def get_rows_stored_in_date_range(
    df: DataFrame,
    storage_ts_col_name: str,
    start_ts: datetime,
    end_ts: datetime,
) -> DataFrame:
    """
    Gets dates within range of start and end timestamps. Converts all
    dates to  iso timestamp strings so that comparisons are clear.
    Spark will behave unexpectedly if datetime objects are used to assess
    timestamp strings.

    args:
        df: Spark DataFrame
        storage_ts_col_name: name of the column that will be converted
        start_ts: datetime object for beginning of date range
        end_ts: datetime object for end of date range
    """
    df = df.withColumn("storage_iso", F.col(storage_ts_col_name))
    df = format_ts_column_as_iso_string(df, "storage_iso")
    start_iso_str = str(start_ts.isoformat(sep="T"))
    end_iso_str = str(end_ts.isoformat(sep="T"))
    df = df.where(
        (
            (F.col("storage_iso") >= start_iso_str)
            & (F.col("storage_iso") < end_iso_str)
        )
    )
    return df.drop("storage_iso")


def get_id_group_col(
    df: DataFrame, id_col_name: str, num_groups: Optional[int]
) -> DataFrame:
    """
    Adds an id group col that is the modulus of the id
    by the number of output partitions and gets a id_group_agg column

    args:
        df: Spark Dataframe
        num_groups: Number of id_groups to make
    """
    if num_groups is None:
        df_id_groups = df.withColumn(f"{id_col_name}_group", F.lit("0"))
    else:
        df_id_groups = df.withColumn(
            "id_group",
            (F.col(id_col_name) % num_groups).cast("bigint"),
        )
    return df_id_groups


def has_invalid_value(col: str):
    return F.upper(F.col(col)).isin(["UNK", "NA", "~01", "~02", "~03"])


def has_invalid_pattern(col: str):
    return F.upper(F.col(col)).rlike(".*[^A-Z0-9].*")


def use_database_udf(
    spark: SparkSession,
    df: DataFrame,
    udf_name: str,
    col: str,
    new_col_name: str,
) -> DataFrame:
    """
    Executes a udf stored in an exeternal database using
    spark SQL.

    args:
        spark: spark session
        df: spark dataframe to execute the udf on
        udf_name: name of udf to execute on col
        col: col that udf will be executed on
        new_col_name: the alias of the new column resulting from
            transformation
    """
    df.registerTempTable("temp_table")
    cols = [col_name for col_name in df.columns if col_name != col]
    return spark.sql(
        f"SELECT {','.join(cols)}, "
        f"{udf_name}({col}) AS {new_col_name} "
        "FROM temp_table"
    )
