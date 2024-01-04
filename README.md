# pyspark-pipeline

pyspark-pipeline is Python package for Pypark-based ETL pipelines.

# Overview:
pyspark-pipeline is designed to allow users to write easily testable and modular ETL pipelines. pyspark-pipeline allows for flexible configuration through a settings yaml to use tables/dataframes from different sources (local, hadoop, snowflake, and AWS etc) without changing query code. It also allows for unit testing at the job, query, and subquery levels.

## Extracting source tables as DataFrames with job_driver
Settings yaml files, see `pyspark_pipeline/settings/sample_settings.yaml` for examples, define the locations and formats
of the source tables. Source tables can be in either Hive, SnowFlake or an object store like
S3 or hdfs. S3 or hdfs tables can be in either CSV, json, or parquet formats.

```
example:
settings/sample_settings.yaml
```

The source tables specified in the settings files are read into pyspark DataFrames by
the job_driver, `pyspark_pipeline.utilities.job_utils.job_driver`.

```
example:
src/pyspark_pipeline/sample.py
```

## Transforming source DataFrames with Jobs and Query
The job_driver passes in all of the DataFrames created from the source tables into a
`Job` class, `pyspark_pipeline.jobs` and executes its `run()` method.

```
example:
src/pyspark_pipeline/jobs/sample_job.py
```

A `Job`'s `run()` method is a series of `Query` classes, `pyspark-pipeline.queries`.
The `Query` classes' `run()` methods performs transformations on the
`Job`'s DataFrames and returns a DataFrame.

```
example:
src/pyspark_pipeline/queries/sample_queries.py
```

The `Job`'s `run()` method returns a dict (`['df_name': DataFrame]`) of the DataFrames created in the `Job`.

## Loading results to object stores and/or Hive
The `Query`'s `run()` method returns a DataFrame (useful for intermediate transformations that do
not need to be persisted) and its `write()` method returns a DataFrame and writes the DataFrame as a subdirectory
in the location specified in the `Job`'s settings file's `target_path`.

The `Query`'s `write()` method also allows users to create either external or managed Hive tables from the
Query's results. These tables will be created in the `target_db` specified in `Job`'s settings file.

```
example:
src/pyspark_pipeline/jobs/sample_jobs.py where sample_df is created
```

## Testing
Since the input to Query classes and Job classes are Pyspark DataFrames unit testing these classes
is possible using DataFrames created with pyspark.sql.SparkSession.createDataFrame and so testing of
jobs and queries can be performed quickly using local spark.

```
examples:
tests/test_sample_fixtures.py
tests/test_queries/test_sample_queries.py
tests/test_jobs/test_sample_jobs.py
```

# Running Tests
```poetry run tox clean,py38,report```

Note: `pytest` can also be used to run specific tests or test modules.


### incremental updates of job
Jobs can be setup for incremental processing. Incremental processing can be enabled using
the following aruguments:
* `-I update`: perform an upsert of the existing tables with changes since the last run
* `-I changes_only`: replace the existing tables with new changes since the last run

If incremental processing is enabled the job will filter rows that have a value of `storage_timestamp` column in the results tables that are not between the values of `incremental_load_start_date` and `incremental_load_end_date`

For an initial run the `incremental_load_start_date` will be set to `1970-01-01` and the `incremental_load_end_date` is set to midnight the day before the run. So only rows  entered on the day of the run should be filtered out in the inital run. These will be retrieved in the next incremental run.

For incremental runs, unless explicitly set by command line arguments, the values of `incremental_load_end_date` and `incremental_load_start_date` will be set based on the entries in the `audit_table` for the previous run with the same `dataset_suffix` and `table_tag`.

* `incremental_load_start_date`: is set to the previous run's `incremental_load_end_date` of the previous run (This 1 day before ensures that timezone differences aren't missed)
* `incremental_load_end_date`: is set to midnight the day before the run

**Note**
the `incremental_load_end_date` is set to midnight the day before the current run so that any rows entered into the table after the run but on the same day will be caught in the next incremental run.

**Note**
the `storage_timestamp` column for most results tables is derived from the column specified by the `sor_column_name` setting).

**Note**
`incremental_load_start_date` and `incremental_load_start_date` can be set with command line arguments, but it is recommended that these arguments only be used for testing or backfilling.

**Note**
To have a continually running incremental update it is recommended to use 8888-12-31 as
the include_end_date so that new members are included in the incremental runs


# Creating Subsets
pyspark-pipeline has several subsetting tools for creating subsets of result. Note, these subsets will
be created from tables with specific `table_tag` and `suffix` specified in the `subset_tables.yaml` settings file.

## subset by list of ids.
### CSV
1. create a csv that has an `id` column where each row contains a single id.
2. run: ```run_subset_by_id_list -s settings/subset_tables.yaml -c <path/to/your/id.csv> -N <name of your subset>```

### Hive Table
# TODO: figure out -N option
1. run:
```sh
run_subset_by_id_hive \
  --settings-yaml settings/subset_tables_by_hive.yaml \
  --subset-name <name of your subset> \
  --table-name <Hive table name with ids column>
```

## subset by number of ids.
1. create a csv that has an `id` column where each row contains a single id.
2. run: ```run_subset_by_number_of_ids -s settings/subset_tables.yaml -n <number of ids>```

optional arguments:
`-x path to csv (same structure as described above) of ids to exclude`
`-i path to csv (same structure as described above)of ids to include`

## subset by a particular column value in a particular table
Use this tool if you want to get a subset of the tables for every id that has a particular value in a particular column.

```run_subset_by_table_column_value -s settings/subset_tables.yaml -d <dataframe name> -c <column name> -p <pattern that will be matched in the column> -N <subset name>```


# Writing New Queries

All queries in this package inherit from the Query class (src/pyspark_pipeline/queries)
which has a few required arguments
- `spark`: a SparkSession
- `settings`: a Settings object that is created from settings.yaml
- `schema`: a Schema object that is inherits from the Schema class

First create a Schema class for your query. Schema objects are Pydantic
models that have column names as attributes and strings for their
Hive data types as their values. These are used for writing the Spark
dataframes created by the query to Hive.

All of the logic for executing the query should be written in the Query
object's `run()` method and this `run()` should return a Spark DataFrame

Add tests for the query in the test file corresponding to your query. For
instance if your query is in `src/pyspark_pipeline/queries/sample_queries.py` add
your tests to `tests/test_queries/test_sample_queries.py` and any input
test dataframes should be added to `tests/test_sample_fixtures.py`.


## Recommendations

Prior to returning the data frame you should include a select statement
like the following to ensure that your DataFrame has the correct columns
```.select(self.schema.get_columns_list())```

It is recommended that any spark DataFrames used within the `run()` method
be passed in as arguments to the Query's `__init__` method to facilitate
testing.

# Writing New Jobs

Jobs are collections of querys and other transformations. Jobs inherit
from the Jobs class (src/pyspark_pipeline/jobs) which has a few required arguments
- `spark`: a SparkSession
- `settings`: a Settings object that is created from settings.yaml
- `args`: an argument parser with specific arguments for the job

Jobs will take dataframes as arguments and return a dict with output
dataframe name as keys and output dataframes as values.

Input tables should be defined in the settings file for the new job
under the `source_tables` key. See `settings/sample_settings.yaml` as an example.

Each source_table listed in `source_tables` should have the same
name as the dataframe argument to the job.
For instance:
if
```
YourJob(
    your_input_dataframe: Dataframe,
    your_other_dataframe: Dataframe
  )
```
then your settings yaml should have the following:
```
source_tables:
  your_input_dataframe:
    table_type: 'hive'
    location: '{database_name}.input_df'
  your_other_dataframe:
    table_type: 'parquet'
    location: 's3://your_other_dataframe.parquet'
```

If your job will take unique command line arguments you can create
a new argument parser using the utils in `pyspark_pipeline.utilities.arg_parse_utils`


If you do not need to write a DataFrame to Hive or an object store you can
use the Query's `.run()` method.

If you do need to write your DataFrame you should use the Query's `write()`
method. This function will execute the Query's `run()` method to obtain a
DataFrame that will be written according to which option you choose for
`write_location`

`write_location` options:
- `hive` (Writes to Hive as a managed table, will overwrite an existing table)
- `csv` (Writes to hdfs or s3 as a CSV and creates an Hive external table from
  this CSV, will overwrite an exiting directory/table)
- `json` (Writes to hdfs or s3 as a JSON and creates an Hive external table from
  this JSON, will overwrite an exiting directory/table)
- `parquet` (Writes to hdfs or s3 as a parquet and creates an Hive external table from
  this parquet, will overwrite an exiting directory/table)
- `append` (appends dataframe to an existing Hive managed table)

The name of the Hive table/ HDFS path will be determined by some of your
settings in the settings.yaml and the option that you choose for `job_name`

Jobs are run with the `pyspark_pipeline.utilities.job_utils.job_driver()` function.
See `src/pyspark_pipeline/sample.py` for an example.

Add tests for the job in `tests/test_jobs`. You should also make a new
fixture file similar to `tests/test_sample_queries_fixtures.py`. The same dataframes
defined in this fixtures file should be used in both the tests for the job's queries and the tests for the job. See `tests/test_queries/test_sample_queries.py` and `tests/test_jobs/test_sample_job.py` for an exmaple.


## Settings and Override Settings:

In some cases it might be useful to apply some settings to all jobs in an environment (for example settings that are common to all jobs in an environment) or use limited sark resources for subset runs.

In these cases you can use an additional settings file, -O, in your cluster_submit arguments. Any key passed in the override_settings file will replace a key in the settings_file EXCEPT source_tables which will be merged.

example:
```
scripts/cluster_submit.sh -m kubernetes -s /users/you/pyspark-pipeline/settings/sample_settings.yaml -O /users/you/pyspark-pipeline/settings/override_settings.yaml -f /users/you/pyspark-pipeline/src/pyspark_pipeline/sample.py
```

## Recommendations

- The Job object's __init__() method should have arguments for all of the input
  dataframes to facilitate easier testing
- The Job object should return a dict of dataframes that were produced by the job
  to facilitate testing the transformations performed by the job
