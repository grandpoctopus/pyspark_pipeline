# spark-query

spark-query is Python package for Spark-based ETL pipelines.

# Overview:
spark-query is designed to allow users to write easily testable and modular ETL pipelines. Spark-query allows for flexible configuration through a settings yaml to use tables/dataframes from different sources (local, hadoop, snowflake, and AWS etc) without changing query code. It also allows for unit testing at the job, query, and subquery levels.

## Extracting source tables as DataFrames with job_driver
Settings yaml files, see `pyspark_pipeline/settings/` for examples, define the locations and formats
of the source tables. Source tables can be in either Hive, SnowFlake or an object store like
S3 or hdfs. S3 or hdfs tables can be in either CSV, json, or parquet formats.

```
example:
settings/sample.yaml
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

A `Job`'s `run()` method is a series of `Query` classes, `spark-query.queries`.
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

## Generating Pipeline Specs

The pachyderm pipeline specs are defined as templates in:
```yaml
pipeline_templates/jsonnet
```
In order to transform the templates into actual pipeline specs you can use `pi-pac-man`.
From a poetry shell with `pi-pac-man` installed (for instance, running directly from pi-pac-man itself)
This command from the top of the spark-query directory generates the specs ..
```shell
python -m pi_pac_man.deploy_project --manifest=settings/deployments/full.yaml --local-specs-for-all save_specs
```
the specs will be saved in the sub-directory `deployed_pipeline_specs`.


# Running Tests
Tests for queries and jobs can be run locally with a spark installation, but running integration tests and some utility tests requires the Discovery2 environment described above.

1. run `kinit` and enter your password
2. run  ```poetry run tox -e clean,py37,report```

Note: `pytest` can also be used to run specific tests or test modules.

# ETL Jobs

## events

The events job uses claim data to create the following tables:
- `diagnosis_events_<table_tag>_<lob_short>_<suffix>`
- `emr_events_<table_tag>_<lob_short>_<suffix>`
- `lab_events_<table_tag>_<lob_short>_<suffix>`
- `prescription_events_<table_tag>_<lob_short>_<suffix>`
- `procedure_events_<table_tag>_<lob_short>_<suffix>`

All of the tables above are related to a particular member by
the member's `id` (Master Consumer Identifier) column.

### how to run the events job on discovery-2:
1. Log into a discovery-2 edge node (Olympus instructions below)
2. Clone this repository
3. From the pyspark_pipeline directory, install this package with `poetry install`.
If you have not yet installed poetry see instructions below in
`Development Worfklow`
4. Edit `settings/events.yaml` to include your username as the
5. Update the eligibility start and end dates
6. Choose an appropriate suffix (usually this is eligibility start
and end dates without `-` separated by a `_` e.g. `20160408_20210408`)
7. run: ```kinit```
8. To run in client-mode: ```run_events -s settings/events.yaml```
9. To run in cluster-mode: ```scripts/cluster_submit.sh -f src/pyspark_pipeline/events.py -s settings/events.yaml```
(see Note 2 below)

If you want to run an integration test to make sure everything is working:
```run_events -s settings/events.yaml -r t```

The most recent log file starting with 'events_<time of executions>'
in `logs/pyspark` will contain the locations of all of the tables
created from the run.

NOTE 1: by default the `job_driver` will package the conda environment every time
it is run and this very slow. If you want to skip this step and use a pre-existing
packaged conda env you can use add the argument `-e F` to either of the above run arguments.

NOTE 2: Credentials for the package repository (currently Artifactory) to run `scripts/cluster_submit.sh`.  The
credentials can be created as environment variables:
```shell
export PACKAGE_REPO_URL="artifacts.anthemai.io/artifactory/api/pypi/pypi-private"
export PACKAGE_REPO_USER="{your artifactory username}"
export PACKAGE_REPO_PASS"{your artifactory api key}"
```
if running from discovery 2 to use artifactory you need to also run:
```
export REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt
```


### how to run the events job on Olympus:
Olympus has some default spark defaults
that override user defined SPARK_HOME
and interfere with spark-query. To get around these
defaults you need to run spark-query in a docker image. The image is availabe in the pi-images
repo.

You can use an existing spark-query image in the olympus registry but you should pull from master to refresh any changes or mount in your local change.
1. `kinit`
2. `klist`
3. look for the ticket cache file in the output of klist. The numbers at the end of the file name are your KRBUSERID.

`export KRBUSERID=<yours>`
4.
```
docker run --rm -ti -e KRB5CCNAME=FILE:/tmp/krb5cc_$KRBUSERID -e KRBUSERID=$KRBUSERID -v /opt/cloudera/:/opt/cloudera/ -v /etc/hadoop/:/etc/hadoop -v /tmp/krb5cc_$KRBUSERID:/tmp/krb5cc_$KRBUSERID --net=host 627080838747.dkr.ecr.us-west-2.amazonaws.com/pip/spark-query:latest bash || true
```
5. `cd /etc/spark-query/ `
6. Pull the latest master or your branch inside the image before running your job.
7. ```scripts/cluster_submit.sh -f src/pyspark_pipeline/events.py -s settings/events.yaml```

If you need to rebuild the image from scratch:

1. `git clone https://gitlab.com/carelon/pip/pi-images.git`
2. obtain a gitlab login token https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html
3. `export GITLAB_TOKEN=<your token>`
4. `cd pi-images`
5. `python src/pi_images/builder/build_image.py -s src/pi_images/images/pyspark_pipeline/builder/build_specs.yaml`

### incremental updates of events job
The events jobs are setup for incremental processing. Incremental processing can be enabled using
the following aruguments:
* `-I update`: perform an upsert of the existing tables with changes since the last run
* `-I changes_only`: replace the existing tables with new changes since the last run

If incremental processing is enabled the job will filter rows that have a value of `storage_timestamp` column in the events tables that are not between the values of `incremental_load_start_date` and `incremental_load_end_date`

For an initial run the `incremental_load_start_date` will be set to `1970-01-01` and the `incremental_load_end_date` is set to midnight the day before the run. So only rows  entered on the day of the run should be filtered out in the inital run. These will be retrieved in the next incremental run.

For incremental runs, unless explicitly set by command line arguments, the values of `incremental_load_end_date` and `incremental_load_start_date` will be set based on the entries in the `audit_table` for the previous run with the same `dataset_suffix` and `table_tag`.

* `incremental_load_start_date`: is set to the previous run's `incremental_load_end_date` of the previous run (This 1 day before ensures that timezone differences aren't missed)
* `incremental_load_end_date`: is set to midnight the day before the run

**Note**
the `incremental_load_end_date` is set to midnight the day before the current run so that any rows entered into the table after the run but on the same day will be caught in the next incremental run.

**Note**
the `storage_timestamp` column for most events tables is derived from the column specified by the `sor_column_name` setting).

**Note**
`incremental_load_start_date` and `incremental_load_start_date` can be set with command line arguments, but it is recommended that these arguments only be used for testing or backfilling.

**Note**
To have a continually running incremental update it is recommended to use 8888-12-31 as
the include_end_date so that new members are included in the incremental runs

## sample

The sample job uses membership and demographics data
to create the following tables:
- `sample_<table_tag>_<lob_short>_<suffix>` (demographics data)

The most recent log file starting with 'sample_<time of executions>'
in `logs/pyspark` will contain the locations of all of the tables
created from the run.

### how to run:

1. Follow steps 1 - 7 in the "how to run events" section
2. client-mode run: ```run_sample -s settings/sample.yaml```
   cluster-mode run: ```scripts/cluster_submit.sh -f src/pyspark_pipeline/sample.py -s settings/sample.yaml```

If you want to run an integration test to make sure everything is working:
```run_sample -s settings/sample.yaml -r t```


# Creating Subsets
spark-query has several subsetting tools for creating subsets of events, sample, and member_formulary tables. Note, these subsets will
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
Use this tool if you want to get a subset of the tables for every member that has a particular diagnosis code or is prescribed a particular drug or has any other particular value in a particular
column.

```run_subset_by_table_column_value -s settings/subset_tables.yaml -d <dataframe name> -c <column name> -p <pattern that will be matched in the column> -N <subset name>```

## Copying subset data from hdfs to local directory
```python scripts/move_data_to_pac.py -N <subset_name>  -o <output directory> -s <path to settings file used in the original job>```

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
instance if your query is in `src/pyspark_pipeline/queries/events_queries.py` add
your tests to `tests/test_queries/test_events_queries.py` and any input
test dataframes should be added to `tests/test_event_fixtures.py`.


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
under the `source_tables` key. See `settings/events.yaml` as an example.

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
See `src/pyspark_pipeline/events.py` for an example.

Add tests for the job in `tests/test_jobs`. You should also make a new
fixture file similar to `tests/test_event_fixtures.py`. The same dataframes
defined in this fixtures file should be used in both the tests for the job's queries and the tests for the job. See `tests/test_queries/test_events_queries.py` and `tests/test_jobs/test_jobs.py` for an exmaple.


## Settings and Override Settings:

In some cases it might be useful to apply some settings to all jobs in an environment (for example settings that are common to all jobs in an environment) or use limited sark resources for subset runs.

In these cases you can use an additional settings file, -O, in your cluster_submit arguments. Any key passed in the override_settings file will replace a key in the settings_file EXCEPT source_tables which will be merged.

example:
```
scripts/cluster_submit.sh -m kubernetes -s /users/you/spark-query/settings/pachyderm_jobs/sample.yaml -O /users/you/spark-query/settings/pachyderm_jobs/preprod_override_settings.yaml -f /users/you/spark-query/src/pyspark_pipeline/sample.py
```

## Recommendations

- The Job object's __init__() method should have arguments for all of the input
  dataframes to facilitate easier testing
- The Job object should return a dict of dataframes that were produced by the job
  to facilitate testing the transformations performed by the job
