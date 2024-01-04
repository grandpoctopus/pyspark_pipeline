This module will automatically generate all of the boilerplate code for a new job from a user provided `project_settings.yaml` file (example included in this directory).

project_settings.yaml:
- project_name: space-separated name of the project e.g. "member infos"

- input_tables: all of the table names that will be read from a source database like snowflake. These should not include any precise location and just the table name. For instance if your input table is `SCHEMA.DATABASE.TABLE` the table name would just be `TABLE`
These names will all be included as arguments for the job class created by this script.

- output_dfs: these define all the dataframes that will be returned by the job. Each output df will also be used to template a Schema class, and Query class to create the dataframe and add them to Job in the order listed.
If `write` is specified then the job will call the query's write method otherwise it will just call the query's `run` method.

    The `input_dfs` will be setup as arguments for the query class.

    Additionally, tests for each query created by the script will be added to the tests/test_queries directory.

to run:

`python project_templating/render_project_files.py -s project_templating/project_settings.yaml -d True`

args:
- -s settings: path to project_settings.yaml
- -d dry-run: if true will write to a tmp directory, if not set then will write to the appropriate locations in the spark-query module and spark-query tests directory
