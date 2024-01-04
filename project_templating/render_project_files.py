import logging
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Dict, List, Optional, cast

import yaml
from jinja2 import Environment, FileSystemLoader, Template
from pydantic import BaseModel, validator

SETTINGS_ENVS = ["preprod", "prod"]

FILE_MAP = {
    "fixtures.jinja": "tests/test_{job_name}_fixtures.py",
    "job.jinja": "src/pyspark_pipeline/jobs/{job_name}_job.py",
    "job_test.jinja": "tests/test_jobs/test_{job_name}_job.py",
    "main.jinja": "src/pyspark_pipeline/{job_name}.py",
    "query_test.jinja": "tests/test_queries/test_{job_name}_queries.py",
    "query.jinja": "src/pyspark_pipeline/queries/{job_name}_queries.py",
    "schema.jinja": "src/pyspark_pipeline/schemas/{job_name}_schemas.py",
    "settings.jinja": "settings/pachyderm_jobs/pi_app_{settings_env}/{job_name}.yaml",
    "pachyderm_job.jinja": "pipeline_templates/{settings_env}/{job_name}.yaml",
    "pach_s3_copy.jinja": "pipeline_templates/{settings_env}/{job_name}_s3_copy.yaml",
}


class OutputDf(BaseModel):
    name: str
    write: bool
    input_dfs: List[str]
    query: Optional[str] = None
    schema_class: Optional[str] = None
    query_class: Optional[str] = None

    @validator("name", always=True)
    def validate_name(cls, name: str):
        if name.endswith("_df"):
            return name
        else:
            return f"{name}_df"

    @validator("input_dfs", always=True)
    def validate_input_dfs(cls, input_dfs: List[str]):
        parsed = []
        for input_df in input_dfs:
            input_df = input_df.lower()
            if input_df.endswith("_df"):
                parsed.append(input_df)
            else:
                parsed.append(f"{input_df}_df")
        return parsed

    @validator("schema_class", always=True)
    def validate_schema(cls, schema_class: Optional[str], values: Dict):
        if schema_class is not None:
            return schema_class
        else:
            base_name = values["name"].replace("_df", "")
            camel_case_name = "".join(
                [x.capitalize() for x in base_name.split(sep="_")]
            )
            return f"{camel_case_name}Schema"

    @validator("query_class", always=True)
    def validate_query(cls, query_class: Optional[str], values: Dict):
        if query_class is not None:
            return query_class
        else:
            base_name = values["name"].replace("_df", "")
            camel_case_name = "".join(
                [x.capitalize() for x in base_name.split(sep="_")]
            )
            return f"{camel_case_name}Query"


class ProjectSettings(BaseModel):
    project_name: str
    input_tables: List[str]
    output_dfs: List[OutputDf]
    input_dfs: Optional[List[str]]
    job_class: Optional[str]
    job_file_name: Optional[str]
    schema_file_name: Optional[str]
    query_file_name: Optional[str]
    file_map: Optional[Dict]

    @staticmethod
    def get_base_name(project_name: str) -> str:
        return "_".join([x.lower() for x in project_name.split(" ") if x != ""])

    @validator("input_dfs", always=True)
    def validate_input_dfs(cls, input_dfs: List[str], values: Dict):
        parsed = []
        for input_df in values["input_tables"]:
            input_df = input_df.lower()
            if input_df.endswith("_df"):
                parsed.append(input_df)
            else:
                parsed.append(f"{input_df}_df")
        return parsed

    @validator("job_class", always=True)
    def validate_job_class(cls, job_class: Optional[str], values: Dict):
        if job_class is not None:
            return job_class
        else:
            base_name = values["project_name"].replace(" ", "_")
            camel_case_name = "".join(
                [x.capitalize() for x in base_name.split(sep="_")]
            )
            return f"{camel_case_name}Job"

    @validator("job_file_name", always=True)
    def validate_jobe_file_name(
        cls, job_file_name: Optional[str], values: Dict
    ):
        if job_file_name is not None:
            return job_file_name
        else:
            base_name = cls.get_base_name(values["project_name"])

            return f"{base_name}_job"

    @validator("schema_file_name", always=True)
    def validate_schema_file_name(
        cls, schema_file_name: Optional[str], values: Dict
    ):
        if schema_file_name is not None:
            return schema_file_name
        else:
            base_name = cls.get_base_name(values["project_name"])
            return f"{base_name}_schemas"

    @validator("query_file_name", always=True)
    def validate_query_file_name(
        cls, query_file_name: Optional[str], values: Dict
    ):
        if query_file_name is not None:
            return query_file_name
        else:
            base_name = cls.get_base_name(values["project_name"])
            return f"{base_name}_queries"

    @validator("file_map", always=True)
    def validate_file_map(cls, file_map: Optional[Dict], values: Dict):
        if file_map is not None:
            return file_map
        else:
            job_name = cls.get_base_name(values["project_name"])
            return {
                k: v.format(job_name=job_name, settings_env="{settings_env}")
                for k, v in FILE_MAP.items()
            }


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        "-s",
        "--settings-yaml",
        metavar="s",
        action="store",
        type=str,
        help="project settings yaml",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        metavar="d",
        action="store",
        type=bool,
        help="produce the files in tmp directory",
        required=False,
        default=False,
    )

    return parser.parse_args()


def render_file(
    template: Template, settings: ProjectSettings, settings_env: str = ""
) -> str:
    """
    Renders a file from a jinja template using
    settings in the settings object

    args:
        template: jinja template
        settings: settings for the render job
    """
    input_dfs = cast(List[str], settings.input_dfs)
    table_df_map = zip(input_dfs, settings.input_tables)
    return template.render(
        job_file_name=settings.job_file_name,
        job_class=settings.job_class,
        input_dfs=settings.input_dfs,
        schema_file_name=settings.schema_file_name,
        query_file_name=settings.query_file_name,
        output_dfs=settings.output_dfs,
        table_df_map=table_df_map,
        settings_env=settings_env,
        project_name=settings.project_name,
    )


def write_rendered_files(
    template_dir: Path,
    settings: ProjectSettings,
    dry_run: bool,
    logger: logging.Logger,
) -> None:
    """
    Reads template files specified in keys of settings.file_map,
    renders and writes to files specified in values of
    settings.file_map

    args:
        template_dir: location of jinja templates
        settings: settings for the render job
        dry_run: whether to write the files to a temp directory
            or if False to the spark-query src
        logger: for logs
    """
    if dry_run:
        destination_base_path = Path.cwd() / "tmp" / "project_creator_dry_run"
    else:
        destination_base_path = Path.cwd()
    environment = Environment(loader=FileSystemLoader(str(template_dir)))

    def render_and_write(
        template,
        settings,
        destination_base_path,
        write_path,
        settings_env="",
    ):
        output_path = destination_base_path / write_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        rendered_file = render_file(template, settings, settings_env)
        if output_path.exists():
            logger.info(f"skipping {output_path} because file already exists")
        else:
            with output_path.open(mode="w", encoding="utf-8") as f:
                f.write(rendered_file)
            logger.info(f"rendered {template_path} to {output_path}")

    for template_path, write_path in cast(Dict, settings.file_map).items():
        if "settings_env" in write_path:
            for env in SETTINGS_ENVS:
                template = environment.get_template(template_path)
                formatted_write_path = write_path.format(settings_env=env)
                render_and_write(
                    template,
                    settings,
                    destination_base_path,
                    formatted_write_path,
                    env,
                )
        else:
            template = environment.get_template(template_path)
            render_and_write(
                template,
                settings,
                destination_base_path,
                write_path,
            )


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    args = parse_args()

    with Path(args.settings_yaml).open() as f:
        settings_json = yaml.safe_load(f)
    settings = ProjectSettings(**settings_json)
    template_dir = Path("project_templating/templates/")
    write_rendered_files(template_dir, settings, args.dry_run, logger)
    logger.info("Successfully rendered all files!")


if __name__ == "__main__":
    main()
