from pyspark_pipeline.jobs.sample_job import SampleJob
from pyspark_pipeline.utilities.arg_parse_utils import parse_job_args
from pyspark_pipeline.utilities.job_utils import job_driver
from pyspark_pipeline.utilities.settings_utils import Settings


def main():
    job_driver(
        app_name="sample_job.py",
        arg_parser=parse_job_args,
        job=SampleJob,
        settings_class=Settings,
    )


if __name__ == "__main__":
    main()
