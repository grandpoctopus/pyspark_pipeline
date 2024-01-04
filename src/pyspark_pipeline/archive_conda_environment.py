from pyspark_pipeline.utilities.spark_utils import get_zipped_conda_env


def main():
    get_zipped_conda_env(rebuild_env=True)


if __name__ == "__main__":
    main()
