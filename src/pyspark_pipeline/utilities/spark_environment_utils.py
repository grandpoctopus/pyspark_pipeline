import subprocess
from pathlib import Path
from typing import Optional


def get_or_create_spark_home(
    spark_home: str,
    remote_spark_build_path: Optional[str] = None,
) -> str:
    """
    Checks for the existance of an existing spark build
    if not present restrieves it from hdfs. Returns
    the path of the spark build

    args:
        spark_home: optional directory to look for spark build
        remote_spark_build_path: object store location of a
            spark build that will be downloaded and extracted
            if spark_build does not exist
    """
    spark_home_path = Path(spark_home)
    if remote_spark_build_path and not spark_home_path.exists():
        if remote_spark_build_path.startswith("hdfs"):
            subprocess.run(
                [
                    "hdfs",
                    "dfs",
                    "-get",
                    remote_spark_build_path,
                    str(spark_home_path),
                ],
                capture_output=True,
                text=True,
            )
        elif remote_spark_build_path.startswith("s3"):
            subprocess.run(
                [
                    "aws",
                    "s3",
                    "cp",
                    remote_spark_build_path,
                    str(spark_home_path),
                ],
                capture_output=True,
                text=True,
            )
        else:
            raise ValueError(
                "remote_spark_build_path must be either an s3 or hdfs location"
            )

        subprocess.run(
            [
                "tar",
                "xfz",
                str(spark_home_path / remote_spark_build_path),
                "-C",
                str(spark_home_path),
            ],
            capture_output=True,
            text=True,
        )
    assert spark_home_path.exists()

    return str(spark_home_path)
