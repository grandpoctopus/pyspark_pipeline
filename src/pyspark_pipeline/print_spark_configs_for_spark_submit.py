from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Dict, Optional

import yaml

from pyspark_pipeline.utilities.settings_utils import (
    Settings,
    combine_settings_jsons,
)

CONFIGS_TO_EXCLUDE = [
    "spark.master",
]


def parse_args() -> Namespace:
    parser = ArgumentParser(description="Generate Spark Confs")
    parser.add_argument(
        "-s",
        "--settings-yaml",
        type=str,
        action="store",
        required=True,
        help="required: yaml file specifying job settings",
    )
    parser.add_argument(
        "-O",
        "--override-settings",
        metavar="B",
        type=str,
        action="store",
        default="",
        required=False,
        help="path to yaml for generic settings for the job",
    )
    return parser.parse_args()


def get_spark_configs(
    settings_path: Path, override_settings_path: Optional[Path] = None
) -> Dict:
    """
    Extracts spark_settings from settings yaml

    args:
        settings_path: path to the settings yaml
        override_settings_path: path to a base settings yaml
    """
    settings_yaml_path = Path(settings_path)
    with settings_yaml_path.open() as f:
        settings_json = yaml.safe_load(f)
    if override_settings_path:
        settings_json = combine_settings_jsons(
            settings_json, str(override_settings_path)
        )
    settings_json["table_tag"] = "Default"
    settings = Settings(**settings_json)
    return settings.spark_configs


def get_spark_conf_string(spark_configs: Dict) -> str:
    """
    Converts a dict of spark configs into a string
    for use with spark-submit

    args:
        spark_configs: Dict containing spark configs
            with key as config name and value as config
            setting
    """
    spark_confs = []

    for config_name, value in spark_configs.items():
        if config_name not in CONFIGS_TO_EXCLUDE:
            spark_confs.append(f"--conf {config_name}={value}")

    return " ".join(spark_confs)


def main():
    args = parse_args()
    spark_configs = get_spark_configs(
        args.settings_yaml, args.override_settings
    )
    spark_conf_string = get_spark_conf_string(spark_configs)
    print(spark_conf_string)


if __name__ == "__main__":
    main()
