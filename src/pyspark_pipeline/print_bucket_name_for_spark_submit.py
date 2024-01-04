from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import yaml

from pyspark_pipeline.utilities.settings_utils import (
    Settings,
    combine_settings_jsons,
)


def parse_args() -> Namespace:
    parser = ArgumentParser(
        description="Extract bucket name from settings file"
    )
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


def get_target_path(
    settings_path: Path, override_settings_path: Optional[Path] = None
) -> str:
    """
    Extracts target_path from settings yaml

    args:
        settings_path: path to the settings yaml
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
    return settings.target_path


def extract_bucket_name(target_path: str) -> str:
    """
    Extracts bucket name from target path

    args:
        target_path: an s3 bucket path
    """
    if not target_path.startswith("s3"):
        raise ValueError("target_path is not an s3 path")
    else:
        return urlparse(target_path, allow_fragments=False).netloc


def main():
    args = parse_args()
    target_path = get_target_path(args.settings_yaml, args.override_settings)
    bucket_name = extract_bucket_name(target_path)
    print(bucket_name)


if __name__ == "__main__":
    main()
