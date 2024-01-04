from unittest import mock

import pytest

from pyspark_pipeline.utilities import arg_parse_utils


@pytest.fixture()
def test_args():
    return [
        "foo",
        "-s",
        "fake/path",
        "-z",
        "another/fake/path",
        "-e",
        "f",
        "-t",
        "fake-tag",
        "-l",
        "debug",
    ]


def test_get_base_arg_parser(test_args):
    parser = arg_parse_utils.get_base_arg_parser()
    with mock.patch("sys.argv", test_args):
        args = parser.parse_args()
    assert args.settings_yaml == "fake/path"
    assert args.rebuild_conda_env is False
    assert args.zipped_environment_path == "another/fake/path"
    assert args.table_tag == "fake-tag"
    assert args.spark_log_level == "debug"
    assert args.override_settings == ""


def test_get_incremental_base_arg_parser(test_args):
    parser = arg_parse_utils.get_incremental_base_arg_parser()
    with mock.patch("sys.argv", test_args):
        args = parser.parse_args()
    assert args.incremental_processing_type is None
    assert args.incremental_load_start_date == ""
    assert args.incremental_load_end_date == ""


def test_parse_base_args(test_args):
    with mock.patch("sys.argv", test_args):
        args = arg_parse_utils.parse_base_args()
    assert args.settings_yaml == "fake/path"
    assert args.rebuild_conda_env is False
    assert args.zipped_environment_path == "another/fake/path"

    assert args.table_tag == "fake-tag"
    assert args.spark_log_level == "debug"


@pytest.mark.parametrize("incremental_type", [("update"), ("changes_only")])
def test_parse_job_args(test_args, incremental_type):
    with mock.patch("sys.argv", test_args):
        args = arg_parse_utils.parse_job_args()
    assert args.incremental_processing_type is None
    test_args += [
        "-I",
        incremental_type,
        "-S",
        "2020-01-01",
        "-E",
        "2021-01-01",
    ]
    with mock.patch("sys.argv", test_args):
        args = arg_parse_utils.parse_job_args()
    assert args.settings_yaml == "fake/path"
    assert args.rebuild_conda_env is False
    assert args.zipped_environment_path == "another/fake/path"

    assert args.table_tag == "fake-tag"
    assert args.spark_log_level == "debug"
    assert args.incremental_processing_type == incremental_type
    assert args.incremental_load_start_date == "2020-01-01"
    assert args.incremental_load_end_date == "2021-01-01"
    assert args.skip_tags is False


def test_incremental_update_choices(test_args):
    test_args += [
        "-I",
        "mountain apple",
        "-S",
        "2020-01-01",
        "-E",
        "2021-01-01",
    ]
    with pytest.raises(SystemExit):
        with mock.patch("sys.argv", test_args):
            arg_parse_utils.parse_job_args()


def test_parse_subset_tables_by_csv_of_ids_args(test_args):
    test_args += ["-N", "fake-subset", "-c", "yet/another/fake/path"]
    with mock.patch("sys.argv", test_args):
        args = arg_parse_utils.parse_subset_tables_by_csv_of_ids_args()
    assert args.settings_yaml == "fake/path"
    assert args.rebuild_conda_env is False
    assert args.zipped_environment_path == "another/fake/path"

    assert args.table_tag == "fake-tag"
    assert args.spark_log_level == "debug"
    assert args.subset_name == "fake-subset"
    assert args.id_csv_path == "yet/another/fake/path"


def test_parse_subset_tables_by_number_of_ids_args(test_args):
    test_args += ["-n", "100"]
    with mock.patch("sys.argv", test_args):
        args = arg_parse_utils.parse_subset_tables_by_number_of_ids_args()
    assert args.settings_yaml == "fake/path"
    assert args.rebuild_conda_env is False
    assert args.zipped_environment_path == "another/fake/path"

    assert args.table_tag == "fake-tag"
    assert args.spark_log_level == "debug"
    assert args.number_of_ids == 100
