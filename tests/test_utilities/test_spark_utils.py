import os

from pyspark import SparkConf

from pyspark_pipeline.utilities.spark_utils import get_spark_conf, set_env_vars


def test_get_spark_conf():
    spark_configs = {
        "config_1": "val_1",
        "config_2": "val_2",
    }

    actual = get_spark_conf(spark_configs)

    expected = SparkConf()
    expected.set("config_1", "val_1")
    expected.set("config_2", "val_2")

    assert actual.__dict__["_conf"] == expected.__dict__["_conf"]


def test_set_env_vars():
    env_vars = {
        "BANANA": "coconut/pineapple.smoothie",
        "MACNUT": "pancackes/are.delicious",
    }
    set_env_vars(env_vars)

    assert os.environ["BANANA"] == "coconut/pineapple.smoothie"
    assert os.environ["MACNUT"] == "pancackes/are.delicious"
