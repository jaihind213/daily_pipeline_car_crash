import datetime
import os
import re
from typing import Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from tabulate import tabulate


def get_output_path(path_prefix, date: datetime.date):
    """
    Get the output path for the given date.
    :param path_prefix: The prefix for the output path.
    :param date: The date to use in the output path.
    :return: The output path.
    """
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    return os.path.join(
        path_prefix, f"year={year}", f"month={month}", f"day={day}"
    )  # noqa: E501


def get_damage_estimate(value: str):
    """
    Get the damage estimate from the given value.
    :param value:
    :return: min_damage or None
    """
    min, max = parse_damage(value)
    if min is None and max is None:
        return None
    if min is not None and max is not None:
        return (min + max) // 2
    if min is not None and max is None:
        return min
    return ValueError(
        f"Invalid damage estimate. got min: None, max: {max}"
    )  # noqa: E501


def parse_damage(value) -> Tuple[int, int]:
    """
    parse damage string and return min_damage, max_damage
    :param value:
    :return: tuple (min_damage, max_damage) or (None,None)
    """
    if value is None:
        return (None, None)
    value = value.replace(",", "").upper().strip()

    # OVER case
    if value.startswith("OVER"):
        num = int(re.search(r"\d+", value).group())
        return (num, None)  # min_damage is num+1, no max

    # OR LESS case
    if "OR LESS" in value:
        num = int(re.search(r"\d+", value).group())
        return (1, num)  # max_damage is num, no min so min atleast 1

    # Range case (500 - 1000)
    if "-" in value:
        nums = re.findall(r"\d+", value)
        return (int(nums[0]), int(nums[1]))

    # Single number case
    nums = re.findall(r"\d+", value)
    if nums:
        num = int(nums[0])
        return (num, num)

    # Default fallback
    return (None, None)


def get_is_yes_no(value: str) -> bool:
    """
    Check if the value is a yes/no string.
    :param value:
    :return:  true if value is yes, false if value is no else None
    """
    if value is None:
        return None
    return value.strip().lower() in ["y", "true", "1", "yes"]


def was_airbag_deployed(value: str) -> bool:
    """
    check if airbag deployed
    :param value:
    :return: True if airbag deployed, False if not deployed else None
    """
    if value is None:
        return None
    if "deployed" in value.lower():
        return True
    elif "not deploy" in value.lower():
        return False
    return None


def cellphone_used(value: str) -> bool:
    return get_is_yes_no(value)


def is_am(timestamp):
    """
    Check if the given timestamp is in the AM.
    :param timestamp:
    :return: true if timestamp is in the AM, false if in the PM
    """
    return timestamp.hour < 12


def get_spark_session(config, app_name):
    """
    Get a Spark session with the given configuration.
    :param config:
    :return: spark
    """
    spark_configs = config["spark"]
    iceberg_configs = config["iceberg"]
    catalog = iceberg_configs["catalog_name"]
    catalog_type = iceberg_configs["catalog_type"]
    warehouse = iceberg_configs["wh_path"]

    if app_name is None:
        app_name = "chicago-data-pipeline" + "_" + str(datetime.datetime.now())
    builder = SparkSession.builder.appName(app_name)

    builder.config(
        f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog"
    )
    builder.config(f"spark.sql.catalog.{catalog}.type", catalog_type)
    builder.config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)

    for key, value in spark_configs.items():
        builder.config(key, value)
    spark = builder.getOrCreate()

    # Register the set sketch UDFs
    jvm = spark._jvm
    jvm_spark = spark._jsparkSession
    jvm.io.github.jaihind213.SetAggregator.register(jvm_spark, 4096, 9001)
    return spark


def debug_dataframes(spark_df, name):
    """
    Debug the dataframe by showing its schema and first few rows.
    :param spark_df: DataFrame to debug
    :param name: Name of the DataFrame for logging
    :param spark: Spark session
    """
    if not os.environ.get("DEBUG_DATAFRAMES", "true").lower() == "true":
        return

    if not spark_df:
        return

    df1 = spark_df.withColumn("debugging_" + name, F.lit(""))
    df1.printSchema()
    df1.show(5, truncate=False)


def debug_pandas_df(df, name):
    """
    Debug the pandas dataframe by showing its schema and first few rows.
    :param df:
    :param name: name of df
    """
    if not os.environ.get("DEBUG_DATAFRAMES", "true").lower() == "true":
        return

    if df is None:
        return

    df[name] = None
    print(df.dtypes)

    print(
        tabulate(
            df.head(10),
            headers="keys",
            tablefmt="psql",
        )
    )
