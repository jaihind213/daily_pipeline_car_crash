import logging

from pyspark.sql.functions import col, to_timestamp, to_utc_timestamp, udf
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from etl.utilz import is_am  # noqa: E501
from etl.utilz import get_damage_estimate, get_is_yes_no, was_airbag_deployed

# Define UDF to get damage estimate
get_damage_udf = udf(get_damage_estimate, IntegerType())
is_airbag_deployed = udf(was_airbag_deployed, BooleanType())

is_yes_no = udf(get_is_yes_no, BooleanType())

is_am = udf(is_am, BooleanType())


# Function to map types to PySpark types
def map_type(type_str):
    type_str = type_str.lower()
    if type_str == "string":
        return StringType()
    elif type_str == "int":
        return IntegerType()
    elif type_str == "timestamp":
        return TimestampType()
    elif type_str == "double":
        return DoubleType()
    elif type_str == "float":
        return FloatType()
    elif type_str == "bigint" or type_str == "long":
        return LongType()
    elif type_str == "boolean" or type_str == "bool":
        return BooleanType()
    else:
        raise ValueError(f"unknown type: {type_str}")


def cast_to_datatype(df, schema: dict, timezone="America/Chicago"):
    """
     Cast the dataframe to the specified schema.
    :param df:
    :param schema:
    :return: df
    """
    logging.info("src timezone %s used converting to utc", timezone)
    for column_name, dtype in schema.items():
        col_lower = column_name.lower()
        if col_lower in df.columns:
            if dtype.lower() == "timestamp":
                # Cast localtime zone timestamp with timezone
                df = df.withColumn(
                    col_lower + "_local_tz",
                    to_timestamp(col(col_lower), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                )
                # utc for iceberg
                df = df.withColumn(
                    col_lower + "_casted",
                    to_utc_timestamp(col(col_lower + "_local_tz"), timezone),
                )
            else:
                df = df.withColumn(
                    col_lower + "_casted", df[col_lower].cast(map_type(dtype))
                )
            df = df.withColumnRenamed(col_lower, col_lower + "_original")
            df = df.withColumnRenamed(col_lower + "_casted", col_lower)
            df = df.drop(col_lower + "_original")
        else:
            logging.error(
                f"Column {col_lower} not found in DataFrame. Sometimes socrata api does not return all columns."  # noqa: E501
            )

    return df


def basic_transform(df, schema: dict[str, str], timezone="America/Chicago"):
    """
    Transform the dataframe by renaming columns and casting to the appropriate data types.  # noqa: E501
    pandas df from socrata api always gives string datatype. hence the casting in the function.  # noqa: E501
    :param df:
    :param schema:
    :return: df
    """
    logging.info("applying base transformation")
    columns_with_i = [
        key.lower() for key in schema if key.lower().endswith("_i")
    ]  # noqa: E501

    # example: ['crash_intersection_related_i', 'crash_hit_and_run_i']
    for boolean_column in columns_with_i:
        if boolean_column in df.columns:
            df = df.withColumnRenamed(
                boolean_column, f"{boolean_column}_original"
            )  # noqa: E501
            df = df.withColumn(
                boolean_column, is_yes_no(df[f"{boolean_column}_original"])
            )
    df = cast_to_datatype(df, schema, timezone)

    return df


def transform_crashes(df, schema: dict[str, str], timezone="America/Chicago"):
    """
    Transform the crashes data by applying basic transformations and calculating damage estimates.  # noqa: E501
    :param df:
    :param schema:
    :param timezone:
    :return: df
    """
    logging.info(
        "transforming crashes data using timezone: %s and schema: %s",
        timezone,
        schema,  # noqa: E501
    )
    df = basic_transform(df, schema, timezone)
    df = df.withColumn("damage_estimate", get_damage_udf(df["damage"]))
    return df.withColumn("is_am", is_am(df["crash_date"]))


def transform_crashes_people(
    df, schema: dict[str, str], timezone="America/Chicago"
):  # noqa: E501
    """
    Transform the crashes_people data by applying basic transformations  # noqa: E501
    :param df:
    :param schema:
    :param timezone:
    :return: df
    """
    logging.info(
        "transforming crashes_people data using timezone: %s and schema: %s",
        timezone,
        schema,
    )

    if "airbag_deployed" in df.columns:
        df = df.withColumnRenamed("airbag_deployed", "airbag_deployed_txt")
        df = df.withColumn(
            "airbag_deployed", is_airbag_deployed(df["airbag_deployed_txt"])
        )
    if "cell_phone_use" in df.columns:
        df = df.withColumnRenamed("cell_phone_use", "cell_phone_use_txt")
        df = df.withColumn(
            "cell_phone_use", is_yes_no(df["cell_phone_use_txt"])
        )  # noqa: E501
    df = basic_transform(df, schema, timezone)
    return df


def transform_crashes_vehicles(
    df, schema: dict[str, str], timezone="America/Chicago"
):  # noqa: E501
    """
    Transform the crashes_vehicles data by applying basic transformations  # noqa: E501
    :param df:
    :param schema:
    :param timezone:
    :return: df
    """
    logging.info(
        "transforming crashes_vehicles data using timezone: %s and schema: %s",
        timezone,
        schema,
    )

    df = basic_transform(df, schema, timezone)
    return df


def transform(fn_name, df, schema: dict[str, str], timezone="America/Chicago"):
    transform_fn = globals()[fn_name]
    return transform_fn(df, schema, timezone)
