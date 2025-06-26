import os
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import transform

os.environ["TZ"] = "GMT"


def test_basic_transform():
    schema = StructType(
        [
            StructField("string", StringType(), True),
            StructField("int", IntegerType(), True),
            StructField("bool", BooleanType(), True),
            StructField("float", FloatType(), True),
            StructField("double", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("long", LongType(), True),
            StructField("bool_i", BooleanType(), True),
        ]
    )
    with SparkSession.builder.appName("test_transform_crashes").master(
        "local[*]"
    ).config("spark.sql.session.timeZone", "GMT").getOrCreate() as spark:
        recs = [
            (
                "a",
                "1",
                "True",
                "1.0",
                "1.0",
                "2023-01-01T17:30:01.000",
                "1362341280",
                "N",
            ),
            (None, None, None, None, None, None, None, None),
        ]
        for idx, rec in enumerate(recs):
            data = [rec]
            pdf = pd.DataFrame(data, columns=schema.names)

            if idx == 1:
                df = spark.createDataFrame(pdf, schema=schema)
            else:
                df = spark.createDataFrame(pdf)

            schema_dict = {field.name: field.name for field in schema.fields}
            # test _i columns
            schema_dict["bool_i"] = "boolean"

            df = transform.basic_transform(
                df, schema_dict, timezone="America/Chicago"
            )  # noqa: E501
            row = df.collect()[0]

            assert row["string"] == rec[0]
            assert row["int"] == (int(rec[1]) if rec[1] is not None else None)
            assert row["bool"] == (
                bool(rec[2]) if rec[2] is not None else None
            )  # noqa: E501
            assert row["float"] == (
                float(rec[3]) if rec[3] is not None else None
            )  # noqa: E501
            assert row["double"] == (
                float(rec[4]) if rec[4] is not None else None
            )  # noqa: E501
            assert row["timestamp"] == (
                datetime.strptime("2023-01-01 23:30:01", "%Y-%m-%d %H:%M:%S")
                if rec[5] is not None
                else None
            )
            assert row["long"] == (1362341280 if rec[6] is not None else None)
            assert row["bool_i"] == (False if rec[2] is not None else None)


def test_transform_crashes():
    with SparkSession.builder.appName("test_transform_crashes").master(
        "local[*]"
    ).config("spark.sql.session.timeZone", "GMT").getOrCreate() as spark:
        recs = [
            ("over $1500", "2023-01-01T17:30:01.000"),
        ]
        pdf = pd.DataFrame(recs, columns=["damage", "crash_date"])
        df = spark.createDataFrame(pdf)
        df = transform.transform_crashes(
            df,
            {"damage": "string", "crash_date": "timestamp"},
            timezone="America/Chicago",
        )
        row = df.collect()[0]
        assert row["damage"] == "over $1500"
        assert row["damage_estimate"] == 1500
        assert row["is_am"] is False
        assert row["crash_date"] == datetime.strptime(
            "2023-01-01 23:30:01", "%Y-%m-%d %H:%M:%S"
        )


def test_transform_crashes_people():
    with SparkSession.builder.appName("test_transform_crashes_people").master(
        "local[*]"
    ).config("spark.sql.session.timeZone", "GMT").getOrCreate() as spark:
        recs = [
            ("DID NOT DEPLOY", "Y"),
        ]
        pdf = pd.DataFrame(recs, columns=["airbag_deployed", "cell_phone_use"])
        df = spark.createDataFrame(pdf)
        df = transform.transform_crashes_people(
            df,
            {"airbag_deployed": "boolean", "cell_phone_use": "boolean"},
            timezone="America/Chicago",
        )
        row = df.collect()[0]
        assert row["airbag_deployed"] is False
        assert row["cell_phone_use"] is True
