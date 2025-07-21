import os
from datetime import datetime

from pyspark.sql import SparkSession

from ingest import ingest_to_iceberg
from puller import pull_chicago_dataset
from utilz import get_output_path

os.environ["TZ"] = "GMT"
# iceberg_jar = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1"
# iceberg_jar = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.9.0"
iceberg_jar = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,io.github.jaihind213:spark-set-udaf:spark3.5.2-scala2.13-1.0.1-jdk11"  # noqa: E501


def test_ingest_crashes_table():
    """
    Test the ingest_to_iceberg function.
    """
    # Setup
    data_dir = "/tmp/ingest_crashes/"

    catalog = "local"
    db = "test"
    table_name = "crashes"

    spark = (
        SparkSession.builder.appName("insert_into_iceberg")
        .config(
            f"spark.sql.catalog.{catalog}",
            "org.apache.iceberg.spark.SparkCatalog",  # noqa: E501
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",  # noqa: E501
        )
        .config("spark.sql.parquet.timestampNTZ.enabled", "true")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(
            f"spark.sql.catalog.{catalog}.warehouse", "file:///tmp/warehouse"
        )  # noqa: E501
        .config("spark.jars.packages", iceberg_jar)
        .config("spark.sql.session.timeZone", "GMT")
        .getOrCreate()
    )

    job_date = datetime(2024, 4, 20)  # daylight saving time in effect so utc-5

    pull_chicago_dataset(
        domain="data.cityofchicago.org",
        dataset_id="85ca-t3if",
        for_year=job_date.year,
        for_month=job_date.month,
        for_day=job_date.day,
        time_filter_column="crash_date",
        output_dir_path=get_output_path(data_dir + "/" + table_name, job_date),
        job_config=None,
        app_token=os.environ.get("SOCRATA_APP_TOKEN"),
        username=None,
        password=None,
        timeout_sec=10,
        batch_size=101,
        sleep_time_millis=100,
    )

    # Call the function to test
    ingest_to_iceberg(
        spark,
        data_dir,
        job_date,
        catalog,
        db,
        table_name,
        transformer_name="transform_crashes",
    )

    # # Validate the ingestion by checking the table count
    fqtn = f"{catalog}.{db}.{table_name}"
    count = spark.table(fqtn).count()
    assert count == 283, "Ingested table is empty or does not exist"

    # query iceberg
    assert (
        count
        == spark.sql(
            "select count(*) from local.test.crashes "
            "WHERE crash_date BETWEEN TIMESTAMP '2024-04-20 05:00:00' "
            "AND TIMESTAMP '2024-04-21 04:59:59';"
        ).first()[0]
    ), "Ingested table is empty or does not exist"

    crash_record_id = "53613f1aba55c906bc5197787304218287cede4b00a489d984112e7cfb049decdaac73a960405e49a1936723be2cc8f969e7d33f1a84c7643b17e20d2a264559"  # noqa: E501

    # validate 1 record to see if we pulled correctly
    df = spark.sql(
        f"select * from local.test.crashes where crash_record_id='{crash_record_id}'"  # noqa: E501
    )
    row = df.collect()[0]

    # 1
    assert row["crash_record_id"] == crash_record_id
    # 2
    assert row["crash_date_est_i"] is None
    # 3
    assert row["crash_date"] == datetime.strptime(
        "2024-04-20 20:40:00", "%Y-%m-%d %H:%M:%S"
    )
    # 4
    assert row["posted_speed_limit"] == 30
    # 5
    assert row["traffic_control_device"] == "STOP SIGN/FLASHER"
    # 6
    assert row["device_condition"] == "FUNCTIONING PROPERLY"
    # 7
    assert row["weather_condition"] == "CLEAR"
    # 8
    assert row["lighting_condition"] == "DAYLIGHT"
    # 9
    assert row["first_crash_type"] == "ANGLE"
    # 10
    assert row["trafficway_type"] == "FOUR WAY"
    # 11
    assert row["lane_cnt"] is None
    # 12
    assert row["alignment"] == "STRAIGHT AND LEVEL"
    # 13
    assert row["roadway_surface_cond"] == "DRY"
    # 14
    assert row["road_defect"] == "NO DEFECTS"
    # 15
    assert row["report_type"] == "ON SCENE"
    # 16
    assert row["crash_type"] == "NO INJURY / DRIVE AWAY"
    # 17
    assert row["intersection_related_i"] is True
    # 18
    assert row["not_right_of_way_i"] is None
    # 19
    assert row["hit_and_run_i"] is None
    # 20
    assert row["damage"] == "OVER $1,500"
    assert row["damage_estimate"] == 1500
    # 21
    assert row["date_police_notified"] == datetime.strptime(
        "2024-04-20 20:52:00", "%Y-%m-%d %H:%M:%S"
    )
    # 22
    assert row["prim_contributory_cause"] == "FAILING TO YIELD RIGHT-OF-WAY"
    # 23
    assert row["sec_contributory_cause"] == "NOT APPLICABLE"
    # 24
    assert row["street_no"] == 666
    # 25
    assert row["street_direction"] == "W"
    # 26
    assert row["street_name"] == "LAKE ST"
    # 27
    assert row["beat_of_occurrence"] == 1214
    # 28
    assert row["photos_taken_i"] is None
    # 29
    assert row["statements_taken_i"] is None
    # 30
    assert row["dooring_i"] is None
    # 31
    assert row["work_zone_i"] is None
    # 32
    assert row["work_zone_type"] is None
    # 33
    assert row["workers_present_i"] is None
    # 34
    assert row["num_units"] == 2
    # 35
    assert row["most_severe_injury"] == "NO INDICATION OF INJURY"
    # 36
    assert row["injuries_total"] == 0
    # 37
    assert row["injuries_fatal"] == 0
    # 38
    assert row["injuries_incapacitating"] == 0
    # 39
    assert row["injuries_non_incapacitating"] == 0
    # 40
    assert row["injuries_reported_not_evident"] == 0
    # 41
    assert row["injuries_no_indication"] == 3
    # 42
    assert row["injuries_unknown"] == 0
    # 43
    assert row["crash_hour"] == 15
    # 44
    assert row["crash_day_of_week"] == 7
    # 45
    assert row["crash_month"] == 4
    # 46
    assert abs(row["latitude"] - 41.885801756) < 1e-9
    # 47
    assert abs(row["longitude"] + 87.6453278635) < 1e-9
    # 48
    assert row["location"] == "{[-87.64532786346, 41.885801756029], Point}"


def test_udf():
    """
    Test the ingest_to_iceberg function.
    """
    # Setup
    catalog = "local"

    spark = (
        SparkSession.builder.appName("insert_into_iceberg")
        .config(
            f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog"
        )  # noqa: E501
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",  # noqa: E501
        )
        .config("spark.sql.parquet.timestampNTZ.enabled", "true")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(
            f"spark.sql.catalog.{catalog}.warehouse", "file:///tmp/warehouse"
        )  # noqa: E501
        .config("spark.jars.packages", iceberg_jar)
        # .config("spark.jars", jar)
        # .config("spark.driver.extraClassPath", jar)
        # .config("spark.executor.extraClassPath", jar)
        .config("spark.sql.session.timeZone", "GMT")
        .config(
            "spark.driver.extraJavaOptions",
            "-Duser.timezone=GMT  -Dfile.encoding=UTF-8 --illegal-access=permit --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED -XX:+UseG1GC",  # noqa: E501
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Duser.timezone=GMT  -Dfile.encoding=UTF-8 --illegal-access=permit --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED --add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED -XX:+UseG1GC",  # noqa: E501
        )
        .getOrCreate()
    )

    jvm = spark._jvm
    print(jvm.io.github.jaihind213.SetAggregator)

    # Access JVM
    jvm = spark._jvm
    jvm_spark = spark._jsparkSession
    jvm.io.github.jaihind213.SetAggregator.register(jvm_spark, 4096, 9001)

    # create df with random string data
    df = spark.createDataFrame([("a",), ("b",), ("c",)], ["col1"])
    df.createOrReplaceTempView("foo")
    spark.sql(
        "select estimate_set(set_sketch(col1)) as estimate_set from foo"
    ).show(  # noqa: E501
        truncate=False
    )
