import json
import logging
import sys
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import tables
import utilz
from config import get_config, logger, setup
from date_util import get_job_date
from metrics import report_job_stats


def __create_job_params_table(
    spark, start_utc, end_utc, iceberg_catalog, iceberg_db
):  # noqa: E501
    """
    Create a temporary view with start and end time for use in SQL queries.
    """
    df = spark.createDataFrame(
        [(start_utc, end_utc)], ["start_time", "end_time"]
    )  # noqa: E501
    table_name = f"{iceberg_catalog}.{iceberg_db}.job_params"
    spark.sql(f"DROP TABLE if exists {table_name}")
    df.writeTo(table_name).using("iceberg").create()
    spark.sql(
        f"select 'job_params_table' as description,* from {table_name}"
    ).show(  # noqa: E501
        100, truncate=False
    )


def process_cubes(configs, job_date: datetime.date, json_path):
    catalog = configs["iceberg"]["catalog_name"]
    db = configs["iceberg"]["db"]

    target_tz = ZoneInfo(
        configs.get("job", "target_timezone", fallback="America/Chicago")
    )
    start_time_local = datetime(
        job_date.year, job_date.month, job_date.day, 0, 0, 0, tzinfo=target_tz
    )
    end_time_local = datetime(
        job_date.year,
        job_date.month,
        job_date.day,
        23,
        59,
        59,
        tzinfo=target_tz,  # noqa: E501
    )

    start_utc = start_time_local.astimezone(ZoneInfo("UTC"))
    end_utc = end_time_local.astimezone(ZoneInfo("UTC"))

    with utilz.get_spark_session(configs, "cubes") as spark:
        with open(json_path, "r") as f:
            config = json.load(f)

        __create_job_params_table(spark, start_utc, end_utc, catalog, db)

        cubelets = config.get("cubelets", [])

        for cubelet in cubelets:
            table_name = cubelet["name"]
            sql = cubelet["sql"]

            print(f"Running cubelet(table): {table_name} with sql: {sql}")
            result_df = spark.sql(sql)

            utilz.debug_dataframes(result_df, "cubelet_result_" + table_name)

            fqtn = f"{catalog}.{db}.{table_name}"
            if not spark.catalog.tableExists(fqtn):
                ddl = tables.tables_ddl[table_name]
                ddl = ddl % (catalog, db)
                logging.info("creating table %s ....", fqtn)
                spark.sql(ddl.lower())

            # Write to Iceberg
            logging.info("ingesting to iceberg cube table %s", fqtn)
            # result_df.writeTo(fqtn).append()
            result_df.writeTo(fqtn).overwritePartitions()


if __name__ == "__main__":
    job_start_time = datetime.now()
    if len(sys.argv) < 2:
        logger.error(
            "usage: python cubes_job.py <config_file_path> <yyyy-mm-dd> "
        )  # noqa: E501
        sys.exit(1)

    config_file_path = sys.argv[1]
    setup(config_file_path)
    config = get_config()
    cubes_json_path = config["cubes"]["json_path"]

    job_date = get_job_date(sys.argv, config)

    logger.info("running cube for date: %s", job_date)
    logger.info("using config file: %s", config_file_path)

    try:
        process_cubes(config, job_date, cubes_json_path)
        report_job_stats(
            "cube",
            job_date,
            job_start_time,
            datetime.now(),
            successful=True,
        )
    except Exception as e:
        logger.error("error during cube processing: %s", str(e))
        traceback.print_exc()
        report_job_stats(
            "cube",
            job_date,
            job_start_time,
            datetime.now(),
            successful=False,
        )
        sys.exit(1)
