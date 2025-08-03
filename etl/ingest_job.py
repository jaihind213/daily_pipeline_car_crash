import json
import sys
import traceback
from datetime import datetime

from etl import utilz
from etl.config import get_config, logger, setup
from etl.date_util import get_job_date
from etl.ingest import ingest_to_iceberg
from etl.metrics import report_job_stats
from etl.utilz import get_output_path

if __name__ == "__main__":
    # look in project dir for defaults.
    job_start_time = datetime.now()
    if len(sys.argv) < 2:
        logger.error(
            "usage: python ingest_job.py <config_file_path> <yyyy-mm-dd> "
        )  # noqa: E501
        sys.exit(1)

    config_file_path = sys.argv[1]
    setup(config_file_path)
    config = get_config()

    job_date = get_job_date(sys.argv, config)

    logger.info("ingesting data for date: %s", job_date)
    logger.info("using config file: %s", config_file_path)

    datasets_map = json.loads(config["pull_job"]["datasets"])
    output_path_prefix = config["pull_job"]["raw_data_path"]
    catalog_name = config["iceberg"]["catalog_name"]
    catalog_db = config["iceberg"]["db"]

    with utilz.get_spark_session(config, "ingestor") as spark:
        for table_name, ds_conf in datasets_map.items():
            try:
                id = ds_conf["id"]
                path_to_read = get_output_path(
                    output_path_prefix + "/" + table_name, job_date
                )
                logger.info(
                    "ingesting dataset name: %s, id: %s, path: %s",
                    table_name,
                    id,
                    path_to_read,
                )
                ingest_to_iceberg(
                    spark,
                    output_path_prefix,
                    job_date,
                    catalog_name,
                    catalog_db,
                    table_name,
                    transformer_name="transform_" + table_name,
                )
                report_job_stats(
                    "ingest_" + table_name,
                    job_date,
                    job_start_time,
                    datetime.now(),
                    successful=True,
                    metrics=None,
                )
            except Exception as e:
                logger.error("Error during ingestion: %s", str(e))
                traceback.print_exc()
                report_job_stats(
                    "ingest_" + table_name,
                    job_date,
                    job_start_time,
                    datetime.now(),
                    successful=False,
                )
                sys.exit(1)
