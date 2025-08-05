import json
import logging
import sys
import traceback
from datetime import datetime

from etl import puller
from etl.config import get_config, setup
from etl.date_util import get_job_date
from etl.metrics import report_job_stats
from etl.utilz import get_output_path

logger = logging.getLogger("pull_data")


def __pull_data(pull_date: datetime.date, konfig) -> dict[str:int]:
    datasets_map = json.loads(konfig["pull_job"]["datasets"])
    output_path_prefix = konfig["pull_job"]["raw_data_path"]
    sleep_time_millis = int(
        konfig.get("pull_job", "sleep_time_millis", fallback="100")
    )  # noqa: E501
    batch_size = int(konfig.get("pull_job", "batch_size", fallback="2000"))
    timeout_sec = int(konfig.get("pull_job", "timeout_sec", fallback="10"))
    domain = konfig.get(
        "pull_job", "domain", fallback="data.cityofchicago.org"
    )  # noqa: E501

    fetched_meta = {}
    # do the pull
    for name, ds_conf in datasets_map.items():
        id = ds_conf["id"]
        time_column = ds_conf["time_column"]
        path_to_save = get_output_path(
            output_path_prefix + "/" + name, pull_date
        )  # noqa: E501
        logger.info(
            "pulling dataset name: %s, id: %s, path: %s",
            name,
            id,
            path_to_save,  # noqa: E501
        )
        num_recs = puller.pull_chicago_dataset(
            id,
            pull_date.year,
            pull_date.month,
            pull_date.day,
            time_column,
            path_to_save,
            konfig,
            app_token=konfig.get(
                "pull_job", "socrata_token", fallback="configure_token"
            ),
            username=konfig.get("pull_job", "socrata_username", fallback=""),
            password=konfig.get("pull_job", "socrata_password", fallback=""),
            timeout_sec=timeout_sec,
            batch_size=batch_size,
            sleep_time_millis=sleep_time_millis,
            domain=domain,
        )
        fetched_meta["num_recs_" + name] = num_recs

    return fetched_meta


if __name__ == "__main__":
    # look in project dir for defaults.
    job_start_time = datetime.now()
    if len(sys.argv) < 2:
        logger.error(
            "usage: python pull_data_job.py <config_file_path> <yyyy-mm-dd> "
        )  # noqa: E501
        sys.exit(1)

    config_file_path = sys.argv[1]
    setup(config_file_path)
    config = get_config()

    date_to_pull = get_job_date(sys.argv, config)

    logger.info("pulling data for date: %s", date_to_pull)
    logger.info("using config file: %s", config_file_path)

    try:
        fetch_meta = __pull_data(date_to_pull, config)
        logger.info("fetched meta: %s", fetch_meta)
        report_job_stats(
            "pull_data_job",
            date_to_pull,
            job_start_time,
            datetime.now(),
            successful=True,
            metrics=fetch_meta,
        )
    except Exception as e:
        logger.error("Error pulling data: %s", e)
        traceback.print_exc()
        report_job_stats(
            "pull_data_job",
            date_to_pull,
            job_start_time,
            datetime.now(),
            successful=False,
        )
        sys.exit(1)
